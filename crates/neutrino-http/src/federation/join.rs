//! Outbound federated join (Milestone A, joining-server side).
//!
//! When a local user joins a room we don't host, the CSAPI `/join` handler
//! delegates here. We run the handshake against each candidate resident server
//! (`?server_name=` hints — a v12 room id carries no server, so we cannot
//! derive one):
//!
//! 1. `make_join` → a membership-event template on the resident's heads.
//! 2. Complete it (fill ts, recompute the reference-hash id — no signature) and
//!    `send_join` it back.
//! 3. **Ingest** the MSC4242 `state_dag` + `timeline`: register the room from
//!    its create event, then **stage** every event and let the per-room drain
//!    worker apply them through `apply_pdu` (auth + state-res + persist). No DAG
//!    cap; incremental memory; crash-resume is free via `staged_rooms()`.
//!
//! The CSAPI request then blocks (polling current state for our `join`) until
//! the worker grounds the DAG, or times out — on timeout the client errors but
//! the drain keeps running, so a later sync still shows the join.

use std::time::Duration;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_state::event_id::{EventBuilder, from_wire};
use neutrino_store::{RoomStore, StagingStore, StateStore};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId};
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tracing::warn;

use crate::federation::client::{FederationClient, SendJoinResponse};
use crate::{AppState, error_response, lock_app};

/// How long the CSAPI `/join` request blocks waiting for the worker to ground
/// the fetched state DAG and apply our join. On timeout the client gets an
/// error but the drain keeps running (a later sync will show the join).
const JOIN_INGEST_TIMEOUT: Duration = Duration::from_secs(20);
/// Poll interval while waiting for the join to land in current state.
const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Join a room we don't host via the federation handshake, trying each
/// candidate resident server in turn. Returns the CSAPI `/join` response.
pub(crate) async fn federated_join(
    state: &AppState,
    user: OwnedUserId,
    room_id: &RoomId,
    candidates: &[OwnedServerName],
) -> Response {
    let (store, worker_poke, own_server) = {
        let app = lock_app(state);
        (
            app.store.clone(),
            app.worker_poke.clone(),
            app.config.server_name.clone(),
        )
    };
    let client = FederationClient::new(own_server);

    let mut last_err = "no resident server could be reached";
    for dest in candidates {
        match try_join_via(&client, &*store, &worker_poke, dest, room_id, &user).await {
            Ok(()) => {
                // Staged + worker poked; block until our join lands (or time out).
                return match wait_for_join(&*store, room_id, &user).await {
                    Ok(()) => (StatusCode::OK, Json(json!({ "room_id": room_id }))).into_response(),
                    Err(resp) => resp,
                };
            }
            Err(e) => {
                warn!(%dest, error = e, "federated join via candidate failed");
                last_err = e;
            }
        }
    }
    error_response(StatusCode::BAD_GATEWAY, "M_UNKNOWN", last_err)
}

/// One candidate's handshake: make_join → complete → send_join → ingest. Any
/// failure returns a short reason so the caller can try the next candidate.
async fn try_join_via(
    client: &FederationClient,
    store: &(impl RoomStore + StagingStore),
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    dest: &ServerName,
    room_id: &RoomId,
    user: &UserId,
) -> Result<(), &'static str> {
    let template = client
        .make_join(dest, room_id, user, ROOM_VERSION_ID)
        .await
        .map_err(|_| "make_join request failed")?;
    if template.room_version != ROOM_VERSION_ID {
        return Err("resident room version is unsupported");
    }

    let join = complete_join_template(&template.event, room_id, user)
        .ok_or("could not complete the join template")?;
    let join_id = join.event_id.clone();

    let resp = client
        .send_join(dest, room_id, &join_id, &join.raw)
        .await
        .map_err(|_| "send_join request failed")?;

    ingest_state_dag(store, worker_poke, dest, room_id, resp).await
}

/// Rebuild the join event from the resident's template, taking its DAG
/// references (`prev_events` / `prev_state_events`) and our own content/ts.
/// `auth_events` are left empty (`apply_pdu` is their sole authority); the id is
/// the reference hash of the result. `None` if the template is unparseable.
fn complete_join_template(
    template: &RawJsonValue,
    room_id: &RoomId,
    user: &UserId,
) -> Option<neutrino_common::Event> {
    let t: Value = serde_json::from_str(template.get()).ok()?;
    EventBuilder::new(user.to_owned(), "m.room.member".to_owned())
        .room_id(room_id.to_owned())
        .state_key(user.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(id_vec(t.get("prev_events")))
        .prev_state_events(id_vec(t.get("prev_state_events")))
        .build()
        .ok()
}

/// Ingest a `send_join` response: register the room from its create event (if
/// new), then stage every returned event for the worker to apply. The create
/// is staged too — it re-applies as an idempotent no-op.
async fn ingest_state_dag(
    store: &(impl RoomStore + StagingStore),
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    origin: &ServerName,
    room_id: &RoomId,
    resp: SendJoinResponse,
) -> Result<(), &'static str> {
    let mut events = Vec::new();
    for raw in resp
        .state_dag
        .into_iter()
        .chain(resp.timeline)
        .chain(std::iter::once(resp.event))
    {
        if let Ok(ev) = from_wire(raw, Vec::new()) {
            events.push(ev);
        }
    }

    // Register the room from its create event so the actor can bootstrap (the
    // worker drops PDUs for an unknown room). The rest is staged + auth-checked.
    if !store
        .room_exists(room_id)
        .await
        .map_err(|_| "storage error checking room")?
    {
        let create = events
            .iter()
            .find(|e| e.event_type == "m.room.create" && e.state_key.as_deref() == Some(""))
            .ok_or("state DAG is missing the create event")?;
        if create.room_id != *room_id {
            return Err("create event is for a different room");
        }
        store
            .create_room(create, &[])
            .await
            .map_err(|_| "could not register the room")?;
    }

    for ev in &events {
        if ev.room_id != *room_id {
            continue; // never stage a cross-room event the peer slipped in
        }
        store
            .stage_pdu(origin, &ev.room_id, &ev.event_id, &ev.raw)
            .await
            .map_err(|_| "could not stage room state")?;
    }
    let _ = worker_poke.try_send(room_id.to_owned());
    Ok(())
}

/// Block until our `join` lands in current state, or time out. On timeout the
/// drain keeps running off the request path, so the client error is recoverable
/// by a later sync.
async fn wait_for_join(
    store: &impl StateStore,
    room_id: &RoomId,
    user: &UserId,
) -> Result<(), Response> {
    let deadline = tokio::time::Instant::now() + JOIN_INGEST_TIMEOUT;
    loop {
        match store
            .current_state_event(room_id, "m.room.member", user.as_str())
            .await
        {
            Ok(Some(ev)) if membership_is_join(&ev) => return Ok(()),
            Ok(_) => {}
            Err(e) => {
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                ));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "M_UNKNOWN",
                "timed out applying room state; the join is still being processed",
            ));
        }
        tokio::time::sleep(JOIN_POLL_INTERVAL).await;
    }
}

/// Parse a JSON array of event-id strings into owned ids, dropping any that
/// don't parse.
fn id_vec(v: Option<&Value>) -> Vec<OwnedEventId> {
    v.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str())
        .filter_map(|s| OwnedEventId::try_from(s).ok())
        .collect()
}

/// True if an `m.room.member` event's `content.membership` is `join`.
fn membership_is_join(event: &neutrino_common::Event) -> bool {
    serde_json::from_str::<Value>(event.content.get())
        .ok()
        .and_then(|c| {
            c.get("membership")
                .and_then(|v| v.as_str())
                .map(|m| m == "join")
        })
        .unwrap_or(false)
}

/// Parse repeated `?server_name=` query values into resident-server candidates.
/// Tolerates a percent-encoded port colon (`%3A`) — the common client encoding;
/// other escapes are left as-is (server names are host[:port], rarely encoded).
pub(crate) fn parse_server_names(raw: Option<&str>) -> Vec<OwnedServerName> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split('&')
        .filter_map(|pair| {
            let (key, val) = pair.split_once('=')?;
            if key != "server_name" {
                return None;
            }
            let decoded = val.replace("%3A", ":").replace("%3a", ":");
            OwnedServerName::try_from(decoded.as_str()).ok()
        })
        .collect()
}
