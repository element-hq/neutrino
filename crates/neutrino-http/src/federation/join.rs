//! Outbound federated join (joining-server side).
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
use neutrino_state::event_id::from_wire;
use neutrino_store::{RoomStore, StagingStore, StateStore, StreamPos};
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::federation::client::{FederationClient, SendJoinResponse};
use crate::{AppState, error_response, lock_app};

/// How long the CSAPI `/join` request blocks waiting for the worker to ground
/// the fetched state DAG and apply our join. On timeout the client gets an
/// error but the drain keeps running (a later sync will show the join).
const JOIN_INGEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Join a room we don't host via the federation handshake, trying each
/// candidate resident server in turn. Returns the CSAPI `/join` response.
pub(crate) async fn federated_join(
    state: &AppState,
    user: OwnedUserId,
    room_id: &RoomId,
    candidates: &[OwnedServerName],
) -> Response {
    federated_join_with(state, user, room_id, candidates, JOIN_INGEST_TIMEOUT).await
}

/// As [`federated_join`], with the ingest-wait timeout injectable so a test can
/// exercise the timeout (504) path without a 20s wall-clock wait.
pub(crate) async fn federated_join_with(
    state: &AppState,
    user: OwnedUserId,
    room_id: &RoomId,
    candidates: &[OwnedServerName],
    timeout: Duration,
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

    // Subscribe to the persist watch *before* staging anything (subscribe-
    // before-query: a persist between staging and subscribing can't be missed),
    // so `wait_for_join` can block on persists instead of polling.
    let mut persists = store.subscribe();

    let mut last_err = "no resident server could be reached";
    for dest in candidates {
        match try_join_via(&client, &*store, &worker_poke, dest, room_id, &user).await {
            Ok(()) => {
                // Staged + worker poked; block until our join lands (or time out).
                return match wait_for_join(&*store, &mut persists, room_id, &user, timeout).await {
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

    let join =
        crate::federation::complete_membership_template(&template.event, room_id, user, "join")
            .ok_or("could not complete the join template")?;
    let join_id = join.event_id.clone();

    let resp = client
        .send_join(dest, room_id, &join_id, &join.raw)
        .await
        .map_err(|_| "send_join request failed")?;

    ingest_state_dag(store, worker_poke, dest, room_id, resp).await
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
        match from_wire(raw, Vec::new()) {
            Ok(ev) => events.push(ev),
            Err(_) => warn!(%room_id, "dropping unparseable event in send_join response"),
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

    // Stage every event + poke the worker (cross-room events are skipped inside;
    // the poke is awaited so a fresh-room ingest can't be silently dropped).
    crate::federation::stage_and_poke(store, worker_poke, origin, room_id, &events)
        .await
        .map_err(|_| "could not stage room state")
}

/// Block until our `join` lands in current state, or time out. Driven by the
/// store's persist watch rather than a fixed poll: only a persist can change
/// current_state, so we re-read state after each persist (any room) instead of
/// spinning. current_state stays the source of truth, so this also catches a
/// join that lands via a concurrent path. On timeout the drain keeps running
/// off the request path, so the client error is recoverable by a later sync.
async fn wait_for_join(
    store: &impl StateStore,
    persists: &mut watch::Receiver<StreamPos>,
    room_id: &RoomId,
    user: &UserId,
    timeout: Duration,
) -> Result<(), Response> {
    let deadline = tokio::time::Instant::now() + timeout;
    let timed_out = || {
        error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "M_UNKNOWN",
            "timed out applying room state; the join is still being processed",
        )
    };
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
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(timed_out());
        }
        // Wait for the next persist, bounded by the deadline. `changed()`
        // coalesces multiple persists into one wakeup — harmless, since the
        // next loop re-reads the full current state.
        match tokio::time::timeout(remaining, persists.changed()).await {
            Ok(Ok(())) => {}
            // Watch sender dropped (store shutting down) — nothing more will land.
            Ok(Err(_)) => {
                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    "store closed while joining",
                ));
            }
            Err(_elapsed) => return Err(timed_out()),
        }
    }
}

/// True if an `m.room.member` event's `content.membership` is `join`.
fn membership_is_join(event: &neutrino_common::Event) -> bool {
    event.content_str("membership").as_deref() == Some("join")
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
