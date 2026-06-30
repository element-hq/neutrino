//! Outbound federated join (joining-server side).
//!
//! When a local user joins a room we don't host, the CSAPI `/join` handler
//! delegates here. We run the handshake against each candidate resident server
//! (`?via=` hints — a v12 room id carries no server, so we cannot derive one):
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
use neutrino_store::{InviteStore, RoomStore, StagingStore, StateStore, StreamPos};
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::federation::client::{FederationClient, FederationClientError, SendJoinResponse};
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
    let (store, worker_poke, own_server, federation_proxy) = {
        let app = lock_app(state);
        (
            app.store.clone(),
            app.worker_poke.clone(),
            app.config.server_name.clone(),
            app.config.federation_proxy.clone(),
        )
    };
    let client = FederationClient::new(own_server, federation_proxy.as_deref());

    // Subscribe to the persist watch *before* staging anything (subscribe-
    // before-query: a persist between staging and subscribing can't be missed),
    // so `wait_for_join` can block on persists instead of polling.
    let mut persists = store.subscribe();

    let mut terminal: Option<JoinFailure> = None;
    for dest in candidates {
        match try_join_via(&client, &*store, &worker_poke, dest, room_id, &user).await {
            Ok(()) => {
                // Staged + worker poked; block until our join lands (or time out).
                return match wait_for_join(&*store, &mut persists, room_id, &user, timeout).await {
                    Ok(()) => {
                        // The join is grounded — drop any out-of-band invite stub
                        // that sourced this join. A lingering stub would make a
                        // later `/leave` route through the OOB-invite *decline*
                        // path (`federation::leave::reject_invite`), which leaves
                        // for the inviting server but never updates our own room
                        // state — so the leaver stays `join` in its own view while
                        // peers see `leave`. Best-effort: a stale stub is otherwise
                        // superseded by the joined state in sync, so a removal
                        // fault must not fail an already-successful join.
                        if let Err(e) = store.remove_invite(room_id, &user).await {
                            warn!(%room_id, %user, error = %e, "failed to clear invite stub after federated join");
                        }
                        (StatusCode::OK, Json(json!({ "room_id": room_id }))).into_response()
                    }
                    Err(resp) => resp,
                };
            }
            Err(f) => {
                warn!(%dest, error = f.reason, "federated join via candidate failed");
                // A 403 is an authoritative auth refusal — once any candidate
                // returns it, keep it rather than let a later unreachable
                // candidate downgrade the client's error back to 502.
                if terminal
                    .as_ref()
                    .is_none_or(|t| t.status != StatusCode::FORBIDDEN)
                {
                    terminal = Some(f);
                }
            }
        }
    }
    let f = terminal.unwrap_or_else(|| gateway("no resident server could be reached"));
    error_response(f.status, f.errcode, f.reason)
}

/// For a room we do NOT host, assemble the candidate resident servers and run a
/// federated join. Candidates are `hints` (explicit `?via=`) followed by the
/// inviter's server when the user holds a pending out-of-band invite — the
/// client cannot supply a `via` for a v12 room id (no domain), so we mirror
/// Synapse (`handlers/room_member.py:1108`) and source it from the invite.
///
/// Trust model: peers in the mesh are implicitly trusted, so the inviter's
/// `server_name` is used as an outbound join target verbatim — there is no
/// loopback / private-range / allowlist check. A hostile invite could point
/// this request at an arbitrary host, but every federated peer is trusted by
/// design (signatures are off for the same reason), so this is intentional, not
/// an oversight.
///
/// Returns:
///   - `Some(_)` when a federated join was attempted (its CSAPI response), or
///     when reading the invite hit a storage fault (surfaced as a `500`), or
///   - `None` when the room is hosted, or no candidate could be sourced — the
///     caller then falls back to the local join path.
pub(crate) async fn federated_join_if_remote(
    state: &AppState,
    user: &UserId,
    room_id: &RoomId,
    hints: &[OwnedServerName],
) -> Option<Response> {
    let (store, own_server) = {
        let app = lock_app(state);
        (app.store.clone(), app.config.server_name.clone())
    };

    let mut candidates: Vec<OwnedServerName> = hints.to_vec();

    // Decide local vs remote join, and source resident candidates:
    //   * Not hosted                     → ordinary remote join (candidates from
    //     the `via` hints + any OOB-invite inviter, below).
    //   * Hosted, ≥1 joined LOCAL member  → our copy is live (we still receive
    //     the room's events) → local join path, return None.
    //   * Hosted, 0 joined local members  → our copy is STALE: federation stops
    //     delivering to a server once its last member leaves, so a re-join must
    //     go through the remote `send_join` handshake (which transfers the
    //     resident's current state DAG and re-syncs us) rather than the local
    //     path, which would build on our stale heads and never reconcile. The
    //     candidates are the other servers we last knew to be joined.
    match store.room_exists(room_id).await {
        Ok(false) => {} // not hosted: ordinary remote join
        Ok(true) => match store.joined_members(room_id).await {
            Ok(members) => {
                let mut have_local_member = false;
                for uid in members.keys() {
                    if uid.server_name().as_str() == own_server.as_str() {
                        have_local_member = true;
                    } else {
                        let srv = uid.server_name().to_owned();
                        if !candidates.contains(&srv) {
                            candidates.push(srv);
                        }
                    }
                }
                // A live local member means our view is current — local join.
                if have_local_member {
                    return None;
                }
            }
            Err(e) => {
                return Some(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                ));
            }
        },
        // A storage fault checking existence falls through to the local path
        // (which surfaces it as a 500), matching the prior behaviour.
        Err(_) => return None,
    }

    // A pending OOB invite supplies the inviter's server as a fallback candidate
    // (the client cannot supply a `via` for a v12 room id). A storage fault is a
    // 500, not silently mistaken for "no invite" — which would 404 a joinable room.
    match store.get_invite(room_id, user).await {
        Ok(Some(invite)) => {
            let inviter = invite.sender.server_name().to_owned();
            if !candidates.contains(&inviter) {
                candidates.push(inviter);
            }
        }
        Ok(None) => {}
        Err(e) => {
            return Some(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            ));
        }
    }

    // Never target ourselves — a stale member list (or a stray hint) could name
    // our own server, and a self-directed make_join would be nonsensical.
    candidates.retain(|c| c.as_str() != own_server.as_str());
    if candidates.is_empty() {
        return None;
    }
    Some(federated_join(state, user.to_owned(), room_id, &candidates).await)
}

/// Why one candidate's join handshake failed, plus how to surface it to the
/// CSAPI client if no candidate succeeds. A remote `403` is an authoritative
/// auth refusal (invite-only / banned) and maps to the spec's `403
/// M_FORBIDDEN`; every other failure (transport, 5xx, version mismatch, ingest)
/// is a gateway failure → `502 M_UNKNOWN`.
struct JoinFailure {
    reason: &'static str,
    status: StatusCode,
    errcode: &'static str,
}

/// A retryable/opaque candidate failure: `502 M_UNKNOWN` carrying `reason`.
fn gateway(reason: &'static str) -> JoinFailure {
    JoinFailure {
        reason,
        status: StatusCode::BAD_GATEWAY,
        errcode: "M_UNKNOWN",
    }
}

/// Map a `make_join`/`send_join` client error into a [`JoinFailure`], promoting
/// a remote `403` into a client-visible `403 M_FORBIDDEN`; everything else is a
/// gateway failure tagged with `reason`.
fn from_client_err(e: FederationClientError, reason: &'static str) -> JoinFailure {
    match e {
        FederationClientError::Status(403) => JoinFailure {
            reason: "resident refused the join",
            status: StatusCode::FORBIDDEN,
            errcode: "M_FORBIDDEN",
        },
        _ => gateway(reason),
    }
}

/// One candidate's handshake: make_join → complete → send_join → ingest. Any
/// failure returns a [`JoinFailure`] so the caller can try the next candidate
/// and, if all fail, surface the most authoritative error to the client.
async fn try_join_via(
    client: &FederationClient,
    store: &(impl RoomStore + StagingStore),
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    dest: &ServerName,
    room_id: &RoomId,
    user: &UserId,
) -> Result<(), JoinFailure> {
    let template = client
        .make_join(dest, room_id, user, ROOM_VERSION_ID)
        .await
        .map_err(|e| from_client_err(e, "make_join request failed"))?;
    if template.room_version != ROOM_VERSION_ID {
        return Err(gateway("resident room version is unsupported"));
    }

    let join =
        crate::federation::complete_membership_template(&template.event, room_id, user, "join")
            .ok_or_else(|| gateway("could not complete the join template"))?;
    let join_id = join.event_id.clone();

    let resp = client
        .send_join(dest, room_id, &join_id, &join.raw)
        .await
        .map_err(|e| from_client_err(e, "send_join request failed"))?;

    ingest_state_dag(store, worker_poke, dest, room_id, resp)
        .await
        .map_err(gateway)
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
    neutrino_engine::stage_and_poke(store, worker_poke, origin, room_id, &events)
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

/// Parse repeated `?via=` (or the pre-1.12 alias `?server_name=`) query values
/// into resident-server candidates. `via` superseded `server_name` in Matrix
/// v1.12; we advertise v1.16, so ruma clients send `via` — accept both.
/// Tolerates a percent-encoded port colon (`%3A`) — the common client encoding;
/// other escapes are left as-is (server names are host[:port], rarely encoded).
pub(crate) fn parse_server_names(raw: Option<&str>) -> Vec<OwnedServerName> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split('&')
        .filter_map(|pair| {
            let (key, val) = pair.split_once('=')?;
            if key != "via" && key != "server_name" {
                return None;
            }
            let decoded = val.replace("%3A", ":").replace("%3a", ":");
            OwnedServerName::try_from(decoded.as_str()).ok()
        })
        .collect()
}
