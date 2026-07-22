//! `PUT /_matrix/federation/v2/send_join/{roomId}/{eventId}` — the second half
//! of the federated join handshake, where **we are the resident** server.
//!
//! The joining server sends the completed `m.room.member`/`join` event. We
//! validate its shape, apply it through the room actor (auth + state-res +
//! persist), fan it out to the other servers in the room (our **distribution
//! duty** — the joiner delivered it only to us), and reply with the room state
//! the joiner needs.
//!
//! ## MSC4242 response
//!
//! Room version 12 + MSC4242 ⇒ the response is `{ state_dag, timeline, event }`
//! and **never** `auth_chain` / `state` / `servers_in_room`:
//! - `state_dag` — the entire state DAG (every state event reachable via
//!   `prev_state_events` from the current state-DAG heads back to the create
//!   event), wire-verbatim so the joiner can recompute reference hashes and run
//!   state resolution itself. No size cap (a large room must still be joinable).
//! - `timeline` — the most recent timeline events, so the joiner has context
//!   and the join event's `prev_events` resolve.
//! - `event` — our copy of the membership event (identical to the request body;
//!   in a signatures world this is where the resident signature would be added).
//!
//! Idempotent: a re-sent `send_join` (our response was lost) re-applies the
//! same join as a no-op and we simply rebuild and return the state again.

use axum::{
    Json,
    extract::rejection::JsonRejection,
    extract::{Path, State},
    http::HeaderMap,
};
use neutrino_store::{DagStore, EventStore, RoomStore};
use ruma::{EventId, OwnedEventId, OwnedRoomId};
use serde::Serialize;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::{FedError, admit_wire, auth, map_apply_err};
use crate::{AppState, lock_app};

/// Recent timeline events to include in the `send_join` response. The joiner
/// backfills deeper history lazily via `/backfill`; this only needs to cover
/// the join event's immediate `prev_events` plus a little context.
const TIMELINE_LIMIT: usize = 20;

/// `send_join` (v2) response — MSC4242 shape. See the module docs.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    /// Every state event in the room's state DAG, wire-verbatim, in no
    /// particular order (the joiner toposorts via `prev_state_events`).
    state_dag: Vec<Box<RawJsonValue>>,
    /// Recent timeline events, wire-verbatim, newest-first.
    timeline: Vec<Box<RawJsonValue>>,
    /// Our copy of the membership event.
    event: Box<RawJsonValue>,
}

/// Federation `/send_join` handler.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, event_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<Box<RawJsonValue>>, JsonRejection>,
) -> Result<Json<ResponseBody>, FedError> {
    let raw = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;

    // Parse + compute the event id from the reference hash. `auth_events`
    // start empty — apply_pdu is their sole authority. Resident membership
    // follows the *local* reject policy (refused, never persisted), so a
    // `Wire::Rejected` join is a 400 like any other malformed event.
    let event = match admit_wire(&state.provenance(), raw).await {
        Ok(neutrino_event::Wire::Valid(ev)) => ev,
        Ok(neutrino_event::Wire::Rejected(ev, defect)) => {
            tracing::warn!(event_id = %ev.event_id, %defect, "send_join: refusing Wire::Rejected join");
            return Err(FedError::BadRequest("malformed join event"));
        }
        Err(e) => {
            tracing::warn!(error = %e, "send_join: refusing unparseable join");
            return Err(FedError::BadRequest("malformed join event"));
        }
    };

    // Structural validation (spec §send_join). The signature check is skipped
    // (no signing keys); the sender-on-origin check is enforced below via the
    // network-attested `X-Matrix` origin. apply_resident remains the real auth.
    if event.event_id.as_str() != event_id {
        return Err(FedError::BadRequest(
            "event_id in path does not match the event",
        ));
    }
    if event.room_id != room_id {
        return Err(FedError::BadRequest(
            "room_id in path does not match the event",
        ));
    }
    if event.event_type != "m.room.member" {
        return Err(FedError::BadRequest("event is not an m.room.member event"));
    }
    if event.content_str("membership").as_deref() != Some("join") {
        return Err(FedError::BadRequest("membership is not join"));
    }
    if event.state_key.as_deref() != Some(event.sender.as_str()) {
        return Err(FedError::BadRequest("state_key must equal sender"));
    }

    let (store, registry, our_name) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.room_registry.clone(),
            app.config.server_name.clone(),
        )
    };

    // A server may only send_join its own user's membership event — the
    // authenticated origin must own the event's sender. Cheap pre-filter;
    // apply_resident's auth rules remain authoritative.
    let origin = auth::authenticated_origin(&headers, &our_name)?;
    if origin != event.sender.server_name() {
        return Err(FedError::Forbidden(
            "origin server does not own the event sender",
        ));
    }

    // Co-sign (PeerAuthenticated deployments): add the resident signature
    // beside the origin's, so the copy we persist + fan out AND the response
    // copy the joiner keeps both carry the two signatures. The event id is
    // unchanged (signatures are outside the reference hash). `None` on a
    // trusted network — the event stays exactly as received.
    let mut event = event;
    if let Some(signer) = state.signer() {
        signer
            .co_sign(&mut event)
            .map_err(|_| FedError::BadRequest("event cannot be co-signed"))?;
    }

    // Keep the wire bytes for the response `event` field before the apply
    // consumes the parsed event.
    let event_raw = event.raw.clone();

    // Apply through the resident path: accept ⇒ persisted + fanned out; reject
    // ⇒ 403; idempotent re-send ⇒ Ok. (`apply_resident` enqueues the fan-out.)
    registry
        .apply_resident(&room_id, event)
        .await
        .map_err(map_apply_err)?;

    // Build the MSC4242 response from the post-apply state.
    let (timeline_fes, state_fes) = store
        .forward_extremities(&room_id)
        .await?
        .ok_or(FedError::RoomNotFound)?;
    let state_dag = collect_state_dag(&*store, &room_id, &state_fes).await?;
    let timeline_refs: Vec<&EventId> = timeline_fes.iter().map(|id| id.as_ref()).collect();
    let timeline =
        crate::federation::events_before_raw(&*store, &room_id, &timeline_refs, TIMELINE_LIMIT)
            .await?;

    Ok(Json(ResponseBody {
        state_dag,
        timeline,
        event: event_raw,
    }))
}

/// The entire state DAG: the current state-DAG heads (`state_fes`) plus every
/// state event reachable from them via `prev_state_events`, back to create.
/// `missing_events(state_dag=true)` walks the ancestry and excludes the seeds,
/// so the heads themselves are fetched separately. No `limit` (whole DAG).
async fn collect_state_dag(
    store: &(impl DagStore + EventStore),
    room_id: &ruma::RoomId,
    state_fes: &std::collections::BTreeSet<OwnedEventId>,
) -> Result<Vec<Box<RawJsonValue>>, FedError> {
    let fe_refs: Vec<&EventId> = state_fes.iter().map(|id| id.as_ref()).collect();
    let mut events = store.get_events(&fe_refs).await?;
    let no_boundary: &[&EventId] = &[];
    let ancestry = store
        .missing_events(room_id, &fe_refs, no_boundary, usize::MAX, true)
        .await?;
    events.extend(ancestry);
    Ok(events.into_iter().map(|e| e.raw).collect())
}
