//! `PUT /_matrix/federation/v2/send_leave/{roomId}/{eventId}` — the second half
//! of the federated leave handshake, where **we are the resident** server.
//!
//! The departing server sends the completed `m.room.member`/`leave` event. We
//! validate its shape, apply it through the room actor (auth + state-res +
//! persist), and fan it out to the other servers in the room (our distribution
//! duty — the departing server delivered it only to us).
//!
//! Unlike `send_join`, the v2 response is an **empty object** `{}`: a leave
//! propagates no state (the leaver already had it, or — for an invite rejection
//! — never needs it), so there is no `state_dag` / `timeline` / `auth_chain`.
//!
//! Idempotent: a re-sent `send_leave` (our response was lost) re-applies the
//! same leave as a no-op (`apply_resident` short-circuits empty effects) and we
//! reply `{}` again.

use axum::{
    Json,
    extract::rejection::JsonRejection,
    extract::{Path, State},
};
use neutrino_state::event_id::from_wire;
use ruma::OwnedRoomId;
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};

use crate::federation::FedError;
use crate::room_actor::RoomActorError;
use crate::{AppState, lock_app};

/// Federation `/send_leave` (v2) handler. Returns `{}` on accept.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, event_id)): Path<(String, String)>,
    body: Result<Json<Box<RawJsonValue>>, JsonRejection>,
) -> Result<Json<Value>, FedError> {
    let raw = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;

    // Parse + compute the event id from the reference hash (also runs the
    // format + semantic validators). `auth_events` start empty — apply_pdu is
    // their sole authority.
    let event =
        from_wire(raw, Vec::new()).map_err(|_| FedError::BadRequest("malformed leave event"))?;

    // Structural validation (spec §send_leave). Signature / sender-on-origin
    // checks are skipped (trusted mesh, no X-Matrix origin header). state_key ==
    // sender is mandatory, so this endpoint can only ever express a *self*-leave;
    // a kick/ban of another user rides `/send` as a normal PDU instead.
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
    if event.content_str("membership").as_deref() != Some("leave") {
        return Err(FedError::BadRequest("membership is not leave"));
    }
    if event.state_key.as_deref() != Some(event.sender.as_str()) {
        return Err(FedError::BadRequest("state_key must equal sender"));
    }

    let registry = lock_app(&state).room_registry.clone();

    // Apply through the resident path: accept ⇒ persisted + fanned out; reject
    // ⇒ 403; idempotent re-send ⇒ Ok. (`apply_resident` enqueues the fan-out.)
    match registry.apply_resident(&room_id, event).await {
        Ok(()) => Ok(Json(json!({}))),
        Err(RoomActorError::Rejected) => Err(FedError::Forbidden(
            "user is not allowed to leave this room",
        )),
        Err(RoomActorError::UnknownRoom) => Err(FedError::RoomNotFound),
        Err(RoomActorError::Storage(e)) => Err(FedError::Storage(e)),
        // Build/Apply (e.g. malformed or unauthorisable against our state) — the
        // departing server sent something we can't admit.
        Err(RoomActorError::Build(_) | RoomActorError::Apply(_)) => {
            Err(FedError::BadRequest("could not authorise leave"))
        }
        Err(RoomActorError::NotApplied | RoomActorError::ActorGone) => {
            Err(FedError::Internal("apply did not produce a result"))
        }
    }
}
