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

use axum::{Json, extract::State, extract::rejection::JsonRejection, http::HeaderMap};
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};

use crate::federation::{FedError, auth, co_sign_if_signed, map_apply_err};
use crate::{AppState, lock_app};

/// Federation `/send_leave` (v2) handler. Returns `{}` on accept.
///
/// The `{roomId}`/`{eventId}` path segments are IGNORED: the event body is
/// authoritative for both (the event id is recomputed from the reference hash,
/// never read), so a transport is free to compress/elide them and send
/// placeholder segments.
pub(crate) async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<Box<RawJsonValue>>, JsonRejection>,
) -> Result<Json<Value>, FedError> {
    let raw = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;

    // Parse + compute the event id from the reference hash. `auth_events`
    // start empty — apply_pdu is their sole authority. Resident membership
    // follows the *local* reject policy (refused, never persisted), so a
    // `Wire::Rejected` leave is a 400 like any other malformed event.
    let event = match state.policy().admit_wire(raw).await {
        Ok(neutrino_event::Wire::Valid(ev)) => ev,
        Ok(neutrino_event::Wire::Rejected(ev, defect)) => {
            tracing::warn!(event_id = %ev.event_id, %defect, "send_leave: refusing Wire::Rejected leave");
            return Err(FedError::BadRequest("malformed leave event"));
        }
        Err(e) => {
            tracing::warn!(error = %e, "send_leave: refusing unparseable leave");
            return Err(FedError::BadRequest("malformed leave event"));
        }
    };
    let room_id = event.room_id.clone();

    // Structural validation (spec §send_leave). The signature check is skipped
    // (no signing keys); the sender-on-origin check is enforced below via the
    // network-attested `X-Matrix` origin. state_key == sender is mandatory, so
    // this endpoint can only ever express a *self*-leave; a kick/ban of another
    // user rides `/send` as a normal PDU instead.
    if event.event_type != "m.room.member" {
        return Err(FedError::BadRequest("event is not an m.room.member event"));
    }
    if event.content_str("membership").as_deref() != Some("leave") {
        return Err(FedError::BadRequest("membership is not leave"));
    }
    if event.state_key.as_deref() != Some(event.sender.as_str()) {
        return Err(FedError::BadRequest("state_key must equal sender"));
    }

    let (registry, our_name) = {
        let app = lock_app(&state);
        (app.room_registry.clone(), app.config.server_name.clone())
    };

    // A server may only send_leave its own user's membership event.
    let origin = auth::authenticated_origin(&headers, &our_name)?;
    if origin != event.sender.server_name() {
        return Err(FedError::Forbidden(
            "origin server does not own the event sender",
        ));
    }

    // Co-sign (signed deployments): the resident signature rides the copy we
    // persist + fan out. The v2 response is an empty object, so the leaver
    // keeps its singly-signed copy — fine, both verify by their sender's-server
    // signature. No-op on a trusted network. Event id unchanged.
    let mut event = event;
    co_sign_if_signed(&state, &mut event)?;

    // Apply through the resident path: accept ⇒ persisted + fanned out; reject
    // ⇒ 403; idempotent re-send ⇒ Ok. (`apply_resident` enqueues the fan-out.)
    // The v2 response is an empty object — no state propagates on leave.
    registry
        .apply_resident(&room_id, event)
        .await
        .map_err(map_apply_err)?;
    Ok(Json(json!({})))
}
