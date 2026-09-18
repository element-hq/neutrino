//! `GET /_matrix/federation/v1/make_leave/{roomId}/{userId}` — the first half of
//! the federated leave handshake, where **we are the resident** server. Used by
//! a remote server to reject an invite (or otherwise depart) on behalf of one of
//! its users.
//!
//! Mostly symmetric to [`crate::federation::make_join`]: it returns an
//! `m.room.member`/`leave` template built on the room's current heads, tagged
//! with the room's `room_version`. Unlike make_join there is **no `?ver=`
//! negotiation**: the spec defines no `ver` query for make_leave (a user must
//! always be able to depart a room), so a requester that omits it — as
//! gomatrixserverlib and Synapse do — must not be refused.
//!
//! Unlike make_join there is **no membership / join-rules eligibility
//! pre-check**: the spec does not require make_leave to verify the user is in
//! the room, and `send_leave`'s apply is authoritative — so a user who cannot
//! actually leave is refused there, not here.
//!
//! Like make_join the template carries `prev_events` (timeline forward
//! extremities) + `prev_state_events` (state-DAG forward extremities, MSC4242)
//! but **no `auth_events`** (server-computed at apply). It is stateless — we
//! persist nothing, so an abandoned template pollutes nothing.

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use neutrino_store::RoomStore;
use ruma::{OwnedRoomId, OwnedUserId};
use serde::Serialize;
use serde_json::json;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::make_join::map_build_err;
use crate::federation::{FedError, auth};
use crate::{AppState, lock_app};

/// `make_leave` response: the leave-event `template` plus the room's version.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    /// The unsigned leave template, wire-verbatim (`Event.raw`).
    event: Box<RawJsonValue>,
    room_version: String,
}

/// Federation `/make_leave` handler.
///
/// 1. Parse `room_id` + `user_id` (400 JSON on a malformed id).
/// 2. Room unknown → 404 `M_NOT_FOUND`.
/// 3. Build the `m.room.member`/`leave` template on the current heads (no
///    persist) and return it with `room_version`. No eligibility pre-check —
///    `send_leave`'s apply is authoritative (a user who cannot leave is refused
///    there).
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, user_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ResponseBody>, FedError> {
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;
    let user_id = OwnedUserId::try_from(user_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid user_id"))?;

    let (store, registry, our_name) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.room_registry.clone(),
            app.config.server_name.clone(),
        )
    };

    // A server may only request a leave template for its own users.
    let origin = auth::authenticated_origin(&headers, &our_name)?;
    if origin != user_id.server_name() {
        return Err(FedError::Forbidden(
            "origin server does not own the leaving user",
        ));
    }

    // The room must exist. No `ver` negotiation on leave (see module docs);
    // the requester learns the version from `room_version` in the response.
    let room_version = store
        .get_room_version(&room_id)
        .await?
        .ok_or(FedError::RoomNotFound)?;

    let template = registry
        .build_event(
            &room_id,
            user_id.clone(),
            "m.room.member".to_owned(),
            Some(user_id.to_string()),
            json!({ "membership": "leave" }),
        )
        .await
        .map_err(map_build_err)?;

    Ok(Json(ResponseBody {
        event: template.raw.clone(),
        room_version: room_version.as_str().to_owned(),
    }))
}
