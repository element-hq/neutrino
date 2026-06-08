//! `GET /_matrix/federation/v1/make_leave/{roomId}/{userId}` — the first half of
//! the federated leave handshake, where **we are the resident** server. Used by
//! a remote server to reject an invite (or otherwise depart) on behalf of one of
//! its users.
//!
//! Symmetric to [`crate::federation::make_join`], with one deliberate
//! asymmetry: **make_leave does NOT negotiate the room version.** The spec lists
//! a `ver` query and a `M_INCOMPATIBLE_ROOM_VERSION` error, but the whole point
//! of this endpoint is that a user must always be able to leave a room it is
//! already in — gating on `ver` (whose absent default is `[1]`, which never
//! matches our room version) would refuse legitimate rejections. So we build the
//! template regardless of `ver`. make_join gates; make_leave is lenient.
//!
//! Like make_join the template carries `prev_events` (timeline forward
//! extremities) + `prev_state_events` (state-DAG forward extremities, MSC4242)
//! but **no `auth_events`** (server-computed at apply). It is stateless — we
//! persist nothing, so an abandoned template pollutes nothing.

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_store::RoomStore;
use ruma::{OwnedRoomId, OwnedUserId};
use serde::Serialize;
use serde_json::json;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::FedError;
use crate::federation::make_join::map_build_err;
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
///    persist) and return it with `room_version`. No `ver` gate, no eligibility
///    pre-check — `send_leave`'s apply is authoritative (a user who cannot leave
///    is refused there).
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, user_id)): Path<(String, String)>,
) -> Result<Json<ResponseBody>, FedError> {
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;
    let user_id = OwnedUserId::try_from(user_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid user_id"))?;

    let (store, registry) = {
        let app = lock_app(&state);
        (app.store.clone(), app.room_registry.clone())
    };

    if store.get_room_version(&room_id).await?.is_none() {
        return Err(FedError::RoomNotFound);
    }

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
        room_version: ROOM_VERSION_ID.to_owned(),
    }))
}
