//! CSAPI membership-change endpoints (testing scope). Each handler emits one
//! `m.room.member` state event through the room actor; authorisation (v12
//! rule 5), state resolution, and persistence all happen inside
//! [`crate::room_actor::RoomRegistry::send_event`] unchanged. See
//! `docs/superpowers/specs/2026-06-02-membership-endpoints-design.md`.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use ruma::{OwnedRoomId, OwnedUserId};
use serde_json::{Value, json};

use crate::{AppState, AuthUser, error_response, lock_app, room_actor_response};

/// Emit one `m.room.member` event through the room actor. `target` is the
/// state_key (the user whose membership changes); `membership` is the
/// resulting membership string; `reason`, when present, is copied into
/// content. Returns `Ok(())` on accept, or a ready HTTP error response
/// (400 for a bad room id, otherwise the actor's standard mapping).
async fn change_membership(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &str,
    target: &OwnedUserId,
    membership: &str,
    reason: Option<&str>,
) -> Result<(), axum::response::Response> {
    let registry = lock_app(state).room_registry.clone();
    let room: OwnedRoomId = match room_id.parse() {
        Ok(r) => r,
        Err(e) => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                &e.to_string(),
            ));
        }
    };
    let mut content = json!({ "membership": membership });
    if let Some(r) = reason {
        content["reason"] = json!(r);
    }
    registry
        .send_event(
            &room,
            sender,
            "m.room.member".to_owned(),
            Some(target.to_string()),
            content,
        )
        .await
        .map(|_| ())
        .map_err(room_actor_response)
}

/// Lift an optional `reason` string from the request body.
fn body_reason(body: Option<&Value>) -> Option<String> {
    body?
        .pointer("/reason")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// `POST /rooms/{roomId}/join` — the caller joins the room. Returns the room
/// id per spec.
pub(crate) async fn join(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    match change_membership(
        &state.0,
        sender.clone(),
        &room_id,
        &sender,
        "join",
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "room_id": room_id }))).into_response(),
        Err(resp) => resp,
    }
}

/// `POST /rooms/{roomId}/leave` — the caller leaves the room.
pub(crate) async fn leave(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    match change_membership(
        &state.0,
        sender.clone(),
        &room_id,
        &sender,
        "leave",
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => resp,
    }
}

/// Shared body for the target-from-body endpoints (`invite`/`kick`/`ban`/
/// `unban`): resolve the required `user_id` target, optionally lift `reason`,
/// emit the member event, and return `{}` on success.
async fn targeted(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &str,
    body: Option<&Value>,
    membership: &str,
    with_reason: bool,
) -> axum::response::Response {
    let raw = match body
        .and_then(|b| b.pointer("/user_id"))
        .and_then(Value::as_str)
    {
        Some(s) => s,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "M_MISSING_PARAM",
                "Missing required parameter: user_id",
            );
        }
    };
    let target = match OwnedUserId::try_from(raw) {
        Ok(u) => u,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };
    let reason = if with_reason { body_reason(body) } else { None };
    match change_membership(
        state,
        sender,
        room_id,
        &target,
        membership,
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => resp,
    }
}

/// `POST /rooms/{roomId}/invite` — invite `body.user_id` to the room.
pub(crate) async fn invite(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "invite",
        true,
    )
    .await
}

/// `POST /rooms/{roomId}/kick` — force `body.user_id` to `leave`.
pub(crate) async fn kick(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "leave",
        true,
    )
    .await
}

/// `POST /rooms/{roomId}/ban` — ban `body.user_id` from the room.
pub(crate) async fn ban(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "ban",
        true,
    )
    .await
}

/// `POST /rooms/{roomId}/unban` — lift a ban on `body.user_id` (membership
/// returns to `leave`). The unban-vs-kick auth arm (rule 5.5.3 vs 5.5.4) is
/// selected by `RoomCore` from the target's current membership, so this emits
/// the same `leave` membership as `kick`.
pub(crate) async fn unban(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "leave",
        false,
    )
    .await
}
