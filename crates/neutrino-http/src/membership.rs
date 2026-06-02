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
use neutrino_store::{RoomStore, StateStore};
use ruma::{OwnedUserId, RoomAliasId, RoomId, UserId};
use serde_json::{Value, json};

use crate::{AppState, AuthUser, error_response, lock_app, room_actor_response};

/// Parse a room id from a path segment, returning a ready 400 response when
/// it is malformed.
// A built HTTP `Response` is the deliberate error payload here (mirroring the
// async helpers below); boxing every client-error response just to satisfy the
// large-Err heuristic would add noise for no real benefit on a per-request path.
#[allow(clippy::result_large_err)]
fn parse_room(room_id: &str) -> Result<ruma::OwnedRoomId, axum::response::Response> {
    room_id.parse().map_err(|e: ruma::IdParseError| {
        error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string())
    })
}

/// Lift the required `user_id` target out of a request body, returning a ready
/// 400 response when it is missing or malformed.
#[allow(clippy::result_large_err)] // see `parse_room`
fn body_target(body: Option<&Value>) -> Result<OwnedUserId, axum::response::Response> {
    let raw = body
        .and_then(|b| b.pointer("/user_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            error_response(
                StatusCode::BAD_REQUEST,
                "M_MISSING_PARAM",
                "Missing required parameter: user_id",
            )
        })?;
    OwnedUserId::try_from(raw)
        .map_err(|e| error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string()))
}

/// Lift an optional `reason` string from the request body.
fn body_reason(body: Option<&Value>) -> Option<String> {
    body?
        .pointer("/reason")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// The current `content.membership` of `target` in `room`, or `None` when the
/// user has no member event. Maps a storage failure to a ready 500 response.
async fn current_membership(
    state: &AppState,
    room: &RoomId,
    target: &UserId,
) -> Result<Option<String>, axum::response::Response> {
    let store = lock_app(state).store.clone();
    let event = store
        .current_state_event(room, "m.room.member", target.as_str())
        .await
        .map_err(|e| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            )
        })?;
    Ok(event
        .and_then(|e| serde_json::from_str::<Value>(e.raw.get()).ok())
        .and_then(|v| {
            v.pointer("/content/membership")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
}

/// Return a ready `404 M_NOT_FOUND` ("Not a known room") when `room` was never
/// created. Mirrors Synapse, which 404s a leave/unban for a room the server is
/// not in (`room_member.py:1135-1152`) before any membership-state check; only
/// a room that *exists* falls through to the no-op / bad-state handling. Maps a
/// storage failure to a ready 500.
#[allow(clippy::result_large_err)] // see `parse_room`
async fn require_room(state: &AppState, room: &RoomId) -> Result<(), axum::response::Response> {
    let store = lock_app(state).store.clone();
    match store.room_exists(room).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(error_response(
            StatusCode::NOT_FOUND,
            "M_NOT_FOUND",
            "Not a known room",
        )),
        Err(e) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        )),
    }
}

/// Emit one `m.room.member` event through the room actor. `target` is the
/// state_key (the user whose membership changes); `membership` is the
/// resulting membership string; `reason`, when present, is copied into
/// content. Returns `Ok(())` on accept, or the actor's standard error response.
async fn change_membership(
    state: &AppState,
    sender: OwnedUserId,
    room: &RoomId,
    target: &UserId,
    membership: &str,
    reason: Option<&str>,
) -> Result<(), axum::response::Response> {
    let registry = lock_app(state).room_registry.clone();
    let mut content = json!({ "membership": membership });
    if let Some(r) = reason {
        content["reason"] = json!(r);
    }
    registry
        .send_event(
            room,
            sender,
            "m.room.member".to_owned(),
            Some(target.to_string()),
            content,
        )
        .await
        .map(|_| ())
        .map_err(room_actor_response)
}

/// `POST /rooms/{roomId}/join` — the caller joins the room. Returns the room
/// id per spec. Re-joining when already `join` is an idempotent `200` with no
/// new event (Synapse `room_member.py:1015-1025`); without this short-circuit
/// every call would stack a duplicate `m.room.member` join into the timeline.
/// Only the `join` state is skipped — `invite`/`leave`/`ban`/absent all fall
/// through so accepting an invite, re-joining after leaving, or a public join
/// still emit an event (and `ban` is left for the auth rules to reject).
pub(crate) async fn join(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    let room = match parse_room(&room_id) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match current_membership(&state.0, &room, &sender).await {
        Ok(Some(m)) if m == "join" => {
            return (StatusCode::OK, Json(json!({ "room_id": room }))).into_response();
        }
        Ok(_) => {}
        Err(resp) => return resp,
    }
    match change_membership(
        &state.0,
        sender.clone(),
        &room,
        &sender,
        "join",
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "room_id": room }))).into_response(),
        Err(resp) => resp,
    }
}

/// `POST /rooms/{roomId}/leave` — the caller leaves the room. A room that was
/// never created is `404 M_NOT_FOUND` (see [`require_room`]). For a room that
/// exists, leaving is only defined from `invite`/`join`/`knock`; from any other
/// state (never joined, already left, banned) the spec treats the call as a
/// no-op success, so the handler short-circuits rather than emit an event the
/// auth rules would reject as an invalid self-leave (rule 5.5.1 → 403).
pub(crate) async fn leave(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    let room = match parse_room(&room_id) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_room(&state.0, &room).await {
        return resp;
    }
    match current_membership(&state.0, &room, &sender).await {
        Ok(Some(m)) if matches!(m.as_str(), "invite" | "join" | "knock") => {}
        Ok(_) => return (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => return resp,
    }
    match change_membership(
        &state.0,
        sender.clone(),
        &room,
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

/// `POST /join/{roomIdOrAlias}` — the global join endpoint most clients (and
/// Complement's `MustJoinRoom`) use; for a room *id* it is the same operation as
/// the room-scoped [`join`], so it delegates straight to it. We have no room
/// directory, so any syntactically valid alias (`#…`) is unresolvable: we report
/// `404 M_NOT_FOUND` ("No such room alias", matching Synapse) rather than the
/// `400` a room-id parse would give, so clients see the alias as *unknown* not
/// *malformed*. A string that is neither a valid id nor a valid alias still
/// falls through to [`join`]'s `400`. The `server_name` query param is accepted
/// and ignored (single-server, trusted mesh).
pub(crate) async fn join_by_id_or_alias(
    state: State<AppState>,
    auth: AuthUser,
    Path(room_id_or_alias): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    if RoomAliasId::parse(&room_id_or_alias).is_ok() {
        return error_response(StatusCode::NOT_FOUND, "M_NOT_FOUND", "No such room alias");
    }
    join(state, auth, Path(room_id_or_alias), body).await
}

/// Shared body for the target-from-body endpoints (`invite`/`kick`/`ban`):
/// resolve the required `user_id` target, lift an optional `reason`, emit the
/// member event, and return `{}` on success.
async fn targeted(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &str,
    body: Option<&Value>,
    membership: &str,
) -> axum::response::Response {
    let target = match body_target(body) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let room = match parse_room(room_id) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let reason = body_reason(body);
    match change_membership(state, sender, &room, &target, membership, reason.as_deref()).await {
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
    )
    .await
}

/// `POST /rooms/{roomId}/unban` — lift a ban on `body.user_id` (membership
/// returns to `leave`). A room that was never created is `404 M_NOT_FOUND` (see
/// [`require_room`]): Synapse treats unban as a leave internally, so it hits the
/// not-a-known-room 404 before any state check. For a room that exists, unban is
/// defined purely as removing a ban, so the target must currently be `ban`:
/// emitting a bare `leave` against a joined user would otherwise be accepted by
/// the auth rules as a *kick* (the kick-vs-unban arm of rule 5.5 is selected
/// from the target's current membership). We pre-check and reject the non-ban
/// case with `403 M_BAD_STATE` (matching Synapse) rather than silently kick.
pub(crate) async fn unban(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let target = match body_target(body) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let room = match parse_room(&room_id) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_room(&state.0, &room).await {
        return resp;
    }
    match current_membership(&state.0, &room, &target).await {
        Ok(Some(m)) if m == "ban" => {}
        Ok(_) => {
            return error_response(
                StatusCode::FORBIDDEN,
                "M_BAD_STATE",
                "Cannot unban a user who is not banned",
            );
        }
        Err(resp) => return resp,
    }
    let reason = body_reason(body);
    match change_membership(&state.0, sender, &room, &target, "leave", reason.as_deref()).await {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => resp,
    }
}
