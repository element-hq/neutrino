//! CSAPI `GET /_matrix/client/v3/rooms/{roomId}/messages` — paginated room history.
//!
//! Mirrors Synapse's `RoomMessageListRestServlet` / `PaginationHandler.get_messages`
//! for the parts neutrino has a mechanism for.
//!
//! KNOWN LIMITATIONS (deliberate — no mechanism in neutrino):
//! - The `filter` query param is **accepted but ignored**: event filtering and
//!   `lazy_load_members` are unimplemented, so the optional `state` field is never
//!   emitted.
//! - No history-visibility filtering: a joined user receives the full timeline chunk.
//! - No federation backfill: `dir=b` returns only locally-held events; an empty
//!   `chunk` with no `end` just means the local timeline start was reached.

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use ruma::OwnedRoomId;
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
use serde_json::{Map, Value, json};

use neutrino_store::{Direction, EventStore, PaginationToken};

use crate::membership::current_membership;
use crate::{AppState, AuthUser, error_response, lock_app};

/// Parse `dir`. Absent → Forward (Synapse default; the spec marks it required,
/// we mirror Synapse's leniency). Only `b`/`f` accepted.
// A built HTTP `Response` is the deliberate error payload (mirroring the
// membership helpers); boxing it just to satisfy the large-Err heuristic adds
// noise on a per-request path.
#[allow(clippy::result_large_err)]
fn parse_dir(params: &HashMap<String, String>) -> Result<Direction, axum::response::Response> {
    match params.get("dir").map(String::as_str) {
        Some("b") => Ok(Direction::Backward),
        Some("f") | None => Ok(Direction::Forward),
        Some(other) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "M_INVALID_PARAM",
            &format!("dir must be 'b' or 'f', got '{other}'"),
        )),
    }
}

/// Parse an opaque pagination token (`from`/`to`). Absent, empty, or the legacy
/// `"END"` sentinel → `None`. A non-numeric value, or one exceeding `i64::MAX`
/// (stream positions are stored as `i64`), is client garbage → 400. Bounding
/// here keeps the store's only `room_messages` error a genuine fault, never a
/// malformed-token 500.
#[allow(clippy::result_large_err)] // see `parse_dir`
fn parse_token(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<PaginationToken>, axum::response::Response> {
    match params.get(key).map(String::as_str) {
        None | Some("") | Some("END") => Ok(None),
        Some(s) => match s.parse::<u64>() {
            Ok(n) if i64::try_from(n).is_ok() => Ok(Some(PaginationToken(n))),
            _ => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                &format!("'{key}' parameter is invalid"),
            )),
        },
    }
}

/// Parse `limit`. Absent → 10; capped at 1000 (mirrors Synapse); non-integer → 400.
#[allow(clippy::result_large_err)] // see `parse_dir`
fn parse_limit(params: &HashMap<String, String>) -> Result<usize, axum::response::Response> {
    match params.get("limit") {
        None => Ok(10),
        Some(s) => match usize::from_str(s) {
            Ok(n) => Ok(n.min(1000)),
            Err(_) => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                "'limit' parameter is invalid",
            )),
        },
    }
}

pub(crate) async fn get_messages(
    state: State<AppState>,
    AuthUser(user): AuthUser,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let rid = match OwnedRoomId::try_from(room_id) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    // Parse query params (→ 400) *before* the membership check (→ 403), so a
    // malformed request is rejected as malformed regardless of membership
    // (mirrors Synapse, which builds the PaginationConfig before the room
    // check).
    let dir = match parse_dir(&params) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let from = match parse_token(&params, "from") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let to = match parse_token(&params, "to") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let limit = match parse_limit(&params) {
        Ok(l) => l,
        Err(resp) => return resp,
    };

    // Join check: must be a current member. Not joined — including an unknown
    // room (no member event) — is 403, the spec's only documented error here.
    match current_membership(&state.0, &rid, &user).await {
        Ok(Some(m)) if m == "join" => {}
        Ok(_) => {
            return error_response(
                StatusCode::FORBIDDEN,
                "M_FORBIDDEN",
                "You aren't a member of the room.",
            );
        }
        Err(resp) => return resp,
    }

    let store = lock_app(&state.0).store.clone();

    // `start`: echo `from` if given; else the boundary we paginate from —
    // Forward → "0" (earliest), Backward → this room's stream head (latest).
    // The head is room-scoped (`room_stream_head`), not the global watch
    // position, which could belong to another room.
    let start = match &from {
        Some(t) => t.0.to_string(),
        None => match dir {
            Direction::Forward => "0".to_string(),
            Direction::Backward => match store.room_stream_head(&rid).await {
                Ok(head) => head.0.to_string(),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "M_UNKNOWN",
                        &e.to_string(),
                    );
                }
            },
        },
    };

    let (events, next) = match store.room_messages(&rid, from, to, dir, limit).await {
        Ok(pair) => pair,
        Err(e) => {
            // Room existence is guaranteed by the join check above, so this is
            // a genuine storage fault, not an unknown-room case.
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    // Order is exactly as room_messages returns it: `b` newest-first,
    // `f` oldest-first. No reversal (unlike sliding-sync).
    let chunk: Vec<Raw<AnyTimelineEvent>> =
        events.iter().map(Raw::<AnyTimelineEvent>::from).collect();

    let mut body: Map<String, Value> = Map::new();
    body.insert("chunk".to_string(), json!(chunk));
    body.insert("start".to_string(), Value::String(start));
    if let Some(t) = next {
        body.insert("end".to_string(), Value::String(t.0.to_string()));
    }

    (StatusCode::OK, axum::Json(Value::Object(body))).into_response()
}
