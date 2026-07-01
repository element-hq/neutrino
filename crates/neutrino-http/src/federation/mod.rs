//! Server-to-server (federation) HTTP handlers.
//!
//! Houses the inbound + outbound Server-Server handlers (see the submodules
//! below). `X-Matrix` origin auth is network-attested (see [`auth`]); the
//! remaining trust-model caveats are deliberate spec deviations under the
//! trusted-mesh assumption: no signature verification, no `min_depth` filter,
//! no history-visibility filter. See `docs/get-missing-events.md` for design.
//!
//! New federation endpoints land as sibling modules and register their
//! routes in `lib.rs::build_router`.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_store::StorageError;
use serde_json::json;
use thiserror::Error;

pub(crate) mod auth;
pub(crate) mod backfill;
pub(crate) mod client;
pub(crate) mod get_missing_events;
pub(crate) mod invite;
pub(crate) mod join;
pub(crate) mod leave;
pub(crate) mod make_join;
pub(crate) mod make_leave;
pub(crate) mod send;
pub(crate) mod send_join;
pub(crate) mod send_leave;

/// Shared test scaffolding for the federation HTTP tests (`client`).
#[cfg(test)]
pub(crate) mod test_support {
    use axum::Router;
    use ruma::OwnedServerName;

    /// Bind an axum stub on an ephemeral localhost port and return its
    /// `ServerName` (`127.0.0.1:{port}`) — exactly what the outbound resolver
    /// turns into `http://…`. The listener is bound before the task spawns, so
    /// the OS accept queue absorbs an immediate client connect (no readiness
    /// race).
    pub(crate) async fn spawn_stub(app: Router) -> OwnedServerName {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A `ServerName` for a port nothing listens on: bind to grab a free port,
    /// then drop the listener so every connect attempt is refused.
    pub(crate) async fn dead_peer() -> OwnedServerName {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("127.0.0.1:{port}").parse().unwrap()
    }
}

#[cfg(test)]
mod tests;

/// Errors any federation handler can surface to the HTTP layer.
///
/// Mirrors `sliding_sync::SyncError`'s mapping pattern: the variant determines
/// both the HTTP status and the Matrix `errcode` (per spec
/// <https://spec.matrix.org/v1.18/client-server-api/#standard-error-response>).
///
/// - [`FedError::BadRequest`] → 400 `M_INVALID_PARAM`
/// - [`FedError::RoomNotFound`] → 404 `M_NOT_FOUND`
/// - [`FedError::Storage`] → 500 `M_UNKNOWN`
#[derive(Debug, Error)]
pub(crate) enum FedError {
    /// Static reason string. The string is the human-readable detail
    /// returned in the response body's `error` field (per the spec's
    /// `M_INVALID_PARAM` shape).
    #[error("bad request: {0}")]
    BadRequest(&'static str),
    /// 401 `M_UNAUTHORIZED` — the `X-Matrix` authorization header is missing,
    /// malformed, or impersonates this server. (`destination` is parsed but not
    /// enforced — see [`auth`].) See [`auth`] for the network-attested
    /// (non-cryptographic) trust model.
    #[error("unauthorized: {0}")]
    Unauthorized(&'static str),
    /// 403 `M_FORBIDDEN` — the user/server is not permitted to perform the
    /// membership change (e.g. an uninvited user joining an invite-only room,
    /// or a `send_join` the auth rules reject).
    #[error("forbidden: {0}")]
    Forbidden(&'static str),
    /// 400 `M_INCOMPATIBLE_ROOM_VERSION` — `make_join`'s `ver` list does not
    /// include the room's version. The wrapped string is the version we support
    /// and is echoed back in the response body's `room_version` field (the spec
    /// shape for this error), so the caller learns what to offer.
    #[error("incompatible room version (supported: {0})")]
    IncompatibleRoomVersion(String),
    /// Fixed message — `"room not found"` is returned verbatim in the
    /// response body's `error` field.
    #[error("room not found")]
    RoomNotFound,
    /// 500 `M_UNKNOWN` for an internal fault that isn't a storage error (e.g. a
    /// room actor that vanished). The static string is the `error` detail.
    #[error("internal: {0}")]
    Internal(&'static str),
    /// The wrapped `StorageError`'s `Display` is rendered into the response
    /// body's `error` field. This is acceptable in Neutrino's trusted-mesh
    /// model; revisit if the server is ever exposed to untrusted peers.
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

impl IntoResponse for FedError {
    fn into_response(self) -> Response {
        // `M_INCOMPATIBLE_ROOM_VERSION` carries an extra `room_version` field
        // (the spec shape for `/make_join`); every other variant is the plain
        // `{errcode, error}` standard error body.
        if let FedError::IncompatibleRoomVersion(supported) = &self {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "errcode": "M_INCOMPATIBLE_ROOM_VERSION",
                    "error": "room version not supported by the requester",
                    "room_version": supported,
                })),
            )
                .into_response();
        }
        let (status, errcode, msg) = match &self {
            FedError::BadRequest(m) => {
                (StatusCode::BAD_REQUEST, "M_INVALID_PARAM", (*m).to_string())
            }
            FedError::Unauthorized(m) => {
                (StatusCode::UNAUTHORIZED, "M_UNAUTHORIZED", (*m).to_string())
            }
            FedError::Forbidden(m) => (StatusCode::FORBIDDEN, "M_FORBIDDEN", (*m).to_string()),
            FedError::RoomNotFound => (
                StatusCode::NOT_FOUND,
                "M_NOT_FOUND",
                "room not found".to_string(),
            ),
            FedError::Internal(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                (*m).to_string(),
            ),
            FedError::Storage(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                e.to_string(),
            ),
            // Handled above with its bespoke body.
            FedError::IncompatibleRoomVersion(_) => unreachable!(),
        };
        (status, Json(json!({"errcode": errcode, "error": msg}))).into_response()
    }
}

/// Walk `prev_events` back from `heads` (seeds included, newest-first, capped at
/// `limit`) and return the events' wire bytes verbatim. Shared by `/backfill`
/// and `send_join`'s `timeline` (both want "recent PDUs as raw bytes").
pub(crate) async fn events_before_raw(
    store: &impl neutrino_store::DagStore,
    room_id: &ruma::RoomId,
    heads: &[&ruma::EventId],
    limit: usize,
) -> Result<Vec<Box<serde_json::value::RawValue>>, neutrino_store::StorageError> {
    Ok(store
        .events_before(room_id, heads, limit)
        .await?
        .into_iter()
        .map(|e| e.raw)
        .collect())
}

/// Map a `RoomRegistry::apply_resident` error onto the HTTP layer. Shared by the
/// `send_join` and `send_leave` handlers, whose apply step is identical: a policy
/// reject is a 403, an unknown room a 404, a malformed/unauthorisable event a
/// 400, and storage / lost-result faults a 500. The human-readable strings are
/// deliberately membership-agnostic so the two endpoints share one mapping.
pub(crate) fn map_apply_err(err: neutrino_engine::RoomActorError) -> FedError {
    use neutrino_engine::RoomActorError;
    match err {
        RoomActorError::Rejected => {
            FedError::Forbidden("you are not permitted to perform this membership change")
        }
        RoomActorError::UnknownRoom => FedError::RoomNotFound,
        RoomActorError::Storage(e) => FedError::Storage(e),
        // Build/Apply — the event is malformed or unauthorisable against our state.
        RoomActorError::Build(_) | RoomActorError::Apply(_) => {
            FedError::BadRequest("could not authorise the membership event")
        }
        RoomActorError::NotApplied | RoomActorError::ActorGone => {
            FedError::Internal("apply did not produce a result")
        }
    }
}

/// Rebuild an `m.room.member` event from a remote `make_join`/`make_leave`
/// template, taking **only** the template's DAG references (`prev_events` /
/// `prev_state_events`) and setting `type` / `sender` / `state_key` / `content`
/// ourselves. `auth_events` are left empty (the resident computes them at apply);
/// the id is the reference hash of the result.
///
/// **Security — complete, don't echo.** Every authoritative field is set by us;
/// only the two DAG-reference vectors come from the remote template. A naïve
/// completion that echoed the template's `type`/`content`/`state_key`/`sender`
/// would let a malicious resident hand back an arbitrary event for us to author
/// as one of our users (the make_join/make_leave template-completion forgery —
/// worst for leave, where `content` is otherwise unconstrained). Do not
/// "simplify" this into reusing the template's fields; a regression test pins
/// the invariant. `None` if the template is unparseable.
pub(crate) fn complete_membership_template(
    template: &serde_json::value::RawValue,
    room_id: &ruma::RoomId,
    user: &ruma::UserId,
    membership: &str,
) -> Option<neutrino_event::Event> {
    use neutrino_event::event_builder::{EventBuilder, from_wire};
    let raw = serde_json::value::RawValue::from_string(template.get().to_owned()).ok()?;
    let parsed = match from_wire(raw, Vec::new()) {
        Ok(parsed) => parsed,
        Err(e) => {
            tracing::warn!(target: "neutrino_http", %room_id, %user, membership, error = %e, "could not parse the membership template from the resident server");
            return None;
        }
    };
    match EventBuilder::new(user.to_owned(), "m.room.member".to_owned())
        .room_id(room_id.to_owned())
        .state_key(user.to_string())
        .content(json!({ "membership": membership }))
        .prev_events(parsed.prev_events)
        .prev_state_events(parsed.prev_state_events)
        .build()
    {
        Ok(event) => Some(event),
        Err(e) => {
            tracing::warn!(target: "neutrino_http", %room_id, %user, membership, error = %e, "could not build the membership event from the resident's template");
            None
        }
    }
}
