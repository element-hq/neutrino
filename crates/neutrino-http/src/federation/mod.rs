//! Server-to-server (federation) HTTP handlers.
//!
//! Currently houses [`get_missing_events`] only — see `docs/get-missing-events.md`
//! for the design and the trust-model caveats (no X-Matrix auth, no signature
//! verification, no `min_depth` filter, no history-visibility filter — all
//! deliberate spec deviations under the trusted-mesh assumption).
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

pub(crate) mod get_missing_events;

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
    /// Fixed message — `"room not found"` is returned verbatim in the
    /// response body's `error` field.
    #[error("room not found")]
    RoomNotFound,
    /// The wrapped `StorageError`'s `Display` is rendered into the response
    /// body's `error` field. This is acceptable in Neutrino's trusted-mesh
    /// model; revisit if the server is ever exposed to untrusted peers.
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

impl IntoResponse for FedError {
    fn into_response(self) -> Response {
        let (status, errcode, msg) = match &self {
            FedError::BadRequest(m) => {
                (StatusCode::BAD_REQUEST, "M_INVALID_PARAM", (*m).to_string())
            }
            FedError::RoomNotFound => (
                StatusCode::NOT_FOUND,
                "M_NOT_FOUND",
                "room not found".to_string(),
            ),
            FedError::Storage(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                e.to_string(),
            ),
        };
        (status, Json(json!({"errcode": errcode, "error": msg}))).into_response()
    }
}
