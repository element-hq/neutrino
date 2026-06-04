//! Server-to-server (federation) HTTP handlers.
//!
//! Currently houses [`get_missing_events`] only — see `docs/get-missing-events.md`
//! for the design and the trust-model caveats (no X-Matrix auth, no signature
//! verification, no `min_depth` filter, no history-visibility filter — all
//! deliberate spec deviations under the trusted-mesh assumption).
//!
//! New federation endpoints land as sibling modules and register their
//! routes in `lib.rs::build_router`.

use std::time::Duration;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_store::StorageError;
use rand::Rng;
use serde_json::json;
use thiserror::Error;

pub(crate) mod backfill;
pub(crate) mod client;
pub(crate) mod gapfill;
pub(crate) mod get_missing_events;
pub(crate) mod send;
pub(crate) mod sender;
pub(crate) mod worker;

/// Spec maximum PDUs per federation transaction
/// (<https://spec.matrix.org/v1.18/server-server-api/#transactions>). The
/// inbound `/send` handler rejects a transaction carrying more than this; the
/// outbound sender chunks to it. One constant so the two halves can't drift.
pub(crate) const MAX_PDUS_PER_TXN: usize = 50;

/// Backoff floor after a transient failure (outbound delivery, inbound
/// staging). Shared so the two retry loops can't drift.
pub(crate) const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Backoff ceiling. The exponential sequence (1, 2, 4, 8, … s) is clamped here.
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);

/// Double the backoff ceiling, clamped at [`BACKOFF_CAP`].
pub(crate) fn next_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(BACKOFF_CAP)
}

/// Full jitter: a uniform random duration in `[0, ceiling]`. Spreads retries
/// (and startup) so a fleet of senders / a gap-fill loop doesn't thunder a
/// recovering peer in lockstep.
pub(crate) fn jitter(ceiling: Duration) -> Duration {
    let max_ms = ceiling.as_millis() as u64;
    if max_ms == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rand::rng().random_range(0..=max_ms))
}

/// Shared test scaffolding for the federation HTTP tests (`client`, `sender`).
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

/// Milliseconds since the Unix epoch, for the federation transaction
/// `origin_server_ts`. Saturates to 0 on a pre-epoch clock — never panics (no
/// `unwrap` on `SystemTime`). Shared by the inbound `backfill` response and the
/// outbound `client`.
pub(crate) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
