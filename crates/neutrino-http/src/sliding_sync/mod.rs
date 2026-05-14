//! MSC4186 simplified sliding sync — CSAPI handler.
//!
//! Endpoint: `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync`.
//! Generic over `S: StorageBackend` so this compiles against the trait alone;
//! production wiring (mapping `SyncState<SqliteStore>` into the axum router)
//! lands when the sqlite `StorageBackend` impl is finished.
//!
//! Per-connection state lives in `ConnRegistry`; see its docs for the lifecycle
//! and persistence story (short version: in-memory, no expiry yet, lost on
//! restart and recovered via `M_UNKNOWN_POS` → client reconnects).

// Items here are reachable from `tests` but not from the live router yet, which
// would normally trip dead_code. Re-evaluate this allow once the router wiring
// lands.
#![allow(dead_code)]

use std::sync::Arc;

use neutrino_common::storage::{StorageBackend, StorageError};
use ruma::UserId;
use ruma::api::client::sync::sync_events::v5;
use thiserror::Error;

mod build;
mod conn;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;

use conn::ConnRegistry;

#[derive(Debug, Error)]
pub enum SyncError {
    /// Returned as HTTP 400 with errcode `M_UNKNOWN_POS`. Client is expected to
    /// retry without `pos`, which allocates a fresh connection. Triggered when:
    /// the pos doesn't parse, the (user_id, conn_id) pair isn't in the registry
    /// (e.g. server restarted), or the supplied pos isn't the one we last issued
    /// for this conn (client is on a stale token).
    #[error("M_UNKNOWN_POS")]
    UnknownPos,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Per-process state for the sliding-sync handler.
///
/// Holds the shared `StorageBackend` plus the in-memory connection registry.
/// `Arc<S>` because handlers run concurrently across axum tasks and need shared
/// read access. One `SyncState` instance per server.
pub struct SyncState<S> {
    pub store: Arc<S>,
    pub registry: ConnRegistry,
}

impl<S> SyncState<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            registry: ConnRegistry::new(),
        }
    }
}

/// Entry point used by the axum handler (when wired) and by tests.
///
/// TODO(phase-5): wrap this call in a long-poll loop that subscribes to
/// `EventStore::subscribe()` *before* building the first response (TOCTOU per
/// the trait's `subscribe()` docs), then `tokio::select!`s on `rx.changed()`
/// vs. the request's `timeout` to decide whether to return early or wait.
/// TODO(phase-6): stub `req.extensions.e2ee` and `req.extensions.to_device`
/// echoes here so clients that send them don't crash; ignore the rest.
/// TODO(phase-6): validate `req.conn_id.len() <= 16` and reject with
/// `M_BAD_JSON` per MSC4186.
pub async fn handle<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: v5::Request,
) -> Result<v5::Response, SyncError> {
    build::build_response(state, user_id, req).await
}
