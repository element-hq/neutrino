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

use neutrino_store::{StorageBackend, StorageError};
use ruma::OneTimeKeyAlgorithm;
use ruma::UInt;
use ruma::UserId;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::AnyToDeviceEvent;
use ruma::serde::Raw;
use thiserror::Error;

mod build;
mod conn;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;

use conn::ConnRegistry;

/// MSC4186 §"Connection Identifier": `conn_id` is max 16 chars on the wire.
const MAX_CONN_ID_LEN: usize = 16;
/// MSC4186 §"Lists": max 100 named lists per request.
const MAX_LISTS: usize = 100;
/// MSC4186 §"Room Subscriptions": max 100 explicit subscriptions per request.
const MAX_ROOM_SUBSCRIPTIONS: usize = 100;

#[derive(Debug, Error)]
pub enum SyncError {
    /// Returned as HTTP 400 with errcode `M_UNKNOWN_POS`. Client is expected to
    /// retry without `pos`, which allocates a fresh connection. Triggered when:
    /// the pos doesn't parse, the (user_id, conn_id) pair isn't in the registry
    /// (e.g. server restarted), or the supplied pos isn't the one we last issued
    /// for this conn (client is on a stale token).
    #[error("M_UNKNOWN_POS")]
    UnknownPos,
    /// Returned as HTTP 400 with errcode `M_BAD_JSON`. Triggered by violations
    /// of MSC4186's size/length limits (`conn_id` over 16 chars, over 100
    /// lists, or over 100 room subscriptions). The string is the
    /// human-readable reason for logging/debugging; clients only see the
    /// errcode.
    #[error("bad request: {0}")]
    BadRequest(&'static str),
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
/// Performs MSC4186 request-shape validation up front (size/length limits),
/// delegates the actual sync work to `build_response`, then post-processes
/// the response to populate extension echoes for clients that opted in via
/// `req.extensions.e2ee.enabled` / `req.extensions.to_device.enabled`. The
/// actual extension data is **stubbed** — no real to-device queue, no real
/// device-key bookkeeping — because EDUs and E2EE are out of scope per
/// CLAUDE.md. The echoes exist only to keep clients (Element et al.) from
/// crashing on missing fields they expect.
///
/// TODO(phase-5): wrap this call in a long-poll loop that subscribes to
/// `EventStore::subscribe()` *before* building the first response (TOCTOU per
/// the trait's `subscribe()` docs), then `tokio::select!`s on `rx.changed()`
/// vs. the request's `timeout` to decide whether to return early or wait.
pub async fn handle<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: v5::Request,
) -> Result<v5::Response, SyncError> {
    validate_request(&req)?;

    let extensions_request = req.extensions.clone();
    let mut resp = build::build_response(state, user_id, req).await?;
    populate_extension_stubs(&extensions_request, &mut resp);
    Ok(resp)
}

/// MSC4186 shape limits applied at the request boundary. Anything that
/// violates these returns `BadRequest` → HTTP 400 / `M_BAD_JSON` when the
/// router wires it up.
fn validate_request(req: &v5::Request) -> Result<(), SyncError> {
    if let Some(id) = &req.conn_id
        && id.len() > MAX_CONN_ID_LEN
    {
        return Err(SyncError::BadRequest("conn_id exceeds 16 chars"));
    }
    if req.lists.len() > MAX_LISTS {
        return Err(SyncError::BadRequest("too many lists (max 100)"));
    }
    if req.room_subscriptions.len() > MAX_ROOM_SUBSCRIPTIONS {
        return Err(SyncError::BadRequest(
            "too many room_subscriptions (max 100)",
        ));
    }
    Ok(())
}

/// Echo the e2ee / to_device extensions when the client opted in.
///
/// The data is fake: we don't track device keys, one-time keys, or to-device
/// messages. The point is *shape* — Element and similar clients abort if
/// these fields are absent on a sync they opted in to. Matches the legacy
/// `sync()` handler's stub semantics.
fn populate_extension_stubs(req_ext: &v5::request::Extensions, resp: &mut v5::Response) {
    if req_ext.e2ee.enabled == Some(true) {
        let mut e2ee = v5::response::E2EE::default();
        // Constant "we have 100 OTKs of this type" stub. Real number tracking
        // would require an OTK store, which is out of scope (CLAUDE.md).
        e2ee.device_one_time_keys_count
            .insert(OneTimeKeyAlgorithm::SignedCurve25519, UInt::from(100u32));
        e2ee.device_unused_fallback_key_types = Some(vec![OneTimeKeyAlgorithm::SignedCurve25519]);
        resp.extensions.e2ee = e2ee;
    }
    if req_ext.to_device.enabled == Some(true) {
        // ruma v5's response types are `#[non_exhaustive]`; build via Default
        // and field assignment rather than struct literal.
        let mut to_device = v5::response::ToDevice::default();
        to_device.next_batch = "0".to_string();
        to_device.events = Vec::<Raw<AnyToDeviceEvent>>::new();
        resp.extensions.to_device = Some(to_device);
    }
}
