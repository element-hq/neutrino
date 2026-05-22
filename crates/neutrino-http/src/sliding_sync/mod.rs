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
use std::time::Duration;

use neutrino_store::{StorageBackend, StorageError};
use ruma::OneTimeKeyAlgorithm;
use ruma::UInt;
use ruma::UserId;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::AnyToDeviceEvent;
use ruma::serde::Raw;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::Instant;

mod build;
mod conn;

#[cfg(test)]
mod tests;

use conn::{Conn, ConnKey, ConnRegistry};

/// MSC4186 §"Connection Identifier": `conn_id` is max 16 chars on the wire.
const MAX_CONN_ID_LEN: usize = 16;
/// MSC4186 §"Lists": max 100 named lists per request.
const MAX_LISTS: usize = 100;
/// MSC4186 §"Room Subscriptions": max 100 explicit subscriptions per request.
const MAX_ROOM_SUBSCRIPTIONS: usize = 100;
/// Server-side cap on `req.timeout`. MSC4186 doesn't pin a number; 30s
/// matches Synapse's default and is short enough that mobile clients don't
/// keep TCP idle long.
const MAX_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

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
/// Orchestrates the four boundary concerns that don't belong in
/// `build::build_response`:
/// 1. **Validation** — MSC4186 size/length limits up front.
/// 2. **Connection resolution + idempotency** — `resolve_conn` either
///    returns a cached response (retry of a previously-served pos), an
///    unlocked `Arc<Mutex<Conn>>` for a fresh request, or `UnknownPos`.
/// 3. **Long-poll loop** — subscribe to the event watch BEFORE the first
///    build (TOCTOU per the trait docs), then iterate
///    build → has_data?-or-timeout? → `rx.changed()`.
/// 4. **Extension stubs + idempotency cache write** — populate the
///    e2ee/to_device echoes the client opted into, then snapshot the final
///    response into `Conn::last_response` so any retry returns the same
///    bytes.
pub async fn handle<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: v5::Request,
) -> Result<v5::Response, SyncError> {
    validate_request(&req)?;

    let key = ConnKey {
        user_id: user_id.to_owned(),
        conn_id: req.conn_id.clone().unwrap_or_default(),
    };

    let resolution = resolve_conn(&state.registry, key, req.pos.as_deref()).await?;
    let conn_arc = match resolution {
        Resolution::Cached(resp) => return Ok(*resp),
        Resolution::Fresh(arc) => arc,
    };

    // TOCTOU: subscribe BEFORE the first `build_response` so any
    // `persist_event` that lands between our query and the watch
    // registration still wakes us. The trait docs spell this out.
    let mut rx = state.store.subscribe();

    let extensions_req = req.extensions.clone();
    let timeout = clamp_timeout(req.timeout);
    let deadline = Instant::now() + timeout;
    let initial_sync = req.pos.is_none();

    let mut conn_guard = conn_arc.lock().await;
    let mut final_resp = loop {
        let resp = build::build_response(state, user_id, &req, &mut conn_guard).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());

        // Initial sync always returns its full snapshot immediately; the
        // client is loading state, not waiting for live updates. After
        // that, we only return early when the response is non-empty (see
        // `has_data` for the precise definition) or the deadline is up.
        if initial_sync || has_data(&resp) || remaining.is_zero() {
            break resp;
        }

        // No data and time left → wait. `rx.changed()` resolves on the next
        // `persist_event` watch update; the next loop iter rebuilds with
        // the new high-water mark.
        match tokio::time::timeout(remaining, rx.changed()).await {
            Ok(_) => continue,
            Err(_) => break resp,
        }
    };

    populate_extension_stubs(&extensions_req, &mut final_resp);

    // `build_response` chose `conn.pos + 1` as the response's pos_token but
    // didn't mutate `conn.pos` — commit the advance here, once per request,
    // now that we're past every fallible step. If the build loop errored
    // mid-way we'd never reach this, so `conn.pos` would still match the
    // last value the client received.
    conn_guard.pos = conn_guard.pos.saturating_add(1);

    // Idempotency cache: remember the input pos that produced this response
    // (or `None` for initial sync) and snapshot the full final response.
    conn_guard.last_request_pos = req.pos.as_ref().and_then(|s| s.parse::<u64>().ok());
    conn_guard.last_response = Some(final_resp.clone());

    Ok(final_resp)
}

/// Outcome of `resolve_conn` — either an exact retry-cache hit (return the
/// cached response with no further processing) or a writable handle to the
/// conn (we're about to mutate it).
///
/// `Cached` is boxed because `v5::Response` is large (~336 bytes) and we'd
/// otherwise pay that cost on every `Resolution::Fresh` too.
enum Resolution {
    Cached(Box<v5::Response>),
    Fresh(Arc<Mutex<Conn>>),
}

/// Three-way classification of an incoming request against the conn state:
/// - `req.pos == None` → initial sync, always allocate a fresh conn.
/// - `req.pos` matches `conn.last_request_pos` → retry of a previously
///   processed request, return the cached response verbatim.
/// - `req.pos` matches `conn.pos` (the value we last issued in a response)
///   → fresh request, return the unlocked Arc for processing.
/// - Anything else → `UnknownPos`.
async fn resolve_conn(
    registry: &ConnRegistry,
    key: ConnKey,
    req_pos: Option<&str>,
) -> Result<Resolution, SyncError> {
    let Some(pos_str) = req_pos else {
        return Ok(Resolution::Fresh(registry.create(key).await));
    };
    let pos: u64 = pos_str.parse().map_err(|_| SyncError::UnknownPos)?;
    let conn_arc = registry.get(&key).await.ok_or(SyncError::UnknownPos)?;

    // Brief lock to inspect cache + pos, then drop so the caller can
    // re-acquire for the long-poll loop. Could just hold and pass the
    // guard, but the type plumbing for a re-entrant lock is worse than the
    // brief re-lock.
    let guard = conn_arc.lock().await;
    if Some(pos) == guard.last_request_pos
        && let Some(cached) = &guard.last_response
    {
        return Ok(Resolution::Cached(Box::new(cached.clone())));
    }
    if pos != guard.pos {
        return Err(SyncError::UnknownPos);
    }
    drop(guard);
    Ok(Resolution::Fresh(conn_arc))
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

/// Convert ruma's `Option<Duration>` into a deadline-friendly `Duration`,
/// capped at `MAX_LONG_POLL_TIMEOUT`. `None` (and any zero/short value)
/// means "no waiting, return immediately."
fn clamp_timeout(req_timeout: Option<Duration>) -> Duration {
    req_timeout
        .unwrap_or(Duration::ZERO)
        .min(MAX_LONG_POLL_TIMEOUT)
}

/// Whether the response carries any user-visible update worth returning to
/// the client right now (vs. continuing to wait in the long-poll loop).
///
/// **Today's definition is deliberately narrow: `!resp.rooms.is_empty()`.**
/// This is correct *only* for the current scope — the embedded server with
/// stubbed extensions and no EDUs. The following signals do NOT cause this
/// helper to return `true`, even though a fully-spec'd server would have to
/// surface them on the wire:
/// - **OTK / fallback-key changes** (`extensions.e2ee.device_one_time_keys_count`).
/// - **Device-list changes** (`extensions.e2ee.device_lists`).
/// - **New to-device messages** (`extensions.to_device.events`).
/// - **Account-data updates** (`extensions.account_data.*`).
/// - **Receipts / typing / presence**.
/// - **List `count` changes** (a room joining/leaving the candidate set
///   without otherwise being included in `resp.rooms`).
///
/// Why it's safe right now: the e2ee/to_device extensions are pure echo
/// stubs of the request, with constant payload — they never *independently*
/// change between long-poll iterations, so waiting on them would be waiting
/// forever. The other extensions are dropped entirely per CLAUDE.md.
///
/// If any of those signals ever gets a real implementation, this helper is
/// the one place that needs to learn about them — or the loop will hold the
/// connection open for events the response no longer reflects.
fn has_data(resp: &v5::Response) -> bool {
    !resp.rooms.is_empty()
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
