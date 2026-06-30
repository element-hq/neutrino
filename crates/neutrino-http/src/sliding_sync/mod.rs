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

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use neutrino_event::event_view::StateEventConversionError;
use neutrino_store::{StorageBackend, StorageError};
use ruma::OneTimeKeyAlgorithm;
use ruma::UInt;
use ruma::UserId;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::AnyToDeviceEvent;
use ruma::serde::Raw;
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

mod build;
mod conn;

#[cfg(test)]
mod tests;

use conn::{ConnEntry, ConnKey, ConnRegistry};

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
    /// Returned as HTTP 400 with errcode `M_INVALID_PARAM`. Triggered by
    /// violations of MSC4186's size/length limits (`conn_id` over 16 chars,
    /// over 100 lists, or over 100 room subscriptions) — the JSON parses
    /// fine but the semantic constraints fail. The string is the
    /// human-readable reason for logging/debugging; clients only see the
    /// errcode.
    #[error("bad request: {0}")]
    BadRequest(&'static str),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    /// Surfaced when a stored `Event` cannot be reshaped for client delivery
    /// (e.g. a row claiming to carry a state event has `state_key = NULL`).
    /// Mapped to HTTP 500 `M_UNKNOWN` at the handler boundary — every
    /// reachable case is a storage-layer invariant violation, not bad input.
    #[error("event conversion: {0}")]
    EventConversion(#[from] StateEventConversionError),
}

/// Per-process state for the sliding-sync handler.
///
/// Holds the shared `StorageBackend` plus the in-memory connection registry.
/// `Arc<S>` because handlers run concurrently across axum tasks and need shared
/// read access. One `SyncState` instance per server.
pub struct SyncState<S> {
    pub store: Arc<S>,
    pub registry: ConnRegistry,
    /// Shared shutdown latch. When fired (via `CancellationToken::cancel`),
    /// every in-flight long-poll breaks out of its idle select so the connection
    /// is released promptly instead of waiting up to 30 s for its deadline.
    shutdown: CancellationToken,
}

impl<S> SyncState<S> {
    pub fn new(store: Arc<S>, shutdown: CancellationToken) -> Self {
        Self {
            store,
            registry: ConnRegistry::new(),
            shutdown,
        }
    }
}

/// Entry point used by the axum handler and by tests.
///
/// Orchestrates the boundary concerns that don't belong in
/// `build::build_response`:
/// 1. **Validation** — MSC4186 size/length limits up front.
/// 2. **Entry resolution + concurrent-request cancellation** — initial
///    sync allocates a fresh entry (cancelling any prior); a delta looks
///    up the existing entry and bumps its cancel signal so an in-flight
///    long-poll on the same `(user_id, conn_id)` wakes promptly and
///    releases the conn lock.
/// 3. **Cache hit / pos validation** — under the conn lock, either return
///    a previously-served response (byte-identical retry) or proceed.
/// 4. **Long-poll loop** — subscribe to the event watch BEFORE the first
///    build (TOCTOU per the trait docs), then iterate
///    build → has_data?-or-timeout?-or-cancelled? → `rx.changed()`.
/// 5. **Extension stubs + idempotency cache write** — populate the
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

    let req_hash = request_body_hash(&req);

    // Parse `pos` up-front so a garbage value fails fast without touching
    // the registry.
    let parsed_pos: Option<u64> = match req.pos.as_deref() {
        None => None,
        Some(s) => Some(s.parse().map_err(|_| SyncError::UnknownPos)?),
    };

    // Resolve the conn entry, with cancellation of any in-flight long-poll
    // on the same `(user_id, conn_id)`:
    //
    // - **Initial sync** (`pos == None`): allocate a fresh entry. If a prior
    //   entry existed for this key, `create` cancels it on the way in so the
    //   orphaned long-poll wakes promptly instead of running its full timeout.
    // - **Delta** (`pos == Some(_)`): look up the existing entry and bump its
    //   cancel signal *before* we queue on the conn lock. Otherwise we'd
    //   block behind the in-flight request's `rx.changed()` await for up to
    //   30 s — MSC3575 forbids concurrent same-conn requests; we cancel
    //   the older one rather than queue, matching the standard handling.
    // Resolve the entry and capture the cancellation generation that marks
    // "this is me, current as of this point". Subsequent code waits for
    // bumps strictly greater than `my_gen`, which is race-proof against a
    // third concurrent request arriving between `cancel()` and the
    // long-poll select: any bump beyond `my_gen` wakes us regardless of
    // when the receiver subscribes.
    let (entry, my_gen): (ConnEntry, u64) = match parsed_pos {
        None => {
            // `registry.create` cancels any prior entry on the way in and
            // returns a fresh `ConnEntry` whose `cancel_gen` starts at 0.
            // We don't bump it ourselves — we're the new entry — so our
            // baseline is whatever the fresh sender's current value is.
            let entry = state.registry.create(key.clone()).await;
            let baseline = entry.current_gen();
            (entry, baseline)
        }
        Some(_) => {
            let Some(entry) = state.registry.get(&key).await else {
                return Err(SyncError::UnknownPos);
            };
            // `cancel()` returns the post-bump value, which is our `my_gen`:
            // the long-poll select will wake on any value strictly greater
            // than this, i.e. any later request's `cancel()` bump.
            let baseline = entry.cancel();
            (entry, baseline)
        }
    };

    let mut cancel_rx = entry.subscribe_cancel();

    // TOCTOU: subscribe to the event watch BEFORE the first `build_response`
    // so any `persist_event` that lands between our query and the watch
    // registration still wakes us. The trait docs spell this out.
    let mut rx = state.store.subscribe();

    // Short wait while the prior holder (if any) observes the cancel above
    // and unwinds.
    let mut conn_guard = entry.conn.lock().await;

    // Cache hit / pos validation, formerly the body of `resolve_conn`. Has
    // to run under the conn lock — `last_request_pos`, `last_request_hash`,
    // `last_response`, and `pos` all live inside the mutex.
    //
    // Note the ordering: cache-hit short-circuits *after* we paid the cost
    // of bumping cancel on the entry. A byte-identical retry that arrives
    // during an in-flight long-poll therefore forces the in-flight to
    // complete early (treating the cancellation as a timeout: it advances
    // pos, writes the cache, releases the lock); we then pick up the
    // freshly-written cached response. One round-trip's worth of "force
    // the holder to flush" rather than blocking for the full timeout.
    // Cache check is gated on `parsed_pos.is_some()`, i.e. delta syncs.
    // Initial-sync retries (`pos = None`) deliberately bypass the cache and
    // pay full re-processing — initial sync's cost is bounded by the size
    // of joined+invited rooms (a single-user embedded server, so small)
    // and we'd rather take the duplicate work than complicate the cache
    // key to also distinguish "fresh initial" from "retried initial". The
    // client orphans the previous initial-sync `pos` token; they re-init
    // from scratch on the next request and converge.
    if let Some(pos) = parsed_pos {
        if Some(pos) == conn_guard.last_request_pos
            && req_hash == conn_guard.last_request_hash
            && let Some(cached) = &conn_guard.last_response
        {
            return Ok(cached.clone());
        }
        if pos != conn_guard.pos {
            return Err(SyncError::UnknownPos);
        }
    }

    let extensions_req = req.extensions.clone();
    let timeout = clamp_timeout(req.timeout);
    let deadline = Instant::now() + timeout;
    let initial_sync = req.pos.is_none();
    // Pulled out so the loop's break condition reads as a single boolean
    // rather than entangling "initial sync" with "non-empty response" with
    // "zero remaining wall time". `wait_for_data` is the only branch that
    // can iterate; everything else exits after one build.
    let wait_for_data = !initial_sync && !timeout.is_zero();

    let mut final_resp = loop {
        let resp = build::build_response(state, user_id, &req, &mut conn_guard).await?;
        if !wait_for_data || has_data(&resp) {
            break resp;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break resp;
        }
        // Four-way race in the idle wait (evaluated biased, in order):
        // - `state.shutdown.cancelled()` fires → server is shutting down;
        //   break with the current (empty) resp so the long-poll releases
        //   promptly instead of holding open for up to 30 s.
        // - `cancel_rx.wait_for(|g| *g > my_gen)` fires → a strictly-later
        //   request has called `cancel()` on our entry (or the registry
        //   replaced our entry on an initial sync, which also bumps our
        //   gen via `prior.cancel()`); break out with the current resp.
        //   The tail below advances `conn.pos` and writes the idempotency
        //   cache, so a byte-identical newer request immediately hits the
        //   cache; a body-differing newer request will see the advanced
        //   `conn.pos` and either match it (process fresh) or return
        //   UnknownPos.
        // - `rx.changed()` fires → new event(s); rebuild with the advanced
        //   high-water mark.
        // - `tokio::time::timeout` elapses → return the current empty resp.
        tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => break resp,            // server shutting down
            _ = cancel_rx.wait_for(|g| *g > my_gen) => break resp,
            res = tokio::time::timeout(remaining, rx.changed()) => match res {
                Ok(_) => continue,
                Err(_) => break resp,
            },
        }
    };

    populate_extension_stubs(&extensions_req, &mut final_resp);

    // `build_response` chose `conn.pos + 1` as the response's pos_token but
    // didn't mutate `conn.pos` — commit the advance here, once per request,
    // now that we're past every fallible step. If the build loop errored
    // mid-way we'd never reach this, so `conn.pos` would still match the
    // last value the client received.
    conn_guard.pos = conn_guard.pos.saturating_add(1);

    // Idempotency cache: remember the input pos (or `None` for initial
    // sync), the body hash, and the full final response. A retry only
    // hits the cache when *both* pos and hash match — anything else
    // (different `timeline_limit`, newly-opted-into extension) must be
    // re-processed.
    conn_guard.last_request_pos = parsed_pos;
    conn_guard.last_request_hash = req_hash;
    conn_guard.last_response = Some(final_resp.clone());

    Ok(final_resp)
}

/// Hash the body fields of a sliding-sync request: everything that
/// influences the response shape *except* `pos`, `timeout`, and
/// `set_presence` (the latter is dropped on the floor; the first two
/// don't change the response content, only its timing / cursor).
///
/// Used as the second half of the idempotency cache key — a "retry" with
/// the same `pos` but a different body (e.g. an extension was just
/// enabled) must be re-processed, not served the stale response.
///
/// We hash the canonical JSON serialisation rather than implementing
/// `Hash` on every ruma field — the field types are `#[non_exhaustive]`
/// and don't all derive `Hash`, but they all derive `Serialize`. A
/// serialisation failure falls back to `0` which trivially won't match
/// the stored hash, so the request gets re-processed; that's the safe
/// direction.
fn request_body_hash(req: &v5::Request) -> u64 {
    #[derive(serde::Serialize)]
    struct BodyView<'a> {
        conn_id: &'a Option<String>,
        txn_id: &'a Option<String>,
        lists: &'a std::collections::BTreeMap<String, v5::request::List>,
        room_subscriptions:
            &'a std::collections::BTreeMap<ruma::OwnedRoomId, v5::request::RoomSubscription>,
        extensions: &'a v5::request::Extensions,
    }
    let view = BodyView {
        conn_id: &req.conn_id,
        txn_id: &req.txn_id,
        lists: &req.lists,
        room_subscriptions: &req.room_subscriptions,
        extensions: &req.extensions,
    };
    let Ok(bytes) = serde_json::to_vec(&view) else {
        return 0;
    };
    BODY_HASH_BUILDER.hash_one(&bytes)
}

/// Per-process random hash key for the idempotency-cache body hash.
///
/// `std::hash::DefaultHasher` uses SipHash13 with a fixed key; a malicious
/// or pathological client could in principle craft two different request
/// bodies with the same hash and retrieve a cached response intended for
/// a different request. Using `RandomState` (which seeds SipHash13 with
/// a fresh random key each process start) closes that door. The key
/// has to be stable for the lifetime of the process — different request
/// bodies during one process must hash consistently — so we initialise
/// it once via `LazyLock` and share it across all `request_body_hash`
/// calls.
static BODY_HASH_BUILDER: LazyLock<RandomState> = LazyLock::new(RandomState::new);

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
