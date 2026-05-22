use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ruma::api::client::sync::sync_events::v5;
use ruma::api::client::sync::sync_events::v5::request;
use ruma::events::StateEventType;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use tokio::sync::{Mutex, watch};

/// Identifies a sliding-sync connection within the registry.
///
/// `conn_id` is the client-supplied `conn_id` field on the request (max 16 chars
/// per MSC4186) or the empty string when the client omits it. MSC4186 allows
/// omitting `conn_id` only for a single connection per user; we use the empty
/// string as that "default" slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnKey {
    pub user_id: OwnedUserId,
    pub conn_id: String,
}

/// Sticky configuration for one named list inside a connection.
///
/// MSC4186 lists are sticky: once the client sends a list config under a given
/// name, the server keeps applying it on subsequent requests unless the client
/// resends with new values. We mirror that by storing the merged result here.
#[derive(Debug, Clone)]
pub struct ListCfg {
    pub timeline_limit: usize,
    pub required_state: Vec<(StateEventType, String)>,
    /// Inclusive window of the sorted candidate list this list cares about.
    /// `None` means "no window requested" → treat as the full window.
    ///
    /// MSC3575 allowed multiple ranges per list (`ranges: [[a,b], [c,d]]`);
    /// MSC4186 removed that and exposes only a single `range: [a,b]`. The
    /// `apply_sticky` boundary already takes `list.ranges.first()` from the
    /// ruma v5 request (ruma still types it as a `Vec` — its v5 module is
    /// half-migrated), so this field's `Option` is the source of truth.
    pub range: Option<(usize, usize)>,
    /// Parsed for forward compatibility but always ignored. The embedded
    /// single-user server returns every candidate room regardless of filters
    /// (decision in PLAN.md 2026-05-14; not a phase TODO — intentional gap).
    #[allow(dead_code)]
    pub filters: Option<request::ListFilters>,
}

#[derive(Debug, Clone)]
pub struct SubCfg {
    pub timeline_limit: usize,
    pub required_state: Vec<(StateEventType, String)>,
}

/// What the server has previously sent to *this connection* about a given room.
///
/// Used to compute deltas:
/// - Timeline delivery is tracked via `Conn::last_event_stream_pos` (a single
///   global high-water mark, since `events_after` returns events in stream
///   order across all rooms). We don't need per-room timeline tracking.
/// - State delivery is tracked here per `(event_type, state_key)` → the event
///   id we last sent for that key. `build_room` compares against this to skip
///   unchanged state and to emit MSC4186 §StateStub markers for keys that
///   were sent before but no longer match current state.
///
/// The presence of an entry in `Conn::sent` for a given room also signals
/// "this room has been emitted at least once" → next emission is a delta, not
/// initial.
#[derive(Debug, Default, Clone)]
pub struct RoomSent {
    pub required_state_keys: HashMap<(String, String), OwnedEventId>,
}

/// One sliding-sync connection's state.
///
/// `pos` is an opaque-to-the-client monotonic counter we hand back as the
/// response `pos` string. It is **not** an event-store `StreamPos`.
///
/// `last_event_stream_pos` is the highest `StreamPos` we've consumed from the
/// event stream when building responses on this connection. The next sync
/// queries `events_after(last_event_stream_pos)` to find what's new. On a
/// fresh connection it starts at 0; after the first response it's bumped to
/// whatever the event store's current head is, so subsequent syncs only see
/// events arriving *after* the initial snapshot.
#[derive(Debug, Default)]
pub struct Conn {
    pub pos: u64,
    pub last_event_stream_pos: u64,
    pub lists: BTreeMap<String, ListCfg>,
    pub subs: BTreeMap<OwnedRoomId, SubCfg>,
    pub sent: HashMap<OwnedRoomId, RoomSent>,
    /// Per-list previously-seen `timeline_limit` so `build_room` can detect a
    /// limit-grew situation and resend older events. MSC4186 calls this
    /// `expanded_timeline`. Ruma v5's `response::Room` doesn't carry that
    /// field, so we can't actually surface it on the wire — kept tracked for
    /// when ruma catches up. See MSC4186-gaps.md.
    pub prev_list_timeline_limits: BTreeMap<String, usize>,
    /// Idempotency cache: the `pos` value the client sent on the most
    /// recently *processed* request (i.e. the input pos, not the output). If
    /// the next request arrives with the same value AND the same body hash,
    /// we return `last_response` verbatim rather than re-processing —
    /// MSC4186 §"Pagination and Tokens" permits clients to retry by
    /// re-using the same `pos`.
    ///
    /// `None` on a freshly-created conn (the initial sync was the most
    /// recent processed request, which has no pos input).
    pub last_request_pos: Option<u64>,
    /// Hash of the most-recently-processed request's body fields
    /// (`conn_id`, `txn_id`, `lists`, `room_subscriptions`, `extensions`).
    /// A retry only hits the cache when *both* `pos` and this hash match
    /// the cached request — otherwise the retry has changed something
    /// (different `timeline_limit`, opted into a new extension, etc.) and
    /// must be re-processed, not served the stale response.
    ///
    /// `0` on a freshly-created conn. Computed via `request_body_hash` in
    /// `super::handle`.
    pub last_request_hash: u64,
    /// Companion to `last_request_pos`/`last_request_hash` — the full
    /// response we returned for that input. On a retry hit
    /// (`(req.pos, hash) == (last_request_pos, last_request_hash)`) we
    /// clone this and return immediately, without re-running
    /// `build_response` or advancing any conn state.
    ///
    /// Includes the post-processing extension stubs so the cached response
    /// matches exactly what the client got the first time, byte-for-byte.
    pub last_response: Option<v5::Response>,
}

impl Conn {
    pub fn new() -> Self {
        Self::default()
    }
}

/// One entry in `ConnRegistry`. Bundles the `Conn` mutex with a cancellation
/// signal so a newer request can pre-empt an in-flight long-poll on the same
/// `(user_id, conn_id)` instead of queueing behind the conn lock for the full
/// long-poll timeout.
///
/// **Signal mechanics.** `cancel_gen` is a `watch::Sender<u64>` whose value is
/// a monotonically incrementing generation counter. The producing side bumps
/// it via `send_modify` *before* attempting `conn.lock().await`; the
/// in-flight request holds a `Receiver` it `select!`s on in its idle wait,
/// breaks out, runs its post-loop tail (pos commit + cache write), and drops
/// the guard. `watch` was chosen over `Notify` / `tokio_util::CancellationToken`
/// because its value is *sticky* — a bump that happens before the holder
/// calls `.changed()` is still observed on the next call, eliminating the
/// lost-wakeup window that `Notify::notify_waiters` has.
///
/// Both fields are `Arc` so handlers can hold them past the registry-lock
/// drop and the entry can outlive any single registry traversal.
#[derive(Clone)]
pub struct ConnEntry {
    pub conn: Arc<Mutex<Conn>>,
    pub cancel_gen: Arc<watch::Sender<u64>>,
}

impl ConnEntry {
    fn new() -> Self {
        let (tx, _rx) = watch::channel(0u64);
        Self {
            conn: Arc::new(Mutex::new(Conn::new())),
            cancel_gen: Arc::new(tx),
        }
    }

    /// Bump the cancellation generation. A holder currently `.changed()`-waiting
    /// on the matching `Receiver` will wake; a holder that hasn't yet
    /// subscribed will observe the bump on its next `.changed()` call because
    /// `watch` values are sticky.
    pub fn cancel(&self) {
        self.cancel_gen.send_modify(|g| *g = g.saturating_add(1));
    }

    /// Subscribe to the cancellation signal. The caller must call
    /// `borrow_and_update` on the returned receiver to seed its baseline
    /// against the current generation — otherwise any bump that has *already*
    /// happened (e.g. by an earlier request that finished and bumped on its
    /// way out) would fire `.changed()` immediately on the first await.
    pub fn subscribe_cancel(&self) -> watch::Receiver<u64> {
        self.cancel_gen.subscribe()
    }
}

/// In-memory registry of active sliding-sync connections.
///
/// **Storage**: `HashMap<ConnKey, ConnEntry>` behind an outer `Mutex` for
/// insert/lookup. Each entry's `Conn` is itself behind a Mutex so concurrent
/// requests on the same `(user_id, conn_id)` serialise. MSC3575 forbids
/// concurrent requests with the same conn_id; we cancel-and-replace the
/// in-flight request rather than reject the newcomer (see
/// `ConnEntry::cancel` / `MSC4186-gaps.md`).
///
/// **Lifecycle**: connections are created on initial sync (no `pos`) and never
/// expire. There is **no eviction**, **no idle timeout**, **no LRU**, no
/// upper bound on the number of conns. For the embedded single-user server
/// that's fine (in practice we expect 1–3 concurrent conns from one device);
/// a multi-user deployment would need bounding before shipping — not currently
/// in scope.
///
/// **Persistence**: state is lost on server restart. Clients recover by
/// receiving `M_UNKNOWN_POS` on their next request and reconnecting without a
/// `pos`. This is by design — the registry is a cache, not a source of truth.
#[derive(Default)]
pub struct ConnRegistry {
    conns: Mutex<HashMap<ConnKey, ConnEntry>>,
}

impl ConnRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh connection, replacing any existing entry for `key`.
    /// If a prior entry existed, cancel it first so any in-flight long-poll
    /// running against that orphan entry wakes promptly instead of running
    /// its full timeout (it would otherwise complete against the abandoned
    /// `Conn` and discard the result).
    pub async fn create(&self, key: ConnKey) -> ConnEntry {
        let new_entry = ConnEntry::new();
        let mut conns = self.conns.lock().await;
        if let Some(prior) = conns.insert(key, new_entry.clone()) {
            prior.cancel();
        }
        new_entry
    }

    pub async fn get(&self, key: &ConnKey) -> Option<ConnEntry> {
        self.conns.lock().await.get(key).cloned()
    }
}
