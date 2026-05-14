use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ruma::api::client::sync::sync_events::v5::request;
use ruma::events::StateEventType;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use tokio::sync::Mutex;

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
    /// TODO(phase-3): apply to slice the candidate-room set into the
    /// requested window in `build::combined_room_configs`.
    #[allow(dead_code)]
    pub ranges: Vec<(usize, usize)>,
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
/// Used to compute deltas: on each response the handler appends to
/// `timeline_event_ids` and updates `required_state_keys` (see
/// `build::update_sent`). Future delta logic will consult this to avoid
/// re-sending events the client already has.
///
/// TODO(phase-4): `timeline_event_ids` grows unbounded — one entry per emitted
/// event for the room's lifetime on this connection. Bound it (keep only the
/// last N) or replace with a single high-water mark, since timeline is already
/// strictly ordered. `required_state_keys` is naturally bounded by the number
/// of distinct `(event_type, state_key)` pairs in the room.
/// TODO(phase-4): this is populated by `update_sent` but never consulted —
/// `build_room` doesn't yet diff against it. Subsequent syncs currently re-send
/// the same timeline window.
#[derive(Debug, Default, Clone)]
pub struct RoomSent {
    pub timeline_event_ids: Vec<OwnedEventId>,
    pub required_state_keys: HashMap<(String, String), OwnedEventId>,
}

/// One sliding-sync connection's state.
///
/// `pos` is an opaque-to-the-client monotonic counter we hand back as the
/// response `pos` string. It is **not** an event-store `StreamPos`.
///
/// TODO(phase-5): add a separate `last_event_stream_pos: StreamPos` field for
/// tracking where in the storage event stream we last looked, used by the
/// long-poll path to subscribe to new events.
#[derive(Debug, Default)]
pub struct Conn {
    pub pos: u64,
    pub lists: BTreeMap<String, ListCfg>,
    pub subs: BTreeMap<OwnedRoomId, SubCfg>,
    pub sent: HashMap<OwnedRoomId, RoomSent>,
}

impl Conn {
    pub fn new() -> Self {
        Self::default()
    }
}

/// In-memory registry of active sliding-sync connections.
///
/// **Storage**: `HashMap<ConnKey, Arc<Mutex<Conn>>>` behind an outer `Mutex` for
/// insert/lookup. Each conn is itself behind a Mutex so concurrent requests on
/// the same `(user_id, conn_id)` serialise (MSC3575 forbids concurrent
/// requests with the same conn_id; we serialise rather than reject).
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
    conns: Mutex<HashMap<ConnKey, Arc<Mutex<Conn>>>>,
}

impl ConnRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh connection, replacing any existing entry for `key`.
    /// A new initial sync from the same `(user_id, conn_id)` resets state.
    pub async fn create(&self, key: ConnKey) -> Arc<Mutex<Conn>> {
        let conn = Arc::new(Mutex::new(Conn::new()));
        self.conns.lock().await.insert(key, conn.clone());
        conn
    }

    pub async fn get(&self, key: &ConnKey) -> Option<Arc<Mutex<Conn>>> {
        self.conns.lock().await.get(key).cloned()
    }
}
