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
    /// Parsed but not yet honoured. Phase 3 will use these to slice the
    /// candidate-room set into the requested window.
    #[allow(dead_code)]
    pub ranges: Vec<(usize, usize)>,
    /// Parsed for forward compatibility but always ignored. The embedded
    /// single-user server returns every candidate room regardless of filters
    /// (per the design decision logged in PLAN.md on 2026-05-14).
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
/// **Unbounded growth caveat**: `timeline_event_ids` currently grows by every
/// emitted timeline event for the room's lifetime on this connection. For a
/// long-lived sync that's a real leak. Phase 4/5 will either bound it (keep
/// only the last N) or replace it with a single high-water mark since timeline
/// is already strictly ordered by `room_messages`. `required_state_keys` is
/// naturally bounded by the number of distinct `(event_type, state_key)`
/// pairs in the room — finite, but document if a room can have unbounded state.
#[derive(Debug, Default, Clone)]
pub struct RoomSent {
    pub timeline_event_ids: Vec<OwnedEventId>,
    pub required_state_keys: HashMap<(String, String), OwnedEventId>,
}

/// One sliding-sync connection's state.
///
/// `last_stream_pos` is misleadingly named — it is **not** an event-store
/// `StreamPos`. It's a per-conn opaque pos token (a monotonically increasing
/// counter we hand to the client and verify on the next request). Storage-layer
/// stream positions are tracked separately when phase 5 wires long-poll wakeup.
#[derive(Debug, Default)]
pub struct Conn {
    pub last_stream_pos: u64,
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
/// for a multi-user deployment this would need bounding before shipping.
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
