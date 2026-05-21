//! Test-only in-memory `StorageBackend` impl for the sliding-sync handler.
//!
//! Scope: implements just enough of the trait surface to drive
//! `sliding_sync::tests`. Methods the handler doesn't currently call are
//! `todo!()` — touching one in a new test will panic loudly so we don't
//! accidentally rely on unimplemented behaviour. Promote into a real testing
//! crate (or behind a `testing` feature in `neutrino-common`) when other crates
//! need it.
//!
//! Limitations vs. a real `StorageBackend` impl:
//! - No `create_room`, `persist_event`, DAG walks, or federation outbox writes
//!   — seed state via the `add_event`/`join_user`/`invite_user` helpers
//!   instead. Those go straight to the underlying maps and bypass the
//!   pre/post conditions in the trait contract.
//! - `room_messages` ignores the `from` pagination token (returns the full
//!   ordered window from one end). Fine for the current tests, will need fixing
//!   when phase 4 exercises delta windows.
//! - `subscribe()` works and fires on every `add_event`, but no other writes
//!   (state changes, room creation) bump the watch. Adequate for phase 5's
//!   long-poll tests, no more.
//! - All locking is `std::sync::Mutex` (held briefly, never across `.await`)
//!   — the trait methods are async only to satisfy the signature.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use neutrino_store::{
    DagStore, Direction, EventStore, FederationInbox, FederationOutbox, PaginationToken, RoomStore,
    StateStore, StorageError, StoredEvent, StoredPdu, StreamPos,
};
use ruma::{
    EventId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId,
    ServerName, UserId,
};
use serde_json::value::RawValue;
use tokio::sync::watch;

pub struct MockStore {
    inner: Mutex<Inner>,
    watch_tx: watch::Sender<StreamPos>,
}

struct Inner {
    rooms: HashMap<OwnedRoomId, RoomVersionId>,
    events: Vec<(StreamPos, StoredEvent)>,
    next_pos: u64,
    current_state: HashMap<OwnedRoomId, HashMap<(String, String), StoredEvent>>,
    joined: HashMap<OwnedUserId, Vec<OwnedRoomId>>,
    invited: HashMap<OwnedUserId, Vec<OwnedRoomId>>,
}

impl MockStore {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(StreamPos(0));
        Self {
            inner: Mutex::new(Inner {
                rooms: HashMap::new(),
                events: Vec::new(),
                next_pos: 1,
                current_state: HashMap::new(),
                joined: HashMap::new(),
                invited: HashMap::new(),
            }),
            watch_tx: tx,
        }
    }

    #[allow(dead_code)]
    pub fn add_room(&self, room_id: &RoomId, version: RoomVersionId) {
        let mut inner = self.inner.lock().unwrap();
        inner.rooms.insert(room_id.to_owned(), version);
    }

    pub fn join_user(&self, user_id: &UserId, room_id: &RoomId) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .joined
            .entry(user_id.to_owned())
            .or_default()
            .push(room_id.to_owned());
    }

    pub fn invite_user(&self, user_id: &UserId, room_id: &RoomId) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .invited
            .entry(user_id.to_owned())
            .or_default()
            .push(room_id.to_owned());
    }

    /// Seed an event into the store. State events automatically update current
    /// state (latest wins) — there is no proper resolution, so tests that need
    /// state-event ordering semantics should add them in the right order.
    pub fn add_event(&self, event: StoredEvent) {
        let mut inner = self.inner.lock().unwrap();
        let pos = StreamPos(inner.next_pos);
        inner.next_pos += 1;
        if let Some(state_key) = event.state_key.clone() {
            let room_state = inner
                .current_state
                .entry(event.room_id.clone())
                .or_default();
            room_state.insert((event.event_type.clone(), state_key), dup(&event));
        }
        inner.events.push((pos, event));
        // `send_if_modified` always updates the stored value; plain `send`
        // returns Err and no-ops the update when there are no live
        // receivers, and the mock's constructor drops the initial receiver.
        // Matches the production store's `notify_watch` pattern.
        self.watch_tx.send_if_modified(|cur| {
            if pos > *cur {
                *cur = pos;
                true
            } else {
                false
            }
        });
    }

    /// Remove a `(event_type, state_key)` entry from current state. The
    /// underlying events stay in the event log (mirroring how a real store
    /// behaves when a state event is overwritten by something that *deletes*
    /// state — e.g. an `m.room.member` with `membership: leave` is the actual
    /// deletion of the join). Tests use this to exercise the state-stub path.
    pub fn remove_state(&self, room_id: &RoomId, event_type: &str, state_key: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.current_state.get_mut(room_id) {
            state.remove(&(event_type.to_string(), state_key.to_string()));
        }
    }
}

/// `StoredEvent` doesn't derive `Clone` in `neutrino-common`. Hand-clone it
/// here rather than touching the trait file from a test-only mock.
fn dup(e: &StoredEvent) -> StoredEvent {
    StoredEvent {
        event_id: e.event_id.clone(),
        room_id: e.room_id.clone(),
        event_type: e.event_type.clone(),
        state_key: e.state_key.clone(),
        sender: e.sender.clone(),
        origin_server_ts: e.origin_server_ts,
        json: e.json.clone(),
    }
}

#[async_trait]
impl RoomStore for MockStore {
    async fn create_room(
        &self,
        _create_event: &StoredEvent,
        _initial_events: &[StoredEvent],
    ) -> Result<(), StorageError> {
        todo!("MockStore::create_room not used in current tests")
    }

    async fn get_room_version(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<RoomVersionId>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.rooms.get(room_id).cloned())
    }

    async fn room_count(&self) -> Result<u64, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.rooms.len() as u64)
    }
}

#[async_trait]
impl EventStore for MockStore {
    async fn persist_event(
        &self,
        _event: &StoredEvent,
        _destinations: &[&ServerName],
    ) -> Result<(), StorageError> {
        todo!("MockStore::persist_event not used in current tests")
    }

    async fn persist_historical_event(&self, _event: &StoredEvent) -> Result<(), StorageError> {
        todo!("MockStore::persist_historical_event not used in current tests")
    }

    async fn get_client_txn(
        &self,
        _txn_id: &str,
        _user_id: &UserId,
    ) -> Result<Option<OwnedEventId>, StorageError> {
        Ok(None)
    }

    async fn record_client_txn(
        &self,
        _txn_id: &str,
        _user_id: &UserId,
        _event_id: &EventId,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn get_events(&self, ids: &[&EventId]) -> Result<Vec<StoredEvent>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let wanted: std::collections::HashSet<OwnedEventId> =
            ids.iter().map(|id| (*id).to_owned()).collect();
        let mut out = Vec::new();
        for (_, ev) in &inner.events {
            if wanted.contains(&ev.event_id) {
                out.push(dup(ev));
            }
        }
        Ok(out)
    }

    async fn events_after(
        &self,
        pos: StreamPos,
        limit: usize,
    ) -> Result<Vec<(StreamPos, StoredEvent)>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<(StreamPos, StoredEvent)> = Vec::new();
        for (p, ev) in &inner.events {
            if *p > pos {
                out.push((*p, dup(ev)));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn room_messages(
        &self,
        room_id: &RoomId,
        _from: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<StoredEvent>, Option<PaginationToken>), StorageError> {
        let inner = self.inner.lock().unwrap();
        let mut room_events: Vec<&StoredEvent> = inner
            .events
            .iter()
            .filter(|(_, ev)| ev.room_id == room_id)
            .map(|(_, ev)| ev)
            .collect();
        if matches!(dir, Direction::Backward) {
            room_events.reverse();
        }
        let truncated: Vec<StoredEvent> = room_events.into_iter().take(limit).map(dup).collect();
        Ok((truncated, None))
    }

    fn subscribe(&self) -> watch::Receiver<StreamPos> {
        self.watch_tx.subscribe()
    }
}

#[async_trait]
impl StateStore for MockStore {
    async fn current_room_state(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<(String, String), StoredEvent>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .current_state
            .get(room_id)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), dup(v))).collect())
            .unwrap_or_default();
        Ok(state)
    }

    async fn current_state_event(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<Option<StoredEvent>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .current_state
            .get(room_id)
            .and_then(|m| m.get(&(event_type.to_string(), state_key.to_string())))
            .map(dup))
    }

    async fn current_state_events_of_type(
        &self,
        room_id: &RoomId,
        event_type: &str,
    ) -> Result<HashMap<String, StoredEvent>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let mut out = HashMap::new();
        if let Some(m) = inner.current_state.get(room_id) {
            for ((ty, sk), ev) in m {
                if ty == event_type {
                    out.insert(sk.clone(), dup(ev));
                }
            }
        }
        Ok(out)
    }

    async fn joined_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.joined.get(user_id).cloned().unwrap_or_default())
    }

    async fn invited_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.invited.get(user_id).cloned().unwrap_or_default())
    }

    async fn joined_members(
        &self,
        _room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, StoredEvent>, StorageError> {
        Ok(HashMap::new())
    }

    async fn rooms_with_membership(
        &self,
        _user_id: &UserId,
        _memberships: &[&str],
    ) -> Result<Vec<(OwnedRoomId, String)>, StorageError> {
        todo!("MockStore::rooms_with_membership not used in current tests")
    }
}

#[async_trait]
impl DagStore for MockStore {
    async fn events_before(
        &self,
        _room_id: &RoomId,
        _from: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!("MockStore::events_before not used in current tests")
    }

    async fn missing_events(
        &self,
        _room_id: &RoomId,
        _latest: &[&EventId],
        _earliest: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!("MockStore::missing_events not used in current tests")
    }
}

#[async_trait]
impl FederationOutbox for MockStore {
    async fn pending_destinations(&self) -> Result<Vec<OwnedServerName>, StorageError> {
        Ok(Vec::new())
    }

    async fn pending_pdus(
        &self,
        _destination: &ServerName,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        Ok(Vec::new())
    }

    async fn remove_pdus(
        &self,
        _destination: &ServerName,
        _event_ids: &[&EventId],
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl FederationInbox for MockStore {
    async fn record_federation_txn(
        &self,
        _origin: &ServerName,
        _txn_id: &str,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }
}

/// Build a `StoredEvent` whose `json` field is a flat object with the standard
/// PDU keys. Tests pass `content` separately so they don't have to construct
/// the wrapper themselves. Bypasses room-version validation, signature checks,
/// and any other pipeline the real persistence path would apply — only
/// suitable for unit tests of the sync handler.
pub fn make_event(
    event_id: &EventId,
    room_id: &RoomId,
    event_type: &str,
    state_key: Option<&str>,
    sender: &UserId,
    origin_server_ts: u64,
    content: serde_json::Value,
) -> StoredEvent {
    let mut obj = serde_json::Map::new();
    obj.insert("event_id".to_string(), serde_json::json!(event_id.as_str()));
    obj.insert("room_id".to_string(), serde_json::json!(room_id.as_str()));
    obj.insert("type".to_string(), serde_json::json!(event_type));
    if let Some(sk) = state_key {
        obj.insert("state_key".to_string(), serde_json::json!(sk));
    }
    obj.insert("sender".to_string(), serde_json::json!(sender.as_str()));
    obj.insert(
        "origin_server_ts".to_string(),
        serde_json::json!(origin_server_ts),
    );
    obj.insert("content".to_string(), content);
    let json: Box<RawValue> =
        serde_json::value::to_raw_value(&serde_json::Value::Object(obj)).unwrap();

    StoredEvent {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: event_type.to_string(),
        state_key: state_key.map(|s| s.to_string()),
        sender: sender.to_owned(),
        origin_server_ts,
        json,
    }
}
