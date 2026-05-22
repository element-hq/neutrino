//! In-memory `StorageBackend` implementation.
//!
//! Used as the seeding target for sliding-sync unit tests. The live router
//! (`lib.rs`) uses `SqliteStore::open_in_memory()` from `neutrino-store-sqlite`
//! — this in-memory impl predates that crate and is kept here because the
//! sliding-sync tests want test helpers (`join_user`, `add_event`,
//! `remove_state`, `set_membership`) that don't make sense on the real
//! trait surface.
//!
//! **Persistence:** none. All state is lost on restart. Clients recover from
//! the resulting `M_UNKNOWN_POS` on their next sync by reconnecting without
//! a pos.
//!
//! **Concurrency:** single internal `std::sync::Mutex` guarding the data.
//! Trait methods are `async` to satisfy the signatures but the critical
//! sections are short and never held across `.await` boundaries. Multiple
//! concurrent axum handlers serialise behind the mutex; for single-user
//! embedded scale this is fine.
//!
//! **Test helpers** (`join_user`, `invite_user`, `add_event`, `remove_state`,
//! `make_event`) bypass the normal `persist_event` / `create_room` pipeline
//! to make scenario seeding less verbose. They're `pub` because the sliding
//! sync tests use them, but they're not part of the trait surface and won't
//! be called from production handler code.
//!
//! **Limitations versus a real backend:**
//! - No room-version derivation from the create event's reference hash —
//!   we just trust whatever `room_id` the caller provides on `create_room`.
//! - DAG-walk methods (`events_before`, `missing_events`) are `todo!()`.
//!   Federation backfill isn't wired through the router yet, so nothing
//!   should hit them. Calling one will panic loudly.
//! - `record_federation_txn` returns `false` always (no inbound federation
//!   dedup); `pending_destinations` / `pending_pdus` return empty.

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

pub struct InMemoryStore {
    inner: Mutex<Inner>,
    watch_tx: watch::Sender<StreamPos>,
}

struct Inner {
    rooms: HashMap<OwnedRoomId, RoomVersionId>,
    events: Vec<(StreamPos, StoredEvent)>,
    next_pos: u64,
    current_state: HashMap<OwnedRoomId, HashMap<(String, String), StoredEvent>>,
    /// Per-user membership index: `user → (room → current membership)`.
    /// Updated by `persist_event` when it sees an `m.room.member` event, and
    /// by the test helpers (`join_user`, `invite_user`, `set_membership`)
    /// directly. Captures all five Matrix membership strings — join, invite,
    /// knock, leave, ban — so `rooms_with_membership` can answer queries
    /// across the full set in one walk.
    memberships: HashMap<OwnedUserId, HashMap<OwnedRoomId, String>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(StreamPos(0));
        Self {
            inner: Mutex::new(Inner {
                rooms: HashMap::new(),
                events: Vec::new(),
                next_pos: 1,
                current_state: HashMap::new(),
                memberships: HashMap::new(),
            }),
            watch_tx: tx,
        }
    }

    #[allow(dead_code)]
    pub fn add_room(&self, room_id: &RoomId, version: RoomVersionId) {
        let mut inner = self.inner.lock().unwrap();
        inner.rooms.insert(room_id.to_owned(), version);
    }

    /// Test helper: directly set `user`'s membership in `room_id` to `join`
    /// without going through `persist_event`. Real production paths get this
    /// via `persist_event` on an `m.room.member` event.
    pub fn join_user(&self, user_id: &UserId, room_id: &RoomId) {
        self.set_membership(user_id, room_id, "join");
    }

    /// Test helper: directly set `user`'s membership in `room_id` to `invite`.
    pub fn invite_user(&self, user_id: &UserId, room_id: &RoomId) {
        self.set_membership(user_id, room_id, "invite");
    }

    /// Test helper: set `user`'s current membership in `room_id` to the
    /// given string. Use this from tests when you want to assert
    /// behaviour for `leave` / `ban` / `knock` without seeding a full
    /// `m.room.member` event. State-event-driven tests should still go
    /// through `add_event` so they exercise the parsing path too.
    pub fn set_membership(&self, user_id: &UserId, room_id: &RoomId, membership: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .memberships
            .entry(user_id.to_owned())
            .or_default()
            .insert(room_id.to_owned(), membership.to_string());
    }

    /// Test helper: insert an event into the log + current state without the
    /// auto-membership tracking that `persist_event` applies. Useful for
    /// seeding scenarios where membership and event content disagree.
    pub fn add_event(&self, event: StoredEvent) {
        let mut inner = self.inner.lock().unwrap();
        insert_event_locked(&mut inner, event, &self.watch_tx, false);
    }

    /// Remove a `(event_type, state_key)` entry from current state. The
    /// underlying events stay in the event log (mirroring how a real store
    /// behaves when a state event is overwritten by something that *deletes*
    /// state). Used by tests to exercise the state-stub path.
    pub fn remove_state(&self, room_id: &RoomId, event_type: &str, state_key: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.current_state.get_mut(room_id) {
            state.remove(&(event_type.to_string(), state_key.to_string()));
        }
    }

    /// Convenience for legacy `lib.rs` handlers (`createRoom`, `put_event`):
    /// take a `serde_json::Value` PDU shape and persist it. Returns the
    /// parsed `event_id` so callers can echo it back to the client.
    ///
    /// On any parse failure we silently drop the event (callers built it
    /// themselves so a malformed event would be a programmer error, not a
    /// client error). Production handlers ought to validate up front.
    pub fn insert_event_from_json(&self, value: &serde_json::Value) -> Option<OwnedEventId> {
        let stored = event_from_value(value)?;
        let event_id = stored.event_id.clone();
        let mut inner = self.inner.lock().unwrap();
        insert_event_locked(&mut inner, stored, &self.watch_tx, true);
        Some(event_id)
    }

    /// Number of distinct rooms the store has seen any event for. Legacy
    /// compat: the previous SQLite store exposed this as `count_distinct_rooms`
    /// for the old `/sync` handler. Kept here for the `versions` response.
    pub fn count_distinct_rooms(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        let mut seen: std::collections::HashSet<&OwnedRoomId> = std::collections::HashSet::new();
        for (_, ev) in &inner.events {
            seen.insert(&ev.room_id);
        }
        seen.len() as u64
    }

    /// All `m.room.member` events for `room_id`, returned as the raw PDU
    /// JSON shape the CSAPI `/members` endpoint serves. State-event variant
    /// (current state, not history).
    pub fn members_of(&self, room_id: &RoomId) -> Vec<serde_json::Value> {
        let inner = self.inner.lock().unwrap();
        let Some(state) = inner.current_state.get(room_id) else {
            return Vec::new();
        };
        state
            .iter()
            .filter(|((t, _), _)| t == "m.room.member")
            .filter_map(|(_, ev)| serde_json::from_str::<serde_json::Value>(ev.json.get()).ok())
            .collect()
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// `StoredEvent` doesn't derive `Clone` in `neutrino-common`. Hand-clone it
/// here in the live backend's hot paths.
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

/// Core insert path: assign a stream pos, write to current state (if it's a
/// state event), optionally track room membership (if it's an
/// `m.room.member` event and the caller asked for tracking), and broadcast
/// the new pos on the watch channel.
fn insert_event_locked(
    inner: &mut Inner,
    event: StoredEvent,
    watch_tx: &watch::Sender<StreamPos>,
    track_membership: bool,
) {
    insert_event_locked_with(inner, event, watch_tx, track_membership, true);
}

/// Lower-level insert that lets `persist_historical_event` skip the
/// `current_state` UPSERT — historical writes feed history, not the resolved
/// head.
fn insert_event_locked_with(
    inner: &mut Inner,
    event: StoredEvent,
    watch_tx: &watch::Sender<StreamPos>,
    track_membership: bool,
    update_current_state: bool,
) {
    let pos = StreamPos(inner.next_pos);
    inner.next_pos += 1;
    if update_current_state && let Some(state_key) = event.state_key.clone() {
        let room_state = inner
            .current_state
            .entry(event.room_id.clone())
            .or_default();
        room_state.insert((event.event_type.clone(), state_key), dup(&event));
    }
    if track_membership && event.event_type == "m.room.member" {
        track_membership_from_event(inner, &event);
    }
    inner.events.push((pos, event));
    // `send_if_modified` notifies even when there are no receivers (`send` is
    // a no-op in that case). Matches the production `notify_watch` pattern
    // pulled in from main (PLAN.md 2026-05-21 entry).
    watch_tx.send_if_modified(|cur| {
        if *cur < pos {
            *cur = pos;
            true
        } else {
            false
        }
    });
}

/// Walk the per-user membership index and return `(room, current_membership)`
/// for rooms whose current membership matches one of `memberships`. Shared
/// backing for `joined_rooms`, `invited_rooms`, and `rooms_with_membership`
/// so the three trait methods agree on the source of truth.
fn rooms_for_user_with(
    store: &InMemoryStore,
    user_id: &UserId,
    memberships: &[&str],
) -> Vec<(OwnedRoomId, String)> {
    let inner = store.inner.lock().unwrap();
    let Some(room_map) = inner.memberships.get(user_id) else {
        return Vec::new();
    };
    let want: std::collections::HashSet<&str> = memberships.iter().copied().collect();
    room_map
        .iter()
        .filter(|(_, v)| want.contains(v.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Apply an `m.room.member` event to the per-user membership map.
/// `state_key` is the user the membership is for; the JSON's
/// `content.membership` is the new state. We store all five canonical
/// strings (join, invite, knock, leave, ban) — sliding sync's
/// `rooms_with_membership` query needs to see them all to apply
/// MSC4186's "rooms included in the server list" rules.
fn track_membership_from_event(inner: &mut Inner, event: &StoredEvent) {
    let Some(state_key) = &event.state_key else {
        return;
    };
    let Ok(user) = state_key.parse::<OwnedUserId>() else {
        return;
    };
    let membership = serde_json::from_str::<serde_json::Value>(event.json.get())
        .ok()
        .and_then(|v| {
            v.pointer("/content/membership")
                .and_then(|m| m.as_str())
                .map(String::from)
        });
    let room_map = inner.memberships.entry(user).or_default();
    match membership {
        Some(m) => {
            room_map.insert(event.room_id.clone(), m);
        }
        None => {
            // No membership in content → can't reason about state; drop
            // any prior entry rather than leave it stale.
            room_map.remove(&event.room_id);
        }
    }
}

/// Parse a PDU-shape `serde_json::Value` into a `StoredEvent`. Returns
/// `None` if any required field is missing or malformed.
fn event_from_value(value: &serde_json::Value) -> Option<StoredEvent> {
    let event_id: OwnedEventId = value.get("event_id")?.as_str()?.try_into().ok()?;
    let room_id: OwnedRoomId = value.get("room_id")?.as_str()?.try_into().ok()?;
    let event_type = value.get("type")?.as_str()?.to_string();
    let state_key = value
        .get("state_key")
        .and_then(|s| s.as_str())
        .map(String::from);
    let sender: OwnedUserId = value.get("sender")?.as_str()?.try_into().ok()?;
    let origin_server_ts = value
        .get("origin_server_ts")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let json: Box<RawValue> = serde_json::value::to_raw_value(value).ok()?;
    Some(StoredEvent {
        event_id,
        room_id,
        event_type,
        state_key,
        sender,
        origin_server_ts,
        json,
    })
}

#[async_trait]
impl RoomStore for InMemoryStore {
    async fn create_room(
        &self,
        create_event: &StoredEvent,
        initial_events: &[StoredEvent],
    ) -> Result<(), StorageError> {
        if create_event.event_type != "m.room.create" {
            return Err(StorageError::InvalidInput(
                "create_event must be type m.room.create".to_string(),
            ));
        }
        let parsed: serde_json::Value = serde_json::from_str(create_event.json.get())
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let version_str = parsed
            .pointer("/content/room_version")
            .and_then(|v| v.as_str())
            .unwrap_or("12");
        let version = RoomVersionId::try_from(version_str);

        let mut inner = self.inner.lock().unwrap();
        if let Ok(v) = version {
            inner.rooms.insert(create_event.room_id.clone(), v);
        }
        insert_event_locked(&mut inner, dup(create_event), &self.watch_tx, true);
        for ev in initial_events {
            insert_event_locked(&mut inner, dup(ev), &self.watch_tx, true);
        }
        Ok(())
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
impl EventStore for InMemoryStore {
    async fn persist_event(
        &self,
        event: &StoredEvent,
        _destinations: &[&ServerName],
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().unwrap();
        insert_event_locked(&mut inner, dup(event), &self.watch_tx, true);
        Ok(())
    }

    async fn persist_historical_event(&self, event: &StoredEvent) -> Result<(), StorageError> {
        // Historical writes feed history (no current_state update, no outbox)
        // — see PLAN.md 2026-05-20.
        let mut inner = self.inner.lock().unwrap();
        insert_event_locked_with(&mut inner, dup(event), &self.watch_tx, false, false);
        Ok(())
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
        let total = room_events.len();
        let truncated: Vec<StoredEvent> = room_events.into_iter().take(limit).map(dup).collect();
        // Trait contract: token is `Some` iff there are more events to walk
        // further in the requested direction. The token value itself is a
        // placeholder — this store doesn't actually paginate through it.
        let prev_batch = if total > truncated.len() {
            Some(PaginationToken(truncated.len() as u64))
        } else {
            None
        };
        Ok((truncated, prev_batch))
    }

    fn subscribe(&self) -> watch::Receiver<StreamPos> {
        self.watch_tx.subscribe()
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
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
        Ok(rooms_for_user_with(self, user_id, &["join"])
            .into_iter()
            .map(|(r, _)| r)
            .collect())
    }

    async fn invited_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        Ok(rooms_for_user_with(self, user_id, &["invite"])
            .into_iter()
            .map(|(r, _)| r)
            .collect())
    }

    async fn rooms_with_membership(
        &self,
        user_id: &UserId,
        memberships: &[&str],
    ) -> Result<Vec<(OwnedRoomId, String)>, StorageError> {
        Ok(rooms_for_user_with(self, user_id, memberships))
    }

    async fn joined_members(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, StoredEvent>, StorageError> {
        let inner = self.inner.lock().unwrap();
        let mut out = HashMap::new();
        if let Some(m) = inner.current_state.get(room_id) {
            for ((ty, sk), ev) in m {
                if ty != "m.room.member" {
                    continue;
                }
                let Ok(user) = sk.parse::<OwnedUserId>() else {
                    continue;
                };
                let is_joined = serde_json::from_str::<serde_json::Value>(ev.json.get())
                    .ok()
                    .and_then(|v| {
                        v.pointer("/content/membership")
                            .and_then(|m| m.as_str())
                            .map(|s| s == "join")
                    })
                    .unwrap_or(false);
                if is_joined {
                    out.insert(user, dup(ev));
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl DagStore for InMemoryStore {
    async fn events_before(
        &self,
        _room_id: &RoomId,
        _from: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!("InMemoryStore::events_before not exercised — federation backfill not wired yet")
    }

    async fn missing_events(
        &self,
        _room_id: &RoomId,
        _latest: &[&EventId],
        _earliest: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!("InMemoryStore::missing_events not exercised — federation backfill not wired yet")
    }
}

#[async_trait]
impl FederationOutbox for InMemoryStore {
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
impl FederationInbox for InMemoryStore {
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
/// the wrapper themselves. Bypasses room-version validation and signature
/// checks — only suitable for unit/integration tests.
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
