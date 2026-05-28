use std::collections::{BTreeSet, HashMap};

use async_trait::async_trait;
pub use neutrino_common::Event;
use ruma::{
    EventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId, ServerName, UserId,
};
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamPos(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationToken(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

/// The five canonical `m.room.member` membership states. Sliding sync's
/// `rooms_with_membership` takes a set of these so the wire-string
/// alphabet is closed and duplicates can't be expressed at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Membership {
    Join,
    Invite,
    Knock,
    Leave,
    Ban,
}

impl Membership {
    /// Canonical wire string, as it appears in `m.room.member.content.membership`.
    pub fn as_str(self) -> &'static str {
        match self {
            Membership::Join => "join",
            Membership::Invite => "invite",
            Membership::Knock => "knock",
            Membership::Leave => "leave",
            Membership::Ban => "ban",
        }
    }

    /// Parse from the wire string; returns `None` for anything else.
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "join" => Some(Membership::Join),
            "invite" => Some(Membership::Invite),
            "knock" => Some(Membership::Knock),
            "leave" => Some(Membership::Leave),
            "ban" => Some(Membership::Ban),
            _ => None,
        }
    }
}

impl std::fmt::Display for Membership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[async_trait]
pub trait RoomStore: Send + Sync {
    /// Pre:  `create_event.event_type` is "m.room.create"; `create_event.room_id` is derived
    ///       from the reference hash of `create_event.raw` (room version 12 semantics);
    ///       the room does not already exist; every event in `initial_events` has the same
    ///       `room_id` as `create_event`.
    /// Post: the room record is registered with the version from `create_event` content;
    ///       `create_event` and all `initial_events` are persisted in a single transaction
    ///       and visible via `events_after`; current state reflects all initial state events;
    ///       no outbox entries are created (new rooms have no remote members yet).
    async fn create_room(
        &self,
        create_event: &Event,
        initial_events: &[Event],
    ) -> Result<(), StorageError>;

    /// Pre:  none.
    /// Post: returns `Some(version)` if the room exists, `None` if it does not.
    async fn get_room_version(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<RoomVersionId>, StorageError>;

    /// Pre:  none.
    /// Post: returns the number of rooms registered via `create_room`.
    async fn room_count(&self) -> Result<u64, StorageError>;
}

#[async_trait]
pub trait EventStore: Send + Sync {
    /// Pre:  the room identified by `event.room_id` must already exist (created via
    ///       `create_room`); `destinations` must be computed from current state *before*
    ///       this call, because this call may update current state (for state events).
    /// Post: the event is persisted with a new `StreamPos` greater than all previous
    ///       positions; if the event is a state event, current state is updated atomically;
    ///       one outbox row is created per destination — `UNIQUE(destination, event_id)`
    ///       makes this idempotent on retry; the `subscribe()` watch is updated with the
    ///       new `StreamPos` after the transaction commits.
    async fn persist_event(
        &self,
        event: &Event,
        destinations: &[&ServerName],
    ) -> Result<(), StorageError>;

    /// Pre:  the room identified by `event.room_id` must already exist.
    /// Post: the event is persisted with a new `StreamPos` greater than all previous
    ///       positions and visible via `events_after` / `get_events` / DAG walks;
    ///       `current_state` is NOT updated even for state events — historical
    ///       events feed history (`events`, `event_edges`, `room_messages`) but
    ///       must not regress the resolved current state, which already reflects
    ///       the room's head; no outbox rows are created (historical events are
    ///       local-only history, not federation traffic — backfill is the read
    ///       direction); the `subscribe()` watch is updated with the new
    ///       `StreamPos` after the transaction commits, so subscribers can wake
    ///       and discover the new history.
    ///
    /// Use this for `/backfill`, `/get_missing_events`, and any other path that
    /// inserts events older than the current head. Use `persist_event` for
    /// forward extension where the new event has been resolved into the room's
    /// current state by the caller.
    async fn persist_historical_event(&self, event: &Event) -> Result<(), StorageError>;

    /// Pre:  none.
    /// Post: returns one `Event` per ID that exists in the store; IDs with no
    ///       matching event are silently omitted (result length may be < `ids.len()`).
    async fn get_events(&self, ids: &[&EventId]) -> Result<Vec<Event>, StorageError>;

    /// Pre:  none (`StreamPos(0)` is valid for an initial full query).
    /// Post: returns all events with `stream_pos > pos` in ascending stream order;
    ///       returns an empty vec if no new events exist since `pos`.
    async fn events_after(
        &self,
        pos: StreamPos,
        limit: usize,
    ) -> Result<Vec<(StreamPos, Event)>, StorageError>;

    /// Pre:  the room must exist; if `from` is `Some`, the token must have been returned
    ///       by a previous call to this method (or constructed from a known `StreamPos`).
    /// Post: returns up to `limit` events in the requested direction; if `from` is `None`
    ///       and `dir` is `Backward`, starts from the most recent event in the room;
    ///       the returned `PaginationToken` is `None` when no further events exist in
    ///       that direction.
    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError>;

    /// Pre:  none.
    /// Post: returns a receiver whose value is the `StreamPos` of the most recently
    ///       committed event — advanced by both `persist_event` and
    ///       `persist_historical_event`. Callers must subscribe *before* performing
    ///       an initial DB query to avoid TOCTOU: any persist that commits during
    ///       the query will have advanced the watch, so the first `changed()` call
    ///       will resolve immediately and the follow-up query will see the new event.
    fn subscribe(&self) -> watch::Receiver<StreamPos>;
}

#[async_trait]
pub trait StateStore: Send + Sync {
    /// Pre:  none (returns empty map if the room does not exist).
    /// Post: returns exactly one event per `(event_type, state_key)` pair, representing
    ///       the current resolved state of the room; superseded state events are excluded.
    async fn current_room_state(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<(String, String), Event>, StorageError>;

    /// Pre:  none.
    /// Post: returns the current state event for `(room_id, event_type, state_key)`, or
    ///       `None` if no such event has been persisted.
    async fn current_state_event(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<Option<Event>, StorageError>;

    /// Pre:  none (returns empty map if the room does not exist or has no state of that type).
    /// Post: returns one current state event per `state_key` for the given `event_type`;
    ///       superseded events are excluded.
    async fn current_state_events_of_type(
        &self,
        room_id: &RoomId,
        event_type: &str,
    ) -> Result<HashMap<String, Event>, StorageError>;

    /// Pre:  none.
    /// Post: returns the `room_id` of every room in which `user_id` has a current
    ///       `m.room.member` event with `content.membership = "join"`; rooms where the
    ///       user has left, been banned, or is only invited are excluded.
    async fn joined_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError>;

    /// Pre:  none.
    /// Post: returns the `room_id` of every room in which `user_id` has a current
    ///       `m.room.member` event with `content.membership = "invite"`; rooms where
    ///       the user has joined, left, or been banned are excluded. Kept separate
    ///       from `joined_rooms` so the trait stays append-only; may be folded into
    ///       a single method with a membership filter in the future.
    async fn invited_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError>;

    /// Pre:  none. An empty `memberships` set returns an empty vec.
    /// Post: returns **exactly one** `(room_id, current_membership)` pair for
    ///       every room in which `user_id`'s current `m.room.member` event has
    ///       a `content.membership` in `memberships`. The `current_membership`
    ///       value is the actual membership the user currently has — duplicates
    ///       per room are impossible by construction since each `(room, user)`
    ///       has a single current member event. The caller can pass multiple
    ///       memberships to get the union in one round-trip (used by sliding
    ///       sync to enumerate candidate rooms across all the MSC4186-eligible
    ///       memberships at once). Result order is unspecified — callers sort
    ///       as needed. Implementations should answer this from an indexed
    ///       lookup rather than a full table scan.
    async fn rooms_with_membership(
        &self,
        user_id: &UserId,
        memberships: &BTreeSet<Membership>,
    ) -> Result<Vec<(OwnedRoomId, Membership)>, StorageError>;

    /// Pre:  none (returns empty map if the room does not exist).
    /// Post: returns one member event per `user_id` (state_key) whose current
    ///       `m.room.member` event has `content.membership = "join"`; left, banned, and
    ///       invited users are excluded; the implementation filters via an indexed
    ///       `membership` column, not by loading all member events into memory.
    async fn joined_members(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, Event>, StorageError>;
}

#[async_trait]
pub trait DagStore: Send + Sync {
    /// Pre:  all event IDs in `from` must exist in the store; `room_id` must exist.
    /// Post: walks `prev_events` backwards from `from`, returning up to `limit` distinct
    ///       events in reverse-chronological order; each `Event` has `prev_events` and
    ///       `prev_state_events` pre-parsed for further DAG traversal; events already
    ///       known to the caller can be excluded by stopping the walk early.
    async fn events_before(
        &self,
        room_id: &RoomId,
        from: &[&EventId],
        limit: usize,
    ) -> Result<Vec<Event>, StorageError>;

    /// Pre:  `room_id` must exist in the store. Event IDs in `latest` and
    ///       `earliest` need not exist; unknown IDs in `latest` contribute
    ///       no parents to expand (empty edges row), unknown IDs in
    ///       `earliest` are no-ops on the walk.
    /// Post: BFS over `prev_events` starting from the *parents* of each
    ///       event in `latest`, skipping any event in `earliest ∪ latest`;
    ///       returns at most `limit` events. **The events in `latest`
    ///       themselves are never included in the result** — they are
    ///       the boundary the requester already has. The events in
    ///       `earliest` are likewise never included. Events in other
    ///       rooms (cross-room seeds or corrupt `event_edges`) are
    ///       treated as if they don't exist — the walk terminates at the
    ///       boundary rather than leaking PDUs from another room.
    ///       Mirrors Synapse's `_get_missing_events`.
    async fn missing_events(
        &self,
        room_id: &RoomId,
        latest: &[&EventId],
        earliest: &[&EventId],
        limit: usize,
    ) -> Result<Vec<Event>, StorageError>;
}

#[async_trait]
pub trait FederationOutbox: Send + Sync {
    /// Pre:  none.
    /// Post: returns the server names of every destination that has at least one outbox
    ///       entry not yet removed via `remove_pdus`; callers should call `subscribe()`
    ///       *before* this on startup to avoid missing a destination added concurrently
    ///       (see `EventStore::subscribe` for the subscribe-before-query pattern).
    async fn pending_destinations(&self) -> Result<Vec<OwnedServerName>, StorageError>;

    /// Pre:  none (returns empty vec if `destination` has no pending entries).
    /// Post: returns all undelivered PDUs for `destination` in insertion (causal) order;
    ///       does not remove them — the caller must call `remove_pdus` after a successful
    ///       `/send` transaction.
    async fn pending_pdus(&self, destination: &ServerName) -> Result<Vec<Event>, StorageError>;

    /// Pre:  each `event_id` in `event_ids` should have been returned by `pending_pdus`
    ///       for this `destination`; must only be called after the remote server returned
    ///       HTTP 200 for the `/send` transaction containing these events.
    /// Post: removes the matching `(destination, event_id)` rows; idempotent — calling
    ///       with already-removed IDs does not error.
    async fn remove_pdus(
        &self,
        destination: &ServerName,
        event_ids: &[&EventId],
    ) -> Result<(), StorageError>;
}

#[async_trait]
pub trait FederationInbox: Send + Sync {
    /// Pre:  none.
    /// Post: records `(origin, txn_id)` as processed; returns `true` if it was already
    ///       recorded (caller should return 200 immediately without reprocessing),
    ///       `false` if this is the first time seeing this transaction.
    async fn record_federation_txn(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<bool, StorageError>;
}

/// Combined storage interface. Use as a generic bound: `S: StorageBackend`.
pub trait StorageBackend:
    RoomStore + EventStore + StateStore + DagStore + FederationOutbox + FederationInbox
{
}

impl<T> StorageBackend for T where
    T: RoomStore + EventStore + StateStore + DagStore + FederationOutbox + FederationInbox
{
}
