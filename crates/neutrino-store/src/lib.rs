use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
pub use neutrino_event::Event;
use ruma::{
    EventId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId,
    ServerName, UserId,
};
use serde_json::value::RawValue as RawJsonValue;
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
pub struct PaginationToken(pub i64);

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
    /// Post: returns `true` if the room exists, `false` otherwise.
    ///
    /// Default impl derives existence from [`RoomStore::get_room_version`]; a
    /// backend with a cheaper existence probe (e.g. a bare `SELECT 1`) may
    /// override.
    async fn room_exists(&self, room_id: &RoomId) -> Result<bool, StorageError> {
        Ok(self.get_room_version(room_id).await?.is_some())
    }

    /// Pre:  none.
    /// Post: returns the number of rooms registered via `create_room`.
    async fn room_count(&self) -> Result<u64, StorageError>;

    /// Pre:  none.
    /// Post: returns the room's two forward-extremity sets as
    ///       `(timeline_fes, state_fes)` — the timeline-DAG heads and the
    ///       state-DAG heads persisted alongside the room — or `None` if the
    ///       room does not exist. A room that exists but whose heads have not
    ///       yet been written by `persist_resolved_event` reads back as two
    ///       empty sets (the columns default to `[]`); the caller treats that
    ///       as "not yet populated". Used to bootstrap an in-memory
    ///       `RoomCore` (see `neutrino_room::RoomCore::hydrate`).
    async fn forward_extremities(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<(BTreeSet<OwnedEventId>, BTreeSet<OwnedEventId>)>, StorageError>;
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
    /// Post: the event is persisted with a new `StreamPos` *less* than all previous
    ///       positions (backfilled events are older than the head, so they occupy
    ///       the descending region below the minimum) and visible via `get_events`
    ///       / DAG walks / backward `room_messages`; `current_state` is NOT updated
    ///       even for state events — historical events feed history (`events`,
    ///       `event_edges`, `room_messages`) but must not regress the resolved
    ///       current state, which already reflects the room's head; no outbox rows
    ///       are created (historical events are local-only history, not federation
    ///       traffic — backfill is the read direction); the `subscribe()` watch is
    ///       NOT advanced (backfilled events are older than the head and never
    ///       surface in incremental sliding sync).
    ///
    /// Use this for `/backfill`, `/get_missing_events`, and any other path that
    /// inserts events older than the current head. Use `persist_event` for
    /// forward extension where the new event has been resolved into the room's
    /// current state by the caller.
    async fn persist_historical_event(&self, event: &Event) -> Result<(), StorageError>;

    /// Pre:  `event.room_id` must exist; `event` is the just-accepted output
    ///       of `neutrino_room::RoomCore::apply`; `timeline_fes` /
    ///       `state_fes` are the head-sets that apply produced (read off the
    ///       post-apply `RoomCore`); and `current_state_delta` is the
    ///       `Effect::UpdateCurrentState` payload apply emitted (empty for a
    ///       non-state event). Every event id referenced by a `Some(_)` entry
    ///       in the delta MUST already be persisted (the just-persisted
    ///       `event`, or a prior event) — the impl asserts this. `destinations`
    ///       are the remote servers this event must be federated to (computed
    ///       by the caller from the post-apply room state); empty for a
    ///       federation-received event that we don't re-originate. `advertise_to`
    ///       are the servers that just became *joined* in the room's current
    ///       state by applying this event and to which we therefore owe a
    ///       forward-extremity advertisement (anti-entropy extension; computed
    ///       by the caller, see `pending_advertisements`); empty when applying
    ///       this event grew no server's membership into the joined set.
    /// Post: in a single write transaction — the event is persisted with a
    ///       new `StreamPos` (event row + DAG edges); the current-state delta
    ///       is applied (each `Some(id)` upserts that `(event_type,
    ///       state_key)` row to point at `id`, each `None` deletes the row);
    ///       the room's forward-extremity columns are replaced with
    ///       `timeline_fes` / `state_fes`; one `outbox` row is written per
    ///       destination (idempotent via `UNIQUE(destination, event_id)`; an
    ///       empty slice writes none); and one `pending_advertisements` row is
    ///       written per `advertise_to` server for `event.room_id` (idempotent
    ///       via the PK; an empty slice writes none) — same transaction as the
    ///       event so a crash can't persist the join but drop the obligation to
    ///       advertise. The `subscribe()` watch advances after commit. This is
    ///       the persist half of the storage⇄`RoomCore` bridge;
    ///       `forward_extremities` is the load half.
    async fn persist_resolved_event(
        &self,
        event: &Event,
        timeline_fes: &BTreeSet<OwnedEventId>,
        state_fes: &BTreeSet<OwnedEventId>,
        current_state_delta: &BTreeMap<(String, String), Option<OwnedEventId>>,
        destinations: &[&ServerName],
        advertise_to: &[&ServerName],
    ) -> Result<(), StorageError>;

    /// Pre:  none.
    /// Post: returns one `Event` per ID that exists in the store; IDs with no
    ///       matching event are silently omitted (result length may be < `ids.len()`).
    async fn get_events(&self, ids: &[&EventId]) -> Result<Vec<Event>, StorageError>;

    /// Pre:  none (`StreamPos(0)` is valid for an initial full query).
    /// Post: returns all **client-visible** events with `stream_pos > pos` in
    ///       ascending stream order; returns an empty vec if no new events exist
    ///       since `pos`. Rejected and soft-failed events are excluded: they are
    ///       persisted (federation persist policy) but must never reach a client
    ///       timeline (spec: soft-failed events "should not be sent to clients";
    ///       rejected events likewise — synapse `allow_rejected=False`).
    ///       Federation reads use `get_events`, which returns them — peers need
    ///       rejected ancestry to ground their state DAGs (MSC4242).
    async fn events_after(
        &self,
        pos: StreamPos,
        limit: usize,
    ) -> Result<Vec<(StreamPos, Event)>, StorageError>;

    /// Pre:  the room must exist; if `from`/`to` are `Some`, the token must have been
    ///       returned by a previous call (or built from a known `StreamPos`).
    /// Post: returns up to `limit` **client-visible** events in the requested
    ///       direction — rejected/soft-failed events are excluded and do not
    ///       count against `limit` (see `events_after`; federation reads use
    ///       `get_events` instead, which returns them). Tokens follow
    ///       Synapse's convention — a token points *after* the row it names — so the
    ///       bounds are asymmetric: `Forward` excludes `from` and includes `to`;
    ///       `Backward` includes `from` and excludes `to`. The continuation token is
    ///       set so re-feeding it as `from` neither repeats nor skips an event. If
    ///       `from` is `None` and `dir` is `Backward`, starts from the most recent event;
    ///       if `Forward`, from the earliest. `to` is `None` for no stop boundary in that
    ///       direction. The returned `PaginationToken` is `None` when no further events
    ///       exist within the range.
    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        to: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError>;

    /// Pre:  none.
    /// Post: returns the `StreamPos` of the most recent event in `room_id`, or
    ///       `StreamPos(0)` if the room holds no events. Room-scoped, unlike
    ///       `subscribe`, whose value is the global most-recent position across
    ///       all rooms.
    async fn room_stream_head(&self, room_id: &RoomId) -> Result<StreamPos, StorageError>;

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
    /// Post: distinct server names with at least one `join` membership in the
    ///       room's current state. Does NOT exclude our own server (callers
    ///       filter). Empty if the room is unknown or has no joined members.
    async fn joined_servers(&self, room_id: &RoomId) -> Result<Vec<OwnedServerName>, StorageError>;

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
    ///       events in reverse-chronological order — newest `origin_server_ts` first,
    ///       ties broken by ascending `event_id` (a max-priority-queue walk, so a
    ///       multi-seed or forked walk merges branches in true newest-first order, not
    ///       per-seed BFS-level order); each `Event` has `prev_events` and
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
    /// Post: walks backward starting from the *parents* of each event in
    ///       `latest`, skipping any event in `earliest ∪ latest`; returns at
    ///       most `limit` events in reverse-chronological order (newest
    ///       `origin_server_ts` first, ties by ascending `event_id` — the same
    ///       priority-queue walk as `events_before`). **The events in `latest`
    ///       themselves are never included in the result** — they are the
    ///       boundary the requester already has. The events in `earliest` are
    ///       likewise never included. Events in other rooms (cross-room seeds
    ///       or corrupt `event_edges`) are treated as if they don't exist — the
    ///       walk terminates at the boundary rather than leaking PDUs from
    ///       another room. Mirrors Synapse's `_get_missing_events`.
    ///
    /// `state_dag` selects which edge kind to walk (MSC4242 `/get_missing_events`
    /// `state_dag` flag): `false` walks `prev_events` (timeline DAG), `true`
    /// walks `prev_state_events` (the state DAG — the ancestry that
    /// `RoomCore::apply_pdu` requires to auth a PDU).
    async fn missing_events(
        &self,
        room_id: &RoomId,
        latest: &[&EventId],
        earliest: &[&EventId],
        limit: usize,
        state_dag: bool,
    ) -> Result<Vec<Event>, StorageError>;

    /// Pre:  none.
    /// Post: returns the distinct event ids referenced as a `prev` edge by an
    ///       event in `room_id` whose target is *not* present in `events` —
    ///       the backward extremities, i.e. the seeds for an outbound
    ///       `/backfill`. Empty for a fully-grounded room. Order unspecified.
    async fn backward_extremities(
        &self,
        room_id: &RoomId,
    ) -> Result<Vec<OwnedEventId>, StorageError>;
}

/// The result of [`StagingStore::ancestry_gap`] — a snapshot of one PDU's
/// state-DAG ancestry split into what is still missing versus what is already
/// cached in the staging area.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AncestryGap {
    /// `prev_state_events` ids reachable from the query heads that are present
    /// in NEITHER the committed `events` NOR the staging area — the still-
    /// missing frontier to fetch from a peer. Empty ⇒ the heads' state
    /// ancestry is fully grounded (staged ∪ committed), so the staged subgraph
    /// can be applied (the inbound worker re-drains it).
    pub missing: Vec<OwnedEventId>,
    /// Staged event ids reachable from the query heads via `prev_state_events`
    /// (a staged head counts itself) — the cached ancestry the worker drains
    /// once `missing` is empty, and the boundary to exclude from the next peer
    /// fetch so we re-request only the frontier. Unordered.
    pub staged: Vec<OwnedEventId>,
}

/// One staged PDU as returned by [`StagingStore::staged_for_room`]: the raw
/// bytes plus the metadata the background worker needs to process it (the
/// originating server, for gap-fill fetches). The `event_id` is the staging key.
#[derive(Debug, Clone)]
pub struct StagedPdu {
    pub event_id: OwnedEventId,
    pub origin: OwnedServerName,
    pub raw: Box<RawJsonValue>,
}

/// Pre-auth staging of inbound federation PDUs and the ancestry fetched while
/// gap-filling them.
///
/// Events staged here are NOT authorised and MUST NOT be given a stream
/// position or surface in any read / state-res path — applying them through
/// the per-room actor (which auths, resolves, and persists) is the only way
/// they become real. See the `staged_events` table comment in `schema.sql`.
/// Presence = pending; absence (after [`unstage_events`](StagingStore::unstage_events))
/// = processed. Retry backoff is the worker's in-memory concern, not stored here.
#[async_trait]
pub trait StagingStore: Send + Sync {
    /// Pre:  `raw` is the canonical post-`from_wire` bytes whose reference hash
    ///       is `event_id` (so id ↔ bytes round-trip), `room_id` matches, and
    ///       `origin` is the server it arrived from (or was fetched from).
    /// Post: `(event_id, room_id, origin, raw)` is recorded in the staging
    ///       area; idempotent — re-staging the same id is a no-op (a peer may
    ///       resend, and gap-fill may re-fetch, the same event). Does NOT
    ///       advance the `subscribe` watch (staged events are invisible).
    ///       Returns `true` if a new row was inserted, `false` if the id was
    ///       already staged (an ignored duplicate) — the gap-fill loop uses
    ///       this to tell "fetched new ancestry" from "peer re-sent what we
    ///       already hold". Staging is deliberately *unbounded*: grounding an
    ///       event requires fetching its entire state-DAG ancestry back to
    ///       `m.room.create`, however deep (inherent to MSC4242 / auth-chain
    ///       CRDTs), and the mesh is trusted.
    async fn stage_pdu(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        raw: &RawJsonValue,
    ) -> Result<bool, StorageError>;

    /// Pre:  none.
    /// Post: returns the distinct `room_id`s that have at least one staged PDU.
    ///       The background worker enumerates these on startup (and on demand)
    ///       to know which rooms have pending work to drain.
    async fn staged_rooms(&self) -> Result<Vec<OwnedRoomId>, StorageError>;

    /// Pre:  none.
    /// Post: returns every staged PDU for `room_id` (in unspecified order — the
    ///       caller toposorts by `prev_events ∪ prev_state_events` before
    ///       applying). Empty if the room has no staged rows.
    async fn staged_for_room(&self, room_id: &RoomId) -> Result<Vec<StagedPdu>, StorageError>;

    /// Pre:  none (`heads` need not exist).
    /// Post: walks `prev_state_events` back from `heads` through staged events
    ///       (a committed `events` row is a grounded boundary and is not
    ///       expanded), returning an [`AncestryGap`]: `missing` = reachable ids
    ///       in neither table, `staged` = reachable ids currently staged. The
    ///       walk is scoped to `room_id`.
    async fn ancestry_gap(
        &self,
        room_id: &RoomId,
        heads: &[&EventId],
    ) -> Result<AncestryGap, StorageError>;

    /// Pre:  none.
    /// Post: deletes the matching staged rows; idempotent — ids not present are
    ///       ignored. Called once a staged PDU has been durably applied.
    async fn unstage_events(&self, event_ids: &[&EventId]) -> Result<(), StorageError>;
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
    /// Post: returns up to `limit` of the oldest undelivered PDUs for `destination` in
    ///       insertion (causal) order; does not remove them — the caller must call
    ///       `remove_pdus` after a successful `/send` transaction. Bounding by `limit`
    ///       keeps a single drain from loading an unbounded backlog into memory; the
    ///       caller drains a long queue one `limit`-sized batch at a time.
    async fn pending_pdus(
        &self,
        destination: &ServerName,
        limit: usize,
    ) -> Result<Vec<Event>, StorageError>;

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

    /// Pre:  none.
    /// Post: returns the server names of every destination with at least one
    ///       pending advertisement obligation (a `pending_advertisements` row)
    ///       not yet cleared via `remove_advertisements`. The anti-entropy
    ///       sibling of `pending_destinations`: the outbound supervisor unions
    ///       the two so a quiescent destination owed only an advertisement still
    ///       gets a sender task. Same subscribe-before-query caveat applies — the
    ///       obligation is enqueued in `persist_resolved_event`'s transaction,
    ///       which advances the same `subscribe()` watch.
    async fn advertisement_destinations(&self) -> Result<Vec<OwnedServerName>, StorageError>;

    /// Pre:  none (returns empty vec if `destination` has no pending obligations).
    /// Post: returns the rooms `destination` is owed a forward-extremity
    ///       advertisement for — the `pending_advertisements` rows for that
    ///       destination. The sender reads the room's current extremities at
    ///       send time, so this returns only the rooms, not a snapshot of heads.
    async fn pending_advertisements(
        &self,
        destination: &ServerName,
    ) -> Result<Vec<OwnedRoomId>, StorageError>;

    /// Pre:  must only be called after the destination returned HTTP 2xx for a
    ///       `/send` transaction that advertised our forward extremities for
    ///       these rooms (an advertisement, or a normal FE-carrying transaction
    ///       that covered them — the piggyback satisfies the obligation).
    /// Post: removes the matching `(destination, room_id)` rows; idempotent —
    ///       calling with rooms that have no pending obligation does not error.
    async fn remove_advertisements(
        &self,
        destination: &ServerName,
        rooms: &[&RoomId],
    ) -> Result<(), StorageError>;
}

#[async_trait]
pub trait FederationInbox: Send + Sync {
    /// Pre:  none.
    /// Post: returns `true` if `(origin, txn_id)` has already been recorded, without
    ///       recording it. The inbound `/send` handler uses this as a cheap whole-
    ///       transaction short-circuit *before* staging — distinct from
    ///       [`record_federation_txn`](Self::record_federation_txn), which is called
    ///       only *after* the transaction's PDUs are durably staged, so a mid-stage
    ///       fault leaves the txn unrecorded and a resend re-stages (never lost).
    async fn federation_txn_seen(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<bool, StorageError>;

    /// Pre:  none.
    /// Post: records `(origin, txn_id)` as processed; returns `true` if it was already
    ///       recorded, `false` if this is the first time seeing this transaction.
    async fn record_federation_txn(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<bool, StorageError>;
}

/// Out-of-band membership invites: invites for rooms where we host the
/// **invitee** but hold no room state — no `m.room.create`, no auth chain —
/// so they cannot go through `RoomCore::apply_pdu` (which auths a PDU against
/// the room's state DAG). The inbound `/invite` federation handler stores the
/// invite here; the sync invite path surfaces it.
///
/// Keyed by `(room_id, user_id)`. For an `m.room.member` invite the
/// `state_key` *is* the invited `user_id`, so the pair uniquely identifies
/// the invite. The stored event's `raw` carries the inviting server's
/// `unsigned.invite_room_state` (already-stripped state events), which is the
/// only room context we have pre-accept — the sync builder renders the room
/// name / inviter from it.
///
/// This is a separate table from the authed room state on purpose: an OOB
/// invite carries no auth and lives outside any room's DAG, so it must not be
/// folded into `current_state` / `rooms_with_membership`. On accept (the room
/// enters normal joined state) or reject (membership → leave) the stub is
/// removed.
#[async_trait]
pub trait InviteStore: Send + Sync {
    /// Pre:  `event` is an `m.room.member` event with `content.membership =
    ///       "invite"`, `state_key == user_id`, and `room_id == room_id`; its
    ///       `raw` is the canonical wire form (round-trips its `event_id`) and
    ///       carries `unsigned.invite_room_state`.
    /// Post: records the invite keyed by `(room_id, user_id)`, **replacing**
    ///       any prior invite for the same pair — latest invite wins (a peer
    ///       may re-invite after a decline; the most recent stripped state is
    ///       the one to render). Does NOT advance the persist watch (an OOB
    ///       invite is not a room event; it surfaces only via the sync invite
    ///       path).
    async fn put_invite(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        event: &Event,
    ) -> Result<(), StorageError>;

    /// Pre:  none.
    /// Post: returns the stored invite `m.room.member` event for
    ///       `(room_id, user_id)`, or `None` if none is held. The returned
    ///       `Event` round-trips the stored wire bytes (including
    ///       `unsigned.invite_room_state`).
    async fn get_invite(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Event>, StorageError>;

    /// Pre:  none.
    /// Post: deletes the invite for `(room_id, user_id)` if present;
    ///       idempotent (a missing pair is a no-op). Called when the invite is
    ///       accepted or rejected.
    async fn remove_invite(&self, room_id: &RoomId, user_id: &UserId) -> Result<(), StorageError>;

    /// Pre:  none.
    /// Post: returns the `room_id` of every room in which `user_id` currently
    ///       holds an out-of-band invite. Sync unions these into its room list
    ///       (as membership = invite), separately from `rooms_with_membership`
    ///       since OOB invites live in a different table and carry no auth.
    async fn invited_oob_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError>;
}

/// The server's persistent identity facts: the opaque 32-byte node secret and
/// the local user's display name. The server derives its stable identity from
/// the secret (and, when unconfigured, its federation `server_name`), so these
/// must survive restarts — see the `server_identity` table in the SQLite schema.
#[async_trait]
pub trait IdentityStore: Send + Sync {
    /// Pre:  `fresh_seed` is 32 cryptographically-random bytes from the caller.
    ///       The store does not generate keys itself (SQLite's `randomblob` is
    ///       not a guaranteed CSPRNG), so generation lives caller-side.
    /// Post: stores `fresh_seed` as the node secret on the first call and
    ///       returns it; on every later call (and after a restart) returns the
    ///       already-persisted secret, ignoring `fresh_seed` — first write wins.
    async fn get_or_create_node_secret(
        &self,
        fresh_seed: [u8; 32],
    ) -> Result<[u8; 32], StorageError>;

    /// The local user's persisted display name, or `None` if never set (the
    /// caller applies the product default). Set via [`set_display_name`].
    async fn get_display_name(&self) -> Result<Option<String>, StorageError>;

    /// Persist the local user's display name, replacing any previous value
    /// (`PUT /profile/{user}/displayname`).
    async fn set_display_name(&self, name: &str) -> Result<(), StorageError>;

    /// Pre:  `current` names the trust domain this process is starting under
    ///       (e.g. `"transitive"` / `"signed"` — the store is agnostic).
    /// Post: first call persists `current` and returns it; every later call
    ///       (and after a restart) returns the originally-persisted value,
    ///       ignoring `current` — first write wins, like the node secret.
    ///       The caller compares the result against its own mode and refuses
    ///       startup on mismatch: a store whose events were persisted without
    ///       signatures can never serve a signed deployment (and vice versa),
    ///       so the two domains must not mix.
    async fn get_or_create_trust_domain(&self, current: &str) -> Result<String, StorageError>;
}

/// Combined storage interface. Use as a generic bound: `S: StorageBackend`.
pub trait StorageBackend:
    RoomStore
    + EventStore
    + StateStore
    + DagStore
    + FederationOutbox
    + FederationInbox
    + StagingStore
    + InviteStore
    + IdentityStore
{
}

impl<T> StorageBackend for T where
    T: RoomStore
        + EventStore
        + StateStore
        + DagStore
        + FederationOutbox
        + FederationInbox
        + StagingStore
        + InviteStore
        + IdentityStore
{
}

/// Bridge a store to a read-only [`StateProvider`] view of itself.
///
/// The per-room actor in the engine owns the state machine (`RoomCore`) but
/// not a store connection, so it cannot build a provider directly. This hands
/// it one for the duration of `f` — `f` runs the apply (a read: immutable
/// events + auth chains, no write transaction) and returns an owned result.
///
/// Kept out of [`StorageBackend`] deliberately: the generic method is not
/// object-safe, so folding it into the super-trait would make
/// `dyn StorageBackend` impossible. Consumers bound on both:
/// `S: StorageBackend + WithStateProvider`.
///
/// `R` must own its result — the provider lives only for the call to `f`, so a
/// returned value cannot borrow from it.
///
/// [`StateProvider`]: neutrino_room::provider::StateProvider
pub trait WithStateProvider: Send + Sync {
    fn with_state_provider<F, R>(
        &self,
        f: F,
    ) -> impl std::future::Future<Output = Result<R, StorageError>> + Send
    where
        F: for<'a> FnOnce(&'a dyn neutrino_room::provider::StateProvider) -> R + Send + 'static,
        R: Send + 'static;
}
