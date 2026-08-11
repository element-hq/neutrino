-- ============================================================================
-- neutrino-store-sqlite — V1 schema (bundled, no migration framework yet).
-- See docs/2026-05-14-sqlite-storage-backend.md for the design rationale.
--
-- This file only ever runs against a fresh database; the loader gates on
-- `PRAGMA user_version` before executing it. See "Open path: version gate
-- & schema bundle" in the design doc.
--
-- Surrounding PRAGMAs are owned by `schema::ensure_schema`, not by this
-- file:
--   * `journal_mode = WAL` is set against the bare connection before
--     the bundle runs (SQLite forbids journal-mode changes inside a
--     transaction).
--   * `user_version = 1` is stamped inside the same transaction that
--     wraps this file's DDL, immediately before commit. That keeps
--     "schema present" and "version stamp set" atomic — a mid-bundle
--     failure rolls back both together, leaving `user_version` at 0
--     so the next open re-runs the (non-`IF NOT EXISTS`) bundle.
--
-- Per-connection PRAGMAs (foreign_keys=ON, synchronous=NORMAL,
-- busy_timeout=5000, trusted_schema=OFF) are applied by the deadpool init
-- hook on every connection check-out, NOT here — they don't persist in
-- the DB file.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- rooms — RoomStore
-- ----------------------------------------------------------------------------
-- `room_version` CHECK pins the only wire identifier this server accepts —
-- mirrors `neutrino_common::ROOM_VERSION_ID` (MSC4242-on-v12). Defends
-- against a corrupt-DB / bad-migration path where a row with a different
-- string gets inserted: the trait read paths would otherwise see
-- `RoomVersionId::Custom(<wrong-string>)` (ruma maps unknown strings to
-- Custom rather than erroring) and propagate it silently. The string is
-- duplicated here rather than templated because it's both a schema-level
-- invariant *and* a stability promise — if/when the value ever changes
-- the schema needs an explicit migration, not a silent code edit.
-- `forward_extremities` / `state_dag_forward_extremities` persist the two
-- head-sets of a room's `RoomCore` (neutrino-state) so the per-room actor
-- can be rebuilt after a restart without replaying the whole DAG:
--   * `forward_extremities`           — timeline-DAG heads (events not yet
--                                       referenced by any event's `prev_events`).
--   * `state_dag_forward_extremities` — state-DAG heads (state events not yet
--                                       referenced by any `prev_state_events`).
-- Both are JSON arrays of event ids. `DEFAULT '[]'` covers the create-time
-- INSERT (which omits them); the storage-side apply+persist path is what
-- maintains them thereafter. No `user_version` bump — no live data, schema
-- amended in place (same policy as the `events.rejected` column).
CREATE TABLE rooms (
    room_id        TEXT NOT NULL PRIMARY KEY,
    room_version   TEXT NOT NULL CHECK (room_version = 'org.matrix.msc4242.12'),
    forward_extremities            TEXT NOT NULL DEFAULT '[]',
    state_dag_forward_extremities  TEXT NOT NULL DEFAULT '[]'
) STRICT, WITHOUT ROWID;

-- ----------------------------------------------------------------------------
-- events — EventStore (persist_event, get_events, events_after, room_messages)
-- stream_pos drives the monotonic stream order, PaginationToken, and the
-- subscribe() watch. AUTOINCREMENT requires a rowid alias, so no WITHOUT ROWID.
-- room_messages orders by stream_pos (insertion order), NOT origin_server_ts
-- (sender clock — untrusted).
-- ----------------------------------------------------------------------------
CREATE TABLE events (
    stream_pos        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT    NOT NULL UNIQUE,
    room_id           TEXT    NOT NULL REFERENCES rooms(room_id),
    event_type        TEXT    NOT NULL,
    state_key         TEXT,                          -- NULL ⇔ non-state event
    sender            TEXT    NOT NULL,
    origin_server_ts  INTEGER NOT NULL,
    json              TEXT    NOT NULL,
    -- MSC4242: `auth_events` is calculated server-side and not on the
    -- wire. Stored here as a JSON array of event ids so it survives the
    -- round-trip into the canonical `neutrino_common::Event`. The
    -- denormalised `event_edges WHERE edge_type='auth'` rows below are
    -- derivable from this column; see `event-id-design.md` §"What
    -- event_edges is doing".
    auth_events_json  TEXT    NOT NULL DEFAULT '[]',
    -- Server-side rejection verdict from auth-rule evaluation. Rejected
    -- events MUST still be observable so state-res can skip their
    -- auth-chains and child events can detect a rejected
    -- `prev_state_events` ancestor; the flag gates downstream
    -- visibility (client reads filter it; federation reads serve it).
    -- Written by every federation ingest path: `apply_pdu` persists
    -- auth-rejects, reference-cascade rejects, and `Wire::Rejected`
    -- state-independent rule failures; backfill persists `Wire::Rejected`
    -- history verbatim. See `Event.rejected` in `neutrino-event`.
    rejected          INTEGER NOT NULL DEFAULT 0
        CHECK (rejected IN (0, 1)),
    -- Server-side soft-fail verdict: the event passed auth against
    -- state-before-event but failed against current room state. It is
    -- persisted and observed, but never becomes a forward extremity and
    -- must be kept out of client timelines. Default 0 covers the common
    -- "accepted, not soft-failed" case. See `Event.soft_failed` in
    -- `neutrino-common`.
    soft_failed       INTEGER NOT NULL DEFAULT 0
        CHECK (soft_failed IN (0, 1))
) STRICT;

CREATE INDEX ix_events_room_stream ON events(room_id, stream_pos);

-- Supporting UNIQUE index for the composite FK on `current_state` below.
-- `event_id` alone is already UNIQUE, so any superset of columns including
-- it is also unique — but SQLite needs an index whose column list matches
-- the FK's parent-key column list verbatim. Without this, the FK on
-- `current_state(event_id, room_id, event_type, state_key)` won't compile.
CREATE UNIQUE INDEX ix_events_id_room_type_key
    ON events(event_id, room_id, event_type, state_key);

-- ----------------------------------------------------------------------------
-- event_edges — DagStore (events_before, missing_events)
-- Single table with edge_type. No FK on
-- `parent_event_id`: federation backfill may insert children referencing
-- parents we haven't yet seen. There IS a FK on `child_event_id` —
-- `write_into_tx` always inserts the child into `events` first within
-- the same transaction, so a satisfiable FK on the child column is free
-- correctness for any future write path that writes edges directly.
-- PK ordering (child, edge_type, parent) supports both "parents of
-- child" and "parents of child for a given edge_type" via PK prefix scans.
-- ----------------------------------------------------------------------------
CREATE TABLE event_edges (
    child_event_id   TEXT NOT NULL REFERENCES events(event_id),
    edge_type        TEXT NOT NULL
        CHECK (edge_type IN ('prev', 'prev_state', 'auth')),
    parent_event_id  TEXT NOT NULL,
    PRIMARY KEY (child_event_id, edge_type, parent_event_id)
) STRICT, WITHOUT ROWID;

-- ----------------------------------------------------------------------------
-- current_state — StateStore
-- One row per (room, event_type, state_key) reflecting resolved current
-- state. Superseded historical events live in `events` only.
-- state_key is TEXT NOT NULL — the empty string is a valid state key
-- (m.room.create, m.room.power_levels, etc.), only SQL NULL is disallowed.
-- `membership` is non-NULL exactly for m.room.member rows, enforced by
-- the CHECK below — m.room.member must hold a canonical value, every
-- other event_type must be NULL. This catches the case where a malformed
-- m.room.member event with no `content.membership` would otherwise have
-- been silently persisted with NULL membership and then quietly filtered
-- out of joined_rooms / joined_members.
--
-- The composite FK on (event_id, room_id, event_type, state_key) enforces
-- that every current_state row references an `events` row that agrees on
-- all four columns — not just `event_id`. A single-column FK would let a
-- buggy or corrupted write desync the two tables (cs row points at an
-- event in another room, or at an event with a different event_type /
-- state_key, or at a non-state event whose state_key IS NULL), which
-- would leak through every StateStore read path. With the composite FK
-- the schema rejects those writes outright. SQL three-valued logic
-- handles the non-state case naturally: `events.state_key IS NULL` makes
-- the FK equality UNKNOWN, so the FK match never resolves to TRUE.
-- See state.rs module docstring for the read-side consequence (JOINs
-- only need `cs.event_id = e.event_id`).
-- ----------------------------------------------------------------------------
CREATE TABLE current_state (
    room_id      TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    state_key    TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    membership   TEXT,
    PRIMARY KEY (room_id, event_type, state_key),
    -- `membership` ⇔ `event_type = 'm.room.member'`. The two-branch CHECK
    -- bundles the existing "valid-value-or-NULL" restriction with the
    -- new structural invariant: member rows must carry one of the five
    -- canonical membership values, non-member rows must be NULL.
    --
    -- The `membership IS NOT NULL` guard on the first branch is
    -- load-bearing under SQL three-valued logic. Without it, a
    -- m.room.member row with NULL membership evaluates the first branch
    -- as `TRUE AND (NULL IN (…))` → `TRUE AND UNKNOWN` → UNKNOWN, and
    -- SQLite treats an UNKNOWN CHECK result as *satisfied* (only FALSE
    -- rejects). The explicit IS NOT NULL collapses the branch to FALSE
    -- so the constraint actually fires for the NULL-membership case.
    CHECK (
        (event_type = 'm.room.member'
            AND membership IS NOT NULL
            AND membership IN ('join','leave','ban','invite','knock'))
        OR
        (event_type <> 'm.room.member' AND membership IS NULL)
    ),
    FOREIGN KEY (event_id, room_id, event_type, state_key)
        REFERENCES events(event_id, room_id, event_type, state_key)
) STRICT, WITHOUT ROWID;

-- joined_rooms(user) and rooms_with_membership(user, memberships): direct
-- lookup by state_key, then filter by membership. `membership` sits
-- immediately after the leading equality column so SQLite can push the
-- `membership = 'join'` / `membership IN (…)` predicate into the index
-- seek; with `room_id` between them (the prior shape) the planner could
-- only seek by `state_key` and then filter `membership` post-scan,
-- because constraints on a trailing index column aren't pushable past
-- an unconstrained intermediate column. `room_id` trails so the index
-- still covers both projected columns (`SELECT room_id, membership`).
-- Partial on `event_type = 'm.room.member'` only — broadened from the
-- prior `membership = 'join'` constraint so the same index serves any
-- membership filter (sliding sync enumerates rooms across the full
-- MSC4186-eligible membership set in one shot).
CREATE INDEX ix_current_state_member
    ON current_state(state_key, membership, room_id)
    WHERE event_type = 'm.room.member';

-- joined_members(room): direct lookup by room_id, then filter by
-- membership. Same column-ordering rule: `membership` directly after
-- the leading equality column so the seek covers both. `state_key`
-- (the user_id) trails so the index can produce the SELECT projection
-- before the JOIN to `events` for full event JSON.
CREATE INDEX ix_current_state_room_member
    ON current_state(room_id, membership, state_key)
    WHERE event_type = 'm.room.member';

-- ----------------------------------------------------------------------------
-- room_state — temporal state-group index (MSC4242 state DAG)
-- ----------------------------------------------------------------------------
-- One interval row per (room, type, state_key) value that has ever been part
-- of the room's resolved state, tracking its lifetime over the `stream_pos`
-- axis as the server's view of the room evolves. This is the state-group
-- implementation: rather than a snapshot (or snapshot+delta chain) per event,
-- a single transition log. It lets state-before-an-event be reconstructed by
-- an indexed interval query instead of a recursive `prev_state_events` walk
-- back to the create event — the read path the federation auth of an old
-- received event needs.
--
--   * `start_pos` — the `stream_pos` of the event at which this value entered
--                   the resolved state for its slot.
--   * `end_pos`   — the `stream_pos` at which it left (was superseded or
--                   removed); NULL means it is still live.
--
-- `stream_pos` is a valid topological order of the state DAG (a child is never
-- persisted before its `prev_state_events`), so the interval query
-- `start_pos <= P AND (end_pos IS NULL OR end_pos > P)` reconstructs the
-- DAG-correct state-after the event at `P`, forks included — see
-- `SqliteStateProvider::state_after`. Maintained transactionally by
-- `maintain_room_state`, called from `persist_resolved_event` and `create_room`
-- — every path that persists a state event keeps it complete, so it is read
-- back unconditionally (no per-room gate; the DB only ever runs against a fresh
-- schema, so there is no un-indexed legacy data).
--
-- PK `(room_id, event_type, state_key, start_pos)` is unique because a slot
-- changes at most once per `stream_pos` (one event), and serves the
-- open-then-close maintenance. It does NOT serve the point-in-time read
-- (`state_after`): that query constrains `room_id` (=) and `start_pos`/`end_pos`
-- (ranges) but not `event_type`/`state_key`, so against the PK the planner can
-- only seek on `room_id` and then scans every historic row in the room. The
-- `ix_room_state_at` index puts `start_pos` directly behind `room_id` so the
-- range bound is usable — chiefly helping historical reads (auth of an old
-- received event, the feature's purpose), where `start_pos <= P` prunes events
-- that did not yet exist at `P`. The partial `ix_room_state_live` index covers
-- the live set, which is the maintenance diff baseline.
CREATE TABLE room_state (
    room_id     TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    state_key   TEXT    NOT NULL,
    event_id    TEXT    NOT NULL,
    start_pos   INTEGER NOT NULL,
    end_pos     INTEGER,
    PRIMARY KEY (room_id, event_type, state_key, start_pos)
) STRICT, WITHOUT ROWID;

CREATE INDEX ix_room_state_at
    ON room_state(room_id, start_pos);

CREATE INDEX ix_room_state_live
    ON room_state(room_id, event_type, state_key)
    WHERE end_pos IS NULL;

-- ----------------------------------------------------------------------------
-- federation_txns — FederationInbox::record_federation_txn
-- Minimal: no received_at / GC for V1.
-- record_federation_txn = INSERT OR IGNORE + check `changes() == 0`.
-- ----------------------------------------------------------------------------
CREATE TABLE federation_txns (
    origin   TEXT NOT NULL,
    txn_id   TEXT NOT NULL,
    PRIMARY KEY (origin, txn_id)
) STRICT, WITHOUT ROWID;

-- ----------------------------------------------------------------------------
-- outbox — FederationOutbox
-- outbox_id is causal/insertion order required by pending_pdus.
-- UNIQUE(destination, event_id) gives persist_event's outbox idempotency
-- on retry. AUTOINCREMENT requires a rowid alias → no WITHOUT ROWID.
-- ----------------------------------------------------------------------------
CREATE TABLE outbox (
    outbox_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    destination  TEXT NOT NULL,
    event_id     TEXT NOT NULL REFERENCES events(event_id),
    UNIQUE (destination, event_id)
) STRICT;

CREATE INDEX ix_outbox_dest_order ON outbox(destination, outbox_id);

-- ----------------------------------------------------------------------------
-- deliveries — DeliveryStore
-- The newest event of a room a destination has acknowledged receiving (2xx'd
-- the /send transaction carrying it). A high-water mark, so exactly one row per
-- (room, destination) — bounded by rooms × peers, not events × peers — and
-- `record_delivery` only moves it forward: the ON CONFLICT update is guarded on
-- `excluded.stream_pos > stream_pos`, which is why `stream_pos` is denormalised
-- out of `events` rather than joined on read. `delivery_pos` orders the marks
-- against each other (MAX+1 on every advance) so sliding sync can ask for "marks
-- that moved since pos N" the same way it asks for events; it is *not* an
-- `events.stream_pos`. No `user_version` bump (additive, no live data, no
-- migration framework — same policy as the other late tables).
-- ----------------------------------------------------------------------------
CREATE TABLE deliveries (
    room_id      TEXT    NOT NULL,
    destination  TEXT    NOT NULL,
    event_id     TEXT    NOT NULL REFERENCES events(event_id),
    stream_pos   INTEGER NOT NULL,
    delivery_pos INTEGER NOT NULL,
    ts           INTEGER NOT NULL,
    PRIMARY KEY (room_id, destination)
) STRICT, WITHOUT ROWID;

CREATE INDEX ix_deliveries_pos ON deliveries(delivery_pos);

-- ----------------------------------------------------------------------------
-- staged_events — StagingStore
-- A pre-auth holding pen for federation ancestry fetched while gap-filling a
-- received PDU's state DAG. Events here are NOT yet authorised: we must auth
-- every PDU (concurrency reorders operations, so a trusted peer's event can
-- still be invalid by DAG position), and an un-vetted event must never be
-- given a stream position or surface in any read / state-res path. So this
-- table is deliberately invisible to every other store read: just the raw
-- wire bytes keyed by their computed event_id (`json` is the canonical
-- post-`from_wire` form, so `event_id` ↔ `json` round-trips).
--
-- The gap-fill loop stages fetched ancestry here, walks `events ∪
-- staged_events` via `prev_state_events` (see StagingStore::ancestry_gap) to
-- find the still-missing frontier, asks the peer only for that frontier, and
-- once the ancestry is grounded promotes the staged subgraph through the
-- per-room actor (where auth, stream positions, and verdicts finally happen)
-- and deletes it from here. Durable across restarts, so a later inbound
-- retry resumes from the cached prefix rather than refetching everything.
--
-- `origin` is the server that sent the PDU (or, for ancestry fetched during
-- gap-fill, the server we asked) — the per-row target the background worker
-- re-asks when it needs to fill a deeper gap. Per-row, not per-room, since
-- different peers can send events into the same room.
--
-- No FK on `room_id` (a holding pen, not history — same posture as the
-- FK-free `event_edges.parent_event_id`). No `user_version` bump: additive,
-- no live data, no migration framework yet (same policy as the `events`
-- rejected/soft_failed and `rooms` FE columns). Retry backoff is NOT stored
-- here — it lives in the worker's in-memory state (same as the outbound
-- sender), so a restart re-drains everything; presence = pending, absence =
-- processed.
CREATE TABLE staged_events (
    event_id  TEXT NOT NULL PRIMARY KEY,
    room_id   TEXT NOT NULL,
    origin    TEXT NOT NULL,
    json      TEXT NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX ix_staged_events_room ON staged_events(room_id);

-- ----------------------------------------------------------------------------
-- oob_invites — InviteStore
-- Out-of-band membership invites: an `m.room.member` invite for a room where
-- we host the *invitee* but hold no room state (no `m.room.create`, no auth
-- chain). Such an invite arrives via `PUT /_matrix/federation/v2/invite` and
-- CANNOT go through `RoomCore::apply_pdu` — there is no state DAG to auth it
-- against — so it lives here, outside `events` / `current_state`, and is
-- invisible to every state-res / timeline read path.
--
-- Keyed by (room_id, state_key): for an invite member event the `state_key` is
-- the invited user, so the pair is unique. `INSERT OR REPLACE` on the PK gives
-- latest-invite-wins (a peer may re-invite after a decline; the freshest
-- stripped state is the one to render). Only the canonical invite event `json`
-- is stored — every other field (event_id, type, sender, ts, content,
-- prev_events) is derivable from it, so `get_invite` rehydrates via the
-- verbatim `compute_event_id` + `parse_event` path (mirrors how
-- `staged_events` rehydrates from `json` alone — no denormalised columns to
-- drift). Crucially it does NOT redact (unlike `from_wire`), so the inviting
-- server's `unsigned.invite_room_state` (stripped state the sync builder
-- renders the room name / inviter from) survives.
--
-- No FK on `room_id` (we don't host the room — same posture as the FK-free
-- `staged_events.room_id`). No `user_version` bump (additive, no live data —
-- same policy as the staged_events / FE columns). Surfaces only via the sync
-- invite path, which unions `invited_oob_rooms(user)` into the room list.
-- `event_id` is stored, not recomputed on read: the id is known at write time,
-- and re-deriving it would mean teaching the storage layer which
-- `EventIdScheme` the deployment runs — the only place it would have needed to
-- know. Storing it also saves a parse + hash per read.
CREATE TABLE oob_invites (
    room_id    TEXT NOT NULL,
    state_key  TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    json       TEXT NOT NULL,
    PRIMARY KEY (room_id, state_key)
) STRICT, WITHOUT ROWID;

-- invited_oob_rooms(user): direct lookup by the invited user (state_key).
CREATE INDEX ix_oob_invites_user ON oob_invites(state_key);

-- ----------------------------------------------------------------------------
-- pending_advertisements — FederationOutbox (anti-entropy extension)
-- A durable per-(destination, room) obligation to advertise our forward
-- extremities to a server that has just become *joined* in the room's current
-- state. MSC anti-entropy-extension: applying a join can make P joined while we
-- hold a forward extremity P does not yet have; the base piggyback exchange
-- only reconciles on the next organic transaction, so a room that falls quiet
-- right after the join would never tell P. The obligation must survive a crash
-- — persisting the join but losing the "owe P an advertisement" record would
-- reopen exactly that divergence — so it is written in the SAME transaction as
-- the join (see `persist_resolved_event`'s `advertise_to`), drained by the
-- outbound sender (an empty-`pdus` `/send` carrying `forward_extremities`), and
-- deleted only after a 2xx (never-lose, same posture as `outbox`).
--
-- A normal FE-carrying `/send` to the destination covering this room also
-- clears the row — the piggyback satisfied the obligation. Keyed by
-- (destination, room_id) so repeated triggers coalesce into one row; INSERT OR
-- IGNORE on the PK. No FK on `room_id` (the obligation outlives nothing it must
-- reference structurally; same FK-free posture as `staged_events.room_id`). No
-- `user_version` bump (additive, no live data, no migration framework — same
-- policy as the staged_events / oob_invites / FE columns).
CREATE TABLE pending_advertisements (
    destination  TEXT NOT NULL,
    room_id      TEXT NOT NULL,
    PRIMARY KEY (destination, room_id)
) STRICT, WITHOUT ROWID;

-- ----------------------------------------------------------------------------
-- server_identity — IdentityStore
-- ----------------------------------------------------------------------------
-- A small key/value bag of the server's persistent identity facts, room to add
-- more without a schema change:
--   'secret'      — 32 raw bytes, generated once on first start by the caller
--                   (a Rust CSPRNG — NOT SQLite's `randomblob`, which is not a
--                   guaranteed CSPRNG; see `get_or_create_node_secret`). The
--                   server derives its stable identity (and, when unconfigured,
--                   its federation `server_name`) from this, so it MUST survive
--                   restarts. How that identity maps onto a network route is the
--                   transport layer's concern, not this table's.
--   'displayname' — the local user's profile display name (text), set via
--                   `PUT /profile/{user}/displayname` and read back by `/profile`.
--   'trust_domain'— which trust domain the store's events belong to
--                   (`transitive`/`signed`); first-write-wins, a mode switch on
--                   existing data is refused (see `get_or_create_trust_domain`).
--   'server_name' — the federation `server_name` the store was born under;
--                   first-write-wins, a mismatch on restart is refused because
--                   it would fork the server identity (see
--                   `get_or_create_server_name`).
-- The `secret` row is length-checked to 32 bytes; other keys are unconstrained.
-- No `user_version` bump (additive, no live data, no migration framework — same
-- policy as the other late tables).
CREATE TABLE server_identity (
    key   TEXT NOT NULL PRIMARY KEY,
    value ANY  NOT NULL,
    CHECK (key <> 'secret' OR (typeof(value) = 'blob' AND length(value) = 32))
) STRICT;
