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
CREATE TABLE rooms (
    room_id        TEXT NOT NULL PRIMARY KEY,
    room_version   TEXT NOT NULL CHECK (room_version = 'org.matrix.msc4242.12')
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
    -- visibility (client relay, etc.). Default 0 covers the common
    -- "freshly persisted, accepted" case — no production write path
    -- emits `rejected = 1` yet (see `Event.rejected` in
    -- `neutrino-common`).
    rejected          INTEGER NOT NULL DEFAULT 0
        CHECK (rejected IN (0, 1))
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
-- Single table with edge_type (design Decision §6). No FK on
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
-- federation_txns — FederationInbox::record_federation_txn
-- Minimal per Decision §6. No received_at / GC for V1.
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
