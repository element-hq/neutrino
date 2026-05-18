-- ============================================================================
-- neutrino-store-sqlite — V1 schema (bundled, no migration framework yet).
-- See docs/2026-05-14-sqlite-storage-backend.md for the design rationale.
--
-- This file only ever runs against a fresh database; the loader gates on
-- `PRAGMA user_version` before executing it. See "Open path: version gate
-- & schema bundle" in the design doc.
--
-- Per-connection PRAGMAs (foreign_keys=ON, synchronous=NORMAL,
-- busy_timeout=5000, trusted_schema=OFF) are applied by the deadpool init
-- hook on every connection check-out, NOT here — they don't persist in
-- the DB file.
-- ============================================================================

-- One-time, persistent in the DB file.
PRAGMA journal_mode = WAL;
PRAGMA user_version = 1;

-- ----------------------------------------------------------------------------
-- rooms — RoomStore
-- ----------------------------------------------------------------------------
CREATE TABLE rooms (
    room_id        TEXT NOT NULL PRIMARY KEY,
    room_version   TEXT NOT NULL
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
    json              TEXT    NOT NULL
) STRICT;

CREATE INDEX ix_events_room_stream ON events(room_id, stream_pos);

-- ----------------------------------------------------------------------------
-- event_edges — DagStore (events_before, missing_events)
-- Single table with edge_type (design Decision §6). No FK to events:
-- federation backfill may insert children referencing parents we haven't yet
-- seen. PK ordering (child, edge_type, parent) supports both "parents of
-- child" and "parents of child for a given edge_type" via PK prefix scans.
-- ----------------------------------------------------------------------------
CREATE TABLE event_edges (
    child_event_id   TEXT NOT NULL,
    edge_type        TEXT NOT NULL
        CHECK (edge_type IN ('prev', 'prev_state')),
    parent_event_id  TEXT NOT NULL,
    PRIMARY KEY (child_event_id, edge_type, parent_event_id)
) STRICT, WITHOUT ROWID;

-- ----------------------------------------------------------------------------
-- current_state — StateStore
-- One row per (room, event_type, state_key) reflecting resolved current
-- state. Superseded historical events live in `events` only.
-- state_key is TEXT NOT NULL — the empty string is a valid state key
-- (m.room.create, m.room.power_levels, etc.), only SQL NULL is disallowed.
-- `membership` is non-NULL exactly for m.room.member rows; carries the
-- canonical value so joined_rooms / joined_members can filter via an
-- indexed column without loading event JSON (per the post-condition).
-- ----------------------------------------------------------------------------
CREATE TABLE current_state (
    room_id      TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    state_key    TEXT NOT NULL,
    event_id     TEXT NOT NULL REFERENCES events(event_id),
    membership   TEXT
        CHECK (membership IS NULL
               OR membership IN ('join','leave','ban','invite','knock')),
    PRIMARY KEY (room_id, event_type, state_key)
) STRICT, WITHOUT ROWID;

-- joined_rooms(user) and rooms_with_membership(user, memberships): direct
-- lookup by state_key. Partial on `event_type = 'm.room.member'` only —
-- broadened from the prior `membership = 'join'` constraint so the same
-- index serves any membership filter (sliding sync enumerates rooms across
-- the full MSC4186-eligible membership set in one shot).
CREATE INDEX ix_current_state_member
    ON current_state(state_key, room_id, membership)
    WHERE event_type = 'm.room.member';

-- joined_members(room): direct lookup by room_id. Same broadening — the
-- caller filters by membership in the WHERE clause and SQLite uses the
-- (room_id, …) prefix.
CREATE INDEX ix_current_state_room_member
    ON current_state(room_id, state_key, membership)
    WHERE event_type = 'm.room.member';

-- ----------------------------------------------------------------------------
-- client_txns — EventStore::{record_client_txn, get_client_txn}
-- INSERT OR IGNORE gives the post-condition's idempotency.
-- ----------------------------------------------------------------------------
CREATE TABLE client_txns (
    txn_id    TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    event_id  TEXT NOT NULL REFERENCES events(event_id),
    PRIMARY KEY (txn_id, user_id)
) STRICT, WITHOUT ROWID;

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
