//! Row ↔ event I/O. Single source of truth for the column shape on both
//! the SELECT side (hydration into `StoredEvent`) and the INSERT side
//! ([`EventRow::write_into_tx`]). Per design doc §3: keeping this in one
//! place means the SELECTs and INSERTs in `store/{events,state,dag,outbox}.rs`
//! all agree on what they project, and a schema change touches one file.
//!
//! Column access on the SELECT side is by *name*, not by position. Any
//! query feeding [`EventRow::try_from`] only needs to project the seven
//! [`EVENT_COLUMNS`] (or [`EVENT_COLUMNS_PREFIXED`] when JOINing with an
//! `e` alias); their position in the row (and any extra leading columns
//! like `stream_pos`) is irrelevant.

use std::{borrow::Cow, ops::Deref};

use deadpool_sqlite::rusqlite::{Row, Transaction, params};
use neutrino_store::StoredEvent;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::error::Error;

/// Canonical event-row column list. SELECTs feeding [`EventRow::try_from`]
/// must project at least these columns by name (order doesn't matter,
/// extra columns are allowed and ignored).
pub(crate) const EVENT_COLUMNS: &str =
    "event_id, room_id, event_type, state_key, sender, origin_server_ts, json";

/// Same as [`EVENT_COLUMNS`] but with an `e.` prefix on each column, for
/// SELECTs that JOIN `events` aliased as `e` (state, outbox, etc.). Keep
/// in sync with [`EVENT_COLUMNS`] — one is just the prefixed sibling of
/// the other.
pub(crate) const EVENT_COLUMNS_PREFIXED: &str =
    "e.event_id, e.room_id, e.event_type, e.state_key, e.sender, e.origin_server_ts, e.json";

/// Row-shape wrapper around a [`StoredEvent`].
///
/// - **Read**: `EventRow::try_from(row)?` produces `EventRow<'static>`
///   (owned). Unwrap via [`EventRow::into_event`].
/// - **Write**: `EventRow::from(&event).write_into_tx(&tx)?` borrows the
///   caller's event; no clone.
pub(crate) struct EventRow<'a>(pub Cow<'a, StoredEvent>);

impl Deref for EventRow<'_> {
    type Target = StoredEvent;
    fn deref(&self) -> &StoredEvent {
        &self.0
    }
}

impl<'a> From<&'a StoredEvent> for EventRow<'a> {
    fn from(event: &'a StoredEvent) -> Self {
        Self(Cow::Borrowed(event))
    }
}

impl From<StoredEvent> for EventRow<'static> {
    fn from(event: StoredEvent) -> Self {
        Self(Cow::Owned(event))
    }
}

impl TryFrom<&Row<'_>> for EventRow<'static> {
    type Error = Error;

    /// Parse failures on ruma IDs or `RawValue` map to `Error::Internal`:
    /// rows in `events` were written by this crate, so a parse failure
    /// means DB corruption or a bug in our writers, not bad caller input.
    fn try_from(row: &Row<'_>) -> Result<Self, Self::Error> {
        let event_id: String = row.get("event_id")?;
        let room_id: String = row.get("room_id")?;
        let event_type: String = row.get("event_type")?;
        let state_key: Option<String> = row.get("state_key")?;
        let sender: String = row.get("sender")?;
        let origin_server_ts: i64 = row.get("origin_server_ts")?;
        let json: String = row.get("json")?;

        let event_id = OwnedEventId::try_from(event_id)
            .map_err(|e| Error::Internal(format!("malformed event_id in DB row: {e}")))?;
        let room_id = OwnedRoomId::try_from(room_id)
            .map_err(|e| Error::Internal(format!("malformed room_id in DB row: {e}")))?;
        let sender = OwnedUserId::try_from(sender)
            .map_err(|e| Error::Internal(format!("malformed sender in DB row: {e}")))?;
        let json = RawValue::from_string(json)
            .map_err(|e| Error::Internal(format!("malformed json in DB row: {e}")))?;
        // SQLite stores INTEGER (i64). The write side rejects values
        // outside `0..=i64::MAX`, so any negative we see here is DB
        // corruption / a manual SQL edit — `Internal`, not `InvalidInput`.
        let origin_server_ts = u64::try_from(origin_server_ts).map_err(|_| {
            Error::Internal(format!(
                "negative origin_server_ts in DB row: {origin_server_ts}"
            ))
        })?;

        Ok(EventRow(Cow::Owned(StoredEvent {
            event_id,
            room_id,
            event_type,
            state_key,
            sender,
            origin_server_ts,
            json,
        })))
    }
}

impl<'a> EventRow<'a> {
    /// Unwrap into the inner [`StoredEvent`], allocating only if the
    /// underlying `Cow` is `Borrowed`. For the `TryFrom<&Row>` path the
    /// `Cow` is always `Owned`, so this is a zero-cost move.
    pub fn into_event(self) -> StoredEvent {
        self.0.into_owned()
    }

    /// Promote a (possibly borrowed) `EventRow` to a `'static` one,
    /// cloning the inner [`StoredEvent`] if necessary. Inherent shadow of
    /// `ToOwned::to_owned` — the std trait would require a `Borrow<Self>`
    /// dance across `EventRow<'a>` ↔ `EventRow<'static>` that the blanket
    /// `impl<T> Borrow<T> for T` makes unsound without specialisation.
    pub fn to_owned(&self) -> EventRow<'static> {
        EventRow::from(self.0.as_ref().clone())
    }

    /// One-shot event write: crack JSON, INSERT into `events`, INSERT
    /// edges, upsert current state if it's a state event. Returns the new
    /// `stream_pos`. Shared between `persist_event` and `create_room`.
    ///
    /// Does NOT touch the outbox or fire the watch — those are the
    /// caller's responsibility (`persist_event` writes outbox rows + fires
    /// the watch; `create_room` does neither because new rooms have no
    /// remote members and the initial-event batch advances the watch
    /// once at the end).
    pub fn write_into_tx(&self, tx: &Transaction<'_>) -> Result<i64, Error> {
        // Inline cracker for the JSON fields we need *and* the ones we
        // cross-check against the `StoredEvent` columns. A caller that
        // passes a `StoredEvent` whose column values disagree with the
        // raw JSON (different `type`, `room_id`, `sender`, or
        // `state_key`) could otherwise silently desync the two tables:
        // every read path projects the columns, but federation re-emits
        // the raw JSON, so a downstream consumer would see one shape via
        // the trait and another via the wire. Reject at the write
        // boundary instead — defence-in-depth against a buggy upstream
        // caller; the trust boundary still nominally lives at the
        // handler.
        #[derive(Deserialize)]
        struct CrackedEvent {
            #[serde(rename = "type", default)]
            event_type: Option<String>,
            #[serde(default)]
            room_id: Option<String>,
            #[serde(default)]
            sender: Option<String>,
            // Outer-None = field absent in JSON (skip cross-check);
            // outer-Some(inner) = field present (inner is the actual
            // state_key, which may itself be JSON `null` → Rust None).
            #[serde(default)]
            state_key: Option<Option<String>>,
            #[serde(default)]
            prev_events: Vec<String>,
            #[serde(default)]
            prev_state_events: Vec<String>,
            #[serde(default)]
            content: CrackedContent,
        }
        #[derive(Deserialize, Default)]
        struct CrackedContent {
            membership: Option<String>,
        }

        let cracked: CrackedEvent = serde_json::from_str(self.json.get())
            .map_err(|e| Error::InvalidInput(format!("event json: {e}")))?;

        // Cross-check every column against the JSON copy. A `None` on
        // the JSON side means "field absent"; we treat that as agreement
        // since the column is the canonical value (callers building a
        // `StoredEvent` may legitimately omit redundant fields from the
        // JSON, though the test helpers always emit them). A `Some`
        // that disagrees with the column is a hard reject. For
        // `state_key` the JSON value is `Option<Option<String>>`:
        // outer-None = field absent (skip check), outer-Some = present
        // (inner is the actual state_key, which may itself be JSON
        // null → Rust None).
        if let Some(t) = cracked.event_type.as_deref()
            && t != self.event_type
        {
            return Err(Error::InvalidInput(format!(
                "event.json `type` ({t:?}) disagrees with column `event_type` ({:?})",
                self.event_type
            )));
        }
        if let Some(r) = cracked.room_id.as_deref()
            && r != self.room_id.as_str()
        {
            return Err(Error::InvalidInput(format!(
                "event.json `room_id` ({r:?}) disagrees with column `room_id` ({:?})",
                self.room_id.as_str()
            )));
        }
        if let Some(s) = cracked.sender.as_deref()
            && s != self.sender.as_str()
        {
            return Err(Error::InvalidInput(format!(
                "event.json `sender` ({s:?}) disagrees with column `sender` ({:?})",
                self.sender.as_str()
            )));
        }
        if let Some(json_state_key) = cracked.state_key.as_ref()
            && json_state_key.as_deref() != self.state_key.as_deref()
        {
            return Err(Error::InvalidInput(format!(
                "event.json `state_key` ({:?}) disagrees with column `state_key` ({:?})",
                json_state_key, self.state_key
            )));
        }

        // `membership` is non-NULL exactly for `m.room.member` rows per
        // the schema comment; that invariant is what makes
        // `joined_members` / `joined_rooms` filterable via the partial
        // indexes. If a member event arrives without
        // `content.membership` *or* without a `state_key` (the user_id),
        // writing it would either leave `current_state.membership` NULL
        // (invisible to member filters) or skip the `current_state`
        // upsert entirely — both silently break the invariant. Reject
        // at the write boundary instead.
        let membership = if self.event_type == "m.room.member" {
            let m = cracked.content.membership.as_deref().ok_or_else(|| {
                Error::InvalidInput("m.room.member event missing content.membership".into())
            })?;
            if self.state_key.is_none() {
                return Err(Error::InvalidInput(
                    "m.room.member event missing state_key".into(),
                ));
            }
            Some(m)
        } else {
            None
        };

        let origin_server_ts = i64::try_from(self.origin_server_ts).map_err(|_| {
            Error::InvalidInput(format!(
                "origin_server_ts {} exceeds i64::MAX",
                self.origin_server_ts
            ))
        })?;
        tx.execute(
            "INSERT INTO events \
             (event_id, room_id, event_type, state_key, sender, origin_server_ts, json) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                self.event_id.as_str(),
                self.room_id.as_str(),
                self.event_type,
                self.state_key,
                self.sender.as_str(),
                origin_server_ts,
                self.json.get(),
            ],
        )?;
        let stream_pos = tx.last_insert_rowid();

        {
            let mut stmt = tx.prepare(
                "INSERT INTO event_edges (child_event_id, edge_type, parent_event_id) \
                 VALUES (?, ?, ?)",
            )?;
            for parent in &cracked.prev_events {
                stmt.execute(params![self.event_id.as_str(), "prev", parent])?;
            }
            for parent in &cracked.prev_state_events {
                stmt.execute(params![self.event_id.as_str(), "prev_state", parent])?;
            }
        }

        if let Some(sk) = self.state_key.as_deref() {
            tx.execute(
                "INSERT INTO current_state \
                 (room_id, event_type, state_key, event_id, membership) \
                 VALUES (?, ?, ?, ?, ?) \
                 ON CONFLICT(room_id, event_type, state_key) DO UPDATE SET \
                     event_id = excluded.event_id, \
                     membership = excluded.membership",
                params![
                    self.room_id.as_str(),
                    self.event_type,
                    sk,
                    self.event_id.as_str(),
                    membership,
                ],
            )?;
        }

        Ok(stream_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::{EVENT_COLUMNS, EVENT_COLUMNS_PREFIXED};

    // C1: drift guard between the two column-list consts. If someone edits
    // one and forgets the other, this test catches it. The two consts are
    // intentionally hand-written (clearer at the query call sites) rather
    // than macro-generated; this is the cost of that choice.
    #[test]
    fn event_columns_prefix_matches() {
        let expected = EVENT_COLUMNS
            .split(", ")
            .map(|c| format!("e.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(EVENT_COLUMNS_PREFIXED, expected);
    }
}
