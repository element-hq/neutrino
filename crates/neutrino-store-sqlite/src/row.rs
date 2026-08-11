//! Row ↔ event I/O. Single source of truth for the column shape on both
//! the SELECT side (hydration into `Event`) and the INSERT side
//! ([`EventRow::write_into_tx`]). Keeping this in one
//! place means the SELECTs and INSERTs in `store/{events,state,dag,outbox}.rs`
//! all agree on what they project, and a schema change touches one file.
//!
//! Column access on the SELECT side is by *name*, not by position. Any
//! query feeding [`EventRow::try_from`] only needs to project the eight
//! [`EVENT_COLUMNS`] (or [`EVENT_COLUMNS_PREFIXED`] when JOINing with an
//! `e` alias); their position in the row (and any extra leading columns
//! like `stream_pos`) is irrelevant.

use std::{borrow::Cow, ops::Deref};

use deadpool_sqlite::rusqlite::{Row, Transaction, params};
use neutrino_event::Event;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::error::Error;

/// Canonical event-row column list. SELECTs feeding [`EventRow::try_from`]
/// must project at least these columns by name (order doesn't matter,
/// extra columns are allowed and ignored).
///
/// `auth_events_json` carries the MSC4242 server-computed auth_events
/// list — it's not on the wire and not in `json`, so the column is its
/// authoritative storage. See `event-id-design.md` §"Co-location pattern".
pub(crate) const EVENT_COLUMNS: &str = "event_id, room_id, event_type, state_key, sender, origin_server_ts, json, auth_events_json, rejected, soft_failed";

/// Same as [`EVENT_COLUMNS`] but with an `e.` prefix on each column, for
/// SELECTs that JOIN `events` aliased as `e` (state, outbox, etc.). Keep
/// in sync with [`EVENT_COLUMNS`] — one is just the prefixed sibling of
/// the other.
pub(crate) const EVENT_COLUMNS_PREFIXED: &str = "e.event_id, e.room_id, e.event_type, e.state_key, e.sender, e.origin_server_ts, e.json, e.auth_events_json, e.rejected, e.soft_failed";

/// Row-shape wrapper around an [`Event`].
///
/// - **Read**: `EventRow::try_from(row)?` produces `EventRow<'static>`
///   (owned). Unwrap via [`EventRow::into_event`].
/// - **Write**: `EventRow::from(&event).write_into_tx(&tx)?` borrows the
///   caller's event; no clone.
///
/// No id round-trip check on the way in: an `Event` can only be built by
/// `EventBuilder::build` or `from_wire`, both of which derive the id from the
/// same canonical bytes a check here would re-read, so it verified the
/// deriving code against itself. Re-deriving also needs the deployment's
/// `EventIdScheme`, which storage would have to be told about purely for an
/// assertion.
pub(crate) struct EventRow<'a>(pub Cow<'a, Event>);

impl Deref for EventRow<'_> {
    type Target = Event;
    fn deref(&self) -> &Event {
        &self.0
    }
}

impl<'a> From<&'a Event> for EventRow<'a> {
    fn from(event: &'a Event) -> Self {
        Self(Cow::Borrowed(event))
    }
}

impl From<Event> for EventRow<'static> {
    fn from(event: Event) -> Self {
        Self(Cow::Owned(event))
    }
}

impl<'a> EventRow<'a> {
    /// Wrap an event without the `debug_assert_event_id_matches_raw` check.
    /// **Only for tests** that exercise the storage layer's *own* JSON
    /// validation by passing intentionally-malformed raw bytes (the column
    /// vs JSON cross-checks, malformed-JSON rejection, missing-membership
    /// CHECK constraint). Production callers must go through `From<Event>`
    /// so the round-trip check fires.
    #[cfg(test)]
    pub(crate) fn unchecked(event: &'a Event) -> Self {
        Self(Cow::Borrowed(event))
    }
}

/// Parse the auth_events_json column into a `Vec<OwnedEventId>`. Failures
/// map to `Error::Internal` — auth_events_json is written by this crate.
fn parse_auth_events_json(s: &str) -> Result<Vec<OwnedEventId>, Error> {
    let ids: Vec<String> = serde_json::from_str(s)
        .map_err(|e| Error::Internal(format!("malformed auth_events_json in DB row: {e}")))?;
    ids.into_iter()
        .map(|id| {
            OwnedEventId::try_from(id).map_err(|e| {
                Error::Internal(format!("malformed event_id in auth_events_json: {e}"))
            })
        })
        .collect()
}

/// Fields extracted from `events.json` to populate `Event`: the `content`
/// sub-RawValue, plus the `prev_events` / `prev_state_events` arrays.
struct ExtractedFields {
    content: Box<RawValue>,
    prev_events: Vec<OwnedEventId>,
    prev_state_events: Vec<OwnedEventId>,
}

/// Parse `events.json` into the fields needed to populate `Event`.
fn extract_event_fields(raw: &RawValue) -> Result<ExtractedFields, Error> {
    let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(raw.get())
        .map_err(|e| Error::Internal(format!("malformed json in DB row: {e}")))?;

    let content_value = map
        .get("content")
        .ok_or_else(|| Error::Internal("event json missing `content`".into()))?;
    let content = serde_json::value::to_raw_value(content_value)
        .map_err(|e| Error::Internal(format!("re-serialising content: {e}")))?;

    let prev_events = parse_event_id_array(&map, "prev_events")?;
    let prev_state_events = parse_event_id_array(&map, "prev_state_events")?;

    Ok(ExtractedFields {
        content,
        prev_events,
        prev_state_events,
    })
}

fn parse_event_id_array(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Vec<OwnedEventId>, Error> {
    let Some(v) = map.get(field) else {
        return Ok(Vec::new());
    };
    let arr = v
        .as_array()
        .ok_or_else(|| Error::Internal(format!("event json `{field}` is not an array")))?;
    arr.iter()
        .map(|item| {
            let s = item.as_str().ok_or_else(|| {
                Error::Internal(format!("event json `{field}` contains non-string entry"))
            })?;
            OwnedEventId::try_from(s.to_owned())
                .map_err(|e| Error::Internal(format!("malformed event_id in `{field}`: {e}")))
        })
        .collect()
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
        let auth_events_json: String = row.get("auth_events_json")?;
        let rejected: bool = row.get("rejected")?;
        let soft_failed: bool = row.get("soft_failed")?;

        let event_id = OwnedEventId::try_from(event_id)
            .map_err(|e| Error::Internal(format!("malformed event_id in DB row: {e}")))?;
        let room_id = OwnedRoomId::try_from(room_id)
            .map_err(|e| Error::Internal(format!("malformed room_id in DB row: {e}")))?;
        let sender = OwnedUserId::try_from(sender)
            .map_err(|e| Error::Internal(format!("malformed sender in DB row: {e}")))?;
        let raw = RawValue::from_string(json)
            .map_err(|e| Error::Internal(format!("malformed json in DB row: {e}")))?;
        // SQLite stores INTEGER (i64). The write side rejects values
        // outside `0..=i64::MAX`, so any negative we see here is DB
        // corruption / a manual SQL edit — `Internal`, not `InvalidInput`.
        let origin_server_ts = u64::try_from(origin_server_ts).map_err(|_| {
            Error::Internal(format!(
                "negative origin_server_ts in DB row: {origin_server_ts}"
            ))
        })?;

        let auth_events = parse_auth_events_json(&auth_events_json)?;
        let extracted = extract_event_fields(&raw)?;

        Ok(EventRow(Cow::Owned(Event {
            event_id,
            room_id,
            sender,
            event_type,
            state_key,
            origin_server_ts,
            content: extracted.content,
            prev_events: extracted.prev_events,
            prev_state_events: extracted.prev_state_events,
            auth_events,
            rejected,
            soft_failed,
            raw,
        })))
    }
}

impl<'a> EventRow<'a> {
    /// Unwrap into the inner [`Event`], allocating only if the underlying
    /// `Cow` is `Borrowed`. For the `TryFrom<&Row>` path the `Cow` is
    /// always `Owned`, so this is a zero-cost move.
    pub fn into_event(self) -> Event {
        self.0.into_owned()
    }

    /// Promote a (possibly borrowed) `EventRow` to a `'static` one,
    /// cloning the inner [`Event`] if necessary. Inherent shadow of
    /// `ToOwned::to_owned` — the std trait would require a `Borrow<Self>`
    /// dance across `EventRow<'a>` ↔ `EventRow<'static>` that the blanket
    /// `impl<T> Borrow<T> for T` makes unsound without specialisation.
    pub fn to_owned(&self) -> EventRow<'static> {
        // The borrow was already checked at the original `EventRow::from`
        // call site (or intentionally bypassed via `unchecked`); cloning
        // doesn't change the relationship between event_id and raw, so the
        // assert is redundant here. Constructing via `Cow::Owned` directly
        // avoids re-firing it.
        EventRow(Cow::Owned(self.0.as_ref().clone()))
    }

    /// Forward-extension write: crack JSON, INSERT into `events`, INSERT
    /// edges, upsert `current_state` if it's a state event. Returns the
    /// new `stream_pos`. Used by `persist_event` and `create_room`.
    ///
    /// Does NOT touch the outbox or fire the watch — those are the
    /// caller's responsibility (`persist_event` writes outbox rows + fires
    /// the watch; `create_room` does neither because new rooms have no
    /// remote members and the initial-event batch advances the watch
    /// once at the end).
    pub fn write_into_tx(&self, tx: &Transaction<'_>) -> Result<i64, Error> {
        self.write_into_tx_inner(
            tx, /* update_current_state */ true, /* explicit_pos */ None,
        )
    }

    /// Historical-backfill write: crack JSON, INSERT into `events`,
    /// INSERT edges. Does NOT upsert `current_state` — used by
    /// `persist_historical_event` for events older than the current head
    /// (`/backfill`, `/get_missing_events`). Current state already
    /// reflects the room's resolved head; backfilled state events feed
    /// history (`events_before`, `room_messages`) but must not regress
    /// the current-state view. JSON cracking, column ↔ JSON cross-
    /// checks, and member-event validation still fire — historical
    /// events have to be well-formed for the read paths to function.
    ///
    /// The position is assigned **explicitly** as
    /// `COALESCE(MIN(stream_pos), 1) - 1`, read inside this same txn, so each
    /// backfilled event lands *below* the existing minimum (negative-or-zero,
    /// decremented per call). SQLite permits explicit values — including values below the
    /// current max — into an `AUTOINCREMENT` column; the autoincrement
    /// monotonicity constraint only applies to auto-*generated* positions.
    /// This is what lets client back-pagination (`room_messages` /
    /// `/messages?dir=b`, which orders `stream_pos DESC`) walk into the
    /// backfilled tail in correct order. Returns the assigned position.
    pub fn write_into_tx_historical(&self, tx: &Transaction<'_>) -> Result<i64, Error> {
        let next_pos: i64 = tx.query_row(
            "SELECT COALESCE(MIN(stream_pos), 1) - 1 FROM events",
            [],
            |r| r.get(0),
        )?;
        self.write_into_tx_inner(tx, /* update_current_state */ false, Some(next_pos))
    }

    /// Resolved-event write: crack JSON, INSERT into `events`, INSERT edges.
    /// Does NOT upsert `current_state` — the caller drives current state
    /// explicitly from a resolved delta (see
    /// `EventStore::persist_resolved_event`). Used when state resolution may
    /// change current state for keys other than (or instead of) this event's
    /// own, so a single implicit per-key upsert would be wrong.
    pub fn write_into_tx_no_current_state(&self, tx: &Transaction<'_>) -> Result<i64, Error> {
        self.write_into_tx_inner(
            tx, /* update_current_state */ false, /* explicit_pos */ None,
        )
    }

    /// Crack + cross-check + INSERT into `events`/`event_edges`, optionally
    /// upserting `current_state`. When `explicit_pos` is `Some`, the
    /// `stream_pos` column is supplied explicitly (used by the historical
    /// backfill path to place events below the minimum); when `None`, SQLite
    /// auto-assigns an ascending position via `AUTOINCREMENT` (the forward
    /// path). Returns the assigned `stream_pos`.
    fn write_into_tx_inner(
        &self,
        tx: &Transaction<'_>,
        update_current_state: bool,
        explicit_pos: Option<i64>,
    ) -> Result<i64, Error> {
        // Inline cracker for the JSON fields we need *and* the ones we
        // cross-check against the `Event` columns. A caller that
        // passes an `Event` whose column values disagree with the raw
        // JSON (different `type`, `room_id`, `sender`, or `state_key`)
        // could otherwise silently desync the two tables: every read
        // path projects the columns, but federation re-emits the raw
        // JSON, so a downstream consumer would see one shape via the
        // trait and another via the wire. Reject at the write
        // boundary instead — defence-in-depth against a buggy upstream
        // caller; the trust boundary still nominally lives at the
        // handler.
        #[derive(Deserialize)]
        struct WriteSideCracked {
            #[serde(rename = "type", default)]
            event_type: Option<String>,
            #[serde(default)]
            room_id: Option<String>,
            #[serde(default)]
            sender: Option<String>,
            #[serde(default)]
            state_key: Option<Option<String>>,
            #[serde(default)]
            prev_events: Vec<String>,
            #[serde(default)]
            prev_state_events: Vec<String>,
            #[serde(default)]
            content: WriteSideContent,
        }
        #[derive(Deserialize, Default)]
        struct WriteSideContent {
            // Lenient `Value`, not `String`: a *rejected* member row may
            // legitimately carry a non-string membership (rule 5.1 treats a
            // present-but-wrong-typed value as a rejection verdict, and the
            // row must still be storable so descendants cascade-reject). The
            // string requirement for non-rejected rows is enforced below via
            // `as_str()` — a non-string degrades to "missing" there and hits
            // the same InvalidInput guard.
            membership: Option<serde_json::Value>,
        }

        let cracked: WriteSideCracked = serde_json::from_str(self.raw.get())
            .map_err(|e| Error::InvalidInput(format!("event json: {e}")))?;

        // Cross-check every column against the JSON copy. A `None` on
        // the JSON side means "field absent"; we treat that as agreement
        // since the column is the canonical value (callers building an
        // `Event` may legitimately omit redundant fields from the JSON,
        // though the test helpers always emit them). A `Some` that
        // disagrees with the column is a hard reject. For `state_key`
        // the JSON value is `Option<Option<String>>`: outer-None =
        // field absent (skip check), outer-Some = present (inner is
        // the actual state_key, which may itself be JSON null → Rust
        // None).
        if let Some(t) = cracked.event_type.as_deref()
            && t != self.event_type
        {
            return Err(Error::InvalidInput(format!(
                "event.raw `type` ({t:?}) disagrees with column `event_type` ({:?})",
                self.event_type
            )));
        }
        if let Some(r) = cracked.room_id.as_deref()
            && r != self.room_id.as_str()
        {
            return Err(Error::InvalidInput(format!(
                "event.raw `room_id` ({r:?}) disagrees with column `room_id` ({:?})",
                self.room_id.as_str()
            )));
        }
        if let Some(s) = cracked.sender.as_deref()
            && s != self.sender.as_str()
        {
            return Err(Error::InvalidInput(format!(
                "event.raw `sender` ({s:?}) disagrees with column `sender` ({:?})",
                self.sender.as_str()
            )));
        }
        if let Some(json_state_key) = cracked.state_key.as_ref()
            && json_state_key.as_deref() != self.state_key.as_deref()
        {
            return Err(Error::InvalidInput(format!(
                "event.raw `state_key` ({:?}) disagrees with column `state_key` ({:?})",
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
        //
        // Rejected rows are exempt: they never reach the `current_state`
        // upsert (their verdict IS the `rejected` flag), and a
        // semantically-malformed member — missing membership/state_key,
        // persisted *as rejected* per rule 5.1's REJECT disposition — must
        // be storable so a descendant's reference check cascade-rejects
        // instead of gapfill-refetching the offender forever.
        let membership = if self.event_type == "m.room.member" && !self.rejected {
            // Absent OR non-string both fail here (see `WriteSideContent`).
            let m = cracked
                .content
                .membership
                .as_ref()
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
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

        // Serialise auth_events for the column. List is small and
        // already validated (the IDs are typed) — serialisation can't
        // fail in practice; map any error to Internal.
        let auth_events_json = serde_json::to_string(
            &self
                .auth_events
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| Error::Internal(format!("serialising auth_events: {e}")))?;

        let origin_server_ts = i64::try_from(self.origin_server_ts).map_err(|_| {
            Error::InvalidInput(format!(
                "origin_server_ts {} exceeds i64::MAX",
                self.origin_server_ts
            ))
        })?;
        let stream_pos = match explicit_pos {
            Some(pos) => {
                tx.execute(
                    "INSERT INTO events \
                     (stream_pos, event_id, room_id, event_type, state_key, sender, origin_server_ts, json, auth_events_json, rejected, soft_failed) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        pos,
                        self.event_id.as_str(),
                        self.room_id.as_str(),
                        self.event_type,
                        self.state_key,
                        self.sender.as_str(),
                        origin_server_ts,
                        self.raw.get(),
                        auth_events_json,
                        self.rejected,
                        self.soft_failed,
                    ],
                )?;
                pos
            }
            None => {
                tx.execute(
                    "INSERT INTO events \
                     (event_id, room_id, event_type, state_key, sender, origin_server_ts, json, auth_events_json, rejected, soft_failed) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        self.event_id.as_str(),
                        self.room_id.as_str(),
                        self.event_type,
                        self.state_key,
                        self.sender.as_str(),
                        origin_server_ts,
                        self.raw.get(),
                        auth_events_json,
                        self.rejected,
                        self.soft_failed,
                    ],
                )?;
                tx.last_insert_rowid()
            }
        };

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
            for parent in &self.auth_events {
                stmt.execute(params![self.event_id.as_str(), "auth", parent.as_str()])?;
            }
        }

        // Rejected events never enter current_state (their verdict is the
        // row flag; every resolved-state path already excludes them). The
        // explicit gate keeps the simple `persist_event` path consistent
        // with `persist_resolved_event`, and shields the CHECK constraint
        // from the NULL-membership shape a rejected malformed member
        // legitimately carries.
        if update_current_state
            && !self.rejected
            && let Some(sk) = self.state_key.as_deref()
        {
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

    // Drift guard between the two column-list consts. If someone edits
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
