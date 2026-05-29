//! `EventStore` impl on `SqliteStore`.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, Transaction, params, params_from_iter};
use neutrino_store::{Direction, Event, EventStore, PaginationToken, StorageError, StreamPos};
use ruma::{EventId, OwnedEventId, OwnedServerName, RoomId, ServerName};
use tokio::sync::watch;

use crate::{
    SqliteStore,
    error::Error,
    row::{EVENT_COLUMNS, EventRow},
};

#[async_trait]
impl EventStore for SqliteStore {
    async fn persist_event(
        &self,
        event: &Event,
        destinations: &[&ServerName],
    ) -> Result<(), StorageError> {
        // event_id <-> raw consistency is asserted inside `EventRow::from`
        // (debug builds only). No need to repeat the check here.
        let event = EventRow::from(event).to_owned();
        let destinations: Vec<OwnedServerName> =
            destinations.iter().map(|s| (*s).to_owned()).collect();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;

            let stream_pos = event.write_into_tx(&tx)?;

            // Outbox: one row per destination, idempotent via the
            // UNIQUE(destination, event_id) constraint. `event.event_id`
            // resolves through `EventRow: Deref<Target = Event>`.
            {
                let mut stmt = tx.prepare(
                    "INSERT OR IGNORE INTO outbox (destination, event_id) VALUES (?, ?)",
                )?;
                for dest in &destinations {
                    stmt.execute(params![dest.as_str(), event.event_id.as_str()])?;
                }
            }

            tx.commit()?;

            // Notify *after* commit — never strand a committed event
            // without a wake-up signal.
            SqliteStore::notify_watch(&watch_tx, stream_pos);

            Ok(())
        })
        .await
    }

    async fn persist_historical_event(&self, event: &Event) -> Result<(), StorageError> {
        // event_id <-> raw consistency asserted inside `EventRow::from`.
        let event = EventRow::from(event).to_owned();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;

            // `write_into_tx_historical` skips the `current_state`
            // upsert — see `row::EventRow::write_into_tx_historical`
            // for the rationale. No outbox writes either: backfill is
            // strictly the read direction, no federation traffic
            // originates from a historical insert.
            let stream_pos = event.write_into_tx_historical(&tx)?;

            tx.commit()?;

            // Watch still advances so subscribers waiting on stream
            // changes can discover the new history (e.g. a paginating
            // client refetching `room_messages`).
            SqliteStore::notify_watch(&watch_tx, stream_pos);

            Ok(())
        })
        .await
    }

    async fn persist_resolved_event(
        &self,
        event: &Event,
        timeline_fes: &BTreeSet<OwnedEventId>,
        state_fes: &BTreeSet<OwnedEventId>,
        current_state_delta: &BTreeMap<(String, String), Option<OwnedEventId>>,
    ) -> Result<(), StorageError> {
        // event_id <-> raw consistency asserted inside `EventRow::from`.
        let event = EventRow::from(event).to_owned();
        let room_id = event.room_id.clone();
        let timeline_json = fe_json(timeline_fes)?;
        let state_json = fe_json(state_fes)?;
        let delta = current_state_delta.clone();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;

            // Event row + DAG edges only — current_state is NOT touched here.
            // State resolution may change current-state keys other than (or
            // instead of) this event's own, so we drive current_state from
            // the explicit `current_state_delta` below rather than via
            // `write_into_tx`'s implicit single-key upsert.
            let stream_pos = event.write_into_tx_no_current_state(&tx)?;

            // Apply the current-state delta. `Some(id)` upserts the key to
            // point at `id`; `None` removes the key. The pointer's metadata
            // (membership for `m.room.member`) is derived from the
            // already-persisted `events` row via `INSERT ... SELECT`, so the
            // delta only needs to carry the event id — and 0 affected rows on
            // an upsert means the referenced event isn't persisted, which is
            // a caller-contract violation we surface rather than swallow.
            apply_current_state_delta(&tx, room_id.as_str(), &delta)?;

            // Replace the room's two head-sets. The `events.room_id` FK that
            // the event write just satisfied guarantees the room exists, so
            // this UPDATE always matches its row.
            tx.execute(
                "UPDATE rooms \
                 SET forward_extremities = ?, state_dag_forward_extremities = ? \
                 WHERE room_id = ?",
                params![timeline_json, state_json, room_id.as_str()],
            )?;

            tx.commit()?;

            // Notify after commit, like `persist_event`.
            SqliteStore::notify_watch(&watch_tx, stream_pos);

            Ok(())
        })
        .await
    }

    async fn get_events(&self, ids: &[&EventId]) -> Result<Vec<Event>, StorageError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = ids.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<Event>, Error> {
            // SQLite caps host parameters per statement (default 999 on
            // older builds, 32766 on 3.32+; the bundled rusqlite tracks
            // this). Chunk the `IN (?, …)` query so callers can pass any
            // size of `ids` without hitting a `too many SQL variables`
            // error from the driver. Trait post-condition doesn't
            // promise ordering, so concatenating per-chunk results is
            // fine.
            const MAX_PARAMS: usize = 900;
            let mut out = Vec::with_capacity(ids.len());
            for chunk in ids.chunks(MAX_PARAMS) {
                let placeholders = vec!["?"; chunk.len()].join(",");
                let query = format!(
                    "SELECT {EVENT_COLUMNS} FROM events WHERE event_id IN ({placeholders})"
                );
                let mut stmt = conn.prepare(&query)?;
                // Wrap the `TryFrom<&Row>` so its `Result<_, Error>` becomes
                // a `rusqlite::Result<Result<_, Error>>` that `query_map`
                // accepts; we peel both layers in the loop below.
                let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| {
                    Ok(EventRow::try_from(row))
                })?;
                for row in rows {
                    out.push(row??.into_event());
                }
            }
            Ok(out)
        })
        .await
    }

    async fn events_after(
        &self,
        pos: StreamPos,
        limit: usize,
    ) -> Result<Vec<(StreamPos, Event)>, StorageError> {
        let pos = i64::try_from(pos.0)
            .map_err(|_| Error::InvalidInput(format!("StreamPos {} exceeds i64::MAX", pos.0)))?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| Error::InvalidInput(format!("limit {limit} exceeds i64::MAX")))?;

        self.run_read(move |conn| -> Result<Vec<(StreamPos, Event)>, Error> {
            let query = format!(
                "SELECT stream_pos, {EVENT_COLUMNS} FROM events \
                 WHERE stream_pos > ? ORDER BY stream_pos ASC LIMIT ?"
            );
            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(params![pos, limit_i64], |row| {
                let stream_pos: i64 = row.get("stream_pos")?;
                Ok((stream_pos, EventRow::try_from(row)))
            })?;

            let mut out = Vec::new();
            for r in rows {
                let (sp, ev) = r?;
                let sp = u64::try_from(sp).map_err(|_| {
                    Error::Internal(format!("Invalid negative stream_pos {sp} in events table"))
                })?;
                out.push((StreamPos(sp), ev?.into_event()));
            }
            Ok(out)
        })
        .await
    }

    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError> {
        let room_id = room_id.to_owned();
        // Fetch one extra row so we can distinguish "exactly `limit`
        // events remain" (post-condition: return `None` token) from
        // "more than `limit` events remain" (return a `Some` token).
        // `events.len() < limit` alone can't tell those cases apart;
        // see trait post-condition "token is None when no further
        // events exist".
        let fetch_limit_i64 = i64::try_from(limit.saturating_add(1))
            .map_err(|_| Error::InvalidInput(format!("limit {limit} exceeds i64::MAX")))?;
        // Default `from` per direction. Forward starts at 0
        // (events with stream_pos > 0); backward starts at i64::MAX
        // (events with stream_pos < MAX).
        let from_pos: i64 = match from {
            Some(t) => i64::try_from(t.0).map_err(|_| {
                Error::InvalidInput(format!("PaginationToken {} exceeds i64::MAX", t.0))
            })?,
            None => match dir {
                Direction::Forward => 0,
                Direction::Backward => i64::MAX,
            },
        };

        self.run_read(
            move |conn| -> Result<(Vec<Event>, Option<PaginationToken>), Error> {
                // Pre-condition (trait: "the room must exist"). Surface a
                // violation rather than silently returning empty — caller
                // wants a fault, not a successful no-op.
                let exists: Option<i64> = conn
                    .query_row(
                        "SELECT 1 FROM rooms WHERE room_id = ?",
                        params![room_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if exists.is_none() {
                    return Err(Error::InvalidInput(format!(
                        "unknown room: {}",
                        room_id.as_str()
                    )));
                }

                // `limit == 0` is degenerate: there's no row we can hang
                // a "more exists past this position" token on, so the
                // overflow detection below can't satisfy the trait
                // post-condition. Short-circuit explicitly rather than
                // returning a possibly-misleading `None` token. Room
                // pre-condition has already been enforced above.
                if limit == 0 {
                    return Ok((Vec::new(), None));
                }

                let (cmp, order) = match dir {
                    Direction::Forward => (">", "ASC"),
                    Direction::Backward => ("<", "DESC"),
                };

                let query = format!(
                    "SELECT stream_pos, {EVENT_COLUMNS} FROM events \
                     WHERE room_id = ? AND stream_pos {cmp} ? \
                     ORDER BY stream_pos {order} LIMIT ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(
                    params![room_id.as_str(), from_pos, fetch_limit_i64],
                    |row| {
                        let stream_pos: i64 = row.get("stream_pos")?;
                        Ok((stream_pos, EventRow::try_from(row)))
                    },
                )?;

                let mut events = Vec::with_capacity(limit);
                let mut last_in_page: Option<i64> = None;
                let mut overflow_seen = false;
                for r in rows {
                    let (sp, ev) = r?;
                    if events.len() == limit {
                        // Sentinel (limit+1)-th row materialised — there
                        // is more data past `last_in_page`. Don't include
                        // it in the page itself.
                        overflow_seen = true;
                        break;
                    }
                    last_in_page = Some(sp);
                    events.push(ev?.into_event());
                }

                // Only emit a token when the sentinel proved more data
                // exists. A "full but final" page (events.len() == limit
                // with no overflow row) terminates the stream cleanly.
                let next = if overflow_seen {
                    match last_in_page {
                        Some(p) => Some(PaginationToken(u64::try_from(p).map_err(|_| {
                            Error::Internal(format!(
                                "negative stream_pos encountered while building pagination token: {}",
                                p
                            ))
                        })?)),
                        None => None,
                    }
                } else {
                    None
                };

                Ok((events, next))
            },
        )
        .await
    }

    fn subscribe(&self) -> watch::Receiver<StreamPos> {
        SqliteStore::subscribe(self)
    }
}

/// Serialise a forward-extremity set to the JSON-array form stored in the
/// `rooms` columns. Mirror of `parse_event_id_set` in `rooms.rs`.
/// Serialisation of a list of typed ids can't fail in practice; any error
/// maps to `Internal`.
fn fe_json(fes: &BTreeSet<OwnedEventId>) -> Result<String, Error> {
    serde_json::to_string(&fes.iter().map(|id| id.as_str()).collect::<Vec<_>>())
        .map_err(|e| Error::Internal(format!("serialising forward_extremities: {e}")))
}

/// Apply a resolved current-state delta within `tx`. Each `Some(id)` upserts
/// the `(room_id, event_type, state_key)` row to point at `id`; each `None`
/// removes the row. The upsert derives `event_id`'s `event_type` /
/// `state_key` / `membership` straight from the already-persisted `events`
/// row, so the delta need only carry the id. An upsert that affects zero rows
/// means the referenced event isn't persisted — a violation of the method's
/// pre-condition — surfaced as `Internal` rather than silently dropped.
fn apply_current_state_delta(
    tx: &Transaction<'_>,
    room_id: &str,
    delta: &BTreeMap<(String, String), Option<OwnedEventId>>,
) -> Result<(), Error> {
    for ((event_type, state_key), change) in delta {
        match change {
            Some(event_id) => {
                let affected = tx.execute(
                    "INSERT INTO current_state \
                         (room_id, event_type, state_key, event_id, membership) \
                     SELECT room_id, event_type, state_key, event_id, \
                            json_extract(json, '$.content.membership') \
                     FROM events WHERE event_id = ? \
                     ON CONFLICT(room_id, event_type, state_key) DO UPDATE SET \
                         event_id = excluded.event_id, \
                         membership = excluded.membership",
                    params![event_id.as_str()],
                )?;
                if affected == 0 {
                    return Err(Error::Internal(format!(
                        "current_state delta references unpersisted event {event_id}"
                    )));
                }
            }
            None => {
                tx.execute(
                    "DELETE FROM current_state \
                     WHERE room_id = ? AND event_type = ? AND state_key = ?",
                    params![room_id, event_type, state_key],
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use deadpool_sqlite::rusqlite::params;
    use neutrino_common::Event;
    use neutrino_store::{
        Direction, EventStore, PaginationToken, RoomStore, StateStore, StorageError, StreamPos,
    };
    use ruma::{EventId, OwnedEventId, event_id, room_id, server_name};
    use serde_json::json;

    use crate::SqliteStore;
    use crate::error::Error;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, make_event, make_event_with_raw_json, message,
        message_with_ts, name_event, setup_room, store,
    };

    /// Open an in-memory store with a single create event in `*ALICE_ROOM_ID`
    /// and return the store along with the create event (for tests that need
    /// its computed event_id).
    async fn store_with_room_and_create() -> (SqliteStore, Event) {
        let s = store().await;
        let ce = setup_room(&s, *ALICE_ROOM_ID, *ALICE_USER_ID).await;
        (s, ce)
    }

    /// Convenience wrapper for tests that don't need the create event id.
    async fn store_with_room() -> SqliteStore {
        store_with_room_and_create().await.0
    }

    /// A single-entry "set" delta for `(event_type, state_key)` → `id`.
    fn set_delta(
        event_type: &str,
        state_key: &str,
        id: &OwnedEventId,
    ) -> BTreeMap<(String, String), Option<OwnedEventId>> {
        BTreeMap::from([(
            (event_type.to_string(), state_key.to_string()),
            Some(id.clone()),
        )])
    }

    // EZ1: persist_resolved_event writes the event and applies a one-key
    // "set" delta to current_state, then replaces both forward-extremity
    // columns. The load half (`forward_extremities`) round-trips the
    // head-sets.
    #[tokio::test]
    async fn persist_resolved_event_state_event_updates_state_and_fe_columns() {
        let (s, create) = store_with_room_and_create().await;
        let name = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "Room");
        let name_id = name.event_id.clone();
        let timeline: BTreeSet<OwnedEventId> = [name_id.clone()].into_iter().collect();
        let state: BTreeSet<OwnedEventId> = [create.event_id.clone(), name_id.clone()]
            .into_iter()
            .collect();
        let delta = set_delta("m.room.name", "", &name_id);

        s.persist_resolved_event(&name, &timeline, &state, &delta)
            .await
            .unwrap();

        assert_eq!(s.get_events(&[&name_id]).await.unwrap().len(), 1);
        let cs = s
            .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap();
        assert_eq!(cs.map(|e| e.event_id), Some(name_id));
        let (tl, st) = s
            .forward_extremities(*ALICE_ROOM_ID)
            .await
            .unwrap()
            .expect("room exists");
        assert_eq!(tl, timeline);
        assert_eq!(st, state);
    }

    // EZ2: persist_resolved_event for a non-state event (empty delta) moves
    // the timeline column but writes no current_state row.
    #[tokio::test]
    async fn persist_resolved_event_non_state_event_writes_no_current_state() {
        let (s, create) = store_with_room_and_create().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let msg_id = msg.event_id.clone();
        let timeline: BTreeSet<OwnedEventId> = [msg_id.clone()].into_iter().collect();
        let state: BTreeSet<OwnedEventId> = [create.event_id.clone()].into_iter().collect();

        s.persist_resolved_event(&msg, &timeline, &state, &BTreeMap::new())
            .await
            .unwrap();

        let (tl, st) = s
            .forward_extremities(*ALICE_ROOM_ID)
            .await
            .unwrap()
            .expect("room exists");
        assert_eq!(tl, timeline);
        assert_eq!(st, state);

        let current = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
        assert!(!current.values().any(|e| e.event_id == msg_id));
    }

    // EZ3: a `None` delta entry removes the current_state row. Set a name,
    // then persist a later event whose delta removes the name key, and
    // confirm the key is gone. (The carrier event is incidental — the test
    // exercises the delete branch of the delta applier.)
    #[tokio::test]
    async fn persist_resolved_event_delta_none_removes_current_state_key() {
        let (s, create) = store_with_room_and_create().await;
        let name = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "Room");
        let name_id = name.event_id.clone();
        let fe1: BTreeSet<OwnedEventId> = [name_id.clone()].into_iter().collect();
        s.persist_resolved_event(&name, &fe1, &fe1, &set_delta("m.room.name", "", &name_id))
            .await
            .unwrap();
        assert!(
            s.current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
                .await
                .unwrap()
                .is_some()
        );

        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let msg_id = msg.event_id.clone();
        let fe2: BTreeSet<OwnedEventId> = [msg_id].into_iter().collect();
        let state_fe: BTreeSet<OwnedEventId> = [create.event_id.clone()].into_iter().collect();
        let remove: BTreeMap<(String, String), Option<OwnedEventId>> =
            BTreeMap::from([(("m.room.name".to_string(), String::new()), None)]);
        s.persist_resolved_event(&msg, &fe2, &state_fe, &remove)
            .await
            .unwrap();

        assert!(
            s.current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
                .await
                .unwrap()
                .is_none()
        );
    }

    // EZ4: a "set" delta referencing an event that isn't persisted is a
    // pre-condition violation → Internal (0 rows affected by the upsert).
    #[tokio::test]
    async fn persist_resolved_event_delta_unpersisted_target_errors() {
        let (s, _create) = store_with_room_and_create().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let msg_id = msg.event_id.clone();
        let fe: BTreeSet<OwnedEventId> = [msg_id].into_iter().collect();
        let ghost = event_id!("$ghost:example.org").to_owned();
        let delta = set_delta("m.room.name", "", &ghost);

        let result = s.persist_resolved_event(&msg, &fe, &fe, &delta).await;
        assert!(
            matches!(result, Err(StorageError::Internal(_))),
            "{result:?}"
        );
    }

    // EZ5: the soft_failed verdict round-trips through the event row.
    #[tokio::test]
    async fn persist_resolved_event_round_trips_soft_failed_flag() {
        let s = store_with_room().await;
        let mut msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        msg.soft_failed = true;
        let msg_id = msg.event_id.clone();
        let fe: BTreeSet<OwnedEventId> = [msg_id.clone()].into_iter().collect();

        s.persist_resolved_event(&msg, &fe, &fe, &BTreeMap::new())
            .await
            .unwrap();

        let got = s.get_events(&[&msg_id]).await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0].soft_failed);
    }

    // E2: persist_event for unknown room → InvalidInput (FK violation)
    #[tokio::test]
    async fn persist_event_rejects_unknown_room() {
        let s = store().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let result = s.persist_event(&msg, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E3: duplicate event_id → InvalidInput (UNIQUE violation)
    #[tokio::test]
    async fn persist_event_rejects_duplicate_event_id() {
        let s = store_with_room().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        s.persist_event(&msg, &[]).await.unwrap();
        let dup = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "again");
        let result = s.persist_event(&dup, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // Helper: write a hand-rolled `Event` via the raw `write_into_tx`
    // path, bypassing the `debug_assert_event_id_matches_raw` check in
    // `EventRow::from`. These tests intentionally use malformed/mismatched
    // raw bytes whose `compute_event_id` wouldn't agree with the column
    // event_id; the debug_assert would mask the storage-layer validation
    // we want to pin.
    async fn write_event_directly(
        s: &SqliteStore,
        ev: &neutrino_common::Event,
    ) -> Result<(), neutrino_store::StorageError> {
        let row = crate::row::EventRow::unchecked(ev).to_owned();
        s.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;
            row.write_into_tx(&tx)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    // E4: malformed JSON shape (top-level is a string)
    #[tokio::test]
    async fn persist_event_rejects_invalid_json() {
        let s = store_with_room().await;
        let bad = make_event_with_raw_json(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            "\"not an object\"",
        );
        let result = write_event_directly(&s, &bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E4a-d: `Event` columns must agree with the raw JSON copy on
    // every axis where both are present. Defence-in-depth at the storage
    // write boundary — a buggy caller (or a future code path) that
    // builds a `Event` with column values disagreeing with the
    // JSON would otherwise silently desync the two surfaces (column
    // reads vs. wire re-emission).
    #[tokio::test]
    async fn persist_event_rejects_event_type_column_json_mismatch() {
        let s = store_with_room().await;
        // Column says `m.room.message`; JSON says `m.room.member`.
        let bad = make_event_with_raw_json(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            r#"{
                "type": "m.room.member",
                "room_id": "!r1:example.com",
                "sender": "@alice:example.com",
                "state_key": null,
                "content": {},
                "origin_server_ts": 0,
                "prev_events": [],
                "prev_state_events": []
            }"#,
        );
        let result = write_event_directly(&s, &bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn persist_event_rejects_room_id_column_json_mismatch() {
        let s = store_with_room().await;
        let bad = make_event_with_raw_json(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            r#"{
                "type": "m.room.message",
                "room_id": "!r2:example.com",
                "sender": "@alice:example.com",
                "state_key": null,
                "content": {},
                "origin_server_ts": 0,
                "prev_events": [],
                "prev_state_events": []
            }"#,
        );
        let result = write_event_directly(&s, &bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn persist_event_rejects_sender_column_json_mismatch() {
        let s = store_with_room().await;
        let bad = make_event_with_raw_json(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            r#"{
                "type": "m.room.message",
                "room_id": "!r1:example.com",
                "sender": "@bob:example.com",
                "state_key": null,
                "content": {},
                "origin_server_ts": 0,
                "prev_events": [],
                "prev_state_events": []
            }"#,
        );
        let result = write_event_directly(&s, &bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn persist_event_rejects_state_key_column_json_mismatch() {
        let s = store_with_room().await;
        // Column says state_key = Some(""); JSON says state_key = "wrong".
        let bad = make_event_with_raw_json(
            event_id!("$n1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.name",
            Some(""),
            r#"{
                "type": "m.room.name",
                "room_id": "!r1:example.com",
                "sender": "@alice:example.com",
                "state_key": "wrong",
                "content": {"name": "X"},
                "origin_server_ts": 0,
                "prev_events": [],
                "prev_state_events": []
            }"#,
        );
        let result = write_event_directly(&s, &bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E4e: JSON omitting any of the cross-checked fields is allowed — the
    // column is the canonical value, and a caller is free to elide
    // redundant fields from the JSON copy. (Today's test helpers always
    // emit them, but the trait surface doesn't require it.)
    #[tokio::test]
    async fn persist_event_accepts_json_missing_cross_check_fields() {
        let s = store_with_room().await;
        let ok = make_event_with_raw_json(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            r#"{
                "content": {"body": "hi", "msgtype": "m.text"},
                "origin_server_ts": 0,
                "prev_events": [],
                "prev_state_events": []
            }"#,
        );
        write_event_directly(&s, &ok).await.unwrap();
    }

    // E17: empty ids → empty result, no SQL run
    #[tokio::test]
    async fn get_events_empty_ids_returns_empty() {
        let s = store().await;
        assert!(s.get_events(&[]).await.unwrap().is_empty());
    }

    // E18: unknown ids → empty result
    #[tokio::test]
    async fn get_events_unknown_returns_empty() {
        let s = store().await;
        let id = event_id!("$nope:example.com");
        assert!(s.get_events(&[id]).await.unwrap().is_empty());
    }

    // E19: mix of known + unknown returns only known
    #[tokio::test]
    async fn get_events_partial_match_returns_subset() {
        let s = store_with_room().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let known_id = msg.event_id.clone();
        s.persist_event(&msg, &[]).await.unwrap();

        let unknown = event_id!("$nope:example.com");
        let result = s.get_events(&[&known_id, unknown]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_id.as_str(), known_id.as_str());
    }

    // E20: StreamPos(0) returns all events
    #[tokio::test]
    async fn events_after_zero_returns_all() {
        let s = store_with_room().await;
        // Distinct ts so the two messages get distinct event_ids (v12
        // redaction strips body for m.room.message).
        let m1 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 1);
        let m2 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 2);
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let result = s.events_after(StreamPos(0), 100).await.unwrap();
        assert_eq!(result.len(), 3); // create + 2 messages
    }

    // E21: limit honored
    #[tokio::test]
    async fn events_after_respects_limit() {
        let s = store_with_room().await;
        for i in 0..5 {
            let m = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64 + 1);
            s.persist_event(&m, &[]).await.unwrap();
        }
        let result = s.events_after(StreamPos(0), 3).await.unwrap();
        assert_eq!(result.len(), 3);
    }

    // E22: strictly ascending stream_pos
    #[tokio::test]
    async fn events_after_ascending() {
        let s = store_with_room().await;
        for i in 0..3 {
            let m = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64 + 1);
            s.persist_event(&m, &[]).await.unwrap();
        }
        let result = s.events_after(StreamPos(0), 100).await.unwrap();
        for w in result.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
    }

    // E22b: `events_after` is intentionally global — no `room_id` filter.
    // Sliding sync's per-connection state walks the cross-room stream and
    // post-filters in the handler, so the trait surface returns events
    // from every room interleaved in commit order. Pin the behaviour with
    // a multi-room test so a future change that adds room-scoping (or
    // drops it) is flagged.
    #[tokio::test]
    async fn events_after_returns_events_across_rooms() {
        let s = store().await;
        setup_room(&s, *ALICE_ROOM_ID, *ALICE_USER_ID).await;
        setup_room(&s, *BOB_ROOM_ID, *ALICE_USER_ID).await;
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "in A"), &[])
            .await
            .unwrap();
        s.persist_event(&message(*BOB_ROOM_ID, *ALICE_USER_ID, "in B"), &[])
            .await
            .unwrap();

        let result = s.events_after(StreamPos(0), 100).await.unwrap();
        let rooms: std::collections::HashSet<&str> =
            result.iter().map(|(_, e)| e.room_id.as_str()).collect();
        assert!(
            rooms.contains(ALICE_ROOM_ID.as_str()),
            "events_after must surface room A events"
        );
        assert!(
            rooms.contains(BOB_ROOM_ID.as_str()),
            "events_after must surface room B events"
        );
        assert_eq!(
            result.len(),
            4,
            "expected 2 creates + 2 messages across both rooms"
        );
    }

    // E33: huge starting pos → empty
    #[tokio::test]
    async fn events_after_high_pos_returns_empty() {
        let s = store_with_room().await;
        let result = s
            .events_after(StreamPos(i64::MAX as u64), 100)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    // E23: Forward, from=None → ascending from earliest
    #[tokio::test]
    async fn room_messages_forward_default_from() {
        let s = store_with_room().await;
        let m1 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 1);
        let m2 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 2);
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let (events, _next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 10)
            .await
            .unwrap();
        let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
        assert_eq!(types, ["m.room.create", "m.room.message", "m.room.message"]);
    }

    // E24: Backward, from=None → descending from latest
    #[tokio::test]
    async fn room_messages_backward_default_from() {
        let (s, ce) = store_with_room_and_create().await;
        let m1 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 1);
        let id_m1 = m1.event_id.clone();
        let m2 = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 2);
        let id_m2 = m2.event_id.clone();
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let (events, _next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Backward, 10)
            .await
            .unwrap();
        let ids: Vec<&str> = events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, [id_m2.as_str(), id_m1.as_str(), ce.event_id.as_str()]);
    }

    // E25: short page → no next token
    #[tokio::test]
    async fn room_messages_short_page_returns_no_token() {
        let s = store_with_room().await;
        // One create event total. Asking for 10 → page is 1 item → no token.
        let (events, next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(next.is_none());
    }

    // E26: pagination round-trip — page, then continue
    #[tokio::test]
    async fn room_messages_pagination_roundtrip() {
        let s = store_with_room().await;
        for i in 0..4 {
            let m = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64 + 1);
            s.persist_event(&m, &[]).await.unwrap();
        }
        let room = *ALICE_ROOM_ID;

        // First page of 2 events (forward).
        let (page1, next1) = s
            .room_messages(room, None, Direction::Forward, 2)
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        let token = next1.unwrap();

        // Second page using the token.
        let (page2, next2) = s
            .room_messages(room, Some(token.clone()), Direction::Forward, 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);

        // No event_id appears in both pages.
        let p1_ids: Vec<&str> = page1.iter().map(|e| e.event_id.as_str()).collect();
        for ev in &page2 {
            assert!(!p1_ids.contains(&ev.event_id.as_str()));
        }

        // Third page should be empty (5 total events, 2+2 consumed → 1 left,
        // but we asked for 2 — third call returns the last one with no token).
        let (page3, next3) = s
            .room_messages(room, next2, Direction::Forward, 2)
            .await
            .unwrap();
        assert_eq!(page3.len(), 1);
        assert!(next3.is_none());
    }

    // E33: page size matches remaining exactly → no next token (trait
    // post-condition: "token is None when no further events exist").
    // Distinguishes "full page + more" from "full page + nothing past it".
    #[tokio::test]
    async fn room_messages_exact_remaining_returns_no_token() {
        let s = store_with_room().await;
        // store_with_room has 1 create event; add 1 more so total = 2.
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();
        let (events, next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 2)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(next.is_none());
    }

    // E34: limit == 0 short-circuits — empty page, no token, even
    // when more events exist. Degenerate caller input but defined.
    #[tokio::test]
    async fn room_messages_zero_limit_returns_empty() {
        let s = store_with_room().await;
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();
        let (events, next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 0)
            .await
            .unwrap();
        assert!(events.is_empty());
        assert!(next.is_none());
    }

    // E27: events from a different room not returned
    #[tokio::test]
    async fn room_messages_filters_by_room_id() {
        let s = store_with_room().await;
        // Set up a second room via raw SQL (bypassing RoomStore).
        setup_room(&s, *BOB_ROOM_ID, *ALICE_USER_ID).await;
        let m_r1 = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "in r1");
        let m_r2 = message(*BOB_ROOM_ID, *ALICE_USER_ID, "in r2");
        s.persist_event(&m_r1, &[]).await.unwrap();
        s.persist_event(&m_r2, &[]).await.unwrap();

        let (events, _) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 10)
            .await
            .unwrap();
        for ev in &events {
            assert_eq!(ev.room_id.as_str(), ALICE_ROOM_ID.as_str());
        }
    }

    // E32: room_messages for unknown room → InvalidInput (precondition violation)
    #[tokio::test]
    async fn room_messages_unknown_room_errors() {
        let s = store().await;
        let unknown = room_id!("!nope:example.com");
        let result = s.room_messages(unknown, None, Direction::Forward, 10).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E28: empty store → subscribe initial value is StreamPos(0)
    #[tokio::test]
    async fn subscribe_initial_zero_for_empty_store() {
        let s = store().await;
        let receiver = s.subscribe();
        assert_eq!(*receiver.borrow(), StreamPos(0));
    }

    // E30: empty string state_key is valid (e.g. m.room.name, m.room.create)
    #[tokio::test]
    async fn persist_event_with_empty_state_key_ok() {
        let s = store_with_room().await;
        let evt = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "Test Room");
        let id = evt.event_id.clone();
        s.persist_event(&evt, &[]).await.unwrap();
        // Verify it landed in events.
        let got = s.get_events(&[&id]).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].state_key.as_deref(), Some(""));
    }

    // E35: m.room.member event without `content.membership` → InvalidInput.
    // Writing it would leave `current_state.membership` NULL, which makes
    // the row invisible to `joined_members` / `joined_rooms` filtering.
    // Reject at the write boundary (`EventRow::write_into_tx`).
    #[tokio::test]
    async fn persist_event_rejects_member_without_membership() {
        let s = store_with_room().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({}), // no `membership` key
            0,
            &[],
            &[],
        );
        let result = s.persist_event(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E36: m.room.member event without state_key → InvalidInput. State
    // key carries the user_id for member rows; missing it would silently
    // skip the `current_state` upsert and leave the membership invisible.
    #[tokio::test]
    async fn persist_event_rejects_member_without_state_key() {
        let s = store_with_room().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            None, // missing state_key
            json!({"membership": "join"}),
            0,
            &[],
            &[],
        );
        let result = s.persist_event(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E37: `persist_event` writes one `outbox` row per destination
    // (idempotent via `UNIQUE(destination, event_id)`). `FederationOutbox`
    // isn't implemented on this branch yet, so this test peeks at the
    // table directly via `run_read`; swap to the trait once it lands.
    #[tokio::test]
    async fn persist_event_writes_outbox_rows_per_destination() {
        let s = store_with_room().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        let msg_id = msg.event_id.as_str().to_owned();
        let dest_a = server_name!("a.example.com");
        let dest_b = server_name!("b.example.com");
        s.persist_event(&msg, &[dest_a, dest_b]).await.unwrap();

        let rows: Vec<String> = s
            .run_read(move |conn| -> Result<Vec<String>, Error> {
                let mut stmt = conn.prepare(
                    "SELECT destination FROM outbox WHERE event_id = ? \
                     ORDER BY destination",
                )?;
                let it = stmt.query_map(params![msg_id.as_str()], |row| row.get::<_, String>(0))?;
                let mut out = Vec::new();
                for r in it {
                    out.push(r?);
                }
                Ok(out)
            })
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec!["a.example.com".to_string(), "b.example.com".to_string()]
        );
    }

    // E38: `subscribe()` receiver observes a value advance after
    // `persist_event` commits — covers the post-condition that
    // notification fires from inside the `spawn_blocking` closure after
    // `tx.commit()?`.
    #[tokio::test]
    async fn subscribe_advances_after_persist_event() {
        let s = store_with_room().await;
        let mut rx = s.subscribe();
        let initial = *rx.borrow();
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
        s.persist_event(&msg, &[]).await.unwrap();
        rx.changed().await.unwrap();
        let after = *rx.borrow();
        assert!(
            after > initial,
            "watch did not advance after persist_event: {initial:?} -> {after:?}"
        );
    }

    // E39: `get_events` chunks `IN (?, …)` at MAX_PARAMS = 900 so it can
    // accept any `ids.len()` without hitting SQLite's host-parameter cap.
    // Persist > MAX_PARAMS events, request all of them, and verify every
    // event makes it back out. Single-chunk paths are covered by E18/E19;
    // this is the explicit multi-chunk witness.
    #[tokio::test]
    async fn get_events_chunks_above_max_params() {
        let s = store_with_room().await;
        let n: usize = 950;
        let mut ids: Vec<OwnedEventId> = Vec::with_capacity(n);
        for i in 0..n {
            let m = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64 + 1);
            ids.push(m.event_id.clone());
            s.persist_event(&m, &[]).await.unwrap();
        }
        let refs: Vec<&EventId> = ids.iter().map(|i| i.as_ref()).collect();
        let result = s.get_events(&refs).await.unwrap();
        assert_eq!(result.len(), n);
    }

    // E40: a duplicate event ID that straddles two chunks currently
    // surfaces twice in the result — SQL `IN (…)` dedupes within a chunk
    // but the impl concatenates per-chunk results without further dedup.
    // Trait post-condition is silent on duplicates ("result length may be
    // < ids.len()"), so this pins current behavior rather than asserting
    // a tighter contract. If the trait gains a "result is a set" promise,
    // this test should flip to `count == 1`.
    #[tokio::test]
    async fn get_events_duplicate_id_across_chunks_returns_twice() {
        let s = store_with_room().await;
        // Need at least MAX_PARAMS (900) IDs in the request to force a
        // second chunk. Persist 900 events; ID at index 0 will appear in
        // both chunks of the request.
        let n: usize = 900;
        let mut ids: Vec<OwnedEventId> = Vec::with_capacity(n);
        for i in 0..n {
            let m = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64 + 1);
            ids.push(m.event_id.clone());
            s.persist_event(&m, &[]).await.unwrap();
        }
        // Construct a request with the first ID duplicated into chunk #2:
        // [id0, id1, …, id899, id0]. Length 901 → chunks of 900 + 1.
        let dup = ids[0].clone();
        let mut req: Vec<&EventId> = ids.iter().map(|i| i.as_ref()).collect();
        req.push(dup.as_ref());

        let result = s.get_events(&req).await.unwrap();
        // 900 distinct events + the cross-chunk duplicate of id0 = 901
        // rows. If a future change adds cross-chunk dedup, this number
        // drops to 900.
        assert_eq!(result.len(), 901);
        let dup_str = dup.as_str();
        let dup_count = result
            .iter()
            .filter(|e| e.event_id.as_str() == dup_str)
            .count();
        assert_eq!(dup_count, 2);
    }

    // E41: `events_after` honors `limit == 0` — empty page even when
    // events exist past `pos`. The bounded SELECT receives `LIMIT 0` and
    // returns no rows; the surrounding code does no post-filtering.
    #[tokio::test]
    async fn events_after_zero_limit_returns_empty() {
        let s = store_with_room().await;
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();
        let result = s.events_after(StreamPos(0), 0).await.unwrap();
        assert!(result.is_empty());
    }

    // E42: `room_messages` Backward with `from = Some(PaginationToken(0))`
    // is the natural start-of-stream terminator: cursor 0 with `<` cmp
    // matches nothing. Empty page, no next token. Mirrors the boundary
    // a paginating client hits after consuming the earliest event.
    #[tokio::test]
    async fn room_messages_backward_from_zero_token_returns_empty() {
        let s = store_with_room().await;
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();
        let (events, next) = s
            .room_messages(
                *ALICE_ROOM_ID,
                Some(PaginationToken(0)),
                Direction::Backward,
                10,
            )
            .await
            .unwrap();
        assert!(events.is_empty());
        assert!(next.is_none());
    }

    // E43: `room_messages` Backward, page size matches remaining exactly
    // → no next token. Mirrors E33 (Forward) for the backward direction;
    // the `limit + 1` sentinel fetch must not be misclassified as
    // "more data exists" when the sentinel row simply doesn't exist.
    #[tokio::test]
    async fn room_messages_backward_exact_remaining_returns_no_token() {
        let s = store_with_room().await;
        // store_with_room has 1 create event; add 1 more so total = 2.
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();
        let (events, next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Backward, 2)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(next.is_none());
    }

    // E44-E49: `persist_historical_event` — backfill-class persistence
    // that writes events + edges but deliberately does *not* update
    // `current_state` or the outbox. Resolves the unconditional-UPSERT
    // ambiguity flagged in the SQLite-store review by giving the
    // backfill handler a separate code path; `persist_event` keeps its
    // forward-extension semantics.

    // E44: a historical event is visible via `get_events` and
    // `events_after` — same observability as a forward-extension write.
    #[tokio::test]
    async fn persist_historical_event_visible_via_reads() {
        let s = store_with_room().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "history");
        let id = msg.event_id.clone();
        s.persist_historical_event(&msg).await.unwrap();

        let got = s.get_events(&[&id]).await.unwrap();
        assert_eq!(got.len(), 1);
        let stream = s.events_after(StreamPos(0), 100).await.unwrap();
        assert!(
            stream
                .iter()
                .any(|(_, e)| e.event_id.as_str() == id.as_str()),
            "historical event must appear in the stream"
        );
    }

    // (`does_not_update_current_state` and `does_not_regress_current_state`
    // are cross-trait scenarios — they live in `tests/storage.rs` as
    // X12 / X13 to keep this in-crate suite scoped to the EventStore
    // trait surface.)

    // E47: `persist_historical_event` does not write any outbox rows —
    // backfill is purely the read direction, no federation traffic
    // originates from a historical insert.
    #[tokio::test]
    async fn persist_historical_event_writes_no_outbox_rows() {
        let s = store_with_room().await;
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "history");
        s.persist_historical_event(&msg).await.unwrap();

        let count: i64 = s
            .run_read(|conn| -> Result<i64, Error> {
                Ok(conn.query_row("SELECT COUNT(*) FROM outbox", [], |r| r.get(0))?)
            })
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "persist_historical_event must not write outbox rows"
        );
    }

    // E48: validation still fires for malformed events — a member
    // event missing `content.membership` is rejected even on the
    // historical path. The cross-checks, FK, etc. still apply: the
    // events table has to be queryable, so well-formedness is a
    // hard requirement regardless of which path wrote the row.
    #[tokio::test]
    async fn persist_historical_event_rejects_malformed_member() {
        let s = store_with_room().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({}), // no `membership`
            0,
            &[],
            &[],
        );
        let result = s.persist_historical_event(&bad).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E49: `persist_historical_event` advances the `subscribe()` watch
    // so subscribers wake and discover the new history. Same wake-up
    // contract as `persist_event` — only the current_state and outbox
    // sides differ.
    #[tokio::test]
    async fn persist_historical_event_advances_watch() {
        let s = store_with_room().await;
        let mut rx = s.subscribe();
        let initial = *rx.borrow();
        let msg = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "history");
        s.persist_historical_event(&msg).await.unwrap();
        rx.changed().await.unwrap();
        let after = *rx.borrow();
        assert!(
            after > initial,
            "watch did not advance after persist_historical_event: {initial:?} -> {after:?}"
        );
    }
}
