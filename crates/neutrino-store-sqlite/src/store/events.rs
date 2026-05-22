//! `EventStore` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params, params_from_iter};
use neutrino_store::{Direction, Event, EventStore, PaginationToken, StorageError, StreamPos};
use ruma::{EventId, OwnedEventId, OwnedServerName, RoomId, ServerName, UserId};
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

    async fn get_client_txn(
        &self,
        txn_id: &str,
        user_id: &UserId,
    ) -> Result<Option<OwnedEventId>, StorageError> {
        let txn_id = txn_id.to_owned();
        let user_id = user_id.to_owned();

        self.run_read(move |conn| -> Result<Option<OwnedEventId>, Error> {
            let result: Option<String> = conn
                .query_row(
                    "SELECT event_id FROM client_txns WHERE txn_id = ? AND user_id = ?",
                    params![txn_id, user_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;

            match result {
                None => Ok(None),
                Some(s) => OwnedEventId::try_from(s)
                    .map(Some)
                    .map_err(|e| Error::Internal(format!("malformed event_id in DB: {e}"))),
            }
        })
        .await
    }

    async fn record_client_txn(
        &self,
        txn_id: &str,
        user_id: &UserId,
        event_id: &EventId,
    ) -> Result<(), StorageError> {
        let txn_id = txn_id.to_owned();
        let user_id = user_id.to_owned();
        let event_id = event_id.to_owned();

        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "INSERT OR IGNORE INTO client_txns (txn_id, user_id, event_id) \
                 VALUES (?, ?, ?)",
                params![txn_id, user_id.as_str(), event_id.as_str()],
            )?;
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

#[cfg(test)]
mod tests {
    use deadpool_sqlite::rusqlite::params;
    use neutrino_store::{Direction, EventStore, PaginationToken, StorageError, StreamPos};
    use ruma::{EventId, OwnedEventId, event_id, room_id, server_name};
    use serde_json::json;

    use crate::SqliteStore;
    use crate::error::Error;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, BOB_USER_ID, make_event,
        make_event_with_raw_json, message, name_event, setup_room, store,
    };

    /// Open an in-memory store with a single create event in `*ALICE_ROOM_ID` —
    /// many tests share this setup.
    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        setup_room(
            &s,
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            event_id!("$create:example.com"),
        )
        .await;
        s
    }

    // E2: persist_event for unknown room → InvalidInput (FK violation)
    #[tokio::test]
    async fn persist_event_rejects_unknown_room() {
        let s = store().await;
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
        let result = s.persist_event(&msg, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E3: duplicate event_id → InvalidInput (UNIQUE violation)
    #[tokio::test]
    async fn persist_event_rejects_duplicate_event_id() {
        let s = store_with_room().await;
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
        s.persist_event(&msg, &[]).await.unwrap();
        let dup = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "again",
        );
        let result = s.persist_event(&dup, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
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
        let result = s.persist_event(&bad, &[]).await;
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
        let result = s.persist_event(&bad, &[]).await;
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
        let result = s.persist_event(&bad, &[]).await;
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
        let result = s.persist_event(&bad, &[]).await;
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
        let result = s.persist_event(&bad, &[]).await;
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
        s.persist_event(&ok, &[]).await.unwrap();
    }

    // E12: get_client_txn on unrecorded → None
    #[tokio::test]
    async fn get_client_txn_none_for_unrecorded() {
        let s = store().await;
        assert!(
            s.get_client_txn("txn1", *ALICE_USER_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    // E13: record then get round-trip
    #[tokio::test]
    async fn record_then_get_client_txn() {
        let s = store_with_room().await;
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
        s.persist_event(&msg, &[]).await.unwrap();

        let user = *ALICE_USER_ID;
        let evt_id = event_id!("$m1:example.com");
        s.record_client_txn("txn1", user, evt_id).await.unwrap();

        let got = s.get_client_txn("txn1", user).await.unwrap();
        assert_eq!(got.as_deref(), Some(evt_id));
    }

    // E14: second record with same (txn_id, user_id) is a no-op (idempotent)
    #[tokio::test]
    async fn record_client_txn_idempotent() {
        let s = store_with_room().await;
        let m1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "first",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "second",
        );
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let user = *ALICE_USER_ID;
        let id1 = event_id!("$m1:example.com");
        s.record_client_txn("txn1", user, id1).await.unwrap();
        // Second record with same key should be a no-op — original wins.
        s.record_client_txn("txn1", user, event_id!("$m2:example.com"))
            .await
            .unwrap();

        let got = s.get_client_txn("txn1", user).await.unwrap();
        assert_eq!(got.as_deref(), Some(id1));
    }

    // E15: same txn_id, different user_id → independent
    #[tokio::test]
    async fn record_client_txn_isolated_per_user() {
        let s = store_with_room().await;
        // Bob sends a message too, so we have a second user available.
        let m1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "from alice",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *BOB_USER_ID,
            "from bob",
        );
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let alice = *ALICE_USER_ID;
        let bob = *BOB_USER_ID;
        let id1 = event_id!("$m1:example.com");
        let id2 = event_id!("$m2:example.com");
        s.record_client_txn("shared", alice, id1).await.unwrap();
        s.record_client_txn("shared", bob, id2).await.unwrap();

        assert_eq!(
            s.get_client_txn("shared", alice).await.unwrap().as_deref(),
            Some(id1)
        );
        assert_eq!(
            s.get_client_txn("shared", bob).await.unwrap().as_deref(),
            Some(id2)
        );
    }

    // E16: record_client_txn with unknown event_id → InvalidInput (FK)
    #[tokio::test]
    async fn record_client_txn_rejects_unknown_event_id() {
        let s = store_with_room().await;
        let unknown = event_id!("$nope:example.com");
        let result = s.record_client_txn("txn1", *ALICE_USER_ID, unknown).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E31: empty txn_id / user_id strings allowed by schema
    #[tokio::test]
    async fn record_client_txn_empty_strings_ok() {
        let s = store_with_room().await;
        // Schema allows empty strings (TEXT NOT NULL ≠ disallow ""), but
        // user_id parsing in ruma would reject "". So we test empty txn_id
        // only — empty user_id can't be constructed as a UserId.
        let user = *ALICE_USER_ID;
        let evt = event_id!("$create:example.com");
        s.record_client_txn("", user, evt).await.unwrap();
        assert_eq!(
            s.get_client_txn("", user).await.unwrap().as_deref(),
            Some(evt)
        );
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
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
        s.persist_event(&msg, &[]).await.unwrap();

        let known = event_id!("$m1:example.com");
        let unknown = event_id!("$nope:example.com");
        let result = s.get_events(&[known, unknown]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_id.as_str(), "$m1:example.com");
    }

    // E20: StreamPos(0) returns all events
    #[tokio::test]
    async fn events_after_zero_returns_all() {
        let s = store_with_room().await;
        let m1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "a",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "b",
        );
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
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x");
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
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x");
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
        setup_room(
            &s,
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            event_id!("$cA:example.com"),
        )
        .await;
        setup_room(
            &s,
            *BOB_ROOM_ID,
            *ALICE_USER_ID,
            event_id!("$cB:example.com"),
        )
        .await;
        s.persist_event(
            &message(
                event_id!("$mA:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "in A",
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message(
                event_id!("$mB:example.com"),
                *BOB_ROOM_ID,
                *ALICE_USER_ID,
                "in B",
            ),
            &[],
        )
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
        let m1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "a",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "b",
        );
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
        let s = store_with_room().await;
        let m1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "a",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "b",
        );
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let (events, _next) = s
            .room_messages(*ALICE_ROOM_ID, None, Direction::Backward, 10)
            .await
            .unwrap();
        let ids: Vec<&str> = events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            ["$m2:example.com", "$m1:example.com", "$create:example.com"]
        );
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
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x");
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
        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "hi",
            ),
            &[],
        )
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
        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "hi",
            ),
            &[],
        )
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
        setup_room(
            &s,
            *BOB_ROOM_ID,
            *ALICE_USER_ID,
            event_id!("$c2:example.com"),
        )
        .await;
        let m_r1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "in r1",
        );
        let m_r2 = message(
            event_id!("$m2:example.com"),
            *BOB_ROOM_ID,
            *ALICE_USER_ID,
            "in r2",
        );
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
        let evt = name_event(
            event_id!("$n1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "Test Room",
        );
        s.persist_event(&evt, &[]).await.unwrap();
        // Verify it landed in events.
        let id = event_id!("$n1:example.com");
        let got = s.get_events(&[id]).await.unwrap();
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
            event_id!("$m:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({}), // no `membership` key
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
            event_id!("$m:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            None, // missing state_key
            json!({"membership": "join"}),
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
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
        let dest_a = server_name!("a.example.com");
        let dest_b = server_name!("b.example.com");
        s.persist_event(&msg, &[dest_a, dest_b]).await.unwrap();

        let rows: Vec<String> = s
            .run_read(|conn| -> Result<Vec<String>, Error> {
                let mut stmt = conn.prepare(
                    "SELECT destination FROM outbox WHERE event_id = ? \
                     ORDER BY destination",
                )?;
                let it =
                    stmt.query_map(params!["$m1:example.com"], |row| row.get::<_, String>(0))?;
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
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "hi",
        );
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
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x");
            s.persist_event(&m, &[]).await.unwrap();
            ids.push(id);
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
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x");
            s.persist_event(&m, &[]).await.unwrap();
            ids.push(id);
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
        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "hi",
            ),
            &[],
        )
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
        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "hi",
            ),
            &[],
        )
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
        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "hi",
            ),
            &[],
        )
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
        let msg = message(
            event_id!("$h1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "history",
        );
        s.persist_historical_event(&msg).await.unwrap();

        let id = event_id!("$h1:example.com");
        let got = s.get_events(&[id]).await.unwrap();
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
        let msg = message(event_id!("$h:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "history");
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
            event_id!("$m:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({}), // no `membership`
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
        let msg = message(event_id!("$h:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "history");
        s.persist_historical_event(&msg).await.unwrap();
        rx.changed().await.unwrap();
        let after = *rx.borrow();
        assert!(
            after > initial,
            "watch did not advance after persist_historical_event: {initial:?} -> {after:?}"
        );
    }
}
