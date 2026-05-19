//! `EventStore` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params, params_from_iter};
use neutrino_store::{
    Direction, EventStore, PaginationToken, StorageError, StoredEvent, StreamPos,
};
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
        event: &StoredEvent,
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
            // resolves through `EventRow: Deref<Target = StoredEvent>`.
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

    async fn get_events(&self, ids: &[&EventId]) -> Result<Vec<StoredEvent>, StorageError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = ids.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<StoredEvent>, Error> {
            let placeholders = vec!["?"; ids.len()].join(",");
            let query =
                format!("SELECT {EVENT_COLUMNS} FROM events WHERE event_id IN ({placeholders})");
            let mut stmt = conn.prepare(&query)?;
            // Wrap the `TryFrom<&Row>` so its `Result<_, Error>` becomes a
            // `rusqlite::Result<Result<_, Error>>` that `query_map` accepts;
            // we peel both layers in the loop below.
            let rows = stmt.query_map(params_from_iter(ids.iter()), |row| {
                Ok(EventRow::try_from(row))
            })?;

            let mut out = Vec::with_capacity(ids.len());
            for row in rows {
                out.push(row??.into_event());
            }
            Ok(out)
        })
        .await
    }

    async fn events_after(
        &self,
        pos: StreamPos,
        limit: usize,
    ) -> Result<Vec<(StreamPos, StoredEvent)>, StorageError> {
        let pos = i64::try_from(pos.0)
            .map_err(|_| Error::InvalidInput(format!("StreamPos {} exceeds i64::MAX", pos.0)))?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| Error::InvalidInput(format!("limit {limit} exceeds i64::MAX")))?;

        self.run_read(
            move |conn| -> Result<Vec<(StreamPos, StoredEvent)>, Error> {
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
            },
        )
        .await
    }

    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<StoredEvent>, Option<PaginationToken>), StorageError> {
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
            move |conn| -> Result<(Vec<StoredEvent>, Option<PaginationToken>), Error> {
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
    use lazy_static::lazy_static;
    use neutrino_store::{Direction, EventStore, StorageError, StreamPos};
    use ruma::{OwnedEventId, RoomId, UserId, event_id, room_id, user_id};

    use crate::SqliteStore;
    use crate::tests::{make_event_with_raw_json, message, name_event, setup_room, store};

    lazy_static! {
        static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
        static ref BOB_ROOM_ID: &'static RoomId = room_id!("!r2:example.com");
        static ref ALICE_ID: &'static UserId = user_id!("@alice:example.com");
        static ref BOB_ID: &'static UserId = user_id!("@bob:example.com");
    }

    /// Open an in-memory store with a single create event in `*ALICE_ROOM_ID` —
    /// many tests share this setup.
    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        setup_room(
            &s,
            *ALICE_ROOM_ID,
            *ALICE_ID,
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
            *ALICE_ID,
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
            *ALICE_ID,
            "hi",
        );
        s.persist_event(&msg, &[]).await.unwrap();
        let dup = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_ID,
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
            *ALICE_ID,
            "m.room.message",
            None,
            "\"not an object\"",
        );
        let result = s.persist_event(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E12: get_client_txn on unrecorded → None
    #[tokio::test]
    async fn get_client_txn_none_for_unrecorded() {
        let s = store().await;
        assert!(s.get_client_txn("txn1", *ALICE_ID).await.unwrap().is_none());
    }

    // E13: record then get round-trip
    #[tokio::test]
    async fn record_then_get_client_txn() {
        let s = store_with_room().await;
        let msg = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_ID,
            "hi",
        );
        s.persist_event(&msg, &[]).await.unwrap();

        let user = *ALICE_ID;
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
            *ALICE_ID,
            "first",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_ID,
            "second",
        );
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let user = *ALICE_ID;
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
            *ALICE_ID,
            "from alice",
        );
        let m2 = message(
            event_id!("$m2:example.com"),
            *ALICE_ROOM_ID,
            *BOB_ID,
            "from bob",
        );
        s.persist_event(&m1, &[]).await.unwrap();
        s.persist_event(&m2, &[]).await.unwrap();

        let alice = *ALICE_ID;
        let bob = *BOB_ID;
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
        let result = s.record_client_txn("txn1", *ALICE_ID, unknown).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // E31: empty txn_id / user_id strings allowed by schema
    #[tokio::test]
    async fn record_client_txn_empty_strings_ok() {
        let s = store_with_room().await;
        // Schema allows empty strings (TEXT NOT NULL ≠ disallow ""), but
        // user_id parsing in ruma would reject "". So we test empty txn_id
        // only — empty user_id can't be constructed as a UserId.
        let user = *ALICE_ID;
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
            *ALICE_ID,
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
        let m1 = message(event_id!("$m1:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "a");
        let m2 = message(event_id!("$m2:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "b");
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
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_ID, "x");
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
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_ID, "x");
            s.persist_event(&m, &[]).await.unwrap();
        }
        let result = s.events_after(StreamPos(0), 100).await.unwrap();
        for w in result.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
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
        let m1 = message(event_id!("$m1:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "a");
        let m2 = message(event_id!("$m2:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "b");
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
        let m1 = message(event_id!("$m1:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "a");
        let m2 = message(event_id!("$m2:example.com"), *ALICE_ROOM_ID, *ALICE_ID, "b");
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
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_ID, "x");
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
                *ALICE_ID,
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
                *ALICE_ID,
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
        setup_room(&s, *BOB_ROOM_ID, *ALICE_ID, event_id!("$c2:example.com")).await;
        let m_r1 = message(
            event_id!("$m1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_ID,
            "in r1",
        );
        let m_r2 = message(
            event_id!("$m2:example.com"),
            *BOB_ROOM_ID,
            *ALICE_ID,
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
            *ALICE_ID,
            "Test Room",
        );
        s.persist_event(&evt, &[]).await.unwrap();
        // Verify it landed in events.
        let id = event_id!("$n1:example.com");
        let got = s.get_events(&[id]).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].state_key.as_deref(), Some(""));
    }
}
