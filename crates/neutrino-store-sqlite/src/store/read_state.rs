//! `ReadStateStore` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{Connection, OptionalExtension, params};
use neutrino_store::{
    OwnReceipt, ReadStateDelta, ReadStatePos, ReadStateStore, RoomAccountData, StorageError,
};
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, UserId};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::watch;

use crate::{SqliteStore, error::Error};

/// The next read-state position, one above the highest across both tables.
const NEXT_POS_SQL: &str = "(SELECT MAX(p) + 1 FROM ( \
        SELECT COALESCE(MAX(read_state_pos), 0) AS p FROM receipts \
        UNION ALL \
        SELECT COALESCE(MAX(read_state_pos), 0) FROM room_account_data))";

/// Same shape as `RECORD_DELIVERY_SQL`: the row is sourced from `events`, so an
/// unknown or foreign-room event inserts nothing, and the conflict branch only
/// moves the receipt forward.
fn record_receipt_sql() -> String {
    format!(
        "INSERT INTO receipts \
            (room_id, user_id, receipt_type, thread, event_id, stream_pos, read_state_pos, ts) \
         SELECT ?1, ?2, ?3, ?4, e.event_id, e.stream_pos, {NEXT_POS_SQL}, ?6 \
         FROM events e \
         WHERE e.event_id = ?5 AND e.room_id = ?1 \
         ON CONFLICT(room_id, user_id, receipt_type, thread) DO UPDATE SET \
             event_id       = excluded.event_id, \
             stream_pos     = excluded.stream_pos, \
             read_state_pos = excluded.read_state_pos, \
             ts             = excluded.ts \
         WHERE excluded.stream_pos > receipts.stream_pos"
    )
}

fn set_room_account_data_sql() -> String {
    format!(
        "INSERT INTO room_account_data (user_id, room_id, data_type, content, read_state_pos) \
         VALUES (?1, ?2, ?3, ?4, {NEXT_POS_SQL}) \
         ON CONFLICT(user_id, room_id, data_type) DO UPDATE SET \
             content        = excluded.content, \
             read_state_pos = excluded.read_state_pos"
    )
}

pub(crate) fn max_read_state_pos(conn: &Connection) -> Result<i64, Error> {
    Ok(conn.query_row(&format!("SELECT {NEXT_POS_SQL} - 1"), [], |row| row.get(0))?)
}

fn parse_room_id(room_id: String) -> Result<OwnedRoomId, Error> {
    OwnedRoomId::try_from(room_id)
        .map_err(|e| Error::Internal(format!("malformed room_id in DB: {e}")))
}

#[async_trait]
impl ReadStateStore for SqliteStore {
    async fn record_receipt(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        receipt_type: &str,
        thread: &str,
        event_id: &EventId,
        ts: u64,
    ) -> Result<(), StorageError> {
        let room_id = room_id.to_owned();
        let user_id = user_id.to_owned();
        let receipt_type = receipt_type.to_owned();
        let thread = thread.to_owned();
        let event_id = event_id.to_owned();
        let read_state_watch_tx = self.read_state_watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let changed = conn.execute(
                &record_receipt_sql(),
                params![
                    room_id.as_str(),
                    user_id.as_str(),
                    receipt_type,
                    thread,
                    event_id.as_str(),
                    ts as i64
                ],
            )?;
            // A receipt that didn't move must not wake long-polls.
            if changed > 0 {
                SqliteStore::notify_read_state_watch(
                    &read_state_watch_tx,
                    max_read_state_pos(conn)?,
                );
            }
            Ok(())
        })
        .await
    }

    async fn set_room_account_data(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        data_type: &str,
        content: &RawJsonValue,
    ) -> Result<(), StorageError> {
        let user_id = user_id.to_owned();
        let room_id = room_id.to_owned();
        let data_type = data_type.to_owned();
        let content = content.get().to_owned();
        let read_state_watch_tx = self.read_state_watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                &set_room_account_data_sql(),
                params![user_id.as_str(), room_id.as_str(), data_type, content],
            )?;
            SqliteStore::notify_read_state_watch(&read_state_watch_tx, max_read_state_pos(conn)?);
            Ok(())
        })
        .await
    }

    async fn room_account_data(
        &self,
        user_id: &UserId,
        room_id: &RoomId,
        data_type: &str,
    ) -> Result<Option<Box<RawJsonValue>>, StorageError> {
        let user_id = user_id.to_owned();
        let room_id = room_id.to_owned();
        let data_type = data_type.to_owned();
        self.run_read(move |conn| -> Result<Option<Box<RawJsonValue>>, Error> {
            let content: Option<String> = conn
                .query_row(
                    "SELECT content FROM room_account_data \
                     WHERE user_id = ? AND room_id = ? AND data_type = ?",
                    params![user_id.as_str(), room_id.as_str(), data_type],
                    |row| row.get(0),
                )
                .optional()?;
            content
                .map(RawJsonValue::from_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("malformed account data in DB: {e}")))
        })
        .await
    }

    async fn read_state_since(
        &self,
        user_id: &UserId,
        after: ReadStatePos,
    ) -> Result<ReadStateDelta, StorageError> {
        let user_id = user_id.to_owned();
        let after = after.0 as i64;
        self.run_read(move |conn| -> Result<ReadStateDelta, Error> {
            // One snapshot for both tables: a write landing between two reads
            // could put the returned position past a row the first read missed.
            let tx = conn.transaction()?;
            let mut delta = ReadStateDelta::default();
            let mut highest = 0i64;

            let mut stmt = tx.prepare(
                "SELECT room_id, receipt_type, thread, event_id, ts, read_state_pos \
                 FROM receipts WHERE user_id = ? AND read_state_pos > ?",
            )?;
            let rows = stmt.query_map(params![user_id.as_str(), after], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?;
            for r in rows {
                let (room_id, receipt_type, thread, event_id, ts, pos) = r?;
                highest = highest.max(pos);
                delta.receipts.push(OwnReceipt {
                    room_id: parse_room_id(room_id)?,
                    receipt_type,
                    thread,
                    event_id: OwnedEventId::try_from(event_id)
                        .map_err(|e| Error::Internal(format!("malformed event_id in DB: {e}")))?,
                    ts: ts as u64,
                });
            }

            let mut stmt = tx.prepare(
                "SELECT room_id, data_type, content, read_state_pos \
                 FROM room_account_data WHERE user_id = ? AND read_state_pos > ?",
            )?;
            let rows = stmt.query_map(params![user_id.as_str(), after], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            for r in rows {
                let (room_id, data_type, content, pos) = r?;
                highest = highest.max(pos);
                delta.account_data.push(RoomAccountData {
                    room_id: parse_room_id(room_id)?,
                    data_type,
                    content: RawJsonValue::from_string(content).map_err(|e| {
                        Error::Internal(format!("malformed account data in DB: {e}"))
                    })?,
                });
            }

            delta.pos = (highest > 0).then_some(ReadStatePos(highest as u64));
            Ok(delta)
        })
        .await
    }

    fn subscribe_read_state(&self) -> watch::Receiver<ReadStatePos> {
        self.read_state_watch_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::{EventStore, ReadStatePos, ReadStateStore, RoomStore};
    use ruma::{event_id, user_id};
    use serde_json::value::to_raw_value;

    use crate::SqliteStore;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_USER_ID, create_event, message_with_ts, store,
    };

    async fn store_with_messages() -> (SqliteStore, ruma::OwnedEventId, ruma::OwnedEventId) {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let first = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "first", 1);
        let second = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "second", 2);
        s.persist_event(&first, &[]).await.unwrap();
        s.persist_event(&second, &[]).await.unwrap();
        (s, first.event_id, second.event_id)
    }

    async fn read(s: &SqliteStore, after: u64) -> neutrino_store::ReadStateDelta {
        s.read_state_since(*ALICE_USER_ID, ReadStatePos(after))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn receipt_only_moves_forward() {
        let (s, first, second) = store_with_messages().await;
        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &second, 20)
            .await
            .unwrap();
        let before = read(&s, 0).await;

        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &first, 30)
            .await
            .unwrap();
        let after = read(&s, 0).await;
        assert_eq!(after.receipts.len(), 1);
        assert_eq!(after.receipts[0].event_id, second);
        assert_eq!(after.receipts[0].ts, 20);
        assert_eq!(
            after.pos, before.pos,
            "an older receipt must not re-advance"
        );
    }

    // Receipt type and thread are part of the key: each moves independently.
    #[tokio::test]
    async fn receipts_are_per_type_and_thread() {
        let (s, first, second) = store_with_messages().await;
        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &second, 1)
            .await
            .unwrap();
        s.record_receipt(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.read.private",
            "",
            &first,
            1,
        )
        .await
        .unwrap();
        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "main", &first, 1)
            .await
            .unwrap();
        assert_eq!(read(&s, 0).await.receipts.len(), 3);
    }

    #[tokio::test]
    async fn unknown_event_is_a_no_op() {
        let (s, _, _) = store_with_messages().await;
        s.record_receipt(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.read",
            "",
            event_id!("$nope"),
            1,
        )
        .await
        .unwrap();
        let delta = read(&s, 0).await;
        assert!(delta.receipts.is_empty());
        assert_eq!(delta.pos, None);
    }

    #[tokio::test]
    async fn account_data_round_trips_and_replaces() {
        let (s, _, _) = store_with_messages().await;
        for unread in [true, false] {
            let content = to_raw_value(&serde_json::json!({ "unread": unread })).unwrap();
            s.set_room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.marked_unread", &content)
                .await
                .unwrap();
        }
        let got = s
            .room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.marked_unread")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.get(), r#"{"unread":false}"#);
        let delta = read(&s, 0).await;
        assert_eq!(delta.account_data.len(), 1);
        assert_eq!(delta.account_data[0].content.get(), r#"{"unread":false}"#);
        assert!(
            s.room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.fully_read")
                .await
                .unwrap()
                .is_none()
        );
    }

    // Both tables share one stream: a delta carries only the rows that moved
    // after the cursor, whichever table they live in.
    #[tokio::test]
    async fn read_state_since_returns_only_rows_that_moved() {
        let (s, first, second) = store_with_messages().await;
        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &first, 1)
            .await
            .unwrap();
        let cursor = read(&s, 0).await.pos.unwrap();

        let content = to_raw_value(&serde_json::json!({ "event_id": second })).unwrap();
        s.set_room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.fully_read", &content)
            .await
            .unwrap();
        let delta = read(&s, cursor.0).await;
        assert!(delta.receipts.is_empty(), "the receipt did not move");
        assert_eq!(delta.account_data.len(), 1);
        let cursor = delta.pos.unwrap();

        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &second, 2)
            .await
            .unwrap();
        let delta = read(&s, cursor.0).await;
        assert_eq!(delta.receipts.len(), 1);
        assert_eq!(delta.receipts[0].event_id, second);
        assert!(
            delta.account_data.is_empty(),
            "the account data did not move"
        );
        assert!(read(&s, delta.pos.unwrap().0).await.pos.is_none());
    }

    #[tokio::test]
    async fn read_state_is_per_user() {
        let (s, first, _) = store_with_messages().await;
        s.record_receipt(*ALICE_ROOM_ID, *BOB_USER_ID, "m.read", "", &first, 1)
            .await
            .unwrap();
        let content = to_raw_value(&serde_json::json!({})).unwrap();
        s.set_room_account_data(user_id!("@carol:test"), *ALICE_ROOM_ID, "m.tag", &content)
            .await
            .unwrap();
        let delta = read(&s, 0).await;
        assert!(delta.receipts.is_empty() && delta.account_data.is_empty());
    }

    #[tokio::test]
    async fn watch_advances_on_each_write() {
        let (s, first, second) = store_with_messages().await;
        let mut rx = s.subscribe_read_state();
        assert_eq!(*rx.borrow_and_update(), ReadStatePos(0));

        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &second, 1)
            .await
            .unwrap();
        assert!(rx.has_changed().unwrap(), "a receipt wakes subscribers");
        let after_receipt = *rx.borrow_and_update();

        let content = to_raw_value(&serde_json::json!({})).unwrap();
        s.set_room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.tag", &content)
            .await
            .unwrap();
        assert!(rx.has_changed().unwrap(), "account data wakes subscribers");
        assert!(*rx.borrow_and_update() > after_receipt);

        s.record_receipt(*ALICE_ROOM_ID, *ALICE_USER_ID, "m.read", "", &first, 2)
            .await
            .unwrap();
        assert!(
            !rx.has_changed().unwrap(),
            "a receipt that did not move must not wake long-polls"
        );
    }

    #[tokio::test]
    async fn watch_seeds_from_existing_read_state() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = SqliteStore::open_in_dir(dir.path()).await.unwrap();
        let content = to_raw_value(&serde_json::json!({})).unwrap();
        s.set_room_account_data(*ALICE_USER_ID, *ALICE_ROOM_ID, "m.tag", &content)
            .await
            .unwrap();
        let before = *s.subscribe_read_state().borrow();
        assert!(before > ReadStatePos(0));
        drop(s);

        let reopened = SqliteStore::open_in_dir(dir.path()).await.unwrap();
        assert_eq!(*reopened.subscribe_read_state().borrow(), before);
    }
}
