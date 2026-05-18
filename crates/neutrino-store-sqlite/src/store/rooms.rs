//! `RoomStore` impl on `SqliteStore`.

use std::str::FromStr;

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_store::{RoomStore, StorageError, StoredEvent};
use ruma::{RoomId, RoomVersionId};
use serde_json::Value;

use crate::{SqliteStore, error::Error, row::EventRow};

#[async_trait]
impl RoomStore for SqliteStore {
    async fn create_room(
        &self,
        create_event: &StoredEvent,
        initial_events: &[StoredEvent],
    ) -> Result<(), StorageError> {
        // Pull `content.room_version` out of the create event JSON before
        // crossing the closure boundary. Caller-supplied JSON, so a
        // missing/malformed field is `InvalidInput`.
        let parsed: Value = serde_json::from_str(create_event.json.get())
            .map_err(|e| Error::InvalidInput(format!("create_event json: {e}")))?;
        let room_version = parsed
            .pointer("/content/room_version")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::InvalidInput("create_event missing content.room_version".into())
            })?;

        // Wrap borrowed events into `'static` `EventRow`s for the closure.
        let create_event = EventRow::from(create_event).to_owned();
        let initial_events: Vec<EventRow<'static>> = initial_events
            .iter()
            .map(|e| EventRow::from(e).to_owned())
            .collect();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;

            // 1. Register the room row first — the events table's
            //    `room_id REFERENCES rooms(room_id)` FK would reject the
            //    create event otherwise. `create_event.room_id` resolves
            //    through `EventRow: Deref<Target = StoredEvent>`.
            tx.execute(
                "INSERT INTO rooms (room_id, room_version) VALUES (?, ?)",
                params![create_event.room_id.as_str(), room_version],
            )?;

            // 2. Write the create event itself.
            let mut last_pos = create_event.write_into_tx(&tx)?;

            // 3. Write every initial event in order.
            for ev in initial_events {
                last_pos = ev.write_into_tx(&tx)?;
            }

            tx.commit()?;

            // 4. One watch advance for the whole batch — clients waking
            //    up will fetch all the new events via a single
            //    `events_after`.
            SqliteStore::notify_watch(&watch_tx, last_pos);

            Ok(())
        })
        .await
    }

    async fn get_room_version(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<RoomVersionId>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(move |conn| -> Result<Option<RoomVersionId>, Error> {
            let row: Option<String> = conn
                .query_row(
                    "SELECT room_version FROM rooms WHERE room_id = ?",
                    params![room_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;

            row.map(|s| {
                RoomVersionId::from_str(&s)
                    .map_err(|e| Error::Internal(format!("malformed room_version in DB: {e}")))
            })
            .transpose()
        })
        .await
    }

    async fn room_count(&self) -> Result<u64, StorageError> {
        self.run_read(move |conn| -> Result<u64, Error> {
            let n: i64 = conn.query_row("SELECT COUNT(*) FROM rooms", [], |r| r.get(0))?;
            Ok(n as u64)
        })
        .await
    }
}
