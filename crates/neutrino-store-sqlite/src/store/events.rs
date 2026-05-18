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
        let pos = pos.0 as i64;
        let limit_i64 = limit as i64;

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
                    out.push((StreamPos(sp as u64), ev?.into_event()));
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
        let limit_i64 = limit as i64;

        self.run_read(
            move |conn| -> Result<(Vec<StoredEvent>, Option<PaginationToken>), Error> {
                // Default `from` per direction. Forward starts at 0
                // (events with stream_pos > 0); backward starts at i64::MAX
                // (events with stream_pos < MAX).
                let from_pos: i64 = from.map(|t| t.0 as i64).unwrap_or(match dir {
                    Direction::Forward => 0,
                    Direction::Backward => i64::MAX,
                });

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
                let rows =
                    stmt.query_map(params![room_id.as_str(), from_pos, limit_i64], |row| {
                        let stream_pos: i64 = row.get("stream_pos")?;
                        Ok((stream_pos, EventRow::try_from(row)))
                    })?;

                let mut events = Vec::new();
                let mut last_pos: Option<i64> = None;
                for r in rows {
                    let (sp, ev) = r?;
                    last_pos = Some(sp);
                    events.push(ev?.into_event());
                }

                // Short page ⇒ no further events in that direction.
                let next = if events.len() < limit {
                    None
                } else {
                    last_pos.map(|p| PaginationToken(p as u64))
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
