//! `DeliveryStore` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::params;
use neutrino_store::{Delivery, DeliveryPos, DeliveryStore, StorageError};
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, ServerName};
use tokio::sync::watch;

use crate::{SqliteStore, error::Error};

/// Move the `(room_id, destination)` mark to `event_id`, or leave it alone.
///
/// The whole decision is in the statement: the row is sourced from `events`
/// (so an unknown/foreign-room event inserts nothing, and `stream_pos` comes
/// from the event itself rather than the caller), and the conflict branch is
/// guarded on `excluded.stream_pos > deliveries.stream_pos` (so a replayed or
/// out-of-order acknowledgement can't walk the mark backwards). `delivery_pos`
/// is `MAX + 1` over the table, evaluated before the insert — an advance always
/// lands above every mark recorded so far, which is what makes "everything
/// since pos N" answerable.
const RECORD_DELIVERY_SQL: &str = "\
    INSERT INTO deliveries (room_id, destination, event_id, stream_pos, delivery_pos, ts) \
    SELECT ?1, ?2, e.event_id, e.stream_pos, \
           (SELECT COALESCE(MAX(delivery_pos), 0) + 1 FROM deliveries), ?4 \
    FROM events e \
    WHERE e.event_id = ?3 AND e.room_id = ?1 \
    ON CONFLICT(room_id, destination) DO UPDATE SET \
        event_id     = excluded.event_id, \
        stream_pos   = excluded.stream_pos, \
        delivery_pos = excluded.delivery_pos, \
        ts           = excluded.ts \
    WHERE excluded.stream_pos > deliveries.stream_pos";

#[async_trait]
impl DeliveryStore for SqliteStore {
    async fn record_delivery(
        &self,
        destination: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        ts: u64,
    ) -> Result<(), StorageError> {
        let destination = destination.to_owned();
        let room_id = room_id.to_owned();
        let event_id = event_id.to_owned();
        let delivery_watch_tx = self.delivery_watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let changed = conn.execute(
                RECORD_DELIVERY_SQL,
                params![
                    room_id.as_str(),
                    destination.as_str(),
                    event_id.as_str(),
                    ts as i64
                ],
            )?;
            // No advance (older event, or an event we don't hold) leaves the
            // watch alone — a wake-up with nothing behind it would spin every
            // long-polling client for no new data.
            if changed == 0 {
                return Ok(());
            }
            let pos: i64 = conn.query_row(
                "SELECT COALESCE(MAX(delivery_pos), 0) FROM deliveries",
                [],
                |row| row.get(0),
            )?;
            // After the write, never before: a mark that is visible to readers
            // must never be missing its wake-up.
            SqliteStore::notify_delivery_watch(&delivery_watch_tx, pos);
            Ok(())
        })
        .await
    }

    async fn deliveries_since(&self, after: DeliveryPos) -> Result<Vec<Delivery>, StorageError> {
        let after = after.0 as i64;
        self.run_read(move |conn| -> Result<Vec<Delivery>, Error> {
            let mut stmt = conn.prepare(
                "SELECT room_id, destination, event_id, delivery_pos, ts \
                 FROM deliveries WHERE delivery_pos > ? ORDER BY delivery_pos ASC",
            )?;
            let rows = stmt.query_map(params![after], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })?;

            let mut out = Vec::new();
            for r in rows {
                let (room_id, destination, event_id, pos, ts) = r?;
                out.push(Delivery {
                    room_id: OwnedRoomId::try_from(room_id)
                        .map_err(|e| Error::Internal(format!("malformed room_id in DB: {e}")))?,
                    destination: OwnedServerName::try_from(destination).map_err(|e| {
                        Error::Internal(format!("malformed destination in DB: {e}"))
                    })?,
                    event_id: OwnedEventId::try_from(event_id)
                        .map_err(|e| Error::Internal(format!("malformed event_id in DB: {e}")))?,
                    ts: ts as u64,
                    pos: DeliveryPos(pos as u64),
                });
            }
            Ok(out)
        })
        .await
    }

    fn subscribe_deliveries(&self) -> watch::Receiver<DeliveryPos> {
        self.delivery_watch_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::{DeliveryPos, DeliveryStore, EventStore, RoomStore};
    use ruma::{event_id, server_name};

    use crate::SqliteStore;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, BOB_USER_ID, create_event, message_with_ts,
        store,
    };

    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        s
    }

    #[tokio::test]
    async fn no_marks_initially() {
        let s = store().await;
        assert!(
            s.deliveries_since(DeliveryPos(0)).await.unwrap().is_empty(),
            "a store nobody has federated from holds no delivery marks"
        );
    }

    // The mark round-trips: what `record_delivery` wrote is what
    // `deliveries_since(0)` reads back, ts included.
    #[tokio::test]
    async fn records_and_reads_back_a_mark() {
        let s = store_with_room().await;
        let msg = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi", 1);
        s.persist_event(&msg, &[]).await.unwrap();

        s.record_delivery(
            server_name!("peer.example.com"),
            *ALICE_ROOM_ID,
            &msg.event_id,
            1_700_000_000_000,
        )
        .await
        .unwrap();

        let marks = s.deliveries_since(DeliveryPos(0)).await.unwrap();
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].room_id, *ALICE_ROOM_ID);
        assert_eq!(marks[0].destination, server_name!("peer.example.com"));
        assert_eq!(marks[0].event_id, msg.event_id);
        assert_eq!(marks[0].ts, 1_700_000_000_000);
        assert!(marks[0].pos > DeliveryPos(0), "an advance takes a position");
    }

    // The high-water guard: a newer event moves the mark, an older one is
    // ignored. Redelivery after a reordering (or a replayed 2xx) must never
    // walk a peer's mark backwards — the receipt would un-tick.
    #[tokio::test]
    async fn mark_only_moves_forward() {
        let s = store_with_room().await;
        let peer = server_name!("peer.example.com");
        let first = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "first", 1);
        let second = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "second", 2);
        s.persist_event(&first, &[]).await.unwrap();
        s.persist_event(&second, &[]).await.unwrap();

        s.record_delivery(peer, *ALICE_ROOM_ID, &second.event_id, 20)
            .await
            .unwrap();
        let after_second = s.deliveries_since(DeliveryPos(0)).await.unwrap();
        assert_eq!(after_second.len(), 1);

        // The older event is a no-op: same event, same pos, same ts.
        s.record_delivery(peer, *ALICE_ROOM_ID, &first.event_id, 30)
            .await
            .unwrap();
        let marks = s.deliveries_since(DeliveryPos(0)).await.unwrap();
        assert_eq!(marks.len(), 1, "still one row per (room, destination)");
        assert_eq!(
            marks[0].event_id, second.event_id,
            "an older delivery must not walk the mark back"
        );
        assert_eq!(marks[0].pos, after_second[0].pos, "and must not re-advance");
        assert_eq!(marks[0].ts, 20, "nor restamp it");
    }

    // Two peers acknowledging the same room are independent marks; a peer
    // acknowledging two rooms likewise. One row per pair, never a merge.
    #[tokio::test]
    async fn marks_are_per_room_and_destination() {
        let s = store_with_room().await;
        s.create_room(&create_event(*BOB_ROOM_ID, *BOB_USER_ID), &[])
            .await
            .unwrap();
        let a_msg = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 1);
        let b_msg = message_with_ts(*BOB_ROOM_ID, *BOB_USER_ID, "b", 1);
        s.persist_event(&a_msg, &[]).await.unwrap();
        s.persist_event(&b_msg, &[]).await.unwrap();

        for peer in [
            server_name!("one.example.com"),
            server_name!("two.example.com"),
        ] {
            s.record_delivery(peer, *ALICE_ROOM_ID, &a_msg.event_id, 1)
                .await
                .unwrap();
        }
        s.record_delivery(
            server_name!("one.example.com"),
            *BOB_ROOM_ID,
            &b_msg.event_id,
            1,
        )
        .await
        .unwrap();

        let marks = s.deliveries_since(DeliveryPos(0)).await.unwrap();
        assert_eq!(marks.len(), 3, "two peers × alice's room, plus bob's room");
    }

    // The delta read sliding sync depends on: only marks that moved after the
    // caller's cursor come back, in ascending position order.
    #[tokio::test]
    async fn deliveries_since_returns_only_later_marks() {
        let s = store_with_room().await;
        let first = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "first", 1);
        let second = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "second", 2);
        s.persist_event(&first, &[]).await.unwrap();
        s.persist_event(&second, &[]).await.unwrap();

        s.record_delivery(
            server_name!("one.example.com"),
            *ALICE_ROOM_ID,
            &first.event_id,
            1,
        )
        .await
        .unwrap();
        let cursor = s.deliveries_since(DeliveryPos(0)).await.unwrap()[0].pos;

        s.record_delivery(
            server_name!("two.example.com"),
            *ALICE_ROOM_ID,
            &second.event_id,
            2,
        )
        .await
        .unwrap();

        let delta = s.deliveries_since(cursor).await.unwrap();
        assert_eq!(delta.len(), 1, "only the mark recorded after the cursor");
        assert_eq!(delta[0].destination, server_name!("two.example.com"));
        assert!(delta[0].pos > cursor);
        assert!(
            s.deliveries_since(delta[0].pos).await.unwrap().is_empty(),
            "reading from the newest position yields nothing"
        );
    }

    // An event we don't hold can't be marked — the statement sources its row
    // from `events`, so there is nothing to insert. Not an error: the caller is
    // an outbox drain that must not stall on a receipt.
    #[tokio::test]
    async fn unknown_event_is_a_no_op() {
        let s = store_with_room().await;
        s.record_delivery(
            server_name!("peer.example.com"),
            *ALICE_ROOM_ID,
            event_id!("$nope"),
            1,
        )
        .await
        .unwrap();
        assert!(s.deliveries_since(DeliveryPos(0)).await.unwrap().is_empty());
    }

    // The long-poll wake-up: an advance fires the watch, a no-op advance does
    // not (a client woken with nothing new spins for no reason).
    #[tokio::test]
    async fn watch_fires_only_on_a_real_advance() {
        let s = store_with_room().await;
        let peer = server_name!("peer.example.com");
        let first = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "first", 1);
        let second = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "second", 2);
        s.persist_event(&first, &[]).await.unwrap();
        s.persist_event(&second, &[]).await.unwrap();

        let mut rx = s.subscribe_deliveries();
        assert_eq!(*rx.borrow_and_update(), DeliveryPos(0));

        s.record_delivery(peer, *ALICE_ROOM_ID, &second.event_id, 1)
            .await
            .unwrap();
        assert!(rx.has_changed().unwrap(), "an advance wakes subscribers");
        let advanced = *rx.borrow_and_update();
        assert!(advanced > DeliveryPos(0));

        // Older event → guard rejects → no write, so no wake-up.
        s.record_delivery(peer, *ALICE_ROOM_ID, &first.event_id, 2)
            .await
            .unwrap();
        assert!(
            !rx.has_changed().unwrap(),
            "a rejected mark must not wake long-polls"
        );
    }

    // The watch is seeded from the table at open, so a mark recorded before a
    // restart doesn't re-fire as "new" to the first subscriber after it.
    #[tokio::test]
    async fn watch_seeds_from_existing_marks() {
        let dir = tempfile::TempDir::new().unwrap();
        let peer = server_name!("peer.example.com");
        let msg = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi", 1);

        let s = SqliteStore::open_in_dir(dir.path()).await.unwrap();
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        s.persist_event(&msg, &[]).await.unwrap();
        s.record_delivery(peer, *ALICE_ROOM_ID, &msg.event_id, 1)
            .await
            .unwrap();
        let before = *s.subscribe_deliveries().borrow();
        drop(s);

        let reopened = SqliteStore::open_in_dir(dir.path()).await.unwrap();
        assert_eq!(
            *reopened.subscribe_deliveries().borrow(),
            before,
            "the delivery watch resumes at the persisted high-water mark"
        );
    }
}
