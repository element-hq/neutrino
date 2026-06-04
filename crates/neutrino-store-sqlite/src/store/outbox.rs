//! `FederationOutbox` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{params, params_from_iter};
use neutrino_store::{Event, FederationOutbox, StorageError};
use ruma::{EventId, OwnedServerName, ServerName};

use crate::{
    SqliteStore,
    error::Error,
    row::{EVENT_COLUMNS_PREFIXED, EventRow},
};

#[async_trait]
impl FederationOutbox for SqliteStore {
    async fn pending_destinations(&self) -> Result<Vec<OwnedServerName>, StorageError> {
        self.run_read(move |conn| -> Result<Vec<OwnedServerName>, Error> {
            let mut stmt = conn.prepare("SELECT DISTINCT destination FROM outbox")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

            let mut out = Vec::new();
            for r in rows {
                let s = r?;
                let server = OwnedServerName::try_from(s)
                    .map_err(|e| Error::Internal(format!("malformed destination in DB: {e}")))?;
                out.push(server);
            }
            Ok(out)
        })
        .await
    }

    async fn pending_pdus(
        &self,
        destination: &ServerName,
        limit: usize,
    ) -> Result<Vec<Event>, StorageError> {
        let destination = destination.to_owned();
        // SQLite `LIMIT` takes an i64; a `usize` past `i64::MAX` saturates to
        // "effectively unbounded", which is the intended meaning.
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);

        self.run_read(move |conn| -> Result<Vec<Event>, Error> {
            let query = format!(
                "SELECT {EVENT_COLUMNS_PREFIXED} \
                 FROM outbox o \
                 JOIN events e ON o.event_id = e.event_id \
                 WHERE o.destination = ? \
                 ORDER BY o.outbox_id ASC \
                 LIMIT ?"
            );
            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(params![destination.as_str(), limit], |row| {
                Ok(EventRow::try_from(row))
            })?;

            let mut out = Vec::new();
            for r in rows {
                out.push(r??.into_event());
            }
            Ok(out)
        })
        .await
    }

    async fn remove_pdus(
        &self,
        destination: &ServerName,
        event_ids: &[&EventId],
    ) -> Result<(), StorageError> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let destination = destination.to_owned();
        let event_ids: Vec<String> = event_ids.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_write(move |conn| -> Result<(), Error> {
            let placeholders = vec!["?"; event_ids.len()].join(",");
            let query = format!(
                "DELETE FROM outbox WHERE destination = ? AND event_id IN ({placeholders})"
            );
            let mut binds: Vec<&str> = Vec::with_capacity(event_ids.len() + 1);
            binds.push(destination.as_str());
            for id in &event_ids {
                binds.push(id.as_str());
            }
            conn.execute(&query, params_from_iter(binds.iter()))?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::{EventStore, FederationOutbox, RoomStore};
    use ruma::{event_id, server_name};

    use crate::SqliteStore;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, create_event, message, message_with_ts, store,
    };

    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        s
    }

    // O1
    #[tokio::test]
    async fn pending_destinations_empty_initially() {
        let s = store().await;
        assert!(s.pending_destinations().await.unwrap().is_empty());
    }

    // O2
    #[tokio::test]
    async fn pending_pdus_empty_for_unknown_destination() {
        let s = store().await;
        assert!(
            s.pending_pdus(server_name!("nope.example.com"), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }

    // O3: same destination on two events → returned once.
    #[tokio::test]
    async fn pending_destinations_distinct() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        let ev_a = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 0);
        s.persist_event(&ev_a, &[dest]).await.unwrap();
        let ev_b = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 1);
        s.persist_event(&ev_b, &[dest]).await.unwrap();

        let dests = s.pending_destinations().await.unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].as_str(), "matrix.org");
    }

    // O4: events returned in insertion (outbox_id) order.
    #[tokio::test]
    async fn pending_pdus_in_outbox_order() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        // Distinct origin_server_ts → distinct event_ids despite identical content.
        let mut expected = Vec::new();
        for i in 0..3 {
            let ev = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i);
            expected.push(ev.event_id.clone());
            s.persist_event(&ev, &[dest]).await.unwrap();
        }

        let pdus = s.pending_pdus(dest, usize::MAX).await.unwrap();
        let ids: Vec<&str> = pdus.iter().map(|e| e.event_id.as_str()).collect();
        let expected_strs: Vec<&str> = expected.iter().map(|e| e.as_str()).collect();
        assert_eq!(ids, expected_strs);
    }

    // O9: `limit` caps the batch at the oldest N, in order.
    #[tokio::test]
    async fn pending_pdus_respects_limit() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        let mut expected = Vec::new();
        for i in 0..5 {
            let ev = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i);
            expected.push(ev.event_id.clone());
            s.persist_event(&ev, &[dest]).await.unwrap();
        }

        // Only the two oldest, in insertion order.
        let pdus = s.pending_pdus(dest, 2).await.unwrap();
        let ids: Vec<&str> = pdus.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, vec![expected[0].as_str(), expected[1].as_str()]);

        // limit ≥ count returns all.
        assert_eq!(s.pending_pdus(dest, 100).await.unwrap().len(), 5);
    }

    // O5: remove the named event_ids only.
    #[tokio::test]
    async fn remove_pdus_removes_specified() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        let ev_a = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 0);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[dest]).await.unwrap();
        let ev_b = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 1);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[dest]).await.unwrap();

        s.remove_pdus(dest, &[&id_a]).await.unwrap();

        let pdus = s.pending_pdus(dest, usize::MAX).await.unwrap();
        assert_eq!(pdus.len(), 1);
        assert_eq!(pdus[0].event_id.as_str(), id_b.as_str());
    }

    // O6: second remove of already-removed IDs is a silent no-op.
    #[tokio::test]
    async fn remove_pdus_idempotent() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        let ev_a = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "a");
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[dest]).await.unwrap();

        s.remove_pdus(dest, &[&id_a]).await.unwrap();
        // Second call with the same IDs — no error, no change.
        s.remove_pdus(dest, &[&id_a]).await.unwrap();

        assert!(s.pending_pdus(dest, usize::MAX).await.unwrap().is_empty());
    }

    // O7: empty event_ids slice short-circuits — no SQL run.
    #[tokio::test]
    async fn remove_pdus_empty_list_short_circuits() {
        let s = store().await;
        s.remove_pdus(server_name!("matrix.org"), &[])
            .await
            .unwrap();
    }

    // O8: unknown event_ids in the slice → silent no-op.
    #[tokio::test]
    async fn remove_pdus_unknown_event_ids_no_error() {
        let s = store().await;
        s.remove_pdus(
            server_name!("matrix.org"),
            &[event_id!("$nope:example.com")],
        )
        .await
        .unwrap();
    }
}
