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

    async fn pending_pdus(&self, destination: &ServerName) -> Result<Vec<Event>, StorageError> {
        let destination = destination.to_owned();

        self.run_read(move |conn| -> Result<Vec<Event>, Error> {
            let query = format!(
                "SELECT {EVENT_COLUMNS_PREFIXED} \
                 FROM outbox o \
                 JOIN events e ON o.event_id = e.event_id \
                 WHERE o.destination = ? \
                 ORDER BY o.outbox_id ASC"
            );
            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(params![destination.as_str()], |row| {
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
    use ruma::{OwnedEventId, event_id, server_name};

    use crate::SqliteStore;
    use crate::tests::{ALICE_ROOM_ID, ALICE_USER_ID, create_event, message, store};

    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        s.create_room(
            &create_event(
                event_id!("$create:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
            ),
            &[],
        )
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
            s.pending_pdus(server_name!("nope.example.com"))
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

        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "a",
            ),
            &[dest],
        )
        .await
        .unwrap();
        s.persist_event(
            &message(
                event_id!("$m2:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "b",
            ),
            &[dest],
        )
        .await
        .unwrap();

        let dests = s.pending_destinations().await.unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].as_str(), "matrix.org");
    }

    // O4: events returned in insertion (outbox_id) order.
    #[tokio::test]
    async fn pending_pdus_in_outbox_order() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        for i in 0..3 {
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            s.persist_event(&message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "x"), &[dest])
                .await
                .unwrap();
        }

        let pdus = s.pending_pdus(dest).await.unwrap();
        let ids: Vec<&str> = pdus.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            ["$m0:example.com", "$m1:example.com", "$m2:example.com"]
        );
    }

    // O5: remove the named event_ids only.
    #[tokio::test]
    async fn remove_pdus_removes_specified() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "a",
            ),
            &[dest],
        )
        .await
        .unwrap();
        s.persist_event(
            &message(
                event_id!("$m2:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "b",
            ),
            &[dest],
        )
        .await
        .unwrap();

        s.remove_pdus(dest, &[event_id!("$m1:example.com")])
            .await
            .unwrap();

        let pdus = s.pending_pdus(dest).await.unwrap();
        assert_eq!(pdus.len(), 1);
        assert_eq!(pdus[0].event_id.as_str(), "$m2:example.com");
    }

    // O6: second remove of already-removed IDs is a silent no-op.
    #[tokio::test]
    async fn remove_pdus_idempotent() {
        let s = store_with_room().await;
        let dest = server_name!("matrix.org");

        s.persist_event(
            &message(
                event_id!("$m1:example.com"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "a",
            ),
            &[dest],
        )
        .await
        .unwrap();

        s.remove_pdus(dest, &[event_id!("$m1:example.com")])
            .await
            .unwrap();
        // Second call with the same IDs — no error, no change.
        s.remove_pdus(dest, &[event_id!("$m1:example.com")])
            .await
            .unwrap();

        assert!(s.pending_pdus(dest).await.unwrap().is_empty());
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
