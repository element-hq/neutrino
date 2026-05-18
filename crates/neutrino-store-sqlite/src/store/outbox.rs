//! `FederationOutbox` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{params, params_from_iter};
use neutrino_store::{FederationOutbox, StorageError, StoredEvent};
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
    ) -> Result<Vec<StoredEvent>, StorageError> {
        let destination = destination.to_owned();

        self.run_read(move |conn| -> Result<Vec<StoredEvent>, Error> {
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
