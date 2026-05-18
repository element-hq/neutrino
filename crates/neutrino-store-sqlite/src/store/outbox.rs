use async_trait::async_trait;
use neutrino_store::{FederationOutbox, StorageError, StoredEvent};
use ruma::{EventId, OwnedServerName, ServerName};

use crate::SqliteStore;

#[async_trait]
impl FederationOutbox for SqliteStore {
    async fn pending_destinations(&self) -> Result<Vec<OwnedServerName>, StorageError> {
        todo!()
    }

    async fn pending_pdus(
        &self,
        _destination: &ServerName,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        todo!()
    }

    async fn remove_pdus(
        &self,
        _destination: &ServerName,
        _event_ids: &[&EventId],
    ) -> Result<(), StorageError> {
        todo!()
    }
}
