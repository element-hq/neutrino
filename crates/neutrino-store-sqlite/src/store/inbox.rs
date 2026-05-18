use async_trait::async_trait;
use neutrino_store::{FederationInbox, StorageError};
use ruma::ServerName;

use crate::SqliteStore;

#[async_trait]
impl FederationInbox for SqliteStore {
    async fn record_federation_txn(
        &self,
        _origin: &ServerName,
        _txn_id: &str,
    ) -> Result<bool, StorageError> {
        todo!()
    }
}
