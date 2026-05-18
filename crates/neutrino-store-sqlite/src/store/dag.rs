use async_trait::async_trait;
use neutrino_store::{DagStore, StorageError, StoredPdu};
use ruma::{EventId, RoomId};

use crate::SqliteStore;

#[async_trait]
impl DagStore for SqliteStore {
    async fn events_before(
        &self,
        _room_id: &RoomId,
        _from: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!()
    }

    async fn missing_events(
        &self,
        _room_id: &RoomId,
        _latest: &[&EventId],
        _earliest: &[&EventId],
        _limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        todo!()
    }
}
