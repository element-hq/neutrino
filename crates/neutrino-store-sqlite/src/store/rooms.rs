use async_trait::async_trait;
use neutrino_store::{RoomStore, StorageError, StoredEvent};
use ruma::{RoomId, RoomVersionId};

use crate::SqliteStore;

#[async_trait]
impl RoomStore for SqliteStore {
    async fn create_room(
        &self,
        _create_event: &StoredEvent,
        _initial_events: &[StoredEvent],
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn get_room_version(
        &self,
        _room_id: &RoomId,
    ) -> Result<Option<RoomVersionId>, StorageError> {
        todo!()
    }

    async fn room_count(&self) -> Result<u64, StorageError> {
        todo!()
    }
}
