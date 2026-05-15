use std::collections::HashMap;

use async_trait::async_trait;
use neutrino_store::{StateStore, StorageError, StoredEvent};
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};

use crate::SqliteStore;

#[async_trait]
impl StateStore for SqliteStore {
    async fn current_room_state(
        &self,
        _room_id: &RoomId,
    ) -> Result<HashMap<(String, String), StoredEvent>, StorageError> {
        todo!()
    }

    async fn current_state_event(
        &self,
        _room_id: &RoomId,
        _event_type: &str,
        _state_key: &str,
    ) -> Result<Option<StoredEvent>, StorageError> {
        todo!()
    }

    async fn current_state_events_of_type(
        &self,
        _room_id: &RoomId,
        _event_type: &str,
    ) -> Result<HashMap<String, StoredEvent>, StorageError> {
        todo!()
    }

    async fn joined_rooms(&self, _user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        todo!()
    }

    async fn joined_members(
        &self,
        _room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, StoredEvent>, StorageError> {
        todo!()
    }
}
