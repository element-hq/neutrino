use async_trait::async_trait;
use neutrino_store::{
    Direction, EventStore, PaginationToken, StorageError, StoredEvent, StreamPos,
};
use ruma::{EventId, OwnedEventId, RoomId, ServerName, UserId};
use tokio::sync::watch;

use crate::SqliteStore;

#[async_trait]
impl EventStore for SqliteStore {
    async fn persist_event(
        &self,
        _event: &StoredEvent,
        _destinations: &[&ServerName],
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn get_client_txn(
        &self,
        _txn_id: &str,
        _user_id: &UserId,
    ) -> Result<Option<OwnedEventId>, StorageError> {
        todo!()
    }

    async fn record_client_txn(
        &self,
        _txn_id: &str,
        _user_id: &UserId,
        _event_id: &EventId,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn get_events(&self, _ids: &[&EventId]) -> Result<Vec<StoredEvent>, StorageError> {
        todo!()
    }

    async fn events_after(
        &self,
        _pos: StreamPos,
        _limit: usize,
    ) -> Result<Vec<(StreamPos, StoredEvent)>, StorageError> {
        todo!()
    }

    async fn room_messages(
        &self,
        _room_id: &RoomId,
        _from: Option<PaginationToken>,
        _dir: Direction,
        _limit: usize,
    ) -> Result<(Vec<StoredEvent>, Option<PaginationToken>), StorageError> {
        todo!()
    }

    fn subscribe(&self) -> watch::Receiver<StreamPos> {
        SqliteStore::subscribe(self)
    }
}
