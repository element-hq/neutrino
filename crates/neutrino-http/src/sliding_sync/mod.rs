use std::sync::Arc;

use neutrino_common::storage::{StorageBackend, StorageError};
use ruma::UserId;
use ruma::api::client::sync::sync_events::v5;
use thiserror::Error;

mod build;
mod conn;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;

use conn::ConnRegistry;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("M_UNKNOWN_POS")]
    UnknownPos,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub struct SyncState<S> {
    pub store: Arc<S>,
    pub registry: ConnRegistry,
}

impl<S> SyncState<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            registry: ConnRegistry::new(),
        }
    }
}

pub async fn handle<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: v5::Request,
) -> Result<v5::Response, SyncError> {
    build::build_response(state, user_id, req).await
}
