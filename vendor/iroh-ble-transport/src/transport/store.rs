//! Pluggable peer persistence for state restoration.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::BleResult;
use crate::transport::peer::KeyPrefix;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSnapshot {
    pub last_device_id: String,
    pub last_seen: SystemTime,
}

#[async_trait]
pub trait PeerStore: Send + Sync + 'static {
    async fn put(&self, prefix: KeyPrefix, snapshot: PeerSnapshot) -> BleResult<()>;
    async fn get(&self, prefix: KeyPrefix) -> BleResult<Option<PeerSnapshot>>;
    async fn list(&self) -> BleResult<Vec<(KeyPrefix, PeerSnapshot)>>;
    async fn forget(&self, prefix: KeyPrefix) -> BleResult<()>;
}

#[derive(Debug, Default)]
pub struct InMemoryPeerStore {
    inner: Mutex<HashMap<KeyPrefix, PeerSnapshot>>,
}

impl InMemoryPeerStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PeerStore for InMemoryPeerStore {
    async fn put(&self, prefix: KeyPrefix, snapshot: PeerSnapshot) -> BleResult<()> {
        self.inner.lock().unwrap().insert(prefix, snapshot);
        Ok(())
    }

    async fn get(&self, prefix: KeyPrefix) -> BleResult<Option<PeerSnapshot>> {
        Ok(self.inner.lock().unwrap().get(&prefix).cloned())
    }

    async fn list(&self) -> BleResult<Vec<(KeyPrefix, PeerSnapshot)>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect())
    }

    async fn forget(&self, prefix: KeyPrefix) -> BleResult<()> {
        self.inner.lock().unwrap().remove(&prefix);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::peer::KEY_PREFIX_LEN;

    fn sample() -> PeerSnapshot {
        PeerSnapshot {
            last_device_id: "test-device".into(),
            last_seen: SystemTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn in_memory_roundtrip() {
        let store = InMemoryPeerStore::new();
        let prefix = [7u8; KEY_PREFIX_LEN];
        store.put(prefix, sample()).await.unwrap();
        assert!(store.get(prefix).await.unwrap().is_some());
        assert_eq!(store.list().await.unwrap().len(), 1);
        store.forget(prefix).await.unwrap();
        assert!(store.get(prefix).await.unwrap().is_none());
    }
}
