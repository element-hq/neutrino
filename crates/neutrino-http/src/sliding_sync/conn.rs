use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ruma::api::client::sync::sync_events::v5::request;
use ruma::events::StateEventType;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnKey {
    pub user_id: OwnedUserId,
    pub conn_id: String,
}

#[derive(Debug, Clone)]
pub struct ListCfg {
    pub timeline_limit: usize,
    pub required_state: Vec<(StateEventType, String)>,
    // Parsed but not yet honoured — phase 3 will slice rooms by ranges.
    #[allow(dead_code)]
    pub ranges: Vec<(usize, usize)>,
    // Parsed for forward compatibility but always ignored (per CLAUDE.md scope).
    #[allow(dead_code)]
    pub filters: Option<request::ListFilters>,
}

#[derive(Debug, Clone)]
pub struct SubCfg {
    pub timeline_limit: usize,
    pub required_state: Vec<(StateEventType, String)>,
}

#[derive(Debug, Default, Clone)]
pub struct RoomSent {
    pub timeline_event_ids: Vec<OwnedEventId>,
    pub required_state_keys: HashMap<(String, String), OwnedEventId>,
}

#[derive(Debug, Default)]
pub struct Conn {
    pub last_stream_pos: u64,
    pub lists: BTreeMap<String, ListCfg>,
    pub subs: BTreeMap<OwnedRoomId, SubCfg>,
    pub sent: HashMap<OwnedRoomId, RoomSent>,
}

impl Conn {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct ConnRegistry {
    conns: Mutex<HashMap<ConnKey, Arc<Mutex<Conn>>>>,
}

impl ConnRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(&self, key: ConnKey) -> Arc<Mutex<Conn>> {
        let conn = Arc::new(Mutex::new(Conn::new()));
        self.conns.lock().await.insert(key, conn.clone());
        conn
    }

    pub async fn get(&self, key: &ConnKey) -> Option<Arc<Mutex<Conn>>> {
        self.conns.lock().await.get(key).cloned()
    }
}
