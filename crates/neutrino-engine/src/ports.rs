//! Outbound federation ports and the data types that cross them.

use std::collections::BTreeMap;

use ruma::{OwnedEventId, OwnedRoomId, RoomId, ServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

/// Outbound transport failure, as the runtime needs to classify it. Neutral
/// over the concrete transport: the `neutrino-http` impl maps its
/// `reqwest`-backed error onto this so the engine never names `reqwest`.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The peer answered with a non-2xx status. Carries the raw code so the
    /// caller can distinguish a 4xx (give up) from a 5xx (retry).
    #[error("peer returned HTTP {0}")]
    Status(u16),
    /// Connection / DNS / timeout / malformed body / URL-build failure —
    /// generally retryable. Carries a rendered description for logging.
    #[error("federation transport error: {0}")]
    Transient(String),
}

/// A room's forward extremities, as exchanged on the wire: the timeline heads
/// and (MSC4242) the state-DAG heads. Carried back on a `send` transaction
/// response so the sender can detect divergence and trigger reconciliation.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ForwardExtremities {
    #[serde(default)]
    pub timeline: Vec<OwnedEventId>,
    #[serde(default)]
    pub state: Vec<OwnedEventId>,
}

impl ForwardExtremities {
    pub fn is_empty(&self) -> bool {
        self.timeline.is_empty() && self.state.is_empty()
    }
}

/// Parameters for a `get_missing_events` walk against a peer.
pub struct MissingEventsQuery<'a> {
    pub origin: &'a ServerName,
    pub room_id: &'a RoomId,
    /// Heads to walk back from.
    pub latest: &'a [OwnedEventId],
    /// Boundary the caller already holds; excluded from the result.
    pub earliest: &'a [OwnedEventId],
    pub limit: u32,
    /// MSC4242: walk `prev_state_events` (the state DAG) rather than `prev_events`.
    pub state_dag: bool,
    /// Anti-entropy: also return any `latest` heads the peer itself holds, not
    /// only their ancestors.
    pub include_latest_events: bool,
}

/// Deliver a federation transaction to one destination.
///
/// The outbound delivery pool drives this; the `neutrino-http` impl
/// (`FederationClient`) owns the direct-vs-low-bandwidth-proxy routing, so the
/// engine is oblivious to it.
#[async_trait::async_trait]
pub trait FederationTransport: Send + Sync {
    /// `PUT /_matrix/federation/v1/send/{txn_id}`. Returns each room's
    /// post-transaction forward extremities as reported by the peer.
    async fn send_transaction(
        &self,
        dest: &ServerName,
        txn_id: &str,
        pdus: &[Box<RawJsonValue>],
        forward_extremities: &BTreeMap<OwnedRoomId, ForwardExtremities>,
    ) -> Result<BTreeMap<OwnedRoomId, ForwardExtremities>, TransportError>;
}

/// Fetch events from a peer via
/// `POST origin/_matrix/federation/v1/get_missing_events`. The production impl
/// is `neutrino-http`'s `ReqwestFetcher`; tests inject a stub. Held by the
/// runtime as an `Arc<dyn MissingEventsFetcher>`.
#[async_trait::async_trait]
pub trait MissingEventsFetcher: Send + Sync {
    /// Walk back from `q.latest` (stopping at `q.earliest`) up to `q.limit`
    /// events, returning opaque PDU bytes oldest-first. `Ok(empty)` means the
    /// peer gave us nothing new (the caller treats it as an unfillable gap);
    /// `Err` is a transport/HTTP failure reaching the peer.
    async fn fetch(
        &self,
        q: MissingEventsQuery<'_>,
    ) -> Result<Vec<Box<RawJsonValue>>, TransportError>;
}
