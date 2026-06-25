use async_trait::async_trait;

use crate::{NodeKey, RelayError};

/// The wire between nodes, abstracted as an unreliable datagram transport.
///
/// Production uses iroh's QUIC datagrams over LAN/BLE; tests use an in-memory
/// fabric. Loss is acceptable — the relay carries IP packets whose upper layers
/// own retransmission — so [`send`](Self::send) is best-effort. Crucially,
/// [`recv`](Self::recv) returns the source node *authenticated by the
/// transport*, not a value the sender can forge; the relay's anti-spoof check
/// depends on that guarantee.
#[async_trait]
pub trait DatagramTransport: Send + Sync {
    /// Send one datagram to `dst`. Best-effort: loss is acceptable.
    async fn send(&self, dst: NodeKey, datagram: &[u8]) -> Result<(), RelayError>;

    /// Next inbound datagram paired with its cryptographically-authenticated
    /// source node, or `None` once the transport is closed.
    async fn recv(&self) -> Option<(NodeKey, Vec<u8>)>;
}
