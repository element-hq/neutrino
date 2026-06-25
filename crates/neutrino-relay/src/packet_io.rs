use async_trait::async_trait;

use crate::RelayError;

/// The host's TUN device, abstracted.
///
/// [`recv`](Self::recv) yields one outbound IP packet the local host wrote into
/// the tunnel; [`send`](Self::send) injects one inbound IP packet back into the
/// host. The production implementation wraps the Android `VpnService` file
/// descriptor; tests use an in-memory channel pair.
#[async_trait]
pub trait PacketIo: Send + Sync {
    /// Next outbound IP packet from the local host, or `None` once the device
    /// is closed and no more packets will arrive.
    async fn recv(&self) -> Option<Vec<u8>>;

    /// Deliver one inbound IP packet to the local host.
    async fn send(&self, packet: &[u8]) -> Result<(), RelayError>;
}
