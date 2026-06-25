//! Virtual-network packet relay core.
//!
//! Carries IP packets between nodes over an unreliable datagram transport,
//! routing by a deterministic node → virtual-IP mapping. This crate is the
//! pure, transport-agnostic core: it knows nothing about iroh, BLE, or the host
//! TUN device. The embedding layer (`neutrino-ffi`) supplies the two seams —
//! [`PacketIo`] (the host's TUN) and [`DatagramTransport`] (the wire) — and
//! drives [`run`]. Keeping this core free of those big dependencies is what
//! lets the whole relay be unit-tested in CI without a device.

mod neighbour;
mod packet_io;
mod relay;
mod transport;
mod vip;

#[cfg(test)]
mod mem;

use thiserror::Error;

pub use neighbour::NeighbourTable;
pub use packet_io::PacketIo;
pub use relay::run;
pub use transport::DatagramTransport;
pub use vip::{VIP_PREFIX_BYTE, VIP_SUBNET, VIP_SUBNET_PREFIX_LEN, vip};

/// A node's stable cryptographic identity, as raw public-key bytes. The
/// transport authenticates the source of every inbound datagram down to these
/// bytes; the embedding layer converts to and from its own key type (e.g.
/// iroh's `NodeId`) at the seam, so no transport-specific key type leaks into
/// the core.
pub type NodeKey = [u8; 32];

/// Errors surfaced across the relay's two seams. Seam *closure* is not an
/// error — it is signalled by `recv` returning `None` — so the only variant is
/// a failed packet transfer.
#[derive(Debug, Error)]
pub enum RelayError {
    /// Delivering a packet to (or reading one from) the host TUN failed.
    #[error("packet i/o failed: {0}")]
    Io(String),
}
