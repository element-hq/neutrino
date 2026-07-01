//! Transport layer module tree. `BleTransport` itself lives in `transport.rs`.

pub mod dedup;
pub mod driver;
pub mod events;
pub mod hook;
pub mod interface;
pub mod l2cap;
pub mod mtu;
pub mod peer;
pub mod pipe;
pub mod registry;
pub mod reliable;
pub mod routing;
pub mod store;
#[allow(clippy::module_inception)]
pub mod transport;
pub mod watchdog;

#[cfg(feature = "testing")]
pub mod test_util;

pub use driver::IncomingPacket;
pub use peer::{ConnectPath, KEY_PREFIX_LEN, KeyPrefix};
pub use store::{InMemoryPeerStore, PeerSnapshot, PeerStore};
pub use transport::{BlePeerInfo, BlePeerPhase, BleTransport, BleTransportConfig, L2capPolicy};
