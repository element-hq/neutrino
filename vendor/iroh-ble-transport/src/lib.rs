pub mod error;
pub mod transport;

pub use blew::BlewError;
pub use blew::central::{CentralEvent, ScanFilter, WriteType};
pub use blew::gatt::props::{AttributePermissions, CharacteristicProperties};
pub use blew::gatt::service::{GattCharacteristic, GattService};
pub use blew::peripheral::{
    AdvertisingConfig, PeripheralRequest, PeripheralStateEvent, ReadResponder, WriteResponder,
};
pub use blew::{BleDevice, Central, CentralConfig, DeviceId, Peripheral};
pub use error::{BleError, BleResult};
pub use transport::hook::{BleDedupHook, VerifiedEndpointEvent};
pub use transport::{
    BlePeerInfo, BlePeerPhase, BleTransport, BleTransportConfig, ConnectPath, InMemoryPeerStore,
    IncomingPacket, KEY_PREFIX_LEN, KeyPrefix, L2capPolicy, PeerSnapshot, PeerStore,
};
