use super::types::{CentralEvent, ScanFilter, WriteType};
use crate::error::BlewResult;
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::types::{BleDevice, DeviceId};
use futures_core::Stream;
use std::future::Future;
use uuid::Uuid;

pub(crate) mod private {
    pub trait Sealed {}
}

/// Trait implemented by each platform backend for the central (scanner/client) role.
///
/// This trait is **sealed**: external crates cannot implement it. The type parameter on
/// [`Central`](super::Central) defaults to the platform backend for the current target,
/// so most code never needs to name this trait.
///
/// All methods take `&self` so [`Central`](super::Central) can be cheaply cloned via `Arc`.
pub trait CentralBackend: private::Sealed + Send + Sync + 'static {
    /// The concrete `Stream` type yielded by [`events`](Self::events).
    type EventStream: Stream<Item = CentralEvent> + Send + Unpin + 'static;

    /// Construct and initialise the backend, performing any async platform setup.
    fn new() -> impl Future<Output = BlewResult<Self>> + Send
    where
        Self: Sized;

    /// Returns `true` if the local Bluetooth adapter is powered on.
    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send;

    /// Start scanning; discovered devices arrive on the event stream.
    fn start_scan(&self, filter: ScanFilter) -> impl Future<Output = BlewResult<()>> + Send;

    /// Stop an active scan.
    fn stop_scan(&self) -> impl Future<Output = BlewResult<()>> + Send;

    /// Snapshot of all peripherals seen since the last [`start_scan`](Self::start_scan).
    fn discovered_devices(&self) -> impl Future<Output = BlewResult<Vec<BleDevice>>> + Send;

    /// Initiate a connection; [`CentralEvent::DeviceConnected`] signals success.
    fn connect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send;

    /// Disconnect from a connected peripheral.
    fn disconnect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send;

    /// Perform GATT service discovery on a connected peripheral.
    fn discover_services(
        &self,
        device_id: &DeviceId,
    ) -> impl Future<Output = BlewResult<Vec<GattService>>> + Send;

    /// Read the current value of a characteristic.
    fn read_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<Vec<u8>>> + Send;

    /// Write a value to a characteristic.
    fn write_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> impl Future<Output = BlewResult<()>> + Send;

    /// Enable notifications/indications for a characteristic.
    fn subscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send;

    /// Disable notifications/indications for a characteristic.
    fn unsubscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send;

    /// Negotiated ATT MTU for a connected device. Returns 23 (ATT minimum) if unknown.
    fn mtu(&self, device_id: &DeviceId) -> impl Future<Output = u16> + Send;

    /// Open an L2CAP CoC channel to a connected peripheral.
    /// Returns [`BlewError::NotSupported`](crate::error::BlewError::NotSupported) until
    /// the platform backend implements L2CAP.
    fn open_l2cap_channel(
        &self,
        device_id: &DeviceId,
        psm: Psm,
    ) -> impl Future<Output = BlewResult<L2capChannel>> + Send;

    /// Subscribe to central-role events. Each call returns an independent stream
    /// backed by its own channel so back-pressure is per-subscriber.
    fn events(&self) -> Self::EventStream;
}
