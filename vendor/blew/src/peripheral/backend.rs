use super::types::{AdvertisingConfig, PeripheralRequest, PeripheralStateEvent};
use crate::error::BlewResult;
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::types::DeviceId;
use futures_core::Stream;
use std::future::Future;
use uuid::Uuid;

pub(crate) mod private {
    pub trait Sealed {}
}

/// Trait implemented by each platform backend for the peripheral (advertiser/GATT server) role.
///
/// This trait is **sealed**: external crates cannot implement it. See [`CentralBackend`](crate::central::backend::CentralBackend)
/// for the sealing rationale.
pub trait PeripheralBackend: private::Sealed + Send + Sync + 'static {
    /// The concrete `Stream` type yielded by [`state_events`](Self::state_events).
    type StateEvents: Stream<Item = PeripheralStateEvent> + Send + Unpin + 'static;

    /// The concrete `Stream` type yielded by [`take_requests`](Self::take_requests).
    type Requests: Stream<Item = PeripheralRequest> + Send + Unpin + 'static;

    /// Construct and initialise the backend.
    fn new() -> impl Future<Output = BlewResult<Self>> + Send
    where
        Self: Sized;

    /// Returns `true` if the local Bluetooth adapter is powered on.
    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send;

    /// Register a GATT service. Must be called before [`start_advertising`](Self::start_advertising).
    fn add_service(&self, service: &GattService) -> impl Future<Output = BlewResult<()>> + Send;

    /// Begin advertising. Returns [`BlewError::AlreadyAdvertising`](crate::error::BlewError::AlreadyAdvertising)
    /// if already active.
    fn start_advertising(
        &self,
        config: &AdvertisingConfig,
    ) -> impl Future<Output = BlewResult<()>> + Send;

    /// Stop advertising.
    fn stop_advertising(&self) -> impl Future<Output = BlewResult<()>> + Send;

    /// Push a characteristic value update to a single subscribed central.
    ///
    /// The notification is unicast to the central identified by `device_id`
    /// when the platform's GATT server API supports per-subscriber targeting
    /// (Apple, Android). On Linux/BlueZ, BlueZ's `CharacteristicNotifier`
    /// callback does not expose the remote device identity, so this degrades
    /// to a broadcast to every subscribed notifier for that characteristic.
    fn notify_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> impl Future<Output = BlewResult<()>> + Send;

    /// Publish an L2CAP CoC channel and return the OS-assigned PSM together with
    /// a stream of incoming [`L2capChannel`] connections.
    ///
    /// The caller should advertise the returned PSM (e.g. via a readable GATT
    /// characteristic) so that centrals know which PSM to connect on.
    ///
    /// Returns [`BlewError::NotSupported`](crate::error::BlewError::NotSupported) until
    /// the platform backend implements L2CAP.
    fn l2cap_listener(
        &self,
    ) -> impl Future<
        Output = BlewResult<(
            Psm,
            impl Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
        )>,
    > + Send;

    /// Subscribe to clone-able peripheral state events (adapter/power, subscription changes).
    /// Each call returns an independent stream — fan-out is safe.
    fn state_events(&self) -> Self::StateEvents;

    /// Take ownership of the GATT request stream. Single-consumer by construction:
    /// the first call returns `Some`, subsequent calls return `None`.
    fn take_requests(&self) -> Option<Self::Requests>;
}
