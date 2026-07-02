//! In-memory mock backends for testing without BLE hardware.
//!
//! ```rust,no_run
//! use blew::testing::MockLink;
//! use blew::{Central, Peripheral};
//!
//! # async fn example() -> blew::BlewResult<()> {
//! let (central_link, peripheral_link) = MockLink::pair();
//! let central: Central<_> = Central::from_backend(central_link.central);
//! let peripheral: Peripheral<_> = Peripheral::from_backend(peripheral_link.peripheral);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc, oneshot};
use uuid::Uuid;

use crate::central::backend::{self, CentralBackend};
use crate::central::types::{CentralEvent, DisconnectCause, ScanFilter, WriteType};
use crate::error::{BlewError, BlewResult};
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::peripheral::backend::{self as periph_backend, PeripheralBackend};
use crate::peripheral::types::{
    AdvertisingConfig, PeripheralRequest, PeripheralStateEvent, ReadResponder, WriteResponder,
};
use crate::types::{BleDevice, DeviceId};
use crate::util::BroadcastEventStream;

const MOCK_MTU: u16 = 512;
/// Policy for injecting L2CAP failures in mock tests.
#[derive(Debug, Clone, Default)]
pub struct MockL2capPolicy {
    pub listener_error: Option<MockErrorKind>,
    pub open_error: Option<MockErrorKind>,
}

/// Error kinds for [`MockL2capPolicy`]. Avoids `BlewError`'s non-`Clone` variants.
#[derive(Debug, Clone, Copy)]
pub enum MockErrorKind {
    /// Maps to [`BlewError::NotSupported`].
    NotSupported,
    /// Maps to [`BlewError::Internal`] with a test-supplied message.
    Internal,
}

impl MockErrorKind {
    fn to_error(self) -> BlewError {
        match self {
            Self::NotSupported => BlewError::NotSupported,
            Self::Internal => BlewError::Internal("mock l2cap policy rejected".into()),
        }
    }
}
struct LinkState {
    services: Vec<GattService>,
    char_values: HashMap<Uuid, Vec<u8>>,
    subscriptions: HashMap<Uuid, mpsc::UnboundedSender<Bytes>>,
    advertising: bool,
    adv_config: Option<AdvertisingConfig>,
    connected: bool,
    l2cap_policy: MockL2capPolicy,
    l2cap_psm: Option<crate::l2cap::types::Psm>,
    l2cap_accept_tx:
        Option<tokio::sync::mpsc::UnboundedSender<BlewResult<(DeviceId, L2capChannel)>>>,
    next_read_error: Option<MockErrorKind>,
    next_write_error: Option<MockErrorKind>,
    next_subscribe_error: Option<MockErrorKind>,
    drop_next_notification: bool,
}

impl LinkState {
    fn new() -> Self {
        Self {
            services: Vec::new(),
            char_values: HashMap::new(),
            subscriptions: HashMap::new(),
            advertising: false,
            adv_config: None,
            connected: false,
            l2cap_policy: MockL2capPolicy::default(),
            l2cap_psm: None,
            l2cap_accept_tx: None,
            next_read_error: None,
            next_write_error: None,
            next_subscribe_error: None,
            drop_next_notification: false,
        }
    }
}

type SharedLink = Arc<Mutex<LinkState>>;
/// Central side of a [`MockLink`].
pub struct MockLinkEndpoint {
    pub central: MockCentral,
}

/// Peripheral side of a [`MockLink`].
pub struct MockPeripheralEndpoint {
    pub peripheral: MockPeripheral,
}

/// Factory for paired mock central + peripheral over an in-memory link.
pub struct MockLink;

impl MockLink {
    /// Create a mock central and mock peripheral wired together.
    #[must_use]
    pub fn pair() -> (MockLinkEndpoint, MockPeripheralEndpoint) {
        let link = Arc::new(Mutex::new(LinkState::new()));

        let (central_event_tx, central_event_rx) = mpsc::unbounded_channel();
        let (periph_request_tx, periph_request_rx) = mpsc::unbounded_channel();
        let (periph_state_tx, _) = broadcast::channel(256);

        let _ = central_event_tx.send(CentralEvent::AdapterStateChanged { powered: true });

        let central = MockCentral {
            link: Arc::clone(&link),
            event_tx: central_event_tx.clone(),
            central_rx: Arc::new(Mutex::new(Some(central_event_rx))),
            periph_request_tx: periph_request_tx.clone(),
            periph_state_tx: periph_state_tx.clone(),
            powered: Arc::new(Mutex::new(true)),
            restored: Arc::new(Mutex::new(None)),
        };

        let peripheral = MockPeripheral {
            link: Arc::clone(&link),
            request_rx: Arc::new(Mutex::new(Some(periph_request_rx))),
            request_tx: periph_request_tx,
            state_tx: periph_state_tx,
            central_sender_keepalive: central_event_tx.clone(),
            powered: Arc::new(Mutex::new(true)),
            restored: Arc::new(Mutex::new(None)),
        };

        (
            MockLinkEndpoint { central },
            MockPeripheralEndpoint { peripheral },
        )
    }

    /// Like [`pair`](Self::pair) but with a custom L2CAP failure-injection policy.
    #[must_use]
    pub fn pair_with_policy(policy: MockL2capPolicy) -> (MockLinkEndpoint, MockPeripheralEndpoint) {
        let (central_ep, periph_ep) = Self::pair();
        central_ep.central.link.lock().l2cap_policy = policy;
        (central_ep, periph_ep)
    }
}
/// In-memory mock [`CentralBackend`].
pub struct MockCentral {
    link: SharedLink,
    event_tx: mpsc::UnboundedSender<CentralEvent>,
    central_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<CentralEvent>>>>,
    periph_request_tx: mpsc::UnboundedSender<PeripheralRequest>,
    periph_state_tx: broadcast::Sender<PeripheralStateEvent>,
    powered: Arc<Mutex<bool>>,
    restored: Arc<Mutex<Option<Vec<BleDevice>>>>,
}

impl Clone for MockCentral {
    fn clone(&self) -> Self {
        Self {
            link: Arc::clone(&self.link),
            event_tx: self.event_tx.clone(),
            central_rx: Arc::clone(&self.central_rx),
            periph_request_tx: self.periph_request_tx.clone(),
            periph_state_tx: self.periph_state_tx.clone(),
            powered: Arc::clone(&self.powered),
            restored: Arc::clone(&self.restored),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockCentral {
    /// Construct a standalone powered mock central (not linked to any peripheral).
    #[must_use]
    pub fn new_powered() -> crate::central::Central<Self> {
        Self::new_standalone(true)
    }

    /// Construct a standalone unpowered mock central (not linked to any peripheral).
    #[must_use]
    pub fn new_unpowered() -> crate::central::Central<Self> {
        Self::new_standalone(false)
    }

    fn new_standalone(powered: bool) -> crate::central::Central<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (periph_request_tx, _periph_request_rx) = mpsc::unbounded_channel();
        let (periph_state_tx, _) = broadcast::channel(256);
        let link = Arc::new(Mutex::new(LinkState::new()));
        crate::central::Central::from_backend(Self {
            link,
            event_tx,
            central_rx: Arc::new(Mutex::new(Some(event_rx))),
            periph_request_tx,
            periph_state_tx,
            powered: Arc::new(Mutex::new(powered)),
            restored: Arc::new(Mutex::new(None)),
        })
    }

    /// Seed the restored-peripherals buffer as if `willRestoreState:` had fired.
    pub fn mock_set_restored(&self, devices: Vec<BleDevice>) {
        *self.restored.lock() = Some(devices);
    }

    /// Emit an `AdapterStateChanged` event and update the powered flag.
    pub fn mock_emit_adapter_state(&self, powered: bool) {
        *self.powered.lock() = powered;
        let _ = self
            .event_tx
            .send(CentralEvent::AdapterStateChanged { powered });
    }

    /// Arm the next [`read_characteristic`] call to fail with `kind`.
    ///
    /// One-shot: consumed by the next matching op. Subsequent reads behave normally.
    ///
    /// [`read_characteristic`]: CentralBackend::read_characteristic
    pub fn inject_next_read_error(&self, kind: MockErrorKind) {
        self.link.lock().next_read_error = Some(kind);
    }

    /// Arm the next [`write_characteristic`] call to fail with `kind`.
    ///
    /// [`write_characteristic`]: CentralBackend::write_characteristic
    pub fn inject_next_write_error(&self, kind: MockErrorKind) {
        self.link.lock().next_write_error = Some(kind);
    }

    /// Arm the next [`subscribe_characteristic`] call to fail with `kind`.
    ///
    /// [`subscribe_characteristic`]: CentralBackend::subscribe_characteristic
    pub fn inject_next_subscribe_error(&self, kind: MockErrorKind) {
        self.link.lock().next_subscribe_error = Some(kind);
    }

    /// Simulate a peer-initiated disconnect: mark the link disconnected, drop
    /// subscriptions, and emit [`CentralEvent::DeviceDisconnected`] with
    /// [`DisconnectCause::LinkLoss`]. Use to verify reconnect/retry flows.
    pub fn simulate_disconnect(&self) {
        {
            let mut link = self.link.lock();
            link.connected = false;
            link.subscriptions.clear();
        }
        let _ = self.event_tx.send(CentralEvent::DeviceDisconnected {
            device_id: DeviceId::from("mock-peripheral"),
            cause: DisconnectCause::LinkLoss,
        });
    }
}

impl backend::private::Sealed for MockCentral {}

impl CentralBackend for MockCentral {
    type EventStream = tokio_stream::wrappers::UnboundedReceiverStream<CentralEvent>;

    async fn new() -> BlewResult<Self>
    where
        Self: Sized,
    {
        Err(BlewError::Internal("use MockLink::pair() instead".into()))
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        let powered = *self.powered.lock();
        async move { Ok(powered) }
    }

    fn start_scan(&self, _filter: ScanFilter) -> impl Future<Output = BlewResult<()>> + Send {
        let link = self.link.lock();
        let advertising = link.advertising;
        let adv_config = link.adv_config.clone();
        let services = link.services.iter().map(|s| s.uuid).collect::<Vec<_>>();
        let tx = self.event_tx.clone();
        async move {
            if advertising {
                let name = adv_config.map(|c| c.local_name);
                let _ = tx.send(CentralEvent::DeviceDiscovered(BleDevice {
                    id: DeviceId::from("mock-peripheral"),
                    name,
                    rssi: Some(-50),
                    services,

                    manufacturer_data: None,
                }));
            }
            Ok(())
        }
    }

    async fn stop_scan(&self) -> BlewResult<()> {
        Ok(())
    }

    async fn discovered_devices(&self) -> BlewResult<Vec<BleDevice>> {
        Ok(vec![])
    }

    fn connect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        link.connected = true;
        let id = device_id.clone();
        let tx = self.event_tx.clone();
        async move {
            let _ = tx.send(CentralEvent::DeviceConnected { device_id: id });
            Ok(())
        }
    }

    fn disconnect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        link.connected = false;
        link.subscriptions.clear();
        let id = device_id.clone();
        let tx = self.event_tx.clone();
        async move {
            let _ = tx.send(CentralEvent::DeviceDisconnected {
                device_id: id,
                cause: DisconnectCause::Unknown(0),
            });
            Ok(())
        }
    }

    fn discover_services(
        &self,
        _device_id: &DeviceId,
    ) -> impl Future<Output = BlewResult<Vec<GattService>>> + Send {
        let services = self.link.lock().services.clone();
        async move { Ok(services) }
    }

    fn read_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<Vec<u8>>> + Send {
        let device_id = device_id.clone();
        let periph_tx = self.periph_request_tx.clone();
        let precheck = {
            let mut link = self.link.lock();
            if let Some(kind) = link.next_read_error.take() {
                Err(kind.to_error())
            } else if !link.connected {
                Err(BlewError::NotConnected(device_id.clone()))
            } else if let Some(value) = link.char_values.get(&char_uuid).cloned()
                && !value.is_empty()
            {
                Ok(Some(value))
            } else if link
                .services
                .iter()
                .any(|s| s.characteristics.iter().any(|c| c.uuid == char_uuid))
            {
                Ok(None)
            } else {
                Err(BlewError::CharacteristicNotFound {
                    device_id: device_id.clone(),
                    char_uuid,
                })
            }
        };

        async move {
            match precheck? {
                Some(value) => Ok(value),
                None => {
                    let (tx, rx) = oneshot::channel();
                    let responder = ReadResponder::new(tx);
                    let _ = periph_tx.send(PeripheralRequest::Read {
                        client_id: DeviceId::from("mock-central"),
                        service_uuid: Uuid::nil(),
                        char_uuid,
                        offset: 0,
                        responder,
                    });
                    match rx.await {
                        Ok(Ok(data)) => Ok(data),
                        _ => Err(BlewError::Gatt {
                            device_id,
                            source: "read failed".into(),
                        }),
                    }
                }
            }
        }
    }

    fn write_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let device_id = device_id.clone();
        let periph_tx = self.periph_request_tx.clone();
        let max = MOCK_MTU as usize - 3;
        let precheck: BlewResult<()> = {
            let mut link = self.link.lock();
            if let Some(kind) = link.next_write_error.take() {
                Err(kind.to_error())
            } else if !link.connected {
                Err(BlewError::NotConnected(device_id.clone()))
            } else if !link
                .services
                .iter()
                .any(|s| s.characteristics.iter().any(|c| c.uuid == char_uuid))
            {
                Err(BlewError::CharacteristicNotFound {
                    device_id: device_id.clone(),
                    char_uuid,
                })
            } else if value.len() > max {
                Err(BlewError::ValueTooLarge {
                    got: value.len(),
                    max,
                })
            } else {
                Ok(())
            }
        };

        async move {
            precheck?;
            let (responder, rx) = match write_type {
                WriteType::WithResponse => {
                    let (tx, rx) = oneshot::channel::<bool>();
                    (Some(WriteResponder::new(tx)), Some(rx))
                }
                WriteType::WithoutResponse => (None, None),
            };

            let _ = periph_tx.send(PeripheralRequest::Write {
                client_id: DeviceId::from("mock-central"),
                service_uuid: Uuid::nil(),
                char_uuid,
                value,
                responder,
            });

            if let Some(rx) = rx {
                match rx.await {
                    Ok(true) => Ok(()),
                    _ => Err(BlewError::Gatt {
                        device_id,
                        source: "write rejected".into(),
                    }),
                }
            } else {
                Ok(())
            }
        }
    }

    fn subscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let device_id = device_id.clone();
        let link = Arc::clone(&self.link);
        let event_tx = self.event_tx.clone();
        let periph_state_tx = self.periph_state_tx.clone();
        async move {
            let rx = {
                let mut link = link.lock();
                if let Some(kind) = link.next_subscribe_error.take() {
                    return Err(kind.to_error());
                }
                if !link.connected {
                    return Err(BlewError::NotConnected(device_id));
                }
                if !link
                    .services
                    .iter()
                    .any(|s| s.characteristics.iter().any(|c| c.uuid == char_uuid))
                {
                    return Err(BlewError::CharacteristicNotFound {
                        device_id,
                        char_uuid,
                    });
                }
                if link.subscriptions.contains_key(&char_uuid) {
                    return Err(BlewError::AlreadySubscribed {
                        device_id,
                        char_uuid,
                    });
                }
                let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
                link.subscriptions.insert(char_uuid, tx);
                rx
            };

            let notif_device_id = DeviceId::from("mock-peripheral");
            tokio::spawn(async move {
                let mut rx = rx;
                while let Some(value) = rx.recv().await {
                    let _ = event_tx.send(CentralEvent::CharacteristicNotification {
                        device_id: notif_device_id.clone(),
                        char_uuid,
                        value,
                    });
                }
            });

            let _ = periph_state_tx.send(PeripheralStateEvent::SubscriptionChanged {
                client_id: DeviceId::from("mock-central"),
                char_uuid,
                subscribed: true,
            });

            Ok(())
        }
    }

    fn unsubscribe_characteristic(
        &self,
        _device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        self.link.lock().subscriptions.remove(&char_uuid);
        async { Ok(()) }
    }

    async fn mtu(&self, _device_id: &DeviceId) -> u16 {
        MOCK_MTU
    }

    fn open_l2cap_channel(
        &self,
        device_id: &DeviceId,
        psm: Psm,
    ) -> impl Future<Output = BlewResult<L2capChannel>> + Send {
        let link = Arc::clone(&self.link);
        let device_id = device_id.clone();
        async move {
            let accept_tx = {
                let guard = link.lock();
                if let Some(kind) = guard.l2cap_policy.open_error {
                    return Err(kind.to_error());
                }
                if guard.l2cap_psm != Some(psm) {
                    return Err(BlewError::Internal(format!(
                        "no l2cap listener for psm {psm:?}"
                    )));
                }
                guard
                    .l2cap_accept_tx
                    .clone()
                    .ok_or_else(|| BlewError::Internal("listener accept tx missing".into()))?
            };
            let (central_side, peripheral_side) = L2capChannel::pair(64 * 1024);
            accept_tx
                .send(Ok((device_id, peripheral_side)))
                .map_err(|_| BlewError::Internal("listener stream closed".into()))?;
            Ok(central_side)
        }
    }

    fn events(&self) -> Self::EventStream {
        let rx = self
            .central_rx
            .lock()
            .take()
            .expect("events() called more than once on MockCentral");
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
    }
}

impl MockCentral {
    fn take_restored(&self) -> Option<Vec<BleDevice>> {
        self.restored.lock().take()
    }
}
/// In-memory mock [`PeripheralBackend`].
pub struct MockPeripheral {
    link: SharedLink,
    request_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<PeripheralRequest>>>>,
    request_tx: mpsc::UnboundedSender<PeripheralRequest>,
    state_tx: broadcast::Sender<PeripheralStateEvent>,
    central_sender_keepalive: mpsc::UnboundedSender<CentralEvent>,
    powered: Arc<Mutex<bool>>,
    restored: Arc<Mutex<Option<Vec<Uuid>>>>,
}

impl Clone for MockPeripheral {
    fn clone(&self) -> Self {
        Self {
            link: Arc::clone(&self.link),
            request_rx: Arc::clone(&self.request_rx),
            request_tx: self.request_tx.clone(),
            state_tx: self.state_tx.clone(),
            central_sender_keepalive: self.central_sender_keepalive.clone(),
            powered: Arc::clone(&self.powered),
            restored: Arc::clone(&self.restored),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockPeripheral {
    fn make_link() -> SharedLink {
        Arc::new(Mutex::new(LinkState::new()))
    }

    /// Construct a standalone powered mock peripheral (not linked to any central).
    #[must_use]
    pub fn new_powered() -> crate::peripheral::Peripheral<Self> {
        Self::new_standalone(true)
    }

    /// Construct a standalone unpowered mock peripheral (not linked to any central).
    #[must_use]
    pub fn new_unpowered() -> crate::peripheral::Peripheral<Self> {
        Self::new_standalone(false)
    }

    fn new_standalone(powered: bool) -> crate::peripheral::Peripheral<Self> {
        let (central_event_tx, _central_event_rx) = mpsc::unbounded_channel::<CentralEvent>();
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = broadcast::channel(256);
        crate::peripheral::Peripheral::from_backend(Self {
            link: Self::make_link(),
            request_rx: Arc::new(Mutex::new(Some(request_rx))),
            request_tx,
            state_tx,
            central_sender_keepalive: central_event_tx,
            powered: Arc::new(Mutex::new(powered)),
            restored: Arc::new(Mutex::new(None)),
        })
    }

    /// Emit an `AdapterStateChanged` event and update the powered flag.
    pub fn mock_emit_adapter_state(&self, powered: bool) {
        *self.powered.lock() = powered;
        let _ = self
            .state_tx
            .send(PeripheralStateEvent::AdapterStateChanged { powered });
    }

    /// Seed the restored-services buffer as if `willRestoreState:` had fired.
    pub fn mock_set_restored(&self, services: Vec<Uuid>) {
        *self.restored.lock() = Some(services);
    }

    /// Arm the next [`notify_characteristic`] call to silently drop the
    /// payload instead of delivering it. One-shot.
    ///
    /// [`notify_characteristic`]: PeripheralBackend::notify_characteristic
    pub fn drop_next_notification(&self) {
        self.link.lock().drop_next_notification = true;
    }
}

impl periph_backend::private::Sealed for MockPeripheral {}

impl PeripheralBackend for MockPeripheral {
    type StateEvents = BroadcastEventStream<PeripheralStateEvent>;
    type Requests = tokio_stream::wrappers::UnboundedReceiverStream<PeripheralRequest>;

    async fn new() -> BlewResult<Self>
    where
        Self: Sized,
    {
        Err(BlewError::Internal("use MockLink::pair() instead".into()))
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        let powered = *self.powered.lock();
        async move { Ok(powered) }
    }

    fn add_service(&self, service: &GattService) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        for ch in &service.characteristics {
            if !ch.value.is_empty() {
                link.char_values.insert(ch.uuid, ch.value.clone());
            }
        }
        link.services.push(service.clone());
        async { Ok(()) }
    }

    fn start_advertising(
        &self,
        config: &AdvertisingConfig,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        if link.advertising {
            return std::future::ready(Err(BlewError::AlreadyAdvertising));
        }
        link.advertising = true;
        link.adv_config = Some(config.clone());
        std::future::ready(Ok(()))
    }

    fn stop_advertising(&self) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        link.advertising = false;
        link.adv_config = None;
        async { Ok(()) }
    }

    fn notify_characteristic(
        &self,
        _device_id: &crate::types::DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let mut link = self.link.lock();
        if std::mem::take(&mut link.drop_next_notification) {
            // Silently discard the notification, as if it were lost in transit.
        } else if let Some(tx) = link.subscriptions.get(&char_uuid) {
            let _ = tx.send(Bytes::from(value));
        }
        async { Ok(()) }
    }

    fn l2cap_listener(
        &self,
    ) -> impl Future<
        Output = BlewResult<(
            crate::l2cap::types::Psm,
            impl futures_core::Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
        )>,
    > + Send {
        let link = Arc::clone(&self.link);
        async move {
            let mut guard = link.lock();
            if let Some(kind) = guard.l2cap_policy.listener_error {
                return Err(kind.to_error());
            }
            // Fixed PSM for deterministic tests.
            let psm = crate::l2cap::types::Psm(0x1001);
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            guard.l2cap_psm = Some(psm);
            guard.l2cap_accept_tx = Some(tx);
            Ok((
                psm,
                tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
            ))
        }
    }

    fn state_events(&self) -> Self::StateEvents {
        BroadcastEventStream::new(self.state_tx.subscribe())
    }

    fn take_requests(&self) -> Option<Self::Requests> {
        self.request_rx
            .lock()
            .take()
            .map(tokio_stream::wrappers::UnboundedReceiverStream::new)
    }
}

impl MockPeripheral {
    fn take_restored(&self) -> Option<Vec<Uuid>> {
        self.restored.lock().take()
    }
}
impl<B: CentralBackend> crate::central::Central<B> {
    /// Create a `Central` from a pre-constructed backend (for testing).
    pub fn from_backend(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: CentralBackend + Clone> Clone for crate::central::Central<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl crate::central::Central<MockCentral> {
    /// Emit an `AdapterStateChanged` event and update the powered flag.
    pub fn mock_emit_adapter_state(&self, powered: bool) {
        self.backend.mock_emit_adapter_state(powered);
    }

    /// Consume the seeded restored-peripherals payload. See
    /// [`MockCentral::mock_set_restored`].
    #[must_use]
    pub fn take_restored(&self) -> Option<Vec<BleDevice>> {
        self.backend.take_restored()
    }
}

impl<B: PeripheralBackend> crate::peripheral::Peripheral<B> {
    /// Create a `Peripheral` from a pre-constructed backend (for testing).
    pub fn from_backend(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: PeripheralBackend + Clone> Clone for crate::peripheral::Peripheral<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl crate::peripheral::Peripheral<MockPeripheral> {
    /// Emit an `AdapterStateChanged` event and update the powered flag.
    pub fn mock_emit_adapter_state(&self, powered: bool) {
        self.backend.mock_emit_adapter_state(powered);
    }

    /// Consume the seeded restored-services payload. See
    /// [`MockPeripheral::mock_set_restored`].
    #[must_use]
    pub fn take_restored(&self) -> Option<Vec<Uuid>> {
        self.backend.take_restored()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::central::Central;
    use crate::gatt::props::{AttributePermissions, CharacteristicProperties};
    use crate::gatt::service::GattCharacteristic;
    use crate::peripheral::Peripheral;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_mock_link_discovery() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let svc_uuid = Uuid::from_u128(0x1234);
        peripheral
            .add_service(&GattService {
                uuid: svc_uuid,
                primary: true,
                characteristics: vec![],
            })
            .await
            .unwrap();

        peripheral
            .start_advertising(&AdvertisingConfig {
                local_name: "test".into(),
                service_uuids: vec![svc_uuid],

                manufacturer_data: None,
            })
            .await
            .unwrap();

        let mut events = central.events();
        // Drain the initial AdapterStateChanged event emitted on construction.
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        central.start_scan(ScanFilter::default()).await.unwrap();

        let event = events.next().await.expect("should get discovery event");
        match event {
            CentralEvent::DeviceDiscovered(device) => {
                assert_eq!(device.name.as_deref(), Some("test"));
                assert!(device.services.contains(&svc_uuid));
            }
            other => panic!("expected DeviceDiscovered, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_mock_link_connect_disconnect() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let _peripheral = Peripheral::from_backend(p.peripheral);

        let mut events = central.events();
        // Drain the initial AdapterStateChanged event emitted on construction.
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        let device_id = DeviceId::from("mock-peripheral");

        central.connect(&device_id).await.unwrap();
        let ev = events.next().await.unwrap();
        assert!(matches!(ev, CentralEvent::DeviceConnected { .. }));

        central.disconnect(&device_id).await.unwrap();
        let ev = events.next().await.unwrap();
        assert!(matches!(ev, CentralEvent::DeviceDisconnected { .. }));
    }

    #[tokio::test]
    async fn test_mock_link_read_static_value() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xAAAA);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: b"static-data".to_vec(),
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        let data = central
            .read_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
        assert_eq!(data, b"static-data");
    }

    #[tokio::test]
    async fn test_mock_link_read_dynamic_value() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xBBBB);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let PeripheralRequest::Read { responder, .. } = request {
                    responder.respond(b"dynamic-response".to_vec());
                }
            }
        });

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        let data = central
            .read_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
        assert_eq!(data, b"dynamic-response");
    }

    #[tokio::test]
    async fn test_mock_link_write_with_response() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xCCCC);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::WRITE,
                    permissions: AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let received = Arc::new(Mutex::new(Vec::new()));
        let received2 = received.clone();
        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let PeripheralRequest::Write {
                    value, responder, ..
                } = request
                {
                    received2.lock().push(value);
                    if let Some(r) = responder {
                        r.success();
                    }
                }
            }
        });

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        central
            .write_characteristic(
                &device_id,
                char_uuid,
                b"hello".to_vec(),
                WriteType::WithResponse,
            )
            .await
            .unwrap();

        tokio::task::yield_now().await;
        let values = received.lock();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], b"hello");
    }

    #[tokio::test]
    async fn test_mock_link_write_without_response() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xDDDD);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::WRITE_WITHOUT_RESPONSE,
                    permissions: AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let received2 = received.clone();
        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let PeripheralRequest::Write {
                    value, responder, ..
                } = request
                {
                    received2.lock().push(value);
                    assert!(responder.is_none());
                }
            }
        });

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        central
            .write_characteristic(
                &device_id,
                char_uuid,
                b"fire-and-forget".to_vec(),
                WriteType::WithoutResponse,
            )
            .await
            .unwrap();

        tokio::task::yield_now().await;
        let values = received.lock();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], b"fire-and-forget");
    }

    #[tokio::test]
    async fn test_mock_link_notifications() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xEEEE);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        let mut events = central.events();
        // Drain the initial AdapterStateChanged and DeviceConnected events.
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::DeviceConnected { .. }
        ));
        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();

        peripheral
            .notify_characteristic(
                &crate::types::DeviceId::from("mock-central"),
                char_uuid,
                b"notify-data".to_vec(),
            )
            .await
            .unwrap();

        let ev = tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
            .await
            .expect("should receive notification")
            .expect("stream should not end");

        match ev {
            CentralEvent::CharacteristicNotification { value, .. } => {
                assert_eq!(&value[..], b"notify-data");
            }
            other => panic!("expected notification, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_mock_peripheral_already_advertising() {
        let (_c, p) = MockLink::pair();
        let peripheral = Peripheral::from_backend(p.peripheral);

        let config = AdvertisingConfig {
            local_name: "test".into(),
            service_uuids: vec![],

            manufacturer_data: None,
        };

        peripheral.start_advertising(&config).await.unwrap();
        let result = peripheral.start_advertising(&config).await;
        assert!(matches!(result, Err(BlewError::AlreadyAdvertising)));
    }

    #[tokio::test]
    async fn test_mock_peripheral_is_powered() {
        let (_c, p) = MockLink::pair();
        let peripheral = Peripheral::from_backend(p.peripheral);
        assert!(peripheral.is_powered().await.unwrap());
    }

    #[tokio::test]
    async fn test_mock_link_discover_services() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let svc_uuid = Uuid::from_u128(0x1111);
        let char_uuid = Uuid::from_u128(0x2222);
        peripheral
            .add_service(&GattService {
                uuid: svc_uuid,
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ | CharacteristicProperties::WRITE,
                    permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        let services = central.discover_services(&device_id).await.unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].uuid, svc_uuid);
        assert_eq!(services[0].characteristics.len(), 1);
        assert_eq!(services[0].characteristics[0].uuid, char_uuid);
    }

    // Backend contract tests: behavioral guarantees that all platform backends must satisfy.
    #[tokio::test]
    async fn contract_write_without_response_returns_immediately() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x1234);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0xAA00),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::WRITE_WITHOUT_RESPONSE,
                    permissions: AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        // BLE spec: Write Command has no ATT response, so central must not block.
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            central.write_characteristic(
                &device_id,
                char_uuid,
                b"data".to_vec(),
                WriteType::WithoutResponse,
            ),
        )
        .await;

        assert!(result.is_ok(), "write-without-response must not block");
        assert!(result.unwrap().is_ok());
    }
    #[tokio::test]
    async fn contract_write_with_response_blocks_until_ack() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x5555);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::WRITE,
                    permissions: AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        let write_fut = central.write_characteristic(
            &device_id,
            char_uuid,
            b"need-ack".to_vec(),
            WriteType::WithResponse,
        );

        let result = tokio::time::timeout(std::time::Duration::from_millis(50), write_fut).await;
        assert!(
            result.is_err(),
            "write-with-response should block until ACK"
        );

        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        let req = tokio::time::timeout(std::time::Duration::from_millis(50), requests.next())
            .await
            .expect("should have request")
            .expect("stream should not end");

        match req {
            PeripheralRequest::Write { responder, .. } => {
                responder.unwrap().success();
            }
            other => panic!("expected Write request, got {other:?}"),
        }
    }
    #[tokio::test]
    async fn contract_read_request_routed_to_peripheral() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x6666);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        let expected_char = char_uuid;
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let PeripheralRequest::Read {
                    char_uuid,
                    responder,
                    ..
                } = request
                {
                    if char_uuid == expected_char {
                        responder.respond(b"correct-char".to_vec());
                    } else {
                        responder.error();
                    }
                }
            }
        });

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        let data = central
            .read_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
        assert_eq!(data, b"correct-char");
    }
    #[tokio::test]
    async fn contract_duplicate_subscribe_rejected() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x5151);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .expect("first subscribe should succeed");

        let err = central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .expect_err("second subscribe must fail");
        match err {
            BlewError::AlreadySubscribed {
                char_uuid: u,
                device_id: d,
            } => {
                assert_eq!(u, char_uuid);
                assert_eq!(d, device_id);
            }
            other => panic!("expected AlreadySubscribed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn contract_subscribe_emits_subscription_event() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x7777);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        let mut state = peripheral.state_events();

        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();

        let ev = tokio::time::timeout(std::time::Duration::from_millis(100), state.next())
            .await
            .expect("should receive subscription event")
            .expect("stream should not end");

        match ev {
            PeripheralStateEvent::SubscriptionChanged {
                char_uuid: uuid,
                subscribed,
                ..
            } => {
                assert_eq!(uuid, char_uuid);
                assert!(subscribed, "should indicate subscription");
            }
            other => panic!("expected SubscriptionChanged, got {other:?}"),
        }
    }
    #[tokio::test]
    async fn contract_notification_before_subscribe_is_dropped() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x8888);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        let mut events = central.events();
        // Drain the initial AdapterStateChanged and DeviceConnected events.
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::DeviceConnected { .. }
        ));

        peripheral
            .notify_characteristic(
                &crate::types::DeviceId::from("mock-central"),
                char_uuid,
                b"too-early".to_vec(),
            )
            .await
            .unwrap();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), events.next()).await;
        assert!(
            result.is_err(),
            "notification before subscribe should not arrive"
        );

        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();

        peripheral
            .notify_characteristic(
                &crate::types::DeviceId::from("mock-central"),
                char_uuid,
                b"after-sub".to_vec(),
            )
            .await
            .unwrap();

        let ev = tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
            .await
            .expect("should receive after subscribe")
            .expect("stream should not end");

        match ev {
            CentralEvent::CharacteristicNotification { value, .. } => {
                assert_eq!(&value[..], b"after-sub");
            }
            other => panic!("expected notification, got {other:?}"),
        }
    }
    #[tokio::test]
    async fn contract_stop_then_restart_advertising() {
        let (_c, p) = MockLink::pair();
        let peripheral = Peripheral::from_backend(p.peripheral);

        let config = AdvertisingConfig {
            local_name: "test".into(),
            service_uuids: vec![],

            manufacturer_data: None,
        };

        peripheral.start_advertising(&config).await.unwrap();
        peripheral.stop_advertising().await.unwrap();
        peripheral.start_advertising(&config).await.unwrap();
    }
    #[tokio::test]
    async fn contract_disconnect_clears_subscriptions() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0x9999);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();

        central.disconnect(&device_id).await.unwrap();

        let mut events = central.events();
        peripheral
            .notify_characteristic(
                &crate::types::DeviceId::from("mock-central"),
                char_uuid,
                b"after-disconnect".to_vec(),
            )
            .await
            .unwrap();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), events.next()).await;

        match result {
            Err(_)
            | Ok(Some(
                CentralEvent::DeviceDisconnected { .. } | CentralEvent::AdapterStateChanged { .. },
            )) => {}
            Ok(Some(CentralEvent::CharacteristicNotification { .. })) => {
                panic!("notification should not arrive after disconnect");
            }
            Ok(other) => panic!("unexpected event: {other:?}"),
        }
    }
    #[tokio::test]
    async fn contract_read_unknown_characteristic_errors() {
        let (c, _p) = MockLink::pair();
        let central = Central::from_backend(c.central);

        let device_id = DeviceId::from("mock-peripheral");
        let unknown_uuid = Uuid::from_u128(0xDEAD_BEEF);
        central.connect(&device_id).await.unwrap();

        let result = central.read_characteristic(&device_id, unknown_uuid).await;

        assert!(
            result.is_err(),
            "reading unknown characteristic should error"
        );
        match result.unwrap_err() {
            BlewError::CharacteristicNotFound { char_uuid, .. } => {
                assert_eq!(char_uuid, unknown_uuid);
            }
            other => panic!("expected CharacteristicNotFound, got {other:?}"),
        }
    }
    #[tokio::test]
    async fn contract_dropped_read_responder_returns_error() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xAAAA);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let mut requests = peripheral
            .take_requests()
            .expect("requests stream available");
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let PeripheralRequest::Read { responder, .. } = request {
                    drop(responder); // RAII: auto-sends error
                }
            }
        });

        let device_id = DeviceId::from("mock-peripheral");
        let result = central.read_characteristic(&device_id, char_uuid).await;
        assert!(
            result.is_err(),
            "dropped responder should cause read to fail"
        );
    }

    #[tokio::test]
    async fn l2cap_listener_returns_psm_and_stream() {
        use crate::peripheral::backend::PeripheralBackend;
        use tokio_stream::StreamExt;

        let (_central_ep, periph_ep) = MockLink::pair();
        let (psm, mut stream) = periph_ep.peripheral.l2cap_listener().await.unwrap();
        assert!(psm.value() >= 0x1001, "psm should be in the dynamic range");
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            _ = stream.next() => panic!("stream yielded unexpectedly"),
        }
    }

    #[tokio::test]
    async fn l2cap_listener_respects_policy_error() {
        use crate::peripheral::backend::PeripheralBackend;

        let policy = MockL2capPolicy {
            listener_error: Some(MockErrorKind::NotSupported),
            ..Default::default()
        };
        let (_central_ep, periph_ep) = MockLink::pair_with_policy(policy);
        let result = periph_ep.peripheral.l2cap_listener().await;
        assert!(matches!(result, Err(BlewError::NotSupported)));
    }
    #[tokio::test]
    async fn contract_notifications_arrive_in_order() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xBBBB);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();

        let mut events = central.events();
        // Drain the initial AdapterStateChanged and DeviceConnected events.
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::DeviceConnected { .. }
        ));
        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();

        for i in 0..10_u8 {
            peripheral
                .notify_characteristic(
                    &crate::types::DeviceId::from("mock-central"),
                    char_uuid,
                    vec![i],
                )
                .await
                .unwrap();
        }

        for expected in 0..10_u8 {
            let ev = tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
                .await
                .unwrap_or_else(|_| panic!("should receive notification {expected}"))
                .expect("stream should not end");

            match ev {
                CentralEvent::CharacteristicNotification { value, .. } => {
                    assert_eq!(&value[..], &[expected]);
                }
                other => panic!("expected notification {expected}, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn open_l2cap_channel_delivers_pair_to_listener() {
        use crate::central::backend::CentralBackend;
        use crate::peripheral::backend::PeripheralBackend;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_stream::StreamExt;

        let (central_ep, periph_ep) = MockLink::pair();

        let (psm, mut listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        let open_fut = central_ep.central.open_l2cap_channel(&device_id, psm);
        let accept_fut = listener.next();

        let (central_side, accepted) = tokio::join!(open_fut, accept_fut);
        let mut central_side = central_side.unwrap();
        let (accepted_device_id, mut periph_side) = accepted.unwrap().unwrap();
        assert_eq!(accepted_device_id, DeviceId::from("mock-peripheral"));

        central_side.write_all(b"ping").await.unwrap();
        let mut buf = [0_u8; 4];
        periph_side.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        periph_side.write_all(b"pong").await.unwrap();
        central_side.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[tokio::test]
    async fn open_l2cap_channel_respects_policy_error() {
        use crate::central::backend::CentralBackend;
        use crate::peripheral::backend::PeripheralBackend;

        let policy = MockL2capPolicy {
            open_error: Some(MockErrorKind::NotSupported),
            ..Default::default()
        };
        let (central_ep, periph_ep) = MockLink::pair_with_policy(policy);
        let (psm, _listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        let result = central_ep.central.open_l2cap_channel(&device_id, psm).await;
        assert!(matches!(result, Err(BlewError::NotSupported)));
    }

    #[tokio::test]
    async fn open_l2cap_channel_fails_when_no_listener() {
        use crate::central::backend::CentralBackend;

        let (central_ep, _periph_ep) = MockLink::pair();
        let device_id = DeviceId::from("mock-peripheral");
        let result = central_ep
            .central
            .open_l2cap_channel(&device_id, crate::l2cap::types::Psm(0x1001))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn open_l2cap_channel_supports_multiple_concurrent_channels() {
        use crate::central::backend::CentralBackend;
        use crate::peripheral::backend::PeripheralBackend;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_stream::StreamExt;

        let (central_ep, periph_ep) = MockLink::pair();
        let (psm, mut listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();
        let device_id = DeviceId::from("mock-peripheral");

        let (open_a, open_b, open_c) = tokio::join!(
            central_ep.central.open_l2cap_channel(&device_id, psm),
            central_ep.central.open_l2cap_channel(&device_id, psm),
            central_ep.central.open_l2cap_channel(&device_id, psm),
        );
        let mut central_channels = vec![open_a.unwrap(), open_b.unwrap(), open_c.unwrap()];

        let accepted_a = listener.next().await.unwrap().unwrap();
        let accepted_b = listener.next().await.unwrap().unwrap();
        let accepted_c = listener.next().await.unwrap().unwrap();
        let mut peripheral_channels = vec![accepted_a.1, accepted_b.1, accepted_c.1];

        let mut central_tasks = Vec::new();
        for (index, mut channel) in central_channels.drain(..).enumerate() {
            central_tasks.push(tokio::spawn(async move {
                let payload = format!("central-{index}").into_bytes();
                channel.write_all(&payload).await.unwrap();
                let mut echoed = vec![0_u8; payload.len()];
                channel.read_exact(&mut echoed).await.unwrap();
                (payload, echoed)
            }));
        }

        let mut peripheral_tasks = Vec::new();
        for mut channel in peripheral_channels.drain(..) {
            peripheral_tasks.push(tokio::spawn(async move {
                let mut buf = vec![0_u8; 16];
                let n = channel.read(&mut buf).await.unwrap();
                buf.truncate(n);
                channel.write_all(&buf).await.unwrap();
                buf
            }));
        }

        let mut central_results = Vec::new();
        for task in central_tasks {
            central_results.push(task.await.unwrap());
        }
        let mut peripheral_results = Vec::new();
        for task in peripheral_tasks {
            peripheral_results.push(task.await.unwrap());
        }

        for (payload, echoed) in &central_results {
            assert_eq!(payload, echoed);
        }

        let mut central_payloads: Vec<Vec<u8>> = central_results
            .into_iter()
            .map(|(payload, _)| payload)
            .collect();
        central_payloads.sort();
        peripheral_results.sort();
        assert_eq!(central_payloads, peripheral_results);
    }

    #[tokio::test]
    async fn closing_one_l2cap_channel_does_not_affect_siblings() {
        use crate::central::backend::CentralBackend;
        use crate::peripheral::backend::PeripheralBackend;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::timeout;
        use tokio_stream::StreamExt;

        let (central_ep, periph_ep) = MockLink::pair();
        let (psm, mut listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();
        let device_id = DeviceId::from("mock-peripheral");

        let (first, second) = tokio::join!(
            central_ep.central.open_l2cap_channel(&device_id, psm),
            central_ep.central.open_l2cap_channel(&device_id, psm),
        );
        let mut first = first.unwrap();
        let mut second = second.unwrap();

        let (_, mut peripheral_first) = listener.next().await.unwrap().unwrap();
        let (_, mut peripheral_second) = listener.next().await.unwrap().unwrap();

        first.close().await.unwrap();
        let mut eof = [0_u8; 1];
        let n = timeout(
            std::time::Duration::from_millis(100),
            peripheral_first.read(&mut eof),
        )
        .await
        .expect("closed channel should reach peer EOF")
        .unwrap();
        assert_eq!(n, 0);

        second.write_all(b"still-open").await.unwrap();
        let mut buf = [0_u8; 10];
        peripheral_second.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"still-open");

        peripheral_second.write_all(b"ack").await.unwrap();
        let mut ack = [0_u8; 3];
        second.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"ack");
    }

    #[tokio::test]
    async fn test_central_emits_adapter_state_on_init() {
        let (central_ep, _periph_ep) = MockLink::pair();
        let central: crate::Central<_> = crate::Central::from_backend(central_ep.central);
        let mut events = central.events();
        match events.next().await {
            Some(CentralEvent::AdapterStateChanged { powered: true }) => {}
            other => panic!("expected AdapterStateChanged(powered=true), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn take_restored_returns_none_when_unset() {
        let central = MockCentral::new_powered();
        assert!(central.take_restored().is_none());

        let peripheral = MockPeripheral::new_powered();
        assert!(peripheral.take_restored().is_none());
    }

    #[tokio::test]
    async fn take_restored_returns_seeded_value_once() {
        let central = MockCentral::new_powered();
        let device = BleDevice {
            id: DeviceId::from("AA:BB:CC:DD:EE:FF"),
            name: Some("restored".into()),
            rssi: None,
            services: vec![],

            manufacturer_data: None,
        };
        central.backend.mock_set_restored(vec![device.clone()]);

        let taken = central.take_restored().expect("first call yields value");
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].id, device.id);
        assert!(
            central.take_restored().is_none(),
            "second call must return None",
        );
    }

    #[tokio::test]
    async fn take_restored_peripheral_returns_seeded_services() {
        let peripheral = MockPeripheral::new_powered();
        let svc = Uuid::from_u128(0x1234);
        peripheral.backend.mock_set_restored(vec![svc]);

        let taken = peripheral.take_restored().expect("first call yields value");
        assert_eq!(taken, vec![svc]);
        assert!(peripheral.take_restored().is_none());
    }

    #[tokio::test]
    async fn contract_gatt_ops_after_disconnect_error() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xAAAA);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ
                        | CharacteristicProperties::WRITE
                        | CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                    value: vec![1, 2, 3],
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        central.disconnect(&device_id).await.unwrap();

        for err in [
            central.read_characteristic(&device_id, char_uuid).await,
            central
                .write_characteristic(
                    &device_id,
                    char_uuid,
                    b"x".to_vec(),
                    WriteType::WithResponse,
                )
                .await
                .map(|()| Vec::new()),
            central
                .subscribe_characteristic(&device_id, char_uuid)
                .await
                .map(|()| Vec::new()),
        ] {
            match err {
                Err(BlewError::NotConnected(id)) => assert_eq!(id, device_id),
                other => panic!("expected NotConnected, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn contract_adapter_off_mid_session_reaches_subscribers() {
        let (c, _p) = MockLink::pair();
        let central_backend = c.central.clone();
        let central = Central::from_backend(c.central);

        let mut events = central.events();
        assert!(matches!(
            events.next().await.unwrap(),
            CentralEvent::AdapterStateChanged { powered: true }
        ));
        assert!(central.is_powered().await.unwrap());

        central_backend.mock_emit_adapter_state(false);

        let ev = tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
            .await
            .expect("should receive AdapterStateChanged(false)")
            .expect("stream should not end");
        assert!(matches!(
            ev,
            CentralEvent::AdapterStateChanged { powered: false }
        ));
        assert!(!central.is_powered().await.unwrap());
    }

    #[tokio::test]
    async fn contract_reconnect_after_disconnect_restores_ops() {
        let (c, p) = MockLink::pair();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);

        let char_uuid = Uuid::from_u128(0xCAFE);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: b"hello".to_vec(),
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();

        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        central.disconnect(&device_id).await.unwrap();
        assert!(matches!(
            central.read_characteristic(&device_id, char_uuid).await,
            Err(BlewError::NotConnected(_))
        ));

        central.connect(&device_id).await.unwrap();
        let value = central
            .read_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
        assert_eq!(value, b"hello");
    }

    async fn connected_pair_with_char(
        char_uuid: Uuid,
    ) -> (
        Central<MockCentral>,
        Peripheral<MockPeripheral>,
        DeviceId,
        MockCentral,
        MockPeripheral,
    ) {
        let (c, p) = MockLink::pair();
        let central_backend = c.central.clone();
        let peripheral_backend = p.peripheral.clone();
        let central = Central::from_backend(c.central);
        let peripheral = Peripheral::from_backend(p.peripheral);
        peripheral
            .add_service(&GattService {
                uuid: Uuid::from_u128(0x1234),
                primary: true,
                characteristics: vec![GattCharacteristic {
                    uuid: char_uuid,
                    properties: CharacteristicProperties::READ
                        | CharacteristicProperties::WRITE
                        | CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                    value: b"static".to_vec(),
                    descriptors: vec![],
                }],
            })
            .await
            .unwrap();
        let device_id = DeviceId::from("mock-peripheral");
        central.connect(&device_id).await.unwrap();
        (
            central,
            peripheral,
            device_id,
            central_backend,
            peripheral_backend,
        )
    }

    #[tokio::test]
    async fn fault_inject_next_read_error_is_one_shot() {
        let char_uuid = Uuid::from_u128(0xF00D);
        let (central, _peripheral, device_id, central_backend, _p) =
            connected_pair_with_char(char_uuid).await;

        central_backend.inject_next_read_error(MockErrorKind::Internal);
        assert!(matches!(
            central.read_characteristic(&device_id, char_uuid).await,
            Err(BlewError::Internal(_))
        ));
        assert_eq!(
            central
                .read_characteristic(&device_id, char_uuid)
                .await
                .unwrap(),
            b"static"
        );
    }

    #[tokio::test]
    async fn fault_inject_next_write_error_is_one_shot() {
        let char_uuid = Uuid::from_u128(0xF00E);
        let (central, _peripheral, device_id, central_backend, _p) =
            connected_pair_with_char(char_uuid).await;

        central_backend.inject_next_write_error(MockErrorKind::NotSupported);
        assert!(matches!(
            central
                .write_characteristic(
                    &device_id,
                    char_uuid,
                    b"x".to_vec(),
                    WriteType::WithoutResponse,
                )
                .await,
            Err(BlewError::NotSupported)
        ));
        central
            .write_characteristic(
                &device_id,
                char_uuid,
                b"y".to_vec(),
                WriteType::WithoutResponse,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fault_inject_next_subscribe_error_is_one_shot() {
        let char_uuid = Uuid::from_u128(0xF00F);
        let (central, _peripheral, device_id, central_backend, _p) =
            connected_pair_with_char(char_uuid).await;

        central_backend.inject_next_subscribe_error(MockErrorKind::Internal);
        assert!(matches!(
            central
                .subscribe_characteristic(&device_id, char_uuid)
                .await,
            Err(BlewError::Internal(_))
        ));
        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fault_simulate_disconnect_drops_link_and_notifies() {
        let char_uuid = Uuid::from_u128(0xF010);
        let (central, _peripheral, device_id, central_backend, _p) =
            connected_pair_with_char(char_uuid).await;

        let mut events = central.events();
        central_backend.simulate_disconnect();

        let disconnect = loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
                .await
                .expect("timed out waiting for disconnect event")
                .expect("stream closed")
            {
                CentralEvent::DeviceDisconnected { device_id, cause } => {
                    break (device_id, cause);
                }
                _ => {}
            }
        };
        assert_eq!(disconnect.0, device_id);
        assert!(matches!(disconnect.1, DisconnectCause::LinkLoss));

        assert!(matches!(
            central.read_characteristic(&device_id, char_uuid).await,
            Err(BlewError::NotConnected(_))
        ));
    }

    #[tokio::test]
    async fn fault_drop_next_notification_swallows_one_payload() {
        let char_uuid = Uuid::from_u128(0xF011);
        let (central, peripheral, device_id, _c, peripheral_backend) =
            connected_pair_with_char(char_uuid).await;

        central
            .subscribe_characteristic(&device_id, char_uuid)
            .await
            .unwrap();
        let mut events = central.events();

        peripheral_backend.drop_next_notification();
        peripheral
            .notify_characteristic(
                &DeviceId::from("mock-central"),
                char_uuid,
                b"dropped".to_vec(),
            )
            .await
            .unwrap();
        peripheral
            .notify_characteristic(
                &DeviceId::from("mock-central"),
                char_uuid,
                b"delivered".to_vec(),
            )
            .await
            .unwrap();

        let delivered = loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), events.next())
                .await
                .expect("timed out")
                .expect("stream closed")
            {
                CentralEvent::CharacteristicNotification { value, .. } => break value,
                _ => {}
            }
        };
        assert_eq!(&delivered[..], b"delivered");
    }

    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum OpKind {
        Read,
        Write,
        Subscribe,
    }

    impl OpKind {
        fn idx(self) -> usize {
            match self {
                Self::Read => 0,
                Self::Write => 1,
                Self::Subscribe => 2,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FaultCmd {
        Arm(OpKind, MockErrorKind),
        Do(OpKind),
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        NotSupported,
        Internal,
        Other,
    }

    impl From<BlewResult<()>> for Outcome {
        fn from(r: BlewResult<()>) -> Self {
            match r {
                Ok(()) => Self::Ok,
                Err(BlewError::NotSupported) => Self::NotSupported,
                Err(BlewError::Internal(_)) => Self::Internal,
                Err(_) => Self::Other,
            }
        }
    }

    fn op_kind_strategy() -> impl Strategy<Value = OpKind> {
        prop_oneof![
            Just(OpKind::Read),
            Just(OpKind::Write),
            Just(OpKind::Subscribe),
        ]
    }

    fn mock_error_kind_strategy() -> impl Strategy<Value = MockErrorKind> {
        prop_oneof![
            Just(MockErrorKind::NotSupported),
            Just(MockErrorKind::Internal),
        ]
    }

    fn fault_cmd_strategy() -> impl Strategy<Value = FaultCmd> {
        prop_oneof![
            (op_kind_strategy(), mock_error_kind_strategy()).prop_map(|(k, e)| FaultCmd::Arm(k, e)),
            op_kind_strategy().prop_map(FaultCmd::Do),
        ]
    }

    fn expected_outcome(slot: Option<MockErrorKind>) -> Outcome {
        match slot {
            None => Outcome::Ok,
            Some(MockErrorKind::NotSupported) => Outcome::NotSupported,
            Some(MockErrorKind::Internal) => Outcome::Internal,
        }
    }

    proptest! {
        /// Each fault slot (`next_read_error`, `next_write_error`,
        /// `next_subscribe_error`) behaves as an independent `Option<MockErrorKind>`:
        /// repeated arms overwrite (last-write-wins), the next matching op consumes
        /// it, and arms on different op kinds never interfere with each other.
        #[test]
        fn fault_injection_matches_three_slot_model(
            cmds in prop_vec(fault_cmd_strategy(), 0..64)
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let trace: Vec<(OpKind, Outcome, Outcome)> = rt.block_on(async move {
                let char_uuid = Uuid::from_u128(0xF012);
                let (central, _peripheral, device_id, central_backend, _p) =
                    connected_pair_with_char(char_uuid).await;

                let mut model: [Option<MockErrorKind>; 3] = [None, None, None];
                let mut trace = Vec::new();

                for cmd in cmds {
                    match cmd {
                        FaultCmd::Arm(k, e) => {
                            match k {
                                OpKind::Read => central_backend.inject_next_read_error(e),
                                OpKind::Write => central_backend.inject_next_write_error(e),
                                OpKind::Subscribe => central_backend.inject_next_subscribe_error(e),
                            }
                            model[k.idx()] = Some(e);
                        }
                        FaultCmd::Do(k) => {
                            let expected = expected_outcome(model[k.idx()].take());
                            let result: BlewResult<()> = match k {
                                OpKind::Read => central
                                    .read_characteristic(&device_id, char_uuid)
                                    .await
                                    .map(|_| ()),
                                OpKind::Write => central
                                    .write_characteristic(
                                        &device_id,
                                        char_uuid,
                                        b"x".to_vec(),
                                        WriteType::WithoutResponse,
                                    )
                                    .await,
                                OpKind::Subscribe => {
                                    let r = central
                                        .subscribe_characteristic(&device_id, char_uuid)
                                        .await;
                                    if r.is_ok() {
                                        central
                                            .unsubscribe_characteristic(&device_id, char_uuid)
                                            .await
                                            .expect("unsubscribe is infallible");
                                    }
                                    r
                                }
                            };
                            trace.push((k, expected, Outcome::from(result)));
                        }
                    }
                }
                trace
            });

            for (k, expected, got) in trace {
                prop_assert_eq!(
                    &expected, &got,
                    "op {:?}: expected {:?}, got {:?}", k, expected, got
                );
            }
        }
    }
}
