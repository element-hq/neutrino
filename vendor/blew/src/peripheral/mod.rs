pub mod backend;
pub mod types;

pub use types::{
    AdvertisingConfig, PeripheralConfig, PeripheralRequest, PeripheralStateEvent, ReadResponder,
    WriteResponder,
};

use crate::error::{BlewError, BlewResult};
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::platform::PlatformPeripheral;
use crate::types::DeviceId;
use crate::util::event_stream::EventStream;
use backend::PeripheralBackend;
use uuid::Uuid;

impl Peripheral {
    /// Initialise the peripheral role with the given configuration.
    ///
    /// On Apple platforms this wires `CBPeripheralManagerOptionRestoreIdentifierKey` when
    /// `config.restore_identifier` is `Some`. On all other platforms the config is ignored.
    ///
    /// When `restore_identifier` is set, this must be called synchronously from
    /// `application:didFinishLaunchingWithOptions:` with the same identifier as the
    /// previous launch. Immediately after it returns, call [`Self::take_restored`] to
    /// recover any preserved service UUIDs — the OS re-registers those services on the
    /// manager for you, so racing `add_service` against the restored list can produce
    /// duplicate-service errors.
    ///
    /// See the crate-level [`State restoration`](crate#state-restoration) docs for the
    /// full iOS contract (entitlements, L2CAP re-open requirement, background runtime
    /// constraints).
    pub async fn with_config(config: PeripheralConfig) -> BlewResult<Self> {
        let backend = PlatformPeripheral::with_config(config).await?;
        Ok(Self { backend })
    }
}

/// BLE peripheral (server in bluetooth-speak) role: advertiser, GATT server, L2CAP listener.
///
/// The type parameter `B` selects the platform backend and defaults to the correct backend for
/// the current build target.
///
/// ```rust
/// # async fn example() -> blew::error::BlewResult<()> {
/// use blew::peripheral::Peripheral;
///
/// let peripheral: Peripheral = Peripheral::new().await?;
/// # Ok(())
/// # }
/// ```
pub struct Peripheral<B: PeripheralBackend = PlatformPeripheral> {
    pub(crate) backend: B,
}

impl<B: PeripheralBackend> Peripheral<B> {
    /// Initialise the peripheral role, acquiring the platform BLE adapter.
    pub async fn new() -> BlewResult<Self> {
        Ok(Self {
            backend: B::new().await?,
        })
    }

    /// Returns `true` if the local Bluetooth adapter is powered on.
    pub async fn is_powered(&self) -> BlewResult<bool> {
        self.backend.is_powered().await
    }

    /// Register a GATT service. Must be called before [`start_advertising`](Self::start_advertising).
    pub async fn add_service(&self, service: &GattService) -> BlewResult<()> {
        self.backend.add_service(service).await
    }

    /// Begin advertising.
    pub async fn start_advertising(&self, config: &AdvertisingConfig) -> BlewResult<()> {
        self.backend.start_advertising(config).await
    }

    /// Stop advertising.
    pub async fn stop_advertising(&self) -> BlewResult<()> {
        self.backend.stop_advertising().await
    }

    /// Push a characteristic value update to a single subscribed central.
    ///
    /// See [`PeripheralBackend::notify_characteristic`] for the per-device
    /// semantics and the Linux-only broadcast fallback.
    pub async fn notify_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> BlewResult<()> {
        self.backend
            .notify_characteristic(device_id, char_uuid, value)
            .await
    }

    /// Publish an L2CAP CoC channel.  Returns the OS-assigned PSM and a stream
    /// of incoming [`L2capChannel`] connections.
    pub async fn l2cap_listener(
        &self,
    ) -> BlewResult<(
        Psm,
        impl futures_core::Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
    )> {
        self.backend.l2cap_listener().await
    }

    /// Subscribe to clone-able peripheral state events (adapter power, subscription changes).
    /// Each call returns an independent stream; fan-out is safe.
    pub fn state_events(&self) -> EventStream<PeripheralStateEvent, B::StateEvents> {
        EventStream::new(self.backend.state_events())
    }

    /// Take ownership of the inbound GATT request stream. Returns `None` on the second call.
    ///
    /// [`PeripheralRequest`] carries RAII [`ReadResponder`]/[`WriteResponder`] handles and
    /// must be consumed by exactly one reader; this method enforces that at the type level.
    pub fn take_requests(&self) -> Option<EventStream<PeripheralRequest, B::Requests>> {
        self.backend.take_requests().map(EventStream::new)
    }

    /// Wait until the adapter is powered on, or return `BlewError::Timeout`.
    pub async fn wait_ready(&self, timeout: std::time::Duration) -> BlewResult<()> {
        let mut events = self.state_events();
        if self.backend.is_powered().await.unwrap_or(false) {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(BlewError::Timeout);
            }
            match tokio::time::timeout(remaining, tokio_stream::StreamExt::next(&mut events)).await
            {
                Err(_) => return Err(BlewError::Timeout),
                Ok(None) => return Err(BlewError::StreamClosed),
                Ok(Some(PeripheralStateEvent::AdapterStateChanged { powered: true })) => {
                    return Ok(());
                }
                Ok(Some(_)) => {}
            }
        }
    }
}

#[cfg(target_vendor = "apple")]
impl Peripheral {
    /// Consume the OS-level state-restoration payload, if any.
    ///
    /// On iOS, when [`Peripheral::with_config`] was called with a `restore_identifier` and
    /// the OS is relaunching the app, the `CBPeripheralManager` delegate's
    /// `willRestoreState:` callback fires during initialisation. `with_config` captures
    /// that payload and buffers the preserved service UUIDs here; this method hands
    /// them to the caller exactly once.
    ///
    /// Returns:
    /// - `Some(uuids)` — this launch is an OS-level restoration and `uuids` lists the
    ///   services the OS re-registered on the manager. The app does **not** need to
    ///   re-call [`add_service`](Self::add_service) for these. If advertising was active
    ///   at termination it resumes automatically.
    /// - `None` — not a restoration launch, or the restored state has already been taken.
    ///
    /// **L2CAP channels are never restored.** If the previous session had one open, the
    /// peripheral must re-publish via [`l2cap_listener`](Self::l2cap_listener).
    ///
    /// See the crate-level [`State restoration`](crate#state-restoration) docs for why
    /// this is a `take_*` style API (the event fires before subscribers can attach).
    #[must_use]
    pub fn take_restored(&self) -> Option<Vec<Uuid>> {
        self.backend.take_restored()
    }
}

#[cfg(all(test, feature = "testing"))]
mod take_requests_tests {
    #[tokio::test]
    async fn second_take_returns_none() {
        let p = crate::testing::MockPeripheral::new_powered();
        assert!(p.take_requests().is_some());
        assert!(p.take_requests().is_none());
    }
}
