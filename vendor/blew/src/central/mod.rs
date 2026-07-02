pub mod backend;
pub mod types;

pub use types::{CentralConfig, CentralEvent, DisconnectCause, ScanFilter, ScanMode, WriteType};

use crate::error::{BlewError, BlewResult};
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::platform::PlatformCentral;
use crate::types::{BleDevice, DeviceId};
use crate::util::event_stream::EventStream;
use backend::CentralBackend;
use uuid::Uuid;

/// BLE Central (a client in bluetooth-speak): scanner, connector, and GATT/L2CAP client.
///
/// The type parameter `B` selects the platform backend and defaults to the correct backend for
/// the current build target.
///
/// ```rust
/// # async fn example() -> blew::error::BlewResult<()> {
/// use blew::central::{Central, ScanFilter};
///
/// let central: Central = Central::new().await?;
/// central.start_scan(ScanFilter::default()).await?;
/// # Ok(())
/// # }
pub struct Central<B: CentralBackend = PlatformCentral> {
    pub(crate) backend: B,
}

impl Central {
    /// Initialise the central role with the given configuration.
    ///
    /// On Apple platforms this wires `CBCentralManagerOptionRestoreIdentifierKey` when
    /// `config.restore_identifier` is `Some`. On all other platforms the config is ignored.
    ///
    /// When `restore_identifier` is set, this must be called synchronously from
    /// `application:didFinishLaunchingWithOptions:` with the same identifier as the
    /// previous launch. Immediately after it returns, call [`Self::take_restored`] to
    /// recover any preserved peripherals — issuing new scans or connects ahead of that
    /// call will diverge from what the OS expects.
    ///
    /// See the crate-level [`State restoration`](crate#state-restoration) docs for the
    /// full iOS contract (entitlements, L2CAP caveats, background runtime constraints).
    pub async fn with_config(config: CentralConfig) -> BlewResult<Self> {
        let backend = PlatformCentral::with_config(config).await?;
        Ok(Self { backend })
    }

    /// Clear the Android GATT service cache for `device_id`.
    ///
    /// This calls the hidden `BluetoothGatt.refresh()` Android API via
    /// reflection, forcing the system to re-read the GATT service database
    /// from the peer on the next `discoverServices()` call. This is the
    /// standard workaround for GATT status 133 errors caused by stale
    /// cached service tables after a peer reboot.
    ///
    /// Returns [`BlewError::NotSupported`] on non-Android platforms or if
    /// the reflective call fails.
    #[cfg(target_os = "android")]
    pub async fn refresh(&self, device_id: &DeviceId) -> BlewResult<()> {
        self.backend.refresh(device_id).await
    }
}

impl<B: CentralBackend> Central<B> {
    /// Initialise the central role, acquiring the platform BLE adapter.
    pub async fn new() -> BlewResult<Self> {
        Ok(Self {
            backend: B::new().await?,
        })
    }

    /// Returns `true` if the local Bluetooth adapter is powered on.
    pub async fn is_powered(&self) -> BlewResult<bool> {
        self.backend.is_powered().await
    }

    /// Start scanning for peripherals matching `filter`.
    /// Discovered devices are emitted on the stream returned by [`events`](Self::events).
    pub async fn start_scan(&self, filter: ScanFilter) -> BlewResult<()> {
        self.backend.start_scan(filter).await
    }

    /// Stop an active scan.
    pub async fn stop_scan(&self) -> BlewResult<()> {
        self.backend.stop_scan().await
    }

    /// Snapshot of all peripherals seen since the last [`start_scan`](Self::start_scan).
    pub async fn discovered_devices(&self) -> BlewResult<Vec<BleDevice>> {
        self.backend.discovered_devices().await
    }

    /// Initiate a connection to a discovered peripheral.
    pub async fn connect(&self, device_id: &DeviceId) -> BlewResult<()> {
        self.backend.connect(device_id).await
    }

    /// Disconnect from a connected peripheral.
    pub async fn disconnect(&self, device_id: &DeviceId) -> BlewResult<()> {
        self.backend.disconnect(device_id).await
    }

    /// Enumerate GATT services and characteristics on a connected peripheral.
    pub async fn discover_services(&self, device_id: &DeviceId) -> BlewResult<Vec<GattService>> {
        self.backend.discover_services(device_id).await
    }

    /// Read the current value of a characteristic.
    pub async fn read_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> BlewResult<Vec<u8>> {
        self.backend.read_characteristic(device_id, char_uuid).await
    }

    /// Write a value to a characteristic.
    pub async fn write_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> BlewResult<()> {
        self.backend
            .write_characteristic(device_id, char_uuid, value, write_type)
            .await
    }

    /// Enable notifications/indications for a characteristic.
    pub async fn subscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> BlewResult<()> {
        self.backend
            .subscribe_characteristic(device_id, char_uuid)
            .await
    }

    /// Disable notifications/indications for a characteristic.
    pub async fn unsubscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> BlewResult<()> {
        self.backend
            .unsubscribe_characteristic(device_id, char_uuid)
            .await
    }

    /// Negotiated ATT MTU for a connected device. Returns 23 (ATT minimum) if unknown.
    pub async fn mtu(&self, device_id: &DeviceId) -> u16 {
        self.backend.mtu(device_id).await
    }

    /// Open an L2CAP CoC channel to a connected peripheral.
    pub async fn open_l2cap_channel(
        &self,
        device_id: &DeviceId,
        psm: Psm,
    ) -> BlewResult<L2capChannel> {
        self.backend.open_l2cap_channel(device_id, psm).await
    }

    /// Subscribe to central-role events. Each call returns an independent stream.
    pub fn events(&self) -> EventStream<CentralEvent, B::EventStream> {
        EventStream::new(self.backend.events())
    }

    /// Clear the Android GATT service cache for `device_id`.
    ///
    /// Android-only. On other platforms this returns
    /// [`BlewError::NotSupported`].
    #[cfg(not(target_os = "android"))]
    #[allow(clippy::unused_async)]
    pub async fn refresh(&self, _device_id: &DeviceId) -> BlewResult<()> {
        Err(BlewError::NotSupported)
    }

    /// Wait until the adapter is powered on, or return `BlewError::Timeout`.
    pub async fn wait_ready(&self, timeout: std::time::Duration) -> BlewResult<()> {
        let mut events = self.events();
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
                Ok(Some(CentralEvent::AdapterStateChanged { powered: true })) => return Ok(()),
                Ok(Some(_)) => {}
            }
        }
    }
}

#[cfg(target_vendor = "apple")]
impl Central {
    /// Consume the OS-level state-restoration payload, if any.
    ///
    /// On iOS, when [`Central::with_config`] was called with a `restore_identifier` and the
    /// OS is relaunching the app, the `CBCentralManager` delegate's `willRestoreState:`
    /// callback fires during initialisation. `with_config` captures that payload and
    /// buffers the preserved peripherals here; this method hands them to the caller
    /// exactly once.
    ///
    /// Returns:
    /// - `Some(devices)` — this launch is an OS-level restoration and `devices` lists
    ///   the peripherals the OS reconnected on the app's behalf. Your app should
    ///   rehydrate its own state from these before issuing new scans/connects.
    /// - `None` — not a restoration launch, or the restored state has already been taken.
    ///
    /// See the crate-level [`State restoration`](crate#state-restoration) docs for why
    /// this is a `take_*` style API (the event fires before subscribers can attach).
    #[must_use]
    pub fn take_restored(&self) -> Option<Vec<BleDevice>> {
        self.backend.take_restored()
    }
}
