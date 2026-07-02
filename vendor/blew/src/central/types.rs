use crate::types::{BleDevice, DeviceId};
use bytes::Bytes;
use uuid::Uuid;

/// Configuration for initialising the central role.
#[derive(Debug, Clone, Default)]
pub struct CentralConfig {
    /// On Apple platforms, passed as `CBCentralManagerOptionRestoreIdentifierKey` to
    /// `initWithDelegate:queue:options:`, enabling state restoration for the app's
    /// background BLE central session. Ignored on all other platforms.
    pub restore_identifier: Option<String>,
}

/// Reason a peripheral disconnected. Used by [`CentralEvent::DeviceDisconnected`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectCause {
    /// We called `disconnect()` locally.
    LocalClose,
    /// Peer sent LL_TERMINATE.
    RemoteClose,
    /// Supervision timeout (link loss).
    LinkLoss,
    /// Bluetooth adapter powered off.
    AdapterOff,
    /// Android `onConnectionStateChange` status 133.
    Gatt133,
    /// Connection setup timed out.
    Timeout,
    /// Platform-specific code we don't map to any category above.
    Unknown(i32),
}

/// Events emitted by the central (scanner/client) role.
///
/// `CentralEvent` is `Clone`; notification payloads use [`Bytes`] to avoid copies
/// when fanning out to multiple subscribers.
#[derive(Debug, Clone)]
pub enum CentralEvent {
    /// The local Bluetooth adapter was powered on or off.
    AdapterStateChanged { powered: bool },
    /// A new peripheral was discovered or its advertising data was updated.
    DeviceDiscovered(BleDevice),
    /// A connection to a peripheral was established.
    DeviceConnected { device_id: DeviceId },
    /// A peripheral disconnected.
    DeviceDisconnected {
        device_id: DeviceId,
        cause: DisconnectCause,
    },
    /// A subscribed characteristic sent a notification or indication.
    CharacteristicNotification {
        device_id: DeviceId,
        char_uuid: Uuid,
        value: Bytes,
    },
}

/// Scan duty cycle / power trade-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanMode {
    /// Highest duty cycle -- discovers devices fastest but uses the most power
    /// and radio bandwidth.
    #[default]
    LowLatency,
    /// Reduced duty cycle -- suitable for background scanning while active
    /// connections are in progress.
    LowPower,
}

/// Filter applied during scanning.
///
/// # Platform caveats
///
/// **Linux/BlueZ:** BlueZ's device cache may emit `DeviceAdded` events for devices that do
/// not match [`services`](Self::services) on the first tick after a fresh scan start. If you
/// need strict filtering, re-check against
/// [`CentralEvent::DeviceDiscovered`] on the client
/// side rather than relying on the filter alone.
#[derive(Debug, Clone, Default)]
pub struct ScanFilter {
    /// Only report peripherals advertising at least one of these service UUIDs.
    /// Empty means report all.
    pub services: Vec<Uuid>,
    pub mode: ScanMode,
}

/// How a characteristic write is delivered to the remote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteType {
    /// ATT Write Request -- the peripheral sends an acknowledgement.
    WithResponse,
    /// ATT Write Command -- no acknowledgement; lower latency.
    WithoutResponse,
}
