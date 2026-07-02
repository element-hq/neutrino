use crate::types::DeviceId;
use std::error::Error;
use uuid::Uuid;

pub type BlewResult<T> = Result<T, BlewError>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlewError {
    #[error("Bluetooth adapter not found")]
    AdapterNotFound,

    #[error("not supported on this platform or not yet implemented")]
    NotSupported,

    #[error("Bluetooth adapter is not powered on")]
    NotPowered,

    #[error("required Android BLE permissions are not granted")]
    PermissionDenied,

    #[error("device not found: {0}")]
    DeviceNotFound(DeviceId),

    #[error("not connected to device: {0}")]
    NotConnected(DeviceId),

    #[error("characteristic {char_uuid} not found on device {device_id}")]
    CharacteristicNotFound {
        device_id: DeviceId,
        char_uuid: Uuid,
    },

    #[error("local characteristic {char_uuid} not found")]
    LocalCharacteristicNotFound { char_uuid: Uuid },

    #[error("already advertising")]
    AlreadyAdvertising,

    #[error("operation timed out")]
    Timeout,

    #[error("GATT busy on device {0} (another operation in flight)")]
    GattBusy(DeviceId),

    #[error("value too large for current MTU: got {got} bytes, max {max}")]
    ValueTooLarge { got: usize, max: usize },

    #[error("already subscribed to characteristic {char_uuid} on {device_id}")]
    AlreadySubscribed {
        device_id: DeviceId,
        char_uuid: Uuid,
    },

    #[error("event stream closed unexpectedly")]
    StreamClosed,

    #[error("device {0} disconnected during operation")]
    DisconnectedDuringOperation(DeviceId),

    #[error("GATT discovery failed on {device_id}: {reason}")]
    DiscoveryFailed { device_id: DeviceId, reason: String },

    #[error("GATT error on device {device_id}: {source}")]
    Gatt {
        device_id: DeviceId,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    #[error("peripheral error: {source}")]
    Peripheral {
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    #[error("central error: {source}")]
    Central {
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    #[error("L2CAP error: {source}")]
    L2cap {
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    #[error("internal error: {0}")]
    Internal(String),
}
