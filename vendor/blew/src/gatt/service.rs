use super::props::{AttributePermissions, CharacteristicProperties};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct GattDescriptor {
    pub uuid: Uuid,
    pub value: Vec<u8>,
}

/// A GATT characteristic.
///
/// # Platform caveats
///
/// **Apple:** If [`value`](Self::value) is non-empty **and** [`properties`](Self::properties)
/// includes [`CharacteristicProperties::WRITE`], CoreBluetooth treats the characteristic as
/// static and raises `NSInvalidArgumentException` → `SIGABRT` when a central writes to it.
/// For any writable characteristic set `value: vec![]` and handle reads via
/// [`PeripheralRequest::Read`](crate::peripheral::PeripheralRequest::Read).
#[derive(Debug, Clone)]
pub struct GattCharacteristic {
    pub uuid: Uuid,
    pub properties: CharacteristicProperties,
    pub permissions: AttributePermissions,
    /// Static value for server-side characteristics; empty for client-discovered ones.
    pub value: Vec<u8>,
    pub descriptors: Vec<GattDescriptor>,
}

#[derive(Debug, Clone)]
pub struct GattService {
    pub uuid: Uuid,
    pub primary: bool,
    pub characteristics: Vec<GattCharacteristic>,
}
