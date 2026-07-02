use uuid::Uuid;

/// Platform-specific device identifier.
/// On Apple platforms this is a UUID string; on Linux/Android it is a hex MAC address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceId(pub(crate) String);

impl DeviceId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for DeviceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for DeviceId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// A discovered BLE device snapshot.
#[derive(Debug, Clone)]
pub struct BleDevice {
    pub id: DeviceId,
    pub name: Option<String>,
    pub rssi: Option<i16>,
    pub services: Vec<Uuid>,
    /// Manufacturer-specific data from the advertisement, as `(company_id,
    /// bytes)`. `None` when the advert carried none (or the platform cannot
    /// surface it). Neutrino carries `node_id ‖ display_name` here under its
    /// company id for peer discovery; the value is opaque to blew.
    pub manufacturer_data: Option<(u16, Vec<u8>)>,
}
