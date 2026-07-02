bitflags::bitflags! {
    /// GATT characteristic properties, matching the BLE spec bit layout.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct CharacteristicProperties: u16 {
        const BROADCAST                   = 0x0001;
        const READ                        = 0x0002;
        const WRITE_WITHOUT_RESPONSE      = 0x0004;
        const WRITE                       = 0x0008;
        const NOTIFY                      = 0x0010;
        const INDICATE                    = 0x0020;
        const AUTHENTICATED_SIGNED_WRITES = 0x0040;
        const EXTENDED_PROPERTIES        = 0x0080;
    }
}

bitflags::bitflags! {
    /// ATT attribute permissions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct AttributePermissions: u16 {
        const READ               = 0x0001;
        const WRITE              = 0x0002;
        const READ_ENCRYPTED     = 0x0004;
        const WRITE_ENCRYPTED    = 0x0008;
        const READ_AUTHENTICATED = 0x0010;
        const WRITE_AUTHENTICATED = 0x0020;
    }
}
