//! Abstraction over `blew::Central` + `blew::Peripheral` so the registry
//! can be tested against a mock.

use async_trait::async_trait;
use blew::{DeviceId, L2capChannel};
use bytes::Bytes;

use crate::error::BleResult;
use crate::transport::peer::ChannelHandle;

#[async_trait]
pub trait BleInterface: Send + Sync + 'static {
    async fn connect(&self, device_id: &DeviceId) -> BleResult<ChannelHandle>;
    async fn disconnect(&self, device_id: &DeviceId) -> BleResult<()>;
    async fn write_c2p(&self, device_id: &DeviceId, bytes: Bytes) -> BleResult<()>;
    async fn notify_p2c(&self, device_id: &DeviceId, bytes: Bytes) -> BleResult<()>;
    async fn read_psm(&self, device_id: &DeviceId) -> BleResult<Option<u16>>;
    /// Read the peer's one-byte VERSION characteristic. Returns `Ok(None)`
    /// if the peer does not publish VERSION (older build or characteristic
    /// absent); callers treat that as "skip the check".
    async fn read_version(&self, device_id: &DeviceId) -> BleResult<Option<u8>>;
    async fn open_l2cap(&self, device_id: &DeviceId, psm: u16) -> BleResult<L2capChannel>;
    async fn start_scan(&self) -> BleResult<()>;
    async fn stop_scan(&self) -> BleResult<()>;
    async fn rebuild_server(&self) -> BleResult<()>;
    async fn restart_advertising(&self) -> BleResult<()>;
    async fn restart_l2cap_listener(&self) -> BleResult<Option<u16>>;
    async fn is_powered(&self) -> bool;
    async fn refresh(&self, device_id: &DeviceId) -> BleResult<()>;
    /// Negotiated ATT MTU for a connected peer, as reported by the platform.
    /// Returns the platform's best current reading; on real hardware this is
    /// typically 23 (BLE spec default) before any MTU exchange has completed.
    /// Callers use `resolve_chunk_size` to interpret the value and apply a
    /// minimum floor.
    async fn mtu(&self, device_id: &DeviceId) -> u16;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dummy;

    #[async_trait]
    impl BleInterface for Dummy {
        async fn connect(&self, _: &DeviceId) -> BleResult<ChannelHandle> {
            Ok(ChannelHandle {
                id: 1,
                path: crate::transport::peer::ConnectPath::Gatt,
            })
        }
        async fn disconnect(&self, _: &DeviceId) -> BleResult<()> {
            Ok(())
        }
        async fn write_c2p(&self, _: &DeviceId, _: Bytes) -> BleResult<()> {
            Ok(())
        }
        async fn notify_p2c(&self, _: &DeviceId, _: Bytes) -> BleResult<()> {
            Ok(())
        }
        async fn read_psm(&self, _: &DeviceId) -> BleResult<Option<u16>> {
            Ok(None)
        }
        async fn read_version(&self, _: &DeviceId) -> BleResult<Option<u8>> {
            Ok(None)
        }
        async fn open_l2cap(&self, _: &DeviceId, _: u16) -> BleResult<L2capChannel> {
            unimplemented!()
        }
        async fn start_scan(&self) -> BleResult<()> {
            Ok(())
        }
        async fn stop_scan(&self) -> BleResult<()> {
            Ok(())
        }
        async fn rebuild_server(&self) -> BleResult<()> {
            Ok(())
        }
        async fn restart_advertising(&self) -> BleResult<()> {
            Ok(())
        }
        async fn restart_l2cap_listener(&self) -> BleResult<Option<u16>> {
            Ok(None)
        }
        async fn is_powered(&self) -> bool {
            true
        }
        async fn refresh(&self, _: &DeviceId) -> BleResult<()> {
            Ok(())
        }
        async fn mtu(&self, _: &DeviceId) -> u16 {
            23
        }
    }

    #[tokio::test]
    async fn trait_object_is_usable() {
        let iface: Box<dyn BleInterface> = Box::new(Dummy);
        let device_id = DeviceId::from("x");
        iface.connect(&device_id).await.unwrap();
    }
}
