//! Test utilities: `MockBleInterface` for exercising the registry without real BLE hardware.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use blew::{DeviceId, L2capChannel};
use bytes::Bytes;

use crate::error::BleResult;
use crate::transport::interface::BleInterface;
use crate::transport::peer::{ChannelHandle, ConnectPath};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallKind {
    Connect(DeviceId),
    Disconnect(DeviceId),
    WriteC2p { device_id: DeviceId, bytes: Bytes },
    NotifyP2c { device_id: DeviceId, bytes: Bytes },
    ReadPsm(DeviceId),
    ReadVersion(DeviceId),
    OpenL2cap(DeviceId, u16),
    StartScan,
    StopScan,
    RebuildServer,
    RestartAdvertising,
    RestartL2capListener,
    IsPowered,
    Refresh(DeviceId),
}

struct Inner {
    calls: Vec<CallKind>,
    /// Per-device FIFO queue of pre-seeded responses; the linear scan + `remove(pos)` in `connect` pops the first matching entry.
    connect_queue: VecDeque<(DeviceId, BleResult<ChannelHandle>)>,
    /// Per-device FIFO queue of pre-seeded L2CAP channels; see `connect_queue` for invariant.
    open_l2cap_queue: VecDeque<(DeviceId, u16, BleResult<L2capChannel>)>,
    disconnect_queue: VecDeque<(DeviceId, BleResult<()>)>,
    write_c2p_queue: VecDeque<(DeviceId, BleResult<()>)>,
    notify_p2c_queue: VecDeque<(DeviceId, BleResult<()>)>,
    is_powered: bool,
    connect_delay: Option<Duration>,
    next_channel_id: u64,
    on_c2p_write: Option<Box<dyn Fn(DeviceId, Bytes) + Send + Sync>>,
    on_p2c_notify: Option<Box<dyn Fn(DeviceId, Bytes) + Send + Sync>>,
    psm_responses: VecDeque<Option<u16>>,
    version_responses: VecDeque<Option<u8>>,
    mtu_responses: VecDeque<u16>,
    mtu_default: u16,
}

/// Test-only mock implementing `BleInterface`.
///
/// **Default policies:**
/// - `connect` without a queued response returns `Ok` with a monotonically increasing `ChannelHandle::id`.
/// - `open_l2cap` without a queued response returns `Err(BleError::Unsupported)`, reflecting that L2CAP is an optional capability.
/// - All other methods default to `Ok(())` / `Ok(None)` / `is_powered = true`.
#[derive(Clone)]
pub struct MockBleInterface {
    inner: Arc<Mutex<Inner>>,
}

impl Default for MockBleInterface {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBleInterface {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                calls: Vec::new(),
                connect_queue: VecDeque::new(),
                open_l2cap_queue: VecDeque::new(),
                disconnect_queue: VecDeque::new(),
                write_c2p_queue: VecDeque::new(),
                notify_p2c_queue: VecDeque::new(),
                is_powered: true,
                connect_delay: None,
                next_channel_id: 1,
                on_c2p_write: None,
                on_p2c_notify: None,
                psm_responses: VecDeque::new(),
                version_responses: VecDeque::new(),
                mtu_responses: VecDeque::new(),
                mtu_default: 512,
            })),
        }
    }

    pub fn calls(&self) -> Vec<CallKind> {
        self.inner.lock().unwrap().calls.clone()
    }

    pub fn on_connect(&self, device_id: DeviceId, result: BleResult<ChannelHandle>) {
        self.inner
            .lock()
            .unwrap()
            .connect_queue
            .push_back((device_id, result));
    }

    pub fn on_disconnect(&self, device_id: DeviceId, result: BleResult<()>) {
        self.inner
            .lock()
            .unwrap()
            .disconnect_queue
            .push_back((device_id, result));
    }

    pub fn on_write_c2p(&self, device_id: DeviceId, result: BleResult<()>) {
        self.inner
            .lock()
            .unwrap()
            .write_c2p_queue
            .push_back((device_id, result));
    }

    pub fn on_notify_p2c(&self, device_id: DeviceId, result: BleResult<()>) {
        self.inner
            .lock()
            .unwrap()
            .notify_p2c_queue
            .push_back((device_id, result));
    }

    pub fn on_open_l2cap(&self, device_id: DeviceId, psm: u16, result: BleResult<L2capChannel>) {
        self.inner
            .lock()
            .unwrap()
            .open_l2cap_queue
            .push_back((device_id, psm, result));
    }

    pub fn set_powered(&self, powered: bool) {
        self.inner.lock().unwrap().is_powered = powered;
    }

    pub fn set_connect_delay(&self, delay: Duration) {
        self.inner.lock().unwrap().connect_delay = Some(delay);
    }

    pub fn set_on_c2p_write(&self, hook: Box<dyn Fn(DeviceId, Bytes) + Send + Sync>) {
        self.inner.lock().unwrap().on_c2p_write = Some(hook);
    }

    pub fn set_on_p2c_notify(&self, hook: Box<dyn Fn(DeviceId, Bytes) + Send + Sync>) {
        self.inner.lock().unwrap().on_p2c_notify = Some(hook);
    }

    /// Push MTU values to be returned on successive `mtu()` calls. When the
    /// queue is empty, subsequent calls return `mtu_default`.
    pub fn push_mtu(&self, value: u16) {
        self.inner.lock().unwrap().mtu_responses.push_back(value);
    }

    /// Fallback returned by `mtu()` when no queued responses remain.
    pub fn set_mtu_default(&self, value: u16) {
        self.inner.lock().unwrap().mtu_default = value;
    }

    pub fn seed_psm(&self, psm: Option<u16>) {
        self.inner.lock().unwrap().psm_responses.push_back(psm);
    }

    /// Queue one response for the next `read_version()` call. `None` means
    /// "peer does not publish VERSION"; `Some(v)` is the byte the central
    /// will observe.
    pub fn seed_version(&self, version: Option<u8>) {
        self.inner
            .lock()
            .unwrap()
            .version_responses
            .push_back(version);
    }

    pub fn assert_called(&self, kind: &CallKind) {
        let calls = self.calls();
        assert!(
            calls.contains(kind),
            "expected call {kind:?} but got: {calls:?}"
        );
    }
}

#[async_trait]
impl BleInterface for MockBleInterface {
    async fn connect(&self, device_id: &DeviceId) -> BleResult<ChannelHandle> {
        let (delay, result) = {
            let mut inner = self.inner.lock().unwrap();
            inner.calls.push(CallKind::Connect(device_id.clone()));
            let delay = inner.connect_delay;
            let result = inner
                .connect_queue
                .iter()
                .position(|(id, _)| id == device_id)
                .map(|pos| inner.connect_queue.remove(pos).unwrap().1)
                .unwrap_or_else(|| {
                    let id = inner.next_channel_id;
                    inner.next_channel_id += 1;
                    Ok(ChannelHandle {
                        id,
                        path: ConnectPath::Gatt,
                    })
                });
            (delay, result)
        };
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        result
    }

    async fn disconnect(&self, device_id: &DeviceId) -> BleResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.calls.push(CallKind::Disconnect(device_id.clone()));
        inner
            .disconnect_queue
            .iter()
            .position(|(id, _)| id == device_id)
            .map(|pos| inner.disconnect_queue.remove(pos).unwrap().1)
            .unwrap_or(Ok(()))
    }

    async fn write_c2p(&self, device_id: &DeviceId, bytes: Bytes) -> BleResult<()> {
        let seeded = {
            let mut inner = self.inner.lock().unwrap();
            inner.calls.push(CallKind::WriteC2p {
                device_id: device_id.clone(),
                bytes: bytes.clone(),
            });
            inner
                .write_c2p_queue
                .iter()
                .position(|(id, _)| id == device_id)
                .map(|pos| inner.write_c2p_queue.remove(pos).unwrap().1)
        };
        if let Some(result) = seeded {
            return result;
        }
        let hook = self.inner.lock().unwrap().on_c2p_write.take();
        if let Some(h) = hook.as_ref() {
            h(device_id.clone(), bytes);
        }
        if let Some(h) = hook {
            self.inner.lock().unwrap().on_c2p_write = Some(h);
        }
        Ok(())
    }

    async fn notify_p2c(&self, device_id: &DeviceId, bytes: Bytes) -> BleResult<()> {
        let seeded = {
            let mut inner = self.inner.lock().unwrap();
            inner.calls.push(CallKind::NotifyP2c {
                device_id: device_id.clone(),
                bytes: bytes.clone(),
            });
            inner
                .notify_p2c_queue
                .iter()
                .position(|(id, _)| id == device_id)
                .map(|pos| inner.notify_p2c_queue.remove(pos).unwrap().1)
        };
        if let Some(result) = seeded {
            return result;
        }
        let hook = self.inner.lock().unwrap().on_p2c_notify.take();
        if let Some(h) = hook.as_ref() {
            h(device_id.clone(), bytes);
        }
        if let Some(h) = hook {
            self.inner.lock().unwrap().on_p2c_notify = Some(h);
        }
        Ok(())
    }

    async fn read_version(&self, device_id: &DeviceId) -> BleResult<Option<u8>> {
        let mut inner = self.inner.lock().unwrap();
        inner.calls.push(CallKind::ReadVersion(device_id.clone()));
        Ok(inner.version_responses.pop_front().flatten())
    }

    async fn read_psm(&self, device_id: &DeviceId) -> BleResult<Option<u16>> {
        let mut inner = self.inner.lock().unwrap();
        inner.calls.push(CallKind::ReadPsm(device_id.clone()));
        Ok(inner.psm_responses.pop_front().flatten())
    }

    async fn open_l2cap(&self, device_id: &DeviceId, psm: u16) -> BleResult<L2capChannel> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .calls
            .push(CallKind::OpenL2cap(device_id.clone(), psm));
        inner
            .open_l2cap_queue
            .iter()
            .position(|(id, p, _)| id == device_id && *p == psm)
            .map(|pos| inner.open_l2cap_queue.remove(pos).unwrap().2)
            .unwrap_or_else(|| Err(crate::error::BleError::Unsupported))
    }

    async fn start_scan(&self) -> BleResult<()> {
        self.inner.lock().unwrap().calls.push(CallKind::StartScan);
        Ok(())
    }

    async fn stop_scan(&self) -> BleResult<()> {
        self.inner.lock().unwrap().calls.push(CallKind::StopScan);
        Ok(())
    }

    async fn rebuild_server(&self) -> BleResult<()> {
        self.inner
            .lock()
            .unwrap()
            .calls
            .push(CallKind::RebuildServer);
        Ok(())
    }

    async fn restart_advertising(&self) -> BleResult<()> {
        self.inner
            .lock()
            .unwrap()
            .calls
            .push(CallKind::RestartAdvertising);
        Ok(())
    }

    async fn restart_l2cap_listener(&self) -> BleResult<Option<u16>> {
        self.inner
            .lock()
            .unwrap()
            .calls
            .push(CallKind::RestartL2capListener);
        Ok(None)
    }

    async fn is_powered(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.calls.push(CallKind::IsPowered);
        inner.is_powered
    }

    async fn refresh(&self, device_id: &DeviceId) -> BleResult<()> {
        self.inner
            .lock()
            .unwrap()
            .calls
            .push(CallKind::Refresh(device_id.clone()));
        Ok(())
    }

    async fn mtu(&self, _device_id: &DeviceId) -> u16 {
        let mut inner = self.inner.lock().unwrap();
        inner.mtu_responses.pop_front().unwrap_or(inner.mtu_default)
    }
}

/// Pairs two `MockBleInterface` instances so that writes on one side appear
/// as inbound fragments on the other side's registry inbox. The N=2 special
/// case of the more general `MockFabric`; preserved so the existing suite
/// of pairwise integration tests doesn't churn.
pub struct MockFabricPair {
    pub central: Arc<MockBleInterface>,
    pub peripheral: Arc<MockBleInterface>,
    pub central_as_device: DeviceId,
    pub peripheral_as_device: DeviceId,
}

impl MockFabricPair {
    pub fn new(
        central_inbox: tokio::sync::mpsc::Sender<crate::transport::peer::PeerCommand>,
        peripheral_inbox: tokio::sync::mpsc::Sender<crate::transport::peer::PeerCommand>,
    ) -> Self {
        let central = Arc::new(MockBleInterface::new());
        let peripheral = Arc::new(MockBleInterface::new());
        let central_as_device = DeviceId::from("fabric-central");
        let peripheral_as_device = DeviceId::from("fabric-peripheral");

        {
            let inbox = peripheral_inbox.clone();
            let from = central_as_device.clone();
            central.set_on_c2p_write(Box::new(move |_target, bytes| {
                let cmd = crate::transport::peer::PeerCommand::InboundGattFragment {
                    device_id: from.clone(),
                    source: crate::transport::peer::FragmentSource::PeripheralReceivedC2p,
                    bytes,
                };
                let _ = inbox.try_send(cmd);
            }));
        }
        {
            let inbox = central_inbox.clone();
            let from = peripheral_as_device.clone();
            peripheral.set_on_p2c_notify(Box::new(move |_target, bytes| {
                let cmd = crate::transport::peer::PeerCommand::InboundGattFragment {
                    device_id: from.clone(),
                    source: crate::transport::peer::FragmentSource::CentralReceivedP2c,
                    bytes,
                };
                let _ = inbox.try_send(cmd);
            }));
        }

        Self {
            central,
            peripheral,
            central_as_device,
            peripheral_as_device,
        }
    }
}

/// N-node fabric that routes by destination `DeviceId`. Each `add_node` call
/// returns a fresh `MockBleInterface` whose `on_c2p_write` and `on_p2c_notify`
/// hooks are pre-wired to look the target up in the shared routing table and
/// dispatch `PeerCommand::InboundGattFragment` into that node's inbox.
///
/// Unknown targets fail silently — models "wrote to a peer we used to know
/// but which has since been removed."
#[derive(Clone, Default)]
pub struct MockFabric {
    routes: Arc<
        Mutex<
            std::collections::HashMap<
                DeviceId,
                tokio::sync::mpsc::Sender<crate::transport::peer::PeerCommand>,
            >,
        >,
    >,
}

impl MockFabric {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a node and returns its `MockBleInterface`. The interface's
    /// c2p/p2c hooks are pre-installed to route by destination through the
    /// shared routing table.
    pub fn add_node(
        &self,
        device_id: DeviceId,
        inbox: tokio::sync::mpsc::Sender<crate::transport::peer::PeerCommand>,
    ) -> Arc<MockBleInterface> {
        self.routes.lock().unwrap().insert(device_id.clone(), inbox);

        let iface = Arc::new(MockBleInterface::new());

        let routes_c2p = Arc::clone(&self.routes);
        let from_c2p = device_id.clone();
        iface.set_on_c2p_write(Box::new(move |target, bytes| {
            let inbox = routes_c2p.lock().unwrap().get(&target).cloned();
            if let Some(inbox) = inbox {
                let _ = inbox.try_send(crate::transport::peer::PeerCommand::InboundGattFragment {
                    device_id: from_c2p.clone(),
                    source: crate::transport::peer::FragmentSource::PeripheralReceivedC2p,
                    bytes,
                });
            }
        }));

        let routes_p2c = Arc::clone(&self.routes);
        let from_p2c = device_id.clone();
        iface.set_on_p2c_notify(Box::new(move |target, bytes| {
            let inbox = routes_p2c.lock().unwrap().get(&target).cloned();
            if let Some(inbox) = inbox {
                let _ = inbox.try_send(crate::transport::peer::PeerCommand::InboundGattFragment {
                    device_id: from_p2c.clone(),
                    source: crate::transport::peer::FragmentSource::CentralReceivedP2c,
                    bytes,
                });
            }
        }));

        iface
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_records_calls() {
        let mock = MockBleInterface::new();
        let iface: Box<dyn BleInterface> = Box::new(mock.clone());
        let dev = DeviceId::from("x");
        iface.connect(&dev).await.unwrap();
        mock.assert_called(&CallKind::Connect(dev));
    }

    #[tokio::test]
    async fn mock_queued_connect_result() {
        let mock = MockBleInterface::new();
        let dev = DeviceId::from("peer-1");
        mock.on_connect(
            dev.clone(),
            Ok(ChannelHandle {
                id: 42,
                path: ConnectPath::L2cap,
            }),
        );
        let handle = mock.connect(&dev).await.unwrap();
        assert_eq!(handle.id, 42);
        assert_eq!(handle.path, ConnectPath::L2cap);
    }

    #[tokio::test]
    async fn mock_open_l2cap_default_err() {
        let mock = MockBleInterface::new();
        let dev = DeviceId::from("peer-2");
        let result = mock.open_l2cap(&dev, 137).await;
        assert!(result.is_err());
        mock.assert_called(&CallKind::OpenL2cap(dev, 137));
    }

    #[tokio::test]
    async fn mock_open_l2cap_queued_channel() {
        let mock = MockBleInterface::new();
        let dev = DeviceId::from("peer-3");
        let (chan, _other) = L2capChannel::pair(1024);
        mock.on_open_l2cap(dev.clone(), 200, Ok(chan));
        let result = mock.open_l2cap(&dev, 200).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mock_is_powered_reflects_set() {
        let mock = MockBleInterface::new();
        assert!(mock.is_powered().await);
        mock.set_powered(false);
        assert!(!mock.is_powered().await);
    }

    #[tokio::test]
    async fn on_c2p_write_hook_fires_with_payload() {
        let mock = MockBleInterface::new();
        let captured = Arc::new(Mutex::new(Vec::<Bytes>::new()));
        let sink = Arc::clone(&captured);
        mock.set_on_c2p_write(Box::new(move |_target, b| {
            sink.lock().unwrap().push(b);
        }));
        mock.write_c2p(&DeviceId::from("peer-x"), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        let got = captured.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(&got[0][..], b"payload");
    }

    #[tokio::test]
    async fn on_p2c_notify_hook_fires_with_payload() {
        let mock = MockBleInterface::new();
        let captured = Arc::new(Mutex::new(Vec::<Bytes>::new()));
        let sink = Arc::clone(&captured);
        mock.set_on_p2c_notify(Box::new(move |_target, b| {
            sink.lock().unwrap().push(b);
        }));
        mock.notify_p2c(&DeviceId::from("peer-y"), Bytes::from_static(b"notify"))
            .await
            .unwrap();
        let got = captured.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(&got[0][..], b"notify");
    }

    #[tokio::test]
    async fn mock_fabric_forwards_central_c2p_write_to_peripheral_inbox() {
        use crate::transport::peer::{FragmentSource, PeerCommand};

        let (central_inbox, _central_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);
        let (peripheral_inbox, mut peripheral_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);

        let fabric = MockFabricPair::new(central_inbox, peripheral_inbox);
        fabric
            .central
            .write_c2p(&fabric.peripheral_as_device, Bytes::from_static(b"hello"))
            .await
            .unwrap();

        let cmd = tokio::time::timeout(std::time::Duration::from_secs(1), peripheral_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match cmd {
            PeerCommand::InboundGattFragment {
                device_id,
                source,
                bytes,
            } => {
                assert_eq!(device_id, fabric.central_as_device);
                assert_eq!(source, FragmentSource::PeripheralReceivedC2p);
                assert_eq!(&bytes[..], b"hello");
            }
            other => panic!("expected InboundGattFragment, got {other:?}"),
        }
    }

    #[test]
    fn mock_seeded_connect_failure() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mock = MockBleInterface::new();
        let device = DeviceId::from("fail-dev");
        mock.on_connect(device.clone(), Err(crate::error::BleError::AdapterOff));
        let result = rt.block_on(mock.connect(&device));
        assert!(matches!(result, Err(crate::error::BleError::AdapterOff)));
    }

    #[test]
    fn mock_seeded_disconnect_failure() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mock = MockBleInterface::new();
        let device = DeviceId::from("fail-dev");
        mock.on_disconnect(device.clone(), Err(crate::error::BleError::NotConnected));
        let result = rt.block_on(mock.disconnect(&device));
        assert!(matches!(result, Err(crate::error::BleError::NotConnected)));
    }

    #[tokio::test]
    async fn mock_fabric_routes_c2p_write_to_target_node_inbox() {
        use crate::transport::peer::{FragmentSource, PeerCommand};

        let (a_inbox_tx, _a_inbox_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);
        let (b_inbox_tx, mut b_inbox_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);
        let fabric = MockFabric::new();
        let iface_a = fabric.add_node(DeviceId::from("a"), a_inbox_tx);
        let _iface_b = fabric.add_node(DeviceId::from("b"), b_inbox_tx);

        iface_a
            .write_c2p(&DeviceId::from("b"), Bytes::from_static(b"hello-b"))
            .await
            .unwrap();

        let cmd = tokio::time::timeout(std::time::Duration::from_secs(1), b_inbox_rx.recv())
            .await
            .expect("timeout waiting for routed fragment")
            .expect("inbox closed");
        match cmd {
            PeerCommand::InboundGattFragment {
                device_id,
                source,
                bytes,
            } => {
                assert_eq!(device_id, DeviceId::from("a"));
                assert_eq!(source, FragmentSource::PeripheralReceivedC2p);
                assert_eq!(&bytes[..], b"hello-b");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_fabric_routes_p2c_notify_to_target_node_inbox() {
        use crate::transport::peer::{FragmentSource, PeerCommand};

        let (a_inbox_tx, mut a_inbox_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);
        let (b_inbox_tx, _b_inbox_rx) = tokio::sync::mpsc::channel::<PeerCommand>(16);
        let fabric = MockFabric::new();
        let _iface_a = fabric.add_node(DeviceId::from("a"), a_inbox_tx);
        let iface_b = fabric.add_node(DeviceId::from("b"), b_inbox_tx);

        iface_b
            .notify_p2c(&DeviceId::from("a"), Bytes::from_static(b"hello-a"))
            .await
            .unwrap();

        let cmd = tokio::time::timeout(std::time::Duration::from_secs(1), a_inbox_rx.recv())
            .await
            .expect("timeout waiting for routed fragment")
            .expect("inbox closed");
        match cmd {
            PeerCommand::InboundGattFragment {
                device_id,
                source,
                bytes,
            } => {
                assert_eq!(device_id, DeviceId::from("b"));
                assert_eq!(source, FragmentSource::CentralReceivedP2c);
                assert_eq!(&bytes[..], b"hello-a");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_fabric_drops_writes_to_unknown_target() {
        let (a_inbox_tx, _a_inbox_rx) = tokio::sync::mpsc::channel(16);
        let fabric = MockFabric::new();
        let iface_a = fabric.add_node(DeviceId::from("a"), a_inbox_tx);
        // No node with DeviceId "ghost" registered — write must not panic.
        iface_a
            .write_c2p(&DeviceId::from("ghost"), Bytes::from_static(b"nope"))
            .await
            .unwrap();
    }
}
