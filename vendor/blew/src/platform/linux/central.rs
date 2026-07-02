use crate::central::backend::{self, CentralBackend};
use crate::central::types::{CentralConfig, CentralEvent, DisconnectCause, ScanFilter, WriteType};
use crate::error::{BlewError, BlewResult};
use crate::gatt::props::{AttributePermissions, CharacteristicProperties};
use crate::gatt::service::{GattCharacteristic, GattService};
use crate::l2cap::{L2capChannel, types::Psm};
use crate::platform::linux::l2cap::bridge_l2cap;
use crate::types::{BleDevice, DeviceId};
use crate::util::BroadcastEventStream;
use bluer::gatt::CharacteristicFlags;
use bluer::{Adapter, AdapterEvent, Session};
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio_stream::StreamExt as _;
use tracing::{debug, trace, warn};
use uuid::Uuid;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

struct CentralInner {
    _session: Session,
    adapter: Adapter,
    discovered: Mutex<HashMap<DeviceId, BleDevice>>,
    mtu_cache: Mutex<HashMap<DeviceId, u16>>,
    event_tx: broadcast::Sender<CentralEvent>,
    scan_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    notify_tasks: Mutex<HashMap<(DeviceId, Uuid), tokio::task::JoinHandle<()>>>,
    adapter_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub struct LinuxCentral(Arc<CentralInner>);

impl LinuxCentral {
    pub async fn with_config(_config: CentralConfig) -> crate::error::BlewResult<Self> {
        Self::new().await
    }
}

impl backend::private::Sealed for LinuxCentral {}

impl CentralInner {
    fn parse_addr(device_id: &DeviceId) -> BlewResult<bluer::Address> {
        device_id
            .as_str()
            .parse()
            .map_err(|_| BlewError::DeviceNotFound(device_id.clone()))
    }

    fn drain_notify_tasks(&self, device_id: &DeviceId) {
        let mut tasks = self.notify_tasks.lock();
        let keys: Vec<_> = tasks
            .keys()
            .filter(|(did, _)| did == device_id)
            .cloned()
            .collect();
        for key in keys {
            if let Some(task) = tasks.remove(&key) {
                task.abort();
            }
        }
    }

    fn update_mtu(&self, device_id: &DeviceId, mtu: usize) {
        let mtu =
            u16::try_from(mtu.clamp(23, usize::from(u16::MAX))).expect("clamped MTU fits in u16");
        self.mtu_cache.lock().insert(device_id.clone(), mtu);
    }

    fn clear_mtu(&self, device_id: &DeviceId) {
        self.mtu_cache.lock().remove(device_id);
    }

    async fn find_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> BlewResult<bluer::gatt::remote::Characteristic> {
        let addr = Self::parse_addr(device_id)?;
        let device = self.adapter.device(addr).map_err(|e| BlewError::Central {
            source: Box::new(e),
        })?;
        let services = device.services().await.map_err(|e| BlewError::Gatt {
            device_id: device_id.clone(),
            source: Box::new(e),
        })?;
        for svc in services {
            let chars = svc.characteristics().await.map_err(|e| BlewError::Gatt {
                device_id: device_id.clone(),
                source: Box::new(e),
            })?;
            for ch in chars {
                let uuid = ch.uuid().await.map_err(|e| BlewError::Gatt {
                    device_id: device_id.clone(),
                    source: Box::new(e),
                })?;
                if uuid == char_uuid {
                    return Ok(ch);
                }
            }
        }
        Err(BlewError::CharacteristicNotFound {
            device_id: device_id.clone(),
            char_uuid,
        })
    }
}

fn flags_to_props(flags: &CharacteristicFlags) -> CharacteristicProperties {
    let mut props = CharacteristicProperties::empty();
    if flags.broadcast {
        props |= CharacteristicProperties::BROADCAST;
    }
    if flags.read {
        props |= CharacteristicProperties::READ;
    }
    if flags.write_without_response {
        props |= CharacteristicProperties::WRITE_WITHOUT_RESPONSE;
    }
    if flags.write {
        props |= CharacteristicProperties::WRITE;
    }
    if flags.notify {
        props |= CharacteristicProperties::NOTIFY;
    }
    if flags.indicate {
        props |= CharacteristicProperties::INDICATE;
    }
    if flags.authenticated_signed_writes {
        props |= CharacteristicProperties::AUTHENTICATED_SIGNED_WRITES;
    }
    if flags.extended_properties {
        props |= CharacteristicProperties::EXTENDED_PROPERTIES;
    }
    props
}

impl CentralBackend for LinuxCentral {
    type EventStream = BroadcastEventStream<CentralEvent>;

    async fn new() -> BlewResult<Self>
    where
        Self: Sized,
    {
        let session = Session::new().await.map_err(|e| BlewError::Central {
            source: Box::new(e),
        })?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|_| BlewError::AdapterNotFound)?;
        debug!(adapter = %adapter.name(), "BLE adapter initialized");

        // Disable pairing so BlueZ won't initiate SMP when it encounters
        // encrypted characteristics on remote devices (e.g. Apple's Battery
        // Service). Our GATT service doesn't require encryption.
        if let Err(e) = adapter.set_pairable(false).await {
            warn!("failed to set adapter non-pairable: {e}");
        }

        check_bluez_config();
        let (event_tx, _) = broadcast::channel(256);
        let inner = Arc::new(CentralInner {
            _session: session,
            adapter,
            discovered: Mutex::new(HashMap::new()),
            mtu_cache: Mutex::new(HashMap::new()),
            event_tx,
            scan_task: Mutex::new(None),
            notify_tasks: Mutex::new(HashMap::new()),
            adapter_task: Mutex::new(None),
        });
        let inner_clone = Arc::clone(&inner);
        let adapter_task = tokio::spawn(async move {
            let Ok(events) = inner_clone.adapter.events().await else {
                warn!("failed to subscribe to adapter events");
                return;
            };
            let mut events = Box::pin(events);
            while let Some(event) = events.next().await {
                if let AdapterEvent::PropertyChanged(bluer::AdapterProperty::Powered(powered)) =
                    event
                {
                    debug!(powered, "central adapter state changed");
                    let _ = inner_clone
                        .event_tx
                        .send(CentralEvent::AdapterStateChanged { powered });
                }
            }
        });
        *inner.adapter_task.lock() = Some(adapter_task);
        Ok(LinuxCentral(inner))
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            handle
                .adapter
                .is_powered()
                .await
                .map_err(|e| BlewError::Central {
                    source: Box::new(e),
                })
        }
    }

    fn start_scan(&self, filter: ScanFilter) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            debug!(service_filter = ?filter.services, "starting BLE scan");
            // BlueZ replays its device cache as DeviceAdded events at the start of
            // every discovery session, using stale names/UUIDs from previous sessions.
            // Remove non-connected cached devices so BlueZ treats them as new and
            // emits fresh advertisement data.
            if let Ok(addrs) = handle.adapter.device_addresses().await {
                for addr in addrs {
                    if let Ok(dev) = handle.adapter.device(addr)
                        && !dev.is_connected().await.unwrap_or(false)
                    {
                        handle.adapter.remove_device(addr).await.ok();
                    }
                }
            }

            if !filter.services.is_empty() {
                let df = bluer::DiscoveryFilter {
                    uuids: filter.services.into_iter().collect(),
                    ..Default::default()
                };
                handle
                    .adapter
                    .set_discovery_filter(df)
                    .await
                    .map_err(|e| BlewError::Central {
                        source: Box::new(e),
                    })?;
            }

            let stream =
                handle
                    .adapter
                    .discover_devices()
                    .await
                    .map_err(|e| BlewError::Central {
                        source: Box::new(e),
                    })?;
            let mut stream = Box::pin(stream);

            // Dropping a JoinHandle only detaches; we must abort explicitly.
            if let Some(old) = handle.scan_task.lock().take() {
                old.abort();
            }

            let handle_task = Arc::clone(&handle);
            let task = tokio::spawn(async move {
                let handle = handle_task;
                while let Some(event) = stream.next().await {
                    match event {
                        AdapterEvent::DeviceAdded(addr) => {
                            let Ok(device) = handle.adapter.device(addr) else {
                                continue;
                            };
                            let name = device.name().await.ok().flatten();
                            let rssi = device.rssi().await.ok().flatten();
                            let services = device
                                .uuids()
                                .await
                                .ok()
                                .flatten()
                                .map(|s| s.into_iter().collect::<Vec<_>>())
                                .unwrap_or_default();
                            // First manufacturer-specific-data entry, if any.
                            let manufacturer_data = device
                                .manufacturer_data()
                                .await
                                .ok()
                                .flatten()
                                .and_then(|m| m.into_iter().next())
                                .filter(|(_, data)| !data.is_empty());
                            let device_id = DeviceId(addr.to_string());
                            debug!(device_id = %device_id, name = ?name, rssi = ?rssi, "device discovered");
                            let ble_device = BleDevice {
                                id: device_id.clone(),
                                name,
                                rssi,
                                services,
                                manufacturer_data,
                            };
                            handle
                                .discovered
                                .lock()
                                .insert(device_id, ble_device.clone());
                            let _ = handle
                                .event_tx
                                .send(CentralEvent::DeviceDiscovered(ble_device));
                        }
                        AdapterEvent::DeviceRemoved(addr) => {
                            let device_id = DeviceId(addr.to_string());
                            debug!(device_id = %device_id, "device removed");
                            handle.discovered.lock().remove(&device_id);
                            handle.clear_mtu(&device_id);
                            handle.drain_notify_tasks(&device_id);
                            let cause = if handle.adapter.is_powered().await.unwrap_or(true) {
                                DisconnectCause::LinkLoss
                            } else {
                                DisconnectCause::AdapterOff
                            };
                            let _ = handle
                                .event_tx
                                .send(CentralEvent::DeviceDisconnected { device_id, cause });
                        }
                        AdapterEvent::PropertyChanged(_) => {}
                    }
                }
            });

            *handle.scan_task.lock() = Some(task);
            Ok(())
        }
    }

    fn stop_scan(&self) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            debug!("stopping BLE scan");
            if let Some(task) = handle.scan_task.lock().take() {
                task.abort();
            }
            Ok(())
        }
    }

    fn discovered_devices(&self) -> impl Future<Output = BlewResult<Vec<BleDevice>>> + Send {
        let handle = Arc::clone(&self.0);
        async move { Ok(handle.discovered.lock().values().cloned().collect()) }
    }

    fn connect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, "connecting to device");
            let addr = CentralInner::parse_addr(&device_id)?;
            let device = handle
                .adapter
                .device(addr)
                .map_err(|e| BlewError::Central {
                    source: Box::new(e),
                })?;
            match tokio::time::timeout(CONNECT_TIMEOUT, device.connect()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return Err(BlewError::Central {
                        source: Box::new(e),
                    });
                }
                Err(_) => return Err(BlewError::Timeout),
            }
            debug!(device_id = %device_id, "device connected");
            handle.clear_mtu(&device_id);
            let _ = handle
                .event_tx
                .send(CentralEvent::DeviceConnected { device_id });
            Ok(())
        }
    }

    fn disconnect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, "disconnecting from device");
            let addr = CentralInner::parse_addr(&device_id)?;
            let device = handle
                .adapter
                .device(addr)
                .map_err(|e| BlewError::Central {
                    source: Box::new(e),
                })?;
            device.disconnect().await.map_err(|e| BlewError::Central {
                source: Box::new(e),
            })?;
            debug!(device_id = %device_id, "device disconnected");
            handle.clear_mtu(&device_id);
            handle.drain_notify_tasks(&device_id);
            let _ = handle.event_tx.send(CentralEvent::DeviceDisconnected {
                device_id,
                cause: DisconnectCause::LocalClose,
            });
            Ok(())
        }
    }

    fn discover_services(
        &self,
        device_id: &DeviceId,
    ) -> impl Future<Output = BlewResult<Vec<GattService>>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, "discovering GATT services");
            let addr = CentralInner::parse_addr(&device_id)?;
            let device = handle
                .adapter
                .device(addr)
                .map_err(|e| BlewError::Central {
                    source: Box::new(e),
                })?;
            let services = device.services().await.map_err(|e| BlewError::Gatt {
                device_id: device_id.clone(),
                source: Box::new(e),
            })?;
            let mut result = Vec::new();
            for svc in services {
                let svc_uuid = svc.uuid().await.map_err(|e| BlewError::Gatt {
                    device_id: device_id.clone(),
                    source: Box::new(e),
                })?;
                let primary = svc.primary().await.unwrap_or(true);
                let chars = svc.characteristics().await.map_err(|e| BlewError::Gatt {
                    device_id: device_id.clone(),
                    source: Box::new(e),
                })?;
                let mut gatt_chars = Vec::new();
                for ch in chars {
                    let ch_uuid = ch.uuid().await.map_err(|e| BlewError::Gatt {
                        device_id: device_id.clone(),
                        source: Box::new(e),
                    })?;
                    let flags = ch.flags().await.unwrap_or_default();
                    let properties = flags_to_props(&flags);
                    gatt_chars.push(GattCharacteristic {
                        uuid: ch_uuid,
                        properties,
                        permissions: AttributePermissions::empty(),
                        value: vec![],
                        descriptors: vec![],
                    });
                }
                result.push(GattService {
                    uuid: svc_uuid,
                    primary,
                    characteristics: gatt_chars,
                });
            }
            Ok(result)
        }
    }

    fn read_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<Vec<u8>>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, %char_uuid, "reading characteristic");
            let ch = handle.find_characteristic(&device_id, char_uuid).await?;
            ch.read().await.map_err(|e| BlewError::Gatt {
                device_id,
                source: Box::new(e),
            })
        }
    }

    fn write_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            trace!(device_id = %device_id, %char_uuid, len = value.len(), ?write_type, "writing characteristic");
            let ch = handle.find_characteristic(&device_id, char_uuid).await?;
            match write_type {
                WriteType::WithResponse => ch.write(&value).await.map_err(|e| BlewError::Gatt {
                    device_id,
                    source: Box::new(e),
                }),
                WriteType::WithoutResponse => {
                    // write_io() acquires a kernel socket for Write Command (no response).
                    use tokio::io::AsyncWriteExt as _;
                    let mut writer = ch.write_io().await.map_err(|e| BlewError::Gatt {
                        device_id: device_id.clone(),
                        source: Box::new(e),
                    })?;
                    handle.update_mtu(&device_id, writer.mtu());
                    writer
                        .write_all(&value)
                        .await
                        .map_err(|e| BlewError::Gatt {
                            device_id: device_id.clone(),
                            source: Box::new(e),
                        })?;
                    writer.flush().await.map_err(|e| BlewError::Gatt {
                        device_id,
                        source: Box::new(e),
                    })
                }
            }
        }
    }

    fn subscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, %char_uuid, "subscribing to characteristic");
            // Reject duplicates before acquiring a new notify_io socket -- otherwise
            // each subscriber reads from its own stream and they each see partial data.
            if handle
                .notify_tasks
                .lock()
                .contains_key(&(device_id.clone(), char_uuid))
            {
                return Err(BlewError::AlreadySubscribed {
                    device_id,
                    char_uuid,
                });
            }
            let ch = handle.find_characteristic(&device_id, char_uuid).await?;
            let reader = ch.notify_io().await.map_err(|e| BlewError::Gatt {
                device_id: device_id.clone(),
                source: Box::new(e),
            })?;
            handle.update_mtu(&device_id, reader.mtu());
            let h = Arc::clone(&handle);
            let did = device_id.clone();
            let task = tokio::spawn(async move {
                use tokio::io::AsyncReadExt as _;
                let mut reader = reader;
                let mut buf = vec![0_u8; 512];
                loop {
                    match reader.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            trace!(device_id = %did, %char_uuid, len = n, "characteristic notification");
                            let _ = h.event_tx.send(CentralEvent::CharacteristicNotification {
                                device_id: did.clone(),
                                char_uuid,
                                value: Bytes::copy_from_slice(&buf[..n]),
                            });
                        }
                    }
                }
            });
            // Race check: another caller may have inserted between our contains_key
            // check and now. Use entry/or_insert_with semantics via a second lock.
            let mut tasks = handle.notify_tasks.lock();
            if tasks.contains_key(&(device_id.clone(), char_uuid)) {
                drop(tasks);
                task.abort();
                return Err(BlewError::AlreadySubscribed {
                    device_id,
                    char_uuid,
                });
            }
            tasks.insert((device_id, char_uuid), task);
            Ok(())
        }
    }

    fn unsubscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            if let Some(task) = handle.notify_tasks.lock().remove(&(device_id, char_uuid)) {
                task.abort();
            }
            Ok(())
        }
    }

    async fn mtu(&self, device_id: &DeviceId) -> u16 {
        self.0
            .mtu_cache
            .lock()
            .get(device_id)
            .copied()
            .unwrap_or(23)
    }

    fn open_l2cap_channel(
        &self,
        device_id: &DeviceId,
        psm: Psm,
    ) -> impl Future<Output = BlewResult<L2capChannel>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            debug!(device_id = %device_id, psm = psm.0, "opening L2CAP channel");
            let addr = CentralInner::parse_addr(&device_id)?;
            let device = handle.adapter.device(addr).map_err(|e| BlewError::L2cap {
                source: Box::new(e),
            })?;
            let addr_type = device.address_type().await.map_err(|e| BlewError::L2cap {
                source: Box::new(e),
            })?;
            let socket_addr = bluer::l2cap::SocketAddr::new(addr, addr_type, psm.0);
            // Brief delay to ensure ACL connection is ready before L2CAP CoC setup
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            // Use low-level Socket API to explicitly set security to Low,
            // preventing BlueZ from triggering a pairing request.
            let socket = bluer::l2cap::Socket::new_stream().map_err(|e| BlewError::L2cap {
                source: Box::new(e),
            })?;
            socket
                .set_security(bluer::l2cap::Security {
                    level: bluer::l2cap::SecurityLevel::Low,
                    key_size: 0,
                })
                .map_err(|e| BlewError::L2cap {
                    source: Box::new(e),
                })?;
            // Advertise a large receive MPS so the peer can send bigger PDUs.
            socket.set_recv_mtu(65535).map_err(|e| BlewError::L2cap {
                source: Box::new(e),
            })?;
            socket
                .bind(bluer::l2cap::SocketAddr::any_le())
                .map_err(|e| BlewError::L2cap {
                    source: Box::new(e),
                })?;
            let stream = socket
                .connect(socket_addr)
                .await
                .map_err(|e| BlewError::L2cap {
                    source: Box::new(e),
                })?;
            debug!(device_id = %device_id, psm = psm.0, "L2CAP channel opened");
            Ok(bridge_l2cap(stream))
        }
    }

    fn events(&self) -> Self::EventStream {
        BroadcastEventStream::new(self.0.event_tx.subscribe())
    }
}

/// Warn if BlueZ is configured in a way that triggers Apple pairing popups.
///
/// Two independent BlueZ behaviours can cause unwanted pairing requests when
/// connecting to Apple devices:
///
/// 1. **Plugins** -- `battery` and `deviceinfo` read encrypted GATT characteristics
///    (Battery Level 0x180F, Device Information 0x180A), triggering security
///    negotiation.
///
/// 2. **GATT cache** -- when `[GATT] Cache` is not set to `no`, BlueZ may read
///    and cache characteristic values during service discovery, including
///    encrypted ones on Apple devices.
fn check_bluez_config() {
    const PROBLEMATIC_PLUGINS: &[&str] = &["battery", "deviceinfo"];

    let path = std::path::Path::new("/etc/bluetooth/main.conf");
    let Ok(contents) = std::fs::read_to_string(path) else {
        return;
    };

    let mut warnings: Vec<String> = Vec::new();

    // Check plugins.
    let disabled_plugins: Vec<String> = contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with('#') {
                return None;
            }
            line.strip_prefix("DisablePlugins")
                .map(|v| v.trim_start_matches([' ', '=']))
        })
        .flat_map(|v| v.split(','))
        .map(|p| p.trim().to_owned())
        .filter(|p| PROBLEMATIC_PLUGINS.contains(&p.as_str()))
        .collect();

    let missing: Vec<&&str> = PROBLEMATIC_PLUGINS
        .iter()
        .filter(|p| !disabled_plugins.iter().any(|d| d == *p))
        .collect();

    if !missing.is_empty() {
        let list = missing.iter().map(|p| **p).collect::<Vec<_>>().join(",");
        warnings.push(format!(
            "plugins that probe encrypted Apple services are active: {list}"
        ));
    }

    // Check GATT cache setting.
    // Look for `Cache = no` (case-insensitive value) under [GATT].
    let mut in_gatt_section = false;
    let mut cache_is_no = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_gatt_section = trimmed.eq_ignore_ascii_case("[gatt]");
            continue;
        }
        if in_gatt_section
            && !trimmed.starts_with('#')
            && let Some(val) = trimmed.strip_prefix("Cache")
        {
            let val = val.trim_start_matches([' ', '=']).trim();
            if val.eq_ignore_ascii_case("no") {
                cache_is_no = true;
            }
        }
    }
    if !cache_is_no {
        warnings.push(
            "GATT cache is enabled (BlueZ may read encrypted characteristics during discovery)"
                .into(),
        );
    }

    if !warnings.is_empty() {
        let detail = warnings.join("; ");
        warn!(
            "BlueZ config may trigger Apple pairing popups: {detail}. \
             To fix, add to /etc/bluetooth/main.conf:\n\n  \
             [General]\n  DisablePlugins=battery,deviceinfo\n\n  \
             [GATT]\n  Cache=no\n\n  \
             Then: sudo systemctl restart bluetooth"
        );
    }
}
