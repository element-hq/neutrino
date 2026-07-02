use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;

use jni::objects::{JObject, JObjectArray};
use jni::{jni_sig, jni_str};
use parking_lot::Mutex;
use tokio::sync::{broadcast, oneshot};
use tracing::debug;
use uuid::Uuid;

use crate::central::backend::{self, CentralBackend};
use crate::central::types::{CentralConfig, CentralEvent, ScanFilter, WriteType};
use crate::error::{BlewError, BlewResult};
use crate::gatt::props::CharacteristicProperties;
use crate::gatt::service::{GattCharacteristic, GattService};
use crate::l2cap::{L2capChannel, types::Psm};
use crate::types::{BleDevice, DeviceId};
use crate::util::BroadcastEventStream;
use crate::util::request_map::RequestMap;

use super::jni_globals::{central_class, jvm};

struct CentralState {
    event_tx: broadcast::Sender<CentralEvent>,
    pending_ops: RequestMap<oneshot::Sender<BlewResult<Vec<u8>>>>,
    pending_connects: RequestMap<oneshot::Sender<BlewResult<()>>>,
    connect_keys: Mutex<HashMap<String, u64>>,
    pending_discover: RequestMap<oneshot::Sender<BlewResult<Vec<GattService>>>>,
    discover_keys: Mutex<HashMap<String, u64>>,
    discovered: Mutex<Vec<BleDevice>>,
    mtu_cache: Mutex<HashMap<String, u16>>,
    pending_op_keys: Mutex<HashMap<String, u64>>,
}

static STATE: OnceLock<CentralState> = OnceLock::new();

fn state() -> &'static CentralState {
    STATE.get().expect("AndroidCentral not initialized")
}

fn init_statics() {
    let (event_tx, _) = broadcast::channel(256);
    let _ = STATE.set(CentralState {
        event_tx,
        pending_ops: RequestMap::new(),
        pending_connects: RequestMap::new(),
        connect_keys: Mutex::new(HashMap::new()),
        pending_discover: RequestMap::new(),
        discover_keys: Mutex::new(HashMap::new()),
        discovered: Mutex::new(Vec::new()),
        mtu_cache: Mutex::new(HashMap::new()),
        pending_op_keys: Mutex::new(HashMap::new()),
    });
    super::l2cap_state::init_statics();
}

pub(crate) fn send_event(event: CentralEvent) {
    if let Some(s) = STATE.get() {
        let _ = s.event_tx.send(event);
    }
}

pub(crate) fn update_discovered(device: BleDevice) {
    if let Some(s) = STATE.get() {
        let mut list = s.discovered.lock();
        if let Some(existing) = list.iter_mut().find(|d| d.id == device.id) {
            *existing = device;
        } else {
            list.push(device);
        }
    }
}

pub(crate) fn complete_connect(addr: &str, result: BlewResult<()>) {
    if let Some(s) = STATE.get() {
        if let Some(id) = s.connect_keys.lock().remove(addr) {
            if let Some(tx) = s.pending_connects.take(id) {
                let _ = tx.send(result);
            }
        }
    }
}

pub(crate) fn complete_discover_services(addr: &str, result: BlewResult<Vec<GattService>>) {
    if let Some(s) = STATE.get() {
        if let Some(id) = s.discover_keys.lock().remove(addr) {
            if let Some(tx) = s.pending_discover.take(id) {
                let _ = tx.send(result);
            }
        }
    }
}

pub(crate) fn complete_pending(key: &str, result: BlewResult<Vec<u8>>) {
    if let Some(s) = STATE.get() {
        if let Some(id) = s.pending_op_keys.lock().remove(key) {
            if let Some(tx) = s.pending_ops.take(id) {
                let _ = tx.send(result);
            }
        }
    }
}

pub(crate) fn set_mtu(addr: &str, mtu: u16) {
    if let Some(s) = STATE.get() {
        s.mtu_cache.lock().insert(addr.to_owned(), mtu);
    }
}

pub(crate) fn parse_services_json(json: &str) -> Vec<GattService> {
    let array: Vec<serde_json::Value> = match serde_json::from_str(json) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("failed to parse services JSON: {e}");
            return Vec::new();
        }
    };

    array
        .into_iter()
        .filter_map(|svc| {
            let uuid: Uuid = svc.get("uuid")?.as_str()?.parse().ok()?;
            let chars = svc.get("characteristics")?.as_array()?;
            let characteristics = chars
                .iter()
                .filter_map(|ch| {
                    let chr_uuid: Uuid = ch.get("uuid")?.as_str()?.parse().ok()?;
                    let props = ch.get("properties")?.as_i64().unwrap_or(0);
                    Some(GattCharacteristic {
                        uuid: chr_uuid,
                        properties: CharacteristicProperties::from_bits_truncate(props as u16),
                        permissions: crate::gatt::props::AttributePermissions::empty(),
                        value: vec![],
                        descriptors: vec![],
                    })
                })
                .collect();
            Some(GattService {
                uuid,
                primary: true,
                characteristics,
            })
        })
        .collect()
}

/// Kotlin status codes returned from GATT operations.
const STATUS_SUCCESS: i32 = 0;
const STATUS_NOT_CONNECTED: i32 = 1;
const STATUS_CHAR_NOT_FOUND: i32 = 2;
const STATUS_GATT_BUSY: i32 = 3;
const STATUS_GATT_FAILED: i32 = 4;

/// Map a Kotlin GATT status code to a [`BlewError`].
fn gatt_status_to_error(status: i32, device_id: &DeviceId, char_uuid: Uuid) -> BlewError {
    match status {
        STATUS_NOT_CONNECTED => BlewError::NotConnected(device_id.clone()),
        STATUS_CHAR_NOT_FOUND => BlewError::CharacteristicNotFound {
            device_id: device_id.clone(),
            char_uuid,
        },
        STATUS_GATT_BUSY => BlewError::GattBusy(device_id.clone()),
        STATUS_GATT_FAILED => BlewError::Gatt {
            device_id: device_id.clone(),
            source: "GATT operation returned failure".into(),
        },
        other => BlewError::Internal(format!("unknown GATT status {other}")),
    }
}

pub struct AndroidCentral;

impl AndroidCentral {
    pub async fn with_config(_config: CentralConfig) -> crate::error::BlewResult<Self> {
        Self::new().await
    }

    /// Clear the GATT cache for `device_id` by calling the hidden
    /// `BluetoothGatt.refresh()` method via reflection on the Kotlin side.
    ///
    /// Returns [`BlewError::NotSupported`] if the reflective call fails or
    /// no active GATT handle exists for the device.
    #[allow(clippy::unused_self)]
    pub(crate) fn refresh(
        &self,
        device_id: &DeviceId,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        async move {
            let ok = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("refresh"),
                        jni_sig!("(Ljava/lang/String;)Z"),
                        &[(&j_addr).into()],
                    )?;
                    result.z()
                })
                .map_err(jni_err)?;

            if ok {
                Ok(())
            } else {
                Err(BlewError::NotSupported)
            }
        }
    }
}

impl backend::private::Sealed for AndroidCentral {}

impl CentralBackend for AndroidCentral {
    type EventStream = BroadcastEventStream<CentralEvent>;

    fn new() -> impl Future<Output = BlewResult<Self>> + Send
    where
        Self: Sized,
    {
        async {
            if !super::are_ble_permissions_granted() {
                return Err(BlewError::PermissionDenied);
            }
            init_statics();
            debug!("AndroidCentral initialized");
            Ok(AndroidCentral)
        }
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        async {
            jvm()
                .attach_current_thread(|env| {
                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("isPowered"),
                        jni_sig!("()Z"),
                        &[],
                    )?;
                    Ok(result.z()?)
                })
                .map_err(jni_err)
        }
    }

    fn start_scan(&self, filter: ScanFilter) -> impl Future<Output = BlewResult<()>> + Send {
        async move {
            let low_power = filter.mode == crate::central::types::ScanMode::LowPower;
            jvm()
                .attach_current_thread(|env| {
                    let string_class = env.find_class(jni_str!("java/lang/String"))?;
                    let uuids: JObjectArray = env.new_object_array(
                        filter.services.len() as i32,
                        &string_class,
                        &JObject::null(),
                    )?;
                    for (i, uuid) in filter.services.iter().enumerate() {
                        let s = env.new_string(uuid.to_string())?;
                        uuids.set_element(env, i, &s)?;
                    }

                    env.call_static_method(
                        central_class(),
                        jni_str!("startScan"),
                        jni_sig!("([Ljava/lang/String;Z)V"),
                        &[(&uuids).into(), low_power.into()],
                    )?;

                    Ok(())
                })
                .map_err(jni_err)?;

            Ok(())
        }
    }

    fn stop_scan(&self) -> impl Future<Output = BlewResult<()>> + Send {
        async {
            jvm()
                .attach_current_thread(|env| {
                    env.call_static_method(
                        central_class(),
                        jni_str!("stopScan"),
                        jni_sig!("()V"),
                        &[],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;
            Ok(())
        }
    }

    fn discovered_devices(&self) -> impl Future<Output = BlewResult<Vec<BleDevice>>> + Send {
        async {
            let list = STATE
                .get()
                .map(|s| s.discovered.lock().clone())
                .unwrap_or_default();
            Ok(list)
        }
    }

    fn connect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        let id_for_err = device_id.clone();
        async move {
            let (tx, rx) = oneshot::channel();

            let s = state();
            let id = s.pending_connects.insert(tx);
            s.connect_keys.lock().insert(addr.clone(), id);

            jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    env.call_static_method(
                        central_class(),
                        jni_str!("connect"),
                        jni_sig!("(Ljava/lang/String;)V"),
                        &[(&j_addr).into()],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;

            rx.await
                .map_err(|_| BlewError::DisconnectedDuringOperation(id_for_err))?
        }
    }

    fn disconnect(&self, device_id: &DeviceId) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        async move {
            jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    env.call_static_method(
                        central_class(),
                        jni_str!("disconnect"),
                        jni_sig!("(Ljava/lang/String;)V"),
                        &[(&j_addr).into()],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;
            Ok(())
        }
    }

    fn discover_services(
        &self,
        device_id: &DeviceId,
    ) -> impl Future<Output = BlewResult<Vec<GattService>>> + Send {
        let addr = device_id.as_str().to_owned();
        let did = device_id.clone();
        async move {
            let (tx, rx) = oneshot::channel();

            let s = state();
            let id = s.pending_discover.insert(tx);
            s.discover_keys.lock().insert(addr.clone(), id);

            let status = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("discoverServices"),
                        jni_sig!("(Ljava/lang/String;)I"),
                        &[(&j_addr).into()],
                    )?;
                    Ok(result.i()?)
                })
                .map_err(jni_err)?;

            if status != STATUS_SUCCESS {
                // Clean up the pending oneshot since the callback won't fire.
                s.discover_keys.lock().remove(&addr);
                s.pending_discover.take(id);
                return Err(gatt_status_to_error(status, &did, Uuid::nil()));
            }

            rx.await
                .map_err(|_| BlewError::DisconnectedDuringOperation(did))?
        }
    }

    fn read_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<Vec<u8>>> + Send {
        let addr = device_id.as_str().to_owned();
        let did = device_id.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            let key = format!("{addr}:read:{char_uuid}");

            let s = state();
            let id = s.pending_ops.insert(tx);
            s.pending_op_keys.lock().insert(key.clone(), id);

            let status = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let j_uuid = env.new_string(char_uuid.to_string())?;
                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("readCharacteristic"),
                        jni_sig!("(Ljava/lang/String;Ljava/lang/String;)I"),
                        &[(&j_addr).into(), (&j_uuid).into()],
                    )?;

                    Ok(result.i()?)
                })
                .map_err(jni_err)?;

            if status != STATUS_SUCCESS {
                s.pending_op_keys.lock().remove(&key);
                s.pending_ops.take(id);
                return Err(gatt_status_to_error(status, &did, char_uuid));
            }

            rx.await
                .map_err(|_| BlewError::DisconnectedDuringOperation(did))?
        }
    }

    fn write_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        let did = device_id.clone();
        async move {
            let android_write_type = match write_type {
                WriteType::WithResponse => 2,    // WRITE_TYPE_DEFAULT
                WriteType::WithoutResponse => 1, // WRITE_TYPE_NO_RESPONSE
            };

            let s = state();

            // For write-without-response, don't wait for a callback.
            let (rx, pending_key, pending_id) = if write_type == WriteType::WithResponse {
                let (tx, rx) = oneshot::channel();
                let key = format!("{addr}:write:{char_uuid}");
                let id = s.pending_ops.insert(tx);
                s.pending_op_keys.lock().insert(key.clone(), id);
                (Some(rx), Some(key), Some(id))
            } else {
                (None, None, None)
            };

            let status = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let j_uuid = env.new_string(char_uuid.to_string())?;
                    let j_value = env.byte_array_from_slice(&value)?;

                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("writeCharacteristic"),
                        jni_sig!("(Ljava/lang/String;Ljava/lang/String;[BI)I"),
                        &[
                            (&j_addr).into(),
                            (&j_uuid).into(),
                            (&j_value).into(),
                            android_write_type.into(),
                        ],
                    )?;

                    Ok(result.i()?)
                })
                .map_err(jni_err)?;

            if status != STATUS_SUCCESS {
                if let (Some(key), Some(id)) = (pending_key, pending_id) {
                    s.pending_op_keys.lock().remove(&key);
                    s.pending_ops.take(id);
                }
                return Err(gatt_status_to_error(status, &did, char_uuid));
            }

            if let Some(rx) = rx {
                rx.await
                    .map_err(|_| BlewError::DisconnectedDuringOperation(did))??;
            }

            Ok(())
        }
    }

    fn subscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        let did = device_id.clone();
        async move {
            let status = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let j_uuid = env.new_string(char_uuid.to_string())?;

                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("subscribeCharacteristic"),
                        jni_sig!("(Ljava/lang/String;Ljava/lang/String;)I"),
                        &[(&j_addr).into(), (&j_uuid).into()],
                    )?;

                    Ok(result.i()?)
                })
                .map_err(jni_err)?;

            if status != STATUS_SUCCESS {
                return Err(gatt_status_to_error(status, &did, char_uuid));
            }

            Ok(())
        }
    }

    fn unsubscribe_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let addr = device_id.as_str().to_owned();
        let did = device_id.clone();
        async move {
            let status = jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    let j_uuid = env.new_string(char_uuid.to_string())?;

                    let result = env.call_static_method(
                        central_class(),
                        jni_str!("unsubscribeCharacteristic"),
                        jni_sig!("(Ljava/lang/String;Ljava/lang/String;)I"),
                        &[(&j_addr).into(), (&j_uuid).into()],
                    )?;

                    Ok(result.i()?)
                })
                .map_err(jni_err)?;

            if status != STATUS_SUCCESS {
                return Err(gatt_status_to_error(status, &did, char_uuid));
            }

            Ok(())
        }
    }

    fn mtu(&self, device_id: &DeviceId) -> impl Future<Output = u16> + Send {
        let addr = device_id.as_str().to_owned();
        async move {
            STATE
                .get()
                .and_then(|s| s.mtu_cache.lock().get(&addr).copied())
                .unwrap_or(23)
        }
    }

    fn open_l2cap_channel(
        &self,
        device_id: &DeviceId,
        psm: Psm,
    ) -> impl Future<Output = BlewResult<L2capChannel>> + Send {
        let addr = device_id.as_str().to_owned();
        let id_for_err = device_id.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            super::l2cap_state::set_pending_open(addr.clone(), tx);

            jvm()
                .attach_current_thread(|env| {
                    let j_addr = env.new_string(&addr)?;
                    env.call_static_method(
                        central_class(),
                        jni_str!("openL2capChannel"),
                        jni_sig!("(Ljava/lang/String;I)V"),
                        &[(&j_addr).into(), (psm.value() as i32).into()],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;

            rx.await
                .map_err(|_| BlewError::DisconnectedDuringOperation(id_for_err))?
        }
    }

    fn events(&self) -> Self::EventStream {
        BroadcastEventStream::new(state().event_tx.subscribe())
    }
}

fn jni_err(e: jni::errors::Error) -> BlewError {
    BlewError::Internal(format!("JNI error: {e}"))
}
