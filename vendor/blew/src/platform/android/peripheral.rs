use std::future::Future;

use jni::objects::{JObject, JObjectArray, JValue};
use jni::{jni_sig, jni_str};
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::debug;
use uuid::Uuid;

use crate::error::{BlewError, BlewResult};
use crate::gatt::props::CharacteristicProperties;
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::peripheral::backend::{self, PeripheralBackend};
use crate::peripheral::types::{
    AdvertisingConfig, PeripheralConfig, PeripheralRequest, PeripheralStateEvent,
};
use crate::types::DeviceId;
use crate::util::BroadcastEventStream;

use super::jni_globals::{jvm, peripheral_class};

struct PeripheralState {
    request_tx: mpsc::UnboundedSender<PeripheralRequest>,
    request_rx: Mutex<Option<mpsc::UnboundedReceiver<PeripheralRequest>>>,
    state_tx: broadcast::Sender<PeripheralStateEvent>,
}

static STATE: Mutex<Option<PeripheralState>> = Mutex::new(None);

pub(crate) fn send_request(request: PeripheralRequest) {
    if let Some(s) = STATE.lock().as_ref() {
        let _ = s.request_tx.send(request);
    }
}

pub(crate) fn send_state_event(event: PeripheralStateEvent) {
    if let Some(s) = STATE.lock().as_ref() {
        let _ = s.state_tx.send(event);
    }
}

pub struct AndroidPeripheral;

impl AndroidPeripheral {
    pub async fn with_config(_config: PeripheralConfig) -> BlewResult<Self> {
        <Self as PeripheralBackend>::new().await
    }
}

impl backend::private::Sealed for AndroidPeripheral {}

impl PeripheralBackend for AndroidPeripheral {
    type StateEvents = BroadcastEventStream<PeripheralStateEvent>;
    type Requests = UnboundedReceiverStream<PeripheralRequest>;

    fn new() -> impl Future<Output = BlewResult<Self>> + Send
    where
        Self: Sized,
    {
        async {
            if !super::are_ble_permissions_granted() {
                return Err(BlewError::PermissionDenied);
            }
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            let (state_tx, _) = broadcast::channel(256);
            *STATE.lock() = Some(PeripheralState {
                request_tx,
                request_rx: Mutex::new(Some(request_rx)),
                state_tx,
            });
            debug!("AndroidPeripheral initialized");
            Ok(AndroidPeripheral)
        }
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        async {
            jvm()
                .attach_current_thread(|env| {
                    let result = env.call_static_method(
                        peripheral_class(),
                        jni_str!("isPowered"),
                        jni_sig!("()Z"),
                        &[],
                    )?;
                    Ok(result.z()?)
                })
                .map_err(jni_err)
        }
    }

    fn add_service(&self, service: &GattService) -> impl Future<Output = BlewResult<()>> + Send {
        let service = service.clone();
        async move {
            jvm()
                .attach_current_thread(|env| {
                    let service_uuid = env.new_string(service.uuid.to_string())?;

                    let n = service.characteristics.len();

                    let string_class = env.find_class(jni_str!("java/lang/String"))?;
                    let byte_array_class = env.find_class(jni_str!("[B"))?;

                    let char_uuids: JObjectArray =
                        env.new_object_array(n as i32, &string_class, &JObject::null())?;
                    let char_values: JObjectArray =
                        env.new_object_array(n as i32, &byte_array_class, &JObject::null())?;
                    let mut props_arr = vec![0i32; n];
                    let mut perms_arr = vec![0i32; n];

                    for (i, ch) in service.characteristics.iter().enumerate() {
                        let uuid_str = env.new_string(ch.uuid.to_string())?;
                        char_uuids.set_element(env, i, &uuid_str)?;

                        props_arr[i] = blew_props_to_android(ch.properties);
                        perms_arr[i] = blew_perms_to_android(ch.permissions);

                        let value = env.byte_array_from_slice(&ch.value)?;
                        char_values.set_element(env, i, &value)?;
                    }

                    let j_props = env.new_int_array(n)?;
                    j_props.set_region(env, 0, &props_arr)?;

                    let j_perms = env.new_int_array(n)?;
                    j_perms.set_region(env, 0, &perms_arr)?;

                    env.call_static_method(
                        peripheral_class(),
                        jni_str!("addService"),
                        jni_sig!("(Ljava/lang/String;[Ljava/lang/String;[I[I[[B)V"),
                        &[
                            (&service_uuid).into(),
                            (&char_uuids).into(),
                            (&j_props).into(),
                            (&j_perms).into(),
                            (&char_values).into(),
                        ],
                    )?;

                    Ok(())
                })
                .map_err(jni_err)?;

            debug!(uuid = %service.uuid, "added GATT service");
            Ok(())
        }
    }

    fn start_advertising(
        &self,
        config: &AdvertisingConfig,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let config = config.clone();
        async move {
            jvm()
                .attach_current_thread(|env| {
                    let name = env.new_string(&config.local_name)?;

                    let string_class = env.find_class(jni_str!("java/lang/String"))?;
                    let uuids: JObjectArray = env.new_object_array(
                        config.service_uuids.len() as i32,
                        &string_class,
                        &JObject::null(),
                    )?;
                    for (i, uuid) in config.service_uuids.iter().enumerate() {
                        let s = env.new_string(uuid.to_string())?;
                        uuids.set_element(env, i, &s)?;
                    }

                    // Manufacturer data → Kotlin as (int company_id, byte[]).
                    // Company id -1 with an empty array means "none" (legacy
                    // advertising); a real id + non-empty payload triggers the
                    // extended-advertising path on the Kotlin side.
                    let mfr_id: i32 = config
                        .manufacturer_data
                        .as_ref()
                        .map_or(-1, |(company, _)| *company as i32);
                    let empty: Vec<u8> = Vec::new();
                    let mfr_slice: &[u8] = config
                        .manufacturer_data
                        .as_ref()
                        .map_or(empty.as_slice(), |(_, data)| data.as_slice());
                    let mfr_arr = env.byte_array_from_slice(mfr_slice)?;

                    env.call_static_method(
                        peripheral_class(),
                        jni_str!("startAdvertising"),
                        jni_sig!("(Ljava/lang/String;[Ljava/lang/String;I[B)V"),
                        &[
                            (&name).into(),
                            (&uuids).into(),
                            JValue::Int(mfr_id),
                            (&mfr_arr).into(),
                        ],
                    )?;

                    Ok(())
                })
                .map_err(jni_err)?;

            debug!("advertising started");
            Ok(())
        }
    }

    fn stop_advertising(&self) -> impl Future<Output = BlewResult<()>> + Send {
        async {
            jvm()
                .attach_current_thread(|env| {
                    env.call_static_method(
                        peripheral_class(),
                        jni_str!("stopAdvertising"),
                        jni_sig!("()V"),
                        &[],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;
            Ok(())
        }
    }

    fn notify_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let device_addr = device_id.as_str().to_owned();
        async move {
            // Retry loop: Kotlin returns 1 ("busy") when the previous
            // notification hasn't completed yet (onNotificationSent pending).
            // We retry with async sleep so we don't block the tokio thread.
            for attempt in 0..50u32 {
                let status: i32 = jvm()
                    .attach_current_thread(|env| {
                        let addr_str = env.new_string(&device_addr)?;
                        let uuid_str = env.new_string(char_uuid.to_string())?;
                        let j_value = env.byte_array_from_slice(&value)?;

                        let ret = env.call_static_method(
                            peripheral_class(),
                            jni_str!("notifyCharacteristic"),
                            jni_sig!("(Ljava/lang/String;Ljava/lang/String;[B)I"),
                            &[(&addr_str).into(), (&uuid_str).into(), (&j_value).into()],
                        )?;
                        ret.i()
                    })
                    .map_err(jni_err)?;

                match status {
                    0 => return Ok(()), // success
                    1 => {
                        // Busy -- previous notification still in flight.
                        // Yield to tokio and retry after a short delay.
                        if attempt < 49 {
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    }
                    2 => return Ok(()), // no subscribers -- not an error
                    3 => {
                        return Err(BlewError::LocalCharacteristicNotFound { char_uuid });
                    }
                    other => {
                        return Err(BlewError::Peripheral {
                            source: format!("notify returned unknown status {other}").into(),
                        });
                    }
                }
            }
            // Exhausted retries -- treat as transient error.
            Err(BlewError::Peripheral {
                source: "notification busy after retries".into(),
            })
        }
    }

    fn l2cap_listener(
        &self,
    ) -> impl Future<
        Output = BlewResult<(
            Psm,
            impl futures_core::Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
        )>,
    > + Send {
        async {
            let (psm_tx, psm_rx) = oneshot::channel();
            super::l2cap_state::set_pending_server(psm_tx);

            let (accept_tx, accept_rx) = mpsc::unbounded_channel();
            super::l2cap_state::set_accept_tx(accept_tx);

            jvm()
                .attach_current_thread(|env| {
                    env.call_static_method(
                        peripheral_class(),
                        jni_str!("openL2capServer"),
                        jni_sig!("()V"),
                        &[],
                    )?;
                    Ok(())
                })
                .map_err(jni_err)?;

            let psm = psm_rx
                .await
                .map_err(|_| BlewError::Internal("L2CAP server open cancelled".into()))??;

            Ok((psm, UnboundedReceiverStream::new(accept_rx)))
        }
    }

    fn state_events(&self) -> Self::StateEvents {
        let receiver = STATE
            .lock()
            .as_ref()
            .expect("AndroidPeripheral not initialized")
            .state_tx
            .subscribe();
        BroadcastEventStream::new(receiver)
    }

    fn take_requests(&self) -> Option<Self::Requests> {
        let rx = {
            let guard = STATE.lock();
            guard.as_ref()?.request_rx.lock().take()
        };
        rx.map(UnboundedReceiverStream::new)
    }
}

fn jni_err(e: jni::errors::Error) -> BlewError {
    BlewError::Internal(format!("JNI error: {e}"))
}

/// Convert blew CharacteristicProperties to Android BluetoothGattCharacteristic property bits.
fn blew_props_to_android(props: CharacteristicProperties) -> i32 {
    let mut out = 0i32;
    if props.contains(CharacteristicProperties::BROADCAST) {
        out |= 0x01; // PROPERTY_BROADCAST
    }
    if props.contains(CharacteristicProperties::READ) {
        out |= 0x02; // PROPERTY_READ
    }
    if props.contains(CharacteristicProperties::WRITE_WITHOUT_RESPONSE) {
        out |= 0x04; // PROPERTY_WRITE_NO_RESPONSE
    }
    if props.contains(CharacteristicProperties::WRITE) {
        out |= 0x08; // PROPERTY_WRITE
    }
    if props.contains(CharacteristicProperties::NOTIFY) {
        out |= 0x10; // PROPERTY_NOTIFY
    }
    if props.contains(CharacteristicProperties::INDICATE) {
        out |= 0x20; // PROPERTY_INDICATE
    }
    out
}

/// Convert blew AttributePermissions to Android permission bits.
fn blew_perms_to_android(perms: crate::gatt::props::AttributePermissions) -> i32 {
    use crate::gatt::props::AttributePermissions;
    let mut out = 0i32;
    if perms.contains(AttributePermissions::READ) {
        out |= 0x01; // PERMISSION_READ
    }
    if perms.contains(AttributePermissions::WRITE) {
        out |= 0x10; // PERMISSION_WRITE
    }
    if perms.contains(AttributePermissions::READ_ENCRYPTED) {
        out |= 0x02; // PERMISSION_READ_ENCRYPTED
    }
    if perms.contains(AttributePermissions::WRITE_ENCRYPTED) {
        out |= 0x20; // PERMISSION_WRITE_ENCRYPTED
    }
    out
}
