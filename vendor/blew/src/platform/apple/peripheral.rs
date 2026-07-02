//! Apple (macOS / iOS) implementation of [`PeripheralBackend`].
//!
//! Architecture:
//! - A dedicated GCD serial queue receives all `CBPeripheralManager` delegate callbacks.
//! - GATT service/characteristic mutable objects are retained so we can push
//!   notifications and respond to read/write requests.
//! - RAII [`ReadResponder`] / [`WriteResponder`] carry ATT responses back to the
//!   CB queue via Tokio oneshot channels + background tasks.

#![allow(
    non_snake_case,
    clippy::too_many_arguments,
    clippy::cast_possible_truncation,
    unsafe_op_in_unsafe_fn
)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use dispatch2::{DispatchQueue, DispatchQueueAttr};
use futures_core::Stream;
use objc2::define_class;
use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObject, ProtocolObject};
use objc2::{AnyThread, DefinedClass};
#[cfg(target_os = "ios")]
use objc2_core_bluetooth::CBPeripheralManagerOptionRestoreIdentifierKey;
use objc2_core_bluetooth::CBPeripheralManagerRestoredStateServicesKey;
use objc2_core_bluetooth::{
    CBATTError, CBATTRequest, CBAdvertisementDataLocalNameKey, CBAdvertisementDataServiceUUIDsKey,
    CBAttributePermissions, CBCentral, CBCharacteristic, CBCharacteristicProperties,
    CBL2CAPChannel, CBL2CAPPSM, CBManagerState, CBMutableCharacteristic, CBMutableService,
    CBPeripheralManager, CBPeripheralManagerConnectionLatency, CBPeripheralManagerDelegate,
    CBService, CBUUID,
};
use objc2_foundation::{NSArray, NSData, NSDictionary, NSError, NSObjectProtocol, NSString};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use tracing::{debug, trace, warn};

use crate::error::{BlewError, BlewResult};
use crate::gatt::props::{AttributePermissions, CharacteristicProperties};
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::peripheral::backend::{self, PeripheralBackend};
use crate::peripheral::types::{
    AdvertisingConfig, PeripheralConfig, PeripheralRequest, PeripheralStateEvent, ReadResponder,
    WriteResponder,
};
use crate::platform::apple::helpers::{
    ObjcSend, cbuuid_to_uuid, central_device_id, retain_send, uuid_to_cbuuid,
};
use crate::platform::apple::l2cap::bridge_l2cap_channel;
use crate::types::DeviceId;
use crate::util::BroadcastEventStream;

fn our_props_to_cb(props: CharacteristicProperties) -> CBCharacteristicProperties {
    let mut out = CBCharacteristicProperties(0);
    if props.contains(CharacteristicProperties::BROADCAST) {
        out |= CBCharacteristicProperties::Broadcast;
    }
    if props.contains(CharacteristicProperties::READ) {
        out |= CBCharacteristicProperties::Read;
    }
    if props.contains(CharacteristicProperties::WRITE_WITHOUT_RESPONSE) {
        out |= CBCharacteristicProperties::WriteWithoutResponse;
    }
    if props.contains(CharacteristicProperties::WRITE) {
        out |= CBCharacteristicProperties::Write;
    }
    if props.contains(CharacteristicProperties::NOTIFY) {
        out |= CBCharacteristicProperties::Notify;
    }
    if props.contains(CharacteristicProperties::INDICATE) {
        out |= CBCharacteristicProperties::Indicate;
    }
    out
}

fn our_perms_to_cb(perms: AttributePermissions) -> CBAttributePermissions {
    let mut out = CBAttributePermissions(0);
    if perms.contains(AttributePermissions::READ) {
        out |= CBAttributePermissions::Readable;
    }
    if perms.contains(AttributePermissions::WRITE) {
        out |= CBAttributePermissions::Writeable;
    }
    if perms.contains(AttributePermissions::READ_ENCRYPTED) {
        out |= CBAttributePermissions::ReadEncryptionRequired;
    }
    if perms.contains(AttributePermissions::WRITE_ENCRYPTED) {
        out |= CBAttributePermissions::WriteEncryptionRequired;
    }
    out
}

struct PeripheralInner {
    /// `CBMutableCharacteristic` objects keyed by UUID, for notification sending.
    chars: Mutex<HashMap<Uuid, ObjcSend<CBMutableCharacteristic>>>,
    /// Retained `CBCentral` handles, keyed by (characteristic UUID, device id),
    /// populated by `didSubscribeToCharacteristic` and cleared by
    /// `didUnsubscribeFromCharacteristic`. Used by `notify_characteristic` to
    /// target a single central rather than broadcasting.
    subscribers: Mutex<HashMap<Uuid, HashMap<DeviceId, ObjcSend<CBCentral>>>>,
    /// Pending `start_advertising()` result.
    adv_tx: Mutex<Option<oneshot::Sender<BlewResult<()>>>>,
    /// Pending `add_service()` results.
    add_svc_tx: Mutex<HashMap<Uuid, oneshot::Sender<BlewResult<()>>>>,
    /// Inbound GATT requests. The receiver is handed out at most once via
    /// [`PeripheralBackend::take_requests`].
    request_tx: mpsc::UnboundedSender<PeripheralRequest>,
    request_rx: Mutex<Option<mpsc::UnboundedReceiver<PeripheralRequest>>>,
    /// Broadcast sender for clone-able state events (adapter power, subscription changes).
    state_tx: broadcast::Sender<PeripheralStateEvent>,
    /// Populated once by `willRestoreState:`; drained exactly once via
    /// [`PeripheralBackend::take_restored`]. Buffered so callers can observe the
    /// restored service list after construction returns — broadcast delivery
    /// would race the callback.
    restored: Mutex<Option<Vec<Uuid>>>,
    /// Powered state watch.
    powered_tx: watch::Sender<bool>,
    /// Result of `publishL2CAPChannelWithEncryption` -- carries the assigned PSM.
    l2cap_publish_tx: Mutex<Option<oneshot::Sender<BlewResult<Psm>>>>,
    /// Sender for incoming L2CAP channels (set by `l2cap_listener`). Unbounded so
    /// the GCD delegate queue is never blocked by a slow accept-stream consumer.
    #[allow(clippy::type_complexity)]
    l2cap_channel_tx: Mutex<Option<mpsc::UnboundedSender<BlewResult<(DeviceId, L2capChannel)>>>>,
    /// Tokio runtime handle, captured at construction time so GCD callbacks
    /// (which run off the Tokio thread) can spawn tasks onto the runtime.
    runtime: Handle,
}

impl PeripheralInner {
    fn new() -> (Arc<Self>, watch::Receiver<bool>) {
        let (powered_tx, powered_rx) = watch::channel(false);
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = broadcast::channel(256);
        let inner = Arc::new(Self {
            chars: Default::default(),
            subscribers: Default::default(),
            adv_tx: Default::default(),
            add_svc_tx: Default::default(),
            request_tx,
            request_rx: Mutex::new(Some(request_rx)),
            state_tx,
            restored: Mutex::new(None),
            powered_tx,
            l2cap_publish_tx: Mutex::new(None),
            l2cap_channel_tx: Mutex::new(None),
            runtime: Handle::current(),
        });
        (inner, powered_rx)
    }

    fn emit_state(&self, event: PeripheralStateEvent) {
        let _ = self.state_tx.send(event);
    }

    fn emit_request(&self, request: PeripheralRequest) {
        let _ = self.request_tx.send(request);
    }
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements.
    #[unsafe(super(NSObject))]
    #[name = "BlewPeripheralDelegate"]
    #[ivars = Arc<PeripheralInner>]
    struct PeripheralDelegate;

    unsafe impl NSObjectProtocol for PeripheralDelegate {}

    unsafe impl CBPeripheralManagerDelegate for PeripheralDelegate {
        #[unsafe(method(peripheralManagerDidUpdateState:))]
        unsafe fn peripheralManagerDidUpdateState(&self, peripheral: &CBPeripheralManager) {
            let powered = unsafe { peripheral.state() } == CBManagerState::PoweredOn;
            debug!(powered, "peripheral adapter state changed");
            let inner = self.ivars();
            let _ = inner.powered_tx.send(powered);
            inner.emit_state(PeripheralStateEvent::AdapterStateChanged { powered });
        }

        #[unsafe(method(peripheralManager:willRestoreState:))]
        unsafe fn peripheralManager_willRestoreState(
            &self,
            _peripheral: &CBPeripheralManager,
            dict: &NSDictionary<NSString, AnyObject>,
        ) {
            let inner = self.ivars();
            let key = unsafe { CBPeripheralManagerRestoredStateServicesKey };
            let Some(obj) = dict.objectForKey(key) else {
                *inner.restored.lock() = Some(Vec::new());
                return;
            };
            // SAFETY: CoreBluetooth guarantees this key's value is NSArray<CBMutableService>.
            let services: Retained<NSArray<CBMutableService>> = Retained::cast_unchecked(obj);
            let mut restored_uuids = Vec::new();
            {
                let mut chars_lock = inner.chars.lock();
                for service in services.to_vec() {
                    let svc_uuid_ret = service.UUID();
                    let Some(svc_uuid) = cbuuid_to_uuid(&svc_uuid_ret) else {
                        continue;
                    };
                    if let Some(chars) = unsafe { service.characteristics() } {
                        for ch in chars.to_vec() {
                            let ch_uuid_ret = ch.UUID();
                            let Some(ch_uuid) = cbuuid_to_uuid(&ch_uuid_ret) else {
                                continue;
                            };
                            // SAFETY: services we publish are always built from CBMutableCharacteristic,
                            // so the runtime class matches.
                            let mutable: &CBMutableCharacteristic =
                                unsafe { &*(&raw const *ch).cast() };
                            chars_lock.insert(ch_uuid, unsafe { retain_send(mutable) });
                        }
                    }
                    restored_uuids.push(svc_uuid);
                }
            }
            debug!(
                count = restored_uuids.len(),
                "OS-level state restoration recovered peripheral services"
            );
            *inner.restored.lock() = Some(restored_uuids);
        }

        #[unsafe(method(peripheralManagerDidStartAdvertising:error:))]
        unsafe fn peripheralManagerDidStartAdvertising_error(
            &self,
            _peripheral: &CBPeripheralManager,
            error: Option<&NSError>,
        ) {
            let inner = self.ivars();
            if let Some(tx) = inner.adv_tx.lock().take() {
                let result = error.map_or_else(
                    || {
                        debug!("advertising started");
                        Ok(())
                    },
                    |e| {
                        warn!(error = %e.localizedDescription(), "advertising failed to start");
                        Err(BlewError::Internal(e.localizedDescription().to_string()))
                    },
                );
                let _ = tx.send(result);
            }
        }

        #[unsafe(method(peripheralManager:didAddService:error:))]
        unsafe fn peripheralManager_didAddService_error(
            &self,
            _peripheral: &CBPeripheralManager,
            service: &CBService,
            error: Option<&NSError>,
        ) {
            let inner = self.ivars();
            let svc_uuid_ret = service.UUID();
            let Some(svc_uuid) = cbuuid_to_uuid(&svc_uuid_ret) else {
                return;
            };
            if let Some(tx) = inner.add_svc_tx.lock().remove(&svc_uuid) {
                let result = error.map_or_else(
                    || {
                        debug!(service_uuid = %svc_uuid, "GATT service added");
                        Ok(())
                    },
                    |e| {
                        warn!(service_uuid = %svc_uuid, error = %e.localizedDescription(), "failed to add GATT service");
                        Err(BlewError::Internal(e.localizedDescription().to_string()))
                    },
                );
                let _ = tx.send(result);
            }
        }

        #[unsafe(method(peripheralManager:central:didSubscribeToCharacteristic:))]
        unsafe fn peripheralManager_central_didSubscribeToCharacteristic(
            &self,
            _peripheral: &CBPeripheralManager,
            central: &CBCentral,
            characteristic: &CBCharacteristic,
        ) {
            let inner = self.ivars();
            let char_uuid_ret = characteristic.UUID();
            let Some(char_uuid) = cbuuid_to_uuid(&char_uuid_ret) else {
                return;
            };
            let client_id = central_device_id(central);
            trace!(client_id = %client_id, %char_uuid, "client subscribed to characteristic");
            {
                let mut subs = inner.subscribers.lock();
                let entry = subs.entry(char_uuid).or_default();
                entry.insert(client_id.clone(), unsafe { retain_send(central) });
            }
            inner.emit_state(PeripheralStateEvent::SubscriptionChanged {
                client_id,
                char_uuid,
                subscribed: true,
            });
        }

        #[unsafe(method(peripheralManager:central:didUnsubscribeFromCharacteristic:))]
        unsafe fn peripheralManager_central_didUnsubscribeFromCharacteristic(
            &self,
            _peripheral: &CBPeripheralManager,
            central: &CBCentral,
            characteristic: &CBCharacteristic,
        ) {
            let inner = self.ivars();
            let char_uuid_ret = characteristic.UUID();
            let Some(char_uuid) = cbuuid_to_uuid(&char_uuid_ret) else {
                return;
            };
            let client_id = central_device_id(central);
            trace!(client_id = %client_id, %char_uuid, "client unsubscribed from characteristic");
            {
                let mut subs = inner.subscribers.lock();
                if let Some(entry) = subs.get_mut(&char_uuid) {
                    entry.remove(&client_id);
                    if entry.is_empty() {
                        subs.remove(&char_uuid);
                    }
                }
            }
            inner.emit_state(PeripheralStateEvent::SubscriptionChanged {
                client_id,
                char_uuid,
                subscribed: false,
            });
        }

        #[unsafe(method(peripheralManager:didReceiveReadRequest:))]
        unsafe fn peripheralManager_didReceiveReadRequest(
            &self,
            peripheral: &CBPeripheralManager,
            request: &CBATTRequest,
        ) {
            let inner = self.ivars();
            let req_char = request.characteristic();
            let req_char_uuid = req_char.UUID();
            let Some(char_uuid) = cbuuid_to_uuid(&req_char_uuid) else {
                peripheral.respondToRequest_withResult(request, CBATTError::AttributeNotFound);
                return;
            };

            let service_uuid = req_char
                .service()
                .and_then(|s| { let u = s.UUID(); cbuuid_to_uuid(&u) })
                .unwrap_or(Uuid::nil());

            let req_central = request.central();
            let client_id = central_device_id(&req_central);
            let offset = request.offset() as u16;

            trace!(client_id = %client_id, %char_uuid, offset, "ATT read request");

            let (tx, rx) = oneshot::channel::<Result<Vec<u8>, ()>>();
            let responder = ReadResponder::new(tx);

            inner.emit_request(PeripheralRequest::Read {
                client_id,
                service_uuid,
                char_uuid,
                offset,
                responder,
            });

            // Spawn a task to relay the ATT response back to CoreBluetooth.
            // Must use the captured runtime handle because this callback fires
            // on the GCD queue, outside the Tokio runtime context.
            let request_retained = unsafe { retain_send(request) };
            let manager_retained = unsafe { retain_send(peripheral) };
            inner.runtime.spawn(async move {
                match rx.await {
                    Ok(Ok(data)) => unsafe {
                        let nsdata = NSData::from_vec(data);
                        request_retained.setValue(Some(&nsdata));
                        manager_retained.respondToRequest_withResult(
                            &request_retained,
                            CBATTError::Success,
                        );
                    },
                    _ => unsafe {
                        manager_retained.respondToRequest_withResult(
                            &request_retained,
                            CBATTError::AttributeNotFound,
                        );
                    },
                }
            });
        }

        #[unsafe(method(peripheralManager:didReceiveWriteRequests:))]
        unsafe fn peripheralManager_didReceiveWriteRequests(
            &self,
            peripheral: &CBPeripheralManager,
            requests: &NSArray<CBATTRequest>,
        ) {
            let inner = self.ivars();

            if requests.count() == 0 {
                return;
            }
            let request = requests.objectAtIndex(0);

            let req_char = request.characteristic();
            let req_char_uuid = req_char.UUID();
            let Some(char_uuid) = cbuuid_to_uuid(&req_char_uuid) else {
                peripheral.respondToRequest_withResult(&request, CBATTError::AttributeNotFound);
                return;
            };

            let service_uuid = req_char
                .service()
                .and_then(|s| { let u = s.UUID(); cbuuid_to_uuid(&u) })
                .unwrap_or(Uuid::nil());

            let req_central = request.central();
            let client_id = central_device_id(&req_central);
            let value = request.value().map(|d| d.to_vec()).unwrap_or_default();

            trace!(client_id = %client_id, %char_uuid, len = value.len(), "ATT write request");

            let (tx, rx) = oneshot::channel::<bool>();
            let responder = WriteResponder::new(tx);

            inner.emit_request(PeripheralRequest::Write {
                client_id,
                service_uuid,
                char_uuid,
                value,
                responder: Some(responder),
            });

            let request_retained = unsafe { retain_send(&*request) };
            let manager_retained = unsafe { retain_send(peripheral) };
            inner.runtime.spawn(async move {
                let success = rx.await.unwrap_or(false);
                let result = if success {
                    CBATTError::Success
                } else {
                    CBATTError::WriteNotPermitted
                };
                unsafe {
                    manager_retained.respondToRequest_withResult(&request_retained, result);
                };
            });
        }

        /// Fires when `publishL2CAPChannelWithEncryption` completes.
        /// Delivers the OS-assigned PSM (or an error) to the waiting `l2cap_listener` call.
        #[unsafe(method(peripheralManager:didPublishL2CAPChannel:error:))]
        unsafe fn peripheralManager_didPublishL2CAPChannel_error(
            &self,
            _peripheral: &CBPeripheralManager,
            PSM: CBL2CAPPSM,
            error: Option<&NSError>,
        ) {
            let inner = self.ivars();
            if let Some(tx) = inner.l2cap_publish_tx.lock().take() {
                let result = if let Some(e) = error {
                    warn!(error = %e.localizedDescription(), "L2CAP channel publish failed");
                    Err(BlewError::Internal(e.localizedDescription().to_string()))
                } else {
                    debug!(psm = PSM, "L2CAP channel published");
                    Ok(Psm(PSM))
                };
                let _ = tx.send(result);
            }
        }

        /// Fires when a central opens an L2CAP channel to us.
        #[unsafe(method(peripheralManager:didOpenL2CAPChannel:error:))]
        unsafe fn peripheralManager_didOpenL2CAPChannel_error(
            &self,
            manager: &CBPeripheralManager,
            channel: Option<&CBL2CAPChannel>,
            error: Option<&NSError>,
        ) {
            let inner = self.ivars();
            let tx = inner.l2cap_channel_tx.lock().clone();
            let Some(tx) = tx else { return };

            if let Some(e) = error {
                warn!(error = %e.localizedDescription(), "incoming L2CAP channel failed");
                let _ = tx.send(Err(BlewError::Internal(
                    e.localizedDescription().to_string(),
                )));
                return;
            }
            let Some(ch) = channel else { return };
            debug!("incoming L2CAP channel accepted");

            // Request low-latency connection parameters for higher throughput.
            // On the peripheral side the channel peer is always CBCentral.
            let device_id = if let Some(peer) = ch.peer() {
                let central: Retained<CBCentral> = Retained::cast_unchecked(peer);
                manager.setDesiredConnectionLatency_forCentral(
                    CBPeripheralManagerConnectionLatency::Low,
                    &central,
                );
                central_device_id(&central)
            } else {
                DeviceId::from("unknown")
            };

            let l2cap = bridge_l2cap_channel(ch, &inner.runtime);
            let _ = tx.send(Ok((device_id, l2cap)));
        }
    }
);

impl PeripheralDelegate {
    fn new(inner: Arc<PeripheralInner>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(inner);
        unsafe { objc2::msg_send![super(this), init] }
    }
}

struct PeripheralHandle {
    manager: ObjcSend<CBPeripheralManager>,
    /// Held here so the CB manager's weak-ref delegate stays alive.
    _delegate: ObjcSend<PeripheralDelegate>,
    inner: Arc<PeripheralInner>,
}

unsafe impl Send for PeripheralHandle {}
unsafe impl Sync for PeripheralHandle {}

pub struct ApplePeripheral(Arc<PeripheralHandle>);

impl ApplePeripheral {
    pub async fn with_config(
        #[cfg_attr(not(target_os = "ios"), allow(unused))] config: PeripheralConfig,
    ) -> BlewResult<Self> {
        let (inner, mut powered_rx) = PeripheralInner::new();
        let delegate = PeripheralDelegate::new(Arc::clone(&inner));

        let queue = DispatchQueue::new("blew.peripheral", DispatchQueueAttr::SERIAL);

        // State restoration (CBPeripheralManagerOptionRestoreIdentifierKey) is iOS-only;
        // passing it on macOS causes CoreBluetooth to throw an NSException.
        #[cfg(target_os = "ios")]
        let manager = ObjcSend(unsafe {
            if let Some(ref id) = config.restore_identifier {
                let key: &NSString = CBPeripheralManagerOptionRestoreIdentifierKey;
                let value = NSString::from_str(id);
                let v_any: &AnyObject = &value;
                let options = NSDictionary::from_slices(&[key], &[v_any]);
                CBPeripheralManager::initWithDelegate_queue_options(
                    CBPeripheralManager::alloc(),
                    Some(ProtocolObject::from_ref(&*delegate)),
                    Some(&queue),
                    Some(&options),
                )
            } else {
                CBPeripheralManager::initWithDelegate_queue(
                    CBPeripheralManager::alloc(),
                    Some(ProtocolObject::from_ref(&*delegate)),
                    Some(&queue),
                )
            }
        });
        #[cfg(not(target_os = "ios"))]
        let manager = ObjcSend(unsafe {
            CBPeripheralManager::initWithDelegate_queue(
                CBPeripheralManager::alloc(),
                Some(ProtocolObject::from_ref(&*delegate)),
                Some(&queue),
            )
        });
        let delegate = ObjcSend(delegate);

        let timeout = tokio::time::sleep(std::time::Duration::from_secs(15));
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                _ = powered_rx.changed() => {
                    let state = unsafe { manager.state() };
                    if state == CBManagerState::PoweredOn {
                        break;
                    }
                    if state == CBManagerState::Unsupported
                        || state == CBManagerState::Unauthorized
                    {
                        return Err(BlewError::AdapterNotFound);
                    }
                    // Unknown / Resetting / PoweredOff -> keep waiting
                }
                () = &mut timeout => {
                    if unsafe { manager.state() } == CBManagerState::PoweredOn {
                        break;
                    }
                    return Err(BlewError::NotPowered);
                }
            }
        }

        let handle = Arc::new(PeripheralHandle {
            manager,
            _delegate: delegate,
            inner,
        });
        Ok(ApplePeripheral(handle))
    }
}

impl backend::private::Sealed for ApplePeripheral {}

impl PeripheralBackend for ApplePeripheral {
    type StateEvents = BroadcastEventStream<PeripheralStateEvent>;
    type Requests = UnboundedReceiverStream<PeripheralRequest>;

    async fn new() -> BlewResult<Self>
    where
        Self: Sized,
    {
        Self::with_config(PeripheralConfig::default()).await
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            let state = unsafe { handle.manager.state() };
            Ok(state == CBManagerState::PoweredOn)
        }
    }

    fn add_service(&self, service: &GattService) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let service = service.clone();
        async move {
            debug!(service_uuid = %service.uuid, characteristics = service.characteristics.len(), "adding GATT service");
            let rx = {
                let svc_uuid = uuid_to_cbuuid(service.uuid);
                let cb_service = unsafe {
                    CBMutableService::initWithType_primary(
                        CBMutableService::alloc(),
                        &svc_uuid,
                        service.primary,
                    )
                };

                let mut cb_chars: Vec<Retained<CBMutableCharacteristic>> = vec![];
                let mut char_map: HashMap<Uuid, ObjcSend<CBMutableCharacteristic>> = HashMap::new();

                for ch in &service.characteristics {
                    let c_uuid = uuid_to_cbuuid(ch.uuid);
                    let props = our_props_to_cb(ch.properties);
                    let perms = our_perms_to_cb(ch.permissions);

                    let value = if ch.value.is_empty() {
                        None
                    } else {
                        Some(NSData::from_vec(ch.value.clone()))
                    };

                    let cb_char = unsafe {
                        CBMutableCharacteristic::initWithType_properties_value_permissions(
                            CBMutableCharacteristic::alloc(),
                            &c_uuid,
                            props,
                            value.as_deref(),
                            perms,
                        )
                    };
                    let retained_char = unsafe { retain_send(&*cb_char) };
                    char_map.insert(ch.uuid, retained_char);
                    cb_chars.push(cb_char);
                }

                let retained_refs: Vec<&CBCharacteristic> = cb_chars
                    .iter()
                    .map(|c| c.as_ref() as &CBCharacteristic)
                    .collect();
                let char_array = NSArray::from_slice(&retained_refs);
                unsafe { cb_service.setCharacteristics(Some(&char_array)) };

                {
                    let mut lock = handle.inner.chars.lock();
                    lock.extend(char_map);
                }
                let (tx, rx) = oneshot::channel();
                {
                    let mut lock = handle.inner.add_svc_tx.lock();
                    lock.insert(service.uuid, tx);
                }

                unsafe { handle.manager.addService(&cb_service) };
                rx
                // All ObjC objects drop here, before .await
            };

            rx.await.unwrap_or(Err(BlewError::Internal(
                "add_service channel dropped".into(),
            )))
        }
    }

    fn start_advertising(
        &self,
        config: &AdvertisingConfig,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let config = config.clone();
        async move {
            if unsafe { handle.manager.isAdvertising() } {
                return Err(BlewError::AlreadyAdvertising);
            }
            debug!(local_name = %config.local_name, "starting advertising");

            let rx = {
                let local_name = NSString::from_str(&config.local_name);

                let service_uuids: Vec<Retained<CBUUID>> = config
                    .service_uuids
                    .iter()
                    .map(|u| uuid_to_cbuuid(*u))
                    .collect();
                let uuid_array = NSArray::from_retained_slice(&service_uuids);

                let key_name = unsafe { CBAdvertisementDataLocalNameKey };
                let key_uuids = unsafe { CBAdvertisementDataServiceUUIDsKey };

                let ln_any: &AnyObject = &local_name;
                let ua_any: &AnyObject = &uuid_array;

                let adv_data = NSDictionary::from_slices(&[key_name, key_uuids], &[ln_any, ua_any]);

                let (tx, rx) = oneshot::channel();
                *handle.inner.adv_tx.lock() = Some(tx);
                unsafe { handle.manager.startAdvertising(Some(&adv_data)) };
                rx
            };

            rx.await.unwrap_or(Err(BlewError::Internal(
                "start_advertising channel dropped".into(),
            )))
        }
    }

    fn stop_advertising(&self) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            debug!("stopping advertising");
            unsafe { handle.manager.stopAdvertising() };
            Ok(())
        }
    }

    fn notify_characteristic(
        &self,
        device_id: &DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let device_id = device_id.clone();
        async move {
            trace!(device = %device_id, %char_uuid, len = value.len(), "notifying characteristic");
            let cb_char = {
                let lock = handle.inner.chars.lock();
                lock.get(&char_uuid).map(|c| unsafe { retain_send(&**c) })
            };

            let Some(cb_char) = cb_char else {
                return Err(BlewError::LocalCharacteristicNotFound { char_uuid });
            };

            let cb_central = {
                let lock = handle.inner.subscribers.lock();
                lock.get(&char_uuid)
                    .and_then(|m| m.get(&device_id))
                    .map(|c| unsafe { retain_send(&**c) })
            };

            let Some(cb_central) = cb_central else {
                // Subscriber disappeared between our caller's decision and
                // now — treat as no-op rather than an error.
                return Ok(());
            };

            let data = NSData::from_vec(value);
            let centrals = NSArray::from_slice(&[cb_central.0.as_ref()]);
            unsafe {
                handle
                    .manager
                    .updateValue_forCharacteristic_onSubscribedCentrals(
                        &data,
                        &cb_char.0,
                        Some(&centrals),
                    );
            }
            Ok(())
        }
    }

    fn l2cap_listener(
        &self,
    ) -> impl Future<
        Output = BlewResult<(
            Psm,
            impl Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
        )>,
    > + Send {
        let handle = Arc::clone(&self.0);
        async move {
            debug!("publishing L2CAP CoC channel");
            let (ch_tx, ch_rx) = mpsc::unbounded_channel::<BlewResult<(DeviceId, L2capChannel)>>();
            let (pub_tx, pub_rx) = oneshot::channel::<BlewResult<Psm>>();
            {
                *handle.inner.l2cap_channel_tx.lock() = Some(ch_tx);
                *handle.inner.l2cap_publish_tx.lock() = Some(pub_tx);
                unsafe { handle.manager.publishL2CAPChannelWithEncryption(false) };
            }
            let psm = pub_rx.await.unwrap_or(Err(BlewError::Internal(
                "l2cap_publish channel dropped".into(),
            )))?;
            debug!(psm = psm.0, "L2CAP listener ready");
            Ok((psm, UnboundedReceiverStream::new(ch_rx)))
        }
    }

    fn state_events(&self) -> Self::StateEvents {
        BroadcastEventStream::new(self.0.inner.state_tx.subscribe())
    }

    fn take_requests(&self) -> Option<Self::Requests> {
        self.0
            .inner
            .request_rx
            .lock()
            .take()
            .map(UnboundedReceiverStream::new)
    }
}

impl ApplePeripheral {
    /// Consume the preserved-services payload captured from
    /// `peripheralManager:willRestoreState:` during `with_config`. Returns
    /// `None` after the first call.
    #[must_use]
    pub fn take_restored(&self) -> Option<Vec<Uuid>> {
        self.0.inner.restored.lock().take()
    }
}
