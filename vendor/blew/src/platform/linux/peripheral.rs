use crate::error::{BlewError, BlewResult};
use crate::gatt::props::CharacteristicProperties;
use crate::gatt::service::GattService;
use crate::l2cap::{L2capChannel, types::Psm};
use crate::peripheral::backend::{self, PeripheralBackend};
use crate::peripheral::types::{
    AdvertisingConfig, PeripheralConfig, PeripheralRequest, PeripheralStateEvent, ReadResponder,
    WriteResponder,
};
use crate::platform::linux::l2cap::bridge_l2cap;
use crate::types::DeviceId;
use crate::util::BroadcastEventStream;
use bluer::adv::{Advertisement, SecondaryChannel, Type as AdvType};
use bluer::gatt::local::{
    Application, ApplicationHandle, Characteristic, CharacteristicControlHandle,
    CharacteristicNotifier, CharacteristicNotify, CharacteristicNotifyMethod, CharacteristicRead,
    CharacteristicReadRequest, CharacteristicWrite, CharacteristicWriteMethod,
    CharacteristicWriteRequest, ReqError, Service, ServiceControlHandle,
};
use bluer::{Adapter, Session};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// tokio::sync::Mutex so we can await `notify()` without holding a std MutexGuard
/// across the await point.
type SharedNotifier = Arc<tokio::sync::Mutex<CharacteristicNotifier>>;

struct PeripheralInner {
    _session: Session,
    adapter: Adapter,
    pending_services: Mutex<Vec<GattService>>,
    adv_handle: Mutex<Option<bluer::adv::AdvertisementHandle>>,
    app_handle: Mutex<Option<ApplicationHandle>>,
    notifiers: Mutex<HashMap<Uuid, Vec<SharedNotifier>>>,
    request_tx: mpsc::UnboundedSender<PeripheralRequest>,
    request_rx: Mutex<Option<mpsc::UnboundedReceiver<PeripheralRequest>>>,
    state_tx: broadcast::Sender<PeripheralStateEvent>,
    _adapter_task: tokio::task::JoinHandle<()>,
}

pub struct LinuxPeripheral(Arc<PeripheralInner>);

impl LinuxPeripheral {
    pub async fn with_config(_config: PeripheralConfig) -> BlewResult<Self> {
        <Self as PeripheralBackend>::new().await
    }
}

impl backend::private::Sealed for LinuxPeripheral {}

fn emit_state(inner: &Arc<PeripheralInner>, event: PeripheralStateEvent) {
    let _ = inner.state_tx.send(event);
}

fn emit_request(inner: &Arc<PeripheralInner>, request: PeripheralRequest) {
    let _ = inner.request_tx.send(request);
}

#[allow(clippy::too_many_lines)]
fn build_characteristic(
    ch: &crate::gatt::service::GattCharacteristic,
    svc_uuid: Uuid,
    inner: &Arc<PeripheralInner>,
) -> Characteristic {
    let uuid = ch.uuid;
    let props = ch.properties;

    let read = if props.contains(CharacteristicProperties::READ) {
        // Static value -- auto-respond without round-tripping through the event
        // handler (matches CoreBluetooth behaviour for characteristics with a
        // non-nil value).
        let static_value = if ch.value.is_empty() {
            None
        } else {
            Some(ch.value.clone())
        };

        let inner_r = Arc::clone(inner);
        Some(CharacteristicRead {
            read: true,
            fun: Box::new(move |req: CharacteristicReadRequest| {
                let inner_r = Arc::clone(&inner_r);
                let static_value = static_value.clone();
                Box::pin(async move {
                    if let Some(val) = static_value {
                        let offset = req.offset as usize;
                        return Ok(if offset > 0 && offset < val.len() {
                            val[offset..].to_vec()
                        } else {
                            val
                        });
                    }

                    let client_id = DeviceId(req.device_address.to_string());
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    emit_request(
                        &inner_r,
                        PeripheralRequest::Read {
                            client_id,
                            service_uuid: svc_uuid,
                            char_uuid: uuid,
                            offset: req.offset,
                            responder: ReadResponder::new(tx),
                        },
                    );
                    match rx.await {
                        Ok(Ok(value)) => Ok(value),
                        _ => Err(ReqError::Failed),
                    }
                })
            }),
            ..Default::default()
        })
    } else {
        None
    };

    let write = if props.intersects(
        CharacteristicProperties::WRITE | CharacteristicProperties::WRITE_WITHOUT_RESPONSE,
    ) {
        let inner_w = Arc::clone(inner);
        let write_req = props.contains(CharacteristicProperties::WRITE);
        let write_cmd = props.contains(CharacteristicProperties::WRITE_WITHOUT_RESPONSE);
        Some(CharacteristicWrite {
            write: write_req,
            write_without_response: write_cmd,
            method: CharacteristicWriteMethod::Fun(Box::new(
                move |value: Vec<u8>, req: CharacteristicWriteRequest| {
                    let inner_w = Arc::clone(&inner_w);
                    Box::pin(async move {
                        let client_id = DeviceId(req.device_address.to_string());
                        let (responder, rx) = if req.op_type == bluer::gatt::WriteOp::Request {
                            let (tx, rx) = tokio::sync::oneshot::channel::<bool>();
                            (Some(WriteResponder::new(tx)), Some(rx))
                        } else {
                            (None, None)
                        };
                        emit_request(
                            &inner_w,
                            PeripheralRequest::Write {
                                client_id,
                                service_uuid: svc_uuid,
                                char_uuid: uuid,
                                value,
                                responder,
                            },
                        );
                        if let Some(rx) = rx {
                            match rx.await {
                                Ok(true) => Ok(()),
                                _ => Err(ReqError::Failed),
                            }
                        } else {
                            Ok(())
                        }
                    })
                },
            )),
            ..Default::default()
        })
    } else {
        None
    };

    let notify = if props.contains(CharacteristicProperties::NOTIFY) {
        let inner_n = Arc::clone(inner);
        Some(CharacteristicNotify {
            notify: true,
            method: CharacteristicNotifyMethod::Fun(Box::new(
                move |notifier: CharacteristicNotifier| {
                    let inner_n = Arc::clone(&inner_n);
                    Box::pin(async move {
                        inner_n
                            .notifiers
                            .lock()
                            .entry(uuid)
                            .or_default()
                            .push(Arc::new(tokio::sync::Mutex::new(notifier)));
                        emit_state(
                            &inner_n,
                            PeripheralStateEvent::SubscriptionChanged {
                                client_id: DeviceId(String::new()),
                                char_uuid: uuid,
                                subscribed: true,
                            },
                        );
                    })
                },
            )),
            ..Default::default()
        })
    } else {
        None
    };

    Characteristic {
        uuid,
        handle: None,
        broadcast: false,
        writable_auxiliaries: false,
        authorize: false,
        descriptors: vec![],
        read,
        write,
        notify,
        control_handle: CharacteristicControlHandle::default(),
        _non_exhaustive: (),
    }
}

impl PeripheralBackend for LinuxPeripheral {
    type StateEvents = BroadcastEventStream<PeripheralStateEvent>;
    type Requests = UnboundedReceiverStream<PeripheralRequest>;

    async fn new() -> BlewResult<Self>
    where
        Self: Sized,
    {
        let session = Session::new().await.map_err(|e| BlewError::Peripheral {
            source: Box::new(e),
        })?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|_| BlewError::AdapterNotFound)?;
        debug!(adapter = %adapter.name(), "BLE adapter initialized");
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = broadcast::channel(256);
        let state_tx_clone = state_tx.clone();
        let adapter_clone = adapter.clone();
        let adapter_task = tokio::spawn(async move {
            use tokio_stream::StreamExt as _;
            let Ok(events) = adapter_clone.events().await else {
                warn!("failed to subscribe to adapter events");
                return;
            };
            let mut events = Box::pin(events);
            while let Some(event) = events.next().await {
                if let bluer::AdapterEvent::PropertyChanged(bluer::AdapterProperty::Powered(
                    powered,
                )) = event
                {
                    debug!(powered, "peripheral adapter state changed");
                    let _ =
                        state_tx_clone.send(PeripheralStateEvent::AdapterStateChanged { powered });
                }
            }
        });
        Ok(LinuxPeripheral(Arc::new(PeripheralInner {
            _session: session,
            adapter,
            pending_services: Mutex::new(Vec::new()),
            adv_handle: Mutex::new(None),
            app_handle: Mutex::new(None),
            notifiers: Mutex::new(HashMap::new()),
            request_tx,
            request_rx: Mutex::new(Some(request_rx)),
            state_tx,
            _adapter_task: adapter_task,
        })))
    }

    fn is_powered(&self) -> impl Future<Output = BlewResult<bool>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            handle
                .adapter
                .is_powered()
                .await
                .map_err(|e| BlewError::Peripheral {
                    source: Box::new(e),
                })
        }
    }

    fn add_service(&self, service: &GattService) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let service = service.clone();
        async move {
            debug!(service_uuid = %service.uuid, characteristics = service.characteristics.len(), "queuing GATT service");
            handle.pending_services.lock().push(service);
            Ok(())
        }
    }

    fn start_advertising(
        &self,
        config: &AdvertisingConfig,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        let config = config.clone();
        async move {
            if handle.adv_handle.lock().is_some() {
                return Err(BlewError::AlreadyAdvertising);
            }
            debug!(local_name = %config.local_name, "starting advertising");

            let pending: Vec<GattService> = handle.pending_services.lock().clone();
            let bluer_services: Vec<Service> = pending
                .iter()
                .map(|svc| {
                    let chars = svc
                        .characteristics
                        .iter()
                        .map(|ch| build_characteristic(ch, svc.uuid, &handle))
                        .collect();
                    Service {
                        uuid: svc.uuid,
                        handle: None,
                        primary: svc.primary,
                        characteristics: chars,
                        control_handle: ServiceControlHandle::default(),
                        _non_exhaustive: (),
                    }
                })
                .collect();

            let app = Application {
                services: bluer_services,
                _non_exhaustive: (),
            };
            let app_handle = handle
                .adapter
                .serve_gatt_application(app)
                .await
                .map_err(|e| BlewError::Peripheral {
                    source: Box::new(e),
                })?;
            *handle.app_handle.lock() = Some(app_handle);

            // Prefer BLE 5 extended advertising with a 2M secondary channel so
            // that BLE 5 centrals can connect at 2M PHY from the start.
            // Fall back to legacy advertising when the hardware or kernel
            // doesn't support extended advertising (BLE 4.x adapters).
            let make_adv = |secondary_channel| Advertisement {
                advertisement_type: AdvType::Peripheral,
                local_name: Some(config.local_name.clone()),
                service_uuids: config.service_uuids.clone().into_iter().collect(),
                manufacturer_data: config
                    .manufacturer_data
                    .iter()
                    .map(|(id, data)| (*id, data.clone()))
                    .collect(),
                secondary_channel,
                ..Default::default()
            };
            let adv_handle = match handle
                .adapter
                .advertise(make_adv(Some(SecondaryChannel::TwoM)))
                .await
            {
                Ok(h) => {
                    debug!("advertising started (BLE 5 extended)");
                    h
                }
                Err(e) => {
                    warn!(error = %e, "BLE 5 extended advertising unavailable, falling back to legacy");
                    let h = handle
                        .adapter
                        .advertise(make_adv(None))
                        .await
                        .map_err(|e| BlewError::Peripheral {
                            source: Box::new(e),
                        })?;
                    debug!("advertising started (legacy)");
                    h
                }
            };
            *handle.adv_handle.lock() = Some(adv_handle);

            Ok(())
        }
    }

    fn stop_advertising(&self) -> impl Future<Output = BlewResult<()>> + Send {
        let handle = Arc::clone(&self.0);
        async move {
            debug!("stopping advertising");
            handle.adv_handle.lock().take();
            handle.app_handle.lock().take();
            handle.notifiers.lock().clear();
            Ok(())
        }
    }

    fn notify_characteristic(
        &self,
        _device_id: &crate::types::DeviceId,
        char_uuid: Uuid,
        value: Vec<u8>,
    ) -> impl Future<Output = BlewResult<()>> + Send {
        // NOTE: BlueZ's `CharacteristicNotifier` callback does not expose the
        // remote device identity, so we cannot route a notification to a
        // specific subscriber here. Every live notifier for the characteristic
        // receives the value. See the trait doc for details.
        let handle = Arc::clone(&self.0);
        async move {
            trace!(%char_uuid, len = value.len(), "notifying characteristic");
            // Collect live notifiers without holding the outer Mutex across awaits.
            let arcs: Vec<SharedNotifier> = handle
                .notifiers
                .lock()
                .get(&char_uuid)
                .cloned()
                .unwrap_or_default();

            let mut any_stopped = false;
            for arc in arcs {
                let mut notifier = arc.lock().await;
                if notifier.is_stopped() {
                    any_stopped = true;
                    continue;
                }
                // Best-effort: ignore errors on individual notifiers.
                let _ = notifier.notify(value.clone()).await;
            }

            if any_stopped {
                handle.notifiers.lock().entry(char_uuid).and_modify(|v| {
                    v.retain(|arc| !arc.try_lock().map_or(true, |n| n.is_stopped()));
                });
            }
            Ok(())
        }
    }

    async fn l2cap_listener(
        &self,
    ) -> BlewResult<(
        Psm,
        impl futures_core::Stream<Item = BlewResult<(DeviceId, L2capChannel)>> + Send + 'static,
    )> {
        debug!("starting L2CAP CoC listener");
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
        let listener = socket.listen(1).map_err(|e| BlewError::L2cap {
            source: Box::new(e),
        })?;
        let local_addr = listener
            .as_ref()
            .local_addr()
            .map_err(|e| BlewError::L2cap {
                source: Box::new(e),
            })?;
        let psm = Psm(local_addr.psm);
        debug!(psm = psm.0, "L2CAP listener ready");

        let (tx, rx) = mpsc::channel::<BlewResult<(DeviceId, L2capChannel)>>(16);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!(peer = ?addr, "incoming L2CAP connection accepted");
                        let device_id = DeviceId(addr.addr.to_string());
                        if tx
                            .send(Ok((device_id, bridge_l2cap(stream))))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "L2CAP accept error");
                        let _ = tx
                            .send(Err(BlewError::L2cap {
                                source: Box::new(e),
                            }))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok((psm, ReceiverStream::new(rx)))
    }

    fn state_events(&self) -> Self::StateEvents {
        BroadcastEventStream::new(self.0.state_tx.subscribe())
    }

    fn take_requests(&self) -> Option<Self::Requests> {
        self.0
            .request_rx
            .lock()
            .take()
            .map(UnboundedReceiverStream::new)
    }
}
