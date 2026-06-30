//! Translates `blew` events into `PeerCommand`s on the registry inbox.

use std::sync::Arc;

use blew::central::CentralEvent;
use blew::peripheral::{PeripheralRequest, PeripheralStateEvent};
use blew::{Central, Peripheral};
use bytes::Bytes;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::transport::peer::{KEY_PREFIX_LEN, KeyPrefix, PeerCommand};
use crate::transport::routing::{DiscoveryUpdate, TransportRouting};

/// Extract the 12-byte key prefix from a list of advertised service UUIDs.
///
/// Returns `Some(prefix)` if any UUID starts with the iroh magic prefix
/// `69 72 6f 00`, indicating an iroh-ble peer. The remaining 12 bytes of
/// that UUID carry the first 12 bytes of the peer's Ed25519 public key.
pub fn extract_prefix_from_services(services: &[Uuid]) -> Option<KeyPrefix> {
    for svc in services {
        let bytes = svc.as_bytes();
        if bytes[0..4] == [0x69, 0x72, 0x6f, 0x00] {
            let mut out = [0u8; KEY_PREFIX_LEN];
            out.copy_from_slice(&bytes[4..16]);
            return Some(out);
        }
    }
    None
}

/// Drive the central event stream, translating each event into a `PeerCommand`
/// and forwarding it to `inbox`. Returns when the stream ends or `inbox` is
/// closed.
pub async fn run_central_events(
    central: Arc<Central>,
    routing: Arc<TransportRouting>,
    inbox: mpsc::Sender<PeerCommand>,
) {
    use tokio_stream::StreamExt as _;
    let mut events = central.events();
    while let Some(ev) = events.next().await {
        let cmd = match ev {
            CentralEvent::DeviceDiscovered(device) => {
                let Some(prefix) = extract_prefix_from_services(&device.services) else {
                    continue;
                };
                let rssi = device.rssi;
                match routing.note_discovery(prefix, device.id.clone()) {
                    DiscoveryUpdate::Unchanged => {}
                    DiscoveryUpdate::New => {
                        tracing::debug!(
                            device = %device.id,
                            ?prefix,
                            rssi,
                            "central DeviceDiscovered (iroh peer)"
                        );
                    }
                    DiscoveryUpdate::Replaced { previous } => {
                        tracing::debug!(
                            device = %device.id,
                            previous = %previous,
                            ?prefix,
                            rssi,
                            "central DeviceDiscovered: prefix flipped to new DeviceId, evicting previous"
                        );
                        routing.forget_device(&previous);
                        if inbox
                            .send(PeerCommand::Forget {
                                device_id: previous,
                            })
                            .await
                            .is_err()
                        {
                            tracing::debug!(
                                "central event pump: inbox closed during forget, shutting down"
                            );
                            break;
                        }
                    }
                    DiscoveryUpdate::ActivelyPinned => {
                        tracing::trace!(
                            device = %device.id,
                            ?prefix,
                            rssi,
                            "ignoring scan: prefix is actively pinned to a different DeviceId"
                        );
                        continue;
                    }
                }
                PeerCommand::Advertised {
                    prefix,
                    device,
                    rssi,
                }
            }
            CentralEvent::DeviceConnected { device_id } => {
                tracing::debug!(device = %device_id, "central DeviceConnected");
                PeerCommand::CentralConnected { device_id }
            }
            CentralEvent::DeviceDisconnected { device_id, cause } => {
                tracing::debug!(device = %device_id, ?cause, "central DeviceDisconnected");
                PeerCommand::CentralDisconnected { device_id, cause }
            }
            CentralEvent::CharacteristicNotification {
                device_id,
                char_uuid: _,
                value,
            } => {
                tracing::trace!(
                    device = %device_id,
                    len = value.len(),
                    "central CharacteristicNotification received (P2C)"
                );
                PeerCommand::InboundGattFragment {
                    device_id,
                    source: crate::transport::peer::FragmentSource::CentralReceivedP2c,
                    bytes: value,
                }
            }
            CentralEvent::AdapterStateChanged { powered } => {
                PeerCommand::AdapterStateChanged { powered }
            }
        };
        if inbox.send(cmd).await.is_err() {
            tracing::debug!("central event pump: inbox closed, shutting down");
            break;
        }
    }
}

/// Drive the peripheral state-event stream (adapter power, subscription changes),
/// translating each event into a `PeerCommand` and forwarding it to `inbox`.
/// Returns when the stream ends or `inbox` is closed.
pub async fn run_peripheral_state_events(
    peripheral: Arc<Peripheral>,
    routing: Arc<TransportRouting>,
    inbox: mpsc::Sender<PeerCommand>,
) {
    use tokio_stream::StreamExt as _;
    let mut events = peripheral.state_events();
    while let Some(ev) = events.next().await {
        let cmd = match ev {
            PeripheralStateEvent::AdapterStateChanged { powered } => {
                PeerCommand::AdapterStateChanged { powered }
            }
            PeripheralStateEvent::SubscriptionChanged {
                client_id,
                char_uuid,
                subscribed,
            } => {
                tracing::debug!(
                    device = %client_id,
                    char = %char_uuid,
                    subscribed,
                    "peripheral SubscriptionChanged"
                );
                if subscribed {
                    let prefix = routing.prefix_for_device(&client_id);
                    PeerCommand::PeripheralClientSubscribed {
                        client_id,
                        char_uuid,
                        prefix,
                    }
                } else {
                    PeerCommand::PeripheralClientUnsubscribed {
                        client_id,
                        char_uuid,
                    }
                }
            }
        };
        if inbox.send(cmd).await.is_err() {
            tracing::debug!("peripheral state event pump: inbox closed, shutting down");
            break;
        }
    }
}

/// Drive the peripheral request stream (GATT reads/writes), translating each
/// request into a `PeerCommand` and forwarding it to `inbox` (or responding
/// inline for read-only characteristics owned by the transport).
///
/// `Peripheral::take_requests` is single-consumer; callers must ensure this
/// is invoked exactly once per `Peripheral`. The returned future completes
/// when the stream ends or `inbox` is closed. If the requests stream has
/// already been taken, logs an error and returns immediately.
pub async fn run_peripheral_requests(
    peripheral: Arc<Peripheral>,
    inbox: mpsc::Sender<PeerCommand>,
    psm: Option<u16>,
) {
    use tokio_stream::StreamExt as _;
    let Some(mut requests) = peripheral.take_requests() else {
        tracing::error!("peripheral requests stream already taken; no request pump will run");
        return;
    };
    while let Some(req) = requests.next().await {
        let cmd = match req {
            PeripheralRequest::Write {
                client_id,
                value,
                responder,
                ..
            } => {
                if let Some(r) = responder {
                    r.success();
                }
                tracing::trace!(
                    device = %client_id,
                    len = value.len(),
                    "peripheral WriteRequest received (C2P)"
                );
                PeerCommand::InboundGattFragment {
                    device_id: client_id,
                    source: crate::transport::peer::FragmentSource::PeripheralReceivedC2p,
                    bytes: Bytes::from(value),
                }
            }
            PeripheralRequest::Read {
                char_uuid,
                responder,
                ..
            } => {
                if char_uuid == crate::transport::transport::IROH_PSM_CHAR_UUID {
                    if let Some(psm_val) = psm {
                        responder.respond(psm_val.to_le_bytes().to_vec());
                    } else {
                        responder.respond(Vec::new());
                    }
                } else if char_uuid == crate::transport::transport::IROH_VERSION_CHAR_UUID {
                    responder.respond(vec![crate::transport::transport::PROTOCOL_VERSION]);
                } else {
                    responder.respond(Vec::new());
                }
                continue;
            }
        };
        if inbox.send(cmd).await.is_err() {
            tracing::debug!("peripheral request pump: inbox closed, shutting down");
            break;
        }
    }
}

pub async fn run_l2cap_accept(
    mut listener: impl tokio_stream::Stream<
        Item = blew::error::BlewResult<(blew::DeviceId, blew::L2capChannel)>,
    > + Send
    + Unpin
    + 'static,
    inbox: mpsc::Sender<PeerCommand>,
) {
    use tokio_stream::StreamExt as _;
    while let Some(result) = listener.next().await {
        match result {
            Ok((device_id, channel)) => {
                tracing::debug!(device = %device_id, "L2CAP accept: incoming channel");
                let cmd = PeerCommand::InboundL2capChannel { device_id, channel };
                if inbox.send(cmd).await.is_err() {
                    tracing::debug!("l2cap accept loop: inbox closed, shutting down");
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "L2CAP accept error, continuing");
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::uuid;

    #[test]
    fn extract_prefix_happy_path() {
        // UUID with iroh magic prefix + 12 key bytes (0x01..=0x0c)
        let svc = uuid!("69726f00-0102-0304-0506-070809100b0c");
        let prefix = extract_prefix_from_services(&[svc]).unwrap();
        assert_eq!(prefix, svc.as_bytes()[4..16]);
    }

    #[test]
    fn extract_prefix_no_match() {
        let svc = uuid!("12345678-1234-1234-1234-123456789abc");
        assert!(extract_prefix_from_services(&[svc]).is_none());
    }

    #[test]
    fn extract_prefix_empty() {
        assert!(extract_prefix_from_services(&[]).is_none());
    }
}
