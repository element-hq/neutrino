//! Integration tests exercising the dedup state machine via MockFabric.

#![cfg(feature = "testing")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use std::task::Waker;

use arc_swap::ArcSwap;
use blew::{BleDevice, DeviceId};
use bytes::Bytes;
use parking_lot::Mutex;

use iroh_ble_transport::transport::{
    driver::{Driver, IncomingPacket},
    peer::{ConnectRole, PeerCommand},
    registry::{PhaseKind, Registry, SnapshotMaps},
    routing::{TransportRouting, prefix_from_endpoint},
    store::InMemoryPeerStore,
    test_util::{CallKind, MockBleInterface},
    transport::L2capPolicy,
};
use tokio::sync::mpsc;

fn zero_counters() -> (Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) {
    (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    )
}

fn ble_device(id: &DeviceId) -> BleDevice {
    BleDevice {
        id: id.clone(),
        name: None,
        rssi: None,
        services: vec![],
    }
}

fn waker_from_channel(tx: mpsc::Sender<()>) -> std::task::Waker {
    use std::task::{Wake, Waker};
    struct W(mpsc::Sender<()>);
    impl Wake for W {
        fn wake(self: Arc<Self>) {
            let _ = self.0.try_send(());
        }
    }
    Waker::from(Arc::new(W(tx)))
}

/// An endpoint with all bytes set to the given value — gives us a
/// controllable ordering for the dedup tiebreaker.
fn endpoint_with_byte(b: u8) -> iroh_base::EndpointId {
    let mut bytes = [0u8; 32];
    bytes[0] = b;
    iroh_base::SecretKey::from_bytes(&bytes).public()
}

struct TestNode {
    #[allow(dead_code)]
    device_id: DeviceId,
    inbox_tx: mpsc::Sender<PeerCommand>,
    snapshots: Arc<ArcSwap<SnapshotMaps>>,
    #[allow(dead_code)]
    iface: Arc<MockBleInterface>,
}

fn spawn_node_with_endpoint(
    device_id: DeviceId,
    inbox_tx: mpsc::Sender<PeerCommand>,
    inbox_rx: mpsc::Receiver<PeerCommand>,
    iface: Arc<MockBleInterface>,
    policy: L2capPolicy,
    endpoint: iroh_base::EndpointId,
) -> TestNode {
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(64);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();

    let driver = Driver::new(
        Arc::clone(&iface),
        inbox_tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let registry = Registry::new_for_test_with_policy_and_endpoint(policy, endpoint);
    let snap = snapshots.clone();
    let routing = Arc::new(TransportRouting::new());
    let wakers = Arc::new(Mutex::new(Vec::<Waker>::new()));
    tokio::spawn(async move {
        registry.run(inbox_rx, driver, snap, wakers, routing).await;
    });

    TestNode {
        device_id,
        inbox_tx,
        snapshots,
        iface,
    }
}

/// Wait until both nodes have exactly one peer in the given phase, then
/// return the summaries.
async fn wait_both_connected(a: &TestNode, b: &TestNode, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let a_snap = a.snapshots.load();
            let b_snap = b.snapshots.load();
            let a_connected = a_snap
                .peer_states
                .values()
                .filter(|s| s.phase_kind == PhaseKind::Connected)
                .count();
            let b_connected = b_snap
                .peer_states
                .values()
                .filter(|s| s.phase_kind == PhaseKind::Connected)
                .count();
            if a_connected >= 1 && b_connected >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("timed out waiting for both nodes to reach Connected");
}

// ─── Task 19 ─────────────────────────────────────────────────────────────────

/// Two nodes discover each other. Node A (higher EndpointId) dials as Central;
/// after both reach Connected and receive VerifiedEndpoint for their peer,
/// each side ends up with exactly one Connected peer. The dedup tiebreaker
/// keeps the Central entry on A's side and the Peripheral entry on B's side.
#[tokio::test(flavor = "multi_thread")]
async fn symmetric_dial_resolves_to_one_pipe_per_side() {
    // A has a higher endpoint byte → A's prefix > B's prefix →
    // A dials immediately; B defers via fairness window.
    let ep_a = endpoint_with_byte(0xFF);
    let ep_b = endpoint_with_byte(0x01);

    let dev_a = DeviceId::from("dedup-a");
    let dev_b = DeviceId::from("dedup-b");

    let (a_inbox_tx, a_inbox_rx) = mpsc::channel::<PeerCommand>(256);
    let (b_inbox_tx, b_inbox_rx) = mpsc::channel::<PeerCommand>(256);

    // The iface for A has its c2p hook route to B's inbox (as a GATT fragment),
    // and p2c hook for B routes to A's inbox. We set these up manually since
    // we need per-node ifaces with distinct endpoints.
    let iface_a = Arc::new(MockBleInterface::new());
    let iface_b = Arc::new(MockBleInterface::new());

    // Wire A's c2p writes → B's inbox as InboundGattFragment from dev_a.
    {
        let inbox = b_inbox_tx.clone();
        let from = dev_a.clone();
        iface_a.set_on_c2p_write(Box::new(move |_target, bytes| {
            let cmd = PeerCommand::InboundGattFragment {
                device_id: from.clone(),
                source: iroh_ble_transport::transport::peer::FragmentSource::PeripheralReceivedC2p,
                bytes,
            };
            let _ = inbox.try_send(cmd);
        }));
    }
    // Wire B's p2c notifies → A's inbox.
    {
        let inbox = a_inbox_tx.clone();
        let from = dev_b.clone();
        iface_b.set_on_p2c_notify(Box::new(move |_target, bytes| {
            let cmd = PeerCommand::InboundGattFragment {
                device_id: from.clone(),
                source: iroh_ble_transport::transport::peer::FragmentSource::CentralReceivedP2c,
                bytes,
            };
            let _ = inbox.try_send(cmd);
        }));
    }

    let a = spawn_node_with_endpoint(
        dev_a.clone(),
        a_inbox_tx.clone(),
        a_inbox_rx,
        iface_a,
        L2capPolicy::Disabled,
        ep_a,
    );
    let b = spawn_node_with_endpoint(
        dev_b.clone(),
        b_inbox_tx.clone(),
        b_inbox_rx,
        iface_b,
        L2capPolicy::Disabled,
        ep_b,
    );

    // A advertises B using B's actual endpoint-derived prefix. Since A's prefix
    // (from ep_a=0xFF) > B's prefix (from ep_b=0x01), A does NOT defer → dials.
    let prefix_b = prefix_from_endpoint(&ep_b);
    let prefix_a = prefix_from_endpoint(&ep_a);

    let (waker_tx, _) = mpsc::channel::<()>(1);
    let waker = waker_from_channel(waker_tx);

    a.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_b,
            device: ble_device(&dev_b),
            rssi: None,
        })
        .await
        .unwrap();
    a.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: dev_b.clone(),
            tx_gen: 0,
            datagram: Bytes::from_static(b"hello-from-a"),
            waker,
        })
        .await
        .unwrap();

    // B advertises A to itself (B's prefix is 0x01, A's prefix is 0xFF).
    // B's my_prefix=0x01 < A's prefix=0xFF → B would defer. However, since
    // we want to test the dedup path we also send a VerifiedEndpoint, and the
    // peripheral-side pipe gets materialised via InboundGattFragment when A
    // writes to B.
    b.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_a,
            device: ble_device(&dev_a),
            rssi: None,
        })
        .await
        .unwrap();

    // Wait for A to reach Connected with B (A connected as Central).
    wait_both_connected(&a, &b, Duration::from_secs(10)).await;

    // Send VerifiedEndpoint to both sides so the dedup pass can run.
    // On A: it has a Connected entry for dev_b. A's should_win(Central, ep_a, ep_b)
    // → ep_a > ep_b → keeps Central → no prune.
    // On B: it has a Connected entry for dev_a (materialised as Peripheral via
    // InboundGattFragment). B's should_win(Peripheral, ep_b, ep_a) → ep_b < ep_a
    // → keeps Peripheral → no prune.
    a.inbox_tx
        .send(PeerCommand::VerifiedEndpoint {
            endpoint_id: ep_b,
            token: None,
        })
        .await
        .unwrap();
    b.inbox_tx
        .send(PeerCommand::VerifiedEndpoint {
            endpoint_id: ep_a,
            token: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Assert: each side has exactly one Connected peer.
    let a_snap = a.snapshots.load();
    let b_snap = b.snapshots.load();

    let a_connected: Vec<_> = a_snap
        .peer_states
        .iter()
        .filter(|(_, s)| s.phase_kind == PhaseKind::Connected)
        .collect();
    let b_connected: Vec<_> = b_snap
        .peer_states
        .iter()
        .filter(|(_, s)| s.phase_kind == PhaseKind::Connected)
        .collect();

    assert_eq!(
        a_connected.len(),
        1,
        "A should have exactly one Connected peer; got {:?}",
        a_connected
    );
    assert_eq!(
        b_connected.len(),
        1,
        "B should have exactly one Connected peer; got {:?}",
        b_connected
    );

    // A's Connected entry must be the Central role (A has higher endpoint).
    let a_peer = a_snap
        .peer_states
        .get(&dev_b)
        .expect("A has no entry for B");
    assert_eq!(
        a_peer.phase_kind,
        PhaseKind::Connected,
        "A's entry for B should be Connected"
    );
    assert_eq!(
        a_peer.role,
        ConnectRole::Central,
        "A (higher endpoint) keeps the Central role after dedup"
    );

    // B's Connected entry must be the Peripheral role.
    let b_peer = b_snap
        .peer_states
        .get(&dev_a)
        .expect("B has no entry for A");
    assert_eq!(
        b_peer.phase_kind,
        PhaseKind::Connected,
        "B's entry for A should be Connected"
    );
    assert_eq!(
        b_peer.role,
        ConnectRole::Peripheral,
        "B (lower endpoint) keeps the Peripheral role after dedup"
    );
}

// ─── Task 20 ─────────────────────────────────────────────────────────────────

/// After a peer is verified (VerifiedEndpoint received), repeated
/// advertisements from that peer must NOT trigger new StartConnect actions.
/// The verified+live guard in handle_advertised short-circuits the dial path.
#[tokio::test(flavor = "multi_thread")]
async fn advertising_flood_does_not_redial_after_verified() {
    let ep_a = endpoint_with_byte(0xFF);
    let ep_b = endpoint_with_byte(0x01);

    let dev_a = DeviceId::from("flood-a");
    let dev_b = DeviceId::from("flood-b");

    let (a_inbox_tx, a_inbox_rx) = mpsc::channel::<PeerCommand>(256);

    let iface_a = Arc::new(MockBleInterface::new());
    // Wire c2p write → b's inbox (we don't care about B's actual registry here;
    // we only need the GATT fragment to flow so B's pipe comes up).
    let (b_inbox_tx, _b_inbox_rx) = mpsc::channel::<PeerCommand>(256);
    {
        let inbox = b_inbox_tx.clone();
        let from = dev_a.clone();
        iface_a.set_on_c2p_write(Box::new(move |_target, bytes| {
            let _ = inbox.try_send(PeerCommand::InboundGattFragment {
                device_id: from.clone(),
                source: iroh_ble_transport::transport::peer::FragmentSource::PeripheralReceivedC2p,
                bytes,
            });
        }));
    }

    let a = spawn_node_with_endpoint(
        dev_a.clone(),
        a_inbox_tx.clone(),
        a_inbox_rx,
        Arc::clone(&iface_a),
        L2capPolicy::Disabled,
        ep_a,
    );

    // Use the endpoint-derived prefix so VerifiedEndpoint can match the entry.
    let prefix_b = prefix_from_endpoint(&ep_b);

    let (waker_tx, _) = mpsc::channel::<()>(1);
    let waker = waker_from_channel(waker_tx);

    // Establish connection: advertise B to A, then send a datagram to kick the dial.
    a.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_b,
            device: ble_device(&dev_b),
            rssi: None,
        })
        .await
        .unwrap();
    a.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: dev_b.clone(),
            tx_gen: 0,
            datagram: Bytes::from_static(b"initial"),
            waker,
        })
        .await
        .unwrap();

    // Wait for the initial Connect call.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if iface_a
                .calls()
                .iter()
                .any(|c| matches!(c, CallKind::Connect(_)))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A never issued a Connect call");

    // Wait for Connected state.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snap = a.snapshots.load();
            if snap
                .peer_states
                .get(&dev_b)
                .map(|s| s.phase_kind == PhaseKind::Connected)
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A never reached Connected with B");

    // Mark B's prefix as verified.
    a.inbox_tx
        .send(PeerCommand::VerifiedEndpoint {
            endpoint_id: ep_b,
            token: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Snapshot the Connect call count before the flood.
    let connect_count_before = iface_a
        .calls()
        .iter()
        .filter(|c| matches!(c, CallKind::Connect(_)))
        .count();

    // Flood A with 20 more advertisements from B.
    for _ in 0..20 {
        a.inbox_tx
            .send(PeerCommand::Advertised {
                prefix: prefix_b,
                device: ble_device(&dev_b),
                rssi: None,
            })
            .await
            .unwrap();
    }

    // Let the registry process all messages.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let connect_count_after = iface_a
        .calls()
        .iter()
        .filter(|c| matches!(c, CallKind::Connect(_)))
        .count();

    assert_eq!(
        connect_count_after, connect_count_before,
        "verified peer must not be redialled after {} floods; connect count grew from {} to {}",
        20, connect_count_before, connect_count_after,
    );
}

// ─── Task 21 ─────────────────────────────────────────────────────────────────

/// With L2capPolicy::PreferL2cap, a VerifiedEndpoint triggers UpgradeToL2cap.
/// When the L2CAP channel is accepted but the peer never reads (buffer fills),
/// the pipe supervisor fires L2capHandoverTimeout → registry sets
/// l2cap_upgrade_failed=true and emits RevertToGattPipe.
#[tokio::test(flavor = "multi_thread")]
async fn l2cap_handover_timeout_reverts_to_gatt() {
    use iroh_ble_transport::transport::peer::ChannelHandle;
    use iroh_ble_transport::transport::peer::ConnectPath;

    let ep_self = endpoint_with_byte(0xFF);
    let ep_peer = endpoint_with_byte(0x01);

    let dev_self = DeviceId::from("l2cap-self");
    let dev_peer = DeviceId::from("l2cap-peer");

    let (self_inbox_tx, self_inbox_rx) = mpsc::channel::<PeerCommand>(256);

    let iface = Arc::new(MockBleInterface::new());

    // Pre-seed a successful connect response for the initial GATT dial.
    iface.on_connect(
        dev_peer.clone(),
        Ok(ChannelHandle {
            id: 1,
            path: ConnectPath::Gatt,
        }),
    );

    // Seed the PSM read that UpgradeToL2cap will trigger.
    let psm: u16 = 0x0080;
    iface.seed_psm(Some(psm));

    // Create an L2CAP channel pair. The peripheral side is held alive but
    // never read from — this fills the underlying duplex buffer and causes
    // any central-side write to block, triggering the handover timeout.
    // The buffer is intentionally tiny (64 bytes) so it fills quickly.
    let (central_chan, _peripheral_chan_never_read) = blew::L2capChannel::pair(64);
    iface.on_open_l2cap(dev_peer.clone(), psm, Ok(central_chan));

    let node = spawn_node_with_endpoint(
        dev_self.clone(),
        self_inbox_tx.clone(),
        self_inbox_rx,
        Arc::clone(&iface),
        L2capPolicy::PreferL2cap,
        ep_self,
    );

    // Use the endpoint-derived prefix so VerifiedEndpoint can match the entry.
    let prefix_peer = prefix_from_endpoint(&ep_peer);

    let (waker_tx, _) = mpsc::channel::<()>(1);
    let waker = waker_from_channel(waker_tx);

    // Advertise peer + send to trigger the dial.
    node.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_peer,
            device: ble_device(&dev_peer),
            rssi: None,
        })
        .await
        .unwrap();
    node.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: dev_peer.clone(),
            tx_gen: 0,
            datagram: Bytes::from_static(b"initial"),
            waker,
        })
        .await
        .unwrap();

    // Wait for Connected{Gatt} state.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snap = node.snapshots.load();
            if snap
                .peer_states
                .get(&dev_peer)
                .map(|s| {
                    s.phase_kind == PhaseKind::Connected
                        && s.connect_path == Some(ConnectPath::Gatt)
                })
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("node never reached Connected{Gatt}");

    // VerifiedEndpoint triggers UpgradeToL2cap (L2capPolicy::PreferL2cap).
    // self's endpoint (0xFF) > peer's endpoint (0x01) → self keeps Central →
    // should_win(Central, ep_self, ep_peer) = true → upgrade is triggered.
    //
    // Read the current tx_gen before sending VerifiedEndpoint; all subsequent
    // sends must carry this value so the registry forwards them to the pipe.
    let live_tx_gen = node
        .snapshots
        .load()
        .peer_states
        .get(&dev_peer)
        .expect("peer entry must exist after Connected")
        .tx_gen;

    node.inbox_tx
        .send(PeerCommand::VerifiedEndpoint {
            endpoint_id: ep_peer,
            token: None,
        })
        .await
        .unwrap();

    // Wait a moment for the upgrade path to complete (read_psm + open_l2cap +
    // SwapPipeToL2cap) before flooding with sends. The mock returns immediately,
    // so 100 ms is ample.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fill the L2CAP pipe chain with sends using the correct tx_gen.
    // The duplex buffer (64 B) fills after the first framed write (2-byte len +
    // 64-byte payload = 66 B). The L2CAP IO task's outbound channel (cap 64)
    // and the pipe supervisor's outbound_fwd channel (cap 32) also need to fill
    // so that `forward_outbound`'s `outbound_fwd_tx.send()` actually blocks.
    // 64 + 32 + 1 = 97 sends is sufficient; send 150 for margin.
    let sender = node.inbox_tx.clone();
    let dev_peer_clone = dev_peer.clone();
    tokio::spawn(async move {
        for _ in 0..150u64 {
            let (waker_tx, _) = mpsc::channel::<()>(1);
            let waker = waker_from_channel(waker_tx);
            let _ = sender
                .send(PeerCommand::SendDatagram {
                    device_id: dev_peer_clone.clone(),
                    tx_gen: live_tx_gen,
                    datagram: Bytes::from(vec![0xAB; 64]),
                    waker,
                })
                .await;
            // Yield between sends so the registry actor can drain the inbox.
            tokio::task::yield_now().await;
        }
    });

    // Wait for l2cap_upgrade_failed to become true in the snapshot.
    // Give it generous headroom above L2CAP_HANDOVER_TIMEOUT (1s).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snap = node.snapshots.load();
            if snap
                .peer_states
                .get(&dev_peer)
                .map(|s| s.l2cap_upgrade_failed)
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("l2cap_upgrade_failed never set after handover timeout");

    // Assert post-revert state: still Connected, l2cap_upgrade_failed=true.
    let snap = node.snapshots.load();
    let peer_state = snap
        .peer_states
        .get(&dev_peer)
        .expect("node has no entry for peer after revert");

    assert_eq!(
        peer_state.phase_kind,
        PhaseKind::Connected,
        "peer should still be Connected after GATT revert"
    );
    assert!(
        peer_state.l2cap_upgrade_failed,
        "l2cap_upgrade_failed must be true after handover timeout"
    );
}
