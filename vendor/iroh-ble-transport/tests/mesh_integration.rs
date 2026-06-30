#![cfg(feature = "testing")]

//! Multi-node integration tests exercising the registry under concurrent-peer
//! load. Each test spins up 3 nodes sharing a single `MockFabric`, each with
//! its own `Registry + Driver + MockBleInterface`. All tests use
//! `L2capPolicy::Disabled` (GATT-only).

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::Waker;

use arc_swap::ArcSwap;
use blew::{BleDevice, DeviceId};
use bytes::Bytes;
use parking_lot::Mutex;

use iroh_ble_transport::transport::{
    driver::{Driver, IncomingPacket},
    peer::{KEY_PREFIX_LEN, KeyPrefix, PeerCommand},
    registry::{Registry, SnapshotMaps},
    routing::TransportRouting,
    store::InMemoryPeerStore,
    test_util::MockFabric,
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

fn prefix_for(byte: u8) -> KeyPrefix {
    [byte; KEY_PREFIX_LEN]
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

struct TestNode {
    device_id: DeviceId,
    inbox_tx: mpsc::Sender<PeerCommand>,
    incoming_rx: mpsc::Receiver<IncomingPacket>,
    snapshots: Arc<ArcSwap<SnapshotMaps>>,
}

fn spawn_node(fabric: &MockFabric, device_id: DeviceId, policy: L2capPolicy) -> TestNode {
    let (inbox_tx, inbox_rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, incoming_rx) = mpsc::channel::<IncomingPacket>(64);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let iface = fabric.add_node(device_id.clone(), inbox_tx.clone());
    let (retransmits, truncations, empty_frames) = zero_counters();

    let driver = Driver::new(
        iface,
        inbox_tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let registry = Registry::new_for_test_with_policy(policy);
    let snap = snapshots.clone();
    let routing = Arc::new(TransportRouting::new());
    let wakers = Arc::new(Mutex::new(Vec::<Waker>::new()));
    tokio::spawn(async move {
        registry.run(inbox_rx, driver, snap, wakers, routing).await;
    });

    TestNode {
        device_id,
        inbox_tx,
        incoming_rx,
        snapshots,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn triangle_send_fans_out_to_both_peers() {
    let fabric = MockFabric::new();
    let a = spawn_node(&fabric, DeviceId::from("a"), L2capPolicy::Disabled);
    let mut b = spawn_node(&fabric, DeviceId::from("b"), L2capPolicy::Disabled);
    let mut c = spawn_node(&fabric, DeviceId::from("c"), L2capPolicy::Disabled);

    a.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_for(0xB1),
            device: ble_device(&b.device_id),
            rssi: None,
        })
        .await
        .unwrap();
    a.inbox_tx
        .send(PeerCommand::Advertised {
            prefix: prefix_for(0xC1),
            device: ble_device(&c.device_id),
            rssi: None,
        })
        .await
        .unwrap();

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(4);
    let waker = waker_from_channel(waker_tx);
    a.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: b.device_id.clone(),
            tx_gen: 0,
            datagram: Bytes::from_static(b"to-b"),
            waker: waker.clone(),
        })
        .await
        .unwrap();
    a.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: c.device_id.clone(),
            tx_gen: 0,
            datagram: Bytes::from_static(b"to-c"),
            waker,
        })
        .await
        .unwrap();

    let got_b = tokio::time::timeout(std::time::Duration::from_secs(10), b.incoming_rx.recv())
        .await
        .expect("B never received datagram")
        .expect("B incoming closed");
    let got_c = tokio::time::timeout(std::time::Duration::from_secs(10), c.incoming_rx.recv())
        .await
        .expect("C never received datagram")
        .expect("C incoming closed");

    assert_eq!(got_b.device_id, a.device_id);
    assert_eq!(got_b.data.as_ref(), b"to-b");
    assert_eq!(got_c.device_id, a.device_id);
    assert_eq!(got_c.data.as_ref(), b"to-c");

    let snapshot = a.snapshots.load();
    let state_b = snapshot.peer_states.get(&b.device_id).expect("A has no B");
    let state_c = snapshot.peer_states.get(&c.device_id).expect("A has no C");
    assert!(state_b.tx_gen > 0, "A's tx_gen for B should have advanced");
    assert!(state_c.tx_gen > 0, "A's tx_gen for C should have advanced");
}

#[tokio::test(flavor = "multi_thread")]
async fn symmetric_connect_converges_to_one_channel() {
    use iroh_ble_transport::transport::registry::PhaseKind;

    let fabric = MockFabric::new();
    let a = spawn_node(&fabric, DeviceId::from("a"), L2capPolicy::Disabled);
    let b = spawn_node(&fabric, DeviceId::from("b"), L2capPolicy::Disabled);

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(4);
    let waker = waker_from_channel(waker_tx);

    tokio::join!(
        async {
            a.inbox_tx
                .send(PeerCommand::Advertised {
                    prefix: prefix_for(0xB1),
                    device: ble_device(&b.device_id),
                    rssi: None,
                })
                .await
                .unwrap();
            a.inbox_tx
                .send(PeerCommand::SendDatagram {
                    device_id: b.device_id.clone(),
                    tx_gen: 0,
                    datagram: Bytes::from_static(b"a-to-b"),
                    waker: waker.clone(),
                })
                .await
                .unwrap();
        },
        async {
            b.inbox_tx
                .send(PeerCommand::Advertised {
                    prefix: prefix_for(0xA1),
                    device: ble_device(&a.device_id),
                    rssi: None,
                })
                .await
                .unwrap();
            b.inbox_tx
                .send(PeerCommand::SendDatagram {
                    device_id: a.device_id.clone(),
                    tx_gen: 0,
                    datagram: Bytes::from_static(b"b-to-a"),
                    waker: waker.clone(),
                })
                .await
                .unwrap();
        }
    );

    let mut a = a;
    let mut b = b;
    let got_on_b = tokio::time::timeout(std::time::Duration::from_secs(10), b.incoming_rx.recv())
        .await
        .expect("B never received A's datagram")
        .expect("B incoming closed");
    let got_on_a = tokio::time::timeout(std::time::Duration::from_secs(10), a.incoming_rx.recv())
        .await
        .expect("A never received B's datagram")
        .expect("A incoming closed");
    assert_eq!(got_on_b.data.as_ref(), b"a-to-b");
    assert_eq!(got_on_a.data.as_ref(), b"b-to-a");

    let a_snap = a.snapshots.load();
    let b_snap = b.snapshots.load();
    let a_view_of_b = a_snap.peer_states.get(&b.device_id).expect("A has no B");
    let b_view_of_a = b_snap.peer_states.get(&a.device_id).expect("B has no A");
    assert_eq!(
        a_view_of_b.phase_kind,
        PhaseKind::Connected,
        "A's view of B ended up in {:?}, expected Connected",
        a_view_of_b.phase_kind,
    );
    assert_eq!(
        b_view_of_a.phase_kind,
        PhaseKind::Connected,
        "B's view of A ended up in {:?}, expected Connected",
        b_view_of_a.phase_kind,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_peer_flapping_does_not_disturb_others() {
    use blew::DisconnectCause;
    use iroh_ble_transport::transport::registry::PhaseKind;

    let fabric = MockFabric::new();
    let a = spawn_node(&fabric, DeviceId::from("a"), L2capPolicy::Disabled);
    let mut b = spawn_node(&fabric, DeviceId::from("b"), L2capPolicy::Disabled);
    let mut c = spawn_node(&fabric, DeviceId::from("c"), L2capPolicy::Disabled);

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(8);
    let waker = waker_from_channel(waker_tx);

    // Bring A into Connected with both B and C.
    for (peer, prefix_byte, payload) in [
        (&b.device_id, 0xB1_u8, &b"to-b"[..]),
        (&c.device_id, 0xC1_u8, &b"to-c"[..]),
    ] {
        a.inbox_tx
            .send(PeerCommand::Advertised {
                prefix: prefix_for(prefix_byte),
                device: ble_device(peer),
                rssi: None,
            })
            .await
            .unwrap();
        a.inbox_tx
            .send(PeerCommand::SendDatagram {
                device_id: peer.clone(),
                tx_gen: 0,
                datagram: Bytes::copy_from_slice(payload),
                waker: waker.clone(),
            })
            .await
            .unwrap();
    }

    // Drain the initial datagrams so the triangle is known to be live.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), b.incoming_rx.recv())
        .await
        .expect("B never got A's initial datagram");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), c.incoming_rx.recv())
        .await
        .expect("C never got A's initial datagram");

    let snap_before = a.snapshots.load();
    let tx_gen_c_before = snap_before
        .peer_states
        .get(&c.device_id)
        .expect("A has no C")
        .tx_gen;
    assert_eq!(
        snap_before
            .peer_states
            .get(&c.device_id)
            .unwrap()
            .phase_kind,
        PhaseKind::Connected,
    );
    drop(snap_before);

    // Flap the A<->B edge: tell A that B disconnected (link loss).
    a.inbox_tx
        .send(PeerCommand::CentralDisconnected {
            device_id: b.device_id.clone(),
            cause: DisconnectCause::LinkLoss,
        })
        .await
        .unwrap();

    // Meanwhile, A can still send to C successfully.
    a.inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: c.device_id.clone(),
            tx_gen: tx_gen_c_before,
            datagram: Bytes::from_static(b"still-talking-to-c"),
            waker: waker.clone(),
        })
        .await
        .unwrap();

    let got_on_c = tokio::time::timeout(std::time::Duration::from_secs(10), c.incoming_rx.recv())
        .await
        .expect("C never got A's follow-up datagram")
        .expect("C incoming closed");
    assert_eq!(got_on_c.data.as_ref(), b"still-talking-to-c");

    // A's view of C stays Connected with unchanged tx_gen; A's view of B has
    // left Connected.
    let snap_after = a.snapshots.load();
    let state_b = snap_after
        .peer_states
        .get(&b.device_id)
        .expect("A has no B");
    let state_c = snap_after
        .peer_states
        .get(&c.device_id)
        .expect("A has no C");
    assert_ne!(
        state_b.phase_kind,
        PhaseKind::Connected,
        "A's B entry should have left Connected after LinkLoss, found {:?}",
        state_b.phase_kind,
    );
    assert_eq!(state_c.phase_kind, PhaseKind::Connected);
    assert_eq!(
        state_c.tx_gen, tx_gen_c_before,
        "A's tx_gen for C must not shift when the unrelated B edge flaps"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn adapter_off_on_one_node_does_not_evict_others() {
    use iroh_ble_transport::transport::registry::PhaseKind;

    let fabric = MockFabric::new();
    let a = spawn_node(&fabric, DeviceId::from("a"), L2capPolicy::Disabled);
    let mut b = spawn_node(&fabric, DeviceId::from("b"), L2capPolicy::Disabled);
    let mut c = spawn_node(&fabric, DeviceId::from("c"), L2capPolicy::Disabled);

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(8);
    let waker = waker_from_channel(waker_tx);

    // Bring all three into a fully-connected triangle: A<->B and A<->C via
    // A's sends; B<->C via B's send to C.
    for (from_inbox, peer, prefix_byte, payload) in [
        (&a.inbox_tx, &b.device_id, 0xB1_u8, &b"a-to-b"[..]),
        (&a.inbox_tx, &c.device_id, 0xC1_u8, &b"a-to-c"[..]),
        (&b.inbox_tx, &c.device_id, 0xC2_u8, &b"b-to-c"[..]),
    ] {
        from_inbox
            .send(PeerCommand::Advertised {
                prefix: prefix_for(prefix_byte),
                device: ble_device(peer),
                rssi: None,
            })
            .await
            .unwrap();
        from_inbox
            .send(PeerCommand::SendDatagram {
                device_id: peer.clone(),
                tx_gen: 0,
                datagram: Bytes::copy_from_slice(payload),
                waker: waker.clone(),
            })
            .await
            .unwrap();
    }

    // Drain all three receiving datagrams to confirm the triangle is live.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), b.incoming_rx.recv())
        .await
        .expect("B never got A's datagram");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), c.incoming_rx.recv())
        .await
        .expect("C never got A's datagram");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), c.incoming_rx.recv())
        .await
        .expect("C never got B's datagram");

    // Sanity: B's view of C is Connected before we touch A.
    let pre = b.snapshots.load();
    assert_eq!(
        pre.peer_states
            .get(&c.device_id)
            .expect("B has no C")
            .phase_kind,
        PhaseKind::Connected,
    );
    drop(pre);

    // Flip A's adapter off.
    a.inbox_tx
        .send(PeerCommand::AdapterStateChanged { powered: false })
        .await
        .unwrap();

    // Give A's registry a moment to process the adapter-off drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // A's entries for B and C must have drained out of Connected.
    let a_snap = a.snapshots.load();
    for peer in [&b.device_id, &c.device_id] {
        let state = a_snap
            .peer_states
            .get(peer)
            .unwrap_or_else(|| panic!("A has no entry for {peer}"));
        assert_ne!(
            state.phase_kind,
            PhaseKind::Connected,
            "A's entry for {peer} should not be Connected after adapter-off, found {:?}",
            state.phase_kind,
        );
    }
    drop(a_snap);

    // B's and C's view of each other stays Connected — nobody's adapter
    // toggled but A's.
    let b_snap = b.snapshots.load();
    let c_snap = c.snapshots.load();
    assert_eq!(
        b_snap
            .peer_states
            .get(&c.device_id)
            .expect("B has no C")
            .phase_kind,
        PhaseKind::Connected,
        "B's view of C should be untouched by A's adapter toggle",
    );
    assert_eq!(
        c_snap
            .peer_states
            .get(&b.device_id)
            .expect("C has no B")
            .phase_kind,
        PhaseKind::Connected,
        "C's view of B should be untouched by A's adapter toggle",
    );
}
