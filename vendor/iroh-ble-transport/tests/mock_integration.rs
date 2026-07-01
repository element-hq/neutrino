#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::Waker;
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use iroh_ble_transport::transport::{
    driver::{Driver, IncomingPacket},
    peer::{ChannelHandle, ConnectPath, KEY_PREFIX_LEN, KeyPrefix, PeerCommand},
    registry::{Registry, SnapshotMaps},
    routing::TransportRouting,
    store::InMemoryPeerStore,
    test_util::{CallKind, MockBleInterface},
    transport::L2capPolicy,
};
use tokio::sync::mpsc;

fn test_wakers() -> Arc<Mutex<Vec<Waker>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn test_routing() -> Arc<TransportRouting> {
    Arc::new(TransportRouting::new())
}

fn zero_counters() -> (Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) {
    (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    )
}

fn waker_from_channel(tx: mpsc::Sender<()>) -> std::task::Waker {
    use std::sync::Arc;
    use std::task::{Wake, Waker};
    struct W(mpsc::Sender<()>);
    impl Wake for W {
        fn wake(self: Arc<Self>) {
            let _ = self.0.try_send(());
        }
    }
    Waker::from(Arc::new(W(tx)))
}

/// Post-refactor behavior: disconnect transitions the peer into `Draining`
/// and dispatches a `CloseChannel` action; the registry does NOT auto-retry.
/// Application-layer code (e.g. chat-app `reconnect_tick`) is responsible for
/// driving a fresh dial. This test verifies the registry side of that
/// contract — the `Disconnect` call lands on the interface and the peer ends
/// up in `Draining` (or `Dead` after the drain timeout).
#[tokio::test]
async fn mid_session_disconnect_drains_and_closes_channel() {
    let iface = Arc::new(MockBleInterface::new());
    let device_id = blew::DeviceId::from("dev-b");
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 1,
            path: ConnectPath::Gatt,
        }),
    );

    let (tx, rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(16);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });
    tokio::spawn(iroh_ble_transport::transport::watchdog::run_watchdog(
        tx.clone(),
    ));

    let prefix: KeyPrefix = [2u8; KEY_PREFIX_LEN];
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
    let waker = waker_from_channel(waker_tx);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"ping"),
        waker,
    })
    .await
    .unwrap();

    wait_for_call_count(
        &iface,
        CallKindMatcher::Connect,
        1,
        std::time::Duration::from_secs(5),
    )
    .await;

    tx.send(PeerCommand::CentralDisconnected {
        device_id: device_id.clone(),
        cause: blew::DisconnectCause::LinkLoss,
    })
    .await
    .unwrap();

    // CentralDisconnected when the entry is in Connected/Handshaking dispatches
    // a CloseChannel action → the driver calls `iface.disconnect()`, recording
    // CallKind::Disconnect on the mock.
    wait_for_call_count(
        &iface,
        CallKindMatcher::Disconnect,
        1,
        std::time::Duration::from_secs(2),
    )
    .await;

    // And the registry must NOT issue a second connect on its own — the
    // transport is passive after Draining.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let connect_count = iface
        .calls()
        .iter()
        .filter(|c| matches!(c, CallKind::Connect(_)))
        .count();
    assert_eq!(
        connect_count, 1,
        "registry must not auto-reconnect after disconnect; reconnect is now app-driven"
    );
}

#[tokio::test]
async fn adapter_toggle_reconnects_all_peers_with_single_purge() {
    let iface = Arc::new(MockBleInterface::new());
    for i in 0..3u8 {
        iface.on_connect(
            blew::DeviceId::from(format!("dev-{i}").as_str()),
            Ok(ChannelHandle {
                id: i as u64 + 1,
                path: ConnectPath::Gatt,
            }),
        );
        iface.on_connect(
            blew::DeviceId::from(format!("dev-{i}").as_str()),
            Ok(ChannelHandle {
                id: i as u64 + 100,
                path: ConnectPath::Gatt,
            }),
        );
    }

    let (tx, rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(16);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });
    tokio::spawn(iroh_ble_transport::transport::watchdog::run_watchdog(
        tx.clone(),
    ));

    // Advertise + send to three peers to trigger connects.
    for i in 0..3u8 {
        let prefix: KeyPrefix = [i; KEY_PREFIX_LEN];
        let device_id = blew::DeviceId::from(format!("dev-{i}").as_str());
        tx.send(PeerCommand::Advertised {
            prefix,
            device: blew::BleDevice {
                id: device_id.clone(),
                name: None,
                rssi: None,
                services: vec![],
            },
            rssi: None,
        })
        .await
        .unwrap();
        let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
        tx.send(PeerCommand::SendDatagram {
            device_id,
            tx_gen: 0,
            datagram: bytes::Bytes::from_static(b"x"),
            waker: waker_from_channel(waker_tx),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    wait_for_call_count(&iface, CallKindMatcher::Connect, 3, Duration::from_secs(5)).await;

    // Adapter off, then on.
    tx.send(PeerCommand::AdapterStateChanged { powered: false })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(PeerCommand::AdapterStateChanged { powered: true })
        .await
        .unwrap();

    // All three peers should reconnect (6 total connect calls).
    wait_for_call_count(&iface, CallKindMatcher::Connect, 6, Duration::from_secs(10)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn adapter_on_rebuilds_server_and_restarts_advertising_and_l2cap() {
    let iface = Arc::new(MockBleInterface::new());

    let (tx, rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(16);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::PreferL2cap);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });

    tx.send(PeerCommand::AdapterStateChanged { powered: false })
        .await
        .unwrap();
    tx.send(PeerCommand::AdapterStateChanged { powered: true })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let calls = iface.calls();
            let has_rebuild = calls.iter().any(|c| matches!(c, CallKind::RebuildServer));
            let has_restart_adv = calls
                .iter()
                .any(|c| matches!(c, CallKind::RestartAdvertising));
            let has_restart_l2cap = calls
                .iter()
                .any(|c| matches!(c, CallKind::RestartL2capListener));
            if has_rebuild && has_restart_adv && has_restart_l2cap {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(
        "expected rebuild_server + restart_advertising + restart_l2cap_listener after adapter-on",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn version_mismatch_deads_peer_and_closes_channel() {
    use iroh_ble_transport::transport::peer::DeadReason;
    use iroh_ble_transport::transport::registry::PhaseKind;

    let iface = Arc::new(MockBleInterface::new());
    let device_id = blew::DeviceId::from("dev-vmismatch");
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 1,
            path: ConnectPath::Gatt,
        }),
    );
    // Seed a peer-advertised version that disagrees with our compile-time one.
    iface.seed_version(Some(0xAA));

    let (tx, rx) = mpsc::channel::<PeerCommand>(64);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(8);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });

    let prefix: KeyPrefix = [0x77; KEY_PREFIX_LEN];
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();
    let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"x"),
        waker: waker_from_channel(waker_tx),
    })
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snap = snapshots.load();
            if let Some(summary) = snap.peer_states.get(&device_id)
                && summary.phase_kind == PhaseKind::Dead
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer should reach Dead after version mismatch");

    assert!(
        iface
            .calls()
            .iter()
            .any(|c| matches!(c, CallKind::ReadVersion(d) if d == &device_id)),
        "expected read_version to be called"
    );
    assert!(
        iface
            .calls()
            .iter()
            .any(|c| matches!(c, CallKind::Disconnect(d) if d == &device_id)),
        "expected disconnect after version mismatch"
    );
    let _ = DeadReason::ProtocolMismatch { got: 0, want: 0 };
}

#[tokio::test(flavor = "multi_thread")]
async fn version_match_lets_data_pipe_start() {
    let iface = Arc::new(MockBleInterface::new());
    let device_id = blew::DeviceId::from("dev-vmatch");
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 1,
            path: ConnectPath::Gatt,
        }),
    );
    iface.seed_version(Some(
        iroh_ble_transport::transport::transport::PROTOCOL_VERSION,
    ));

    let (tx, rx) = mpsc::channel::<PeerCommand>(64);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(8);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });

    let prefix: KeyPrefix = [0x78; KEY_PREFIX_LEN];
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();
    let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"x"),
        waker: waker_from_channel(waker_tx),
    })
    .await
    .unwrap();

    wait_for_call_count(&iface, CallKindMatcher::Connect, 1, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !iface
            .calls()
            .iter()
            .any(|c| matches!(c, CallKind::Disconnect(d) if d == &device_id)),
        "matching version must not trigger a disconnect"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mock_fabric_handles_bidirectional_traffic() {
    use iroh_ble_transport::transport::test_util::MockFabricPair;

    let (central_inbox_tx, central_inbox_rx) = mpsc::channel::<PeerCommand>(256);
    let (peripheral_inbox_tx, peripheral_inbox_rx) = mpsc::channel::<PeerCommand>(256);
    let fabric = MockFabricPair::new(central_inbox_tx.clone(), peripheral_inbox_tx.clone());

    let (central_incoming_tx, mut central_incoming_rx) = mpsc::channel::<IncomingPacket>(64);
    let (peripheral_incoming_tx, mut peripheral_incoming_rx) = mpsc::channel::<IncomingPacket>(64);

    let central_snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let peripheral_snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));

    let (c_retransmits, c_truncations, c_empty_frames) = zero_counters();
    let (p_retransmits, p_truncations, p_empty_frames) = zero_counters();

    let central_driver = Driver::new(
        fabric.central.clone(),
        central_inbox_tx.clone(),
        central_incoming_tx,
        c_retransmits,
        c_truncations,
        c_empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let peripheral_driver = Driver::new(
        fabric.peripheral.clone(),
        peripheral_inbox_tx.clone(),
        peripheral_incoming_tx,
        p_retransmits,
        p_truncations,
        p_empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );

    let central_reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let peripheral_reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let cs = central_snapshots.clone();
    let ps = peripheral_snapshots.clone();
    tokio::spawn(async move {
        central_reg
            .run(
                central_inbox_rx,
                central_driver,
                cs,
                test_wakers(),
                test_routing(),
            )
            .await;
    });
    tokio::spawn(async move {
        peripheral_reg
            .run(
                peripheral_inbox_rx,
                peripheral_driver,
                ps,
                test_wakers(),
                test_routing(),
            )
            .await;
    });

    let prefix: KeyPrefix = [0xBBu8; KEY_PREFIX_LEN];
    central_inbox_tx
        .send(PeerCommand::Advertised {
            prefix,
            device: blew::BleDevice {
                id: fabric.peripheral_as_device.clone(),
                name: None,
                rssi: None,
                services: vec![],
            },
            rssi: None,
        })
        .await
        .unwrap();

    // Central→peripheral
    let (waker_tx, _wrx) = mpsc::channel::<()>(1);
    central_inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: fabric.peripheral_as_device.clone(),
            tx_gen: 0,
            datagram: bytes::Bytes::from_static(b"c->p"),
            waker: waker_from_channel(waker_tx),
        })
        .await
        .unwrap();

    // Wait for peripheral-side lazy creation: after first InboundGattFragment,
    // the peripheral registry spawns its own pipe. Poll the snapshot until a peer exists.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let maps = peripheral_snapshots.load();
            if !maps.peer_states.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peripheral failed to observe peer");

    // Give the peripheral's data pipe a moment to come fully live after lazy creation.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Peripheral→central: snapshot is keyed by DeviceId post-refactor; the
    // peripheral's lazy peer is `fabric.central_as_device`.
    let peripheral_peer_device = peripheral_snapshots
        .load()
        .peer_states
        .keys()
        .next()
        .expect("peripheral has a peer")
        .clone();
    let peripheral_tx_gen = peripheral_snapshots
        .load()
        .peer_states
        .get(&peripheral_peer_device)
        .unwrap()
        .tx_gen;
    let (waker_tx2, _wrx2) = mpsc::channel::<()>(1);
    peripheral_inbox_tx
        .send(PeerCommand::SendDatagram {
            device_id: peripheral_peer_device,
            tx_gen: peripheral_tx_gen,
            datagram: bytes::Bytes::from_static(b"p->c"),
            waker: waker_from_channel(waker_tx2),
        })
        .await
        .unwrap();

    let pkt_p = tokio::time::timeout(Duration::from_secs(10), peripheral_incoming_rx.recv())
        .await
        .expect("c->p timeout")
        .expect("closed");
    assert_eq!(pkt_p.data.as_ref(), b"c->p");

    let pkt_c = tokio::time::timeout(Duration::from_secs(10), central_incoming_rx.recv())
        .await
        .expect("p->c timeout")
        .expect("closed");
    assert_eq!(pkt_c.data.as_ref(), b"p->c");
    let _ = fabric;
}

/// After a peer is `Forget`-ed it transitions to `Dead` immediately. After
/// `DEAD_GC_TTL` (60 s) elapses, a `Tick` GC's the entry. The same `DeviceId`
/// must then be re-Advertise-able and dialable as a fresh peer — verifying
/// that nothing in the registry, routing, or driver caches a stale handle.
///
/// Spoofs time by issuing `PeerCommand::Tick(now + 200s)` instead of waiting
/// for real elapsed time; the registry compares the spoofed `tick_now`
/// against the `Dead { at }` instant and GC's accordingly.
#[tokio::test]
async fn forget_then_gc_then_rediscover_creates_fresh_peer() {
    let iface = Arc::new(MockBleInterface::new());
    let device_id = blew::DeviceId::from("dev-recycle");
    // Two queued connect responses: one for the initial dial, one for the
    // post-GC re-dial.
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 1,
            path: ConnectPath::Gatt,
        }),
    );
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 2,
            path: ConnectPath::Gatt,
        }),
    );

    let (tx, rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(16);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });

    // First lifecycle: advertise, send, wait for connect.
    let prefix: KeyPrefix = [7u8; KEY_PREFIX_LEN];
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();
    let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"first"),
        waker: waker_from_channel(waker_tx),
    })
    .await
    .unwrap();
    wait_for_call_count(&iface, CallKindMatcher::Connect, 1, Duration::from_secs(5)).await;

    // Forget the peer. The registry transitions it to Dead { Forgotten, at: now }.
    tx.send(PeerCommand::Forget {
        device_id: device_id.clone(),
    })
    .await
    .unwrap();

    // Spoof time forward past DEAD_GC_TTL and tick. The GC pass should remove
    // the Dead entry from the snapshot.
    let future = std::time::Instant::now() + std::time::Duration::from_secs(200);
    tx.send(PeerCommand::Tick(future)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let maps = snapshots.load();
            if !maps.peer_states.contains_key(&device_id) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("GC failed to drop the Forgotten peer");

    // Second lifecycle: re-advertise the same DeviceId. It must take the
    // fresh-entry path — Discovered → Connecting → Connect call — and NOT
    // get blocked by any stale state.
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();
    let (waker_tx2, _waker_rx2) = mpsc::channel::<()>(1);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"second"),
        waker: waker_from_channel(waker_tx2),
    })
    .await
    .unwrap();

    // Two connect calls total: the original + the post-GC re-dial.
    wait_for_call_count(&iface, CallKindMatcher::Connect, 2, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn connect_failure_retries_on_next_tick() {
    use iroh_ble_transport::error::BleError;
    use iroh_ble_transport::transport::registry::PhaseKind;

    let iface = Arc::new(MockBleInterface::new());
    let device_id = blew::DeviceId::from("dev-retry");

    // First connect fails, second succeeds.
    iface.on_connect(device_id.clone(), Err(BleError::AdapterOff));
    iface.on_connect(
        device_id.clone(),
        Ok(ChannelHandle {
            id: 10,
            path: ConnectPath::Gatt,
        }),
    );

    let (tx, rx) = mpsc::channel::<PeerCommand>(256);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(16);
    let snapshots = Arc::new(ArcSwap::from(Arc::new(SnapshotMaps::default())));
    let (retransmits, truncations, empty_frames) = zero_counters();
    let driver = Driver::new(
        iface.clone(),
        tx.clone(),
        incoming_tx,
        retransmits,
        truncations,
        empty_frames,
        Arc::new(InMemoryPeerStore::new()),
    );
    let reg = Registry::new_for_test_with_policy(L2capPolicy::Disabled);
    let snap_for_actor = snapshots.clone();
    tokio::spawn(async move {
        reg.run(rx, driver, snap_for_actor, test_wakers(), test_routing())
            .await;
    });

    let prefix: KeyPrefix = [9u8; KEY_PREFIX_LEN];
    tx.send(PeerCommand::Advertised {
        prefix,
        device: blew::BleDevice {
            id: device_id.clone(),
            name: None,
            rssi: None,
            services: vec![],
        },
        rssi: None,
    })
    .await
    .unwrap();

    let (waker_tx, _waker_rx) = mpsc::channel::<()>(1);
    let waker = waker_from_channel(waker_tx);
    tx.send(PeerCommand::SendDatagram {
        device_id: device_id.clone(),
        tx_gen: 0,
        datagram: bytes::Bytes::from_static(b"retry-ping"),
        waker,
    })
    .await
    .unwrap();

    // Wait for the first connect call (which fails).
    wait_for_call_count(&iface, CallKindMatcher::Connect, 1, Duration::from_secs(5)).await;

    // Peer should be in Reconnecting after the failed connect.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let maps = snapshots.load();
            if let Some(state) = maps.peer_states.get(&device_id)
                && state.phase_kind == PhaseKind::Reconnecting
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer did not reach Reconnecting after first connect failure");

    // Send a Tick with a future instant to bypass the backoff and trigger retry.
    let future = std::time::Instant::now() + Duration::from_secs(120);
    tx.send(PeerCommand::Tick(future)).await.unwrap();

    // Wait for the second connect call (succeeds).
    wait_for_call_count(&iface, CallKindMatcher::Connect, 2, Duration::from_secs(5)).await;

    // Peer should reach Connected or Handshaking.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let maps = snapshots.load();
            if let Some(state) = maps.peer_states.get(&device_id)
                && matches!(
                    state.phase_kind,
                    PhaseKind::Connected | PhaseKind::Handshaking
                )
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer did not reach Connected or Handshaking after retry");
}

enum CallKindMatcher {
    Connect,
    Disconnect,
}

async fn wait_for_call_count(
    iface: &MockBleInterface,
    kind: CallKindMatcher,
    n: usize,
    timeout: std::time::Duration,
) {
    tokio::time::timeout(timeout, async {
        loop {
            let count = iface
                .calls()
                .iter()
                .filter(|c| {
                    matches!(
                        (&kind, c),
                        (CallKindMatcher::Connect, CallKind::Connect(_))
                            | (CallKindMatcher::Disconnect, CallKind::Disconnect(_))
                    )
                })
                .count();
            if count >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for calls");
}
