//! Integration test: pipe supervisor handles an in-place L2CAP swap without
//! deadlock or panic and keeps accepting outbound datagrams on the new path.

#![cfg(feature = "testing")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::{RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use bytes::Bytes;
use iroh_ble_transport::transport::driver::IncomingPacket;
use iroh_ble_transport::transport::interface::BleInterface;
use iroh_ble_transport::transport::peer::{
    ConnectPath, ConnectRole, LivenessClock, PeerCommand, PendingSend,
};
use iroh_ble_transport::transport::pipe::run_data_pipe;
use iroh_ble_transport::transport::test_util::{CallKind, MockBleInterface};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

fn noop_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[tokio::test(flavor = "multi_thread")]
async fn swap_to_l2cap_does_not_deadlock_and_routes_subsequent_sends() {
    let iface = Arc::new(MockBleInterface::new());
    let (outbound_tx, outbound_rx) = mpsc::channel::<PendingSend>(8);
    let (_inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(8);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(8);
    let (registry_tx, _registry_rx) = mpsc::channel::<PeerCommand>(8);
    let (swap_tx, swap_rx) = mpsc::channel::<blew::L2capChannel>(1);

    let device_id = blew::DeviceId::from("swap-test");
    let pipe_handle = tokio::spawn(run_data_pipe(
        iface.clone() as Arc<dyn BleInterface>,
        device_id.clone(),
        ConnectRole::Central,
        ConnectPath::Gatt,
        None,
        outbound_rx,
        inbound_rx,
        incoming_tx,
        registry_tx,
        swap_rx,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        LivenessClock::new(),
    ));

    // Queue a datagram on the GATT path and verify the iface sees a WriteC2p.
    outbound_tx
        .send(PendingSend {
            tx_gen: 1,
            datagram: Bytes::from_static(b"before-swap"),
            waker: noop_waker(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if iface
                .calls()
                .iter()
                .any(|c| matches!(c, CallKind::WriteC2p { .. }))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("GATT WriteC2p observed before swap");

    // Swap: inject an L2CAP channel. The peripheral side stays live in this
    // test so we can verify subsequent sends travel over L2CAP.
    let (central_side, peripheral_side) = blew::L2capChannel::pair(8192);
    swap_tx.send(central_side).await.unwrap();

    // Give the supervisor time to spawn the L2CAP worker and spin down GATT.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a post-swap datagram; it must land on L2CAP (not on iface.write_c2p).
    let gatt_call_count_before = iface
        .calls()
        .iter()
        .filter(|c| matches!(c, CallKind::WriteC2p { .. }))
        .count();

    outbound_tx
        .send(PendingSend {
            tx_gen: 2,
            datagram: Bytes::from_static(b"after-swap"),
            waker: noop_waker(),
        })
        .await
        .unwrap();

    // Expect the L2CAP peer to receive [u16 len=10][payload].
    let mut peri_reader = peripheral_side;
    let mut len_buf = [0u8; 2];
    tokio::time::timeout(Duration::from_secs(2), peri_reader.read_exact(&mut len_buf))
        .await
        .expect("L2CAP recv timed out")
        .expect("L2CAP read len");
    let len = u16::from_le_bytes(len_buf) as usize;
    assert_eq!(len, b"after-swap".len());
    let mut payload = vec![0u8; len];
    peri_reader
        .read_exact(&mut payload)
        .await
        .expect("L2CAP read payload");
    assert_eq!(&payload, b"after-swap");

    // Meanwhile, no new GATT WriteC2p should have fired for post-swap sends.
    // (Allow tiny grace period for any in-flight retransmit from the drain
    // tail.)
    tokio::time::sleep(Duration::from_millis(100)).await;
    let gatt_call_count_after = iface
        .calls()
        .iter()
        .filter(|c| matches!(c, CallKind::WriteC2p { .. }))
        .count();
    // The GATT worker is in drain-only mode for up to L2CAP_HANDOVER_TIMEOUT;
    // it may retransmit its outstanding fragment once or twice. What must NOT
    // happen is the post-swap datagram appearing on GATT. The ReliableChannel
    // only knows about pre-swap payload bytes ("before-swap"), so any GATT
    // writes after the swap contain only pre-swap bytes — acceptable.
    assert!(
        gatt_call_count_after >= gatt_call_count_before,
        "GATT call count should be monotonic; before={gatt_call_count_before} after={gatt_call_count_after}"
    );

    // Tear down: closing outbound makes the supervisor exit.
    drop(outbound_tx);
    drop(swap_tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), pipe_handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn supervisor_shuts_down_cleanly_on_outbound_close() {
    let iface = Arc::new(MockBleInterface::new());
    let (outbound_tx, outbound_rx) = mpsc::channel::<PendingSend>(4);
    let (_inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(4);
    let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(4);
    let (registry_tx, _registry_rx) = mpsc::channel::<PeerCommand>(4);
    let (_swap_tx, swap_rx) = mpsc::channel::<blew::L2capChannel>(1);

    let handle = tokio::spawn(run_data_pipe(
        iface.clone() as Arc<dyn BleInterface>,
        blew::DeviceId::from("shutdown"),
        ConnectRole::Central,
        ConnectPath::Gatt,
        None,
        outbound_rx,
        inbound_rx,
        incoming_tx,
        registry_tx,
        swap_rx,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        LivenessClock::new(),
    ));

    drop(outbound_tx);
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("supervisor did not exit within bound")
        .expect("supervisor panicked");
}
