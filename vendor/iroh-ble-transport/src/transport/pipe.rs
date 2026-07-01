//! Per-peer data pipe task. A supervisor owns `outbound_rx` and `inbound_rx`,
//! forwarding each item to whichever worker (GATT or L2CAP) is currently
//! active. On `swap_rx.recv()`, the supervisor spawns the L2CAP worker,
//! drops the GATT worker's forwarding senders (so it flushes and exits), and
//! schedules a delayed abort after `L2CAP_HANDOVER_TIMEOUT` to bound the
//! drain tail.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Duration;

/// Wraps a `JoinHandle` so the inner task is aborted (not just detached) when
/// the wrapper is dropped — including when the owning task is cancelled mid
/// `select!`. Used to keep child tasks like the GATT send loop from outliving
/// their parent worker after a supervisor swap aborts the worker.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

use crate::transport::dedup::L2CAP_HANDOVER_TIMEOUT;
use crate::transport::driver::IncomingPacket;
use crate::transport::interface::BleInterface;
use crate::transport::mtu::{ATT_OVERHEAD, MIN_SANE_MTU, resolve_chunk_size};
use crate::transport::peer::{ConnectPath, ConnectRole, LivenessClock, PeerCommand, PendingSend};
use crate::transport::reliable::ReliableChannel;

/// Conservative initial chunk size for a freshly started GATT pipe, used
/// while the async MTU resolver runs in parallel. Sized to the BLE-spec
/// default ATT MTU floor (`MIN_SANE_MTU` = 24) minus ATT overhead so any
/// fragments sent before the resolver lands are safe on any peer. The
/// resolver calls `ReliableChannel::set_chunk_size` to bump this up.
const INITIAL_CHUNK_SIZE: usize = (MIN_SANE_MTU as usize) - ATT_OVERHEAD;

enum ActiveWorker {
    Gatt {
        outbound_fwd_tx: mpsc::Sender<PendingSend>,
        inbound_fwd_tx: mpsc::Sender<Bytes>,
        shutdown_tx: oneshot::Sender<()>,
        teardown_flag: Arc<AtomicBool>,
        handle: JoinHandle<()>,
    },
    L2cap {
        outbound_fwd_tx: mpsc::Sender<PendingSend>,
        teardown_flag: Arc<AtomicBool>,
        handle: JoinHandle<()>,
    },
}

impl ActiveWorker {
    fn is_l2cap(&self) -> bool {
        matches!(self, ActiveWorker::L2cap { .. })
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_data_pipe(
    iface: Arc<dyn BleInterface>,
    device_id: blew::DeviceId,
    role: ConnectRole,
    initial_path: ConnectPath,
    initial_l2cap: Option<blew::L2capChannel>,
    outbound_rx: mpsc::Receiver<PendingSend>,
    inbound_rx: mpsc::Receiver<Bytes>,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    swap_rx: mpsc::Receiver<blew::L2capChannel>,
    retransmit_counter: Arc<AtomicU64>,
    truncation_counter: Arc<AtomicU64>,
    empty_frames_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) {
    run_pipe_supervisor(
        iface,
        device_id,
        role,
        initial_path,
        initial_l2cap,
        outbound_rx,
        inbound_rx,
        incoming_tx,
        registry_tx,
        swap_rx,
        retransmit_counter,
        truncation_counter,
        empty_frames_counter,
        last_rx_at,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_pipe_supervisor(
    iface: Arc<dyn BleInterface>,
    device_id: blew::DeviceId,
    role: ConnectRole,
    initial_path: ConnectPath,
    initial_l2cap: Option<blew::L2capChannel>,
    mut outbound_rx: mpsc::Receiver<PendingSend>,
    mut inbound_rx: mpsc::Receiver<Bytes>,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    swap_rx: mpsc::Receiver<blew::L2capChannel>,
    retransmit_counter: Arc<AtomicU64>,
    truncation_counter: Arc<AtomicU64>,
    empty_frames_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) {
    let mut swap_rx: Option<mpsc::Receiver<blew::L2capChannel>> = Some(swap_rx);
    let mut active = match initial_path {
        ConnectPath::Gatt => spawn_gatt_worker(
            Arc::clone(&iface),
            device_id.clone(),
            role,
            incoming_tx.clone(),
            registry_tx.clone(),
            Arc::clone(&retransmit_counter),
            Arc::clone(&truncation_counter),
            last_rx_at.clone(),
        ),
        ConnectPath::L2cap => {
            let Some(channel) = initial_l2cap else {
                tracing::error!(device = %device_id, "StartDataPipe(L2cap) without channel");
                return;
            };
            spawn_l2cap_worker(
                device_id.clone(),
                channel,
                incoming_tx.clone(),
                registry_tx.clone(),
                Arc::clone(&empty_frames_counter),
                last_rx_at.clone(),
            )
        }
    };

    let mut l2cap_timeout_reported = false;

    loop {
        tokio::select! {
            maybe_send = outbound_rx.recv() => {
                let Some(send) = maybe_send else { break; };
                match forward_outbound(&active, send, &device_id, &registry_tx, &mut l2cap_timeout_reported).await {
                    ForwardResult::Ok => {}
                    ForwardResult::WorkerGone => break,
                    ForwardResult::L2capTimeout => break,
                }
            }
            maybe_bytes = inbound_rx.recv() => {
                let Some(bytes) = maybe_bytes else { break; };
                match &active {
                    ActiveWorker::Gatt { inbound_fwd_tx, .. } => {
                        if inbound_fwd_tx.send(bytes).await.is_err() {
                            break;
                        }
                    }
                    // Post-swap: L2CAP reads from its channel directly, and the
                    // old GATT worker's inbound_fwd_tx was dropped with the old
                    // ActiveWorker. Late GATT fragments are unrecoverable and
                    // the drain tail can only flush already-queued outbound
                    // ACKs, not new inbound work.
                    ActiveWorker::L2cap { .. } => {}
                }
            }
            maybe_chan = recv_swap(&mut swap_rx) => {
                let Some(channel) = maybe_chan else {
                    // swap_tx was dropped; disable the swap arm permanently so
                    // the select loop does not busy-poll on a closed recv.
                    swap_rx = None;
                    continue;
                };
                if active.is_l2cap() {
                    tracing::debug!(
                        device = %device_id,
                        "ignoring redundant L2CAP swap request; already on L2CAP"
                    );
                    continue;
                }
                let new_active = spawn_l2cap_worker(
                    device_id.clone(),
                    channel,
                    incoming_tx.clone(),
                    registry_tx.clone(),
                    Arc::clone(&empty_frames_counter),
                    last_rx_at.clone(),
                );
                let old = std::mem::replace(&mut active, new_active);
                tracing::debug!(
                    device = %device_id,
                    old_path = "Gatt",
                    new_path = "L2cap",
                    "retiring old pipe after L2CAP handover"
                );
                spawn_drain_old_worker(old, device_id.clone(), L2CAP_HANDOVER_TIMEOUT);
            }
        }
    }

    // Supervisor exiting: drop the forwarding senders so the active worker
    // observes outbound/inbound EOF and tears itself down, then wait briefly
    // for it to exit (so its send sub-tasks and any outstanding ACK flushes
    // are joined) before returning. The join is bounded so a wedged worker
    // cannot hold the caller hostage — on timeout we abort the task rather
    // than letting the JoinHandle drop (which would only detach it).
    let mut handle = match active {
        ActiveWorker::Gatt {
            teardown_flag,
            handle,
            ..
        } => {
            teardown_flag.store(true, Ordering::Relaxed);
            handle
        }
        ActiveWorker::L2cap {
            teardown_flag,
            handle,
            ..
        } => {
            teardown_flag.store(true, Ordering::Relaxed);
            handle
        }
    };
    if tokio::time::timeout(L2CAP_HANDOVER_TIMEOUT, &mut handle)
        .await
        .is_err()
    {
        handle.abort();
        tracing::debug!(
            device = %device_id,
            "pipe supervisor: worker did not exit within handover timeout; aborted during teardown"
        );
    }
}

/// Poll the optional swap receiver. If `None`, never resolves — lets the
/// supervisor's `select!` ignore the arm without busy-looping.
async fn recv_swap(
    rx: &mut Option<mpsc::Receiver<blew::L2capChannel>>,
) -> Option<blew::L2capChannel> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

enum ForwardResult {
    Ok,
    WorkerGone,
    L2capTimeout,
}

async fn forward_outbound(
    active: &ActiveWorker,
    send: PendingSend,
    device_id: &blew::DeviceId,
    registry_tx: &mpsc::Sender<PeerCommand>,
    l2cap_timeout_reported: &mut bool,
) -> ForwardResult {
    match active {
        ActiveWorker::Gatt {
            outbound_fwd_tx, ..
        } => {
            if outbound_fwd_tx.send(send).await.is_err() {
                return ForwardResult::WorkerGone;
            }
            ForwardResult::Ok
        }
        ActiveWorker::L2cap {
            outbound_fwd_tx, ..
        } => match tokio::time::timeout(L2CAP_HANDOVER_TIMEOUT, outbound_fwd_tx.send(send)).await {
            Ok(Ok(())) => ForwardResult::Ok,
            Ok(Err(_closed)) => ForwardResult::WorkerGone,
            Err(_elapsed) => {
                if !*l2cap_timeout_reported {
                    *l2cap_timeout_reported = true;
                    let _ = registry_tx
                        .send(PeerCommand::L2capHandoverTimeout {
                            device_id: device_id.clone(),
                        })
                        .await;
                }
                ForwardResult::L2capTimeout
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_gatt_worker(
    iface: Arc<dyn BleInterface>,
    device_id: blew::DeviceId,
    role: ConnectRole,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    retransmit_counter: Arc<AtomicU64>,
    truncation_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) -> ActiveWorker {
    let (outbound_fwd_tx, outbound_fwd_rx) = mpsc::channel::<PendingSend>(32);
    let (inbound_fwd_tx, inbound_fwd_rx) = mpsc::channel::<Bytes>(64);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let teardown_flag = Arc::new(AtomicBool::new(false));
    let handle = tokio::spawn(run_gatt_pipe(
        iface,
        device_id,
        role,
        outbound_fwd_rx,
        inbound_fwd_rx,
        shutdown_rx,
        Arc::clone(&teardown_flag),
        incoming_tx,
        registry_tx,
        retransmit_counter,
        truncation_counter,
        last_rx_at,
    ));
    ActiveWorker::Gatt {
        outbound_fwd_tx,
        inbound_fwd_tx,
        shutdown_tx,
        teardown_flag,
        handle,
    }
}

fn spawn_l2cap_worker(
    device_id: blew::DeviceId,
    channel: blew::L2capChannel,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    empty_frames_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) -> ActiveWorker {
    let (outbound_fwd_tx, outbound_fwd_rx) = mpsc::channel::<PendingSend>(32);
    let teardown_flag = Arc::new(AtomicBool::new(false));
    let handle = tokio::spawn(run_l2cap_pipe(
        device_id,
        channel,
        outbound_fwd_rx,
        Arc::clone(&teardown_flag),
        incoming_tx,
        registry_tx,
        empty_frames_counter,
        last_rx_at,
    ));
    ActiveWorker::L2cap {
        outbound_fwd_tx,
        teardown_flag,
        handle,
    }
}

fn spawn_drain_old_worker(old: ActiveWorker, device_id: blew::DeviceId, timeout: Duration) {
    tokio::spawn(async move {
        // Dropping the forwarding senders closes the worker's input channels;
        // the GATT worker's select loop then breaks, marks the ReliableChannel
        // dead, and joins its send sub-task before returning. On timeout we
        // abort the task — dropping the JoinHandle alone would only detach it,
        // letting a wedged worker leak.
        let mut handle = match old {
            ActiveWorker::Gatt {
                shutdown_tx,
                teardown_flag,
                handle,
                ..
            } => {
                teardown_flag.store(true, Ordering::Relaxed);
                let _ = shutdown_tx.send(());
                handle
            }
            ActiveWorker::L2cap {
                teardown_flag,
                handle,
                ..
            } => {
                teardown_flag.store(true, Ordering::Relaxed);
                handle
            }
        };
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {
                tracing::debug!(
                    device = %device_id,
                    "old pipe worker drained cleanly during teardown"
                );
            }
            Ok(Err(join_err)) => {
                tracing::debug!(
                    device = %device_id,
                    ?join_err,
                    "old pipe worker join error during teardown"
                );
            }
            Err(_elapsed) => {
                handle.abort();
                // The abort is the recovery path — we don't leak the worker
                // and the new pipe is already serving traffic. Happens when
                // the old GATT worker still has in-flight ACK-waits at swap
                // time; not actionable, so debug rather than warn.
                tracing::debug!(
                    device = %device_id,
                    "old pipe worker did not drain within handover timeout; aborted during teardown"
                );
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_gatt_pipe(
    iface: Arc<dyn BleInterface>,
    device_id: blew::DeviceId,
    role: ConnectRole,
    mut outbound_rx: mpsc::Receiver<PendingSend>,
    mut inbound_rx: mpsc::Receiver<Bytes>,
    mut shutdown_rx: oneshot::Receiver<()>,
    teardown_flag: Arc<AtomicBool>,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    retransmit_counter: Arc<AtomicU64>,
    truncation_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) {
    // Start with a conservative chunk size so the select loop can begin
    // processing inbound fragments immediately. Blocking on the MTU resolver
    // here would starve inbound reassembly for up to `MTU_READY_DEADLINE`
    // (≈3s) — enough to collide with an L2CAP accept landing mid-handshake.
    let (channel, mut datagram_rx) =
        ReliableChannel::new(INITIAL_CHUNK_SIZE, retransmit_counter, truncation_counter);
    let channel = Arc::new(channel);

    let resolver_handle = {
        let channel = Arc::clone(&channel);
        let iface = Arc::clone(&iface);
        let device_id = device_id.clone();
        tokio::spawn(async move {
            let chunk_size = resolve_chunk_size(iface.as_ref(), &device_id).await;
            channel.set_chunk_size(chunk_size);
        })
    };
    let _resolver_guard = AbortOnDrop(resolver_handle);

    let send_loop_handle = {
        let channel = Arc::clone(&channel);
        let iface = Arc::clone(&iface);
        let device_id = device_id.clone();
        let send_loop_teardown = Arc::clone(&teardown_flag);
        let span = tracing::info_span!("ble_pipe", device = %device_id);
        tokio::spawn(tracing::Instrument::instrument(
            async move {
                channel
                    .run_send_loop(
                        move |bytes| {
                            let iface = Arc::clone(&iface);
                            let device_id = device_id.clone();
                            let role = role;
                            async move {
                                let buf = Bytes::from(bytes);
                                let result = match role {
                                    ConnectRole::Central => iface.write_c2p(&device_id, buf).await,
                                    ConnectRole::Peripheral => {
                                        iface.notify_p2c(&device_id, buf).await
                                    }
                                };
                                result.map_err(|e| format!("{e}"))
                            }
                        },
                        {
                            let teardown_flag = Arc::clone(&send_loop_teardown);
                            move || teardown_flag.load(Ordering::Relaxed)
                        },
                    )
                    .await
            },
            span,
        ))
    };

    // If the supervisor aborts us mid-swap, the send loop is a separately
    // spawned task — dropping its JoinHandle would only detach it, leaving an
    // orphan that keeps retransmitting ghost fragments over the now-handed-off
    // channel for `LINK_DEAD_DEADLINE` (≈6 s). Guard it so cancellation here
    // also tears down the send loop.
    let send_loop_handle = AbortOnDrop(send_loop_handle);
    tokio::pin!(send_loop_handle);
    let mut link_dead = false;
    let mut send_loop_done = false;
    loop {
        tokio::select! {
            maybe_send = outbound_rx.recv() => {
                match maybe_send {
                    Some(send) => {
                        let _ = channel.enqueue_datagram(send.datagram.to_vec()).await;
                        send.waker.wake();
                    }
                    None => break,
                }
            }
            maybe_bytes = inbound_rx.recv() => {
                match maybe_bytes {
                    Some(bytes) => channel.receive_fragment(&bytes).await,
                    None => break,
                }
            }
            maybe_datagram = datagram_rx.recv() => {
                match maybe_datagram {
                    Some(data) => {
                        tracing::trace!(
                            device = %device_id,
                            len = data.len(),
                            "pipe reassembled datagram -> incoming_tx"
                        );
                        last_rx_at.bump();
                        let _ = incoming_tx
                            .send(IncomingPacket {
                                device_id: device_id.clone(),
                                data: Bytes::from(data),
                            })
                            .await;
                    }
                    None => break,
                }
            }
            shutdown = &mut shutdown_rx => {
                let _ = shutdown;
                teardown_flag.store(true, Ordering::Relaxed);
                tracing::trace!(device = %device_id, "gatt pipe quiesce requested");
                break;
            }
            join = &mut send_loop_handle => {
                send_loop_done = true;
                if let Ok(Err(_link_dead)) = join {
                    link_dead = true;
                }
                break;
            }
        }
    }

    if !send_loop_done {
        channel.mark_dead().await;
        let _ = (&mut send_loop_handle).await;
    }

    if link_dead {
        let _ = registry_tx
            .send(PeerCommand::Stalled {
                device_id: device_id.clone(),
            })
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_l2cap_pipe(
    device_id: blew::DeviceId,
    channel: blew::L2capChannel,
    mut outbound_rx: mpsc::Receiver<PendingSend>,
    teardown_flag: Arc<AtomicBool>,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    registry_tx: mpsc::Sender<PeerCommand>,
    empty_frames_counter: Arc<AtomicU64>,
    last_rx_at: LivenessClock,
) {
    let (reader, writer) = tokio::io::split(channel);

    let (l2cap_tx, _send_task, _recv_task, done) = crate::transport::l2cap::spawn_l2cap_io_tasks(
        reader,
        writer,
        device_id.clone(),
        incoming_tx,
        last_rx_at,
        Arc::clone(&teardown_flag),
        Arc::clone(&empty_frames_counter),
    );

    let mut io_died = false;
    loop {
        tokio::select! {
            maybe_send = outbound_rx.recv() => {
                match maybe_send {
                    Some(send) => {
                        let datagram = send.datagram.to_vec();
                        tracing::trace!(
                            device = %device_id,
                            tx_gen = send.tx_gen,
                            len = datagram.len(),
                            "l2cap pipe got outbound"
                        );
                        // Guard iroh's `socket.rs:575` div-by-zero panic on the
                        // peer: if we forward a zero-length Transmit onto the
                        // wire, the remote's poll_recv hands iroh `stride = 0`
                        // and it panics. Ack the send so the waker unblocks
                        // (iroh treats it as delivered), but skip the wire.
                        if datagram.is_empty() {
                            empty_frames_counter.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                device = %device_id,
                                tx_gen = send.tx_gen,
                                "l2cap pipe dropping zero-length outbound datagram; not forwarding to peer"
                            );
                            send.waker.wake();
                            continue;
                        }
                        match l2cap_tx.send(datagram).await {
                            Ok(()) => {
                                tracing::trace!(
                                    device = %device_id,
                                    tx_gen = send.tx_gen,
                                    "l2cap pipe forwarded outbound to send task"
                                );
                            }
                            Err(_closed) => {
                                if teardown_flag.load(Ordering::Relaxed) {
                                    tracing::debug!(
                                        device = %device_id,
                                        "l2cap pipe: send task channel closed during teardown; stopping"
                                    );
                                } else {
                                    tracing::warn!(
                                        device = %device_id,
                                        "l2cap pipe: send task channel closed unexpectedly; stopping"
                                    );
                                }
                                send.waker.wake();
                                io_died = true;
                                break;
                            }
                        }
                        send.waker.wake();
                    }
                    None => break,
                }
            }
            _ = done.notified() => {
                io_died = true;
                break;
            }
        }
    }

    if io_died {
        let _ = registry_tx
            .send(PeerCommand::Stalled {
                device_id: device_id.clone(),
            })
            .await;
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::transport::peer::PendingSend;
    use crate::transport::test_util::{CallKind, MockBleInterface};
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[tokio::test]
    async fn outbound_datagram_reaches_iface_write_c2p() {
        let iface = Arc::new(MockBleInterface::new());
        let (outbound_tx, outbound_rx) = mpsc::channel::<PendingSend>(4);
        let (_inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(4);
        let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(4);
        let (registry_tx, _registry_rx) = mpsc::channel::<PeerCommand>(4);
        let (_swap_tx, swap_rx) = mpsc::channel::<blew::L2capChannel>(1);

        let device_id = blew::DeviceId::from("pipe-central");
        tokio::spawn(run_data_pipe(
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

        outbound_tx
            .send(PendingSend {
                tx_gen: 1,
                datagram: Bytes::from_static(b"hello-pipe"),
                waker: noop_waker(),
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let calls = iface.calls();
                if calls.iter().any(|c| matches!(c, CallKind::WriteC2p { .. })) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expected WriteC2p call");
    }

    #[tokio::test]
    async fn peripheral_role_uses_notify_p2c() {
        let iface = Arc::new(MockBleInterface::new());
        let (outbound_tx, outbound_rx) = mpsc::channel::<PendingSend>(4);
        let (_inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(4);
        let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(4);
        let (registry_tx, _registry_rx) = mpsc::channel::<PeerCommand>(4);
        let (_swap_tx, swap_rx) = mpsc::channel::<blew::L2capChannel>(1);

        tokio::spawn(run_data_pipe(
            iface.clone() as Arc<dyn BleInterface>,
            blew::DeviceId::from("pipe-peri"),
            ConnectRole::Peripheral,
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

        outbound_tx
            .send(PendingSend {
                tx_gen: 1,
                datagram: Bytes::from_static(b"peri-out"),
                waker: noop_waker(),
            })
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if iface
                    .calls()
                    .iter()
                    .any(|c| matches!(c, CallKind::NotifyP2c { .. }))
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expected NotifyP2c call");
    }

    #[tokio::test]
    async fn gatt_worker_quiesce_exits_without_stalled() {
        let iface = Arc::new(MockBleInterface::new());
        let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(4);
        let (registry_tx, mut registry_rx) = mpsc::channel::<PeerCommand>(4);

        let worker = spawn_gatt_worker(
            iface as Arc<dyn BleInterface>,
            blew::DeviceId::from("pipe-quiesce"),
            ConnectRole::Central,
            incoming_tx,
            registry_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            LivenessClock::new(),
        );

        let (shutdown_tx, handle) = match worker {
            ActiveWorker::Gatt {
                shutdown_tx,
                handle,
                ..
            } => (shutdown_tx, handle),
            ActiveWorker::L2cap { .. } => panic!("expected GATT worker"),
        };

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("gatt worker should exit promptly")
            .expect("gatt worker should not panic");

        // Worker exit drops its registry_tx clone, so `Disconnected` is the
        // expected steady state here. Either Empty or Disconnected satisfies
        // "nothing was ever sent"; only an `Ok(...)` would indicate a leaked
        // Stalled notification.
        let got = registry_rx.try_recv();
        assert!(
            got.is_err(),
            "quiesce must not emit any PeerCommand; got {got:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn l2cap_pipe_drops_zero_length_outbound_and_counts() {
        let iface = Arc::new(MockBleInterface::new());
        let (outbound_tx, outbound_rx) = mpsc::channel::<PendingSend>(4);
        let (_inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(4);
        let (incoming_tx, _incoming_rx) = mpsc::channel::<IncomingPacket>(4);
        let (registry_tx, _registry_rx) = mpsc::channel::<PeerCommand>(4);
        let (_swap_tx, swap_rx) = mpsc::channel::<blew::L2capChannel>(1);

        let (central_side, peripheral_side) = blew::L2capChannel::pair(8192);
        let empty_frames = Arc::new(AtomicU64::new(0));

        let _pipe = tokio::spawn(run_data_pipe(
            iface as Arc<dyn BleInterface>,
            blew::DeviceId::from("l2cap-empty-out"),
            ConnectRole::Central,
            ConnectPath::L2cap,
            Some(central_side),
            outbound_rx,
            inbound_rx,
            incoming_tx,
            registry_tx,
            swap_rx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&empty_frames),
            LivenessClock::new(),
        ));

        // Empty outbound must not be framed onto the wire; only the real one
        // must reach the peer.
        outbound_tx
            .send(PendingSend {
                tx_gen: 1,
                datagram: Bytes::new(),
                waker: noop_waker(),
            })
            .await
            .unwrap();
        outbound_tx
            .send(PendingSend {
                tx_gen: 2,
                datagram: Bytes::from_static(b"post-empty"),
                waker: noop_waker(),
            })
            .await
            .unwrap();

        let (mut peri_rd, _peri_wr) = tokio::io::split(peripheral_side);
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            crate::transport::l2cap::read_framed_datagram(&mut peri_rd),
        )
        .await
        .expect("peer should see a framed datagram")
        .expect("read_framed_datagram must succeed")
        .expect("frame present");
        assert_eq!(got, b"post-empty");
        assert_eq!(
            empty_frames.load(Ordering::Relaxed),
            1,
            "outbound empty must be counted exactly once"
        );
    }
}
