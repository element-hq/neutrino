//! L2CAP data-path primitives for `BleTransport`.
//!
//! Framing and I/O task spawning for L2CAP CoC streams (`AsyncRead +
//! AsyncWrite`). Used by `pipe.rs` when a peer's `ConnectPath` is `L2cap`.
//!
//! Wire format: each datagram is `[u16 LE length] [payload bytes]`. There is
//! no pre-handshake identity exchange — the I/O task is told the peer's
//! `blew::DeviceId` up front by the caller.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use super::IncomingPacket;
use super::mtu::MAX_DATAGRAM_SIZE;

pub(super) async fn write_framed_datagram<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_DATAGRAM_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "datagram exceeds MAX_DATAGRAM_SIZE",
        ));
    }
    let len = u16::try_from(payload.len()).unwrap();
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Returns `Ok(None)` on clean EOF at a frame boundary.
pub(super) async fn read_framed_datagram<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 2];
    // Separate first-byte read distinguishes clean EOF from truncated frame.
    if reader.read(&mut len_buf[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut len_buf[1..]).await?;
    let len = u16::from_le_bytes(len_buf) as usize;
    if len > MAX_DATAGRAM_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds MAX_DATAGRAM_SIZE"),
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_l2cap_io_tasks<R, W>(
    reader: R,
    writer: W,
    device_id: blew::DeviceId,
    incoming_tx: mpsc::Sender<IncomingPacket>,
    last_rx_at: crate::transport::peer::LivenessClock,
    tearing_down: Arc<AtomicBool>,
    empty_frames_counter: Arc<AtomicU64>,
) -> (
    mpsc::Sender<Vec<u8>>,
    JoinHandle<()>,
    JoinHandle<()>,
    Arc<Notify>,
)
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(64);

    let done = Arc::new(Notify::new());

    let dev = device_id.clone();
    let send_guard = NotifyOnDrop(Arc::clone(&done));
    let mut writer = writer;
    let send_teardown = Arc::clone(&tearing_down);
    let send_task = tokio::spawn(async move {
        let _guard = send_guard;
        while let Some(datagram) = outbound_rx.recv().await {
            trace!(device = %dev, len = datagram.len(), "l2cap send task -> write_framed_datagram");
            if let Err(e) = write_framed_datagram(&mut writer, &datagram).await {
                if send_teardown.load(Ordering::Relaxed) {
                    debug!(device = %dev, ?e, "l2cap send task exiting during teardown");
                } else {
                    warn!(device = %dev, ?e, "l2cap send task exiting on writer error");
                }
                break;
            }
            trace!(device = %dev, "l2cap send task wrote datagram");
        }
        debug!(device = %dev, "l2cap send task exiting");
    });

    let recv_guard = NotifyOnDrop(Arc::clone(&done));
    let mut reader = reader;
    let recv_teardown = Arc::clone(&tearing_down);
    let recv_task = tokio::spawn(async move {
        let _guard = recv_guard;
        loop {
            match read_framed_datagram(&mut reader).await {
                Ok(Some(data)) => {
                    last_rx_at.bump();
                    // Guard iroh's `socket.rs:575` div-by-zero panic on
                    // `stride == 0`. A `[0x00, 0x00]` frame on the wire
                    // (either from a noq/iroh regression on the peer's
                    // outbound side, or from a misbehaving peer) decodes
                    // to an empty payload; forwarding it would panic the
                    // iroh socket driver. Drop + count for diagnosis.
                    if data.is_empty() {
                        empty_frames_counter.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            device = %device_id,
                            "l2cap recv task dropping zero-length frame (would trip iroh stride=0 panic)"
                        );
                        continue;
                    }
                    if incoming_tx
                        .send(IncomingPacket {
                            device_id: device_id.clone(),
                            data: data.into(),
                        })
                        .await
                        .is_err()
                    {
                        debug!(device = %device_id, "incoming_tx closed, exiting recv task");
                        break;
                    }
                }
                Ok(None) => {
                    debug!(device = %device_id, "l2cap recv task EOF");
                    break;
                }
                Err(e) => {
                    if recv_teardown.load(Ordering::Relaxed) {
                        debug!(device = %device_id, ?e, "l2cap recv task exiting during teardown");
                    } else {
                        warn!(device = %device_id, ?e, "l2cap recv task exiting on reader error");
                    }
                    break;
                }
            }
        }
    });

    (outbound_tx, send_task, recv_task, done)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blew::l2cap::L2capChannel;
    use tokio::io::split;

    #[tokio::test]
    async fn round_trip_single_datagram() {
        let (a, b) = L2capChannel::pair(8192);
        let (mut a_rd, mut a_wr) = split(a);
        let (mut b_rd, mut b_wr) = split(b);

        let payload = b"hello world".to_vec();
        write_framed_datagram(&mut a_wr, &payload).await.unwrap();
        let got = read_framed_datagram(&mut b_rd).await.unwrap().unwrap();
        assert_eq!(got, payload);

        let payload2 = b"reply".to_vec();
        write_framed_datagram(&mut b_wr, &payload2).await.unwrap();
        let got2 = read_framed_datagram(&mut a_rd).await.unwrap().unwrap();
        assert_eq!(got2, payload2);
    }

    #[tokio::test]
    async fn back_to_back_datagrams() {
        let (a, b) = L2capChannel::pair(8192);
        let (_a_rd, mut a_wr) = split(a);
        let (mut b_rd, _b_wr) = split(b);

        for i in 0_u8..5 {
            let payload = vec![i; 10];
            write_framed_datagram(&mut a_wr, &payload).await.unwrap();
        }
        for i in 0_u8..5 {
            let got = read_framed_datagram(&mut b_rd).await.unwrap().unwrap();
            assert_eq!(got, vec![i; 10]);
        }
    }

    #[tokio::test]
    async fn oversized_payload_rejected_by_writer() {
        let (a, _b) = L2capChannel::pair(8192);
        let (_rd, mut wr) = split(a);
        let too_big = vec![0_u8; MAX_DATAGRAM_SIZE + 1];
        let err = write_framed_datagram(&mut wr, &too_big).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (a, b) = L2capChannel::pair(8192);
        let (_a_rd, mut a_wr) = split(a);
        let (mut b_rd, _b_wr) = split(b);
        a_wr.shutdown().await.unwrap();
        let got = read_framed_datagram(&mut b_rd).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn truncated_length_prefix_is_error() {
        let (a, b) = L2capChannel::pair(8192);
        let (_a_rd, mut a_wr) = split(a);
        let (mut b_rd, _b_wr) = split(b);
        a_wr.write_all(&[0x01]).await.unwrap();
        a_wr.shutdown().await.unwrap();
        let err = read_framed_datagram(&mut b_rd).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn truncated_payload_is_error() {
        let (a, b) = L2capChannel::pair(8192);
        let (_a_rd, mut a_wr) = split(a);
        let (mut b_rd, _b_wr) = split(b);
        a_wr.write_all(&10_u16.to_le_bytes()).await.unwrap();
        a_wr.write_all(&[1, 2, 3]).await.unwrap();
        a_wr.shutdown().await.unwrap();
        let err = read_framed_datagram(&mut b_rd).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn spawn_l2cap_io_tasks_round_trips_datagrams() {
        use tokio::sync::mpsc;

        let (central_side, peripheral_side) = L2capChannel::pair(8192);

        let (a_rd, a_wr) = split(central_side);
        let (incoming_tx, mut incoming_rx) = mpsc::channel(16);
        let device_id = blew::DeviceId::from("l2cap-test");
        let (tx, _send_task, _recv_task, _done) = super::spawn_l2cap_io_tasks(
            a_rd,
            a_wr,
            device_id.clone(),
            incoming_tx,
            crate::transport::peer::LivenessClock::new(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        );

        let (mut b_rd, mut b_wr) = split(peripheral_side);

        tx.send(b"ping".to_vec()).await.unwrap();
        let got = super::read_framed_datagram(&mut b_rd)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, b"ping");

        super::write_framed_datagram(&mut b_wr, b"pong")
            .await
            .unwrap();
        let pkt = tokio::time::timeout(std::time::Duration::from_secs(1), incoming_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pkt.data.as_ref(), b"pong");
        assert_eq!(pkt.device_id, device_id);
    }

    #[tokio::test]
    async fn spawn_l2cap_io_tasks_stamps_incoming_with_device_id() {
        use tokio::sync::mpsc;

        let (central_side, peripheral_side) = L2capChannel::pair(8192);
        let (a_rd, a_wr) = split(central_side);
        let (incoming_tx, mut incoming_rx) = mpsc::channel(16);
        let device_id = blew::DeviceId::from("device-id-stamp");
        let (_tx, _send_task, _recv_task, _done) = super::spawn_l2cap_io_tasks(
            a_rd,
            a_wr,
            device_id.clone(),
            incoming_tx,
            crate::transport::peer::LivenessClock::new(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        );

        let (_b_rd, mut b_wr) = split(peripheral_side);
        for i in 0_u8..3 {
            let payload = vec![i; 8];
            super::write_framed_datagram(&mut b_wr, &payload)
                .await
                .unwrap();
        }

        for i in 0_u8..3 {
            let pkt = tokio::time::timeout(std::time::Duration::from_secs(1), incoming_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pkt.device_id, device_id);
            assert_eq!(pkt.data.as_ref(), vec![i; 8].as_slice());
        }
    }

    #[tokio::test]
    async fn done_notify_fires_on_task_exit() {
        let (a, b) = L2capChannel::pair(8192);
        let (a_rd, a_wr) = split(a);
        let (incoming_tx, _incoming_rx) = mpsc::channel(16);
        let (_tx, _send_task, _recv_task, done) = super::spawn_l2cap_io_tasks(
            a_rd,
            a_wr,
            blew::DeviceId::from("exit-test"),
            incoming_tx,
            crate::transport::peer::LivenessClock::new(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        );

        drop(b);

        tokio::time::timeout(std::time::Duration::from_secs(1), done.notified())
            .await
            .expect("done notify should fire when I/O task exits");
    }

    #[tokio::test]
    async fn spawn_l2cap_io_tasks_drops_zero_length_frame_and_counts() {
        use tokio::sync::mpsc;

        let (central_side, peripheral_side) = L2capChannel::pair(8192);
        let (a_rd, a_wr) = split(central_side);
        let (incoming_tx, mut incoming_rx) = mpsc::channel(16);
        let empty_frames = Arc::new(AtomicU64::new(0));
        let device_id = blew::DeviceId::from("empty-frame-recv");
        let (_tx, _send_task, _recv_task, _done) = super::spawn_l2cap_io_tasks(
            a_rd,
            a_wr,
            device_id.clone(),
            incoming_tx,
            crate::transport::peer::LivenessClock::new(),
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&empty_frames),
        );

        // Peer writes a zero-length framed datagram (`[0x00, 0x00]`), then a
        // normal one. The recv task should drop the empty frame (bumping
        // `empty_frames`) and deliver only the normal one.
        let (_b_rd, mut b_wr) = split(peripheral_side);
        b_wr.write_all(&0_u16.to_le_bytes()).await.unwrap();
        super::write_framed_datagram(&mut b_wr, b"real")
            .await
            .unwrap();

        let pkt = tokio::time::timeout(std::time::Duration::from_secs(1), incoming_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pkt.device_id, device_id);
        assert_eq!(pkt.data.as_ref(), b"real");
        assert_eq!(
            empty_frames.load(Ordering::Relaxed),
            1,
            "empty frame must be counted and dropped"
        );
    }

    #[tokio::test]
    async fn done_notify_fires_on_task_abort() {
        let (a, _b) = L2capChannel::pair(8192);
        let (a_rd, a_wr) = split(a);
        let (incoming_tx, _incoming_rx) = mpsc::channel(16);
        let (_tx, send_task, recv_task, done) = super::spawn_l2cap_io_tasks(
            a_rd,
            a_wr,
            blew::DeviceId::from("abort-test"),
            incoming_tx,
            crate::transport::peer::LivenessClock::new(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        );

        send_task.abort();
        recv_task.abort();

        tokio::time::timeout(std::time::Duration::from_secs(1), done.notified())
            .await
            .expect("done notify should fire when tasks are aborted");
    }
}
