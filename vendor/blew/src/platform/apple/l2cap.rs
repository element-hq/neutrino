//! Shared L2CAP transport construction for Apple platforms.
//!
//! CoreBluetooth vends `NSInputStream` / `NSOutputStream` objects for each
//! `CBL2CAPChannel`. Those streams are run-loop driven and thread-affine enough
//! that the backend needs an explicit owner for all stream lifecycle work.
//!
//! The long-term shape here is a shared reactor thread. Each bridged channel is
//! registered with that reactor, which owns stream scheduling, reads, writes,
//! and close/unschedule. Tokio only bridges bytes between the app-facing
//! transport and the reactor.

#![allow(clippy::cast_possible_truncation)]

use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, mpsc};

use objc2::rc::autoreleasepool;
use objc2_core_bluetooth::CBL2CAPChannel;
use objc2_foundation::{NSDate, NSDefaultRunLoopMode, NSInputStream, NSOutputStream, NSRunLoop};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc as tokio_mpsc;
use tracing::{debug, trace, warn};

use crate::l2cap::L2capChannel;
use crate::platform::apple::helpers::{ObjcSend, retain_send};

const L2CAP_DUPLEX_BUF_SIZE: usize = 65536;
const L2CAP_READ_BUF_SIZE: usize = 4096;
const RUN_LOOP_POLL_SECS: f64 = 0.05;

type InputStream = Arc<ObjcSend<NSInputStream>>;
type OutputStream = Arc<ObjcSend<NSOutputStream>>;

static REACTOR_TX: OnceLock<mpsc::Sender<ReactorCmd>> = OnceLock::new();
static NEXT_CHANNEL_ID: AtomicU64 = AtomicU64::new(1);

struct RegisterChannel {
    id: u64,
    channel_ref: Arc<ObjcSend<CBL2CAPChannel>>,
    input: InputStream,
    output: OutputStream,
    inbound_tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
}

enum ReactorCmd {
    Register(RegisterChannel),
    Write { id: u64, data: Vec<u8> },
    Close { id: u64 },
}

struct ReactorChannel {
    channel_ref: Arc<ObjcSend<CBL2CAPChannel>>,
    input: InputStream,
    output: OutputStream,
    inbound_tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
}

struct L2capReactor {
    cmd_rx: mpsc::Receiver<ReactorCmd>,
    channels: HashMap<u64, ReactorChannel>,
}

fn default_run_loop_mode() -> &'static objc2_foundation::NSRunLoopMode {
    unsafe { NSDefaultRunLoopMode }
}

fn reactor_tx() -> &'static mpsc::Sender<ReactorCmd> {
    REACTOR_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("blew-l2cap-reactor".to_string())
            .spawn(move || L2capReactor::new(rx).run())
            .expect("spawn blew-l2cap-reactor");
        tx
    })
}

fn next_channel_id() -> u64 {
    NEXT_CHANNEL_ID.fetch_add(1, Ordering::Relaxed)
}

fn close_channel(run_loop: &NSRunLoop, id: u64, channel: ReactorChannel, reason: &'static str) {
    let ReactorChannel {
        channel_ref,
        input,
        output,
        inbound_tx: _,
    } = channel;
    trace!(id, reason, "apple L2CAP reactor closing channel");
    unsafe {
        input.removeFromRunLoop_forMode(run_loop, default_run_loop_mode());
        output.removeFromRunLoop_forMode(run_loop, default_run_loop_mode());
    }
    input.close();
    output.close();
    drop(channel_ref);
}

impl ReactorChannel {
    fn write_chunk(&self, id: u64, data: &[u8]) -> bool {
        trace!(
            id,
            len = data.len(),
            "apple L2CAP reactor writing outbound chunk"
        );
        let mut pos = 0;
        while pos < data.len() {
            let n = unsafe {
                self.output.write_maxLength(
                    NonNull::new(data[pos..].as_ptr().cast_mut()).expect("data ptr"),
                    data.len() - pos,
                )
            };
            if n <= 0 {
                trace!(id, "apple L2CAP reactor write failed");
                return false;
            }
            pos += n.cast_unsigned();
        }
        true
    }

    fn forward_available_input(&self, id: u64) -> bool {
        while self.input.hasBytesAvailable() {
            let mut buf = vec![0_u8; L2CAP_READ_BUF_SIZE];
            let n = unsafe {
                self.input
                    .read_maxLength(NonNull::new(buf.as_mut_ptr()).expect("buf ptr"), buf.len())
            };
            if n <= 0 {
                trace!(id, "apple L2CAP reactor input returned EOF/closed");
                return false;
            }
            buf.truncate(n.cast_unsigned());
            trace!(
                id,
                len = buf.len(),
                "apple L2CAP reactor forwarding inbound chunk"
            );
            if self.inbound_tx.send(buf).is_err() {
                trace!(id, "apple L2CAP reactor inbound channel closed");
                return false;
            }
        }

        true
    }
}

impl L2capReactor {
    fn new(cmd_rx: mpsc::Receiver<ReactorCmd>) -> Self {
        Self {
            cmd_rx,
            channels: HashMap::new(),
        }
    }

    fn run(mut self) {
        autoreleasepool(|_| {
            trace!("apple L2CAP reactor started");
            let run_loop = NSRunLoop::currentRunLoop();
            loop {
                autoreleasepool(|_| {
                    self.drain_commands(&run_loop);
                    self.forward_available_input(&run_loop);
                    Self::poll_run_loop(&run_loop);
                });
            }
        });
    }

    fn drain_commands(&mut self, run_loop: &NSRunLoop) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                ReactorCmd::Register(channel) => self.register_channel(run_loop, channel),
                ReactorCmd::Write { id, data } => self.write_channel(run_loop, id, &data),
                ReactorCmd::Close { id } => self.remove_channel(run_loop, id, "close requested"),
            }
        }
    }

    fn register_channel(&mut self, run_loop: &NSRunLoop, channel: RegisterChannel) {
        let RegisterChannel {
            id,
            channel_ref,
            input,
            output,
            inbound_tx,
        } = channel;

        trace!(id, "apple L2CAP reactor registering channel");
        unsafe {
            input.scheduleInRunLoop_forMode(run_loop, default_run_loop_mode());
            output.scheduleInRunLoop_forMode(run_loop, default_run_loop_mode());
        }
        input.open();
        output.open();
        self.channels.insert(
            id,
            ReactorChannel {
                channel_ref,
                input,
                output,
                inbound_tx,
            },
        );
    }

    fn write_channel(&mut self, run_loop: &NSRunLoop, id: u64, data: &[u8]) {
        let should_close = self
            .channels
            .get(&id)
            .is_some_and(|channel| !channel.write_chunk(id, data));
        if should_close {
            self.remove_channel(run_loop, id, "write failed");
        }
    }

    fn forward_available_input(&mut self, run_loop: &NSRunLoop) {
        let mut closed = Vec::new();
        for (&id, channel) in &self.channels {
            if !channel.forward_available_input(id) {
                closed.push(id);
            }
        }
        for id in closed {
            self.remove_channel(run_loop, id, "input ended");
        }
    }

    fn remove_channel(&mut self, run_loop: &NSRunLoop, id: u64, reason: &'static str) {
        if let Some(channel) = self.channels.remove(&id) {
            close_channel(run_loop, id, channel, reason);
        }
    }

    fn poll_run_loop(run_loop: &NSRunLoop) {
        let deadline = NSDate::dateWithTimeIntervalSinceNow(RUN_LOOP_POLL_SECS);
        run_loop.acceptInputForMode_beforeDate(default_run_loop_mode(), &deadline);
    }
}

/// Wrap an Apple `CBL2CAPChannel` into a `L2capChannel` (AsyncRead + AsyncWrite).
///
/// # Ownership / lifetime
/// The `CBL2CAPChannel` is retained for the lifetime of the bridge: Apple's
/// docs say "you should retain the channel yourself" -- without this, CB may
/// release the channel after the delegate callback returns, closing the streams.
pub(crate) fn bridge_l2cap_channel(channel: &CBL2CAPChannel, runtime: &Handle) -> L2capChannel {
    let channel = Arc::new(unsafe { retain_send(channel) });

    let inp = unsafe { channel.inputStream() };
    let Some(inp) = inp else {
        warn!("apple L2CAP channel missing input stream");
        let (app_side, _io_side) = tokio::io::duplex(64);
        return L2capChannel::from_duplex(app_side);
    };
    let out = unsafe { channel.outputStream() };
    let Some(out) = out else {
        warn!("apple L2CAP channel missing output stream");
        let (app_side, _io_side) = tokio::io::duplex(64);
        return L2capChannel::from_duplex(app_side);
    };

    let input = Arc::new(unsafe { retain_send(&*inp) });
    let output = Arc::new(unsafe { retain_send(&*out) });
    let (inbound_tx, mut inbound_rx) = tokio_mpsc::unbounded_channel();
    let reactor = reactor_tx().clone();
    let channel_id = next_channel_id();

    reactor
        .send(ReactorCmd::Register(RegisterChannel {
            id: channel_id,
            channel_ref: Arc::clone(&channel),
            input,
            output,
            inbound_tx,
        }))
        .expect("apple L2CAP reactor available");

    let (app_side, io_side) = tokio::io::duplex(L2CAP_DUPLEX_BUF_SIZE);
    let (mut io_reader, mut io_writer) = tokio::io::split(io_side);

    runtime.spawn(async move {
        trace!(id = channel_id, "apple L2CAP inbound async bridge started");
        while let Some(bytes) = inbound_rx.recv().await {
            if io_writer.write_all(&bytes).await.is_err() {
                trace!(
                    id = channel_id,
                    "apple L2CAP inbound async bridge stopping: app-side writer closed"
                );
                break;
            }
        }
        trace!(id = channel_id, "apple L2CAP inbound async bridge exited");
    });

    let reactor_for_io = reactor.clone();
    runtime.spawn(async move {
        trace!(id = channel_id, "apple L2CAP outbound async bridge started");
        let mut buf = vec![0_u8; L2CAP_READ_BUF_SIZE];
        loop {
            let n = match io_reader.read(&mut buf).await {
                Ok(0) => {
                    trace!(
                        id = channel_id,
                        "apple L2CAP outbound async bridge stopping: app-side reader EOF"
                    );
                    break;
                }
                Err(e) => {
                    debug!(
                        id = channel_id,
                        "apple L2CAP outbound async bridge stopping: app-side read failed: {e}"
                    );
                    break;
                }
                Ok(n) => n,
            };
            if reactor_for_io
                .send(ReactorCmd::Write {
                    id: channel_id,
                    data: buf[..n].to_vec(),
                })
                .is_err()
            {
                trace!(
                    id = channel_id,
                    "apple L2CAP outbound async bridge stopping: reactor unavailable"
                );
                break;
            }
        }
        trace!(id = channel_id, "apple L2CAP outbound async bridge exited");
    });

    L2capChannel::from_duplex_with_close_hook(app_side, move || {
        let _ = reactor.send(ReactorCmd::Close { id: channel_id });
    })
}
