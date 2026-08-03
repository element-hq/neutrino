// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Runtime-toggleable pcap capture of the federation conversation, taken at the
//! HTTP/JSON edges of the proxy.
//!
//! # Why this exists at all
//!
//! Both halves of the proxy already talk real HTTP/JSON over real loopback TCP:
//! `neutrino-http` dials the egress as its forward proxy, and the ingress dials
//! `neutrino-http` as its upstream. On a desktop, `tcpdump -i lo` captures that
//! traffic with perfect fidelity and this module is redundant.
//!
//! It exists for Android, which is where the capture toggle is actually used. A
//! non-rooted app cannot capture loopback: there is no `tcpdump`, `CAP_NET_RAW`
//! is not granted, and `VpnService`'s TUN never sees `127.0.0.0/8`. So on the
//! target platform an in-process writer is the only option, and this module is
//! the stand-in for `tcpdump -i lo`.
//!
//! That framing sets the goal: produce the artifact you would have got from
//! `tcpdump` on the same box. The two are directly comparable on a desktop,
//! which is how this synthesiser is validated (capture both ways, diff).
//!
//! # Where the tap sits
//!
//! At the four points where the real JSON bytes exist — the endpoints of the two
//! loopback legs:
//!
//! | leg | request recorded | response recorded |
//! |-----|------------------|-------------------|
//! | egress (lb is the server) | after the body is read, before `json_to_cbor` | after `cbor_to_json`, before the response is built |
//! | ingress (lb is the client) | after `cbor_to_json`, before the upstream call | after the upstream body is read, before `json_to_cbor` |
//!
//! The ingress points are mirrored relative to the egress ones — lb is a server
//! on one leg and a client on the other. Recording the real bytes rather than
//! re-transcoding CBOR at a lower layer matters: `json_to_cbor` → `cbor_to_json`
//! is not byte-identical (key mapping in [`crate::codec::keys`], key ordering,
//! number forms), so a re-transcode would show a canonicalised view and would
//! hide exactly the codec bugs a JSON capture is for.
//!
//! Because the request is recorded before the call it initiates and the response
//! after it returns, the in-stream delta is meaningful: true wire RTT on the
//! egress leg, upstream service time on the ingress leg.
//!
//! Every response path is recorded, including the error ones (502s, transcode
//! failures, the ingress's non-federation 404). A failed exchange must appear as
//! a request/response pair, not as a dangling request — the failures are the
//! reason to be looking.
//!
//! # What it does not show
//!
//! This is the Matrix conversation, not the medium. There is no CoAP framing, no
//! medium compression (a `LinkCodec`'s deflate is invisible here), no Q-Block
//! segmentation, no retransmissions. Those live below this tap. It answers "what
//! was federated, did the auth arrive, what is in this event" — not "how did the
//! bytes cross the air".
//!
//! # Framing choices
//!
//! `LINKTYPE_RAW`, so each frame starts at the IPv4 header with no synthetic
//! Ethernet MACs. The local node is always `10.0.0.1`; each distinct peer
//! `server_name` is minted a stable `10.0.0.N` for the session (logged at info,
//! so the file is decodable afterwards).
//!
//! Direction alone identifies the leg — an egress exchange runs us → peer, an
//! ingress exchange runs peer → us — so both legs can use port 80 for the server
//! endpoint. That is deliberate: 8448 and 8008 are not in Wireshark's default
//! `http.tcp.port` list and would need "Decode As" on every capture, whereas 80
//! dissects with zero configuration, and the addresses are synthetic anyway. The
//! client endpoint gets a fresh ephemeral port per exchange, so every exchange is
//! its own TCP stream and a request always sits beside its own response.
//!
//! Each exchange is a real TCP conversation: SYN, SYN-ACK, ACK, then the request
//! and response segmented at [`MSS`] with correct sequence and acknowledgement
//! numbers. Bodies therefore have no size ceiling — the previous CoAP-over-UDP
//! capture silently dropped anything over 64 KiB, which meant large `send_join`
//! responses and fat `/send` transactions were missing from exactly the captures
//! taken to investigate them.
//!
//! Timestamps are wall-clock ([`SystemTime`]); a merged two-device timeline
//! (`mergecap`) is only as good as the two device clocks.
//!
//! # Threading
//!
//! The file is written on a dedicated std thread draining an unbounded channel,
//! not on the Tokio runtime — so the hot path only serializes frames and does a
//! non-blocking channel push, and [`CaptureControl::start`]/[`stop`] are plain
//! sync calls safe to invoke from the FFI/JNI thread. `stop` joins the writer,
//! guaranteeing the file is flushed and closed before it returns — i.e.
//! immediately ready for `adb pull`.
//!
//! [`stop`]: CaptureControl::stop

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::headers::is_forwardable;

/// pcap classic magic (microsecond, little-endian on-disk).
const PCAP_MAGIC: u32 = 0xa1b2_c3d4;
/// `LINKTYPE_RAW`: each frame starts at the IP header, no link-layer wrapper.
const LINKTYPE_RAW: u32 = 101;
/// The synthetic HTTP server port for both legs. Port 80 so Wireshark's HTTP
/// dissector engages with no "Decode As" — the leg is told apart by direction,
/// not by port, so nothing is lost by not using the real loopback ports.
const SERVER_PORT: u16 = 80;
/// First synthetic client port; each exchange mints the next one.
const FIRST_CLIENT_PORT: u16 = 49152;
/// The synthetic address of the local node in the capture.
const US_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
/// IPv4 header (no options) + TCP header (no options), in bytes.
const IP_TCP_OVERHEAD: usize = 20 + 20;
/// Bytes of HTTP payload per TCP segment. The classic Ethernet MSS; keeps
/// Wireshark's reassembly display close to what a real capture would show.
const MSS: usize = 1460;

// TCP flag bits.
const FIN: u8 = 0x01;
const SYN: u8 = 0x02;
const PSH: u8 = 0x08;
const ACK: u8 = 0x10;

/// Which loopback leg an exchange belongs to. Decides which endpoint is the
/// client, and therefore the direction the request travels in the capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Leg {
    /// `neutrino-http` → egress → peer. We are the client: us → peer.
    Egress,
    /// peer → ingress → `neutrino-http`. The peer is the client: peer → us.
    Ingress,
}

/// Shared, runtime-toggleable capture state. One instance is created by the host
/// edge, threaded into both proxy halves and kept on the FFI handle for the
/// Settings toggle. Off (no file, no allocation on the hot path) until [`start`]
/// arms it.
///
/// [`start`]: CaptureControl::start
#[derive(Default)]
pub struct CaptureControl {
    /// `None` = not capturing. The `Mutex` is only ever held for a few map ops,
    /// never across an await and never while frames are serialized.
    active: Mutex<Option<Session>>,
}

/// The state of one armed capture session.
struct Session {
    /// Serialized pcap records, drained by the writer thread. Unbounded so the
    /// proxy is never back-pressured; a dead writer just drops frames.
    frames: mpsc::Sender<Vec<u8>>,
    /// Per-session peer → synthetic address map, minted fresh each `start` so
    /// every capture's addresses begin at `10.0.0.2`.
    peers: PeerRegistry,
    /// The writer thread, joined on `stop` for a deterministic final flush.
    writer: Option<JoinHandle<()>>,
}

/// One recorded HTTP exchange in flight: returned by
/// [`CaptureControl::record_request`] and consumed by
/// [`CaptureControl::record_response`], which is what pairs the two halves into
/// a single TCP stream. Absent (`None`) when the capture is disarmed.
pub(crate) struct Exchange {
    client_ip: Ipv4Addr,
    server_ip: Ipv4Addr,
    /// The exchange's synthetic client port — its TCP stream identity.
    client_port: u16,
    /// Request bytes already sent, so the server's sequence/ack numbers line up.
    req_len: u32,
}

impl CaptureControl {
    /// A fresh, disarmed control.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Arm capture, writing the pcap to `path`. Truncates any existing file and
    /// writes the global header before returning, so a bad/unwritable path fails
    /// loudly here rather than silently dropping frames. Re-arming while already
    /// capturing rotates: the prior session is stopped (flushed) first.
    pub fn start(&self, path: &str) -> io::Result<()> {
        let mut file = std::fs::File::create(path)?;
        file.write_all(&global_header())?;
        file.flush()?;
        let (tx, rx) = mpsc::channel();
        let writer = std::thread::Builder::new()
            .name("pcap-capture".to_string())
            .spawn(move || writer_loop(rx, file))?;
        let session = Session {
            frames: tx,
            peers: PeerRegistry::default(),
            writer: Some(writer),
        };
        // Install, capturing any prior session to stop it *after* releasing the
        // lock (its join must not run while we hold the hot-path mutex).
        let previous = self
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(session);
        finish(previous);
        tracing::info!(path, "pcap capture: started");
        Ok(())
    }

    /// Disarm capture and block until the file is flushed + closed. Returns
    /// whether a capture was actually running. Idempotent.
    pub fn stop(&self) -> bool {
        let session = self
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let was_active = session.is_some();
        finish(session);
        if was_active {
            tracing::info!("pcap capture: stopped");
        }
        was_active
    }

    /// Whether a capture is currently armed. Drives the Settings toggle state.
    pub fn is_active(&self) -> bool {
        self.active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Open a TCP stream for one federation exchange and write its request:
    /// handshake, then the request segmented at [`MSS`]. `peer` is the remote
    /// `server_name` — the destination on the egress leg, the claimed origin on
    /// the ingress leg. Returns the handle [`record_response`] needs, or `None`
    /// when disarmed (the caller then records nothing).
    ///
    /// [`record_response`]: CaptureControl::record_response
    pub(crate) fn record_request(
        &self,
        leg: Leg,
        peer: &str,
        method: &str,
        path: &str,
        headers: &[(String, Vec<u8>)],
        body: &[u8],
    ) -> Option<Exchange> {
        // Mint the identity under the lock, then release it: serializing a large
        // body into segments must not hold the hot-path mutex.
        let (frames, peer_ip, client_port) = {
            let mut guard = self.active.lock().unwrap_or_else(PoisonError::into_inner);
            let session = guard.as_mut()?;
            let peer_ip = session.peers.ip_for(peer);
            let client_port = session.peers.next_client_port();
            (session.frames.clone(), peer_ip, client_port)
        };
        let (client_ip, server_ip) = match leg {
            Leg::Egress => (US_IP, peer_ip),
            Leg::Ingress => (peer_ip, US_IP),
        };
        let wire = request_head(method, path, peer, headers, body.len());
        let exchange = Exchange {
            client_ip,
            server_ip,
            client_port,
            req_len: saturating_len(wire.len() + body.len()),
        };
        let mut out = Vec::new();
        // Three-way handshake, so stream reassembly and `tcp.stream` filtering
        // are unambiguous rather than starting mid-conversation.
        out.push(exchange.segment(true, SYN, 0, 0, &[]));
        out.push(exchange.segment(false, SYN | ACK, 0, 1, &[]));
        out.push(exchange.segment(true, ACK, 1, 1, &[]));
        exchange.push_stream(&mut out, true, 1, 1, &wire, body);
        send_all(&frames, out);
        Some(exchange)
    }

    /// Write the response half of `exchange` and close the stream. Consumes the
    /// handle: one request, one response, one TCP stream.
    pub(crate) fn record_response(
        &self,
        exchange: Exchange,
        status: u16,
        headers: &[(String, Vec<u8>)],
        body: &[u8],
    ) {
        let frames = {
            let guard = self.active.lock().unwrap_or_else(PoisonError::into_inner);
            // Stopped mid-exchange: drop the response rather than resurrect the
            // session. The request half is already flushed.
            match guard.as_ref() {
                Some(session) => session.frames.clone(),
                None => return,
            }
        };
        let wire = response_head(status, headers, body.len());
        let ack = exchange.req_len.wrapping_add(1);
        let mut out = Vec::new();
        exchange.push_stream(&mut out, false, 1, ack, &wire, body);
        // FIN from the server, so the stream is visibly complete and Wireshark
        // does not hold it open waiting for more.
        let resp_len = saturating_len(wire.len() + body.len());
        out.push(exchange.segment(false, FIN | ACK, resp_len.wrapping_add(1), ack, &[]));
        send_all(&frames, out);
    }
}

impl Exchange {
    /// One TCP segment on this exchange's stream. `from_client` picks the
    /// direction; `seq`/`ack` are relative to each side's ISN of 0.
    fn segment(&self, from_client: bool, flags: u8, seq: u32, ack: u32, payload: &[u8]) -> Vec<u8> {
        let (src_ip, src_port, dst_ip, dst_port) = if from_client {
            (
                self.client_ip,
                self.client_port,
                self.server_ip,
                SERVER_PORT,
            )
        } else {
            (
                self.server_ip,
                SERVER_PORT,
                self.client_ip,
                self.client_port,
            )
        };
        ipv4_tcp_frame(src_ip, src_port, dst_ip, dst_port, seq, ack, flags, payload)
    }

    /// Segment `head ++ body` at [`MSS`] onto this stream, appending frames to
    /// `out`. The head and body are concatenated first so a small body rides in
    /// the same segment as its headers, exactly as a real socket write would.
    fn push_stream(
        &self,
        out: &mut Vec<Vec<u8>>,
        from_client: bool,
        start_seq: u32,
        ack: u32,
        head: &[u8],
        body: &[u8],
    ) {
        let mut stream = Vec::with_capacity(head.len() + body.len());
        stream.extend_from_slice(head);
        stream.extend_from_slice(body);
        let mut seq = start_seq;
        let mut chunks = stream.chunks(MSS).peekable();
        // An empty stream still needs no segment; the handshake already carried
        // the direction. (`chunks` on an empty slice yields nothing.)
        while let Some(chunk) = chunks.next() {
            // PSH only on the last segment of the message, like a real stack
            // flushing a completed write.
            let flags = if chunks.peek().is_some() {
                ACK
            } else {
                PSH | ACK
            };
            out.push(self.segment(from_client, flags, seq, ack, chunk));
            seq = seq.wrapping_add(saturating_len(chunk.len()));
        }
    }
}

/// Close a capture's request/response pair. A no-op when no capture is armed
/// (or the request half was never recorded, so there is nothing to answer).
///
/// Free rather than a method because both proxy halves hold the sink as an
/// `Option` and would otherwise each grow the same unwrapping wrapper.
pub(crate) fn record_response(
    capture: &Option<Arc<CaptureControl>>,
    exchange: Option<Exchange>,
    status: u16,
    headers: &[(String, Vec<u8>)],
    body: &[u8],
) {
    if let (Some(capture), Some(exchange)) = (capture, exchange) {
        capture.record_response(exchange, status, headers, body);
    }
}

/// Push every serialized frame, timestamped as it is written. A closed channel
/// (writer died / session stopped) drops them silently — a debug tap must never
/// fail the proxy.
fn send_all(frames: &mpsc::Sender<Vec<u8>>, out: Vec<Vec<u8>>) {
    for frame in out {
        let (sec, usec) = now_parts();
        let _ = frames.send(pcap_record(sec, usec, &frame));
    }
}

/// Byte length as a `u32` sequence-space delta, saturating rather than wrapping
/// so an absurd body can never make sequence numbers run backwards.
fn saturating_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// Drop a session's sender and join its writer, so the file is fully flushed and
/// closed by the time this returns.
fn finish(session: Option<Session>) -> Option<()> {
    let mut session = session?;
    drop(session.frames); // signals the writer loop to drain and exit
    if let Some(writer) = session.writer.take() {
        let _ = writer.join();
    }
    Some(())
}

/// Serialize the HTTP request line + headers, ending with the blank line.
///
/// `Host`, `Content-Length` and `Content-Type` are synthesized here rather than
/// forwarded: [`is_forwardable`] is an allowlist that drops framing headers
/// precisely because they would lie after the body is re-serialized, so there is
/// no inherited copy to contradict.
fn request_head(
    method: &str,
    path: &str,
    peer: &str,
    headers: &[(String, Vec<u8>)],
    body_len: usize,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("{method} {path} HTTP/1.1\r\n").as_bytes());
    out.extend_from_slice(format!("Host: {peer}\r\n").as_bytes());
    write_headers(&mut out, headers, body_len);
    out
}

/// Serialize the HTTP status line + headers, ending with the blank line.
fn response_head(status: u16, headers: &[(String, Vec<u8>)], body_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let reason = reason_phrase(status);
    out.extend_from_slice(format!("HTTP/1.1 {status} {reason}\r\n").as_bytes());
    write_headers(&mut out, headers, body_len);
    out
}

/// The forwardable headers plus synthesized framing, then the blank line.
fn write_headers(out: &mut Vec<u8>, headers: &[(String, Vec<u8>)], body_len: usize) {
    for (name, value) in headers {
        // Same filter the real hop applies, so the capture shows what actually
        // crosses. A value containing CR/LF would corrupt the synthesized
        // message (and is not a legal header value), so drop it.
        if !is_forwardable(name) || value.iter().any(|b| *b == b'\r' || *b == b'\n') {
            continue;
        }
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    if body_len > 0 {
        // Drives Wireshark's JSON dissector on the body.
        out.extend_from_slice(b"Content-Type: application/json\r\n");
    }
    out.extend_from_slice(format!("Content-Length: {body_len}\r\n\r\n").as_bytes());
}

/// A reason phrase for the status line. Only the codes this proxy actually
/// emits are named; anything else falls back to its class, which is all
/// Wireshark needs to dissect the response.
fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        s => match s / 100 {
            1 => "Informational",
            2 => "Success",
            3 => "Redirection",
            4 => "Client Error",
            _ => "Server Error",
        },
    }
}

/// Synthetic endpoint registry: peer `server_name` → IP, plus the monotonic
/// per-exchange client port. Peers are minted from `10.0.0.2` upward
/// (`10.0.0.1` is the local node), stable per peer for the life of a session.
#[derive(Default)]
struct PeerRegistry {
    map: HashMap<String, Ipv4Addr>,
    /// Client ports minted so far; the next is `FIRST_CLIENT_PORT + this`. Not
    /// derivable from `map`: ports count exchanges, addresses count peers.
    ports: u16,
}

impl PeerRegistry {
    /// The stable synthetic IP for `peer`, minting one on first sight and
    /// logging the mapping so a captured file can be read back afterwards.
    fn ip_for(&mut self, peer: &str) -> Ipv4Addr {
        if let Some(ip) = self.map.get(peer) {
            return *ip;
        }
        // First peer is 10.0.0.2, rolling upward; fine for a debug capture.
        // The count of peers seen IS `map.len()` — no separate counter.
        let ip = Ipv4Addr::from(u32::from(US_IP).wrapping_add(1) + self.map.len() as u32);
        self.map.insert(peer.to_owned(), ip);
        tracing::info!(%peer, %ip, "pcap capture: peer address assigned");
        ip
    }

    /// A fresh client port, i.e. a fresh TCP stream. Wraps within the ephemeral
    /// range after ~16k exchanges — acceptable for a debug capture window.
    fn next_client_port(&mut self) -> u16 {
        let port = FIRST_CLIENT_PORT.wrapping_add(self.ports);
        self.ports = self.ports.wrapping_add(1) % (u16::MAX - FIRST_CLIENT_PORT);
        port
    }
}

/// Drain serialized records to `file`. Ends when the sender drops (session torn
/// down) or a write fails; flushes once more on the way out.
fn writer_loop(rx: mpsc::Receiver<Vec<u8>>, mut file: std::fs::File) {
    while let Ok(record) = rx.recv() {
        if let Err(e) = file.write_all(&record) {
            tracing::warn!(error = %e, "pcap capture: write failed, stopping");
            break;
        }
    }
    let _ = file.flush();
}

/// The 24-byte pcap global header (little-endian, microsecond, `LINKTYPE_RAW`).
fn global_header() -> [u8; 24] {
    let mut h = [0u8; 24];
    h[0..4].copy_from_slice(&PCAP_MAGIC.to_le_bytes());
    h[4..6].copy_from_slice(&2u16.to_le_bytes()); // version_major
    h[6..8].copy_from_slice(&4u16.to_le_bytes()); // version_minor
    // thiszone (i32) + sigfigs (u32) stay zero.
    h[16..20].copy_from_slice(&u32::from(u16::MAX).to_le_bytes()); // snaplen
    h[20..24].copy_from_slice(&LINKTYPE_RAW.to_le_bytes()); // network
    h
}

/// Prepend the 16-byte pcap record header (little-endian) to a captured frame.
fn pcap_record(sec: u32, usec: u32, frame: &[u8]) -> Vec<u8> {
    let len = frame.len() as u32;
    let mut rec = Vec::with_capacity(16 + frame.len());
    rec.extend_from_slice(&sec.to_le_bytes());
    rec.extend_from_slice(&usec.to_le_bytes());
    rec.extend_from_slice(&len.to_le_bytes()); // incl_len
    rec.extend_from_slice(&len.to_le_bytes()); // orig_len (never truncated)
    rec.extend_from_slice(frame);
    rec
}

/// Wrap `payload` in a synthetic IPv4 + TCP frame. `payload` is at most [`MSS`],
/// so the frame always fits an IPv4 total-length field.
#[allow(clippy::too_many_arguments)]
fn ipv4_tcp_frame(
    src: Ipv4Addr,
    src_port: u16,
    dst: Ipv4Addr,
    dst_port: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let total = (IP_TCP_OVERHEAD + payload.len()) as u16;
    let mut buf = Vec::with_capacity(total as usize);
    // IPv4 header.
    buf.push(0x45); // version 4, IHL 5 (no options)
    buf.push(0x00); // DSCP/ECN
    buf.extend_from_slice(&total.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // identification
    buf.extend_from_slice(&0x4000u16.to_be_bytes()); // don't fragment
    buf.push(64); // TTL
    buf.push(6); // protocol = TCP
    buf.extend_from_slice(&0u16.to_be_bytes()); // header checksum (filled below)
    buf.extend_from_slice(&src.octets());
    buf.extend_from_slice(&dst.octets());
    let checksum = ones_complement(&buf);
    buf[10..12].copy_from_slice(&checksum.to_be_bytes());
    // TCP header.
    let tcp_start = buf.len();
    buf.extend_from_slice(&src_port.to_be_bytes());
    buf.extend_from_slice(&dst_port.to_be_bytes());
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&ack.to_be_bytes());
    buf.push(0x50); // data offset 5 (20 bytes), no options
    buf.push(flags);
    buf.extend_from_slice(&u16::MAX.to_be_bytes()); // window: never the bottleneck
    buf.extend_from_slice(&0u16.to_be_bytes()); // checksum (filled below)
    buf.extend_from_slice(&0u16.to_be_bytes()); // urgent pointer
    buf.extend_from_slice(payload);
    let checksum = tcp_checksum(src, dst, &buf[tcp_start..]);
    buf[tcp_start + 16..tcp_start + 18].copy_from_slice(&checksum.to_be_bytes());
    buf
}

/// TCP checksum over the IPv4 pseudo-header + segment. Computed rather than
/// zeroed so the capture is clean under any validator.
fn tcp_checksum(src: Ipv4Addr, dst: Ipv4Addr, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len());
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.push(0);
    pseudo.push(6); // protocol = TCP
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    ones_complement(&pseudo)
}

/// Ones-complement checksum, padding an odd-length input with a zero byte.
fn ones_complement(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for word in bytes.chunks(2) {
        let hi = u16::from(word[0]) << 8;
        let lo = u16::from(*word.get(1).unwrap_or(&0));
        sum += u32::from(hi | lo);
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// Wall-clock seconds + microseconds since the Unix epoch, for the pcap record
/// header. Clamps a pre-epoch clock to zero rather than failing the capture.
fn now_parts() -> (u32, u32) {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as u32, d.subsec_micros()),
        Err(_) => (0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A unique temp path per test, no rng/clock (both are unavailable/forbidden).
    fn temp_pcap() -> std::path::PathBuf {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("neutrino-pcap-{}-{n}.pcap", std::process::id()))
    }

    /// Split a pcap file into its captured IPv4/TCP frames (skips the 24-byte
    /// global header; walks 16-byte record headers).
    fn frames_of(path: &std::path::Path) -> Vec<Vec<u8>> {
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            PCAP_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            LINKTYPE_RAW
        );
        let mut frames = Vec::new();
        let mut off = 24;
        while off + 16 <= bytes.len() {
            let incl = u32::from_le_bytes(bytes[off + 8..off + 12].try_into().unwrap()) as usize;
            off += 16;
            frames.push(bytes[off..off + incl].to_vec());
            off += incl;
        }
        frames
    }

    fn payload(frame: &[u8]) -> &[u8] {
        &frame[IP_TCP_OVERHEAD..]
    }

    fn ports(frame: &[u8]) -> (u16, u16) {
        (
            u16::from_be_bytes([frame[20], frame[21]]),
            u16::from_be_bytes([frame[22], frame[23]]),
        )
    }

    fn ips(frame: &[u8]) -> (&[u8], &[u8]) {
        (&frame[12..16], &frame[16..20])
    }

    fn flags(frame: &[u8]) -> u8 {
        frame[33]
    }

    fn seq(frame: &[u8]) -> u32 {
        u32::from_be_bytes(frame[24..28].try_into().unwrap())
    }

    /// The concatenated payload of every data-carrying frame, i.e. the
    /// reassembled TCP stream in the direction the first data frame travels.
    fn reassemble(frames: &[Vec<u8>]) -> Vec<u8> {
        frames
            .iter()
            .filter(|f| !payload(f).is_empty())
            .flat_map(|f| payload(f).to_vec())
            .collect()
    }

    fn auth_header() -> Vec<(String, Vec<u8>)> {
        vec![(
            "authorization".to_owned(),
            b"X-Matrix origin=\"a.example\",destination=\"b.example\"".to_vec(),
        )]
    }

    /// The whole point of moving the tap: the capture must hold the caller's
    /// literal JSON bytes. A re-transcode through CBOR would reorder keys and
    /// remap short names, hiding exactly the codec bugs this file is for.
    #[test]
    fn body_is_the_caller_s_literal_json() {
        // Key order here is deliberately not what a CBOR round-trip produces.
        let body = br#"{"z_last":1,"a_first":{"nested":[1,2,3]},"origin_server_ts":0}"#;
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let ex = control
            .record_request(
                Leg::Egress,
                "peer.example",
                "PUT",
                "/_matrix/federation/v1/send/1",
                &auth_header(),
                body,
            )
            .unwrap();
        control.record_response(ex, 200, &[], br#"{"pdus":{}}"#);
        control.stop();

        let frames = frames_of(&path);
        let stream = reassemble(&frames);
        let text = String::from_utf8(stream).unwrap();
        assert!(
            text.contains(std::str::from_utf8(body).unwrap()),
            "the literal request JSON must appear byte-for-byte: {text}"
        );
        assert!(text.contains(r#"{"pdus":{}}"#), "response JSON must appear");
        std::fs::remove_file(&path).ok();
    }

    /// The synthesized HTTP must be well-formed enough for a dissector: request
    /// line, Host, the forwarded credential, accurate Content-Length, and the
    /// blank line before the body.
    #[test]
    fn request_is_well_formed_http() {
        let body = br#"{"a":1}"#;
        let head = request_head(
            "PUT",
            "/_matrix/federation/v1/send/1",
            "peer.example",
            &auth_header(),
            body.len(),
        );
        let text = String::from_utf8(head).unwrap();
        assert!(text.starts_with("PUT /_matrix/federation/v1/send/1 HTTP/1.1\r\n"));
        assert!(text.contains("Host: peer.example\r\n"));
        assert!(text.contains("authorization: X-Matrix origin=\"a.example\""));
        assert!(text.contains("Content-Type: application/json\r\n"));
        assert!(text.contains("Content-Length: 7\r\n"));
        assert!(text.ends_with("\r\n\r\n"), "headers end with a blank line");
    }

    /// An empty body must not claim a JSON content type, but must still carry an
    /// accurate (zero) length — otherwise Wireshark waits for a body forever.
    #[test]
    fn empty_body_has_zero_length_and_no_content_type() {
        let head = response_head(404, &[], 0);
        let text = String::from_utf8(head).unwrap();
        assert!(text.starts_with("HTTP/1.1 404 Not Found\r\n"));
        assert!(!text.contains("Content-Type"));
        assert!(text.contains("Content-Length: 0\r\n"));
    }

    /// Non-forwardable headers are dropped (they would lie after re-serialization),
    /// and a value carrying CR/LF must never be able to inject a header line.
    #[test]
    fn header_filter_matches_the_real_hop_and_rejects_injection() {
        let headers = vec![
            (
                "authorization".to_owned(),
                b"X-Matrix origin=\"a\"".to_vec(),
            ),
            ("content-length".to_owned(), b"99999".to_vec()),
            ("user-agent".to_owned(), b"curl".to_vec()),
            ("x-matrix-evil".to_owned(), b"a\r\nInjected: yes".to_vec()),
        ];
        let head = request_head("GET", "/x", "p", &headers, 4);
        let text = String::from_utf8(head).unwrap();
        assert!(text.contains("authorization: X-Matrix origin=\"a\""));
        assert!(!text.contains("user-agent"), "allowlist drops user-agent");
        assert!(!text.contains("Injected"), "CR/LF value must be dropped");
        assert_eq!(
            text.matches("Content-Length").count(),
            1,
            "only the synthesized Content-Length may appear"
        );
    }

    /// The previous CoAP-over-UDP capture silently skipped anything over 64 KiB,
    /// so large send_join responses were missing from the captures taken to
    /// investigate them. TCP segments instead of dropping.
    #[test]
    fn large_body_is_segmented_not_dropped() {
        let big = vec![b'x'; 200_000];
        let body = [br#"{"big":""#.to_vec(), big, br#""}"#.to_vec()].concat();
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let ex = control
            .record_request(Leg::Egress, "peer.example", "PUT", "/send", &[], &body)
            .unwrap();
        control.record_response(ex, 200, &[], b"{}");
        control.stop();

        let frames = frames_of(&path);
        // Request direction only: client -> server carries the big body.
        let from_client: Vec<Vec<u8>> = frames
            .iter()
            .filter(|f| ips(f).0 == [10, 0, 0, 1])
            .cloned()
            .collect();
        let stream = reassemble(&from_client);
        assert!(
            stream.ends_with(&body),
            "the whole body must survive segmentation"
        );
        assert!(
            frames.len() > 100,
            "a 200 KB body must span many segments, got {}",
            frames.len()
        );
        // Sequence numbers must advance by exactly the payload carried.
        let data: Vec<&Vec<u8>> = from_client
            .iter()
            .filter(|f| !payload(f).is_empty())
            .collect();
        for pair in data.windows(2) {
            assert_eq!(
                seq(pair[1]),
                seq(pair[0]) + payload(pair[0]).len() as u32,
                "seq must advance by the bytes sent"
            );
        }
        std::fs::remove_file(&path).ok();
    }

    /// Direction identifies the leg: egress runs us -> peer, ingress peer -> us,
    /// with port 80 always on the server side. This is what lets both legs share
    /// a port without ambiguity.
    #[test]
    fn leg_decides_direction() {
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let out = control
            .record_request(Leg::Egress, "peer.example", "GET", "/a", &[], b"")
            .unwrap();
        control.record_response(out, 200, &[], b"{}");
        let inb = control
            .record_request(Leg::Ingress, "peer.example", "GET", "/b", &[], b"")
            .unwrap();
        control.record_response(inb, 200, &[], b"{}");
        control.stop();

        let frames = frames_of(&path);
        let syns: Vec<&Vec<u8>> = frames.iter().filter(|f| flags(f) == SYN).collect();
        assert_eq!(syns.len(), 2, "one SYN per exchange");
        // Egress: we are the client, so us -> peer with 80 as the destination.
        assert_eq!(ips(syns[0]), (&[10, 0, 0, 1][..], &[10, 0, 0, 2][..]));
        assert_eq!(ports(syns[0]).1, SERVER_PORT);
        // Ingress: the peer is the client, so peer -> us.
        assert_eq!(ips(syns[1]), (&[10, 0, 0, 2][..], &[10, 0, 0, 1][..]));
        assert_eq!(ports(syns[1]).1, SERVER_PORT);
        std::fs::remove_file(&path).ok();
    }

    /// Each exchange is its own TCP stream, so concurrent exchanges to one peer
    /// never contaminate each other's reassembly — and the same peer keeps one
    /// address across both legs.
    #[test]
    fn each_exchange_is_its_own_stream() {
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let a = control
            .record_request(Leg::Egress, "peer.example", "GET", "/a", &[], b"")
            .unwrap();
        let b = control
            .record_request(Leg::Egress, "peer.example", "GET", "/b", &[], b"")
            .unwrap();
        // Interleaved completion: b answers before a.
        control.record_response(b, 200, &[], b"{}");
        control.record_response(a, 200, &[], b"{}");
        control.stop();

        let frames = frames_of(&path);
        let syns: Vec<&Vec<u8>> = frames.iter().filter(|f| flags(f) == SYN).collect();
        assert_eq!(syns.len(), 2);
        assert_ne!(
            ports(syns[0]).0,
            ports(syns[1]).0,
            "distinct client ports = distinct streams"
        );
        // One peer, one address, regardless of how many exchanges.
        assert_eq!(ips(syns[0]).1, ips(syns[1]).1);
        std::fs::remove_file(&path).ok();
    }

    /// A response must acknowledge exactly the request bytes, or Wireshark shows
    /// the stream as broken.
    #[test]
    fn response_acknowledges_the_request_bytes() {
        let control = CaptureControl::new();
        let path = temp_pcap();
        let body = br#"{"a":1}"#;

        control.start(path.to_str().unwrap()).unwrap();
        let ex = control
            .record_request(Leg::Egress, "p", "PUT", "/x", &[], body)
            .unwrap();
        let expected_ack = ex.req_len + 1;
        control.record_response(ex, 200, &[], b"{}");
        control.stop();

        let frames = frames_of(&path);
        let resp = frames
            .iter()
            .find(|f| ips(f).0 == [10, 0, 0, 2] && !payload(f).is_empty())
            .expect("a response data frame");
        let ack = u32::from_be_bytes(resp[28..32].try_into().unwrap());
        assert_eq!(ack, expected_ack, "response must ack every request byte");
        std::fs::remove_file(&path).ok();
    }

    /// Error responses must be recorded too — a dangling request with no
    /// response is worst exactly when something has gone wrong.
    #[test]
    fn error_responses_are_recorded() {
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let ex = control
            .record_request(Leg::Ingress, "p", "GET", "/nope", &[], b"")
            .unwrap();
        control.record_response(ex, 502, &[], b"");
        control.stop();

        let frames = frames_of(&path);
        let text = String::from_utf8(reassemble(&frames)).unwrap();
        assert!(text.contains("HTTP/1.1 502 Bad Gateway"));
        std::fs::remove_file(&path).ok();
    }

    /// Disarmed is genuinely free: no handle, so the caller records nothing.
    #[test]
    fn disarmed_records_nothing() {
        let control = CaptureControl::new();
        assert!(!control.is_active());
        assert!(
            control
                .record_request(Leg::Egress, "p", "GET", "/x", &[], b"")
                .is_none()
        );
    }

    /// Stopping mid-exchange must not resurrect the session or panic; the
    /// already-flushed request half is all the file gets.
    #[test]
    fn response_after_stop_is_dropped() {
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        let ex = control
            .record_request(Leg::Egress, "p", "GET", "/x", &[], b"")
            .unwrap();
        control.stop();
        control.record_response(ex, 200, &[], b"{}"); // must be a no-op

        let text = String::from_utf8(reassemble(&frames_of(&path))).unwrap();
        assert!(text.contains("GET /x"), "the request half was flushed");
        assert!(!text.contains("HTTP/1.1 200"), "the response was dropped");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn frame_has_valid_ip_and_tcp_checksums() {
        let frame = ipv4_tcp_frame(
            Ipv4Addr::new(10, 0, 0, 1),
            49152,
            Ipv4Addr::new(10, 0, 0, 2),
            SERVER_PORT,
            1,
            1,
            PSH | ACK,
            b"hello",
        );
        assert_eq!(frame[0], 0x45);
        assert_eq!(frame[9], 6, "protocol = TCP");
        assert_eq!(
            u16::from_be_bytes([frame[2], frame[3]]) as usize,
            IP_TCP_OVERHEAD + 5
        );
        assert_eq!(ones_complement(&frame[..20]), 0, "IP checksum must verify");
        assert_eq!(
            tcp_checksum(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(10, 0, 0, 2),
                &frame[20..]
            ),
            0,
            "TCP checksum must verify to zero"
        );
        assert_eq!(payload(&frame), b"hello");
    }

    #[test]
    fn restart_rotates_file_and_resets_addresses() {
        let control = CaptureControl::new();

        let first = temp_pcap();
        control.start(first.to_str().unwrap()).unwrap();
        control.record_request(Leg::Egress, "peer-1", "GET", "/a", &[], b"");
        control.record_request(Leg::Egress, "peer-2", "GET", "/b", &[], b"");
        control.stop();

        // A second session must start a fresh file AND a fresh registry, so the
        // first peer seen is 10.0.0.2 again (no bleed from the prior session).
        let second = temp_pcap();
        control.start(second.to_str().unwrap()).unwrap();
        control.record_request(Leg::Egress, "peer-2", "GET", "/b", &[], b"");
        control.stop();

        let f1 = frames_of(&first);
        let syns1: Vec<&Vec<u8>> = f1.iter().filter(|f| flags(f) == SYN).collect();
        assert_eq!(ips(syns1[0]).1, &[10, 0, 0, 2]);
        assert_eq!(ips(syns1[1]).1, &[10, 0, 0, 3]);
        let f2 = frames_of(&second);
        let syns2: Vec<&Vec<u8>> = f2.iter().filter(|f| flags(f) == SYN).collect();
        assert_eq!(
            ips(syns2[0]).1,
            &[10, 0, 0, 2],
            "addresses reset per session"
        );
        std::fs::remove_file(&first).ok();
        std::fs::remove_file(&second).ok();
    }

    #[test]
    fn stop_without_start_is_false() {
        let control = CaptureControl::new();
        assert!(!control.stop());
    }
}
