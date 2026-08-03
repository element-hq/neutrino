// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Runtime-toggleable pcap capture of the federation conversation, taken on the
//! INSIDE of the medium codec.
//!
//! [`PcapCaptureLink`] attaches a [`CaptureControl`] to a link; the transport
//! stays inert until that control is armed via [`CaptureControl::start`] (and
//! disarmed via [`CaptureControl::stop`]). The host drives those from a Settings
//! toggle over FFI, so a capture spans exactly the window the developer wants.
//!
//! ## What this capture is, and what it deliberately is not
//!
//! The tap sits at the four codec boundaries — before `encode_request` and after
//! `decode_response` on egress, after `decode_request` and before
//! `encode_response` on ingress — so it records the request and response as the
//! Matrix layer sees them: real paths, the full `authorization` header,
//! uncompressed CBOR. Each is written as ONE unfragmented CoAP message
//! synthesized from the `WireRequest`/`WireResponse`, not as the datagrams that
//! actually crossed the link.
//!
//! That means this file shows you *what* was federated and answers "is the DAG
//! right, did the auth arrive, what is in this event". It does NOT show you the
//! wire: no medium compression, no Q-Block segmentation, no retransmissions, no
//! per-block option overhead.
//!
//! Because a synthesized message is never fragmented, Wireshark needs no
//! block-wise reassembly here — it dissects each message, and its CBOR body,
//! directly.
//!
//! Framing choices: link type is `LINKTYPE_RAW` (the frame begins at the IPv4
//! header — no synthetic Ethernet MACs). "Us" is always `10.0.0.1`; each distinct
//! peer node id is minted a stable `10.0.0.N` for the session. Ports are assigned
//! by CoAP *role*, not fixed: the server endpoint of each message uses 5683 and
//! the client uses a synthetic ephemeral port, so a request and its response form
//! one client↔server:5683 conversation rather than collapsing into one direction.
//!
//! The client port is scoped per *token*, not per node, so each exchange is its
//! own conversation and a request always sits next to its own response even when
//! several are in flight to one peer. The token is the exchange's real one, taken
//! from the egress client / the inbound packet rather than invented, so the two
//! halves match. Token-less records fall back to a per-node port. Timestamps are
//! wall-clock ([`SystemTime`]); a merged two-device timeline (`mergecap`) is only
//! as good as the two device clocks.
//!
//! ## Threading
//!
//! The file is written on a dedicated std thread draining an unbounded channel,
//! not on the Tokio runtime — so the hot path ([`CaptureControl::record`], called
//! from the async request/response paths) only does a non-blocking channel push, and
//! [`CaptureControl::start`]/[`stop`] are plain sync calls safe to invoke from the
//! FFI/JNI thread. `stop` joins the writer, guaranteeing the file is flushed and
//! closed before it returns — i.e. immediately ready for `adb pull`.
//!
//! [`stop`]: CaptureControl::stop

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use coap_lite::{CoapRequest, CoapResponse};

use super::datagram::{DatagramLink, LinkAddr, LinkProfile};
use super::message;
use crate::transport::{WireRequest, WireResponse};

/// pcap classic magic (microsecond, little-endian on-disk).
const PCAP_MAGIC: u32 = 0xa1b2_c3d4;
/// `LINKTYPE_RAW`: each frame starts at the IP header, no link-layer wrapper.
const LINKTYPE_RAW: u32 = 101;
/// Well-known CoAP server port. A datagram's *server* endpoint uses this; the
/// *client* endpoint uses a synthetic ephemeral port (see [`Session::ips`]). This
/// asymmetry is what lets Wireshark tell request from response and pair the two
/// into one conversation, which its block-wise reassembly relies on — using 5683
/// for both ports collapses direction and defeats reassembly of fragmented CBOR.
const SERVER_PORT: u16 = 5683;
/// The synthetic *fallback* client port for the local node, used only for
/// token-less datagrams; tokened exchanges get a per-(client, token) port (see
/// [`NodeRegistry::port_for`]).
const US_CLIENT_PORT: u16 = 49152;
/// The synthetic address of the local node in the capture.
const US_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
/// IPv4 header (no options) + UDP header, in bytes.
const IP_UDP_OVERHEAD: usize = 20 + 8;

/// Shared, runtime-toggleable capture state. One instance is created by the host
/// edge, cloned into the [`PcapCaptureLink`] on the transport hot path and kept
/// on the FFI handle for the Settings toggle. Off (no file, no allocation on the
/// hot path) until [`start`] arms it.
///
/// [`start`]: CaptureControl::start
#[derive(Default)]
pub struct CaptureControl {
    /// `None` = not capturing. The `Mutex` is only ever held for one map op / one
    /// non-blocking channel push, never across an await.
    active: Mutex<Option<Session>>,
}

/// The state of one armed capture session.
struct Session {
    /// Serialized pcap records, drained by the writer thread. Unbounded so the
    /// transport is never back-pressured; a dead writer just drops frames.
    frames: mpsc::Sender<Vec<u8>>,
    /// Per-session node → synthetic address map, minted fresh each `start` so
    /// every capture's addresses begin at `10.0.0.2`.
    ips: NodeRegistry,
    /// The writer thread, joined on `stop` for a deterministic final flush.
    writer: Option<JoinHandle<()>>,
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
            ips: NodeRegistry::default(),
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

    /// Mirror one federation request into the active session, as the single
    /// unfragmented CoAP message it would be without a medium codec. Cheap when
    /// disarmed: the `is_active` check short-circuits before any serialization.
    pub(super) fn record_request(
        &self,
        local_is_src: bool,
        node: &[u8],
        token: &[u8],
        req: &WireRequest,
    ) {
        if !self.is_active() {
            return;
        }
        let mut out = message::build_request(req);
        out.message.set_token(token.to_vec());
        if let Ok(bytes) = out.message.to_bytes() {
            self.record(local_is_src, node, &bytes);
        }
    }

    /// Mirror one federation response, as above. `seed` is any request packet
    /// (coap-lite derives a response's type and message id from one); the token
    /// is then overwritten with the exchange's, so the response lands in the same
    /// Wireshark conversation as its request.
    pub(super) fn record_response(
        &self,
        local_is_src: bool,
        node: &[u8],
        token: &[u8],
        resp: &WireResponse,
    ) {
        if !self.is_active() {
            return;
        }
        let seed: CoapRequest<SocketAddr> = CoapRequest::new();
        let Some(mut out) = CoapResponse::new(&seed.message) else {
            return;
        };
        message::write_response(&mut out, resp);
        out.message.set_token(token.to_vec());
        if let Ok(bytes) = out.message.to_bytes() {
            self.record(local_is_src, node, &bytes);
        }
    }

    /// Mirror one CoAP message into the active session, if any. A no-op (single
    /// lock, no allocation) when disarmed. Skips (debug-logged) anything too
    /// large for a single IPv4/UDP frame.
    fn record(&self, local_is_src: bool, node: &[u8], payload: &[u8]) {
        let mut guard = self.active.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(session) = guard.as_mut() else {
            return;
        };
        if u16::try_from(IP_UDP_OVERHEAD + payload.len()).is_err() {
            tracing::debug!(
                len = payload.len(),
                "pcap capture: datagram too large, skipping"
            );
            return;
        }
        let (peer_ip, peer_fallback_port) = session.ips.addr_for(node);
        // A request's sender is the client; a response's sender is the server. When
        // the code doesn't classify (empty/signalling), treat the sender as client —
        // such datagrams carry no CBOR, so reassembly is unaffected either way.
        let sender_is_client = coap_is_request(payload).unwrap_or(true);
        // The client endpoint's port is scoped to the exchange token (every
        // message of an exchange echoes it), so each exchange is its own
        // client↔server:5683 conversation in Wireshark — no cross-transfer
        // block-reassembly contamination. Token-less → the per-node fallback.
        let client_is_local = sender_is_client == local_is_src;
        let (client_key, fallback_port) = if client_is_local {
            (None, US_CLIENT_PORT)
        } else {
            (Some(node), peer_fallback_port)
        };
        let client_port = match coap_token(payload) {
            Some(token) => session.ips.port_for(client_key, token),
            None => fallback_port,
        };
        let (src_ip, dst_ip) = if local_is_src {
            (US_IP, peer_ip)
        } else {
            (peer_ip, US_IP)
        };
        let (src_port, dst_port) = if sender_is_client {
            (client_port, SERVER_PORT)
        } else {
            (SERVER_PORT, client_port)
        };
        let (sec, usec) = now_parts();
        let frame = ipv4_udp_frame(src_ip, src_port, dst_ip, dst_port, payload);
        let _ = session.frames.send(pcap_record(sec, usec, &frame));
    }
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

/// A [`DatagramLink`] decorator that attaches a [`CaptureControl`] to a link.
///
/// It passes datagrams straight through — the recording happens above, at the
/// codec boundaries (see the module docs). This exists so the host edge can hand
/// lb a capture sink the same way a medium hands it a codec: by declaring it on
/// the link it injects, with no transport constructor plumbing.
pub struct PcapCaptureLink {
    inner: Arc<dyn DatagramLink>,
    control: Arc<CaptureControl>,
}

impl PcapCaptureLink {
    /// Always-installed wrapper: the tap is present but inert until `control` is
    /// armed, so capture can be toggled at runtime with no link rebuild.
    pub fn wrap(
        inner: Arc<dyn DatagramLink>,
        control: Arc<CaptureControl>,
    ) -> Arc<dyn DatagramLink> {
        Arc::new(Self { inner, control })
    }
}

#[async_trait]
impl DatagramLink for PcapCaptureLink {
    async fn send(&self, dst: &[u8], datagram: &[u8]) -> io::Result<()> {
        self.inner.send(dst, datagram).await
    }

    async fn recv(&self) -> Option<(LinkAddr, Vec<u8>)> {
        self.inner.recv().await
    }

    fn profile(&self) -> LinkProfile {
        // A tap changes no link fact; answering with the trait default here
        // would mask the wrapped medium's declared MTU on every embedded
        // build (`start_with` wraps every injected medium in this tap).
        self.inner.profile()
    }

    fn codec(&self) -> Option<Arc<dyn super::datagram::LinkCodec>> {
        // Same delegation rule as `profile`: answering `None` here would strip
        // the wrapped medium's codec on every embedded build.
        self.inner.codec()
    }

    fn capture(&self) -> Option<Arc<CaptureControl>> {
        // The whole purpose of this decorator: surface the host's control to the
        // CoAP layer, which does the recording at the codec boundaries.
        Some(self.control.clone())
    }
}

/// Synthetic endpoint registry: link address → IP (plus a token-less fallback
/// port), and (client, token) → per-exchange client port. Peers are minted from
/// `10.0.0.2` / fallback port `49153` upward (`10.0.0.1` / `49152` is the local
/// node), stable per node for the life of a session. Per-token ports are minted
/// *downward* from `65535` so the two spaces cannot collide in any realistic
/// debug capture.
#[derive(Default)]
struct NodeRegistry {
    next: u32,
    map: HashMap<LinkAddr, (Ipv4Addr, u16)>,
    /// Count of per-token ports minted; port = `65535 - token_minted`.
    token_minted: u16,
    /// (client node — `None` is the local node — and exchange token) → port.
    token_ports: HashMap<(Option<LinkAddr>, Vec<u8>), u16>,
}

impl NodeRegistry {
    fn addr_for(&mut self, node: &[u8]) -> (Ipv4Addr, u16) {
        if let Some(addr) = self.map.get(node) {
            return *addr;
        }
        // First peer is 10.0.0.2 / 49153, rolling upward; fine for a debug capture.
        let ip = Ipv4Addr::from(u32::from(US_IP).wrapping_add(1) + self.next);
        let client_port = US_CLIENT_PORT + 1 + self.next as u16;
        self.next += 1;
        let addr = (ip, client_port);
        self.map.insert(node.to_vec(), addr);
        addr
    }

    /// The stable synthetic client port for one exchange: keyed by the client
    /// endpoint (so two clients reusing the same token bytes stay distinct) and
    /// the CoAP token. Wraps after 16k exchanges — acceptable for a debug
    /// capture window.
    fn port_for(&mut self, client: Option<&[u8]>, token: &[u8]) -> u16 {
        let key = (client.map(|c| c.to_vec()), token.to_vec());
        if let Some(&port) = self.token_ports.get(&key) {
            return port;
        }
        let port = u16::MAX - self.token_minted;
        self.token_minted = self.token_minted.wrapping_add(1);
        self.token_ports.insert(key, port);
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

/// Classify a CoAP datagram by its code byte: `Some(true)` = a request method
/// (code class 0, detail 1–31), `Some(false)` = a response (class 2/4/5), `None`
/// for empty/signalling/non-CoAP where the role can't be told. Used only to pick
/// synthetic ports; it never inspects the payload.
fn coap_is_request(datagram: &[u8]) -> Option<bool> {
    // byte0: version(2)|type(2)|token-length(4); byte1: code = class(3)|detail(5).
    if datagram.first()? >> 6 != 1 {
        return None; // not CoAP version 1
    }
    let code = *datagram.get(1)?;
    match (code >> 5, code & 0x1f) {
        (0, detail) if detail != 0 => Some(true),
        (2 | 4 | 5, _) => Some(false),
        _ => None,
    }
}

/// The CoAP token of a datagram, or `None` for token-less (TKL 0), reserved TKL
/// (> 8), truncated, or non-CoAP data. Used only to scope synthetic ports.
fn coap_token(datagram: &[u8]) -> Option<&[u8]> {
    let b0 = *datagram.first()?;
    if b0 >> 6 != 1 {
        return None; // not CoAP version 1
    }
    let tkl = (b0 & 0x0f) as usize;
    if tkl == 0 || tkl > 8 {
        return None;
    }
    datagram.get(4..4 + tkl)
}

/// Wrap `payload` in a synthetic IPv4 + UDP frame with the given ports. Caller
/// guarantees `IP_UDP_OVERHEAD + payload.len() <= u16::MAX`.
fn ipv4_udp_frame(
    src: Ipv4Addr,
    src_port: u16,
    dst: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total = (IP_UDP_OVERHEAD + payload.len()) as u16;
    let mut buf = Vec::with_capacity(total as usize);
    // IPv4 header.
    buf.push(0x45); // version 4, IHL 5 (no options)
    buf.push(0x00); // DSCP/ECN
    buf.extend_from_slice(&total.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // identification
    buf.extend_from_slice(&0u16.to_be_bytes()); // flags + fragment offset
    buf.push(64); // TTL
    buf.push(17); // protocol = UDP
    buf.extend_from_slice(&0u16.to_be_bytes()); // header checksum (filled below)
    buf.extend_from_slice(&src.octets());
    buf.extend_from_slice(&dst.octets());
    let checksum = ipv4_checksum(&buf);
    buf[10..12].copy_from_slice(&checksum.to_be_bytes());
    // UDP header. Checksum 0 = "not computed", legal for IPv4/UDP.
    let udp_len = (8 + payload.len()) as u16;
    buf.extend_from_slice(&src_port.to_be_bytes());
    buf.extend_from_slice(&dst_port.to_be_bytes());
    buf.extend_from_slice(&udp_len.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // checksum
    buf.extend_from_slice(payload);
    buf
}

/// Ones-complement checksum over the (even-length) IPv4 header.
fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for word in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([word[0], word[1]]));
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
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Minimal in-memory link: records sends, replays a preloaded inbox on recv.
    struct MockLink {
        sent: Mutex<Vec<(LinkAddr, Vec<u8>)>>,
        inbox: Mutex<VecDeque<(LinkAddr, Vec<u8>)>>,
    }

    impl MockLink {
        fn new(inbox: Vec<(LinkAddr, Vec<u8>)>) -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                inbox: Mutex::new(inbox.into()),
            })
        }
    }

    #[async_trait]
    impl DatagramLink for MockLink {
        async fn send(&self, dst: &[u8], datagram: &[u8]) -> io::Result<()> {
            self.sent
                .lock()
                .unwrap()
                .push((dst.to_vec(), datagram.to_vec()));
            Ok(())
        }
        async fn recv(&self) -> Option<(LinkAddr, Vec<u8>)> {
            self.inbox.lock().unwrap().pop_front()
        }
    }

    /// A link that declares non-default facts, to prove the tap surfaces them.
    struct ProfiledLink;

    #[async_trait]
    impl DatagramLink for ProfiledLink {
        async fn send(&self, _dst: &[u8], _datagram: &[u8]) -> io::Result<()> {
            Ok(())
        }
        async fn recv(&self) -> Option<(LinkAddr, Vec<u8>)> {
            None
        }
        fn profile(&self) -> LinkProfile {
            LinkProfile {
                max_datagram: 640,
                authenticates_connections: false,
                ..LinkProfile::default()
            }
        }
    }

    // The tap wraps every injected medium (`start_with`), so answering with the
    // trait default instead of delegating would mask the medium's declared
    // MTU on every embedded build.
    #[test]
    fn tap_delegates_link_profile() {
        let wrapped =
            PcapCaptureLink::wrap(Arc::new(ProfiledLink), Arc::new(CaptureControl::default()));
        assert_eq!(wrapped.profile(), ProfiledLink.profile());
    }

    /// A unique temp path per test, no rng/clock (both are unavailable/forbidden).
    fn temp_pcap() -> std::path::PathBuf {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("neutrino-pcap-{}-{n}.pcap", std::process::id()))
    }

    /// Split a pcap file into its captured IPv4/UDP frames (skips the 24-byte
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

    fn udp_payload(frame: &[u8]) -> &[u8] {
        &frame[IP_UDP_OVERHEAD..]
    }

    fn udp_ports(frame: &[u8]) -> (u16, u16) {
        (
            u16::from_be_bytes([frame[20], frame[21]]),
            u16::from_be_bytes([frame[22], frame[23]]),
        )
    }

    #[test]
    fn frame_has_valid_ip_udp_headers() {
        let payload = b"\x40\x01\x30\x39hello";
        let frame = ipv4_udp_frame(
            Ipv4Addr::new(10, 0, 0, 1),
            49152,
            Ipv4Addr::new(10, 0, 0, 2),
            SERVER_PORT,
            payload,
        );
        assert_eq!(frame[0], 0x45);
        assert_eq!(frame[9], 17); // UDP
        assert_eq!(
            u16::from_be_bytes([frame[2], frame[3]]) as usize,
            IP_UDP_OVERHEAD + payload.len()
        );
        assert_eq!(
            ipv4_checksum(&frame[..20]),
            0,
            "checksum must verify to zero"
        );
        assert_eq!(&frame[12..16], &[10, 0, 0, 1]);
        assert_eq!(&frame[16..20], &[10, 0, 0, 2]);
        assert_eq!(udp_ports(&frame), (49152, SERVER_PORT));
        assert_eq!(udp_payload(&frame), payload);
    }

    #[test]
    fn coap_token_extraction() {
        assert_eq!(
            coap_token(b"\x44\x01\x00\x01\x0A\x0B\x0C\x0D"),
            Some(&[0x0A, 0x0B, 0x0C, 0x0D][..])
        );
        assert_eq!(coap_token(b"\x40\x01\x00\x01"), None); // TKL 0
        assert_eq!(coap_token(b"\x49\x01\x00\x01ttttttttt"), None); // TKL 9 reserved
        assert_eq!(coap_token(b"\x44\x01\x00\x01\x0A"), None); // truncated token
        assert_eq!(coap_token(b"\x04\x01\x00\x01\x0A\x0B\x0C\x0D"), None); // not v1
    }

    /// Each exchange (token) gets its own client port, shared by its request and
    /// response and distinct per client endpoint; token-less datagrams fall back
    /// to the per-node port. This is what keeps an abandoned or interleaved
    /// Q-Block transfer out of the next transfer's Wireshark reassembly stream.
    #[test]
    fn token_scoped_client_ports_separate_exchanges() {
        let node = b"peer-9".to_vec();
        // Inbound: a response to our token-A request, then a request from the
        // peer with its own token C.
        let resp_a = b"\x64\x45\x00\x01\x0A\x0B\x0C\x0D".to_vec(); // ACK 2.05, tok A
        let req_c = b"\x44\x02\x00\x03\xC0\xC1\xC2\xC3".to_vec(); // CON POST, tok C
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        control.record(true, &node, b"\x44\x01\x00\x01\x0A\x0B\x0C\x0D"); // req, tok A
        control.record(true, &node, b"\x44\x01\x00\x02\x0E\x0F\x10\x11"); // req, tok B
        control.record(false, &node, &resp_a); // response, tok A
        control.record(false, &node, &req_c); // peer request, tok C
        control.record(true, &node, b"\x64\x45\x00\x03\xC0\xC1\xC2\xC3"); // our resp, tok C
        control.record(true, &node, b"\x40\x01\x00\x09"); // token-less req
        control.stop();

        let frames = frames_of(&path);
        // Two exchanges from us: distinct ports minted downward from 65535.
        assert_eq!(udp_ports(&frames[0]), (65535, SERVER_PORT));
        assert_eq!(udp_ports(&frames[1]), (65534, SERVER_PORT));
        // The response reuses token A's port — one conversation with frames[0].
        assert_eq!(udp_ports(&frames[2]), (SERVER_PORT, 65535));
        // The peer's own exchange gets the next port, on the peer (client) side.
        assert_eq!(udp_ports(&frames[3]), (65533, SERVER_PORT));
        assert_eq!(udp_ports(&frames[4]), (SERVER_PORT, 65533));
        // Token-less falls back to the local node's fixed client port.
        assert_eq!(udp_ports(&frames[5]), (US_CLIENT_PORT, SERVER_PORT));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn coap_request_response_classification() {
        assert_eq!(coap_is_request(b"\x40\x01\x00\x00"), Some(true)); // 0.01 GET
        assert_eq!(coap_is_request(b"\x40\x02\x00\x00"), Some(true)); // 0.02 POST
        assert_eq!(coap_is_request(b"\x60\x45\x00\x00"), Some(false)); // 2.05 Content
        assert_eq!(coap_is_request(b"\x60\x84\x00\x00"), Some(false)); // 4.04 Not Found
        assert_eq!(coap_is_request(b"\x40\x00\x00\x00"), None); // 0.00 empty
        assert_eq!(coap_is_request(b"\x00"), None); // not version 1
    }

    // The tap no longer records datagrams (recording moved to the codec
    // boundaries in the CoAP layer) — but it must still pass bytes through
    // untouched and surface the control to that layer, or capture is dead.
    #[tokio::test]
    async fn tap_delegates_bytes_and_surfaces_the_control() {
        let node = b"peer-1".to_vec();
        let inbound = b"\x60\x45\x00\x00in".to_vec();
        let mock = MockLink::new(vec![(node.clone(), inbound.clone())]);
        let control = CaptureControl::new();
        let link = PcapCaptureLink::wrap(mock.clone(), control.clone());
        assert!(!control.is_active());

        link.send(&node, b"out").await.unwrap();
        assert_eq!(
            mock.sent.lock().unwrap().len(),
            1,
            "send must reach the link"
        );
        assert_eq!(mock.sent.lock().unwrap()[0].1, b"out", "bytes unaltered");
        assert_eq!(link.recv().await.unwrap().1, inbound, "recv unaltered");
        assert!(link.capture().is_some(), "the CoAP layer needs this sink");
        assert!(
            !control.is_active(),
            "datagrams must not arm or record anything"
        );
    }

    /// Even armed, the tap records nothing at the datagram level
    #[tokio::test]
    async fn armed_tap_records_no_datagrams() {
        let node = b"peer-1".to_vec();
        let mock = MockLink::new(vec![(node.clone(), b"\x60\x45\x00\x00in".to_vec())]);
        let control = CaptureControl::new();
        let link = PcapCaptureLink::wrap(mock, control.clone());
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        link.send(&node, b"\x40\x01\x00\x01out").await.unwrap();
        link.recv().await.unwrap();
        control.stop();

        assert!(
            frames_of(&path).is_empty(),
            "datagram-level tap must be gone"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn start_capture_stop_writes_both_directions() {
        let node = b"peer-7".to_vec();
        // A real 2.05 Content response inbound; a 0.01 GET request outbound.
        let inbound = b"\x60\x45\x00\x00coap-response".to_vec();
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        assert!(control.is_active());
        let outbound = b"\x40\x01\xAB\xCDcoap-request";
        control.record(true, &node, outbound);
        control.record(false, &node, &inbound);
        assert!(control.stop(), "stop reports it was capturing");
        assert!(!control.is_active());

        // stop() joined the writer: the file is complete and parseable now.
        let frames = frames_of(&path);
        assert_eq!(frames.len(), 2);
        // tx request: we're the client → us:49152 -> peer:5683.
        assert_eq!(&frames[0][12..16], &[10, 0, 0, 1]);
        assert_eq!(&frames[0][16..20], &[10, 0, 0, 2]);
        assert_eq!(udp_ports(&frames[0]), (US_CLIENT_PORT, SERVER_PORT));
        assert_eq!(udp_payload(&frames[0]), outbound);
        // rx response: peer is the server → peer:5683 -> us:49152. Same
        // conversation (us:49152 ↔ peer:5683), so Wireshark pairs + reassembles.
        assert_eq!(&frames[1][12..16], &[10, 0, 0, 2]);
        assert_eq!(&frames[1][16..20], &[10, 0, 0, 1]);
        assert_eq!(udp_ports(&frames[1]), (SERVER_PORT, US_CLIENT_PORT));
        assert_eq!(udp_payload(&frames[1]), inbound.as_slice());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn ports_reflect_server_role_for_inbound_request() {
        let node = b"peer-3".to_vec();
        let request = b"\x40\x02\x00\x00hello".to_vec(); // 0.02 POST from the peer
        let control = CaptureControl::new();
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        control.record(false, &node, &request); // inbound request: peer is client
        control.record(true, &node, b"\x60\x45\x00\x00world"); // our 2.05 response
        control.stop();

        let frames = frames_of(&path);
        // rx request: peer client (49153) -> us server (5683).
        assert_eq!(&frames[0][12..16], &[10, 0, 0, 2]);
        assert_eq!(&frames[0][16..20], &[10, 0, 0, 1]);
        assert_eq!(udp_ports(&frames[0]), (US_CLIENT_PORT + 1, SERVER_PORT));
        // tx response: us server (5683) -> same peer client port (49153), so the
        // pair is one conversation peer:49153 ↔ us:5683.
        assert_eq!(&frames[1][12..16], &[10, 0, 0, 1]);
        assert_eq!(&frames[1][16..20], &[10, 0, 0, 2]);
        assert_eq!(udp_ports(&frames[1]), (SERVER_PORT, US_CLIENT_PORT + 1));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn restart_rotates_file_and_resets_ips() {
        let control = CaptureControl::new();

        // First session: peer `b` is the second node seen, so it gets 10.0.0.3.
        let first = temp_pcap();
        control.start(first.to_str().unwrap()).unwrap();
        control.record(true, b"peer-1", b"a");
        control.record(true, b"peer-2", b"b");
        control.stop();

        // Second session must start a fresh file AND a fresh IP registry, so the
        // first node seen is 10.0.0.2 again (no bleed from the prior session).
        let second = temp_pcap();
        control.start(second.to_str().unwrap()).unwrap();
        control.record(true, b"peer-2", b"b");
        control.stop();

        let f1 = frames_of(&first);
        assert_eq!(&f1[0][16..20], &[10, 0, 0, 2]);
        assert_eq!(&f1[1][16..20], &[10, 0, 0, 3]);
        let f2 = frames_of(&second);
        assert_eq!(f2.len(), 1);
        assert_eq!(&f2[0][16..20], &[10, 0, 0, 2], "IPs reset per session");
        std::fs::remove_file(&first).ok();
        std::fs::remove_file(&second).ok();
    }

    #[test]
    fn stop_without_start_is_false() {
        let control = CaptureControl::new();
        assert!(!control.stop());
    }
}
