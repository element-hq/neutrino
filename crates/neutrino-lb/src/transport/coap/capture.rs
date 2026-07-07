// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Runtime-toggleable pcap capture for the [`DatagramLink`] seam.
//!
//! [`PcapCaptureLink`] always wraps the inner link, but stays inert until a
//! [`CaptureControl`] is armed via [`CaptureControl::start`] (and disarmed via
//! [`CaptureControl::stop`]). The host drives those from a Settings toggle over
//! FFI, so a capture spans exactly the window the developer wants. When armed,
//! every datagram — both directions — is mirrored into a classic-format pcap;
//! the byte stream to the inner link is never altered.
//!
//! ## Why this is enough to read the whole conversation in Wireshark
//!
//! Each Q-Block / RFC 7959 blockwise fragment IS a complete CoAP message. So a
//! datagram written as one UDP/IPv4 frame on the CoAP port (5683) is dissected
//! natively: Wireshark attaches its CoAP dissector, runs its OWN block-wise
//! reassembly across the fragments, and then dissects the reassembled CBOR body.
//! We therefore get both the MTU-chunking view (per-fragment Block options +
//! sizes) and the decoded payload without decoding anything ourselves.
//!
//! Framing choices: link type is `LINKTYPE_RAW` (the frame begins at the IPv4
//! header — no synthetic Ethernet MACs). "Us" is always `10.0.0.1`; each distinct
//! peer node id is minted a stable `10.0.0.N` for the session, so tx/rx render as
//! clean Wireshark conversations with direction. UDP ports are both 5683 so the
//! CoAP dissector attaches regardless of direction; request/response matching is
//! by CoAP token, not port. Timestamps are wall-clock ([`SystemTime`]); a merged
//! two-device timeline (`mergecap`) is only as good as the two device clocks.
//!
//! ## Threading
//!
//! The file is written on a dedicated std thread draining an unbounded channel,
//! not on the Tokio runtime — so the hot path ([`CaptureControl::record`], called
//! from the async `send`/`recv`) only does a non-blocking channel push, and
//! [`CaptureControl::start`]/[`stop`] are plain sync calls safe to invoke from the
//! FFI/JNI thread. `stop` joins the writer, guaranteeing the file is flushed and
//! closed before it returns — i.e. immediately ready for `adb pull`.
//!
//! [`stop`]: CaptureControl::stop

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::datagram::DatagramLink;

/// pcap classic magic (microsecond, little-endian on-disk).
const PCAP_MAGIC: u32 = 0xa1b2_c3d4;
/// `LINKTYPE_RAW`: each frame starts at the IP header, no link-layer wrapper.
const LINKTYPE_RAW: u32 = 101;
/// Well-known CoAP/UDP port; used as both ports so the CoAP dissector attaches.
const COAP_PORT: u16 = 5683;
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
    /// Per-session node → synthetic IP map, minted fresh each `start` so every
    /// capture's addresses begin at `10.0.0.2`.
    ips: IpRegistry,
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
            ips: IpRegistry::default(),
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

    /// Mirror one datagram into the active session, if any. A no-op (single lock,
    /// no allocation) when disarmed. Skips (debug-logged) any datagram too large
    /// for a single IPv4/UDP frame — a non-event for MTU-sized CoAP blocks.
    fn record(&self, local_is_src: bool, node: [u8; 32], payload: &[u8]) {
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
        let peer = session.ips.ip_for(node);
        let (src, dst) = if local_is_src {
            (US_IP, peer)
        } else {
            (peer, US_IP)
        };
        let (sec, usec) = now_parts();
        let frame = ipv4_udp_frame(src, dst, payload);
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

/// A [`DatagramLink`] decorator that mirrors every datagram into a [`CaptureControl`].
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
    async fn send(&self, dst: [u8; 32], datagram: &[u8]) -> io::Result<()> {
        self.control.record(true, dst, datagram);
        self.inner.send(dst, datagram).await
    }

    async fn recv(&self) -> Option<([u8; 32], Vec<u8>)> {
        let (node, data) = self.inner.recv().await?;
        self.control.record(false, node, &data);
        Some((node, data))
    }
}

/// Node id → synthetic peer IP. Peers are minted from `10.0.0.2` upward (`.1` is
/// the local node), stable per node for the life of a session.
#[derive(Default)]
struct IpRegistry {
    next: u32,
    map: HashMap<[u8; 32], Ipv4Addr>,
}

impl IpRegistry {
    fn ip_for(&mut self, node: [u8; 32]) -> Ipv4Addr {
        if let Some(ip) = self.map.get(&node) {
            return *ip;
        }
        // 10.0.0.0 + (2 + next): first peer is 10.0.0.2, rolling into 10.0.1.x etc.
        // past .255 — fine for a debug capture.
        let ip = Ipv4Addr::from(u32::from(US_IP).wrapping_add(1) + self.next);
        self.next += 1;
        self.map.insert(node, ip);
        ip
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

/// Wrap `payload` in a synthetic IPv4 + UDP frame (both ports 5683). Caller
/// guarantees `IP_UDP_OVERHEAD + payload.len() <= u16::MAX`.
fn ipv4_udp_frame(src: Ipv4Addr, dst: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
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
    buf.extend_from_slice(&COAP_PORT.to_be_bytes()); // source port
    buf.extend_from_slice(&COAP_PORT.to_be_bytes()); // destination port
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
        sent: Mutex<Vec<([u8; 32], Vec<u8>)>>,
        inbox: Mutex<VecDeque<([u8; 32], Vec<u8>)>>,
    }

    impl MockLink {
        fn new(inbox: Vec<([u8; 32], Vec<u8>)>) -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                inbox: Mutex::new(inbox.into()),
            })
        }
    }

    #[async_trait]
    impl DatagramLink for MockLink {
        async fn send(&self, dst: [u8; 32], datagram: &[u8]) -> io::Result<()> {
            self.sent.lock().unwrap().push((dst, datagram.to_vec()));
            Ok(())
        }
        async fn recv(&self) -> Option<([u8; 32], Vec<u8>)> {
            self.inbox.lock().unwrap().pop_front()
        }
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

    #[test]
    fn frame_has_valid_ip_udp_headers() {
        let payload = b"\x40\x01\x30\x39hello";
        let frame = ipv4_udp_frame(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
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
        assert_eq!(u16::from_be_bytes([frame[20], frame[21]]), COAP_PORT);
        assert_eq!(u16::from_be_bytes([frame[22], frame[23]]), COAP_PORT);
        assert_eq!(udp_payload(&frame), payload);
    }

    #[tokio::test]
    async fn disarmed_by_default_and_delegates() {
        let mock = MockLink::new(vec![]);
        let control = CaptureControl::new();
        let link = PcapCaptureLink::wrap(mock.clone(), control.clone());
        assert!(!control.is_active());
        // Traffic while disarmed is delegated untouched and captures nothing.
        link.send([1u8; 32], b"hi").await.unwrap();
        assert_eq!(mock.sent.lock().unwrap().len(), 1);
        assert!(!control.is_active());
    }

    #[tokio::test]
    async fn start_capture_stop_writes_both_directions() {
        let node = [7u8; 32];
        let inbound = b"coap-response".to_vec();
        let mock = MockLink::new(vec![(node, inbound.clone())]);
        let control = CaptureControl::new();
        let link = PcapCaptureLink::wrap(mock.clone(), control.clone());
        let path = temp_pcap();

        control.start(path.to_str().unwrap()).unwrap();
        assert!(control.is_active());
        let outbound = b"\x40\x01\xAB\xCDcoap-request";
        link.send(node, outbound).await.unwrap();
        let (_, got) = link.recv().await.unwrap();
        assert_eq!(got, inbound);
        assert!(control.stop(), "stop reports it was capturing");
        assert!(!control.is_active());

        // stop() joined the writer: the file is complete and parseable now.
        let frames = frames_of(&path);
        assert_eq!(frames.len(), 2);
        // tx: us -> peer, payload preserved.
        assert_eq!(&frames[0][12..16], &[10, 0, 0, 1]);
        assert_eq!(&frames[0][16..20], &[10, 0, 0, 2]);
        assert_eq!(udp_payload(&frames[0]), outbound);
        // rx: peer -> us.
        assert_eq!(&frames[1][12..16], &[10, 0, 0, 2]);
        assert_eq!(&frames[1][16..20], &[10, 0, 0, 1]);
        assert_eq!(udp_payload(&frames[1]), inbound.as_slice());
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn restart_rotates_file_and_resets_ips() {
        let mock = MockLink::new(vec![]);
        let control = CaptureControl::new();
        let link = PcapCaptureLink::wrap(mock, control.clone());

        // First session: peer `b` is the second node seen, so it gets 10.0.0.3.
        let first = temp_pcap();
        control.start(first.to_str().unwrap()).unwrap();
        link.send([1u8; 32], b"a").await.unwrap();
        link.send([2u8; 32], b"b").await.unwrap();
        control.stop();

        // Second session must start a fresh file AND a fresh IP registry, so the
        // first node seen is 10.0.0.2 again (no bleed from the prior session).
        let second = temp_pcap();
        control.start(second.to_str().unwrap()).unwrap();
        link.send([2u8; 32], b"b").await.unwrap();
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
