/*
 * Copyright (c) 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.
 * Please see LICENSE files in the repository root for full details.
 */

//! TUN packet capture for the Android VpnService tunnel.
//!
//! The host (EX Android) establishes a TUN interface via `VpnService`, detaches
//! the resulting file descriptor and hands ownership to us over FFI
//! ([`Tunnel::start`]). We read IP packets off the fd and log a metadata-only
//! summary of each through the Neutrino tracing stack — moving the previous
//! Kotlin-side `IpPacket.describe` logging down into Rust. Nothing is forwarded
//! yet; this mirrors the capture-and-log spike that was on the Kotlin side.
//!
//! ## Lifecycle: tun depends on the homeserver (one-way)
//!
//! The homeserver may run without a tunnel, but a tunnel must not run without the
//! homeserver. We get that direction for free by running the reader as a **task on
//! the server's runtime**: when the homeserver shuts down its runtime is dropped,
//! which cancels the reader task and drops its [`AsyncFd`], closing the fd. There is
//! nothing extra to enforce.
//!
//! Within that, the tunnel is still independently toggleable: [`Tunnel::start`] and
//! [`Tunnel::stop`] can be called in pairs any number of times on a live handle as
//! the VPN is toggled (each toggle a fresh fd). Stop aborts the task; abort drops
//! the `AsyncFd` it owns, so the fd is closed by the task itself — we never close an
//! fd out from under an in-flight read, so there is no fd-reuse race.
//!
//! ## Non-blocking contract
//!
//! [`AsyncFd`] requires the fd to be non-blocking: after the reactor reports
//! readability we issue one `read`, and on a current-thread runtime a blocking read
//! here would stall the whole server executor. The host MUST set `O_NONBLOCK` on the
//! fd before handing it over (Android: `Os.fcntlInt(fd, F_SETFL, O_NONBLOCK)` before
//! `detachFd()`). A blocking read that returns `EAGAIN` is treated as a spurious
//! wakeup and retried.

use std::fs::File;
use std::io::Read;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

// Generous upper bound for a single read; TUN packets are MTU-bounded in practice,
// but the kernel may hand up larger frames (e.g. GRO), so size the buffer well
// above the MTU to avoid truncating a read. Mirrors the previous Kotlin value.
const READ_BUFFER_SIZE: usize = 32_767;

/// Owns the running tunnel reader task, if any. Held by `NeutrinoHandle`; cheap to
/// construct (no task, no fd) so it can sit on an idle handle.
pub(crate) struct Tunnel {
    /// Handle to the server's runtime, published by the runtime thread once built.
    /// The reader is spawned onto this runtime so it is cancelled when the
    /// homeserver shuts down — this is what enforces "no tun without a homeserver".
    runtime: Arc<OnceLock<Handle>>,
    /// The running reader task, if any. Aborting it drops its [`AsyncFd`], closing
    /// the fd.
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tunnel {
    pub(crate) fn new(runtime: Arc<OnceLock<Handle>>) -> Self {
        Tunnel {
            runtime,
            task: Mutex::new(None),
        }
    }

    /// Take ownership of `tun_fd` and spawn a reader task on the server runtime.
    ///
    /// `tun_fd` must be a TUN fd whose ownership has been transferred to native code
    /// (Kotlin `ParcelFileDescriptor.detachFd()`) and which has been set non-blocking
    /// (see the module-level non-blocking contract); we close it on stop/teardown.
    pub(crate) fn start(&self, tun_fd: i32, mtu: u32) {
        // Take ownership of the fd up front so every early return below closes it
        // rather than leaking it.
        let tun = unsafe { OwnedFd::from_raw_fd(tun_fd) };

        // No runtime published yet means the homeserver isn't up. Tun requires a
        // homeserver, so refuse — and close the fd by dropping `tun`.
        let Some(runtime) = self.runtime.get() else {
            tracing::error!(
                target: "neutrino::tunnel",
                "start_tunnel before the server runtime is up; ignoring (tun requires a running homeserver)",
            );
            return;
        };

        // Recover from a poisoned lock: the protected state is just an Option<task>,
        // so there's nothing to corrupt.
        let mut guard = self.task.lock().unwrap_or_else(|e| e.into_inner());

        // Replace any existing reader (e.g. a re-toggle without an intervening stop):
        // aborting it drops its AsyncFd, closing the old fd. The new fd is distinct
        // (the old one isn't closed until the aborted task drops), so they can't
        // collide.
        if let Some(old) = guard.take() {
            tracing::warn!(target: "neutrino::tunnel", "start_tunnel while already running; replacing existing reader");
            old.abort();
        }

        let task = runtime.spawn(reader(tun, mtu));
        *guard = Some(task);
        tracing::info!(target: "neutrino::tunnel", "tunnel reader started: fd={tun_fd}, mtu={mtu}");
    }

    /// Abort the reader task; its [`AsyncFd`] drops, closing the fd. Idempotent: a
    /// call when no tunnel is running is a no-op.
    pub(crate) fn stop(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            tracing::info!(target: "neutrino::tunnel", "tunnel reader stopped");
        }
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        // If the handle is dropped with a tunnel still up, don't leak the task/fd.
        // (The runtime drop would cancel it anyway, but be explicit.)
        self.stop();
    }
}

/// Read packets from `tun` until EOF, an unrecoverable read error, or the task is
/// aborted (stop / runtime shutdown). Owns the tun fd via the [`AsyncFd`]/[`File`]:
/// the fd is closed when this future is dropped.
async fn reader(tun: OwnedFd, mtu: u32) {
    // Reserved for future write-back / buffer sizing once forwarding lands; the read
    // buffer is intentionally sized for the worst case (GRO) regardless of MTU.
    let _ = mtu;

    // `File` gives us a `Read` impl that maps `EAGAIN` to `ErrorKind::WouldBlock`,
    // which is exactly what `try_io` wants. The fd must already be non-blocking.
    let file = File::from(tun);
    let async_fd = match AsyncFd::new(file) {
        Ok(afd) => afd,
        Err(e) => {
            tracing::error!(target: "neutrino::tunnel", "failed to register tun fd with reactor ({e}); reader exiting");
            return;
        }
    };

    let mut buf = vec![0u8; READ_BUFFER_SIZE];
    loop {
        // Cancellation point: an abort (stop/shutdown) drops this future here, which
        // drops `async_fd` and closes the fd.
        let mut guard = match async_fd.readable().await {
            Ok(guard) => guard,
            Err(e) => {
                tracing::warn!(target: "neutrino::tunnel", "tun readiness failed ({e}); reader exiting");
                return;
            }
        };

        // `try_io` clears the readiness flag and retries on `WouldBlock`, so a
        // spurious wakeup just loops back to `readable().await`.
        match guard.try_io(|inner| {
            let mut file: &File = inner.get_ref();
            file.read(&mut buf)
        }) {
            Ok(Ok(0)) => return, // EOF: fd closed.
            Ok(Ok(n)) => {
                tracing::debug!(target: "neutrino::tunnel", "tunnel tx: {}", describe(&buf[..n]));
            }
            Ok(Err(e)) => {
                tracing::warn!(target: "neutrino::tunnel", "tun read failed ({e}); reader exiting");
                return;
            }
            Err(_would_block) => continue,
        }
    }
}

// ---------------------------------------------------------------------------
// Packet description: a one-line, log-safe summary of metadata only.
//
// Port of the former Kotlin `IpPacket.describe`. Reads IP version, L4 protocol,
// source/destination address and (for TCP/UDP) ports, and length. It never reads
// or logs payload bytes, so no user content is exposed; IP addresses and ports are
// network metadata and safe to log. For IPv6 it only reads ports when the fixed
// header's Next Header is directly TCP/UDP; extension-header packets omit ports.
// ---------------------------------------------------------------------------

const PROTO_ICMP: u8 = 1;
const PROTO_TCP: u8 = 6;
const PROTO_UDP: u8 = 17;
const PROTO_ICMPV6: u8 = 58;

const IPV4_MIN_HEADER: usize = 20;
const IPV6_HEADER: usize = 40;

fn describe(buf: &[u8]) -> String {
    if buf.is_empty() {
        return "empty packet".to_string();
    }
    match buf[0] >> 4 {
        4 => describe_ipv4(buf),
        6 => describe_ipv6(buf),
        version => format!("unknown IP version {version} ({} bytes)", buf.len()),
    }
}

fn describe_ipv4(buf: &[u8]) -> String {
    if buf.len() < IPV4_MIN_HEADER {
        return format!("truncated IPv4 ({} bytes)", buf.len());
    }
    let header_len = ((buf[0] & 0x0F) as usize) * 4;
    let protocol = buf[9];
    let src = std::net::Ipv4Addr::new(buf[12], buf[13], buf[14], buf[15]).to_string();
    let dst = std::net::Ipv4Addr::new(buf[16], buf[17], buf[18], buf[19]).to_string();
    format_summary(protocol, "IPv4", &src, &dst, header_len, buf)
}

fn describe_ipv6(buf: &[u8]) -> String {
    if buf.len() < IPV6_HEADER {
        return format!("truncated IPv6 ({} bytes)", buf.len());
    }
    let next_header = buf[6];
    let mut src_octets = [0u8; 16];
    let mut dst_octets = [0u8; 16];
    src_octets.copy_from_slice(&buf[8..24]);
    dst_octets.copy_from_slice(&buf[24..40]);
    let src = std::net::Ipv6Addr::from(src_octets).to_string();
    let dst = std::net::Ipv6Addr::from(dst_octets).to_string();
    format_summary(next_header, "IPv6", &src, &dst, IPV6_HEADER, buf)
}

fn format_summary(protocol: u8, version: &str, src: &str, dst: &str, l4_offset: usize, buf: &[u8]) -> String {
    let (src_str, dst_str) = match l4_ports(buf, l4_offset, protocol) {
        Some((src_port, dst_port)) => (format!("{src}:{src_port}"), format!("{dst}:{dst_port}")),
        None => (src.to_string(), dst.to_string()),
    };
    format!(
        "{version} {} {src_str} -> {dst_str} ({} bytes)",
        protocol_name(protocol),
        buf.len(),
    )
}

fn l4_ports(buf: &[u8], l4_offset: usize, protocol: u8) -> Option<(u16, u16)> {
    if protocol != PROTO_TCP && protocol != PROTO_UDP {
        return None;
    }
    if buf.len() < l4_offset + 4 {
        return None;
    }
    let src = u16::from_be_bytes([buf[l4_offset], buf[l4_offset + 1]]);
    let dst = u16::from_be_bytes([buf[l4_offset + 2], buf[l4_offset + 3]]);
    Some((src, dst))
}

fn protocol_name(protocol: u8) -> String {
    match protocol {
        PROTO_ICMP => "ICMP".to_string(),
        PROTO_TCP => "TCP".to_string(),
        PROTO_UDP => "UDP".to_string(),
        PROTO_ICMPV6 => "ICMPv6".to_string(),
        other => format!("proto {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describes_empty_and_unknown() {
        assert_eq!(describe(&[]), "empty packet");
        assert_eq!(describe(&[0x00]), "unknown IP version 0 (1 bytes)");
    }

    #[test]
    fn describes_ipv4_tcp_with_ports() {
        // 20-byte IPv4 header, protocol TCP, 192.168.1.5 -> 10.0.0.2, then ports.
        let mut pkt = vec![0u8; 24];
        pkt[0] = 0x45; // version 4, IHL 5 (20 bytes)
        pkt[9] = PROTO_TCP;
        pkt[12..16].copy_from_slice(&[192, 168, 1, 5]);
        pkt[16..20].copy_from_slice(&[10, 0, 0, 2]);
        pkt[20..22].copy_from_slice(&12345u16.to_be_bytes());
        pkt[22..24].copy_from_slice(&443u16.to_be_bytes());
        assert_eq!(describe(&pkt), "IPv4 TCP 192.168.1.5:12345 -> 10.0.0.2:443 (24 bytes)");
    }

    #[test]
    fn describes_ipv4_icmp_without_ports() {
        let mut pkt = vec![0u8; 20];
        pkt[0] = 0x45;
        pkt[9] = PROTO_ICMP;
        pkt[12..16].copy_from_slice(&[10, 0, 0, 2]);
        pkt[16..20].copy_from_slice(&[10, 0, 0, 1]);
        assert_eq!(describe(&pkt), "IPv4 ICMP 10.0.0.2 -> 10.0.0.1 (20 bytes)");
    }

    #[test]
    fn describes_ipv6_udp_with_ports() {
        let mut pkt = vec![0u8; 44];
        pkt[0] = 0x60; // version 6
        pkt[6] = PROTO_UDP; // next header
        pkt[8..24].copy_from_slice(&std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2).octets());
        pkt[24..40].copy_from_slice(&std::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1).octets());
        pkt[40..42].copy_from_slice(&5353u16.to_be_bytes());
        pkt[42..44].copy_from_slice(&53u16.to_be_bytes());
        assert_eq!(describe(&pkt), "IPv6 UDP fd00::2:5353 -> fd00::1:53 (44 bytes)");
    }

    #[test]
    fn reports_truncated_headers() {
        assert_eq!(describe(&[0x45, 0x00]), "truncated IPv4 (2 bytes)");
        assert_eq!(describe(&[0x60, 0x00]), "truncated IPv6 (2 bytes)");
    }
}
