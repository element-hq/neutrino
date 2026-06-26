// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! [`PacketIo`] over a packet-framed file descriptor — the host TUN.
//!
//! On Android the fd is the `VpnService` TUN (raw IP, one packet per read). The
//! privilege is only in *opening* `/dev/net/tun`; the fd itself is ordinary
//! packet-framed I/O, so tests fake it with a `SOCK_DGRAM` socketpair
//! ([`UnixDatagram::pair`]), which preserves message boundaries the same way (1
//! write = 1 read = 1 packet). The fd MUST be non-blocking — [`AsyncFd`]
//! requires it, and the host sets `O_NONBLOCK` before handing it over (see the
//! contract in [`crate::tunnel`]).

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;

use async_trait::async_trait;
use neutrino_relay::{PacketIo, RelayError};
use tokio::io::unix::AsyncFd;

/// Upper bound on a single read. TUN packets are MTU-bounded, but the kernel
/// may hand up larger frames (GRO), so size well above the MTU to never
/// truncate a read. Matches [`crate::tunnel`]'s reader.
const READ_BUFFER_SIZE: usize = 32_767;

/// The host TUN as a relay [`PacketIo`]: `recv` reads one outbound IP packet the
/// host wrote into the tunnel; `send` injects one inbound IP packet back.
pub(crate) struct TunPacketIo {
    fd: AsyncFd<File>,
}

impl TunPacketIo {
    /// Wrap a packet-framed, **non-blocking** fd (a TUN device, or a
    /// `SOCK_DGRAM` socketpair in tests).
    pub(crate) fn new(fd: OwnedFd) -> std::io::Result<Self> {
        Ok(Self {
            fd: AsyncFd::new(File::from(fd))?,
        })
    }
}

#[async_trait]
impl PacketIo for TunPacketIo {
    async fn recv(&self) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; READ_BUFFER_SIZE];
        loop {
            let mut guard = self.fd.readable().await.ok()?;
            // `File`'s `Read` maps `EAGAIN` to `WouldBlock`, which `try_io`
            // treats as a spurious wakeup and retries.
            let read = guard.try_io(|inner| {
                let mut file: &File = inner.get_ref();
                file.read(&mut buf)
            });
            match read {
                Ok(Ok(0)) => return None, // EOF: fd closed.
                Ok(Ok(n)) => {
                    buf.truncate(n);
                    return Some(buf);
                }
                Ok(Err(_)) => return None, // unrecoverable read error.
                Err(_would_block) => continue,
            }
        }
    }

    async fn send(&self, packet: &[u8]) -> Result<(), RelayError> {
        loop {
            let mut guard = self
                .fd
                .writable()
                .await
                .map_err(|e| RelayError::Io(e.to_string()))?;
            let written = guard.try_io(|inner| {
                let mut file: &File = inner.get_ref();
                file.write(packet)
            });
            match written {
                // One write = one datagram = one injected packet.
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => return Err(RelayError::Io(e.to_string())),
                Err(_would_block) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay_stack::RelayStack;
    use iroh::EndpointAddr;
    use neutrino_relay::mem::ipv6_packet;
    use neutrino_relay::vip;
    use std::net::SocketAddr;
    use std::os::unix::net::UnixDatagram;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    /// A fake TUN: a `SOCK_DGRAM` socketpair. The returned [`TunPacketIo`] owns
    /// the "device" end; the test drives the "host" end (async) to inject and
    /// observe IP packets.
    fn fake_tun() -> (tokio::net::UnixDatagram, TunPacketIo) {
        let (host, device) = UnixDatagram::pair().expect("socketpair");
        host.set_nonblocking(true).expect("host non-blocking");
        device.set_nonblocking(true).expect("device non-blocking");
        let host = tokio::net::UnixDatagram::from_std(host).expect("tokio host");
        let device = TunPacketIo::new(device.into()).expect("tun io");
        (host, device)
    }

    // The fd path itself: one datagram in = one packet out of `recv`, and a
    // `send` is one datagram the host reads — boundaries preserved both ways.
    #[tokio::test]
    async fn reads_and_writes_one_packet_per_datagram() {
        let (host, device) = fake_tun();

        host.send(b"packet-one").await.expect("host send");
        assert_eq!(device.recv().await.as_deref(), Some(&b"packet-one"[..]));

        device.send(b"packet-two").await.expect("device send");
        let mut buf = vec![0u8; 64];
        let n = host.recv(&mut buf).await.expect("host recv");
        assert_eq!(&buf[..n], b"packet-two");
    }

    // The full stack with no device and no privileges: an IP packet written to
    // A's fake TUN is carried over real iroh and delivered to B's fake TUN.
    #[tokio::test]
    async fn packet_crosses_fake_tun_relay_iroh_relay_fake_tun() {
        let lo: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let a = RelayStack::build(&[1u8; 32], lo).await.expect("build A");
        let b = RelayStack::build(&[2u8; 32], lo).await.expect("build B");
        let a_key = a.node_key();
        let b_key = b.node_key();

        // A learns how to reach B and B's route (the discovery + invite paths).
        let b_sock = b
            .bound_sockets()
            .into_iter()
            .find(|s| s.ip().is_loopback())
            .expect("loopback socket");
        a.add_peer_addr(EndpointAddr::new(b.endpoint_id()).with_ip_addr(b_sock));
        a.register_peer(&b.endpoint_id().to_string());

        let (a_host, a_dev) = fake_tun();
        let (b_host, b_dev) = fake_tun();
        a.spawn(Arc::new(a_dev));
        b.spawn(Arc::new(b_dev));

        let payload = b"federation-over-fake-tun";
        let pkt = ipv6_packet(vip(&a_key), vip(&b_key), payload);
        a_host.send(&pkt).await.expect("inject at A's TUN");

        let mut buf = vec![0u8; 2048];
        let n = timeout(Duration::from_secs(10), b_host.recv(&mut buf))
            .await
            .expect("B's TUN receives in time")
            .expect("B's TUN recv");
        // The whole IP packet arrived at B's TUN, intact.
        assert_eq!(&buf[..n], pkt.as_slice());
    }
}
