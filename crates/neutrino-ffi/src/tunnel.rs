/*
 * Copyright (c) 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.
 * Please see LICENSE files in the repository root for full details.
 */

//! The host TUN tunnel: feeds the Android `VpnService` fd into the packet relay.
//!
//! The host establishes a TUN interface via `VpnService`, detaches the resulting
//! file descriptor and hands ownership to us over FFI ([`Tunnel::start`]). We
//! build the iroh relay over the server's persisted identity (handed across from
//! the entrypoint) and carry IP packets between that fd and the wire.
//!
//! Lifetime: the relay runs as a task on the **server's** runtime, so when the
//! homeserver stops (the runtime drops) the relay is cancelled — a tunnel cannot
//! outlive its homeserver. `start` replaces any running relay (a VPN re-toggle
//! hands a fresh fd); `stop` aborts the task, which drops the relay's
//! [`TunPacketIo`] and closes the fd. The fd is owned the moment it arrives, so
//! every early return closes it rather than leaking it.
//!
//! Non-blocking contract: [`AsyncFd`](tokio::io::unix::AsyncFd) (in
//! [`crate::tun_io`]) requires the fd to be non-blocking. The host MUST set
//! `O_NONBLOCK` before handing it over (Android: `Os.fcntlInt(fd, F_SETFL,
//! O_NONBLOCK)` before `detachFd()`).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex, OnceLock};

use neutrino_main::TunnelHandoff;
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::relay_stack::RelayStack;
use crate::tun_io::TunPacketIo;

/// UDP bind for the relay's iroh endpoint. Ephemeral port; service discovery
/// (mDNS/BLE) will advertise the bound address to peers (not yet wired).
const RELAY_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// Owns the running tunnel relay task, if any. Held by `NeutrinoHandle`; cheap to
/// construct (no task, no fd) so it can sit on an idle handle.
pub(crate) struct Tunnel {
    /// Server runtime handle, published by the runtime thread once built. The
    /// relay is spawned onto it, so it is cancelled when the homeserver stops.
    runtime: Arc<OnceLock<Handle>>,
    /// The node secret + shared route table, published by the entrypoint once the
    /// server identity is resolved. The relay task awaits it (so a start that
    /// races ahead of identity resolution waits rather than losing the tunnel).
    handoff: watch::Receiver<Option<TunnelHandoff>>,
    /// The running relay task, if any. Aborting it stops the relay and closes the
    /// fd (its `TunPacketIo` drops).
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tunnel {
    pub(crate) fn new(
        runtime: Arc<OnceLock<Handle>>,
        handoff: watch::Receiver<Option<TunnelHandoff>>,
    ) -> Self {
        Tunnel {
            runtime,
            handoff,
            task: Mutex::new(None),
        }
    }

    /// Take ownership of `tun_fd` and start the relay over it.
    ///
    /// `tun_fd` must be a non-blocking TUN fd whose ownership has been
    /// transferred to native code (Kotlin `ParcelFileDescriptor.detachFd()`); we
    /// close it on stop/teardown.
    pub(crate) fn start(&self, tun_fd: i32, mtu: u32) {
        // Own the fd up front so every early return below closes it.
        let tun = unsafe { OwnedFd::from_raw_fd(tun_fd) };

        let Some(runtime) = self.runtime.get() else {
            tracing::error!(
                target: "neutrino::tunnel",
                "start_tunnel before the server runtime is up; ignoring (tun requires a running homeserver)",
            );
            return;
        };

        // Recover from a poisoned lock: the state is just an Option<task>.
        let mut guard = self.task.lock().unwrap_or_else(|e| e.into_inner());
        // Replace any existing relay (e.g. a re-toggle without an intervening
        // stop): aborting it drops its `TunPacketIo`, closing the old fd.
        if let Some(old) = guard.take() {
            tracing::warn!(target: "neutrino::tunnel", "start_tunnel while already running; replacing existing relay");
            old.abort();
        }
        // The relay task awaits the handoff, so a fd handed over before the
        // server identity resolves waits (holding the fd) rather than being lost.
        *guard = Some(runtime.spawn(relay_driver(self.handoff.clone(), tun, mtu)));
        tracing::info!(target: "neutrino::tunnel", "tunnel relay starting: fd={tun_fd}, mtu={mtu}");
    }

    /// Abort the relay task; closes the fd. Idempotent.
    pub(crate) fn stop(&self) {
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            task.abort();
            tracing::info!(target: "neutrino::tunnel", "tunnel relay stopped");
        }
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        // Don't leak the task/fd if the handle is dropped with a tunnel up.
        self.stop();
    }
}

/// Build the relay over the persisted identity + shared table and carry packets
/// between the host TUN `tun` and the wire until the task is aborted (stop /
/// runtime teardown).
async fn relay_driver(mut handoff: watch::Receiver<Option<TunnelHandoff>>, tun: OwnedFd, mtu: u32) {
    // Clamping the TUN MTU below the iroh datagram limit (so a packet is never
    // dropped as too-large) is a relay-layer concern not yet wired; the host
    // sets the MTU for now.
    let _ = mtu;
    // Wait for the entrypoint to publish the identity + shared table. This may
    // already be set (fd arrived after boot) or pending (fd raced ahead).
    let handoff = match handoff.wait_for(Option::is_some).await {
        Ok(guard) => (*guard).clone(),
        Err(_) => {
            tracing::error!(target: "neutrino::tunnel", "server stopped before identity resolved; tunnel not started");
            return;
        }
    };
    let Some(handoff) = handoff else {
        return; // unreachable: `wait_for` guaranteed `is_some`
    };
    let stack = match RelayStack::build(handoff.secret(), RELAY_BIND, handoff.table().clone()).await
    {
        Ok(stack) => stack,
        Err(e) => {
            tracing::error!(target: "neutrino::tunnel", "relay endpoint build failed ({e}); tunnel not started");
            return;
        }
    };
    let io = match TunPacketIo::new(tun) {
        Ok(io) => Arc::new(io),
        Err(e) => {
            tracing::error!(target: "neutrino::tunnel", "tun fd registration failed ({e}); tunnel not started");
            return;
        }
    };
    tracing::info!(target: "neutrino::tunnel", "tunnel relay up");
    stack.drive(io).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use neutrino_relay::NeighbourTable;
    use std::os::fd::IntoRawFd;
    use std::os::unix::net::UnixDatagram;
    use std::time::Duration;

    fn empty_handoff() -> watch::Receiver<Option<TunnelHandoff>> {
        watch::channel(None).1
    }

    // No runtime published → start is a no-op, but it still OWNS and closes the
    // fd it was handed (the host end then sees EOF) rather than leaking it.
    #[test]
    fn start_without_runtime_closes_the_fd() {
        let (host, dev) = UnixDatagram::pair().expect("socketpair");
        let dev_fd = dev.into_raw_fd();
        let tunnel = Tunnel::new(Arc::new(OnceLock::new()), empty_handoff());
        tunnel.start(dev_fd, 1500);
        // Sending to the now-closed peer fails (ECONNREFUSED), confirming close.
        assert!(host.send(b"probe").is_err());
    }

    // Full lifecycle: start waits for the handoff, builds the relay over real
    // iroh + the TUN fd, and stop aborts it — closing the fd.
    #[tokio::test]
    async fn start_builds_relay_then_stop_closes_the_fd() {
        let runtime = Arc::new(OnceLock::new());
        let _ = runtime.set(tokio::runtime::Handle::current());
        let (htx, hrx) = watch::channel(None);
        // A real (any 32-byte) secret + a fresh shared table.
        let _ = htx.send(Some(TunnelHandoff::new(
            [7u8; 32],
            Arc::new(NeighbourTable::new()),
            String::new(),
            String::new(),
        )));
        let _htx = htx; // keep the sender alive for the receiver

        let (host, dev) = UnixDatagram::pair().expect("socketpair");
        dev.set_nonblocking(true).expect("nonblock dev");
        let dev_fd = dev.into_raw_fd();

        let tunnel = Tunnel::new(runtime, hrx);
        tunnel.start(dev_fd, 1500);
        // Let the relay task resolve the handoff + bind the iroh endpoint.
        tokio::time::sleep(Duration::from_millis(300)).await;
        tunnel.stop();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The relay's TunPacketIo dropped on abort → dev fd closed → send fails.
        assert!(host.send(b"probe").is_err());
    }
}
