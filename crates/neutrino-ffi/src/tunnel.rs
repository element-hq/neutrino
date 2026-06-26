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

use std::net::SocketAddr;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex, OnceLock};

use neutrino_main::TunnelHandoff;
use neutrino_relay::NeighbourTable;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::relay_stack::RelayStack;
use crate::tun_io::TunPacketIo;

/// UDP bind for the relay's iroh endpoint. Ephemeral port; service discovery
/// (mDNS/BLE) will advertise the bound address to peers (not yet wired).
const RELAY_BIND: &str = "0.0.0.0:0";

/// Owns the running tunnel relay task, if any. Held by `NeutrinoHandle`; cheap to
/// construct (no task, no fd) so it can sit on an idle handle.
pub(crate) struct Tunnel {
    /// Server runtime handle, published by the runtime thread once built. The
    /// relay is spawned onto it, so it is cancelled when the homeserver stops.
    runtime: Arc<OnceLock<Handle>>,
    /// The node secret + shared route table, published by the entrypoint once the
    /// server identity is resolved. The relay is built over these.
    handoff: Arc<OnceLock<TunnelHandoff>>,
    /// The running relay task, if any. Aborting it stops the relay and closes the
    /// fd (its `TunPacketIo` drops).
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tunnel {
    pub(crate) fn new(
        runtime: Arc<OnceLock<Handle>>,
        handoff: Arc<OnceLock<TunnelHandoff>>,
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
        let Some(handoff) = self.handoff.get() else {
            tracing::error!(
                target: "neutrino::tunnel",
                "start_tunnel before the server identity is ready; ignoring",
            );
            return;
        };
        let secret = handoff.secret;
        let table = handoff.table.clone();

        // Recover from a poisoned lock: the state is just an Option<task>.
        let mut guard = self.task.lock().unwrap_or_else(|e| e.into_inner());
        // Replace any existing relay (e.g. a re-toggle without an intervening
        // stop): aborting it drops its `TunPacketIo`, closing the old fd.
        if let Some(old) = guard.take() {
            tracing::warn!(target: "neutrino::tunnel", "start_tunnel while already running; replacing existing relay");
            old.abort();
        }
        *guard = Some(runtime.spawn(relay_driver(secret, table, tun, mtu)));
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
async fn relay_driver(secret: [u8; 32], table: Arc<NeighbourTable>, tun: OwnedFd, mtu: u32) {
    // Clamping the TUN MTU below the iroh datagram limit (so a packet is never
    // dropped as too-large) is a relay-layer concern not yet wired; the host
    // sets the MTU for now.
    let _ = mtu;
    let bind: SocketAddr = match RELAY_BIND.parse() {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(target: "neutrino::tunnel", "invalid relay bind addr ({e})");
            return;
        }
    };
    let stack = match RelayStack::build(&secret, bind, table).await {
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
