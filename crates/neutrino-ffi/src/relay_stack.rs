// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Assembles the relay's iroh-side moving parts — the [`IrohTransport`] and the
//! shared [`NeighbourTable`] — built from the persisted node secret, for the
//! entrypoint to spawn over the host TUN.
//!
//! The name-aware routing (federation `server_name` → virtual IP, and route
//! registration) is pure and lives in `neutrino-main`; the table is shared with
//! it so routes registered there are seen here. This layer is iroh-only, keeping
//! iroh confined to the TUN side.

use std::net::SocketAddr;
use std::sync::Arc;

use iroh::{EndpointAddr, EndpointId};
use neutrino_relay::{NeighbourTable, NodeKey, PacketIo, run};
#[cfg(test)]
use tokio::task::JoinHandle;

use crate::relay_transport::IrohTransport;

/// The iroh transport plus the (shared) neighbour table the relay routes on.
pub(crate) struct RelayStack {
    transport: Arc<IrohTransport>,
    table: Arc<NeighbourTable>,
}

impl RelayStack {
    /// Build the transport (identity derived from `secret`) over a `table`
    /// shared with the federation routing layer in `neutrino-main`.
    pub(crate) async fn build(
        secret: &[u8; 32],
        bind_addr: SocketAddr,
        table: Arc<NeighbourTable>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            transport: IrohTransport::bind(secret, bind_addr).await?,
            table,
        })
    }

    /// This node's identity, as the relay sees it.
    pub(crate) fn node_key(&self) -> NodeKey {
        self.transport.node_key()
    }

    /// Record a route to a peer by node key. Live registration goes through
    /// `neutrino-main`'s name-based `register_route` on the shared table; this is
    /// the by-key counterpart used to seed routes in tests.
    #[cfg(test)]
    pub(crate) fn register(&self, node: NodeKey) {
        self.table.register(node);
    }

    // Discovery accessors: wired when service discovery (mDNS/BLE) lands; until
    // then they're used only via the tests.
    /// Seed how to reach a peer (service discovery on device; the test seeds a
    /// loopback address).
    #[allow(dead_code)]
    pub(crate) fn add_peer_addr(&self, addr: EndpointAddr) {
        self.transport.add_peer(addr);
    }

    /// This endpoint's id, for building its own advertised [`EndpointAddr`].
    #[allow(dead_code)]
    pub(crate) fn endpoint_id(&self) -> EndpointId {
        self.transport.endpoint_id()
    }

    /// The sockets the transport is bound to.
    #[allow(dead_code)]
    pub(crate) fn bound_sockets(&self) -> Vec<SocketAddr> {
        self.transport.bound_sockets()
    }

    /// Spawn the relay loop on the current runtime (the live path uses [`drive`]
    /// so it can be aborted by the tunnel task; this detached form is for tests).
    #[cfg(test)]
    pub(crate) fn spawn<P: PacketIo + 'static>(&self, io: Arc<P>) -> JoinHandle<()> {
        tokio::spawn(run(
            self.node_key(),
            self.table.clone(),
            io,
            self.transport.clone(),
        ))
    }

    /// Drive the relay loop in the caller's task (so dropping/aborting that task
    /// stops the relay and closes `io`). Used by the tunnel driver, whose task is
    /// aborted on stop / runtime teardown.
    pub(crate) async fn drive<P: PacketIo + 'static>(&self, io: Arc<P>) {
        run(
            self.node_key(),
            self.table.clone(),
            io,
            self.transport.clone(),
        )
        .await;
    }
}
