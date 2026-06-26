// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Tunnel routing glue: turns a federation `server_name` into both a relay
//! route and a dialable virtual address, and assembles the relay's moving parts.
//!
//! A peer's `server_name` is its iroh node id (hex). The data plane
//! (`neutrino-relay`) only ever sees IP packets addressed to a *virtual IP*, so
//! two name-aware steps live here, above the relay:
//! - [`TunnelResolver`] rewrites the federation egress destination
//!   (`server_name`) to the peer's vip, so outbound CoAP/UDP is addressed into
//!   the tunnel. It is a **pure** mapping (no side effects).
//! - [`RelayStack::register_peer`] is the invite-time route registration: when a
//!   peer is first learned (its `server_name` appears), its vip→node route is
//!   added so the relay can carry the first packet (inbound learning covers the
//!   reverse direction thereafter).
//!
//! [`RelayStack`] bundles the iroh transport and the neighbour table so the
//! entrypoint can build everything from the persisted node secret, hand the
//! resolver to the federation sidecar, and spawn the relay over the host TUN.

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use iroh::{EndpointAddr, EndpointId};
use neutrino_lb::DestinationResolver;
use neutrino_relay::{NeighbourTable, NodeKey, PacketIo, run, vip};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::relay_transport::IrohTransport;

/// Decode a federation `server_name` (the peer's iroh node id, hex, with an
/// optional `:port`) into a relay [`NodeKey`]. `None` if the host isn't a node
/// id (e.g. a dev `localhost`) — such a name has no tunnel route.
fn server_name_to_node_key(authority: &str) -> Option<NodeKey> {
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _port)| host);
    EndpointId::from_str(host).ok().map(|id| *id.as_bytes())
}

/// [`DestinationResolver`] for the tunnel: maps a peer `server_name` to the
/// virtual IP the relay routes to, preserving any port. A non-node authority is
/// passed through unchanged so direct-dial behaviour is unaffected.
pub(crate) struct TunnelResolver;

impl DestinationResolver for TunnelResolver {
    fn resolve(&self, authority: String) -> String {
        match server_name_to_node_key(&authority) {
            Some(key) => {
                let v = vip(&key);
                match authority.rsplit_once(':') {
                    Some((_host, port)) => format!("[{v}]:{port}"),
                    None => format!("[{v}]"),
                }
            }
            None => {
                warn!(%authority, "tunnel resolver: server_name is not a node id; dialing verbatim");
                authority
            }
        }
    }
}

/// The relay's moving parts, built from the persisted node secret: the iroh
/// transport and the shared neighbour table. Hands the entrypoint everything it
/// needs to route federation through the tunnel.
pub(crate) struct RelayStack {
    transport: Arc<IrohTransport>,
    table: Arc<NeighbourTable>,
}

impl RelayStack {
    /// Build the transport (identity derived from `secret`) and an empty route
    /// table.
    pub(crate) async fn build(
        secret: &[u8; 32],
        bind_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            transport: IrohTransport::bind(secret, bind_addr).await?,
            table: Arc::new(NeighbourTable::new()),
        })
    }

    /// This node's identity, as the relay sees it.
    pub(crate) fn node_key(&self) -> NodeKey {
        self.transport.node_key()
    }

    /// The address resolver to inject into the federation egress
    /// (`LbConfig::resolver`).
    pub(crate) fn resolver(&self) -> Arc<dyn DestinationResolver> {
        Arc::new(TunnelResolver)
    }

    /// Invite-time route registration: record how to route to a peer learned by
    /// `server_name`. No-op if it isn't a node id.
    pub(crate) fn register_peer(&self, server_name: &str) {
        match server_name_to_node_key(server_name) {
            Some(key) => self.table.register(key),
            None => warn!(%server_name, "tunnel: cannot register non-node server_name"),
        }
    }

    /// Seed how to reach a peer (service discovery on device; the test seeds a
    /// loopback address).
    pub(crate) fn add_peer_addr(&self, addr: EndpointAddr) {
        self.transport.add_peer(addr);
    }

    /// This endpoint's id, for building its own advertised [`EndpointAddr`].
    pub(crate) fn endpoint_id(&self) -> EndpointId {
        self.transport.endpoint_id()
    }

    /// The sockets the transport is bound to.
    pub(crate) fn bound_sockets(&self) -> Vec<SocketAddr> {
        self.transport.bound_sockets()
    }

    /// Spawn the relay loop, carrying IP packets between `io` (the host TUN) and
    /// the wire. Runs until a seam closes.
    pub(crate) fn spawn<P: PacketIo + 'static>(&self, io: Arc<P>) -> JoinHandle<()> {
        tokio::spawn(run(
            self.node_key(),
            self.table.clone(),
            io,
            self.transport.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use neutrino_relay::mem::{ipv6_packet, mem_packet_io};
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn server_name_round_trips_to_node_key() {
        let pk = SecretKey::from_bytes(&[9u8; 32]).public();
        assert_eq!(
            server_name_to_node_key(&pk.to_string()),
            Some(*pk.as_bytes())
        );
        // With a port, and a non-node host.
        assert_eq!(
            server_name_to_node_key(&format!("{pk}:8448")),
            Some(*pk.as_bytes())
        );
        assert_eq!(server_name_to_node_key("localhost:8008"), None);
    }

    #[test]
    fn resolver_passes_through_non_node_authority() {
        assert_eq!(
            TunnelResolver.resolve("localhost:8008".to_owned()),
            "localhost:8008"
        );
    }

    #[test]
    fn resolver_maps_node_server_name_to_vip() {
        let pk = SecretKey::from_bytes(&[5u8; 32]).public();
        let key = *pk.as_bytes();
        let v = vip(&key);
        assert_eq!(TunnelResolver.resolve(pk.to_string()), format!("[{v}]"));
        assert_eq!(
            TunnelResolver.resolve(format!("{pk}:8448")),
            format!("[{v}]:8448")
        );
    }

    // End-to-end through the assembly: a packet addressed by B's server_name is
    // carried to B over real iroh, using a route seeded by `register_peer` and
    // an address resolved by the resolver.
    #[tokio::test]
    async fn server_name_addressed_packet_is_relayed_over_iroh() {
        let lo: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let a = RelayStack::build(&[1u8; 32], lo).await.expect("build A");
        let b = RelayStack::build(&[2u8; 32], lo).await.expect("build B");

        let b_server_name = b.endpoint_id().to_string();
        let b_key = b.node_key();

        // A discovers B's address and learns its route from the server_name (the
        // invite-time path). B learns A's route from the inbound datagram.
        let b_sock = b
            .bound_sockets()
            .into_iter()
            .find(|s| s.ip().is_loopback())
            .expect("loopback socket");
        a.add_peer_addr(EndpointAddr::new(b.endpoint_id()).with_ip_addr(b_sock));
        a.register_peer(&b_server_name);

        // The resolver agrees on B's vip.
        assert_eq!(
            a.resolver().resolve(format!("{b_server_name}:8448")),
            format!("[{}]:8448", vip(&b_key))
        );

        let (a_io, a_host) = mem_packet_io();
        let (b_io, b_host) = mem_packet_io();
        a.spawn(Arc::new(a_io));
        b.spawn(Arc::new(b_io));

        let payload = b"federation-bytes";
        a_host
            .emit(ipv6_packet(vip(&a.node_key()), vip(&b_key), payload))
            .await
            .expect("emit");
        let got = timeout(Duration::from_secs(10), b_host.next())
            .await
            .expect("B receives in time")
            .expect("B channel open");
        assert_eq!(&got[40..], payload);
    }
}
