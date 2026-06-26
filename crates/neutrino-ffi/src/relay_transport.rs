// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! iroh-backed [`DatagramTransport`] for the packet relay.
//!
//! Carries one IP packet per unreliable QUIC datagram between endpoints. iroh
//! is confined to this layer (the TUN side); `neutrino-relay` stays
//! transport-agnostic and speaks only `NodeKey` ([`u8; 32`]). QUIC datagrams
//! are per-connection, so the transport keeps a `NodeKey → Connection`
//! send-side table (populated by dialing on egress and by accepting — a
//! connection is bidirectional, so one accepted from a peer is reused to send
//! back). Every connection (dialed or accepted) gets a reader task that tags
//! each inbound datagram with the cryptographically-authenticated remote node
//! id; a reader removes its own send-side entry when its connection dies, so
//! the next send re-dials instead of reusing a dead connection.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use iroh::endpoint::presets::N0DisableRelay;
use iroh::endpoint::{Connection, VarInt};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use neutrino_relay::{DatagramTransport, NodeKey, RelayError};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

/// ALPN for the federation packet relay.
const RELAY_ALPN: &[u8] = b"neutrino/iroh-relay/0";

/// Bound on buffered inbound datagrams before the per-connection readers block
/// (back-pressure onto the wire, which is acceptable for a best-effort link).
const INBOUND_CAPACITY: usize = 256;

/// Send-side route table: the connection to use for sending to each peer.
type ConnMap = Arc<AsyncMutex<HashMap<NodeKey, Connection>>>;

pub(crate) struct IrohTransport {
    endpoint: Endpoint,
    conns: ConnMap,
    /// Where to reach a peer. Seeded out of band — service discovery on device,
    /// the test seeds loopback addresses. A peer with no entry that has never
    /// dialed us cannot be reached.
    addrs: Mutex<HashMap<NodeKey, EndpointAddr>>,
    inbound_tx: mpsc::Sender<(NodeKey, Vec<u8>)>,
    inbound_rx: AsyncMutex<mpsc::Receiver<(NodeKey, Vec<u8>)>>,
    /// The accept-loop task. Aborted on drop so the endpoint (and its UDP
    /// socket) can close — the loop captures only clones, never an `Arc<Self>`,
    /// so it doesn't keep this transport alive (which would leak an endpoint per
    /// VPN re-toggle).
    accept_task: Mutex<Option<JoinHandle<()>>>,
}

impl IrohTransport {
    /// Bind an endpoint whose identity is derived from `secret` (the same
    /// persisted node secret the server's `server_name` is derived from, so
    /// `vip(self)` names this exact node), and start accepting connections.
    pub(crate) async fn bind(
        secret: &[u8; 32],
        bind_addr: SocketAddr,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = Endpoint::builder(N0DisableRelay)
            .secret_key(SecretKey::from_bytes(secret))
            .alpns(vec![RELAY_ALPN.to_vec()])
            .bind_addr(bind_addr)?
            .bind()
            .await?;
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_CAPACITY);
        let conns: ConnMap = Arc::new(AsyncMutex::new(HashMap::new()));
        // Spawn the accept loop with clones (endpoint/conns/tx), NOT an
        // `Arc<Self>` — so the transport isn't kept alive by its own loop.
        let accept_task = tokio::spawn(accept_loop(
            endpoint.clone(),
            conns.clone(),
            inbound_tx.clone(),
        ));
        Ok(Arc::new(Self {
            endpoint,
            conns,
            addrs: Mutex::new(HashMap::new()),
            inbound_tx,
            inbound_rx: AsyncMutex::new(inbound_rx),
            accept_task: Mutex::new(Some(accept_task)),
        }))
    }

    /// This node's identity (its iroh endpoint id) as a relay [`NodeKey`].
    pub(crate) fn node_key(&self) -> NodeKey {
        *self.endpoint.id().as_bytes()
    }

    // The discovery accessors below are wired when service discovery (mDNS/BLE)
    // lands; until then they're used only via the test seeders.
    /// This endpoint's id, for handing to peers as part of an [`EndpointAddr`].
    #[allow(dead_code)]
    pub(crate) fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// The sockets this endpoint is bound to.
    #[allow(dead_code)]
    pub(crate) fn bound_sockets(&self) -> Vec<SocketAddr> {
        self.endpoint.bound_sockets()
    }

    /// Teach the transport how to reach a peer. Keyed by the address's own
    /// endpoint id, so the mapping has a single source of truth.
    #[allow(dead_code)]
    pub(crate) fn add_peer(&self, addr: EndpointAddr) {
        let key = *addr.id.as_bytes();
        self.addrs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, addr);
    }

    /// Drain a connection's datagrams into the inbound queue until it closes,
    /// then drop its send-side entry iff the map still points to *this*
    /// connection — so the next send re-dials and we never evict a newer one.
    fn spawn_reader(
        peer: NodeKey,
        conn: Connection,
        conns: ConnMap,
        tx: mpsc::Sender<(NodeKey, Vec<u8>)>,
    ) {
        let conn_id = conn.stable_id();
        tokio::spawn(async move {
            // `read_datagram` errors only on connection close — terminal.
            while let Ok(bytes) = conn.read_datagram().await {
                if tx.send((peer, bytes.to_vec())).await.is_err() {
                    break; // the relay dropped its receiver
                }
            }
            let mut map = conns.lock().await;
            if map.get(&peer).map(Connection::stable_id) == Some(conn_id) {
                map.remove(&peer);
            }
        });
    }

    /// A live connection to `dst`, dialing (and starting its reader) on a miss.
    async fn connection(&self, dst: NodeKey) -> Result<Connection, RelayError> {
        {
            let conns = self.conns.lock().await;
            if let Some(conn) = conns.get(&dst)
                && conn.close_reason().is_none()
            {
                return Ok(conn.clone());
            }
        }
        let addr = self
            .addrs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&dst)
            .cloned()
            .ok_or_else(|| RelayError::Io("relay: no known address for peer".to_owned()))?;
        let conn = self
            .endpoint
            .connect(addr, RELAY_ALPN)
            .await
            .map_err(|e| RelayError::Io(format!("relay connect: {e}")))?;
        {
            let mut conns = self.conns.lock().await;
            // A live connection may have appeared while we dialed (a concurrent
            // dial, or an accepted one): keep it and close our loser.
            if let Some(existing) = conns.get(&dst)
                && existing.close_reason().is_none()
            {
                let existing = existing.clone();
                conn.close(VarInt::from_u32(0), b"superseded");
                return Ok(existing);
            }
            conns.insert(dst, conn.clone());
        }
        Self::spawn_reader(
            dst,
            conn.clone(),
            self.conns.clone(),
            self.inbound_tx.clone(),
        );
        Ok(conn)
    }
}

impl Drop for IrohTransport {
    fn drop(&mut self) {
        // Stop accepting so the endpoint (and its UDP socket) can close. The
        // accept loop holds only clones, so this Drop fires once the relay drops
        // its `Arc<Self>` — without this, an endpoint would leak per re-toggle.
        if let Some(task) = self
            .accept_task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

/// Accept inbound connections (with clones of the shared state, not an
/// `Arc<IrohTransport>`, so the loop never keeps the transport alive).
async fn accept_loop(
    endpoint: Endpoint,
    conns: ConnMap,
    inbound_tx: mpsc::Sender<(NodeKey, Vec<u8>)>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let conns = conns.clone();
        let inbound_tx = inbound_tx.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => adopt(conn, conns, inbound_tx).await,
                Err(err) => warn!(?err, "relay transport: inbound connection failed"),
            }
        });
    }
}

/// Adopt an accepted connection: always read from it (the peer may send on it),
/// but only take over the send-side route if we have no live connection to this
/// peer yet — don't clobber an existing one (glare).
async fn adopt(conn: Connection, conns: ConnMap, inbound_tx: mpsc::Sender<(NodeKey, Vec<u8>)>) {
    let peer = *conn.remote_id().as_bytes();
    {
        let mut map = conns.lock().await;
        let vacant = match map.get(&peer) {
            Some(existing) => existing.close_reason().is_some(),
            None => true,
        };
        if vacant {
            map.insert(peer, conn.clone());
        }
    }
    IrohTransport::spawn_reader(peer, conn, conns, inbound_tx);
}

#[async_trait]
impl DatagramTransport for IrohTransport {
    async fn send(&self, dst: NodeKey, datagram: &[u8]) -> Result<(), RelayError> {
        let conn = self.connection(dst).await?;
        conn.send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|e| RelayError::Io(format!("send_datagram: {e}")))
    }

    async fn recv(&self) -> Option<(NodeKey, Vec<u8>)> {
        self.inbound_rx.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neutrino_relay::mem::{ipv6_packet, mem_packet_io};
    use neutrino_relay::{NeighbourTable, run, vip};
    use std::time::Duration;
    use tokio::time::timeout;

    /// Pick a node's loopback dialing address from its bound sockets.
    fn loopback_addr(tp: &IrohTransport) -> EndpointAddr {
        let sock = tp
            .bound_sockets()
            .into_iter()
            .find(|s| s.ip().is_loopback())
            .expect("a loopback bound socket");
        EndpointAddr::new(tp.endpoint_id()).with_ip_addr(sock)
    }

    // Full relay flow over real iroh: inject a packet at A's TUN, watch it reach
    // B's TUN, then a reply B→A. Exercises dial, accept, bidirectional reuse of
    // the accepted connection, inbound route learning, and identity-from-secret.
    #[tokio::test]
    async fn packet_relays_a_to_b_to_a_over_iroh() {
        let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let a_tp = IrohTransport::bind(&[1u8; 32], loopback)
            .await
            .expect("bind A");
        let b_tp = IrohTransport::bind(&[2u8; 32], loopback)
            .await
            .expect("bind B");

        let a_key = a_tp.node_key();
        let b_key = b_tp.node_key();
        assert_ne!(a_key, b_key);

        // A can reach B (the resolver/invite path); B will learn A on inbound.
        a_tp.add_peer(loopback_addr(&b_tp));

        let a_table = Arc::new(NeighbourTable::new());
        let b_table = Arc::new(NeighbourTable::new());
        a_table.register(b_key);

        let (a_io, a_host) = mem_packet_io();
        let (b_io, b_host) = mem_packet_io();

        tokio::spawn(run(a_key, a_table, Arc::new(a_io), a_tp));
        tokio::spawn(run(b_key, b_table.clone(), Arc::new(b_io), b_tp));

        // A → B.
        let to_b = b"hello-b";
        a_host
            .emit(ipv6_packet(vip(&a_key), vip(&b_key), to_b))
            .await
            .expect("emit a->b");
        let got = timeout(Duration::from_secs(10), b_host.next())
            .await
            .expect("B receives in time")
            .expect("B channel open");
        assert_eq!(&got[40..], to_b);
        // B learned the reverse route from the authenticated inbound datagram.
        assert_eq!(b_table.lookup(&vip(&a_key)), Some(a_key));

        // B → A, routed via the learned entry and the reused (accepted) conn.
        let to_a = b"hello-a";
        b_host
            .emit(ipv6_packet(vip(&b_key), vip(&a_key), to_a))
            .await
            .expect("emit b->a");
        let got = timeout(Duration::from_secs(10), a_host.next())
            .await
            .expect("A receives in time")
            .expect("A channel open");
        assert_eq!(&got[40..], to_a);
    }

    // The one error the transport itself originates: a destination with no
    // seeded address that has never dialed us is unroutable.
    #[tokio::test]
    async fn send_to_unknown_peer_is_an_error() {
        let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let tp = IrohTransport::bind(&[3u8; 32], loopback)
            .await
            .expect("bind");
        assert!(tp.send([9u8; 32], b"x").await.is_err());
    }
}
