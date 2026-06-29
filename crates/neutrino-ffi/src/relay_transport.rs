// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! iroh-backed [`DatagramLink`] for the low-bandwidth federation transport.
//!
//! Carries one CoAP/CBOR datagram per unreliable QUIC datagram between nodes,
//! keyed by 32-byte node id — no OS socket, no TUN, no virtual IPs. iroh is
//! confined to this layer; `neutrino-lb` stays iroh-free and speaks only the
//! [`DatagramLink`] seam (`[u8; 32]` node ids). QUIC datagrams are
//! per-connection, so the transport keeps a `[u8; 32] → Connection` send-side
//! table (populated by dialing on egress and by accepting — a connection is
//! bidirectional, so one accepted from a peer is reused to send back). Every
//! connection (dialed or accepted) gets a reader task that tags each inbound
//! datagram with the cryptographically-authenticated remote node id; a reader
//! removes its own send-side entry when its connection dies, so the next send
//! re-dials instead of reusing a dead connection.
//!
//! The endpoint still binds its own ephemeral loopback UDP socket for QUIC
//! transport (see [`RELAY_BIND`]); that is iroh-internal and unrelated to the
//! deleted host-TUN data path.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use iroh::endpoint::presets::N0DisableRelay;
use iroh::endpoint::{Connection, VarInt};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use neutrino_main::DatagramLink;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

/// A node's stable cryptographic identity, as raw public-key bytes — the
/// [`DatagramLink`] node id. iroh's endpoint id IS these bytes.
type NodeKey = [u8; 32];

/// UDP bind for the iroh endpoint's QUIC transport. Ephemeral loopback port; on
/// device the BLE custom transport carries packets to peers and discovery
/// advertises reachability, so the UDP socket is only iroh's local plumbing.
pub(crate) const RELAY_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// ALPN for the federation datagram link.
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
    /// rebind).
    accept_task: Mutex<Option<JoinHandle<()>>>,
}

impl IrohTransport {
    /// Bind an endpoint whose identity is derived from `secret` (the same
    /// persisted node secret the server's `server_name` is derived from, so this
    /// endpoint's id equals that node id), and start accepting connections.
    pub(crate) async fn bind(
        secret: &[u8; 32],
        bind_addr: SocketAddr,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let secret_key = SecretKey::from_bytes(secret);
        // The BLE transport needs our public key; capture it before the key is
        // moved into the builder.
        #[cfg(feature = "ble")]
        let public = secret_key.public();
        let builder = Endpoint::builder(N0DisableRelay)
            .secret_key(secret_key)
            .alpns(vec![RELAY_ALPN.to_vec()]);

        // On the embedded (Android) target, add the BLE custom transport
        // *alongside* IP, so federation reaches peers over both LAN and BLE
        // (phones); the transport's `address_lookup` resolves peers over the BLE
        // mesh, while LAN peers are seeded via `add_peer`. Desktop/CI is IP-only.
        #[cfg(feature = "ble")]
        let endpoint = {
            // Bootstrap blew's Android JNI layer (no-op off Android), else
            // `Central::new` panics with "JVM not initialized".
            crate::ble_android::ensure_initialised()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
            let central = Arc::new(iroh_ble_transport::Central::new().await?);
            let peripheral = Arc::new(iroh_ble_transport::Peripheral::new().await?);
            let ble = iroh_ble_transport::transport::BleTransport::new(public, central, peripheral)
                .await?;
            let lookup = ble.address_lookup();
            let ble: Arc<dyn iroh::endpoint::transports::CustomTransport> = Arc::new(ble);
            builder
                .add_custom_transport(ble)
                .address_lookup(lookup)
                .bind_addr(bind_addr)?
                .bind()
                .await?
        };
        #[cfg(not(feature = "ble"))]
        let endpoint = builder.bind_addr(bind_addr)?.bind().await?;

        tracing::info!(
            node_id = %endpoint.id(),
            sockets = ?endpoint.bound_sockets(),
            ble = cfg!(feature = "ble"),
            "datagram link: iroh endpoint bound"
        );

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

    /// This node's identity (its iroh endpoint id) as a [`NodeKey`].
    #[allow(dead_code)]
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
                // Best-effort: drop rather than block this reader (which would
                // stall every peer's inbound — head-of-line) when the consumer is
                // slow to drain. Closed channel = the link is gone → stop.
                match tx.try_send((peer, bytes.to_vec())) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::trace!("datagram link: inbound queue full, dropping datagram");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            let mut map = conns.lock().await;
            if map.get(&peer).map(Connection::stable_id) == Some(conn_id) {
                map.remove(&peer);
            }
        });
    }

    /// A live connection to `dst`, dialing (and starting its reader) on a miss.
    async fn connection(&self, dst: NodeKey) -> std::io::Result<Connection> {
        {
            let conns = self.conns.lock().await;
            if let Some(conn) = conns.get(&dst)
                && conn.close_reason().is_none()
            {
                return Ok(conn.clone());
            }
        }
        let seeded = self
            .addrs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&dst)
            .cloned();
        let addr = match seeded {
            Some(addr) => addr,
            // No seeded IP address. With the BLE transport present, dial by
            // endpoint id alone and let the endpoint's `address_lookup` (the BLE
            // mesh discovery wired in `bind`) resolve a path — this is the device
            // path: nothing seeds `addrs` on a phone (`add_peer` is the test/LAN
            // seam only). Without BLE (desktop/CI) an unseeded peer is genuinely
            // unreachable, so keep failing fast.
            #[cfg(feature = "ble")]
            None => EndpointAddr::new(
                EndpointId::from_bytes(&dst)
                    .map_err(|e| std::io::Error::other(format!("link: invalid peer id: {e}")))?,
            ),
            #[cfg(not(feature = "ble"))]
            None => {
                return Err(std::io::Error::other("link: no known address for peer"));
            }
        };
        tracing::debug!(addrs = ?addr.addrs, "datagram link: dialing peer (by id over discovery if no addrs)");
        let conn = self
            .endpoint
            .connect(addr, RELAY_ALPN)
            .await
            .map_err(|e| std::io::Error::other(format!("link connect: {e}")))?;
        tracing::debug!("datagram link: connection established");
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
        // accept loop holds only clones, so this Drop fires once the last
        // `Arc<Self>` drops — without this, an endpoint would leak per rebind.
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
                Err(err) => warn!(?err, "datagram link: inbound connection failed"),
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
impl DatagramLink for IrohTransport {
    async fn send(&self, dst: NodeKey, datagram: &[u8]) -> std::io::Result<()> {
        let conn = self.connection(dst).await?;
        conn.send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|e| std::io::Error::other(format!("send_datagram: {e}")))
    }

    async fn recv(&self) -> Option<(NodeKey, Vec<u8>)> {
        self.inbound_rx.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // Full link flow over real iroh, driving the `DatagramLink` seam directly:
    // A sends a datagram to B, B receives it tagged with A's authenticated node
    // id, then B replies A over the reused (accepted) connection. Exercises dial,
    // accept, bidirectional reuse, the inbound source tagging, and
    // identity-from-secret.
    #[tokio::test]
    async fn datagram_relays_a_to_b_to_a_over_iroh() {
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

        // A can reach B (the egress/dial path); B learns A on inbound.
        a_tp.add_peer(loopback_addr(&b_tp));

        // A → B.
        let to_b = b"hello-b";
        a_tp.send(b_key, to_b).await.expect("send a->b");
        let (src, got) = timeout(Duration::from_secs(10), b_tp.recv())
            .await
            .expect("B receives in time")
            .expect("B link open");
        assert_eq!(src, a_key, "datagram tagged with A's authenticated node id");
        assert_eq!(got, to_b);

        // B → A, routed via the reused (accepted) connection — B never seeded A.
        let to_a = b"hello-a";
        b_tp.send(a_key, to_a).await.expect("send b->a");
        let (src, got) = timeout(Duration::from_secs(10), a_tp.recv())
            .await
            .expect("A receives in time")
            .expect("A link open");
        assert_eq!(src, b_key);
        assert_eq!(got, to_a);
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

    // Load-bearing cross-layer invariant: the link's `node_key` (iroh endpoint
    // id) must equal the ed25519 public key that neutrino-main derives the
    // server_name from for the same secret — otherwise the host would advertise
    // one node id while the link answers on another, silently breaking
    // federation. iroh's node id IS the raw ed25519 pubkey today; this pins it so
    // a future iroh key-derivation change fails loudly here.
    #[tokio::test]
    async fn node_key_matches_ed25519_public_key() {
        let secret = [7u8; 32];
        let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let tp = IrohTransport::bind(&secret, loopback).await.expect("bind");
        let expected = ed25519_dalek::SigningKey::from_bytes(&secret)
            .verifying_key()
            .to_bytes();
        assert_eq!(tp.node_key(), expected);
    }
}
