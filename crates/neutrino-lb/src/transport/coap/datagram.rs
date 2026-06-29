//! In-process datagram-backed CoAP wire path, a sibling of the UDP path in the
//! parent module. For the embedded/Android target the OS UDP socket is replaced
//! by an iroh QUIC transport keyed by a 32-byte peer **node id** — no socket, no
//! IP, no ports. iroh itself stays in `neutrino-ffi`; this module is iroh-free
//! and defines the [`DatagramLink`] seam that ffi will implement.
//!
//! ## Why a Hub
//!
//! UDP gave us a free separation that we now have to recreate. With UDP, our
//! egress client and our ingress server each bind their *own* socket (own port),
//! so responses to our outbound requests and inbound requests to our server
//! arrive on physically distinct queues. A [`DatagramLink`] has a *single*
//! inbound queue per process (one iroh endpoint) carrying BOTH. The [`Hub`] runs
//! one drain task over `link.recv()` and classifies each datagram by its CoAP
//! header code:
//!
//! - a `Request(_)` code → an inbound request → the server side (the listener).
//! - any other valid code → a response → the per-node client inbox.
//! - a hard parse failure → dropped (see [`classify`]).
//!
//! ## Why a synthetic SocketAddr
//!
//! coap-rs is structured around `SocketAddr`: it keys per-peer blockwise / Q-Block
//! reassembly state by `Responder::address()`, and `ClientTransport::recv`
//! returns an informational `Option<SocketAddr>`. We have no real address, so
//! [`synthetic_addr`] derives a deterministic, collision-free-per-node
//! `SocketAddr` PURELY as that in-process key. It is never bound and never put on
//! a wire; the real routing is the 32-byte node id carried alongside.

use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use coap::Server;
use coap::client::CoAPClient;
use coap::server::{Listener, Responder, TransportRequestSender};
use coap_lite::{BlockHandlerConfig, MessageClass, Packet};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::transport::{
    MAX_WIRE_BODY_BYTES, WireClient, WireError, WireHandler, WireRequest, WireResponse, WireServer,
};

use super::{CoapDispatch, MAX_QBLOCK_INFLIGHT_TRANSFERS, exchange, random_token_seed};

/// A datagram tagged with the 32-byte node it came from / is bound for.
type NodeDatagram = ([u8; 32], Vec<u8>);

/// The iroh-free transport seam. ffi implements this over an iroh QUIC endpoint,
/// keyed by a 32-byte peer node id; the rest of lb stays iroh-free. A single
/// instance multiplexes BOTH directions (see the module-level Hub note).
#[async_trait]
pub trait DatagramLink: Send + Sync {
    /// Best-effort send of one datagram to peer node `dst` (32-byte id). A
    /// CoAP-level CON/Q-Block exchange tolerates loss, so "best effort" is the
    /// right contract: coap-rs drives retransmit/recovery on top.
    async fn send(&self, dst: [u8; 32], datagram: &[u8]) -> std::io::Result<()>;
    /// Next inbound `(cryptographically-authenticated source node, datagram)`, or
    /// `None` once the link is closed (which ends the [`Hub`] drain task).
    async fn recv(&self) -> Option<([u8; 32], Vec<u8>)>;
}

/// Mux/demux over one [`DatagramLink`]. Owns the link, runs one background drain
/// task that classifies every inbound datagram, and hands out per-node client
/// inboxes (for [`IrohClientTransport`]) plus the single requests receiver (for
/// [`IrohCoapListener`]).
pub struct Hub {
    link: Arc<dyn DatagramLink>,
    /// Per-node client inbox senders. A response datagram for node `n` is pushed
    /// onto `clients[n]`; the matching `IrohClientTransport::recv` drains it.
    /// A `std::sync::Mutex` (never held across an await — every critical section
    /// is one map op), so the inbox can be installed *synchronously* while the
    /// egress pool slot is claimed, keeping the pooled client and its inbox in
    /// lockstep (see [`IrohCoapWireClient::client_for`]).
    clients: std::sync::Mutex<HashMap<[u8; 32], mpsc::UnboundedSender<Vec<u8>>>>,
    /// The server-side request feed. The drain task pushes inbound *requests*
    /// here; the listener takes the receiver exactly once via [`Hub::take_requests`].
    requests_tx: mpsc::UnboundedSender<NodeDatagram>,
    requests_rx: Mutex<Option<mpsc::UnboundedReceiver<NodeDatagram>>>,
}

impl Hub {
    /// Build a hub over `link` and spawn its single drain task. Held behind an
    /// `Arc` because the drain task, every client transport, and every responder
    /// share it.
    pub fn new(link: Arc<dyn DatagramLink>) -> Arc<Self> {
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let hub = Arc::new(Self {
            link,
            clients: std::sync::Mutex::new(HashMap::new()),
            requests_tx,
            requests_rx: Mutex::new(Some(requests_rx)),
        });
        tokio::spawn(drain_loop(hub.clone()));
        hub
    }

    /// The underlying link, so client transports can `send` and responders can
    /// reply over the same iroh endpoint.
    pub fn link(&self) -> Arc<dyn DatagramLink> {
        self.link.clone()
    }

    /// Install the inbox sender for `node`, replacing any prior one. Called by the
    /// egress pool's *winner* (under its pool lock) so the registered inbox and the
    /// pooled `CoAPClient` are always the same transport — a concurrent first-send
    /// to the same node can't leave the pool pointing at one transport while the
    /// drain task routes responses to another's (discarded) inbox.
    fn install_client(&self, node: [u8; 32], tx: mpsc::UnboundedSender<Vec<u8>>) {
        self.clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node, tx);
    }

    /// Take the server-side requests receiver. Returns `None` if already taken —
    /// one hub drives at most one ingress listener.
    async fn take_requests(&self) -> Option<mpsc::UnboundedReceiver<NodeDatagram>> {
        self.requests_rx.lock().await.take()
    }

    /// Route one classified response datagram to its per-node client inbox.
    /// Drops (debug-logged) when no inbox is registered for `node` — e.g. a late
    /// response after the exchange's transport was dropped.
    fn deliver_response(&self, node: [u8; 32], bytes: Vec<u8>) {
        let tx = self
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&node)
            .cloned();
        match tx {
            Some(tx) => {
                if tx.send(bytes).is_err() {
                    tracing::debug!("datagram: client inbox closed, dropping response");
                }
            }
            None => tracing::debug!("datagram: no client inbox for node, dropping response"),
        }
    }
}

/// The single drain task: pull each inbound datagram off the link, classify it,
/// and route it. Ends when `link.recv()` returns `None` (link closed).
async fn drain_loop(hub: Arc<Hub>) {
    while let Some((node, bytes)) = hub.link.recv().await {
        match classify(&bytes) {
            Class::Request => {
                // Listener may not exist yet / may be gone; best-effort enqueue.
                if hub.requests_tx.send((node, bytes)).is_err() {
                    tracing::debug!("datagram: requests channel closed, dropping request");
                }
            }
            Class::Response => hub.deliver_response(node, bytes),
            Class::Drop => tracing::debug!("datagram: undecodable datagram, dropping"),
        }
    }
}

/// Classification of an inbound datagram by its CoAP header code.
enum Class {
    /// A CoAP request → the server/listener side.
    Request,
    /// A response/empty/reserved code → a client inbox.
    Response,
    /// Undecodable (hard parse failure) → drop.
    Drop,
}

/// Classify a datagram by parsing only its CoAP header. A `Request(_)` code is an
/// inbound request to our server; every other valid code (`Response`, `Empty`,
/// reserved) is treated as a response to one of our outbound exchanges and routed
/// to the client side. A hard parse failure is dropped — we cannot attribute it
/// to either side, and forging a request-looking header gains nothing (the source
/// node is cryptographically authenticated by the link, and an empty/garbage body
/// fails downstream at the handler).
fn classify(bytes: &[u8]) -> Class {
    match Packet::from_bytes(bytes) {
        Ok(packet) => match packet.header.code {
            MessageClass::Request(_) => Class::Request,
            _ => Class::Response,
        },
        Err(_) => Class::Drop,
    }
}

/// A deterministic, collision-free-per-node `SocketAddr` used PURELY as coap-rs's
/// in-process per-peer key — never bound, never on a wire. The first 16 node-id
/// bytes form the IPv6 address and the next 2 the port, so distinct nodes map to
/// distinct addresses (collisions only on a deliberate 144-bit prefix match, not
/// by accident). The client transport's `recv` return and the responder's
/// `address()` for a node MUST agree on this value, since coap-rs keys per-peer
/// reassembly state by it.
pub(crate) fn synthetic_addr(node: [u8; 32]) -> SocketAddr {
    let mut ip = [0u8; 16];
    ip.copy_from_slice(&node[..16]);
    let port = u16::from_be_bytes([node[16], node[17]]);
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ip), port, 0, 0))
}

/// coap-rs [`ClientTransport`](coap::client::ClientTransport) bound to one peer
/// node. Sends go straight to the link; receives drain this node's Hub inbox.
pub struct IrohClientTransport {
    node: [u8; 32],
    hub: Arc<Hub>,
    /// This node's response inbox (its sender is installed into the Hub by
    /// [`IrohCoapWireClient::client_for`] when it wins the pool slot). `Mutex`
    /// because coap-rs calls `recv` from its receive loop and the trait takes
    /// `&self`.
    inbox: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl IrohClientTransport {
    /// Build a transport bound to `node`, draining `inbox` for this node's response
    /// datagrams. Construction has no shared side effect: the matching sender is
    /// installed into the Hub by the pool winner, so a discarded losing build
    /// registers nothing.
    fn new(node: [u8; 32], hub: Arc<Hub>, inbox: mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            node,
            hub,
            inbox: Mutex::new(inbox),
        }
    }
}

#[async_trait]
impl coap::client::ClientTransport for IrohClientTransport {
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.hub.link().send(self.node, buf).await?;
        Ok(buf.len())
    }

    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<(usize, Option<SocketAddr>)> {
        match self.inbox.lock().await.recv().await {
            Some(bytes) => {
                // Mirror std UDP recv truncation: copy what fits. coap-rs sizes
                // its buffer at `u16::MAX`, so a CoAP datagram never exceeds it in
                // practice; the clamp is defensive.
                let n = bytes.len().min(buf.len());
                buf[..n].copy_from_slice(&bytes[..n]);
                Ok((n, Some(synthetic_addr(self.node))))
            }
            // Hub gone (link closed): end coap-rs's receive loop cleanly rather
            // than spinning. `BrokenPipe` is the natural "peer/link went away".
            None => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "datagram link closed",
            )),
        }
    }
}

/// coap-rs [`Listener`] over the Hub's requests feed. `listen` spawns a task that
/// turns each classified inbound request into a `(bytes, responder)` for the
/// coap-rs server loop.
struct IrohCoapListener {
    hub: Arc<Hub>,
    requests_rx: mpsc::UnboundedReceiver<NodeDatagram>,
}

#[async_trait]
impl Listener for IrohCoapListener {
    async fn listen(
        self: Box<Self>,
        sender: TransportRequestSender,
    ) -> std::io::Result<tokio::task::JoinHandle<std::io::Result<()>>> {
        let hub = self.hub;
        let mut requests_rx = self.requests_rx;
        Ok(tokio::spawn(async move {
            while let Some((node, bytes)) = requests_rx.recv().await {
                let responder = Arc::new(IrohResponder {
                    node,
                    link: hub.link(),
                    addr: synthetic_addr(node),
                });
                if sender.send((bytes, responder)).is_err() {
                    break; // server loop gone
                }
            }
            Ok(())
        }))
    }
}

/// coap-rs [`Responder`] that sends a response datagram back to the originating
/// node over the link. `address()` returns the same synthetic key coap-rs used to
/// track this peer's reassembly state.
struct IrohResponder {
    node: [u8; 32],
    link: Arc<dyn DatagramLink>,
    addr: SocketAddr,
}

#[async_trait]
impl Responder for IrohResponder {
    async fn respond(&self, response: Vec<u8>) {
        if let Err(e) = self.link.send(self.node, &response).await {
            tracing::debug!("datagram: responder send failed: {e}");
        }
    }
    fn address(&self) -> SocketAddr {
        self.addr
    }
}

/// Egress wire client over a [`DatagramLink`]. The slim iroh twin of
/// [`super::CoapWireClient`]: it pools one `CoAPClient<IrohClientTransport>` per
/// peer node and delegates the actual exchange to the shared [`exchange`] helper,
/// so token tagging, CON/Q-Block selection, the response body cap and the
/// whole-exchange timeout are NOT duplicated. The peer node is parsed from
/// `WireRequest.dest` as a 64-char hex node id.
pub struct IrohCoapWireClient {
    hub: Arc<Hub>,
    block1_size: Option<usize>,
    qblock: Option<coap::qblock::QBlockConfig>,
    request_timeout: Duration,
    max_body_bytes: usize,
    /// Per-node client pool. Each `CoAPClient` owns one inbox registration and
    /// spawns one coap-rs receive task; pooling reuses it across requests to a
    /// peer. Sharing is safe because each request carries a unique token.
    pool: Mutex<HashMap<[u8; 32], Arc<CoAPClient<IrohClientTransport>>>>,
    token_counter: AtomicU32,
}

impl IrohCoapWireClient {
    /// CON-mode client. `block1_size` caps the per-request Block1 payload (`None`
    /// = coap-rs's 1024 B default).
    pub fn new(hub: Arc<Hub>, block1_size: Option<usize>) -> Self {
        Self {
            hub,
            block1_size,
            qblock: None,
            request_timeout: crate::REQUEST_TIMEOUT,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            pool: Mutex::new(HashMap::new()),
            token_counter: AtomicU32::new(random_token_seed()),
        }
    }

    /// Q-Block (RFC 9177) NON-mode client. Grows `request_timeout` to cover the
    /// recovery linger exactly as [`super::CoapWireClient::with_qblock`] does, so
    /// a long tuning is not killed mid-recovery.
    pub fn with_qblock(
        hub: Arc<Hub>,
        block1_size: Option<usize>,
        qblock: coap::qblock::QBlockConfig,
    ) -> Self {
        let rounds = qblock.non_max_retransmit.saturating_add(2);
        let linger = qblock.non_receive_timeout.saturating_mul(rounds);
        let request_timeout = crate::REQUEST_TIMEOUT.max(linger.saturating_mul(2));
        Self {
            qblock: Some(qblock),
            request_timeout,
            ..Self::new(hub, block1_size)
        }
    }

    fn next_token(&self) -> Vec<u8> {
        self.token_counter
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes()
            .to_vec()
    }

    /// The pooled client for `node`, building one (registering its inbox +
    /// spawning its receive task) on first use. The pool lock is not held across
    /// the `await` that builds the client; a race on the same node discards the
    /// loser.
    async fn client_for(&self, node: [u8; 32]) -> Arc<CoAPClient<IrohClientTransport>> {
        if let Some(client) = self.pool.lock().await.get(&node).cloned() {
            return client;
        }
        // Build the (cheap, I/O-free) transport + client outside the pool lock; its
        // inbox sender (`tx`) is installed into the Hub only if we win the slot
        // below, so a losing concurrent first-send registers nothing and is just
        // dropped (its `rx` dies with it).
        let (tx, rx) = mpsc::unbounded_channel();
        let mut client =
            CoAPClient::from_transport(IrohClientTransport::new(node, self.hub.clone(), rx));
        if let Some(size) = self.block1_size {
            client.set_block1_size(size);
        }
        if let Some(cfg) = &self.qblock {
            client.set_qblock_config(cfg.clone());
            // Bound Q-Block2 response reassembly at the framing layer (same OOM
            // rationale as the UDP client's `client_for`).
            client.set_max_total_message_size(Some(self.max_body_bytes));
        }
        let client = Arc::new(client);
        // Claim the pool slot and install the matching inbox atomically: the
        // install is a synchronous map op done while holding the pool lock, so no
        // other builder can interleave between the slot check and the
        // registration. This guarantees the pooled client and `Hub.clients[node]`
        // are the SAME transport even under concurrent first-sends to one peer —
        // otherwise the drain task could route responses to a discarded
        // transport's inbox and every request to that peer would time out until an
        // error rebuilt the client.
        let mut pool = self.pool.lock().await;
        if let Some(existing) = pool.get(&node) {
            return existing.clone();
        }
        self.hub.install_client(node, tx);
        pool.insert(node, client.clone());
        client
    }

    /// Drop the pooled client for `node` so the next send rebuilds it (re-registers
    /// the inbox, ends the stale receive task).
    async fn evict(&self, node: [u8; 32]) {
        self.pool.lock().await.remove(&node);
    }
}

/// Parse `dest` as a 64-char (32-byte) hex node id (either case).
fn parse_node(dest: &str) -> Result<[u8; 32], WireError> {
    if dest.len() != 64 {
        return Err(WireError::Transport(format!(
            "node id must be 64 hex chars, got {}",
            dest.len()
        )));
    }
    let bytes = dest.as_bytes();
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_nibble(bytes[i * 2])?;
        let lo = hex_nibble(bytes[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8, WireError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(WireError::Transport("node id has non-hex char".to_owned())),
    }
}

#[async_trait]
impl WireClient for IrohCoapWireClient {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
        // Reject a zero Block1 size before dialing — `chunks(0)` panics in coap-rs
        // (same guard as the UDP client).
        if self.block1_size == Some(0) {
            return Err(WireError::Transport(
                "coap block1_size must be non-zero".to_owned(),
            ));
        }
        let node = parse_node(&req.dest)?;
        let client = self.client_for(node).await;
        let token = self.next_token();
        let result = exchange(
            &client,
            &req,
            token,
            self.qblock.is_some(),
            self.max_body_bytes,
            self.request_timeout,
        )
        .await;
        // On failure drop the pooled client: ends the stale receive task and
        // forces a fresh inbox registration on the next send.
        if result.is_err() {
            self.evict(node).await;
        }
        result
    }
}

/// Ingress wire server over a [`DatagramLink`]. The iroh twin of
/// [`super::CoapWireServer`]: it stands up a coap-rs `Server` from an
/// [`IrohCoapListener`] and reuses [`CoapDispatch`] for per-request handling, so
/// the 413 over-cap guard, the no-response-slot skip and the response writing are
/// NOT duplicated.
pub struct IrohCoapWireServer {
    hub: Arc<Hub>,
    max_message_size: Option<usize>,
    max_body_bytes: usize,
    qblock: Option<coap::qblock::QBlockConfig>,
}

impl IrohCoapWireServer {
    pub fn new(hub: Arc<Hub>, max_message_size: Option<usize>) -> Self {
        Self {
            hub,
            max_message_size,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            qblock: None,
        }
    }

    /// Q-Block (RFC 9177) NON-mode server.
    pub fn with_qblock(hub: Arc<Hub>, qblock: coap::qblock::QBlockConfig) -> Self {
        Self {
            qblock: Some(qblock),
            ..Self::new(hub, None)
        }
    }

    /// Build the coap-rs `Server` from the Hub's requests receiver, applying the
    /// same size/Q-Block settings `CoapWireServer::serve` applies to the UDP path.
    async fn build_server(&self) -> Result<Server, WireError> {
        let requests_rx = self.hub.take_requests().await.ok_or_else(|| {
            WireError::Serve("datagram hub already has an ingress listener".to_owned())
        })?;
        let listener: Box<dyn Listener> = Box::new(IrohCoapListener {
            hub: self.hub.clone(),
            requests_rx,
        });
        let mut server = match self.max_message_size {
            Some(max_total_message_size) => Server::from_listeners_with_config(
                vec![listener],
                BlockHandlerConfig {
                    max_total_message_size,
                    ..Default::default()
                },
            ),
            None => Server::from_listeners(vec![listener]),
        };
        if let Some(cfg) = self.qblock.clone() {
            server.set_qblock_config(cfg);
            server.set_qblock_max_body_len(self.max_body_bytes);
            server.set_qblock_max_transfers(MAX_QBLOCK_INFLIGHT_TRANSFERS);
        }
        Ok(server)
    }
}

#[async_trait]
impl WireServer for IrohCoapWireServer {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError> {
        let server = self.build_server().await?;
        let dispatch = Arc::new(CoapDispatch {
            handler,
            max_body_bytes: self.max_body_bytes,
        });
        // Same shutdown discipline as the UDP server: race `run` against the
        // token; dropping the run future aborts the listener task (AbortOnDrop in
        // the fork's `Server::run`).
        tokio::select! {
            r = server.run(move |request| {
                let dispatch = dispatch.clone();
                async move { dispatch.handle(request).await }
            }) => r.map_err(|e| WireError::Serve(format!("coap serve: {e}"))),
            _ = shutdown.cancelled() => Ok(()),
        }
    }
}

#[cfg(test)]
mod mock {
    use super::*;

    /// One end of an in-memory [`DatagramLink`] pair. `send(dst, bytes)` enqueues
    /// onto the peer's inbound channel tagged with *this* end's node id, so the
    /// peer's `recv` yields `(our_node, bytes)` — exactly the link contract
    /// (recv's source node is cryptographically authenticated; here it's just the
    /// sender's known id).
    pub struct MockLink {
        our_node: [u8; 32],
        peer_node: [u8; 32],
        to_peer: mpsc::UnboundedSender<NodeDatagram>,
        inbound: Mutex<mpsc::UnboundedReceiver<NodeDatagram>>,
    }

    impl MockLink {
        /// Build a connected pair `(a, b)` with the given node ids.
        pub fn pair(node_a: [u8; 32], node_b: [u8; 32]) -> (Arc<MockLink>, Arc<MockLink>) {
            let (a_to_b, b_in) = mpsc::unbounded_channel();
            let (b_to_a, a_in) = mpsc::unbounded_channel();
            let a = Arc::new(MockLink {
                our_node: node_a,
                peer_node: node_b,
                to_peer: a_to_b,
                inbound: Mutex::new(a_in),
            });
            let b = Arc::new(MockLink {
                our_node: node_b,
                peer_node: node_a,
                to_peer: b_to_a,
                inbound: Mutex::new(b_in),
            });
            (a, b)
        }
    }

    #[async_trait]
    impl DatagramLink for MockLink {
        async fn send(&self, dst: [u8; 32], datagram: &[u8]) -> std::io::Result<()> {
            // The mock is a one-to-one link, so the only valid destination is the
            // configured peer; reject anything else so a mis-keyed test fails loud.
            if dst != self.peer_node {
                return Err(std::io::Error::other("mock link: unknown destination"));
            }
            self.to_peer
                .send((self.our_node, datagram.to_vec()))
                .map_err(|_| std::io::Error::other("mock link: peer gone"))
        }

        async fn recv(&self) -> Option<([u8; 32], Vec<u8>)> {
            self.inbound.lock().await.recv().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::MockLink;
    use super::*;
    use axum::http::Method;

    const NODE_A: [u8; 32] = [0xAA; 32];
    const NODE_B: [u8; 32] = [0xBB; 32];

    fn hex_node(node: [u8; 32]) -> String {
        node.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Echoes the decoded path (as a forwardable header) + body, mirroring the UDP
    /// `server_tests::EchoHandler`.
    struct EchoHandler;

    #[async_trait]
    impl WireHandler for EchoHandler {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![("x-matrix-seen-path".to_owned(), req.path.into_bytes())],
                body: req.body,
            }
        }
    }

    type ServeHandle = tokio::task::JoinHandle<Result<(), WireError>>;

    /// Stand up B's hub+server (echo) and A's hub+client over a mock link pair,
    /// returning A's client and B's shutdown token + join handle.
    async fn rig(
        client_qblock: Option<coap::qblock::QBlockConfig>,
        server_qblock: Option<coap::qblock::QBlockConfig>,
        block1_size: Option<usize>,
        max_message_size: Option<usize>,
    ) -> (IrohCoapWireClient, CancellationToken, ServeHandle) {
        let (a_link, b_link) = MockLink::pair(NODE_A, NODE_B);
        let hub_a = Hub::new(a_link);
        let hub_b = Hub::new(b_link);

        let server = match server_qblock {
            Some(cfg) => IrohCoapWireServer::with_qblock(hub_b, cfg),
            None => IrohCoapWireServer::new(hub_b, max_message_size),
        };
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { server.serve(Arc::new(EchoHandler), server_token).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = match client_qblock {
            Some(cfg) => IrohCoapWireClient::with_qblock(hub_a, block1_size, cfg),
            None => IrohCoapWireClient::new(hub_a, block1_size),
        };
        (client, token, handle)
    }

    async fn shutdown(token: CancellationToken, handle: ServeHandle) {
        token.cancel();
        let joined = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server did not wind down cleanly: {joined:?}"
        );
    }

    // synthetic_addr must be deterministic and distinct per node — it is coap-rs's
    // only per-peer reassembly key.
    #[test]
    fn synthetic_addr_is_deterministic_and_distinct() {
        assert_eq!(synthetic_addr(NODE_A), synthetic_addr(NODE_A));
        assert_ne!(synthetic_addr(NODE_A), synthetic_addr(NODE_B));
    }

    #[test]
    fn parse_node_round_trips_hex() {
        assert_eq!(parse_node(&hex_node(NODE_A)).unwrap(), NODE_A);
        assert!(parse_node("xyz").is_err());
        assert!(parse_node(&"zz".repeat(32)).is_err());
    }

    // A small CON request/response must round-trip over the mock link: status,
    // body, and a path-derived header all survive the classify→server→client path.
    #[tokio::test]
    async fn small_con_round_trips() {
        let (client, token, handle) = rig(None, None, None, None).await;
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![1, 2, 3],
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![1, 2, 3]);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "x-matrix-seen-path" && v == b"/_matrix/federation/v1/send/txn1"),
            "path header missing: {:?}",
            resp.headers
        );
        shutdown(token, handle).await;
    }

    // A body well over one CoAP block must round-trip via Block1 (request) +
    // Block2 (response) blockwise — the load-bearing send_join shape.
    #[tokio::test]
    async fn large_body_round_trips_via_blockwise() {
        let (client, token, handle) = rig(None, None, Some(64), Some(256)).await;
        // 2 KiB body, 64 B Block1, 256 B server budget -> many blocks each way.
        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
                headers: vec![],
                body: req_body.clone(),
            })
            .await
            .expect("blockwise send");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, req_body, "blockwise body corrupted");
        shutdown(token, handle).await;
    }

    // A Q-Block NON-mode request must round-trip over the mock link.
    #[tokio::test]
    async fn qblock_round_trips() {
        let qcfg = coap::qblock::QBlockConfig::default();
        let (client, token, handle) = rig(Some(qcfg.clone()), Some(qcfg), Some(128), None).await;
        let req_body: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txnq".to_owned(),
                headers: vec![],
                body: req_body.clone(),
            })
            .await
            .expect("qblock send");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, req_body, "qblock body corrupted");
        shutdown(token, handle).await;
    }

    // The classifier must route a response back to the originating client inbox
    // and NOT onto the server's requests feed: a misrouted response would never
    // reach the client's `send`, surfacing as a timeout instead of this echo. A
    // successful round-trip with a short timeout proves correct routing in both
    // directions (request reached the server; its response reached this client).
    #[tokio::test]
    async fn classifier_routes_response_to_client_not_server() {
        let (a_link, b_link) = MockLink::pair(NODE_A, NODE_B);
        let hub_a = Hub::new(a_link);
        let hub_b = Hub::new(b_link);
        let server = IrohCoapWireServer::new(hub_b, None);
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { server.serve(Arc::new(EchoHandler), server_token).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = IrohCoapWireClient::new(hub_a, None);
        client.request_timeout = Duration::from_secs(2);
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::GET,
                path: "/_matrix/federation/v1/event/$e".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await
            .expect("response must reach the originating client, not the server loop");
        assert_eq!(resp.status, 200);
        shutdown(token, handle).await;
    }

    // A second ingress server on the same hub must be refused: the requests
    // receiver is taken exactly once.
    #[tokio::test]
    async fn second_ingress_on_hub_is_refused() {
        let (_a, b_link) = MockLink::pair(NODE_A, NODE_B);
        let hub_b = Hub::new(b_link);
        let first = IrohCoapWireServer::new(hub_b.clone(), None);
        assert!(first.build_server().await.is_ok());
        let second = IrohCoapWireServer::new(hub_b, None);
        assert!(
            second.build_server().await.is_err(),
            "a hub must drive at most one ingress listener"
        );
    }

    // Regression for the concurrent first-send race: many overlapping *first*
    // sends to one node hit the cold pool together and all race through
    // `client_for`'s build+claim. The fix installs the inbox sender only for the
    // pool winner, so every request reaches the one pooled client and gets its own
    // echo. Pre-fix, a losing build could win `Hub.clients` while the pool kept a
    // different transport, so responses routed to a discarded inbox and these
    // requests timed out. Distinct per-task bodies also catch any cross-talk. The
    // short request timeout makes a regression fail fast instead of hanging 60 s.
    #[tokio::test]
    async fn concurrent_first_sends_to_one_node_all_succeed() {
        let (mut client, token, handle) = rig(None, None, None, None).await;
        client.request_timeout = Duration::from_secs(5);
        let client = Arc::new(client);
        let mut tasks = Vec::new();
        for i in 0u8..16 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                let body = vec![i; 32 + i as usize];
                let resp = client
                    .send(WireRequest {
                        dest: hex_node(NODE_B),
                        method: Method::PUT,
                        path: format!("/_matrix/federation/v1/send/txn{i}"),
                        headers: vec![],
                        body: body.clone(),
                    })
                    .await
                    .expect("concurrent first-send must not time out");
                (resp, body)
            }));
        }
        for t in tasks {
            let (resp, expected) = t.await.expect("task");
            assert_eq!(resp.status, 200);
            assert_eq!(
                resp.body, expected,
                "a concurrent first-send received the wrong echo"
            );
        }
        shutdown(token, handle).await;
    }

    /// Encode a CoAP datagram carrying `code` (default header otherwise), so
    /// `classify` can be exercised on real wire bytes.
    fn coap_bytes(code: MessageClass) -> Vec<u8> {
        let mut p = Packet::new();
        p.header.code = code;
        p.to_bytes().expect("encode coap packet")
    }

    // `classify` is the load-bearing demux predicate (the separation UDP got free
    // from ports): a request-coded datagram routes to the ingress server, every
    // other valid code to a client inbox. Test the pure fn directly rather than
    // only via the round-trip rig, which never feeds it a response/empty/garbage.
    #[test]
    fn classify_routes_by_coap_code_class() {
        use coap_lite::{RequestType, ResponseType};
        // Request codes → the server/listener side.
        assert!(matches!(
            classify(&coap_bytes(MessageClass::Request(RequestType::Put))),
            Class::Request
        ));
        assert!(matches!(
            classify(&coap_bytes(MessageClass::Request(RequestType::Get))),
            Class::Request
        ));
        // Response codes → a client inbox.
        assert!(matches!(
            classify(&coap_bytes(MessageClass::Response(ResponseType::Content))),
            Class::Response
        ));
        assert!(matches!(
            classify(&coap_bytes(MessageClass::Response(ResponseType::NotFound))),
            Class::Response
        ));
        // A bare Empty message (e.g. an ACK/RST) is not a request → client side.
        assert!(matches!(
            classify(&coap_bytes(MessageClass::Empty)),
            Class::Response
        ));
    }

    // An undecodable datagram must be DROPPED — never misclassified as a request
    // (which would let a peer inject garbage into the ingress server) nor as a
    // response (which would feed a malformed packet to a client exchange).
    #[test]
    fn classify_drops_undecodable_datagrams() {
        assert!(matches!(classify(b""), Class::Drop), "empty");
        // A CoAP header is 4 bytes; anything shorter fails to parse.
        assert!(matches!(classify(&[0x40]), Class::Drop), "1-byte");
        assert!(
            matches!(classify(&[0x40, 0x01, 0x00]), Class::Drop),
            "3-byte"
        );
    }
}
