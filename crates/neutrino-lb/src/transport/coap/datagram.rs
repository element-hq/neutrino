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
//! returns an informational `Option<SocketAddr>`. We have no real address, so the
//! [`Hub`] hands each 32-byte node a UNIQUE synthetic `SocketAddr` PURELY as that
//! in-process key ([`Hub::addr_for`]). It is never bound and never put on a wire;
//! the real routing is the 32-byte node id carried alongside.
//!
//! The mapping is a lossless **bijection** (a monotonic counter), not a hash of
//! the node bytes — because the *exact* source node must be recoverable from
//! `request.source` ([`Hub::node_for`]) for the origin↔node binding below. A
//! lossy projection would let a peer grind a key whose projection collides a
//! victim's and be resolved as the victim.
//!
//! ## Why an origin↔node binding
//!
//! The link cryptographically authenticates the source node, and a peer's
//! federation `server_name` IS that node's hex id. `federation::auth` trusts the
//! `X-Matrix origin` header *solely because the network layer authenticated the
//! peer* — so this layer MUST enforce that an authenticated peer asserts only its
//! own origin. [`Hub::origin_binding_violation`] rejects a request whose claimed
//! `origin` node id ≠ the authenticated source node (impersonation).

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
    /// Lossless node ↔ synthetic-`SocketAddr` bijection (see the module note).
    /// A `std::sync::Mutex` (every critical section is one or two map ops, never
    /// held across an await).
    addrs: std::sync::Mutex<AddrRegistry>,
}

/// The node ↔ synthetic-`SocketAddr` bijection. A monotonic counter mints a fresh
/// address per node, so distinct nodes never alias and the exact node is
/// recoverable from any address it minted — the soundness requirement for the
/// origin↔node binding.
#[derive(Default)]
struct AddrRegistry {
    next: u128,
    node_to_addr: HashMap<[u8; 32], SocketAddr>,
    addr_to_node: HashMap<SocketAddr, [u8; 32]>,
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
            addrs: std::sync::Mutex::new(AddrRegistry::default()),
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

    /// The stable synthetic `SocketAddr` for `node`, minting one on first use.
    /// Purely an in-process coap-rs key — never bound, never on a wire. Distinct
    /// nodes get distinct addresses (a monotonic counter), so the mapping is a
    /// bijection and [`Hub::node_for`] recovers the exact node.
    fn addr_for(&self, node: [u8; 32]) -> SocketAddr {
        let mut reg = self.addrs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(addr) = reg.node_to_addr.get(&node) {
            return *addr;
        }
        let id = reg.next;
        reg.next = reg.next.wrapping_add(1);
        let addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(id), 0, 0, 0));
        reg.node_to_addr.insert(node, addr);
        reg.addr_to_node.insert(addr, node);
        addr
    }

    /// Recover the exact node a synthetic `SocketAddr` was minted for, or `None`
    /// if it was not minted by this hub.
    fn node_for(&self, addr: SocketAddr) -> Option<[u8; 32]> {
        self.addrs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .addr_to_node
            .get(&addr)
            .copied()
    }

    /// SECURITY — the trust-boundary binding `federation::auth` defers to. The
    /// link authenticates the source node, and a peer's federation `server_name`
    /// IS that node's 64-hex id, so a peer may assert only its OWN origin. Returns
    /// `true` if the request MUST be rejected: a forwarded `X-Matrix origin` whose
    /// node id ≠ the authenticated source `node` (impersonation), or a malformed
    /// origin / unrecognised source over an authenticated link. A request with no
    /// `authorization` header claims no server identity, so it is left for the
    /// upstream's own auth gate (a 401 on protected routes) rather than
    /// over-rejected here (e.g. an unauthenticated `/version`).
    pub(super) fn origin_binding_violation(
        &self,
        source: Option<SocketAddr>,
        headers: &[(String, Vec<u8>)],
    ) -> bool {
        let Some((_, value)) = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        else {
            return false; // no claimed origin — defer to the upstream auth gate
        };
        // A server identity is now being asserted; it MUST match the authenticated
        // source node. An unknown source or unparseable origin is a hard reject.
        let Some(node) = source.and_then(|addr| self.node_for(addr)) else {
            return true;
        };
        // The origin is a `server_name` == 64-hex node id; decode and compare bytes
        // (case-insensitive via `parse_node`) rather than re-encoding the node.
        match std::str::from_utf8(value)
            .ok()
            .and_then(xmatrix_origin)
            .and_then(|origin| parse_node(origin).ok())
        {
            Some(claimed) => claimed != node,
            None => true,
        }
    }
}

/// Extract the unquoted `origin` auth-param from an `X-Matrix origin="…",…`
/// Authorization value, for the transport-layer identity binding
/// ([`Hub::origin_binding_violation`]). `None` if the scheme prefix or `origin`
/// is absent. Mirrors `neutrino_http::federation::auth`'s parse, kept local so
/// this Matrix-agnostic transport needn't depend on the http crate; it extracts
/// the bytes only — the http layer still owns the real auth policy.
fn xmatrix_origin(value: &str) -> Option<&str> {
    let params = value.strip_prefix("X-Matrix ")?;
    for part in params.split(',') {
        let Some((key, val)) = part.split_once('=') else {
            continue;
        };
        if key.trim() == "origin" {
            return Some(val.trim().trim_matches('"'));
        }
    }
    None
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
                Ok((n, Some(self.hub.addr_for(self.node))))
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
                    addr: hub.addr_for(node),
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
        // Per-request federation egress trace: the destination node, the request,
        // and the body size (the load-bearing field — a body over the Q-Block
        // block size must fit `block size + option overhead` within coap-lite's
        // 1280 message cap, which is why `block1_size` is kept small).
        tracing::debug!(dest = %req.dest, method = %req.method, path = %req.path, body_len = req.body.len(), qblock = self.qblock.is_some(), "datagram wire: dispatching federation request");
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
        match &result {
            Ok(r) => {
                tracing::debug!(dest = %req.dest, status = r.status, "datagram wire: federation request completed")
            }
            Err(e) => {
                tracing::warn!(dest = %req.dest, error = %e, "datagram wire: federation request failed")
            }
        }
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
            // The datagram ingress is the authenticated trust boundary: bind every
            // request's claimed origin to its source node via the hub.
            node_binding: Some(self.hub.clone()),
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
        rig_with_links(
            a_link,
            b_link,
            client_qblock,
            server_qblock,
            block1_size,
            max_message_size,
        )
        .await
    }

    /// [`rig`] over caller-supplied links, so a test can interpose a tap (e.g.
    /// [`PcapCaptureLink`](super::capture::PcapCaptureLink)) on A's side.
    async fn rig_with_links(
        a_link: Arc<dyn DatagramLink>,
        b_link: Arc<dyn DatagramLink>,
        client_qblock: Option<coap::qblock::QBlockConfig>,
        server_qblock: Option<coap::qblock::QBlockConfig>,
        block1_size: Option<usize>,
        max_message_size: Option<usize>,
    ) -> (IrohCoapWireClient, CancellationToken, ServeHandle) {
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

    /// A Q-Block config shaped the way production
    /// (`QBlockTuning::to_qblock_config`) builds it: peer support is assumed at
    /// the wire's block size (both ends run this stack; requests carry no
    /// per-request Q-Block2 opt-in), `None` = coap-rs's 1024 B default.
    fn qblock_cfg(block1_size: Option<usize>) -> coap::qblock::QBlockConfig {
        coap::qblock::QBlockConfig {
            assume_peer_block_size: Some(block1_size.unwrap_or(1024)),
            ..Default::default()
        }
    }

    async fn shutdown(token: CancellationToken, handle: ServeHandle) {
        token.cancel();
        let joined = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server did not wind down cleanly: {joined:?}"
        );
    }

    // The node↔addr registry must be a lossless bijection: stable per node,
    // distinct across nodes, and the exact node recoverable from any minted addr.
    // This is the soundness requirement for the origin↔node binding — a lossy map
    // would let a peer be resolved as a different node.
    #[tokio::test]
    async fn addr_registry_is_a_lossless_bijection() {
        let (link, _peer) = MockLink::pair(NODE_A, NODE_B);
        let hub = Hub::new(link);
        let addr_a = hub.addr_for(NODE_A);
        let addr_b = hub.addr_for(NODE_B);
        // Stable per node, distinct across nodes.
        assert_eq!(addr_a, hub.addr_for(NODE_A));
        assert_ne!(addr_a, addr_b);
        // The exact node is recoverable; an addr this hub never minted resolves
        // to nothing.
        assert_eq!(hub.node_for(addr_a), Some(NODE_A));
        assert_eq!(hub.node_for(addr_b), Some(NODE_B));
        let unminted = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(u128::MAX), 9999, 0, 0));
        assert_eq!(hub.node_for(unminted), None);
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
        let qcfg = qblock_cfg(Some(128));
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

    // Reproduces the on-device invite stall: a long federation path + a header,
    // 1141-byte body, with the DEFAULT 1024 block. Each Q-Block PDU also carries
    // the request options (the long `/invite/!room.../$event...` path), and a
    // 1024 payload + those options exceeds coap-lite's 1280 MAX_SIZE, so
    // `build_block`'s `to_bytes()` fails on the first block and the send stalls.
    // With a short request timeout this surfaces as an error rather than a 60s
    // hang. This is exactly what `build_lb_config` avoids by setting block1_size
    // to 512.
    // The per-block CoAP PDU = the request's options (path + forwarded headers,
    // repeated in EVERY block) + the block payload. coap-lite caps a serialized
    // message at `Packet::MAX_SIZE` (1280 B). With the 1024 default block and a
    // realistic federation request (long `/invite/!room/$event` path + a few
    // hundred bytes of forwarded headers), 1024 + options exceeds 1280, so
    // `build_block`'s `to_bytes()` fails on the first block. coap-rs's `drive_send`
    // drops that error, so nothing is sent and the exchange hangs to its timeout —
    // exactly the on-device invite stall. `INVITE_PATH`/`HEADERS` model that.
    const INVITE_PATH: &str = "/_matrix/federation/v2/invite/!88A_2bLzwjMwxHxn5qqlhVZziTR6FgpPA2Id9z181ho/$XaldBICd02USS2D4qfkuKS0Zmw86YYlucgi_yB7CpkI";
    fn invite_headers() -> Vec<(String, Vec<u8>)> {
        // The federation `Authorization: X-Matrix` header is the heavy, always-
        // present FORWARDABLE option (origin + destination node ids + key + sig);
        // `content-type` etc. are dropped by the egress allowlist, but this is
        // forwarded into every block's PDU. ~280 B — enough that a 1024 block's PDU
        // exceeds coap-lite's 1280 cap, but a 512 block stays under it.
        let auth = format!(
            "X-Matrix origin=\"{}\",destination=\"{}\",key=\"ed25519:1\",sig=\"{}\"",
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(86),
        );
        vec![
            ("content-type".to_owned(), b"application/cbor".to_vec()),
            ("authorization".to_owned(), auth.into_bytes()),
        ]
    }

    #[tokio::test]
    async fn qblock_long_federation_request_overflows_default_1024_block() {
        let qcfg = qblock_cfg(None);
        let (mut client, token, handle) = rig(Some(qcfg.clone()), Some(qcfg), None, None).await;
        client.request_timeout = std::time::Duration::from_secs(3);
        let body: Vec<u8> = (0..1141).map(|i| (i % 251) as u8).collect();
        let result = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: INVITE_PATH.to_owned(),
                headers: invite_headers(),
                body,
            })
            .await;
        assert!(
            result.is_err(),
            "default 1024 block + a real federation request must overflow coap-lite's 1280 and stall, got {result:?}"
        );
        shutdown(token, handle).await;
    }

    // The fix: with block1_size=512 (what `build_lb_config` now sets), the same
    // request round-trips — 512 payload + options stays under 1280.
    #[tokio::test]
    async fn qblock_long_federation_request_round_trips_with_512_block() {
        let qcfg = qblock_cfg(Some(512));
        let (client, token, handle) = rig(Some(qcfg.clone()), Some(qcfg), Some(512), None).await;
        let body: Vec<u8> = (0..1141).map(|i| (i % 251) as u8).collect();
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: INVITE_PATH.to_owned(),
                headers: invite_headers(),
                body: body.clone(),
            })
            .await
            .expect("512-block long federation request must round-trip");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, body, "body corrupted");
        shutdown(token, handle).await;
    }

    /// UDP payloads of a classic `LINKTYPE_RAW` pcap as written by the capture
    /// tap (24 B global header, 16 B record headers, 20 B IPv4 + 8 B UDP).
    fn pcap_udp_payloads(bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut payloads = Vec::new();
        let mut off = 24;
        while off + 16 <= bytes.len() {
            let incl = u32::from_le_bytes(bytes[off + 8..off + 12].try_into().unwrap()) as usize;
            off += 16;
            assert!(incl >= 28, "frame shorter than IPv4+UDP headers");
            payloads.push(bytes[off + 28..off + incl].to_vec());
            off += incl;
        }
        payloads
    }

    /// (code byte, option numbers) of a CoAP message.
    fn coap_code_and_options(payload: &[u8]) -> (u8, Vec<u16>) {
        let tkl = (payload[0] & 0x0f) as usize;
        let code = payload[1];
        let mut i = 4 + tkl;
        let mut num = 0u16;
        let mut opts = Vec::new();
        while i < payload.len() && payload[i] != 0xFF {
            let mut delta = (payload[i] >> 4) as u16;
            let mut len = (payload[i] & 0x0f) as usize;
            i += 1;
            if delta == 13 {
                delta = payload[i] as u16 + 13;
                i += 1;
            } else if delta == 14 {
                delta = u16::from_be_bytes([payload[i], payload[i + 1]]) + 269;
                i += 2;
            }
            if len == 13 {
                len = payload[i] as usize + 13;
                i += 1;
            } else if len == 14 {
                len = u16::from_be_bytes([payload[i], payload[i + 1]]) as usize + 269;
                i += 2;
            }
            num += delta;
            opts.push(num);
            i += len;
        }
        (code, opts)
    }

    // The 1a wire pin (Wireshark CBOR reassembly): request PDUs must NOT carry
    // Q-Block2 (option 31). Wireshark keeps one block-state slot per message
    // and dissects options in ascending number order, so a request-side
    // Q-Block2 (31 > Q-Block1's 19) clobbered the real Q-Block1 state and left
    // multi-block CBOR request bodies undecodable ("Malformed packet: CBOR").
    // Asserted on the actual datagrams via the pcap tap, over a full Q-Block
    // round-trip; the response leg must still stream Q-Block2 (via
    // `assume_peer_block_size`, not a per-request opt-in).
    #[tokio::test]
    async fn qblock_requests_carry_no_qblock2_on_the_wire() {
        use super::super::capture::{CaptureControl, PcapCaptureLink};
        const Q_BLOCK1: u16 = 19;
        const Q_BLOCK2: u16 = 31;

        let (a_link, b_link) = MockLink::pair(NODE_A, NODE_B);
        let control = CaptureControl::new();
        let a_link = PcapCaptureLink::wrap(a_link, control.clone());
        let path = std::env::temp_dir().join(format!(
            "neutrino-qblock2-wire-pin-{}.pcap",
            std::process::id()
        ));
        control.start(path.to_str().unwrap()).unwrap();

        let qcfg = qblock_cfg(Some(128));
        let (client, token, handle) = rig_with_links(
            a_link,
            b_link,
            Some(qcfg.clone()),
            Some(qcfg),
            Some(128),
            None,
        )
        .await;
        let req_body: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txnw".to_owned(),
                headers: vec![],
                body: req_body.clone(),
            })
            .await
            .expect("qblock send");
        assert_eq!(resp.body, req_body);
        shutdown(token, handle).await;
        control.stop();

        let (mut q1_requests, mut q2_requests, mut q2_responses) = (0, 0, 0);
        for payload in pcap_udp_payloads(&std::fs::read(&path).unwrap()) {
            let (code, opts) = coap_code_and_options(&payload);
            match code >> 5 {
                0 if code != 0 => {
                    q1_requests += usize::from(opts.contains(&Q_BLOCK1));
                    q2_requests += usize::from(opts.contains(&Q_BLOCK2));
                }
                2..=5 => q2_responses += usize::from(opts.contains(&Q_BLOCK2)),
                _ => {}
            }
        }
        std::fs::remove_file(&path).ok();
        assert!(
            q1_requests > 1,
            "multi-block Q-Block1 request not exercised"
        );
        assert_eq!(
            q2_requests, 0,
            "a request carried Q-Block2 — this breaks Wireshark's Q-Block reassembly of CBOR request bodies"
        );
        assert!(
            q2_responses > 1,
            "response was not streamed as multi-block Q-Block2"
        );
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

    /// An `Authorization: X-Matrix origin="<server>"` header value.
    fn xmatrix_auth(origin: &str) -> (String, Vec<u8>) {
        (
            "authorization".to_owned(),
            format!("X-Matrix origin=\"{origin}\",destination=\"x\"").into_bytes(),
        )
    }

    // SECURITY: the rig authenticates the inbound peer as NODE_A (B's link yields
    // that source). A request whose `X-Matrix origin` is NODE_A's own id is the
    // peer asserting its own identity, so it must be accepted and echoed.
    #[tokio::test]
    async fn authenticated_origin_matching_source_node_is_accepted() {
        let (client, token, handle) = rig(None, None, None, None).await;
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![xmatrix_auth(&hex_node(NODE_A))],
                body: vec![1, 2, 3],
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 200, "own-origin request must be accepted");
        assert_eq!(resp.body, vec![1, 2, 3]);
        shutdown(token, handle).await;
    }

    // SECURITY (SEC1): the authenticated peer is NODE_A, but the request claims to
    // be NODE_B. That is impersonation over an authenticated link and MUST be
    // rejected with 401 before the handler runs — the EchoHandler can only ever
    // return 200, so a 401 proves the binding refused it pre-dispatch.
    #[tokio::test]
    async fn spoofed_origin_is_rejected_with_401() {
        let (client, token, handle) = rig(None, None, None, None).await;
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                // Claims NODE_B while the link authenticated the sender as NODE_A.
                headers: vec![xmatrix_auth(&hex_node(NODE_B))],
                body: vec![1, 2, 3],
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 401, "a foreign origin must be rejected");
        assert!(resp.body.is_empty(), "rejected request must not be echoed");
        shutdown(token, handle).await;
    }

    // A malformed origin (not a 64-hex node id) on an authenticated link cannot be
    // bound to the source node, so it is rejected rather than trusted.
    #[tokio::test]
    async fn malformed_origin_is_rejected_with_401() {
        let (client, token, handle) = rig(None, None, None, None).await;
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::GET,
                path: "/_matrix/federation/v1/event/$e".to_owned(),
                headers: vec![xmatrix_auth("not-a-node-id.example")],
                body: vec![],
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 401, "an unbindable origin must be rejected");
        shutdown(token, handle).await;
    }

    // A request with NO authorization header asserts no server identity, so the
    // transport binding must NOT reject it — the upstream homeserver's own auth
    // gate decides (a protected route 401s there). Proves we don't over-reject
    // unauthenticated routes like `/version`.
    #[tokio::test]
    async fn missing_authorization_is_passed_through() {
        let (client, token, handle) = rig(None, None, None, None).await;
        let resp = client
            .send(WireRequest {
                dest: hex_node(NODE_B),
                method: Method::GET,
                path: "/_matrix/federation/v1/version".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 200, "no-auth request must reach the handler");
        shutdown(token, handle).await;
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
