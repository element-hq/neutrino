//! v2 CoAP/UDP wire transport. `CoapWireClient` (egress→peer) and
//! `CoapWireServer` (peer→ingress), a sibling of `transport::http`, selected in
//! `crate::serve`. The codec stays opaque: this transport carries the CBOR body
//! verbatim and never inspects it.
//!
//! OOM note: this transport enforces `MAX_WIRE_BODY_BYTES` on the *assembled*
//! body — the ingress refuses an over-cap request with 413 before it reaches the
//! handler/transcode (`CoapDispatch::handle`), and the egress rejects an over-cap
//! peer response (`CoapWireClient::send`). That matches the HTTP transport's
//! handler-facing contract and bounds the JSON↔CBOR transcode amplification + the
//! loopback forward for a legitimately large body (e.g. a big room's `send_join`
//! state), which is the realistic case under Neutrino's trusted-network
//! assumption. What it does *not* bound is the buffer coap-lite 0.13 accumulates
//! *during* blockwise reassembly: `max_total_message_size` caps only the
//! negotiated per-block size, not the running total across Block1 chunks (verified
//! in `coap_lite::block_handler`), so a peer streaming unbounded chunks can still
//! grow that internal buffer before our post-reassembly check fires. A true
//! reassembly-time cap is a follow-up (see `PLAN.md`); it needs coap-lite to bound
//! the block accumulator, which the current API does not allow from the outside.

pub mod capture;
pub mod datagram;
mod message;
mod paths;

/// Exact HTTP status, carried as 2 big-endian bytes (CoAP response codes are not
/// 1:1 with HTTP, and federation needs the precise code).
pub(crate) const OPT_HTTP_STATUS: u16 = 2048;
/// One forwarded header per occurrence: `name` + 0x00 + `value`.
pub(crate) const OPT_FWD_HEADER: u16 = 2050;
/// The X-Matrix federation credential, compacted to bare `origin,destination`
/// (`,` is not a valid server-name char). CoAP re-sends every option in every
/// block, so the static `authorization` + `X-Matrix origin="…",destination="…"`
/// framing is stripped on the wire and re-synthesised on ingress (see
/// `message`).
pub(crate) const OPT_X_MATRIX_AUTH: u16 = 2052;
pub(crate) use crate::transport::CBOR_CONTENT_FORMAT;

/// Cap on concurrent in-flight Q-Block1 (inbound request) reassemblies on the
/// public federation port. Each holds up to `max_body_bytes`, so the worst-case
/// Q-Block1 memory is `MAX_QBLOCK_INFLIGHT_TRANSFERS * MAX_WIRE_BODY_BYTES`. A
/// trusted low-bandwidth mesh has few concurrent inbound transfers; this is a
/// DoS backstop against a peer opening many partial transfers, not a throughput
/// limit.
const MAX_QBLOCK_INFLIGHT_TRANSFERS: usize = 16;

/// A per-client random starting point for the monotonic CoAP token counter.
/// coap-rs seeds the Q-Block Request-Tag from the token, so a random base means
/// an observer can't predict the tags of in-flight transfers — defence in depth
/// for the server's source-address binding (an attacker spoofing a peer's
/// address would still have to guess the tag). Monotonic-from-random keeps tokens
/// collision-free for response correlation, like a TCP initial sequence number.
/// Uses `RandomState` (OS-seeded) to avoid a dedicated RNG dependency.
fn random_token_seed() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as u32
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use coap::UdpCoAPClient;
use coap::client::{ClientTransport, CoAPClient};
use tokio::sync::Mutex;

use crate::transport::{MAX_WIRE_BODY_BYTES, WireClient, WireError, WireRequest, WireResponse};

/// Egress wire client over CoAP/UDP. Pools one `UdpCoAPClient` per `dest` (see
/// `pool`); coap-rs handles CON retransmit and Block1/Block2 blockwise
/// transparently.
pub struct CoapWireClient {
    /// Max bytes per Block1 (request) chunk. `None` uses coap-rs's 1024 B
    /// default. Lower it to fit a constrained link's datagram budget. (Block2 —
    /// the response chunk size — is fixed by the responding server's coap-rs
    /// default and not yet tunable; see the module docs / `PLAN.md`.)
    block1_size: Option<usize>,
    /// When `Some`, `send` uses RFC 9177 Q-Block NON-mode (`send_qblock`) with
    /// this config; when `None`, the CON path (`send`). Set on the pooled client
    /// at construction (see `client_for`).
    qblock: Option<coap::qblock::QBlockConfig>,
    /// Total wall-clock ceiling on a single `send` (the whole blockwise
    /// exchange). coap-rs bounds each CON block by its own receive-timeout ×
    /// retries, but nothing bounds a multi-block transfer where a peer answers
    /// each block slowly-but-just-in-time — so without this a slow/black-hole
    /// peer pins the egress task and its buffered body indefinitely. Mirrors the
    /// HTTP transport's `REQUEST_TIMEOUT` rationale.
    request_timeout: Duration,
    /// Cap on a peer's assembled response body, mirroring the ingress request
    /// cap. The peer is the untrusted network side and its response is buffered
    /// then re-transcoded, so an unbounded body is an OOM surface even from a
    /// non-hostile peer. See the module OOM note.
    max_body_bytes: usize,
    /// Per-`dest` client pool. `UdpCoAPClient::new` resolves `dest` (a DNS lookup
    /// for a hostname) and binds a fresh UDP socket + background receive task on
    /// every call, so a burst of federation requests to one peer would otherwise
    /// pay that repeatedly; pooling reuses one client per peer. Sharing is safe
    /// because each request carries a unique token (`next_token`) and coap-rs
    /// correlates responses by token. A failed or timed-out request evicts its
    /// client (see `send`): dropping the last `Arc` closes the socket and ends
    /// the receive task (which holds only a `Weak`), so the next attempt
    /// re-resolves `dest` (handling a peer that moved) from clean coap-rs state.
    pool: Mutex<HashMap<String, Arc<UdpCoAPClient>>>,
    /// Monotonic source of unique per-request CoAP tokens. coap-rs keys its
    /// response-correlation map by token; the empty default token would alias
    /// across concurrent requests sharing a pooled client.
    token_counter: AtomicU32,
}

impl CoapWireClient {
    pub fn new() -> Self {
        Self {
            block1_size: None,
            qblock: None,
            request_timeout: crate::REQUEST_TIMEOUT,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            pool: Mutex::new(HashMap::new()),
            token_counter: AtomicU32::new(random_token_seed()),
        }
    }

    /// Build a client with a specific Block1 (request) chunk size. `None` keeps
    /// coap-rs's 1024 B default.
    pub fn with_block1_size(block1_size: Option<usize>) -> Self {
        Self {
            block1_size,
            ..Self::new()
        }
    }

    /// Build a Q-Block (RFC 9177) NON-mode client. `block1_size` caps the
    /// per-burst block payload (also the Q-Block block size); `None` keeps
    /// coap-rs's 1024 B default.
    ///
    /// `request_timeout` is grown to cover the Q-Block recovery window so a long
    /// tuning can't be killed mid-recovery by the outer timeout: coap-rs drives
    /// request-send recovery for `linger = non_receive_timeout *
    /// (non_max_retransmit + 2)` after the burst, and the response-receive side
    /// recovers within a comparable window, so the whole exchange's recovery is
    /// bounded by ~2× linger. We floor at `REQUEST_TIMEOUT`, so the default tuning
    /// (linger 24 s → 48 s < 60 s) is unchanged; only larger tunings scale it up.
    pub fn with_qblock(block1_size: Option<usize>, qblock: coap::qblock::QBlockConfig) -> Self {
        let rounds = qblock.non_max_retransmit.saturating_add(2);
        let linger = qblock.non_receive_timeout.saturating_mul(rounds);
        let request_timeout = crate::REQUEST_TIMEOUT.max(linger.saturating_mul(2));
        Self {
            block1_size,
            qblock: Some(qblock),
            request_timeout,
            ..Self::new()
        }
    }

    /// The pooled client for `dest`, creating (resolving + binding) one on first
    /// use. The pool lock is never held across the `await` on construction, so
    /// concurrent first-sends to different peers don't serialise; a race on the
    /// same `dest` simply discards the loser's freshly-built client.
    async fn client_for(&self, dest: &str) -> Result<Arc<UdpCoAPClient>, WireError> {
        if let Some(client) = self.pool.lock().await.get(dest).cloned() {
            return Ok(client);
        }
        let mut client = UdpCoAPClient::new(dest)
            .await
            .map_err(|e| WireError::Transport(format!("coap dial {dest}: {e}")))?;
        if let Some(size) = self.block1_size {
            client.set_block1_size(size);
        }
        if let Some(cfg) = &self.qblock {
            client.set_qblock_config(cfg.clone());
            // Bound Q-Block2 response reassembly at the framing layer. Without
            // this coap-rs builds the receiver with `usize::MAX`, so the
            // post-reassembly cap in `send` only fires *after* the whole body is
            // buffered — an OOM surface (fatal on mobile) from a hostile peer's
            // response. Setting it makes coap-rs abort on the first block that
            // would exceed the cap, before it allocates. Safe for the request
            // side: `send_qblock` sizes Q-Block1 from the static `block1_size`,
            // not from `max_total_message_size` (only the CON `send` path derives
            // its block size from the MTU), so request blocking is unaffected.
            client.set_max_total_message_size(Some(self.max_body_bytes));
        }
        let client = Arc::new(client);
        Ok(self
            .pool
            .lock()
            .await
            .entry(dest.to_owned())
            .or_insert(client)
            .clone())
    }

    /// Drop the pooled client for `dest` so the next send rebuilds it.
    async fn evict(&self, dest: &str) {
        self.pool.lock().await.remove(dest);
    }

    /// A unique non-empty CoAP token, so concurrent requests sharing a pooled
    /// client don't alias in coap-rs's token-keyed response map.
    fn next_token(&self) -> Vec<u8> {
        self.token_counter
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes()
            .to_vec()
    }

    #[cfg(test)]
    fn for_test(block1_size: Option<usize>, request_timeout: Duration) -> Self {
        Self {
            request_timeout,
            ..Self::with_block1_size(block1_size)
        }
    }

    #[cfg(test)]
    fn with_request_timeout(request_timeout: Duration) -> Self {
        Self {
            request_timeout,
            ..Self::new()
        }
    }

    #[cfg(test)]
    fn with_max_body_bytes(max_body_bytes: usize) -> Self {
        Self {
            max_body_bytes,
            ..Self::new()
        }
    }

    #[cfg(test)]
    fn with_qblock_and_max_body_bytes(
        block1_size: Option<usize>,
        qblock: coap::qblock::QBlockConfig,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            max_body_bytes,
            ..Self::with_qblock(block1_size, qblock)
        }
    }

    // A Q-Block client with an explicit `request_timeout`, overriding the value
    // `with_qblock` derives from the tuning — so a black-hole test need not wait
    // the 60 s floor.
    #[cfg(test)]
    fn with_qblock_and_request_timeout(
        qblock: coap::qblock::QBlockConfig,
        request_timeout: Duration,
    ) -> Self {
        Self {
            request_timeout,
            ..Self::with_qblock(None, qblock)
        }
    }
}

impl Default for CoapWireClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WireClient for CoapWireClient {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
        // A zero Block1 size is a misconfiguration: coap-rs chunks the request
        // body with `payload.chunks(block1_size)`, and `chunks(0)` panics. Reject
        // it as a transport error rather than letting it crash the egress task.
        if self.block1_size == Some(0) {
            return Err(WireError::Transport(
                "coap block1_size must be non-zero".to_owned(),
            ));
        }
        let dest = req.dest.clone();
        let client = self.client_for(&dest).await?;
        let token = self.next_token();
        // Drive the actual exchange through the transport-agnostic helper shared
        // with the link datagram client (see `datagram`); the only UDP-specific
        // part is `client_for` (pooling + `UdpCoAPClient::new`) above.
        let result = exchange(
            &client,
            &req,
            token,
            self.qblock.is_some(),
            self.max_body_bytes,
            self.request_timeout,
        )
        .await;
        // On any failure (incl. timeout) drop the pooled client: it closes the
        // socket, ends the receive task, frees any state an abandoned exchange
        // left in the token map, and forces the next send to re-resolve `dest`.
        if result.is_err() {
            self.evict(&dest).await;
        }
        result
    }
}

/// Transport-agnostic CoAP request/response exchange, shared by the UDP client
/// (`CoapWireClient`) and the link datagram client (`datagram::LinkCoapWireClient`).
/// The ONLY transport-specific concern either path adds is how the
/// `CoAPClient<T>` is built/pooled; the security-relevant logic — token tagging,
/// CON-vs-Q-Block send selection, the untrusted-side response body cap, the
/// whole-exchange timeout that bounds a slow/black-hole peer — lives here once.
///
/// `qblock` selects `send_qblock` (RFC 9177 NON-mode) over `send` (CON); it
/// mirrors `CoapWireClient::qblock.is_some()`. The caller is responsible for any
/// post-failure cleanup (e.g. the UDP pool eviction) since that *is* transport
/// specific.
async fn exchange<T: ClientTransport + 'static>(
    client: &CoAPClient<T>,
    req: &WireRequest,
    token: Vec<u8>,
    qblock: bool,
    max_body_bytes: usize,
    request_timeout: Duration,
) -> Result<WireResponse, WireError> {
    let do_exchange = async {
        let mut coap_req = message::build_request(req);
        coap_req.message.set_token(token);
        let resp = if qblock {
            client
                .send_qblock(coap_req)
                .await
                .map_err(|e| WireError::Transport(format!("coap send_qblock: {e}")))?
        } else {
            client
                .send(coap_req)
                .await
                .map_err(|e| WireError::Transport(format!("coap send: {e}")))?
        };
        // Bound the peer's (post-reassembly) response before re-transcoding it —
        // the untrusted-side OOM guard, symmetric with the ingress cap.
        if resp.message.payload.len() > max_body_bytes {
            return Err(WireError::Transport(format!(
                "peer response body exceeds {max_body_bytes}-byte cap"
            )));
        }
        Ok(message::parse_response(&resp))
    };
    // Total ceiling on the whole exchange, so a slow/black-hole peer can't pin
    // this task past `request_timeout` (the per-block coap-rs retries do not
    // bound a multi-block transfer).
    match tokio::time::timeout(request_timeout, do_exchange).await {
        Ok(inner) => inner,
        Err(_) => Err(WireError::Transport(format!(
            "coap request to {} exceeded {request_timeout:?}",
            req.dest
        ))),
    }
}

use std::net::SocketAddr;
use std::sync::Arc;

use coap::Server;
use coap_lite::{BlockHandlerConfig, CoapRequest};
use tokio_util::sync::CancellationToken;

use crate::transport::{WireHandler, WireServer};

/// Ingress wire server over CoAP/UDP. Binds the public federation UDP port and
/// dispatches each inbound request to the `WireHandler`. coap-rs reassembles
/// blockwise requests and segments large responses internally.
pub struct CoapWireServer {
    bind: SocketAddr,
    /// Total framed-message budget (`BlockHandlerConfig.max_total_message_size`).
    /// `None` uses coap-rs's ~1152 B default. Bounds both the largest accepted
    /// inbound request datagram and the outbound Block2 fragment size (see the
    /// `WireKind::Coap` docs for the coupling with the client's `block1_size`).
    max_message_size: Option<usize>,
    /// Cap on the assembled inbound request body handed to the handler (the
    /// network-exposed OOM guard — see the module OOM note). Distinct from
    /// `max_message_size`, which bounds per-block framing, not the running total.
    max_body_bytes: usize,
    /// When `Some`, `serve` enables RFC 9177 Q-Block NON-mode on the listener via
    /// `set_qblock_config`; when `None`, the CON path.
    qblock: Option<coap::qblock::QBlockConfig>,
}

impl CoapWireServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            max_message_size: None,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            qblock: None,
        }
    }

    /// Build a server with a specific per-message size budget. `None` keeps
    /// coap-rs's ~1152 B default.
    pub fn with_max_message_size(bind: SocketAddr, max_message_size: Option<usize>) -> Self {
        Self {
            max_message_size,
            ..Self::new(bind)
        }
    }

    /// Build a Q-Block (RFC 9177) NON-mode server. Uses coap-rs's default
    /// per-message size (no `max_message_size` budget in v1; Block2 follows the
    /// szx default — see the spec's Block2 follow-up).
    pub fn with_qblock(bind: SocketAddr, qblock: coap::qblock::QBlockConfig) -> Self {
        Self {
            qblock: Some(qblock),
            ..Self::new(bind)
        }
    }

    #[cfg(test)]
    fn with_max_body_bytes(bind: SocketAddr, max_body_bytes: usize) -> Self {
        Self {
            max_body_bytes,
            ..Self::new(bind)
        }
    }
}

/// Adapter so `coap::Server` can call into our `WireHandler`.
struct CoapDispatch {
    handler: Arc<dyn WireHandler>,
    max_body_bytes: usize,
    /// Set only on the authenticated datagram ingress (the embedded build). When
    /// present, every inbound request's claimed `X-Matrix origin` is bound to the
    /// link-authenticated source node before the handler runs — a peer may assert
    /// only its own origin (see [`datagram::Hub::origin_binding_violation`]). The
    /// UDP/HTTP transports run on a trusted LAN with no peer authentication, so
    /// they leave this `None` and keep `federation::auth`'s existing behaviour.
    node_binding: Option<Arc<datagram::Hub>>,
    /// The medium's wire codec ([`datagram::LinkCodec`]), ingress half:
    /// `decode_request` after parse and BEFORE the origin binding (so the gate
    /// compares canonical names), `encode_response` before the response is
    /// written (pre-Block2). Link ingress only; `None` on the UDP transport.
    codec: Option<Arc<dyn datagram::LinkCodec>>,
}

impl CoapDispatch {
    async fn handle(
        &self,
        mut request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        // `from_packet` populates `response` for any valid request; if it is
        // absent (malformed / non-confirmable inbound) we must NOT run the
        // handler — doing so would forward a side-effecting request upstream
        // whose reply is then discarded. Let coap-rs answer with its default.
        if request.response.is_none() {
            return request;
        }
        // Refuse an over-cap assembled body with 413 before it reaches the
        // handler/transcode — the network-exposed OOM guard, mirroring the HTTP
        // ingress. (coap-lite has already reassembled it; this still bounds the
        // downstream transcode + loopback forward — see the module OOM note.)
        let wire_resp = if request.message.payload.len() > self.max_body_bytes {
            status_only(413)
        } else {
            self.dispatch(&request).await
        };
        // Medium codec, ingress half: transform the response BEFORE it is
        // written (and Block2-segmented). A response our own codec can't
        // encode is a bug surfaced as a 500, never a corrupt wire message.
        let wire_resp = match &self.codec {
            Some(codec) => {
                let mut resp = wire_resp;
                match codec.encode_response(&mut resp) {
                    Ok(()) => resp,
                    Err(e) => {
                        tracing::warn!(error = %e, "link codec encode_response failed");
                        status_only(500)
                    }
                }
            }
            None => wire_resp,
        };
        if let Some(ref mut response) = request.response {
            message::write_response(response, &wire_resp);
        }
        request
    }

    /// Parse → decode (medium codec) → origin-binding gate → handler.
    async fn dispatch(&self, request: &CoapRequest<SocketAddr>) -> WireResponse {
        let mut wire_req = message::parse_request(request);
        // Medium codec, ingress half: undo the peer's `encode_request`. Runs
        // BEFORE the origin binding so the gate compares canonical names. A
        // request that doesn't decode is malformed — 400, upgrade-together
        // mesh, no fallback.
        if let Some(codec) = &self.codec
            && let Err(e) = codec.decode_request(&mut wire_req)
        {
            tracing::warn!(error = %e, "link codec decode_request failed");
            return status_only(400);
        }
        // Authenticated datagram ingress: a peer may assert only its own
        // origin. Reject (401) before the handler runs if the claimed
        // `X-Matrix origin` is not the link-authenticated source node.
        if self
            .node_binding
            .as_ref()
            .is_some_and(|hub| hub.origin_binding_violation(request.source, &wire_req.headers))
        {
            return status_only(401);
        }
        self.handler.handle(wire_req).await
    }
}

/// A bodyless, headerless `WireResponse` carrying only `status`.
fn status_only(status: u16) -> WireResponse {
    WireResponse {
        status,
        headers: vec![],
        body: vec![],
        ..Default::default()
    }
}

#[async_trait]
impl WireServer for CoapWireServer {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError> {
        let mut server = match self.max_message_size {
            Some(max_total_message_size) => Server::new_udp_with_config(
                self.bind,
                BlockHandlerConfig {
                    max_total_message_size,
                    ..Default::default()
                },
            ),
            None => Server::new_udp(self.bind),
        }
        .map_err(|e| WireError::Serve(format!("bind {}: {e}", self.bind)))?;
        if let Some(cfg) = self.qblock.clone() {
            server.set_qblock_config(cfg);
            // Bound the network-exposed Q-Block1 reassembly. Aligning the
            // per-transfer cap with our assembled-body contract makes coap-rs
            // abort reassembly at the same threshold the 413 guard enforces (the
            // ingress analogue of the egress response cap), instead of coap-rs's
            // hardcoded 16 MiB default; capping concurrent transfers stops a peer
            // opening many Request-Tags from exhausting memory.
            server.set_qblock_max_body_len(self.max_body_bytes);
            server.set_qblock_max_transfers(MAX_QBLOCK_INFLIGHT_TRANSFERS);
        }
        let dispatch = Arc::new(CoapDispatch {
            handler,
            max_body_bytes: self.max_body_bytes,
            // UDP runs on a trusted LAN with no peer authentication — no binding,
            // and no medium codec (that seam is link-only).
            node_binding: None,
            codec: None,
        });

        // `coap::Server::run` has no native shutdown, so race it against the
        // token: when shutdown fires the run future is dropped. Our `coap` fork
        // aborts the listener task(s) on that drop (the `AbortOnDrop` guard in
        // `Server::run`), which closes the UDP socket — so the public federation
        // port is released promptly rather than leaked until process exit.
        // (coap-rs blanket-impls RequestHandler for closures returning a
        // boxed-request future.)
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
mod smoke_tests {
    use coap::Server;
    use coap::UdpCoAPClient;
    use coap_lite::{CoapRequest, RequestType};
    use std::net::SocketAddr;

    // Confirms the coap-rs 0.27 client+server API and a basic CON round-trip.
    // If this compiles and passes, the signatures the later tasks rely on hold.
    // `Server` exposes no bound-address accessor, so grab a free port with a
    // probe socket, drop it, and bind the server there.
    #[tokio::test]
    async fn coap_rs_client_server_roundtrip() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let server = Server::new_udp(addr).expect("bind udp server");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    if let Some(ref mut resp) = request.response {
                        resp.message.payload = b"pong".to_vec();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = UdpCoAPClient::new(addr).await.expect("client");
        let mut req: CoapRequest<SocketAddr> = CoapRequest::new();
        req.set_method(RequestType::Get);
        req.set_path("/ping");
        let resp = client.send(req).await.expect("send");
        assert_eq!(resp.message.payload, b"pong");
    }
}

#[cfg(test)]
mod client_tests {
    use super::*;
    use crate::transport::{WireClient, WireRequest};
    use axum::http::Method;
    use coap::Server;
    use coap_lite::{CoapOption, CoapRequest};
    use std::net::SocketAddr;

    // A coap-rs server that echoes the decoded request path + body back as a 200,
    // so the client's request construction and response parsing are observable.
    #[tokio::test]
    async fn client_sends_request_and_parses_response() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let server = Server::new_udp(addr).expect("server");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    let path_segs = request
                        .message
                        .get_option(CoapOption::UriPath)
                        .map(|l| {
                            l.iter()
                                .map(|b| String::from_utf8_lossy(b).into_owned())
                                .collect::<Vec<_>>()
                                .join("/")
                        })
                        .unwrap_or_default();
                    let echo = request.message.payload.clone();
                    if let Some(ref mut resp) = request.response {
                        resp.message.add_option(
                            CoapOption::Unknown(OPT_HTTP_STATUS),
                            200u16.to_be_bytes().to_vec(),
                        );
                        resp.message.payload = [path_segs.as_bytes(), b"|", &echo].concat();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
                headers: vec![],
                body: vec![1, 2, 3],
                ..Default::default()
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        // Server echoes the decoded coap path segments (code f8 + dynamic) + body.
        assert_eq!(resp.body, b"f8/!r:a/$e|\x01\x02\x03");
    }

    // A peer that answers without an OPT_HTTP_STATUS option must surface as a
    // retryable 502 through the client (not a panic) — the transport-level twin
    // of `message::tests::missing_status_option_defaults_to_bad_gateway`, on the
    // path that actually feeds the federation retry loop.
    #[tokio::test]
    async fn missing_status_option_surfaces_502() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let server = Server::new_udp(addr).expect("server");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    // Reply with a body but deliberately no OPT_HTTP_STATUS.
                    if let Some(ref mut resp) = request.response {
                        resp.message.payload = b"no-status".to_vec();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::GET,
                path: "/_matrix/federation/v1/event/$e".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
            })
            .await
            .expect("send");
        assert_eq!(resp.status, 502, "missing status must surface as 502");
    }

    // A zero Block1 size is an operator misconfiguration that would panic coap-rs
    // (`payload.chunks(0)`); the client must reject it as a Transport error before
    // dialing, not crash the egress task. Body is non-empty so the chunking path
    // (the panic site) would be reached but for the guard.
    #[tokio::test]
    async fn zero_block1_size_errors_instead_of_panicking() {
        let client = CoapWireClient::with_block1_size(Some(0));
        let err = client
            .send(WireRequest {
                dest: "127.0.0.1:1".to_owned(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0u8; 64],
                ..Default::default()
            })
            .await
            .expect_err("zero block1_size must error, not panic");
        assert!(matches!(err, WireError::Transport(_)), "got {err:?}");
    }
}

#[cfg(test)]
mod server_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    struct EchoHandler;

    #[async_trait]
    impl WireHandler for EchoHandler {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            // Echo the decoded path (as a forwardable header) + body so we can
            // assert the server mapped them.
            WireResponse {
                status: 200,
                headers: vec![("x-matrix-seen-path".to_owned(), req.path.into_bytes())],
                body: req.body,
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn server_dispatches_to_handler_and_client_round_trips() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let server = CoapWireServer::new(addr);
        let handle =
            tokio::spawn(async move { server.serve(Arc::new(EchoHandler), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn9".to_owned(),
                headers: vec![],
                body: vec![9, 9, 9],
                ..Default::default()
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![9, 9, 9]);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "x-matrix-seen-path" && v == b"/_matrix/federation/v1/send/txn9"),
            "server mapped path wrong: {:?}",
            resp.headers
        );

        // Shutdown returns cleanly.
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "server did not wind down on cancel");
    }
}

#[cfg(test)]
mod blockwise_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    // Echoes a body 64x larger than the request, exercising Block2 (large
    // response) on top of Block1 (large request).
    struct BigEcho;

    #[async_trait]
    impl WireHandler for BigEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            let big = req
                .body
                .iter()
                .cycle()
                .take(req.body.len() * 64)
                .copied()
                .collect();
            WireResponse {
                status: 200,
                headers: vec![],
                body: big,
                ..Default::default()
            }
        }
    }

    // A body well over one ~1 KiB CoAP block must round-trip intact in both
    // directions — the load-bearing case (a real `send_join` serializes the whole
    // room state DAG). Proves coap-rs Block1 (request) + Block2 (response)
    // reassembly is wired through the transport.
    #[tokio::test]
    async fn large_body_round_trips_via_blockwise() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            CoapWireServer::new(addr)
                .serve(Arc::new(BigEcho), server_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 8 KiB request -> 512 KiB response, both far over one datagram.
        let req_body = vec![0xABu8; 8 * 1024];
        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            })
            .await
            .expect("blockwise send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.len(), req_body.len() * 64);
        assert!(
            resp.body.iter().all(|b| *b == 0xAB),
            "blockwise payload corrupted"
        );

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server task did not wind down cleanly on cancel: {joined:?}"
        );
    }

    // A configured small Block1 size must still round-trip a request body many
    // times larger than one block — forcing ~16 Block1 chunks at 64 B and proving
    // the `set_block1_size` tuning is applied and correct under tiny packets (the
    // low-bandwidth-link case). Echoes the body back 1:1.
    struct PlainEcho;

    #[async_trait]
    impl WireHandler for PlainEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![],
                body: req.body,
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn small_block1_size_chunks_request_and_round_trips() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            CoapWireServer::new(addr)
                .serve(Arc::new(PlainEcho), server_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 1 KiB body with a 64 B Block1 cap -> ~16 request blocks.
        let req_body: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let client = CoapWireClient::with_block1_size(Some(64));
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            })
            .await
            .expect("small-block send");

        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.body, req_body,
            "small-block request round-trip corrupted"
        );

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server task did not wind down cleanly on cancel: {joined:?}"
        );
    }

    // A small server `max_message_size` budget (forked coap-rs `*_with_config`)
    // forces small Block2 *response* fragments; paired with a coordinated small
    // Block1 (so inbound request blocks stay within budget — see the coupling in
    // `WireKind::Coap` docs), a multi-KiB body must still round-trip intact in
    // both directions. Pins that the budget is applied and the coordinated
    // constrained-link config works end to end.
    #[tokio::test]
    async fn small_message_budget_round_trips_both_directions() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        // 256 B total-message budget -> Block2 response fragments well under it.
        let handle = tokio::spawn(async move {
            CoapWireServer::with_max_message_size(addr, Some(256))
                .serve(Arc::new(PlainEcho), server_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 2 KiB body. Block1 at 128 B payload leaves headroom under the 256 B
        // budget for CoAP option overhead, so inbound is not down-negotiated.
        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let client = CoapWireClient::with_block1_size(Some(128));
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            })
            .await
            .expect("budgeted send");

        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.body, req_body,
            "small-budget round-trip corrupted across Block1+Block2"
        );

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server task did not wind down cleanly on cancel: {joined:?}"
        );
    }
}

#[cfg(test)]
mod cap_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    // Echoes the request body and records whether it ran, so the ingress cap can
    // assert the handler is bypassed for an over-cap body.
    struct SpyEcho(Arc<AtomicBool>);

    #[async_trait]
    impl WireHandler for SpyEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            self.0.store(true, Ordering::SeqCst);
            WireResponse {
                status: 200,
                headers: vec![],
                body: req.body,
                ..Default::default()
            }
        }
    }

    // An over-cap inbound body must be refused with 413 before the handler runs
    // — the network-exposed OOM guard, symmetric with the HTTP ingress.
    #[tokio::test]
    async fn server_rejects_oversized_body_with_413() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let ran = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handler_ran = ran.clone();
        let handle = tokio::spawn(async move {
            CoapWireServer::with_max_body_bytes(addr, 8)
                .serve(Arc::new(SpyEcho(handler_ran)), server_token)
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0u8; 100], // > 8-byte cap
                ..Default::default()
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 413, "oversized body must be rejected");
        assert!(
            !ran.load(Ordering::SeqCst),
            "handler must not run for an over-cap body"
        );

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server task did not wind down cleanly on cancel: {joined:?}"
        );
    }

    // A peer response over the client's cap must surface as a Transport error
    // rather than being buffered + re-transcoded (the untrusted-side OOM guard).
    #[tokio::test]
    async fn client_rejects_oversized_response_body() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            // Default 64 MiB cap: the 100-byte request passes; the handler echoes
            // it back, giving the client a 100-byte response to reject.
            CoapWireServer::new(addr)
                .serve(
                    Arc::new(SpyEcho(Arc::new(AtomicBool::new(false)))),
                    server_token,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = CoapWireClient::with_max_body_bytes(8); // cap below 100
        let err = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0u8; 100],
                ..Default::default()
            })
            .await
            .expect_err("oversized peer response must error, not buffer");
        assert!(matches!(err, WireError::Transport(_)), "got {err:?}");

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "server task did not wind down cleanly on cancel: {joined:?}"
        );
    }

    // A black-hole peer (a UDP socket that never answers) must surface as a
    // Transport error within the client's request timeout, not hang — the CoAP
    // analogue of the HTTP slow-peer timeout. The outer timeout fails the test if
    // `send` ever hangs past the bound.
    #[tokio::test]
    async fn dead_peer_send_is_bounded_by_request_timeout() {
        // Bind a socket and never read/respond: every CON goes unanswered.
        let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sink.local_addr().unwrap();

        let client = CoapWireClient::with_request_timeout(Duration::from_millis(200));
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            client.send(WireRequest {
                dest: addr.to_string(),
                method: Method::GET,
                path: "/_matrix/federation/v1/event/$e".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
            }),
        )
        .await
        .expect("send must return within the request timeout, not hang")
        .expect_err("a black-hole peer must surface a Transport error");
        assert!(matches!(err, WireError::Transport(_)), "got {err:?}");
        drop(sink);
    }

    // The documented Block1/budget mis-coordination failure: a client Block1
    // payload larger than the server's total-message budget forces a server
    // down-negotiation the coap-rs client cannot satisfy. The contract (see the
    // `WireKind::Coap` docs) is that this must NOT silently round-trip — it must
    // surface as a failure (a transport error, or a non-2xx status), bounded by
    // the request timeout rather than hanging or corrupting the body.
    #[tokio::test]
    async fn mismatched_block1_over_budget_does_not_silently_succeed() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        // Tiny 64 B budget vs a 512 B client request block -> uncoordinated.
        let handle = tokio::spawn(async move {
            CoapWireServer::with_max_message_size(addr, Some(64))
                .serve(
                    Arc::new(SpyEcho(Arc::new(AtomicBool::new(false)))),
                    server_token,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 512 B Block1 over a 64 B budget; a short timeout so a (regressed) hang
        // fails fast rather than waiting the full default.
        let client = CoapWireClient::for_test(Some(512), Duration::from_secs(3));
        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let result = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body,
                ..Default::default()
            })
            .await;
        match result {
            Err(WireError::Transport(_)) => {} // down-negotiation surfaced as an error
            Ok(resp) => assert_ne!(
                resp.status, 200,
                "uncoordinated block1/budget must not silently succeed"
            ),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }

        token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use crate::transport::{WireHandler, WireRequest, WireResponse};
    use coap_lite::{CoapRequest, CoapResponse};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Spy(Arc<AtomicBool>);

    #[async_trait]
    impl WireHandler for Spy {
        async fn handle(&self, _req: WireRequest) -> WireResponse {
            self.0.store(true, Ordering::SeqCst);
            WireResponse {
                status: 200,
                headers: vec![],
                body: vec![],
                ..Default::default()
            }
        }
    }

    // No response slot (malformed / non-confirmable inbound): the handler must
    // NOT run, so a side-effecting upstream forward with a discarded reply can't
    // happen.
    #[tokio::test]
    async fn skips_handler_when_no_response_slot() {
        let ran = Arc::new(AtomicBool::new(false));
        let dispatch = CoapDispatch {
            handler: Arc::new(Spy(ran.clone())),
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            node_binding: None,
            codec: None,
        };
        let mut request: Box<CoapRequest<SocketAddr>> = Box::new(CoapRequest::new());
        request.response = None;
        let _ = dispatch.handle(request).await;
        assert!(
            !ran.load(Ordering::SeqCst),
            "handler ran without a response slot"
        );
    }

    // With a response slot present, the handler runs and its reply is written.
    #[tokio::test]
    async fn runs_handler_when_response_slot_present() {
        let ran = Arc::new(AtomicBool::new(false));
        let dispatch = CoapDispatch {
            handler: Arc::new(Spy(ran.clone())),
            max_body_bytes: MAX_WIRE_BODY_BYTES,
            node_binding: None,
            codec: None,
        };
        let mut request: Box<CoapRequest<SocketAddr>> = Box::new(CoapRequest::new());
        request.response = Some(CoapResponse::new(&request.message).expect("response"));
        let out = dispatch.handle(request).await;
        assert!(
            ran.load(Ordering::SeqCst),
            "handler did not run with a response slot"
        );
        assert!(out.response.is_some());
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    // Echoes the request body verbatim, so each concurrent caller's response is
    // identifiable by its own (distinct) body.
    struct BodyEcho;

    #[async_trait]
    impl WireHandler for BodyEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![],
                body: req.body,
                ..Default::default()
            }
        }
    }

    // Many overlapping `send`s through one (shared, `Arc`-cloned) client must each
    // receive *their own* response, never another in-flight request's. They share
    // a single pooled `UdpCoAPClient` for the dest, so correlation rests entirely
    // on the unique per-request token (`next_token`); coap-rs keys its response map
    // by token and the empty default token would alias. This is the regression
    // guard for the per-`dest` client pool.
    #[tokio::test]
    async fn concurrent_requests_correlate_to_their_own_responses() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            CoapWireServer::new(addr)
                .serve(Arc::new(BodyEcho), server_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = Arc::new(CoapWireClient::new());
        let mut handles = Vec::new();
        for i in 0u8..16 {
            let client = client.clone();
            let dest = addr.to_string();
            handles.push(tokio::spawn(async move {
                // Each request carries a unique body keyed by `i`; the echo must
                // come back byte-for-byte.
                let body = vec![i; 48 + i as usize];
                let resp = client
                    .send(WireRequest {
                        dest,
                        method: Method::PUT,
                        path: format!("/_matrix/federation/v1/send/txn{i}"),
                        headers: vec![],
                        body: body.clone(),
                        ..Default::default()
                    })
                    .await
                    .expect("concurrent send");
                (resp, body)
            }));
        }
        for h in handles {
            let (resp, expected) = h.await.expect("task");
            assert_eq!(resp.status, 200);
            assert_eq!(
                resp.body, expected,
                "a concurrent request received the wrong response body"
            );
        }

        token.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
}

#[cfg(test)]
mod loss_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::UdpSocket;
    use tokio_util::sync::CancellationToken;

    struct PlainEcho;

    #[async_trait]
    impl WireHandler for PlainEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![],
                body: req.body,
                ..Default::default()
            }
        }
    }

    // A deterministic lossy UDP relay sitting between the client and `server`. It
    // forwards datagrams both ways but silently drops the client→server datagrams
    // whose 1-based arrival index is in `drop_to_server` — modelling a lossy radio
    // link. A dropped CON forces coap-rs to retransmit (5 retries × 2s), so the
    // transfer must still complete. Runs until `token` fires.
    async fn run_lossy_relay(
        front: UdpSocket,
        server: SocketAddr,
        drop_to_server: Vec<usize>,
        dropped: Arc<AtomicUsize>,
        token: CancellationToken,
    ) {
        let back = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("relay back bind");
        let mut client_addr: Option<SocketAddr> = None;
        let mut to_server_seen = 0usize;
        let mut from_client = vec![0u8; u16::MAX as usize];
        let mut from_server = vec![0u8; u16::MAX as usize];
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                r = front.recv_from(&mut from_client) => {
                    let Ok((n, src)) = r else { return };
                    client_addr = Some(src);
                    to_server_seen += 1;
                    if drop_to_server.contains(&to_server_seen) {
                        // drop this datagram; the sender must retransmit/recover.
                        dropped.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    let _ = back.send_to(&from_client[..n], server).await;
                }
                r = back.recv_from(&mut from_server) => {
                    let Ok((n, _)) = r else { return };
                    if let Some(dst) = client_addr {
                        let _ = front.send_to(&from_server[..n], dst).await;
                    }
                }
            }
        }
    }

    // Spawn the echo server and a lossy relay; return (relay_front_addr, tokens).
    async fn spawn_server_and_relay(drop_to_server: Vec<usize>) -> (SocketAddr, CancellationToken) {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        tokio::spawn(async move {
            CoapWireServer::new(server_addr)
                .serve(Arc::new(PlainEcho), server_token)
                .await
        });

        let front = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("relay front bind");
        let front_addr = front.local_addr().unwrap();
        let relay_token = token.clone();
        // CON callers don't assert drop counts; discard the counter.
        tokio::spawn(run_lossy_relay(
            front,
            server_addr,
            drop_to_server,
            Arc::new(AtomicUsize::new(0)),
            relay_token,
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        (front_addr, token)
    }

    // A dropped initial request datagram must be recovered by coap-rs's CON
    // retransmission — the body still round-trips, just later. Pins that the
    // transport survives loss on a single-datagram request (the common lossy-link
    // case), not only on a lossless loopback.
    #[tokio::test]
    async fn dropped_request_datagram_is_retransmitted() {
        let (relay_addr, token) = spawn_server_and_relay(vec![1]).await;

        // Generous total timeout so the ~2s retransmit comfortably fits.
        let client = CoapWireClient::for_test(None, Duration::from_secs(15));
        let resp = client
            .send(WireRequest {
                dest: relay_addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0x5Au8; 64],
                ..Default::default()
            })
            .await
            .expect("send must recover from the dropped datagram");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![0x5Au8; 64]);
        token.cancel();
    }

    // A datagram dropped *mid* blockwise transfer must likewise be retransmitted
    // and the multi-block body reassembled intact. With a 64 B Block1 a ~2 KiB
    // body spans many datagrams; dropping the 3rd exercises retransmission of a
    // single interior block rather than the opening CON.
    #[tokio::test]
    async fn blockwise_survives_a_dropped_mid_transfer_datagram() {
        let (relay_addr, token) = spawn_server_and_relay(vec![3]).await;

        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let client = CoapWireClient::for_test(Some(64), Duration::from_secs(15));
        let resp = client
            .send(WireRequest {
                dest: relay_addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            })
            .await
            .expect("blockwise send must recover from the mid-transfer drop");

        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.body, req_body,
            "blockwise body corrupted after a dropped-and-retransmitted block"
        );
        token.cancel();
    }

    // Spawn a *Q-Block* echo server behind the same lossy relay. The server's
    // Q-Block config is passed in so its recovery timing can be aligned with the
    // client's: the client's `drive_send` only lingers to service missing-block
    // (4.08) requests for `non_receive_timeout * (non_max_retransmit + 2)`, so a
    // server still using the 4 s default would fire its 4.08 long after the
    // fast-timed client stopped listening, and the dropped block would never be
    // recovered. Matching the configs keeps recovery inside that window.
    async fn spawn_qblock_server_and_relay(
        drop_to_server: Vec<usize>,
        server_qcfg: coap::qblock::QBlockConfig,
    ) -> (SocketAddr, CancellationToken, Arc<AtomicUsize>) {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        tokio::spawn(async move {
            CoapWireServer::with_qblock(server_addr, server_qcfg)
                .serve(Arc::new(PlainEcho), server_token)
                .await
        });

        let front = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("relay front bind");
        let front_addr = front.local_addr().unwrap();
        let relay_token = token.clone();
        // Returned so the test can assert the targeted datagram was actually
        // dropped — otherwise a framing change could make "recovery" a vacuous
        // lossless pass.
        let dropped = Arc::new(AtomicUsize::new(0));
        tokio::spawn(run_lossy_relay(
            front,
            server_addr,
            drop_to_server,
            dropped.clone(),
            relay_token,
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        (front_addr, token, dropped)
    }

    // A datagram dropped mid Q-Block burst must be recovered by the 4.08
    // missing-blocks mechanism and the multi-block body reassembled intact. With
    // a 64 B block a ~2 KiB body spans many blocks; dropping the 3rd exercises
    // recovery of an interior burst block (not the opening datagram).
    #[tokio::test]
    async fn qblock_survives_a_dropped_mid_burst_datagram() {
        // Fast recovery timing so the test doesn't wait the 4 s default. The same
        // config drives both ends so the server's 4.08 fires while the client is
        // still lingering to service it (see `spawn_qblock_server_and_relay`).
        // Peer support is assumed (as production does): requests carry no
        // Q-Block2 opt-in, so the flag is what streams the echoed body back.
        let qcfg = coap::qblock::QBlockConfig {
            non_timeout: Duration::from_millis(50),
            non_receive_timeout: Duration::from_millis(100),
            assume_peer_block_size: Some(64),
            ..Default::default()
        };
        let (relay_addr, token, dropped) =
            spawn_qblock_server_and_relay(vec![3], qcfg.clone()).await;

        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let client = CoapWireClient::with_qblock(Some(64), qcfg);
        let resp = tokio::time::timeout(
            Duration::from_secs(15),
            client.send(WireRequest {
                dest: relay_addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("qblock send must complete within the bound")
        .expect("qblock send must recover from the dropped burst datagram");

        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.body, req_body,
            "qblock body corrupted after a dropped-and-recovered burst block"
        );
        // The body matching is only meaningful if a datagram was actually
        // dropped: in NON mode there is no CON retransmit, so reassembling an
        // intact body *after a confirmed drop* is Q-Block's 4.08 recovery at work.
        assert!(
            dropped.load(Ordering::SeqCst) >= 1,
            "relay never dropped the targeted datagram — recovery was not exercised"
        );
        token.cancel();
    }
}

#[cfg(test)]
mod qblock_client_tests {
    use super::*;
    use crate::transport::{WireClient, WireRequest};
    use axum::http::Method;
    use coap::Server;
    use coap_lite::{CoapOption, CoapRequest};
    use std::net::SocketAddr;

    // A raw coap-rs server with Q-Block enabled, echoing the request body back as
    // a 200, so the Q-Block client's send path and response parse are observable.
    #[tokio::test]
    async fn qblock_client_sends_and_parses_response() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let mut server = Server::new_udp(addr).expect("server");
        server.set_qblock_config(coap::qblock::QBlockConfig::default());

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    let echo = request.message.payload.clone();
                    if let Some(ref mut resp) = request.response {
                        resp.message.add_option(
                            CoapOption::Unknown(OPT_HTTP_STATUS),
                            200u16.to_be_bytes().to_vec(),
                        );
                        resp.message.payload = echo;
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::with_qblock(None, coap::qblock::QBlockConfig::default());
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![1, 2, 3, 4],
                ..Default::default()
            })
            .await
            .expect("qblock send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![1, 2, 3, 4]);
    }

    // A peer Q-Block2 response larger than the client's body cap must abort *in
    // the framing layer* (coap-rs's reassembly), not after the whole body is
    // buffered. Regression for the OOM hole where the client never set
    // `max_total_message_size`, leaving coap-rs's receiver bound at `usize::MAX`.
    // A 64 B block size + a 2 KiB echo forces a multi-block Q-Block2 response, so
    // the abort happens mid-stream once the running offset passes the 256 B cap —
    // the error therefore comes from `send_qblock` (the framing layer), provably
    // distinct from the post-reassembly `send` check that fires only on a fully
    // assembled body.
    #[tokio::test]
    async fn qblock_client_aborts_oversized_response_in_framing_layer() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let mut server = Server::new_udp(addr).expect("server");
        // Requests carry no Q-Block2 opt-in; the assumed peer support (64 B
        // blocks, matching the client) is what streams the 2 KiB echo back.
        server.set_qblock_config(coap::qblock::QBlockConfig {
            assume_peer_block_size: Some(64),
            ..Default::default()
        });

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    let echo = request.message.payload.clone();
                    if let Some(ref mut resp) = request.response {
                        resp.message.add_option(
                            CoapOption::Unknown(OPT_HTTP_STATUS),
                            200u16.to_be_bytes().to_vec(),
                        );
                        resp.message.payload = echo;
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 64 B blocks, 256 B cap; the server echoes a 2 KiB body, so the
        // Q-Block2 response spans ~32 blocks and the cap trips long before the
        // body is whole.
        let client = CoapWireClient::with_qblock_and_max_body_bytes(
            Some(64),
            coap::qblock::QBlockConfig::default(),
            256,
        );
        let err = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0u8; 2048],
                ..Default::default()
            })
            .await
            .expect_err("oversized Q-Block2 response must error, not OOM");
        let WireError::Transport(msg) = &err else {
            panic!("expected Transport error, got {err:?}");
        };
        assert!(
            msg.contains("send_qblock"),
            "over-cap Q-Block2 response should abort in the framing layer \
             (send_qblock), not the post-reassembly check: {msg}"
        );
    }

    // `with_qblock` must size `request_timeout` to cover the recovery window so a
    // long tuning is not killed mid-recovery by the outer timeout. Default tuning
    // (linger 24 s, 2× = 48 s) stays at the 60 s floor; a large tuning scales up.
    #[test]
    fn qblock_request_timeout_covers_recovery_linger() {
        use std::time::Duration;

        let default_client =
            CoapWireClient::with_qblock(None, coap::qblock::QBlockConfig::default());
        assert_eq!(
            default_client.request_timeout,
            crate::REQUEST_TIMEOUT,
            "default tuning's 2× linger (48 s) is below the 60 s floor"
        );

        // non_receive_timeout 30 s, non_max_retransmit 4 → linger 180 s, 2× = 360 s.
        let big = coap::qblock::QBlockConfig {
            non_receive_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let big_client = CoapWireClient::with_qblock(None, big);
        assert!(
            big_client.request_timeout >= Duration::from_secs(360),
            "request_timeout must cover 2× linger for a large tuning, got {:?}",
            big_client.request_timeout
        );
    }

    // A black-hole peer on the Q-Block path must surface as a Transport error
    // within `request_timeout`, not hang — the Q-Block analogue of the CON
    // `dead_peer_send_is_bounded_by_request_timeout`. Q-Block's post-burst linger
    // is new timeout-interacting behaviour, so the outer bound must still fire.
    #[tokio::test]
    async fn dead_peer_qblock_send_is_bounded_by_request_timeout() {
        use std::time::Duration;

        // Bind a socket and never answer: the Q-Block burst goes unacknowledged
        // and no Q-Block2 response ever comes back.
        let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sink.local_addr().unwrap();

        let client = CoapWireClient::with_qblock_and_request_timeout(
            coap::qblock::QBlockConfig::default(),
            Duration::from_millis(300),
        );
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            client.send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0u8; 4096], // multi-block burst with the default 1024 B block
                ..Default::default()
            }),
        )
        .await
        .expect("qblock send must return within request_timeout, not hang")
        .expect_err("a black-hole peer must surface a Transport error");
        assert!(matches!(err, WireError::Transport(_)), "got {err:?}");
        drop(sink);
    }
}

#[cfg(test)]
mod qblock_server_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    struct PlainEcho;

    #[async_trait]
    impl WireHandler for PlainEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![("x-matrix-seen-path".to_owned(), req.path.into_bytes())],
                body: req.body,
                ..Default::default()
            }
        }
    }

    // A Q-Block server reassembles the request and routes it to our handler, and
    // the handler's response returns to the Q-Block client — the small-body path
    // (single PDU each way) proving the serve wiring + dispatch reuse.
    #[tokio::test]
    async fn qblock_server_dispatches_and_round_trips() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let server = CoapWireServer::with_qblock(addr, coap::qblock::QBlockConfig::default());
        let handle =
            tokio::spawn(async move { server.serve(Arc::new(PlainEcho), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::with_qblock(None, coap::qblock::QBlockConfig::default());
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txnq".to_owned(),
                headers: vec![],
                body: vec![7, 7, 7],
                ..Default::default()
            })
            .await
            .expect("qblock send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![7, 7, 7]);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "x-matrix-seen-path"
                    && v == b"/_matrix/federation/v1/send/txnq"),
            "server mapped path wrong: {:?}",
            resp.headers
        );

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "qblock server did not wind down cleanly on cancel: {joined:?}"
        );
    }

    // A large body (over one block) must round-trip via the Q-Block burst:
    // multi-block Q-Block1 up, multi-block Q-Block2 down. This is the load-bearing
    // send_join shape, and it confirms the reassembled request still reaches our
    // CoapDispatch handler with a populated response slot.
    #[tokio::test]
    async fn qblock_large_body_round_trips() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            // Peer support assumed at the client's 128 B block size (requests
            // carry no Q-Block2 opt-in), as production configures it.
            CoapWireServer::with_qblock(
                addr,
                coap::qblock::QBlockConfig {
                    assume_peer_block_size: Some(128),
                    ..Default::default()
                },
            )
            .serve(Arc::new(PlainEcho), server_token)
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 4 KiB body with a 128 B Q-Block block -> ~32 blocks each way.
        let req_body: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let client = CoapWireClient::with_qblock(Some(128), coap::qblock::QBlockConfig::default());
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
                headers: vec![],
                body: req_body.clone(),
                ..Default::default()
            })
            .await
            .expect("qblock large send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, req_body, "qblock large body corrupted");

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "qblock server did not wind down cleanly: {joined:?}"
        );
    }
}

#[cfg(test)]
mod feature_gate_tests {
    // Proves the `coap/q-block` feature is enabled and the fork's Q-Block API is
    // linkable from neutrino-lb: if the feature were off (or the patch missing)
    // `coap::qblock::QBlockConfig` would not resolve and this would not compile.
    #[test]
    fn qblock_config_type_is_reachable() {
        let cfg = coap::qblock::QBlockConfig::default();
        // 10 == coap-rs's QBlockConfig::default().max_payloads (RFC 9177 §6.2
        // MAX_PAYLOADS); a sanity check that the type resolved, not a contract.
        assert_eq!(cfg.max_payloads, 10);
    }
}

#[cfg(test)]
mod qblock_concurrency_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    struct BodyEcho;

    #[async_trait]
    impl WireHandler for BodyEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            WireResponse {
                status: 200,
                headers: vec![],
                body: req.body,
                ..Default::default()
            }
        }
    }

    // Overlapping send_qblock calls through one pooled client must each receive
    // their own response. Q-Block adds background drive tasks + Request-Tag demux
    // on top of token correlation; this is the gate deciding pool-vs-fresh-client.
    // Bodies are sized (with a 64 B block) to span
    // several Q-Block blocks each, so the test exercises *multi-block burst*
    // correlation under concurrency — not just the single-PDU case.
    #[tokio::test]
    async fn concurrent_qblock_requests_correlate_to_their_own_responses() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            CoapWireServer::with_qblock(addr, coap::qblock::QBlockConfig::default())
                .serve(Arc::new(BodyEcho), server_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 64 B blocks so the bodies below (256–496 B) are each ≥4 blocks → genuine
        // multi-block Q-Block1 up + Q-Block2 down, concurrently, through one client.
        let client = Arc::new(CoapWireClient::with_qblock(
            Some(64),
            coap::qblock::QBlockConfig::default(),
        ));
        let mut handles = Vec::new();
        for i in 0u8..16 {
            let client = client.clone();
            let dest = addr.to_string();
            handles.push(tokio::spawn(async move {
                // Distinct byte value AND distinct length per request, so a
                // cross-wired response is caught by content or by length.
                let body = vec![i; 256 + i as usize * 16];
                let resp = client
                    .send(WireRequest {
                        dest,
                        method: Method::PUT,
                        path: format!("/_matrix/federation/v1/send/txn{i}"),
                        headers: vec![],
                        body: body.clone(),
                        ..Default::default()
                    })
                    .await
                    .expect("concurrent qblock send");
                (resp, body)
            }));
        }
        for h in handles {
            let (resp, expected) = h.await.expect("task");
            assert_eq!(resp.status, 200);
            assert_eq!(
                resp.body, expected,
                "a concurrent qblock request received the wrong response body"
            );
        }

        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(
            matches!(joined, Ok(Ok(Ok(())))),
            "qblock server did not wind down cleanly on cancel: {joined:?}"
        );
    }
}
