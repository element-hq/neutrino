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

mod message;
mod paths;

/// Exact HTTP status, carried as 2 big-endian bytes (CoAP response codes are not
/// 1:1 with HTTP, and federation needs the precise code).
pub(crate) const OPT_HTTP_STATUS: u16 = 2048;
/// One forwarded header per occurrence: `name` + 0x00 + `value`.
pub(crate) const OPT_FWD_HEADER: u16 = 2050;
/// `application/cbor` (RFC 8949 §9.1).
pub(crate) const CBOR_CONTENT_FORMAT: u16 = 60;

use std::time::Duration;

use async_trait::async_trait;
use coap::UdpCoAPClient;

use crate::transport::{MAX_WIRE_BODY_BYTES, WireClient, WireError, WireRequest, WireResponse};

/// Egress wire client over CoAP/UDP. Dials `req.dest` per send; coap-rs handles
/// CON retransmit and Block1/Block2 blockwise transparently.
pub struct CoapWireClient {
    /// Max bytes per Block1 (request) chunk. `None` uses coap-rs's 1024 B
    /// default. Lower it to fit a constrained link's datagram budget. (Block2 —
    /// the response chunk size — is fixed by the responding server's coap-rs
    /// default and not yet tunable; see the module docs / `PLAN.md`.)
    block1_size: Option<usize>,
    /// Total wall-clock ceiling on a single `send` (dial + the whole blockwise
    /// exchange). coap-rs bounds each CON block by its own receive-timeout ×
    /// retries, but nothing bounds a multi-block transfer where a peer answers
    /// each block slowly-but-just-in-time — so without this a slow/black-hole
    /// peer pins the egress task and its buffered body indefinitely. Mirrors the
    /// HTTP transport's `REQUEST_TIMEOUT` rationale (UDP has no connect phase, so
    /// one total bound covers dial — incl. DNS resolve — and the exchange).
    request_timeout: Duration,
    /// Cap on a peer's assembled response body, mirroring the ingress request
    /// cap. The peer is the untrusted network side and its response is buffered
    /// then re-transcoded, so an unbounded body is an OOM surface even from a
    /// non-hostile peer. See the module OOM note.
    max_body_bytes: usize,
}

impl CoapWireClient {
    pub fn new() -> Self {
        Self {
            block1_size: None,
            request_timeout: crate::REQUEST_TIMEOUT,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
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
        let block1_size = self.block1_size;
        let max_body_bytes = self.max_body_bytes;
        let exchange = async move {
            let mut client = UdpCoAPClient::new(req.dest.as_str())
                .await
                .map_err(|e| WireError::Transport(format!("coap dial {}: {e}", req.dest)))?;
            if let Some(size) = block1_size {
                client.set_block1_size(size);
            }
            let coap_req = message::build_request(&req);
            let resp = client
                .send(coap_req)
                .await
                .map_err(|e| WireError::Transport(format!("coap send: {e}")))?;
            // Bound the peer's (post-reassembly) response before re-transcoding
            // it — the untrusted-side OOM guard, symmetric with the ingress cap.
            if resp.message.payload.len() > max_body_bytes {
                return Err(WireError::Transport(format!(
                    "peer response body exceeds {max_body_bytes}-byte cap"
                )));
            }
            Ok(message::parse_response(&resp))
        };
        // Total ceiling on the whole exchange, so a slow/black-hole peer can't
        // pin this task past `request_timeout` (the per-block coap-rs retries do
        // not bound a multi-block transfer). Dropping the future closes the socket.
        tokio::time::timeout(self.request_timeout, exchange)
            .await
            .map_err(|_| {
                WireError::Transport(format!(
                    "coap request to {dest} exceeded {:?}",
                    self.request_timeout
                ))
            })?
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
}

impl CoapWireServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self {
            bind,
            max_message_size: None,
            max_body_bytes: MAX_WIRE_BODY_BYTES,
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
            WireResponse {
                status: 413,
                headers: vec![],
                body: vec![],
            }
        } else {
            let wire_req = message::parse_request(&request);
            self.handler.handle(wire_req).await
        };
        if let Some(ref mut response) = request.response {
            message::write_response(response, &wire_resp);
        }
        request
    }
}

#[async_trait]
impl WireServer for CoapWireServer {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError> {
        let server = match self.max_message_size {
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
        let dispatch = Arc::new(CoapDispatch {
            handler,
            max_body_bytes: self.max_body_bytes,
        });

        // `coap::Server::run` has no native shutdown, so race it against the
        // token: when shutdown fires the run future is dropped, closing the
        // socket. (coap-rs blanket-impls RequestHandler for closures returning a
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
        let client = CoapWireClient {
            block1_size: Some(512),
            request_timeout: Duration::from_secs(3),
            max_body_bytes: MAX_WIRE_BODY_BYTES,
        };
        let req_body: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let result = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body,
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
            }
        }
    }

    // Many overlapping `send`s against one server must each receive *their own*
    // response, never another in-flight request's. CoAP correlates by token/
    // message-id, and each `send` here opens its own UDP socket, so a response
    // cannot be mis-delivered — this pins that property and guards against a
    // future client-pooling change quietly breaking correlation.
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
                        continue; // drop this datagram; the sender must retransmit
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
        tokio::spawn(run_lossy_relay(
            front,
            server_addr,
            drop_to_server,
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
        let client = CoapWireClient {
            block1_size: None,
            request_timeout: Duration::from_secs(15),
            max_body_bytes: MAX_WIRE_BODY_BYTES,
        };
        let resp = client
            .send(WireRequest {
                dest: relay_addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![0x5Au8; 64],
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
        let client = CoapWireClient {
            block1_size: Some(64),
            request_timeout: Duration::from_secs(15),
            max_body_bytes: MAX_WIRE_BODY_BYTES,
        };
        let resp = client
            .send(WireRequest {
                dest: relay_addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: req_body.clone(),
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
}
