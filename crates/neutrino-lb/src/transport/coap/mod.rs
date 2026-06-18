//! v2 CoAP/UDP wire transport. `CoapWireClient` (egress→peer) and
//! `CoapWireServer` (peer→ingress), a sibling of `transport::http`, selected in
//! `crate::serve`. The codec stays opaque: this transport carries the CBOR body
//! verbatim and never inspects it.

mod message;
mod paths;

/// Exact HTTP status, carried as 2 big-endian bytes (CoAP response codes are not
/// 1:1 with HTTP, and federation needs the precise code).
pub(crate) const OPT_HTTP_STATUS: u16 = 2048;
/// One forwarded header per occurrence: `name` + 0x00 + `value`.
pub(crate) const OPT_FWD_HEADER: u16 = 2050;
/// `application/cbor` (RFC 8949 §9.1).
pub(crate) const CBOR_CONTENT_FORMAT: u16 = 60;

use async_trait::async_trait;
use coap::UdpCoAPClient;

use crate::transport::{WireClient, WireError, WireRequest, WireResponse};

/// Egress wire client over CoAP/UDP. Dials `req.dest` per send; coap-rs handles
/// CON retransmit and Block1/Block2 blockwise transparently.
pub struct CoapWireClient {
    _private: (),
}

impl CoapWireClient {
    pub fn new() -> Self {
        Self { _private: () }
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
        let client = UdpCoAPClient::new(req.dest.as_str())
            .await
            .map_err(|e| WireError::Transport(format!("coap dial {}: {e}", req.dest)))?;
        let coap_req = message::build_request(&req);
        let resp = client
            .send(coap_req)
            .await
            .map_err(|e| WireError::Transport(format!("coap send: {e}")))?;
        Ok(message::parse_response(&resp))
    }
}

use std::net::SocketAddr;
use std::sync::Arc;

use coap::Server;
use coap_lite::CoapRequest;
use tokio_util::sync::CancellationToken;

use crate::transport::{WireHandler, WireServer};

/// Ingress wire server over CoAP/UDP. Binds the public federation UDP port and
/// dispatches each inbound request to the `WireHandler`. coap-rs reassembles
/// blockwise requests and segments large responses internally.
pub struct CoapWireServer {
    bind: SocketAddr,
}

impl CoapWireServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }
}

/// Adapter so `coap::Server` can call into our `WireHandler`.
struct CoapDispatch {
    handler: Arc<dyn WireHandler>,
}

impl CoapDispatch {
    async fn handle(
        &self,
        mut request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        let wire_req = message::parse_request(&request);
        let wire_resp = self.handler.handle(wire_req).await;
        // `from_packet` populates `response` for any valid request; if absent
        // (malformed inbound packet) coap-rs answers with its default.
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
        let server = Server::new_udp(self.bind)
            .map_err(|e| WireError::Serve(format!("bind {}: {e}", self.bind)))?;
        let dispatch = Arc::new(CoapDispatch { handler });

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
