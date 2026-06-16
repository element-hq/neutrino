//! v1 HTTP+CBOR wire transport. `HttpWireClient` (egress→peer) and
//! `HttpWireServer` (peer→ingress). The CoAP/UDP transport will be a sibling
//! module selected in `crate::serve`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::any;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::headers::is_forwardable;
use crate::transport::{WireClient, WireError, WireHandler, WireRequest, WireResponse, WireServer};

/// Marks transcoded bodies on the wire.
const CBOR_CONTENT_TYPE: &str = "application/cbor";

/// reqwest-backed wire client. Sends `req.body` (CBOR) to `http://{dest}{path}`.
pub struct HttpWireClient {
    http: reqwest::Client,
}

impl HttpWireClient {
    pub fn new() -> Self {
        Self::with_timeouts(crate::CONNECT_TIMEOUT, crate::REQUEST_TIMEOUT)
    }

    fn with_timeouts(connect: Duration, request: Duration) -> Self {
        // Direct connections only: a trusted mesh resolves peers to raw
        // IP:port, so bypass any ambient proxy. Timeouts bound the real
        // network leg — the homeserver's own timeout only covers its loopback
        // hop to the egress, so without these a dead peer leaks this request.
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(connect)
            .timeout(request)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http }
    }
}

impl Default for HttpWireClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WireClient for HttpWireClient {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
        let url = format!("http://{}{}", req.dest, req.path);
        let mut rb = self.http.request(req.method, &url);
        for (name, value) in &req.headers {
            if is_forwardable(name) {
                rb = rb.header(name.as_str(), value.as_slice());
            }
        }
        rb = rb.header(reqwest::header::CONTENT_TYPE, CBOR_CONTENT_TYPE);
        let resp = rb
            .body(req.body)
            .send()
            .await
            .map_err(|e| WireError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| WireError::Transport(e.to_string()))?
            .to_vec();
        Ok(WireResponse {
            status,
            headers,
            body,
        })
    }
}

/// reqwest/axum-backed wire server. Binds the public federation port and routes
/// every inbound request (any method, any path — a `fallback`, not a route
/// table) to the supplied `WireHandler`.
pub struct HttpWireServer {
    bind: SocketAddr,
}

impl HttpWireServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }
}

#[async_trait]
impl WireServer for HttpWireServer {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError> {
        let app = Router::new().fallback(any(dispatch)).with_state(handler);
        let listener = TcpListener::bind(self.bind)
            .await
            .map_err(|e| WireError::Serve(e.to_string()))?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .map_err(|e| WireError::Serve(e.to_string()))?;
        Ok(())
    }
}

/// Catch-all: build a `WireRequest` from the inbound HTTP request (the body is
/// CBOR), call the handler, write its `WireResponse` back out as CBOR.
async fn dispatch(State(handler): State<Arc<dyn WireHandler>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| parts.uri.path().to_owned());
    let headers = parts
        .headers
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
        .collect();
    // No body-size cap (`usize::MAX`): this is the public peer-facing port, so
    // an unbounded body is a memory-exhaustion surface. Neutrino deliberately
    // ignores this — it runs on a trusted network and assumes peers are
    // well-behaved (the same assumption that lets us skip signatures and auth,
    // per `neutrino/CLAUDE.md`). A future hostile-peer transport would add a
    // cap (and the CoAP/UDP transport bounds bodies via blockwise framing).
    let body = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            warn!(%e, "failed to read wire request body");
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    let wire_resp = handler
        .handle(WireRequest {
            dest: String::new(),
            method: parts.method,
            path,
            headers,
            body,
        })
        .await;
    build_response(wire_resp)
}

/// Build an axum `Response` from a `WireResponse`, copying forwardable headers
/// and tagging the CBOR body.
fn build_response(wire: WireResponse) -> Response {
    let mut builder = Response::builder().status(wire.status);
    for (name, value) in &wire.headers {
        if !is_forwardable(name) {
            continue;
        }
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value),
        ) {
            builder = builder.header(n, v);
        }
    }
    builder = builder.header(axum::http::header::CONTENT_TYPE, "application/cbor");
    builder
        .body(axum::body::Body::from(wire.body))
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY))
}

fn error_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(axum::body::Body::empty())
        .expect("static empty response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use axum::body::Bytes;
    use axum::http::Method;
    use axum::routing::{get, put};
    use axum::serve;

    // Minimal upstream that echoes the request body and a header back, so the
    // client's request construction (method, path, body, header pass-through)
    // is observable.
    #[tokio::test]
    async fn client_sends_method_path_body_and_forwardable_headers() {
        let app = Router::new().route(
            "/_matrix/federation/v1/send/abc",
            put(|headers: axum::http::HeaderMap, body: Bytes| async move {
                let auth = headers
                    .get("authorization")
                    .map(|v| v.to_str().unwrap_or("").to_owned())
                    .unwrap_or_default();
                let ct = headers
                    .get("content-type")
                    .map(|v| v.to_str().unwrap_or("").to_owned())
                    .unwrap_or_default();
                Json(serde_json::json!({
                    "auth": auth,
                    "content_type": ct,
                    "body": String::from_utf8_lossy(&body),
                }))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, app).await.unwrap() });

        let client = HttpWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/abc".to_owned(),
                headers: vec![
                    (
                        "Authorization".to_owned(),
                        b"X-Matrix origin=\"a\"".to_vec(),
                    ),
                    // Must be stripped — proves the client honours the filter.
                    ("Content-Length".to_owned(), b"999".to_vec()),
                ],
                body: b"CBORBYTES".to_vec(),
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["auth"], "X-Matrix origin=\"a\"");
        assert_eq!(v["content_type"], "application/cbor");
        assert_eq!(v["body"], "CBORBYTES");
    }

    // A peer that never answers in time must surface as a Transport error, not
    // hang forever — the request timeout bounds the real network leg.
    #[tokio::test]
    async fn client_times_out_on_a_slow_peer() {
        let app = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "too late"
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, app).await.unwrap() });

        let client =
            HttpWireClient::with_timeouts(Duration::from_millis(500), Duration::from_millis(150));
        let err = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::GET,
                path: "/slow".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await
            .expect_err("slow peer must time out");
        assert!(matches!(err, WireError::Transport(_)), "got {err:?}");
    }

    // A WireHandler that echoes the request body and reports the seen path.
    struct EchoHandler;

    #[async_trait]
    impl WireHandler for EchoHandler {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            let payload = serde_json::json!({ "path": req.path, "body": req.body });
            WireResponse {
                status: 200,
                headers: vec![],
                body: serde_json::to_vec(&payload).unwrap(),
            }
        }
    }

    #[tokio::test]
    async fn server_dispatches_to_handler_and_client_reads_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // free the port for HttpWireServer to bind
        let server = HttpWireServer::new(addr);
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { server.serve(Arc::new(EchoHandler), server_token).await });
        // Give the listener a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = HttpWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/xyz".to_owned(),
                headers: vec![],
                body: vec![1, 2, 3],
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["path"], "/_matrix/federation/v1/send/xyz");
        assert_eq!(v["body"], serde_json::json!([1, 2, 3]));

        token.cancel();
        let _ = handle.await;
    }
}
