//! Ingress: the wire→local half. Implements `WireHandler` — decodes the CBOR
//! request body to JSON, forwards it verbatim (method/path/forwardable headers)
//! to the loopback `neutrino-http` upstream, and re-encodes the JSON response
//! to CBOR. Path/method are never interpreted, so no federation routes are
//! mirrored.

use std::time::Duration;

use async_trait::async_trait;
use tracing::warn;

use crate::codec::{cbor_to_json, json_to_cbor};
use crate::headers::is_forwardable;
use crate::transport::{WireHandler, WireRequest, WireResponse};

/// Forwards transcoded requests to the local homeserver.
pub struct IngressHandler {
    http: reqwest::Client,
    /// Base URL of local `neutrino-http`, e.g. `http://127.0.0.1:8008`.
    upstream: String,
}

impl IngressHandler {
    pub fn new(upstream: String) -> Self {
        Self::with_timeouts(upstream, crate::CONNECT_TIMEOUT, crate::REQUEST_TIMEOUT)
    }

    fn with_timeouts(upstream: String, connect: Duration, request: Duration) -> Self {
        // Bound the loopback hop to `neutrino-http`: a hung upstream must not
        // pin this wire-handler task (and its buffers) indefinitely.
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(connect)
            .timeout(request)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http, upstream }
    }

    /// `502` with an empty body, used when transcoding or the upstream fails.
    fn bad_gateway() -> WireResponse {
        WireResponse {
            status: 502,
            headers: vec![],
            body: vec![],
        }
    }
}

#[async_trait]
impl WireHandler for IngressHandler {
    async fn handle(&self, req: WireRequest) -> WireResponse {
        let json_body = match cbor_to_json(&req.body) {
            Ok(b) => b,
            Err(e) => {
                warn!(%e, "ingress: CBOR request body decode failed");
                return Self::bad_gateway();
            }
        };
        let url = format!("{}{}", self.upstream, req.path);
        let mut rb = self.http.request(req.method, &url);
        for (name, value) in &req.headers {
            if is_forwardable(name) {
                rb = rb.header(name.as_str(), value.as_slice());
            }
        }
        if !json_body.is_empty() {
            rb = rb.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        let resp = match rb.body(json_body).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "ingress: upstream request failed");
                return Self::bad_gateway();
            }
        };
        let status = resp.status().as_u16();
        // Carry the upstream's response headers across the wire (the framing
        // ones — content-length/-type, etc. — are dropped per hop by the
        // downstream `is_forwardable` filter). Collected before the body is
        // consumed, mirroring `HttpWireClient::send`.
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        let resp_bytes = match resp.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => {
                warn!(%e, "ingress: reading upstream response failed");
                return Self::bad_gateway();
            }
        };
        match json_to_cbor(&resp_bytes) {
            Ok(cbor_body) => WireResponse {
                status,
                headers,
                body: cbor_body,
            },
            // A non-2xx with a non-JSON body (e.g. a framework error page) must
            // keep its status so the originating server's 4xx-give-up /
            // 5xx-retry decision survives the proxy. Only a 2xx payload we
            // cannot encode is a genuine bad-gateway.
            Err(e) if (200..300).contains(&status) => {
                warn!(%e, "ingress: 2xx JSON response body encode failed");
                Self::bad_gateway()
            }
            Err(e) => {
                warn!(%e, status, "ingress: non-JSON error body; forwarding status without it");
                WireResponse {
                    status,
                    headers,
                    body: vec![],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::WireRequest;
    use axum::extract::State;
    use axum::http::Method;
    use axum::routing::put;
    use axum::{Json, Router};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn forwards_decoded_json_to_upstream_and_recodes_response() {
        // Upstream records the JSON body it received and replies with JSON.
        let seen: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let seen_c = seen.clone();
        let app = Router::new()
            .route(
                "/_matrix/federation/v1/send/1",
                put(
                    |State(s): State<Arc<Mutex<Option<serde_json::Value>>>>,
                     body: axum::body::Bytes| async move {
                        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        *s.lock().unwrap() = Some(v);
                        Json(serde_json::json!({"ok": true}))
                    },
                ),
            )
            .with_state(seen_c);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let handler = IngressHandler::new(format!("http://{addr}"));
        let cbor_in = json_to_cbor(br#"{"hello":"world"}"#).unwrap();
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/1".to_owned(),
                headers: vec![],
                body: cbor_in,
            })
            .await;

        assert_eq!(resp.status, 200);
        assert_eq!(
            *seen.lock().unwrap(),
            Some(serde_json::json!({"hello": "world"}))
        );
        // Response body must come back as CBOR of the upstream JSON.
        let decoded = cbor_to_json(&resp.body).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(v, serde_json::json!({"ok": true}));
    }

    // Upstream response headers must be carried back across the wire, not
    // dropped — the homeserver relies on the proxy being transparent.
    #[tokio::test]
    async fn forwards_upstream_response_headers() {
        let app = Router::new().fallback(|| async {
            (
                [("x-custom-header", "via-upstream")],
                Json(serde_json::json!({"ok": true})),
            )
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let handler = IngressHandler::new(format!("http://{addr}"));
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/anything".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await;

        assert_eq!(resp.status, 200);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k.eq_ignore_ascii_case("x-custom-header") && v == b"via-upstream"),
            "upstream response header was dropped: {:?}",
            resp.headers
        );
    }

    // A non-2xx upstream response whose body is not JSON (e.g. a framework
    // error page) must keep its status. Masking it as a generic 502 would flip
    // the homeserver's "drop a 4xx / retry a 5xx" decision into a retry storm.
    #[tokio::test]
    async fn preserves_non_2xx_status_when_upstream_body_is_not_json() {
        let app = Router::new().fallback(|| async { (axum::http::StatusCode::FORBIDDEN, "no") });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let handler = IngressHandler::new(format!("http://{addr}"));
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/_matrix/federation/v1/make_join/!r/@u".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await;

        assert_eq!(resp.status, 403, "4xx give-up status must survive");
        assert!(resp.body.is_empty());
    }

    // A 2xx whose body we cannot transcode is a genuine proxy failure → 502.
    #[tokio::test]
    async fn masks_2xx_with_undecodable_body_as_bad_gateway() {
        let app = Router::new().fallback(|| async { "200 but not json" });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let handler = IngressHandler::new(format!("http://{addr}"));
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/whatever".to_owned(),
                headers: vec![],
                body: vec![],
            })
            .await;

        assert_eq!(resp.status, 502);
    }
}
