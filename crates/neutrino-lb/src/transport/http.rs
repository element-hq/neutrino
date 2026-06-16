//! v1 HTTP+CBOR wire transport. `HttpWireClient` (egress→peer) and
//! `HttpWireServer` (peer→ingress). The CoAP/UDP transport will be a sibling
//! module selected in `crate::serve`.

use async_trait::async_trait;

use crate::headers::is_forwardable;
use crate::transport::{WireClient, WireError, WireRequest, WireResponse};

/// Marks transcoded bodies on the wire.
const CBOR_CONTENT_TYPE: &str = "application/cbor";

/// reqwest-backed wire client. Sends `req.body` (CBOR) to `http://{dest}{path}`.
pub struct HttpWireClient {
    http: reqwest::Client,
}

impl HttpWireClient {
    pub fn new() -> Self {
        // Direct connections only: a trusted mesh resolves peers to raw
        // IP:port, so bypass any ambient proxy.
        let http = reqwest::Client::builder()
            .no_proxy()
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use axum::body::Bytes;
    use axum::http::Method;
    use axum::routing::put;
    use axum::{Router, serve};
    use tokio::net::TcpListener;

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
}
