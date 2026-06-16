//! Egress: the local→wire half. A forward proxy. `neutrino-http`'s reqwest is
//! configured with this as its HTTP proxy, so requests arrive in absolute form
//! (`PUT http://{dest}/path`). We read `dest` from the request authority,
//! transcode the JSON body to CBOR, hand it to the `WireClient`, and re-encode
//! the CBOR response to JSON.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::any;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::codec::{cbor_to_json, json_to_cbor};
use crate::headers::is_forwardable;
use crate::transport::{WireClient, WireRequest};

/// Shared egress state: the wire client used to reach peers.
#[derive(Clone)]
struct EgressState {
    client: Arc<dyn WireClient>,
}

/// Bind the egress forward proxy on `bind` and run until `shutdown` fires.
pub async fn serve(
    bind: SocketAddr,
    client: Arc<dyn WireClient>,
    shutdown: CancellationToken,
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .fallback(any(proxy))
        .with_state(EgressState { client });
    let listener = TcpListener::bind(bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

async fn proxy(State(state): State<EgressState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    // Forward-proxy requests carry an absolute-form target, so the authority is
    // the destination server. Its absence means we were called as an origin
    // server, which is a misconfiguration.
    let Some(authority) = parts.uri.authority().map(|a| a.as_str().to_owned()) else {
        warn!(uri = %parts.uri, "egress: request missing authority (not proxied?)");
        return error_response(StatusCode::BAD_REQUEST);
    };
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
    let json_body = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            warn!(%e, "egress: failed to read request body");
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    let cbor_body = match json_to_cbor(&json_body) {
        Ok(b) => b,
        Err(e) => {
            warn!(%e, "egress: JSON request body encode failed");
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    let wire_resp = match state
        .client
        .send(WireRequest {
            dest: authority,
            method: parts.method,
            path,
            headers,
            body: cbor_body,
        })
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(%e, "egress: wire send failed");
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    let json_resp = match cbor_to_json(&wire_resp.body) {
        Ok(b) => b,
        Err(e) => {
            warn!(%e, "egress: CBOR response body decode failed");
            return error_response(StatusCode::BAD_GATEWAY);
        }
    };
    build_response(wire_resp.status, &wire_resp.headers, json_resp)
}

fn build_response(status: u16, headers: &[(String, Vec<u8>)], json_body: Vec<u8>) -> Response {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
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
    builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
    builder
        .body(axum::body::Body::from(json_body))
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
    use crate::transport::{WireError, WireResponse};
    use async_trait::async_trait;
    use std::sync::Mutex;

    // A WireClient that records the WireRequest and returns a canned CBOR body.
    struct RecordingClient {
        seen: Mutex<Option<WireRequest>>,
    }

    #[async_trait]
    impl WireClient for RecordingClient {
        async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
            let body = json_to_cbor(br#"{"pong":true}"#).unwrap();
            *self.seen.lock().unwrap() = Some(req);
            Ok(WireResponse {
                status: 200,
                headers: vec![],
                body,
            })
        }
    }

    #[tokio::test]
    async fn proxies_absolute_uri_transcoding_both_ways() {
        let client = Arc::new(RecordingClient {
            seen: Mutex::new(None),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let token = CancellationToken::new();
        let client_dyn: Arc<dyn WireClient> = client.clone();
        let server_token = token.clone();
        let handle = tokio::spawn(async move { serve(addr, client_dyn, server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Drive the egress as a forward proxy: reqwest in proxy mode emits an
        // absolute-form request to `http://peer.example/...`.
        let http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{addr}")).unwrap())
            .build()
            .unwrap();
        let resp = http
            .put("http://peer.example:8448/_matrix/federation/v1/send/9")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(r#"{"ping":true}"#)
            .send()
            .await
            .expect("proxied request");

        assert_eq!(resp.status(), 200);
        let body = resp.bytes().await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v, serde_json::json!({"pong": true}));

        // The egress saw the real destination + path, and a CBOR body.
        let seen = client.seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen.dest, "peer.example:8448");
        assert_eq!(seen.path, "/_matrix/federation/v1/send/9");
        let decoded = cbor_to_json(&seen.body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded).unwrap(),
            serde_json::json!({"ping": true})
        );

        token.cancel();
        let _ = handle.await;
    }
}
