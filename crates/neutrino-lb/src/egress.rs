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
use tracing::{debug, warn};

use crate::codec::{cbor_to_json, json_to_cbor};
use crate::headers::is_forwardable;
use crate::transport::{DestinationResolver, WireClient, WireRequest};

/// Shared egress state: the wire client used to reach peers, and the resolver
/// that turns a destination `server_name` into the address to dial.
#[derive(Clone)]
struct EgressState {
    client: Arc<dyn WireClient>,
    resolver: Arc<dyn DestinationResolver>,
}

/// Bind the egress forward proxy on `bind` and run until `shutdown` fires.
pub async fn serve(
    bind: SocketAddr,
    client: Arc<dyn WireClient>,
    resolver: Arc<dyn DestinationResolver>,
    shutdown: CancellationToken,
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .fallback(any(proxy))
        .with_state(EgressState { client, resolver });
    let listener = TcpListener::bind(bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

async fn proxy(State(state): State<EgressState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    // Forward-proxy requests carry an absolute-form target, so the authority is
    // the destination server. Its absence means we were called as an origin
    // server, which is a misconfiguration — but answer 502 (like every other
    // egress-internal failure below), not 400: the homeserver's sender drops a
    // 4xx permanently while it retries a 5xx, and a recoverable misconfig must
    // not make it silently discard queued PDUs.
    let Some(authority) = parts.uri.authority().map(|a| a.as_str().to_owned()) else {
        warn!(uri = %parts.uri, "egress: request missing authority (not proxied?)");
        return error_response(StatusCode::BAD_GATEWAY);
    };
    // Map the destination server_name to the address actually dialled (identity
    // on a direct network; server_name → 64-char hex node id on the datagram link).
    let dest = state.resolver.resolve(authority.clone());
    debug!(%authority, %dest, "egress: forwarding federation request to resolved destination");
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
    // No body-size cap (`usize::MAX`): the body here is our own homeserver's
    // outbound request (loopback), so it is trusted. Neutrino assumes a trusted
    // network throughout (see the peer-facing note in `transport::http` and
    // `neutrino/CLAUDE.md`), so no limit is imposed.
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
            dest,
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
    match cbor_to_json(&wire_resp.body) {
        Ok(json_resp) => build_response(wire_resp.status, &wire_resp.headers, json_resp),
        // A non-2xx whose body we can't decode must keep its status: the
        // homeserver's sender drops a 4xx but retries a 5xx, so masking the
        // real code as a generic 502 would invert that decision. Only a 2xx
        // payload we cannot deliver is a genuine proxy failure.
        Err(e) if (200..300).contains(&wire_resp.status) => {
            warn!(%e, "egress: CBOR response body decode failed");
            error_response(StatusCode::BAD_GATEWAY)
        }
        Err(e) => {
            warn!(%e, status = wire_resp.status, "egress: undecodable error body; forwarding status without it");
            build_response(wire_resp.status, &wire_resp.headers, Vec::new())
        }
    }
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
    use crate::transport::{DirectResolver, WireError, WireResponse};
    use async_trait::async_trait;
    use std::sync::Mutex;

    // Identity resolver as a trait object, for the tests that don't rewrite.
    fn direct() -> Arc<dyn DestinationResolver> {
        Arc::new(DirectResolver)
    }

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
        let handle =
            tokio::spawn(async move { serve(addr, client_dyn, direct(), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Drive the egress as a forward proxy: reqwest in proxy mode emits an
        // absolute-form request to `http://peer.example/...`.
        crate::install_crypto_provider();
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
        // The wire body must be CBOR, not JSON — a no-op transcode would fail here.
        assert!(
            serde_json::from_slice::<serde_json::Value>(&seen.body).is_err(),
            "egress put JSON on the wire instead of CBOR"
        );
        let decoded = cbor_to_json(&seen.body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded).unwrap(),
            serde_json::json!({"ping": true})
        );

        token.cancel();
        let _ = handle.await;
    }

    // A resolver that rewrites the authority, standing in for the tunnel's
    // server_name → virtual-IP mapping.
    struct RewriteResolver;

    impl DestinationResolver for RewriteResolver {
        fn resolve(&self, authority: String) -> String {
            format!("rewritten[{authority}]")
        }
    }

    #[tokio::test]
    async fn resolver_rewrites_the_dialled_destination() {
        let client = Arc::new(RecordingClient {
            seen: Mutex::new(None),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let token = CancellationToken::new();
        let client_dyn: Arc<dyn WireClient> = client.clone();
        let resolver: Arc<dyn DestinationResolver> = Arc::new(RewriteResolver);
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { serve(addr, client_dyn, resolver, server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        crate::install_crypto_provider();
        let http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{addr}")).unwrap())
            .build()
            .unwrap();
        let _ = http
            .put("http://peer.example:8448/_matrix/federation/v1/send/9")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(r#"{"ping":true}"#)
            .send()
            .await
            .expect("proxied request");

        // The wire client dials the resolver's output, not the raw authority.
        let seen = client.seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen.dest, "rewritten[peer.example:8448]");

        token.cancel();
        let _ = handle.await;
    }

    // A WireClient that returns a chosen status and an undecodable body.
    struct StatusClient {
        status: u16,
        body: Vec<u8>,
    }

    #[async_trait]
    impl WireClient for StatusClient {
        async fn send(&self, _req: WireRequest) -> Result<WireResponse, WireError> {
            Ok(WireResponse {
                status: self.status,
                headers: vec![],
                body: self.body.clone(),
            })
        }
    }

    // Called as an origin server — a direct, non-proxied request — the egress
    // has no destination authority to forward to. It must answer a *retryable*
    // 5xx, not a 4xx: the homeserver's sender drops a 4xx permanently but
    // retries a 5xx, so a 4xx here would silently discard queued PDUs on what is
    // only a (recoverable) misconfiguration.
    #[tokio::test]
    async fn missing_authority_is_retryable_5xx_not_4xx() {
        let client = Arc::new(StatusClient {
            status: 200,
            body: json_to_cbor(b"{}").unwrap(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let token = CancellationToken::new();
        let client_dyn: Arc<dyn WireClient> = client.clone();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { serve(addr, client_dyn, direct(), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Direct (origin-form) request, NOT proxy mode: the request target has
        // no authority, so the egress is being used as an origin server.
        crate::install_crypto_provider();
        let http = reqwest::Client::builder().no_proxy().build().unwrap();
        let resp = http
            .get(format!("http://{addr}/_matrix/federation/v1/send/1"))
            .send()
            .await
            .expect("direct request");

        assert!(
            resp.status().is_server_error(),
            "missing authority must be a retryable 5xx, got {}",
            resp.status()
        );

        token.cancel();
        let _ = handle.await;
    }

    // A peer's non-2xx response whose body isn't valid CBOR must keep its
    // status, not collapse to a retryable 502.
    #[tokio::test]
    async fn preserves_non_2xx_status_when_wire_body_not_cbor() {
        // 0xff is a lone CBOR "break" — guaranteed to fail decode.
        let client = Arc::new(StatusClient {
            status: 404,
            body: vec![0xff],
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let token = CancellationToken::new();
        let client_dyn: Arc<dyn WireClient> = client.clone();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { serve(addr, client_dyn, direct(), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        crate::install_crypto_provider();
        let http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{addr}")).unwrap())
            .build()
            .unwrap();
        let resp = http
            .get("http://peer.example:8448/_matrix/federation/v1/make_join/!r/@u")
            .send()
            .await
            .expect("proxied request");

        assert_eq!(resp.status(), 404, "4xx give-up status must survive");

        token.cancel();
        let _ = handle.await;
    }

    // The mirror of the above: a *2xx* whose body isn't valid CBOR is a genuine
    // proxy failure — we can't hand back a success payload we couldn't decode —
    // so it must surface as a retryable 502, not a 2xx with a broken body.
    // Pins the `(200..300)` arm in `proxy` (egress.rs).
    #[tokio::test]
    async fn masks_2xx_with_undecodable_wire_body_as_bad_gateway() {
        // 0xff is a lone CBOR "break" — guaranteed to fail decode.
        let client = Arc::new(StatusClient {
            status: 200,
            body: vec![0xff],
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let token = CancellationToken::new();
        let client_dyn: Arc<dyn WireClient> = client.clone();
        let server_token = token.clone();
        let handle =
            tokio::spawn(async move { serve(addr, client_dyn, direct(), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        crate::install_crypto_provider();
        let http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{addr}")).unwrap())
            .build()
            .unwrap();
        let resp = http
            .get("http://peer.example:8448/_matrix/federation/v1/event/$x")
            .send()
            .await
            .expect("proxied request");

        assert_eq!(
            resp.status(),
            502,
            "a 2xx with an undecodable body must become a retryable 502"
        );

        token.cancel();
        let _ = handle.await;
    }
}
