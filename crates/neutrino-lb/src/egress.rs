//! Egress: the local→wire half. Takes the homeserver's outbound federation
//! requests in-process, each addressed by its absolute URI. It transcodes the
//! JSON body to CBOR, hands it to the `WireClient`, and re-encodes the CBOR
//! response to JSON.

use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use tracing::{debug, warn};

use crate::capture::{CaptureControl, Leg, record_response};
use crate::codec::{cbor_to_json, json_to_cbor};
use crate::headers::{GROUP_HEADER, is_forwardable};
use crate::transport::{DestinationResolver, WireClient, WireRequest};

/// Sends the homeserver's federation requests to peers over the wire.
#[derive(Clone)]
pub struct Egress {
    client: Arc<dyn WireClient>,
    resolver: Arc<dyn DestinationResolver>,
    /// Runtime-toggleable pcap sink, `None` when the host declared no tap. This
    /// is the local→peer leg of the capture (see [`crate::capture`]).
    capture: Option<Arc<CaptureControl>>,
}

impl Egress {
    pub fn new(
        client: Arc<dyn WireClient>,
        resolver: Arc<dyn DestinationResolver>,
        capture: Option<Arc<CaptureControl>>,
    ) -> Self {
        Self {
            client,
            resolver,
            capture,
        }
    }

    /// Deliver `req` to the peer named by its URI authority and return the
    /// peer's response. Every egress-internal failure is a 502, never a 4xx:
    /// the homeserver's sender drops a 4xx permanently while it retries a 5xx.
    pub async fn send(&self, req: Request<Vec<u8>>) -> Response<Vec<u8>> {
        let (mut parts, json_body) = req.into_parts();
        let Some(authority) = parts.uri.authority().map(|a| a.as_str().to_owned()) else {
            warn!(uri = %parts.uri, "egress: request has no destination authority");
            return error_response(StatusCode::BAD_GATEWAY);
        };
        // Map the destination server_name to the address actually dialled (identity
        // on a direct network; server_name → 64-char hex node id on the datagram link).
        let dest = self.resolver.resolve(authority.clone());
        debug!(%authority, %dest, "egress: forwarding federation request to resolved destination");
        let path = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str().to_owned())
            .unwrap_or_else(|| parts.uri.path().to_owned());
        let group_size = parts
            .headers
            .remove(GROUP_HEADER)
            .and_then(|v| v.to_str().ok()?.parse().ok());
        let headers: Vec<(String, Vec<u8>)> = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        // pcap tap, request half: the literal JSON our homeserver emitted, before
        // any transcode. Recorded before the send, so the gap to the response half
        // is the true wire RTT.
        let exchange = self.capture.as_ref().and_then(|capture| {
            capture.record_request(
                Leg::Egress,
                &authority,
                parts.method.as_str(),
                &path,
                &headers,
                &json_body,
            )
        });
        let cbor_body = match json_to_cbor(&json_body) {
            Ok(b) => b,
            Err(e) => {
                warn!(%e, "egress: JSON request body encode failed");
                record_response(&self.capture, exchange, 502, &[], b"");
                return error_response(StatusCode::BAD_GATEWAY);
            }
        };
        let wire_resp = match self
            .client
            .send(WireRequest {
                dest,
                method: parts.method,
                path,
                headers,
                body: cbor_body,
                group_size,
                ..Default::default()
            })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, "egress: wire send failed");
                record_response(&self.capture, exchange, 502, &[], b"");
                return error_response(StatusCode::BAD_GATEWAY);
            }
        };
        match cbor_to_json(&wire_resp.body) {
            Ok(json_resp) => {
                record_response(
                    &self.capture,
                    exchange,
                    wire_resp.status,
                    &wire_resp.headers,
                    &json_resp,
                );
                build_response(wire_resp.status, &wire_resp.headers, json_resp)
            }
            // A non-2xx whose body we can't decode must keep its status: the
            // homeserver's sender drops a 4xx but retries a 5xx, so masking the
            // real code as a generic 502 would invert that decision. Only a 2xx
            // payload we cannot deliver is a genuine proxy failure.
            Err(e) if (200..300).contains(&wire_resp.status) => {
                warn!(%e, "egress: CBOR response body decode failed");
                record_response(&self.capture, exchange, 502, &[], b"");
                error_response(StatusCode::BAD_GATEWAY)
            }
            Err(e) => {
                warn!(%e, status = wire_resp.status, "egress: undecodable error body; forwarding status without it");
                record_response(
                    &self.capture,
                    exchange,
                    wire_resp.status,
                    &wire_resp.headers,
                    b"",
                );
                build_response(wire_resp.status, &wire_resp.headers, Vec::new())
            }
        }
    }
}

fn build_response(
    status: u16,
    headers: &[(String, Vec<u8>)],
    json_body: Vec<u8>,
) -> Response<Vec<u8>> {
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
        .body(json_body)
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY))
}

fn error_response(status: StatusCode) -> Response<Vec<u8>> {
    let mut resp = Response::new(Vec::new());
    *resp.status_mut() = status;
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{DirectResolver, WireError, WireResponse};
    use async_trait::async_trait;
    use axum::http::Method;
    use std::sync::Mutex;

    // A WireClient that records the WireRequest and returns a canned CBOR body.
    #[derive(Default)]
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
                ..Default::default()
            })
        }
    }

    fn egress(client: Arc<dyn WireClient>) -> Egress {
        Egress::new(client, Arc::new(DirectResolver), None)
    }

    fn put(uri: &str, body: &[u8]) -> Request<Vec<u8>> {
        Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .body(body.to_vec())
            .unwrap()
    }

    #[tokio::test]
    async fn transcodes_both_ways() {
        let client = Arc::new(RecordingClient::default());
        let resp = egress(client.clone())
            .send(put(
                "http://peer.example:8448/_matrix/federation/v1/send/9",
                br#"{"ping":true}"#,
            ))
            .await;

        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
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
    }

    #[tokio::test]
    async fn the_group_header_becomes_the_group_size() {
        for (header, size) in [(Some("3"), Some(3)), (Some("three"), None), (None, None)] {
            let client = Arc::new(RecordingClient::default());
            let mut req = put(
                "http://peer.example:8448/_matrix/federation/v1/send/9",
                b"{}",
            );
            if let Some(group) = header {
                req.headers_mut()
                    .insert(GROUP_HEADER, HeaderValue::from_static(group));
            }
            assert_eq!(egress(client.clone()).send(req).await.status(), 200);
            let seen = client.seen.lock().unwrap().clone().unwrap();
            assert_eq!(seen.group_size, size, "{header:?}");
            assert!(
                !seen
                    .headers
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(GROUP_HEADER)),
                "{header:?}: the header must not reach the wire"
            );
        }
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
        let client = Arc::new(RecordingClient::default());
        Egress::new(client.clone(), Arc::new(RewriteResolver), None)
            .send(put(
                "http://peer.example:8448/_matrix/federation/v1/send/9",
                br#"{"ping":true}"#,
            ))
            .await;

        // The wire client dials the resolver's output, not the raw authority.
        let seen = client.seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen.dest, "rewritten[peer.example:8448]");
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
                ..Default::default()
            })
        }
    }

    // A request with no destination authority has nowhere to go. It must be
    // answered with a *retryable* 5xx, not a 4xx: the homeserver's sender drops
    // a 4xx permanently but retries a 5xx.
    #[tokio::test]
    async fn missing_authority_is_retryable_5xx_not_4xx() {
        let client = Arc::new(StatusClient {
            status: 200,
            body: json_to_cbor(b"{}").unwrap(),
        });
        let resp = egress(client)
            .send(put("/_matrix/federation/v1/send/1", b"{}"))
            .await;
        assert!(
            resp.status().is_server_error(),
            "missing authority must be a retryable 5xx, got {}",
            resp.status()
        );
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
        let resp = egress(client)
            .send(put(
                "http://peer.example:8448/_matrix/federation/v1/make_join/!r/@u",
                b"",
            ))
            .await;
        assert_eq!(resp.status(), 404, "4xx give-up status must survive");
    }

    // The mirror of the above: a *2xx* whose body isn't valid CBOR is a genuine
    // proxy failure — we can't hand back a success payload we couldn't decode —
    // so it must surface as a retryable 502, not a 2xx with a broken body.
    #[tokio::test]
    async fn masks_2xx_with_undecodable_wire_body_as_bad_gateway() {
        // 0xff is a lone CBOR "break" — guaranteed to fail decode.
        let client = Arc::new(StatusClient {
            status: 200,
            body: vec![0xff],
        });
        let resp = egress(client)
            .send(put(
                "http://peer.example:8448/_matrix/federation/v1/event/$x",
                b"",
            ))
            .await;
        assert_eq!(
            resp.status(),
            502,
            "a 2xx with an undecodable body must become a retryable 502"
        );
    }
}
