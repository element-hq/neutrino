//! Ingress: the wire→local half. Implements `WireHandler` — decodes the CBOR
//! request body to JSON, hands it verbatim (method/path/forwardable headers)
//! to the homeserver's federation router, and re-encodes the JSON response to
//! CBOR. Path/method are never interpreted, so no federation routes are
//! mirrored.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::http::Request;
use tower::ServiceExt;
use tracing::warn;

use crate::capture::{CaptureControl, Leg, record_response};
use crate::codec::{cbor_to_json, json_to_cbor};
use crate::headers::{claimed_origin, is_forwardable};
use crate::transport::{WireHandler, WireRequest, WireResponse};

/// Hands transcoded requests to the homeserver.
pub struct IngressHandler {
    /// The routes peers may reach; nothing else is served.
    upstream: Router,
    /// Runtime-toggleable pcap sink, `None` when the host declared no tap. This
    /// is the peer→local leg of the capture (see [`crate::capture`]).
    capture: Option<Arc<CaptureControl>>,
}

impl IngressHandler {
    pub fn new(upstream: Router, capture: Option<Arc<CaptureControl>>) -> Self {
        Self { upstream, capture }
    }

    /// `502` with an empty body, used when transcoding or the upstream fails.
    fn bad_gateway() -> WireResponse {
        WireResponse {
            status: 502,
            headers: vec![],
            body: vec![],
            ..Default::default()
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
        // pcap tap, request half: the literal JSON about to be handed upstream.
        // The peer's `server_name` is the claimed `X-Matrix origin` — an inbound
        // `WireRequest` carries no source, so this is the only peer identity the
        // ingress has. (On the datagram transport the claim was already bound to
        // the link-authenticated node; see `Hub::origin_binding_violation`.)
        // Recorded before the upstream call, so the gap to the response half is
        // the upstream's service time.
        let exchange = self.capture.as_ref().and_then(|capture| {
            capture.record_request(
                Leg::Ingress,
                claimed_origin(&req.headers).unwrap_or("unknown"),
                req.method.as_str(),
                &req.path,
                &req.headers,
                &json_body,
            )
        });
        let mut builder = Request::builder().method(req.method).uri(&req.path);
        for (name, value) in &req.headers {
            if is_forwardable(name) {
                builder = builder.header(name.as_str(), value.as_slice());
            }
        }
        if !json_body.is_empty() {
            builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
        }
        let request = match builder.body(axum::body::Body::from(json_body)) {
            Ok(r) => r,
            Err(e) => {
                warn!(%e, path = %req.path, "ingress: unroutable request");
                record_response(&self.capture, exchange, 502, &[], b"");
                return Self::bad_gateway();
            }
        };
        // A hung handler must not pin this wire-handler task (and its buffers)
        // indefinitely.
        let call = self.upstream.clone().oneshot(request);
        let resp = match tokio::time::timeout(crate::REQUEST_TIMEOUT, call).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(infallible)) => match infallible {},
            Err(_) => {
                warn!("ingress: upstream timed out");
                record_response(&self.capture, exchange, 502, &[], b"");
                return Self::bad_gateway();
            }
        };
        let (parts, body) = resp.into_parts();
        let status = parts.status.as_u16();
        // Carry the upstream's response headers across the wire (the framing
        // ones — content-length/-type, etc. — are dropped per hop by the
        // downstream `is_forwardable` filter).
        let headers: Vec<(String, Vec<u8>)> = parts
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        let read = axum::body::to_bytes(body, usize::MAX);
        let resp_bytes = match tokio::time::timeout(crate::BODY_TIMEOUT, read).await {
            Ok(Ok(b)) => b.to_vec(),
            Ok(Err(e)) => {
                warn!(%e, "ingress: reading upstream response failed");
                record_response(&self.capture, exchange, 502, &[], b"");
                return Self::bad_gateway();
            }
            Err(_) => {
                warn!("ingress: upstream response body timed out");
                record_response(&self.capture, exchange, 502, &[], b"");
                return Self::bad_gateway();
            }
        };
        // pcap tap, response half: the upstream's literal JSON, before the
        // transcode. Recorded on the non-JSON path too — what the homeserver
        // actually returned (a framework error page, say) is the whole
        // diagnostic there, even though it is not forwarded over the wire.
        record_response(&self.capture, exchange, status, &headers, &resp_bytes);
        match json_to_cbor(&resp_bytes) {
            Ok(cbor_body) => WireResponse {
                status,
                headers,
                body: cbor_body,
                ..Default::default()
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
                    ..Default::default()
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
        let handler = IngressHandler::new(app, None);
        let cbor_in = json_to_cbor(br#"{"hello":"world"}"#).unwrap();
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/1".to_owned(),
                headers: vec![],
                body: cbor_in,
                ..Default::default()
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
        let handler = IngressHandler::new(app, None);
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/_matrix/federation/v1/backfill/!r".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
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
        let handler = IngressHandler::new(app, None);
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/_matrix/federation/v1/make_join/!r/@u".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
            })
            .await;

        assert_eq!(resp.status, 403, "4xx give-up status must survive");
        assert!(resp.body.is_empty());
    }

    // A 2xx whose body we cannot transcode is a genuine proxy failure → 502.
    #[tokio::test]
    async fn masks_2xx_with_undecodable_body_as_bad_gateway() {
        let app = Router::new().fallback(|| async { "200 but not json" });
        let handler = IngressHandler::new(app, None);
        let resp = handler
            .handle(WireRequest {
                dest: String::new(),
                method: Method::GET,
                path: "/_matrix/federation/v1/backfill/!r".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
            })
            .await;

        assert_eq!(resp.status, 502);
    }
}
