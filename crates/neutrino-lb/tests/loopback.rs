//! End-to-end: egress (forward proxy) → ingress (reverse proxy) → mock
//! upstream. Proves a JSON federation request survives the JSON→CBOR→JSON
//! round trip across both halves with method, path, and body intact.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::State;
use axum::routing::put;
use axum::{Json, Router};
use neutrino_lb::codec::cbor_to_json;
use neutrino_lb::transport::http::{HttpWireClient, HttpWireServer};
use neutrino_lb::transport::{
    DestinationResolver, DirectResolver, WireClient, WireError, WireRequest, WireResponse,
    WireServer,
};
use neutrino_lb::{egress, ingress::IngressHandler};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn free_addr() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// Wraps the real `HttpWireClient` and records the body it puts on the wire, so
/// the test can prove the egress→ingress hop actually carries CBOR (and not
/// JSON) — a no-op transcode would otherwise pass this test silently.
struct SniffingClient {
    inner: HttpWireClient,
    wire_body: Arc<Mutex<Option<Vec<u8>>>>,
}

#[async_trait]
impl WireClient for SniffingClient {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
        *self.wire_body.lock().unwrap() = Some(req.body.clone());
        self.inner.send(req).await
    }
}

#[tokio::test]
async fn json_request_survives_egress_ingress_roundtrip() {
    // 1. Mock upstream homeserver.
    let seen: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
    let seen_c = seen.clone();
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let app = Router::new()
        .route(
            "/_matrix/federation/v1/send/42",
            put(
                |State(s): State<Arc<Mutex<Option<serde_json::Value>>>>,
                 body: axum::body::Bytes| async move {
                    *s.lock().unwrap() = Some(serde_json::from_slice(&body).unwrap());
                    (
                        [
                            // Allowlisted (X-Matrix family) → must survive both hops.
                            ("x-matrix-test", "header-survives"),
                            // Not allowlisted → must be dropped by the proxy.
                            ("x-neutrino-test", "should-be-dropped"),
                        ],
                        Json(serde_json::json!({"pdus": {}})),
                    )
                },
            ),
        )
        .with_state(seen_c);
    tokio::spawn(async move { axum::serve(upstream_listener, app).await.unwrap() });

    let token = CancellationToken::new();

    // 2. Ingress on the "public" port, forwarding to the mock upstream.
    let ingress_addr = free_addr().await;
    let ingress_server = HttpWireServer::new(ingress_addr);
    let handler = Arc::new(IngressHandler::new(format!("http://{upstream_addr}"), None));
    let it = token.clone();
    let ingress_task = tokio::spawn(async move { ingress_server.serve(handler, it).await });

    // 3. Egress on a loopback port, wire client points at the wider network
    //    (here: directly at the ingress, since dest == ingress_addr).
    let egress_addr = free_addr().await;
    let wire_body: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let wire_client: Arc<dyn WireClient> = Arc::new(SniffingClient {
        inner: HttpWireClient::new(),
        wire_body: wire_body.clone(),
    });
    let et = token.clone();
    let resolver: Arc<dyn DestinationResolver> = Arc::new(DirectResolver);
    let egress_task =
        tokio::spawn(
            async move { egress::serve(egress_addr, wire_client, resolver, None, et).await },
        );

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // 4. Act as neutrino-http: send through the egress proxy to `ingress_addr`.
    neutrino_lb::install_crypto_provider();
    let http = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all(format!("http://{egress_addr}")).unwrap())
        .build()
        .unwrap();
    let resp = http
        .put(format!(
            "http://{ingress_addr}/_matrix/federation/v1/send/42"
        ))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"edus":[],"pdus":[{"type":"m.room.message"}]}"#)
        .send()
        .await
        .expect("send through proxy");

    assert_eq!(resp.status(), 200);
    // Header policy across both sidecar hops (ingress → wire → egress): an
    // allowlisted (X-Matrix family) response header survives; a non-allowlisted
    // one is dropped. Captured before the body consumes `resp`.
    let survived = resp
        .headers()
        .get("x-matrix-test")
        .map(|v| v.as_bytes().to_vec());
    let dropped = resp.headers().get("x-neutrino-test").is_some();
    // neutrino-lb's reqwest has no `json` feature (production uses `.bytes()`),
    // so decode the body the same way rather than via `resp.json()`.
    let raw = resp.bytes().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(body, serde_json::json!({"pdus": {}}));
    assert_eq!(
        *seen.lock().unwrap(),
        Some(serde_json::json!({"edus": [], "pdus": [{"type": "m.room.message"}]}))
    );

    // The bytes that crossed the egress→ingress hop must be CBOR, not JSON: a
    // no-op (identity) transcode would leave JSON on the wire and fail here.
    let on_wire = wire_body
        .lock()
        .unwrap()
        .clone()
        .expect("wire body captured");
    assert!(
        serde_json::from_slice::<serde_json::Value>(&on_wire).is_err(),
        "wire body parsed as JSON — transcode did not run"
    );
    let decoded = cbor_to_json(&on_wire).expect("wire body must be valid CBOR");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&decoded).unwrap(),
        serde_json::json!({"edus": [], "pdus": [{"type": "m.room.message"}]}),
    );

    assert_eq!(
        survived.as_deref(),
        Some(b"header-survives".as_ref()),
        "allowlisted (X-Matrix) response header must pass through both sidecars"
    );
    assert!(
        !dropped,
        "non-allowlisted response header must be dropped by the proxy"
    );

    token.cancel();
    let _ = ingress_task.await;
    let _ = egress_task.await;
}

/// Split a pcap into its captured IPv4 frames (24-byte global header, 16-byte
/// record headers), then into (src_ip, dst_ip, tcp_payload) triples.
fn pcap_frames(bytes: &[u8]) -> Vec<([u8; 4], [u8; 4], Vec<u8>)> {
    let mut out = Vec::new();
    let mut off = 24;
    while off + 16 <= bytes.len() {
        let incl = u32::from_le_bytes(bytes[off + 8..off + 12].try_into().unwrap()) as usize;
        off += 16;
        let frame = &bytes[off..off + incl];
        out.push((
            frame[12..16].try_into().unwrap(),
            frame[16..20].try_into().unwrap(),
            frame[40..].to_vec(),
        ));
        off += incl;
    }
    out
}

/// The capture must record BOTH loopback legs of a real round trip, as HTTP
/// with the literal JSON bodies — the artifact `tcpdump -i lo` would have
/// produced on a desktop. This is the end-to-end proof that the tap is wired
/// into both halves and that nothing CBOR leaks into the file.
#[tokio::test]
async fn capture_records_both_legs_as_http_json() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let app = Router::new().fallback(put(|| async {
        Json(serde_json::json!({"pdus": {"upstream_marker": 1}}))
    }));
    tokio::spawn(async move { axum::serve(upstream_listener, app).await.unwrap() });

    let token = CancellationToken::new();
    // ONE control, threaded into both halves — one file, both legs.
    let capture = neutrino_lb::CaptureControl::new();
    let path = std::env::temp_dir().join(format!("neutrino-lb-e2e-{}.pcap", std::process::id()));
    capture.start(path.to_str().unwrap()).unwrap();

    let ingress_addr = free_addr().await;
    let ingress_server = HttpWireServer::new(ingress_addr);
    let handler = Arc::new(IngressHandler::new(
        format!("http://{upstream_addr}"),
        Some(capture.clone()),
    ));
    let it = token.clone();
    let ingress_task = tokio::spawn(async move { ingress_server.serve(handler, it).await });

    let egress_addr = free_addr().await;
    let wire_client: Arc<dyn WireClient> = Arc::new(HttpWireClient::new());
    let resolver: Arc<dyn DestinationResolver> = Arc::new(DirectResolver);
    let et = token.clone();
    let cap = capture.clone();
    let egress_task = tokio::spawn(async move {
        egress::serve(egress_addr, wire_client, resolver, Some(cap), et).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    neutrino_lb::install_crypto_provider();
    let http = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all(format!("http://{egress_addr}")).unwrap())
        .build()
        .unwrap();
    let resp = http
        .put(format!(
            "http://{ingress_addr}/_matrix/federation/v1/send/99"
        ))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            reqwest::header::AUTHORIZATION,
            r#"X-Matrix origin="a.example",destination="b.example""#,
        )
        .body(r#"{"pdus":[{"marker":"request_marker"}]}"#)
        .send()
        .await
        .expect("send through proxy");
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    token.cancel();
    let _ = ingress_task.await;
    let _ = egress_task.await;
    assert!(capture.stop(), "capture was running");

    let frames = pcap_frames(&std::fs::read(&path).unwrap());
    std::fs::remove_file(&path).ok();
    let text: String = frames
        .iter()
        .map(|(_, _, p)| String::from_utf8_lossy(p).into_owned())
        .collect();

    // Both legs, as HTTP.
    assert_eq!(
        text.matches("PUT /_matrix/federation/v1/send/99 HTTP/1.1")
            .count(),
        2,
        "the request must appear on both the egress and ingress legs:\n{text}"
    );
    assert_eq!(
        text.matches("HTTP/1.1 200 OK").count(),
        2,
        "the response must appear on both legs:\n{text}"
    );
    // Literal JSON, both directions — no CBOR, no re-transcode.
    assert!(
        text.contains(r#"{"pdus":[{"marker":"request_marker"}]}"#),
        "the request JSON must be byte-exact:\n{text}"
    );
    assert!(
        text.contains("upstream_marker"),
        "response JSON must appear"
    );
    // The credential survives into the capture (the reason to look at one).
    assert!(text.contains(r#"X-Matrix origin="a.example""#));

    // Direction identifies the leg: egress runs us(10.0.0.1) -> peer, ingress
    // runs peer -> us. Both must be present.
    let us = [10u8, 0, 0, 1];
    assert!(
        frames.iter().any(|(s, _, p)| *s == us && !p.is_empty()),
        "egress leg (us -> peer) missing"
    );
    assert!(
        frames.iter().any(|(_, d, p)| *d == us && !p.is_empty()),
        "ingress leg (peer -> us) missing"
    );
}
