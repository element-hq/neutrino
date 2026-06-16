//! End-to-end: egress (forward proxy) → ingress (reverse proxy) → mock
//! upstream. Proves a JSON federation request survives the JSON→CBOR→JSON
//! round trip across both halves with method, path, and body intact.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::put;
use axum::{Json, Router};
use neutrino_lb::transport::WireServer;
use neutrino_lb::transport::http::{HttpWireClient, HttpWireServer};
use neutrino_lb::{egress, ingress::IngressHandler};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn free_addr() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
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
                    Json(serde_json::json!({"pdus": {}}))
                },
            ),
        )
        .with_state(seen_c);
    tokio::spawn(async move { axum::serve(upstream_listener, app).await.unwrap() });

    let token = CancellationToken::new();

    // 2. Ingress on the "public" port, forwarding to the mock upstream.
    let ingress_addr = free_addr().await;
    let ingress_server = HttpWireServer::new(ingress_addr);
    let handler = Arc::new(IngressHandler::new(format!("http://{upstream_addr}")));
    let it = token.clone();
    let ingress_task = tokio::spawn(async move { ingress_server.serve(handler, it).await });

    // 3. Egress on a loopback port, wire client points at the wider network
    //    (here: directly at the ingress, since dest == ingress_addr).
    let egress_addr = free_addr().await;
    let wire_client: Arc<dyn neutrino_lb::transport::WireClient> = Arc::new(HttpWireClient::new());
    let et = token.clone();
    let egress_task =
        tokio::spawn(async move { egress::serve(egress_addr, wire_client, et).await });

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // 4. Act as neutrino-http: send through the egress proxy to `ingress_addr`.
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
    // neutrino-lb's reqwest has no `json` feature (production uses `.bytes()`),
    // so decode the body the same way rather than via `resp.json()`.
    let raw = resp.bytes().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(body, serde_json::json!({"pdus": {}}));
    assert_eq!(
        *seen.lock().unwrap(),
        Some(serde_json::json!({"edus": [], "pdus": [{"type": "m.room.message"}]}))
    );

    token.cancel();
    let _ = ingress_task.await;
    let _ = egress_task.await;
}
