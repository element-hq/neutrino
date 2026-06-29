//! End-to-end: two `neutrino-http` homeservers, each behind its own
//! `neutrino-lb` sidecar, federate over **CoAP+CBOR/UDP using RFC 9177 Q-Block**
//! (`WireKind::CoapQBlock`). The Q-Block twin of `e2e_lb_coap_federation.rs`:
//! same topology and scenario, only the inter-sidecar wire hop uses NON-mode
//! Q-Block bursts with missing-block recovery instead of CON stop-and-wait. A
//! small `block1_size` (64 B) forces the make_join/send_join handshake across
//! many burst blocks, genuinely exercising Q-Block1 (request) + Q-Block2
//! (response) reassembly end to end.
//!
//! Lives in `neutrino-http`'s tests (not `neutrino-lb`'s) because it drives full
//! homeservers; `neutrino-lb` cannot depend on `neutrino-http`.
#![cfg(not(feature = "multi-user-shim"))]

use std::net::SocketAddr;
use std::time::Duration;

use neutrino_common::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Reserve an ephemeral loopback port and free it.
async fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A running homeserver + its Q-Block CoAP sidecar.
struct Node {
    server_name: String,
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _shutdown: CancellationToken,
    _tmp: tempfile::TempDir,
}

/// Stand up one node fronted by a `WireKind::CoapQBlock` sidecar.
async fn start_node(localpart: &str) -> Node {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();

    let ingress = free_port().await; // == server_name (UDP, what peers reach)
    let egress = free_port().await; // federation_proxy target (loopback HTTP)

    let tmp = tempfile::TempDir::new().unwrap();
    let server_name = ingress.to_string();
    let config = Config {
        server_name: server_name.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        federation_proxy: Some(format!("http://{egress}")),
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let store = std::sync::Arc::new(
            neutrino_store_sqlite::SqliteStore::open_in_dir(&config.storage_dir)
                .await
                .unwrap(),
        );

        let _ = neutrino_http::serve(http_listener, config, store, cmd_rx).await;
    });

    let shutdown = CancellationToken::new();
    let lb = neutrino_lb::LbConfig {
        ingress_bind: ingress,
        egress_bind: egress,
        upstream: format!("http://{http_addr}"),
        // Small Q-Block blocks so the handshake spans many burst blocks and
        // genuinely exercises Q-Block1/Q-Block2 reassembly (defaults would fit an
        // empty room's state in one datagram). RFC 9177 default timing.
        wire: neutrino_lb::WireKind::CoapQBlock {
            block1_size: Some(64),
            qblock: neutrino_lb::QBlockTuning::default(),
        },
        resolver: None,
        link: None,
    };
    let lb_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = neutrino_lb::serve(lb, lb_shutdown).await;
    });

    Node {
        server_name,
        http_base: format!("http://{http_addr}"),
        _cmd_tx: cmd_tx,
        _shutdown: shutdown,
        _tmp: tmp,
    }
}

#[tokio::test]
async fn message_converges_through_qblock_sidecars() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;

    tokio::time::sleep(Duration::from_millis(250)).await;

    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // 1. A creates a public room.
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", a.http_base))
        .json(&json!({ "preset": "public_chat" }))
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let body: Value = resp.json().await.unwrap();
    let room_id = body["room_id"].as_str().expect("room_id").to_owned();

    // 2. B joins A's room over federation (make_join/send_join via Q-Block).
    let join_url = format!(
        "{}/_matrix/client/v3/join/{}?server_name={}",
        b.http_base, room_id, a.server_name
    );
    let resp = http
        .post(&join_url)
        .json(&json!({}))
        .send()
        .await
        .expect("join request");
    let join_status = resp.status();
    let join_body: Value = resp.json().await.unwrap();
    assert_eq!(
        join_status, 200,
        "federated join through Q-Block sidecars failed: {join_body:?}"
    );
    assert_eq!(join_body["room_id"], room_id);

    // 3. A sends a message.
    let send_url = format!(
        "{}/_matrix/client/v3/rooms/{}/send/m.room.message/txn1",
        a.http_base, room_id
    );
    let resp = http
        .put(&send_url)
        .json(&json!({ "msgtype": "m.text", "body": "hello via qblock" }))
        .send()
        .await
        .expect("send request");
    assert_eq!(resp.status(), 200, "send message");

    // 4. Poll B's timeline until the message converges.
    let messages_url = format!(
        "{}/_matrix/client/v3/rooms/{}/messages?dir=b&limit=50",
        b.http_base, room_id
    );
    let mut converged = false;
    for _ in 0..100 {
        let resp = http
            .get(&messages_url)
            .send()
            .await
            .expect("messages request");
        if resp.status() == 200 {
            let body: Value = resp.json().await.unwrap();
            if let Some(chunk) = body["chunk"].as_array()
                && chunk
                    .iter()
                    .any(|e| e["content"]["body"] == "hello via qblock")
            {
                converged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "message sent on A did not converge to B through the Q-Block sidecars"
    );
}
