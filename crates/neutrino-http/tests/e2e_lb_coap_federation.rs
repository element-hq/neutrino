//! End-to-end: two `neutrino-http` homeservers, each behind its own
//! `neutrino-lb` sidecar, federate over **CoAP+CBOR/UDP** (`WireKind::Coap`).
//! The CoAP twin of `e2e_lb_federation.rs`: same topology and scenario, only the
//! inter-sidecar wire hop is CoAP over UDP instead of HTTP/TCP. Proves the
//! `make_join`/`send_join` handshake and an outbox-driven `/send` converge
//! across the CoAP transport. A deliberately small, coordinated CoAP budget
//! (128 B block / 512 B message, set per node below) forces the handshake to
//! cross Block1/Block2 boundaries, so this genuinely exercises blockwise
//! reassembly end to end (the defaults would fit an empty room's state in a
//! single ~1 KiB datagram and never run the blockwise path).
//!
//! Lives in `neutrino-http`'s tests (not `neutrino-lb`'s) because it drives full
//! homeservers; `neutrino-lb` cannot depend on `neutrino-http` (that crate
//! depends on it). Only the ingress hop is UDP; egress stays a loopback HTTP
//! forward proxy that the homeserver's reqwest targets.
#![cfg(not(feature = "multi-user-shim"))]

use std::net::SocketAddr;
use std::time::Duration;

use neutrino_common::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Reserve an ephemeral loopback port and free it. Used for the egress (TCP
/// loopback) and to pick the ingress port number (the CoAP server binds UDP on
/// it); the readiness wait below covers the small bind race.
async fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A running homeserver + its CoAP sidecar. Holds what must stay alive: the
/// command sender (dropping it shuts the server down), the sidecar shutdown
/// token, and the storage tempdir (dropping it deletes the DB).
struct Node {
    /// Public name peers resolve to — the sidecar ingress (UDP) address.
    server_name: String,
    /// Loopback base URL of the homeserver's own HTTP, for driving CSAPI.
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _shutdown: CancellationToken,
    _tmp: tempfile::TempDir,
}

/// Stand up one node: a homeserver whose `federation_proxy` points at its
/// egress, fronted by a `WireKind::Coap` sidecar whose ingress is the node's
/// `server_name`.
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
        // Small, *coordinated* CoAP budget so the make_join/send_join handshake
        // genuinely crosses Block1/Block2 boundaries (with the defaults an empty
        // room's state fits one ~1 KiB datagram and the blockwise path never
        // runs). 128 B request blocks force multi-block Block1; the 512 B budget
        // is below the ~1.5 KiB send_join response (forcing Block2) yet well above
        // the per-block message size — each block also carries the repeated path
        // + `authorization` (X-Matrix) options (~165 B), so the budget must clear
        // `block1_size + options`, not just `block1_size` (256 B is too tight).
        wire: neutrino_lb::WireKind::Coap {
            block1_size: Some(128),
            max_message_size: Some(512),
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
async fn message_converges_through_coap_sidecars() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;

    // Let both sidecars bind their UDP listeners and the servers come up.
    tokio::time::sleep(Duration::from_millis(250)).await;

    neutrino_lb::install_crypto_provider();
    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // 1. A creates a public room (so B can join by server-name hint, no invite).
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", a.http_base))
        .json(&json!({ "preset": "public_chat" }))
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let body: Value = resp.json().await.unwrap();
    let room_id = body["room_id"].as_str().expect("room_id").to_owned();

    // 2. B joins A's room over federation — make_join/send_join traverses
    //    B-egress → A-ingress (CoAP/UDP) and back, transcoded JSON↔CBOR.
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
        "federated join through CoAP sidecars failed: {join_body:?}"
    );
    assert_eq!(join_body["room_id"], room_id);

    // 3. A sends a message; A's sender pool delivers it via A-egress → B-ingress.
    let send_url = format!(
        "{}/_matrix/client/v3/rooms/{}/send/m.room.message/txn1",
        a.http_base, room_id
    );
    let resp = http
        .put(&send_url)
        .json(&json!({ "msgtype": "m.text", "body": "hello via coap" }))
        .send()
        .await
        .expect("send request");
    assert_eq!(resp.status(), 200, "send message");

    // 4. Poll B's timeline until the message converges (async outbox delivery).
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
                    .any(|e| e["content"]["body"] == "hello via coap")
            {
                converged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "message sent on A did not converge to B through the CoAP sidecars"
    );
}
