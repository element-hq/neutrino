//! End-to-end: two `neutrino-http` homeservers, each behind its own
//! `neutrino-lb` sidecar, federate over HTTP+CBOR. Proves the CBOR transcode
//! survives real federation traffic in both directions — the `make_join` /
//! `send_join` handshake (B joins a room on A) and an outbox-driven `/send`
//! (a message on A converges to B) — with `federation_proxy` doing the routing.
//!
//! Each node is the full production stack (`neutrino_http::serve`: router +
//! outbound sender pool), driven only over its public HTTP/CSAPI surface, so
//! this needs no crate internals. Topology per node:
//!
//! ```text
//! CSAPI client ─▶ homeserver (loopback La) ─▶ egress (Ea) ═CBOR═▶ peer ingress
//! peer egress ═CBOR═▶ ingress (Ia == server_name) ─▶ homeserver (loopback La)
//! ```
#![cfg(not(feature = "multi-user-shim"))]

use std::net::SocketAddr;
use std::time::Duration;

use neutrino_common::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Reserve an ephemeral loopback port and free it, so a sidecar (which binds a
/// `SocketAddr` itself) can claim it. A small bind race is unavoidable here, as
/// in `neutrino-lb`'s own tests; the readiness wait below covers it.
async fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A running homeserver + its sidecar. Holds what must stay alive for the test:
/// the command sender (dropping every sender shuts the server down), the
/// sidecar shutdown token, and the storage tempdir (dropping it deletes the DB).
struct Node {
    /// Public name peers resolve to — the sidecar ingress address.
    server_name: String,
    /// Loopback base URL of the homeserver's own HTTP, for driving CSAPI.
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _shutdown: CancellationToken,
    _tmp: tempfile::TempDir,
}

/// Stand up one node: a homeserver whose `federation_proxy` points at its
/// egress, fronted by a sidecar whose ingress is the node's `server_name`.
async fn start_node(localpart: &str) -> Node {
    // Homeserver HTTP listener (loopback), pre-bound so we know its port and
    // there is no bind race — `serve()` consumes this listener directly.
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();

    let ingress = free_port().await; // == server_name (what peers reach)
    let egress = free_port().await; // federation_proxy target (loopback)

    let tmp = tempfile::TempDir::new().unwrap();
    let server_name = ingress.to_string();
    let config = Config {
        server_name: server_name.clone(),
        // Unused: `serve()` binds the listener we pass, not `bind_addr`.
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        federation_proxy: Some(format!("http://{egress}")),
        ..Default::default()
    };

    // Full homeserver stack (router + outbound federation sender pool).
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let store = std::sync::Arc::new(
            neutrino_store_sqlite::SqliteStore::open_in_dir(&config.storage_dir)
                .await
                .unwrap(),
        );

        let _ = neutrino_http::serve(http_listener, config, store, cmd_rx, None).await;
    });

    // Sidecar: ingress on the public port, egress on loopback, upstream = the
    // homeserver's loopback HTTP.
    let shutdown = CancellationToken::new();
    let lb = neutrino_lb::LbConfig {
        ingress_bind: ingress,
        egress_bind: egress,
        upstream: format!("http://{http_addr}"),
        wire: neutrino_lb::WireKind::Http,
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
async fn message_converges_through_lb_sidecars() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;

    // Let both sidecars bind and the servers come up.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // A plain client (no ambient proxy) talking directly to each homeserver's
    // loopback CSAPI. The federation hop between the servers is what goes
    // through the sidecars; these CSAPI calls do not.
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

    // 2. B joins A's room over federation — the make_join/send_join handshake
    //    traverses B-egress → A-ingress and back, transcoded JSON↔CBOR.
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
        "federated join through sidecars failed: {join_body:?}"
    );
    assert_eq!(join_body["room_id"], room_id);

    // 3. A sends a message. A now has B (on B's server_name) as a remote member,
    //    so A's sender pool delivers it via A-egress → B-ingress.
    let send_url = format!(
        "{}/_matrix/client/v3/rooms/{}/send/m.room.message/txn1",
        a.http_base, room_id
    );
    let resp = http
        .put(&send_url)
        .json(&json!({ "msgtype": "m.text", "body": "hello via cbor" }))
        .send()
        .await
        .expect("send request");
    assert_eq!(resp.status(), 200, "send message");

    // 4. Poll B's timeline until the message converges (outbox delivery is
    //    asynchronous), or time out.
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
                    .any(|e| e["content"]["body"] == "hello via cbor")
            {
                converged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "message sent on A did not converge to B through the neutrino-lb sidecars"
    );
}
