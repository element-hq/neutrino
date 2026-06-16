//! In-process composition: `neutrino_main::entrypoint` runs a `neutrino-lb`
//! sidecar alongside the homeserver, driven entirely by `Config`
//! (`lb_ingress_bind` + `federation_proxy`) — the embedded-on-mobile topology,
//! the in-process analogue of the legacy `DendriteService` owning the monolith.
//! Two such nodes federate over HTTP+CBOR end to end, proving the sidecar is
//! actually co-launched and wired (egress + ingress) by `entrypoint` itself,
//! not just by a hand-rolled test harness.
//!
//! Per node, all three ports are loopback/ephemeral: the homeserver bind (==
//! sidecar upstream), the ingress (== `server_name`, the public federation
//! port), and the egress (== `federation_proxy`, the loopback the homeserver
//! routes outbound through).
#![cfg(not(feature = "multi-user-shim"))]

use std::net::SocketAddr;
use std::time::Duration;

use neutrino_main::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Reserve an ephemeral loopback port and free it. A small bind race is
/// unavoidable (the server re-binds it); the readiness sleep below covers it,
/// as in `neutrino-lb`'s own tests and the `neutrino-http` e2e.
async fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// A node started via `entrypoint`. Holds what must stay alive: the command
/// sender (dropping every sender shuts the node down) and the storage tempdir.
struct Node {
    /// Public name peers resolve to — the sidecar ingress address.
    server_name: String,
    /// Loopback base URL of the homeserver's own HTTP, for driving CSAPI.
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _tmp: tempfile::TempDir,
}

async fn start_node(localpart: &str) -> Node {
    let hs = free_port().await; // homeserver loopback bind (== sidecar upstream)
    let ingress = free_port().await; // public federation port (== server_name)
    let egress = free_port().await; // loopback egress (== federation_proxy)
    let tmp = tempfile::TempDir::new().unwrap();
    let server_name = ingress.to_string();
    let config = Config {
        server_name: server_name.clone(),
        bind_addr: hs.to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        federation_proxy: Some(format!("http://{egress}")),
        lb_ingress_bind: Some(ingress.to_string()),
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _ = neutrino_main::entrypoint(config, cmd_rx).await;
    });

    Node {
        server_name,
        http_base: format!("http://{hs}"),
        _cmd_tx: cmd_tx,
        _tmp: tmp,
    }
}

#[tokio::test]
async fn two_in_process_nodes_federate_over_cbor() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;

    // Let both homeservers + sidecars bind.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Plain client (no ambient proxy) talking to each homeserver's loopback
    // CSAPI directly. Only the server↔server federation hop goes through the
    // sidecars. reqwest here has no `json` feature, so bodies are sent/decoded
    // as bytes.
    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // 1. A creates a public room (so B can join by server-name hint, no invite).
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", a.http_base))
        .header("content-type", "application/json")
        .body(json!({ "preset": "public_chat" }).to_string())
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let body: Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    let room_id = body["room_id"].as_str().expect("room_id").to_owned();

    // 2. B joins A's room over federation — make_join/send_join traverse
    //    B-egress → A-ingress and back, transcoded JSON↔CBOR by the sidecars.
    let join_url = format!(
        "{}/_matrix/client/v3/join/{}?server_name={}",
        b.http_base, room_id, a.server_name
    );
    let resp = http
        .post(&join_url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("join request");
    let join_status = resp.status();
    let join_body: Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
    assert_eq!(
        join_status, 200,
        "federated join through in-process sidecars failed: {join_body:?}"
    );

    // 3. A sends a message; A's sender pool delivers it via A-egress → B-ingress.
    let send_url = format!(
        "{}/_matrix/client/v3/rooms/{}/send/m.room.message/txn1",
        a.http_base, room_id
    );
    let resp = http
        .put(&send_url)
        .header("content-type", "application/json")
        .body(json!({ "msgtype": "m.text", "body": "hello in-process cbor" }).to_string())
        .send()
        .await
        .expect("send request");
    assert_eq!(resp.status(), 200, "send message");

    // 4. Poll B's timeline until the message converges (delivery is async).
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
            let body: Value = serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap();
            if let Some(chunk) = body["chunk"].as_array()
                && chunk
                    .iter()
                    .any(|e| e["content"]["body"] == "hello in-process cbor")
            {
                converged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "message sent on A did not converge to B through the in-process neutrino-lb sidecars"
    );
}
