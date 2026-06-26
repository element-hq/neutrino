//! In-process composition: `neutrino_main::entrypoint` runs a `neutrino-lb`
//! sidecar alongside the homeserver, driven entirely by `Config`
//! (`lb_federation_port`) — the embedded-on-mobile topology, the in-process
//! analogue of the legacy `DendriteService` owning the monolith. Two such nodes
//! federate over CoAP+CBOR/UDP end to end, proving the sidecar is actually
//! co-launched and wired (egress + ingress) by `entrypoint` itself, not just by
//! a hand-rolled test harness.
//!
//! Per node: the homeserver binds a loopback port (== sidecar upstream); the
//! ingress is `host(bind_addr):lb_federation_port` (== `server_name`, the public
//! federation port, UDP); the egress is an internal loopback port `entrypoint`
//! allocates itself.
#![cfg(not(feature = "multi-user-shim"))]

use std::net::SocketAddr;
use std::time::Duration;

use neutrino_main::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

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
    let tmp = tempfile::TempDir::new().unwrap();
    // The ingress is derived as `host(bind_addr):lb_federation_port`; `bind_addr`
    // and `ingress` are both loopback, so `server_name` == the derived ingress.
    let server_name = ingress.to_string();
    let config = Config {
        server_name: server_name.clone(),
        bind_addr: hs.to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        lb_federation_port: Some(ingress.port()),
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _ = neutrino_main::entrypoint(config, cmd_rx, None).await;
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

/// #11, teardown arm: when the homeserver winds down (here via
/// `Command::Shutdown`), `entrypoint`'s `select!` must cancel the sidecar token
/// and *join* the sidecar before returning `Ok` — the `DendriteService.stop()`
/// analogue. A hang in that join, or a sidecar that never releases its port, is
/// exactly what this pins. The federation test above holds the command channel
/// open for its whole life, so this path was previously unexercised.
#[tokio::test]
async fn entrypoint_tears_down_sidecar_when_homeserver_stops() {
    let hs = free_port().await;
    let ingress = free_port().await;
    let tmp = tempfile::TempDir::new().unwrap();
    let config = Config {
        server_name: ingress.to_string(),
        bind_addr: hs.to_string(),
        localpart: "alice".to_string(),
        storage_dir: tmp.path().to_path_buf(),
        lb_federation_port: Some(ingress.port()),
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    // `entrypoint`'s error is `Box<dyn Error>` (not `Send`), so map it to a
    // `String` inside the task to keep the `JoinHandle` output `Send`.
    let handle = tokio::spawn(async move {
        neutrino_main::entrypoint(config, cmd_rx, None)
            .await
            .map_err(|e| e.to_string())
    });

    // Let the homeserver + sidecar bind.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Wind the homeserver down; entrypoint must cancel + join the sidecar and
    // return Ok, not hang.
    cmd_tx.send(Command::Shutdown).expect("send shutdown");

    let res = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("entrypoint did not return within 5s of shutdown — teardown/join hung")
        .expect("entrypoint task panicked");
    assert!(res.is_ok(), "clean shutdown returned an error: {res:?}");

    // The sidecar released the public ingress port — proof it actually wound
    // down, not merely that entrypoint returned.
    // The sidecar released the public ingress port — proof it actually wound
    // down, not merely that entrypoint returned. The ingress is CoAP over UDP,
    // so this must be a UDP bind; it relies on the `coap` fork aborting its
    // listener task on shutdown (without that the UDP socket leaks — see the
    // shutdown comment in `neutrino-lb`'s `transport::coap`).
    assert!(
        tokio::net::UdpSocket::bind(ingress).await.is_ok(),
        "ingress UDP port still held after teardown — sidecar did not stop"
    );
}

/// The embedded/mobile FFI config specifically: an **empty** `server_name` (so
/// the identity is derived from the persisted secret) launched **with a handoff**
/// (so `peer_sink` is wired into the homeserver). The other tests here use a
/// concrete name and `None`, so this is the only one exercising the exact shape
/// `neutrino-ffi::start` builds. Asserts the server stays up at startup (an early
/// return would have dropped the listener — the failure mode that, on device,
/// only surfaced as a silently-swallowed error) and that the handoff publishes a
/// resolved 64-hex identity.
#[tokio::test]
async fn embedded_config_with_handoff_comes_up_and_publishes_identity() {
    let hs = free_port().await;
    let ingress = free_port().await;
    let tmp = tempfile::TempDir::new().unwrap();
    let config = Config {
        server_name: String::new(),
        bind_addr: hs.to_string(),
        localpart: "n".to_string(),
        storage_dir: tmp.path().to_path_buf(),
        lb_federation_port: Some(ingress.port()),
        ..Default::default()
    };

    let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (handoff_tx, handoff_rx) = watch::channel(None);
    let handle = tokio::spawn(async move {
        neutrino_main::entrypoint(config, cmd_rx, Some(handoff_tx))
            .await
            .map_err(|e| e.to_string())
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "embedded entrypoint exited at startup: {:?}",
        handle.await.expect("join")
    );

    let published = handoff_rx.borrow();
    let handoff = published
        .as_ref()
        .expect("entrypoint must publish the handoff once identity resolves");
    assert_eq!(
        handoff.server_name().len(),
        64,
        "derived identity should be a 64-char hex node id, got {:?}",
        handoff.server_name()
    );
    drop(published);
    handle.abort();
}

/// #11, sidecar-fails-first arm: if the public ingress port is already taken the
/// sidecar's bind fails, and `entrypoint` must surface that as an error
/// (dropping the homeserver) rather than hang on the still-running homeserver.
#[tokio::test]
async fn entrypoint_surfaces_a_sidecar_bind_failure() {
    let hs = free_port().await;
    let ingress = free_port().await;
    let tmp = tempfile::TempDir::new().unwrap();

    // Occupy the ingress port so the sidecar's bind can't succeed. The embedded
    // sidecar's ingress is CoAP over **UDP**, so the blocker must hold the UDP
    // port (a TCP listener wouldn't collide with a UDP bind).
    let _blocker = tokio::net::UdpSocket::bind(ingress).await.unwrap();

    let config = Config {
        server_name: ingress.to_string(),
        bind_addr: hs.to_string(),
        localpart: "alice".to_string(),
        storage_dir: tmp.path().to_path_buf(),
        lb_federation_port: Some(ingress.port()),
        ..Default::default()
    };

    // Keep the command sender alive so the homeserver stays up: the only way out
    // of the `select!` is the sidecar failing.
    let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        neutrino_main::entrypoint(config, cmd_rx, None),
    )
    .await
    .expect("entrypoint did not return after the sidecar failed to bind");
    assert!(
        res.is_err(),
        "a sidecar bind failure must surface as an error from entrypoint"
    );
}
