//! End-to-end: two homeservers federate in **signed mode**
//! (`EventSecurity::Signed`, i.e. `trusted_network = false`). Every locally-authored event
//! is signed, every inbound event must carry a valid sender's-server
//! signature, and the join handshake co-signs through the send_join
//! round-trip. Convergence IS the assertion: with verification on at every
//! ingress (send, send_join state-DAG ingest, backfill), a single missing or
//! invalid signature drops the event and the room never converges.
//!
//! The nodes' `server_name`s are their HTTP addresses (direct federation, as
//! in `e2e_backfill`), so the node-id resolver doesn't apply; a shared
//! `KeyDirectory` maps each server name to its verify key — the test-sized
//! stand-in for a DNS/notary `KeyResolver`. It is filled after both nodes
//! bind (names are unknown until then) and strictly before any federation
//! traffic (B's join is the first).
#![cfg(not(feature = "multi-user-shim"))]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use neutrino_ctl::{Command, Config, DiscoveryRegistry};
use neutrino_event::{EventSecurity, EventSigner, KeyResolveError, KeyResolver};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Test key directory: server_name → ed25519 verify key, shared by both nodes
/// so each can verify the other's events.
struct KeyDirectory(RwLock<HashMap<String, [u8; 32]>>);

impl KeyResolver for KeyDirectory {
    fn verify_key<'a>(
        &'a self,
        server_name: &'a str,
        key_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], KeyResolveError>> + Send + 'a>> {
        let result = self
            .0
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(server_name)
            .copied()
            .ok_or_else(|| KeyResolveError {
                server_name: server_name.to_owned(),
                key_id: key_id.to_owned(),
                reason: "unknown server".to_owned(),
            });
        Box::pin(std::future::ready(result))
    }
}

struct Node {
    server_name: String,
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _tmp: tempfile::TempDir,
}

/// Stand up one signed-mode node with a caller-chosen secret.
async fn start_signed_node(
    localpart: &str,
    secret: [u8; 32],
    resolver: Arc<dyn KeyResolver>,
) -> Node {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let server_name = http_addr.to_string();
    let signer = Arc::new(EventSigner::new(&secret, server_name.clone()));

    let tmp = tempfile::TempDir::new().unwrap();
    let config = Config {
        server_name: server_name.clone(),
        // Unused: `serve()` binds the listener we pass, not `bind_addr`.
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        federation_proxy: None,
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let store = std::sync::Arc::new(
            neutrino_store_sqlite::SqliteStore::open_in_dir(&config.storage_dir)
                .await
                .unwrap(),
        );
        let _ = neutrino_http::serve(
            http_listener,
            config,
            store,
            cmd_rx,
            std::sync::Arc::new(DiscoveryRegistry::new()),
            None,
            EventSecurity::Signed { signer, resolver },
        )
        .await;
    });

    Node {
        server_name,
        http_base: format!("http://{http_addr}"),
        _cmd_tx: cmd_tx,
        _tmp: tmp,
    }
}

#[tokio::test]
async fn signed_federation_join_message_and_backfill_converge() {
    let secret_a = [1u8; 32];
    let secret_b = [2u8; 32];
    // Verify keys depend only on the secrets; the names are learned at bind.
    let key_a = EventSigner::new(&secret_a, "").public_key();
    let key_b = EventSigner::new(&secret_b, "").public_key();

    let directory = Arc::new(KeyDirectory(RwLock::new(HashMap::new())));
    let a = start_signed_node("alice", secret_a, directory.clone()).await;
    let b = start_signed_node("bob", secret_b, directory.clone()).await;
    {
        let mut map = directory.0.write().unwrap_or_else(|e| e.into_inner());
        map.insert(a.server_name.clone(), key_a);
        map.insert(b.server_name.clone(), key_b);
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // 1. A creates a public room and seeds history B will have to backfill —
    //    more than the send_join timeline window (20), so the oldest messages
    //    reach B only through the (signature-verified) backfill path.
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", a.http_base))
        .json(&json!({ "preset": "public_chat" }))
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let body: Value = resp.json().await.unwrap();
    let room_id = body["room_id"].as_str().expect("room_id").to_owned();

    const N: usize = 25;
    for i in 1..=N {
        let resp = http
            .put(format!(
                "{}/_matrix/client/v3/rooms/{}/send/m.room.message/pre{}",
                a.http_base, room_id, i
            ))
            .json(&json!({ "msgtype": "m.text", "body": format!("m{i}") }))
            .send()
            .await
            .expect("send request");
        assert_eq!(resp.status(), 200, "send pre-join message m{i}");
    }

    // 2. B joins over federation: B signs its completed join event, A
    //    verifies + co-signs it (the send_join round-trip), and B ingests the
    //    signed state DAG — every event passes B's Signed admission.
    let resp = http
        .post(format!(
            "{}/_matrix/client/v3/join/{}?server_name={}",
            b.http_base, room_id, a.server_name
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("join request");
    let join_status = resp.status();
    let join_body: Value = resp.json().await.unwrap();
    assert_eq!(
        join_status, 200,
        "signed federated join failed: {join_body:?}"
    );

    // 3. Post-join, B sends a message — it federates A-ward through A's
    //    Signed /send admission.
    let resp = http
        .put(format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/post1",
            b.http_base, room_id
        ))
        .json(&json!({ "msgtype": "m.text", "body": "from-b" }))
        .send()
        .await
        .expect("post-join send");
    assert_eq!(resp.status(), 200, "post-join send from B");

    // 4. Convergence both ways: B back-paginates until A's oldest pre-join
    //    message appears (backfilled + verified), and A's timeline shows B's
    //    message (delivered + verified).
    let fetch_bodies = |base: String, room: String| {
        let http = http.clone();
        async move {
            let resp = http
                .get(format!(
                    "{base}/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=100"
                ))
                .send()
                .await
                .expect("messages request");
            assert_eq!(resp.status(), 200, "messages page");
            let body: Value = resp.json().await.unwrap();
            body["chunk"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter_map(|e| e["content"]["body"].as_str().map(|s| s.to_owned()))
                .collect::<Vec<_>>()
        }
    };

    let mut b_sees_m1 = false;
    let mut a_sees_from_b = false;
    let mut last_b: Vec<String> = Vec::new();
    let mut last_a: Vec<String> = Vec::new();
    for _ in 0..100 {
        if !b_sees_m1 {
            last_b = fetch_bodies(b.http_base.clone(), room_id.clone()).await;
            b_sees_m1 = last_b.iter().any(|m| m == "m1");
        }
        if !a_sees_from_b {
            last_a = fetch_bodies(a.http_base.clone(), room_id.clone()).await;
            a_sees_from_b = last_a.iter().any(|m| m == "from-b");
        }
        if b_sees_m1 && a_sees_from_b {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        b_sees_m1,
        "B never backfilled A's oldest signed message m1 — signed backfill \
         did not converge; B saw: {last_b:?}"
    );
    assert!(
        a_sees_from_b,
        "A never received B's post-join signed message — signed /send \
         delivery did not converge; A saw: {last_a:?}"
    );
}
