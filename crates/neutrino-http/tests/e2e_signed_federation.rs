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
//!
//! The two other tests pin the *rejection* direction (convergence alone would
//! pass even against a no-op verifier): `..._rejects_join_when_signer_key_unknown`
//! proves a resident refuses a join it cannot verify, and
//! `signed_invite_requires_invitee_co_signature` proves the outbound `/invite`
//! round-trip rejects a return the invitee server never co-signed.
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

/// Rejection direction with an **in-fixture positive control** (mirrors the
/// invite test's two-case structure): verification must *gate* ingress, not
/// merely pass valid signatures through. A hosts a room; B joins over
/// federation. First B's key is ABSENT from A's directory, so A cannot verify
/// B's `send_join` signature — the join must be refused and A must not admit B.
/// Then the SAME B's key is added and the SAME join retried — it must now
/// succeed. Same fixtures throughout, so the stage-1 failure is provably the
/// missing key (a no-op verifier would 200 stage 1; a join broken for an
/// unrelated reason — make_join, template completion, setup — would fail stage
/// 2), rather than merely "the join failed for some reason".
#[tokio::test]
async fn signed_federation_rejects_join_when_signer_key_unknown() {
    let secret_a = [3u8; 32];
    let secret_b = [4u8; 32];
    let key_a = EventSigner::new(&secret_a, "").public_key();
    let key_b = EventSigner::new(&secret_b, "").public_key();

    let directory = Arc::new(KeyDirectory(RwLock::new(HashMap::new())));
    let a = start_signed_node("alice", secret_a, directory.clone()).await;
    let b = start_signed_node("bob", secret_b, directory.clone()).await;
    // Register ONLY A's key for now. A can verify its own locally-authored
    // events, but B is unknown to A's resolver, so A cannot yet verify B's
    // send_join signature.
    {
        let mut map = directory.0.write().unwrap_or_else(|e| e.into_inner());
        map.insert(a.server_name.clone(), key_a);
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // A creates a public room B will try to join.
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", a.http_base))
        .json(&json!({ "preset": "public_chat" }))
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let body: Value = resp.json().await.unwrap();
    let room_id = body["room_id"].as_str().expect("room_id").to_owned();

    // Runs B's federated join against A (the same request both stages).
    let join = |room: String| {
        let http = http.clone();
        let base = b.http_base.clone();
        let via = a.server_name.clone();
        async move {
            http.post(format!(
                "{base}/_matrix/client/v3/join/{room}?server_name={via}"
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("join request")
        }
    };

    // Stage 1 — key absent: A's send_join admission cannot verify B → refuse.
    let resp = join(room_id.clone()).await;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert!(
        !status.is_success(),
        "join MUST be refused when the resident cannot verify the joiner's \
         signature (a no-op verifier would return 200); got {status}: {body:?}"
    );

    // ...and A must not have admitted B into the room (rejection is synchronous
    // in send_join, so B is never enqueued — a single immediate check is sound).
    let resp = http
        .get(format!(
            "{}/_matrix/client/v3/rooms/{}/members",
            a.http_base, room_id
        ))
        .send()
        .await
        .expect("members request");
    assert_eq!(resp.status(), 200, "members");
    let members: Value = resp.json().await.unwrap();
    let joined_b = members["chunk"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|e| e["state_key"].as_str() == Some(&format!("@bob:{}", b.server_name)));
    assert!(
        !joined_b,
        "B must not be a member of A's room after a rejected join: {members:?}"
    );

    // Stage 2 — positive control: register B's key and retry the SAME join.
    // Now A can verify B, so the identical handshake must succeed, proving
    // stage 1's failure was the missing key and not a broken make_join / setup.
    {
        let mut map = directory.0.write().unwrap_or_else(|e| e.into_inner());
        map.insert(b.server_name.clone(), key_b);
    }
    let resp = join(room_id.clone()).await;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        status, 200,
        "join MUST succeed once the resident can verify the joiner's signature; \
         got {status}: {body:?}"
    );
}

/// Stub invitee server for the outbound-`/invite` test: it answers the v2
/// `/invite` handshake by returning our candidate event, optionally co-signing
/// it first. `None` echoes the event verbatim (singly-signed by us — the
/// hostile/buggy invitee that never endorsed it); `Some(signer)` adds the
/// invitee server's co-signature (the honest invitee).
fn stub_invitee_router(signer: Option<Arc<EventSigner>>) -> axum::Router {
    axum::Router::new()
        .route(
            "/_matrix/federation/v2/invite/{room_id}/{event_id}",
            axum::routing::put(stub_invite_handler),
        )
        .with_state(signer)
}

async fn stub_invite_handler(
    axum::extract::State(signer): axum::extract::State<Option<Arc<EventSigner>>>,
    axum::Json(body): axum::Json<Value>,
) -> axum::Json<Value> {
    let event = &body["event"];
    let returned = match &signer {
        None => event.clone(),
        Some(s) => {
            let raw = serde_json::value::RawValue::from_string(event.to_string())
                .expect("candidate serializes");
            let mut ev = neutrino_event::event_builder::from_wire(raw, Vec::new())
                .expect("candidate parses")
                .admit_on_faith()
                .into_event();
            s.co_sign(&mut ev).expect("stub co-signs");
            serde_json::from_str(ev.raw.get()).expect("co-signed raw is JSON")
        }
    };
    axum::Json(json!({ "event": returned }))
}

/// Bind a stub invitee server on an ephemeral port and return its `server_name`
/// plus (if it co-signs) its verify key for the resident's directory.
async fn spawn_invitee_stub(signer_secret: Option<[u8; 32]>) -> (String, Option<[u8; 32]>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let name = listener.local_addr().unwrap().to_string();
    let signer = signer_secret.map(|s| Arc::new(EventSigner::new(&s, name.clone())));
    let key = signer.as_ref().map(|s| s.public_key());
    let app = stub_invitee_router(signer);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (name, key)
}

/// The outbound `/invite` round-trip exists to collect the **invitee server's**
/// co-signature; the resident must reject a return the invitee never co-signed
/// (else a hostile/buggy invitee could make us distribute an invite its server
/// never endorsed) and accept one that carries it. Both stubs return our exact
/// candidate (same event id), so only the co-signature distinguishes them —
/// this pins the co-sign gate specifically, not the id/parse checks.
#[tokio::test]
async fn signed_invite_requires_invitee_co_signature() {
    let secret_r = [5u8; 32];
    let key_r = EventSigner::new(&secret_r, "").public_key();
    let directory = Arc::new(KeyDirectory(RwLock::new(HashMap::new())));
    let r = start_signed_node("resident", secret_r, directory.clone()).await;

    // Two invitees: one that does NOT co-sign, one that does.
    let (bad_name, _) = spawn_invitee_stub(None).await;
    let (good_name, good_key) = spawn_invitee_stub(Some([6u8; 32])).await;
    {
        let mut map = directory.0.write().unwrap_or_else(|e| e.into_inner());
        // R verifies the returned event's sender signature (its own), and the
        // honest invitee's co-signature. `bad_name` is intentionally absent —
        // its return carries no signature by it at all, so verification fails
        // before any key lookup.
        map.insert(r.server_name.clone(), key_r);
        map.insert(good_name.clone(), good_key.expect("good stub co-signs"));
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();

    // R's local user creates a room to invite remote users into.
    let resp = http
        .post(format!("{}/_matrix/client/v3/createRoom", r.http_base))
        .json(&json!({ "preset": "public_chat" }))
        .send()
        .await
        .expect("createRoom request");
    assert_eq!(resp.status(), 200, "createRoom");
    let room_id = resp.json::<Value>().await.unwrap()["room_id"]
        .as_str()
        .expect("room_id")
        .to_owned();

    let invite = |target: String| {
        let http = http.clone();
        let base = r.http_base.clone();
        let room = room_id.clone();
        async move {
            http.post(format!("{base}/_matrix/client/v3/rooms/{room}/invite"))
                .json(&json!({ "user_id": target }))
                .send()
                .await
                .expect("invite request")
        }
    };

    // Invitee did not co-sign → resident refuses with a distinct error.
    let resp = invite(format!("@x:{bad_name}")).await;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        status, 502,
        "invite MUST be refused when the invitee server did not co-sign; got {status}: {body:?}"
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("co-sign")),
        "rejection must be the co-signature check, not the id/parse check: {body:?}"
    );

    // Invitee co-signed → resident accepts (proves the gate is not vacuous).
    let resp = invite(format!("@y:{good_name}")).await;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        status, 200,
        "invite MUST succeed when the invitee server co-signed; got {status}: {body:?}"
    );
}
