//! End-to-end: two `neutrino-http` homeservers federate directly over HTTP and
//! exercise the *outbound backfill* path. A builds timeline history BEFORE B
//! joins, so B never receives those messages forward (no `/send` for them) — B
//! can only obtain them by backfilling from A on a backward `/messages` page.
//!
//! Proves the whole chain (Tasks 1-7): a `dir=b` page on B that underflows its
//! local history, with B holding a backward extremity (its join event's
//! `prev_events` point at A's unheld history) and A as the only other joined
//! server, triggers a synchronous outbound backfill round (Task 7) that fetches
//! A's pre-join messages and serves them newest-first.
//!
//! Each node is the full production stack (`neutrino_http::serve`: router +
//! outbound sender pool), driven only over its public HTTP/CSAPI surface, so
//! this needs no crate internals. Unlike `e2e_lb_federation`, there are no LB
//! sidecars: with `federation_proxy: None` the outbound resolver maps a peer's
//! `server_name` straight to `http://{server_name}`, so setting each node's
//! `server_name` to its own bound HTTP address makes the two reachable directly.
//!
//! ```text
//! CSAPI/S2S client ─▶ homeserver (server_name == its own HTTP addr)
//! B ──make_join/send_join──▶ A      (B joins; gets current state, not history)
//! B ──/backfill──▶ A                (driven by B's dir=b /messages underflow)
//! ```
#![cfg(not(feature = "multi-user-shim"))]

use std::time::Duration;

use neutrino_ctl::{Command, Config, DiscoveryRegistry};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// A running homeserver, reachable by peers at `server_name`. Holds what must
/// stay alive for the test: the command sender (dropping every sender shuts the
/// server down) and the storage tempdir (dropping it deletes the DB).
struct Node {
    /// Public name peers resolve to — here, the node's own HTTP address, so the
    /// direct (`http://{server_name}`) outbound resolver reaches it.
    server_name: String,
    /// Base URL of the homeserver's HTTP, for driving CSAPI.
    http_base: String,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _tmp: tempfile::TempDir,
}

/// Stand up one node: a full homeserver whose `server_name` is its own bound
/// HTTP address and whose `federation_proxy` is `None` (direct federation).
async fn start_node(localpart: &str) -> Node {
    // Bind the HTTP listener up front so we know its address (no bind race —
    // `serve()` consumes this listener directly) and can use it as server_name.
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let server_name = http_addr.to_string();

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
        // The entrypoint opens the store once and threads the live handle into
        // `serve` (mirrors `neutrino-main` and the other e2e tests).
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
            neutrino_event::Provenance::Faith,
            None,
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
async fn backfill_serves_remote_history_on_back_pagination() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;

    // Let both servers come up.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // A plain client (no ambient proxy) talking directly to each homeserver's
    // CSAPI. The federation hops (make_join/send_join, backfill) are server↔
    // server; these CSAPI calls are not.
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

    // 2. A builds timeline history BEFORE B joins. `send_join` hands the joiner
    //    only the most-recent `TIMELINE_LIMIT` (20) events as context, so to leave
    //    history B genuinely lacks we send MORE than that: m1..m30. The OLDEST
    //    (here m1..m5) fall outside that window, so B does NOT receive them on
    //    join — the only way B can ever see them is by backfilling. (B is not yet
    //    a member, so none of these federate forward either way.)
    const N: usize = 30;
    let msgs: Vec<String> = (1..=N).map(|i| format!("m{i}")).collect();
    // The oldest few — guaranteed outside the send_join timeline window — are the
    // ones whose presence proves a real backfill round happened.
    let oldest = ["m1", "m2", "m3", "m4", "m5"];
    for (i, m) in msgs.iter().enumerate() {
        let send_url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/pre{}",
            a.http_base, room_id, i
        );
        let resp = http
            .put(&send_url)
            .json(&json!({ "msgtype": "m.text", "body": m }))
            .send()
            .await
            .expect("send request");
        assert_eq!(resp.status(), 200, "send pre-join message {m}");
    }

    // 3. B joins A's room over federation (make_join/send_join). B receives the
    //    current state plus the recent timeline window, but NOT the oldest
    //    m1..m5; the oldest event B does hold points (via prev_events) at A's
    //    unheld history, opening a backward extremity.
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
    assert_eq!(join_status, 200, "federated join failed: {join_body:?}");
    assert_eq!(join_body["room_id"], room_id);

    // Helper: one `dir=b` page from B, optionally continuing from a `from` token.
    // Returns (full chunk array, `end` token if any). Each backward page that
    // underflows `limit` on a room with a backward extremity drives ONE
    // synchronous backfill round inside the handler (Task 7).
    async fn page_back(
        http: &reqwest::Client,
        base: &str,
        room: &str,
        limit: usize,
        from: Option<&str>,
    ) -> (Vec<Value>, Option<String>) {
        let mut url = format!("{base}/_matrix/client/v3/rooms/{room}/messages?dir=b&limit={limit}");
        if let Some(f) = from {
            url.push_str(&format!("&from={f}"));
        }
        let resp = http.get(&url).send().await.expect("messages request");
        assert_eq!(resp.status(), 200, "backward page");
        let body: Value = resp.json().await.unwrap();
        let chunk = body["chunk"].as_array().cloned().unwrap_or_default();
        let end = body["end"].as_str().map(|s| s.to_owned());
        (chunk, end)
    }

    // Collect the message bodies from a chunk, in chunk order, restricted to the
    // m1..mN set (so room state events are ignored).
    let msg_bodies = |chunk: &[Value]| -> Vec<String> {
        chunk
            .iter()
            .filter_map(|e| e["content"]["body"].as_str())
            .filter(|b| msgs.iter().any(|m| m == b))
            .map(|s| s.to_owned())
            .collect::<Vec<_>>()
    };
    // "m12" -> 12, for ordering assertions.
    let order_of = |m: &str| m[1..].parse::<usize>().unwrap();

    // 4. On B: a backward `/messages` page with a `limit` that exceeds B's local
    //    event count. The page underflows; B holds backward extremities (its
    //    oldest held event's prev_events point at A's unheld history) and A is the
    //    only other joined server, so the handler runs a synchronous outbound
    //    backfill round (Task 7) against A's `/backfill` responder, persists the
    //    older PDUs, and re-reads — serving A's pre-join history in the SAME page.
    //
    //    Poll-with-retry: join state propagation can race the first read, so
    //    mirror the template's robustness idiom rather than asserting on one shot.
    let mut bodies: Vec<String> = Vec::new();
    let mut converged = false;
    for _ in 0..100 {
        let (chunk, _end) = page_back(&http, &b.http_base, &room_id, 100, None).await;
        bodies = msg_bodies(&chunk);
        // m1 is OUTSIDE the send_join timeline window (it kept only the most
        // recent TIMELINE_LIMIT events), so B can only have it via backfill.
        if bodies.iter().any(|b| b == "m1") {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "B's backward /messages page never contained A's oldest pre-join \
         message m1 — backfill did not converge; saw: {bodies:?}"
    );

    // 5a. ASSERT: ALL of the oldest pre-join messages (m1..m5) — those outside the
    //     send_join window, obtainable only by backfill — are present.
    for m in oldest {
        assert!(
            bodies.iter().any(|b| b == m),
            "backfilled history is missing {m}; saw: {bodies:?}"
        );
    }
    // And the whole timeline is present, newest-first (m30, m29, …, m1).
    assert_eq!(bodies.len(), N, "expected all {N} messages: {bodies:?}");
    for w in bodies.windows(2) {
        assert!(
            order_of(&w[0]) > order_of(&w[1]),
            "page must be strictly newest-first; {} before {} in {bodies:?}",
            w[0],
            w[1]
        );
    }

    // 5b. Now that B has backfilled the history, a small-`limit` paginated walk
    //     from the head must keep descending into older history across pages,
    //     each page's `end` token continuing strictly below the previous page's
    //     oldest — validating the descending-stream_pos ordering end-to-end.
    let mut walk: Vec<String> = Vec::new();
    let mut from: Option<String> = None;
    for _ in 0..30 {
        let (chunk, end) = page_back(&http, &b.http_base, &room_id, 5, from.as_deref()).await;
        walk.extend(msg_bodies(&chunk));
        match end {
            Some(e) => from = Some(e),
            None => break,
        }
    }
    assert_eq!(
        walk.len(),
        N,
        "paginated backward walk should surface every message exactly once: {walk:?}"
    );
    for w in walk.windows(2) {
        assert!(
            order_of(&w[0]) > order_of(&w[1]),
            "paginated walk must keep descending; {} before {} in {walk:?}",
            w[0],
            w[1]
        );
    }
}
