//! End-to-end tests for `GET /_matrix/client/v3/rooms/{roomId}/messages`.
//! Drives the live router via `oneshot`. The default config user creates the
//! room and is therefore joined.
#![cfg(not(feature = "multi-user-shim"))]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_ctl::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
        ..Default::default()
    }
}

/// Build a router over a throwaway storage directory the test owns. The
/// returned `TempDir` MUST be held for the lifetime of the router — dropping
/// it deletes the database directory.
async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().expect("create storage tempdir");
    let mut cfg = config();
    cfg.storage_dir = tmp.path().to_path_buf();
    let app = router(cfg).await.expect("router");
    (app, tmp)
}

async fn post(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    drive(app, "POST", path, Some(body)).await
}

async fn put(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    drive(app, "PUT", path, Some(body)).await
}

async fn get(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    drive(app, "GET", path, None).await
}

async fn drive(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    let req = match body {
        Some(b) => {
            builder = builder.header("content-type", "application/json");
            builder
                .body(Body::from(serde_json::to_vec(b).unwrap()))
                .unwrap()
        }
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// Create a room and send `n` text messages; returns the room id.
async fn room_with_messages(app: &axum::Router, n: usize) -> String {
    let (status, body) = post(app, "/_matrix/client/v3/createRoom", &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let room_id = body["room_id"].as_str().expect("room_id").to_string();
    for i in 0..n {
        let path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn{i}");
        let (s, _) = put(
            app,
            &path,
            &json!({"msgtype": "m.text", "body": format!("msg {i}")}),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "send {i}");
    }
    room_id
}

fn chunk_len(body: &Value) -> usize {
    body["chunk"].as_array().expect("chunk array").len()
}

#[tokio::test]
async fn backward_no_from_returns_recent_newest_first() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 3).await;
    let (status, body) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("start").is_some(), "start always present");
    // chunk includes the 3 messages + create/member state events, newest-first.
    let chunk = body["chunk"].as_array().unwrap();
    assert!(chunk.len() >= 3);
    let bodies: Vec<&str> = chunk
        .iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str))
        .collect();
    assert_eq!(bodies, vec!["msg 2", "msg 1", "msg 0"], "newest-first");
}

#[tokio::test]
async fn pagination_roundtrip_via_end_token() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 5).await;
    let (s1, p1) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=2"),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(chunk_len(&p1), 2);
    let end = p1["end"]
        .as_str()
        .expect("end token when more remain")
        .to_string();
    let (s2, p2) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=2&from={end}"),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(chunk_len(&p2), 2);
    // Disjoint pages: no event id appears in both.
    let ids1: Vec<&str> = p1["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["event_id"].as_str())
        .collect();
    let ids2: Vec<&str> = p2["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["event_id"].as_str())
        .collect();
    assert!(ids1.iter().all(|id| !ids2.contains(id)), "pages disjoint");
}

#[tokio::test]
async fn forward_from_zero_oldest_first() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 3).await;
    let (status, body) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=f&from=0&limit=100"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let bodies: Vec<&str> = body["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str))
        .collect();
    assert_eq!(bodies, vec!["msg 0", "msg 1", "msg 2"], "oldest-first");
}

#[tokio::test]
async fn limit_is_capped_not_rejected() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 1).await;
    let (status, _) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=99999"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn filter_param_is_ignored() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 2).await;
    let plain = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10"),
    )
    .await;
    let filtered = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10&filter=%7B%22types%22%3A%5B%22m.room.message%22%5D%7D"),
    )
    .await;
    assert_eq!(plain.0, StatusCode::OK);
    assert_eq!(filtered.0, StatusCode::OK);
    assert_eq!(
        chunk_len(&plain.1),
        chunk_len(&filtered.1),
        "filter is a no-op"
    );
}

#[tokio::test]
async fn unknown_room_is_forbidden() {
    let (app, _tmp) = test_router().await;
    let (status, body) = get(
        &app,
        "/_matrix/client/v3/rooms/!nope:example.org/messages?dir=b",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

#[tokio::test]
async fn sync_prev_batch_works_as_messages_from() {
    // Sync-token interop (design case 5): a `prev_batch` emitted by sliding-sync
    // is the same `stream_pos` decimal that `/messages` accepts as `from`. Prove
    // the token crosses endpoints and yields the older events before the synced
    // window.
    let (app, _tmp) = test_router().await;
    // Enough messages that sliding-sync's timeline is `limited` and emits a prev_batch.
    let room = room_with_messages(&app, 6).await;

    // Sliding-sync with a small timeline_limit → room.prev_batch present.
    let sync_body = json!({
        "lists": { "all": { "ranges": [[0, 99]], "timeline_limit": 2, "required_state": [] } }
    });
    let (s, sync) = post(
        &app,
        "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
        &sync_body,
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Collect the event ids in the synced timeline window so we can prove the
    // /messages page below is disjoint (strictly older).
    let window_ids: Vec<String> = sync
        .pointer(&format!("/rooms/{room}/timeline"))
        .and_then(Value::as_array)
        .expect("synced room has a timeline")
        .iter()
        .filter_map(|e| e["event_id"].as_str().map(str::to_string))
        .collect();

    let prev_batch = sync["rooms"][&room]["prev_batch"]
        .as_str()
        .expect("limited timeline emits prev_batch")
        .to_string();

    // Use it as `from` for backward /messages — must be accepted and return older events.
    let (ms, body) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&from={prev_batch}&limit=10"),
    )
    .await;
    assert_eq!(
        ms,
        StatusCode::OK,
        "sync prev_batch accepted as /messages from"
    );
    assert!(
        chunk_len(&body) > 0,
        "older events returned before the sync window"
    );

    // The /messages page must be disjoint from the synced window: paginating
    // backward from prev_batch yields strictly older events, not the ones the
    // sync already delivered.
    let page_ids: Vec<&str> = body["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["event_id"].as_str())
        .collect();
    assert!(
        page_ids
            .iter()
            .all(|id| !window_ids.contains(&id.to_string())),
        "messages page is disjoint from the synced timeline window: \
         page={page_ids:?} window={window_ids:?}"
    );
}

/// Paginate the whole room in `dir`, following the `end` token until it is
/// absent, with a deliberately small `page` size. Returns event ids in the
/// order pages delivered them.
async fn paginate_all(app: &axum::Router, room: &str, dir: &str, page: usize) -> Vec<String> {
    let mut ids = Vec::new();
    let mut from: Option<String> = None;
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 1000, "pagination did not terminate");
        let q = match &from {
            Some(f) => {
                format!("/_matrix/client/v3/rooms/{room}/messages?dir={dir}&limit={page}&from={f}")
            }
            None => format!("/_matrix/client/v3/rooms/{room}/messages?dir={dir}&limit={page}"),
        };
        let (status, body) = get(app, &q).await;
        assert_eq!(status, StatusCode::OK);
        for e in body["chunk"].as_array().expect("chunk array") {
            if let Some(id) = e["event_id"].as_str() {
                ids.push(id.to_string());
            }
        }
        match body["end"].as_str() {
            Some(end) => from = Some(end.to_string()),
            None => break,
        }
    }
    ids
}

/// Full backward sweep in pages of 2 must reconstruct the exact same sequence
/// as a single unbounded fetch — no skipped event (gap), no repeated event
/// (overlap), order preserved. This is the headline pagination invariant.
#[tokio::test]
async fn backward_pagination_recovers_every_event_exactly_once() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 7).await;
    let (_, full) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=1000"),
    )
    .await;
    let full_ids: Vec<String> = full["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["event_id"].as_str().map(str::to_string))
        .collect();
    assert!(full_ids.len() >= 7, "create + member + 7 messages");

    let paged = paginate_all(&app, &room, "b", 2).await;
    assert_eq!(
        paged, full_ids,
        "paged backward == unbounded: no gaps, no overlap, order preserved"
    );
    let mut deduped = paged.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(deduped.len(), paged.len(), "no event returned twice");
}

/// Same property forward — the direction with the least pre-existing coverage
/// (only a single unbounded forward fetch was tested before).
#[tokio::test]
async fn forward_pagination_recovers_every_event_exactly_once() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 7).await;
    let (_, full) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=f&from=0&limit=1000"),
    )
    .await;
    let full_ids: Vec<String> = full["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["event_id"].as_str().map(str::to_string))
        .collect();
    assert!(full_ids.len() >= 7, "create + member + 7 messages");

    let paged = paginate_all(&app, &room, "f", 2).await;
    assert_eq!(
        paged, full_ids,
        "paged forward == unbounded: no gaps, no overlap, order preserved"
    );
    let mut deduped = paged.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(deduped.len(), paged.len(), "no event returned twice");
}

#[tokio::test]
async fn bad_params_are_rejected() {
    let (app, _tmp) = test_router().await;
    let room = room_with_messages(&app, 1).await;
    for q in ["dir=x", "dir=b&from=notanumber", "dir=b&limit=abc"] {
        let (status, body) = get(
            &app,
            &format!("/_matrix/client/v3/rooms/{room}/messages?{q}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query: {q}");
        assert_eq!(body["errcode"], "M_INVALID_PARAM", "query: {q}");
    }
}
