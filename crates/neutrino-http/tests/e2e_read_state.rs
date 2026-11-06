//! End-to-end tests for own-user receipts, read markers and room account data:
//! the write routes, and their echo through sliding sync.
//!
//! Gated off under `multi-user-shim` for the same reason as
//! `e2e_sliding_sync.rs`: setup goes through tokenless CSAPI.
#![cfg(not(feature = "multi-user-shim"))]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_ctl::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

const SYNC_PATH: &str = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync";
const ALICE: &str = "@alice:example.org";

async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().expect("create storage tempdir");
    let cfg = Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
        storage_dir: tmp.path().to_path_buf(),
        ..Default::default()
    };
    let store = std::sync::Arc::new(
        neutrino_store_sqlite::SqliteStore::open_in_dir(&cfg.storage_dir)
            .await
            .expect("open store"),
    );
    let app = router(cfg, store).await.expect("router");
    (app, tmp)
}

async fn request(app: &axum::Router, method: &str, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
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

/// A room with two messages; returns `(room_id, older, newer)`.
async fn room_with_two_messages(app: &axum::Router) -> (String, String, String) {
    let (_, body) = request(app, "POST", "/_matrix/client/v3/createRoom", &json!({})).await;
    let room = body["room_id"].as_str().unwrap().to_owned();
    let mut ids = Vec::new();
    for txn in ["1", "2"] {
        let path = format!("/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}");
        let (status, body) = request(
            app,
            "PUT",
            &path,
            &json!({"msgtype": "m.text", "body": txn}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        ids.push(body["event_id"].as_str().unwrap().to_owned());
    }
    let newer = ids.pop().unwrap();
    let older = ids.pop().unwrap();
    (room, older, newer)
}

async fn post_receipt(
    app: &axum::Router,
    room: &str,
    receipt_type: &str,
    event_id: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let path = format!("/_matrix/client/v3/rooms/{room}/receipt/{receipt_type}/{event_id}");
    request(app, "POST", &path, body).await
}

async fn post_read_markers(app: &axum::Router, room: &str, body: &Value) -> StatusCode {
    let path = format!("/_matrix/client/v3/rooms/{room}/read_markers");
    request(app, "POST", &path, body).await.0
}

fn sync_body(conn_id: &str) -> Value {
    json!({
        "conn_id": conn_id,
        "lists": {"all": {"ranges": [[0, 99]], "timeline_limit": 1, "required_state": []}},
        "extensions": {"receipts": {"enabled": true}, "account_data": {"enabled": true}},
    })
}

async fn sync(app: &axum::Router, conn_id: &str, query: Option<&str>) -> Value {
    let uri = match query {
        Some(q) => format!("{SYNC_PATH}?{q}"),
        None => SYNC_PATH.to_owned(),
    };
    let (status, body) = request(app, "POST", &uri, &sync_body(conn_id)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// The `m.receipt` event for `room` in a sync response.
fn receipt_event<'a>(sync: &'a Value, room: &str) -> &'a Value {
    &sync["extensions"]["receipts"]["rooms"][room]
}

/// Alice's receipt of `receipt_type` on `event_id`, if any.
fn own_receipt<'a>(sync: &'a Value, room: &str, event_id: &str, receipt_type: &str) -> &'a Value {
    &receipt_event(sync, room)["content"][event_id][receipt_type][ALICE]
}

/// The content of the room account data event of `data_type`, if any.
fn account_data<'a>(sync: &'a Value, room: &str, data_type: &str) -> Option<&'a Value> {
    sync["extensions"]["account_data"]["rooms"][room]
        .as_array()?
        .iter()
        .find(|e| e["type"] == data_type)
        .map(|e| &e["content"])
}

fn fully_read(sync: &Value, room: &str) -> Option<String> {
    account_data(sync, room, "m.fully_read")?["event_id"]
        .as_str()
        .map(str::to_owned)
}

// Complement TestRoomReceipts, Synapse test_send_receipt.
#[tokio::test]
async fn receipt_comes_back_in_sync_without_room_id() {
    let (app, _tmp) = test_router().await;
    let (room, _, newer) = room_with_two_messages(&app).await;

    let (status, body) = post_receipt(&app, &room, "m.read", &newer, &json!({})).await;
    assert_eq!((status, body), (StatusCode::OK, json!({})));

    let sync = sync(&app, "c", None).await;
    assert_eq!(receipt_event(&sync, &room)["type"], "m.receipt");
    assert!(own_receipt(&sync, &room, &newer, "m.read")["ts"].is_u64());
    assert!(
        receipt_event(&sync, &room).get("room_id").is_none(),
        "receipts down sync carry no room_id"
    );
}

// Synapse test_private_read_receipts: the own user sees its private receipt.
#[tokio::test]
async fn private_and_threaded_receipts_come_back_to_their_owner() {
    let (app, _tmp) = test_router().await;
    let (room, older, newer) = room_with_two_messages(&app).await;

    let (status, _) = post_receipt(&app, &room, "m.read.private", &older, &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) =
        post_receipt(&app, &room, "m.read", &newer, &json!({"thread_id": "main"})).await;
    assert_eq!(status, StatusCode::OK);

    let sync = sync(&app, "c", None).await;
    assert!(own_receipt(&sync, &room, &older, "m.read.private").is_object());
    assert_eq!(
        own_receipt(&sync, &room, &newer, "m.read")["thread_id"],
        "main"
    );
}

// Synapse test_send_receipt_unknown_event.
#[tokio::test]
async fn receipt_for_unknown_event_or_room_is_accepted_and_dropped() {
    let (app, _tmp) = test_router().await;
    let (room, _, _) = room_with_two_messages(&app).await;

    for room in [room.as_str(), "!abc:beep"] {
        let (status, _) = post_receipt(&app, room, "m.read", "$def", &json!({})).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = post_receipt(&app, room, "m.fully_read", "$def", &json!({})).await;
        assert_eq!(status, StatusCode::OK);
    }
    let sync = sync(&app, "c", None).await;
    assert!(sync["extensions"]["receipts"]["rooms"].is_null(), "{sync}");
    assert!(
        sync["extensions"]["account_data"]["rooms"].is_null(),
        "{sync}"
    );
}

// Synapse test_send_receipt_invalid_room_id / _invalid_event_id /
// _invalid_receipt_type, and the spec's 400 for a threaded m.fully_read.
#[tokio::test]
async fn malformed_receipts_are_rejected() {
    let (app, _tmp) = test_router().await;
    for (room, receipt_type, event_id, body) in [
        ("not-a-room-id", "m.read", "$def", json!({})),
        ("!abc:beep", "m.read", "not-an-event-id", json!({})),
        ("!abc:beep", "invalid-receipt-type", "$def", json!({})),
        (
            "!abc:beep",
            "m.fully_read",
            "$def",
            json!({"thread_id": "main"}),
        ),
    ] {
        let (status, _) = post_receipt(&app, room, receipt_type, event_id, &body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{room} {receipt_type} {event_id}"
        );
    }
}

// Complement TestRoomReadMarkers.
#[tokio::test]
async fn read_markers_set_receipt_and_fully_read() {
    let (app, _tmp) = test_router().await;
    let (room, _, newer) = room_with_two_messages(&app).await;

    let status = post_read_markers(
        &app,
        &room,
        &json!({"m.fully_read": newer, "m.read": newer}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let sync = sync(&app, "c", None).await;
    assert!(own_receipt(&sync, &room, &newer, "m.read").is_object());
    assert_eq!(fully_read(&sync, &room), Some(newer));
}

// Synapse test_fully_read_receipt_does_not_move_backwards_by_default,
// test_send_read_marker_does_not_move_backwards_by_default, and the
// `allow_backward`-ignored variants of both.
#[tokio::test]
async fn fully_read_never_moves_backwards() {
    let (app, _tmp) = test_router().await;
    let (room, older, newer) = room_with_two_messages(&app).await;

    let (status, _) = post_receipt(&app, &room, "m.fully_read", &newer, &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    for body in [json!({}), json!({"com.beeper.allow_backward": true})] {
        let (status, _) = post_receipt(&app, &room, "m.fully_read", &older, &body).await;
        assert_eq!(status, StatusCode::OK);
    }
    for allow_backward in [false, true] {
        let body = json!({"m.fully_read": older, "com.beeper.allow_backward": allow_backward});
        assert_eq!(post_read_markers(&app, &room, &body).await, StatusCode::OK);
    }

    assert_eq!(fully_read(&sync(&app, "c", None).await, &room), Some(newer));
}

// EX's "Mark as unread".
#[tokio::test]
async fn room_account_data_comes_back_in_sync() {
    let (app, _tmp) = test_router().await;
    let (room, _, _) = room_with_two_messages(&app).await;

    let path = format!("/_matrix/client/v3/user/{ALICE}/rooms/{room}/account_data/m.marked_unread");
    let (status, body) = request(&app, "PUT", &path, &json!({"unread": true})).await;
    assert_eq!((status, body), (StatusCode::OK, json!({})));

    let sync = sync(&app, "c", None).await;
    assert_eq!(
        account_data(&sync, &room, "m.marked_unread"),
        Some(&json!({"unread": true}))
    );
}

#[tokio::test]
async fn fully_read_cannot_be_put_as_account_data() {
    let (app, _tmp) = test_router().await;
    let (room, _, newer) = room_with_two_messages(&app).await;

    let path = format!("/_matrix/client/v3/user/{ALICE}/rooms/{room}/account_data/m.fully_read");
    let (status, body) = request(&app, "PUT", &path, &json!({"event_id": newer})).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["errcode"], "M_BAD_JSON");
}

// The badge clears only once sync echoes the receipt, and a client restart
// (a new connection) must still see it.
#[tokio::test]
async fn read_state_is_sent_once_per_connection_and_again_to_a_new_one() {
    let (app, _tmp) = test_router().await;
    let (room, older, newer) = room_with_two_messages(&app).await;
    let (status, _) = post_receipt(&app, &room, "m.read", &older, &json!({})).await;
    assert_eq!(status, StatusCode::OK);

    let first = sync(&app, "c", None).await;
    assert!(own_receipt(&first, &room, &older, "m.read").is_object());

    let pos = format!("pos={}", first["pos"].as_str().unwrap());
    let delta = sync(&app, "c", Some(&pos)).await;
    assert!(
        delta["extensions"]["receipts"]["rooms"].is_null(),
        "{delta}"
    );

    let (status, _) = post_receipt(&app, &room, "m.read", &newer, &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let pos = format!("pos={}", delta["pos"].as_str().unwrap());
    let delta = sync(&app, "c", Some(&pos)).await;
    assert!(own_receipt(&delta, &room, &newer, "m.read").is_object());

    let fresh = sync(&app, "other", None).await;
    assert!(own_receipt(&fresh, &room, &newer, "m.read").is_object());
}

#[tokio::test]
async fn receipt_wakes_a_waiting_long_poll() {
    let (app, _tmp) = test_router().await;
    let (room, _, newer) = room_with_two_messages(&app).await;
    let first = sync(&app, "c", None).await;

    let app_for_post = app.clone();
    let (room_for_post, event_for_post) = (room.clone(), newer.clone());
    let waker = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        post_receipt(
            &app_for_post,
            &room_for_post,
            "m.read",
            &event_for_post,
            &json!({}),
        )
        .await
    });

    let start = std::time::Instant::now();
    let pos = format!("pos={}&timeout=2000", first["pos"].as_str().unwrap());
    let delta = sync(&app, "c", Some(&pos)).await;
    let elapsed = start.elapsed();
    assert_eq!(waker.await.unwrap().0, StatusCode::OK);
    // Same latency reasoning as `long_poll_wakes_on_concurrent_put_event`.
    assert!(
        elapsed < std::time::Duration::from_millis(1500),
        "should wake on the receipt well before 2s; elapsed = {elapsed:?}"
    );
    assert!(own_receipt(&delta, &room, &newer, "m.read").is_object());
}
