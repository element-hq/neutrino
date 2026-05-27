//! End-to-end tests for the legacy `/_matrix/client/v3/sync` stub.
//!
//! Mirrors `tests/e2e_sliding_sync.rs`: each test builds the same axum
//! `Router` the production binary serves and drives it with
//! `tower::ServiceExt::oneshot`. Exercises:
//! - The HTTP/JSON edge in `legacy_sync::handle`.
//! - The v3 query-string → v5 request synthesis (`translate::synthesize_v5_request`).
//! - The v5 response → v3 envelope translation
//!   (`translate::translate_response`).
//! - The full `sliding_sync::handle` pipeline behind it (long-poll, pos
//!   validation, idempotency cache).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

const LEGACY_SYNC_PATH: &str = "/_matrix/client/v3/sync";

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

/// GET helper for the legacy `/sync` endpoint.
async fn get(app: &axum::Router, path: &str, query: Option<&str>) -> (StatusCode, Value) {
    let uri = match query {
        Some(q) => format!("{path}?{q}"),
        None => path.to_string(),
    };
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// POST a JSON body to `path` (createRoom etc.).
async fn post(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// PUT helper for the `/send/{type}/{txn}` endpoint.
async fn put(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

#[tokio::test]
async fn legacy_sync_returns_v3_envelope() {
    let app = router(config()).await.expect("router init");

    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);

    let obj = body.as_object().expect("top-level object");
    // The full set of top-level keys the design doc pins down.
    let expected_keys = [
        "next_batch",
        "rooms",
        "presence",
        "account_data",
        "to_device",
        "device_lists",
        "device_one_time_keys_count",
    ];
    for k in &expected_keys {
        assert!(obj.contains_key(*k), "top-level key {k:?} missing: {body}");
    }
    assert_eq!(
        obj.len(),
        expected_keys.len(),
        "no extra top-level keys: {body}",
    );

    // `rooms` carries all four buckets (empty objects on an empty sync).
    let rooms = body["rooms"].as_object().expect("rooms is an object");
    for bucket in ["join", "invite", "leave", "knock"] {
        assert!(rooms.contains_key(bucket), "rooms.{bucket} missing");
        assert!(
            rooms[bucket].is_object(),
            "rooms.{bucket} is an object: {body}",
        );
    }
}

#[tokio::test]
async fn send_event_then_legacy_sync_delivers_it_in_timeline() {
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .expect("createRoom returns a room_id")
        .to_string();

    let put_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-1");
    let (status, _) = put(
        &app,
        &put_path,
        &json!({"body": "hello legacy", "msgtype": "m.text"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);

    let room_v = body
        .pointer(&format!("/rooms/join/{}", room_id))
        .expect("room landed in rooms.join");

    // Joined-room shape per the design doc.
    assert_eq!(room_v["timeline"]["limited"], json!(false));
    assert_eq!(room_v["timeline"]["prev_batch"], json!(""));

    let timeline = room_v["timeline"]["events"]
        .as_array()
        .expect("timeline.events is an array");
    assert!(
        timeline
            .iter()
            .any(|ev| ev.pointer("/content/body").and_then(|v| v.as_str()) == Some("hello legacy")),
        "the message we PUT shows up in the legacy timeline: {timeline:?}",
    );

    // `state` and `org.matrix.msc4222.state_after` are both present and
    // carry identical content (the design doc commits to dual emission).
    let state = &room_v["state"]["events"];
    let state_after = &room_v["org.matrix.msc4222.state_after"]["events"];
    assert!(state.is_array(), "state.events present");
    assert!(state_after.is_array(), "state_after.events present");
    assert_eq!(
        state, state_after,
        "state and state_after carry identical events"
    );
}

#[tokio::test]
async fn legacy_sync_advertises_state_after_alongside_state() {
    let app = router(config()).await.expect("router init");

    // createRoom currently only honours `name` (see `create_room` in
    // lib.rs); that's enough state to verify both fields carry it.
    let (_, body) = post(
        &app,
        "/_matrix/client/v3/createRoom",
        &json!({"name": "My Legacy Room"}),
    )
    .await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);

    let room_v = body
        .pointer(&format!("/rooms/join/{}", room_id))
        .expect("room in join bucket");

    let state_events = room_v["state"]["events"]
        .as_array()
        .expect("state.events array");
    let state_after_events = room_v["org.matrix.msc4222.state_after"]["events"]
        .as_array()
        .expect("state_after.events array");

    // Identical contents.
    assert_eq!(state_events, state_after_events);

    // The name event we asked for is in there.
    let has_name = state_events.iter().any(|ev| {
        ev.get("type").and_then(|v| v.as_str()) == Some("m.room.name")
            && ev.pointer("/content/name").and_then(|v| v.as_str()) == Some("My Legacy Room")
    });
    assert!(has_name, "m.room.name event present: {state_events:?}");

    // And the create event (every room has one).
    let has_create = state_events
        .iter()
        .any(|ev| ev.get("type").and_then(|v| v.as_str()) == Some("m.room.create"));
    assert!(has_create, "m.room.create event present: {state_events:?}");
}

#[tokio::test]
async fn legacy_sync_passes_since_through_v5_pos() {
    let app = router(config()).await.expect("router init");

    // Initial sync, capture next_batch.
    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);
    let next_batch = body
        .get("next_batch")
        .and_then(|v| v.as_str())
        .expect("next_batch is a string")
        .to_string();
    assert!(!next_batch.is_empty(), "non-empty next_batch");

    // Second sync with ?since={next_batch} — no events occurred between
    // syncs, so rooms.join should be empty.
    let (status, body) = get(&app, LEGACY_SYNC_PATH, Some(&format!("since={next_batch}"))).await;
    assert_eq!(status, StatusCode::OK);

    let join = body
        .pointer("/rooms/join")
        .and_then(|v| v.as_object())
        .expect("rooms.join object");
    assert!(
        join.is_empty(),
        "no new events between syncs → empty rooms.join: {body}",
    );
}

#[tokio::test]
async fn legacy_sync_bad_since_returns_m_unknown_pos() {
    let app = router(config()).await.expect("router init");

    // Garbage `since` — sliding_sync's pos parser is u64, so a non-numeric
    // value fails fast with `SyncError::UnknownPos`, which the legacy
    // wrapper maps to 400 M_UNKNOWN_POS (mirrors the MSC4186 wrapper).
    let (status, body) = get(&app, LEGACY_SYNC_PATH, Some("since=garbage")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(|v| v.as_str()),
        Some("M_UNKNOWN_POS"),
        "errcode pass-through: {body}",
    );
}

#[tokio::test]
async fn legacy_sync_timeout_zero_returns_immediately() {
    let app = router(config()).await.expect("router init");

    // `?timeout=0` (and `timeout` absent) must both return promptly — the
    // legacy default is no-wait. We bound the wall clock at ~1s to catch
    // any accidental long-poll.
    let start = std::time::Instant::now();
    let (status, _body) = get(&app, LEGACY_SYNC_PATH, Some("timeout=0")).await;
    let elapsed = start.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "timeout=0 sync returned promptly (elapsed = {elapsed:?})",
    );

    // Sanity: absent timeout behaves the same way.
    let start = std::time::Instant::now();
    let (status, _body) = get(&app, LEGACY_SYNC_PATH, None).await;
    let elapsed = start.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "no-timeout sync returned promptly (elapsed = {elapsed:?})",
    );
}
