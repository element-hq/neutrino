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
//!
//! Gated off under `multi-user-shim`: every test seeds via tokenless CSAPI
//! (`createRoom` / `/send`), which the shim rejects (401). These run in the
//! default build; the shim's coverage lives in `tests/e2e_multi_user.rs`.
#![cfg(not(feature = "multi-user-shim"))]

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
        ..Default::default()
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
async fn legacy_sync_unknown_since_falls_back_to_initial() {
    let app = router(config()).await.expect("router init");

    // Garbage `since` — sliding_sync's pos parser is u64, so a non-numeric value
    // fails with `SyncError::UnknownPos`. Legacy `since` tokens are durable, so
    // rather than 400 (a sliding-sync-only reconnect signal) the wrapper falls
    // back to a full initial sync: 200 with a fresh `next_batch`.
    let (status, body) = get(&app, LEGACY_SYNC_PATH, Some("since=garbage")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("next_batch").and_then(|v| v.as_str()).is_some(),
        "fallback initial sync issues a fresh next_batch: {body}",
    );
}

/// A stale legacy `since` token — one the connection has since advanced past —
/// is recovered as a full sync (not a 400) and reflects *current* state: after
/// join→leave→join the room appears in `rooms.join`, never `rooms.leave`.
/// Single-user mirror of Complement's `TestCumulativeJoinLeaveJoinSync`.
#[tokio::test]
async fn legacy_sync_cumulative_join_leave_join() {
    let app = router(config()).await.expect("router init");

    // A token from before the room exists; the connection advances past it.
    let (s, before) = get(&app, LEGACY_SYNC_PATH, Some("timeout=0")).await;
    assert_eq!(s, StatusCode::OK, "{before}");
    let old = before["next_batch"]
        .as_str()
        .expect("next_batch")
        .to_owned();

    let (s, room) = post(
        &app,
        "/_matrix/client/v3/createRoom",
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Walk the connection forward: join (via create) → leave → join, syncing
    // with the latest token each time so `old` is left well behind.
    let (_s, b1) = get(&app, LEGACY_SYNC_PATH, Some(&format!("since={old}"))).await;
    let t1 = b1["next_batch"].as_str().unwrap().to_owned();

    let (s, _) = post(
        &app,
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (_s, b2) = get(&app, LEGACY_SYNC_PATH, Some(&format!("since={t1}"))).await;
    let t2 = b2["next_batch"].as_str().unwrap().to_owned();

    let (s, _) = post(
        &app,
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let _ = get(&app, LEGACY_SYNC_PATH, Some(&format!("since={t2}"))).await;

    // Replay the now-stale `old` token: must not 400, and must show current
    // state — room joined, absent from the leave section.
    let (s, body) = get(
        &app,
        LEGACY_SYNC_PATH,
        Some(&format!("since={old}&timeout=0")),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "stale token must not 400: {body}");
    assert!(
        body.pointer(&format!("/rooms/leave/{room_id}")).is_none(),
        "join→leave→join room must not appear in rooms.leave: {body}"
    );
    assert!(
        body.pointer(&format!("/rooms/join/{room_id}")).is_some(),
        "current membership is join, so it must appear in rooms.join: {body}"
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

#[tokio::test]
async fn send_event_then_legacy_sync_returns_event_with_event_id() {
    // Regression test for the complement failure
    // `TestRoomCreate/Parallel/Can_/sync_newly_created_room`: PUT an event,
    // capture its event_id, and verify the same id appears on the event when
    // it comes back via /sync. The v12 / MSC4242 wire bytes don't carry
    // event_id; this exercises the `event_view::From<&Event>` enrichment
    // path that injects it for CSAPI delivery.
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let put_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-evtid");
    let (status, put_body) = put(
        &app,
        &put_path,
        &json!({"body": "regression-canary", "msgtype": "m.text"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sent_event_id = put_body
        .get("event_id")
        .and_then(|v| v.as_str())
        .expect("PUT returns event_id")
        .to_string();

    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);

    let timeline = body
        .pointer(&format!("/rooms/join/{room_id}/timeline/events"))
        .and_then(Value::as_array)
        .expect("timeline.events is an array");

    let found = timeline
        .iter()
        .any(|ev| ev.get("event_id").and_then(Value::as_str) == Some(sent_event_id.as_str()));
    assert!(
        found,
        "PUT'd event must appear in the legacy /sync timeline with event_id {sent_event_id:?}: \
         timeline={timeline:?}",
    );
}

#[tokio::test]
async fn legacy_sync_create_event_carries_room_id_and_event_id() {
    // The v12 / MSC4242 wire bytes of an m.room.create event don't carry
    // room_id (derived from event_id via sigil swap) and never carry
    // event_id. Both must be injected when delivered via CSAPI /sync,
    // otherwise complement / clients can't address the event.
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let (status, body) = get(&app, LEGACY_SYNC_PATH, None).await;
    assert_eq!(status, StatusCode::OK);

    // The create event is delivered as state on initial sync (via the
    // wildcard required_state pulled into rooms.join.<id>.state.events).
    let state_events = body
        .pointer(&format!("/rooms/join/{room_id}/state/events"))
        .and_then(Value::as_array)
        .expect("state.events is an array");

    let create = state_events
        .iter()
        .find(|ev| ev.get("type").and_then(Value::as_str) == Some("m.room.create"))
        .expect("create event present in initial-sync state");

    assert!(
        create
            .get("event_id")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "create event carries event_id: {create}",
    );
    assert_eq!(
        create.get("room_id").and_then(Value::as_str),
        Some(room_id.as_str()),
        "create event carries injected room_id (wire bytes lack it for v12): {create}",
    );
}
