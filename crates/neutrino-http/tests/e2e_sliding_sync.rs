//! End-to-end tests against the live axum router. Exercises:
//! - The HTTP/JSON edge in `lib.rs::sync` (body parse, query extraction,
//!   response serialization, error → HTTP mapping).
//! - The full `sliding_sync::handle` pipeline.
//! - The `SqliteStore`'s `RoomStore::create_room` / `EventStore::persist_event`
//!   paths via the legacy `/createRoom` and `/send/{type}/{txn}` endpoints.
//!
//! These tests build the same `Router` the production binary serves and
//! drive it with `tower::ServiceExt::oneshot` — no TCP, no real server, but
//! every byte goes through axum's routing, extractors, and serialization.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

const SYNC_PATH: &str = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync";

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

/// POST a JSON body to `path` with the optional query string appended.
async fn post(
    app: &axum::Router,
    path: &str,
    query: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let uri = match query {
        Some(q) => format!("{path}?{q}"),
        None => path.to_string(),
    };
    let req = Request::builder()
        .method("POST")
        .uri(uri)
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

fn sync_body_with_default_list() -> Value {
    json!({
        "lists": {
            "all": {
                "ranges": [[0, 99]],
                "timeline_limit": 5,
                "required_state": []
            }
        }
    })
}

#[tokio::test]
async fn create_room_then_initial_sync_returns_the_room() {
    let app = router(config()).await.expect("router init");

    let (status, body) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .expect("createRoom returns a room_id")
        .to_string();

    let (status, body) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    assert_eq!(status, StatusCode::OK);

    let rooms = body
        .get("rooms")
        .and_then(|v| v.as_object())
        .expect("response has rooms");
    assert!(
        rooms.contains_key(&room_id),
        "freshly-created room shows up"
    );

    let room = rooms.get(&room_id).unwrap();
    assert_eq!(room.get("initial").and_then(|v| v.as_bool()), Some(true));
    let pos = body
        .get("pos")
        .and_then(|v| v.as_str())
        .expect("pos string");
    assert!(!pos.is_empty(), "non-empty pos");
}

#[tokio::test]
async fn put_event_then_sync_delivers_it_in_timeline() {
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // Initial sync to register a conn and get a pos.
    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // Send a message.
    let put_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-1");
    let (status, _) = put(
        &app,
        &put_path,
        &json!({"body": "hello world", "msgtype": "m.text"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Second sync — should include the message in the timeline.
    let pos_query = format!("pos={}", pos1);
    let (status, body) = post(
        &app,
        SYNC_PATH,
        Some(&pos_query),
        &sync_body_with_default_list(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let timeline = body
        .pointer(&format!("/rooms/{}/timeline", room_id))
        .and_then(|v| v.as_array())
        .expect("timeline in delta response");
    assert!(
        timeline
            .iter()
            .any(|ev| ev.pointer("/content/body").and_then(|v| v.as_str()) == Some("hello world")),
        "the message we PUT shows up: {timeline:?}"
    );
}

#[tokio::test]
async fn put_event_then_sliding_sync_returns_event_with_event_id() {
    // Regression test mirroring the legacy /sync version: events delivered
    // via the v5 sliding-sync endpoint must carry the same event_id that
    // PUT /send returned to the caller. v12 / MSC4242 wire bytes don't
    // carry event_id; this pins the `event_view::From<&Event>` enrichment
    // for the v5 timeline path.
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let put_path = format!("/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-evtid-v5");
    let (status, put_body) = put(
        &app,
        &put_path,
        &json!({"body": "regression-canary-v5", "msgtype": "m.text"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sent_event_id = put_body
        .get("event_id")
        .and_then(|v| v.as_str())
        .expect("PUT returns event_id")
        .to_string();

    let pos_query = format!("pos={pos1}");
    let (status, body) = post(
        &app,
        SYNC_PATH,
        Some(&pos_query),
        &sync_body_with_default_list(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let timeline = body
        .pointer(&format!("/rooms/{room_id}/timeline"))
        .and_then(Value::as_array)
        .expect("timeline in delta response");

    let found = timeline
        .iter()
        .any(|ev| ev.get("event_id").and_then(Value::as_str) == Some(sent_event_id.as_str()));
    assert!(
        found,
        "PUT'd event must appear in the v5 timeline with event_id {sent_event_id:?}: \
         timeline={timeline:?}",
    );
}

#[tokio::test]
async fn stale_pos_returns_m_unknown_pos() {
    let app = router(config()).await.expect("router init");

    // Advance the conn so pos="1" is no longer current.
    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let (_, resp2) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}")),
        &sync_body_with_default_list(),
    )
    .await;
    let _pos2 = resp2.get("pos").unwrap().as_str().unwrap();
    let (_, _resp3) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={_pos2}")),
        &sync_body_with_default_list(),
    )
    .await;

    // Now retry pos1 — past the cache.
    let (status, body) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}")),
        &sync_body_with_default_list(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(|v| v.as_str()),
        Some("M_UNKNOWN_POS")
    );
}

#[tokio::test]
async fn idempotent_retry_returns_same_response_bytes() {
    let app = router(config()).await.expect("router init");

    let (_, _) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // First "real" second sync: processes, advances pos, caches.
    let (status, first_real) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}")),
        &sync_body_with_default_list(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Retry with the same pos — must return identical bytes.
    let (status, retry) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}")),
        &sync_body_with_default_list(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retry, first_real, "retry returns cached response verbatim");
}

#[tokio::test]
async fn invalid_conn_id_too_long_returns_m_invalid_param() {
    let app = router(config()).await.expect("router init");

    let body = json!({
        "conn_id": "this-string-is-much-longer-than-sixteen-characters",
        "lists": {"all": {"ranges": [[0, 99]], "timeline_limit": 1, "required_state": []}}
    });
    let (status, body) = post(&app, SYNC_PATH, None, &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(|v| v.as_str()),
        Some("M_INVALID_PARAM")
    );
}

#[tokio::test]
async fn extension_e2ee_echoed_on_request() {
    let app = router(config()).await.expect("router init");

    let body = json!({
        "lists": {"all": {"ranges": [[0, 99]], "timeline_limit": 1, "required_state": []}},
        "extensions": {"e2ee": {"enabled": true}}
    });
    let (status, body) = post(&app, SYNC_PATH, None, &body).await;
    assert_eq!(status, StatusCode::OK);

    let otk_count = body
        .pointer("/extensions/e2ee/device_one_time_keys_count")
        .and_then(|v| v.as_object())
        .expect("e2ee echo populated");
    assert!(!otk_count.is_empty(), "OTK count map present");
}

#[tokio::test]
async fn long_poll_returns_within_timeout_when_no_events() {
    let app = router(config()).await.expect("router init");

    let (_, _) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // No events between syncs, request a 100ms long-poll, expect to wait
    // ~100ms and come back with no rooms.
    let start = std::time::Instant::now();
    let (status, body) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}&timeout=100")),
        &sync_body_with_default_list(),
    )
    .await;
    let elapsed = start.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= std::time::Duration::from_millis(80),
        "should have long-polled; elapsed = {elapsed:?}"
    );
    let rooms = body
        .get("rooms")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(rooms, 0);
}

#[tokio::test]
async fn long_poll_wakes_on_concurrent_put_event() {
    // Start a long-poll, then PUT an event after a short delay, then assert
    // the long-poll returns the event before its full timeout.
    let app = router(config()).await.expect("router init");

    let (_, body) = post(&app, "/_matrix/client/v3/createRoom", None, &json!({})).await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    let (_, resp1) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    let pos1 = resp1
        .get("pos")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let app_for_put = app.clone();
    let room_for_put = room_id.clone();
    let waker = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let put_path =
            format!("/_matrix/client/v3/rooms/{room_for_put}/send/m.room.message/late-txn");
        put(
            &app_for_put,
            &put_path,
            &json!({"body": "late", "msgtype": "m.text"}),
        )
        .await
    });

    let start = std::time::Instant::now();
    let (status, body) = post(
        &app,
        SYNC_PATH,
        Some(&format!("pos={pos1}&timeout=2000")),
        &sync_body_with_default_list(),
    )
    .await;
    let elapsed = start.elapsed();
    waker.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed < std::time::Duration::from_millis(1500),
        "should have woken on the event well before 2s; elapsed = {elapsed:?}"
    );
    let timeline = body
        .pointer(&format!("/rooms/{room_id}/timeline"))
        .and_then(|v| v.as_array())
        .expect("delta response carries the room");
    assert!(
        timeline
            .iter()
            .any(|ev| ev.pointer("/content/body").and_then(|v| v.as_str()) == Some("late")),
        "the concurrent message is in the wake response"
    );
}

#[tokio::test]
async fn initial_sync_with_named_room_returns_the_name() {
    let app = router(config()).await.expect("router init");

    let (_, body) = post(
        &app,
        "/_matrix/client/v3/createRoom",
        None,
        &json!({"name": "My Room"}),
    )
    .await;
    let room_id = body
        .get("room_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    let (status, body) = post(&app, SYNC_PATH, None, &sync_body_with_default_list()).await;
    assert_eq!(status, StatusCode::OK);

    let name = body
        .pointer(&format!("/rooms/{room_id}/name"))
        .and_then(|v| v.as_str());
    assert_eq!(name, Some("My Room"));
}
