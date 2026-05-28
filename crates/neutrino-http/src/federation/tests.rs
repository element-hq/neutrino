//! End-to-end tests for the federation /get_missing_events endpoint.
//! Lives in src/ rather than tests/ so the test-only helpers
//! (`router_with_store`, `AppState::from_store`) can stay pub(crate).
//!
//! Two seeding paths:
//!
//! - **CSAPI seeding** (`/createRoom`, `/send/{type}/{txn}`) — used by tests
//!   that only need the existence/absence of events at the storage edge
//!   (bad-request paths, 404, unknown-IDs). Mirrors the pattern in
//!   `tests/e2e_sliding_sync.rs`.
//! - **Direct storage seeding** via [`build_seeded_router`] — used by tests
//!   that need a *non-flat* DAG. The CSAPI `/send` path currently writes
//!   events with empty `prev_events` (Phase 6 will wire the head pointer),
//!   so the BFS walker has nothing to traverse. These tests open a fresh
//!   `SqliteStore`, build chains with explicit `prev_events`, persist them
//!   via the trait, then mount the router on top with
//!   `router_with_store`.
//!
//! See `docs/get-missing-events.md` §Tests B for the table this file covers.

#![cfg(test)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::{Config, ROOM_VERSION_ID};
use neutrino_state::event_id::EventBuilder;
use neutrino_store::{EventStore, RoomStore};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tower::ServiceExt;

use crate::{router, router_with_store};

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

fn alice() -> OwnedUserId {
    "@alice:example.org".parse().unwrap()
}

fn fed_path(room_id: &str) -> String {
    format!("/_matrix/federation/v1/get_missing_events/{room_id}")
}

/// Drive a POST against the router with a JSON body. Returns the status
/// code and parsed body (or `Value::Null` on empty responses).
async fn post_json(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    drive(app, req).await
}

/// Drive a POST with a raw byte body (so tests can send malformed JSON
/// and assert the HTTP edge maps it to 400).
async fn post_raw(
    app: &axum::Router,
    path: &str,
    body: Vec<u8>,
    content_type: &str,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();
    drive(app, req).await
}

async fn drive(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body parses as JSON")
    };
    (status, value)
}

/// Open a fresh sqlite store on a tempfile. Returns both so the caller
/// can pass the store to `router_with_store` and keep the tempfile guard
/// alive for the lifetime of the router.
async fn fresh_store() -> (Arc<SqliteStore>, NamedTempFile) {
    let tempfile = NamedTempFile::new().expect("tempfile");
    let store = Arc::new(
        SqliteStore::open(tempfile.path())
            .await
            .expect("open sqlite"),
    );
    (store, tempfile)
}

/// Build a seeded room with a linear chain of `n` message events whose
/// `prev_events` form a real DAG. Returns the router (mounted over the
/// seeded store), the room id, the create event id, and the IDs of the
/// `n` message events in causal (oldest-first) order.
///
/// The chain is:
///
/// ```text
///     create ← member-join ← msg[0] ← msg[1] ← … ← msg[n-1]
/// ```
async fn build_seeded_router(
    n_messages: usize,
) -> (axum::Router, OwnedRoomId, OwnedEventId, Vec<OwnedEventId>) {
    let (store, tempfile) = fresh_store().await;
    let sender = alice();

    // create event
    let create = EventBuilder::new(sender.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let create_id = create.event_id.clone();

    // self-join referencing the create event
    let join = EventBuilder::new(sender.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(sender.as_str().to_owned())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .build()
        .expect("build join");
    let join_id = join.event_id.clone();

    store
        .create_room(&create, &[join])
        .await
        .expect("create_room");

    // Linear chain of message events.
    let mut prev = join_id;
    let mut ids = Vec::with_capacity(n_messages);
    for i in 0..n_messages {
        let ev = EventBuilder::new(sender.clone(), "m.room.message".to_owned())
            .room_id(room_id.clone())
            .content(json!({ "msgtype": "m.text", "body": format!("msg {i}") }))
            .prev_events(vec![prev.clone()])
            .origin_server_ts(1_700_000_000_000 + i as u64)
            .build()
            .expect("build msg");
        let id = ev.event_id.clone();
        store
            .persist_historical_event(&ev)
            .await
            .expect("persist_historical_event");
        ids.push(id.clone());
        prev = id;
    }

    let router = router_with_store(config(), store, tempfile);
    (router, room_id, create_id, ids)
}

// --- B1 ----------------------------------------------------------------

#[tokio::test]
async fn bad_request_empty_latest_events_returns_400() {
    let app = router(config()).await.expect("router");
    let (_, body) = post_json(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body.get("room_id").and_then(Value::as_str).unwrap();

    let (status, body) = post_json(
        &app,
        &fed_path(room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [],
            "limit": 10,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B2 ----------------------------------------------------------------

#[tokio::test]
async fn bad_request_non_json_body_returns_400() {
    let app = router(config()).await.expect("router");
    let (_, body) = post_json(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body.get("room_id").and_then(Value::as_str).unwrap();

    let (status, body) = post_raw(
        &app,
        &fed_path(room_id),
        b"this is not json {{{ ".to_vec(),
        "application/json",
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B3 ----------------------------------------------------------------

#[tokio::test]
async fn bad_request_missing_required_field_returns_400() {
    let app = router(config()).await.expect("router");
    let (_, body) = post_json(&app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body.get("room_id").and_then(Value::as_str).unwrap();

    // No `latest_events` field at all.
    let (status, body) =
        post_json(&app, &fed_path(room_id), &json!({ "earliest_events": [] })).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B4 ----------------------------------------------------------------

#[tokio::test]
async fn unknown_room_returns_404() {
    let app = router(config()).await.expect("router");

    let (status, body) = post_json(
        &app,
        &fed_path("!nope:example.org"),
        &json!({
            "earliest_events": [],
            "latest_events": ["$some_event:example.org"],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_NOT_FOUND"),
        "body = {body}"
    );
}

// --- B5 ----------------------------------------------------------------

#[tokio::test]
async fn happy_path_returns_events_between_earliest_and_latest() {
    // B5 - happy path. The 4 message events between create (earliest) and
    // msg 4 (latest) should be reachable. Assert the set of message bodies;
    // the BFS interleaving order across multiple latest seeds is
    // implementation detail per `DagStore::missing_events` (trait doc in
    // neutrino-store/src/lib.rs), so we collect into a `BTreeSet` and
    // compare set-equality only.
    let (app, room_id, create_id, msgs) = build_seeded_router(5).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [create_id.as_str()],
            "latest_events": [msgs[4].as_str()],
            "limit": 20,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");

    let events = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array");

    // The boundary IDs themselves never appear in the result. Expect exactly
    // msgs 0..=3 — msg 4 is `latest_events` (excluded), the create event is
    // `earliest_events` (excluded). The join event is on the path but its
    // body has no "msg N" string so it's filtered out by the prefix check.
    let msg_ids: std::collections::BTreeSet<_> = events
        .iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str))
        .filter(|s| s.starts_with("msg "))
        .collect();
    assert_eq!(
        msg_ids,
        std::collections::BTreeSet::from(["msg 0", "msg 1", "msg 2", "msg 3"]),
    );
}

// --- B6 ----------------------------------------------------------------

#[tokio::test]
async fn respects_limit() {
    // Seed >20 events in a chain, ask for limit=50, receive at most 20.
    let (app, room_id, _create_id, msgs) = build_seeded_router(25).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[24].as_str()],
            "limit": 50,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let n = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array")
        .len();
    assert_eq!(n, 20, "MAX_LIMIT cap not enforced; got {n}");
}

// --- B7 ----------------------------------------------------------------

#[tokio::test]
async fn default_limit_is_10() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(15).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[14].as_str()],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let n = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array")
        .len();
    assert_eq!(n, 10, "default limit is 10");
}

// --- B8 ----------------------------------------------------------------

#[tokio::test]
async fn empty_earliest_walks_back_to_room_root() {
    // With no `earliest_events`, the walk continues all the way back.
    // Seed 3 messages; ask for everything up to msg[2]; expect msg[1],
    // msg[0], join, create — in particular the create event must appear.
    let (app, room_id, create_id, msgs) = build_seeded_router(3).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[2].as_str()],
            "limit": 20,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let events = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array");

    // `event_id` isn't on the wire; find the create by `type` == "m.room.create".
    let has_create = events
        .iter()
        .any(|e| e.get("type").and_then(Value::as_str) == Some("m.room.create"));
    assert!(
        has_create,
        "walk back to room root must include the create event (id {create_id})"
    );
}

// --- B9 ----------------------------------------------------------------

#[tokio::test]
async fn latest_event_not_in_room_returns_empty() {
    // Room exists; the requested `latest` ID isn't in it. Per the design
    // doc, this is a no-op walk: no events reachable, no error, just
    // empty.
    let (app, room_id, _create_id, _msgs) = build_seeded_router(2).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": ["$fabricated:example.org"],
            "limit": 10,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let events = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array");
    assert!(events.is_empty(), "no events reachable from unknown latest");
}

// --- B10 ---------------------------------------------------------------

#[tokio::test]
async fn min_depth_field_ignored() {
    // `min_depth: huge` must still return the same events as omitting
    // the field — Neutrino doesn't store depth, so the filter is a no-op
    // (the field is parsed only to satisfy serde when the wire shape
    // includes it). Pins the spec divergence.
    let (app, room_id, _create_id, msgs) = build_seeded_router(3).await;

    let (_, baseline) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[2].as_str()],
            "limit": 20,
        }),
    )
    .await;
    let (_, with_min_depth) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[2].as_str()],
            "limit": 20,
            "min_depth": 999_999,
        }),
    )
    .await;

    let baseline_n = baseline
        .get("events")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let with_min_depth_n = with_min_depth
        .get("events")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    assert_eq!(
        baseline_n, with_min_depth_n,
        "min_depth must not change the result count"
    );
    assert!(
        baseline_n > 0,
        "should have walked back to find some events"
    );
}

// --- B12 ---------------------------------------------------------------

#[tokio::test]
async fn malformed_room_id_returns_400_with_errcode() {
    // The path extractor takes a `String` so we can parse manually and
    // surface a JSON `M_INVALID_PARAM` body rather than axum's default
    // plain-text 400. Mirrors the `members` handler precedent in
    // `lib.rs`.
    let app = router(config()).await.expect("router");

    let (status, body) = post_json(
        &app,
        &fed_path("not-a-room-id"),
        &json!({
            "earliest_events": [],
            "latest_events": ["$some_event:example.org"],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B11 ---------------------------------------------------------------

#[tokio::test]
async fn wire_bytes_passthrough() {
    // Federation responses ship `Event.raw` verbatim — no enrichment.
    // v12 / MSC4242 wire bytes never carry `event_id` (it's derived from
    // the reference hash), so the field must be absent from every event
    // in the response. This pins federation = raw, CSAPI = enriched.
    let (app, room_id, _create_id, msgs) = build_seeded_router(2).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[1].as_str()],
            "limit": 20,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    let events = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array");
    assert!(
        !events.is_empty(),
        "expected non-empty events for assertion"
    );
    for ev in events {
        assert!(
            ev.get("event_id").is_none(),
            "federation wire bytes must not carry event_id: {ev}"
        );
    }
}
