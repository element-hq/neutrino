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
//!   so the DAG walker has nothing to traverse. These tests open a fresh
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
use neutrino_store::{EventStore, RoomStore, StateStore};
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
        ..Default::default()
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

// Gated off under `multi-user-shim`: it seeds the room via tokenless CSAPI
// `createRoom`, which the shim rejects (401). The shim's own coverage lives in
// `tests/e2e_multi_user.rs`; this bad-request case runs in the default build.
#[cfg(not(feature = "multi-user-shim"))]
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

// Gated off under `multi-user-shim` — see `bad_request_empty_latest_events`.
#[cfg(not(feature = "multi-user-shim"))]
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

// Gated off under `multi-user-shim` — see `bad_request_empty_latest_events`.
#[cfg(not(feature = "multi-user-shim"))]
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
    // the ordering across multiple latest seeds is an
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

// --- B13 (I4: result ordering) -----------------------------------------

#[tokio::test]
async fn events_returned_oldest_first() {
    // The handler reverses `missing_events`' newest-first walk so the
    // response is in topological (oldest-first) order, matching Synapse.
    // Linear chain create ← join ← msg0 ← msg1 ← msg2 ← msg3; walk back
    // from msg3 with no earliest. The message-bodied events must appear
    // oldest-first — msg 0, msg 1, msg 2 (msg3 is the excluded boundary,
    // create/join carry no "msg N" body). Asserts a *sequence*, not a set,
    // so a regression to newest-first fails here.
    let (app, room_id, _create_id, msgs) = build_seeded_router(4).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[3].as_str()],
            "limit": 20,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    let ordered: Vec<&str> = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array")
        .iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str))
        .filter(|s| s.starts_with("msg "))
        .collect();
    assert_eq!(
        ordered,
        ["msg 0", "msg 1", "msg 2"],
        "events must be returned oldest-first"
    );
}

// --- B14 (I5: earliest boundary is excluded at the HTTP layer) ---------

#[tokio::test]
async fn earliest_message_boundary_is_excluded() {
    // B5 used the create event (no body) as `earliest`, so a leak of the
    // earliest boundary would slip past its body-prefix filter. Here the
    // earliest boundary is a *message* event (detectable by body): with
    // latest=msg3, earliest=msg1, only msg2 is strictly between them.
    // msg1 (earliest) and everything below it must NOT appear.
    let (app, room_id, _create_id, msgs) = build_seeded_router(4).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [msgs[1].as_str()],
            "latest_events": [msgs[3].as_str()],
            "limit": 20,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    let bodies: std::collections::BTreeSet<&str> = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array")
        .iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str))
        .filter(|s| s.starts_with("msg "))
        .collect();
    assert_eq!(
        bodies,
        std::collections::BTreeSet::from(["msg 2"]),
        "only msg 2 is strictly between earliest=msg1 and latest=msg3; \
         the earliest boundary must not leak into the response"
    );
}

// --- B15 (I6: malformed min_depth still 400s via serde) ----------------

#[tokio::test]
async fn malformed_min_depth_returns_400() {
    // `min_depth` is parsed (then ignored). A non-integer value must still
    // 400 at serde — the whole reason the field is typed rather than
    // dropped. Pins the doc claim on `RequestBody._min_depth`.
    let (app, room_id, _create_id, msgs) = build_seeded_router(2).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[1].as_str()],
            "min_depth": "not-an-integer",
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B16 (I6: malformed event id in latest 400s) ----------------------

#[tokio::test]
async fn malformed_event_id_in_latest_returns_400() {
    // A `latest_events` entry that isn't a valid event ID fails
    // `OwnedEventId` deserialization → 400, before the store is touched.
    let (app, room_id, _create_id, _msgs) = build_seeded_router(1).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": ["not-an-event-id"],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B17 (I6: wrong content-type 400s) --------------------------------

#[tokio::test]
async fn wrong_content_type_returns_400() {
    // A body sent with a non-JSON content-type is rejected by the `Json`
    // extractor; the handler maps that rejection to 400 M_INVALID_PARAM
    // rather than letting axum's default (415/422) through.
    let (app, room_id, _create_id, msgs) = build_seeded_router(2).await;

    let payload = serde_json::to_vec(&json!({
        "earliest_events": [],
        "latest_events": [msgs[1].as_str()],
    }))
    .unwrap();
    let (status, body) = post_raw(&app, &fed_path(room_id.as_str()), payload, "text/plain").await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- B18 (I6: explicit limit=0 returns empty) -------------------------

#[tokio::test]
async fn explicit_limit_zero_returns_empty() {
    // An explicit `limit: 0` is honored as 0 (matching Synapse's
    // `min(limit, 20)`); the handler returns 200 with an empty `events`
    // array rather than substituting the default of 10.
    let (app, room_id, _create_id, msgs) = build_seeded_router(3).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[2].as_str()],
            "limit": 0,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let events = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array");
    assert!(events.is_empty(), "explicit limit=0 must return no events");
}

// --- B19 (I3: earliest_events is required per spec) -------------------

#[tokio::test]
async fn missing_earliest_events_returns_400() {
    // `earliest_events` is Required per the spec; omitting it 400s at
    // deserialization rather than being silently treated as empty.
    let (app, room_id, _create_id, msgs) = build_seeded_router(2).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "latest_events": [msgs[1].as_str()],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// === GET /_matrix/federation/v1/backfill/{roomId} ======================

/// Drive a GET against the router. Mirrors [`post_json`] for query-param
/// endpoints.
async fn get(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    drive(app, req).await
}

/// Build a `/backfill` URL. Each `v` is percent-encoded the way ruma's
/// client serialises the query (`$` → `%24`), so the tests exercise the
/// handler's percent-decoding end-to-end. Base64url id chars (`-`, `_`,
/// alphanumerics) are unreserved and pass through untouched.
fn backfill_path(room_id: &str, v: &[&str], limit: Option<u32>) -> String {
    let mut q: Vec<String> = v
        .iter()
        .map(|id| format!("v={}", id.replace('$', "%24")))
        .collect();
    if let Some(l) = limit {
        q.push(format!("limit={l}"));
    }
    format!("/_matrix/federation/v1/backfill/{room_id}?{}", q.join("&"))
}

/// Collect the `content.body` of every PDU that carries one (i.e. the
/// message events), in response order. Lets a test pin newest-first
/// ordering without knowing the create/join event ids (their wire bytes
/// carry no `body`).
fn pdu_bodies(body: &Value) -> Vec<String> {
    body.get("pdus")
        .and_then(Value::as_array)
        .expect("pdus array")
        .iter()
        .filter_map(|p| p.pointer("/content/body").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

// BF1: walk back from the chain head returns every ancestor plus the head
// itself, newest-first. create ← join ← msg0 ← msg1 ← msg2; backfill from
// msg2 yields all 5 events. The three message bodies must appear
// newest-first.
#[tokio::test]
async fn backfill_returns_chain_newest_first() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(3).await;

    let (status, body) = get(
        &app,
        &backfill_path(room_id.as_str(), &[msgs[2].as_str()], Some(50)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let pdus = body.get("pdus").and_then(Value::as_array).expect("pdus");
    // create + join + msg0 + msg1 + msg2 = 5 events, seed included.
    assert_eq!(pdus.len(), 5, "expected full chain incl. seed: {body}");
    assert_eq!(
        pdu_bodies(&body),
        vec!["msg 2".to_owned(), "msg 1".to_owned(), "msg 0".to_owned()],
        "messages must be newest-first"
    );
}

// BF2: limit is a hard cap on the number of PDUs returned. Backfill from
// msg2 with limit=2 yields exactly the seed and its immediate parent.
#[tokio::test]
async fn backfill_respects_limit() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(3).await;

    let (status, body) = get(
        &app,
        &backfill_path(room_id.as_str(), &[msgs[2].as_str()], Some(2)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let pdus = body.get("pdus").and_then(Value::as_array).expect("pdus");
    assert_eq!(pdus.len(), 2, "limit=2 must cap the result");
    assert_eq!(
        pdu_bodies(&body),
        vec!["msg 2".to_owned(), "msg 1".to_owned()]
    );
}

// BF3: the transaction envelope carries `origin` (our server name) and a
// numeric `origin_server_ts` alongside `pdus`, per the ruma /
// Synapse backfill response shape.
#[tokio::test]
async fn backfill_response_has_transaction_envelope() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(1).await;

    let (status, body) = get(
        &app,
        &backfill_path(room_id.as_str(), &[msgs[0].as_str()], Some(10)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.get("origin").and_then(Value::as_str),
        Some("example.org"),
        "origin must be our server name: {body}"
    );
    assert!(
        body.get("origin_server_ts")
            .and_then(Value::as_u64)
            .is_some(),
        "origin_server_ts must be a number: {body}"
    );
    assert!(body.get("pdus").and_then(Value::as_array).is_some());
}

// BF4: wire bytes verbatim — v12 / MSC4242 PDUs never carry `event_id`
// (it's derived from the reference hash). Federation peers must receive the
// exact bytes that produced the id, so no enrichment is applied.
#[tokio::test]
async fn backfill_ships_wire_bytes_without_event_id() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(2).await;

    let (status, body) = get(
        &app,
        &backfill_path(room_id.as_str(), &[msgs[1].as_str()], Some(50)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let pdus = body.get("pdus").and_then(Value::as_array).expect("pdus");
    assert!(!pdus.is_empty());
    for pdu in pdus {
        assert!(
            pdu.get("event_id").is_none(),
            "federation wire bytes must not carry event_id: {pdu}"
        );
    }
}

// BF5: unknown room → 404 M_NOT_FOUND (spec-required), not a 500 or the
// bare-text fallback.
#[tokio::test]
async fn backfill_unknown_room_returns_404() {
    let (app, _room_id, _create_id, msgs) = build_seeded_router(1).await;
    let unknown = "!nope:example.org";

    let (status, body) = get(&app, &backfill_path(unknown, &[msgs[0].as_str()], Some(10))).await;

    assert_eq!(status, StatusCode::NOT_FOUND, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_NOT_FOUND"),
        "body = {body}"
    );
}

// BF6: a request with no `v` parameter is rejected — there's nothing to walk
// back from. 400 M_INVALID_PARAM, mirroring the empty-`latest_events`
// rejection on the sibling endpoint.
#[tokio::test]
async fn backfill_missing_v_returns_400() {
    let (app, room_id, _create_id, _msgs) = build_seeded_router(1).await;

    let (status, body) = get(&app, &backfill_path(room_id.as_str(), &[], Some(10))).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// BF7: a `v` event we don't hold is skipped, not 500'd. `events_before`
// would reject an unknown seed with InvalidInput; the handler pre-filters
// via `get_events` so an unknown seed yields an empty (200) backfill.
#[tokio::test]
async fn backfill_unknown_v_is_skipped_not_500() {
    let (app, room_id, _create_id, _msgs) = build_seeded_router(1).await;
    // Syntactically valid v12 id (43 base64url chars) that isn't in the room.
    let ghost = "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let (status, body) = get(&app, &backfill_path(room_id.as_str(), &[ghost], Some(10))).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let pdus = body.get("pdus").and_then(Value::as_array).expect("pdus");
    assert!(pdus.is_empty(), "unknown seed must yield no events: {body}");
}

// BF8: a raw (un-percent-encoded) `$` in the query still resolves. Proves
// the decoder is lenient about already-decoded sigils as well as `%24`.
#[tokio::test]
async fn backfill_accepts_raw_dollar_sigil_in_query() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(1).await;
    // Build the path with the sigil left raw.
    let path = format!(
        "/_matrix/federation/v1/backfill/{}?v={}&limit=10",
        room_id.as_str(),
        msgs[0].as_str()
    );

    let (status, body) = get(&app, &path).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        pdu_bodies(&body),
        vec!["msg 0".to_owned()],
        "raw-sigil seed must resolve: {body}"
    );
}

// BF9 (I1): a present-but-blank `?v=` is rejected like a wholly-missing `v`.
// It must NOT slip past the empty-`v` guard as an empty-string seed and
// return a misleading 200 with no PDUs.
#[tokio::test]
async fn backfill_blank_v_returns_400() {
    let (app, room_id, _create_id, _msgs) = build_seeded_router(1).await;
    let path = format!(
        "/_matrix/federation/v1/backfill/{}?v=&limit=10",
        room_id.as_str()
    );

    let (status, body) = get(&app, &path).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// BF10 (I3): an explicit `limit=0` is rejected (400), matching Synapse's
// `if not limit: return 400` — asking for zero events is a client bug, not
// a valid empty backfill. (A *missing* limit still defaults to 10.)
#[tokio::test]
async fn backfill_limit_zero_returns_400() {
    let (app, room_id, _create_id, msgs) = build_seeded_router(1).await;

    let (status, body) = get(
        &app,
        &backfill_path(room_id.as_str(), &[msgs[0].as_str()], Some(0)),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// --- /send (inbound federation transactions) ---------------------------

/// Drive a PUT with a JSON body against the router.
async fn put_json(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    drive(app, req).await
}

fn send_path(txn_id: &str) -> String {
    format!("/_matrix/federation/v1/send/{txn_id}")
}

/// Wrap PDU events into a transaction envelope. `pdus` are the events' raw wire
/// bytes embedded verbatim — exactly how a peer would send them.
fn txn(pdus: &[&neutrino_common::Event]) -> Value {
    let pdus: Vec<Value> = pdus
        .iter()
        .map(|e| serde_json::from_str(e.raw.get()).expect("pdu raw is valid JSON"))
        .collect();
    json!({
        "origin": "remote.example.org",
        "origin_server_ts": 1_700_000_000_000u64,
        "pdus": pdus,
    })
}

/// Seed a room (create + alice's self-join) on a fresh store and mount the
/// router over it. forward_extremities are seeded to the join, so the actor can
/// bootstrap when a PDU for this room arrives. Returns the router, a store
/// handle for assertions, the room id, alice, and the join event id (the sole
/// head of both DAGs).
async fn seed_joined_room() -> (
    axum::Router,
    Arc<SqliteStore>,
    OwnedRoomId,
    OwnedUserId,
    OwnedEventId,
) {
    let (store, tempfile) = fresh_store().await;
    let alice = alice();
    let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let create_id = create.event_id.clone();
    let join = EventBuilder::new(alice.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(alice.as_str().to_owned())
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
    let router = router_with_store(config(), store.clone(), tempfile);
    (router, store, room_id, alice, join_id)
}

/// Build a message PDU sitting on `head` (both DAGs).
fn message_on(
    sender: &OwnedUserId,
    room_id: &OwnedRoomId,
    head: &OwnedEventId,
    body: &str,
    ts: u64,
) -> neutrino_common::Event {
    EventBuilder::new(sender.clone(), "m.room.message".to_owned())
        .room_id(room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": body }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head.clone()])
        .origin_server_ts(ts)
        .build()
        .expect("build message")
}

#[tokio::test]
async fn send_accepts_pdu_and_persists() {
    let (app, store, room_id, alice, join_id) = seed_joined_room().await;
    let msg = message_on(
        &alice,
        &room_id,
        &join_id,
        "hello over federation",
        1_700_000_001_000,
    );
    let msg_id = msg.event_id.clone();

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&msg])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    // Per-PDU result is an empty object (success, no error).
    let result = body.get("pdus").and_then(|p| p.get(msg_id.as_str()));
    assert_eq!(result, Some(&json!({})), "body = {body}");

    // The event landed in the store and the timeline head advanced to it.
    let fetched = store.get_events(&[msg_id.as_ref()]).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert!(!fetched[0].rejected);
    let (timeline, _state) = store.forward_extremities(&room_id).await.unwrap().unwrap();
    assert_eq!(timeline, [msg_id].into_iter().collect());
}

#[tokio::test]
async fn send_persists_rejected_pdu_as_success_result() {
    // bob — never invited — sends a join PDU into alice's invite-only room.
    // Federation policy: the reject is *persisted*, and from the transaction's
    // point of view the PDU was processed → empty (error-free) result.
    let (app, store, room_id, _alice, join_id) = seed_joined_room().await;
    let bob: OwnedUserId = "@bob:remote.example.org".parse().unwrap();
    let bob_join = EventBuilder::new(bob.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(bob.as_str().to_owned())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![join_id.clone()])
        .prev_state_events(vec![join_id.clone()])
        .build()
        .expect("build bob join");
    let bob_join_id = bob_join.event_id.clone();

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&bob_join])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(bob_join_id.as_str())),
        Some(&json!({})),
        "a persisted reject is a successful PDU result; body = {body}"
    );
    // Persisted-but-rejected; bob is absent from current_state.
    let fetched = store.get_events(&[bob_join_id.as_ref()]).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert!(fetched[0].rejected);
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", bob.as_str())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn send_reports_error_for_unfillable_missing_ancestry() {
    // A PDU referencing a parent we don't have and can't backfill (NoFetcher).
    // Terminal condition 2: no progress → the PDU gets an error.
    let (app, _store, room_id, alice, join_id) = seed_joined_room().await;
    // An orphan ancestor that is never persisted nor included in the txn.
    let orphan = message_on(&alice, &room_id, &join_id, "orphan", 1_700_000_002_000);
    let child = message_on(
        &alice,
        &room_id,
        &orphan.event_id,
        "child",
        1_700_000_003_000,
    );
    let child_id = child.event_id.clone();

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&child])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let err = body
        .get("pdus")
        .and_then(|p| p.get(child_id.as_str()))
        .and_then(|r| r.get("error"))
        .and_then(Value::as_str);
    assert!(err.is_some(), "expected an error result; body = {body}");
}

#[tokio::test]
async fn send_toposorts_out_of_order_batch() {
    // Two new message events arrive in the same transaction, child *before*
    // parent in the array. The handler must toposort so the parent applies
    // first; both end up accepted.
    let (app, store, room_id, alice, join_id) = seed_joined_room().await;
    let first = message_on(&alice, &room_id, &join_id, "first", 1_700_000_001_000);
    // `second` chains the timeline off `first` but its state head stays the
    // join (messages don't move the state DAG), so `prev_state_events` points
    // at `join_id`, not at the `first` message.
    let second = EventBuilder::new(alice.clone(), "m.room.message".to_owned())
        .room_id(room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "second" }))
        .prev_events(vec![first.event_id.clone()])
        .prev_state_events(vec![join_id.clone()])
        .origin_server_ts(1_700_000_002_000)
        .build()
        .expect("build second");
    let (first_id, second_id) = (first.event_id.clone(), second.event_id.clone());

    // child (second) listed before parent (first).
    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&second, &first])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(first_id.as_str())),
        Some(&json!({})),
        "body = {body}"
    );
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(second_id.as_str())),
        Some(&json!({})),
        "body = {body}"
    );
    // The later child is the timeline head.
    let (timeline, _state) = store.forward_extremities(&room_id).await.unwrap().unwrap();
    assert_eq!(timeline, [second_id].into_iter().collect());
}

#[tokio::test]
async fn send_handles_duplicate_pdu_in_batch() {
    // A peer repeats the same PDU bytes in one transaction, and a third event
    // references it. Before the dedup fix this underflowed `toposort`'s
    // indegree bookkeeping (panic in debug). Now the duplicate is dropped and
    // both distinct events are accepted.
    let (store, tempfile) = fresh_store().await;
    let alice = alice();
    let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let create_id = create.event_id.clone();
    store.create_room(&create, &[]).await.expect("create_room");
    let app = router_with_store(config(), store.clone(), tempfile);

    let join = EventBuilder::new(alice.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(alice.as_str().to_owned())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .build()
        .expect("build join");
    let join_id = join.event_id.clone();
    let msg = message_on(&alice, &room_id, &join_id, "after join", 1_700_000_002_000);
    let msg_id = msg.event_id.clone();

    // `join` appears twice, `msg` (which references join) once.
    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&join, &join, &msg])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(join_id.as_str())),
        Some(&json!({})),
        "body = {body}"
    );
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(msg_id.as_str())),
        Some(&json!({})),
        "body = {body}"
    );
    // Both distinct events persisted; the message is the timeline head.
    let (timeline, _state) = store.forward_extremities(&room_id).await.unwrap().unwrap();
    assert_eq!(timeline, [msg_id].into_iter().collect());
}

#[tokio::test]
async fn send_is_idempotent_on_duplicate_txn_id() {
    let (app, store, room_id, alice, join_id) = seed_joined_room().await;
    let msg = message_on(&alice, &room_id, &join_id, "once", 1_700_000_001_000);
    let msg_id = msg.event_id.clone();

    let (s1, _) = put_json(&app, &send_path("dup"), &txn(&[&msg])).await;
    assert_eq!(s1, StatusCode::OK);

    // Re-send the same (origin, txn_id): acknowledged without reprocessing,
    // empty results map.
    let (s2, body2) = put_json(&app, &send_path("dup"), &txn(&[&msg])).await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(body2, json!({ "pdus": {} }), "body = {body2}");

    // The event is present exactly once.
    let fetched = store.get_events(&[msg_id.as_ref()]).await.unwrap();
    assert_eq!(fetched.len(), 1);
}

#[tokio::test]
async fn send_ignores_edus() {
    // A transaction carrying EDUs is accepted; the EDUs are dropped and the
    // PDU is still processed.
    let (app, _store, room_id, alice, join_id) = seed_joined_room().await;
    let msg = message_on(&alice, &room_id, &join_id, "with edus", 1_700_000_001_000);
    let msg_id = msg.event_id.clone();
    let mut body = txn(&[&msg]);
    body["edus"] = json!([{ "edu_type": "m.typing", "content": {} }]);

    let (status, resp) = put_json(&app, &send_path("txn1"), &body).await;

    assert_eq!(status, StatusCode::OK, "body = {resp}");
    assert_eq!(
        resp.get("pdus").and_then(|p| p.get(msg_id.as_str())),
        Some(&json!({})),
        "body = {resp}"
    );
}

#[tokio::test]
async fn send_rejects_oversized_transaction() {
    let (app, _store, room_id, alice, join_id) = seed_joined_room().await;
    // 51 PDUs > the 50 spec maximum.
    let events: Vec<neutrino_common::Event> = (0..51)
        .map(|i| message_on(&alice, &room_id, &join_id, "x", 1_700_000_001_000 + i))
        .collect();
    let refs: Vec<&neutrino_common::Event> = events.iter().collect();

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&refs)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

#[tokio::test]
async fn send_empty_transaction_is_ok() {
    let (app, _store, _room_id, _alice, _join_id) = seed_joined_room().await;
    let (status, body) = put_json(
        &app,
        &send_path("txn1"),
        &json!({ "origin": "remote.example.org", "origin_server_ts": 1u64 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(body, json!({ "pdus": {} }), "body = {body}");
}

#[tokio::test]
async fn send_malformed_body_returns_400() {
    let (app, _store, _room_id, _alice, _join_id) = seed_joined_room().await;
    let req = Request::builder()
        .method("PUT")
        .uri(send_path("txn1"))
        .header("content-type", "application/json")
        .body(Body::from(b"not json".to_vec()))
        .unwrap();
    let (status, body) = drive(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
}
