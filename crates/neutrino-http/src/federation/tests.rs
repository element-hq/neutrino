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
use neutrino_store::{EventStore, FederationOutbox, RoomStore, StagingStore, StateStore};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tower::ServiceExt;

use crate::federation::client::FederationClientError;
use crate::federation::gapfill::MissingEventsFetcher;
use crate::{router, router_with_store, router_with_store_and_fetcher};

/// The arguments one `fetch` call was made with, recorded so a test can assert
/// the gap-fill loop targets the right frontier / boundary / limit.
#[derive(Clone)]
struct FetchCall {
    latest: Vec<OwnedEventId>,
    earliest: Vec<OwnedEventId>,
    limit: u32,
}

/// Deterministic gap-fill [`MissingEventsFetcher`] for the inbound `/send`
/// tests. Returns a scripted outcome and records every call's arguments, so a
/// test can drive (and inspect) the staging gap-fill loop without any network.
struct StubFetcher {
    // Interior mutability so a test can seed the router with the stub, then set
    // the response *after* learning the real room/event ids from the seed.
    outcome: std::sync::Mutex<StubOutcome>,
    calls: std::sync::Mutex<Vec<FetchCall>>,
}

enum StubOutcome {
    /// `Ok(empty)` — the peer has nothing new (an unfillable gap).
    NoProgress,
    /// `Ok(events)` — return these raw PDUs (rebuilt from JSON) on every call.
    Events(Vec<String>),
    /// `Ok(batch)` per call, popped front-first; exhausted ⇒ `Ok(empty)`. Drives
    /// a multi-round gap-fill (peer dribbles ancestry a chunk at a time).
    Sequence(std::collections::VecDeque<Vec<String>>),
    /// `Err(Status(code))` — a peer HTTP failure.
    Error(u16),
}

impl StubFetcher {
    fn no_progress() -> std::sync::Arc<Self> {
        Self::with(StubOutcome::NoProgress)
    }

    fn erroring(code: u16) -> std::sync::Arc<Self> {
        Self::with(StubOutcome::Error(code))
    }

    fn with(outcome: StubOutcome) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            outcome: std::sync::Mutex::new(outcome),
            calls: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn raws_of(events: &[&neutrino_common::Event]) -> Vec<String> {
        events.iter().map(|e| e.raw.get().to_owned()).collect()
    }

    /// Make subsequent `fetch` calls return these events (their canonical raw
    /// bytes), the same batch every call. Used after seeding.
    fn set_events(&self, events: &[&neutrino_common::Event]) {
        *self.outcome.lock().unwrap() = StubOutcome::Events(Self::raws_of(events));
    }

    /// Make `fetch` return each batch in turn (one per round). Used to drive a
    /// multi-round gap-fill where the peer reveals ancestry incrementally.
    fn set_sequence(&self, batches: Vec<Vec<&neutrino_common::Event>>) {
        let q = batches.iter().map(|b| Self::raws_of(b)).collect();
        *self.outcome.lock().unwrap() = StubOutcome::Sequence(q);
    }

    fn calls(&self) -> Vec<FetchCall> {
        self.calls.lock().unwrap().clone()
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl MissingEventsFetcher for StubFetcher {
    async fn fetch(
        &self,
        _origin: &ServerName,
        _room_id: &RoomId,
        latest: &[OwnedEventId],
        earliest: &[OwnedEventId],
        limit: u32,
    ) -> Result<Vec<Box<RawJsonValue>>, FederationClientError> {
        self.calls.lock().unwrap().push(FetchCall {
            latest: latest.to_vec(),
            earliest: earliest.to_vec(),
            limit,
        });
        let rebuild = |jsons: &[String]| {
            jsons
                .iter()
                .map(|s| RawJsonValue::from_string(s.clone()).expect("stub pdu is valid JSON"))
                .collect()
        };
        match &mut *self.outcome.lock().unwrap() {
            StubOutcome::NoProgress => Ok(Vec::new()),
            StubOutcome::Events(jsons) => Ok(rebuild(jsons)),
            StubOutcome::Sequence(batches) => {
                Ok(batches.pop_front().map(|b| rebuild(&b)).unwrap_or_default())
            }
            StubOutcome::Error(code) => Err(FederationClientError::Status(*code)),
        }
    }
}

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
    // Default to a no-progress fetcher: the in-order tests never trigger
    // gap-fill, and the one that does (`…unfillable_missing_ancestry`) wants
    // exactly "the peer has nothing" — deterministic, no network.
    seed_joined_room_with_fetcher(StubFetcher::no_progress()).await
}

/// As [`seed_joined_room`] but with an injected gap-fill fetcher, for the
/// tests that exercise the staging gap-fill loop.
async fn seed_joined_room_with_fetcher(
    fetcher: Arc<dyn MissingEventsFetcher>,
) -> (
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
    let router = router_with_store_and_fetcher(config(), store.clone(), tempfile, fetcher);
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

/// Build an `m.room.topic` *state* PDU sitting on `head` (both DAGs). Used as
/// gap-fill ancestry — a state event belongs in the state DAG that a child's
/// `prev_state_events` reference, and a topic set by the creator auth-passes.
fn topic_on(
    sender: &OwnedUserId,
    room_id: &OwnedRoomId,
    head: &OwnedEventId,
    topic: &str,
    ts: u64,
) -> neutrino_common::Event {
    EventBuilder::new(sender.clone(), "m.room.topic".to_owned())
        .room_id(room_id.clone())
        .state_key(String::new())
        .content(json!({ "topic": topic }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head.clone()])
        .origin_server_ts(ts)
        .build()
        .expect("build topic")
}

// ── async-worker poll helpers ────────────────────────────────────────────────
//
// `/send` now stages PDUs and returns 200 immediately; the background worker
// (`federation::worker`, auto-spawned by the test router) integrates them
// asynchronously. So the e2e tests assert the *immediate* response, then poll
// the store for the eventual outcome. ~5s budget at 10ms granularity — the
// success path has no backoff, so this resolves in tens of ms; the bound only
// guards against a hang.

/// Poll until `id` is committed (present in `events`), returning the row.
async fn wait_committed(store: &SqliteStore, id: &ruma::EventId) -> neutrino_common::Event {
    for _ in 0..500 {
        if let Some(e) = store.get_events(&[id]).await.unwrap().into_iter().next() {
            return e;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("event {id} not committed within timeout");
}

/// Poll until the room's timeline forward extremity set is exactly `{expected}`.
async fn wait_timeline_head(store: &SqliteStore, room_id: &RoomId, expected: &ruma::EventId) {
    let want: std::collections::BTreeSet<OwnedEventId> =
        [expected.to_owned()].into_iter().collect();
    for _ in 0..500 {
        if let Ok(Some((timeline, _state))) = store.forward_extremities(room_id).await
            && timeline == want
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timeline head did not advance to {expected} within timeout");
}

/// Poll until `id` is one of the room's timeline forward extremities (a leaf).
/// Weaker than [`wait_timeline_head`] — used where the async worker may leave a
/// *transient* extra extremity (e.g. a child applied before its timeline parent
/// across two drain passes), which is valid federation behaviour and self-heals.
async fn wait_timeline_contains(store: &SqliteStore, room_id: &RoomId, id: &ruma::EventId) {
    for _ in 0..500 {
        if let Ok(Some((timeline, _state))) = store.forward_extremities(room_id).await
            && timeline.contains(id)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("{id} did not become a timeline extremity within timeout");
}

/// Poll until the room has no staged rows left (the worker drained it).
async fn wait_staging_empty(store: &SqliteStore, room_id: &RoomId) {
    for _ in 0..500 {
        if store
            .staged_for_room(room_id)
            .await
            .map(|v| v.is_empty())
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("staging for {room_id} did not drain within timeout");
}

/// Poll until the stub fetcher has recorded at least one call — i.e. the worker
/// reached the gap-fill for a PDU with missing ancestry.
async fn wait_fetch_attempted(fetcher: &StubFetcher) {
    for _ in 0..500 {
        if fetcher.call_count() >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("fetcher was never called within timeout");
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
    // The PDU was staged: an optimistic empty per-PDU result (no error).
    let result = body.get("pdus").and_then(|p| p.get(msg_id.as_str()));
    assert_eq!(result, Some(&json!({})), "body = {body}");

    // The worker integrates it asynchronously: it lands in the store (not
    // rejected) and the timeline head advances to it.
    let fetched = wait_committed(&store, msg_id.as_ref()).await;
    assert!(!fetched.rejected);
    wait_timeline_head(&store, &room_id, msg_id.as_ref()).await;
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
        "staging always reports an empty result; body = {body}"
    );
    // The worker persists the reject (federation policy); bob stays absent from
    // current_state.
    let fetched = wait_committed(&store, bob_join_id.as_ref()).await;
    assert!(fetched.rejected);
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", bob.as_str())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn send_unfillable_ancestry_stays_unapplied() {
    // A PDU referencing a parent we don't have, and the peer (a no-progress
    // fetcher) returns nothing → the gap is unfillable. `/send` still 200s
    // (staged); the worker tries the gap-fill, fails, backs off, and the PDU is
    // never committed (left durably staged for a later retry/restart).
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
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
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "staging succeeds even when the eventual gap-fill won't; body = {body}"
    );

    // The worker reaches the gap-fill (fetcher called) but can't ground the
    // ancestry, so the child is never committed.
    wait_fetch_attempted(&fetcher).await;
    assert!(
        store
            .get_events(&[child_id.as_ref()])
            .await
            .unwrap()
            .is_empty(),
        "child must not be committed while its ancestry is unfillable"
    );
}

#[tokio::test]
async fn send_gapfills_missing_ancestry_then_accepts() {
    // The success path that was inert under the old `NoFetcher`: a PDU arrives
    // referencing an `orphan` we don't hold; the fetcher supplies the orphan,
    // it is staged → promoted (authed) → and the child is then accepted. Both
    // events end up committed.
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
    // The missing ancestor must be a *state* event (it lives in the state DAG
    // the child references via `prev_state_events`); a non-state parent would
    // be rejected. A topic set by the creator auth-passes.
    let orphan = topic_on(
        &alice,
        &room_id,
        &join_id,
        "set in the gap",
        1_700_000_002_000,
    );
    let child = message_on(
        &alice,
        &room_id,
        &orphan.event_id,
        "child",
        1_700_000_003_000,
    );
    let orphan_id = orphan.event_id.clone();
    let child_id = child.event_id.clone();

    // Now that the orphan exists, make the peer supply it on the next fetch.
    fetcher.set_events(&[&orphan]);

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&child])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    // Optimistic staged result (no error) — the actual accept happens async.
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "child should stage; body = {body}"
    );

    // The worker gap-fills the orphan and applies both. Once the child commits,
    // the worker has parked, so the fetch count is stable.
    let child_row = wait_committed(&store, child_id.as_ref()).await;
    assert!(!child_row.rejected);
    // Ancestry grounds in a single round, so exactly one fetch (a needless
    // extra round would mean wasted peer traffic).
    assert_eq!(fetcher.call_count(), 1, "exactly one gap-fill round");

    // Both the fetched orphan and the child are committed (not rejected), and
    // nothing lingers in staging.
    let committed = store
        .get_events(&[orphan_id.as_ref(), child_id.as_ref()])
        .await
        .unwrap();
    assert_eq!(committed.len(), 2, "orphan + child both committed");
    assert!(committed.iter().all(|e| !e.rejected));
    let still_missing = store
        .ancestry_gap(&room_id, &[child_id.as_ref()])
        .await
        .unwrap();
    assert!(
        still_missing.staged.is_empty(),
        "promoted ancestry must be unstaged"
    );
}

#[tokio::test]
async fn send_gapfill_fetch_targets_frontier_and_state_boundary() {
    // Pin the outbound fetch arguments: `latest` is the triggering event (the
    // walk-from point), `earliest` is the room's *state-DAG* forward extremity
    // (not the timeline one — the `state_dag_boundary` this PR introduced), and
    // the first round uses the initial limit. A no-progress fetcher records one
    // call; the resulting unfillable error is irrelevant here.
    let fetcher = StubFetcher::no_progress();
    let (app, _store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
    let orphan = topic_on(&alice, &room_id, &join_id, "x", 1_700_000_002_000);
    let child = message_on(
        &alice,
        &room_id,
        &orphan.event_id,
        "child",
        1_700_000_003_000,
    );

    let _ = put_json(&app, &send_path("txn1"), &txn(&[&child])).await;

    // The gap is unfillable (no-progress peer), so the worker backs off and
    // retries — pin the *first* round's arguments rather than the call count.
    wait_fetch_attempted(&fetcher).await;
    let calls = fetcher.calls();
    assert_eq!(
        calls[0].latest,
        vec![child.event_id.clone()],
        "latest is the triggering event (nothing staged yet)"
    );
    assert_eq!(
        calls[0].earliest,
        vec![join_id.clone()],
        "earliest is the state-DAG forward extremity (the join), not the timeline head"
    );
    assert_eq!(
        calls[0].limit, 10,
        "first round uses the initial gap-fill limit"
    );
}

#[tokio::test]
async fn send_gapfills_over_multiple_rounds() {
    // The peer dribbles ancestry one event per round: child→A→B→join(held).
    // Round 1 fetches A, round 2 fetches B; the loop must double the limit and
    // carry the staged frontier in `latest` so it doesn't re-request A.
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
    let b = topic_on(&alice, &room_id, &join_id, "b", 1_700_000_002_000);
    let a = topic_on(&alice, &room_id, &b.event_id, "a", 1_700_000_003_000);
    let child = message_on(&alice, &room_id, &a.event_id, "child", 1_700_000_004_000);
    let child_id = child.event_id.clone();
    // Newest-first dribble: A (child's parent) then B (A's parent).
    fetcher.set_sequence(vec![vec![&a], vec![&b]]);

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&child])).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "child stages; body = {body}"
    );

    // The two-round gap-fill all happens inside one worker drain (fetch A, then
    // B, then ground); once the child commits the worker parks, so the recorded
    // calls are stable.
    wait_committed(&store, child_id.as_ref()).await;
    let calls = fetcher.calls();
    assert_eq!(calls.len(), 2, "two gap-fill rounds");
    assert_eq!(calls[0].limit, 10);
    assert_eq!(calls[1].limit, 20, "limit doubles each round");
    assert!(
        calls[1].latest.contains(&a.event_id),
        "round 2 carries the staged frontier (A) in `latest` so the peer skips it"
    );

    // All of A, B, child committed and not rejected.
    let committed = store
        .get_events(&[a.event_id.as_ref(), b.event_id.as_ref(), child_id.as_ref()])
        .await
        .unwrap();
    assert_eq!(committed.len(), 3, "B + A + child all committed");
    assert!(committed.iter().all(|e| !e.rejected));
}

#[tokio::test]
async fn send_resend_after_gapfill_is_idempotent() {
    // After a gap-fill commits the child, a re-send (different txn_id, same
    // event) is a clean no-op via the fast-path persisted-check — no error, and
    // no second peer fetch.
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
    let orphan = topic_on(&alice, &room_id, &join_id, "x", 1_700_000_002_000);
    let child = message_on(
        &alice,
        &room_id,
        &orphan.event_id,
        "child",
        1_700_000_003_000,
    );
    let child_id = child.event_id.clone();
    fetcher.set_events(&[&orphan]);

    let (_s1, body1) = put_json(&app, &send_path("txn1"), &txn(&[&child])).await;
    assert_eq!(
        body1.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "first send staged; body = {body1}"
    );
    // Let the gap-fill complete and the child commit.
    wait_committed(&store, child_id.as_ref()).await;
    let after_first = fetcher.call_count();

    // Resend under a fresh txn_id (a same txn_id would short-circuit on txn
    // dedup; we want to exercise the apply-level idempotency instead). The
    // handler re-stages the (now-committed) event; the worker applies it via
    // the persisted-check no-op and unstages it — no gap-fill, no re-fetch.
    let (status, body2) = put_json(&app, &send_path("txn2"), &txn(&[&child])).await;
    assert_eq!(status, StatusCode::OK, "body = {body2}");
    assert_eq!(
        body2.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "resend stages; body = {body2}"
    );
    // Wait for the worker to actually process the re-staged event (it drains
    // back to empty), then assert it took the fast-path apply — no re-fetch.
    // Deterministic: the count can never exceed `after_first`, since an
    // already-committed event hits the persisted-check, never the gap-fill.
    wait_staging_empty(&store, &room_id).await;
    assert_eq!(
        fetcher.call_count(),
        after_first,
        "resend of an already-committed event must not re-fetch"
    );
}

#[tokio::test]
async fn send_fetcher_failure_leaves_pdu_unapplied() {
    // The peer is unreachable / errors: the worker's gap-fill can't proceed, so
    // the staged PDU is never committed (it backs off and waits for a retry /
    // restart). `/send` itself still 200s — the failure is off the request path.
    let fetcher = StubFetcher::erroring(502);
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;
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
    assert_eq!(
        body.get("pdus").and_then(|p| p.get(child_id.as_str())),
        Some(&json!({})),
        "staging succeeds; the peer failure is async; body = {body}"
    );
    // The worker asked the (failing) peer, then gave up for now.
    wait_fetch_attempted(&fetcher).await;
    assert!(
        store
            .get_events(&[child_id.as_ref()])
            .await
            .unwrap()
            .is_empty(),
        "child must not be persisted on fetch failure"
    );
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

    // child (second) listed before parent (first). The worker integrates the
    // whole batch off the request path; both must end up committed (not
    // rejected) regardless of array order. Deterministic toposort *ordering* is
    // covered by `toposort_orders_parents_before_children` below — here we only
    // assert durability + that the child lands as a timeline leaf, because the
    // async worker may drain the two across separate passes (a child applied
    // before its timeline parent is valid out-of-order federation receipt and
    // leaves a transient extra extremity that self-heals).
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
    // Both commit, neither rejected; the child is a timeline extremity.
    let first_row = wait_committed(&store, first_id.as_ref()).await;
    let second_row = wait_committed(&store, second_id.as_ref()).await;
    assert!(!first_row.rejected && !second_row.rejected);
    wait_timeline_contains(&store, &room_id, second_id.as_ref()).await;
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

    // `join` appears twice, `msg` (which references join) once. The handler
    // dedups by event_id before staging (and staging is event_id-keyed), so the
    // worker's toposort never sees the duplicate that used to underflow it.
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
    // Both distinct events persisted; the message ends up the timeline head.
    wait_committed(&store, join_id.as_ref()).await;
    wait_timeline_head(&store, &room_id, msg_id.as_ref()).await;
}

#[tokio::test]
async fn send_is_idempotent_on_duplicate_txn_id() {
    let (app, store, room_id, alice, join_id) = seed_joined_room().await;
    let msg = message_on(&alice, &room_id, &join_id, "once", 1_700_000_001_000);
    let msg_id = msg.event_id.clone();

    let (s1, _) = put_json(&app, &send_path("dup"), &txn(&[&msg])).await;
    assert_eq!(s1, StatusCode::OK);
    // The worker integrates it.
    wait_committed(&store, msg_id.as_ref()).await;

    // Re-send the same (origin, txn_id): acknowledged without reprocessing,
    // empty results map (the cheap whole-txn dedup short-circuits before any
    // staging).
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
    // PDU is still staged + processed.
    let (app, store, room_id, alice, join_id) = seed_joined_room().await;
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
    // The PDU is integrated despite the EDUs in the envelope.
    wait_committed(&store, msg_id.as_ref()).await;
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

#[tokio::test]
async fn worker_drains_rows_staged_before_startup() {
    // Restart recovery: PDUs staged by a previous run (here: staged directly,
    // simulating a crash after staging but before processing) are drained when
    // the worker starts — its startup enumeration of `staged_rooms()` picks the
    // room up with no poke from the handler.
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

    // Stage a message *before* the router (and therefore the worker) exists.
    let msg = message_on(
        &alice,
        &room_id,
        &join_id,
        "staged before boot",
        1_700_000_001_000,
    );
    let msg_id = msg.event_id.clone();
    let origin: &ServerName = "remote.example.org".try_into().unwrap();
    assert!(
        store
            .stage_pdu(origin, &room_id, &msg.event_id, &msg.raw)
            .await
            .unwrap()
    );

    // Mounting the router spawns the worker, which enumerates the staged room
    // on startup and drains it — no `/send` request involved.
    let _app = router_with_store_and_fetcher(
        config(),
        store.clone(),
        tempfile,
        StubFetcher::no_progress(),
    );
    wait_committed(&store, msg_id.as_ref()).await;
    wait_timeline_head(&store, &room_id, msg_id.as_ref()).await;
}

#[tokio::test]
async fn worker_wedged_pdu_does_not_block_sibling() {
    // One PDU has unfillable ancestry (it backs off forever); an independent,
    // directly-appliable PDU in the same room must still be processed. Proves a
    // backing-off event is skipped, not head-of-line blocking.
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id) =
        seed_joined_room_with_fetcher(fetcher.clone()).await;

    // Wedged: references an orphan we never hold and the peer never supplies.
    let orphan = message_on(&alice, &room_id, &join_id, "orphan", 1_700_000_002_000);
    let wedged = message_on(
        &alice,
        &room_id,
        &orphan.event_id,
        "wedged",
        1_700_000_003_000,
    );
    // Healthy: sits directly on the committed join, applies immediately.
    let healthy = message_on(&alice, &room_id, &join_id, "healthy", 1_700_000_004_000);
    let wedged_id = wedged.event_id.clone();
    let healthy_id = healthy.event_id.clone();

    let (status, _) = put_json(&app, &send_path("txn1"), &txn(&[&wedged, &healthy])).await;
    assert_eq!(status, StatusCode::OK);

    // `healthy` commits despite the wedged sibling, and the wedged PDU reaches
    // its (failing) gap-fill so it is now in a backoff window.
    wait_committed(&store, healthy_id.as_ref()).await;
    wait_fetch_attempted(&fetcher).await;

    // Second wave: a *fresh* event arrives after `wedged` has failed once and is
    // backing off. It must still drain — proving a permanently-failing PDU never
    // head-of-line-blocks later arrivals across drain passes (the worker re-reads
    // the backlog and makes progress on the eligible event whether `wedged` is
    // skipped in its backoff window or retried-and-fails again). The first wave
    // alone can't show this: there both PDUs were indegree-0 and processed in one
    // pass, so `healthy` would commit even if backoff were broken.
    let healthy2 = message_on(&alice, &room_id, &join_id, "healthy2", 1_700_000_005_000);
    let healthy2_id = healthy2.event_id.clone();
    let (status, _) = put_json(&app, &send_path("txn2"), &txn(&[&healthy2])).await;
    assert_eq!(status, StatusCode::OK);
    wait_committed(&store, healthy2_id.as_ref()).await;

    // The wedged PDU is still uncommitted (its ancestry is permanently unfillable).
    assert!(
        store
            .get_events(&[wedged_id.as_ref()])
            .await
            .unwrap()
            .is_empty(),
        "the wedged PDU must not be committed"
    );
}

#[tokio::test]
async fn send_drops_pdu_for_unknown_room() {
    // A PDU for a room we never created (never joined) is dropped by the worker
    // rather than retried forever — otherwise a peer could accumulate
    // un-drainable staged rows + a permanent per-room task by naming nonexistent
    // rooms. `/send` still 200s (the drop is async, off the request path).
    let (app, store, _room_id, alice, _join_id) = seed_joined_room().await;

    // A standalone room id we never register, plus a message that references it.
    let other_create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let other_room = other_create.room_id.clone();
    let msg = message_on(
        &alice,
        &other_room,
        &other_create.event_id,
        "into the void",
        1_700_000_009_000,
    );
    let msg_id = msg.event_id.clone();

    let (status, body) = put_json(&app, &send_path("txn1"), &txn(&[&msg])).await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    // The worker drains the staged row by *dropping* it (unknown room), so
    // staging empties and the event is never committed.
    wait_staging_empty(&store, &other_room).await;
    assert!(
        store
            .get_events(&[msg_id.as_ref()])
            .await
            .unwrap()
            .is_empty(),
        "a PDU for an unknown room must not be committed"
    );
}

// ===================================================================
// Server-Server join (Milestone A) — inbound make_join + send_join,
// where WE are the resident server. A remote user (`@zara:remote.example`)
// joins a room we host.
// ===================================================================

const ALICE: &str = "@alice:example.org";
const ZARA: &str = "@zara:remote.example";
const YAN: &str = "@yan:other.example";

/// Seed a room created by `alice` (our user). `initial` is a chain of state
/// events linked oldest-first after the create event (each references the
/// previous as its sole `prev`/`prev_state`). Returns the router (mounted over
/// the seeded store), a store handle (for outbox/state assertions), the room
/// id, and the current state-DAG head — the event a federated membership must
/// reference.
async fn seed_room(
    initial: &[(&str, &str, &str, Value)],
) -> (axum::Router, Arc<SqliteStore>, OwnedRoomId, OwnedEventId) {
    let (store, tempfile) = fresh_store().await;
    let creator = alice();
    let create = EventBuilder::new(creator, "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let mut head = create.event_id.clone();
    let mut events = Vec::new();
    for (sender, ty, state_key, content) in initial {
        let sender: OwnedUserId = sender.parse().expect("sender");
        let ev = EventBuilder::new(sender, (*ty).to_owned())
            .room_id(room_id.clone())
            .state_key((*state_key).to_owned())
            .content(content.clone())
            .prev_events(vec![head.clone()])
            .prev_state_events(vec![head.clone()])
            .build()
            .expect("build initial state event");
        head = ev.event_id.clone();
        events.push(ev);
    }
    store
        .create_room(&create, &events)
        .await
        .expect("create_room");
    let router = router_with_store(config(), store.clone(), tempfile);
    (router, store, room_id, head)
}

/// A public room: alice joins, then opens it to `public`.
async fn seed_public_room() -> (axum::Router, Arc<SqliteStore>, OwnedRoomId, OwnedEventId) {
    seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (
            ALICE,
            "m.room.join_rules",
            "",
            json!({ "join_rule": "public" }),
        ),
    ])
    .await
}

/// Build a completed remote `m.room.member`/`join` event referencing `head`
/// (as the joining server would after `make_join`).
fn remote_join(room_id: &RoomId, head: &OwnedEventId, user: &str) -> neutrino_common::Event {
    let user: OwnedUserId = user.parse().expect("user");
    EventBuilder::new(user.clone(), "m.room.member".to_owned())
        .room_id(room_id.to_owned())
        .state_key(user.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head.clone()])
        .build()
        .expect("build remote join")
}

fn make_join_path(room_id: &RoomId, user: &str) -> String {
    format!("/_matrix/federation/v1/make_join/{room_id}/{user}?ver={ROOM_VERSION_ID}")
}

fn send_join_path(room_id: &RoomId, event_id: &OwnedEventId) -> String {
    format!("/_matrix/federation/v2/send_join/{room_id}/{event_id}")
}

/// PUT a raw event body (the `send_join` request shape).
async fn put_event(app: &axum::Router, path: &str, raw: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(raw.to_owned().into_bytes()))
        .unwrap();
    drive(app, req).await
}

// --- make_join ---------------------------------------------------------

#[tokio::test]
async fn make_join_returns_template_without_auth_events() {
    let (router, _store, room_id, head) = seed_public_room().await;

    let (status, body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["room_version"], ROOM_VERSION_ID);

    let event = &body["event"];
    assert_eq!(event["type"], "m.room.member");
    assert_eq!(event["content"]["membership"], "join");
    assert_eq!(event["sender"], ZARA);
    assert_eq!(event["state_key"], ZARA);

    // MSC4242: prev_state_events present (points at our current state head),
    // and NO non-empty auth_events (the resident computes them at apply time).
    let prev_state = event["prev_state_events"]
        .as_array()
        .expect("prev_state_events array");
    assert_eq!(prev_state.len(), 1);
    assert_eq!(prev_state[0], head.as_str());
    match event.get("auth_events") {
        None => {}
        Some(Value::Array(a)) => assert!(a.is_empty(), "auth_events must be empty: {a:?}"),
        other => panic!("unexpected auth_events: {other:?}"),
    }
}

#[tokio::test]
async fn make_join_unknown_room_returns_404() {
    let (router, _store, _room_id, _head) = seed_public_room().await;
    let unknown = ruma::RoomId::parse("!nope:example.org").unwrap();
    let (status, body) = get(&router, &make_join_path(&unknown, ZARA)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}

#[tokio::test]
async fn make_join_incompatible_version_returns_400() {
    let (router, _store, room_id, _head) = seed_public_room().await;
    // No `ver` matching ours (request an old version).
    let path = format!("/_matrix/federation/v1/make_join/{room_id}/{ZARA}?ver=1");
    let (status, body) = get(&router, &path).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INCOMPATIBLE_ROOM_VERSION");
    assert_eq!(body["room_version"], ROOM_VERSION_ID);
}

#[tokio::test]
async fn make_join_invite_only_uninvited_returns_403() {
    // Default join rule is invite-only; zara was never invited.
    let (router, _store, room_id, _head) = seed_room(&[(
        ALICE,
        "m.room.member",
        ALICE,
        json!({ "membership": "join" }),
    )])
    .await;
    let (status, body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

#[tokio::test]
async fn make_join_banned_user_returns_403() {
    // Public room, but zara is banned.
    let (router, _store, room_id, _head) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (
            ALICE,
            "m.room.join_rules",
            "",
            json!({ "join_rule": "public" }),
        ),
        (ALICE, "m.room.member", ZARA, json!({ "membership": "ban" })),
    ])
    .await;
    let (status, body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

// --- send_join ---------------------------------------------------------

#[tokio::test]
async fn send_join_admits_remote_user_and_returns_state_dag() {
    let (router, store, room_id, head) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    let join_id = join.event_id.clone();

    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &join_id), join.raw.get()).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // MSC4242 response shape: state_dag + timeline + event, and NO auth_chain.
    assert!(
        body.get("auth_chain").is_none(),
        "must not return auth_chain"
    );
    assert!(body["timeline"].is_array());
    assert_eq!(body["event"]["sender"], ZARA);

    let state_dag = body["state_dag"].as_array().expect("state_dag array");
    // The whole state DAG back to create is present.
    assert!(
        state_dag.iter().any(|e| e["type"] == "m.room.create"),
        "state_dag must include the create event"
    );
    // zara's join landed in our current state.
    let member = store
        .current_state_event(&room_id, "m.room.member", ZARA)
        .await
        .unwrap()
        .expect("zara member row");
    assert_eq!(member.event_id, join_id);
}

#[tokio::test]
async fn send_join_distributes_to_other_room_servers_not_the_joiner() {
    // Room already has a remote member on other.example. zara (remote.example)
    // joins → we must fan the join out to other.example, but NOT back to the
    // joiner's own server, nor to ourselves.
    let (router, store, room_id, head) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (
            ALICE,
            "m.room.join_rules",
            "",
            json!({ "join_rule": "public" }),
        ),
        (YAN, "m.room.member", YAN, json!({ "membership": "join" })),
    ])
    .await;

    let join = remote_join(&room_id, &head, ZARA);
    let join_id = join.event_id.clone();
    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &join_id), join.raw.get()).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // other.example gets the join (distribution duty).
    let other = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert!(
        other.iter().any(|e| e.event_id == join_id),
        "other.example must receive zara's join"
    );
    // The joiner's own server already has it — never echoed back.
    assert!(
        store
            .pending_pdus(ruma::server_name!("remote.example"), usize::MAX)
            .await
            .unwrap()
            .is_empty(),
        "must not echo the join back to the joiner"
    );
    // We never federate to ourselves.
    assert!(
        store
            .pending_pdus(ruma::server_name!("example.org"), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn send_join_rejected_join_returns_403() {
    // Invite-only room, zara not invited → apply rejects → 403, not persisted.
    let (router, store, room_id, head) = seed_room(&[(
        ALICE,
        "m.room.member",
        ALICE,
        json!({ "membership": "join" }),
    )])
    .await;
    let join = remote_join(&room_id, &head, ZARA);
    let join_id = join.event_id.clone();
    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &join_id), join.raw.get()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", ZARA)
            .await
            .unwrap()
            .is_none(),
        "a refused join must not enter current state"
    );
}

#[tokio::test]
async fn send_join_event_id_path_mismatch_returns_400() {
    let (router, _store, room_id, head) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    // Path id deliberately does not match the body's computed id.
    let wrong = ruma::EventId::parse("$wrongwrongwrong").unwrap().to_owned();
    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &wrong), join.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_join_is_idempotent_on_resend() {
    let (router, _store, room_id, head) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    let join_id = join.event_id.clone();
    let path = send_join_path(&room_id, &join_id);

    let (s1, _b1) = put_event(&router, &path, join.raw.get()).await;
    assert_eq!(s1, StatusCode::OK);
    // A re-sent send_join (our response was lost) re-applies as a no-op and
    // returns the state again.
    let (s2, b2) = put_event(&router, &path, join.raw.get()).await;
    assert_eq!(s2, StatusCode::OK, "{b2:?}");
    assert_eq!(b2["event"]["sender"], ZARA);
}

#[tokio::test]
async fn make_join_then_send_join_round_trips() {
    // Drive the full handshake: take our make_join template, complete it the
    // way a joining server would, and send_join it back.
    let (router, store, room_id, _head) = seed_public_room().await;

    let (status, body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // Complete the template: reuse its prev_events / prev_state_events.
    let template = &body["event"];
    let prev_events = id_list(&template["prev_events"]);
    let prev_state = id_list(&template["prev_state_events"]);
    let zara: OwnedUserId = ZARA.parse().unwrap();
    let join = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(zara.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(prev_events)
        .prev_state_events(prev_state)
        .build()
        .expect("complete the template");
    let join_id = join.event_id.clone();

    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &join_id), join.raw.get()).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(body["state_dag"].is_array());

    let member = store
        .current_state_event(&room_id, "m.room.member", ZARA)
        .await
        .unwrap()
        .expect("zara joined via the handshake");
    assert_eq!(member.event_id, join_id);
}

/// Parse a JSON array of event-id strings into owned ids (dropping any that
/// don't parse). Used to lift `prev_events` / `prev_state_events` out of a
/// make_join template.
fn id_list(v: &Value) -> Vec<OwnedEventId> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str())
        .filter_map(|s| OwnedEventId::try_from(s).ok())
        .collect()
}

// ===================================================================
// Server-Server join (Milestone A) — OUTBOUND, where WE are the joining
// server. A local user joins a remote public room via the handshake.
// Two real servers: B (resident, served on an ephemeral port) and A (us,
// driven via oneshot; its outbound reqwest reaches B).
// ===================================================================

/// A `Config` with an explicit server name + localpart (the joining server A
/// is a distinct homeserver from the resident).
fn config_for(server_name: &str, localpart: &str) -> Config {
    Config {
        server_name: server_name.to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: localpart.to_string(),
        ..Default::default()
    }
}

/// `content.membership` of an event, if present.
fn membership_str(ev: &neutrino_common::Event) -> Option<String> {
    serde_json::from_str::<Value>(ev.content.get())
        .ok()
        .and_then(|c| {
            c.get("membership")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

#[tokio::test]
async fn outbound_federated_join_ingests_remote_room() {
    // Resident B hosts a public room.
    let (b_router, _b_store, room_id, _head) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;

    // Joining server A (a.example, user @bob:a.example) starts empty.
    let (a_store, a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone(), a_temp);

    let path = format!("/_matrix/client/v3/join/{room_id}?server_name={b_server}");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["room_id"], room_id.as_str());

    // @bob:a.example is now joined in A's own store...
    let member = a_store
        .current_state_event(&room_id, "m.room.member", "@bob:a.example")
        .await
        .unwrap()
        .expect("bob joined in A's store");
    assert_eq!(membership_str(&member).as_deref(), Some("join"));

    // ...and A ingested the room's state DAG (the public join rule).
    let rules = a_store
        .current_state_event(&room_id, "m.room.join_rules", "")
        .await
        .unwrap()
        .expect("join_rules ingested");
    let rule: Value = serde_json::from_str(rules.content.get()).unwrap();
    assert_eq!(rule["join_rule"], "public");
}

#[tokio::test]
async fn outbound_join_falls_back_to_next_candidate() {
    // First candidate is a dead port; the join must fall back to the live
    // resident B and still succeed.
    let (b_router, _b_store, room_id, _head) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;
    let dead = crate::federation::test_support::dead_peer().await;

    let (a_store, a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone(), a_temp);

    let path =
        format!("/_matrix/client/v3/join/{room_id}?server_name={dead}&server_name={b_server}");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    assert!(
        a_store
            .current_state_event(&room_id, "m.room.member", "@bob:a.example")
            .await
            .unwrap()
            .is_some(),
        "join must succeed via the second candidate"
    );
}

#[tokio::test]
async fn outbound_join_all_candidates_dead_returns_502() {
    let dead1 = crate::federation::test_support::dead_peer().await;
    let dead2 = crate::federation::test_support::dead_peer().await;
    let (a_store, a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store, a_temp);
    // A syntactically valid room id we don't host.
    let room = "!unknown:b.example";
    let path = format!("/_matrix/client/v3/join/{room}?server_name={dead1}&server_name={dead2}");
    let (status, _body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[test]
fn parse_server_names_handles_repeats_and_encoded_colon() {
    use crate::federation::join::parse_server_names;
    let got = parse_server_names(Some(
        "server_name=127.0.0.1%3A8008&server_name=other.example&x=y",
    ));
    let got: Vec<String> = got.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        got,
        vec!["127.0.0.1:8008".to_string(), "other.example".to_string()]
    );
    assert!(parse_server_names(None).is_empty());
}
