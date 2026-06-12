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
//!   that need a *non-flat* DAG. The CSAPI `/send` path only ever links
//!   events linearly onto the current heads, so it can't produce forks or
//!   other arbitrary shapes the DAG walker needs to traverse. These tests open
//!   a fresh `SqliteStore`, build chains with explicit `prev_events`, persist
//!   them via the trait, then mount the router on top with
//!   `router_with_store`.
//!
//! See `docs/get-missing-events.md` for the behaviour this file covers.

#![cfg(test)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::{Config, ROOM_VERSION_ID};
use neutrino_state::event_id::EventBuilder;
use neutrino_store::{
    EventStore, FederationOutbox, InviteStore, RoomStore, StagingStore, StateStore,
};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::federation::client::FederationClientError;
use crate::federation::gapfill::{MissingEventsFetcher, MissingEventsQuery};
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
        q: MissingEventsQuery<'_>,
    ) -> Result<Vec<Box<RawJsonValue>>, FederationClientError> {
        self.calls.lock().unwrap().push(FetchCall {
            latest: q.latest.to_vec(),
            earliest: q.earliest.to_vec(),
            limit: q.limit,
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

/// Build a router over a throwaway storage directory the test owns. The
/// returned `TempDir` MUST be held for the lifetime of the router — dropping
/// it deletes the database directory. Use this instead of `router(config())`
/// so each test gets an isolated DB rather than sharing `./neutrino.db`.
async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().expect("create storage tempdir");
    let mut cfg = config();
    cfg.storage_dir = tmp.path().to_path_buf();
    let app = router(cfg).await.expect("router");
    (app, tmp)
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

/// Open a fresh sqlite store in a throwaway directory. Returns both so the
/// caller can pass the store to `router_with_store` and keep the `TempDir`
/// guard alive for the lifetime of the router — dropping it removes the
/// directory and the DB (plus its `-wal`/`-shm` sidecars), unlike a bare
/// `NamedTempFile` which would orphan the sidecars.
async fn fresh_store() -> (Arc<SqliteStore>, TempDir) {
    let tempdir = TempDir::new().expect("tempdir");
    let store = Arc::new(
        SqliteStore::open_in_dir(tempdir.path())
            .await
            .expect("open sqlite"),
    );
    (store, tempdir)
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
) -> (
    axum::Router,
    OwnedRoomId,
    OwnedEventId,
    Vec<OwnedEventId>,
    TempDir,
) {
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

    let router = router_with_store(config(), store);
    (router, room_id, create_id, ids, tempfile)
}

// --- bad request: empty latest_events -----------------------------------

// Gated off under `multi-user-shim`: it seeds the room via tokenless CSAPI
// `createRoom`, which the shim rejects (401). The shim's own coverage lives in
// `tests/e2e_multi_user.rs`; this bad-request case runs in the default build.
#[cfg(not(feature = "multi-user-shim"))]
#[tokio::test]
async fn bad_request_empty_latest_events_returns_400() {
    let (app, _tmp) = test_router().await;
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

// --- bad request: non-JSON body -----------------------------------------

// Gated off under `multi-user-shim` — see `bad_request_empty_latest_events`.
#[cfg(not(feature = "multi-user-shim"))]
#[tokio::test]
async fn bad_request_non_json_body_returns_400() {
    let (app, _tmp) = test_router().await;
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

// --- bad request: missing required field --------------------------------

// Gated off under `multi-user-shim` — see `bad_request_empty_latest_events`.
#[cfg(not(feature = "multi-user-shim"))]
#[tokio::test]
async fn bad_request_missing_required_field_returns_400() {
    let (app, _tmp) = test_router().await;
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

// --- unknown room returns 404 -------------------------------------------

#[tokio::test]
async fn unknown_room_returns_404() {
    let (app, _tmp) = test_router().await;

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

// --- happy path: events between earliest and latest ---------------------

#[tokio::test]
async fn happy_path_returns_events_between_earliest_and_latest() {
    // Happy path. The 4 message events between create (earliest) and
    // msg 4 (latest) should be reachable. Assert the set of message bodies;
    // the ordering across multiple latest seeds is an
    // implementation detail per `DagStore::missing_events` (trait doc in
    // neutrino-store/src/lib.rs), so we collect into a `BTreeSet` and
    // compare set-equality only.
    let (app, room_id, create_id, msgs, _tempfile) = build_seeded_router(5).await;

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

// --- respects limit -----------------------------------------------------

#[tokio::test]
async fn respects_limit() {
    // A requested `limit` well above Synapse's old 20-floor but below MAX_LIMIT
    // is honoured in full: seed a 25-message chain, ask for limit=50, receive
    // all 26 ancestors of the latest (24 earlier messages + the self-join + the
    // create; the latest itself is the excluded boundary). Pins the divergence
    // from `min(limit, 20)` that lets the gap-fill caller's exponential growth
    // actually take effect.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(25).await;

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
    assert_eq!(n, 26, "limit above old 20-floor must be honoured; got {n}");
}

// --- MAX_LIMIT anti-spam ceiling ----------------------------------------

#[tokio::test]
async fn limit_capped_at_max() {
    // Seed > MAX_LIMIT (1000) events and ask for more: the response saturates
    // at the 1000-event anti-spam ceiling rather than honouring the request or
    // returning the whole chain.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(1001).await;

    let (status, body) = post_json(
        &app,
        &fed_path(room_id.as_str()),
        &json!({
            "earliest_events": [],
            "latest_events": [msgs[1000].as_str()],
            "limit": 5000,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let n = body
        .get("events")
        .and_then(Value::as_array)
        .expect("events array")
        .len();
    assert_eq!(n, 1000, "MAX_LIMIT ceiling not enforced; got {n}");
}

// --- default limit is 10 ------------------------------------------------

#[tokio::test]
async fn default_limit_is_10() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(15).await;

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

// --- empty earliest walks back to room root -----------------------------

#[tokio::test]
async fn empty_earliest_walks_back_to_room_root() {
    // With no `earliest_events`, the walk continues all the way back.
    // Seed 3 messages; ask for everything up to msg[2]; expect msg[1],
    // msg[0], join, create — in particular the create event must appear.
    let (app, room_id, create_id, msgs, _tempfile) = build_seeded_router(3).await;

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

// --- latest event not in room returns empty -----------------------------

#[tokio::test]
async fn latest_event_not_in_room_returns_empty() {
    // Room exists; the requested `latest` ID isn't in it. Per the design
    // doc, this is a no-op walk: no events reachable, no error, just
    // empty.
    let (app, room_id, _create_id, _msgs, _tempfile) = build_seeded_router(2).await;

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

// --- min_depth field ignored --------------------------------------------

#[tokio::test]
async fn min_depth_field_ignored() {
    // `min_depth: huge` must still return the same events as omitting
    // the field — Neutrino doesn't store depth, so the filter is a no-op
    // (the field is parsed only to satisfy serde when the wire shape
    // includes it). Pins the spec divergence.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(3).await;

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

// --- malformed room id returns 400 with errcode -------------------------

#[tokio::test]
async fn malformed_room_id_returns_400_with_errcode() {
    // The path extractor takes a `String` so we can parse manually and
    // surface a JSON `M_INVALID_PARAM` body rather than axum's default
    // plain-text 400. Mirrors the `members` handler precedent in
    // `lib.rs`.
    let (app, _tmp) = test_router().await;

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

// --- wire bytes passthrough ---------------------------------------------

#[tokio::test]
async fn wire_bytes_passthrough() {
    // Federation responses ship `Event.raw` verbatim — no enrichment.
    // v12 / MSC4242 wire bytes never carry `event_id` (it's derived from
    // the reference hash), so the field must be absent from every event
    // in the response. This pins federation = raw, CSAPI = enriched.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(2).await;

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

// --- result ordering ----------------------------------------------------

#[tokio::test]
async fn events_returned_oldest_first() {
    // The handler reverses `missing_events`' newest-first walk so the
    // response is in topological (oldest-first) order, matching Synapse.
    // Linear chain create ← join ← msg0 ← msg1 ← msg2 ← msg3; walk back
    // from msg3 with no earliest. The message-bodied events must appear
    // oldest-first — msg 0, msg 1, msg 2 (msg3 is the excluded boundary,
    // create/join carry no "msg N" body). Asserts a *sequence*, not a set,
    // so a regression to newest-first fails here.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(4).await;

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

// --- earliest boundary is excluded at the HTTP layer --------------------

#[tokio::test]
async fn earliest_message_boundary_is_excluded() {
    // The happy-path test used the create event (no body) as `earliest`, so a leak of the
    // earliest boundary would slip past its body-prefix filter. Here the
    // earliest boundary is a *message* event (detectable by body): with
    // latest=msg3, earliest=msg1, only msg2 is strictly between them.
    // msg1 (earliest) and everything below it must NOT appear.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(4).await;

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

// --- malformed min_depth still 400s via serde ---------------------------

#[tokio::test]
async fn malformed_min_depth_returns_400() {
    // `min_depth` is parsed (then ignored). A non-integer value must still
    // 400 at serde — the whole reason the field is typed rather than
    // dropped. Pins the doc claim on `RequestBody._min_depth`.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(2).await;

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

// --- malformed event id in latest 400s ----------------------------------

#[tokio::test]
async fn malformed_event_id_in_latest_returns_400() {
    // A `latest_events` entry that isn't a valid event ID fails
    // `OwnedEventId` deserialization → 400, before the store is touched.
    let (app, room_id, _create_id, _msgs, _tempfile) = build_seeded_router(1).await;

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

// --- wrong content-type 400s --------------------------------------------

#[tokio::test]
async fn wrong_content_type_returns_400() {
    // A body sent with a non-JSON content-type is rejected by the `Json`
    // extractor; the handler maps that rejection to 400 M_INVALID_PARAM
    // rather than letting axum's default (415/422) through.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(2).await;

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

// --- explicit limit=0 returns empty -------------------------------------

#[tokio::test]
async fn explicit_limit_zero_returns_empty() {
    // An explicit `limit: 0` is honored as 0 (matching Synapse's
    // `min(limit, 20)`); the handler returns 200 with an empty `events`
    // array rather than substituting the default of 10.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(3).await;

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

// --- earliest_events is required per spec --------------------------------

#[tokio::test]
async fn missing_earliest_events_returns_400() {
    // `earliest_events` is Required per the spec; omitting it 400s at
    // deserialization rather than being silently treated as empty.
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(2).await;

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

// Walk back from the chain head returns every ancestor plus the head
// itself, newest-first. create ← join ← msg0 ← msg1 ← msg2; backfill from
// msg2 yields all 5 events. The three message bodies must appear
// newest-first.
#[tokio::test]
async fn backfill_returns_chain_newest_first() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(3).await;

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

// Limit is a hard cap on the number of PDUs returned. Backfill from
// msg2 with limit=2 yields exactly the seed and its immediate parent.
#[tokio::test]
async fn backfill_respects_limit() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(3).await;

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

// The transaction envelope carries `origin` (our server name) and a
// numeric `origin_server_ts` alongside `pdus`, per the ruma /
// Synapse backfill response shape.
#[tokio::test]
async fn backfill_response_has_transaction_envelope() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(1).await;

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

// Wire bytes verbatim — v12 / MSC4242 PDUs never carry `event_id`
// (it's derived from the reference hash). Federation peers must receive the
// exact bytes that produced the id, so no enrichment is applied.
#[tokio::test]
async fn backfill_ships_wire_bytes_without_event_id() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(2).await;

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

// Unknown room → 404 M_NOT_FOUND (spec-required), not a 500 or the
// bare-text fallback.
#[tokio::test]
async fn backfill_unknown_room_returns_404() {
    let (app, _room_id, _create_id, msgs, _tempfile) = build_seeded_router(1).await;
    let unknown = "!nope:example.org";

    let (status, body) = get(&app, &backfill_path(unknown, &[msgs[0].as_str()], Some(10))).await;

    assert_eq!(status, StatusCode::NOT_FOUND, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_NOT_FOUND"),
        "body = {body}"
    );
}

// A request with no `v` parameter is rejected — there's nothing to walk
// back from. 400 M_INVALID_PARAM, mirroring the empty-`latest_events`
// rejection on the sibling endpoint.
#[tokio::test]
async fn backfill_missing_v_returns_400() {
    let (app, room_id, _create_id, _msgs, _tempfile) = build_seeded_router(1).await;

    let (status, body) = get(&app, &backfill_path(room_id.as_str(), &[], Some(10))).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(Value::as_str),
        Some("M_INVALID_PARAM"),
        "body = {body}"
    );
}

// A `v` event we don't hold is skipped, not 500'd. `events_before`
// would reject an unknown seed with InvalidInput; the handler pre-filters
// via `get_events` so an unknown seed yields an empty (200) backfill.
#[tokio::test]
async fn backfill_unknown_v_is_skipped_not_500() {
    let (app, room_id, _create_id, _msgs, _tempfile) = build_seeded_router(1).await;
    // Syntactically valid v12 id (43 base64url chars) that isn't in the room.
    let ghost = "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let (status, body) = get(&app, &backfill_path(room_id.as_str(), &[ghost], Some(10))).await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    let pdus = body.get("pdus").and_then(Value::as_array).expect("pdus");
    assert!(pdus.is_empty(), "unknown seed must yield no events: {body}");
}

// A raw (un-percent-encoded) `$` in the query still resolves. Proves
// the decoder is lenient about already-decoded sigils as well as `%24`.
#[tokio::test]
async fn backfill_accepts_raw_dollar_sigil_in_query() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(1).await;
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

// A present-but-blank `?v=` is rejected like a wholly-missing `v`.
// It must NOT slip past the empty-`v` guard as an empty-string seed and
// return a misleading 200 with no PDUs.
#[tokio::test]
async fn backfill_blank_v_returns_400() {
    let (app, room_id, _create_id, _msgs, _tempfile) = build_seeded_router(1).await;
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

// An explicit `limit=0` is rejected (400), matching Synapse's
// `if not limit: return 400` — asking for zero events is a client bug, not
// a valid empty backfill. (A *missing* limit still defaults to 10.)
#[tokio::test]
async fn backfill_limit_zero_returns_400() {
    let (app, room_id, _create_id, msgs, _tempfile) = build_seeded_router(1).await;

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
    TempDir,
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
    TempDir,
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
    let router = router_with_store_and_fetcher(config(), store.clone(), fetcher);
    (router, store, room_id, alice, join_id, tempfile)
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
    let (app, store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (app, store, room_id, _alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    // The worker commits a PDU and unstages it in two separate steps, so the
    // child can be visible in `events` a beat before its (and the orphan's)
    // staged rows are deleted. Wait for the drain to settle before asserting
    // emptiness, rather than racing the worker's follow-up unstage.
    wait_staging_empty(&store, &room_id).await;
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
    let (app, _store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (store, _tempfile) = fresh_store().await;
    let alice = alice();
    let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let create_id = create.event_id.clone();
    store.create_room(&create, &[]).await.expect("create_room");
    let app = router_with_store(config(), store.clone());

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
    let (app, store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (app, store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (app, _store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
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
    let (app, _store, _room_id, _alice, _join_id, _tempfile) = seed_joined_room().await;
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
    let (app, _store, _room_id, _alice, _join_id, _tempfile) = seed_joined_room().await;
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
    let (store, _tempfile) = fresh_store().await;
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
    let _app = router_with_store_and_fetcher(config(), store.clone(), StubFetcher::no_progress());
    wait_committed(&store, msg_id.as_ref()).await;
    wait_timeline_head(&store, &room_id, msg_id.as_ref()).await;
}

#[tokio::test]
async fn worker_wedged_pdu_does_not_block_sibling() {
    // One PDU has unfillable ancestry (it backs off forever); an independent,
    // directly-appliable PDU in the same room must still be processed. Proves a
    // backing-off event is skipped, not head-of-line blocking.
    let fetcher = StubFetcher::no_progress();
    let (app, store, room_id, alice, join_id, _tempfile) =
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
    let (app, store, _room_id, alice, _join_id, _tempfile) = seed_joined_room().await;

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
// Server-Server join — inbound make_join + send_join,
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
) -> (
    axum::Router,
    Arc<SqliteStore>,
    OwnedRoomId,
    OwnedEventId,
    TempDir,
) {
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
    let router = router_with_store(config(), store.clone());
    (router, store, room_id, head, tempfile)
}

/// A public room: alice joins, then opens it to `public`.
async fn seed_public_room() -> (
    axum::Router,
    Arc<SqliteStore>,
    OwnedRoomId,
    OwnedEventId,
    TempDir,
) {
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
    let (router, _store, room_id, head, _tempfile) = seed_public_room().await;

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
    let (router, _store, _room_id, _head, _tempfile) = seed_public_room().await;
    let unknown = ruma::RoomId::parse("!nope:example.org").unwrap();
    let (status, body) = get(&router, &make_join_path(&unknown, ZARA)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}

#[tokio::test]
async fn make_join_incompatible_version_returns_400() {
    let (router, _store, room_id, _head, _tempfile) = seed_public_room().await;
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
    let (router, _store, room_id, _head, _tempfile) = seed_room(&[(
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
    let (router, _store, room_id, _head, _tempfile) = seed_room(&[
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
    let (router, store, room_id, head, _tempfile) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    let join_id = join.event_id.clone();

    let (status, body) =
        put_event(&router, &send_join_path(&room_id, &join_id), join.raw.get()).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // MSC4242 response shape: state_dag + timeline + event, and NONE of the
    // pre-MSC4242 fields.
    for forbidden in ["auth_chain", "state", "servers_in_room"] {
        assert!(body.get(forbidden).is_none(), "must not return {forbidden}");
    }
    assert!(body["timeline"].is_array());
    assert_eq!(body["event"]["sender"], ZARA);

    // The state_dag is EXACTLY the room's state DAG back to create: create,
    // alice's join, the join rule, and zara's just-applied join — no more, no
    // fewer, no duplicates.
    let state_dag = body["state_dag"].as_array().expect("state_dag array");
    let mut got: Vec<(String, String)> = state_dag
        .iter()
        .map(|e| {
            (
                e["type"].as_str().unwrap_or_default().to_owned(),
                e["state_key"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    got.sort();
    let mut want = vec![
        ("m.room.create".to_owned(), String::new()),
        ("m.room.member".to_owned(), ALICE.to_owned()),
        ("m.room.join_rules".to_owned(), String::new()),
        ("m.room.member".to_owned(), ZARA.to_owned()),
    ];
    want.sort();
    assert_eq!(got, want, "state_dag must be exactly the room's state DAG");

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
    let (router, store, room_id, head, _tempfile) = seed_room(&[
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
    // Invite-only room (with a second server present so fan-out *would* be
    // non-empty if it happened), zara not invited → apply rejects → 403, not
    // persisted, and crucially NOT fanned out (the reject path returns before
    // persist_resolved_event).
    let (router, store, room_id, head, _tempfile) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (YAN, "m.room.member", YAN, json!({ "membership": "join" })),
    ])
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
    assert!(
        store
            .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
            .await
            .unwrap()
            .is_empty(),
        "a refused join must not be fanned out"
    );
}

#[tokio::test]
async fn send_join_event_id_path_mismatch_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    // Path id is a *real, valid, different* event id (the room's state head), so
    // the 400 can only come from the body-vs-path comparison, not a malformed id.
    let (status, body) = put_event(&router, &send_join_path(&room_id, &head), join.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_join_is_idempotent_on_resend() {
    // Use a room with a second server so the join genuinely fans out — that lets
    // us assert the *re-send* is a true no-op: no duplicate state row, and no
    // second outbox enqueue (the `effects.is_empty()` guard short-circuits).
    let (router, store, room_id, head, _tempfile) = seed_room(&[
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
    let path = send_join_path(&room_id, &join_id);

    let (s1, _b1) = put_event(&router, &path, join.raw.get()).await;
    assert_eq!(s1, StatusCode::OK);

    let outbox_after_first = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert_eq!(outbox_after_first.len(), 1, "one fan-out on first apply");

    // A re-sent send_join (our response was lost) re-applies as a no-op.
    let (s2, b2) = put_event(&router, &path, join.raw.get()).await;
    assert_eq!(s2, StatusCode::OK, "{b2:?}");
    assert_eq!(b2["event"]["sender"], ZARA);

    // No duplicate state row.
    let member = store
        .current_state_event(&room_id, "m.room.member", ZARA)
        .await
        .unwrap()
        .expect("zara member row");
    assert_eq!(member.event_id, join_id);
    // No second fan-out enqueue — the re-send took the empty-effects path.
    let outbox_after_second = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        outbox_after_second.len(),
        1,
        "re-send must not enqueue the join a second time"
    );
}

#[tokio::test]
async fn make_join_then_send_join_round_trips() {
    // Drive the full handshake: take our make_join template, complete it the
    // way a joining server would, and send_join it back.
    let (router, store, room_id, _head, _tempfile) = seed_public_room().await;

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
// Server-Server join — OUTBOUND, where WE are the joining
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
    let (b_router, _b_store, room_id, _head, _tempfile) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;

    // Joining server A (a.example, user @bob:a.example) starts empty.
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

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
    let (b_router, _b_store, room_id, _head, _tempfile) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;
    let dead = crate::federation::test_support::dead_peer().await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

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
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store);
    // A syntactically valid room id we don't host.
    let room = "!unknown:b.example";
    let path = format!("/_matrix/client/v3/join/{room}?server_name={dead1}&server_name={dead2}");
    let (status, _body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn room_scoped_join_uses_pending_invite_server() {
    // Resident B hosts a public room and is reachable on an ephemeral port.
    let (b_router, _b_store, room_id, _head, _b_temp) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;

    // Joining server A (@bob:a.example) starts empty — no `server_name` hint.
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

    // Plant a pending OOB invite for @bob:a.example whose inviter lives on B,
    // so the inviter's server resolves to the live resident.
    let inviter: OwnedUserId = format!("@alice:{b_server}").parse().unwrap();
    let throwaway = EventBuilder::new(inviter.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build throwaway create for a valid prev id");
    let invite = member_pdu(
        &inviter,
        "@bob:a.example",
        &room_id,
        "invite",
        std::slice::from_ref(&throwaway.event_id),
    );
    let bob: OwnedUserId = "@bob:a.example".parse().unwrap();
    a_store.put_invite(&room_id, &bob, &invite).await.unwrap();

    // Room-scoped join with NO `server_name` — the SDK's invite-accept path.
    let path = format!("/_matrix/client/v3/rooms/{room_id}/join");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["room_id"], room_id.as_str());

    let member = a_store
        .current_state_event(&room_id, "m.room.member", "@bob:a.example")
        .await
        .unwrap()
        .expect("bob joined via the invite-sourced server");
    assert_eq!(membership_str(&member).as_deref(), Some("join"));
}

#[tokio::test]
async fn room_scoped_join_unknown_room_no_invite_returns_404() {
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

    // Syntactically valid v12-style room id we don't host, no invite planted.
    let room = "!unknown:b.example";
    let path = format!("/_matrix/client/v3/rooms/{room}/join");
    let (status, _body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let rid = RoomId::parse(room).unwrap();
    assert!(
        a_store
            .current_state_event(&rid, "m.room.member", "@bob:a.example")
            .await
            .unwrap()
            .is_none(),
        "no join must be created when nothing could source a server"
    );
}

#[tokio::test]
async fn join_tries_hint_before_invite_fallback() {
    // Live resident B hosts the room and will satisfy the join.
    let (b_router, _b_store, room_id, _head, _b_temp) = seed_public_room().await;
    let b_server = crate::federation::test_support::spawn_stub(b_router).await;

    // A live *decoy* hint that records each make_join hit and refuses (500), so
    // the test can prove the explicit hint is attempted *before* the invite
    // server — a plain `dead_peer` can't distinguish "tried first" from
    // "silently dropped".
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let decoy = {
        let hits = hits.clone();
        axum::Router::new().route(
            "/_matrix/federation/v1/make_join/{room_id}/{user_id}",
            axum::routing::get(move || {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }),
        )
    };
    let decoy_server = crate::federation::test_support::spawn_stub(decoy).await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

    // Pending invite whose sender lives on the live resident B.
    let inviter: OwnedUserId = format!("@alice:{b_server}").parse().unwrap();
    let throwaway = EventBuilder::new(inviter.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build throwaway create");
    let invite = member_pdu(
        &inviter,
        "@bob:a.example",
        &room_id,
        "invite",
        std::slice::from_ref(&throwaway.event_id),
    );
    let bob: OwnedUserId = "@bob:a.example".parse().unwrap();
    a_store.put_invite(&room_id, &bob, &invite).await.unwrap();

    // `via` lists a dead hint (transport failure, skipped) then the live decoy
    // (contacted, refuses) ahead of the invite server — so the join can only
    // succeed by exhausting both hints in order and falling back to the invite.
    let dead = crate::federation::test_support::dead_peer().await;
    let path = format!("/_matrix/client/v3/join/{room_id}?via={dead}&via={decoy_server}");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    assert!(
        hits.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the explicit hints must be attempted before falling back to the invite server"
    );
    let member = a_store
        .current_state_event(&room_id, "m.room.member", "@bob:a.example")
        .await
        .unwrap()
        .expect("bob joined via the invite-sourced fallback");
    assert_eq!(membership_str(&member).as_deref(), Some("join"));
}

#[tokio::test]
async fn hosted_room_with_live_local_member_and_pending_invite_does_not_federate() {
    // A hosted room with a *live local* member (@carol:a.example joined) is a
    // current copy, so a local user's join must take the local path — never
    // federate. (A hosted room with *no* joined local member is the stale-copy
    // case that re-syncs; see `rejoining_hosted_room_with_no_local_members…`.)
    let (_seed_router, store, room_id, _head, _temp) = seed_room(&[
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
        (
            "@carol:a.example",
            "m.room.member",
            "@carol:a.example",
            json!({ "membership": "join" }),
        ),
    ])
    .await;
    let router = router_with_store(config_for("a.example", "bob"), store.clone());

    // Plant a stale OOB invite for the local user whose inviter lives on a dead
    // server. If the gate ever regressed and this federated, the join would
    // contact that dead server and 502; because the copy is live it must take
    // the local path and join (200) without any outbound request.
    let dead = crate::federation::test_support::dead_peer().await;
    let inviter: OwnedUserId = format!("@alice:{dead}").parse().unwrap();
    let throwaway = EventBuilder::new(inviter.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build throwaway create");
    let invite = member_pdu(
        &inviter,
        "@bob:a.example",
        &room_id,
        "invite",
        std::slice::from_ref(&throwaway.event_id),
    );
    let bob: OwnedUserId = "@bob:a.example".parse().unwrap();
    store.put_invite(&room_id, &bob, &invite).await.unwrap();

    let path = format!("/_matrix/client/v3/rooms/{room_id}/join");
    let (status, body) = post_json(&router, &path, &json!({})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a hosted room must join locally, never federate via the invite: {body:?}"
    );
    let member = store
        .current_state_event(&room_id, "m.room.member", "@bob:a.example")
        .await
        .unwrap()
        .expect("bob joined the resident room locally");
    assert_eq!(membership_str(&member).as_deref(), Some("join"));
}

#[tokio::test]
async fn rejoining_hosted_room_with_no_local_members_resyncs_from_resident() {
    // A previously hosted this room but has no joined local member left, so its
    // copy is stale (federation stopped delivering once its last member left). A
    // local user re-joining must take the REMOTE path and pull the resident's
    // current state — not build on the stale local heads. The resident's state
    // carries an m.room.name A never saw; after the re-join it must appear.
    let alice = alice(); // @alice:example.org — a *remote* member from A's view
    let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .unwrap();
    let room_id = create.room_id.clone();
    let alice_join = EventBuilder::new(alice.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(alice.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create.event_id.clone()])
        .prev_state_events(vec![create.event_id.clone()])
        .build()
        .unwrap();
    let rules = EventBuilder::new(alice.clone(), "m.room.join_rules".to_owned())
        .room_id(room_id.clone())
        .state_key(String::new())
        .content(json!({ "join_rule": "public" }))
        .prev_events(vec![alice_join.event_id.clone()])
        .prev_state_events(vec![alice_join.event_id.clone()])
        .build()
        .unwrap();
    // The state event A is missing while away.
    let name = EventBuilder::new(alice.clone(), "m.room.name".to_owned())
        .room_id(room_id.clone())
        .state_key(String::new())
        .content(json!({ "name": "synced-from-resident" }))
        .prev_events(vec![rules.event_id.clone()])
        .prev_state_events(vec![rules.event_id.clone()])
        .build()
        .unwrap();
    let bob: OwnedUserId = "@bob:a.example".parse().unwrap();
    let bob_join = EventBuilder::new(bob.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(bob.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![name.event_id.clone()])
        .prev_state_events(vec![name.event_id.clone()])
        .build()
        .unwrap();

    // Resident hands back current state including the name A never saw.
    let mj = json!({ "event": raw_to_value(&bob_join), "room_version": ROOM_VERSION_ID });
    let sj = json!({
        "state_dag": [
            raw_to_value(&create),
            raw_to_value(&alice_join),
            raw_to_value(&rules),
            raw_to_value(&name),
        ],
        "timeline": [],
        "event": raw_to_value(&bob_join),
    });
    let resident = crate::federation::test_support::spawn_stub(stub_resident(mj, sj)).await;

    // A's STALE copy: hosts the room only up to join_rules (no name), and has no
    // joined local member (alice is remote).
    let (a_store, _a_temp) = fresh_store().await;
    a_store
        .create_room(&create, &[alice_join.clone(), rules.clone()])
        .await
        .unwrap();
    assert!(a_store.room_exists(&room_id).await.unwrap());
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

    // Re-join, hinting the live resident.
    let path = format!("/_matrix/client/v3/join/{room_id}?server_name={resident}");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // bob re-joined...
    let member = a_store
        .current_state_event(&room_id, "m.room.member", bob.as_str())
        .await
        .unwrap()
        .expect("bob re-joined");
    assert_eq!(membership_str(&member).as_deref(), Some("join"));
    // ...and A pulled in the resident state it had missed (the proof of re-sync;
    // the old local-join path would leave A on its stale heads with no name).
    let name_ev = a_store
        .current_state_event(&room_id, "m.room.name", "")
        .await
        .unwrap()
        .expect("the missed m.room.name must be pulled in by the re-join");
    let got: Value = serde_json::from_str(name_ev.content.get()).unwrap();
    assert_eq!(got["name"], "synced-from-resident");
}

#[tokio::test]
async fn federated_join_remote_403_surfaces_as_forbidden() {
    // A resident that refuses make_join with 403 (invite-only / banned).
    let forbidding = axum::Router::new().route(
        "/_matrix/federation/v1/make_join/{room_id}/{user_id}",
        axum::routing::get(|| async { StatusCode::FORBIDDEN }),
    );
    let server = crate::federation::test_support::spawn_stub(forbidding).await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config_for("a.example", "bob"), a_store.clone());

    // Non-hosted room, explicit `via` at the forbidding resident. The remote
    // 403 must surface to the client as 403 M_FORBIDDEN, not a 502 M_UNKNOWN.
    let room = "!forbidden:b.example";
    let path = format!("/_matrix/client/v3/join/{room}?via={server}");
    let (status, body) = post_json(&a_router, &path, &json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

#[test]
fn parse_server_names_accepts_via_and_server_name() {
    use crate::federation::join::parse_server_names;
    // `via` is the v1.12+ name; a modern client sends only `via` to our v1.16.
    let via = parse_server_names(Some("via=127.0.0.1%3A8008&via=other.example"));
    let via: Vec<String> = via.iter().map(ToString::to_string).collect();
    assert_eq!(
        via,
        vec!["127.0.0.1:8008".to_string(), "other.example".to_string()]
    );
    // The pre-1.12 `server_name` alias is still accepted for older clients.
    let legacy = parse_server_names(Some("server_name=legacy.example"));
    assert_eq!(
        legacy.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["legacy.example".to_string()]
    );
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

// --- additional make_join / send_join / ingest coverage (review fixups) ---

/// Parse an event's canonical wire bytes into a JSON `Value` (for embedding in
/// a stub resident's canned responses).
fn raw_to_value(ev: &neutrino_common::Event) -> Value {
    serde_json::from_str(ev.raw.get()).expect("event raw is valid JSON")
}

#[tokio::test]
async fn make_join_invited_user_is_allowed() {
    // Invite-only room (default rule), zara HAS a pending invite → make_join 200.
    let (router, _store, room_id, _head, _tempfile) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (
            ALICE,
            "m.room.member",
            ZARA,
            json!({ "membership": "invite" }),
        ),
    ])
    .await;
    let (status, body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["event"]["content"]["membership"], "join");
}

#[tokio::test]
async fn make_join_is_read_only() {
    // make_join must not persist anything: heads unchanged, no member row created.
    let (router, store, room_id, head, _tempfile) = seed_public_room().await;
    let before = store.forward_extremities(&room_id).await.unwrap();

    let (status, _body) = get(&router, &make_join_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::OK);

    let after = store.forward_extremities(&room_id).await.unwrap();
    assert_eq!(
        before, after,
        "make_join must not advance forward extremities"
    );
    assert_eq!(
        after.unwrap().1.into_iter().next(),
        Some(head),
        "state head unchanged"
    );
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", ZARA)
            .await
            .unwrap()
            .is_none(),
        "make_join must not create a member row"
    );
}

#[tokio::test]
async fn make_join_with_our_version_among_several_succeeds() {
    let (router, _store, room_id, _head, _tempfile) = seed_public_room().await;
    let path = format!(
        "/_matrix/federation/v1/make_join/{room_id}/{ZARA}?ver=1&ver={ROOM_VERSION_ID}&ver=11"
    );
    let (status, body) = get(&router, &path).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
}

#[tokio::test]
async fn send_join_non_join_membership_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_public_room().await;
    let zara: OwnedUserId = ZARA.parse().unwrap();
    let leave = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(zara.to_string())
        .content(json!({ "membership": "leave" }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head])
        .build()
        .unwrap();
    let (status, body) = put_event(
        &router,
        &send_join_path(&room_id, &leave.event_id),
        leave.raw.get(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_join_state_key_not_sender_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_public_room().await;
    let zara: OwnedUserId = ZARA.parse().unwrap();
    // sender = zara, but state_key = a different user.
    let bad = EventBuilder::new(zara, "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key("@someone:other.example".to_owned())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head])
        .build()
        .unwrap();
    let (status, body) = put_event(
        &router,
        &send_join_path(&room_id, &bad.event_id),
        bad.raw.get(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_join_room_id_path_mismatch_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_public_room().await;
    let join = remote_join(&room_id, &head, ZARA);
    // Correct event_id in the path, but a different room id.
    let other_room = ruma::RoomId::parse("!other:example.org").unwrap();
    let path = send_join_path(&other_room, &join.event_id);
    let (status, body) = put_event(&router, &path, join.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[test]
fn parse_server_names_lowercase_colon_and_drops_garbage() {
    use crate::federation::join::parse_server_names;
    let got = parse_server_names(Some(
        "server_name=127.0.0.1%3a8008&server_name=!!!bad&server_name=ok.example",
    ));
    let got: Vec<String> = got.iter().map(|s| s.to_string()).collect();
    // lowercase %3a decoded; the garbage entry dropped; the good ones kept.
    assert_eq!(
        got,
        vec!["127.0.0.1:8008".to_string(), "ok.example".to_string()]
    );
}

/// A canned-response resident server: make_join returns `make_join_body`,
/// send_join returns `send_join_body`, get_missing_events returns no events.
/// Lets a test drive the outbound ingest path against deliberately broken state.
fn stub_resident(make_join_body: Value, send_join_body: Value) -> axum::Router {
    use axum::routing::{get as rget, post as rpost, put as rput};
    let mj = Arc::new(make_join_body);
    let sj = Arc::new(send_join_body);
    axum::Router::new()
        .route(
            "/_matrix/federation/v1/make_join/{room}/{user}",
            rget(move || {
                let mj = mj.clone();
                async move { axum::Json((*mj).clone()) }
            }),
        )
        .route(
            "/_matrix/federation/v2/send_join/{room}/{event}",
            rput(move || {
                let sj = sj.clone();
                async move { axum::Json((*sj).clone()) }
            }),
        )
        .route(
            "/_matrix/federation/v1/get_missing_events/{room}",
            rpost(|| async { axum::Json(json!({ "events": [] })) }),
        )
}

#[tokio::test]
async fn federated_join_times_out_when_state_never_grounds() {
    // The resident hands back a join whose prev_state references a "ghost" event
    // nobody has (and get_missing_events returns nothing), so the worker can
    // never ground it → the CSAPI join times out with 504, but the room shell
    // is registered and our membership never appears.
    let alice = alice();
    let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .unwrap();
    let room_id = create.room_id.clone();
    let ghost = EventBuilder::new(alice, "m.room.message".to_owned())
        .room_id(room_id.clone())
        .content(json!({ "body": "ghost" }))
        .prev_events(vec![create.event_id.clone()])
        .build()
        .unwrap();
    let zara: OwnedUserId = ZARA.parse().unwrap();
    let template = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(zara.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create.event_id.clone()])
        .prev_state_events(vec![ghost.event_id.clone()])
        .build()
        .unwrap();
    let mj = json!({ "event": raw_to_value(&template), "room_version": ROOM_VERSION_ID });
    let sj = json!({
        "state_dag": [raw_to_value(&create), raw_to_value(&template)],
        "timeline": [],
        "event": raw_to_value(&template),
    });
    let b = crate::federation::test_support::spawn_stub(stub_resident(mj, sj)).await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_state = crate::AppState::from_store(config_for("a.example", "bob"), a_store.clone());
    let resp = crate::federation::join::federated_join_with(
        &a_state,
        zara.clone(),
        &room_id,
        &[b],
        std::time::Duration::from_millis(800),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    // Room shell registered (create grounded), but the join never landed.
    assert!(a_store.room_exists(&room_id).await.unwrap());
    assert!(
        a_store
            .current_state_event(&room_id, "m.room.member", zara.as_str())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn federated_join_missing_create_in_response_fails_without_registering() {
    // send_join response omits the create event → ingest can't register the room
    // → the handshake fails (no candidate succeeds → 502) and nothing is created.
    let alice = alice();
    let create = EventBuilder::new(alice, "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .unwrap();
    let room_id = create.room_id.clone();
    let zara: OwnedUserId = ZARA.parse().unwrap();
    let template = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(zara.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create.event_id.clone()])
        .prev_state_events(vec![create.event_id.clone()])
        .build()
        .unwrap();
    let mj = json!({ "event": raw_to_value(&template), "room_version": ROOM_VERSION_ID });
    // No create anywhere in the response.
    let sj = json!({ "state_dag": [], "timeline": [], "event": raw_to_value(&template) });
    let b = crate::federation::test_support::spawn_stub(stub_resident(mj, sj)).await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_state = crate::AppState::from_store(config_for("a.example", "bob"), a_store.clone());
    let resp = crate::federation::join::federated_join_with(
        &a_state,
        zara,
        &room_id,
        &[b],
        std::time::Duration::from_millis(500),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(
        !a_store.room_exists(&room_id).await.unwrap(),
        "a create-less response must not register the room"
    );
}

// --- /invite/v2 (inbound federated invite) ------------------------------------

fn invite_path(room_id: &str, event_id: &str) -> String {
    format!("/_matrix/federation/v2/invite/{room_id}/{event_id}")
}

/// The remote inviting server.
fn inviter() -> OwnedUserId {
    "@bob:remote.example.org".parse().unwrap()
}

/// The v2 `/invite` request envelope `{ event, room_version, invite_room_state }`
/// (the v2 endpoint wraps the PDU). `invite_room_state` is the optional stripped
/// state the inviting server includes; the handler merges it into the stored
/// event's `unsigned`.
fn invite_body(event: &neutrino_common::Event, invite_room_state: Option<Value>) -> Value {
    let mut body = json!({
        "event": serde_json::from_str::<Value>(event.raw.get()).unwrap(),
        "room_version": ROOM_VERSION_ID,
    });
    if let Some(irs) = invite_room_state {
        body["invite_room_state"] = irs;
    }
    body
}

/// Build an `m.room.member` event off `prevs` (used for both DAGs) with the
/// given membership, sender and target — the shape an inviting resident emits.
fn member_pdu(
    sender: &OwnedUserId,
    target: &str,
    room_id: &OwnedRoomId,
    membership: &str,
    prevs: &[OwnedEventId],
) -> neutrino_common::Event {
    EventBuilder::new(sender.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(target.to_owned())
        .content(json!({ "membership": membership }))
        .prev_events(prevs.to_vec())
        .prev_state_events(prevs.to_vec())
        .build()
        .expect("build member event")
}

/// An out-of-band invite for a room we don't host is stored as a stub
/// (bypassing `apply_pdu`, since there's no room state to auth against) and the
/// handler echoes the event back. Its `unsigned.invite_room_state` survives.
#[tokio::test]
async fn invite_oob_stores_stub_and_returns_event() {
    let (store, _tempfile) = fresh_store().await;
    let router = router_with_store(config(), store.clone());
    let bob = inviter();
    let invited = alice(); // local to example.org

    // A throwaway create only to source a valid room id + a syntactically-valid
    // prev event id; the room is NOT created in our store (out-of-band).
    let create = EventBuilder::new(bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let invite = member_pdu(
        &bob,
        invited.as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&create.event_id),
    );
    let invite_id = invite.event_id.clone();
    let irs = json!([
        {"type": "m.room.name", "state_key": "", "sender": bob.as_str(),
         "content": {"name": "Remote Room"}}
    ]);

    let (status, body) = put_json(
        &router,
        &invite_path(room_id.as_str(), invite_id.as_str()),
        &invite_body(&invite, Some(irs)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body = {body}");
    // Response echoes the event with its computed id.
    assert_eq!(
        body.pointer("/event/state_key").and_then(|v| v.as_str()),
        Some(invited.as_str()),
        "body = {body}"
    );

    // Stub stored, retrievable, and `unsigned.invite_room_state` preserved.
    let stored = store
        .get_invite(&room_id, &invited)
        .await
        .unwrap()
        .expect("OOB invite stub stored");
    assert_eq!(stored.event_id, invite_id);
    let v: Value = serde_json::from_str(stored.raw.get()).unwrap();
    assert_eq!(
        v.pointer("/unsigned/invite_room_state/0/content/name")
            .and_then(|n| n.as_str()),
        Some("Remote Room"),
        "invite_room_state preserved in the stored stub"
    );
    // The room itself was never registered (no apply_pdu, no create).
    assert!(!store.room_exists(&room_id).await.unwrap());
}

/// An invite for a room we DO host is not an error (the inviting server may not
/// know we're resident): it is staged and integrated through the worker via
/// `apply_pdu` (auth + state-res + persist) — landing in `current_state` — NOT
/// stored as an out-of-band stub. (2026-06-05 decision.)
#[tokio::test]
async fn invite_for_hosted_room_applies_via_worker_not_oob_stub() {
    // We host the room; alice is the joined creator (power to invite).
    let (app, store, room_id, alice, join_id, _tempfile) = seed_joined_room().await;
    let carol: OwnedUserId = "@carol:example.org".parse().unwrap();
    // The invite arrives over federation but is authored by our own joined
    // creator (so apply_pdu accepts it against our state) and sits on the head.
    let invite = member_pdu(
        &alice,
        carol.as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&join_id),
    );
    let invite_id = invite.event_id.clone();

    let (status, body) = put_json(
        &app,
        &invite_path(room_id.as_str(), invite_id.as_str()),
        &invite_body(&invite, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    // Decision 3: hosted-room invite is applied (committed + current_state),
    // not parked as an OOB stub.
    let committed = wait_committed(&store, invite_id.as_ref()).await;
    assert!(
        !committed.rejected,
        "creator's invite of a local user is authorised"
    );
    let member = store
        .current_state_event(&room_id, "m.room.member", carol.as_str())
        .await
        .unwrap()
        .expect("carol now has a member event in current state");
    assert_eq!(member.content_str("membership").as_deref(), Some("invite"));
    assert!(
        store.get_invite(&room_id, &carol).await.unwrap().is_none(),
        "a hosted-room invite must NOT be stored as an out-of-band stub"
    );
}

/// `state_key` must be one of our local users — an invite addressed to another
/// server's user is rejected (we have no business storing it).
#[tokio::test]
async fn invite_rejects_non_local_invitee() {
    let (store, _tempfile) = fresh_store().await;
    let router = router_with_store(config(), store.clone());
    let bob = inviter();
    let create = EventBuilder::new(bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    // Invitee on a different server.
    let invite = member_pdu(
        &bob,
        "@dave:other.example.org",
        &room_id,
        "invite",
        std::slice::from_ref(&create.event_id),
    );
    let invite_id = invite.event_id.clone();

    let (status, body) = put_json(
        &router,
        &invite_path(room_id.as_str(), invite_id.as_str()),
        &invite_body(&invite, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(|c| c.as_str()),
        Some("M_INVALID_PARAM")
    );
    // Nothing stored.
    assert!(
        store
            .get_invite(
                &room_id,
                &"@dave:other.example.org".parse::<OwnedUserId>().unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
}

/// A non-invite membership on the invite endpoint is a 400.
#[tokio::test]
async fn invite_rejects_non_invite_membership() {
    let (store, _tempfile) = fresh_store().await;
    let router = router_with_store(config(), store);
    let bob = inviter();
    let create = EventBuilder::new(bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    // A leave (kick-shaped: sender != target) is structurally valid but not an
    // invite.
    let leave = member_pdu(
        &bob,
        alice().as_str(),
        &room_id,
        "leave",
        std::slice::from_ref(&create.event_id),
    );
    let (status, body) = put_json(
        &router,
        &invite_path(room_id.as_str(), leave.event_id.as_str()),
        &invite_body(&leave, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(|c| c.as_str()),
        Some("M_INVALID_PARAM")
    );
}

/// The `eventId` path segment must match the event's computed reference hash.
#[tokio::test]
async fn invite_rejects_event_id_path_mismatch() {
    let (store, _tempfile) = fresh_store().await;
    let router = router_with_store(config(), store);
    let bob = inviter();
    let create = EventBuilder::new(bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let invite = member_pdu(
        &bob,
        alice().as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&create.event_id),
    );
    // Path carries a different (but syntactically valid) event id.
    let (status, body) = put_json(
        &router,
        &invite_path(room_id.as_str(), create.event_id.as_str()),
        &invite_body(&invite, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body = {body}");
    assert_eq!(
        body.get("errcode").and_then(|c| c.as_str()),
        Some("M_INVALID_PARAM")
    );
}

// --- outbound CSAPI /invite of a remote user ----------------------------------

/// Bind a real neutrino router on an ephemeral port whose `server_name` IS that
/// address, so a user `@local:{addr}` is local to it (its inbound `/invite/v2`
/// accepts the invitee). Returns the server name, the served store, and the
/// tempfile guard (held by the caller so the backing DB outlives the server).
async fn serve_invitee_server(localpart: &str) -> (String, Arc<SqliteStore>, TempDir) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let name = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let (store, tempfile) = fresh_store().await;
    let router = router_with_store(config_for(&name, localpart), store.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (name, store, tempfile)
}

/// CSAPI `/invite` of a remote user federates the invite to the invitee's
/// server (storing an OOB stub there with our `invite_room_state`), then commits
/// locally (current_state membership=invite). This is also the round-trip guard
/// — our outbound body is parsed by the real inbound `/invite/v2` on the peer.
#[tokio::test]
async fn outbound_invite_federates_then_persists() {
    // A (example.org) hosts the room; alice is the joined creator.
    let (a_app, a_store, room_id, _alice, _join, _tempfile) = seed_joined_room().await;
    // B serves the invitee `@dave:{B}`.
    let (b_name, b_store, _b_tempfile) = serve_invitee_server("dave").await;
    let dave = format!("@dave:{b_name}");
    let dave_uid: OwnedUserId = dave.parse().unwrap();

    let (status, body) = post_json(
        &a_app,
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        &json!({ "user_id": dave }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body = {body}");

    // A persisted the invite into current state (committed via apply_resident).
    let member = a_store
        .current_state_event(&room_id, "m.room.member", dave_uid.as_str())
        .await
        .unwrap()
        .expect("dave is invited in A's current state");
    assert_eq!(membership_str(&member).as_deref(), Some("invite"));

    // B stored the OOB stub (it doesn't host the room) with our stripped
    // invite_room_state (the round-trip through B's real /invite/v2).
    let stub = b_store
        .get_invite(&room_id, &dave_uid)
        .await
        .unwrap()
        .expect("B stored the OOB invite stub");
    let v: Value = serde_json::from_str(stub.raw.get()).unwrap();
    let types: Vec<&str> = v
        .pointer("/unsigned/invite_room_state")
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|e| e.pointer("/type").and_then(|t| t.as_str()))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        types.contains(&"m.room.create"),
        "invite_room_state carries the create event; got {types:?}"
    );
}

/// Invitee server unreachable ⇒ CSAPI errors (502) and **nothing** is persisted
/// locally (the atomicity property — federate-then-persist).
#[tokio::test]
async fn outbound_invite_peer_unreachable_persists_nothing() {
    let (a_app, a_store, room_id, _alice, _join, _tempfile) = seed_joined_room().await;
    let dead = crate::federation::test_support::dead_peer().await;
    let dave = format!("@dave:{dead}");
    let dave_uid: OwnedUserId = dave.parse().unwrap();

    let (status, _body) = post_json(
        &a_app,
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        &json!({ "user_id": dave }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(
        a_store
            .current_state_event(&room_id, "m.room.member", dave_uid.as_str())
            .await
            .unwrap()
            .is_none(),
        "an unreachable invitee server must leave nothing persisted"
    );
}

/// Invitee server returns 403 ⇒ CSAPI 403 and nothing persisted.
#[tokio::test]
async fn outbound_invite_peer_403_persists_nothing() {
    let (a_app, a_store, room_id, _alice, _join, _tempfile) = seed_joined_room().await;

    // A stub invitee server that 403s every /invite/v2.
    let stub = axum::Router::new().route(
        "/_matrix/federation/v2/invite/{room_id}/{event_id}",
        axum::routing::put(|| async {
            (
                StatusCode::FORBIDDEN,
                axum::Json(json!({"errcode": "M_FORBIDDEN", "error": "no"})),
            )
        }),
    );
    let stub_name = crate::federation::test_support::spawn_stub(stub).await;
    let dave = format!("@dave:{stub_name}");
    let dave_uid: OwnedUserId = dave.parse().unwrap();

    let (status, _body) = post_json(
        &a_app,
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        &json!({ "user_id": dave }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        a_store
            .current_state_event(&room_id, "m.room.member", dave_uid.as_str())
            .await
            .unwrap()
            .is_none(),
        "a peer 403 must leave nothing persisted"
    );
}

/// Inbound `/invite/v2` for a room we DO host, authored by an **unauthorised**
/// remote inviter (not in the room, no power): the decision-3 path stages it and
/// the worker integrates it through `apply_pdu`, which auth-REJECTS it. The
/// invitee must NOT end up in current_state, and must NOT fall back to an
/// out-of-band stub. (Exercises the hosted-room reject branch — the one most
/// likely to hide a bug.)
#[tokio::test]
async fn invite_for_hosted_room_unauthorised_inviter_is_rejected() {
    let (app, store, room_id, _alice, join_id, _tempfile) = seed_joined_room().await;
    // A remote user with no membership/power in our room "invites" our local
    // carol. (The invitee is local — that gate passes; auth is what refuses it.)
    let bob: OwnedUserId = "@bob:remote.example.org".parse().unwrap();
    let carol: OwnedUserId = "@carol:example.org".parse().unwrap();
    let invite = member_pdu(
        &bob,
        carol.as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&join_id),
    );
    let invite_id = invite.event_id.clone();

    let (status, body) = put_json(
        &app,
        &invite_path(room_id.as_str(), invite_id.as_str()),
        &invite_body(&invite, None),
    )
    .await;
    // The handler stages optimistically and 200s; the worker does the auth.
    assert_eq!(status, StatusCode::OK, "body = {body}");

    // Let the worker drain (it applies → auth-rejects → unstages).
    wait_staging_empty(&store, &room_id).await;

    // Rejected ⇒ carol is NOT in current state…
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", carol.as_str())
            .await
            .unwrap()
            .is_none(),
        "an unauthorised invite must not land in current state"
    );
    // …and a hosted-room invite is NEVER parked as an out-of-band stub.
    assert!(
        store.get_invite(&room_id, &carol).await.unwrap().is_none(),
        "hosted-room invite must not fall back to an OOB stub"
    );
}

/// Outbound CSAPI `/invite`: if the invitee server 200s but returns a
/// **different** event in `{ event }`, the `event_id` round-trip guard refuses
/// it (502) and nothing is persisted — a peer cannot substitute a different
/// event for `apply_resident` to commit.
#[tokio::test]
async fn outbound_invite_peer_returns_wrong_event_persists_nothing() {
    let (a_app, a_store, room_id, _alice, _join, _tempfile) = seed_joined_room().await;

    // A valid but unrelated event the stub will echo instead of our candidate.
    let other_create = EventBuilder::new(inviter(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build a different event");
    let bogus = json!({ "event": raw_to_value(&other_create) });

    let stub = axum::Router::new().route(
        "/_matrix/federation/v2/invite/{room_id}/{event_id}",
        axum::routing::put(move || {
            let body = bogus.clone();
            async move { (StatusCode::OK, axum::Json(body)) }
        }),
    );
    let stub_name = crate::federation::test_support::spawn_stub(stub).await;
    let dave = format!("@dave:{stub_name}");
    let dave_uid: OwnedUserId = dave.parse().unwrap();

    let (status, _body) = post_json(
        &a_app,
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        &json!({ "user_id": dave }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "a substituted event must be refused"
    );
    assert!(
        a_store
            .current_state_event(&room_id, "m.room.member", dave_uid.as_str())
            .await
            .unwrap()
            .is_none(),
        "nothing persisted when the peer returns the wrong event"
    );
}

// ===================================================================
// Server-Server leave / invite-rejection.
// INBOUND (we are the resident): make_leave / send_leave for a remote
// user departing a room we host. OUTBOUND (we are the joining server):
// CSAPI /leave of an out-of-band invite → federated reject + local removal.
// ===================================================================

/// A room created by alice with zara invited (the state a rejection acts on).
async fn seed_room_with_invited_zara() -> (
    axum::Router,
    Arc<SqliteStore>,
    OwnedRoomId,
    OwnedEventId,
    TempDir,
) {
    seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (
            ALICE,
            "m.room.member",
            ZARA,
            json!({ "membership": "invite" }),
        ),
    ])
    .await
}

fn make_leave_path(room_id: &RoomId, user: &str) -> String {
    format!("/_matrix/federation/v1/make_leave/{room_id}/{user}?ver={ROOM_VERSION_ID}")
}

fn send_leave_path(room_id: &RoomId, event_id: &OwnedEventId) -> String {
    format!("/_matrix/federation/v2/send_leave/{room_id}/{event_id}")
}

/// A completed remote `m.room.member`/`leave` (self-leave) referencing `head`.
fn remote_leave(room_id: &RoomId, head: &OwnedEventId, user: &str) -> neutrino_common::Event {
    let user: OwnedUserId = user.parse().expect("user");
    EventBuilder::new(user.clone(), "m.room.member".to_owned())
        .room_id(room_id.to_owned())
        .state_key(user.to_string())
        .content(json!({ "membership": "leave" }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head.clone()])
        .build()
        .expect("build remote leave")
}

// --- make_leave (inbound) ----------------------------------------------

#[tokio::test]
async fn make_leave_returns_leave_template() {
    let (router, _store, room_id, head, _tempfile) = seed_room_with_invited_zara().await;
    let (status, body) = get(&router, &make_leave_path(&room_id, ZARA)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["room_version"], ROOM_VERSION_ID);

    let event = &body["event"];
    assert_eq!(event["type"], "m.room.member");
    assert_eq!(event["content"]["membership"], "leave");
    assert_eq!(event["sender"], ZARA);
    assert_eq!(event["state_key"], ZARA);

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
async fn make_leave_unknown_room_returns_404() {
    let (router, _store, _room_id, _head, _tempfile) = seed_room_with_invited_zara().await;
    let unknown = ruma::RoomId::parse("!nope:example.org").unwrap();
    let (status, body) = get(&router, &make_leave_path(&unknown, ZARA)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}

#[tokio::test]
async fn make_leave_incompatible_version_returns_400() {
    // make_leave negotiates the room version like make_join: a `ver` that does
    // not include ours — or an absent `ver` (which defaults to `[1]`) — yields
    // 400 M_INCOMPATIBLE_ROOM_VERSION with our `room_version` in the body.
    let (router, _store, room_id, _head, _tempfile) = seed_room_with_invited_zara().await;

    let path = format!("/_matrix/federation/v1/make_leave/{room_id}/{ZARA}?ver=1");
    let (status, body) = get(&router, &path).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INCOMPATIBLE_ROOM_VERSION");
    assert_eq!(body["room_version"], ROOM_VERSION_ID);

    let path = format!("/_matrix/federation/v1/make_leave/{room_id}/{ZARA}");
    let (status, body) = get(&router, &path).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INCOMPATIBLE_ROOM_VERSION");
}

// --- send_leave (inbound) ----------------------------------------------

#[tokio::test]
async fn send_leave_admits_leave_and_returns_empty() {
    // zara is invited; she rejects → leaves. yan (other.example) is joined so
    // the leave genuinely fans out (distribution duty).
    let (router, store, room_id, head, _tempfile) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (YAN, "m.room.member", YAN, json!({ "membership": "join" })),
        (
            ALICE,
            "m.room.member",
            ZARA,
            json!({ "membership": "invite" }),
        ),
    ])
    .await;
    let leave = remote_leave(&room_id, &head, ZARA);
    let leave_id = leave.event_id.clone();

    let (status, body) = put_event(
        &router,
        &send_leave_path(&room_id, &leave_id),
        leave.raw.get(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    // The v2 response is an empty object — no state_dag / auth_chain / state.
    assert_eq!(body, json!({}), "send_leave must return an empty object");

    // zara is now `leave` in our current state.
    let member = store
        .current_state_event(&room_id, "m.room.member", ZARA)
        .await
        .unwrap()
        .expect("zara member row");
    assert_eq!(member.event_id, leave_id);
    assert_eq!(membership_str(&member).as_deref(), Some("leave"));

    // Distribution duty matches send_join's: fan out to the *other* room servers,
    // but NOT back to the server that delivered the leave (zara's own
    // remote.example — `apply_resident` excludes the sender), nor to ourselves.
    let other = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert!(
        other.iter().any(|e| e.event_id == leave_id),
        "other.example must receive zara's leave"
    );
    assert!(
        store
            .pending_pdus(ruma::server_name!("remote.example"), usize::MAX)
            .await
            .unwrap()
            .is_empty(),
        "must not echo the leave back to the departing server"
    );
    assert!(
        store
            .pending_pdus(ruma::server_name!("example.org"), usize::MAX)
            .await
            .unwrap()
            .is_empty(),
        "must never federate to ourselves"
    );
}

#[tokio::test]
async fn send_leave_non_leave_membership_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_room_with_invited_zara().await;
    // A join event sent to send_leave must be refused on the membership check.
    let join = remote_join(&room_id, &head, ZARA);
    let id = join.event_id.clone();
    let (status, body) = put_event(&router, &send_leave_path(&room_id, &id), join.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_leave_event_id_path_mismatch_returns_400() {
    let (router, _store, room_id, head, _tempfile) = seed_room_with_invited_zara().await;
    let leave = remote_leave(&room_id, &head, ZARA);
    // Path id is a real, valid, different id (the state head), so the 400 can
    // only come from the body-vs-path comparison.
    let (status, body) =
        put_event(&router, &send_leave_path(&room_id, &head), leave.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_leave_state_key_not_sender_returns_400() {
    // A leave whose state_key != sender (a kick shape). send_leave only expresses
    // a self-leave, so it must refuse this; a kick rides /send instead.
    let (router, _store, room_id, head, _tempfile) = seed_room_with_invited_zara().await;
    let zara: OwnedUserId = ZARA.parse().unwrap();
    let leave = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(YAN.to_owned())
        .content(json!({ "membership": "leave" }))
        .prev_events(vec![head.clone()])
        .prev_state_events(vec![head.clone()])
        .build()
        .expect("build mismatched leave");
    let id = leave.event_id.clone();
    let (status, body) = put_event(&router, &send_leave_path(&room_id, &id), leave.raw.get()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn send_leave_is_idempotent_on_resend() {
    let (router, store, room_id, head, _tempfile) = seed_room(&[
        (
            ALICE,
            "m.room.member",
            ALICE,
            json!({ "membership": "join" }),
        ),
        (YAN, "m.room.member", YAN, json!({ "membership": "join" })),
        (
            ALICE,
            "m.room.member",
            ZARA,
            json!({ "membership": "invite" }),
        ),
    ])
    .await;
    let leave = remote_leave(&room_id, &head, ZARA);
    let id = leave.event_id.clone();
    let path = send_leave_path(&room_id, &id);

    let (s1, _b1) = put_event(&router, &path, leave.raw.get()).await;
    assert_eq!(s1, StatusCode::OK);
    let after_first = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert_eq!(after_first.len(), 1, "one fan-out on first apply");

    // A re-sent send_leave re-applies as a no-op.
    let (s2, b2) = put_event(&router, &path, leave.raw.get()).await;
    assert_eq!(s2, StatusCode::OK, "{b2:?}");
    let after_second = store
        .pending_pdus(ruma::server_name!("other.example"), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        after_second.len(),
        1,
        "re-send must not enqueue the leave a second time"
    );
    let member = store
        .current_state_event(&room_id, "m.room.member", ZARA)
        .await
        .unwrap()
        .expect("zara member row");
    assert_eq!(member.event_id, id);
}

#[tokio::test]
async fn send_leave_unknown_room_returns_404() {
    // A leave for a room we don't host: `apply_resident` can't bootstrap an actor
    // (no forward extremities) → UnknownRoom → 404. Exercises the apply-error
    // mapping past the structural validators.
    let (router, _store, _room, head, _tempfile) = seed_room_with_invited_zara().await;
    let unknown = ruma::RoomId::parse("!nope:example.org").unwrap();
    let leave = remote_leave(&unknown, &head, ZARA);
    let id = leave.event_id.clone();
    let (status, body) = put_event(&router, &send_leave_path(&unknown, &id), leave.raw.get()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}

#[tokio::test]
async fn send_leave_unauthorised_leave_returns_403() {
    // zara was never in this public room, so a self-leave has no valid prior
    // membership: auth rejects it → 403, nothing persisted. Exercises the
    // `apply_resident` Rejected → 403 arm (the structural 400 tests all trip a
    // guard *before* apply).
    let (router, store, room_id, head, _tempfile) = seed_public_room().await;
    let leave = remote_leave(&room_id, &head, ZARA);
    let id = leave.event_id.clone();
    let (status, body) = put_event(&router, &send_leave_path(&room_id, &id), leave.raw.get()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
    assert!(
        store
            .current_state_event(&room_id, "m.room.member", ZARA)
            .await
            .unwrap()
            .is_none(),
        "a refused leave must not enter current state"
    );
}

// --- outbound CSAPI /leave (invite rejection) --------------------------

/// A resident server B (served on an ephemeral port) hosting a room created by
/// `@bob:{B}` with `invitee` invited. Returns its name, store, room id, and the
/// invite event (whose `sender` domain is B — the reject handshake target).
async fn serve_resident_with_invite(
    invitee: &str,
) -> (
    String,
    Arc<SqliteStore>,
    OwnedRoomId,
    neutrino_common::Event,
    TempDir,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let name = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let bob: OwnedUserId = format!("@bob:{name}").parse().unwrap();
    let (store, tempfile) = fresh_store().await;

    let create = EventBuilder::new(bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let bob_join = EventBuilder::new(bob.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(bob.to_string())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create.event_id.clone()])
        .prev_state_events(vec![create.event_id.clone()])
        .build()
        .expect("build bob join");
    let invite = EventBuilder::new(bob.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(invitee.to_owned())
        .content(json!({ "membership": "invite" }))
        .prev_events(vec![bob_join.event_id.clone()])
        .prev_state_events(vec![bob_join.event_id.clone()])
        .build()
        .expect("build invite");
    store
        .create_room(&create, &[bob_join, invite.clone()])
        .await
        .expect("create_room on B");
    let router = router_with_store(config_for(&name, "bob"), store.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (name, store, room_id, invite, tempfile)
}

#[tokio::test]
async fn outbound_reject_invite_federates_leave_and_removes_stub() {
    // B hosts the room and invited our local alice. A holds only the OOB stub.
    let alice = alice();
    let (_b_name, b_store, room_id, invite, _b_tempfile) =
        serve_resident_with_invite(alice.as_str()).await;

    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config(), a_store.clone());
    a_store.put_invite(&room_id, &alice, &invite).await.unwrap();

    let (status, body) = post_json(
        &a_router,
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    // A's stub is gone (declined).
    assert!(
        a_store
            .get_invite(&room_id, &alice)
            .await
            .unwrap()
            .is_none(),
        "the OOB invite stub must be removed on reject"
    );
    // B applied the leave: alice is now `leave` in B's current state.
    let member = b_store
        .current_state_event(&room_id, "m.room.member", alice.as_str())
        .await
        .unwrap()
        .expect("alice member on B");
    assert_eq!(
        membership_str(&member).as_deref(),
        Some("leave"),
        "B must record alice's leave via the handshake"
    );
}

#[tokio::test]
async fn outbound_reject_invite_unreachable_server_still_removes_stub() {
    // The inviting server is dead; local rejection must proceed regardless.
    let dead = crate::federation::test_support::dead_peer().await;
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config(), a_store.clone());
    let alice = alice();

    let dead_bob: OwnedUserId = format!("@bob:{dead}").parse().unwrap();
    let create = EventBuilder::new(dead_bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let invite = member_pdu(
        &dead_bob,
        alice.as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&create.event_id),
    );
    a_store.put_invite(&room_id, &alice, &invite).await.unwrap();

    let (status, body) = post_json(
        &a_router,
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(
        a_store
            .get_invite(&room_id, &alice)
            .await
            .unwrap()
            .is_none(),
        "stub must be removed even when the inviting server is unreachable"
    );
}

#[tokio::test]
async fn reject_then_reinvite_resurrects_stub() {
    // Inbound invite → stub; reject (inviting server dead) → stub gone; a fresh
    // inbound invite resurrects it. The declined-then-reinvited round trip.
    let dead = crate::federation::test_support::dead_peer().await;
    let (a_store, _a_temp) = fresh_store().await;
    let a_router = router_with_store(config(), a_store.clone());
    let alice = alice();

    let dead_bob: OwnedUserId = format!("@bob:{dead}").parse().unwrap();
    let create = EventBuilder::new(dead_bob.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
    let room_id = create.room_id.clone();
    let invite = member_pdu(
        &dead_bob,
        alice.as_str(),
        &room_id,
        "invite",
        std::slice::from_ref(&create.event_id),
    );
    let invite_id = invite.event_id.clone();
    let path = invite_path(room_id.as_str(), invite_id.as_str());

    let (s, _b) = put_json(&a_router, &path, &invite_body(&invite, None)).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        a_store
            .get_invite(&room_id, &alice)
            .await
            .unwrap()
            .is_some()
    );

    let (s, _b) = post_json(
        &a_router,
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        a_store
            .get_invite(&room_id, &alice)
            .await
            .unwrap()
            .is_none()
    );

    // Re-invite resurrects the declined stub.
    let (s, _b) = put_json(&a_router, &path, &invite_body(&invite, None)).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        a_store
            .get_invite(&room_id, &alice)
            .await
            .unwrap()
            .is_some(),
        "a re-invite after a decline must resurrect the stub"
    );
}
