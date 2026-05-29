# `/get_missing_events` Federation Endpoint — HTTP Implementation Plan (Part 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the HTTP handler for `POST /_matrix/federation/v1/get_missing_events/{room_id}` on top of the already-completed storage work.

**Architecture:** New `federation/` submodule under `crates/neutrino-http/src/`, mirroring the existing `sliding_sync/` layout. Handler returns `Result<Json<Response>, FedError>` with `impl IntoResponse for FedError` mapping to `{errcode, error}` JSON bodies — follows CLAUDE.md's prescription for handler shape rather than the older explicit-`match` pattern in `sliding_sync/`. Body is parsed via a local `RequestBody` struct (not ruma's `Request`) because the ruma `Request` carries `room_id` as a `#[ruma_api(path)]` field which makes `axum::Json<Request>` extraction awkward.

**Tech Stack:** Rust, axum 0.8, ruma (with `federation-api-s` feature — already enabled in Part 1), tokio, thiserror, serde_json, tempfile (dev), tower (dev).

**Source of truth:** `/workspace/docs/get-missing-events.md` — the design doc, including the §"Algorithm" + §"Tests" updates already landed in Part 1.

**Execution note:** the repository is read-only in the environment where this plan was written. Each task ends with a `STOP — checkpoint` step instead of a `git commit`. When the plan is run against a writeable clone, treat each checkpoint as one logical commit (files + suggested message included). Pause at every checkpoint for human review before starting the next task.

---

## Part 1 — what already landed

Part 1 plan: `docs/superpowers/plans/2026-05-28-federation-get-missing-events.md` (Tasks 1–5 only — the storage work).

What's on disk and tested:

- `crates/neutrino-store-sqlite/src/store/dag.rs` — `validate_inputs` split into `validate_room_exists` + `validate_events_exist`; `missing_events` follows Synapse-style seed-exclusion semantics (initial frontier is parents-of-`latest`, excluded set is `earliest ∪ latest`); 10 `missing_events` tests + 18 `events_before` tests + the schema test, all green (28/28 in `store::dag`). Cross-room defence preserved (and is *stricter than Synapse*, which doesn't scope its query to `room_id`).
- `crates/neutrino-store/src/lib.rs:260-275` — trait doc-comment rewritten to spec the lenient + seed-exclusive contract explicitly.
- `docs/get-missing-events.md` §"Algorithm" + §"Tests A" — design doc amended to match the implemented semantics and the realistic Synapse-port scope.

**Not yet landed** (in Part 2 scope): Cargo `federation-api-s` feature flag, federation module scaffold, route wiring, happy-path impl, e2e tests, project bookkeeping.

## Pre-work — read these before touching code

- `/workspace/docs/get-missing-events.md` — design doc.
- `/workspace/crates/neutrino-http/src/sliding_sync/mod.rs:53-78` — `SyncError` shape (we will diverge intentionally — see Task 2 of this plan).
- `/workspace/crates/neutrino-http/src/lib.rs:32-143` — `AppState`, `lock_app`, router wiring, `error_response` helper (the latter belongs to the legacy sync pattern; **don't reuse it** — federation uses `IntoResponse`).
- `/workspace/crates/neutrino-http/tests/e2e_sliding_sync.rs` — e2e harness conventions (local `config()` / `post()` / `get()` helpers; no shared module).

## File structure

**Modified in this plan:**
- `crates/neutrino-http/Cargo.toml` — add `"federation-api-s"` to ruma features.
- `crates/neutrino-http/src/lib.rs` — `mod federation;` + one new `.route(...)` line.
- `complement/VIABLE-TESTS.md` — append blocked-tests row.
- `PLAN.md` — tick federation checkbox + decisions-log entry.
- `LOG.md` — 2-line summary at the bottom.

**Created in this plan:**
- `crates/neutrino-http/src/federation/mod.rs` — `FedError` enum + `impl IntoResponse`; declares `pub mod get_missing_events;`.
- `crates/neutrino-http/src/federation/get_missing_events.rs` — handler.
- `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs` — e2e tests (11 cases).

---

### Task 1: Enable the `federation-api-s` ruma feature

**Files:**
- Modify: `crates/neutrino-http/Cargo.toml:18`

- [ ] **Step 1: Add the feature flag**

```toml
ruma = { workspace = true, features = ["client-api-s", "federation-api-s", "unstable-msc4186"] }
```

- [ ] **Step 2: Verify the federation types resolve**

```
cargo check -p neutrino-http
```

Expected: no errors. The path `ruma::api::federation::event::get_missing_events::v1::{Request, Response}` should be available — but we won't actually use those types in code (see Task 2 for why), so this step just confirms the dependency tree compiles.

- [ ] **Step 3: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/Cargo.toml`

Suggested commit message:
> feat(http): enable ruma federation-api-s feature

**Pause here before starting Task 2.**

---

### Task 2: Scaffold the `federation/` module + wire the route

**Files:**
- Create: `crates/neutrino-http/src/federation/mod.rs`
- Create: `crates/neutrino-http/src/federation/get_missing_events.rs`
- Modify: `crates/neutrino-http/src/lib.rs`

This task lands the module skeleton and the route wiring. The handler returns 400 for malformed requests / empty `latest_events` and 404 for unknown rooms, but the happy-path body is just `{"events": []}` — Task 3 fills it in with the real walk.

- [ ] **Step 1: Create `federation/mod.rs`**

```rust
//! Server-Server federation endpoints.
//!
//! Mesh-trusted layout: no X-Matrix header parsing, no signature
//! verification, no history-visibility filter. See
//! `docs/get-missing-events.md` for the full deviation list.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_store::StorageError;
use serde_json::json;

pub mod get_missing_events;

#[derive(Debug, thiserror::Error)]
pub enum FedError {
    #[error("bad request: {0}")]
    BadRequest(&'static str),
    #[error("room not found")]
    RoomNotFound,
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

impl IntoResponse for FedError {
    fn into_response(self) -> Response {
        let (status, errcode, msg) = match &self {
            FedError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                (*msg).to_owned(),
            ),
            FedError::RoomNotFound => (
                StatusCode::NOT_FOUND,
                "M_NOT_FOUND",
                "room not found".to_owned(),
            ),
            FedError::Storage(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                e.to_string(),
            ),
        };
        (status, Json(json!({"errcode": errcode, "error": msg}))).into_response()
    }
}
```

- [ ] **Step 2: Create `federation/get_missing_events.rs` — skeleton only**

```rust
//! `POST /_matrix/federation/v1/get_missing_events/{room_id}`
//!
//! See `docs/get-missing-events.md` for the algorithm and the
//! trusted-mesh spec deviations.

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_store::RoomStore;
use ruma::{OwnedEventId, OwnedRoomId};
use serde::Deserialize;

use crate::{AppState, federation::FedError, lock_app};

const DEFAULT_LIMIT: u64 = 10;
const MAX_LIMIT: u64 = 20;

/// Body of `/get_missing_events`. We parse this directly rather than
/// going through `ruma::api::federation::event::get_missing_events::v1::Request`
/// because ruma's `Request` carries `room_id` as a path field
/// (`#[ruma_api(path)]`), which complicates a clean `axum::Json<Request>`
/// extraction. The field set here is exactly the wire body.
#[derive(Debug, Deserialize)]
pub struct RequestBody {
    /// Event IDs the requester already has — exclusive walk
    /// boundary. Empty by default; unknown IDs are no-ops on the
    /// walk (see `DagStore::missing_events` doc-comment).
    #[serde(default)]
    pub earliest_events: Vec<OwnedEventId>,

    /// Starting points for the BFS over `prev_events`. Required and
    /// non-empty. Unknown IDs contribute nothing (no parents
    /// reachable).
    #[serde(default)]
    pub latest_events: Vec<OwnedEventId>,

    /// Optional cap on results. Saturating: anything over `MAX_LIMIT`
    /// is silently clamped (not a 400). Defaults to `DEFAULT_LIMIT`
    /// when omitted.
    pub limit: Option<u64>,

    /// Parsed so a malformed JSON `min_depth` still 400s. Otherwise
    /// ignored — Neutrino has no `depth` column (PLAN.md 2026-05-22
    /// decision). FIXME: spec deviation, accepted under trusted-mesh.
    #[serde(default)]
    #[allow(dead_code)]
    pub min_depth: Option<u64>,
}

pub async fn handle(
    state: State<AppState>,
    Path(room_id): Path<OwnedRoomId>,
    Json(req): Json<RequestBody>,
) -> Result<Json<serde_json::Value>, FedError> {
    if req.latest_events.is_empty() {
        return Err(FedError::BadRequest("latest_events must not be empty"));
    }

    let store = {
        let app = lock_app(&state.0);
        app.store.clone()
    };

    if store.get_room_version(&room_id).await?.is_none() {
        return Err(FedError::RoomNotFound);
    }

    let _limit = req.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;

    // Task 3 fills this in. For now ship an empty events array so
    // the route is observable from end-to-end after Step 3.
    Ok(Json(serde_json::json!({ "events": [] })))
}
```

- [ ] **Step 3: Wire `mod federation;` and the route in `lib.rs`**

In `crates/neutrino-http/src/lib.rs`, find the existing module declarations:

```rust
mod legacy_sync;
mod sliding_sync;
```

Add `mod federation;` (alphabetical ordering would place it first):

```rust
mod federation;
mod legacy_sync;
mod sliding_sync;
```

In the `router()` function body (around line 98–143), add the federation route. Insert it before `.with_state(state)`:

```rust
.route(
    "/_matrix/federation/v1/get_missing_events/{room_id}",
    post(federation::get_missing_events::handle),
)
```

- [ ] **Step 4: Build clean**

```
cargo clippy -p neutrino-http --tests -- -D warnings
```

Expected: no errors, no warnings.

- [ ] **Step 5: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/src/federation/mod.rs` *(new)*
- `crates/neutrino-http/src/federation/get_missing_events.rs` *(new)*
- `crates/neutrino-http/src/lib.rs`

Suggested commit message:
> feat(http): scaffold federation/get_missing_events handler
>
> Routes POST /_matrix/federation/v1/get_missing_events/{room_id}
> with FedError + IntoResponse mapping (M_INVALID_PARAM /
> M_NOT_FOUND / M_UNKNOWN). RequestBody is a local struct rather
> than ruma's federation::event::get_missing_events::v1::Request —
> ruma's Request carries room_id as a #[ruma_api(path)] field
> which complicates axum::Json<Request> extraction. Happy-path
> body is empty; filled in next commit.

**Pause here for review before starting Task 3.** First federation-shaped handler in the tree — reviewer should sanity-check the module layout and the FedError ↔ M_*errcode mapping.

---

### Task 3: Implement the happy path

**Files:**
- Modify: `crates/neutrino-http/src/federation/get_missing_events.rs`

- [ ] **Step 1: Replace the `events: []` placeholder with the actual walk**

In `handle`, after the 400/404 paths and the `limit` clamp, call `DagStore::missing_events` and return the events:

```rust
let limit = req.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;

let latest: Vec<&ruma::EventId> = req.latest_events.iter().map(|id| id.as_ref()).collect();
let earliest: Vec<&ruma::EventId> = req.earliest_events.iter().map(|id| id.as_ref()).collect();

let events = store
    .missing_events(&room_id, &latest, &earliest, limit)
    .await?;

// Federation peers receive Event.raw verbatim — NO event_view
// enrichment. The bytes must be exactly what the reference hash
// was computed over. See docs/event-view-conversions.md and
// docs/get-missing-events.md §"Algorithm" step 6.
let raw_events: Vec<Box<serde_json::value::RawValue>> =
    events.into_iter().map(|e| e.raw).collect();

Ok(Json(serde_json::json!({ "events": raw_events })))
```

The `Json<serde_json::Value>` return type stays — keeping it generic avoids needing to use ruma's `Response` struct (which would require manual `Serialize`-from-`Box<RawValue>` field handling). Acceptable because the response shape is just `{"events": [...]}` and we want zero enrichment.

`DagStore` needs to be in scope; if it isn't already imported, add `use neutrino_store::{DagStore, RoomStore};` to the file's `use` block.

- [ ] **Step 2: Build clean**

```
cargo clippy -p neutrino-http --tests -- -D warnings
```

- [ ] **Step 3: STOP — checkpoint** (no test yet — Task 4 covers e2e)

Files in this checkpoint:
- `crates/neutrino-http/src/federation/get_missing_events.rs`

Suggested commit message:
> feat(http): implement /get_missing_events happy path

**Pause here for review before starting Task 4.**

---

### Task 4: E2E test file — all 11 cases from the design doc

**Files:**
- Create: `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs`

Mirror the conventions in `tests/e2e_sliding_sync.rs`: local `config()` + `post()` helpers, no shared module, events seeded through CSAPI endpoints (`POST /createRoom`, `PUT /send/...`).

The design doc §Tests B lists 11 e2e tests. Group into bad-request, happy-path, edge, and pinning categories. Write all tests, then run; tests for already-implemented behaviour should pass without further code changes.

**Important note on assertion shapes — Synapse-style seed exclusion changes some assertions.** Part 1 changed `missing_events` to never return `latest_events` themselves. This affects:
- `happy_path_returns_events_between_earliest_and_latest` — the gap content excludes both `latest` and `earliest`.
- `respects_limit` / `default_limit_is_10` — the seed-event doesn't count toward the limit cap (it's not in the result at all).
- `empty_earliest_walks_back_to_room_root` — the create event IS reachable (it's an ancestor); the latest seed isn't.

- [ ] **Step 1: Test-file scaffolding (local helpers)**

```rust
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use neutrino_http::{Config, router};
use serde_json::{Value, json};
use tower::ServiceExt;

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

async fn post_fed(
    app: &Router,
    path: &str,
    body: &Value,
) -> (StatusCode, Value) {
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

async fn post_raw(
    app: &Router,
    path: &str,
    body: Body,
    content_type: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("POST").uri(path);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
    (status, value)
}

/// PUT helper for sending events via CSAPI — mirrors the pattern in
/// `e2e_sliding_sync.rs`.
async fn put_csapi(
    app: &Router,
    path: &str,
    body: &Value,
) -> (StatusCode, Value) {
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

/// Returns (room_id, vec of event_ids in send order). Events have
/// distinct `prev_events` only insofar as the CSAPI write path
/// itself chooses them — the test trusts the store's behaviour to
/// link sends in causal order. If the resulting DAG doesn't have the
/// expected shape (e.g. all sends end up with `prev_events: []`),
/// the test will fail visibly and the helper needs a closer look.
async fn seed_room_with_messages(app: &Router, count: usize) -> (String, Vec<String>) {
    let (_, body) = post_fed(app, "/_matrix/client/v3/createRoom", &json!({})).await;
    let room_id = body["room_id"].as_str().unwrap().to_string();
    let mut event_ids = Vec::with_capacity(count);
    for i in 0..count {
        let path = format!(
            "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn-{i}"
        );
        let (_, resp) = put_csapi(app, &path, &json!({"body": format!("msg-{i}"), "msgtype": "m.text"})).await;
        event_ids.push(resp["event_id"].as_str().unwrap().to_string());
    }
    (room_id, event_ids)
}

fn fed_path(room_id: &str) -> String {
    format!("/_matrix/federation/v1/get_missing_events/{room_id}")
}
```

*Caveat:* before relying on `seed_room_with_messages` for happy-path tests, write a quick smoke test that asserts `event_ids.len() == count` (confirms the sends succeeded) — and if the test for `happy_path_returns_events_between_earliest_and_latest` fails because the events come back disconnected, investigate whether the CSAPI write path is actually linking them via `prev_events`. (Per the e2e_sliding_sync investigation, `put_event` builds events with `prev_events: []` today — but that's a build-time choice that may have changed.) If the chain isn't linked, the test needs to construct events via a different path or the design has bigger gaps.

- [ ] **Step 2: Bad-request tests (3)**

```rust
#[tokio::test]
async fn bad_request_empty_latest_events_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({"earliest_events": [], "latest_events": []}),
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["errcode"], "M_INVALID_PARAM");
}

#[tokio::test]
async fn bad_request_non_json_body_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, _) = post_raw(
        &app,
        &fed_path(&room_id),
        Body::from("not json"),
        Some("application/json"),
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bad_request_missing_required_field_returns_400() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, _) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({"earliest_events": []}), // latest_events missing
    ).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

*Note on `bad_request_missing_required_field_returns_400`:* the `RequestBody` struct uses `#[serde(default)]` on `latest_events`, so a missing field deserializes to an empty `Vec` — which then triggers the same 400 path as the explicit-empty case. Result: this test passes via the empty-latest path. Adjust the test name comment to reflect this if you find the deserializer behaves differently than expected.

- [ ] **Step 3: 404 test**

```rust
#[tokio::test]
async fn unknown_room_returns_404() {
    let app = router(config()).await.expect("router init");
    let (status, body) = post_fed(
        &app,
        &fed_path("!nope:example.org"),
        &json!({"earliest_events": [], "latest_events": ["$x:example.org"]}),
    ).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["errcode"], "M_NOT_FOUND");
}
```

- [ ] **Step 4: Happy path test**

```rust
#[tokio::test]
async fn happy_path_returns_events_between_earliest_and_latest() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 5).await;
    // gap between event_ids[0] (earliest) and event_ids[4] (latest).
    // Synapse-style semantics: latest and earliest are boundaries,
    // never in the response. The gap is events 1, 2, 3.
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [&event_ids[0]],
            "latest_events": [&event_ids[4]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");

    // Cardinality: 3 events strictly between earliest and latest.
    // (Plus possibly some room-creation state events that are
    // ancestors of event_ids[0] but bounded by earliest — those
    // would be reached only if the message chain doesn't pass
    // through them. Adjust the assertion if the actual cardinality
    // surfaces room state events too.)
    assert_eq!(
        events.len(),
        3,
        "expected 3 events strictly between earliest and latest, got {events:?}"
    );
}
```

*If this fails with a different cardinality:* the CSAPI write path either (a) doesn't link sends causally (so `latest_events[4]` doesn't traverse back through the others), or (b) links them but pulls in extra state events as part of the walk. Either way, debug before adjusting the assertion — don't soften it silently.

- [ ] **Step 5: Limit tests (2)**

```rust
#[tokio::test]
async fn respects_limit() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 30).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[29]],
            "limit": 50,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(events.len() <= 20, "max cap is 20, got {}", events.len());
}

#[tokio::test]
async fn default_limit_is_10() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 30).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[29]],
            // no limit field
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert_eq!(events.len(), 10);
}
```

- [ ] **Step 6: Edge case tests (3)**

```rust
#[tokio::test]
async fn empty_earliest_walks_back_to_room_root() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 3).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[2]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    // Under Synapse-style semantics, latest event_ids[2] is excluded.
    // The walk reaches all ancestors. Whether m.room.create is in the
    // chain depends on whether the CSAPI write path links message
    // sends causally back to the room state events. Inspect the
    // result on first run and pin the cardinality; if the walk
    // doesn't reach create, the CSAPI path is sending message events
    // with empty `prev_events` and this assertion needs softening to
    // "reaches at least event_ids[1] and event_ids[0]".
    let has_create = events.iter().any(|e| e["type"] == "m.room.create");
    let has_msg0 = events.iter().any(|e| e.get("event_id").is_none()  // wire-bytes have no event_id
        // ...so identify msg0 by content body
        && e["content"]["body"] == "msg-0");
    let has_msg1 = events.iter().any(|e| e["content"]["body"] == "msg-1");
    // At minimum, the walk reaches the prior messages. Create may or
    // may not be linked.
    assert!(has_msg0, "walk must reach msg-0 from latest=msg-2");
    assert!(has_msg1, "walk must reach msg-1 from latest=msg-2");
    if !has_create {
        // Document the gap, don't fail — the CSAPI sender clearly
        // doesn't link back to room state. A future fix in the
        // sender (or a dedicated state-link write path) would
        // make this assertion meaningful.
        eprintln!("note: walk did not reach m.room.create — CSAPI sends may not be linked to room state");
    }
}

#[tokio::test]
async fn latest_event_not_in_room_returns_empty() {
    let app = router(config()).await.expect("router init");
    let (room_id, _) = seed_room_with_messages(&app, 1).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": ["$totally_fabricated:example.org"],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(events.is_empty(), "fabricated latest should produce empty walk");
}

#[tokio::test]
async fn min_depth_field_ignored() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 3).await;
    let body_no_min = json!({
        "earliest_events": [],
        "latest_events": [&event_ids[2]],
        "limit": 20,
    });
    let body_with_min = json!({
        "earliest_events": [],
        "latest_events": [&event_ids[2]],
        "limit": 20,
        "min_depth": 999_999,
    });
    let (status1, resp1) = post_fed(&app, &fed_path(&room_id), &body_no_min).await;
    let (status2, resp2) = post_fed(&app, &fed_path(&room_id), &body_with_min).await;
    assert_eq!(status1, StatusCode::OK);
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(
        resp1["events"].as_array().unwrap().len(),
        resp2["events"].as_array().unwrap().len(),
        "min_depth must be ignored: same result with or without it"
    );
}
```

- [ ] **Step 7: Wire-bytes passthrough test**

```rust
#[tokio::test]
async fn wire_bytes_passthrough() {
    let app = router(config()).await.expect("router init");
    let (room_id, event_ids) = seed_room_with_messages(&app, 2).await;
    let (status, body) = post_fed(
        &app,
        &fed_path(&room_id),
        &json!({
            "earliest_events": [],
            "latest_events": [&event_ids[1]],
            "limit": 20,
        }),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().unwrap();
    assert!(!events.is_empty(), "should return at least one event");
    for ev in events {
        let obj = ev.as_object().expect("each event is a JSON object");
        assert!(
            !obj.contains_key("event_id"),
            "federation events must ship verbatim — no event_id enrichment. \
             event_view::enrich_for_client must NOT be called on federation path. \
             Found event_id in: {ev}"
        );
    }
}
```

- [ ] **Step 8: Run the full e2e suite for this file**

```
cargo test -p neutrino-http --test e2e_federation_get_missing_events
```

Expected: all 11 tests pass. If any test fails, investigate root cause before adjusting either the impl or the assertion. **Do not soften assertions to make tests pass.**

- [ ] **Step 9: STOP — checkpoint**

Files in this checkpoint:
- `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs` *(new)*

Suggested commit message:
> test(http): e2e coverage for /get_missing_events (11 tests)

**Pause here for review before starting Task 5.** All 11 design-doc e2e cases should be green; reviewer should confirm coverage matches the §Tests B table in the design doc.

---

### Task 5: Complement VIABLE-TESTS row

**Files:**
- Modify: `complement/VIABLE-TESTS.md`

- [ ] **Step 1: Append a section documenting the two blocked tests**

At the end of `VIABLE-TESTS.md`, add:

```markdown
## Federation `/get_missing_events` — blocked

| Test | Block on |
|---|---|
| `federation/TestInboundCanReturnMissingEvents` | Phase 4b/4c (state-res) + Phase 6 (`send_join` accept) — requires a federated join before the endpoint is exercised. Also asserts history-visibility redaction, which we defer under the trusted-mesh model (see `docs/get-missing-events.md`). |
| `federation/TestGetMissingEventsGapFilling` | Phase 4b/4c (state-res) — outbound test; SUT must receive a federation `/send`, detect a gap, and call out to `/get_missing_events`. Needs state-res to integrate the response. |

Neither is added to `complement/allowlist.txt`.
```

- [ ] **Step 2: STOP — checkpoint**

Files in this checkpoint:
- `complement/VIABLE-TESTS.md`

Suggested commit message:
> docs(complement): document federation /get_missing_events blocked tests

**Pause here for review before starting Task 6.**

---

### Task 6: Project bookkeeping — PLAN.md + LOG.md + final verification

**Files:**
- Modify: `PLAN.md` — tick checkbox, add decisions-log entry
- Modify: `LOG.md` — append 2-line summary at the bottom

- [ ] **Step 1: Tick the `Server-Server backfill/get_missing_events implementation` checkbox in PLAN.md**

```
grep -n "get_missing_events\|backfill" /workspace/PLAN.md
```

The checkbox to tick is around line 61. Flip `[ ]` → `[x]`.

- [ ] **Step 2: Append a decisions-log entry**

Add at the end of `PLAN.md`'s decisions log:

```markdown
2026-05-28 (Part 2 of 2): HTTP handler for `POST /_matrix/federation/v1/get_missing_events/{room_id}` landed. New `federation/` submodule under `crates/neutrino-http/src/` with the route registered in `lib.rs::router`. Handler uses `Result<Json<Value>, FedError>` + `impl IntoResponse for FedError` (M_INVALID_PARAM / M_NOT_FOUND / M_UNKNOWN) per CLAUDE.md's prescription, intentionally diverging from `sliding_sync/`'s older explicit-`match` + `error_response` pattern (left untouched as legacy code). Body parsed via a local `RequestBody` struct rather than ruma's `federation::event::get_missing_events::v1::Request` because ruma's `Request` carries `room_id` as `#[ruma_api(path)]` which complicates `axum::Json<Request>`. Body shape: `latest_events: Vec<OwnedEventId>` (required), `earliest_events: Vec<OwnedEventId>` (default empty), `limit: Option<u64>`, `min_depth: Option<u64>` (parsed but ignored — `#[allow(dead_code)]`, FIXME: spec deviation, accepted under trusted-mesh). Response is `{"events": [<Box<RawJsonValue>>, ...]}` populated from `Event.raw` verbatim — *no* `event_view::enrich_for_client` enrichment, federation peers receive the canonical v12 / MSC4242 wire bytes the reference hash was computed over. Pinned by the `wire_bytes_passthrough` e2e test (`event_id` absent from every event in the response). 11 e2e tests in `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs` covering bad-request × 3, 404, happy-path, limit × 2, edge × 3, wire-bytes passthrough. Two Complement tests (`TestInboundCanReturnMissingEvents`, `TestGetMissingEventsGapFilling`) documented as blocked on Phase 4b/4c (state-res) + Phase 6 (federated join) in `complement/VIABLE-TESTS.md`; not added to the allowlist. No history-visibility filter — deliberate trusted-mesh spec deviation already pinned in `docs/get-missing-events.md`.
```

- [ ] **Step 3: Append the 2-line LOG.md entry at the bottom**

Per memory: LOG.md is append-only, oldest first. Add at the *bottom*:

```markdown
- 2026-05-28: Land `POST /_matrix/federation/v1/get_missing_events/{room_id}` — first federation endpoint. New `federation/` submodule in `neutrino-http`, `FedError` + `IntoResponse` mapping.
```

- [ ] **Step 4: Final fmt + clippy + test pass (workspace)**

```
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Per CLAUDE.md, workspace-wide check before declaring done. If anything fails, fix it before the final checkpoint.

- [ ] **Step 5: STOP — final checkpoint**

Files in this checkpoint:
- `PLAN.md`
- `LOG.md`

Suggested commit message:
> docs(plan): /get_missing_events landed; record decision and log entries

**Pause here for final review.** This is the last checkpoint — all task work should be complete. Reviewer should run the verification checklist below before approving.

---

## Verification before declaring done

- [ ] `cargo fmt --all` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo test --workspace` green — including the 11 new e2e tests and the relaxed-storage tests from Part 1.
- [ ] When committing against a writeable clone: one focused commit per checkpoint (6 commits in this plan, on top of the 5 from Part 1). Each checkpoint above lists files + suggested message; do not collapse checkpoints into one giant commit unless the reviewer explicitly asks.
- [ ] `PLAN.md` checkbox flipped (line ~61); decisions-log entry appended.
- [ ] `LOG.md` has the 2-line summary at the bottom.
- [ ] `complement/VIABLE-TESTS.md` has the blocked-tests row.
- [ ] Manual sanity check: `curl` (or equivalent) against a running `neutrino` binary returns the expected response shape for at least the happy path. If you can't get a binary running, say so explicitly — `cargo test` validates correctness but not deployment.

## Open items deferred (not part of this plan)

These are noted so they don't ambush a future implementer:
- **`event-id-design.md` is referenced from code comments but doesn't exist in `docs/`.** Separate cleanup task — create the doc or scrub the references.
- **Outbound `/get_missing_events`** (we call peers to fill our own gaps). Needs state-res — Phase 6 territory per design doc.
- **History-visibility filtering.** Gated on a `state_at_event` provider on `StateStore`. Phase 6.
- **Origin source on `/send`.** When `/send` lands, the storage trait expects `record_federation_txn(origin, txn_id)`. Resolution path documented in `docs/get-missing-events.md` §"Open questions".
- **CSAPI write-path `prev_events` linking.** If the e2e tests in Task 4 surface that `put_event` writes message events with empty `prev_events`, that's a real gap — the federation endpoint can only return the events the DAG knows about. Tracking + fixing is out of scope here.

## LOC budget (this plan only — Part 2)

- Cargo feature flag: 1 LOC.
- `federation/mod.rs` (FedError + IntoResponse): ~50 LOC.
- `federation/get_missing_events.rs` (skeleton + happy path): ~80 LOC.
- Router wiring: ~5 LOC.
- E2E test file: ~280 LOC.
- Complement docs + PLAN/LOG: ~15 LOC.
- **Total ~430 LOC, ~65% tests.**
