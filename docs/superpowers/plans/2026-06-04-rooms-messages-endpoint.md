# `GET /rooms/{roomId}/messages` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the C2S `GET /_matrix/client/v3/rooms/{roomId}/messages` endpoint (paginated room history) so a downstream client stops getting 404s.

**Architecture:** Extend the existing `EventStore::room_messages` store method with a `to` stop-bound, then add a thin HTTP handler in a new `messages.rs` module that does a join-membership check, parses pagination params, calls `room_messages`, and serialises `{chunk, start, end}`. Mirrors Synapse where neutrino has the mechanism; skips history-visibility, backfill, filters, and lazy `state`.

**Tech Stack:** Rust, axum, rusqlite, ruma (`Raw<AnyTimelineEvent>`), neutrino-store/-sqlite/-http.

> ⚠️ **Environment:** `/workspace/.git` is mounted **read-only** — the `git commit` steps cannot run in this sandbox. Make the edits + run `cargo`; the human commits. Skip commit steps here.

> **Spec:** `docs/superpowers/specs/2026-06-04-rooms-messages-endpoint-design.md`.

---

## File Structure

**Modify:**
- `crates/neutrino-store/src/lib.rs` — `room_messages` trait signature + doc (`to` param).
- `crates/neutrino-store-sqlite/src/store/events.rs` — `room_messages` impl (`to` bound) + new `to` tests + update in-file test call sites.
- `crates/neutrino-http/src/sliding_sync/build.rs` — 2 `room_messages` call sites pass `None`.
- `crates/neutrino-http/src/membership.rs` — promote `current_membership` to `pub(crate)`.
- `crates/neutrino-http/src/lib.rs` — `mod messages;` + one route.
- `PLAN.md`, `LOG.md` — bookkeeping.

**Create:**
- `crates/neutrino-http/src/messages.rs` — the handler + param parsing.
- `crates/neutrino-http/tests/e2e_messages.rs` — e2e tests.

---

## Task 1: Add `to` bound to `room_messages` (trait + impl + call sites)

**Files:**
- Modify: `crates/neutrino-store/src/lib.rs` (around lines 206–218)
- Modify: `crates/neutrino-store-sqlite/src/store/events.rs` (impl ~210–324; test call sites)
- Modify: `crates/neutrino-http/src/sliding_sync/build.rs` (lines 404, 566)

- [ ] **Step 1: Update the trait signature + doc** (`crates/neutrino-store/src/lib.rs`)

Replace the existing `room_messages` declaration with:

```rust
    /// Pre:  the room must exist; if `from`/`to` are `Some`, the token must have been
    ///       returned by a previous call (or built from a known `StreamPos`).
    /// Post: returns up to `limit` events in the requested direction, starting just past
    ///       `from` and stopping *before* `to` (both exclusive). If `from` is `None` and
    ///       `dir` is `Backward`, starts from the most recent event; if `Forward`, from the
    ///       earliest. `to` is `None` for no stop boundary in that direction. The returned
    ///       `PaginationToken` is `None` when no further events exist within the range.
    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        to: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError>;
```

- [ ] **Step 2: Update the SQLite impl** (`crates/neutrino-store-sqlite/src/store/events.rs`)

Replace the whole `async fn room_messages(...) { ... }` body with the version below (adds the `to_pos` resolution and the second WHERE bound; the room-existence check, `limit == 0` short-circuit, and overflow→token logic are unchanged):

```rust
    async fn room_messages(
        &self,
        room_id: &RoomId,
        from: Option<PaginationToken>,
        to: Option<PaginationToken>,
        dir: Direction,
        limit: usize,
    ) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError> {
        let room_id = room_id.to_owned();
        // Fetch one extra row so we can distinguish "exactly `limit` events
        // remain" (return `None` token) from "more than `limit` remain"
        // (return a `Some` token).
        let fetch_limit_i64 = i64::try_from(limit.saturating_add(1))
            .map_err(|_| Error::InvalidInput(format!("limit {limit} exceeds i64::MAX")))?;
        // Default `from` per direction: Forward starts at 0, Backward at i64::MAX.
        let from_pos: i64 = match from {
            Some(t) => i64::try_from(t.0).map_err(|_| {
                Error::InvalidInput(format!("PaginationToken {} exceeds i64::MAX", t.0))
            })?,
            None => match dir {
                Direction::Forward => 0,
                Direction::Backward => i64::MAX,
            },
        };
        // Exclusive stop boundary. Unconstraining sentinels when `to` is None:
        // Forward never reaches i64::MAX, Backward never reaches i64::MIN.
        let to_pos: i64 = match to {
            Some(t) => i64::try_from(t.0).map_err(|_| {
                Error::InvalidInput(format!("PaginationToken {} exceeds i64::MAX", t.0))
            })?,
            None => match dir {
                Direction::Forward => i64::MAX,
                Direction::Backward => i64::MIN,
            },
        };

        self.run_read(
            move |conn| -> Result<(Vec<Event>, Option<PaginationToken>), Error> {
                // Pre-condition (trait: "the room must exist").
                let exists: Option<i64> = conn
                    .query_row(
                        "SELECT 1 FROM rooms WHERE room_id = ?",
                        params![room_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if exists.is_none() {
                    return Err(Error::InvalidInput(format!(
                        "unknown room: {}",
                        room_id.as_str()
                    )));
                }

                // `limit == 0` is degenerate: no row to hang a token on.
                if limit == 0 {
                    return Ok((Vec::new(), None));
                }

                // `from` faces one way, the exclusive `to` faces the other.
                let (from_cmp, to_cmp, order) = match dir {
                    Direction::Forward => (">", "<", "ASC"),
                    Direction::Backward => ("<", ">", "DESC"),
                };

                let query = format!(
                    "SELECT stream_pos, {EVENT_COLUMNS} FROM events \
                     WHERE room_id = ? AND stream_pos {from_cmp} ? AND stream_pos {to_cmp} ? \
                     ORDER BY stream_pos {order} LIMIT ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(
                    params![room_id.as_str(), from_pos, to_pos, fetch_limit_i64],
                    |row| {
                        let stream_pos: i64 = row.get("stream_pos")?;
                        Ok((stream_pos, EventRow::try_from(row)))
                    },
                )?;

                let mut events = Vec::with_capacity(limit);
                let mut last_in_page: Option<i64> = None;
                let mut overflow_seen = false;
                for r in rows {
                    let (sp, ev) = r?;
                    if events.len() == limit {
                        // Sentinel (limit+1)-th row within range — more data exists.
                        overflow_seen = true;
                        break;
                    }
                    last_in_page = Some(sp);
                    events.push(ev?.into_event());
                }

                let next = if overflow_seen {
                    match last_in_page {
                        Some(p) => Some(PaginationToken(u64::try_from(p).map_err(|_| {
                            Error::Internal(format!(
                                "negative stream_pos encountered while building pagination token: {}",
                                p
                            ))
                        })?)),
                        None => None,
                    }
                } else {
                    None
                };

                Ok((events, next))
            },
        )
        .await
    }
```

- [ ] **Step 3: Update production call sites** (`crates/neutrino-http/src/sliding_sync/build.rs`)

Insert `None` as the new third argument at both call sites:
- Line ~404: `.room_messages(room_id, None, Direction::Backward, 1)` → `.room_messages(room_id, None, None, Direction::Backward, 1)`
- Line ~566: `.room_messages(room_id, None, Direction::Backward, cfg.timeline_limit)` → `.room_messages(room_id, None, None, Direction::Backward, cfg.timeline_limit)`

- [ ] **Step 4: Update existing in-file test call sites** (`crates/neutrino-store-sqlite/src/store/events.rs`)

Insert `None` as the third arg in every existing test call (grep `room_messages(` to confirm all). The known sites and their new forms:
- `.room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 10)` → `.room_messages(*ALICE_ROOM_ID, None, None, Direction::Forward, 10)` (occurs at ~898, 930, 1024)
- `.room_messages(*ALICE_ROOM_ID, None, Direction::Backward, 10)` → `... None, None, Direction::Backward, 10)` (~917)
- `.room_messages(room, None, Direction::Forward, 2)` → `... room, None, None, Direction::Forward, 2)` (~949)
- `.room_messages(room, Some(token.clone()), Direction::Forward, 2)` → `... room, Some(token.clone()), None, Direction::Forward, 2)` (~957)
- `.room_messages(room, next2, Direction::Forward, 2)` → `... room, next2, None, Direction::Forward, 2)` (~971)
- `.room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 2)` → `... None, None, Direction::Forward, 2)` (~989)
- `.room_messages(*ALICE_ROOM_ID, None, Direction::Forward, 0)` → `... None, None, Direction::Forward, 0)` (~1005)
- `.room_messages(unknown, None, Direction::Forward, 10)` → `... unknown, None, None, Direction::Forward, 10)` (~1037)
- the multi-line call at ~1302 — add `None,` after the `from` argument line.
- `.room_messages(*ALICE_ROOM_ID, None, Direction::Backward, 2)` → `... None, None, Direction::Backward, 2)` (~1326)

- [ ] **Step 5: Add new `to`-bound store tests** (`crates/neutrino-store-sqlite/src/store/events.rs`, test module)

Add these tests next to the existing `room_messages_*` tests. They reuse the existing test helpers (`store_with_room()` / whatever seeds `*ALICE_ROOM_ID` with an ordered set of events — match the helper the neighbouring tests use; the events are in stream order). Read the neighbouring tests first to reuse their exact seeding helper and event count.

```rust
    #[tokio::test]
    async fn room_messages_to_bound_forward_is_exclusive() {
        // Seed helper used by the sibling tests gives a room with several
        // events in ascending stream order. Page forward from the start,
        // stopping *before* the token returned by a 1-event probe.
        let s = store_with_room().await;
        // First two events forward.
        let (first_two, _) = s
            .room_messages(*ALICE_ROOM_ID, None, None, Direction::Forward, 2)
            .await
            .unwrap();
        assert_eq!(first_two.len(), 2);
        // A token whose stream_pos sits at the 2nd event: use the `next` from a
        // limit-1 page as an opaque interior token.
        let (_, tok1) = s
            .room_messages(*ALICE_ROOM_ID, None, None, Direction::Forward, 1)
            .await
            .unwrap();
        let stop = tok1.expect("more than one event seeded");
        // Forward from start, stopping before `stop` (exclusive) → fewer than
        // the unbounded page, and never includes the stop event itself.
        let (bounded, _) = s
            .room_messages(*ALICE_ROOM_ID, None, Some(stop.clone()), Direction::Forward, 100)
            .await
            .unwrap();
        assert!(
            bounded.iter().all(|e| e.event_id != first_two[1].event_id)
                || stop.0 != /* stream pos of first_two[1] */ stop.0,
            "events at/after the exclusive `to` are excluded"
        );
        // The bounded page is a strict prefix of the unbounded forward page.
        let (unbounded, _) = s
            .room_messages(*ALICE_ROOM_ID, None, None, Direction::Forward, 100)
            .await
            .unwrap();
        assert!(bounded.len() <= unbounded.len());
    }

    #[tokio::test]
    async fn room_messages_to_none_matches_unbounded() {
        let s = store_with_room().await;
        let (with_none, t1) = s
            .room_messages(*ALICE_ROOM_ID, None, None, Direction::Backward, 100)
            .await
            .unwrap();
        // `to = None` must not truncate vs the historical 4-arg semantics.
        assert!(t1.is_none(), "all events fit, so no further-token");
        assert!(!with_none.is_empty());
    }

    #[tokio::test]
    async fn room_messages_from_to_bracket_backward() {
        // Backward range (from > x > to): build two interior tokens by probing.
        let s = store_with_room().await;
        let (_, hi) = s
            .room_messages(*ALICE_ROOM_ID, None, None, Direction::Backward, 1)
            .await
            .unwrap();
        let from = hi.expect("seeded");
        let (page, _) = s
            .room_messages(*ALICE_ROOM_ID, Some(from), None, Direction::Backward, 100)
            .await
            .unwrap();
        // Everything strictly older than `from`.
        assert!(page.iter().all(|_| true));
    }
```

> NOTE for the implementer: the three tests above are intentionally written against the *sibling* seeding helper. Before finalising, read the existing `room_messages_pagination_roundtrip` test to (a) use the exact same seed helper name, (b) replace the `/* stream pos … */` placeholder in `room_messages_to_bound_forward_is_exclusive` with a concrete assertion that the bounded page does not contain the event at/after `stop` (compare `event_id`s against the unbounded page's tail). Keep each test's assertion meaningful (exclusivity + prefix relationship), not tautological. If the seed helper exposes event ids in order, assert exact membership instead of the `<=`/`all(|_| true)` weakenings.

- [ ] **Step 6: Verify storage**

Run: `cargo test -p neutrino-store-sqlite`
Expected: all `room_messages_*` tests pass (existing + 3 new).
Run: `cargo clippy -p neutrino-store -p neutrino-store-sqlite --tests -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit** (skip in read-only-.git sandbox)

```bash
git add crates/neutrino-store/src/lib.rs crates/neutrino-store-sqlite/src/store/events.rs crates/neutrino-http/src/sliding_sync/build.rs
git commit -m "feat(store): add exclusive 'to' bound to room_messages"
```

---

## Task 2: Promote `current_membership` to `pub(crate)`

**Files:**
- Modify: `crates/neutrino-http/src/membership.rs:87`

- [ ] **Step 1: Change visibility**

Change `async fn current_membership(` to `pub(crate) async fn current_membership(`. No other change.

- [ ] **Step 2: Verify it still compiles**

Run: `cargo build -p neutrino-http`
Expected: success (a `pub(crate)` on an item with existing in-crate callers may trigger no warnings; if clippy later flags it as unused-pub it is not — it gains a cross-module caller in Task 3).

- [ ] **Step 3: Commit** (skip in read-only-.git sandbox)

```bash
git add crates/neutrino-http/src/membership.rs
git commit -m "refactor(http): expose current_membership to the crate"
```

---

## Task 3: New `messages.rs` handler + router wiring

**Files:**
- Create: `crates/neutrino-http/src/messages.rs`
- Modify: `crates/neutrino-http/src/lib.rs` (`mod messages;` + route)

- [ ] **Step 1: Create `crates/neutrino-http/src/messages.rs`**

```rust
//! CSAPI `GET /_matrix/client/v3/rooms/{roomId}/messages` — paginated room history.
//!
//! Mirrors Synapse's `RoomMessageListRestServlet` / `PaginationHandler.get_messages`
//! for the parts neutrino has a mechanism for.
//!
//! KNOWN LIMITATIONS (deliberate — no mechanism in neutrino):
//! - The `filter` query param is **accepted but ignored**: event filtering and
//!   `lazy_load_members` are unimplemented, so the optional `state` field is never
//!   emitted.
//! - No history-visibility filtering: a joined user receives the full timeline chunk.
//! - No federation backfill: `dir=b` returns only locally-held events; an empty
//!   `chunk` with no `end` just means the local timeline start was reached.

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use ruma::OwnedRoomId;
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
use serde_json::{Map, Value, json};

use neutrino_store::{Direction, EventStore, PaginationToken};

use crate::membership::current_membership;
use crate::{AppState, AuthUser, error_response, lock_app};

/// Parse `dir`. Absent → Forward (Synapse default; the spec marks it required,
/// we mirror Synapse's leniency). Only `b`/`f` accepted.
fn parse_dir(params: &HashMap<String, String>) -> Result<Direction, axum::response::Response> {
    match params.get("dir").map(String::as_str) {
        Some("b") => Ok(Direction::Backward),
        Some("f") | None => Ok(Direction::Forward),
        Some(other) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "M_INVALID_PARAM",
            &format!("dir must be 'b' or 'f', got '{other}'"),
        )),
    }
}

/// Parse an opaque pagination token (`from`/`to`). Absent, empty, or the legacy
/// `"END"` sentinel → `None`; a non-numeric value → 400.
fn parse_token(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<PaginationToken>, axum::response::Response> {
    match params.get(key).map(String::as_str) {
        None | Some("") | Some("END") => Ok(None),
        Some(s) => match u64::from_str(s) {
            Ok(n) => Ok(Some(PaginationToken(n))),
            Err(_) => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                &format!("'{key}' parameter is invalid"),
            )),
        },
    }
}

/// Parse `limit`. Absent → 10; capped at 1000 (mirrors Synapse); non-integer → 400.
fn parse_limit(params: &HashMap<String, String>) -> Result<usize, axum::response::Response> {
    match params.get("limit") {
        None => Ok(10),
        Some(s) => match usize::from_str(s) {
            Ok(n) => Ok(n.min(1000)),
            Err(_) => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                "'limit' parameter is invalid",
            )),
        },
    }
}

pub(crate) async fn get_messages(
    state: State<AppState>,
    AuthUser(user): AuthUser,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let rid = match OwnedRoomId::try_from(room_id) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    // Join check: must be a current member. Not joined — including an unknown
    // room (no member event) — is 403, the spec's only documented error here.
    match current_membership(&state.0, &rid, &user).await {
        Ok(Some(m)) if m == "join" => {}
        Ok(_) => {
            return error_response(
                StatusCode::FORBIDDEN,
                "M_FORBIDDEN",
                "You aren't a member of the room.",
            );
        }
        Err(resp) => return resp,
    }

    let dir = match parse_dir(&params) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let from = match parse_token(&params, "from") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let to = match parse_token(&params, "to") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let limit = match parse_limit(&params) {
        Ok(l) => l,
        Err(resp) => return resp,
    };

    let store = lock_app(&state.0).store.clone();

    // `start`: echo `from` if given; else the boundary we paginate from —
    // Forward → "0" (earliest), Backward → the current stream head (latest).
    let start = match &from {
        Some(t) => t.0.to_string(),
        None => match dir {
            Direction::Forward => "0".to_string(),
            Direction::Backward => store.subscribe().borrow().0.to_string(),
        },
    };

    let (events, next) = match store.room_messages(&rid, from, to, dir, limit).await {
        Ok(pair) => pair,
        Err(e) => {
            // Room existence is guaranteed by the join check above, so this is
            // a genuine storage fault, not an unknown-room case.
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    // Order is exactly as room_messages returns it: `b` newest-first,
    // `f` oldest-first. No reversal (unlike sliding-sync).
    let chunk: Vec<Raw<AnyTimelineEvent>> =
        events.iter().map(Raw::<AnyTimelineEvent>::from).collect();

    let mut body: Map<String, Value> = Map::new();
    body.insert("chunk".to_string(), json!(chunk));
    body.insert("start".to_string(), Value::String(start));
    if let Some(t) = next {
        body.insert("end".to_string(), Value::String(t.0.to_string()));
    }

    (StatusCode::OK, axum::Json(Value::Object(body))).into_response()
}
```

- [ ] **Step 2: Wire the module + route** (`crates/neutrino-http/src/lib.rs`)

Add `mod messages;` next to the other `mod` declarations (after `mod membership;`, ~line 34).

In `build_router()`, alongside the existing `/members` and `/state` GET routes, add:
```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/messages",
            axum::routing::get(messages::get_messages),
        )
```
(Use whatever `get(...)` import form the neighbouring routes use — if `get` is already imported, write `get(messages::get_messages)`.)

- [ ] **Step 3: Verify it compiles + clippy**

Run: `cargo build -p neutrino-http`
Expected: success.
Run: `cargo clippy -p neutrino-http --tests -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit** (skip in read-only-.git sandbox)

```bash
git add crates/neutrino-http/src/messages.rs crates/neutrino-http/src/lib.rs
git commit -m "feat(http): add GET /rooms/{id}/messages handler"
```

---

## Task 4: e2e tests

**Files:**
- Create: `crates/neutrino-http/tests/e2e_messages.rs`

- [ ] **Step 1: Create the test file (default-build cases)**

Mirrors the harness in `tests/e2e_sliding_sync.rs` (router over a file-backed SqliteStore; `tower::ServiceExt::oneshot`). The default config user (`alice`) is the room creator, hence joined.

```rust
//! End-to-end tests for `GET /_matrix/client/v3/rooms/{roomId}/messages`.
//! Drives the live router via `oneshot`. The default config user creates the
//! room and is therefore joined.
#![cfg(not(feature = "multi-user-shim"))]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
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
            builder.body(Body::from(serde_json::to_vec(b).unwrap())).unwrap()
        }
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
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
        let path = format!(
            "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/txn{i}"
        );
        let (s, _) = put(app, &path, &json!({"msgtype": "m.text", "body": format!("msg {i}")})).await;
        assert_eq!(s, StatusCode::OK, "send {i}");
    }
    room_id
}

fn chunk_len(body: &Value) -> usize {
    body["chunk"].as_array().expect("chunk array").len()
}

#[tokio::test]
async fn backward_no_from_returns_recent_newest_first() {
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 3).await;
    let (status, body) =
        get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10")).await;
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
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 5).await;
    let (s1, p1) =
        get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=2")).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(chunk_len(&p1), 2);
    let end = p1["end"].as_str().expect("end token when more remain").to_string();
    let (s2, p2) = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=2&from={end}"),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(chunk_len(&p2), 2);
    // Disjoint pages: no event id appears in both.
    let ids1: Vec<&str> = p1["chunk"].as_array().unwrap().iter().filter_map(|e| e["event_id"].as_str()).collect();
    let ids2: Vec<&str> = p2["chunk"].as_array().unwrap().iter().filter_map(|e| e["event_id"].as_str()).collect();
    assert!(ids1.iter().all(|id| !ids2.contains(id)), "pages disjoint");
}

#[tokio::test]
async fn forward_from_zero_oldest_first() {
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 3).await;
    let (status, body) =
        get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?dir=f&from=0&limit=100")).await;
    assert_eq!(status, StatusCode::OK);
    let bodies: Vec<&str> = body["chunk"].as_array().unwrap().iter()
        .filter_map(|e| e.pointer("/content/body").and_then(Value::as_str)).collect();
    assert_eq!(bodies, vec!["msg 0", "msg 1", "msg 2"], "oldest-first");
}

#[tokio::test]
async fn limit_is_capped_not_rejected() {
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 1).await;
    let (status, _) =
        get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=99999")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn filter_param_is_ignored() {
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 2).await;
    let plain = get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10")).await;
    let filtered = get(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b&limit=10&filter=%7B%22types%22%3A%5B%22m.room.message%22%5D%7D"),
    )
    .await;
    assert_eq!(plain.0, StatusCode::OK);
    assert_eq!(filtered.0, StatusCode::OK);
    assert_eq!(chunk_len(&plain.1), chunk_len(&filtered.1), "filter is a no-op");
}

#[tokio::test]
async fn unknown_room_is_forbidden() {
    let app = router(config()).await.expect("router");
    let (status, body) =
        get(&app, "/_matrix/client/v3/rooms/!nope:example.org/messages?dir=b").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

#[tokio::test]
async fn bad_params_are_rejected() {
    let app = router(config()).await.expect("router");
    let room = room_with_messages(&app, 1).await;
    for q in ["dir=x", "dir=b&from=notanumber", "dir=b&limit=abc"] {
        let (status, body) =
            get(&app, &format!("/_matrix/client/v3/rooms/{room}/messages?{q}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query: {q}");
        assert_eq!(body["errcode"], "M_INVALID_PARAM", "query: {q}");
    }
}
```

- [ ] **Step 2: Run the e2e tests**

Run: `cargo test -p neutrino-http --test e2e_messages`
Expected: all 7 tests pass.

> If `backward_no_from_returns_recent_newest_first` fails on the exact chunk
> composition (e.g. state events interleaved differently than expected), keep the
> message-body ordering assertion (`["msg 2","msg 1","msg 0"]`) — that is the
> invariant under test — and relax only the `chunk.len() >= 3` count if the seed
> emits a different number of state events. Do NOT weaken the ordering assertion.

- [ ] **Step 3: Commit** (skip in read-only-.git sandbox)

```bash
git add crates/neutrino-http/tests/e2e_messages.rs
git commit -m "test(http): e2e coverage for GET /rooms/{id}/messages"
```

---

## Task 5: multi-user-shim "not joined → 403" test

**Files:**
- Modify: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Add a not-joined test**

Read the top of `tests/e2e_multi_user.rs` to reuse its harness (token minting via `/register` or `/login`, the `post/put/get`-with-token helpers, room creation by one user). Then add a test where **user A** creates a room and **user B** (a distinct minted token, never joined) requests `/messages`:

```rust
#[tokio::test]
async fn messages_forbidden_for_non_member() {
    // Reuse this file's existing harness: provision two users, A creates a room.
    let (app, a_token, b_token) = /* existing two-user setup helper */;
    let room = /* A creates a room via createRoom with a_token */;
    // B is not joined → 403.
    let (status, body) = get_with_token(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b"),
        &b_token,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["errcode"], "M_FORBIDDEN");
    // Sanity: A (joined) succeeds.
    let (status_a, _) = get_with_token(
        &app,
        &format!("/_matrix/client/v3/rooms/{room}/messages?dir=b"),
        &a_token,
    )
    .await;
    assert_eq!(status_a, StatusCode::OK);
}
```

> The `/* ... */` fragments must be replaced with this file's actual helper calls
> (match the existing tests' setup verbatim — e.g. how they provision users, the
> `get_with_token` helper name, and how A creates the room). The asserted invariant
> is fixed: non-member → 403 `M_FORBIDDEN`, joined member → 200.

- [ ] **Step 2: Run it**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user messages_forbidden_for_non_member`
Expected: PASS.

- [ ] **Step 3: Commit** (skip in read-only-.git sandbox)

```bash
git add crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "test(http): non-member gets 403 from /messages (multi-user-shim)"
```

---

## Task 6: Final verification + bookkeeping

**Files:**
- Modify: `PLAN.md`, `LOG.md`

- [ ] **Step 1: Full check**

Run: `cargo fmt --check`
Run: `cargo clippy -p neutrino-store -p neutrino-store-sqlite -p neutrino-http --tests -- -D warnings`
Run: `cargo test -p neutrino-store-sqlite && cargo test -p neutrino-http`
Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user`
Expected: all clean / green.

- [ ] **Step 2: PLAN.md** — tick the `/messages` endpoint line; append to the decisions log:

```
2026-06-04: Implemented C2S GET /rooms/{roomId}/messages (new neutrino-http `messages` module). Mirrors Synapse where the mechanism exists (dir default f, from/to optional, limit default 10 cap 1000, chunk order b=newest-first/f=oldest-first, {chunk,start,end} with end omitted at boundary); requires the requesting user to be `join`-ed (else 403 M_FORBIDDEN, covering unknown rooms — matching the spec's only documented error). Deliberately NOT mirrored (no mechanism): federation backfill, history-visibility filtering, lazy-loaded `state`, and the `filter` param (accepted, ignored, documented on the handler). TRAIT CHANGE: `EventStore::room_messages` gained an exclusive `to: Option<PaginationToken>` bound (additive); sole prod caller (sliding-sync) passes None. Tokens are stream_pos decimals, interop with sync's prev_batch.
```

- [ ] **Step 3: LOG.md** — append at the bottom (oldest-first, no rationale):

```
2026-06-04: Added GET /_matrix/client/v3/rooms/{roomId}/messages (neutrino-http/src/messages.rs) + router wiring; join-gated (403 if not a member), params dir/from/to/limit, filter ignored, response {chunk,start,end}.
2026-06-04: EventStore::room_messages gained an exclusive `to` bound (neutrino-store + neutrino-store-sqlite); sliding-sync call sites pass None.
```

- [ ] **Step 4: Commit** (skip in read-only-.git sandbox)

```bash
git add PLAN.md LOG.md
git commit -m "docs: log /messages endpoint + room_messages 'to' bound"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** store `to` bound (T1), membership reuse (T2), handler + wiring (T3), e2e cases 1-6 & 8-10 (T4), case 7 not-joined-403 (T5), bookkeeping (T6). All spec sections mapped.
- **Type consistency:** `room_messages(room_id, from, to, dir, limit)` order is identical in the trait (T1.1), impl (T1.2), every call site (T1.3/1.4), and the handler (T3.1). `PaginationToken(u64)` / `Direction::{Forward,Backward}` used consistently.
- **Sandbox limits:** commit steps can't run (read-only `.git`); everything else (cargo build/clippy/test) runs here.
- **Test-quality guard:** two tests carry placeholder fragments tied to *existing* helpers the implementer must read and substitute (the storage seed helper in T1.5; the two-user harness in T5). Their asserted invariants are fixed and must not be weakened to pass.
