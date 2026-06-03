# GET Room State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the read side of room state — `GET /_matrix/client/v3/rooms/{roomId}/state`, `…/state/{eventType}/{stateKey}`, and `…/state/{eventType}` (empty key) — from the already-materialised current-state table.

**Architecture:** Pure `neutrino-http` change. State resolution already runs at write time (`RoomCore::apply_pdu` → `Effect::UpdateCurrentState`) and the resolved state is materialised in SQLite, so these are plain reads via the existing `StateStore::current_room_state` (full map) and `current_state_event` (single key). Full-event responses reuse the existing `From<&Event> for Raw<AnyTimelineEvent>` enrichment (adds `event_id` + `room_id`, preserves `state_key`); the default single-key response returns the event's `content`. No `neutrino-store` / `neutrino-common` / `neutrino-state` changes.

**Tech Stack:** axum 0.8, ruma (`Raw<AnyTimelineEvent>`), serde_json, SQLite store behind `StorageBackend`.

---

## File Structure

- **Modify** `crates/neutrino-http/src/lib.rs`:
  - 3 new handlers next to `members` / `put_state`: `get_state_all`, `get_state_event`, `get_state_event_empty_key`, plus a private `state_event_response` helper.
  - Merge `.get(..)` onto the three existing PUT state routes; add one new GET-only `…/state` route.
- **Modify** `crates/neutrino-http/tests/e2e_multi_user.rs`: 4 e2e tests (shim-on; has `register`/`createRoom`/`send` helpers).

No new files. No trait or storage changes.

---

## Key facts (verified)

- `StateStore::current_room_state(&RoomId) -> HashMap<(String,String), Event>` — already on the trait (`neutrino-store/src/lib.rs:230`), used by the actor bootstrap. Returns one event per `(type,state_key)`, already deduped.
- `StateStore::current_state_event(&RoomId, &str, &str) -> Option<Event>` — already on the trait (`:238`), used by `membership.rs`.
- `From<&Event> for Raw<AnyTimelineEvent>` (`neutrino-common/src/event_view.rs:87`) enriches with `event_id` **and** `room_id` (`IncludeRoomId::Always`) and keeps `state_key` → exactly the ClientEvent shape `?format=event` and the full list need.
- `Event.content` is a raw-JSON value with `.get() -> &str` (see `event_view.rs:121`) → the default single-key `content` response.
- Spec/Complement response shapes (from `apidoc_room_state_test.go`):
  - `GET …/state/{type}/{key}` default → the event **content** object (e.g. `{ "membership": "join" }`).
  - `…?format=event` → the **full event** (asserts `sender`, `room_id`, `content.membership`).
  - `GET …/state` → a **bare JSON array** of full state events (not wrapped in `{chunk}`).
  - Missing `(type,key)` → `404 M_NOT_FOUND`.
- **Auth:** no `AuthUser` extractor — consistent with `members` (also an unauthenticated state read); the embedded trusted C-S surface elides membership/history-visibility gating.

---

## Task 1: Single state-event read (`/state/{type}/{key}` and empty-key)

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs` (handlers + helper + route merges)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing tests** (append in `e2e_multi_user.rs`, before `member_membership`)

```rust
/// `GET /state/m.room.member/{user}` returns the event *content* by default
/// (top-level `membership`); `?format=event` returns the full event (with
/// `room_id`, `sender`, nested `content`).
#[tokio::test]
async fn get_state_member_content_and_format_event() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Default: content only.
    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.member/{alice_id}"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["membership"], json!("join"), "content-only shape: {body}");

    // ?format=event: full event.
    let (s, ev) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.member/{alice_id}?format=event"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{ev}");
    assert_eq!(ev["room_id"], json!(room_id), "{ev}");
    assert_eq!(ev["sender"], json!(alice_id), "{ev}");
    assert_eq!(ev["content"]["membership"], json!("join"), "{ev}");
}

/// A `(type, state_key)` with no current state event is `404 M_NOT_FOUND`.
#[tokio::test]
async fn get_state_unknown_key_is_404() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.name"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["errcode"], json!("M_NOT_FOUND"), "{body}");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user get_state_ -- --include-ignored`
Expected: FAIL — the GET routes 404/405 (handlers not wired yet).

- [ ] **Step 3: Add imports** (top of `crates/neutrino-http/src/lib.rs`, only if not already present)

```rust
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
```
(`axum::extract::Query`, `std::collections::HashMap`, `OwnedRoomId`, `StatusCode`, `Json`, `error_response`, `lock_app` are already in scope — confirm before adding.)

- [ ] **Step 4: Add the handlers + helper** (in `lib.rs`, immediately after `put_state_empty_key`)

```rust
/// `GET /rooms/{room}/state/{type}/{stateKey}` — the current state event.
/// Default response is the event `content`; `?format=event` returns the full
/// enriched event. No auth/visibility gating (embedded trusted surface; see
/// `members`).
async fn get_state_event(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type, state_key)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    state_event_response(&state.0, &room_id, &event_type, &state_key, &query.0).await
}

/// `GET /rooms/{room}/state/{type}` (and the trailing-slash form) — same as
/// [`get_state_event`] with the empty state key.
async fn get_state_event_empty_key(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type)): axum::extract::Path<(String, String)>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    state_event_response(&state.0, &room_id, &event_type, "", &query.0).await
}

async fn state_event_response(
    state: &AppState,
    room_id: &str,
    event_type: &str,
    state_key: &str,
    query: &std::collections::HashMap<String, String>,
) -> axum::response::Response {
    let store = lock_app(state).store.clone();
    let rid = match ruma::OwnedRoomId::try_from(room_id) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string()),
    };
    let event = match store.current_state_event(&rid, event_type, state_key).await {
        Ok(Some(e)) => e,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "M_NOT_FOUND", "Event not found."),
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    if query.get("format").map(String::as_str) == Some("event") {
        (StatusCode::OK, Json(Raw::<AnyTimelineEvent>::from(&event))).into_response()
    } else {
        match serde_json::from_str::<Value>(event.content.get()) {
            Ok(content) => (StatusCode::OK, Json(content)).into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            ),
        }
    }
}
```

- [ ] **Step 5: Merge GET onto the existing PUT state routes** (in `build_router`, `lib.rs`)

Change the three existing routes:
```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}/{state_key}",
            put(put_state).get(get_state_event),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}",
            put(put_state_empty_key).get(get_state_event_empty_key),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}/",
            put(put_state_empty_key).get(get_state_event_empty_key),
        )
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user get_state_member_content_and_format_event get_state_unknown_key_is_404`
Expected: PASS (2 tests).

---

## Task 2: Full room-state read (`/state`)

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs`
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing test**

```rust
/// `GET /state` returns a bare array of full state events; a freshly created
/// room contains at least `m.room.create` and the creator's `m.room.member`.
#[tokio::test]
async fn get_state_all_lists_current_state() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let events = body.as_array().expect("bare array of state events");
    assert!(
        events
            .iter()
            .any(|e| e["type"] == json!("m.room.create") && e["room_id"] == json!(room_id)),
        "state contains m.room.create with room_id: {body}"
    );
    assert!(
        events.iter().any(|e| e["type"] == json!("m.room.member")
            && e["state_key"] == json!(alice_id)
            && e["content"]["membership"] == json!("join")),
        "state contains the creator's join member event: {body}"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user get_state_all_lists_current_state`
Expected: FAIL — `/state` route not wired (404).

- [ ] **Step 3: Add the handler** (in `lib.rs`, after `state_event_response`)

```rust
/// `GET /rooms/{room}/state` — every current state event as a bare array of
/// full (enriched) events.
async fn get_state_all(
    state: State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let rid = match ruma::OwnedRoomId::try_from(room_id.as_str()) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string()),
    };
    let map = match store.current_room_state(&rid).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let events: Vec<Raw<AnyTimelineEvent>> =
        map.values().map(Raw::<AnyTimelineEvent>::from).collect();
    (StatusCode::OK, Json(events)).into_response()
}
```

- [ ] **Step 4: Add the route** (in `build_router`, immediately before the `…/state/{type}/{state_key}` route)

```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state",
            get(get_state_all),
        )
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user get_state_all_lists_current_state`
Expected: PASS.

---

## Task 3: Verify, review, finalise

- [ ] **Step 1: Full gates, both feature states**

Run:
```bash
cargo fmt -p neutrino-http
cargo clippy -p neutrino-http --tests -- -D warnings
cargo clippy -p neutrino-http --features multi-user-shim --tests -- -D warnings
cargo test -p neutrino-http
cargo test -p neutrino-http --features multi-user-shim
```
Expected: all green; the 4 new e2e tests pass.

- [ ] **Step 2: Diff review (CLAUDE.md "Reviewing the git diff")**

Spawn the five fresh-context subagents in parallel (code / security / spec-conformance / test-quality / architecture) over `git diff HEAD`. Synthesize, drop false positives with reasons, present the merged list. Apply no fix without asking.

- [ ] **Step 3: Docs + bookkeeping**

- `complement/VIABLE-TESTS.md`: move the GET-state-unlocked `TestRoomState` / `apidoc_room_create` name-topic / `apidoc_room_members` read-back subtests out of the "blocked on GET room state" section.
- `PLAN.md`: tick the endpoint and add a decisions-log entry.
- `LOG.md`: 2-line summary (append).

- [ ] **Step 4: Allowlist (after a CI Complement run confirms green)**

Candidate adds (verify empirically first, per the established convention — no local docker):
`TestRoomState/Parallel/{GET …state/m.room.member/:user_id fetches my membership, …?format=event…, …m.room.power_levels…, …m.room.name gets name, POST …m.room.name sets name, PUT …m.room.topic sets topic, …m.room.topic gets topic, GET …/state fetches entire room state}`; the `apidoc_room_create` name/topic read-back subtests; the `apidoc_room_members` invite/ban/leave/reinvite subtests. **Not** here: `power_levels` "can set" (needs `GET /event`); `joined_members`/`publicRooms`/`directory`/`joined_rooms` (separate endpoints).

---

## Self-review notes

- **Spec coverage:** all three GET variants (full / single / empty-key + trailing slash) + `format` param + 404 are covered by Tasks 1–2 and their tests.
- **No new types/methods:** every symbol used (`current_room_state`, `current_state_event`, `Raw<AnyTimelineEvent>::from`, `Event.content`) exists today.
- **Design audit:** no new struct/field; handlers are thin and delegate to one shared `state_event_response`; the full-list and `format=event` paths both reuse the single existing enrichment conversion (no bespoke serialisation). Each handler is well under 40 lines.
