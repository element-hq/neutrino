# Client-Server Membership Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the six CSAPI membership-change POST endpoints (`/join`, `/leave`, `/invite`, `/kick`, `/ban`, `/unban`) and make `/createRoom` honour its `invite` list, so cross-user membership flows (and the Complement tests that exercise them) work.

**Architecture:** Each endpoint is a thin axum handler that emits one `m.room.member` state event through the existing per-room actor (`RoomRegistry::send_event`). Authorisation (v12 rule 5), state resolution, DAG linkage, and persistence all happen inside that actor path unchanged — these handlers only decide `(target, membership)` and shape the HTTP response. The handlers live in a new `crates/neutrino-http/src/membership.rs` submodule. `/createRoom`'s batch builder gains a final step that appends a creator-authored invite event per listed user.

**Tech Stack:** Rust, axum 0.8, ruma, serde_json, tokio. Tests use `tower::ServiceExt::oneshot` against an in-process router with the `multi-user-shim` feature (the only build with multiple distinct users).

**Spec:** `docs/superpowers/specs/2026-06-02-membership-endpoints-design.md`

---

## Pre-flight notes for the executor

- **Privacy:** `membership.rs` is a child module of the crate root (`lib.rs`). In Rust a descendant module can use a parent's *private* items, so `crate::error_response`, `crate::lock_app`, and `crate::room_actor_response` are all reachable without any `pub`/`pub(crate)` change (this is exactly how `legacy_sync` already imports `crate::{AppState, error_response, lock_app}`). **No visibility edits to existing items are required.** The new handlers, however, must be `pub(crate)` because the crate root (`build_router`) is their *parent* and cannot see a child's private items.
- **Working tree:** the tree may still carry the earlier multi-user-shim review fixes (touching `lib.rs`, `tests/e2e_multi_user.rs`, `PLAN.md`, `LOG.md`). When committing, stage only the files each task names — do **not** `git add -A`. Confirm commit strategy with the user before the first commit if those earlier changes are still uncommitted.
- **No body on POST:** all six handlers take `body: Option<Json<Value>>` so a bodyless POST (valid per spec for `/join` and `/leave`) does not 415/400. axum 0.8 implements `OptionalFromRequest` for `Json<T>`, so `Option<Json<Value>>` yields `None` when there is no JSON body and only errors on a present-but-malformed body. If for any reason it fails to compile, fall back to `body: Json<Value>` on `/invite`/`/kick`/`/ban`/`/unban` (which always carry a body) and keep `Option<Json<Value>>` on `/join`/`/leave`.
- **Room id in the URL path:** room ids like `!abc:example.org` contain `:` and `!`, both legal in a URL path segment and matched by axum's `{room_id}` capture (no `/` in a room id). The existing `/send/{type}/{txn}` e2e tests already exercise this, so no percent-encoding is needed in test URLs.

---

## Task 1: Scaffold `membership.rs` with `/join` and `/leave`

**Files:**
- Create: `crates/neutrino-http/src/membership.rs`
- Modify: `crates/neutrino-http/src/lib.rs` (add `mod membership;`; add two routes)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
/// Joining a public room needs no prior invite: the join is authorised by the
/// `public` join rule, and the room then appears in the joiner's sync.
#[tokio::test]
async fn join_public_room_without_invite_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

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
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["room_id"], room_id, "join echoes the room id");

    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    let rooms = bob_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(
        rooms.contains_key(&room_id),
        "bob should see the joined room: {bob_sync}"
    );
}

/// A joined user leaving themselves moves their membership to `leave`.
#[tokio::test]
async fn self_leave_sets_membership_to_leave() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    // GET /members reflects bob's membership as `leave`.
    let (s, members) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/members"),
        None,
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{members}");
    let membership = members["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["state_key"] == json!(bob_id))
        .and_then(|ev| ev["content"]["membership"].as_str());
    assert_eq!(membership, Some("leave"), "{members}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user join_public_room_without_invite_succeeds self_leave_sets_membership_to_leave`
Expected: FAIL — the `/join` and `/leave` routes don't exist yet, so the router's fallback returns 404 and `assert_eq!(s, StatusCode::OK)` fails.

- [ ] **Step 3: Create the `membership.rs` module**

Create `crates/neutrino-http/src/membership.rs` with exactly this content:

```rust
//! CSAPI membership-change endpoints (testing scope). Each handler emits one
//! `m.room.member` state event through the room actor; authorisation (v12
//! rule 5), state resolution, and persistence all happen inside
//! [`crate::room_actor::RoomRegistry::send_event`] unchanged. See
//! `docs/superpowers/specs/2026-06-02-membership-endpoints-design.md`.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use ruma::{OwnedRoomId, OwnedUserId};
use serde_json::{Value, json};

use crate::{AppState, AuthUser, error_response, lock_app, room_actor_response};

/// Emit one `m.room.member` event through the room actor. `target` is the
/// state_key (the user whose membership changes); `membership` is the
/// resulting membership string; `reason`, when present, is copied into
/// content. Returns `Ok(())` on accept, or a ready HTTP error response
/// (400 for a bad room id, otherwise the actor's standard mapping).
async fn change_membership(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &str,
    target: &OwnedUserId,
    membership: &str,
    reason: Option<&str>,
) -> Result<(), axum::response::Response> {
    let registry = lock_app(state).room_registry.clone();
    let room: OwnedRoomId = match room_id.parse() {
        Ok(r) => r,
        Err(e) => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                &e.to_string(),
            ));
        }
    };
    let mut content = json!({ "membership": membership });
    if let Some(r) = reason {
        content["reason"] = json!(r);
    }
    registry
        .send_event(
            &room,
            sender,
            "m.room.member".to_owned(),
            Some(target.to_string()),
            content,
        )
        .await
        .map(|_| ())
        .map_err(room_actor_response)
}

/// Lift an optional `reason` string from the request body.
fn body_reason(body: Option<&Value>) -> Option<String> {
    body?
        .pointer("/reason")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// `POST /rooms/{roomId}/join` — the caller joins the room. Returns the room
/// id per spec.
pub(crate) async fn join(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    match change_membership(
        &state.0,
        sender.clone(),
        &room_id,
        &sender,
        "join",
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "room_id": room_id }))).into_response(),
        Err(resp) => resp,
    }
}

/// `POST /rooms/{roomId}/leave` — the caller leaves the room.
pub(crate) async fn leave(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    let body = body.as_ref().map(|j| &j.0);
    let reason = body_reason(body);
    match change_membership(
        &state.0,
        sender.clone(),
        &room_id,
        &sender,
        "leave",
        reason.as_deref(),
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => resp,
    }
}
```

- [ ] **Step 4: Declare the module and wire the two routes in `lib.rs`**

In `crates/neutrino-http/src/lib.rs`, add the module declaration to the existing `mod` block (it is **not** feature-gated — the handlers compile and route in both feature states):

```rust
mod federation;
mod legacy_sync;
mod membership;
mod room_actor;
mod sliding_sync;
```

Then, in `build_router`, add the join and leave routes immediately after the `/_matrix/client/v3/rooms/{room_id}/state/{type}` (`put_state_empty_key`) route block:

```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/join",
            post(membership::join),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/leave",
            post(membership::leave),
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user join_public_room_without_invite_succeeds self_leave_sets_membership_to_leave`
Expected: PASS (2 passed).

- [ ] **Step 6: Lint and format**

Run: `cargo fmt -p neutrino-http && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/neutrino-http/src/membership.rs crates/neutrino-http/src/lib.rs crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "feat(http): add /join and /leave membership endpoints"
```

---

## Task 2: `/invite` (+ invite-only join gating)

**Files:**
- Modify: `crates/neutrino-http/src/membership.rs` (add `body_target`, `targeted`, `invite`)
- Modify: `crates/neutrino-http/src/lib.rs` (add the `/invite` route)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
/// In an invite-only room, a user who was invited can see the room as an
/// invite, then join; after joining the room shows up and `GET /members`
/// reports their membership as `join`.
#[tokio::test]
async fn invite_then_join_makes_room_visible() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Alice invites bob.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    // Bob's sync surfaces the room (as an invite).
    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    assert!(
        bob_sync["rooms"]
            .as_object()
            .map(|r| r.contains_key(&room_id))
            .unwrap_or(false),
        "bob should see the invited room: {bob_sync}"
    );

    // Bob joins.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let membership = member_membership(&app, &room_id, &bob_id).await;
    assert_eq!(membership.as_deref(), Some("join"));
}

/// Joining an invite-only room with no prior invite is rejected.
#[tokio::test]
async fn join_invite_only_without_invite_is_403() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}
```

Also append this shared test helper (used by this and later tasks) to the same file:

```rust
/// Read the current `m.room.member` membership of `user_id` in `room_id` via
/// the unauthenticated `GET /members` endpoint.
async fn member_membership(app: &axum::Router, room_id: &str, user_id: &str) -> Option<String> {
    let (_s, members) = send(
        app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/members"),
        None,
        &json!({}),
    )
    .await;
    members["chunk"].as_array()?.iter().find_map(|ev| {
        if ev["state_key"] == json!(user_id) {
            ev["content"]["membership"].as_str().map(str::to_owned)
        } else {
            None
        }
    })
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user invite_then_join_makes_room_visible join_invite_only_without_invite_is_403`
Expected: FAIL — `invite_then_join_makes_room_visible` fails because `/invite` 404s. (`join_invite_only_without_invite_is_403` may already pass, since `/join` exists and the auth rules reject the join — that is fine.)

- [ ] **Step 3: Add `body_target`, `targeted`, and the `invite` handler**

Add to `crates/neutrino-http/src/membership.rs` (after `body_reason`):

```rust
/// Parse the required `user_id` target from the request body.
fn body_target(body: Option<&Value>) -> Result<OwnedUserId, axum::response::Response> {
    let raw = match body.and_then(|b| b.pointer("/user_id")).and_then(Value::as_str) {
        Some(s) => s,
        None => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_MISSING_PARAM",
                "Missing required parameter: user_id",
            ));
        }
    };
    match OwnedUserId::try_from(raw) {
        Ok(u) => Ok(u),
        Err(e) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "M_INVALID_PARAM",
            &e.to_string(),
        )),
    }
}

/// Shared body for the target-from-body endpoints (`invite`/`kick`/`ban`/
/// `unban`): resolve the target, optionally lift `reason`, emit the member
/// event, and return `{}` on success.
async fn targeted(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &str,
    body: Option<&Value>,
    membership: &str,
    with_reason: bool,
) -> axum::response::Response {
    let target = match body_target(body) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let reason = if with_reason { body_reason(body) } else { None };
    match change_membership(state, sender, room_id, &target, membership, reason.as_deref()).await {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(resp) => resp,
    }
}

/// `POST /rooms/{roomId}/invite` — invite `body.user_id` to the room.
pub(crate) async fn invite(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "invite",
        true,
    )
    .await
}
```

- [ ] **Step 4: Wire the `/invite` route in `lib.rs`**

Add after the `/leave` route added in Task 1:

```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/invite",
            post(membership::invite),
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user invite_then_join_makes_room_visible join_invite_only_without_invite_is_403`
Expected: PASS (2 passed).

- [ ] **Step 6: Lint and format**

Run: `cargo fmt -p neutrino-http && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/neutrino-http/src/membership.rs crates/neutrino-http/src/lib.rs crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "feat(http): add /invite membership endpoint"
```

---

## Task 3: `/kick`

**Files:**
- Modify: `crates/neutrino-http/src/membership.rs` (add `kick`)
- Modify: `crates/neutrino-http/src/lib.rs` (add the `/kick` route)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
/// A room creator (power 50 ≥ default kick level) can kick a joined member;
/// the target's membership becomes `leave`.
#[tokio::test]
async fn kick_sets_target_membership_to_leave() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(member_membership(&app, &room_id, &bob_id).await.as_deref(), Some("join"));

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/kick"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id, "reason": "spam" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(member_membership(&app, &room_id, &bob_id).await.as_deref(), Some("leave"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user kick_sets_target_membership_to_leave`
Expected: FAIL — `/kick` 404s, so the kick `assert_eq!(s, StatusCode::OK)` fails.

- [ ] **Step 3: Add the `kick` handler**

Add to `crates/neutrino-http/src/membership.rs` (after `invite`):

```rust
/// `POST /rooms/{roomId}/kick` — force `body.user_id` to `leave`.
pub(crate) async fn kick(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "leave",
        true,
    )
    .await
}
```

- [ ] **Step 4: Wire the `/kick` route in `lib.rs`**

Add after the `/invite` route:

```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/kick",
            post(membership::kick),
        )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user kick_sets_target_membership_to_leave`
Expected: PASS.

- [ ] **Step 6: Lint and format**

Run: `cargo fmt -p neutrino-http && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/neutrino-http/src/membership.rs crates/neutrino-http/src/lib.rs crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "feat(http): add /kick membership endpoint"
```

---

## Task 4: `/ban` and `/unban`

**Files:**
- Modify: `crates/neutrino-http/src/membership.rs` (add `ban`, `unban`)
- Modify: `crates/neutrino-http/src/lib.rs` (add the `/ban` and `/unban` routes)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
/// Banning a member sets `ban` and blocks rejoin; unbanning returns them to
/// `leave` and lets them join again (public room).
#[tokio::test]
async fn ban_blocks_rejoin_until_unban() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Bob joins, then alice bans him.
    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/ban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(member_membership(&app, &room_id, &bob_id).await.as_deref(), Some("ban"));

    // A banned user cannot rejoin.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");

    // Alice unbans bob → membership back to leave.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/unban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(member_membership(&app, &room_id, &bob_id).await.as_deref(), Some("leave"));

    // Bob can join the public room again.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(member_membership(&app, &room_id, &bob_id).await.as_deref(), Some("join"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user ban_blocks_rejoin_until_unban`
Expected: FAIL — `/ban` 404s.

- [ ] **Step 3: Add the `ban` and `unban` handlers**

Add to `crates/neutrino-http/src/membership.rs` (after `kick`):

```rust
/// `POST /rooms/{roomId}/ban` — ban `body.user_id` from the room.
pub(crate) async fn ban(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "ban",
        true,
    )
    .await
}

/// `POST /rooms/{roomId}/unban` — lift a ban on `body.user_id` (membership
/// returns to `leave`). The unban-vs-kick auth arm (rule 5.5.3 vs 5.5.4) is
/// selected by `RoomCore` from the target's current membership, so this emits
/// the same `leave` membership as `kick`.
pub(crate) async fn unban(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    Path(room_id): Path<String>,
    body: Option<Json<Value>>,
) -> axum::response::Response {
    targeted(
        &state.0,
        sender,
        &room_id,
        body.as_ref().map(|j| &j.0),
        "leave",
        false,
    )
    .await
}
```

- [ ] **Step 4: Wire the `/ban` and `/unban` routes in `lib.rs`**

Add after the `/kick` route:

```rust
        .route(
            "/_matrix/client/v3/rooms/{room_id}/ban",
            post(membership::ban),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/unban",
            post(membership::unban),
        )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user ban_blocks_rejoin_until_unban`
Expected: PASS.

- [ ] **Step 6: Lint and format**

Run: `cargo fmt -p neutrino-http && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/neutrino-http/src/membership.rs crates/neutrino-http/src/lib.rs crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "feat(http): add /ban and /unban membership endpoints"
```

---

## Task 5: Honour the `invite` list in `/createRoom`

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs:806-827` (extend `build_initial_events`)
- Test: `crates/neutrino-http/tests/e2e_multi_user.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
/// `/createRoom` with an `invite` list emits a creator-authored invite member
/// event per listed user, so the invitee sees the room in sync without any
/// explicit `/invite` call and `GET /members` reports them as `invite`.
#[tokio::test]
async fn createroom_invite_list_invites_listed_users() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat", "invite": [bob_id] }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("invite"),
        "bob should be invited by createRoom"
    );

    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    assert!(
        bob_sync["rooms"]
            .as_object()
            .map(|r| r.contains_key(&room_id))
            .unwrap_or(false),
        "bob should see the invited room in sync: {bob_sync}"
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user createroom_invite_list_invites_listed_users`
Expected: FAIL — createRoom ignores `invite`, so bob has no member event (`member_membership` returns `None`, asserted `Some("invite")`).

- [ ] **Step 3: Extend `build_initial_events`**

In `crates/neutrino-http/src/lib.rs`, in `build_initial_events`, insert the invite-list loop after the optional name/topic block and before `Ok((create, initial))` (currently around line 827). The `add` closure already takes `(event_type, state_key, content)`:

```rust
    // Honour the request's `invite` list (the membership follow-up to the
    // multi-user shim): emit one invite member event per well-formed, non-self
    // target, authored by the creator — who is joined with implicit MAX power,
    // so rule 5.4 accepts it. Malformed entries are skipped rather than failing
    // room creation (test server, best-effort). `is_direct` is propagated onto
    // the invite content when the request sets it.
    if let Some(invitees) = body.pointer("/invite").and_then(Value::as_array) {
        let is_direct = body.pointer("/is_direct").and_then(Value::as_bool) == Some(true);
        for entry in invitees {
            let Some(target) = entry.as_str() else { continue };
            if target == sender.as_str() || OwnedUserId::try_from(target).is_err() {
                continue;
            }
            let mut content = json!({ "membership": "invite" });
            if is_direct {
                content["is_direct"] = json!(true);
            }
            add("m.room.member", target, content)?;
        }
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user createroom_invite_list_invites_listed_users`
Expected: PASS.

- [ ] **Step 5: Lint and format**

Run: `cargo fmt -p neutrino-http && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/neutrino-http/src/lib.rs crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "feat(http): honour the invite list in createRoom"
```

---

## Task 6: Full verification and documentation

**Files:**
- Modify: `PLAN.md` (status + decision entry)
- Modify: `LOG.md` (2-line summary appended at the bottom)

- [ ] **Step 1: Run the full membership e2e suite (feature on)**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user`
Expected: PASS — all pre-existing multi-user tests plus the 8 new membership tests
(`join_public_room_without_invite_succeeds`, `self_leave_sets_membership_to_leave`,
`invite_then_join_makes_room_visible`, `join_invite_only_without_invite_is_403`,
`kick_sets_target_membership_to_leave`, `ban_blocks_rejoin_until_unban`,
`createroom_invite_list_invites_listed_users`).

- [ ] **Step 2: Run the feature-OFF regression suite**

Run: `cargo test -p neutrino-http`
Expected: PASS — the new handlers compile and route with the shim off (they take `AuthUser`, which resolves the single configured user); `e2e_multi_user.rs` compiles to 0 tests with the feature off.

- [ ] **Step 3: Clippy in both feature states**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: no warnings in either state.

- [ ] **Step 4: Workspace build/test (final check)**

Run: `cargo test --workspace --no-fail-fast`
Expected: PASS (no regressions in other crates).

- [ ] **Step 5: Update `PLAN.md`**

Under `## status`, after the multi-user-shim entry (the `Testing-only multi-user identity shim` bullet, ~line 65-66), add:

```markdown
- [x] Client-Server membership endpoints (`/join`, `/leave`, `/invite`, `/kick`, `/ban`, `/unban`)
    - `crates/neutrino-http/src/membership.rs`: six thin handlers emit `m.room.member` state events through the existing `RoomRegistry::send_event` actor path — auth (v12 rule 5), state-res, and persistence are unchanged. `/join`/`/leave` target the caller; `/invite`/`/kick`/`/ban`/`/unban` target `body.user_id`; `/kick` and `/unban` both emit `membership: leave` (RoomCore picks the 5.5.4-kick vs 5.5.3-unban arm from current membership). `/createRoom` now emits a creator-authored invite per `invite[]` entry. 8 e2e tests in `tests/e2e_multi_user.rs` (feature `multi-user-shim`). Not in scope: `/forget`, join-by-alias, displayname/avatar carry-over, remote-user delivery.
```

Append to the `## decisions log` (top of the dated entries):

```markdown
2026-06-02: Client-Server membership endpoints land as thin HTTP handlers in a new `neutrino-http/src/membership.rs`, reusing the per-room actor (`RoomRegistry::send_event`) — no change to the actor, `RoomCore`, the auth rules, or the storage trait. Each endpoint emits one `m.room.member` event; the actor's `apply_pdu` + rule 5 do all authorisation, so the handlers only choose `(target, membership)` and shape the response (`/join` → `{room_id}`, the rest → `{}`). `/kick` and `/unban` both emit `membership: leave`; the unban-vs-kick auth arm is resolved by `RoomCore` from the target's current membership, so the endpoints need no extra signalling. Bodies are `Option<Json<Value>>` so a bodyless `/join`/`/leave` POST doesn't 415. `room_actor_response` / `error_response` / `lock_app` are reused as crate-root privates (descendant modules can see them — no visibility change). `/createRoom` now honours its `invite` list: one creator-authored invite event per well-formed non-self target appended to the initial batch (creator has implicit MAX power ⇒ rule 5.4 passes), `is_direct` propagated; malformed entries skipped rather than failing creation. Errors reuse the existing mapping (404 unknown room / 403 auth reject / 400 bad room-id-or-user-id / 500 fault); `/join` is idempotent. Synapse/Complement membership test ports deferred to a follow-up PR (per request). fmt + clippy -D warnings clean in both feature states; full e2e suite green.
```

- [ ] **Step 6: Append to `LOG.md` (bottom, oldest-first; no rationale)**

```markdown
2026-06-02: Added CSAPI membership endpoints (/join, /leave, /invite, /kick, /ban, /unban) in neutrino-http/src/membership.rs, routed through the per-room actor.
2026-06-02: createRoom now emits an invite m.room.member event per entry in the request's invite[] list. 8 new e2e tests in e2e_multi_user.rs.
```

- [ ] **Step 7: Commit**

```bash
git add PLAN.md LOG.md
git commit -m "docs: record membership endpoints in PLAN.md and LOG.md"
```

---

## Self-review (completed during planning)

- **Spec coverage:** all six endpoints (Tasks 1–4), createRoom invite list (Task 5), error mapping (reused `room_actor_response`, asserted in the 403 tests), `/join` returning `{room_id}` (Task 1 test), out-of-scope items left untouched (`/forget`, join-by-alias, GET /members auth). Testing matches the spec's e2e list; Synapse/Complement port explicitly deferred. ✓
- **Placeholders:** none — every step carries full code or an exact command. ✓
- **Type/name consistency:** `change_membership` / `body_target` / `body_reason` / `targeted` signatures are stable across tasks; `member_membership` test helper defined once (Task 2) and referenced by name afterwards; handler names match the route wirings. ✓
