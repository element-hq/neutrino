# Multi-user Identity Shim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a testing-only multi-user identity shim — behind a `multi-user-shim` cargo feature — so Complement can register distinct local users, authenticate each request with a Bearer token, and have events/sync attributed to the right user. The single-user production path stays byte-identical when the feature is off.

**Architecture:** A `user_tokens: HashMap<String, OwnedUserId>` lives in `App` (feature-gated). `/register` and `/login` mint a random `syt_<…>` token per user and insert it. A new `AuthUser(OwnedUserId)` axum extractor resolves `Authorization: Bearer <token>` to the calling user (401 on missing/unknown when the feature is on; returns the configured default user when off). The identity-bearing handlers (createRoom, send/state, sliding sync, legacy sync) take `AuthUser(user_id)` instead of reading `config.user_id()`.

**Tech Stack:** Rust, axum 0.8 (`FromRequestParts` extractor), ruma (`OwnedUserId`), `rand` 0.9 (token generation — already a workspace dep), tower (`oneshot` in tests). Cargo features for gating.

**Spec:** `docs/superpowers/specs/2026-06-01-multi-user-shim-design.md`

**Conventions for every task:** after edits run `cargo fmt`, and for the crate under test `cargo clippy -p neutrino-http --tests -- -D warnings` and `cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings` (clippy must be clean in BOTH feature states). The final task does the workspace-wide check. Never use `.unwrap()`/`.expect()` in handler code (tests may).

---

## File Structure

- `crates/neutrino-http/Cargo.toml` — add `[features] multi-user-shim = []`.
- `crates/neutrino-main/Cargo.toml`, `crates/neutrino/Cargo.toml` — pass-through feature.
- `crates/neutrino-http/src/multi_user.rs` — **new, entirely feature-gated.** Token-store type alias, `mint_token`, the `AuthUser` extractor's resolution logic.
- `crates/neutrino-http/src/lib.rs` — `App.user_tokens` field; declare `AuthUser` (always) with a cfg-split body; thread `AuthUser` into `create_room`, `send_via_actor` callers, `sync`; de-hardcode `/profile` + `/account_data` routes; feature-split `post_register` / `post_login`.
- `crates/neutrino-http/src/legacy_sync/mod.rs` — `AuthUser` in `handle`.
- `crates/neutrino-http/tests/e2e_multi_user.rs` — **new, feature-gated** e2e tests.
- `docker/complement/Dockerfile` — build with `--features multi-user-shim`.
- `.github/workflows/ci.yml` — a feature-on build/clippy/test step.
- `PLAN.md` / `LOG.md` — status checkbox + decision + summary.

A NOTE ON THE EXTRACTOR LOCATION: `AuthUser` (the public struct) is declared in `lib.rs` so handler signatures are stable in both feature states; its `FromRequestParts` impl body delegates to `multi_user::resolve` (feature on) or returns the config default (feature off). This keeps the `#[cfg]` surface to one delegating impl plus one gated module.

---

## Task 1: Add the cargo feature and pass-throughs

**Files:**
- Modify: `crates/neutrino-http/Cargo.toml`
- Modify: `crates/neutrino-main/Cargo.toml`
- Modify: `crates/neutrino/Cargo.toml`

- [ ] **Step 1: Add the feature to `neutrino-http`**

In `crates/neutrino-http/Cargo.toml`, after the `[package]` block (before `[dependencies]`), add:

```toml
[features]
# Testing-only multi-user identity shim. Enables per-request Bearer-token →
# user resolution and per-user /register + /login. OFF in the Android/FFI
# build; ON for the local dev binary and the Complement image.
multi-user-shim = []
```

- [ ] **Step 2: Add a pass-through in `neutrino-main`**

In `crates/neutrino-main/Cargo.toml`, add (create the section if absent):

```toml
[features]
multi-user-shim = ["neutrino-http/multi-user-shim"]
```

Confirm `neutrino-main` depends on `neutrino-http` (it does — it wires the router). No change needed to the dependency line.

- [ ] **Step 3: Add a pass-through in the `neutrino` binary crate**

In `crates/neutrino/Cargo.toml`, add:

```toml
[features]
multi-user-shim = ["neutrino-main/multi-user-shim"]
```

- [ ] **Step 4: Verify both feature states compile**

Run: `cargo build -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: both succeed (the feature is inert so far — no `#[cfg]` consumers yet).

- [ ] **Step 5: Commit**

```bash
git add crates/neutrino-http/Cargo.toml crates/neutrino-main/Cargo.toml crates/neutrino/Cargo.toml
git commit -m "feat(http): add multi-user-shim cargo feature + pass-throughs"
```

> If git reports `.git` is read-only in this environment, skip the commit step throughout and note it; all other steps still apply.

---

## Task 2: Token store field + `mint_token` helper (feature-gated module)

**Files:**
- Create: `crates/neutrino-http/src/multi_user.rs`
- Modify: `crates/neutrino-http/src/lib.rs` (declare module, add `App.user_tokens`, populate in `from_store`)

This task lays the data structure and a unit-tested token generator. The extractor and handler wiring come in later tasks.

- [ ] **Step 1: Write the failing unit test for `mint_token`**

Create `crates/neutrino-http/src/multi_user.rs` with ONLY a test module first:

```rust
//! Testing-only multi-user identity shim. Compiled only under the
//! `multi-user-shim` cargo feature. Holds the in-memory access-token →
//! user map, a token minter, and the `AuthUser` extractor's resolution
//! logic. None of this ships in the single-user (Android/FFI) build.

#![cfg(feature = "multi-user-shim")]

use std::collections::HashMap;

use ruma::OwnedUserId;

/// In-memory map of opaque access token → the user it authenticates.
/// Ephemeral: lives in `App`, lost on restart (acceptable for tests).
pub(crate) type UserTokens = HashMap<String, OwnedUserId>;

/// Mint a fresh, unique access token of the Synapse-ish `syt_<random>`
/// shape. 32 random alphanumerics give ample collision resistance for a
/// test server.
pub(crate) fn mint_token() -> String {
    use rand::Rng;
    use rand::distr::Alphanumeric;
    let suffix: String = rand::rng()
        .sample_iter(Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    format!("syt_{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_token_has_prefix_and_is_unique() {
        let a = mint_token();
        let b = mint_token();
        assert!(a.starts_with("syt_"), "got {a}");
        assert!(a.len() > 4);
        assert_ne!(a, b, "two mints must differ");
    }
}
```

- [ ] **Step 2: Declare the module in `lib.rs`**

In `crates/neutrino-http/src/lib.rs`, with the other `mod` declarations (after `mod federation;` etc., around line 30-33), add:

```rust
#[cfg(feature = "multi-user-shim")]
mod multi_user;
```

- [ ] **Step 3: Run the test (feature on) to verify it passes**

Run: `cargo test -p neutrino-http --features multi-user-shim multi_user::tests::mint_token -- --nocapture`
Expected: PASS. (If `rand::distr` / `rand::rng` don't resolve, the installed `rand` is <0.9 — adjust to `rand::thread_rng()` + `rand::distributions::Alphanumeric`. Lockfile shows 0.9.4, so the above is correct.)

- [ ] **Step 4: Add the `user_tokens` field to `App`**

In `crates/neutrino-http/src/lib.rs`, inside `struct App` (around line 38-50), add as the last field:

```rust
    /// Testing-only access-token → user map (multi-user shim). See
    /// `multi_user`. Absent from the production single-user build.
    #[cfg(feature = "multi-user-shim")]
    user_tokens: std::sync::Mutex<multi_user::UserTokens>,
```

Rationale for the inner `Mutex`: `App` is already behind `Mutex<App>`, but `/register` and `/login` need to insert while holding only a short lock, and the `AuthUser` extractor reads it; wrapping the map in its own `Mutex` lets the extractor lock just the map (see Task 3) without taking the whole-`App` lock semantics into the extractor. Use `std::sync::Mutex` to match the existing `lock_app` poison-tolerant style.

- [ ] **Step 5: Initialise the field in `from_store`**

In `from_store` (around line 93-105), add the field to the `App { … }` literal:

```rust
            #[cfg(feature = "multi-user-shim")]
            user_tokens: std::sync::Mutex::new(multi_user::UserTokens::new()),
```

- [ ] **Step 6: Verify both feature states compile**

Run: `cargo build -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: both succeed. Feature-off must NOT warn about an unused field (there is none — the field itself is `#[cfg]`-gated).

- [ ] **Step 7: clippy both states + commit**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean.

```bash
git add crates/neutrino-http/src/multi_user.rs crates/neutrino-http/src/lib.rs
git commit -m "feat(http): add feature-gated user_tokens store + token minter"
```

---

## Task 3: `AuthUser` extractor

**Files:**
- Modify: `crates/neutrino-http/src/multi_user.rs` (add `resolve` + a `TokenError`)
- Modify: `crates/neutrino-http/src/lib.rs` (declare `AuthUser` + `FromRequestParts` impl, cfg-split)

`AuthUser` is the public, always-present struct. Its impl body delegates: feature-on → `multi_user::resolve(headers, &app.user_tokens)`; feature-off → config default.

- [ ] **Step 1: Add the resolution helper to `multi_user.rs`**

Append to `crates/neutrino-http/src/multi_user.rs` (before the `#[cfg(test)]` module):

```rust
use axum::http::HeaderMap;

/// Why a token failed to resolve. Maps to a 401 errcode at the HTTP edge.
pub(crate) enum TokenError {
    /// No `Authorization` header at all.
    Missing,
    /// Header present but malformed, or token not in the map.
    Unknown,
}

/// Resolve a request's `Authorization: Bearer <token>` against the token
/// map. `Ok(user)` on a hit; `Err` otherwise (the caller maps to 401).
pub(crate) fn resolve(
    headers: &HeaderMap,
    tokens: &std::sync::Mutex<UserTokens>,
) -> Result<OwnedUserId, TokenError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(TokenError::Missing)?;
    let value = header.to_str().map_err(|_| TokenError::Unknown)?;
    let token = value.strip_prefix("Bearer ").ok_or(TokenError::Unknown)?;
    let map = tokens.lock().unwrap_or_else(|e| e.into_inner());
    map.get(token).cloned().ok_or(TokenError::Unknown)
}
```

- [ ] **Step 2: Write the failing extractor test**

Add to the `#[cfg(test)] mod tests` in `multi_user.rs`:

```rust
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use std::sync::Mutex;

    #[test]
    fn resolve_hit_miss_and_missing() {
        let mut t = UserTokens::new();
        let alice: OwnedUserId = "@alice:example.org".try_into().unwrap();
        t.insert("syt_abc".to_owned(), alice.clone());
        let tokens = Mutex::new(t);

        // Hit.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("Bearer syt_abc"));
        assert_eq!(resolve(&h, &tokens).ok(), Some(alice));

        // Unknown token.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("Bearer nope"));
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Unknown)));

        // Missing header.
        let h = HeaderMap::new();
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Missing)));

        // Malformed (no Bearer prefix) → Unknown.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("syt_abc"));
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Unknown)));
    }
```

- [ ] **Step 3: Run the test (feature on)**

Run: `cargo test -p neutrino-http --features multi-user-shim multi_user::tests::resolve -- --nocapture`
Expected: PASS.

- [ ] **Step 4: Declare `AuthUser` + the `FromRequestParts` impl in `lib.rs`**

In `crates/neutrino-http/src/lib.rs`, add near the top-level types (e.g. just after the `AppState` definition, around line 53). Add the needed imports to the existing `use axum::{…}` block: `extract::FromRequestParts` and `http::request::Parts`.

```rust
/// Per-request caller identity. Yields the authenticated user.
///
/// - feature `multi-user-shim` ON: resolves `Authorization: Bearer <token>`
///   against the in-memory token map; 401 on missing/unknown.
/// - feature OFF: ignores any token and yields the single configured user
///   (`config.user_id()`), exactly matching today's single-user behaviour.
pub struct AuthUser(pub OwnedUserId);

impl axum::extract::FromRequestParts<AppState> for AuthUser {
    type Rejection = axum::response::Response;

    #[cfg(feature = "multi-user-shim")]
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let app = lock_app(state);
        match multi_user::resolve(&parts.headers, &app.user_tokens) {
            Ok(user) => Ok(AuthUser(user)),
            Err(multi_user::TokenError::Missing) => Err(error_response(
                StatusCode::UNAUTHORIZED,
                "M_MISSING_TOKEN",
                "Missing access token",
            )),
            Err(multi_user::TokenError::Unknown) => Err(error_response(
                StatusCode::UNAUTHORIZED,
                "M_UNKNOWN_TOKEN",
                "Unrecognised access token",
            )),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let user_id = lock_app(state).config.user_id();
        match user_id.parse() {
            Ok(u) => Ok(AuthUser(u)),
            Err(e) => Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            )),
        }
    }
}
```

NOTE: axum 0.8's `FromRequestParts` is an `async fn` trait method (no `#[async_trait]` needed). `error_response`, `lock_app`, `StatusCode`, and `OwnedUserId` are already in scope in `lib.rs`.

- [ ] **Step 5: Verify both feature states compile**

Run: `cargo build -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: both succeed. `AuthUser` is defined but not yet used — a feature-off `dead_code` warning is possible; it will be consumed in Task 4 (same compile unit, both states). If clippy flags it now, that's expected and resolved by Task 4; do not add `#[allow]`.

- [ ] **Step 6: Run the multi_user tests + commit**

Run: `cargo test -p neutrino-http --features multi-user-shim multi_user -- --nocapture`
Expected: PASS.

```bash
git add crates/neutrino-http/src/multi_user.rs crates/neutrino-http/src/lib.rs
git commit -m "feat(http): AuthUser extractor (token-resolving / config-default)"
```

---

## Task 4: Thread `AuthUser` through createRoom and the send/state path

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs` (`create_room`, `put_event`, `put_state`, `put_state_empty_key`, `send_via_actor`)

Replace the `app.config.user_id()` sender reads with the extracted user. Extractor params must come BEFORE the `Json` body extractor in axum handler argument order (body-consuming extractors go last).

- [ ] **Step 1: Update `create_room` to take `AuthUser`**

Replace the signature and the identity block of `create_room` (around line 549-564). Old:

```rust
async fn create_room(state: State<AppState>, body: Json<Value>) -> axum::response::Response {
    let (store, user_id) = {
        let app = lock_app(&state.0);
        (app.store.clone(), app.config.user_id())
    };

    let sender: OwnedUserId = match user_id.parse() {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
```

New:

```rust
async fn create_room(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    body: Json<Value>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
```

(The `sender` is now the extracted `OwnedUserId`; the parse block is gone. The rest of the function — `build_initial_events(&sender, &body.0)` onward — is unchanged.)

- [ ] **Step 2: Thread the user into `send_via_actor`**

Change `send_via_actor`'s signature to accept the sender and drop the config read. Old (around line 796-816):

```rust
async fn send_via_actor(
    state: &AppState,
    room_id: String,
    event_type: String,
    state_key: Option<String>,
    content: Value,
) -> axum::response::Response {
    let (registry, user_id) = {
        let app = lock_app(state);
        (app.room_registry.clone(), app.config.user_id())
    };
    let sender: OwnedUserId = match user_id.parse() {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let parsed_room_id: OwnedRoomId = match room_id.parse() {
```

New:

```rust
async fn send_via_actor(
    state: &AppState,
    sender: OwnedUserId,
    room_id: String,
    event_type: String,
    state_key: Option<String>,
    content: Value,
) -> axum::response::Response {
    let registry = lock_app(state).room_registry.clone();
    let parsed_room_id: OwnedRoomId = match room_id.parse() {
```

(The rest — the `registry.send_event(&parsed_room_id, sender, …)` call — is unchanged.)

- [ ] **Step 3: Update the three callers to extract + pass the sender**

`put_event` (around line 757-767):

```rust
async fn put_event(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type, _msg_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(&state.0, sender, room_id, event_type, None, body.0).await
}
```

`put_state` (around line 770-780):

```rust
async fn put_state(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type, state_key)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(&state.0, sender, room_id, event_type, Some(state_key), body.0).await
}
```

`put_state_empty_key` (around line 784-790):

```rust
async fn put_state_empty_key(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type)): axum::extract::Path<(String, String)>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(&state.0, sender, room_id, event_type, Some(String::new()), body.0).await
}
```

- [ ] **Step 4: Verify both feature states compile + existing tests still pass (feature off)**

Run: `cargo test -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: feature-off test suite (existing e2e, which sends no Authorization header) PASSES — proving the feature-off `AuthUser` correctly defaults to `@alice`. Feature-on build succeeds.

- [ ] **Step 5: clippy both states + commit**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean (the `AuthUser` dead-code concern from Task 3 is now resolved — it's used).

```bash
git add crates/neutrino-http/src/lib.rs
git commit -m "feat(http): attribute createRoom + send/state writes to AuthUser"
```

---

## Task 5: Thread `AuthUser` through sliding sync and legacy sync

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs` (`sync`)
- Modify: `crates/neutrino-http/src/legacy_sync/mod.rs` (`handle`)

- [ ] **Step 1: Update the sliding-sync `sync` handler**

In `lib.rs`, replace the head of `sync` (around line 285-305). Old:

```rust
async fn sync(
    state: State<AppState>,
    query: Query<HashMap<String, String>>,
    body: Json<Value>,
) -> axum::response::Response {
    let body_value = body.0;
    let (sync_state, user_id_str) = {
        let app = lock_app(&state.0);
        (app.sync_state.clone(), app.config.user_id())
    };

    let user_id: ruma::OwnedUserId = match user_id_str.as_str().try_into() {
        Ok(u) => u,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    let req = match build_sync_request(&query.0, body_value) {
```

New:

```rust
async fn sync(
    state: State<AppState>,
    AuthUser(user_id): AuthUser,
    query: Query<HashMap<String, String>>,
    body: Json<Value>,
) -> axum::response::Response {
    let body_value = body.0;
    let sync_state = lock_app(&state.0).sync_state.clone();

    let req = match build_sync_request(&query.0, body_value) {
```

(The rest — `sliding_sync::handle(&sync_state, &user_id, req)` onward — is unchanged.)

- [ ] **Step 2: Update the legacy `handle`**

In `legacy_sync/mod.rs`, replace the head of `handle` (around line 37-55). Old:

```rust
pub(crate) async fn handle(
    state: State<AppState>,
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    let (sync_state, user_id_str) = {
        let app = lock_app(&state.0);
        (app.sync_state.clone(), app.config.user_id())
    };

    let user_id: OwnedUserId = match user_id_str.as_str().try_into() {
        Ok(u) => u,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    let legacy_query = parse_legacy_query(&query.0);
```

New:

```rust
pub(crate) async fn handle(
    state: State<AppState>,
    crate::AuthUser(user_id): crate::AuthUser,
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    let sync_state = lock_app(&state.0).sync_state.clone();

    let legacy_query = parse_legacy_query(&query.0);
```

NOTE: `AuthUser` is `pub` in `lib.rs` (crate root). Reference it as `crate::AuthUser`. The now-unused `OwnedUserId` import in `legacy_sync/mod.rs` — check whether it's still used elsewhere in the file (`fetch_memberships` takes `&UserId`); if `OwnedUserId` becomes unused, remove it from the `use ruma::{…}` line to avoid an unused-import warning.

- [ ] **Step 3: Verify both feature states compile + feature-off tests pass**

Run: `cargo test -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: feature-off e2e (incl. `e2e_sliding_sync.rs`, `e2e_legacy_sync.rs`) PASSES; feature-on builds.

- [ ] **Step 4: clippy both states + commit**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean.

```bash
git add crates/neutrino-http/src/lib.rs crates/neutrino-http/src/legacy_sync/mod.rs
git commit -m "feat(http): attribute sliding + legacy sync to AuthUser"
```

---

## Task 6: Per-user `/register` and `/login` (feature-gated bodies)

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs` (`post_register`, `post_login`)
- Modify: `crates/neutrino-http/src/multi_user.rs` (a `register_user` helper that builds the user id, mints + stores a token)

Both endpoints stay single-user when the feature is off (today's behaviour). When on, they read the requested localpart, mint a token, store it, and return it.

- [ ] **Step 1: Add the provisioning helper to `multi_user.rs`**

Append to `multi_user.rs` (before the test module):

```rust
/// Resolve a requested localpart to a full user id on this server, mint a
/// fresh token, store it, and return `(user_id, token)`. An absent/blank
/// localpart falls back to the configured default (single-user parity).
pub(crate) fn provision(
    tokens: &std::sync::Mutex<UserTokens>,
    server_name: &str,
    default_user_id: &str,
    requested_localpart: Option<&str>,
) -> Result<(OwnedUserId, String), String> {
    let user_id: OwnedUserId = match requested_localpart {
        Some(lp) if !lp.is_empty() => format!("@{lp}:{server_name}")
            .as_str()
            .try_into()
            .map_err(|e: ruma::IdParseError| e.to_string())?,
        _ => default_user_id
            .try_into()
            .map_err(|e: ruma::IdParseError| e.to_string())?,
    };
    let token = mint_token();
    tokens
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(token.clone(), user_id.clone());
    Ok((user_id, token))
}
```

(`ruma::IdParseError` is the error `TryFrom<&str> for OwnedUserId` yields. If the exact path differs, the build error in Step 4 will name it — adjust the `map_err` type accordingly.)

- [ ] **Step 2: Add a feature-on test for `provision`**

Add to the `multi_user.rs` test module:

```rust
    #[test]
    fn provision_uses_localpart_and_stores_token() {
        let tokens = Mutex::new(UserTokens::new());
        let (user, token) =
            provision(&tokens, "example.org", "@alice:example.org", Some("bob")).unwrap();
        assert_eq!(user.as_str(), "@bob:example.org");
        assert_eq!(
            tokens.lock().unwrap().get(&token).map(|u| u.to_string()),
            Some("@bob:example.org".to_owned())
        );
    }

    #[test]
    fn provision_falls_back_to_default_when_absent() {
        let tokens = Mutex::new(UserTokens::new());
        let (user, _) = provision(&tokens, "example.org", "@alice:example.org", None).unwrap();
        assert_eq!(user.as_str(), "@alice:example.org");
    }
```

- [ ] **Step 3: Run the helper tests (feature on)**

Run: `cargo test -p neutrino-http --features multi-user-shim multi_user::tests::provision -- --nocapture`
Expected: PASS.

- [ ] **Step 4: Feature-split `post_register`**

In `lib.rs`, replace the success branch of `post_register`. The UIA first-step (the `if body.0.get("auth").is_none()` block, lines 232-242) stays unchanged for both feature states. Replace the part AFTER it (lines 244-260) so the OK response is feature-split:

```rust
    let device_id = body
        .0
        .pointer("/device_id")
        .and_then(|v| v.as_str())
        .unwrap_or("DEVICEID")
        .to_string();

    #[cfg(feature = "multi-user-shim")]
    {
        let app = lock_app(&state.0);
        let requested = body.0.pointer("/username").and_then(|v| v.as_str());
        let default_user_id = app.config.user_id();
        match multi_user::provision(
            &app.user_tokens,
            &app.config.server_name,
            &default_user_id,
            requested,
        ) {
            Ok((user_id, token)) => (
                StatusCode::OK,
                Json(json!({
                    "user_id": user_id,
                    "access_token": token,
                    "home_server": app.config.server_name,
                    "device_id": device_id,
                })),
            ),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "errcode": "M_INVALID_USERNAME", "error": e })),
            ),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    {
        let app = lock_app(&state.0);
        (
            StatusCode::OK,
            Json(json!({
                "user_id": app.config.user_id(),
                "access_token": "syt_1234567890abcdef",
                "home_server": app.config.server_name,
                "device_id": device_id,
            })),
        )
    }
```

(Both arms are the trailing expression of the function. Note `device_id` is computed once before the cfg split.)

- [ ] **Step 5: Feature-split `post_login`**

Replace `post_login` (lines 263-276) wholesale:

```rust
async fn post_login(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Logged in");
    let app = lock_app(&state.0);
    let server_name = app.config.server_name.clone();

    #[cfg(feature = "multi-user-shim")]
    {
        // Accept `identifier.user` (full UIA shape) or a bare `user`.
        let requested = body
            .0
            .pointer("/identifier/user")
            .or_else(|| body.0.pointer("/user"))
            .and_then(|v| v.as_str())
            .map(localpart_of);
        let default_user_id = app.config.user_id();
        match multi_user::provision(
            &app.user_tokens,
            &server_name,
            &default_user_id,
            requested.as_deref(),
        ) {
            Ok((user_id, token)) => Json(json!({
                "user_id": user_id,
                "access_token": token,
                "home_server": server_name,
                "device_id": "DEVICEID",
            })),
            // Provisioning only fails on an unparseable id; echo the default.
            Err(_) => Json(json!({
                "user_id": app.config.user_id(),
                "access_token": "syt_1234567890abcdef",
                "home_server": server_name,
                "device_id": "DEVICEID",
            })),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    {
        let _ = &body; // body unused in single-user mode
        Json(json!({
            "user_id": app.config.user_id(),
            "access_token": "syt_1234567890abcdef",
            "home_server": server_name,
            "device_id": "DEVICEID",
        }))
    }
}
```

NOTE: `post_login` gains a `body: Json<Value>` param (it had none). This is safe — axum will deserialize the JSON body; the existing allowlisted login tests send a JSON identifier body. Add the small `localpart_of` helper (feature-gated) near the other free fns in `lib.rs`:

```rust
/// Extract the localpart from a login identifier that may be a full MXID
/// (`@bob:server`) or already a bare localpart (`bob`).
#[cfg(feature = "multi-user-shim")]
fn localpart_of(identifier: &str) -> String {
    identifier
        .strip_prefix('@')
        .and_then(|rest| rest.split_once(':').map(|(lp, _)| lp))
        .unwrap_or(identifier)
        .to_owned()
}
```

- [ ] **Step 6: Verify both feature states compile + feature-off tests pass**

Run: `cargo test -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: feature-off PASSES (login/register stubs unchanged in behaviour). Feature-on builds.

- [ ] **Step 7: clippy both states + commit**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean.

```bash
git add crates/neutrino-http/src/lib.rs crates/neutrino-http/src/multi_user.rs
git commit -m "feat(http): per-user register/login minting under the shim feature"
```

---

## Task 7: De-hardcode the `/profile` and `/account_data` routes

**Files:**
- Modify: `crates/neutrino-http/src/lib.rs` (`build_router`, `profile`, `get_account_data`)

Under multi-user, Complement queries these for arbitrary users; the current routes bake `config.user_id()` into the path and would 404. Make them `{user_id}` path-param routes returning the same stub bodies. This is unconditional (no feature gate needed — a `{user_id}` param is harmless in single-user mode too) and removes the `let user_id = config.user_id();` line in `build_router`.

- [ ] **Step 1: Change the routes to path params**

In `build_router` (around line 140-176), remove `let user_id = config.user_id();` (line 141) and replace the two `format!`-built routes (lines 166-176) with:

```rust
        .route("/_matrix/client/v3/profile/{user_id}", get(profile))
        .route(
            "/_matrix/client/v3/user/{user_id}/account_data/{account_data_type}",
            get(get_account_data),
        )
```

Since `config` is now unused in `build_router` (only `state` matters), change the signature to `fn build_router(state: AppState) -> Router` and update the two callers (`router` at line 118: `Ok(build_router(state))`; `router_with_store` at line 137: `build_router(state)`). Remove the now-unused `config` param threading.

- [ ] **Step 2: Update `profile` to accept the path param**

`profile` (line 521-525) gains a path extractor (ignored body):

```rust
async fn profile(
    axum::extract::Path(_user_id): axum::extract::Path<String>,
) -> Json<Value> {
    Json(json!({
        "displayname": "Alice",
    }))
}
```

`get_account_data` already takes a `Path<String>` for the data type — change it to a 2-tuple so the `{user_id}` segment is captured:

```rust
async fn get_account_data(
    axum::extract::Path((_user_id, _account_data_type)): axum::extract::Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
             "errcode": "M_NOT_FOUND",
              "error": "No current backup version"
        })),
    )
}
```

- [ ] **Step 3: Verify both states compile + feature-off tests pass**

Run: `cargo test -p neutrino-http && cargo build -p neutrino-http --features multi-user-shim`
Expected: PASS / build OK. (The `capabilities.rs` / `versions.rs` e2e tests don't touch profile; nothing should regress.)

- [ ] **Step 4: clippy both states + commit**

Run: `cargo clippy -p neutrino-http --tests -- -D warnings && cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean (no unused `config` warning).

```bash
git add crates/neutrino-http/src/lib.rs
git commit -m "refactor(http): make profile/account_data routes user-id params"
```

---

## Task 8: Feature-gated e2e tests (the verification)

**Files:**
- Create: `crates/neutrino-http/tests/e2e_multi_user.rs`

These run only with `--features multi-user-shim`. They prove distinct tokens, per-request attribution, and the 401 paths.

- [ ] **Step 1: Write the e2e test file**

Create `crates/neutrino-http/tests/e2e_multi_user.rs`:

```rust
//! End-to-end tests for the testing-only multi-user identity shim. Compiled
//! and run only with `--features multi-user-shim`:
//!   cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user
//!
//! Proves: distinct per-user tokens; events + sync attributed to the token's
//! user; spec-correct 401 on missing/unknown tokens.
#![cfg(feature = "multi-user-shim")]

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

/// Send a request with an optional Bearer token and JSON body.
async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let req = builder
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

/// Register a user via the two-step UIA stub; return (user_id, access_token).
async fn register(app: &axum::Router, username: &str) -> (String, String) {
    let (_s, _flows) = send(
        app,
        "POST",
        "/_matrix/client/v3/register",
        None,
        &json!({ "username": username }),
    )
    .await;
    let (status, body) = send(
        app,
        "POST",
        "/_matrix/client/v3/register",
        None,
        &json!({ "username": username, "auth": { "type": "m.login.dummy" } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register body: {body}");
    (
        body["user_id"].as_str().unwrap().to_owned(),
        body["access_token"].as_str().unwrap().to_owned(),
    )
}

fn sync_body() -> Value {
    json!({
        "lists": { "all": { "ranges": [[0, 99]], "timeline_limit": 5, "required_state": [] } }
    })
}

#[tokio::test]
async fn register_two_users_yields_distinct_tokens() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    assert_eq!(alice_id, "@alice:example.org");
    assert_eq!(bob_id, "@bob:example.org");
    assert_ne!(alice_tok, bob_tok, "tokens must differ");
}

#[tokio::test]
async fn createroom_and_sync_are_attributed_to_the_token_user() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    // Each user creates their own room.
    let (s, a_room) = send(&app, "POST", "/_matrix/client/v3/createRoom", Some(&alice_tok), &json!({})).await;
    assert_eq!(s, StatusCode::OK, "{a_room}");
    let alice_room = a_room["room_id"].as_str().unwrap().to_owned();

    let (s, b_room) = send(&app, "POST", "/_matrix/client/v3/createRoom", Some(&bob_tok), &json!({})).await;
    assert_eq!(s, StatusCode::OK, "{b_room}");
    let bob_room = b_room["room_id"].as_str().unwrap().to_owned();

    assert_ne!(alice_room, bob_room);

    // Alice's sync sees alice_room, not bob_room.
    let (s, alice_sync) = send(&app, "POST", SYNC_PATH, Some(&alice_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{alice_sync}");
    let alice_rooms = alice_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(alice_rooms.contains_key(&alice_room), "alice should see her room: {alice_sync}");
    assert!(!alice_rooms.contains_key(&bob_room), "alice must NOT see bob's room: {alice_sync}");

    // Bob's sync sees bob_room, not alice_room.
    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    let bob_rooms = bob_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(bob_rooms.contains_key(&bob_room), "bob should see his room: {bob_sync}");
    assert!(!bob_rooms.contains_key(&alice_room), "bob must NOT see alice's room: {bob_sync}");
}

#[tokio::test]
async fn missing_token_is_401_missing() {
    let app = router(config()).await.expect("router init");
    let (s, body) = send(&app, "POST", "/_matrix/client/v3/createRoom", None, &json!({})).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["errcode"], "M_MISSING_TOKEN");
}

#[tokio::test]
async fn unknown_token_is_401_unknown() {
    let app = router(config()).await.expect("router init");
    let (s, body) = send(&app, "POST", "/_matrix/client/v3/createRoom", Some("syt_bogus"), &json!({})).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["errcode"], "M_UNKNOWN_TOKEN");
}
```

- [ ] **Step 2: Run the e2e tests (feature on)**

Run: `cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user -- --nocapture`
Expected: all four tests PASS. If `createroom_and_sync_…` fails on the room-key shape, inspect the printed sync body — the sliding-sync response keys rooms by room_id under `rooms`; adjust the assertion to match the actual wire shape (do NOT weaken the cross-user-isolation assertion).

- [ ] **Step 3: Confirm the test file is excluded when the feature is off**

Run: `cargo test -p neutrino-http --test e2e_multi_user`
Expected: compiles to an empty test binary (the `#![cfg(...)]` strips everything); 0 tests run, no failure.

- [ ] **Step 4: clippy (feature on) + commit**

Run: `cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings`
Expected: clean.

```bash
git add crates/neutrino-http/tests/e2e_multi_user.rs
git commit -m "test(http): e2e multi-user attribution + 401 token paths"
```

---

## Task 9: Wire the feature into the Complement image and CI

**Files:**
- Modify: `docker/complement/Dockerfile`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Build the Complement image with the feature**

Open `docker/complement/Dockerfile`, find the `cargo build` line that builds the `neutrino` binary, and add `--features multi-user-shim`. For example, change:

```dockerfile
RUN cargo build --release --bin neutrino
```

to:

```dockerfile
RUN cargo build --release --bin neutrino --features multi-user-shim
```

(Match the exact existing invocation — it may already pass `--bin neutrino` or a workspace path. Only add the `--features multi-user-shim` flag; do not otherwise change the line. If the build is `cargo build --release` with no bin filter, append the flag to that line.)

- [ ] **Step 2: Add a feature-on CI step**

In `.github/workflows/ci.yml`, locate the `test` job's test step and the `clippy` job. Add a feature-on pass. In the `test` job, after the existing `cargo test` step, add:

```yaml
      - name: Test (multi-user-shim feature)
        run: cargo test -p neutrino-http --features multi-user-shim
```

In the `clippy` job, after the existing clippy step, add:

```yaml
      - name: Clippy (multi-user-shim feature)
        run: cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings
```

(Match the existing YAML indentation/step style exactly. If clippy currently runs `--workspace --all-targets`, keep that as the default-feature pass and add this as the additional feature-on pass.)

- [ ] **Step 3: Verify CI YAML is well-formed**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add docker/complement/Dockerfile .github/workflows/ci.yml
git commit -m "ci: build + lint + test the multi-user-shim feature"
```

---

## Task 10: Full verification, PLAN.md / LOG.md, final commit

**Files:**
- Modify: `PLAN.md` (status + decisions log)
- Modify: `LOG.md` (2-line summary)

- [ ] **Step 1: Full workspace verification, both feature states**

Run:
```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p neutrino-http --tests --features multi-user-shim -- -D warnings
cargo test --workspace
cargo test -p neutrino-http --features multi-user-shim
```
Expected: fmt clean, both clippy passes clean, both test runs green (the existing workspace suite plus the four new shim e2e tests).

- [ ] **Step 2: Update PLAN.md status**

In `PLAN.md`, under `## status`, add a new checked top-level item (place it near the CSAPI items):

```markdown
- [x] Testing-only multi-user identity shim (behind `multi-user-shim` cargo feature)
    - `user_tokens` map in `App`; `AuthUser` extractor resolves `Authorization: Bearer <token>` → per-request sender/syncing-user (401 M_MISSING_TOKEN / M_UNKNOWN_TOKEN). Feature OFF = single-user `@alice`, byte-identical. `/register` + `/login` mint a `syt_<random>` token per user. Wired into createRoom, send/state, sliding + legacy sync; profile/account_data routes de-hardcoded to `{user_id}`. 4 feature-gated e2e tests + multi_user unit tests. Complement image + CI build with the feature. Membership endpoints (/join, /invite, /leave) remain the follow-up that actually unlocks multi-user Complement tests; no allowlist changes here.
```

- [ ] **Step 3: Append a decision-log entry to PLAN.md**

Under `## decisions log`, add (date 2026-06-01):

```markdown
2026-06-01: Testing-only multi-user identity shim, gated by a new `multi-user-shim` cargo feature on `neutrino-http` (pass-through from `neutrino-main` + the `neutrino` binary; `neutrino-ffi`/Android never enables it; the Complement image + a CI step do). An `AuthUser(OwnedUserId)` axum extractor resolves `Authorization: Bearer <token>` against an in-memory `user_tokens: Mutex<HashMap<token, OwnedUserId>>` in `App`; feature-on → 401 M_MISSING_TOKEN / M_UNKNOWN_TOKEN on miss, feature-off → returns `config.user_id()` so the single-user path is byte-identical. `/register` + `/login` mint a `syt_<32 alnum>` token per user (token→user only; new login = new token = new device). Threaded through createRoom, send/state (`send_via_actor`), sliding + legacy sync. Profile/account_data routes de-hardcoded from baked-in `@alice` to `{user_id}` params. Verification is per-user-createRoom attribution (createRoom does NOT honour the `invite` list yet, so cross-user visibility waits on the membership follow-up); shared-secret admin register and per-device token scoping deliberately excluded. Spec: docs/superpowers/specs/2026-06-01-multi-user-shim-design.md.
```

- [ ] **Step 4: Append a LOG.md summary**

Append to the BOTTOM of `LOG.md` (append-only, oldest first; no rationale — that lives in PLAN.md):

```markdown
2026-06-01: Added testing-only multi-user identity shim behind `multi-user-shim` cargo feature — `AuthUser` extractor resolves Bearer tokens to per-request users; /register + /login mint per-user tokens; createRoom/send/state/sync attributed to the caller.
```

- [ ] **Step 5: Final commit**

```bash
git add PLAN.md LOG.md
git commit -m "docs: record multi-user-shim in PLAN.md + LOG.md"
```

---

## Self-Review Notes (already applied)

- **Spec coverage:** feature gate (T1), token store (T2), extractor + 401s (T3), sender threading createRoom/send (T4), sync threading (T5), per-user register/login (T6), route de-hardcoding (T7), verification e2e (T8), Complement image + CI (T9), PLAN/LOG (T10). All spec sections map to a task.
- **Verification correction:** the spec's original invite-based attribution test was infeasible (createRoom ignores `invite`); both the spec and T8 now use per-user-createRoom isolation instead.
- **Type consistency:** `AuthUser(pub OwnedUserId)`, `UserTokens = HashMap<String, OwnedUserId>`, `mint_token() -> String`, `provision(...) -> Result<(OwnedUserId, String), String>`, `resolve(...) -> Result<OwnedUserId, TokenError>` are used consistently across tasks.
- **Known adjustment points flagged inline:** `ruma::IdParseError` path (T6 S1), `rand` 0.9 `distr`/`rng` names (T2 S3), the sliding-sync response room-key shape (T8 S2), and the exact `cargo build` / clippy lines in the Dockerfile + CI (T9) — each step says what to check and how to adjust without weakening the test intent.
