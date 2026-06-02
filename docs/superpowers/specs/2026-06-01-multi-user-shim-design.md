# Multi-user identity shim (testing only) — design

**Date:** 2026-06-01
**Status:** approved (design)
**Scope:** identity shim ONLY. Membership endpoints (`/join`, `/invite`,
`/leave`) and read endpoints (`/event`, `/messages`) are explicitly out of
scope and tracked as separate follow-up plans.

## Problem

Neutrino is a single-user embedded homeserver. Today there is no token
handling at all:

- `POST /login` and `POST /register` return a hardcoded token
  (`syt_1234567890abcdef`) and the single configured user
  `config.user_id()` (always `@alice:<server>`).
- No request path extracts or validates an `Authorization` header — there is
  no `Bearer`/`access_token` handling anywhere in `neutrino-http`.
- Every write/sync path reads `config.user_id()` directly to decide the event
  sender or the syncing user (createRoom, `put_event`/`put_state` via the room
  actor, sliding sync, legacy sync).

The Complement compliance suite registers multiple users on one homeserver and
authenticates each request with the `Authorization: Bearer <token>` it got
back from `/register`/`/login`. Roughly 60+ csapi subtests plus ~10 local v12
tests become reachable once the server can attribute requests to distinct
local users. This spec covers the *identity* foundation only; on its own it
unlocks little in Complement (no `/join` yet), but it is the prerequisite for
everything else and is independently testable.

## Goals

- Multiple local users can register/login, each receiving a distinct access
  token, and every authenticated request is attributed to the right user.
- The single-user production/Android path is **unchanged and byte-identical**
  when the shim is disabled — with the one documented exception of the
  profile/account-data routes, which are de-hardcoded to `{user_id}`
  unconditionally (see §5). Feature-off this only widens those two routes to
  match any user id (returning the same stub bodies); the embedded client only
  ever queries its own user, so there is no observable change in production.
- Spec-correct `401` on missing/unknown tokens (so Complement auth-error
  assertions pass).

## Non-goals

- Federation, E2EE, EDUs, account data, push, media, search, directory,
  aliases, per-user profiles — all remain out of scope.
- Membership endpoints (`/join`, `/invite`, `/leave`, `/kick`, `/ban`) —
  separate follow-up plan.
- Read endpoints (`GET /event`, `GET /messages`) — separate follow-up.
- Shared-secret `POST /_synapse/admin/v1/register` (HMAC) — **not**
  implemented; the few Complement tests that use it stay off the allowlist.
- Per-device token scoping — a token maps to a *user*, not a `(user, device)`
  pair. (A few txnid tests that assert device-scoped idempotency stay out.)
- Persisting the token map — it is in-memory and ephemeral. Complement keeps
  the container alive for the duration of a test, so a restart-loss is
  acceptable; nothing in scope needs durability.

## Feature gate

A new cargo feature isolates the shim from the production binary.

- Define `multi-user-shim` in `crates/neutrino-http/Cargo.toml`
  (`[features] multi-user-shim = []`).
- Add a pass-through feature of the same name in `neutrino-main` and in the
  `neutrino` binary crate
  (`multi-user-shim = ["neutrino-http/multi-user-shim"]`) so it can be enabled
  from the top-level build.
- `neutrino-ffi` (Android) never enables it.
- The complement Dockerfile and the local dev binary build with
  `--features multi-user-shim`.

When the feature is **off**, the single-user behaviour is exactly as today:
the extractor returns the configured `@alice` user and no token map exists.

## Components

### 1. Token store

`App` (in `crates/neutrino-http/src/lib.rs`) gains, behind the existing
`Mutex<App>`:

```rust
#[cfg(feature = "multi-user-shim")]
user_tokens: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, ruma::OwnedUserId>>>,
```

- Keyed by the opaque access-token string; value is the resolved user id.
- Named `user_tokens` to make explicit these are *user* access tokens (not
  device keys, registration tokens, etc.).
- Populated by `/register` and `/login`; never pruned (ephemeral, test-only).
- The `#[cfg]` is on the field itself so the production struct does not carry
  an unused field (no dead-code warning when the feature is off).

### 2. Identity extractor `AuthUser`

A new extractor, always defined so handler signatures are identical in both
feature states:

```rust
pub struct AuthUser(pub ruma::OwnedUserId);

impl axum::extract::FromRequestParts<AppState> for AuthUser { /* ... */ }
```

Handlers destructure it directly — `AuthUser(user_id)` — so call sites read
`user_id`, not `auth.0`.

Body is `#[cfg]`-split:

- **feature on:** read the `Authorization` header.
  - missing/blank header → `Err` → `401 { "errcode": "M_MISSING_TOKEN", ... }`
  - present but not `Bearer <token>` or token not in `user_tokens` →
    `401 { "errcode": "M_UNKNOWN_TOKEN", ... }`
  - hit → `Ok(AuthUser(user_id.clone()))`
- **feature off:** ignore any header; return
  `Ok(AuthUser(config.user_id().parse()?))`. Parse failure (config invariant
  broken) → `500 M_UNKNOWN`, matching the existing handler behaviour.

The rejection produces the project's standard Matrix error JSON via the same
helper the handlers already use (`error_response`).

Unauthed endpoints — `GET /_matrix/client/versions`, `GET /login` (flows),
`POST /register`, `POST /login` — do **not** take `AuthUser`.

### 3. `/register` and `/login` (feature on)

Both endpoints are `#[cfg]`-split (the feature-off bodies are exactly today's
stubs).

- **`POST /register`:** keep the existing UIA behaviour — a request with no
  `auth` block returns the `m.login.dummy` flow stub (Complement performs the
  two-step). On the success branch (feature on), read `username` (localpart)
  from the body, build `@{username}:{server_name}`, mint a fresh token, insert
  `token → user_id`, and return `{ user_id, access_token, home_server,
  device_id }`. `device_id` keeps the existing body-or-`DEVICEID` behaviour.
- **`POST /login`:** read the identifier from the body
  (`identifier.user` / `user`, accepting a bare localpart per the existing
  allowlisted test), resolve to a user id, mint+store a token, and return the
  same response shape.
- **Missing identifier:** if `username` (register) or the login identifier is
  absent from the body, fall back to the configured default localpart — i.e.
  the current single-user behaviour — rather than erroring. Complement always
  supplies one; this just keeps the unauthed-default case sane.
- **Token format:** `syt_<32 alnum>` (32 random alphanumerics).
  Uniqueness is probabilistic and more than sufficient for a test-only server; no collision handling needed.

A small private helper module (e.g. `multi_user`, gated by the feature) holds
the token-store type alias, the mint/insert helper, and the
`AuthUser::from_request_parts` resolution logic, so the `#[cfg]` surface is
concentrated rather than sprinkled across handlers.

### 4. Handler identity threading

Swap `config.user_id()` for the extracted `AuthUser(user_id)` in the
identity-bearing handlers:

| Handler | Today | Change |
|---|---|---|
| `create_room` | `config.user_id()` → sender | add `AuthUser(user_id)`; sender = `user_id` |
| `put_event` / `put_state` (`send_via_actor`) | `config.user_id()` → sender | add `AuthUser(user_id)`; sender = `user_id` |
| `sync` (sliding) | `config.user_id()` → syncing user | add `AuthUser(user_id)`; pass to `sliding_sync::handle` |
| `legacy_sync::handle` | `config.user_id()` → syncing user | add `AuthUser(user_id)`; pass through |

The sync pipeline (`sliding_sync::handle`, `build_response`,
`candidate_rooms`) already takes `user_id: &UserId`, so no deeper rewiring is
needed — only the handler boundary changes.

The E2EE keys stubs (`keys_upload`, `signatures_upload`) read
`config.user_id()` too. They may adopt `AuthUser` for correct attribution, but
because E2EE is stubbed and out of scope, this is optional and only done if
trivial; otherwise left as-is.

### 5. De-hardcoding the user-templated routes

`build_router` currently bakes `config.user_id()` into the *path* of the
profile and account-data routes (e.g.
`/_matrix/client/v3/profile/@alice:server`). Under multi-user, Complement
queries these for other users and would hard-`404`.

Change these to `{user_id}` path-parameter routes returning the same stub
bodies they return today. Profiles and account data remain out of scope as
*features*; this is purely so routing does not 404 for a non-default user.

## Data flow (feature on)

```
POST /register {username: "bob"}      -> mint syt_<uuid>, user_tokens[token]=@bob:server
                                      -> 200 {user_id:@bob, access_token:token, ...}

PUT  /rooms/{id}/send/... (Bearer token)
   -> AuthUser extractor: user_tokens[token] -> @bob:server
   -> send_via_actor(sender=@bob) -> RoomRegistry -> RoomCore::build_local_event(@bob, ...)

POST /sync (Bearer token)
   -> AuthUser extractor -> @bob:server
   -> sliding_sync::handle(state, @bob, req) -> rooms_with_membership(@bob, ...)
```

## Error handling

- Missing `Authorization` → `401 M_MISSING_TOKEN`.
- Malformed header or unknown token → `401 M_UNKNOWN_TOKEN`.
- Config user unparseable (feature off, broken invariant) → `500 M_UNKNOWN`
  (unchanged from today).
- All errors use the existing `error_response` helper / `AppError` mapping; no
  new error enum.

## Testing & verification

New e2e tests in `crates/neutrino-http/tests/`, gated `#[cfg(feature =
"multi-user-shim")]`, run with `cargo test -p neutrino-http --features
multi-user-shim`:

1. **Distinct tokens:** register `alice` and `bob` → two different
   `access_token`s; both resolve to the expected user ids.
2. **Per-request attribution (end-to-end, no `/join` needed):** alice's token
   `createRoom`s room A; bob's token `createRoom`s room B. Then alice `POST
   /sync` with *alice's* token sees room A but **not** room B, and bob's sync
   sees room B but not A. This proves both the event sender (the create/join
   batch is authored by the token's user) and the syncing-user are driven by
   the token, using only existing endpoints. (createRoom does not yet honour
   the `invite` list — cross-user visibility waits on the membership
   follow-up.)
3. **Auth errors:** request with no `Authorization` → `401 M_MISSING_TOKEN`;
   request with a garbage Bearer token → `401 M_UNKNOWN_TOKEN`.

Regression: the existing feature-off e2e suite (which sends no `Authorization`
header) stays green unchanged.

CI: add a step that builds, clippies (`-D warnings`), and tests
`neutrino-http` with `--features multi-user-shim`, so the feature-on code is
linted and exercised. Clippy must be clean in **both** feature states.

The complement Dockerfile gains `--features multi-user-shim` so the image is
ready for the membership follow-up; **no `allowlist.txt` changes in this
plan** — multi-user Complement tests need `/join`, which is the next plan.

## Affected files (anticipated)

- `crates/neutrino-http/Cargo.toml` — feature definition.
- `crates/neutrino-main/Cargo.toml`, `crates/neutrino/Cargo.toml` —
  pass-through feature.
- `crates/neutrino-http/src/lib.rs` — `App.user_tokens` field, `AuthUser`
  wiring, handler signature changes, route de-hardcoding, register/login
  bodies.
- `crates/neutrino-http/src/multi_user.rs` (new, feature-gated) — token store
  type, mint helper, extractor resolution.
- `crates/neutrino-http/src/legacy_sync/mod.rs` — `AuthUser` in the handler.
- `crates/neutrino-http/tests/` — new feature-gated e2e test file.
- `docker/complement/Dockerfile` — `--features multi-user-shim`.
- `.github/workflows/ci.yml` — feature-on build/clippy/test step.
- `PLAN.md` / `LOG.md` — status + decision/summary entries per project rules.

## Open questions

None outstanding. (Per-device token scoping and shared-secret admin register
are deliberately excluded; revisit only if a targeted Complement test needs
them.)
