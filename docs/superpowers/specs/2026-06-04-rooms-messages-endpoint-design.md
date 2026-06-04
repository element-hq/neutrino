# C2S `GET /rooms/{roomId}/messages` endpoint — design

**Date:** 2026-06-04
**Goal:** Implement the Client-Server `GET /_matrix/client/v3/rooms/{roomId}/messages`
endpoint so a downstream client stops receiving 404s. Mirror Synapse's behaviour
where neutrino has the underlying mechanism; explicitly skip the parts where it
does not.

## Decisions (locked with Skye, 2026-06-04)

1. Mirror Synapse (`RoomMessageListRestServlet` → `PaginationHandler.get_messages`)
   and the spec (`matrix-spec-main/data/api/client-server/message_pagination.yaml`).
2. Support all pagination query params a client may send: `from`, `to`, `dir`,
   `limit`. "Optional" in the spec means the client *may* send it, not that the
   server may ignore it.
3. `to` requires an **additive** change to the `EventStore::room_messages` trait
   method (`to: Option<PaginationToken>`). Allowed; record in the PLAN.md
   decisions log (per the trait-change rule).
4. **Membership:** neutrino has membership semantics (but no history-visibility).
   Require the requesting user to be `join`-ed; otherwise **403 `M_FORBIDDEN`**.
   This also covers an unknown room (no member event ⇒ not joined ⇒ 403), which
   matches the spec's only documented error response (403 "You aren't a member of
   the room") — there is no 404 for this endpoint.
5. **`filter`:** accept the param but **no-op** it (option 3). Document the
   limitation on the handler. Event filtering / `lazy_load_members` are an
   existing project-wide gap and out of scope here.

## Non-goals (mechanism absent in neutrino — deliberately not mirrored)

- Federation backfill on `dir=b` gaps (Synapse `maybe_backfill`). neutrino has no
  inbound history backfill and runs in a trusted mesh — return only what we hold.
- History-visibility filtering (`filter_events_for_client`). No auth/visibility
  model; other read endpoints don't filter either.
- Lazy-loaded `state` (driven by `filter.lazy_load_members`). Always omitted.
- `filter` application of any kind (`types`/`senders`/`contains_url`/`limit`).
- Matching Synapse's opaque topological token *format* — neutrino tokens are
  `stream_pos` decimals (same as sync's `prev_batch`); clients treat them as
  opaque, and interop with sync is a feature.

## Components

### 1. Storage: extend `EventStore::room_messages` with a `to` bound

`crates/neutrino-store/src/lib.rs` — change the trait method signature to:

```rust
async fn room_messages(
    &self,
    room_id: &RoomId,
    from: Option<PaginationToken>,
    to: Option<PaginationToken>,
    dir: Direction,
    limit: usize,
) -> Result<(Vec<Event>, Option<PaginationToken>), StorageError>;
```

Update the doc-comment: `to` is the **exclusive** stop boundary (matches Synapse's
`start >= x > end` for `b` / `start < x < end` for `f`); when `None`, there is no
stop boundary in that direction.

`crates/neutrino-store-sqlite/src/store/events.rs` — the impl currently builds a
single bound `stream_pos {cmp} ?`. Add the second (`to`) bound:

- Resolve `to_pos`:
  - `Some(t)` → `i64::try_from(t.0)` (overflow → `InvalidInput`).
  - `None` → `Direction::Forward` ⇒ `i64::MAX`; `Direction::Backward` ⇒ `i64::MIN`
    (unconstraining sentinels).
- Direction → comparators:
  - `Forward`: `stream_pos > from_pos AND stream_pos < to_pos`, `ORDER BY ASC`.
  - `Backward`: `stream_pos < from_pos AND stream_pos > to_pos`, `ORDER BY DESC`.
- Everything else (the `limit+1` sentinel overflow detection → `next` token, the
  room-existence pre-check, the `limit == 0` short-circuit) is unchanged: the
  sentinel is simply the `(limit+1)`-th row *within the bounded range*, so a
  page that ends because it hit `to` has no sentinel ⇒ `next = None` (clean
  stream termination), exactly as desired.

**Caller updates (additive `None`):**
- `crates/neutrino-http/src/sliding_sync/build.rs` — the one production caller
  (`room_messages(room_id, None, Direction::Backward, cfg.timeline_limit)`) →
  pass `None` for `to`.
- Existing `room_messages` tests in
  `crates/neutrino-store-sqlite/src/store/events.rs` (and any in
  `crates/neutrino-store-sqlite/tests/storage.rs`) — add the `None` argument.
  (Grep `room_messages(` to enumerate all call sites before editing.)

**New storage tests** for the `to` bound (in `events.rs` test module):
- `dir=b` with `to` stops *before* the `to` event (exclusive) and returns
  `next = None` when the bounded range is exhausted within `limit`.
- `dir=f` with `to` symmetric.
- `from` + `to` bracket: only events strictly between are returned, correct order.
- `to` beyond all data behaves like `None` (no spurious truncation).

### 2. Membership helper: promote `current_membership`

`crates/neutrino-http/src/membership.rs` — change `async fn current_membership`
from private to `pub(crate)` so the new module can reuse it. No behaviour change.
(Signature: `current_membership(&AppState, &RoomId, &UserId) -> Result<Option<String>, Response>`,
where `Err` is already a ready 500 response.)

### 3. New module `crates/neutrino-http/src/messages.rs`

A focused sibling of `membership.rs`. Module-level doc comment MUST state the
known limitation: *the `filter` query param is accepted but ignored — event
filtering and `lazy_load_members` (the `state` field) are not implemented.*

Handler:

```rust
pub(crate) async fn get_messages(
    state: State<AppState>,
    AuthUser(user): AuthUser,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response
```

Flow:
1. Parse `room_id` → `OwnedRoomId`; invalid → 400 `M_INVALID_PARAM`.
2. **Join check** via `current_membership(&state.0, &rid, &user)`:
   - `Ok(Some(m)) if m == "join"` → proceed.
   - `Ok(_)` (absent / non-join) → 403 `M_FORBIDDEN` "You aren't a member of the room."
   - `Err(resp)` → return `resp` (ready 500).
3. Parse query params:
   - `dir`: `"f"`→`Forward`, `"b"`→`Backward`, **absent → `Forward`** (Synapse
     default; spec marks it required, we mirror Synapse's leniency), other → 400
     `M_INVALID_PARAM`.
   - `from`: absent or `"END"` → `None`; else `u64::from_str` → `PaginationToken`,
     parse error → 400 `M_INVALID_PARAM`.
   - `to`: absent or `"END"` → `None`; else parsed like `from`.
   - `limit`: absent → `10`; parse `usize`, error → 400 `M_INVALID_PARAM`;
     clamp to `1000` (`min`).
   - `filter`: ignored (no-op).
4. `store.room_messages(&rid, from, to, dir, limit)` → `(events, next)`.
   Any `Err` → 500 `M_UNKNOWN` (room existence already guaranteed by the join
   check, so the `InvalidInput` unknown-room path is unreachable here).
5. Build response JSON:
   - `chunk`: `events.iter().map(Raw::<AnyTimelineEvent>::from).collect()` — the
     `event_view` conversion that injects `event_id` + `room_id`. Order is exactly
     as `room_messages` returns it (`b` newest-first, `f` oldest-first) — **no
     reversal** (unlike sliding-sync, which reverses to oldest-first).
   - `start`: always present. `from` echoed if provided; else `Forward` → `"0"`,
     `Backward` → the current stream head (`store.subscribe().borrow().0` as a
     decimal string — the "latest" token).
   - `end`: `next.map(|t| t.0.to_string())`; **omit the key entirely when `None`**.
   - `state`: never emitted.
   - 200 OK.

### 4. Router wiring

`crates/neutrino-http/src/lib.rs` `build_router()` — add:
```rust
.route(
    "/_matrix/client/v3/rooms/{room_id}/messages",
    get(messages::get_messages),
)
```
alongside the existing `/members` and `/state` GET routes, and `mod messages;`.
This is the one allowed `lib.rs` router edit (the task requires it).

## Error mapping summary

| Condition | HTTP | errcode |
| --- | --- | --- |
| Malformed `room_id` | 400 | `M_INVALID_PARAM` |
| Bad `dir` / `from` / `to` / `limit` | 400 | `M_INVALID_PARAM` |
| Requesting user not `join`-ed (incl. unknown room) | 403 | `M_FORBIDDEN` |
| `room_messages` / membership storage fault | 500 | `M_UNKNOWN` |
| Success | 200 | — |

## Testing

New e2e file `crates/neutrino-http/tests/e2e_messages.rs` (pattern: fresh
`router(config)` over a file-backed `SqliteStore` on a `tempfile::NamedTempFile`,
as in `tests/e2e_sliding_sync.rs`). The default config user is the room creator,
so it is `join`-ed after `createRoom`. Cases:

1. **Happy path `dir=b` no `from`:** create room, send N messages → `chunk`
   newest-first, `start` present, `end` present when N > limit.
2. **Pagination roundtrip:** page 1 with `limit=2`, then page 2 using the returned
   `end` as `from` (`dir=b`) → disjoint, contiguous, terminates with no `end`.
3. **`dir=f` from `"0"`:** chunk oldest-first.
4. **`to` bound:** `from`+`to` bracket returns only the events strictly between.
5. **Sync-token interop:** take a `prev_batch` from a sliding-sync/legacy-sync
   response and use it as `from` to `/messages` → succeeds, returns older events.
6. **`limit` cap:** `limit=99999` is accepted (clamped), not an error.
7. **Not joined → 403:** a second user (multi-user-shim) who never joined →
   403 `M_FORBIDDEN`. (Feature-gated like other multi-user tests.)
8. **Unknown room → 403:** a never-created room id → 403 `M_FORBIDDEN`.
9. **Bad params → 400:** `dir=x`, non-numeric `from`, non-numeric `limit`.
10. **`filter` ignored:** request with `?filter={...}` behaves identically to no
    filter (documents the no-op).

Plus the storage-layer `to`-bound tests in §1.

## Verification

- `cargo fmt`, `cargo clippy -p neutrino-store -p neutrino-store-sqlite -p neutrino-http --tests -- -D warnings`.
- `cargo test -p neutrino-store-sqlite` (room_messages incl. new `to` tests).
- `cargo test -p neutrino-http` and the multi-user-shim test invocations for the
  403 cases.
- PLAN.md: tick the `/messages` endpoint; add the `room_messages` trait-change
  decision to the decisions log. LOG.md: 2-line change summary.

## Notes / limitations (also recorded on the handler)

- `filter` accepted but ignored; no `state` (lazy members) ever emitted.
- No history-visibility filtering; a joined user sees the full timeline chunk.
- No federation backfill: `dir=b` returns only locally-held events; an empty
  `chunk` with no `end` simply means the local timeline start was reached.
