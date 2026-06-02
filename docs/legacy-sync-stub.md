# Legacy `/sync` stub over sliding-sync (MSC4222 semantics)

Status: implemented (2026-05-26). The handler lives at `crates/neutrino-http/src/legacy_sync/`; the wildcard / dual-emission / knock decisions called out below in the original sketch were all carried through. See the PLAN.md decisions log entry of 2026-05-26 for the build-out detail and any deviations from this sketch.

## Goal

Add a `GET /_matrix/client/v3/sync` stub that translates legacy CSAPI sync
requests into MSC4186 sliding-sync calls and translates the response back into
the v3 shape. Returns state under the MSC4222 `state_after` field (and
optionally also `state`) since sliding-sync's `required_state` is already
state-after-the-event semantically — there is no impedance mismatch.

Motivation: unblock complement tests that use `MustSync` / `MustSyncUntil` /
`SendEventSynced` as a sync barrier. Many CSAPI tests are written against
legacy `/sync` and there is no other path to make them pass without writing
the legacy machinery from scratch.

This stub does **not** unlock tests that require distinct users (that's
blocked by `post_register` returning a fixed `app.config.user_id()`), nor
tests that hit endpoints we don't have (`/join`, `/invite`, `/leave`, `/ban`,
`/messages`, `/event/{id}`, `/whoami`).

## Why MSC4222 fits

Legacy `/sync` historically delivers `state` = state changes between the
prior sync and the **start** of the timeline. MSC4222 replaces that with
`state_after` = state changes between the prior sync and the **end** of the
timeline.

Sliding-sync's `required_state` field is "current state of the room as of
the events we just returned". That is precisely state-after semantics.
Without MSC4222 we'd have to either (a) recompute state-at-start-of-batch
or (b) ship an incorrect `state` field. With MSC4222 we put the data we
already have under the `state_after` key and the semantics line up.

MSC4222 reference: <https://github.com/matrix-org/matrix-spec-proposals/pull/4222>

## Endpoint and routing

- Route: `GET /_matrix/client/v3/sync`
- Handler: new `legacy_sync` in `crates/neutrino-http/src/lib.rs`,
  parallel to the existing `sync` (sliding-sync) handler.
- Reuses `AppState::sync_state` and calls `sliding_sync::handle` directly
  so the existing long-poll, cancellation, and `(pos, body_hash)`
  idempotency cache machinery applies for free.

## Query parameter mapping (v3 → v5::Request)

| v3 query param | v5::Request field | Notes |
|---|---|---|
| `since` | `pos` | Pass through verbatim. Both opaque strings; sliding-sync's pos format happens to be a stringified `u64`, but clients don't care. **Durable-token caveat:** sliding-sync's `pos` is an *ephemeral, single-cursor* per-connection value — it rejects anything but the last-issued value with `UnknownPos`. Legacy `since` tokens are durable (a client may replay any past token). To bridge the mismatch, an unknown/stale `since` does **not** 400 with `M_UNKNOWN_POS`; `handle` retries once with `pos = None`, i.e. a full initial sync that returns current state under a fresh token. A stale token therefore collapses to "state now" rather than a faithful cumulative delta — adequate for the embedded single client and for `TestCumulativeJoinLeaveJoinSync` (which only asserts current membership), but real incremental replay would need a durable stream-position token decoupled from sliding-sync's `pos`. |
| `timeout` (ms) | `timeout` (`Duration`) | Default 0 if absent. |
| `filter` | dropped | Legacy passes JSON or a numeric server-side filter id. Incompatible model. Most complement tests don't rely on filter behavior. |
| `full_state=true` | noop | We already emit current state on every sync via the wildcard `required_state`. |
| `set_presence` | noop | No presence implementation. |
| `use_state_after` / `org.matrix.msc4222.use_state_after` | controls response shape | See below. |

Synthesized into the v5 request:

- `conn_id = Some("__legacy__")` — distinct namespace so legacy and any real
  sliding-sync calls never collide in the `ConnRegistry`.
- `lists`: one list named `"all"` with:
  - `ranges: vec![(0u32, u32::MAX)]`
  - `required_state: vec![(StateEventType::from("*"), "*".into())]` (wildcard)
  - `timeline_limit: 50`
- `room_subscriptions: BTreeMap::new()`
- `extensions: Default::default()`

**Resolved:** `StateEventType::from("*")` round-trips cleanly. The existing
sliding-sync tests (`sliding_sync/tests.rs::required_state_wildcard_*`)
already exercise this exact pair through `request::List::room_details
::required_state` and `build::required_state_matches`; no fallback to
explicit enumeration is needed. The implementation uses the wildcard.

## Response translation (v5::Response → v3 JSON)

Top-level mapping:

- `pos` → `next_batch`
- `rooms` → buckets `rooms.join`, `rooms.invite`, `rooms.leave`, `rooms.knock` (see below)
- Stubs:
  - `presence: {"events": []}`
  - `account_data: {"events": []}`
  - `to_device: {"events": []}`
  - `device_lists: {"changed": [], "left": []}`
  - `device_one_time_keys_count: {}`

### Per-room bucketing

Sliding-sync returns a flat `rooms` map keyed by room id. The v3 shape
needs each room bucketed by current membership. Query
`store.rooms_with_membership(user_id, &all_memberships)` once at the start
of the response build and bucket from that map. Avoids re-walking JSON
to find the user's `m.room.member` event in `required_state`.

### Join / leave room shape

```json
{
  "timeline": {
    "events": [<from v5 room.timeline>],
    "limited": false,
    "prev_batch": ""
  },
  "state": {
    "events": [<from v5 room.required_state>]
  },
  "org.matrix.msc4222.state_after": {
    "events": [<same as state.events>]
  },
  "ephemeral": {"events": []},
  "account_data": {"events": []}
}
```

- `prev_batch` is empty string. `/messages` isn't implemented; the token
  is never redeemed.
- `state` and `state_after` carry identical data because our underlying
  source is current state (= state-after). Emitting both gives max compat:
  MSC4222-aware clients (our FFI app) read `state_after`; non-aware
  clients (complement test framework) read `state`.
- Strict spec behavior would emit `state_after` only when the client
  passes `?use_state_after=true`. The dual-emission approach is non-strict
  but lossless and ~3 LOC simpler. Recommended unless a complement test
  is found that asserts on the *absence* of `state_after`.

### Invite room shape

```json
{
  "invite_state": {
    "events": [<from v5 room.invite_state>]
  }
}
```

Sliding-sync's invite-room representation already matches the v3
stripped-state shape — lift `invite_state` directly with no
transformation.

### Leave room inclusion

Sliding-sync gates leave-room inclusion through `include_room_per_msc4186`
in `sliding_sync/build.rs` (kicks always included; self-leave / ban only
if previously emitted on the same conn). Inherit this — if a leave room
shows up in the v5 response, surface it in `rooms.leave`.

### Knock room shape

```json
{
  "knock_state": {
    "events": [<stripped {type, state_key, sender, content} from v5 required_state>]
  }
}
```

Knock rooms are not in the original three-bucket enumeration above but the
upstream `candidate_rooms` *does* include `Membership::Knock`
(`sliding_sync/build.rs:292,311`), so they reach the translator and would
otherwise need to be silently dropped. We surface them under `rooms.knock`
with the v3 spec's `knock_state.events` shape.

Stripping: v5's `Room.required_state` carries full
`Raw<AnySyncStateEvent>` values (with `event_id`, `origin_server_ts`,
`unsigned`, `prev_content`, `room_id`). The v3 spec defines `knock_state`
as stripped state — `{type, state_key, sender, content}` only. Ruma
0.15 has no `JsonCastable` impl from the type-erased `AnySyncStateEvent`
enum to `AnyStrippedStateEvent` and no general `fn strip()`; `Raw::cast_unchecked`
would emit the full event JSON under a stripped type label (a soft lie about
the bytes), so the translator reshapes the JSON manually via
`translate::strip_state_event` — parse, pull the four canonical keys, emit
a new object. Missing canonical fields are dropped silently.

The upstream sliding-sync handler's `is_invited` check
(`sliding_sync/build.rs:647-663`) matches only `membership == "invite"`, so
knock rooms go through the non-invite build path and do **not** get their
state put into `v5::Room.invite_state`. Our wildcard `required_state`
captures the state events into `Room.required_state` instead, which is
where the stripper reads from.

## `/versions` advertisement

Add to the existing `versions` handler:

```rust
"unstable_features": {
    "org.matrix.msc4222": true
}
```

So MSC4222-aware clients know to send `?use_state_after=true`.

## State events in both `timeline` and `state_after`

MSC4222: "Clients MUST only update their local state using state_after and
NOT consider the events that appear in the timeline section." State events
that land in the timeline window appear in both. Sliding-sync's `timeline`
already includes state events when they're in the window, and
`required_state` carries current state. No special handling needed —
emit both as-is.

## Conn state and idempotency

- `conn_id = "__legacy__"` gives each legacy client its own
  `(user_id, "__legacy__")` entry in the registry.
- Cancellation: a fresh legacy sync request with no `pos` cancels the
  prior in-flight one (existing behavior).
- Idempotency cache keys on `(pos, body_hash)`. Legacy `/sync` is a GET
  with no body, so `body_hash` is constant for a given query string —
  effectively "same `since` + same query params → same response bytes".
  Matches v3 expectations.
- The `Conn::sent` map (used to track "previously emitted" rooms for
  MSC4186 leave-inclusion gating) piggybacks on the same machinery.

## What it unlocks

Single-user complement tests that use legacy `/sync` as a sync barrier:

- `TestRoomCreate/Parallel/Can_/sync_newly_created_room` — alice creates a
  room and `SendEventSynced` waits for the event to appear in
  `rooms.join.{id}.timeline.events`.
- `TestRoomCreate/Parallel/POST_/createRoom_ignores_attempts_to_set_the_room_version_via_creation_content`
   — alice creates with `creation_content.room_version: "test"`, reads
  `timeline.events.0` from full sync. Requires `create_room` to merge
  `creation_content` (other than `room_version`) into the create event;
  currently it doesn't. Bundled fix needed.

Combined with the three cheap-win fixes already identified
(echo `device_id` in `post_login`; reject non-v12 `room_version` in
`create_room`; wire `GET /rooms/{id}/state/{type}/{key}` + persist
`topic` in `create_room`):

- `TestLogin/parallel/POST_/login_returns_the_same_device_id_as_that_in_the_request`
- `TestRoomCreate/.../rejects_attempts_to_create_rooms_with_numeric_versions`
- `TestRoomCreate/.../rejects_attempts_to_create_rooms_with_unknown_versions`
- `TestRoomCreate/.../makes_a_room_with_a_topic`
- `TestRoomCreate/.../makes_a_room_with_a_name`

Total addressable from this batch of work: ~7 additional allowlisted
subtests.

## What it does NOT unlock

- Anything requiring distinct users (most TestRoomCreate invite tests,
  TestGetRoomMembers, TestTxn*, TestMembersLocal). Blocked by the fixed
  `user_id` in `post_register`.
- Anything hitting `/join`, `/invite`, `/leave`, `/ban`, `/messages`,
  `/event/{id}`, `/whoami`, `/account/whoami`. Endpoints not wired.
- Tests scanning `summary.m.heroes` or `summary.m.joined_member_count`.
  We'd return no `summary`.
- `lazy_load_members` semantics — filter is dropped.

## Risks and caveats

1. **Filter dropped silently.** Tests that pass a strict filter expecting
   it to be honored will see a fatter response than they wanted. Should
   still pass assertions because they're typically over-specified, not
   restrictive.
2. **`prev_batch` per room is empty.** A test that tries to paginate
   backwards via `/messages` would fail — but `/messages` isn't wired,
   so that test would fail upstream anyway.
3. **Strict MSC4222 spec compliance vs. dual-emission.** The MSC says
   the server emits `state_after` *only* when the client opts in. We're
   recommending unconditional dual emission for compat. Revisit if a
   real MSC4222-aware client breaks on seeing both fields (it shouldn't
   per the MSC — extra unknown fields are ignored).
4. **MSC4222 unstable name might shift.** Use
   `org.matrix.msc4222.state_after` until the MSC stabilizes; flip to
   plain `state_after` then. Same pattern as
   `org.matrix.msc4242.12` already in `neutrino-common`.

## Cost estimate

- New handler + response translation: ~120–150 LOC.
- `/versions` `unstable_features` advertisement: ~3 LOC.
- Tests: round-trip unit tests for the response translation + an e2e
  test using the existing `httptest` harness. ~60 LOC.
- No changes to sliding-sync internals.

## Implementation order

1. Land the three cheap-win fixes independently (each unlocks 1–2 tests
   without depending on this stub).
2. Land this stub. Grow the allowlist by the four tests it unlocks.
3. Land `create_room` `creation_content` merge fix; allowlist the
   `creation_content` test.

Each step is independently mergeable and each grows the allowlist.

## Out of scope for this stub

- Implementing legacy `/sync` end-to-end (filters, presence, account_data,
  to_device, device_lists, etc.) — pure stub.
- Implementing `/messages`, `/event/{id}`, `/state/{type}/{key}` (the
  last is one of the cheap wins but tracked separately).
- Distinguishing MSC4222-aware vs non-aware clients — we emit both
  fields unconditionally.
- Federation — orthogonal.
