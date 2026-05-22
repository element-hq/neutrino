# MSC4186 sliding sync — current limitations

Living inventory of what `crates/neutrino-http/src/sliding_sync/` does **not** do, why, and (where applicable) what would unblock it. Entries fall into three buckets:

- **Decided out of scope** — intentional gaps per CLAUDE.md or a PLAN.md decision. Won't be filled in unless the scope changes.
- **Blocked on external** — needs ruma to expose a new field, or a `neutrino-common::storage` trait addition (which requires a decisions-log entry first).
- **Known caveats** — real edge cases or implementation details a consumer should be aware of, even if they're not strict spec gaps.

Everything previously tagged "closed in phase N" has been removed — see `LOG.md` for that history.

---

## Decided out of scope

### Filters ignored
`is_dm`, `is_encrypted`, `is_invite`, `room_types`, `not_room_types`, `spaces`, `tags`, `not_tags`.
`apply_sticky` parses them into `ListCfg.filters` but no code reads it back. `candidate_rooms` returns every joined+invited room. Decided in PLAN.md 2026-05-14 — the embedded single-user server doesn't need server-side filtering.

### `set_presence` ignored
Parsed by ruma and silently dropped. No presence layer in the server.

### Lazy-loaded members not supported
`lazy_members: true` would return only the senders of timeline events. We always return whatever `required_state` matches. PLAN.md — embedded server, little to gain.

### `required_state` special tokens (`$LAZY`, `$ME`) not supported
Treated as literal strings by `required_state_matches`. `$LAZY` is paired with lazy-members which is out of scope; `$ME` is implementable in ~10 lines if a client ever sends it.

### Heroes not emitted
Server-computed user list for room-name fallback. Clients with no `m.room.name` and no other context may render the raw room ID. Re-evaluate if a target client's DM display breaks visibly.

### `unread_notifications` (notification / highlight counts) not emitted
MSC4186 explicitly removed this versus MSC3575; ruma v5 still exposes the fields. We leave them empty.

### `is_dm` field on `response::Room` not emitted
Sources from account_data `m.direct`, which we don't model.

### `account_data` / `receipts` / `typing` extensions silently dropped
No EDUs, no account data. Only `e2ee` and `to_device` get the echo stub treatment (request `enabled: true` → response carries a fixed-shape payload), so clients that depend on them seeing *something* don't crash.

### Real e2ee bootstrap
The `e2ee` echo stub gets a client past the initial handshake (it sees a `signed_curve25519: 100` OTK count and a fallback-key entry), but actual key exchange needs real to-device delivery, which needs federation EDUs — explicitly out of scope. Encrypted messages will *send* but recipients won't decrypt without manual key sharing.

### State stubs for deleted state, gated off by default
Detection works (`diff_required_state` produces `deleted_state_keys`; covered by `unit_tests::diff_required_state_detects_deletion`), but the wire emission is gated behind a hardcoded `EMIT_STATE_STUBS: bool = false` in `build.rs`. Practical failure mode: a client keeps its last view of a removed state key until the room re-sets it to something new (at which point the normal diff path kicks in). Flip the const to `true` when a target client confirms it needs stubs.

---

## Blocked on external

### `expanded_timeline` not emitted
MSC4186: when the client increases `timeline_limit`, resend the older events with `expanded_timeline: true`. `Conn.prev_list_timeline_limits` tracks the previous limit per list, but ruma 0.15.1's `sync_events::v5::response::Room` has no `expanded_timeline` field, so there's nowhere to put it. **Blocked on ruma v5.**

### `lists` array on `response::Room` not emitted
MSC4186 addition. Ruma v5 doesn't model it. **Blocked on ruma v5.**

### `membership` field on `response::Room` not emitted
Same — MSC4186 addition that ruma v5 doesn't model. **Blocked on ruma v5.**

---

## Known caveats

### Ruma v5 wire dialect is half-migrated to MSC4186
ruma 0.15.1's `sync_events::v5` still uses several MSC3575-era shapes:
- `required_state: Vec<(StateEventType, String)>` rather than MSC4186's `{include, exclude}` object.
- `invite_state` (MSC3575 name) rather than MSC4186's `stripped_state`.
- `ranges: Vec<(UInt, UInt)>` (plural) rather than MSC4186's singular `range`.

We accept this — it's what the target client sends. For `ranges` specifically `apply_sticky` honours only `ranges[0]` and silently drops any extras (covered by `multi_range_request_only_honours_first`). Re-evaluate when ruma fully migrates.

### `compute_bump_stamp` is O(n) storage round-trips per sync
`candidate_rooms` calls `compute_bump_stamp` once per joined+invited room — one `room_messages(.., Backward, 1)` plus possibly one `current_state_event("m.room.create", "")` per room. Linear in candidates, re-done on every sync. Fine for embedded single-user scale; would be the first thing to optimise at scale. Three plausible directions, listed in `compute_bump_stamp`'s doc comment in `build.rs`:
1. Cache `room_id → bump_stamp` in `SyncState`, invalidate on `EventStore::subscribe()` wakeups.
2. Add a batched `StateStore::room_bump_stamps(rooms)` trait method (trait change — ask).
3. Maintain a `bump_stamp` column on the rooms table updated transactionally in `EventStore::persist_event`. Cleanest long-term.

### `bump_stamp` is `None` for rooms with truly no stored state
`compute_bump_stamp` falls back to the `m.room.create` event's `origin_server_ts` when there's no other event; rooms with neither (effectively only externally-invited rooms whose state we haven't received) get `0` → `None` and sort to the bottom. Unlikely in practice — the invite member event itself is enough to seed the timestamp.

### `bump_stamp` doesn't filter by event type
Synapse's `bump_event_types` excludes reactions/redactions; we bump on any event's `origin_server_ts`. Reaction-spam could nominally re-order rooms. Not a problem for the embedded use case but worth knowing.

### `invite_state` not re-emitted across syncs
`build_invite_room` only emits the stripped state on the first sync after an invite arrives (and never again until the user accepts/rejects). If the inviter renames the room or changes the avatar after sending the invite, the invitee won't see the update until they accept. The `invite_room_state` blob is fixed at invite time on the federation side so this matches the canonical S2S model.

### Ban / self-leave inclusion uses a "previously emitted" approximation
MSC4186 §"Rooms included in the server list" wants:
- self-leave: include if previously sent to this connection,
- ban: include if the user previously joined the room.

We approximate "previously joined" with "previously emitted on this conn" (`conn.sent.contains_key`) because we don't keep a separate per-conn record of historical join events. The approximation is exact within a single connection's lifetime; a fresh conn after a server restart will not include rooms the user was banned from before the restart. For an embedded single-user server this trade-off is acceptable. Covered by `banned_room_only_appears_if_previously_emitted` (negative) and `banned_room_remains_visible_after_being_emitted_while_joined` (positive).

### `joined_count` accuracy depends on the storage backend
`populate_room_metadata` derives `joined_count` from `StateStore::joined_members(room).len()`. The live `SqliteStore` populates this from the `current_state.membership` column, indexed and filtered to `"join"` — accurate as long as `persist_event` is the only path that updates membership.

### `has_data` in the long-poll loop is intentionally narrow
The loop returns early when `!resp.rooms.is_empty()`. It does NOT wake on OTK / device-list changes, to-device messages, account-data updates, receipts, typing, or list `count` shifts. Safe today because all of those are stubbed or dropped; if real extensions are ever wired in, `mod.rs::has_data` is the single place to expand. The function's doc comment is the canonical list.

### `pos` is a per-conn sequence counter, not a stream position
The token clients receive is `conn.pos`, incremented per response. It's opaque and locally meaningful only. Idempotency cache covers the most-recent processed input pos only; older pos values get `M_UNKNOWN_POS` even if the client *just* dropped them.

### `ConnRegistry` has no eviction
Connections live forever until the process restarts. Each carries a cached `last_response` (~hundreds of bytes plus the room snapshot). Single-user mobile scale → not a concern; a multi-user deployment would need an LRU before shipping.

### Initial sync `num_live` is `None`
Live events on delta syncs report `num_live = timeline.len()`. The first sync intentionally returns `None`: a client just loading state isn't being notified of "new" activity.
