# Client-Server membership endpoints — design

**Date:** 2026-06-02
**Status:** approved (design)
**Scope:** the six CSAPI membership-change POST endpoints (`/join`, `/invite`,
`/leave`, `/kick`, `/ban`, `/unban`) plus honouring the `invite` list in
`/createRoom`. Federation join/leave (`make_join`/`send_join`/…) is a separate
plan (PLAN.md "Server-Server invite/join/leave"); this is Client-Server only.

## Problem

Neutrino can create rooms and send timeline/state events, and the multi-user
identity shim now attributes each request to a distinct local user. But there
is no way for one user to change another user's (or their own) membership: the
`m.room.member`-emitting CSAPI endpoints don't exist. Without them, the
Complement membership tests (invite → join → leave → kick → ban flows) are
unreachable, and `/createRoom`'s `invite` list is silently ignored, so even
the common "create a room and invite Bob in one call" pattern doesn't work.

Everything underneath is already in place:

- The per-room actor (`RoomRegistry::send_event`, `crates/neutrino-http/src/room_actor.rs`)
  builds a local event on the room's current heads, runs it through
  `RoomCore::apply_pdu`, and persists the result.
- `apply_pdu` computes `auth_events` and enforces the full v12 auth ruleset,
  including rule 5 in all its arms — 5.3 join, 5.4 invite, 5.5 leave/kick,
  5.6 ban, 5.7 knock (`crates/neutrino-state/src/auth_rules.rs`).
- `GET /_matrix/client/v3/rooms/{roomId}/members` already reads back the
  resolved member state.

So a membership endpoint is a thin handler: decide the `(target, membership)`
pair from the request, emit one `m.room.member` state event through the actor,
and shape the response. Authorisation, state resolution, DAG linkage, and
persistence all happen inside the actor path unchanged.

## Goals

- The six membership-change endpoints below are reachable over CSAPI and emit
  correctly-shaped `m.room.member` state events that pass (or are correctly
  rejected by) the existing v12 auth rules.
- `/createRoom` emits an invite `m.room.member` event for each entry in the
  request's `invite` array, authored by the creator.
- No change to the actor, `RoomCore`, the auth rules, or the storage trait —
  this task only adds HTTP handlers and one `build_initial_events` extension.

## Non-goals

- `/forget` (purely local membership bookkeeping, emits no event) — deferred.
- `POST /join/{roomIdOrAlias}` (alias resolution + `server_name` federation
  routing) — deferred with the rest of federation join.
- Federation delivery of membership events to remote servers — trusted
  single-server; any well-formed MXID is accepted as a target, but an invite
  to a user on another server simply won't be delivered (irrelevant for the
  in-scope Complement single-server tests).
- Carrying `displayname` / `avatar_url` from the target's prior member event
  into a kick/ban event — the auth rules don't require it; minimal content is
  emitted.
- Adding auth (`AuthUser`) to the pre-existing `GET …/members` handler — left
  exactly as today.
- Honouring `membership` / `not_membership` query filters, `power_level_content_override`,
  `trusted_private_chat`'s invitee-power bump — all remain out of scope as
  before.

## Components

### 1. New module `crates/neutrino-http/src/membership.rs`

Holds the six handlers and one shared helper. Consistent with the existing
submodule layout (`legacy_sync`, `federation`, `room_actor`, `sliding_sync`,
`multi_user`). Keeps `lib.rs` (already ~1k lines) from growing.

`lib.rs::room_actor_response` is promoted from private to `pub(crate)` so the
membership handlers can reuse the exact `RoomActorError → HTTP` mapping
(`UnknownRoom`→404 `M_NOT_FOUND`, `Build`→400 `M_BAD_JSON`,
`Apply`/`Rejected`→403 `M_FORBIDDEN`, else→500 `M_UNKNOWN`). The handlers do
**not** reuse `send_via_actor` because that hardcodes a `{ "event_id": … }`
response body, whereas membership endpoints return `{ "room_id": … }` (join)
or `{}` (the rest).

### 2. Shared helper

```rust
/// Emit one `m.room.member` event through the room actor and return the
/// membership response. `target` is the state_key (the user whose membership
/// changes); `membership` is the resulting membership string; `reason` (if
/// present in the request body) is copied into content.
async fn change_membership(
    state: &AppState,
    sender: OwnedUserId,
    room_id: String,
    target: OwnedUserId,
    membership: &str,
    reason: Option<String>,
) -> Result<Arc<Event>, MembershipOutcome> { … }
```

It parses `room_id`, builds `content = { "membership": <membership> }` plus
`"reason"` when supplied, calls `registry.send_event(room, sender,
"m.room.member", Some(target.to_string()), content)`, and returns the applied
event on success. The thin per-endpoint handlers shape the HTTP response from
that (join → `{ room_id }`, others → `{}`) and map errors via
`room_actor_response`. A small private enum or `Result` carries the room-id
parse error (400 `M_INVALID_PARAM`) distinctly from actor errors.

### 3. The six handlers

All `async fn (State<AppState>, AuthUser(sender), Path(...), Json<Value>)`.

| Endpoint (POST) | target (state_key) | membership | success body |
|---|---|---|---|
| `…/rooms/{roomId}/join` | `sender` | `join` | `{ "room_id": <roomId> }` |
| `…/rooms/{roomId}/leave` | `sender` | `leave` | `{}` |
| `…/rooms/{roomId}/invite` | `body.user_id` | `invite` | `{}` |
| `…/rooms/{roomId}/kick` | `body.user_id` | `leave` | `{}` |
| `…/rooms/{roomId}/ban` | `body.user_id` | `ban` | `{}` |
| `…/rooms/{roomId}/unban` | `body.user_id` | `leave` | `{}` |

- `join` / `leave` ignore the body for the target (the target is always the
  caller); `leave` still lifts an optional `reason`.
- `invite` / `kick` / `ban` / `unban` read `body.user_id` (required) and parse
  it as an `OwnedUserId`; absent or malformed → 400 (`M_MISSING_PARAM` /
  `M_INVALID_PARAM`). They also lift an optional `reason` (not `unban`, which
  per spec takes only `user_id`).
- `unban` and `kick` both emit `membership: leave`; the auth-rule arm that
  applies (5.5.3 unban vs 5.5.4 kick) is selected by `RoomCore` from the
  target's current membership, so the endpoints need no extra signalling.

### 4. Route registration

Six `.route(…, post(handler))` lines added to the router builder in `lib.rs`
alongside the existing `/createRoom` and `/send` routes. Paths use the
`{room_id}` path-parameter style already in use.

### 5. `/createRoom` invite list

`build_initial_events` (`lib.rs`) gains a final step after the standard batch
(create → creator-join → power_levels → join_rules → optional name/topic):
for each well-formed user id in the request body's `invite` array, emit an
`m.room.member` invite event via the existing `add(...)` closure
(`state_key = target`, `content = { "membership": "invite" }`, plus
`"is_direct": true` when the request body sets `is_direct`). These are authored
by the creator, who is joined and holds implicit MAX power, so they pass rule
5.4. Malformed entries in the `invite` array are skipped (best-effort, test
server); a fully malformed `invite` value (not an array) is ignored.

## Data flow

```
POST /rooms/{id}/invite  {user_id:"@bob:server"}   (alice's token)
  -> AuthUser -> @alice
  -> change_membership(@alice, id, @bob, "invite", None)
  -> RoomRegistry::send_event(id, @alice, "m.room.member", Some("@bob:server"),
                              {membership:"invite"})
  -> actor: build_local_event -> apply_pdu (rule 5.4: alice joined + invite power) -> persist
  -> 200 {}

POST /rooms/{id}/join                               (bob's token)
  -> AuthUser -> @bob
  -> change_membership(@bob, id, @bob, "join", None)
  -> actor: rule 5.3 (join_rule=invite ⇒ bob must have prior invite — he does)
  -> 200 {room_id: id}
```

## Error handling

All via the existing helpers; no new error enum on the wire.

- Unknown / unparseable `room_id` → 400 `M_INVALID_PARAM`.
- Missing `user_id` (invite/kick/ban/unban) → 400 `M_MISSING_PARAM`;
  malformed → 400 `M_INVALID_PARAM`.
- Room not bootstrappable (doesn't exist locally) → 404 `M_NOT_FOUND`
  (`RoomActorError::UnknownRoom`).
- Auth-rule rejection (join invite-only without invite, invite a joined/banned
  user, kick/ban without power, unban without ban power) → 403 `M_FORBIDDEN`
  (`RoomActorError::Apply`/`Rejected`).
- Storage / actor faults → 500 `M_UNKNOWN`.

`/join` on a room the caller is already in is idempotent at the HTTP level: it
emits a fresh `join` member event (join→join is auth-valid) and returns
`{ room_id }`.

## Testing & verification

New e2e tests in `crates/neutrino-http/tests/e2e_multi_user.rs` (gated
`#[cfg(feature = "multi-user-shim")]`, the only place two distinct users
exist), run with `cargo test -p neutrino-http --features multi-user-shim`:

1. **invite → join visibility:** alice creates an invite-only room, invites
   bob; bob's sync shows the room as an invite; bob joins; bob's sync now shows
   it as joined and `GET …/members` lists bob with `membership: join`.
2. **join public room without invite:** alice creates a `public_chat` room; bob
   joins directly → 200 `{ room_id }`.
3. **join invite-only without invite → 403.**
4. **kick:** alice invites+`/join`s bob, then kicks him; bob's current
   membership is `leave`; bob can be re-invited.
5. **ban blocks rejoin:** alice bans bob; bob's `/join` → 403; after `/unban`,
   bob can be invited and join again.
6. **self leave:** bob joins then `/leave`s; his membership is `leave`.
7. **createRoom with invite list:** alice creates a room with
   `invite: ["@bob:server"]`; bob's sync surfaces the invite without any
   explicit `/invite` call.

Porting the relevant Synapse `tests/rest/client/test_rooms.py` membership
assertions and the matching Complement `csapi` membership cases is **out of
scope for this PR** — it lands in a separate follow-up PR.

Feature-off regression: the existing `neutrino-http` suite (no `Authorization`
header, single configured user) stays green; the new handlers compile and are
routed in both feature states (they take `AuthUser`, which resolves the
configured user when the shim is off).

CI: covered by the existing `--features multi-user-shim` lane (build + clippy
`-D warnings` + test); clippy must stay clean in both feature states.

## Affected files (anticipated)

- `crates/neutrino-http/src/membership.rs` (new) — six handlers + helper.
- `crates/neutrino-http/src/lib.rs` — `mod membership;`, six routes,
  `room_actor_response` → `pub(crate)`, `build_initial_events` invite-list
  extension.
- `crates/neutrino-http/tests/e2e_multi_user.rs` — new membership e2e tests.
- `PLAN.md` / `LOG.md` — status + decision/summary entries per project rules.

## Open questions

None outstanding. (Federation membership, `/forget`, and join-by-alias are
deliberately deferred to their own plans.)
