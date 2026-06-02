# Viable Complement tests

Audit date: 2026-06-02. Re-audited after the **global `POST /_matrix/client/v3/join/{roomIdOrAlias}`** endpoint landed (the path Complement's `MustJoinRoom`/`JoinRoom` actually use — `complement-main/client/client.go:236-239`). Supersedes the 2026-05-27 audit, which predated the membership endpoints, the `/state` write routes, and the multi-user shim.

## Ground truth: what the axum router actually wires today

From `crates/neutrino-http/src/lib.rs:201-277`. The Complement image is built with **`--features multi-user-shim`** (`docker/complement/Dockerfile`), so distinct registered users get distinct tokens/identities — multi-user flows work.

| Method | Path | Notes |
|---|---|---|
| GET | `/_matrix/client/versions` | advertises `org.matrix.simplified_msc3575` + `org.matrix.msc4222` |
| GET/POST | `/_matrix/client/{version}/login` | per-user token stub (shim on) |
| POST | `/_matrix/client/{version}/register` | two-step UIA stub, per-user token |
| POST | `/_matrix/client/unstable/org.matrix.simplified_msc3575/sync` | MSC4186 |
| GET | `/_matrix/client/v3/sync` | legacy → MSC4186 translator; MSC4222 `state_after` dual-emitted; `invite`/`leave`+`ban`/`knock` buckets handled; **ignores `?filter=`** |
| POST | `/_matrix/client/v3/createRoom` | honours `preset`/`visibility`/`invite[]`(+`is_direct`)/`name`/`topic`; **drops** `room_alias_name`, arbitrary `initial_state`, `power_level_content_override`, `creation_content`, `room_version` (no rejection — unknown versions still 200) |
| GET | `/_matrix/client/v3/capabilities` | `m.room_versions.default = "12"` |
| GET | `/_matrix/client/v3/rooms/{room_id}/members` | **ignores** `at` / `membership` / `not_membership` filters |
| PUT | `/_matrix/client/v3/rooms/{room_id}/send/{type}/{msg_id}` | message events |
| PUT | `/_matrix/client/v3/rooms/{room_id}/state/{type}/{state_key}` | state **write** |
| PUT | `/_matrix/client/v3/rooms/{room_id}/state/{type}` | state write, empty key |
| POST | `/_matrix/client/v3/rooms/{room_id}/join` | room-scoped join |
| POST | `/_matrix/client/v3/join/{room_id_or_alias}` | **NEW** — global join; room **ids only** (valid alias → 404 `M_NOT_FOUND`), `server_name` ignored; idempotent re-join |
| POST | `/_matrix/client/v3/rooms/{room_id}/{leave,invite,kick,ban,unban}` | `m.room.member` via the room actor; real v12 auth (rule 5) + state-res |
| GET/POST | keys/*, profile (self), account_data (GET self), room_keys/version, pushers/set | stubs |

**Still NOT wired** (these gate tests below):
- **No GET of room state**: no `GET /rooms/{room}/state`, no `GET /rooms/{room}/state/{type}/{key}`. State is readable only via `/sync` or `GET /members`.
- No `GET /rooms/{room}/event/{eventId}`, `GET /rooms/{room}/messages`.
- No `POST /user/{uid}/filter` (+ `GET …/filter/{id}`) — blocks the large filtered-`/sync` tranche.
- No `/joined_members`, `/joined_rooms`, `/publicRooms`, `/directory/room/{alias}` (no room directory).
- No `/forget`, `/redact`, `/upgrade`, `/typing`, profile/displayname/avatar writes, account_data writes.
- 404 fallback returns plain text, not `{"errcode":"M_UNRECOGNIZED"}`.
- No EDUs / E2EE / receipts / presence / push rules; no working cross-server federation join.

**Harness constraint:** `scripts/complement.sh` runs `go test … ./tests/csapi/...` only. Tests outside `csapi` — `tests/msc4222/*` (the MSC4222 dual-emission suite), `tests/v12_test.go`, all `tests/federation_*` — are **not executed by the allowlist loop**. Running them needs a harness change (extra package globs), not just an allowlist line.

---

## Already allowlisted (`complement/allowlist.txt`)

`TestVersionStructure`; 3× `TestLogin/parallel/*`; 5× `TestRegistration/parallel/*`; `TestRoomCreate/Parallel/{makes_a_private_room, …_private_room_with_invites, …_public_room, Can_/sync_newly_created_room}`; 2× `TestRoomCreationReportsEventsToMyself/parallel/{m.room.create, m.room.member}`.

---

## Newly allowlisted, now that global `/join` (+ membership endpoints + multi-user shim) landed

Confirmed against a Complement run (2026-06-02). 12 entries are now in `allowlist.txt`; 2 tests are parked (below). The empirical run surfaced three server fixes (all applied) — these are not test-selection issues, they were real gaps:
- **empty-body POST → 400**: a bare `POST …/join` sends a 0-byte body with `application/json`; `Option<Json<_>>` 400s it. Replaced with an `OptionalBody` extractor that treats an empty body as `None`.
- **kick of a non-member → 200**: now `403 M_FORBIDDEN` ("The target user is not in the room") via a current-membership pre-check, mirroring Synapse `room_member.py:1027-1045`.
- **`PUT /state/{type}/` (trailing slash, empty key) → 404**: added the trailing-slash route (spec: "when an empty string, the trailing slash … is optional").

| Test | Verifies via | Needed fix |
|---|---|---|
| `TestRoomsInvite/Parallel/{Can_invite…, Uninvited…, Invited_user_can_reject_invite, …reject_invite_for_empty_room, Users_cannot_invite_themselves…, …already_in_the_room}` | `/sync` invite/join/leave + 403s | none (passed as-is) |
| `TestGetRoomMembers` | `GET /members` | none |
| `TestRoomMembers/Parallel/POST_/rooms/:room_id/join_can_join_a_room` | join 200 + `/sync` | empty-body extractor |
| `TestRoomMembers/Parallel/POST_/join/:room_id_can_join_a_room` | join 200 + `/sync` (global path) | empty-body extractor |
| `TestCannotKickNonPresentUser` | kick 403 | kick non-member 403 |
| `TestCannotKickLeftUser` | `/sync` + kick 403 | kick non-member 403 |
| `TestNotPresentUserCannotBanOthers` | PUT power_levels + ban 403 | trailing-slash state route |

### Parked (failed the run; not a quick fix)
- **`TestCumulativeJoinLeaveJoinSync`** — re-syncs with a stale `since` token after join→leave→join; legacy `/sync` reuses sliding-sync's *ephemeral* `pos`, so the old token → `400 M_UNKNOWN_POS` (`legacy_sync/mod.rs:66`). Needs a durable legacy stream-position token (or tolerating a stale pos as an initial sync) — see the `GET` of room state / sync-token work, not a one-liner.
- **`TestMembersLocal/*`** — *not viable*: the test fixture does `PUT /presence/{user}/status` before any subtest (`rooms_members_local_test.go:26`); presence is an out-of-scope EDU. Dropped, not parked.

Lower-confidence candidates not yet added (would need verifying they don't read `GET /state`):
- `TestRoomsInvite/Parallel/Test_that_we_can_be_reinvited_to_a_room_we_created`
- `TestTentativeEventualJoiningAfterRejecting`

---

## Still blocked, grouped by the one missing capability

### `GET` of room state (`GET /rooms/{room}/state[/{type}/{key}]`) — the biggest remaining unlock
The whole read-back surface: most of `rooms_state_test.go` and `apidoc_room_state_test.go`, all of `power_levels_test.go`, `apidoc_room_create_test.go`'s name/topic/version subtests, `TestRoomsInvite/…/Invited_user_can_see_room_metadata`, the `apidoc_room_members` invite/ban/leave/reinvite subtests, and `TestLeftRoomFixture/Can_get_…state…`. These PUT or createRoom fine but then `GET /state` to assert — which 404s.

### `POST /user/{uid}/filter` (could be a no-op opaque-id stub)
Blocks nearly all of `sync_test.go` and `sync_archive_test.go` (`TestSync/*`, `TestSyncLeaveSection/*`, `TestArchivedRoomsHistory/*`, `TestLeaveEventInviteRejection`, …) — they create a filter first and pass its id to `/sync`. A stub returning any id + `GET …/filter/{id} → {}` would unlock the tranche cheaply, since the translator already ignores `?filter=`.

### `GET /rooms/{room}/event/{eventId}` / `GET /rooms/{room}/messages`
`apidoc_room_history_visibility_test.go::TestFetchEvent`; `txnid_test.go::TestTxnInEvent`; `power_levels_test.go` (reads the PL event by id); `TestLeftRoomFixture/Can_get_…messages…`.

### `/members` query filters (`at`, `membership`, `not_membership`)
`TestGetRoomMembersAtPoint`, all `TestGetFilteredRoomMembers/*` — we ignore the filters, so the asserted subset is wrong.

### createRoom fidelity (`room_version` rejection, `initial_state`, `power_level_content_override`, `room_alias_name`)
`TestRoomCreate/…/{with a name, with a topic[…], given version, rejects …versions}`, `TestDemotingUsersViaUsersDefault`, `TestRoomState/…/GET /directory/room…`. createRoom silently drops these (and never 400s an unknown version).

### Presence / EDU / federation (out of scope per CLAUDE.md / PLAN.md)
`TestMembersLocal/…presence…`, `TestPresenceSyncDifferentRooms`, `TestSync/…presence/device-list…`, `TestSyncTimelineGap` (remote events), all `tests/federation_*`, E2EE/device-list/key-backup, account-data/push/ignored-users, media/search/directory, aliases/spaces, `/forget`/`/redact`/`/upgrade`/`/typing`.

---

## Cheap-win ordering (impact / effort)

1. **Empirically run the 15 "newly viable" candidates above; allowlist the green ones.** Headline: `TestCumulativeJoinLeaveJoinSync` + the `TestRoomsInvite/Parallel` invite-flow subtests. (Effort: 0 code; one Complement run.)
2. **No-op `POST /user/{uid}/filter` stub** → unlocks the `TestSync/*` + `TestSyncLeaveSection/*` tranche. (~30 lines.)
3. **`GET /rooms/{room}/state[/{type}/{key}]`** → unlocks the entire state read-back surface (`rooms_state`, `apidoc_room_state`, `power_levels`, createRoom name/topic, apidoc_room_members invite/ban/leave). Largest single unlock.
4. **`GET /rooms/{room}/event/{eventId}`** → history-visibility fetch + power-levels-by-id tests.
5. **Teach `scripts/complement.sh` to also run `./tests/msc4222/...`** → the marquee MSC4222 `state_after` dual-emission validation (`tests/msc4222/TestSync/*`), currently unreachable because the runner is csapi-only.
6. **404 fallback → `{"errcode":"M_UNRECOGNIZED"}`** → `TestUnknownEndpoints/*`, and correct per spec.
