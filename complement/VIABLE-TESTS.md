# Viable Complement tests

Audit date: 2026-06-02. Re-audited after the **global `POST /_matrix/client/v3/join/{roomIdOrAlias}`** endpoint landed (the path Complement's `MustJoinRoom`/`JoinRoom` actually use — `complement-main/client/client.go:236-239`). Supersedes the 2026-05-27 audit, which predated the membership endpoints, the `/state` write routes, and the multi-user shim.

**Update 2026-06-03:** GET room state landed and was allowlisted (13 entries); a second allowlist pass then added 4 more tests and dropped 2 as v12/design-incompatible — see the "Second allowlist pass — 2026-06-03" section below. The router table and the "Still NOT wired" list have been updated to match (GET state is now wired).

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
| GET | `/_matrix/client/v3/rooms/{room_id}/state` | **NEW (2026-06-03)** — full current-state array |
| GET | `/_matrix/client/v3/rooms/{room_id}/state/{type}/{state_key}` (+ `/{type}`, + trailing slash) | **NEW (2026-06-03)** — state read; content default / full event on `?format=event` (validated → 400); `404 M_NOT_FOUND` on missing |
| POST | `/_matrix/client/v3/rooms/{room_id}/join` | room-scoped join |
| POST | `/_matrix/client/v3/join/{room_id_or_alias}` | **NEW** — global join; room **ids only** (valid alias → 404 `M_NOT_FOUND`), `server_name` ignored; idempotent re-join |
| POST | `/_matrix/client/v3/rooms/{room_id}/{leave,invite,kick,ban,unban}` | `m.room.member` via the room actor; real v12 auth (rule 5) + state-res |
| GET/POST | keys/*, profile (self), account_data (GET self), room_keys/version, pushers/set | stubs |

**Still NOT wired** (these gate tests below):
- No `GET /rooms/{room}/event/{eventId}`. (`GET /rooms/{room}/messages` landed 2026-06-04 — join-gated pagination.)
- No `POST /user/{uid}/filter` (+ `GET …/filter/{id}`) — blocks the large filtered-`/sync` tranche.
- No `/joined_members`, `/joined_rooms`, `/publicRooms`, `/directory/room/{alias}` (no room directory).
- No `/forget`, `/redact`, `/upgrade`, `/typing`, profile/displayname/avatar writes, account_data writes.
- 404 fallback returns plain text, not `{"errcode":"M_UNRECOGNIZED"}`.
- No EDUs / E2EE / receipts / presence / push rules; no working cross-server federation join.

**Harness constraint:** `scripts/complement.sh` runs `go test … ./tests/csapi/...` only. Tests outside `csapi` — `tests/msc4222/*` (the MSC4222 dual-emission suite), `tests/v12_test.go`, all `tests/federation_*` — are **not executed by the allowlist loop**. Running them needs a harness change (extra package globs), not just an allowlist line. (2026-06-03: the CI `complement` job now builds the neutrino image with buildx + GitHub Actions layer cache, so the cargo-chef deps layer persists across runs; `scripts/complement.sh` reuses a pre-built image via `SKIP_IMAGE_BUILD`.)

---

## Already allowlisted (`complement/allowlist.txt`)

`TestVersionStructure`; 3× `TestLogin/parallel/*`; 5× `TestRegistration/parallel/*`; `TestRoomCreate/Parallel/{makes_a_private_room, …_private_room_with_invites, …_public_room, Can_/sync_newly_created_room}`; 2× `TestRoomCreationReportsEventsToMyself/parallel/{m.room.create, m.room.member}`.

---

## Newly allowlisted, now that global `/join` (+ membership endpoints + multi-user shim) landed

Confirmed against a Complement run (2026-06-02). 13 entries are now in `allowlist.txt`; 1 test dropped as not viable (below). The empirical run surfaced four server fixes (all applied) — these are not test-selection issues, they were real gaps:
- **empty-body POST → 400**: a bare `POST …/join` sends a 0-byte body with `application/json`; `Option<Json<_>>` 400s it. Replaced with an `OptionalBody` extractor that treats an empty body as `None`.
- **kick of a non-member → 200**: now `403 M_FORBIDDEN` ("The target user is not in the room") via a current-membership pre-check, mirroring Synapse `room_member.py:1027-1045`.
- **`PUT /state/{type}/` (trailing slash, empty key) → 404**: added the trailing-slash route (spec: "when an empty string, the trailing slash … is optional").
- **stale legacy `since` token → `400 M_UNKNOWN_POS`**: legacy `/sync` maps `since` onto sliding-sync's *ephemeral* per-connection `pos`, which rejects anything but the last-issued value. Legacy tokens are durable, so the wrapper now recovers an unknown/stale token with a full initial sync instead of 400ing (`legacy_sync/mod.rs`). Stale tokens collapse to "state now" rather than a faithful cumulative delta — option C (stream-position tokens) is the eventual fix; see `docs/legacy-sync-stub.md`.

| Test | Verifies via | Needed fix |
|---|---|---|
| `TestRoomsInvite/Parallel/{Can_invite…, Uninvited…, Invited_user_can_reject_invite, …reject_invite_for_empty_room, Users_cannot_invite_themselves…, …already_in_the_room}` | `/sync` invite/join/leave + 403s | none (passed as-is) |
| `TestGetRoomMembers` | `GET /members` | none |
| `TestRoomMembers/Parallel/POST_/rooms/:room_id/join_can_join_a_room` | join 200 + `/sync` | empty-body extractor |
| `TestRoomMembers/Parallel/POST_/join/:room_id_can_join_a_room` | join 200 + `/sync` (global path) | empty-body extractor |
| `TestCannotKickNonPresentUser` | kick 403 | kick non-member 403 |
| `TestCannotKickLeftUser` | `/sync` + kick 403 | kick non-member 403 |
| `TestNotPresentUserCannotBanOthers` | PUT power_levels + ban 403 | trailing-slash state route |
| `TestCumulativeJoinLeaveJoinSync` | `/sync` join+leave sections, stale-token replay | legacy stale-since full-resync |

### Dropped — not viable
- **`TestMembersLocal/*`** — the test fixture does `PUT /presence/{user}/status` before any subtest (`rooms_members_local_test.go:26`); presence is an out-of-scope EDU.

(The two former lower-confidence candidates are resolved in the next section: `TestTentativeEventualJoiningAfterRejecting` is now allowlisted; the reinvite flow is dropped as v12-incompatible.)

---

## Second allowlist pass — 2026-06-03 (GET state + 4 more, no new code)

After GET room state landed (see below), a second pass added tests needing only already-wired capability. **Static verification proved insufficient for behavioural asserts:** a CI run caught two tests that are wired-clean at the endpoint level but assert behaviour this v12 server doesn't have. Net — 4 added, 2 dropped, 1 deferred.

**Added (4):**

| Test | Exercises |
|---|---|
| `TestRoomCreationReportsEventsToMyself/parallel/Setting_room_topic_reports_m.room.topic_to_myself` | PUT `m.room.topic` + `/sync` echo |
| `TestRoomCreationReportsEventsToMyself/parallel/Joining_room_twice_is_idempotent` | join ×2 → same member event id (join is idempotent) + GET state |
| `TestTentativeEventualJoiningAfterRejecting` | invite → reject (leave) → reinvite → join, via bare `MustSync` (no filter) |
| `TestRoomCreate/Parallel/Rooms_can_be_created_with_an_initial_invite_list_(SYN-205)` | createRoom `invite[]` → invitee sees the invite |

**Dropped — not viable (v12 / design mismatch, NOT test selection):**
- `TestRoomMembers/Parallel/Test_that_we_can_be_reinvited_to_a_room_we_created` — PUTs `m.room.power_levels` naming the **creator** (alice) in `users`; **room v12 auth rule 10.4** forbids listing a creator (creators hold implicit infinite PL) → 403 reject (`crates/neutrino-state/src/auth_rules.rs:737-745`). The test assumes a pre-v12 room where the creator is a level-100 user.
- `TestRoomCreationReportsEventsToMyself/parallel/Setting_state_twice_is_idempotent` — asserts a repeated identical state PUT returns the **same** event id (Synapse dedups unchanged state). neutrino builds a fresh event per PUT (`crates/neutrino-http/src/room_actor.rs:64-66`), so ids differ. Would need an unchanged-state dedup feature (not spec-mandated).

**Deferred — needs an empirical run:**
- `TestPowerLevels/Parallel/…/PUT_power_levels_should_not_explode_if_the_old_power_levels_were_empty` — expects **403 not 500** on an empty-`users` PUT; depends on v12 auth producing a clean reject rather than faulting internally. Worth a targeted run before allowlisting.

---

## Still blocked, grouped by the one missing capability

### ~~`GET` of room state~~ — IMPLEMENTED 2026-06-02
`GET /rooms/{room}/state`, `…/state/{type}/{key}`, and `…/state/{type}` (+ trailing slash) now read the materialised current state (content by default / full event on `?format=event`; `format` validated, `404 M_NOT_FOUND` on missing — Synapse-aligned). This unblocks the read-back surface. **Allowlisted 2026-06-03** (13 entries, pending the next CI run to confirm; verified statically against the handlers): the 8 `TestRoomState` GET-state subtests (member, `?format=event`, power_levels, name get/set, topic get/set, full `/state`), `apidoc_room_create`'s `makes_a_room_with_a_{topic,name}`, and `apidoc_room_members` ban/invite/leave. Deliberately **not** added: join-with-custom-content (our `/join` ignores arbitrary member content), the `apidoc_room_members` reinvite flow (later tried 2026-06-03 and dropped as v12-incompatible — see "Second allowlist pass"), `TestRoomsInvite/…/Invited_user_can_see_room_metadata` (re-check). Still blocked elsewhere: `power_levels` "can set" (reads via `GET /event`), `TestLeftRoomFixture/Can_get_…state…` (state-at-leave for a departed user — gated out by our no-visibility model), createRoom `version`/`initial_state`/rich-topic subtests (createRoom drops those / no rich `m.text`), and `joined_rooms`/`joined_members`/`publicRooms`/`directory` (separate unimplemented endpoints — `joined_rooms` is a trivial wire-up over the existing `StateStore::joined_rooms`, deferred to its own change).

Allowlist regex note: the GET-state entries anchor the `/^rooms$/` segment because Go's `-run` matches each `/`-split segment unanchored, so a bare `rooms` would substring-match (and silently run) the sibling `joined_rooms` subtest. The proper fix — anchoring segments in the allowlist parser (`scripts/complement.sh`) — is deferred.

### ~~`POST /user/{uid}/filter`~~ — IMPLEMENTED 2026-06-15 (no-op stub)
`POST /user/{userId}/filter` returns a constant `{"filter_id":"0"}`; `GET …/filter/{id}` returns `{}`. Filters are never stored or applied — the real sync path is MSC4186 sliding sync (which self-filters); legacy `/v3/sync` is Complement-only and already discards `?filter=`. Pinned by `tests/e2e_filter.rs`. This unblocks Complement's `CreateFilter`, which previously 404'd ahead of nearly all of `sync_test.go` / `sync_archive_test.go` (`TestSync/*`, `TestSyncLeaveSection/*`, `TestArchivedRoomsHistory/*`, `TestLeaveEventInviteRejection`, …). **Not yet allowlisted** — needs an empirical Complement run (Docker+Go) to confirm which subtests are green vs. still gated on other gaps (e.g. lazy-loading, `GET /event`). The `filter_test.go` round-trip cases (`Can download filter`) will NOT pass: the stub returns `{}` rather than echoing the uploaded filter (Option A, deliberate).

### `GET /rooms/{room}/event/{eventId}`
`apidoc_room_history_visibility_test.go::TestFetchEvent`; `txnid_test.go::TestTxnInEvent`; `power_levels_test.go` (reads the PL event by id).

### `GET /rooms/{room}/messages` — IMPLEMENTED 2026-06-04
Join-gated pagination over `EventStore::room_messages` (`dir`/`from`/`to`/`limit`; `filter` accepted-but-ignored; no lazy `state`, history-visibility, or backfill). Triage of `tests/csapi/room_messages_test.go`:
- **Allowlisted:** `TestFetchMessagesFromNonExistentRoom` — non-member (incl. unknown room) → **403**, matching the endpoint's only documented error.
- **Blocked — `TestSendAndFetchMessage`:** uses a top-level `/sync` **`next_batch`** as `?from=` and asserts (every-element `JSONArrayEach`) the chunk is *only* the new message. Our legacy `/sync` `next_batch` is the sliding-sync **connection `pos` counter** (`legacy_sync/translate.rs:221` ← `sliding_sync/mod.rs:269`), not a stream position, so it can't act as a `/messages` cursor (the chunk would also include the setup state events). Unblocked only by the durable stream-position legacy token (option C, deferred 2026-06-02). The per-room `prev_batch` *is* a real stream_pos token and does interop — covered by the `sync_prev_batch_works_as_messages_from` Rust e2e — but this complement test uses `next_batch`.
- **Not viable:** `TestRoomMessagesLazyLoading` / `…LocalUser` (need `filter.lazy_load_members` → the `state` field we omit); `TestMessagesOverFederation` (2 servers + backfill); `TestSendMessageWithTxn` (not a `/messages` test — asserts `/send` **txn-id dedup**, which neutrino doesn't do); `TestLeftRoomFixture/Can_get_…messages…` (state-at-leave for a departed user — gated out by our no-history-visibility model).

### `/members` query filters (`at`, `membership`, `not_membership`)
`TestGetRoomMembersAtPoint`, all `TestGetFilteredRoomMembers/*` — we ignore the filters, so the asserted subset is wrong.

### createRoom fidelity (`room_version` rejection, `initial_state`, `power_level_content_override`, `room_alias_name`)
`TestRoomCreate/…/{with a name, with a topic[…], given version, rejects …versions}`, `TestDemotingUsersViaUsersDefault`, `TestRoomState/…/GET /directory/room…`. createRoom silently drops these (and never 400s an unknown version).

### Presence / EDU / federation (out of scope per CLAUDE.md / PLAN.md)
`TestMembersLocal/…presence…`, `TestPresenceSyncDifferentRooms`, `TestSync/…presence/device-list…`, `TestSyncTimelineGap` (remote events), all `tests/federation_*`, E2EE/device-list/key-backup, account-data/push/ignored-users, media/search/directory, aliases/spaces, `/forget`/`/redact`/`/upgrade`/`/typing`.

---

## Cheap-win ordering (impact / effort)

1. **[DONE 2026-06-02]** ~~Empirically run the "newly viable" candidates; allowlist the green ones.~~ Headline landed: `TestCumulativeJoinLeaveJoinSync` + the `TestRoomsInvite/Parallel` invite-flow subtests.
2. **[DONE 2026-06-15]** ~~No-op `POST /user/{uid}/filter` stub → unlocks the `TestSync/*` + `TestSyncLeaveSection/*` tranche.~~ Endpoint landed (`tests/e2e_filter.rs`); allowlisting pending an empirical Complement run.
3. **[DONE 2026-06-03]** ~~`GET /rooms/{room}/state[/{type}/{key}]`~~ → unlocked the state read-back surface (`rooms_state`, `apidoc_room_state`, createRoom name/topic, apidoc_room_members invite/ban/leave). Largest single unlock.
4. **`GET /rooms/{room}/event/{eventId}`** → history-visibility fetch + power-levels-by-id tests.
5. **Teach `scripts/complement.sh` to also run `./tests/msc4222/...`** → the marquee MSC4222 `state_after` dual-emission validation (`tests/msc4222/TestSync/*`), currently unreachable because the runner is csapi-only.
6. **404 fallback → `{"errcode":"M_UNRECOGNIZED"}`** → `TestUnknownEndpoints/*`, and correct per spec.
