# Viable Complement tests given the new `/v3/sync` translator

Audit date: 2026-05-27. Branch: `kaylendog/tests/complement` (delta vs `main`: legacy `/v3/sync` → sliding-sync translator only, +1715 LOC).

## Ground truth: what the axum router actually wires today

From `crates/neutrino-http/src/lib.rs:99-144`:

| Method | Path | Notes |
|---|---|---|
| GET | `/_matrix/client/versions` | advertises `org.matrix.simplified_msc3575` + `org.matrix.msc4222` |
| GET/POST | `/_matrix/client/{version}/login` | fixed-credential single-user stub |
| POST | `/_matrix/client/{version}/register` | single-user stub |
| POST | `/_matrix/client/unstable/org.matrix.simplified_msc3575/sync` | MSC4186 |
| GET | `/_matrix/client/v3/sync` | **NEW on this branch** — translates to MSC4186 |
| POST | `/_matrix/client/v3/keys/{query,upload,device_signing/upload,signatures/upload}` | stubs |
| GET | `/_matrix/client/v3/profile/{self}` | stub |
| GET | `/_matrix/client/v3/user/{self}/account_data/{type}` | stub |
| GET | `/_matrix/client/v3/room_keys/version` | stub |
| POST | `/_matrix/client/v3/createRoom` | basic; **drops** `preset`, `topic`, `visibility`, `creation_content`, `initial_state`, `invite`, `room_alias_name`, `power_level_content_override`, `room_version` |
| GET | `/_matrix/client/v3/capabilities` | advertises `m.room_versions.default = "12"`, `available = {"12": "stable"}` (see PLAN.md 2026-05-27 — the wire `room_version` is the MSC4242 unstable id, the advertised string is the stable `"12"` to keep `gomatrixserverlib.MustGetRoomVersion` happy) |
| GET | `/_matrix/client/v3/rooms/{room_id}/members` | ignores `at`/`membership` filters |
| PUT | `/_matrix/client/v3/rooms/{room_id}/send/{type}/{msg_id}` | the only write endpoint |
| POST | `/_matrix/client/v3/pushers/set` | stub |

PLAN.md lists `/state/{type}/{key}` PUT, `/state` GET, `/state/{type}/{key}` GET, `/event/{eventId}` GET, `/invite` POST, `/leave` POST — **none are wired**. The 404 fallback returns plain text, not `{"errcode": "M_UNRECOGNIZED", ...}`.

This drastically narrows what is genuinely viable. Complement's `MustJoinRoom` calls `POST /v3/join/{…}` (`client.go:226-239`) and `SendEventSynced` routes via `/state/…` whenever `StateKey != nil` (`client.go:419-429`) — both 404.

---

## VIABLE today (zero code changes)

### Already in `complement/allowlist.txt`
1. `csapi/TestVersionStructure`
2. `csapi/TestLogin/parallel/GET_/login_yields_a_set_of_flows`
3. `csapi/TestLogin/parallel/POST_/login_can_login_as_user`
4. `csapi/TestLogin/parallel/POST_/login_can_log_in_as_a_user_with_just_the_local_part_of_the_id`
5. `csapi/TestRegistration/parallel/POST_{}_returns_a_set_of_flows`
6. `csapi/TestRegistration/parallel/POST_/register_can_create_a_user`
7. `csapi/TestRegistration/parallel/POST_/register_returns_the_same_device_id_as_that_in_the_request`
8. `csapi/TestRegistration/parallel/POST_/register_allows_registration_of_usernames_with_$`
9. `csapi/TestRegistration/parallel/Registration_accepts_non-ascii_passwords`
10. `csapi/TestRoomCreate/Parallel/POST_/createRoom_makes_a_private_room`
11. `csapi/TestRoomCreate/Parallel/POST_/createRoom_makes_a_private_room_with_invites`
12. `csapi/TestRoomCreate/Parallel/POST_/createRoom_makes_a_public_room`

### Candidates to add (single user; no second-user join, no `/state/…`, no `/event/…`)
| Test | What it asserts | Risk |
|---|---|---|
| `csapi/TestRoomCreate/Parallel/Can_/sync_newly_created_room` | After `createRoom`+`PUT /send`, the message appears in `/sync` | Low — exercises exactly the new translator end-to-end |
| `csapi/TestRoomCreationReportsEventsToMyself/parallel/Room_creation_reports_m.room.create_to_myself` | Self sees create event in `/sync` | Low — pure single-user create+sync |
| `csapi/TestRoomCreationReportsEventsToMyself/parallel/Room_creation_reports_m.room.member_to_myself` | Self sees own join member event in `/sync` | Low — same shape |

These three are the highest-value adds because they directly validate the new `/v3/sync` translator's join-bucket shape on a single user, without needing any unimplemented endpoint. Recommend running them empirically first; if they pass, allowlist.

---

## NOT VIABLE without further endpoint work

Grouped by what would need to land.

### Need `POST /_matrix/client/v3/join/{roomIdOrAlias}` (the biggest unlock)
Without this, every multi-user test 404s on `MustJoinRoom`. Affects ~80% of the room/state/membership suite:
- `csapi/TestCumulativeJoinLeaveJoinSync`, `csapi/TestSyncLeaveSection` subtests, `csapi/TestLeaveEventInviteRejection`
- `msc4222/TestSync/*` (both `public_room` and `private_room` subtests) — these would otherwise be the marquee validation for the new translator + MSC4222 dual-emission
- All of `csapi/room_members_test.go`, `csapi/rooms_invite_test.go`, `csapi/rooms_members_local_test.go`, `csapi/apidoc_room_members_test.go`

### Need `PUT /rooms/{}/state/{type}/{state_key}` + `GET /rooms/{}/state[/...]`
Unlocks the room/state read/write surface that PLAN.md already plans:
- `csapi/TestRoomCreate/Parallel/…makes_a_room_with_a_{name,topic}`, `…initial_state` variants
- `csapi/rooms_state_test.go` — most subtests
- `csapi/apidoc_room_state_test.go` — most subtests
- `csapi/power_levels_test.go`
- `tests/v12_test.go::TestMSC4291RoomIDAsHashOfCreateEvent` — would be the cleanest v12/MSC4242 smoke test (pure local, no federation)

### Need `GET /rooms/{}/event/{eventId}`
- `csapi/apidoc_room_history_visibility_test.go::TestFetchEvent`
- `csapi/txnid_test.go::TestTxnInEvent` (also blocked on txn dedup, deleted 2026-05-26)

### Need `POST /user/{uid}/filter` (could be a no-op opaque-ID stub)
The translator already ignores `?filter=` — a minimal stub returning any filter_id would unlock the largest tranche of legacy-sync tests against the new translator:
- `csapi/TestSync/parallel/Can_sync_a_joined_room`
- `csapi/TestSync/parallel/Full_state_sync_includes_joined_rooms`
- `csapi/TestSync/parallel/Newly_joined_room_is_included_in_an_incremental_sync`
- `csapi/TestSyncLeaveSection/Left_rooms_appear_in_the_leave_section_of_sync` (also needs join)
- `csapi/TestSyncLeaveSection/Newly_left_rooms_appear_in_the_leave_section_of_incremental_sync` (also needs join)

The stub need only `POST /user/{uid}/filter → {"filter_id": "<uuid>"}` and `GET /user/{uid}/filter/{id} → {}`.

### Hard NO (out of scope per CLAUDE.md / PLAN.md)
- All federation tests (`tests/federation_*`, `tests/msc3902`, `TestMSC4297StateResolutionV2_1_*`, `TestMSC4291…AuthEventsOmitsCreateEvent`, `TestComplementCanCreateValidV12Rooms`, `TestMSC4311FullCreateEventOnStrippedState`)
- All E2EE / device-list / cross-signing / key-backup tests
- All EDU tests (typing, presence, receipts, to-device)
- Account data, push rules, ignored users
- Media, search, user directory, public-rooms directory
- Room aliases, hierarchy/spaces
- `/messages`, `/redact`, `/forget`, `/upgrade`, `/kick`, `/ban`, `/unban`, `/typing`, `/devices`
- Profile per-user APIs
- All `tests/msc{2836,3391,3757,3874,3890,3930,3967,4140,4155,4306}` directories

---

## Cheap-win ordering (impact / effort)

1. **Try `Can_/sync_newly_created_room` + the two `TestRoomCreationReportsEventsToMyself` create/member subtests as-is.** If green, allowlist them — three direct validations of the new translator with no new endpoints. (Effort: 0.)
2. **Add a no-op `POST /user/{uid}/filter` stub** → unlocks 3–5 `TestSync/*` subtests once `/v3/join` lands. (Effort: ~30 lines.)
3. **Add `POST /_matrix/client/v3/join/{roomIdOrAlias}`** → unlocks `msc4222/TestSync/*` (the marquee MSC4222 dual-emission validation), `TestCumulativeJoinLeaveJoinSync`, and the rest of the sync surveyor's PARTIAL list.
4. **Add `PUT /state/{type}/{key}` + `GET /state[/...]`** → unlocks v12/MSC4291 hash-as-ID test plus the room-state read/write tranche.
5. **Add `GET /rooms/{}/event/{eventId}`** → unlocks history-visibility fetch tests.
6. **Make the 404 fallback emit `{"errcode": "M_UNRECOGNIZED", ...}`** → unlocks `TestUnknownEndpoints/Client-server_endpoints` (and is just generally right per the spec).

A pure-local Rust regression test asserting that events emitted by `createRoom` carry `prev_state_events` and no on-wire `auth_events` (the MSC4242 contract) would close a real coverage gap that Complement does not cover at the CSAPI layer.
