# Server-Server invite / join / leave

Design and implementation notes for federated membership. Status: **implemented.**
This doc is the detailed design home.

The work covers three areas, ordered to verify federation incrementally:

- **Join** → federation for **public** rooms (both directions).
- **Invite** → federation for **private** rooms.
- **Invite rejection** → the decline path + its unreachable-server failure modes.

---

## 0. Scope, constraints, and what we deliberately do NOT do

Target spec: Matrix v1.18 Server-Server API, **room version 12 + MSC4242 (State DAGs)
only**. Read these together — MSC4242 reshapes the join response.

Hard constraints from CLAUDE.md / project shape:

- **No signatures, no signature checks, no server signing key.** Trusted mesh. Every
  place the spec says "sign the event" / "validate signatures" is a no-op for us. We
  still preserve the *data flow* the signatures imply (see §3, federate-then-persist) so
  a future signatures world isn't a rewrite.
- **No X-Matrix auth** on inbound federation endpoints (trusted mesh) — matches the
  existing `/send`, `/backfill`, `/get_missing_events` handlers.
- **No EDUs / E2EE / pagination / rate-limiting / access control.**
- **Restricted rooms are out of scope** (`join_authorised_via_users_server`,
  `M_UNABLE_TO_AUTHORISE_JOIN`, `M_UNABLE_TO_GRANT_JOIN`). Public + invite join rules
  only. Document the gap; do not implement.
- **3pid / third-party invites out of scope** (no identity server; `/3pid/onbind`,
  `content.third_party_invite` not handled).
- **Knock** is already surfaced in sync but federated knock is **not** in scope (no
  `make_knock`/`send_knock`).

### MSC4242 response shape (the thing that's easy to get wrong)

For room versions implementing state DAGs, the `/send_join` (and by extension
`/send_leave`) response **MUST NOT** return `auth_chain`, `state`, or
`servers_in_room`. Instead it returns:

- `state_dag` — the **entire state DAG**: every state event reachable by walking
  `prev_state_events` from the join event's `prev_state_events` back to `m.room.create`.
- `timeline` — the most recent timeline events (last N), so the joiner has something to
  render and so the join event's `prev_events` resolve.
- `event` — the resident's copy of the membership event (in a signatures world this
  carries the resident's signature; for us it's the event as we persisted it).

We never emit `auth_chain` / `partial_state_event_ids` / `partial_auth_chain_ids`
(those last two are faster-room-joins / `omit_members`, explicitly out of scope).

**Why no auth_events on the wire matters:** under MSC4242, `auth_events` is
server-computed at apply time (`apply_pdu` is the sole authority) and is **not part of
the hashed `raw`** (the co-located-metadata pattern — same as `rejected` / `soft_failed`).
Consequence: the joining server, which has no state DAG yet, does **not** and **cannot**
compute `auth_events`, and doesn't need to — it computes a stable `event_id` from the raw
without them, and the resident's `apply_pdu` fills them.

### Server resolution (room ids are server-less in v12)

A v12 room id (`!opaque`) carries no server name, so we **cannot** derive the
resident/target server from `room_id`. We thread server hints explicitly:

- **Join** → the CSAPI `?server_name=` query (a *list* of candidates).
- **Invite accept / reject** → the inviting server = the invite event's `sender`
  domain (and/or `unsigned` hints stored with the invite stub).

Resolution itself is the existing trusted-mesh resolver: `http://{server_name}` (raw
IP:port, no `.well-known` / SRV / TLS — `federation::client`).

---

## 1. What already exists (reuse, do not reinvent)

| Component | Location | Reused for |
|---|---|---|
| `RoomCore::apply_pdu` — single ingest path; DROP/RETRY/REJECT disposition; computes `auth_events`; idempotent persisted-check | `crates/neutrino-state/src/room_core.rs` | every inbound membership apply |
| `RoomRegistry` / `RoomActor` — per-room serialised actor; `Command::Send` (local build+apply+persist+enqueue), `Command::ApplyPdu` (received PDU, persists rejects, `&[]` destinations) | `crates/neutrino-http/src/room_actor.rs` | apply + distribution |
| Staging table + `StagingStore` (`staged_events(event_id PK, room_id, json, origin)`; `stage_pdu` / `staged_rooms` / `staged_for_room` / `ancestry_gap` / `unstage_events`) | `crates/neutrino-store-sqlite/src/store/staging.rs` | **join ingest** + gap-fill |
| Per-room **drain worker** (toposort → `apply_pdu` → `fill_state_ancestry` on retryable → in-memory backoff; supervisor enumerates `staged_rooms()` on startup + in-process `mpsc<RoomId>` poke) | `crates/neutrino-http/src/federation/worker.rs` | join ingest drains here |
| Shared gap-fill (`fill_state_ancestry` / `state_dag_boundary` / `MissingEventsFetcher`) | `crates/neutrino-http/src/federation/gapfill.rs` | join ingest gap-fill |
| `FederationClient` (`send_transaction`, `get_missing_events`) + `ReqwestFetcher` + resolver + `TxnIdGen` + `FederationClientError` | `crates/neutrino-http/src/federation/client.rs` | **outbound handshakes** (extend with make/send_join, invite, make/send_leave) |
| Outbound sender pool + outbox (`pending_pdus` / `remove_pdus`; retry-forever, full-jitter backoff) | `crates/neutrino-http/src/federation/sender.rs` | durable `/send` delivery (joined-room leave, distribution duty) |
| `outbound_destinations` (post-apply join-member servers + departing target, − own) | `crates/neutrino-http/src/room_actor.rs` | distribution duty destination set |
| `EventStore::persist_resolved_event(event, timeline_fes, state_fes, delta, destinations)` | store-sqlite | atomic event + outbox write |
| `RoomCore::hydrate(room_id, timeline_fes, state_fes, current_state)` | neutrino-state | actor bootstrap |
| CSAPI membership handlers (`/join` `/leave` `/invite` `/kick` `/ban` `/unban`) → `RoomRegistry::send_event` | `crates/neutrino-http/src/membership.rs` | branch local-vs-remote |
| Inbound `/send`, `/backfill`, `/get_missing_events` handlers + `FedError` + `router_with_store` / `from_store_with_fetcher` test ctors | `crates/neutrino-http/src/federation/` | sibling module pattern + e2e harness |
| Sliding-sync + legacy-sync invite-room surfacing (`invite_state` / stripped state) | `sliding_sync/`, `legacy_sync/` | invite stub appears in sync |
| `--features multi-user-shim` (per-user tokens) | neutrino-http | multi-user e2e tests |

All federation endpoints are inbound HTTP under `crates/neutrino-http/src/federation/`
(new files per endpoint, sibling to `send.rs` / `backfill.rs`). Outbound handshakes are
methods on `FederationClient`. CSAPI changes are in `membership.rs`.

### A shared primitive both join and invite need: a read-only event builder

Both the inbound `make_join` template and the outbound invite candidate need to
**construct an `m.room.member` event off current room state without persisting it**:
read current heads (timeline FEs + state-DAG FEs) → set `prev_events` / `prev_state_events`
→ build content → (for the fully-built case) compute `event_id`. This is a **read-only
RoomCore** snapshot, NOT a `Command::Send` (which applies+persists). Factored once
(a `RoomCore`/registry read method that takes a hydrated read snapshot) and used in both
places. There is no need to split `Command::Send`.

---

## 2. Join (public-room federation)

Goal: a local user can join a remote public room and receive its state; a remote user
can join a public room we host. Two neutrino servers can then share a public room and
exchange timeline events over `/send`.

### 2.1 Inbound — we are the resident (remote user joins our room)

**`GET /_matrix/federation/v1/make_join/{roomId}/{userId}?ver=…`** —
`federation/make_join.rs`.

1. Parse `roomId` (manual `OwnedRoomId::try_from` → JSON 400 on bad id, matching the
   `get_missing_events` precedent), `userId`.
2. Room unknown → **404 `M_NOT_FOUND`**.
3. Version negotiation: if `ver` does not include our MSC4242 room-version id →
   **400 `M_INCOMPATIBLE_ROOM_VERSION`** with `room_version` in the body.
4. **Join-rules pre-check** against current state:
   - banned → **403 `M_FORBIDDEN`**.
   - join rule `invite` and the user has no pending invite → **403 `M_FORBIDDEN`**.
   - join rule `public` → allow.
   - join rule `restricted`/`knock` → **out of scope**; return 403 (documented) for now.
5. Build the **template** via the read-only event builder: `type: m.room.member`,
   `sender = state_key = userId`, `content: { membership: "join" }`, `origin_server_ts`,
   `prev_events` = current timeline FEs, `prev_state_events` = current state-DAG FEs
   (**MSC4242**), **no `auth_events`**.
6. Respond `{ event: <template>, room_version: <our id> }`. **Persist nothing** —
   make_join is stateless, so an abandoned template pollutes nothing.

**`PUT /_matrix/federation/v2/send_join/{roomId}/{eventId}`** — `federation/send_join.rs`.

1. Parse the body event (`event_id::from_wire`). Validate:
   - `eventId` path param == the event's computed id → else **400 `M_INVALID_PARAM`**.
   - `type == m.room.member`, `content.membership == "join"`, `state_key == sender`,
     `sender`'s domain == the origin server, `room_id` == path → else **400 `M_INVALID_PARAM`**.
2. **Apply synchronously** through `apply_pdu` (NOT staged — the joiner is blocked on the
   `state_dag` response, and as resident we already hold the full ancestry so apply is
   never retryable). Disposition:
   - REJECT (`Event.rejected`, e.g. user got banned between make_join and send_join) →
     **403 `M_FORBIDDEN`**, persist nothing client-visible.
   - DROP (malformed/misrouted) → 400.
   - Accept → persist.
3. **Distribution duty:** the joiner sent the event only to us. As resident we are the
   fan-out origin, so we must enqueue the accepted join to **all other servers in the
   room (− joiner − us)** — the `outbound_destinations` set. NOTE this is *not* the
   `Command::ApplyPdu` path (that passes `&[]` because received `/send` PDUs were
   already broadcast by their origin). send_join/send_leave/invite-accept are
   origination points for *our* distribution.
4. Build the response: `state_dag` (entire current state DAG via `prev_state_events`
   back to create), `timeline` (last N timeline events incl. the join's `prev_events`
   so references resolve), `event` (our persisted copy). **No `auth_chain` / `state` /
   `servers_in_room`.** Wire bytes verbatim from `Event.raw` (peers recompute the v12
   reference hash — same rule as backfill/get_missing_events).
5. **Idempotent re-send:** a retried `send_join` (our response was lost) re-applies the
   same join — `apply_pdu`'s persisted-check returns accepted-with-no-effect — and we
   simply rebuild and return the `state_dag` again.

### 2.2 Outbound — we are the joining server (local user joins a remote room)

CSAPI entry: `POST /_matrix/client/v3/join/{roomIdOrAlias}` (and room-scoped
`/rooms/{id}/join`). Today these only emit a *local* join via the actor; we add a
**remote branch**: if the room is unknown locally → federated join.

1. Candidate servers = the `?server_name=` list. (Alias resolution stays unimplemented —
   `#…` → 400 `M_INVALID_PARAM`, as today.)
2. For each candidate until one succeeds:
   a. `FederationClient::make_join(dest, room_id, user_id, ver=<our id>)` →
      template `event` + `room_version`. On 404/403/`M_INCOMPATIBLE_ROOM_VERSION` or
      transport error → try next candidate. Room not our version → hard 400 to client.
   b. Complete the template: fill `content`/`origin`/`origin_server_ts`, **compute
      `event_id`** (no `auth_events`, no signing). Validate per spec: `room_id` == path,
      `sender` & `state_key` == our user, `type == m.room.member`, `membership == join`.
   c. `FederationClient::send_join(dest, room_id, event_id, event)` → `{ state_dag,
      timeline, event }`.
3. **Ingest = stage-then-drain (NOT load-the-whole-DAG-into-a-RoomCore).**
   - Stage every returned `state_dag` event + every `timeline` event + the join itself
     into `staged_events` (origin = the resident), then **poke the existing per-room
     drain worker**.
   - The worker drains as for any inbound ancestry: toposort → `apply_pdu` each
     oldest-first (the create event first → bootstraps the room) → `fill_state_ancestry`
     gap-fill if anything is still missing (MSC4242 `state_dag: true`) → unstage on
     commit.
   - **No DAG-size cap** — a cap makes large rooms unjoinable (must walk the whole DAG to
     ground, inherent to MSC4242 / auth-chain CRDTs). Memory is bounded by *incremental*
     apply (one event at a time), not by holding the whole DAG in a RoomCore.
   - **Crash-resume is free:** staging is durable; on restart `staged_rooms()` re-drains
     the half-joined room like any other staged room. No bespoke outbound-join recovery
     code, no atomicity flag.
4. **CSAPI response:** block, polling current_state for our `membership == join`, with a
   **client-facing timeout** (same poll-for-eventual-apply the `/send` e2e tests use).
   The timeout returns an error to the client but does **not** abort the drain (it keeps
   grinding; a later sync will show the join). Success → 200 with the room id.

### 2.3 Mechanisms to verify (not assume)

1. **Actor bootstrap of a never-seen room.** The drain applies into a room with no
   `rooms`-table FEs. The actor must `hydrate` an empty room and let the staged
   `m.room.create` seed it as the drain applies forward. If `RoomRegistry` can't
   currently bootstrap-from-nothing (it expects `forward_extremities` to exist), that's
   a small change. **Check first** (`room_actor.rs` bootstrap path).
2. **Re-join tolerance** (re-join is NOT a second handshake):
   - A *joined* user re-joining is a normal `/send` event: locally we have state, so we
     emit + broadcast via the actor like any membership change. No handshake.
   - A full `send_join` from an already-joined user only happens after the joiner nuked
     its DB. The inbound side must tolerate it: `apply_pdu`'s persisted-check + state-res
     make it an idempotent refresh. **Add tests, not new logic.**
3. **Joined-room leave falls out for free.** Leaving a room we're joined to = build
   the leave `m.room.member` locally (we have state) + broadcast via the durable `/send`
   outbox. No new endpoint. `make_leave`/`send_leave` are **invite-reject only**
   (invite rejection, §4). Kick/ban of a remote user likewise already federates via
   `/send` + the departing-target destination clause — **no new federation code**.

### 2.4 Failure modes (the "what goes wrong")

| Scenario | Handling |
|---|---|
| make_join 404 / 403 / version mismatch | map to CSAPI 404 / 403 / 400; iterate to next `server_name` on 403/transport |
| All candidate servers unreachable | CSAPI 5xx / error; client retries |
| Room isn't our version | hard 400 `M_INCOMPATIBLE_ROOM_VERSION` (we only do v12+MSC4242) |
| `state_dag` incomplete (a path to create is missing) | drain's `fill_state_ancestry` gap-fills (bounded by being grounded); genuinely unfillable → worker backs off, CSAPI times out, nothing half-committed |
| Ingest takes minutes (huge room) | drain keeps working; CSAPI times out gracefully; join completes on a later sync |
| Crash mid-ingest | `staged_rooms()` resumes; mid-event re-apply is an idempotent no-op |
| Banned between make_join & send_join (inbound) | `apply_pdu` REJECT → 403, nothing persisted |
| Template stale (FEs advanced, inbound) | join is a fork/merge; state-res absorbs it |
| send_join response lost, joiner retries (inbound) | idempotent: re-apply no-op, rebuild `state_dag` |

### 2.5 Tests

- e2e against ephemeral-port axum stub peers (the `sender`/`fetcher`/`send` test
  pattern): outbound join drives a stub resident exposing make_join + send_join;
  inbound join drives our router with a stub joiner.
- Inbound: make_join template shape (no auth_events, prev_state_events set, version
  echoed); join-rules pre-check (public allow, invite-only-no-invite 403, banned 403);
  send_join validation 400s (id mismatch, wrong membership, sender≠state_key, foreign
  origin); accept → persisted + `state_dag` has no `auth_chain` + distribution enqueued
  to other servers; idempotent re-send.
- Outbound: server_name fallback (first candidate down → second succeeds); successful
  ingest → eventual `membership=join` in sync + room state present; incomplete state_dag
  → gap-fill → accept; unfillable → CSAPI timeout, room not joined.
- Re-join: `/send`-path refresh; full `send_join` from already-joined.
- Round-trip: our outbound make_join/send_join bodies parse through our own inbound
  handlers (field-drift guard, like the client.rs round-trip test).

---

## 3. Invite (private rooms)

Goal: a local user invites a remote user (and vice-versa). Invites are **out-of-band
membership**: an inbound invite arrives for a room we have no state for (no create, no
auth chain), so it **cannot** go through `apply_pdu` / RoomCore.

### 3.1 `InviteStore` (the one storage-trait change — Kegan approved)

A lightweight store for invites where we are the **invitee's** server and hold no room
state. Shape:

- `put_invite(room_id, user_id, invite_event_raw, invite_room_state: Vec<StrippedState>)`
- `get_invite(room_id, user_id) -> Option<…>`
- `remove_invite(room_id, user_id)` (on accept or reject)
- enumeration for sync (the sliding/legacy-sync `rooms_with_membership` path must see
  these invite rooms — either fold invites into the existing membership query or add an
  `invited_oob_rooms(user_id)` the sync builder unions in).

Keyed by `(room_id, user_id)`. Storing the stripped `invite_room_state` lets sync render
the room (name/avatar/who-invited) without any room state. New table, no `user_version`
bump (matches prior in-place amendments).

This is a storage-trait change — flagged in the decisions log when landed, since trait
stability matters.

### 3.2 Inbound — we host the invited user

**`PUT /_matrix/federation/v2/invite/{roomId}/{eventId}`** — `federation/invite.rs`.

1. Parse + validate: `type == m.room.member`, `content.membership == "invite"`,
   `state_key` is one of **our** local users (domain == our server), `eventId` path ==
   computed id → else 400 `M_INVALID_PARAM`. Inviter lacks permission is the *sender's*
   server's concern (it built the event); we don't have state to re-check, so we accept
   structurally.
2. **Bypass `apply_pdu` entirely** (no room state). Store via `InviteStore.put_invite`
   with `unsigned.invite_room_state` (the stripped state the inviting server included).
3. Respond `{ event: <the event> }` (in a signatures world we'd add our signature;
   here it's the event verbatim).
4. Sync now surfaces the invite (membership=invite room with `invite_state`).

### 3.3 Outbound — we are the resident inviting a remote user

CSAPI `/invite` with a remote `user_id`: add a **remote branch**.
**Federate-then-persist, atomic, non-durable** — chosen to preserve the data flow a
future signatures world needs (don't reshape flows just because signatures are stubbed):

1. **Build candidate** invite event via the **read-only RoomCore** snapshot (current
   heads → prev_events/prev_state_events; local auth pre-check that the inviter *can*
   invite; compute `event_id`). **Not persisted.**
2. `FederationClient::invite(dest = invitee's server, room_id, event_id, event)` with
   `unsigned.invite_room_state` (stripped current state for rendering). Peer acks and
   returns the event (future: signed).
3. **On 200 → commit:** feed the returned event through the **incoming-federation path**
   — i.e. `apply_pdu` (exactly as we process a received PDU) — which persists it +
   updates current_state to `invite`, then **enqueue distribution** to room servers.
   CSAPI → 200.
4. **On any failure** (unreachable / 403 / non-2xx) → persist nothing, CSAPI → error;
   the client re-invites to retry.

So **200 OK ⟺ invitee server acked AND it's persisted AND propagating**; otherwise
nothing exists and retry is clean. No outbox / durability for the invite handshake
(non-durable by design — the client is the retry mechanism).

Mechanism note: there is **no** `Command::Send` split. Build = read-only snapshot;
commit = the same `apply_pdu` path used for inbound PDUs, plus the distribution enqueue.
A small build→commit race (heads move between step 1 and step 3) is **accepted** — the
event becomes a fork and state-res absorbs it. If the re-apply at step 3 happens to
reject (inviter lost power in the window), CSAPI errors and the invitee server has a
harmless dangling stub (cleaned up on re-invite / decline).

### 3.4 Failure modes

| Scenario | Handling |
|---|---|
| Invitee server unreachable / 403 (outbound) | nothing persisted, CSAPI error, client re-invites (atomic) |
| Inbound invite for a user that isn't ours | 400 `M_INVALID_PARAM` |
| Re-invite after a prior reject/leave | `put_invite` resurrects the stub; sync must show leave→invite transition |
| Build→commit race rejects the candidate | CSAPI error; dangling remote stub is harmless / re-cleanable |
| Inbound invite when we already have the room | shouldn't happen for an out-of-band invitee; if it does, prefer the in-room membership (edge, document) |

### 3.5 Tests

- `InviteStore` round-trip (put/get/remove; invite_state preserved); sync enumeration
  shows the invite room.
- Inbound `/invite/v2`: validation 400s; accept → stub stored → appears in
  sliding+legacy sync with `invite_state`.
- Outbound `/invite`: success → invitee stub created on the peer (stub resident) + local
  persist + distribution; peer-down → CSAPI error + **nothing** persisted (the atomicity
  assertion); peer-403 → CSAPI 403 + nothing persisted.
- Round-trip of our outbound invite body through our inbound `/invite/v2` parser.

---

## 4. Invite rejection

Goal: an invited local user declines, including when the inviting server is unreachable.
`make_leave`/`send_leave` exist **only** for this case (self-leave with no room state) —
every other leave/kick/ban already goes via `/send` (see §2.3).

**As built:** `federation/make_leave.rs` + `send_leave.rs` (inbound, resident;
send_leave → `apply_resident` → `{}` 200, no state_dag) + `federation/leave.rs::reject_invite`
(outbound CSAPI `/leave` of an OOB stub, branched before `require_room`) +
`FederationClient::make_leave`/`send_leave`. Two refinements landed beyond the sketch
below:

- **make_leave negotiates `ver` like make_join** → 400 `M_INCOMPATIBLE_ROOM_VERSION`
  when our version isn't offered (spec-conformant). It still omits make_join's
  *membership/join-rules* eligibility pre-check — the spec requires none for leave, and
  send_leave's apply is authoritative. (First shipped lenient on `ver`; gated after the
  post-commit review flagged the deviation.)
- **Template-completion forgery (CVE) mitigation:** the outbound completion never echoes
  the resident's template — `complete_join_template` and `complete_leave_template` were
  collapsed into the shared `federation::complete_membership_template`, which rebuilds
  the event (type/sender/state_key/content ours) and lifts only `prev_events` /
  `prev_state_events`. A hostile-template regression test pins the invariant.

### 4.1 Outbound — local user rejects an invite

CSAPI `/leave` (or `/rooms/{id}/leave`) for a room where the caller is `invite` and we
hold only an `InviteStore` stub (no joined state):

1. Resolve the inviting server from the invite event's `sender` domain.
2. **Best-effort federated decline:** `FederationClient::make_leave(dest, room, user)` →
   template → complete → `FederationClient::send_leave(dest, room, event_id, event)`.
   Bounded retries; on persistent failure, **give up** (the invite was never real room
   state for us).
3. **Unconditionally** set local membership = `leave` and `remove_invite` the stub
   (Synapse local-rejection): the user stops seeing the invite **even if the inviting
   server is down**. Never block the client on the federation call.
4. Terminal state of a declined invite = membership `leave`, stub removed from sync.
   A subsequent inbound `/invite/v2` for the same `(room,user)` resurrects the stub
   cleanly (re-invite works).

### 4.2 Inbound — a remote invited user declines an invite we issued

We are the resident; we hold the invite as real room state.

**`GET /_matrix/federation/v1/make_leave/{roomId}/{userId}`** — `federation/make_leave.rs`.
Symmetric to make_join: build a `membership: leave` template off current heads
(read-only builder), no auth_events, stateless. 404 unknown room.

**`PUT /_matrix/federation/v2/send_leave/{roomId}/{eventId}`** — `federation/send_leave.rs`.
Symmetric to send_join: validate (id match, type=member, membership=leave,
sender==state_key, sender domain == origin) → `apply_pdu` (synchronous; we have
ancestry) → on accept persist + **distribution duty** enqueue to other servers. Response
is an ack (no `state_dag` needed on leave; return `{}` / 200). Idempotent on re-send.

### 4.3 Failure modes

| Scenario | Handling |
|---|---|
| Inviting server unreachable on decline | local membership=leave anyway, stub removed; federated send_leave abandoned after bounded retry; client never blocked |
| send_leave slow / unprocessable | never block client; local leave immediate, federation async/best-effort |
| Decline then re-invite | stub resurrected by a fresh inbound `/invite/v2` |
| Inbound send_leave for an only-invited user (resident) | apply the leave → clears the invite from current_state, broadcast |

### 4.4 Tests

- Outbound decline: inviting-server-up → stub removed + send_leave delivered;
  inviting-server-down → stub still removed (local-rejection), no hang.
- Inbound make_leave/send_leave: template shape; send_leave validation 400s; accept →
  membership=leave in current_state + distribution enqueued; idempotent re-send.
- Decline → re-invite round-trip shows leave→invite in sync.

---

## 5. Cross-cutting

- **Error-code mapping** (reuse `FedError` → `IntoResponse`): 400 `M_INVALID_PARAM`
  (validation), 403 `M_FORBIDDEN` (auth/join-rules), 404 `M_NOT_FOUND` (unknown room),
  400 `M_INCOMPATIBLE_ROOM_VERSION`, 500 `M_UNKNOWN` (storage). A rejected (not 4xx)
  inbound event is the spec'd path-specific code, not a transaction-level error.
- **Wire bytes verbatim** everywhere a PDU is emitted (`Event.raw`), so peers recompute
  the reference hash bit-for-bit — same discipline as backfill/get_missing_events.
- **No ruma `federation-api`** server types (the feature pulls an unpublished sub-crate on
  ruma 0.15.1) — hand-roll request/response mirrors, as `send.rs` /
  `get_missing_events.rs` already do.
- **`FederationClient` extensions** (client.rs): `make_join`, `send_join`, `invite`,
  `make_leave`, `send_leave` — each a thin reqwest call with a hand-rolled body type;
  keep `FederationClientError` (Transport/Status/InvalidUrl) so callers can branch
  retry-vs-fail. Percent-encode `room_id`/`event_id`/`user_id` path segments.
- **`timeline` length N** in send_join: pick a small fixed N (e.g. 20, mirroring the
  get_missing_events cap); deeper timeline is backfilled lazily via `/backfill`.
- **`apply_pdu`'s `validate_references`** depends on the state DAG (`prev_state_events`),
  not timeline `prev_events` — under MSC4242 auth/state are state-DAG-driven, so the
  send_join `timeline` slice only needs to cover the join's immediate `prev_events`.

## 6. Deferred / explicitly out of scope

- Restricted-room joins (`join_authorised_via_users_server`), knock federation, 3pid
  invites, alias resolution, displayname/avatar carry-over.
- Durable retry of the invite handshake (the invite path is non-durable by design —
  client retries).
- Streaming JSON for very large `state_dag` serialization (resident-side OOM on huge
  rooms is accepted for now; symmetric to the ingest-side acceptance).
- Synapse/Complement test ports — the join work should unblock several previously
  state-res-blocked Complement membership tests.

## 7. Implementation order

Built incrementally, each piece testable against ephemeral-port stub peers:

- inbound make_join + send_join (+ distribution duty) — testable against a stub joiner
  without the outbound side. Verify actor bootstrap-from-nothing here.
- outbound make_join/send_join client + CSAPI `/join` remote branch + stage-drain
  ingest + server_name fallback. Now two neutrinos can share a public room.
- `InviteStore` + sync surfacing (storage + read path).
- inbound `/invite/v2`.
- outbound CSAPI `/invite` remote branch (federate-then-persist).
- make_leave/send_leave (inbound, resident).
- outbound CSAPI `/leave` invite-reject (best-effort handshake + unconditional local
  leave).

Each change: e2e against ephemeral-port stub peers; `cargo fmt`; `cargo clippy -p … --tests
-D warnings` (default + multi-user-shim); update PLAN.md + LOG.md; decisions log on any
trait change (InviteStore) or design deviation.
