## endpoints

### Client-Server API

- POST /_matrix/client/v3/createRoom 
- PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId} 
- PUT /_matrix/client/v3/rooms/{roomId}/state/{eventType}/{stateKey}
- POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync (only supporting room events & state events)
- GET /_matrix/client/v3/rooms/{roomId}/members 
- GET /_matrix/client/v3/rooms/{roomId}/messages 
- GET /_matrix/client/v3/rooms/{roomId}/event/{eventId}
- GET /_matrix/client/v3/rooms/{roomId}/state 
- GET /_matrix/client/v3/rooms/{roomId}/state/{eventType}/{stateKey}
- POST /_matrix/client/v3/rooms/{roomId}/invite 
- POST /_matrix/client/v3/rooms/{roomId}/leave

### Server-Server API

- PUT /_matrix/federation/v1/send/{txnId}
- GET /_matrix/federation/v1/backfill/{roomId}
- POST /_matrix/federation/v1/get_missing_events/{roomId}
- GET /_matrix/federation/v1/event/{eventId}
- GET /_matrix/federation/v1/make_join/{roomId}/{userId}
- PUT /_matrix/federation/v2/send_join/{roomId}/{eventId} 
- PUT /_matrix/federation/v2/invite/{roomId}/{eventId}
- GET /_matrix/federation/v1/make_leave/{roomId}/{userId}
- PUT /_matrix/federation/v2/send_leave/{roomId}/{eventId}

## outstanding work

All status points MUST have tests.

### Low-bandwidth proxy (`neutrino-lb`)

v1 done (JSON↔CBOR over HTTP). `neutrino-lb` is an in-process sidecar that
transcodes Server-Server federation **bodies** between JSON (local side) and
CBOR (wire side), behind a `WireClient`/`WireServer` trait seam (the future
CoAP/UDP transport drops in there). `neutrino-http` routes outbound federation
through it via the optional `Config.federation_proxy` (reqwest proxy mode);
`None` = direct (the default, so existing tests/deployments are unchanged). Two
homeservers behind two sidecars converge a real join + message — see
`crates/neutrino-http/tests/e2e_lb_federation.rs`. Design:
`docs/superpowers/specs/2026-06-15-neutrino-lb-cbor-proxy-design.md`.

Open question answered: CBOR/CoAP lives in the separate `neutrino-lb` crate,
**not** baked into the `Event`/`Request`/`Response` types — so it stays out of
Ruma's way and `neutrino-http`'s Router is untouched.

CoAP/UDP transport done (Layer B). `transport::coap` is a second
`WireClient`/`WireServer` impl (sibling of `transport::http`) over `coap-rs` +
`coap-lite`: CON request/response, blockwise (RFC 7959) for large `send_join`
state, Dendrite v1 federation path codes (+ literal fallback), forwarded
headers + exact HTTP status carried as CoAP options. Selected via
`LbConfig.wire: WireKind` (`Http` default). Two homeservers converge a join +
message over CoAP/UDP — see `crates/neutrino-http/tests/e2e_lb_coap_federation.rs`.
Per-message sizing for constrained links is tunable via
`WireKind::Coap { block1_size, max_message_size }` (request Block1 payload, and
the node's total framed-message budget that bounds inbound accept + outbound
Block2). `max_message_size` requires the `kaylendog/coap-rs` fork (patched in via
`[patch.crates-io]`), which adds `Server::*_with_config`.
Design: `docs/superpowers/specs/2026-06-18-neutrino-lb-coap-udp-transport-design.md`;
plan: `docs/superpowers/plans/2026-06-18-neutrino-lb-coap-udp-transport.md`.

Q-Block transport done (RFC 9177 NON-mode). `WireKind::CoapQBlock` is a sibling
of CON `Coap`: federation bodies travel as non-confirmable Q-Block bursts (up to
MAX_PAYLOADS unacked) with 4.08 missing-block recovery, via coap-rs
`send_qblock` / `set_qblock_config` (the `q-block` feature on the
`kaylendog/coap-rs` fork, backed by `kaylendog/coap-lite` `qblock-phase1`). The
`message.rs` mapping is reused verbatim. It is the **`neutrino-main` default**;
CON `Coap` and `Http` remain selectable. Tuning via
`WireKind::CoapQBlock { block1_size, qblock: QBlockTuning }` (RFC 9177 §6.2
timing). Two homeservers converge a join + message over Q-Block — see
`crates/neutrino-http/tests/e2e_lb_coap_qblock_federation.rs`. Design:
`docs/superpowers/specs/2026-06-24-neutrino-lb-qblock-transport-design.md`; plan:
`docs/superpowers/plans/2026-06-24-neutrino-lb-qblock-transport.md`.

Deferred follow-ups (write-ups, not done):
- integer-key / enum-key CBOR codec (Layer A; port of Dendrite `internal/lb`
  `cbor_codec.go` / `cbor_v1.go`); both transports still carry an **opaque**
  `JSON value ⇄ CBOR bytes` transcode. The single-byte CoAP path enums are now
  done (`transport::coap::paths`). MSC3079:
  https://github.com/matrix-org/matrix-spec-proposals/blob/kegan/low-bandwidth/proposals/3079-low-bandwidth-csapi.md
- value-level wire compression (small-MTU sizing analysis @ MTU 200): per-block
  cost is dominated by the re-sent Uri-Path. Path enums (done) collapse the fixed
  prefix to 2 B (1-B value + the mandatory CoAP option header — not 1 B). v12
  room/event IDs are `sigil + base64(32-B SHA-256)` (44 ch); carrying them as
  **raw 32 B** is lossless and saves 12 B/ID, but needs a
  decode→re-encode→verify-or-fall-back-to-text guard (2 trailing base64 bits;
  event IDs are compared as opaque strings, so a non-canonical re-encode would
  change identity). Even so, the two-hash-ID endpoints
  (`send_join`/`invite`/`send_leave`) are **capped at 64-B blocks @ MTU 200**:
  the two 32-B hashes are ~68 B of irreducible per-block options (128-B blocks
  need options ≤ 59 B). Only moving room/event IDs into the **once-sent body**
  (block 0) instead of the per-block path breaks that cap. `/send` + a short
  `txnId` already reaches 128-B blocks.
- X-Matrix wire form: the client sends `origin="…",destination="…"` (~55 B for
  short names) but the inbound side reads **only `origin`** (`auth.rs`). "Bare
  origin" (one CoAP option, drop `destination` + scheme/quote framing) → ~12 B; a
  per-peer 1-B index → ~2 B (name-length independent). It must ride **every
  block** (not body-only) where the network can rewrite source addresses, and
  would then double as the Block1 reassembly key — coap-lite keys partial
  assembly on `SocketAddr`, which is unstable under rewrite. send_join's real
  cost is its **Block2 state-DAG response**, which re-sends these request options
  on every block pull (the case the legacy stack SZX-hacked to 64 KB to avoid).
- CoAP blockwise *reassembly-time* cap: the transport now enforces
  `MAX_WIRE_BODY_BYTES` on the **assembled** body (ingress → 413, egress →
  transport error), matching the HTTP transport's handler-facing contract and
  bounding the transcode + forward of a legitimately large body. What remains: a
  cap *during* reassembly — coap-lite 0.13's `max_total_message_size` bounds only
  the negotiated per-block size, not the running total across Block1 chunks, so a
  peer streaming unbounded chunks can still grow coap-lite's internal buffer
  before the post-reassembly check fires. Acceptable under the trusted-network
  assumption; a true bound needs an upstream/forked cap on the block accumulators.
- Upstream the `coap-rs` `Server::*_with_config` change (carried on the
  `kaylendog/coap-rs` fork) so the `[patch.crates-io]` git pin can drop. Until
  then the build depends on the fork rev.
- SLIP / serial-link framing on top of the CoAP/UDP transport (the
  `cmd/test-coap/bridge` work) — the physical low-bandwidth link.
- per-hop timeouts on the sidecar's own reqwest clients (`LbConfig.timeouts`);
  today the originating `FederationClient` request timeout bounds the egress hop.
- Q-Block2 (response) per-fragment size knob: v1 has no `max_message_size`
  equivalent for the Q-Block path; Block2 follows coap-rs's szx default. A knob is
  a follow-up if a constrained link needs smaller response fragments.
- FFI/Element X exposure of transport choice: `CoapQBlock` is the `neutrino-main`
  default, not yet selectable from `NeutrinoConfig` / `DefaultNeutrinoService`.

### Sliding sync (MSC4186) gaps

Intentional gaps in the sliding-sync implementation — see `MSC4186-gaps.md`:

- request filters (`is_dm` / `is_encrypted` / `is_invite` / `room_types` / `not_room_types`) are parsed but ignored
- lazy-loading members
- room heroes
- `expanded_timeline`
- state-stub emission for removed state (clients keep their stale view of removed state)

### createRoom

- `initial_state`, `power_level_content_override`, and `creation_content` are not honoured
- `trusted_private_chat`'s invitee power-level bump is not modelled (createRoom does not process the `invite` list for power levels)

### Client-Server follow-ons

- client-timeline filtering on `soft_failed`: the column + field exist, but relayed timelines do not yet drop soft-failed events
- room-alias resolution: `#alias` → room id is unresolvable (no alias directory); the global `/join/{roomIdOrAlias}` reports `M_INVALID_PARAM` on any alias
- `/forget`
- displayname / avatar carry-over onto membership events

### Server-Server follow-ons

- orphan-staging garbage collection: a max-age sweep of `staged_events` (federation gap-fill ancestry that never grounds)
- inbound `/get_missing_events` deterministic ordering: the spec wants min-hops + lexicographic, we sort by `origin_server_ts`; the responder also stops at `limit` rather than continuing to the create event (the multi-round requester compensates functionally)
- large state-DAG handling: `send_join` / `send_leave` serialize the whole state DAG into one response and state-res holds it in memory — streaming JSON is a future option if rooms get large
- anti-entropy advertisement coalescing: the joined-set-growth advertisement (MSC anti-entropy-extension) drains all of a destination's owed rooms in one transaction, but the MSC's optional ~30s debounce window — coalescing triggers that arrive moments apart into a single send — is not implemented; each trigger that finds the link quiet advertises promptly. Deferred (MAY).

### Code-quality follow-ons (noted, not blocking)

- thread `own_server` as a parsed `OwnedServerName` rather than a `String` compared by value
- relocate `outbound_destinations` from the HTTP actor into `neutrino-state`
- fold the repeated `FederationClient` PUT-event idiom into one helper
- federated-invite error-code parity with the local invite path
- OOB-invite join review leftovers (noted, not blocking): `federated_join_if_remote` runs twice on the `join_by_id_or_alias`→`join` fall-through; a single private `join_core(hints)` would collapse it. No single shared "is this room remote / who is the inviter's resident server" predicate (invite/leave/join each re-derive it).
- multi-server / idempotency federation tests
- port the relevant Synapse + Complement membership-endpoint tests
- `storage_dir` empty-string handling: an exported-but-empty `NEUTRINO_STORAGE_DIR=` (and the FFI
  `NeutrinoConfig.storage_dir`) is taken literally as `""` rather than falling back to the default,
  aborting startup with an opaque `creating storage dir : …`; validate/normalise empty → default

## stack
- framework: axum + tokio
- serialization: serde + serde_json + some cbor library
- error handling: thiserror
- testing: cargo test + httptest

## architecture
storage is behind a StorageBackend trait — do not couple handlers to a concrete type.
SQLite is the only concrete implementation for now.
all handlers return Result<Json<T>, AppError>. AppError implements IntoResponse.
never use .unwrap() in handler code.

## non-goals (do not implement)
- Rate limiting
- Any authentication (access tokens) on the client-server API
- Tracing (distributed tracing / span instrumentation as telemetry) — note: basic HTTP request/response access logging IS in scope; that is not the same thing
- Restricted rooms (`join_authorised_via_users_server`) — documented, not implemented

## decisions log

### temporal state-group index (2026-06-22)
- State groups are implemented as a **temporal interval table** (`room_state`),
  not Synapse-style per-event snapshots or snapshot+delta chains. One row per
  `(room, type, state_key)` value tracks its lifetime `[start_pos, end_pos)`
  over the `stream_pos` axis. Current state at a point = `end_pos IS NULL`;
  historic = `start_pos <= P AND (end_pos IS NULL OR end_pos > P)`.
- Axis is the existing `events.stream_pos`. It is a valid topological order of
  the state DAG (a child is refused until its `prev_state_events` are
  persisted), so the interval query reconstructs the DAG-correct state-after
  **any** event, forks included — no separate state-group ids / edge table.
- Purpose is correctness *and* speed: authing an old event received over
  federation needs state-at-that-event, previously a recursive
  `prev_state_events` walk to the create event on every applied event. The
  index turns `state_at_heads` into one indexed read per head.
- Maintenance lives in the storage layer (`maintain_room_state`, called inside
  the `persist_resolved_event`, `create_room`, and `setup_room` write txns)
  rather than being plumbed down from `apply_pdu` via a new `Effect`: the store
  already runs state-res via `SqliteStateProvider`, so it computes the event's
  state-after from its own `prev_state_events` through the index (bounded, not
  recursive) and is atomic with the event write by construction. Smaller
  surface, no cross-crate signature change. A state event that *loses*
  resolution (empty `current_state` delta) is still recorded — it remains a
  state-DAG head a later event can name in `prev_state_events`.
- `current_state` is **kept** as the forward-extremity-merge authority. The
  index's "live" set (`end_pos IS NULL`) is state-after-the-latest-event, which
  equals current state only when there is a single state-DAG forward extremity;
  deriving current state from the index would mean re-running state-res over the
  FEs on every read (or materialising hypothetical merge events).
- **No read-side gate.** Every path that persists a state event maintains the
  index, so `room_state` is complete for every room the server knows about, and
  `SqliteStateProvider::state_after` reads it back unconditionally (the only
  `None` is a genuinely unknown event → caller recurses). "No migration" means
  the schema only ever runs against a fresh DB — there is no legacy un-indexed
  data to backfill — *not* a per-room tracked/untracked flag (an earlier
  `rooms.state_tracked` design was scrapped as redundant). NB the `None` signal
  keys on the `events` row, not on index presence: a state event reaching
  `events` *without* `maintain_room_state` would read back a wrong (incomplete)
  map, not `None`. That invariant ("every state-event write maintains the
  index") is enforced by a `debug_assert!` in `state_after` (own slot must
  resolve to the event itself), which fails loud in tests/debug rather than
  feeding bad state into auth.
- The point-in-time read is indexed by `ix_room_state_at (room_id, start_pos)`:
  the PK `(room_id, event_type, state_key, start_pos)` can't serve it (only
  `room_id` is an equality, the rest are ranges / unconstrained), so without
  this index the query scans the room's whole history. Confirmed via
  `EXPLAIN QUERY PLAN`; `(room_id, end_pos)` does *not* help (the planner
  ignores it through the `end_pos IS NULL OR end_pos > P` OR).
- Follow-up: the index is not maintained for `persist_historical_event`
  (timeline backfill, no production callers today). If a future backfill path
  persists **state** events it MUST maintain `room_state` too (out-of-order
  insertion is correct — closes use monotonic `stream_pos`, so earlier
  point-in-time queries are unaffected and the diff baseline self-heals — just
  churnier); leaving them out makes `state_after` return a wrong `Some`, which
  the `debug_assert!` above would catch. Fork/merge + supersession +
  slot-removal + losing-branch recording are covered by
  `room_state_fork_merge_matches_recursive_oracle` (differential vs the
  recursive walk).

### neutrino-lb CBOR proxy (2026-06-16)
- `neutrino-lb` is a **standalone sidecar proxy**; the CBOR transcode lives in it,
  not in `neutrino-http`'s HTTP layer.
- v1 is an **opaque** JSON↔CBOR transcode (no key remapping); the integer-key /
  enum-key codec is deferred.
- Outbound handoff is via **reqwest proxy mode** (one `Config.federation_proxy`
  field, default `None`); no URL construction or Router change in `neutrino-http`.
- `WireClient`/`WireServer`/`WireHandler` traits are scoped to the **wire hop
  only**; local hops stay plain HTTP. They are the CoAP seam; v1 ships one
  HTTP+CBOR impl (`transport::http`) behind them.
- HTTP-server substrate: **axum (`fallback`) + reqwest**.
- Deviations from the design spec (deliberate, recorded here rather than left
  silent): wire types were simplified — `WireRequest.dest: String` (not
  `OwnedServerName`), `body: Vec<u8>` (not `bytes::Bytes`), `LbConfig.upstream:
  String` (not `Url`); the `ruma`/`bytes` deps the spec listed were not pulled in.
  The ingress intentionally drops upstream **response** headers (Matrix S2S
  responses carry no semantic headers). Per-hop sidecar timeouts
  (`LbConfig.timeouts`) were not implemented (see deferred follow-ups).
- Forward risk (out of scope today): the JSON→CBOR→JSON round trip does **not**
  preserve byte-exact canonical JSON, so if event **signatures** are ever added,
  this proxy would break signature verification — it would have to canonicalize
  identically to the signer on both ends.
- In-process mode requires a loopback-reachable `bind_addr`: `upstream_url`
  rejects a concrete non-loopback address (e.g. a LAN interface) rather than
  sending the ingress→upstream hop off the loopback path (which would expose the
  unauthenticated CSAPI on the network). Loopback is used verbatim; `0.0.0.0` is
  rewritten to loopback (it still listens there). The sidecar config is
  derived/validated *before* binding the listener, so an illegal in-process combo
  fails fast without first claiming the public port. Deferred (option 2 of the
  review's finding #4): collapsing the three coupled `Config` fields (`bind_addr`
  / `federation_proxy` / `lb_ingress_bind`) into a `FederationMode { Direct |
  Proxied | InProcess }` enum so illegal states are unrepresentable at
  construction (incl. a fallible FFI `TryFrom`) — the current fix validates early
- Header pass-through is an **allowlist** (`authorization` + `x-matrix-*`), not a
  denylist: since the body is re-serialized JSON↔CBOR per hop, only headers the
  proxy explicitly understands may survive — a peer's stale `Content-Encoding` or
  smuggled framing header can't leak onward. `authorization` carries the
  `X-Matrix` origin credential the inbound side reads (no signatures); responses
  carry no semantic headers, so the allowlist forwards nothing on that path.
- Egress, called as an origin server (no destination authority), answers 502 not
  400: the sender retries a 5xx but permanently drops a 4xx, so a recoverable
  misconfig must not silently discard queued PDUs.
- Removed the vestigial standalone `neutrino-lb` binary (`src/main.rs`) +
  `LbConfig::from_env` and its `NEUTRINO_LB_EGRESS_BIND`/`_UPSTREAM` env vars:
  nothing built or ran that binary (the in-process sidecar — `neutrino` +
  `NEUTRINO_LB_INGRESS_BIND`, what the testrig uses — superseded it). `LbConfig`
  (the struct) and `serve()` stay; `neutrino-main` builds the config
  programmatically and runs the sidecar in-process.
  but still surfaces the error at `entrypoint`.

### Q-Block transport (2026-06-24)
- Q-Block (RFC 9177) NON-mode added as `WireKind::CoapQBlock`, sibling of CON
  `Coap`; both selectable, CON retained as lossless/debug baseline. Q-Block is the
  `neutrino-main` default. `message.rs` reused verbatim — Q-Block changes only the
  send call (`send_qblock`) + config. `QBlockTuning` wraps the NON knobs so
  coap-rs's `QBlockConfig` (CON fields panic) is not leaked into `LbConfig`.
  Cargo: `coap` rev bumped to the Q-Block tip, `coap-lite` pinned to
  `qblock-phase1`, `q-block` feature enabled.
- Q-Block2 response OOM fix (review I1): the Q-Block client now calls
  `set_max_total_message_size(max_body_bytes)` in `client_for`, so coap-rs bounds
  Q-Block2 response reassembly at the cap (`QBlockReceiver` aborts before allocating)
  instead of leaving it at `usize::MAX` and only catching it post-reassembly. Q-Block1
  request sizing is unaffected (`send_qblock` uses the static `block1_size`, not the
  MTU-derived path).
- `QBlockTuning` config & docs (review group B — I4/I6/I8): `with_qblock` now derives
  `request_timeout` from the tuning (floor `REQUEST_TIMEOUT`, ≥ 2× coap-rs's
  `non_receive_timeout * (non_max_retransmit + 2)` linger) so a long custom tuning can't be
  killed mid-recovery by the outer timeout (I4). Doc corrected: coap-rs's extra fields are
  *unread* on the NON path, not panicking, and `non_partial_timeout` is a NON-mode (not
  CON-only) knob (I6). The "both ends must agree" note is reframed as a coap-rs linger-model
  limitation (RFC 9177 allows independent per-peer timing), with the "linger until exchange
  completes" follow-up recorded (I8).
- Cargo reproducibility (review group C — I7): `coap-lite` in `[patch.crates-io]` is now
  rev-pinned (`d45e952…`) instead of branch-pinned (`qblock-phase1`), so `cargo update` can't
  silently move it — matching the `coap` rev-pin discipline (Cargo.lock unchanged, same commit).
  Reconciled the design doc's stale `coap` rev (`0af46cd1e…` → the shipped `df0a355…`) and the
  `coap-lite` pin mechanism.
- Q-Block test hardening (review group D): the lossy relay now counts dropped datagrams and the
  Q-Block loss test asserts the targeted drop actually happened (so "recovery" can't be a vacuous
  lossless pass — I9); the concurrency test uses 64 B blocks + 256–496 B bodies so it exercises
  *multi-block* Request-Tag burst demux, not just single-PDU correlation (I10); added a black-hole
  dead-peer Q-Block timeout test via a `request_timeout`-override test ctor (I12); tightened loose
  `is_ok()`/`let _` shutdown asserts to `matches!(.., Ok(Ok(Ok(()))))` (I17). Deliberately not done:
  inject loss into the e2e (I11 — needs a relay between sidecars + server_name rework; unit loss
  test covers recovery), de-flaking the fast recovery timers (I13 — outer timeouts already prevent
  hangs), dropping the change-detector/compile-gate tests (I16 — kept per the no-delete-tests rule;
  the feature-gate test is a real compile guard, the defaults test guards interop-critical
  constants), and de-duplicating the per-module `PlainEcho`/`BodyEcho` test helpers (I18 — matches
  existing file style, low value).
- Q-Block NON-mode DoS hardening (review group A — I2/I3/I5; cross-repo). coap-rs patched
  (`kaylendog/coap-rs` rev `36cc2dc`): the inbound Q-Block1 reassembly cap is now settable
  (`set_qblock_max_body_len`), concurrent partial transfers are capped
  (`set_qblock_max_transfers`, default 64), `non_partial_timeout` is wired as an absolute
  partial-transfer TTL in `drive_receive`, each reassembly is bound to its first block's source
  address (cross-source blocks dropped), and the client's background `drive_send` is aborted when
  `send_qblock` returns. coap-lite needed no changes (`RangeSet` already bounded, `QBlockReceiver`
  caps before allocating). neutrino-lb bumps the `coap` rev and wires the new knobs in
  `CoapWireServer::serve`: `set_qblock_max_body_len(max_body_bytes)` (aligns the reassembly abort
  with the 413 contract — the ingress analogue of I1) and `set_qblock_max_transfers(16)`
  (`MAX_QBLOCK_INFLIGHT_TRANSFERS`; worst case 16 × 64 MiB). The client token counter now starts
  from a random base (`random_token_seed`) so Request-Tags aren't predictable — defence in depth
  for the source binding.
