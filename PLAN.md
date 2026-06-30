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

An in-process sidecar that transcodes Server-Server federation **bodies**
JSON↔CBOR behind a `WireClient`/`WireServer` trait seam. The codec is
integer-key (well-known Matrix object keys → small ints; event IDs → raw 32 B;
unknown keys/strings pass through), a port of Dendrite `internal/lb`
(`codec::keys`). `neutrino-http` routes
outbound federation through it via the optional `Config.federation_proxy` (`None`
= direct, the default). The CBOR/CoAP layer is contained in this crate — out of
Ruma's and the Router's way. Each transport has a two-homeserver join+message e2e
(`crates/neutrino-http/tests/e2e_lb_*`).

Three wire transports, selected by `LbConfig.wire: WireKind`:
- `Http` — v1, JSON↔CBOR over HTTP (the `WireKind` default; debuggable baseline).
- `Coap` — CON CoAP/UDP, RFC 7959 blockwise, Dendrite v1 path enums, forwarded
  headers + exact HTTP status as CoAP options; tunable via
  `Coap { block1_size, max_message_size }`.
- `CoapQBlock` — RFC 9177 NON-mode robust transfer (burst + 4.08 missing-block
  recovery); the **`neutrino-main` default**. Reuses the `Coap` message mapping;
  tuned via `CoapQBlock { block1_size, qblock: QBlockTuning }` (RFC 9177 §6.2).

DoS posture (the public UDP port is the only network-exposed surface): assembled
bodies are capped at `MAX_WIRE_BODY_BYTES` (ingress→413, egress→error); the
Q-Block path also caps response/inbound reassembly *before* allocation, caps
concurrent inbound transfers, enforces an absolute partial-transfer TTL, binds
each reassembly to its source address, and derives `request_timeout` from the
tuning so recovery isn't killed mid-exchange. Needs the `kaylendog/coap-rs` +
`kaylendog/coap-lite` forks (q-block API, DoS knobs, `Server::*_with_config`),
rev-pinned in `[patch.crates-io]`. Designs:
`docs/superpowers/specs/2026-06-{15,18,24}-neutrino-lb-*`.

In-process datagram transport (`transport::coap::datagram`, STEP 1 done): an
additive iroh-free CoAP path beside the UDP one, for the embedded/Android target.
Replaces the OS UDP socket with a `DatagramLink` trait (ffi implements it over an
iroh QUIC endpoint, keyed by 32-byte node ids — no socket/IP/ports). A `Hub` runs
one drain task that classifies each inbound datagram by CoAP header code
(request→server listener, else→per-node client inbox). `IrohCoapWireClient` /
`IrohCoapWireServer` reuse the shared `exchange` helper + `CoapDispatch` from the
UDP path. Wired into `LbConfig`/`serve` (STEP 2 done): `LbConfig.link:
Option<Arc<dyn DatagramLink>>` selects the path purely by injection — `Some`
routes the CoAP wire over the link, `None` keeps UDP. `neutrino-main::entrypoint`
takes a `FederationLinkFactory` (4th param) that builds the link from the resolved
node secret; `TunnelResolver` now yields the bare 64-char hex node id (not a vip).
Not yet implemented in ffi.

STEP 3 done: ffi's `IrohTransport` implements `neutrino_main::DatagramLink`
(iroh QUIC datagram keyed by 32-byte node id); `start()` builds it via a
`FederationLinkFactory` injected into `entrypoint`. The TUN/IP-relay data path
(`Tunnel`/`RelayStack`/`relay_driver`/`TunPacketIo`/`start_tunnel`/`stop_tunnel`/
`tunnel_address`, `TableSink`, the route table + vip) and the `neutrino-relay`
crate are deleted. `TunnelHandoff` now carries only the resolved `server_name`.
The federation data path is: homeserver → lb egress (CoAP/CBOR) →
`IrohCoapWireClient` → `DatagramLink::send(node, datagram)` → iroh over BLE.
Done:
- Integer-key CBOR codec (Layer A; port of Dendrite `internal/lb`): all transports
  now carry the integer-key transcode (`codec::keys`, 143 keys: 137 from Dendrite
  + 6 MSC4242 state-DAG keys) plus event-ID
  →raw-32 B packing with a re-encode/fall-back-to-text guard. CoAP path enums were
  already done (`transport::coap::paths`). (MSC3079.)

Deferred follow-ups (write-ups, not done):
- Wire-size reduction for small MTUs: carry v12 **room** IDs as **raw 32 B**
  (vs `sigil+base64`, −12 B/ID; needs a re-encode-or-fall-back-to-text guard like
  the event-ID path) and
  X-Matrix as a bare/indexed origin (~55 B → ~2–12 B; must ride every block and
  doubles as the reassembly key). send_join's real cost is its Block2 state-DAG
  response re-sending these options per block.
- CON-path reassembly-time cap: the Q-Block path now bounds reassembly *before*
  allocation, but the RFC 7959 CON path still relies on the post-reassembly cap —
  coap-lite's accumulator isn't externally bounded mid-transfer. Acceptable under
  the trusted-network assumption.
- Upstream the fork changes (`Server::*_with_config`, q-block API, DoS knobs) so
  the `[patch.crates-io]` git pins can drop.
- SLIP / serial-link framing on the CoAP/UDP transport — the physical link.
- Per-hop timeouts on the sidecar's own reqwest clients (`LbConfig.timeouts`).
- Q-Block2 (response) per-fragment size knob (no `max_message_size` equivalent
  on the Q-Block path yet; Block2 follows coap-rs's szx default).
- FFI/Element X exposure of transport choice (`CoapQBlock` is the default, not yet
  selectable from `NeutrinoConfig`).

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
- `neutrino-engine` extraction COMPLETE (phases 0-2 landed 2026-06-30): the room
  runtime lives in `neutrino-engine`; `neutrino-http` is axum glue + transport
  impls. No in-memory `StorageBackend` double was needed — the engine tests open a
  real sqlite `:memory:` via a dev-dep (store-sqlite doesn't depend on engine, so
  it's acyclic). Remaining *optional* follow-on: split `neutrino-http` into c2s and
  s2s route groups (now trivial — both are thin layers over the engine).
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

### user discovery over BLE — registry + directory search (2026-06-30)
- The embedded server has no Matrix user directory; peers are learned out of
  band over the BLE mesh. Each device advertises its display name + node id
  (manufacturer data, company id `0xDFFF`); a search of the invite box answers
  from the discovered set.
- `DiscoveryRegistry` lives in `neutrino-ctl` (server-wide host-pushed state, the
  read-queried sibling of `Command` — `Command` flows through the mpsc and is
  *consumed*, this is *queried*, so it's a shared `Arc<DiscoveryRegistry>` read
  directly by the handler, not the command channel). Keyed by `server_name`
  (== node id; stored as a plain `String`, NOT iroh's `NodeId`, so ctl gains no
  iroh coupling — it's the same identifier federation already keys on).
- Registry stays **localpart-agnostic**: `DiscoveredPeer` carries `localpart`
  verbatim and the registry never builds a user id. The embedded host (ffi,
  iroh-aware) supplies the fixed constant `n`, but that's the host's choice — a
  future multi-user host needs no registry change.
- Write model is **snapshot replacement** (`replace`, one call per scan): a peer
  out of range simply stops appearing, no removal bookkeeping. `upsert` kept for
  incremental callers. `search` is case-insensitive substring on display name,
  sorted `(display_name, server_name)` for determinism; the handler applies the
  `limit` cap + `limited` flag.
- `POST /_matrix/client/v3/user_directory/search` rebuilds `@{localpart}:{server_name}`
  per hit and skips any peer whose advertised fields don't parse to a valid id
  (defensive, not fatal). Hand-rolled request/response JSON (matching the spec
  wire shape) + ruma `OwnedUserId` for id validation — avoids enabling a new ruma
  feature, consistent with the `members`/`profile` handlers.
- **FFI handoff (done):** the `Arc<DiscoveryRegistry>` is dependency-injected —
  ffi `start` creates it, keeps a clone on `NeutrinoHandle`, and passes a clone
  through `entrypoint` (new trailing `Option<Arc<DiscoveryRegistry>>` param,
  defaulted; non-embedded callers pass `None`) → `serve` (required `Arc` param)
  → `AppState::from_store_with_discovery`. `NeutrinoHandle::set_discovered_peers(
  Vec<DiscoveredPeer{node_id, display_name}>)` replaces the snapshot, stamping
  the fixed localpart `n` and a `last_seen_ms` clock read. `display_name` added
  to `NeutrinoConfig`/`Config`; `/profile` de-stubbed (self → configured name,
  peer → registry lookup by `server_name`, unknown → `{}`). ctl/http/main/dev
  binary compile + clippy-clean + tests green; **ffi itself is unbuildable in
  the sandbox** (bluer/iroh uncached) — verified by review + the buildable stack.
- **Outstanding:** Android — fork `blew` to extended advertising
  (`startAdvertisingSet`, `setLegacyMode(false)`; the 32 B node id + ≤20 B name
  overflow the 31 B legacy cap) + manufacturer data `0xDFFF` =
  `[version][node_id:32][display_name]`, and switch the scanner to
  `setLegacy(false)` + surface `getManufacturerSpecificData(0xDFFF)` up to a new
  discovery callback that calls `set_discovered_peers`.

### room runtime moved into `neutrino-engine` (2026-06-30)
- Phase 2 of the extraction: `room_actor` (RoomActor/RoomRegistry), `sender`,
  `worker`, `reconcile`, `gapfill` physically moved from `neutrino-http` into
  `neutrino-engine`, along with the shared runtime utilities (backoff/jitter
  consts, `TxnIdGen`, `now_ms`, `stage_and_poke`) and `server_in_room` (a
  store-backed membership predicate, now in `engine::reconcile`). `auth.rs`'s
  X-Matrix `authenticated_origin` stays in http; `map_apply_err`
  (RoomActorError→FedError) stays in http as the handler-side adapter.
- **2a (in-place generic-ization) first, then 2b (move):** `sender`/`worker`/
  `reconcile`/`gapfill`/`server_in_room` were made generic over
  `S: StorageBackend (+ WithStateProvider)` while still in http (tests green),
  so the physical move was a near-pure relocation. Same pattern as Phase 0.
- **Engine deps:** prod = common, state, store(traits), ruma, rand, tokio,
  tokio-util, tracing, serde(_json), async-trait, thiserror. **Dev-only** =
  store-sqlite + tempfile (the runtime tests open a real `:memory:` store). No
  production dep on a concrete backend; no cycle (store-sqlite ⊀ engine).
- **No hand-rolled in-memory `StorageBackend`:** the deferred Phase-0 question is
  moot — engine tests bind sqlite via the dev-dep, so a second impl would be pure
  duplication.
- **Sender tests rewritten to an engine-local `FederationTransport` stub** (no
  axum/reqwest): the 14 sender tests assert sender *behaviour* (drain / retry /
  4xx-drop / txn-id reuse / advertisement), all observable at the one-method
  port. The stub records decoded PDUs + serialised forward-extremities so the
  assertions are preserved verbatim. Real-HTTP wire coverage stays in the
  `e2e_lb_*` + `neutrino` federation tests. (Per the readability discussion with
  Kegan: one port method is not a "shadow API".)
- `now_ms` / `MAX_PDUS_PER_TXN` are engine-owned (`pub`) and imported by the http
  handlers/transport that still need them.

### engine outbound ports — `neutrino-engine` crate created (2026-06-30)
- Phase 1 of the extraction: created the `neutrino-engine` crate holding only the
  outbound port traits and the types that cross them. No runtime code moved yet —
  the runtime (still in `neutrino-http`) now depends on engine traits, not on
  http/reqwest types, so Phase 2 is a near-pure file move.
- Two ports: `FederationTransport::send_transaction` (the per-destination sender
  pool's only network call) and `MissingEventsFetcher::fetch` (promoted from the
  `pub(crate)` trait in `gapfill.rs`). Both object-safe → honest `dyn`.
- Three types relocated into engine: `MissingEventsQuery`, `ForwardExtremities`
  (+ `is_empty`), and a new neutral `TransportError { Status(u16), Transient }`
  replacing `FederationClientError` at the boundary — `FederationClientError`
  carries `reqwest::Error` and must NOT leak into engine. `FederationClientError`
  stays in http for the membership-client methods (`make_join`/`send_join`/… —
  handler-driven, not runtime, deliberately not ported). `From<FederationClientError>
  for TransportError` maps `Status → Status`, else `Transient(display)`.
- The inversion: `sender::spawn` now takes an injected `Arc<dyn FederationTransport>`
  instead of building the `FederationClient` from `origin`/`federation_proxy`;
  `serve()` constructs the client and injects it. The direct-vs-lb-proxy routing
  stays inside the http `FederationClient` impl, so engine is transport-oblivious
  (verified: the three `e2e_lb_*` proxy tests still pass).
- Engine deps are minimal (ruma, serde, serde_json, async-trait, thiserror) — no
  reqwest, no axum, no neutrino-common/store/state yet; those arrive in Phase 2
  when the runtime moves.

### room runtime decoupled from concrete store (2026-06-30)
- Phase 0 of the planned `neutrino-engine` extraction: make the `StorageBackend`
  trait genuinely load-bearing instead of cosmetic. `RoomActor` / `RoomRegistry`
  were hardcoded to `Arc<SqliteStore>`; they are now generic
  `<S: StorageBackend + WithStateProvider>`, so the room-runtime code compiles
  against the trait surface alone — proving it sufficient.
- `with_state_provider` moved from an inherent `SqliteStore` method to a new
  `neutrino_store::WithStateProvider` trait (implemented by `SqliteStore`). It is
  a **generic** method (HRTB closure + owned return `R`), so it is NOT
  object-safe — `dyn StorageBackend` is impossible. Hence the runtime is generic,
  not `dyn`, and `WithStateProvider` is kept OUT of the `StorageBackend`
  super-trait so `dyn StorageBackend` stays available for any future read-only
  consumer.
- New workspace dep `neutrino-store → neutrino-state`: the trait references both
  `StorageError` (store) and `StateProvider` (state), so its only acyclic home is
  `neutrino-store`. `neutrino-state` does not depend on `neutrino-store`, so no
  cycle. This is the seam-1 coupling from the extraction sketch.
- The `App` composition root in `neutrino-http` still instantiates with the
  concrete `SqliteStore` (and `worker`/`SyncState` name it) — intentional: http
  is the composition root and may know the concrete backend. Only the runtime
  *internals* are store-agnostic.
- Deliberately did NOT add a hand-rolled in-memory `StorageBackend` double: tests
  already use `SqliteStore::open_in_memory()`, so a second impl would duplicate
  it, and its only payoff (engine tests without the sqlite dep) does not exist
  until the `neutrino-engine` crate does. Deferred to the extraction.

### datagram origin↔node binding (2026-06-29)
- The iroh datagram link is the one trust boundary (the homeserver itself runs no
  signature checks — trusted mesh). `federation::auth` trusts `X-Matrix origin`
  *only because the network layer authenticated the peer*, so that binding MUST be
  enforced at the transport. The datagram ingress (`CoapDispatch.node_binding` →
  `Hub::origin_binding_violation`) rejects (401) any request whose claimed origin
  node id ≠ the link-authenticated source node — a peer may assert only its own
  origin. Enforced in neutrino-lb's datagram path only; UDP/HTTP (trusted LAN, no
  peer auth) keep prior behaviour. open/permissionless federation is intended, so
  there is deliberately NO peer allowlist — anyone may connect, but no one may
  impersonate another node.
- Soundness prerequisite: the node↔synthetic-`SocketAddr` map is now a lossless
  bijection (`Hub::addr_for`/`node_for`, a monotonic counter), not the prior
  18-of-32-byte hash. coap-rs stamps `request.source` from the responder address,
  so the exact source node must be recoverable from it; the old lossy projection
  was a grindable 144-bit-prefix collision that would let a peer be resolved as
  another node.

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
- v1 shipped an **opaque** JSON↔CBOR transcode (no key remapping). **Superseded
  2026-06-25:** the codec now does Dendrite-style integer-key remapping
  (`codec::keys`) + event-ID→raw-32 B packing, in place (no opaque
  fallback). Wire bytes changed; safe because a sidecar pair always deploys
  together. `canonical`-mode (Dendrite's test-only flag) intentionally not ported
  (YAGNI; `serde_json` already sorts keys). Decode *errors* on a malformed CBOR
  map key where Dendrite silently drops it — no-data-loss over Dendrite parity.
- **2026-06-26:** Extended the key table with 6 MSC4242 state-DAG keys
  (codes 138-143: `prev_state_events`, `state_dag`, `partial_state_event_ids`,
  `partial_auth_chain_ids`, `members_omitted`, `additional_creators`). Codes 138+
  are neutrino's own (the table is no longer Dendrite-identical; fine — we federate
  neutrino↔neutrino). Event-ID *values* in these fields auto-pack to 32 B.
- **2026-06-26:** Event-ID-shaped CBOR *map keys* now also pack to raw 32 B
  (symmetric with values), closing the `partial_auth_chain_ids` follow-up. Encode
  routes non-table object keys through `string_to_cbor`; decode reverses via
  `bytes_to_event_id`. Field-agnostic (any `$`-shaped canonical key), lossless
  (non-canonical/non-event-ID keys stay text), no panics.
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
- Added as `WireKind::CoapQBlock`, a sibling of CON `Coap` (both selectable, CON
  kept as the lossless/debug baseline); the `neutrino-main` default. `message.rs`
  reused verbatim — Q-Block changes only the send call + config. `QBlockTuning`
  exposes just the NON knobs, mapped to coap-rs's `QBlockConfig` at construction.
- Review hardening (I1 + groups A–D), much of it on the `kaylendog/coap-rs` fork
  (rev `36cc2dc`):
  - Reassembly is bounded *before* allocation on both legs — egress response
    (`set_max_total_message_size` in `client_for`, I1) and ingress request
    (`set_qblock_max_body_len`, aligned to the 413 contract). Concurrent inbound
    transfers are capped (`set_qblock_max_transfers(16)`) with an absolute
    `non_partial_timeout` TTL, so a slow / many-transfer peer can't pin memory (I2).
  - Each inbound reassembly binds to its source address (cross-source blocks
    dropped) and the client token base is randomised so Request-Tags aren't
    guessable (I3); the background `drive_send` is aborted when `send_qblock`
    returns (I5); `request_timeout` is derived from the tuning so recovery isn't
    killed mid-exchange (I4).
  - `coap-lite` rev-pinned for reproducibility (I7); docs corrected — coap-rs's
    extra NON-config fields are unread, not panicking (I6); tests hardened — loss
    test proves real recovery, concurrency test is multi-block, dead-peer /
    oversized / source-binding cases added (I9/I10/I12).
  - Deferred with rationale: e2e loss injection (I11), recovery-timer de-flake
    (I13), change-detector / helper-test cleanup (I16/I18).
