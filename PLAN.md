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
- DM `is_direct` is carried onto *local* baked invites only, not federated remote invites; `m.direct` account-data write 405s — remote DMs are not tagged direct

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

### createRoom federates remote invitees (2026-07-02)
- **Bug:** starting a DM (element-x `create_dm` → one `createRoom` with
  `is_direct:true` + `invite:[remote_user]`) never delivered the invite. The
  createRoom path baked an invite `m.room.member` straight into the initial batch
  via `store.create_room`, bypassing the room actor entirely — so neither the
  actor's transaction fan-out (`outbound_destinations`) nor the dedicated
  `/invite` handshake ever ran. A remote invitee's server isn't in the room's
  joined-set anyway, so fan-out could never reach it; invites *require*
  `PUT /federation/v2/invite`. The standalone `POST /invite` handler already did
  this, which is why explicit invites worked but DM-creation invites did not.
- **Fix (option a):** `build_initial_events` now bakes only *local* invitees;
  `create_room`, after persisting the room, federates each *remote* invitee via
  `federation::invite::federated_invite` (same path as the standalone handler).
  Best-effort — the room is already persisted, so a failed invite is logged and
  left for the client to retry rather than unwinding the room. Local/remote split
  shares one helper (`invite_targets`) so both sides apply identical rules.
- **Still outstanding (DM polish, not fixed here):** `is_direct` is not carried
  onto the federated/remote invite member event (only local baked invites get
  it), and `PUT …/account_data/m.direct` returns 405 — so remote DMs won't be
  tagged as direct on either side yet.

### sliding-sync wedge: HTTP backstop timeout + task-dump-on-fire (2026-07-02)
- **Revises the 2026-07-01 "offline sync hang → executor starvation" theory.** A
  fresh repro (create-room-hang.log) shows a single sliding-sync long-poll
  (`pos=29`) that starts and never returns — its own 30s deadline never fires —
  while, *during the hang*, store reads (`/members`, 1.77ms) and a store write
  (`createRoom`, 4.4ms, which fires `notify_watch`) both complete, and the
  executor-stall watchdog never logs. So the executor is live and the pools are
  healthy; only that one task's wakers are lost (it's wedged before/at its
  long-poll `select!`, since createRoom's watch edge — confirmed fired at
  store/rooms.rs:162 — didn't wake it either). Root cause of the lost waker is
  not yet pinned from logs alone.
- **Backstop (symptom fix, always on):** the http `sync()` wrapper now runs
  `sliding_sync::handle` inside `tokio::time::timeout(BACKSTOP_TIMEOUT)`
  (`BACKSTOP_TIMEOUT = 40s = MAX_LONG_POLL_TIMEOUT + 10s slack`). The outer
  timer registers its own waker with the time driver — which is provably live
  (the watchdog heartbeat, itself a `tokio::time::interval`, keeps firing) — so
  it fires regardless of which inner await is stuck. On fire the wrapper drops
  `handle` (dropping its held `conn` guard → frees the per-conn lock) and
  returns 504 `M_UNKNOWN`, which the client's serial sync loop retries, instead
  of hanging forever. Invariant test guards `BACKSTOP_TIMEOUT > MAX_LONG_POLL_TIMEOUT`.
- **Diagnostic (root-cause pin, opt-in):** on backstop fire, `dump_wedged_tasks()`
  logs every task's await backtrace via `Handle::dump()`, gated on
  `all(tokio_unstable, feature = "task-dump")`. The `task-dump` cargo feature
  (chained neutrino-ffi → -main → -http → `tokio/taskdump`) is OFF by default;
  tokio's `taskdump` hard-requires `--cfg tokio_unstable`, so a diagnostic build
  is `RUSTFLAGS="--cfg tokio_unstable" cargo build -p neutrino-ffi --features
  task-dump` (Android = linux/aarch64 satisfies tokio's platform gate; usable
  traces also want `-C force-frame-pointers=yes`). Without the feature the
  backstop still fires and logs context (user/conn_id/pos/elapsed) — just no
  per-task backtraces. The real dump path is API-verified against tokio 1.52 but
  not compiled in-sandbox (`backtrace`/`addr2line` uncached, network blocked).

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
  the fixed localpart `n` and a `last_seen_ms` clock read. ctl/http/main/dev
  binary compile + clippy-clean + tests green; **ffi itself is unbuildable in
  the sandbox** (bluer/iroh uncached) — verified by review + the buildable stack.
- **Local display name is persisted in the store, not config (2026-06-30).**
  The client sets it via `PUT /_matrix/client/v3/profile/{user}/displayname`
  (+ `GET .../displayname`), so it MUST persist — it lives in the `IdentityStore`,
  not a startup `Config` field (an earlier `Config.display_name` was removed as a
  redundant second source of truth). The `node_identity` table was replaced by a
  key/value `server_identity` table (`key='secret'` → 32 B, `key='displayname'`
  → text; room for more k/v facts without a schema change — no migration, fresh
  DB only). `IdentityStore` gained `get_display_name`/`set_display_name`.
  `/profile` resolves: self → stored name (default **"Neutrino"**, not "Alice"),
  discovered peer → registry by `server_name`, unknown remote → `{}`.
- **BLE discovery landed end-to-end (2026-07-01).** Manufacturer id **`0x0E1E`**,
  payload `node_id:32 ‖ display_name` in a BLE-5 **extended** advert (full 32 B
  node id + name overflows the 31 B legacy cap; carrying only a 12 B prefix was
  rejected — the directory must return a real `@n:{node_id}` and federation dials
  the full id, and a prefix only resolves post-handshake). Layers:
  - **`vendor/blew`** (forked crates.io `blew` 0.2.3, `[patch.crates-io]`):
    `manufacturer_data` on `AdvertisingConfig`/`BleDevice`; Android peripheral →
    `startAdvertisingSet`(non-legacy)+`addManufacturerData`, Android central →
    `setLegacy(false)` scan surfacing the first mfr entry; Linux/bluer + JNI both
    wired; Apple advertise left `None` (CoreBluetooth peripherals can't carry mfr
    data — not a target).
  - **`vendor/iroh-ble-transport::discovery`**: payload codec + `DiscoverySink`;
    `BleTransportConfig.display_name` → advert; central loop decodes on
    `DeviceDiscovered` → `BleTransport::discovered_peers()` snapshot stream;
    `BleTransport::set_display_name` re-advertises (mutable driver config).
  - **main**: reads `get_display_name()` (default "Neutrino") into a
    `watch<String>` — tx→http (PUT handler pulses on `set_display_name`),
    rx→`link_factory` (initial advert + re-advertise).
  - **ffi**: `link_factory` builds the transport with the name, drains
    `discovered_peers()` → the `DiscoveryRegistry` (hex node id = `server_name`,
    fixed localpart `n`), and re-advertises on the name watch.
  - **`set_discovered_peers` removed**: the transport is now the single registry
    writer (it scans+decodes), so the earlier host-push FFI method + its
    `DiscoveredPeer` uniffi record were redundant and dropped.
  - Verified: ctl/http/main/dev-binary build + clippy-clean + tests green
    (http 257, main 14+4, ctl 10, transport codec tests CI-runnable). blew +
    iroh-ble-transport + ffi are review/parse-verified locally (no libdbus);
    Kegan confirmed `neutrino-ffi --release --features ble` builds.
  - **Remaining (host-side, not Rust):** none in-tree; the Kotlin `blew`
    peripheral/central changes live in `vendor/blew/android/` — confirm the
    EX-Android build consumes those (vs the `bindings/...` copy).
### BLE permanent-wedge on peer restart: registry dial-stage timeout + Dead revive (2026-07-01)
- **Symptom:** force-stop a BLE peer then bring it back → permanently unreachable,
  symmetric on both devices ("as if BLE is off"), never recovers.
- **Root cause (registry state machine, `iroh-ble-transport`):** an entry that
  reaches `Handshaking` (QUIC handshake stalled on a re-established link) is stuck
  forever — `Handshaking` has no tick timeout, `handle_advertised` is a no-op
  (`_ => {}`) for `Handshaking`/`Connecting`/`Connected`/`Draining`/`Dead`, and
  `handle_send_datagram` only *buffers* (Handshaking/Connecting) or *rejects*
  (Connected-no-pipe/Draining/Dead) — none re-dial. So sends buffer into a
  never-completing handshake, adverts are ignored, and GC never reaps it.
- **Why only now:** disabling relay/DNS (correct for the offline BLE-mesh target)
  forces all traffic onto the BLE custom transport. WiFi+iroh-DNS testing resolved
  peers' IPs and connected over the IP transport, so the BLE registry was never
  exercised — the wedge was always latent. NOT caused by the discovery-disable;
  revealed by it. Discovery itself is healthy (confirmed: scanner sees the peer
  strongly, `discovered[prefix]` is current). iroh's `resolve_remote`
  (path_state.rs) also short-circuits on a cached path, but the BLE token resolves
  live to `discovered[prefix]`, so that isn't the blocker.
- **Fix 1:** `DIAL_STAGE_DEADLINE` (10s) — `handle_tick` now times out a
  `Connecting`/`Handshaking` entry, closes any half-open channel, and drops it to
  `Reconnecting` (or `Dead` past `MAX_CONNECT_ATTEMPTS`) so it re-dials.
- **Fix 2:** `handle_advertised` revives a `Dead` peer that starts advertising
  again into a fresh dial (mirrors the `Unknown` arm; resets `consecutive_failures`)
  instead of waiting out `DEAD_GC_TTL` + a subsequent send.
- 4 unit tests added. Vendored crate ⇒ not compilable in-sandbox (no dbus);
  rustfmt-checked, CI verifies. Keep the discovery-disable (BLE-only is correct).
- **Follow-up (same day):** recovery was still ~78s (3 retries) because a dead
  pipe wasn't torn down until the 20s wedged-pipe watchdog. `handle_stalled` (the
  reliable-LINK_DEAD `Stalled` handler) only drained `Connected` and dropped the
  `Stalled` for any other phase — including `Handshaking`, the common case (pipe
  dies mid-handshake, peer never ACKs). Fixed: `handle_stalled` now drains
  `Connected` **or** `Handshaking` → teardown at ~6s not ~20s. Also added always-on
  registry phase-transition logging at `info` (snapshot `PhaseKind` per peer in
  `handle()` before dispatch, log change/create/remove after) so future wedges are
  debuggable from logs. The deeper root — a *fresh* BLE connection passing no data
  for the first 1-2 tries (peer ACKs nothing despite a healthy pipe + correct MTU)
  — is still open; needs trace-level (`iroh_ble_transport::transport::reliable=trace`)
  fragment RX/TX logs on both ends to pin.
- **Root cause found (trace):** the failing leg is **peripheral→central GATT notify**,
  and it's in the Kotlin glue (`bindings/.../BlePeripheralManager.kt`), not the Rust.
  The per-device notify semaphore was released only by `onNotificationSent`; when the
  Android stack accepts `notifyCharacteristicChanged` but can't transmit it
  (`ais_request_cback: Unable to send GATT server response`), that callback never
  fires → the permit wedges for the whole connection → the receiver can't notify its
  QUIC handshake response → LINK_DEAD, 2-3 retries (~78s). Fixed by replacing the
  held-until-callback semaphore with a self-healing in-flight **deadline** (reopens on
  `onNotificationSent` OR after `NOTIFY_INFLIGHT_TIMEOUT_MS=500` if the callback is
  lost). Kotlin not compilable in-sandbox; verified by inspection, Kegan builds.
  Rust registry `handle_stalled` (Connected|Handshaking) + `Handshaking` tick timeout
  + Dead-revive-on-advert speed each failed-attempt teardown as defence-in-depth.

### Offline sync hang: disable iroh DNS discovery + executor-stall watchdog (2026-07-01)
- **Symptom:** creating a room with **no network** hung — the client's `/sync`
  long-polls never returned (nor hit their 30s timeout). Reproduces only offline
  (works with network), and intermittently.
- **iroh discovery disabled.** `Endpoint::builder(N0DisableRelay)` applies the
  full `N0` preset, which *appends* a `PkarrPublisher` + `DnsAddressLookup` both
  targeting `dns.iroh.link`; `.address_lookup()` appends, so those ran alongside
  the BLE lookup. Offline they churn (constant failed DNS/pkarr). Switched to the
  `Minimal` preset + explicit `RelayMode::Disabled` so BLE is the only address
  lookup — an offline BLE-mesh homeserver must never touch `dns.iroh.link`.
  Verified on the non-ble path (`cargo test -p neutrino-ffi relay_transport`).
- **Mechanism honestly unconfirmed.** iroh's discovery is fully async
  (`TokioResolver`/`TokioRuntimeProvider`; no `block_on`/`block_in_place` in iroh
  source) and the failing log shows the executor *alive* (pkarr retries,
  createRoom returns) — so it is **not** a hard thread-block. Leading theory:
  offline netcheck/discovery churn *starves* the single-threaded (`current_thread`)
  FFI runtime, so the long-poll's 30s timer isn't serviced. The long-poll loop
  itself is sound (`remaining` shrinks monotonically → can't exceed 30s unless the
  task is never polled). Disabling discovery removes the most likely churn source
  but is not proven to be the sole cause.
- **Executor-stall watchdog added** (`neutrino-ffi/src/watchdog.rs`) precisely
  because the fix isn't provable and the hang is intermittent: an in-runtime task
  bumps a monotonic heartbeat; an off-runtime OS thread (holds a `Weak`, exits with
  the runtime) logs a loud `WARN` when the heartbeat lags >3s. Turns a future
  recurrence into a timestamped, greppable event and distinguishes "executor
  stalled" from "client stopped syncing". Pure `evaluate()` decision unit-tested.
  Deeper root-cause tier (deferred, not implemented): `tokio_unstable` +
  `Handle::dump()` on stall for per-task backtraces, gated behind a debug feature.
- **Runtime stays `current_thread`** — defensible for an embedded single-user
  server (footprint, `!Send` ergonomics, trivial load). Multi-thread would only
  *mask* an executor-blocking bug, not fix it; revisit only as post-root-cause
  hardening.

### BLE invite failures: sync wake on OOB invite + vendored iroh-ble-transport (2026-06-30)
- Two independent bugs surfaced when `/invite` failed over BLE (sender pixel.log,
  receiver recv.log).
- **Bug B (in-tree, fixed):** an inbound OOB federated invite is stored via
  `InviteStore::put_invite` (the `oob_invites` table), which — unlike
  `persist_event` — never bumped the stream watch, so an in-flight sliding-sync
  long-poll only surfaced the invite at its next poll (~10-30s, the observed
  delay). Fix: `put_invite`/`remove_invite` now call
  `SqliteStore::notify_watch_changed`, which `send_modify`s the watch to fire a
  `changed()` edge **without** advancing the `StreamPos` cursor — preserving both
  `build_response`'s head read and `notify_watch`'s monotonic guard for real
  events. Regression test `long_poll_wakes_on_oob_invite`.
- **Bug A (vendored dep, fix UNVERIFIED in sandbox):** after a peer restarts under
  a new BLE MAC, the sender deadlocks dialing the stale address. Root cause in
  `iroh-ble-transport`: the send path sets `entry.pipe = None` when its outbound
  channel closes on a dead link but leaves the entry `Connected`; the wedged-pipe
  watchdog skipped pipeless entries, so the entry kept pinning its prefix
  (`active_prefix_bindings`) and `note_discovery` rejected the new MAC as
  `ActivelyPinned` forever — no fresh GATT connect ever fired. **Vendored the dep
  in-house** at `vendor/iroh-ble-transport` (path dep from neutrino-ffi; Kegan
  expects further changes). Fix: the watchdog now also wedges a `Connected` entry
  with `pipe == None` once it has been pipeless past `CONNECTED_IDLE_DEADLINE`
  (gated on time-in-`Connected` to preserve the legitimate connect/L2CAP-upgrade
  install window). Tests added, but the crate pulls `blew`→`bluer`→`libdbus-sys`
  (no `dbus-1` / uncached here) so it **cannot be compiled or tested in the
  sandbox — needs an on-device / Linux-with-dbus build to verify.** Recovery is
  bounded by the 45s deadline; faster dead-link detection at send time is a
  possible follow-up (riskier — `pipe: None` is also a tolerated transient).

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

### outbound federation backfill (2026-06-26)
- Wired the *outbound* side of `GET /_matrix/federation/v1/backfill/{roomId}` to
  client back-pagination, so a freshly-joined server (state via `send_join`, no
  history) can serve timeline history its client back-paginates into. Mirrors
  Synapse's model on neutrino's single `stream_pos` axis. Design:
  `docs/superpowers/specs/2026-06-26-outbound-federation-backfill-design.md`.
- **Trigger:** a client backward `/messages?dir=b` page that underflows the
  requested `limit` runs **one** best-effort backfill round synchronously, then
  re-reads and returns. The client paginating again drives the next round
  (naturally rate-limited; never loops within a single request).
- **Seeds:** backward extremities (dangling `prev` edges whose parent isn't held)
  are computed **on the fly** per pagination — no `event_backward_extremities`
  table, consistent with the "derive, don't store redundant state" stance. Capped
  (default 5, Synapse parity) to bound the `v` query URI.
- **Destinations:** joined servers from current state, minus self, tried
  sequentially with failover (no joined peer → no-op).
- **Ordering:** backfilled (older) events are allocated **descending `stream_pos`
  below the current minimum** (`COALESCE(MIN(stream_pos),1)-1`, decremented per
  event in a batch) via an **explicit** value into the `INTEGER PRIMARY KEY
  AUTOINCREMENT` column (AUTOINCREMENT only governs auto-assigned values; explicit
  inserts below the minimum are legal). `/messages?dir=b` already orders
  `stream_pos DESC`, so it walks straight into them with no query change, and they
  sit below any positive sliding-sync cursor. `persist_historical_event` **no
  longer advances the forward `subscribe()` watch** and does not touch
  `current_state`. `PaginationToken(pub u64)` → `PaginationToken(pub i64)` (and
  `messages.rs` `parse_token`) to address the negative region; `StreamPos(pub u64)`
  is **unchanged** (forward-only watch/sliding-sync cursor).
- **Auth:** well-formedness only on the existing historical write path (no
  state-res re-auth), per the trusted-network / no-signatures posture; PDUs whose
  `room_id` ≠ the requested room are rejected and already-held events deduped.
- **Documented downsides (accepted):** `stream_pos` is no longer `≥ 0` /
  monotonic-from-zero — it grows a signed backfilled region; backfilled events are
  visible only via `/messages`, never through sliding sync; the temporal
  state-DAG index is **not** maintained for backfilled state events (tolerable
  only because they aren't re-authed); on-the-fly extremity recompute re-scans per
  pagination (no indexed table); no failed-pull backoff (a dead seed is retried
  every back-page); positions drift monotonically toward `i64::MIN` (practically
  unbounded, never resets).
