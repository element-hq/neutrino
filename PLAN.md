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
- GET /_matrix/key/v2/server (signed deployments only; 404 on a trusted network)

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
  recovery; a one-block request, having no burst for 4.08 to reach, is re-sent by
  the receiver's request probe until its first response block arrives, and the
  peer drops a probe for a request it is still answering); the **`neutrino-main`
  default**. Reuses the `Coap` mapping;
  tuned via `CoapQBlock { block1_size, qblock: QBlockTuning }` (RFC 9177 §6.2).

`WireKind::coap_qblock_for_profile` sizes that tuning from everything the medium
declares, not just its MTU. A medium that *meters* its sends declares
`LinkProfile.pacing: Option<LinkPacing>`, and `QBlockTuning::for_pacing` derives
`max_payloads` / `non_timeout` / `non_receive_timeout` from its service rate. The
load-bearing one is `non_receive_timeout`: coap-rs fires recovery on
time-since-last-activity, so a receive timeout at or below the sender's own
inter-burst pause turns that deliberate pause into phantom loss, and the recovery
traffic then competes for the air time that made the link slow. Unmetered mediums
(LAN, QUIC) declare `None` and keep the RFC defaults.

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
additive CoAP path beside the UDP one, for the embedded/Android target.
Replaces the OS UDP socket with a `DatagramLink` trait (implemented out of tree
over an authenticated QUIC endpoint, keyed by 32-byte node ids — no
socket/IP/ports). A `Hub` runs one drain task that classifies each inbound
datagram by CoAP header code (request→server listener, else→per-node client
inbox). `LinkCoapWireClient` / `LinkCoapWireServer` reuse the shared `exchange`
helper + `CoapDispatch` from the UDP path. Wired into `LbConfig`/`serve` (STEP 2
done): `LbConfig.link: Option<Arc<dyn DatagramLink>>` selects the path purely by
injection — `Some` routes the CoAP wire over the link, `None` keeps UDP.
`neutrino-main::entrypoint` takes a `FederationLinkFactory` (4th param) that
builds the link from a `LinkContext` (resolved node secret + display-name watch
+ discovery registry + command sender); `NodeIdResolver` yields the bare 64-char
hex node id (not a vip).

STEP 3 done, then externalised (2026-07-20, licensing): the concrete
`IrohTransport` (iroh QUIC datagram keyed by 32-byte node id) and the whole
iroh/BLE stack (vendored `blew` + `iroh-ble-transport`, both AGPL-or-later, the
blew Kotlin companions, `NativeBle`, the BLE manifest) moved to `iroh_repo/` —
a self-contained workspace destined for its own repository. It composes via
`neutrino_ffi::start_with(config, factory)` (pub, not uniffi-exported) and
exports `start_ble` from its own crate; its .aar carries both uniffi namespaces
in one `libneutrino.so` (see `iroh_repo/README.md`). In-tree `start()` passes
no factory (plain UDP; LAN mode when it lands). This workspace has zero iroh
deps. The embedded federation data path (BLE build) is unchanged: homeserver →
lb egress (CoAP/CBOR) → `LinkCoapWireClient` → `DatagramLink::send(node,
datagram)` → iroh over BLE.
Done:
- Link facts → encoding policy (`LinkProfile`, 2026-07-21): `DatagramLink`
  gained a defaulted `profile()` returning `LinkProfile { max_datagram }`;
  `build_lb_config` sizes the Q-Block1 block from the MTU
  (`WireKind::coap_qblock_for_mtu`; the default profile reproduces the old
  hardcoded 512 B). The medium's
  *trust* declaration moved the same day to `neutrino-main::LinkTrust`
  riding the link factory's result (see the event-signatures decisions
  entry).
- Integer-key CBOR codec (Layer A; port of Dendrite `internal/lb`): all transports
  now carry the integer-key transcode (`codec::keys`, 143 keys: 137 from Dendrite
  + 6 MSC4242 state-DAG keys) plus event-ID
  →raw-32 B packing with a re-encode/fall-back-to-text guard. CoAP path enums were
  already done (`transport::coap::paths`). (MSC3079.)
- Wireshark pcap tap (`capture`, moved to the HTTP/JSON edges 2026-08-03): the
  Android stand-in for `tcpdump -i lo`. Both proxy hops are real loopback TCP,
  so on a desktop tcpdump is strictly better — but a non-rooted Android app
  cannot capture loopback (no tcpdump, no `CAP_NET_RAW`, VpnService's TUN never
  sees 127/8), and that is where the toggle is used. Taps the four points where
  the literal JSON exists: egress request (post body-read) / response (post
  `cbor_to_json`), ingress request (post `cbor_to_json`) / response (the
  upstream's bytes). Mirrored, because lb is a server on one leg and a client on
  the other. Real bytes, not a CBOR re-transcode — a round trip is not
  byte-identical (`codec::keys`, key order), so re-transcoding would hide codec
  bugs. Emits HTTP/1.1 over synthetic IPv4/TCP (`LINKTYPE_RAW`; us=10.0.0.1 /
  peer=10.0.0.N keyed by `server_name`; server port 80 so Wireshark dissects with
  no "Decode As", the leg told apart by direction; one TCP stream per exchange
  with SYN/SYN-ACK and MSS segmentation, so bodies have no size ceiling — the
  old CoAP/UDP tap silently dropped >64 KiB, losing big `send_join`s). Every
  response path is recorded including the errors. Best-effort background writer;
  never breaks the proxy. Runtime-toggleable (`CaptureControl` on `LbConfig`,
  threaded from ffi alongside `Config` since neutrino-ctl is dependency-free):
  `start_capture(path)` / `stop_capture()` / `is_capturing()`. `stop` joins the
  std-thread writer, so the file is flushed + `adb pull`-ready the moment it
  returns. Transport-independent, so it now works on the UDP/LAN build too.
  Blind by construction to anything below the tap: no CoAP framing, no `LinkCodec`
  compression (deflate included), no Q-Block, no retransmits.

- Link-owned wire codec seam (`LinkCodec`, 2026-07-28): `DatagramLink` gained
  a defaulted `codec()` (the `profile()` pattern); a medium can rewrite the
  assembled `WireRequest`/`WireResponse` — path, headers incl. the X-Matrix
  value, CBOR body — on both wire directions. Hooks: `encode_request` pre
  CoAP-build (transforms ride every block), `decode_request` post-parse and
  before the origin binding, response pair around Block2. Failure mapping:
  egress errors → Transport (outbox retries); `decode_request` → 400
  (malformed, upgrade-together mesh); `encode_response` → 500. A codec's effect
  is invisible to the pcap capture, which sits above it at the HTTP/JSON edges.

Deferred follow-ups (write-ups, not done):
- Wire-size reduction for small MTUs: carry v12 **room** IDs as **raw 32 B**
  (vs `sigil+base64`, −12 B/ID; needs a re-encode-or-fall-back-to-text guard like
  the event-ID path) and
  X-Matrix as a bare/indexed origin (~55 B → ~2–12 B; must ride every block and
  doubles as the reassembly key). send_join's real cost is its Block2 state-DAG
  response re-sending these options per block.
  DONE (2026-07-28) — the static-framing half: the canonical
  `authorization: X-Matrix origin="…",destination="…"` credential now rides
  CoAP as bare `origin,destination` under dedicated option 2052 (`,` is not a
  server-name char), re-expanded to the full header on ingress; non-canonical
  values (key/sig) still travel verbatim as option 2050.
  DONE (2026-07-31) — content hashes: `trusted_network = true` now drops
  `hashes` alongside `signatures` (66 B of JSON / ~54 B of CBOR per PDU). The
  cost: on a trusted network **redactable content no longer reaches the event
  id** (the reference hash is over the redacted form, and `hashes` was what
  bound `content` into it), so two events are distinguished only by
  `sender` / heads / `origin_server_ts` / non-redactable content. Local sends
  chain through the room actor, so a collision needs two same-millisecond
  events from one sender on identical heads. If that ever bites, the fix is a
  monotonic per-room `origin_server_ts` at build time, not the hash.
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
- iroh_repo medium migration: `FederationLinkFactory` now resolves to
  `FederationLink { link, key_resolver: Option<Arc<dyn KeyResolver>> }` — the
  out-of-tree medium must wrap its link (`key_resolver: None` compiles and
  preserves today's behaviour; nominating `Some(Arc::new(NodeIdKeyResolver))`
  costs nothing and lets the app opt into signed mode with zero key
  infrastructure, since node-id server names ARE the verify keys — whether
  signing happens is the app's `trusted_network` config, not the medium's
  call).
- Per-peer / mid-session MTU: `LinkProfile.max_datagram` is read once at
  startup, but BLE MTU is per-peer and changes mid-session (L2CAP upgrade).
  The end state is a per-peer profile query or watch — needs lb-side
  block-size renegotiation (block size is fixed at wire construction today).
  The `iroh_repo` medium should also override `profile()` with its real BLE
  MTU rather than inheriting the 1280 B default.
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
- avatar carry-over onto membership events (displayname is done: the server-wide
  name from the `IdentityStore` is embedded into every local-user member event —
  createRoom join + local invites, `change_membership` join/leave/invite/kick/ban,
  and completed federated join/leave templates)

### Server-Server follow-ons

- orphan-staging garbage collection: a max-age sweep of `staged_events` (federation gap-fill ancestry that never grounds)
- inbound `/get_missing_events` deterministic ordering: the spec wants min-hops + lexicographic, we sort by `origin_server_ts`; the responder also stops at `limit` rather than continuing to the create event (the multi-round requester compensates functionally)
- large state-DAG handling: `send_join` / `send_leave` serialize the whole state DAG into one response and state-res holds it in memory — streaming JSON is a future option if rooms get large
- anti-entropy advertisement coalescing: the joined-set-growth advertisement (MSC anti-entropy-extension) drains all of a destination's owed rooms in one transaction, but the MSC's optional ~30s debounce window — coalescing triggers that arrive moments apart into a single send — is not implemented; each trigger that finds the link quiet advertises promptly. Deferred (MAY).
- the per-PDU `error` map in a `/send` response is ignored, so a PDU the peer 200s but fails to stage (a peer-side storage fault — staging itself is unbounded, there is no cap) is dropped from our outbox and lost. Reading that map and re-enqueueing an advertisement obligation for the affected room would restore the heal; the extremity omission narrowed the accidental cover this used to get.

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
- `Config.log_dir` is set once at startup and the file sink's level is fixed for the
  process. The host's tracing-log-level preference could drive it via a `Command`
  variant, mirroring how the SDK's `updateWriteToFilesConfiguration` is re-applied.
- the file sink is not `cfg`-gated to Android, so the dev binary can opt in with
  `NEUTRINO_LOG_DIR`. Nothing consumes that yet beyond manual debugging.

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
