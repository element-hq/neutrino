# Architecture

How the crates relate. What each crate contains is in its code; this file covers what no single crate shows: why the split, who owns what, what flows between them, and the invariants one crate relies on another to keep.

## Why this split

Crates are scoped narrowest first, so each layer can be reasoned about without the ones above it:

| Crate | Owns |
|---|---|
| `neutrino-event` | One event: wire parsing, ids, redaction, signing, room-version registry. |
| `neutrino-room` | One room, pure and I/O-free: auth rules, state resolution, `RoomCore::apply_pdu`. |
| `neutrino-store` | Storage traits. `neutrino-store-sqlite` is the only implementation. |
| `neutrino-engine` | Many rooms: per-room actors, inbound worker, gap-fill, outbound sender, anti-entropy. Generic over storage, and over federation through the `ports` traits. |
| `neutrino-http` | axum glue: C-S and S-S handlers on one router, plus the transport implementations of the engine ports. |
| `neutrino-lb` | Matrix-agnostic transport sidecar: JSON↔CBOR, CoAP / Q-Block, the `DatagramLink` seam for out-of-tree media. |
| `neutrino-ctl` | `Config` and `Command`: the vocabulary every layer and the host share. |
| `neutrino-main` | Composition root: identity, trust domain, room-version registry, lb wiring. |
| `neutrino-ffi` / `neutrino` | Entrypoints: UniFFI for Android, the dev binary. |
| `neutrino-testkit` | Child-process federation rig; drives the `neutrino` binary as a black box. |

Some boundaries exist for a reason the dependency list doesn't show:

- **`neutrino-engine`** names no concrete store or transport. That keeps tokio actors and federation logic testable against an in-memory SQLite (a dev-dependency) and free of axum/reqwest.
- **`neutrino-lb`** depends on no neutrino crate, so a medium can be built out of tree against `DatagramLink` alone.
- **`neutrino-ctl`** depends on nothing, so the FFI and every layer can read config without pulling in the stack. As a consequence, lb-typed controls such as `CaptureControl` travel beside `Config`, not inside it.
- **`neutrino-testkit`** depends on nothing: its contract with the server is env-var names, SIGUSR2 and HTTP. Renaming an env var in `Config::from_env` makes the rig silently fall back to defaults.
- **`neutrino-ffi`** depends only on `neutrino-main` (whose re-exports carry everything it needs). It mirrors `Config`/`Command` to keep UniFFI out of the shared crates. Out-of-tree media compose through `neutrino_ffi::start_with`.

Allowed internal dependencies (normal deps):

```
ctl, event, lb, testkit   → (none)
room                      → event
store                     → event, room
store-sqlite              → event, room, store
engine                    → event, room, store
http                      → ctl, event, room, store, engine, store-sqlite   (store-sqlite: see the TODO on App.store)
main                      → ctl, event, http, lb, store, store-sqlite
ffi, neutrino             → main
```

## Data flow

- **Inbound PDU:** peer → lb ingress (codec decode, origin check, `/_matrix/federation/` gate, CBOR→JSON) → loopback → http `/send` (`X-Matrix` origin, parse) → `staged_events` → engine worker (re-admit, order, gap-fill) → room actor → `RoomCore::apply_pdu` → `Vec<Effect>` → `persist_resolved_event`.
- **Local write:** C-S handler → `RoomRegistry::send_event` → `EventBuilder` → the same actor and persist path → outbox.
- **Outbound:** engine sender → `FederationTransport` (http `FederationClient`: `X-Matrix`, signature in signed mode) → lb egress on loopback → `NodeIdResolver` → CoAP over a `DatagramLink` or UDP.
- **Config and commands:** host `NeutrinoConfig` or env → `Config` → `neutrino_main::entrypoint`, which fills in `server_name`, overrides `federation_proxy` and builds `LbConfig`/`EventPolicy` → `neutrino_http::serve`. Commands go host → unbounded channel → http dispatch → shutdown token or backoff kick.
- **Client reads:** store → `Event` → `event_view` conversions. Wake-ups come over the store's `watch` channels.

## Cross-crate invariants

Each states the rule, who keeps it, who relies on it, and what breaks without it.

### Identity

- **Event ids are derived, never received.** `EventBuilder::build` and `from_wire` (event) derive every id from the bytes under the room's version; no path segment, body field or response id is trusted. Federation round trips (invite, send_join) accept a returned event only if its derived id equals ours. The store dedupes by id and never re-checks id against bytes. If any ingress keyed storage by a claimed id, one event could silently replace another.
- **The room version is resolved before any bytes are parsed.** It comes from `rooms.room_version` for a known room, or the create event's `content.room_version` for a new one (`engine::util::room_version_for_wire`). The `RoomVersions` registry built in main is the only authority; the store records the string verbatim. A build that stops declaring a medium's version makes that version's rooms unreadable.
- **The redaction keep-list retains `prev_state_events`.** This is an MSC4242 divergence from ruma, applied in `event_id::redact_for_hash`. A PDU with a content-hash mismatch is staged redacted, and the worker re-derives its id; without the key, the second derivation gives a different id and the state-DAG link disappears.
- **A store's trust domain is fixed on first boot.** `neutrino-main` refuses a `trusted_network` flip on an existing store. Trusted-mode events carry no `hashes`, so the two modes name the same content differently.
- **Server identity comes from the store.** The node secret is persisted there. An empty `server_name` becomes hex(ed25519 pubkey), pinned on first boot. On node-id media, `server_name` = verify key (`NodeIdKeyResolver`) = link address (lb compares bytes, so it must be lowercase hex).

### Event processing

- **Only `RoomCore::apply_pdu` decides `auth_events`, `rejected` and `soft_failed`.** The store writes them verbatim and re-validates nothing. Every crate treats a row in `events` as integrated: idempotency, the `prev_state_events` check, `state_after` and gap-fill grounding all depend on that. ⚠ Exception: `http::federation::backfill_out` persists backfilled events through `persist_historical_event` without `apply_pdu`.
- **Each room has one writer.** Every apply and persist goes through that room's actor (`RoomRegistry`), which adopts the new `RoomCore` only after the persist commits. createRoom and join ingest write directly, but only before the room exists.
- **A `rooms` row exists before any of the room's PDUs are staged.** The worker drops staged rows for unknown rooms.
- **Parents are persisted before children.** `prev_state_events` must be persisted before apply. If they aren't, apply returns a retryable error and the engine gap-fills into staging, never straight into `events`. `stream_pos` is therefore a topological order of the state DAG, which `state_after` depends on.
- **Staging is untrusted.** `/send` parses on faith (`admit_on_faith`). The worker re-admits every staged row through `EventPolicy::admit_wire`, which checks signatures in signed mode, before apply. Nothing else reads `staged_events`.
- **`RoomCore` reports a reject; the engine chooses the policy.** `RoomCore` returns `Ok` with `rejected = true`. A local write is answered 403 and not persisted. A federated PDU is persisted as rejected so that its descendants cascade.
- **Soft-fail is always computed.** `Config.enable_soft_failure` only controls whether clients see soft-failed events; DAG and relay behaviour don't change.
- **C-S reads never return rejected events**, nor soft-failed ones when they're hidden. Federation reads serve both, because peers need the ancestry. C-S handlers must use the store's client read methods.
- **`persist_resolved_event` is one SQLite transaction.** It writes the event, the state delta, forward extremities, the room_state index, out-of-band pruning, the outbox and anti-entropy obligations, and notifies the watch only after commit. The outbox is the never-lose ledger; sync subscribes before it queries.
- **"Server is in the room" has one definition**, `engine::reconcile::server_in_room`. The store's out-of-band membership write re-checks the same fact atomically. If the two drift, a room PDU gets diverted into the out-of-band table, acknowledged and never applied.

### Federated membership

- **A remote `make_*` template is never echoed.** Only its `prev_events`/`prev_state_events` are read; the event is rebuilt through `EventBuilder` (`complete_membership_template`). Reusing template fields would let a resident author events as our user.
- **A `send_join` response must be a closed state DAG**, and its `event` must be our join by derived id. Co-signing touches only `signatures`, so ids survive the round trip.

### Transport and trust

- **`X-Matrix` origin is an identity only because the transport binds it.**
  - On an injected link, lb checks that the claimed origin equals the link source.
  - http trusts `origin` and never verifies a request `key`/`sig`, even in signed mode.
  - The UDP and HTTP wires, and direct mode, bind nothing.
  - Event signatures, checked in signed mode, are the only cryptographic identity.
  - ⚠ lb reads the first `origin=` parameter (`headers::xmatrix_origin`), http the last (`auth::parse_origin`).
- **In trusted mode authorship rests on an honest mesh.** No crate checks that an event's sender belongs to the delivering origin, except on send_join, send_leave and the out-of-band paths.
- **The C-S API is off the network only in lb mode.**
  - lb ingress forwards only `/_matrix/federation/`, judged after URL normalisation.
  - `neutrino-main` refuses a concrete non-loopback `bind_addr`.
  - In direct mode nothing guards it.
  - ⚠ An unspecified `bind_addr` (`0.0.0.0`, `::`) is accepted, and the listener then binds every interface.
- **The lb egress is loopback-only by construction**: main allocates it. That is why it has no body cap.
- **In lb mode main replaces `Config.federation_proxy` with the egress.** A configured proxy must parse or startup fails (`AppState::validate_config`), never falling back to direct, which would send JSON to a CBOR peer.
- **Proxied URLs carry a `~` host suffix.** The federation client adds it and lb egress strips it; the two copies of this rule must change together.
- **Only the engine sender retries.**
  - A 4xx drops the batch permanently; anything else backs off.
  - lb must preserve the status class, so its own failures map to 502.
  - A peer's 2xx on `/send` means "durably staged", and our handler only 4xxs a whole envelope. The per-PDU error map is ignored.
  - In gap-fill only 400 and 404 count as unfillable; anything else, including 403, retries.

### Lifecycle

- **`neutrino-ffi` owns the tokio runtime**, a `current_thread` one. A blocking call on the executor stalls every task; the watchdog reports stalls.
- **The host closing its command channel becomes `Command::Shutdown`.** A medium's `KickBackoff` sender keeps the internal channel open, so closure alone never stops the server.
- **Tracing and the panic hook are installed before anything that can fail**, because Android discards stderr. Without `RUST_LOG`, only `neutrino*` targets (plus the named transport crates) are logged.
