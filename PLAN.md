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

Not yet started — the `neutrino-lb` crate does not exist. It must translate
server-to-server HTTP + JSON into CoAP + CBOR (MSC3079
https://github.com/matrix-org/matrix-spec-proposals/blob/kegan/low-bandwidth/proposals/3079-low-bandwidth-csapi.md).

Open question: should CBOR/CoAP live in a separate crate/proxy or be baked into the
`Event` / `Request` / `Response` types, and how does that interact with Ruma?

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

### Code-quality follow-ons (noted, not blocking)

- thread `own_server` as a parsed `OwnedServerName` rather than a `String` compared by value
- relocate `outbound_destinations` from the HTTP actor into `neutrino-state`
- fold the repeated `FederationClient` PUT-event idiom into one helper
- federated-invite error-code parity with the local invite path
- OOB-invite join review leftovers (noted, not blocking): `federated_join_if_remote` runs twice on the `join_by_id_or_alias`→`join` fall-through; a single private `join_core(hints)` would collapse it. `room_exists` is a coarse "do we host it" gate (a timed-out federated join leaves the room registered mid-ingest, so a retry takes the local path). No single shared "is this room remote / who is the inviter's resident server" predicate (invite/leave/join each re-derive it). Stale OOB stub not removed on a successful federated accept (harmless — both sync builders supersede it).
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
