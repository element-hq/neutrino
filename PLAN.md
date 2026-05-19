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


## status
- [x] project scaffold (Cargo.toml, main.rs, module structure)
- [x] Write a .github/workflows/ci.yml to run cargo tests, complement, clippy, formatting, compile.sh script for uniffi, and to fail if any fail. Run linting in parallel.
- [x] StorageBackend trait defined
    - multiple developers will be relying on this not changing (one to implement the interface, the other using it). If it later turns out that the trait needs to change, prompt and add decision log immediately.
- [ ] SQLite storage backend implementation
    - cannibalise existing neutrino-sqlite crate, then prompt user to clean crate up
- [ ] Room state machine (v12 auth + state-res v2.1, MSC4242)
    - [x] Phase 0: types (`neutrino-state` crate: `RoomVersion`, `Event`, `StateMap`, `FormatError`, `AuthError`, `CoreError`)
    - [x] Phase 1: format validation (`validate::parse_event`, 40 tests)
    - [ ] Phase 2: auth events selection
    - [ ] Phase 3: auth rules v12
    - [ ] Phase 4a: separate + auth_chain_difference
    - [ ] Phase 4b: reverse-topo power sort + IAC pass 1
    - [ ] Phase 4c: mainline + IAC pass 2 + unconflicted merge → `resolve_state`
    - [ ] Phase 5: in-memory `StateProvider`
    - [ ] Phase 6: `RoomCore::apply`
- [ ] Client-Server Sliding Sync MSC4186 implementation
    - Does not need to be performant as it’s all local to the device.
- [ ] Client-Server write endpoints (PUT/POST) implementation
    - Must be persisted via the storage backend.
- [ ] Server-Server /send implementation
    - MUST handle retrying on restart, MUST NOT lose events. Events MUST eventually be sent over federation.
- [ ] Server-Server invite/join/leave implementation
- [ ] Server-Server backfill/get_missing_events implementation
    - MUST incrementally persist progress when filling in the state DAG

All status points MUST have tests.

## in progress

- [ ] SQLite storage backend implementation

## open questions
- how should low bandwidth CBOR/CoAP integrate with HTTP/JSON? As a separate crate/proxy or baked into the Event / Request / Response types? How does this affect working with Ruma?



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
- Tracing

 ## decisions log

Decided to use Claude.

2026-05-12: StorageBackend split into five sub-traits (RoomStore, EventStore, StateStore, DagStore, FederationOutbox) combined via a StorageBackend supertrait. StoredEvent pre-parses event_id, room_id, event_type, state_key, sender, origin_server_ts alongside raw JSON. StoredPdu extends this with prev_events and prev_state_events (MSC4242). Traits live in neutrino-common::storage. Usage pattern is generic bounds (S: StorageBackend), not dyn StorageBackend. insert_events replaced with persist_event(event, destinations) for atomic event+outbox write. create_room takes all initial events atomically. EventStore::subscribe() returns a watch::Receiver<StreamPos> for push-based sync and federation wakeup (subscribe before querying to avoid TOCTOU). StateStore gains joined_rooms(user_id) and joined_members(room_id) with membership=join filtering.

2026-05-13: StorageBackend extended to six sub-traits. EventStore gains get_client_txn/record_client_txn for CSAPI txnId deduplication across restarts. New FederationInbox sub-trait with record_federation_txn(origin, txn_id) for inbound federation txnId deduplication (returns true if already seen). DagStore::missing_events drops min_depth parameter — depth tracking not implemented. Backfill cursor is implicit: derive frontier from events whose prev_events reference IDs absent from the store; no explicit cursor storage needed. Room IDs must be derived from the reference hash of the create event (room version 12), not randomly generated. Outbox startup wiring (enumerate pending_destinations on boot, spawn sender per destination) goes in neutrino-main.

2026-05-19: Room state machine lives in a new `neutrino-state` crate (deps: ruma, serde, serde_json, thiserror — no async, no tokio). `StateProvider` will be a sync trait. Provider returns **state-before-event** (the new event sits on top of state-before; state-after doesn't expose that hinge). `prev_state_events` (MSC4242) is read directly off the event, not via the provider. State groups deferred. Soft-fail does not gate current-state updates (matches Synapse: `_check_for_soft_fail` only sets the flag; state-res runs over the DAG unaware of it; the flag gates client relay and `_get_prevs_before_rejected` only). Phase order: 0 types → 1 format → 2 auth_events selection → 3 auth rules → 4a separate+auth_chain_diff → 4b reverse-topo+IAC1 → 4c mainline+IAC2+merge → 5 in-memory provider → 6 RoomCore::apply. IAC pass 1 starts from **empty** state per v12 (not unconflicted).

2026-05-19: MSC4242 removes `auth_events` from the wire (events carry `prev_state_events` instead). Servers calculate `auth_events` server-side from state-before via the selection algorithm. State-res algorithm itself is unchanged from synapse `state/v2.py`; only the input source changes (calculated, not on-wire). `Event` struct does not carry `auth_events`. Phase 1 rejects any incoming event that includes `auth_events` on the wire (cross-ref synapse `events/__init__.py` `assert "auth_events" not in event_dict`). `signatures` field is ignored entirely (trusted-network policy). `hashes` field is required and shape-validated even though we don't verify the values (content hashes are independent of signatures/keys). v12 rule 5.8 (membership ∈ {join,leave,invite,ban,knock}) lives in Phase 3's switch/case, not as an upfront assertion in Phase 1, to avoid sync drift between the assertion and the per-arm handlers. Room ID for `m.room.create` events is derived inline from the caller-provided `event_id` via `$ → !` sigil swap; full reference-hash event_id computation is out of Phase 1 scope.
