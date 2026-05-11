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
- [ ] Write a .gitlab-ci.yml to run cargo tests, complement, clippy, formatting, compile.sh script for uniffi, and to fail if any fail. Run linting in parallel.
- [ ] StorageBackend trait defined
    - multiple developers will be relying on this not changing (one to implement the interface, the other using it). If it later turns out that the trait needs to change, prompt and add decision log immediately.
- [ ] SQLite storage backend implementation
    - cannibalise existing neutrino-sqlite crate, then prompt user to clean crate up
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

- [ ] StorageBackend trait defined

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
