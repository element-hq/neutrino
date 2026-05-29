# Federation `/get_missing_events` — design

Status: proposed 2026-05-27. Branch TBD. Not yet implemented.

## Goal

Land `POST /_matrix/federation/v1/get_missing_events/{roomId}` — the first
server-side federation endpoint on Neutrino. Inbound only: we respond when
another mesh peer asks us to fill a DAG gap between events they have
(`earliest_events`) and events they don't (`latest_events`). Pure read over
`DagStore::missing_events`; no state-res, no signature verification, no
history-visibility filter.

## Trust model & spec deviations

Neutrino targets a **trusted mesh of up to ~12 homeservers**, communicated
via a low-bandwidth transport (CBOR/CoAP per MSC3079). Every byte costs.
The federation surface is therefore a deliberate, documented subset of
the spec:

1. **No `Authorization: X-Matrix` header.** We do not read it, parse it,
   or verify it. Peers in the mesh are presumed trusted; the bytes
   X-Matrix would cost are not spent. This is a deviation from the
   server-server spec §"Authentication" — accepted.
2. **No event signatures.** Events on the wire and in storage carry no
   `signatures` field (per `event-id-design.md`). We don't sign outbound
   PDUs and don't verify inbound ones.
3. **No `min_depth` filter.** Neutrino has no `depth` column — Synapse's
   backfill ordering responsibility is taken over by `origin_server_ts`
   (per the 2026-05-22 decision in PLAN.md). The `min_depth` field of
   the request is parsed (so malformed JSON still 400s) and then
   ignored. Documented FIXME on the handler.
4. **No `m.room.history_visibility` filter on the response.** Spec
   requires servers to redact / drop events the requesting peer
   wasn't joined for at the time. Under the trusted-mesh model, peers
   are presumed not to abuse history reads, so the filter is accepted
   as missing. *This is a real spec gap, not a theoretical one — peer A
   in the mesh can request history from a time peer A was not joined.*
   Adding it later requires a new `StateStore::state_at_event` method
   plus the `filter_events_for_server` adaptation; out of scope until
   Phase 6 `RoomCore::apply` lands a state-at-event provider.

## Why this endpoint first

Per the dependency analysis in chat 2026-05-27: of the five federation
read endpoints we could ship without state-res, `/get_missing_events` is
the only one with any standalone test coverage in either Complement or
Synapse. The handler itself is a thin wrapper over the existing
`DagStore::missing_events` storage primitive, so the bulk of the work
is test-side, not implementation-side. Good first endpoint to scaffold
the `federation/` module.

## Module layout

New `federation/` submodule of `neutrino-http`, mirroring the existing
`sliding_sync/` and `legacy_sync/` layout:

- `crates/neutrino-http/src/federation/mod.rs` — route registration,
  shared `FedError` enum, shared response/error mapping helpers.
- `crates/neutrino-http/src/federation/get_missing_events.rs` — the
  handler.
- `crates/neutrino-http/src/lib.rs::router` adds the route:

  ```rust
  .route(
      "/_matrix/federation/v1/get_missing_events/{room_id}",
      post(federation::get_missing_events::handle),
  )
  ```

No origin extractor. No middleware. When the second federation endpoint
lands (likely `/send`, which needs an origin string for
`FederationInbox::record_federation_txn`), the question of "where does
origin come from in the absence of X-Matrix" is settled then — see
**Open questions** below.

## Handler

Signature:

```rust
async fn handle(
    State(state): State<AppState>,
    Path(room_id): Path<OwnedRoomId>,
    Json(req): Json<v1::Request>,
) -> Result<Json<v1::Response>, FedError>
```

Where `v1` is `ruma::api::federation::event::get_missing_events::v1`.
This requires adding the `federation-api-s` feature to the `ruma` dep
of `neutrino-http` (the `*-s` flavour is the server-side request /
response types, matching the existing `client-api-s` usage).

### Algorithm

1. Reject 400 `M_INVALID_PARAM` if `req.latest_events` is empty.
   (Synapse-equivalent: `test_bad_request`.)
2. 404 `M_NOT_FOUND` if the room is unknown
   (`RoomStore::get_room_version(&room_id)?` returns `None`).
3. Clamp `req.limit`: default 10, max 20. Spec §`/get_missing_events`:
   *"This endpoint MUST return no more than 20 events in a single
   response."* — saturating cap, not a 400.
4. Drop `req.min_depth` on the floor. Documented.
5. Call `DagStore::missing_events(&room_id, &latest, &earliest, limit)`.
   The trait contract (Synapse-style, post the 2026-05-28 relaxation):
   BFS over `prev_events` starting from the *parents* of each
   `latest` event, skipping any event in `earliest ∪ latest`, return
   up to `limit` events in walker order. **The events in `latest`
   themselves are never in the result** — they are the boundary the
   requester already has. The events in `earliest` are likewise
   never returned. This matches Synapse's
   `_get_missing_events` (`synapse/storage/databases/main/event_federation.py`)
   and the strict reading of the spec text *"the missing events"*.
6. Build the response: `events` is `Vec<Box<RawJsonValue>>` populated
   from each `Event.raw` *verbatim*. **No `event_view` enrichment** —
   federation peers receive the canonical v12 / MSC4242 wire bytes the
   reference hash was computed over. Enrichment is a CSAPI-only
   concern.
7. 500 `M_UNKNOWN` on `StorageError`.

### `FedError`

New error enum in `federation/mod.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum FedError {
    #[error("bad request: {0}")]
    BadRequest(&'static str),       // → 400 M_INVALID_PARAM
    #[error("room not found")]
    RoomNotFound,                   // → 404 M_NOT_FOUND
    #[error("storage: {0}")]
    Storage(#[from] StorageError),  // → 500 M_UNKNOWN
}
```

`impl IntoResponse for FedError` maps to `{errcode, error}` JSON bodies
identical in shape to `SyncError`'s mapping in `sliding_sync` —
M_INVALID_PARAM / M_NOT_FOUND / M_UNKNOWN.

## Edge cases pinned

These are gaps in Synapse's own test coverage that the Rust port should
fill (see **Tests** below):

- **Cycle in `prev_events`.** Storage corruption: A → B → A. The walker
  must terminate without revisiting events, regardless of the cycle.
- **Missing parent.** A `latest_events` entry references an event we
  don't have; or the walk reaches a parent ID not in `events`. The
  walker stops cleanly, returns whatever was reachable.
- **Wide fan-out at a single event.** One node with 50+
  `prev_events`. BFS frontier handles correctly under the `limit`.
- **`limit = 1`.** Returns exactly one event.
- **`earliest_events` boundary is exclusive.** Earliest IDs themselves
  must never appear in the result.
- **`earliest_events` references not in this room.** Current
  `DagStore::missing_events` contract is to ignore unknown IDs in
  `earliest`; the walk just proceeds without a stop signal for them.
  Pinned as-is — no error, no warning.
- **`latest_events` references not in this room.** The walker treats
  them as starting points with no incoming edges; result is empty (no
  events reachable). No 404 — the room exists, the requested events
  just aren't in it.

## Tests

Three layers. Synapse ports + Complement gating + new Rust coverage.

### A. Storage / DAG-walk tests

Live in `crates/neutrino-store-sqlite/src/store/dag.rs::tests` (or
alongside existing `missing_events` tests — wherever the trait method
is currently exercised).

**Ports from Synapse** (`tests/storage/test_event_federation.py`) — reality check after reading the source tests:

| Synapse test                          | Status                                                          |
|---------------------------------------|-----------------------------------------------------------------|
| `test_get_backfill_points_in_room`    | DAG shape ported as `missing_events_synapse_backfill_dag_shape` — the 13-event spine + two-branch fan-out shape from Synapse's `_setup_room_for_backfill_tests`. Assertions are fresh because Synapse's test is over `get_backfill_points_in_room`, a different primitive that pins ordering by `depth`. Our walker walks `event_edges` (sorted by parent_event_id, not by ts), so the port asserts cardinality + presence on two walks (full DAG, and earliest-bounded). |
| `test_conflicted_subgraph`            | Not ported. Synapse-specific (`chain_cover_index` tables) and tests state-res chain walking, not `missing_events`. |
| `test_auth_chain_ids`                 | Not ported. Tests `auth_events` traversal, not `prev_events`. |

**New tests for the edge cases in §"Edge cases pinned":**

- `missing_events_terminates_on_cycle`
- `missing_events_terminates_on_missing_parent`
- `missing_events_wide_fanout`
- `missing_events_limit_one`
- `missing_events_earliest_boundary_exclusive`
- `missing_events_unknown_earliest_ignored`
- `missing_events_unknown_latest_returns_empty`

### B. Handler-level e2e tests

Live in `crates/neutrino-http/tests/e2e_federation_get_missing_events.rs`
(new file). Pattern mirrors `tests/e2e_legacy_sync.rs` and
`tests/e2e_sliding_sync.rs`: each test builds a fresh `router(config)`,
seeds events through the live storage backend (`AppState::new` opens a
file-backed `SqliteStore` on `tempfile::NamedTempFile`), and drives the
endpoint via `tower::ServiceExt::oneshot`.

| Test                                                | Asserts                                                              |
|-----------------------------------------------------|----------------------------------------------------------------------|
| `bad_request_empty_latest_events_returns_400`       | Empty `latest_events` array → 400 M_INVALID_PARAM (port of Synapse `test_bad_request`). |
| `bad_request_non_json_body_returns_400`             | Body that isn't JSON → 400.                                          |
| `bad_request_missing_required_field_returns_400`    | Missing `latest_events` field → 400.                                 |
| `unknown_room_returns_404`                          | Room ID never seeded → 404 M_NOT_FOUND.                              |
| `happy_path_returns_events_between_earliest_and_latest` | Seed create + 5 messages; ask for gap between create and last message; receive the 5 in BFS order. |
| `respects_limit`                                    | Seed >20 events, ask for limit=50, receive ≤20.                       |
| `default_limit_is_10`                               | Seed >10 events, omit `limit`, receive 10.                            |
| `empty_earliest_walks_back_to_room_root`            | Empty `earliest_events`, walk includes the create event.              |
| `latest_event_not_in_room_returns_empty`            | `latest_events` references a fabricated ID; response is `{events: []}`. |
| `min_depth_field_ignored`                           | Request with `min_depth: 999_999` returns the same events as without — pin the spec divergence. |
| `wire_bytes_passthrough`                            | Each response event JSON lacks `event_id` (verbatim `Event.raw`, not enriched). |

### C. Complement

Both candidate tests remain blocked:

- `TestInboundCanReturnMissingEvents` — requires `charlie` to federate-
  join the room before the endpoint is reached. Federated join needs
  state-res (Phase 4b/4c) and accept-on-`send_join` (Phase 6). Also
  asserts history-visibility redaction, which we're deferring.
- `TestGetMissingEventsGapFilling` — outbound test (SUT calls
  `/get_missing_events` on the peer). Requires us to receive a
  federation `/send` transaction, detect the gap, and call out —
  state-res-blocked.

Action: append a row to `complement/VIABLE-TESTS.md` documenting
both tests as "blocked on Phase 4b/4c (state-res) + Phase 6". Do not
add to `complement/allowlist.txt`.

## Cargo changes

- `neutrino-http/Cargo.toml`: extend the `ruma` features list with
  `"federation-api-s"`. No new top-level deps.

## LOC estimate

- handler: ~80 LOC
- `federation/mod.rs` (route, `FedError`, `IntoResponse`): ~50 LOC
- router wiring in `lib.rs`: 4 LOC
- storage tests (ports + new): ~350 LOC
- handler e2e tests: ~280 LOC

Total ~770 LOC, of which ~80% is tests. One PR.

## Deferred / out of scope

Captured here so the next implementer doesn't re-derive the question:

- **History-visibility filtering** — gated on a `state_at_event`
  provider on `StateStore`. Acceptable spec gap under trusted-mesh.
- **`min_depth` filter** — Neutrino has no depth.
- **X-Matrix auth header** — deliberate spec deviation, low-bandwidth
  motivated. Never landing.
- **Per-peer origin tracking** — not needed for this endpoint. See
  **Open questions** for when `/send` requires it.
- **Outbound `/get_missing_events`** (we call peers to fill our own
  gaps) — needs state-res to integrate the response. Phase 6 territory.

## Open questions

- **Origin source on `/send`.** When `/send` lands, the storage trait
  expects `record_federation_txn(origin, txn_id)`. With no X-Matrix
  header to read `origin` from, the source has to be: (a) a custom
  header from the low-bandwidth proxy layer (`neutrino-lb` could set
  `X-Neutrino-Origin: <peer-name>` after demuxing the CoAP/CBOR
  envelope), (b) globally unique `txn_id`s so `origin` becomes
  vestigial in the dedup key, or (c) drop `origin` from the trait
  entirely and accept (b). Defer until `/send` is on deck — none of
  these affect `/get_missing_events`.
- **Wire bytes vs. enrichment for federation responses generally.**
  Pinning here: federation responses ship `Event.raw` verbatim. If a
  future endpoint needs to expose `event_id` to peers (e.g., `/event/{eventId}` — though there the path itself carries the
  ID), revisit. For now, federation = raw, CSAPI = enriched.
