# `Event` ↔ ruma client-view conversions

Status: design (2026-05-27). Drives the fix for the failing complement test
`TestRoomCreate/Parallel/Can_/sync_newly_created_room`. See PLAN.md decisions
log once landed for any deviations from this sketch.

## Goal

Add conventional Rust `From` / `TryFrom` impls converting
`neutrino_common::Event` to ruma's client-facing `Raw<T>` shapes, so that the
CSAPI delivers events with the server-computed fields (`event_id`, and
`room_id` on create events) that v12 / MSC4242 wire bytes deliberately omit.

## Root cause being fixed

`Event.raw` is the canonical v12 / MSC4242 wire bytes — by design those bytes
don't carry `event_id` (computed from the reference hash, never on the wire)
and for `m.room.create` they don't carry `room_id` either (derived from the
event_id via sigil swap). `event_id`, `room_id`, and `auth_events` live on
the `Event` struct as server-computed sidecar fields. See
`event-id-design.md` §"Co-location pattern".

`sliding_sync/build.rs` currently constructs the `/sync` timeline and
`required_state` arrays by passing `e.raw.clone()` straight into
`Raw::<…>::from_json`, with no enrichment. The legacy `/v3/sync` wrapper
(`legacy_sync/translate.rs`) forwards these verbatim into
`rooms.join.<id>.timeline.events`.

So the CSAPI `/sync` response delivers events with no `event_id`. Meanwhile
`PUT /_matrix/client/v3/rooms/{}/send/{type}/{txn}` returns
`{event_id: "$abc…"}` (the computed reference hash). Complement's
`SendEventSynced` calls
`MustSyncUntil(SyncTimelineHas(roomID, ev => ev.event_id == eventID))` —
which can never match.

## Module location

New module: `crates/neutrino-common/src/event_view.rs`, declared from
`lib.rs`. No new dependencies — `neutrino-common` already pulls in `ruma`,
`serde_json`, and `thiserror` (since the 2026-05-22 PR-1 type unification).

## The five conversions

Infallible — timeline targets accept both message and state events:

- `impl From<&Event> for Raw<AnySyncTimelineEvent>`
  — `/sync` `timeline.events` (v3 and v5)
- `impl From<&Event> for Raw<AnyTimelineEvent>`
  — `/_matrix/client/v3/rooms/{}/event/{eventId}` full-event view
  (anticipates the endpoint listed in PLAN.md but not yet built)

Fallible — state-shaped targets require `state_key`:

- `impl TryFrom<&Event> for Raw<AnySyncStateEvent>`
  — `/sync` `state.events` / v5 `required_state`
- `impl TryFrom<&Event> for Raw<AnyStrippedStateEvent>`
  — `invite_state` / `knock_state` (MSC1772 stripped form)

All four are orphan-rule-legal because `Event` (local to `neutrino-common`)
appears as a type parameter to the trait — the local-type anchor the orphan
rule requires.

Free function for the Raw→Raw strip — orphan rule forbids it as a trait
impl (both `Raw<AnySyncStateEvent>` and `Raw<AnyStrippedStateEvent>` are
foreign types):

```rust
pub fn strip_state_event(
    raw: &Raw<AnySyncStateEvent>,
) -> Result<Raw<AnyStrippedStateEvent>, StateEventConversionError>;
```

## Error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum StateEventConversionError {
    #[error("event of type {event_type} has no state_key — cannot represent as a state event")]
    NotAStateEvent { event_type: String },
    #[error("Raw<AnySyncStateEvent> JSON is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("Raw<AnySyncStateEvent> is missing required field {0}")]
    MissingField(&'static str),
}
```

The two `&Event` TryFrom impls only return `NotAStateEvent`; the Raw→Raw
`strip_state_event` can return any of the three (the input is type-opaque
JSON behind `Raw<T>`).

## Shared helper

```rust
enum IncludeRoomId { OnlyOnCreate, Always }

fn enrich_for_client(ev: &Event, policy: IncludeRoomId) -> Box<RawValue> {
    let mut obj: Map<String, Value> = serde_json::from_str(ev.raw.get())
        .expect("Event.raw is a JSON object by parse_event invariant");
    obj.insert("event_id".into(), Value::String(ev.event_id.to_string()));
    let is_create = ev.event_type == "m.room.create";
    match (policy, is_create) {
        (IncludeRoomId::Always, _) | (IncludeRoomId::OnlyOnCreate, true) => {
            obj.insert("room_id".into(), Value::String(ev.room_id.to_string()));
        }
        (IncludeRoomId::OnlyOnCreate, false) => {
            obj.remove("room_id");
        }
    }
    to_raw_value(&Value::Object(obj)).expect("JSON object → RawValue is infallible")
}
```

`OnlyOnCreate` matches Synapse's `/sync` shape: `room_id` is redundant on
non-create events because the response is already per-room indexed under
`rooms.join.<id>.…`. `Always` is for the full-event endpoints where the
event is delivered standalone.

The `.expect`s are justified by `Event` invariants — `parse_event` already
validated that `raw` is a JSON object and re-serialising a `serde_json::Map`
back to `RawValue` is type-system-guaranteed infallible.

## Call-site migrations

| File:line | Before | After |
|---|---|---|
| `sliding_sync/build.rs:585-588` | `Raw::from_json(e.raw.clone())` | `e.into()` |
| `sliding_sync/build.rs:610-613` | `Raw::from_json(e.raw.clone())` | `e.try_into().expect("current_state events have state_keys by HashMap key invariant")` |
| `sliding_sync/build.rs:812-828` (`strip_state_event(&Event)`) | bespoke fn | delete; callers use `TryFrom<&Event>` |
| `legacy_sync/translate.rs:303-315` (`strip_state_event(&Raw<…>)`) | bespoke fn | delete; callers use `event_view::strip_state_event` |

Sliding-sync's `required_state` events come from `current_room_state`, a
`HashMap<(String, String), Event>` keyed by `(type, state_key)`, so
`state_key` is structurally guaranteed at the call site. The `.expect`
message documents *why* the conversion is safe there. The alternative —
`filter_map(|e| e.try_into().ok())` — silently drops bugs, so we don't use
it.

Federation paths (`StorageBackend::persist_event`, the outbox sender) are
untouched. They continue to ship `Event.raw` verbatim, which is correct
because federation peers verify the reference hash against the wire bytes.

## Tests

Unit tests in `neutrino-common::event_view::tests`:

- `from_event_for_sync_timeline_injects_event_id`
- `from_event_for_sync_timeline_create_event_carries_room_id`
- `from_event_for_sync_timeline_non_create_strips_room_id`
- `try_from_event_for_sync_state_err_when_no_state_key`
- `try_from_event_for_sync_state_ok_when_state_key_present`
- `try_from_event_for_stripped_state_keeps_only_four_canonical_fields`
- `try_from_event_for_stripped_state_err_when_no_state_key`
- `from_event_for_room_event_carries_both_ids`
- `strip_state_event_drops_non_canonical_fields`
- `strip_state_event_err_when_state_key_missing`

E2e regressions — landing these closes the gap that let the original bug
ship:

- `tests/e2e_legacy_sync.rs::send_event_then_legacy_sync_returns_event_with_event_id`
- `tests/e2e_legacy_sync.rs::legacy_sync_create_event_carries_room_id_and_event_id`
- `tests/e2e_sliding_sync.rs::put_event_then_sliding_sync_returns_event_with_event_id`

## Order of operations

Small, atomic commits:

1. New `event_view` module + the five conversions + unit tests.
   `cargo test -p neutrino-common`.
2. Migrate `sliding_sync/build.rs` timeline + state-events sites; delete
   the local `strip_state_event(&Event)` helper.
   `cargo test -p neutrino-http`.
3. Migrate `legacy_sync/translate.rs`; delete the local
   `strip_state_event(&Raw<…>)` helper. `cargo test -p neutrino-http`.
4. Land the three e2e regressions. They pass against the migrated code.
5. `cargo fmt && cargo clippy --workspace --all-targets -- -D warnings &&
   cargo test --workspace`. Update PLAN.md status + decisions log; append
   LOG.md.

## Non-goals (explicitly out of scope)

- Refactoring how sliding-sync produces `invite_state` / `knock_state`.
  Knock currently goes via `required_state` + manual strip; a native
  knock-state path is a separate change.
- `unsigned.age` injection. Not currently used by any complement test we
  care about.
- Caching the client-view JSON alongside `Event.raw` to skip the re-parse.
  Embedded single-user server; perf is irrelevant.
