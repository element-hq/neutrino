# `Event` ↔ ruma client-view conversions

Status: landed 2026-05-27 (branch `kaylendog/fix/sss-eventid`). The bits flagged
"future endpoint" stay descriptive — the impl exists, callers do not.

## Goal

Conventional Rust `From` / `TryFrom` impls converting `neutrino_common::Event`
to ruma's client-facing `Raw<T>` shapes, so the CSAPI delivers events with the
server-computed fields (`event_id`, and `room_id` on create events) that v12 /
MSC4242 wire bytes deliberately omit.

## Root cause being fixed

`Event.raw` is the canonical v12 / MSC4242 wire bytes — by design those bytes
don't carry `event_id` (computed from the reference hash, never on the wire)
and for `m.room.create` they don't carry `room_id` either (derived from the
event_id via sigil swap). `event_id`, `room_id`, and `auth_events` live on
the `Event` struct as server-computed sidecar fields. See
`event-id-design.md` §"Co-location pattern".

`sliding_sync/build.rs` previously constructed the `/sync` timeline and
`required_state` arrays by passing `e.raw.clone()` straight into
`Raw::<…>::from_json`, with no enrichment. The legacy `/v3/sync` wrapper
(`legacy_sync/translate.rs`) forwarded these verbatim into
`rooms.join.<id>.timeline.events`.

So the CSAPI `/sync` response delivered events with no `event_id`. Meanwhile
`PUT /_matrix/client/v3/rooms/{}/send/{type}/{txn}` returns
`{event_id: "$abc…"}` (the computed reference hash). Complement's
`SendEventSynced` calls
`MustSyncUntil(SyncTimelineHas(roomID, ev => ev.event_id == eventID))` —
which could never match.

## Module location

Module: `crates/neutrino-common/src/event_view.rs`, declared from `lib.rs`.
The `ruma` dep on `neutrino-common` gained the `events` feature so the
`Raw<AnyXxx>` types are reachable directly (was implicitly enabled
transitively via `neutrino-http`'s `client-api-s` feature).

## The five conversions

Infallible — timeline targets accept both message and state events:

- `impl From<&Event> for Raw<AnySyncTimelineEvent>`
  — `/sync` `timeline.events` (v3 and v5)
- `impl From<&Event> for Raw<AnyTimelineEvent>`
  — `/_matrix/client/v3/rooms/{}/event/{eventId}` full-event view
  (forward-declared; the endpoint itself is not yet routed)

Fallible — state-shaped targets require `Event.state_key.is_some()`:

- `impl TryFrom<&Event> for Raw<AnySyncStateEvent>`
  — `/sync` `state.events` / v5 `required_state`
- `impl TryFrom<&Event> for Raw<AnyStrippedStateEvent>`
  — `invite_state` / `knock_state` (MSC1772 stripped form)

All four are orphan-rule-legal because `Event` (local to `neutrino-common`)
appears as a type parameter to the trait — the local-type anchor the orphan
rule requires.

Infallible free function for the Raw→Raw strip — orphan rule forbids it as
a trait impl (both `Raw<…>` types are foreign):

```rust
pub fn strip_state_event(
    raw: &Raw<AnySyncStateEvent>,
) -> Raw<AnyStrippedStateEvent>;
```

Why infallible: `Raw<T>` doesn't validate against `T` at construction
(`from_json` / `cast_unchecked` accept any JSON; validation only happens at
`.deserialize()` time), so we can't lean on the input being a state event.
What we *can* do is "collect whichever of `type` / `state_key` / `sender` /
`content` exist at the root", which is well-defined for any JSON value —
`Value::get(&str)` returns `None` on non-objects, so the loop is a no-op
and the result is `{}`. Structural validity surfaces at the caller's
downstream `.deserialize::<AnyStrippedStateEvent>()`, not here.

## Error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum StateEventConversionError {
    #[error("event of type {event_type} has no state_key — cannot represent as a state event")]
    NotAStateEvent { event_type: String },
}
```

Single variant: the two `TryFrom<&Event>` impls hit it when
`Event.state_key` is `None`. It's `#[from]`-bridged into
`sliding_sync::SyncError::EventConversion`, mapped to HTTP 500 `M_UNKNOWN`
in `lib.rs::sync` and `legacy_sync::handle` — every reachable case is a
storage-layer invariant violation (a row claiming to carry a state event
has `state_key = NULL`), not bad input.

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

## Call sites

| File | What it does |
|---|---|
| `sliding_sync/build.rs` timeline loop | `timeline_events.iter().map(Into::into)` → `Vec<Raw<AnySyncTimelineEvent>>` |
| `sliding_sync/build.rs` required_state | `.map(TryInto::try_into).collect::<Result<_, _>>()?` — error propagates via `SyncError::EventConversion` |
| `sliding_sync/build.rs` invite member | `ev.try_into()?` — same propagation |
| `legacy_sync/translate.rs::knock_room_shape` | `room.required_state.iter().map(event_view::strip_state_event).collect::<Vec<Raw<AnyStrippedStateEvent>>>()`, handed straight to `json!` |

The `expect`-message at each `TryInto` site argued the conversion couldn't
fail (state map / member lookup guarantees `state_key.is_some()`). True
for well-formed storage, but CLAUDE.md bans `.expect` in handler code:
a storage-layer regression would have panicked instead of returning 500.
We now propagate.

Federation paths (`StorageBackend::persist_event` → outbox) untouched.
They continue to ship `Event.raw` verbatim, which is correct because
federation peers verify the reference hash against the wire bytes.

## Tests

Unit tests in `neutrino-common::event_view::tests` (13 total):

- `from_event_for_sync_timeline_injects_event_id`
- `from_event_for_sync_timeline_create_event_carries_room_id`
- `from_event_for_sync_timeline_non_create_strips_room_id`
- `from_event_for_sync_timeline_preserves_unsigned`
- `try_from_event_for_sync_state_err_when_no_state_key`
- `try_from_event_for_sync_state_ok_when_state_key_present`
- `try_from_event_for_sync_state_preserves_unsigned_and_prev_content`
- `try_from_event_for_stripped_state_keeps_only_four_canonical_fields`
- `try_from_event_for_stripped_state_err_when_no_state_key`
- `from_event_for_room_event_carries_both_ids`
- `from_event_for_room_event_create_event_also_carries_both_ids`
- `strip_state_event_drops_non_canonical_fields`
- `strip_state_event_returns_empty_for_non_object_input`

In `neutrino-state::event_id::tests`:

- `build_output_raw_lacks_event_id` — pins that `EventBuilder` never
  serialises `event_id` into `raw` for either create or non-create
  output; the load-bearing invariant the enrichment pipeline rests on.

E2e regressions — landing these closes the gap that let the original bug
ship:

- `tests/e2e_legacy_sync.rs::send_event_then_legacy_sync_returns_event_with_event_id`
- `tests/e2e_legacy_sync.rs::legacy_sync_create_event_carries_room_id_and_event_id`
- `tests/e2e_sliding_sync.rs::put_event_then_sliding_sync_returns_event_with_event_id`

## Non-goals (explicitly out of scope)

- Refactoring how sliding-sync produces `invite_state` / `knock_state`.
  Knock currently goes via `required_state` + manual strip; a native
  knock-state path is a separate change.
- `unsigned.age` injection. Not currently used by any complement test we
  care about.
- Caching the client-view JSON alongside `Event.raw` to skip the re-parse.
  Embedded single-user server; perf is irrelevant.
