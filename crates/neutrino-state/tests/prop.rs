//! Property-based tests.
//!
//! The 69 case tests in `src/` are the spec-anchored documentation — each one
//! corresponds to a quoted "reject" / "MUST" clause. The properties below
//! supplement those by sweeping inputs that hand-written cases couldn't
//! efficiently cover:
//!
//! 1. `auth_event_keys_never_includes_create` — universal version of the
//!    `v12_omits_create_event_key` case test: holds for *every* event.
//! 2. `calculate_auth_events_is_strict_pass_through_filter` — couples the
//!    selector to the lookup: every key returned by `auth_event_keys` that
//!    resolves in `state` must appear in `result`, and no other ids may.
//! 3. `calculate_auth_events_excludes_create_even_when_in_state` —
//!    universal version of the create-exclusion case test: v12 must drop
//!    `(m.room.create, "")` from any input state.
//!
//! `parse_event_rejects_any_missing_required_field` is kept here as a plain
//! `#[test]` that loops over `REQUIRED_FIELDS` — sweeping a fixed array
//! doesn't need proptest entropy.

use std::collections::HashSet;

use neutrino_state::auth_events::{auth_event_keys, calculate_auth_events};
use neutrino_state::validate::parse_event;
use neutrino_state::{Event, FormatError, RoomVersion, StateMap};
use proptest::prelude::*;
use ruma::OwnedEventId;
use serde_json::value::RawValue;
use serde_json::{Value, json};

// ---------- helpers ----------

fn raw(v: Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(&v).expect("fixture")
}

fn eid(s: &str) -> OwnedEventId {
    s.parse().expect("event id")
}

/// Minimal valid non-create event with every required PDU field populated.
fn base_message() -> Value {
    json!({
        "type": "m.room.message",
        "sender": "@alice:example.org",
        "room_id": "!room:example.org",
        "content": { "msgtype": "m.text", "body": "hi" },
        "prev_events": [],
        "prev_state_events": [],
        "depth": 1,
        "origin_server_ts": 1_700_000_000_000_u64,
        "hashes": { "sha256": "abc" }
    })
}

const REQUIRED_FIELDS: &[&str] = &[
    "type",
    "sender",
    "content",
    "depth",
    "origin_server_ts",
    "prev_events",
    "prev_state_events",
    "room_id",
    "hashes",
];

/// Removing any required top-level field from an otherwise-valid event
/// produces `FormatError::MissingField` naming exactly that field. Plain
/// enumeration — proptest isn't the right tool for sweeping a fixed list.
#[test]
fn parse_event_rejects_any_missing_required_field() {
    for &field in REQUIRED_FIELDS {
        let mut v = base_message();
        v.as_object_mut().expect("object").remove(field);
        let result = parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12);
        match result {
            Err(FormatError::MissingField(f)) => {
                assert_eq!(f, field, "field {field} produced wrong MissingField");
            }
            other => panic!("expected MissingField({field}), got {other:?}"),
        }
    }
}

// ---------- strategies ----------

/// "localpart" that's safe across user-id / event-id parsers (lowercase ASCII).
fn arb_localpart() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

fn arb_user_id() -> impl Strategy<Value = String> {
    arb_localpart().prop_map(|s| format!("@{s}:example.org"))
}

/// v12 event id: `$` + 43 chars of URL-safe unpadded base64 (the encoded
/// SHA-256 reference hash). Synthetic for tests — we don't compute a real
/// hash, just shape the string the way ruma will parse a v12 event id.
fn arb_event_id() -> impl Strategy<Value = OwnedEventId> {
    "\\$[A-Za-z0-9_-]{43}".prop_filter_map("parseable as OwnedEventId", |s| s.parse().ok())
}

/// Variable-length `prev_events`. Cap chosen low — anything more just
/// churns the shrinker without improving coverage.
fn arb_prev_events() -> impl Strategy<Value = Vec<OwnedEventId>> {
    prop::collection::vec(arb_event_id(), 0..=3)
}

/// Variable-length `prev_state_events`. Phase 1b validates these against a
/// provider — only the wire shape matters here, so any ids do.
fn arb_prev_state_events() -> impl Strategy<Value = Vec<OwnedEventId>> {
    prop::collection::vec(arb_event_id(), 0..=3)
}

fn ids_as_json(ids: &[OwnedEventId]) -> Value {
    json!(ids.iter().map(|e| e.as_str()).collect::<Vec<_>>())
}

/// Strategy for an `m.room.message` event with varied sender and ancestry.
fn arb_message_event() -> impl Strategy<Value = Event> {
    (
        arb_user_id(),
        arb_event_id(),
        arb_prev_events(),
        arb_prev_state_events(),
    )
        .prop_filter_map("valid message", |(sender, id, prevs, prev_states)| {
            let mut v = base_message();
            v["sender"] = json!(sender);
            v["prev_events"] = ids_as_json(&prevs);
            v["prev_state_events"] = ids_as_json(&prev_states);
            parse_event(raw(v), id, RoomVersion::V12).ok()
        })
}

/// Strategy for an `m.room.member` event with varied sender/target and a
/// membership chosen by cases: when `sender == target` the wire-impossible
/// memberships `invite` and `ban` are excluded by construction — no
/// `prop_filter` rejection cycle.
fn arb_member_event() -> impl Strategy<Value = Event> {
    (arb_user_id(), arb_user_id())
        .prop_flat_map(|(sender, target)| {
            let memberships: Vec<&'static str> = if sender == target {
                vec!["join", "leave", "knock"]
            } else {
                vec!["join", "leave", "invite", "ban", "knock"]
            };
            (
                Just(sender),
                Just(target),
                proptest::sample::select(memberships),
                arb_event_id(),
                arb_prev_events(),
                arb_prev_state_events(),
            )
        })
        .prop_filter_map(
            "valid member",
            |(sender, target, membership, id, prevs, prev_states)| {
                let mut v = base_message();
                v["type"] = json!("m.room.member");
                v["sender"] = json!(sender);
                v["state_key"] = json!(target);
                v["content"] = json!({ "membership": membership });
                v["prev_events"] = ids_as_json(&prevs);
                v["prev_state_events"] = ids_as_json(&prev_states);
                parse_event(raw(v), id, RoomVersion::V12).ok()
            },
        )
}

/// Strategy for an `m.room.create` event. Create events carry no `room_id`
/// (derived from `event_id`) and no ancestry (rule 1.1 / MSC4242).
fn arb_create_event() -> impl Strategy<Value = Event> {
    (arb_user_id(), arb_event_id()).prop_filter_map("valid create", |(sender, id)| {
        let mut v = base_message();
        v["type"] = json!("m.room.create");
        v["sender"] = json!(sender);
        v["content"] = json!({ "room_version": "12" });
        v["state_key"] = json!("");
        let obj = v.as_object_mut()?;
        obj.remove("room_id");
        obj.remove("prev_state_events");
        // prev_events stays [] from base_message — rule 1.1.
        parse_event(raw(v), id, RoomVersion::V12).ok()
    })
}

/// Arbitrary `Event` — mix of message, member, and create events. Senders,
/// targets, memberships, and ancestry all vary.
fn arb_event() -> impl Strategy<Value = Event> {
    prop_oneof![arb_message_event(), arb_member_event(), arb_create_event(),]
}

/// Arbitrary `StateMap<OwnedEventId>` — keys are arbitrary `(type, state_key)`
/// tuples, values are arbitrary event IDs. Not constrained to "well-formed"
/// state — the properties under test only care about lookup behaviour.
fn arb_state_map() -> impl Strategy<Value = StateMap<OwnedEventId>> {
    prop::collection::hash_map(
        ("[a-z.]{1,20}", "[a-zA-Z@:_-]{0,30}"),
        arb_event_id(),
        0..20,
    )
}

// ---------- properties ----------

proptest! {
    /// Universal v12 invariant: `auth_event_keys` never asks for
    /// `m.room.create`. Holds for any event — create events themselves
    /// return an empty Vec, non-create events must exclude the key.
    #[test]
    fn auth_event_keys_never_includes_create(event in arb_event()) {
        let keys = auth_event_keys(&event);
        prop_assert!(
            !keys.iter().any(|(t, _)| t == "m.room.create"),
            "v12 must not request m.room.create"
        );
    }

    /// `calculate_auth_events` is a strict pass-through filter: the
    /// returned ids are exactly the values that `auth_event_keys` requests
    /// and that resolve in `state` — no fabrication, no dropping.
    ///
    /// A no-op `vec![]` implementation would have satisfied "no fabrication"
    /// on its own; the set equality below couples the property to the real
    /// selection behaviour.
    #[test]
    fn calculate_auth_events_is_strict_pass_through_filter(
        event in arb_event(),
        state in arb_state_map(),
    ) {
        let result: HashSet<_> = calculate_auth_events(&event, &state).into_iter().collect();
        let expected: HashSet<_> = auth_event_keys(&event)
            .into_iter()
            .filter_map(|k| state.get(&k).cloned())
            .collect();
        prop_assert_eq!(result, expected);
    }

    /// v12: even when the state map carries a `(m.room.create, "")` entry,
    /// `calculate_auth_events` never includes that id in its output.
    #[test]
    fn calculate_auth_events_excludes_create_even_when_in_state(
        event in arb_event(),
        state in arb_state_map(),
        create_id in arb_event_id(),
    ) {
        let mut state = state;
        // Avoid the (astronomically rare) collision where `create_id` is
        // already a value at some other key — keeps the property
        // unconditional rather than probabilistic.
        state.retain(|_, v| v != &create_id);
        state.insert(("m.room.create".to_string(), String::new()), create_id.clone());

        let result = calculate_auth_events(&event, &state);
        prop_assert!(
            !result.contains(&create_id),
            "v12 must exclude m.room.create from auth_events even when present in state"
        );
    }
}
