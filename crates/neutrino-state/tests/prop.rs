//! Property-based tests.
//!
//! The 69 case tests in `src/` are the spec-anchored documentation — each one
//! corresponds to a quoted "reject" / "MUST" clause. The properties below
//! cover gaps where case enumeration would be tedious or non-exhaustive:
//!
//! 1. `parse_event_rejects_any_missing_required_field` — sweep removal of
//!    every required PDU field, replacing the ~10 hand-written
//!    `rejects_missing_*` case tests with one parameterised property.
//! 2. `auth_event_keys_never_includes_create` — universal version of the
//!    `v12_omits_create_event_key` case test: holds for *every* event,
//!    not just the specific one we wrote.
//! 3. `calculate_auth_events_returns_only_state_values` — proves the lookup
//!    half is a strict pass-through filter and never fabricates IDs.

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
    "\\$[A-Za-z0-9_-]{43}".prop_map(|s| s.parse().expect("v12 event id"))
}

fn arb_membership() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("join".to_string()),
        Just("leave".to_string()),
        Just("invite".to_string()),
        Just("ban".to_string()),
        Just("knock".to_string()),
    ]
}

/// Arbitrary `Event` — a mix of `m.room.message` and `m.room.member`, with
/// varied senders, targets, and memberships.
///
/// Membership/target combinations are filtered to ones that make sense on a
/// real wire: self-invite and self-ban are dropped (sender can't invite or
/// ban themselves). Other auth-validity is *not* enforced — Phase 1a accepts
/// any well-formed wire shape, so the strategy reflects that.
fn arb_event() -> impl Strategy<Value = Event> {
    let message = (arb_user_id(), arb_event_id()).prop_map(|(sender, id)| {
        let mut v = base_message();
        v["sender"] = json!(sender);
        parse_event(raw(v), id, RoomVersion::V12).expect("valid message")
    });
    let member = (
        arb_user_id(),
        arb_user_id(),
        arb_membership(),
        arb_event_id(),
    )
        .prop_filter(
            "self-invite and self-ban are impossible on the wire",
            |(s, t, m, _)| s != t || (m != "invite" && m != "ban"),
        )
        .prop_map(|(sender, target, membership, id)| {
            let mut v = base_message();
            v["type"] = json!("m.room.member");
            v["sender"] = json!(sender);
            v["state_key"] = json!(target);
            v["content"] = json!({ "membership": membership });
            parse_event(raw(v), id, RoomVersion::V12).expect("valid member")
        });
    prop_oneof![message, member]
}

/// Arbitrary `StateMap<OwnedEventId>` — keys are arbitrary `(type, state_key)`
/// tuples, values are arbitrary event IDs. Not constrained to "well-formed"
/// state — the property under test only cares about lookup behaviour.
fn arb_state_map() -> impl Strategy<Value = StateMap<OwnedEventId>> {
    prop::collection::hash_map(
        ("[a-z.]{1,20}", "[a-zA-Z@:_-]{0,30}"),
        arb_event_id(),
        0..20,
    )
}

// ---------- properties ----------

proptest! {
    /// Removing any required top-level field from an otherwise-valid event
    /// produces `FormatError::MissingField` naming exactly that field.
    #[test]
    fn parse_event_rejects_any_missing_required_field(idx in 0usize..REQUIRED_FIELDS.len()) {
        let field = REQUIRED_FIELDS[idx];
        let mut v = base_message();
        v.as_object_mut().expect("object").remove(field);
        let result = parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12);
        match result {
            Err(FormatError::MissingField(f)) => prop_assert_eq!(f, field),
            other => prop_assert!(
                false,
                "expected MissingField({}), got {:?}",
                field,
                other
            ),
        }
    }

    /// Universal v12 invariant: `auth_event_keys` never asks for
    /// `m.room.create`. Holds for any event, any sender, any membership.
    #[test]
    fn auth_event_keys_never_includes_create(event in arb_event()) {
        let keys = auth_event_keys(&event);
        prop_assert!(
            !keys.iter().any(|(t, _)| t == "m.room.create"),
            "v12 must not request m.room.create"
        );
    }

    /// `calculate_auth_events` is a strict pass-through filter — every
    /// returned event ID is a value of *some* key in the state map. It
    /// never fabricates IDs.
    #[test]
    fn calculate_auth_events_returns_only_state_values(
        event in arb_event(),
        state in arb_state_map(),
    ) {
        let result = calculate_auth_events(&event, &state);
        let state_values: HashSet<_> = state.values().cloned().collect();
        for id in &result {
            prop_assert!(
                state_values.contains(id),
                "calculate_auth_events fabricated event id: {}",
                id
            );
        }
    }
}
