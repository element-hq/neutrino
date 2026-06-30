//! Property tests for `event_id`'s public hash + id functions.
//!
//! The case tests in `src/event_id.rs` are the spec-anchored documentation
//! — each one pins a verbatim spec clause or known input/output vector.
//! The properties below sweep input shapes that case enumeration can't
//! efficiently cover:
//!
//! 1. `content_hash_is_invariant_under_arbitrary_unsigned_signatures_hashes`
//!    — generalises the existing strip-invariance case test across any JSON
//!    value (nested objects, arrays, nulls) the strippable fields can hold.
//! 2. `reference_hash_is_invariant_under_arbitrary_unsigned_signatures`
//!    — same property for the reference hash, with `hashes` *excluded*
//!    (it's in the v11 redaction keep-list).
//! 3. `reference_hash_distinguishes_distinct_prev_state_events_lists`
//!    — pins the MSC4242 carve-out at the property level: distinct
//!    `prev_state_events` arrays yield distinct reference hashes. The
//!    existing case test only proves the property for one pair.

use neutrino_event::event_id::{content_hash, reference_hash};
use proptest::prelude::*;
use ruma::canonical_json::{CanonicalJsonObject, CanonicalJsonValue};
use serde_json::{Value, json};

// ---------- helpers ----------

/// Convert a `serde_json::Value` (expected to be an object) into a
/// `CanonicalJsonObject`. Panics on non-object input — only used here on
/// values we construct from `json!({...})`.
fn obj(v: Value) -> CanonicalJsonObject {
    let CanonicalJsonValue::Object(o) = v.try_into().expect("canonical conversion") else {
        panic!("expected object");
    };
    o
}

/// Minimal valid message-event shape — every required PDU field present so
/// `reference_hash`'s call into ruma's `redact_in_place` doesn't trip on a
/// missing `type` etc.
fn base_message() -> Value {
    json!({
        "type": "m.room.message",
        "sender": "@alice:example.org",
        "room_id": "!room:example.org",
        "content": { "msgtype": "m.text", "body": "hi" },
        "prev_events": [],
        "prev_state_events": [],
        "origin_server_ts": 1_700_000_000_000_u64
    })
}

// ---------- strategies ----------

/// Arbitrary JSON value bounded to canonical-JSON's accepted subset.
///
/// Recursive: leaves are null / bool / i32-bounded ints / short strings;
/// branches are objects (≤ 4 fields) and arrays (≤ 4 elements). Depth ≤ 3,
/// total nodes ≤ 32, which keeps shrinking fast and stays within
/// `js_int::Int` range so `CanonicalJsonValue::try_from` doesn't refuse.
fn arb_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        // js_int::Int range is ±2^53; i32 is well within that.
        any::<i32>().prop_map(|i| Value::Number(i.into())),
        "[a-z]{0,8}".prop_map(Value::String),
    ];
    leaf.prop_recursive(
        3,  // depth
        32, // total nodes
        4,  // items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{1,4}", inner), 0..4)
                    .prop_map(|kvs| Value::Object(kvs.into_iter().collect())),
            ]
        },
    )
}

/// Arbitrary list of v12-shaped event IDs (0-4 entries). Used to drive
/// `prev_state_events`. v12 event_ids are `$` + 43 url-safe-base64 chars
/// (no domain suffix) — see `event_id_from_hash`. Only distinctness matters
/// for the property, but matching the project's actual on-wire shape avoids
/// inconsistency with the rest of the codebase.
fn arb_event_id_list() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        "[A-Za-z0-9_-]{43}".prop_map(|suffix| format!("${suffix}")),
        0..4,
    )
}

// ---------- properties ----------

proptest! {
    /// Adding any JSON value as `unsigned`, `signatures`, or `hashes` on an
    /// event must not change its `content_hash`. The spec specifies these
    /// three fields are stripped before hashing, so any shape they take is
    /// equivalent to their absence.
    #[test]
    fn content_hash_is_invariant_under_arbitrary_unsigned_signatures_hashes(
        unsigned in arb_json_value(),
        signatures in arb_json_value(),
        hashes in arb_json_value(),
    ) {
        let base = base_message();
        let h_base = content_hash(&obj(base.clone()));

        let mut with_overlays = base.as_object().expect("object").clone();
        with_overlays.insert("unsigned".to_owned(), unsigned);
        with_overlays.insert("signatures".to_owned(), signatures);
        with_overlays.insert("hashes".to_owned(), hashes);
        let h_overlaid = content_hash(&obj(Value::Object(with_overlays)));

        prop_assert_eq!(h_base, h_overlaid);
    }

    /// Same for `reference_hash`, but only `unsigned` and `signatures` are
    /// stripped — `hashes` is in the v11 redaction keep-list so it must
    /// survive redaction. Sweeping `hashes` would (correctly) produce
    /// different reference hashes, so it's omitted from this property.
    #[test]
    fn reference_hash_is_invariant_under_arbitrary_unsigned_signatures(
        unsigned in arb_json_value(),
        signatures in arb_json_value(),
    ) {
        let base = base_message();
        let h_base = reference_hash(&obj(base.clone())).expect("redacts");

        let mut with_overlays = base.as_object().expect("object").clone();
        with_overlays.insert("unsigned".to_owned(), unsigned);
        with_overlays.insert("signatures".to_owned(), signatures);
        let h_overlaid = reference_hash(&obj(Value::Object(with_overlays))).expect("redacts");

        prop_assert_eq!(h_base, h_overlaid);
    }

    /// MSC4242 invariant: two events differing only in their
    /// `prev_state_events` array produce different reference hashes. The
    /// case test in `src/event_id.rs` only proves this for one specific
    /// pair; this property generalises it across any two distinct lists.
    ///
    /// SHA-256 collision over distinct inputs is mathematically negligible
    /// at 256 bits, so no collision caveat is needed in practice.
    #[test]
    fn reference_hash_distinguishes_distinct_prev_state_events_lists(
        list_a in arb_event_id_list(),
        list_b in arb_event_id_list(),
    ) {
        prop_assume!(list_a != list_b);

        let mut a = base_message();
        a.as_object_mut().expect("object").insert(
            "prev_state_events".to_owned(),
            Value::Array(list_a.into_iter().map(Value::String).collect()),
        );
        let mut b = base_message();
        b.as_object_mut().expect("object").insert(
            "prev_state_events".to_owned(),
            Value::Array(list_b.into_iter().map(Value::String).collect()),
        );

        let h_a = reference_hash(&obj(a)).expect("redacts");
        let h_b = reference_hash(&obj(b)).expect("redacts");

        prop_assert_ne!(h_a, h_b);
    }
}
