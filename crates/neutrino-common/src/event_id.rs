//! Hashing and event-id derivation primitives (PR 2 / B0).
//!
//! Building blocks used by:
//! - `EventBuilder` in `neutrino-state::event_id` (server-authored events)
//! - `Event::from_wire` here in `neutrino-common::event` (federation receive)
//!
//! This module owns the four primitives the v12 / MSC4242 hash flow needs:
//! canonical-JSON encoding, SHA-256, two base64 flavours, and a redaction
//! wrapper. PR 2 / B1 layers `content_hash` / `reference_hash` /
//! `event_id_from_hash` on top of these.
//!
//! See `event-id-design.md` for the full flow.

// Consumed by B1 (`content_hash` / `reference_hash` / `event_id_from_hash`)
// within this same module in the next commit; the dead-code allowance lifts
// then.
#![allow(dead_code)]

use base64::Engine;
use base64::engine::general_purpose;
use ruma::canonical_json::{CanonicalJsonObject, RedactionError, redact_in_place};
use ruma::room_version_rules::RoomVersionRules;
use sha2::{Digest, Sha256};

/// Serialise a canonical-JSON object to its canonical byte representation.
///
/// `CanonicalJsonObject` is a `BTreeMap<String, CanonicalJsonValue>` whose
/// `serde::Serialize` impl already produces canonical-JSON output (sorted
/// keys, no whitespace, integer range check, no NaN/Inf). This wrapper
/// pinpoints the call site so any future change to the encoding lives in
/// one place.
pub(crate) fn canonical(obj: &CanonicalJsonObject) -> Vec<u8> {
    // `serde_json::to_vec` against a `CanonicalJsonObject` cannot fail: the
    // value tree was already validated when constructing the
    // `CanonicalJsonValue`s, and `BTreeMap`/`Vec`/`String`/primitives never
    // fail to serialise.
    serde_json::to_vec(obj).expect("CanonicalJsonObject is always serialisable")
}

/// SHA-256 of `bytes`.
pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Standard-alphabet base64, no padding.
///
/// Used for the `hashes.sha256` field — the Matrix spec specifies
/// "unpadded Base64" (standard alphabet, no `=` padding) for content hashes.
pub(crate) fn b64_unpadded(bytes: &[u8]) -> String {
    general_purpose::STANDARD_NO_PAD.encode(bytes)
}

/// URL-safe-alphabet base64, no padding.
///
/// Used for the event_id suffix (v3+): `event_id = "$" + b64url_unpadded(reference_hash)`.
pub(crate) fn b64_url_unpadded(bytes: &[u8]) -> String {
    general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Redaction for reference-hash computation, with the MSC4242 carve-out.
///
/// Runs ruma's `redact_in_place` against the v12 redaction rules (== v11),
/// but preserves `prev_state_events` across the call. MSC4242 adds
/// `prev_state_events` as a state-DAG parentage field that must be covered
/// by the reference hash, but the v11/v12 spec redaction keep-list doesn't
/// (yet) mention it. Save → redact → restore is the minimal divergence
/// pending the MSC landing in the spec.
///
/// See `project-msc4242-redaction` memory and `event-id-design.md`
/// §"ruma redaction wrapper".
pub(crate) fn redact_for_hash(obj: &mut CanonicalJsonObject) -> Result<(), RedactionError> {
    let saved_prev_state = obj.remove("prev_state_events");
    redact_in_place(obj, &RoomVersionRules::V12.redaction, None)?;
    if let Some(v) = saved_prev_state {
        obj.insert("prev_state_events".to_owned(), v);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruma::canonical_json::CanonicalJsonValue;
    use serde_json::json;

    fn obj(v: serde_json::Value) -> CanonicalJsonObject {
        let CanonicalJsonValue::Object(o) = v.try_into().unwrap() else {
            panic!("expected object");
        };
        o
    }

    #[test]
    fn canonical_sorts_keys_and_omits_whitespace() {
        let o = obj(json!({ "b": 1, "a": "x" }));
        let bytes = canonical(&o);
        assert_eq!(bytes, br#"{"a":"x","b":1}"#);
    }

    #[test]
    fn canonical_nested_objects_also_sorted() {
        let o = obj(json!({ "outer": { "z": 1, "a": 2 } }));
        assert_eq!(canonical(&o), br#"{"outer":{"a":2,"z":1}}"#);
    }

    #[test]
    fn sha256_known_vector() {
        // FIPS 180-2 test vector — sha256("abc")
        let h = sha256(b"abc");
        let expected: [u8; 32] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        assert_eq!(h, expected);
    }

    #[test]
    fn b64_unpadded_is_standard_alphabet_no_padding() {
        // RFC 4648 §10 — standard alphabet uses '+' and '/', and a 1-byte
        // input ("f") would normally be padded "Zg==".
        assert_eq!(b64_unpadded(b"f"), "Zg");
        // '?' in input → '/' appears in standard output (and the URL-safe
        // helper would emit '_' instead).
        assert_eq!(b64_unpadded(&[0xff, 0xff, 0xff]), "////");
    }

    #[test]
    fn b64_url_unpadded_is_url_safe_no_padding() {
        // Same bytes as above, but URL-safe alphabet (`-_` instead of `+/`).
        assert_eq!(b64_url_unpadded(&[0xff, 0xff, 0xff]), "____");
        assert_eq!(b64_url_unpadded(b"f"), "Zg");
    }

    #[test]
    fn redact_for_hash_preserves_prev_state_events() {
        // A message event with prev_state_events set — under stock V11
        // redaction prev_state_events would be stripped; our wrapper must
        // restore it after redaction.
        let mut o = obj(json!({
            "type": "m.room.message",
            "sender": "@a:example.org",
            "room_id": "!r:example.org",
            "content": { "body": "hi" },
            "prev_events": ["$p:example.org"],
            "prev_state_events": ["$ps:example.org"],
            "origin_server_ts": 1000,
            "unsigned": { "age": 5 }
        }));
        redact_for_hash(&mut o).expect("redaction succeeds");

        // prev_state_events survives.
        assert_eq!(
            o.get("prev_state_events"),
            Some(&CanonicalJsonValue::Array(vec![
                CanonicalJsonValue::String("$ps:example.org".to_owned())
            ]))
        );
        // prev_events is in the v11 keep-list, so it also survives.
        assert!(o.contains_key("prev_events"));
        // content gets emptied for m.room.message (no body in v11 keep-list).
        assert_eq!(
            o.get("content"),
            Some(&CanonicalJsonValue::Object(CanonicalJsonObject::new()))
        );
    }

    #[test]
    fn redact_for_hash_no_op_when_prev_state_events_absent() {
        // Same flow on an event without prev_state_events — the wrapper
        // must not synthesise the key.
        let mut o = obj(json!({
            "type": "m.room.message",
            "sender": "@a:example.org",
            "room_id": "!r:example.org",
            "content": { "body": "hi" },
            "prev_events": [],
            "origin_server_ts": 1000
        }));
        redact_for_hash(&mut o).expect("redaction succeeds");
        assert!(!o.contains_key("prev_state_events"));
    }
}
