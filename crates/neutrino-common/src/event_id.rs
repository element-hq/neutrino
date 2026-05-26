//! Hashing and event-id derivation for v12 / MSC4242.
//!
//! Used by:
//! - `EventBuilder` in `neutrino-state::event_id` (server-authored events)
//! - `Event::from_wire` here in `neutrino-common::event` (federation receive)
//!
//! Layered as:
//! - **B0** — internal primitives (`canonical`, `sha256`, two base64 flavours,
//!   `redact_for_hash`).
//! - **B1** — public spec functions (`content_hash`, `reference_hash`,
//!   `event_id_from_hash`, `room_id_from_create`).
//!
//! See `event-id-design.md` for the full flow.

use base64::Engine;
use base64::engine::general_purpose;
use ruma::canonical_json::{CanonicalJsonObject, RedactionError, redact_in_place};
use ruma::room_version_rules::RoomVersionRules;
use ruma::{EventId, OwnedEventId, OwnedRoomId};
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
///
/// Consumed by `EventBuilder` in B2 — kept under `#[allow(dead_code)]`
/// until then so the helper sits next to its url-safe counterpart.
#[allow(dead_code)]
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

/// Content hash of an unhashed event object.
///
/// Spec: <https://spec.matrix.org/v1.18/server-server-api/#calculating-the-content-hash-for-an-event>.
/// Removes `unsigned`, `signatures`, `hashes` from a clone of the input,
/// canonical-encodes, SHA-256s. The b64-encoded form goes into the event's
/// `hashes.sha256` field before signing.
pub fn content_hash(obj: &CanonicalJsonObject) -> [u8; 32] {
    let mut clone = obj.clone();
    clone.remove("unsigned");
    clone.remove("signatures");
    clone.remove("hashes");
    sha256(&canonical(&clone))
}

/// Reference hash of an event object — feeds the v3+ event_id.
///
/// Spec: <https://spec.matrix.org/v1.18/server-server-api/#calculating-the-reference-hash-for-an-event>.
/// Removes `unsigned` and `signatures`, then runs [`redact_for_hash`] (V12
/// redaction with the MSC4242 `prev_state_events` carve-out), canonical-encodes,
/// SHA-256s.
///
/// Returns `RedactionError` only if the input object violates the redaction
/// preconditions ruma checks (e.g. non-object `content`, missing `type`).
pub fn reference_hash(obj: &CanonicalJsonObject) -> Result<[u8; 32], RedactionError> {
    let mut clone = obj.clone();
    clone.remove("unsigned");
    clone.remove("signatures");
    redact_for_hash(&mut clone)?;
    Ok(sha256(&canonical(&clone)))
}

/// Format an event_id from a reference hash. v3+ rooms only.
///
/// `event_id = "$" + url-safe-unpadded-base64(reference_hash)`.
pub fn event_id_from_hash(hash: &[u8; 32]) -> OwnedEventId {
    let s = format!("${}", b64_url_unpadded(hash));
    // 43 url-safe-b64 chars after `$` is always a syntactically valid event_id.
    OwnedEventId::try_from(s).expect("'$' + 43 url-safe-b64 chars is a valid event_id")
}

/// Derive a room_id from a create event's event_id.
///
/// Room v12 uses `RoomIdFormatVersion::V2`: the room_id is the create event's
/// event_id with the leading `$` swapped to `!`. The suffix is identical
/// (43 url-safe-b64 chars). Spec: <https://spec.matrix.org/v1.18/rooms/v12/#room-ids>.
pub fn room_id_from_create(create_event_id: &EventId) -> OwnedRoomId {
    let s = create_event_id.as_str();
    debug_assert!(
        s.starts_with('$'),
        "EventId by construction starts with '$'"
    );
    let swapped = format!("!{}", &s[1..]);
    OwnedRoomId::try_from(swapped)
        .expect("'!' + valid event_id suffix is a valid room_id under format v2")
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

    // ---- B1: content_hash, reference_hash, event_id_from_hash, room_id_from_create ----

    /// Spec appendix §event-signing, vector 1 ("minimally-sized event").
    /// v1-shaped (`origin` field, v1 event_id format, `auth_events: []` on a
    /// non-create event) — valid as a content-hash test because the algorithm
    /// is byte-level and doesn't care about field semantics.
    #[test]
    fn content_hash_spec_vector_minimally_sized_event() {
        let o = obj(json!({
            "room_id": "!x:domain",
            "sender": "@a:domain",
            "origin": "domain",
            "origin_server_ts": 1_000_000,
            "signatures": {},
            "hashes": {},
            "type": "X",
            "content": {},
            "prev_events": [],
            "auth_events": [],
            "depth": 3,
            "unsigned": { "age_ts": 1_000_000 }
        }));
        let h = content_hash(&o);
        assert_eq!(
            b64_unpadded(&h),
            "5jM4wQpv6lnBo7CLIghJuHdW+s2CMBJPUOGOC89ncos"
        );
    }

    /// Spec appendix §event-signing, vector 2 ("event containing redactable content").
    #[test]
    fn content_hash_spec_vector_redactable_content() {
        let o = obj(json!({
            "content": { "body": "Here is the message content" },
            "event_id": "$0:domain",
            "origin": "domain",
            "origin_server_ts": 1_000_000,
            "type": "m.room.message",
            "room_id": "!r:domain",
            "sender": "@u:domain",
            "signatures": {},
            "unsigned": { "age_ts": 1_000_000 }
        }));
        let h = content_hash(&o);
        assert_eq!(
            b64_unpadded(&h),
            "onLKD1bGljeBWQhWZ1kaP9SorVmRQNdN5aM2JYU2n/g"
        );
    }

    #[test]
    fn content_hash_is_invariant_under_unsigned_signatures_hashes() {
        let base = obj(json!({
            "type": "X", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1
        }));
        let h_base = content_hash(&base);

        // Adding unsigned/signatures/hashes must not change the hash.
        let with_strippable = obj(json!({
            "type": "X", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "unsigned": { "age": 42 },
            "signatures": { "d": { "ed25519:a": "sig" } },
            "hashes": { "sha256": "junk" }
        }));
        assert_eq!(h_base, content_hash(&with_strippable));

        // Changing a non-stripped field must change the hash.
        let altered = obj(json!({
            "type": "Y", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1
        }));
        assert_ne!(h_base, content_hash(&altered));
    }

    #[test]
    fn reference_hash_strips_unsigned_and_signatures() {
        let base = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": [], "auth_events": []
        }));
        let h_base = reference_hash(&base).expect("redacts");

        let with_strippable = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": [], "auth_events": [],
            "unsigned": { "age": 42 },
            "signatures": { "d": { "ed25519:a": "sig" } }
        }));
        assert_eq!(h_base, reference_hash(&with_strippable).expect("redacts"));
    }

    #[test]
    fn reference_hash_covers_prev_state_events_msc4242() {
        // Two otherwise-identical events differing only in prev_state_events
        // must produce different reference hashes — proof that our MSC4242
        // carve-out actually feeds the hash.
        let a = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": [], "prev_state_events": ["$ps_a:d"]
        }));
        let b = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": [], "prev_state_events": ["$ps_b:d"]
        }));
        assert_ne!(
            reference_hash(&a).expect("redacts"),
            reference_hash(&b).expect("redacts"),
        );
    }

    #[test]
    fn reference_hash_differs_from_content_hash_for_redactable_event() {
        // m.room.message's content gets stripped on redaction → ref hash
        // must differ from content hash (which preserves content).
        let o = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": { "body": "hi" }, "origin_server_ts": 1,
            "prev_events": [], "auth_events": []
        }));
        let c = content_hash(&o);
        let r = reference_hash(&o).expect("redacts");
        assert_ne!(c, r);
    }

    #[test]
    fn event_id_from_hash_dollar_plus_url_safe_b64() {
        // All-0xff input exercises the URL-safe alphabet distinction
        // (`-` / `_` instead of `+` / `/`). 32 bytes = 256 bits, encoded
        // as 43 base64 chars: 42 sextets of `111111` = `_`, plus the trailing
        // sextet `1111_00` (4 bits of payload + 2 padding zero bits) = '8'.
        let hash = [0xff_u8; 32];
        let id = event_id_from_hash(&hash);
        assert_eq!(id.as_str(), "$__________________________________________8");
        assert_eq!(id.as_str().len(), 1 + 43);
    }

    #[test]
    fn event_id_from_hash_zero_vector() {
        let hash = [0_u8; 32];
        let id = event_id_from_hash(&hash);
        assert_eq!(id.as_str(), "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

    #[test]
    fn room_id_from_create_sigil_swap() {
        let hash = [0xab_u8; 32];
        let event_id = event_id_from_hash(&hash);
        let room_id = room_id_from_create(&event_id);
        assert!(room_id.as_str().starts_with('!'));
        // Suffix is byte-identical to the event_id (everything after the sigil).
        assert_eq!(&room_id.as_str()[1..], &event_id.as_str()[1..]);
    }
}
