//! Hashing and event-id derivation.
//!
//! Used by:
//! - [`EventBuilder`](crate::event_builder::EventBuilder) (server-authored
//!   events)
//! - [`from_wire`](crate::event_builder::from_wire) (federation receive)
//!
//! Layered as:
//! - **Internal primitives** — `canonical`, `sha256`, two base64 flavours,
//!   `redact_for_hash`.
//! - **Public spec functions** — `content_hash`, `reference_hash`,
//!   `event_id_from_hash`, `room_id_from_create`.
//!
//! Which redaction keep-list and which derivation apply is a property of the
//! room's version, so the functions here take a [`RoomVersion`] rather than
//! hardcoding v12 — see `room_version.rs`.
//!
//! See `event-id-design.md` for the full flow.

use base64::Engine;
use base64::engine::general_purpose;
use ruma::canonical_json::{
    CanonicalJsonObject, CanonicalJsonValue, RedactionError, redact_in_place,
};
use ruma::{EventId, OwnedEventId, OwnedRoomId};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::room_version::RoomVersion;

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
/// Exposed for consumers (e.g. `EventBuilder`) that need to embed the b64
/// form of a content hash into wire JSON.
pub fn b64_unpadded(bytes: &[u8]) -> String {
    general_purpose::STANDARD_NO_PAD.encode(bytes)
}

/// URL-safe-alphabet base64, no padding.
///
/// Used internally for the event_id suffix (v3+):
/// `event_id = "$" + b64url_unpadded(reference_hash)`. Not exposed publicly
/// — callers should use [`event_id_from_hash`] which wraps it.
pub(crate) fn b64_url_unpadded(bytes: &[u8]) -> String {
    general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Redaction for reference-hash computation, under one room version's rules.
///
/// Runs ruma's `redact_in_place` against the version's redaction rules, but
/// preserves the version's [`extra_redaction_keys`] across the call — the keys
/// that must survive redaction even though ruma's keep-list doesn't mention
/// them. For every version this repo speaks that includes MSC4242's
/// `prev_state_events`, a state-DAG parentage field that must be covered by
/// the reference hash; a stamping [`EventIdScheme`] adds its own identity
/// fields there too. Save → redact → restore is the minimal divergence
/// pending the MSC landing in the spec.
///
/// See `event-id-design.md` §"ruma redaction wrapper".
///
/// On error the saved keys are restored before returning, so the caller sees
/// the input in its original shape regardless of outcome.
///
/// [`extra_redaction_keys`]: RoomVersion::extra_redaction_keys
pub(crate) fn redact_for_hash(
    obj: &mut CanonicalJsonObject,
    version: &RoomVersion,
) -> Result<(), RedactionError> {
    // Only the keys actually present are touched; a version with nothing to
    // preserve (or an event carrying none of them) allocates nothing.
    let saved: Vec<(&'static str, CanonicalJsonValue)> = version
        .extra_redaction_keys
        .iter()
        .filter_map(|k| obj.remove(*k).map(|v| (*k, v)))
        .collect();
    let result = redact_in_place(obj, &version.rules.redaction, None);
    for (k, v) in saved {
        obj.insert(k.to_owned(), v);
    }
    result
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
/// Runs [`redact_to_canonical_bytes`] and SHA-256s the result.
pub fn reference_hash(
    obj: &CanonicalJsonObject,
    version: &RoomVersion,
) -> Result<[u8; 32], RedactionError> {
    Ok(sha256(&redact_to_canonical_bytes(obj, version)?))
}

/// Apply the room version's redaction rules (keeping its
/// [`extra_redaction_keys`](RoomVersion::extra_redaction_keys)), strip
/// `signatures` and `unsigned`, and return the canonical-JSON bytes.
///
/// Used as an internal step of [`reference_hash`], as the byte string a
/// signature covers, and by from-wire callers that need the redacted form on
/// content-hash mismatch (Matrix S2S §"Validating hashes and signatures on
/// received events": "If the content hashes are not present, or do not match
/// the supplied content, then the receiving server must redact the event
/// before accepting it").
///
/// Returns `RedactionError` only if the input violates ruma's redaction
/// preconditions (non-object `content`/`hashes`/`signatures`, missing `type`).
pub fn redact_to_canonical_bytes(
    obj: &CanonicalJsonObject,
    version: &RoomVersion,
) -> Result<Vec<u8>, RedactionError> {
    let mut clone = obj.clone();
    redact_for_hash(&mut clone, version)?;
    clone.remove("signatures");
    clone.remove("unsigned");
    Ok(canonical(&clone))
}

/// The three states an inbound event's content hash can be in. `Absent` is
/// distinct from `Mismatch` because a trusted-network deployment omits
/// `hashes` entirely (it is dead weight without signatures to anchor it), so
/// "no hash" and "wrong hash" call for different receipt handling — see
/// [`from_wire`](crate::event_builder::from_wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentHashCheck {
    /// No `hashes` field on the event at all.
    Absent,
    /// `hashes.sha256` is present and equals the recomputed content hash.
    Matches,
    /// `hashes` is present but its `sha256` entry is missing, not a string, or
    /// does not equal the recomputed content hash.
    Mismatch,
}

/// Check an event's content hash against its `hashes.sha256` field.
///
/// An event carrying `hashes` at all is held to it: a missing / non-string /
/// unequal `sha256` is a [`ContentHashCheck::Mismatch`], not an
/// [`Absent`](ContentHashCheck::Absent).
///
/// Spec: <https://spec.matrix.org/v1.18/server-server-api/#calculating-the-content-hash-for-an-event>.
pub fn check_content_hash(obj: &CanonicalJsonObject) -> ContentHashCheck {
    let Some(hashes) = obj.get("hashes") else {
        return ContentHashCheck::Absent;
    };
    let CanonicalJsonValue::Object(hashes) = hashes else {
        return ContentHashCheck::Mismatch;
    };
    let Some(CanonicalJsonValue::String(expected)) = hashes.get("sha256") else {
        return ContentHashCheck::Mismatch;
    };
    if *expected == b64_unpadded(&content_hash(obj)) {
        ContentHashCheck::Matches
    } else {
        ContentHashCheck::Mismatch
    }
}

/// Format an event_id from a reference hash. v3+ rooms only.
///
/// `event_id = "$" + url-safe-unpadded-base64(reference_hash)`.
pub fn event_id_from_hash(hash: &[u8; 32]) -> OwnedEventId {
    let s = format!("${}", b64_url_unpadded(hash));
    // 43 url-safe-b64 chars after `$` is always a syntactically valid event_id.
    OwnedEventId::try_from(s).expect("'$' + 43 url-safe-b64 chars is a valid event_id")
}

/// **Test fixtures only**: an event's id under the base room version, from its
/// canonical wire bytes.
///
/// Production code cannot name an event this way, and is not able to reach this
/// function: an event is named under the version of the room it belongs to
/// ([`RoomVersion::event_id`]), which raw bytes alone cannot say. It exists for
/// fixtures that hand-assemble an [`Event`](crate::Event) instead of going
/// through [`EventBuilder`](crate::event_builder::EventBuilder) and still want
/// an id that matches their bytes, so it is gated behind the `test-support`
/// feature (enabled as a dev-dependency by the crates whose tests need it).
#[cfg(any(test, feature = "test-support"))]
pub fn base_version_event_id(
    raw: &serde_json::value::RawValue,
) -> Result<OwnedEventId, EventIdError> {
    let parsed: CanonicalJsonValue = serde_json::from_str(raw.get())
        .map_err(|e| EventIdError::Scheme(format!("raw is not valid JSON: {e}")))?;
    let CanonicalJsonValue::Object(obj) = parsed else {
        return Err(EventIdError::Scheme(
            "raw JSON root is not an object".to_owned(),
        ));
    };
    crate::room_version::base_version().event_id(&obj)
}

/// How event ids are derived from event bytes.
///
/// v12 mandates the reference hash ([`ReferenceHashIds`], the base version's
/// scheme and the only one in this repository). The trait exists so another
/// room version can name events more cheaply — which version a room uses is
/// agreed mesh-wide because it rides in the create event, so this is a
/// [`RoomVersion`] field and never a per-deployment setting.
///
/// The load-bearing invariant is that an id stays a pure function of the event's
/// canonical bytes: nothing transmits an id, every receiver recomputes the same
/// one, and no side table exists. That is why the outbound and inbound halves
/// are split — [`stamp`](Self::stamp) may *add* the inputs an id is derived
/// from, but only [`derive`](Self::derive) turns bytes into an id, and both the
/// authoring and the receiving side call it.
pub trait EventIdScheme: Send + Sync + std::fmt::Debug {
    /// Outbound only: stamp any identity-bearing fields this scheme needs into
    /// `obj`. Runs *before* the content hash and the signature are computed, so
    /// both cover whatever is inserted. Default: insert nothing.
    fn stamp(&self, obj: &mut CanonicalJsonObject) -> Result<(), EventIdError> {
        let _ = obj;
        Ok(())
    }

    /// The event's id, from its canonical object alone. Pure — no side effects,
    /// no I/O, no dependence on room state or on what the receiver has seen.
    /// Reached through [`RoomVersion::event_id`], which is what supplies
    /// `version`: a scheme that reads the version's redaction rules (the
    /// reference hash does) gets the rules of the room the event is *in*, not
    /// of whichever version happens to hold this scheme.
    ///
    /// Three further obligations the reference hash meets for free, and which
    /// an implementor MUST uphold because subsystems depend on them:
    ///
    /// - **Redaction-invariant.** Receipt-check 3 redacts an event whose
    ///   content hash does not match and carries on with it, so the id must be
    ///   the same before and after. This is what the version's
    ///   [`extra_redaction_keys`](RoomVersion::extra_redaction_keys) exist to
    ///   guarantee: whatever `derive` reads must be listed there, or redaction
    ///   strips it.
    /// - **Signature-invariant.** A resident co-signing a join/leave/invite
    ///   must not rename the event, so `derive` must ignore `signatures`
    ///   (and `unsigned`, which is likewise not covered).
    /// - **Collision-free.** Two distinct events must never derive the same id.
    ///   Stores dedupe by id, so a reused id is a silent substitution: the
    ///   second event is discarded as a duplicate of the first. `invite`'s
    ///   response check also accepts the invitee's event on id equality alone.
    ///   A counter-based scheme satisfies this only if a counter value is never
    ///   reused for the life of the store.
    fn derive(
        &self,
        obj: &CanonicalJsonObject,
        version: &RoomVersion,
    ) -> Result<OwnedEventId, EventIdError>;
}

/// Failure modes for an [`EventIdScheme`].
#[derive(Debug, Error)]
pub enum EventIdError {
    /// The object violates ruma's redaction preconditions (missing `type`,
    /// non-object `content`/`hashes`/`signatures`) — only the reference-hash
    /// scheme can raise this.
    #[error("redaction precondition failed: {0}")]
    Redaction(RedactionError),

    /// The scheme's own inputs are missing or malformed on this event.
    #[error("event id scheme: {0}")]
    Scheme(String),
}

/// The v12 scheme: `event_id = "$" + b64url(reference_hash(event))`.
///
/// Stamps nothing — the id is already a function of the bytes as they stand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReferenceHashIds;

impl EventIdScheme for ReferenceHashIds {
    fn derive(
        &self,
        obj: &CanonicalJsonObject,
        version: &RoomVersion,
    ) -> Result<OwnedEventId, EventIdError> {
        let rh = reference_hash(obj, version).map_err(EventIdError::Redaction)?;
        Ok(event_id_from_hash(&rh))
    }
}

/// Derive a room_id from a create event's event_id.
///
/// Room v12 uses `RoomIdFormatVersion::V2`: the room_id is the create event's
/// event_id with the leading `$` swapped to `!`. The suffix is identical
/// (43 url-safe-b64 chars). Spec: <https://spec.matrix.org/v1.18/rooms/v12/#room-ids>.
pub fn room_id_from_create(create_event_id: &EventId) -> OwnedRoomId {
    let suffix = create_event_id
        .as_str()
        .strip_prefix('$')
        .expect("EventId by construction starts with '$'");
    OwnedRoomId::try_from(format!("!{suffix}"))
        .expect("'!' + valid event_id suffix is a valid room_id under format v2")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::room_version::base_version;
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
        redact_for_hash(&mut o, base_version()).expect("redaction succeeds");

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
        redact_for_hash(&mut o, base_version()).expect("redaction succeeds");
        assert!(!o.contains_key("prev_state_events"));
    }

    // ---- public spec functions: content_hash, reference_hash, event_id_from_hash, room_id_from_create ----

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
        let h_base = reference_hash(&base, base_version()).expect("redacts");

        let with_strippable = obj(json!({
            "type": "m.room.message", "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": [], "auth_events": [],
            "unsigned": { "age": 42 },
            "signatures": { "d": { "ed25519:a": "sig" } }
        }));
        assert_eq!(
            h_base,
            reference_hash(&with_strippable, base_version()).expect("redacts")
        );
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
            reference_hash(&a, base_version()).expect("redacts"),
            reference_hash(&b, base_version()).expect("redacts"),
        );
    }

    #[test]
    fn reference_hash_propagates_redaction_error_on_missing_type() {
        // ruma's `redact_in_place` returns `MissingField { path: "type" }` when
        // the input lacks a `type` field. Pins that the `?` in `reference_hash`
        // surfaces the error rather than panicking or masking it.
        let o = obj(json!({
            "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1,
            "prev_events": []
        }));
        let err =
            reference_hash(&o, base_version()).expect_err("missing `type` must trip redaction");
        assert!(
            matches!(err, RedactionError::MissingField { ref path } if path == "type"),
            "expected MissingField {{ path: \"type\" }}, got {err:?}",
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
        let r = reference_hash(&o, base_version()).expect("redacts");
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

    /// A pluggable [`EventIdScheme`] can mint ids that are neither 43 chars
    /// nor base64, and `room_id_from_create` turns any of them into a room id
    /// by sigil swap — so if ruma rejected such a shape, the swap would panic
    /// on every create. Uses a short id containing `_`, the character most
    /// likely to be refused by a stricter parser.
    #[test]
    fn short_non_base64_id_parses_as_event_id_and_room_id() {
        let id = OwnedEventId::try_from("$1700000000000_3").expect("short event id parses");
        assert_eq!(id.as_str(), "$1700000000000_3");
        let room_id = room_id_from_create(&id);
        assert_eq!(room_id.as_str(), "!1700000000000_3");
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

    /// Hand-authored v12 reference-hash vector with independent verification.
    ///
    /// **Keep this test even though `base_version_event_id_matches_real_matrix_org_event`
    /// and `base_version_event_id_matches_real_msc4242_event` below also exercise
    /// the same pipeline against real homeserver outputs.** The hand-traced
    /// `EXPECTED_POST_REDACTION` literal is the only place where the
    /// post-redaction canonical bytes are spelled out in human-readable form,
    /// which makes it auditable: when a regression splits between "redaction
    /// changed shape" vs "sha256/canonical/b64 changed", the dual assertion
    /// (`reference_hash == sha256(EXPECTED)` AND `id == "$pinned"`) tells you
    /// which layer broke. The real-event vectors are end-to-end black-box
    /// checks; this one is a glass-box check.
    ///
    /// The spec appendix only carries v1-shaped content-hash vectors (already
    /// pinned by `content_hash_spec_vector_*` above). There is no published
    /// v12 reference-hash vector, so we author one and cross-check it by:
    ///
    /// 1. Constructing an input event (`INPUT`).
    /// 2. Manually computing what V11 redaction + MSC4242 carve-out + strip
    ///    `signatures`/`unsigned` should leave behind (`EXPECTED_POST_REDACTION`).
    /// 3. Asserting `reference_hash(INPUT) == sha256(EXPECTED_POST_REDACTION)`.
    ///    The right-hand side runs SHA-256 (FIPS-verified by `sha256_known_vector`)
    ///    against bytes a human can read — if our redaction step diverges, this
    ///    assertion catches it.
    /// 4. Additionally pinning `event_id_from_hash(reference_hash(INPUT))` so
    ///    a regression in `b64_url_unpadded` or the event_id format would also
    ///    fail this test.
    ///
    /// For `m.room.message` events the V11 content keep-list is empty, so
    /// `content` collapses to `{}`. `unsigned` is not in the top-level keep-list
    /// → stripped by redaction. `signatures` IS in the keep-list but we strip
    /// it after redaction (spec step). `prev_state_events` is preserved by our
    /// MSC4242 wrapper. Final keys, sorted alphabetically:
    /// `content, hashes, origin_server_ts, prev_events, prev_state_events,
    ///  room_id, sender, type`.
    #[test]
    fn reference_hash_v12_authored_vector() {
        // The input event — every field a v12 m.room.message can carry.
        let input = obj(json!({
            "type": "m.room.message",
            "sender": "@alice:example.org",
            "room_id": "!room:example.org",
            "content": { "msgtype": "m.text", "body": "hello" },
            "prev_events": ["$prev:example.org"],
            "prev_state_events": ["$ps:example.org"],
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "Y29udGVudGhhc2g" },
            "unsigned": { "age": 42 },
            "signatures": {}
        }));

        // What our pipeline (redact_for_hash → strip signatures → strip
        // unsigned → canonical-encode) MUST produce. Keys sorted; content
        // emptied (no keep-list entries for m.room.message); unsigned and
        // signatures removed; everything else preserved (prev_state_events
        // is the MSC4242 carve-out — V11 alone would have stripped it).
        const EXPECTED_POST_REDACTION: &[u8] = br#"{"content":{},"hashes":{"sha256":"Y29udGVudGhhc2g"},"origin_server_ts":1700000000000,"prev_events":["$prev:example.org"],"prev_state_events":["$ps:example.org"],"room_id":"!room:example.org","sender":"@alice:example.org","type":"m.room.message"}"#;

        let h = reference_hash(&input, base_version()).expect("redacts");

        // Independent cross-check: SHA-256 over the human-readable expected
        // bytes must equal our reference_hash output. If our redaction step
        // produces different bytes, this will fail.
        assert_eq!(
            h,
            sha256(EXPECTED_POST_REDACTION),
            "reference_hash diverges from sha256(hand-traced redaction bytes)"
        );

        // Pinned event_id — guards against regressions in `b64_url_unpadded`
        // or the `$<43 chars>` format. Recorded from the verified hash above.
        let id = event_id_from_hash(&h);
        assert_eq!(id.as_str(), "$mY2a13t3rnoKFepL_yWIHDCPjw7WoP1Rem5QJyvom9w",);
    }

    /// Cross-check against a real matrix.org-produced event_id.
    /// Pre-MSC4242 event with `auth_events` on the wire
    /// (not `prev_state_events`), so our redaction wrapper's save/restore
    /// is a no-op and the result must match stock V11 behaviour byte-for-byte.
    /// If this drifts, either ruma's `redact_in_place` changed or our
    /// canonical encoding broke — both would be regressions.
    #[test]
    fn base_version_event_id_matches_real_matrix_org_event() {
        let raw_json = r#"{
  "auth_events": [
    "$Fw7pQdLu79h74bsZabn1UKXoXo7-q5M-cOwQxQxfh2c",
    "$WadCIT8wxAK3K7zCT9OmewBHyQFIzTRLo15lobAE3zE",
    "$7qryV2SHr6Vb7ztIf20gqFyCKWD6A7faRdHQnJaeXGc"
  ],
  "content": {
    "body": "ping",
    "m.mentions": {},
    "msgtype": "m.text"
  },
  "depth": 8,
  "hashes": {
    "sha256": "ASg2wblVle+n4idsXYALIQuBTk6I99UtfeOsvsMZX0I"
  },
  "origin_server_ts": 1779866621967,
  "prev_events": [
    "$7z8Yl5LzVNoP6iqWr580M0fv9-ZV8nE73ojfFEKATJc"
  ],
  "room_id": "!ySniwzsmihjTTwbBtv:matrix.org",
  "sender": "@kegan:matrix.org",
  "type": "m.room.message",
  "signatures": {
    "matrix.org": {
      "ed25519:a_RXGa": "zr5GHROMC0lSeRnBonZE1nFC1dqJLwe1IKxcOU66cuQuJaIH6KZCCwMAz6IqgXUQz3hX4FOmzem13vo3sz6WDg"
    }
  },
  "unsigned": {
    "age_ts": 1779866621967
  }
}"#;
        let raw = serde_json::value::RawValue::from_string(raw_json.to_owned()).expect("raw");
        let id = base_version_event_id(&raw).expect("computes");
        assert_eq!(id.as_str(), "$KXQOIuyr9pVHI6YAqMtwYCJbeh8-KtZbl8XCDHA53qY");
    }

    /// Cross-check against a real MSC4242 / v12 event_id. This event has
    /// `prev_state_events` (not `auth_events`)
    /// on the wire and a v12-format room_id (`!` + 43 url-safe-b64 chars),
    /// so it exercises **the MSC4242 carve-out path itself**: without our
    /// save/restore of `prev_state_events` across V11 redaction, the field
    /// would be stripped and the resulting event_id would differ.
    /// Complements `base_version_event_id_matches_real_matrix_org_event` which
    /// only exercised stock V11 redaction.
    #[test]
    fn base_version_event_id_matches_real_msc4242_event() {
        let raw_json = r#"{
  "prev_events": [
    "$zo_-jrWvI_eBqBktaYF2uIZ4pS2hngkQ4J9wWm37w0g"
  ],
  "type": "m.room.power_levels",
  "sender": "@alice2:localhost",
  "content": {
    "ban": 50,
    "events": {
      "m.room.avatar": 50,
      "m.room.canonical_alias": 50,
      "m.room.encryption": 100,
      "m.room.history_visibility": 100,
      "m.room.name": 50,
      "m.room.power_levels": 100,
      "m.room.server_acl": 100,
      "m.room.tombstone": 150
    },
    "events_default": 0,
    "historical": 100,
    "invite": 50,
    "kick": 50,
    "m.call.invite": 50,
    "redact": 50,
    "state_default": 50,
    "users": {},
    "users_default": 0
  },
  "depth": 3,
  "prev_state_events": [
    "$zo_-jrWvI_eBqBktaYF2uIZ4pS2hngkQ4J9wWm37w0g"
  ],
  "room_id": "!KlUQEifCY1P4t_lJDp5enSu82bnnQWvjZXGXsv9FA_4",
  "state_key": "",
  "origin_server_ts": 1779867947339,
  "hashes": {
    "sha256": "nRF+dz1Qn0cLXL9GLjTEyR6BbsoOtx01LIYi9AUld1A"
  },
  "signatures": {
    "localhost": {
      "ed25519:a_vQAB": "NHb3pa0sPIxe8uEIxmrApZgj2rBCRwTDGAfmsnQ6IUq8jZBirpNo+BpMO+jC4geK89GJ1/yqz1z44rlLlVjpAg"
    }
  },
  "unsigned": {
    "age_ts": 1779867947339
  }
}"#;
        let raw = serde_json::value::RawValue::from_string(raw_json.to_owned()).expect("raw");
        let id = base_version_event_id(&raw).expect("computes");
        assert_eq!(id.as_str(), "$B551KEsRXrNE3knHLSP-QszuqJYSjasJECVcmP1JIkI");
    }
}
