//! Server-authored event construction and inbound-wire parsing.
//!
//! Two public entry points share the same downstream pipeline:
//!
//! - [`EventBuilder::build`] assembles a v12 / MSC4242 PDU, computes the
//!   content hash, inserts it into the event, computes the reference hash,
//!   derives the event_id, and (for `m.room.create`) lets [`parse_event`]
//!   derive the room_id from the event_id. It runs `parse_event` (wire
//!   format) and then `validate_pdu` (semantic rules) as final
//!   defence-in-depth checks so the bytes we just produced are guaranteed
//!   to round-trip through both validators.
//!
//! - [`from_wire`] is the inbound counterpart: it reads the canonical bytes
//!   as the source of truth, computes the reference hash to derive the
//!   event_id, then runs the same `parse_event` + `validate_pdu` pair.
//!
//! See `event-id-design.md` §"Updated `EventBuilder`".

use std::time::{SystemTime, UNIX_EPOCH};

use crate::event_id::{
    b64_unpadded, content_hash, event_id_from_hash, redact_to_canonical_bytes, reference_hash,
    verify_content_hash,
};
use ruma::canonical_json::{CanonicalJsonObject, CanonicalJsonValue, try_from_json_map};
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde::Serialize;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value};

use crate::validate::{parse_event, validate_pdu};
use crate::{Event, FormatError};

/// Builder for server-authored Matrix v12 PDUs.
///
/// `new` takes the two strictly-required fields (`sender`, `type`); everything
/// else has a sensible default applied at `build()` time. Setters consume and
/// return `Self` for chaining.
///
/// **For `m.room.create` events**: do not set `room_id` (it's derived from
/// the computed event_id post-hash). The `state_key` must be `""`.
///
/// **For all other events**: `room_id` must be set, otherwise `build()`
/// returns `FormatError::MissingField("room_id")`.
#[derive(Debug, Clone)]
pub struct EventBuilder {
    sender: OwnedUserId,
    event_type: String,
    state_key: Option<String>,
    content: Value,
    room_id: Option<OwnedRoomId>,
    prev_events: Vec<OwnedEventId>,
    prev_state_events: Vec<OwnedEventId>,
    auth_events: Vec<OwnedEventId>,
    origin_server_ts: Option<u64>,
    unsigned: Option<Value>,
}

impl EventBuilder {
    /// Start a new builder. Defaults: `content` = `{}`, `origin_server_ts` =
    /// `now_ms()` (applied at `build()` time), all parent lists empty,
    /// `state_key` / `room_id` / `unsigned` absent.
    pub fn new(sender: OwnedUserId, event_type: String) -> Self {
        Self {
            sender,
            event_type,
            state_key: None,
            content: Value::Object(Map::new()),
            room_id: None,
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            auth_events: Vec::new(),
            origin_server_ts: None,
            unsigned: None,
        }
    }

    pub fn state_key(mut self, state_key: String) -> Self {
        self.state_key = Some(state_key);
        self
    }

    /// Set the event `content`. Must serialise to a JSON object — non-object
    /// content is rejected at `build()` time with `InvalidFieldType`.
    pub fn content<T: Serialize>(mut self, content: T) -> Self {
        // A failed `to_value` here turns into a non-object Value which is
        // caught by `build()` rather than producing a spurious error from a
        // setter that the caller can't react to.
        self.content = serde_json::to_value(content).unwrap_or(Value::Null);
        self
    }

    pub fn room_id(mut self, room_id: OwnedRoomId) -> Self {
        self.room_id = Some(room_id);
        self
    }

    pub fn prev_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.prev_events = ids;
        self
    }

    pub fn prev_state_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.prev_state_events = ids;
        self
    }

    /// Set the server-calculated `auth_events` list (MSC4242: not on the
    /// wire). The caller computes this against state-before-event via
    /// `auth_events::calculate_auth_events`.
    pub fn auth_events(mut self, ids: Vec<OwnedEventId>) -> Self {
        self.auth_events = ids;
        self
    }

    pub fn origin_server_ts(mut self, ts: u64) -> Self {
        self.origin_server_ts = Some(ts);
        self
    }

    pub fn unsigned<T: Serialize>(mut self, unsigned: T) -> Self {
        self.unsigned = Some(serde_json::to_value(unsigned).unwrap_or(Value::Null));
        self
    }

    pub fn build(self) -> Result<Event, FormatError> {
        let is_create = self.event_type == "m.room.create";

        // Skeleton: non-create needs a room_id. (Create derives its own from
        // the event_id post-hash; any `.room_id(...)` on a create builder is
        // ignored and would in any case be rejected by `parse_event` if we
        // tried to inject it.)
        if !is_create && self.room_id.is_none() {
            return Err(FormatError::MissingField("room_id"));
        }
        // Skeleton: content must be a JSON object (v12 PDU schema).
        if !self.content.is_object() {
            return Err(FormatError::InvalidFieldType {
                field: "content",
                expected: "object",
            });
        }
        // Skeleton: unsigned, if set, must be a JSON object.
        if let Some(u) = &self.unsigned
            && !u.is_object()
        {
            return Err(FormatError::InvalidFieldType {
                field: "unsigned",
                expected: "object",
            });
        }

        // Assemble the unhashed JSON map: type, sender, content, prev_events,
        // prev_state_events, origin_server_ts, [state_key], [room_id],
        // [unsigned]. `room_id` is omitted iff this is a create event;
        // `auth_events` is struct-only and never appears in the raw (MSC4242).
        let mut map = Map::new();
        map.insert("type".to_owned(), Value::String(self.event_type.clone()));
        map.insert(
            "sender".to_owned(),
            Value::String(self.sender.as_str().to_owned()),
        );
        map.insert("content".to_owned(), self.content);
        map.insert(
            "prev_events".to_owned(),
            Value::Array(
                self.prev_events
                    .iter()
                    .map(|e| Value::String(e.as_str().to_owned()))
                    .collect(),
            ),
        );
        map.insert(
            "prev_state_events".to_owned(),
            Value::Array(
                self.prev_state_events
                    .iter()
                    .map(|e| Value::String(e.as_str().to_owned()))
                    .collect(),
            ),
        );
        let origin_server_ts = self.origin_server_ts.unwrap_or_else(now_ms);
        map.insert("origin_server_ts".to_owned(), Value::from(origin_server_ts));
        if let Some(sk) = &self.state_key {
            map.insert("state_key".to_owned(), Value::String(sk.clone()));
        }
        if !is_create {
            let rid = self.room_id.as_ref().expect("room_id checked above");
            map.insert("room_id".to_owned(), Value::String(rid.as_str().to_owned()));
        }
        if let Some(u) = self.unsigned {
            map.insert("unsigned".to_owned(), u);
        }

        // Convert to canonical-JSON. Surfaces float-in-content, out-of-range
        // integers, duplicate keys (impossible from serde Map but the API
        // exposes them) — anything that can't round-trip canonical JSON.
        let mut canon: CanonicalJsonObject =
            try_from_json_map(map).map_err(FormatError::NonCanonical)?;

        // Content hash → `hashes.sha256` (canonical-base64 standard alphabet
        // per spec). Order: content hash, insert, then reference hash, so the
        // reference hash covers the inserted content hash.
        let ch = content_hash(&canon);
        let mut hashes = CanonicalJsonObject::new();
        hashes.insert(
            "sha256".to_owned(),
            CanonicalJsonValue::String(b64_unpadded(&ch)),
        );
        canon.insert("hashes".to_owned(), CanonicalJsonValue::Object(hashes));

        // Reference hash → event_id. By construction `canon` has a string
        // `type`, an object `content`, and an object `hashes`, so ruma's
        // redaction preconditions all hold — `reference_hash` cannot fail.
        let rh = reference_hash(&canon)
            .expect("builder-assembled object satisfies ruma redaction preconditions");
        let event_id = event_id_from_hash(&rh);

        let raw = serialise_canonical(&canon);

        // Defence-in-depth: round-trip through the wire-format validator
        // (parse_event) then the semantic validator (validate_pdu). For
        // create events, parse_event derives room_id from event_id (sigil
        // swap), so we don't need to do that here.
        //
        // Failures here are caller bugs (not builder bugs): the builder
        // doesn't validate every field validate_pdu does — content shape
        // for `m.room.create` / `m.room.member` / `m.room.power_levels` is
        // rule-checked, count limits on `prev_events` /
        // `prev_state_events`, rule 9 (`@`-prefixed state_key vs sender)
        // etc. Bubble the `FormatError` up so the caller sees the specific
        // reason rather than a panic from inside the builder.
        let event = parse_event(raw, event_id, self.auth_events)?;
        validate_pdu(&event)?;
        Ok(event)
    }
}

/// Parse + event-id-derive an inbound wire event.
///
/// Reads `raw` as the source of truth, computes its reference hash to derive
/// the event_id, verifies the content hash, then runs [`parse_event`] for the
/// structured fields (and the create-event room_id derivation). `auth_events`
/// are supplied by the caller because MSC4242 removes them from the wire.
///
/// **Content hash verification (Matrix S2S §"Validating hashes and signatures
/// on received events")**: if `hashes.sha256` is absent or doesn't match the
/// recomputed content hash, the event is redacted before being accepted —
/// `raw` is replaced with the canonical redacted form and that's what
/// `parse_event` sees. The event_id is unaffected (it's already computed
/// over the redacted form). The receiving server is expected to accept the
/// redacted version rather than drop the event entirely.
///
/// Errors:
/// - `raw` is not a JSON object (`InvalidFieldType { field: "<root>" }`).
/// - The object lacks `type` or has malformed `content`/`hashes`/`signatures`
///   shape (mapped via [`ref_hash_error_to_format_error`]).
/// - Any downstream `parse_event` rejection.
///
/// Performance note: this parses `raw` twice — once here to a
/// `CanonicalJsonValue` for hash computation, then again in `parse_event`.
/// Acceptable for the federation receive path's current scale.
pub fn from_wire(raw: Box<RawValue>, auth_events: Vec<OwnedEventId>) -> Result<Event, FormatError> {
    let parsed: CanonicalJsonValue = serde_json::from_str(raw.get())?;
    let CanonicalJsonValue::Object(obj) = parsed else {
        return Err(FormatError::InvalidFieldType {
            field: "<root>",
            expected: "object",
        });
    };
    let rh = reference_hash(&obj).map_err(ref_hash_error_to_format_error)?;
    let event_id = event_id_from_hash(&rh);

    // Replace raw with the canonical redacted form on content-hash mismatch.
    // `reference_hash` already exercised the same redaction step successfully
    // above, so `redact_to_canonical_bytes` here cannot fail.
    let raw_to_parse = if verify_content_hash(&obj) {
        raw
    } else {
        let bytes =
            redact_to_canonical_bytes(&obj).expect("redaction succeeded above for reference_hash");
        let s = String::from_utf8(bytes).expect("canonical JSON is valid UTF-8");
        RawValue::from_string(s).expect("canonical JSON parses as a RawValue")
    };
    let event = parse_event(raw_to_parse, event_id, auth_events)?;
    validate_pdu(&event)?;
    Ok(event)
}

fn ref_hash_error_to_format_error(err: ruma::canonical_json::RedactionError) -> FormatError {
    use ruma::canonical_json::RedactionError;
    match err {
        RedactionError::MissingField { path } if path == "type" => {
            // ruma's only documented `MissingField` here is the top-level
            // `type` field. Map to FormatError::MissingField for parity with
            // parse_event's vocabulary.
            FormatError::MissingField("type")
        }
        RedactionError::InvalidType { path, .. } => {
            // ruma's path values for redaction preconditions are one of
            // "type" / "content" / "hashes" / "signatures". Map to
            // InvalidFieldType with a static field name.
            let field: &'static str = match path.as_str() {
                "type" => "type",
                "content" => "content",
                "hashes" => "hashes",
                "signatures" => "signatures",
                _ => "<unknown>",
            };
            FormatError::InvalidFieldType {
                field,
                expected: "object",
            }
        }
        // `RedactionError` is `#[non_exhaustive]`; any future variant lands
        // here as a generic wire-malformed signal.
        _ => FormatError::InvalidFieldType {
            field: "<root>",
            expected: "well-formed v12 PDU",
        },
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn serialise_canonical(obj: &CanonicalJsonObject) -> Box<RawValue> {
    to_raw_value(obj).expect("CanonicalJsonObject is always serialisable to RawValue")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user(s: &str) -> OwnedUserId {
        s.parse().expect("user id")
    }

    fn room(s: &str) -> OwnedRoomId {
        s.parse().expect("room id")
    }

    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("event id")
    }

    // ---------- happy path ----------

    #[test]
    fn build_create_event_derives_room_id_from_event_id() {
        let ev = EventBuilder::new(user("@alice:example.org"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
            .origin_server_ts(1_700_000_000_000)
            .build()
            .expect("create event builds");

        // event_id and room_id share their suffix (43 url-safe-b64 chars).
        assert!(ev.event_id.as_str().starts_with('$'));
        assert!(ev.room_id.as_str().starts_with('!'));
        assert_eq!(&ev.event_id.as_str()[1..], &ev.room_id.as_str()[1..]);
        // room_id is NOT in raw (v12 spec: create events omit room_id on the wire).
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert!(raw.get("room_id").is_none());
        // hashes.sha256 is present, standard-alphabet base64 (no `-`/`_`), no padding.
        // 32-byte sha256 → 43 unpadded standard-b64 chars.
        let hash_str = raw["hashes"]["sha256"].as_str().expect("sha256 string");
        assert_eq!(hash_str.len(), 43);
        assert!(
            !hash_str.contains(['-', '_']),
            "hashes.sha256 must use STANDARD b64 alphabet (`+`/`/`), not url-safe (`-`/`_`): {hash_str}"
        );
    }

    /// Wire bytes never carry `event_id` — it's the reference hash, computed
    /// post-canonicalisation and stored only on the `Event` struct as a
    /// sidecar field. The whole `event_view` enrichment pipeline rests on
    /// this invariant; a regression that serialised `event_id` back into
    /// `raw` would let stale ids slip through with no defence beyond a
    /// `Map::insert` overwrite. Pin both arms (create + message).
    #[test]
    fn build_output_raw_lacks_event_id() {
        let create = EventBuilder::new(user("@alice:example.org"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
            .origin_server_ts(1_700_000_000_000)
            .build()
            .expect("create event builds");
        let create_raw: serde_json::Value = serde_json::from_str(create.raw.get()).unwrap();
        assert!(
            create_raw.get("event_id").is_none(),
            "create event wire bytes must not carry event_id: {create_raw}",
        );

        let msg = EventBuilder::new(user("@alice:example.org"), "m.room.message".to_owned())
            .room_id(create.room_id.clone())
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .prev_events(vec![create.event_id.clone()])
            .origin_server_ts(1_700_000_000_001)
            .build()
            .expect("message event builds");
        let msg_raw: serde_json::Value = serde_json::from_str(msg.raw.get()).unwrap();
        assert!(
            msg_raw.get("event_id").is_none(),
            "non-create event wire bytes must not carry event_id either: {msg_raw}",
        );
    }

    #[test]
    fn build_message_event_round_trips() {
        let create = EventBuilder::new(user("@alice:example.org"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
            .build()
            .expect("create");
        let msg = EventBuilder::new(user("@alice:example.org"), "m.room.message".to_owned())
            .room_id(create.room_id.clone())
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .prev_events(vec![create.event_id.clone()])
            .origin_server_ts(1_700_000_000_001)
            .build()
            .expect("message");

        // event_id format check.
        assert!(msg.event_id.as_str().starts_with('$'));
        assert_eq!(msg.event_id.as_str().len(), 44);
        // room_id matches what the caller passed in.
        assert_eq!(msg.room_id, create.room_id);
        // raw contains the room_id (non-create events keep it on the wire).
        let raw: serde_json::Value = serde_json::from_str(msg.raw.get()).unwrap();
        assert_eq!(raw["room_id"].as_str(), Some(create.room_id.as_str()));
    }

    #[test]
    fn build_is_deterministic_for_identical_inputs() {
        // Same inputs (same ts, same fields) must produce the same event_id —
        // the hash is a pure function of the wire bytes.
        let mk = || {
            EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
                .room_id(room("!r:d"))
                .content(json!({ "body": "x" }))
                .origin_server_ts(1)
                .build()
                .expect("builds")
        };
        assert_eq!(mk().event_id, mk().event_id);
    }

    #[test]
    fn build_diverges_on_different_content() {
        let a = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "body": "a" }))
            .origin_server_ts(1)
            .build()
            .expect("a");
        let b = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "body": "b" }))
            .origin_server_ts(1)
            .build()
            .expect("b");
        assert_ne!(a.event_id, b.event_id);
    }

    #[test]
    fn build_includes_prev_state_events_in_raw_and_struct() {
        // MSC4242: `prev_state_events` is a top-level wire field carried into
        // the reference hash via the carve-out. Two builds differing only on
        // `prev_state_events` must produce different event_ids, and the
        // `prev_state_events` list must round-trip onto both `raw` and the
        // `Event` struct.
        let ps = vec![eid("$ps1:d"), eid("$ps2:d")];
        let ev = EventBuilder::new(user("@a:d"), "m.room.member".to_owned())
            .room_id(room("!r:d"))
            .state_key("@a:d".to_owned())
            .content(json!({ "membership": "join" }))
            .prev_state_events(ps.clone())
            .origin_server_ts(1)
            .build()
            .expect("builds");
        assert_eq!(ev.prev_state_events, ps);
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        let raw_ps: Vec<&str> = raw["prev_state_events"]
            .as_array()
            .expect("array")
            .iter()
            .map(|v| v.as_str().expect("string"))
            .collect();
        assert_eq!(raw_ps, vec!["$ps1:d", "$ps2:d"]);

        // Differential coverage: changing prev_state_events changes event_id.
        let other = EventBuilder::new(user("@a:d"), "m.room.member".to_owned())
            .room_id(room("!r:d"))
            .state_key("@a:d".to_owned())
            .content(json!({ "membership": "join" }))
            .prev_state_events(vec![eid("$other:d")])
            .origin_server_ts(1)
            .build()
            .expect("builds");
        assert_ne!(ev.event_id, other.event_id);
    }

    #[test]
    fn build_includes_unsigned_object_in_raw() {
        // Happy-path complement to `build_rejects_non_object_unsigned`. The
        // `unsigned` field is the sliding-sync invite-state carrier and must
        // round-trip onto `raw` when set.
        let ev = EventBuilder::new(user("@a:d"), "m.room.member".to_owned())
            .room_id(room("!r:d"))
            .state_key("@b:d".to_owned())
            .content(json!({ "membership": "invite" }))
            .unsigned(json!({ "invite_room_state": [] }))
            .origin_server_ts(1)
            .build()
            .expect("builds");
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert_eq!(
            raw["unsigned"]["invite_room_state"]
                .as_array()
                .map(|a| a.len()),
            Some(0)
        );
    }

    #[test]
    fn build_attaches_caller_supplied_auth_events_to_struct_only() {
        let auth_ids = vec![eid("$auth1:d"), eid("$auth2:d")];
        let ev = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({}))
            .auth_events(auth_ids.clone())
            .origin_server_ts(1)
            .build()
            .expect("builds");
        assert_eq!(ev.auth_events, auth_ids);
        // Wire bytes must NOT contain auth_events (MSC4242).
        let raw: serde_json::Value = serde_json::from_str(ev.raw.get()).unwrap();
        assert!(raw.get("auth_events").is_none());
    }

    // ---------- error paths ----------

    #[test]
    fn build_rejects_non_create_without_room_id() {
        let err = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .content(json!({}))
            .origin_server_ts(1)
            .build()
            .expect_err("missing room_id");
        assert!(matches!(err, FormatError::MissingField("room_id")));
    }

    #[test]
    fn build_rejects_non_object_content() {
        let err = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content("not an object")
            .origin_server_ts(1)
            .build()
            .expect_err("non-object content");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "content",
                expected: "object"
            }
        ));
    }

    #[test]
    fn build_rejects_non_object_unsigned() {
        let err = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({}))
            .unsigned("oops")
            .origin_server_ts(1)
            .build()
            .expect_err("non-object unsigned");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "unsigned",
                expected: "object"
            }
        ));
    }

    #[test]
    fn build_surfaces_parse_event_error_when_caller_violates_rule_1_1() {
        // The builder doesn't reject every shape that `parse_event` rejects
        // — e.g. a create event with `prev_events` set passes the skeleton
        // checks but trips v12 rule 1.1. Previously this `.expect`-panicked
        // from inside `build()`; now the `FormatError` bubbles up so the
        // caller sees the specific rule that fired.
        let err = EventBuilder::new(user("@a:d"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
            .prev_events(vec![eid("$bogus:d")])
            .origin_server_ts(1)
            .build()
            .expect_err("create with prev_events must surface as FormatError");
        assert!(
            matches!(err, FormatError::CreateHasPrevEvents),
            "expected CreateHasPrevEvents, got: {err:?}"
        );
    }

    #[test]
    fn build_rejects_event_over_max_pdu_size() {
        // Local-send path of the S-S §"Size limits" whole-PDU check: an
        // oversized event surfaces from `build()` (via validate_pdu) so the
        // C-S handler can 400 it. Boundary precision lives in validate.rs.
        let err = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "body": "a".repeat(70_000) }))
            .origin_server_ts(1)
            .build()
            .expect_err("oversized event must not build");
        assert!(matches!(err, FormatError::EventTooLarge));
    }

    // ---------- from_wire ----------

    #[test]
    fn from_wire_round_trips_builder_output() {
        let built = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "body": "hi" }))
            .origin_server_ts(42)
            .build()
            .expect("builds");

        let parsed = from_wire(built.raw.clone(), Vec::new()).expect("from_wire");
        // event_id is recomputed from raw — must match the builder's output.
        assert_eq!(parsed.event_id, built.event_id);
        assert_eq!(parsed.room_id, built.room_id);
        assert_eq!(parsed.sender, built.sender);
        assert_eq!(parsed.event_type, built.event_type);
        assert_eq!(parsed.origin_server_ts, built.origin_server_ts);
    }

    #[test]
    fn from_wire_round_trips_create_event_with_derived_room_id() {
        let built = EventBuilder::new(user("@a:d"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": crate::ROOM_VERSION_ID }))
            .origin_server_ts(42)
            .build()
            .expect("create");

        let parsed = from_wire(built.raw.clone(), Vec::new()).expect("from_wire");
        assert_eq!(parsed.event_id, built.event_id);
        // parse_event re-derived room_id from event_id via sigil swap —
        // must match the builder's.
        assert_eq!(parsed.room_id, built.room_id);
        // Round-tripped raw still lacks `room_id` (v12 create invariant).
        let raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert!(raw.get("room_id").is_none());
    }

    #[test]
    fn from_wire_attaches_caller_supplied_auth_events() {
        let built = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({}))
            .origin_server_ts(1)
            .build()
            .expect("builds");

        let auth = vec![eid("$x:d"), eid("$y:d")];
        let parsed = from_wire(built.raw.clone(), auth.clone()).expect("from_wire");
        assert_eq!(parsed.auth_events, auth);
    }

    #[test]
    fn from_wire_rejects_non_object_root() {
        let raw = to_raw_value(&json!([1, 2, 3])).unwrap();
        let err = from_wire(raw, Vec::new()).expect_err("array root");
        assert!(matches!(
            err,
            FormatError::InvalidFieldType {
                field: "<root>",
                expected: "object"
            }
        ));
    }

    #[test]
    fn from_wire_redacts_event_with_mismatched_content_hash() {
        // Spec: receive-side redacts events whose content hash doesn't match.
        // Tamper with `hashes.sha256` on a builder-produced event and verify
        // from_wire returns the redacted form (content collapsed to {}) while
        // keeping the SAME event_id (which is computed over the redacted form).
        let built = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "secret" }))
            .origin_server_ts(1)
            .build()
            .expect("builds");

        // Tamper: rewrite `hashes.sha256` to a junk value so verification fails.
        let mut raw_obj: serde_json::Value = serde_json::from_str(built.raw.get()).unwrap();
        raw_obj["hashes"]["sha256"] = json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let tampered_raw = serde_json::value::to_raw_value(&raw_obj).expect("raw");

        let parsed = from_wire(tampered_raw, Vec::new()).expect("from_wire");
        // event_id is unchanged — reference_hash runs over the redacted form
        // which doesn't include `hashes` directly… wait, it does. After
        // tampering the reference_hash differs too, so the parsed event_id
        // here is the *tampered* hash. The point of the test is that the
        // content was redacted (body stripped) rather than the event rejected.
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert!(
            parsed_raw["content"]
                .as_object()
                .expect("content object")
                .is_empty(),
            "content must be redacted on hash mismatch, got: {}",
            parsed_raw["content"]
        );
        // type / room_id / sender are preserved (V11 keep-list).
        assert_eq!(parsed_raw["type"].as_str(), Some("m.room.message"));
        assert_eq!(parsed_raw["room_id"].as_str(), Some("!r:d"));
        assert_eq!(parsed_raw["sender"].as_str(), Some("@a:d"));
    }

    #[test]
    fn from_wire_accepts_event_with_matching_content_hash() {
        // Builder produces events with a valid content hash; from_wire must
        // accept them as-is (no redaction).
        let built = EventBuilder::new(user("@a:d"), "m.room.message".to_owned())
            .room_id(room("!r:d"))
            .content(json!({ "msgtype": "m.text", "body": "preserved" }))
            .origin_server_ts(1)
            .build()
            .expect("builds");
        let parsed = from_wire(built.raw.clone(), Vec::new()).expect("from_wire");
        // Content survives — body field still there.
        let parsed_raw: serde_json::Value = serde_json::from_str(parsed.raw.get()).unwrap();
        assert_eq!(parsed_raw["content"]["body"].as_str(), Some("preserved"));
    }

    #[test]
    fn from_wire_rejects_object_without_type() {
        // No `type` field — ruma's `redact_in_place` reports
        // `MissingField { path: "type" }`; from_wire translates to
        // FormatError::MissingField("type").
        let raw = to_raw_value(&json!({
            "sender": "@a:d", "room_id": "!r:d",
            "content": {}, "origin_server_ts": 1
        }))
        .unwrap();
        let err = from_wire(raw, Vec::new()).expect_err("missing type");
        assert!(matches!(err, FormatError::MissingField("type")));
    }
}
