//! Conversions from the canonical [`Event`] struct to ruma's client-facing
//! `Raw<…>` shapes.
//!
//! `Event.raw` is the canonical v12 / MSC4242 wire bytes — by design those
//! bytes don't carry `event_id` (it's the computed reference hash, never on
//! the wire) and for `m.room.create` they don't carry `room_id` either
//! (derived from the event_id via sigil swap). Those three server-computed
//! fields live on the `Event` struct as sidecar fields; this module is the
//! single place that merges them back into the JSON before delivery to a
//! CSAPI client.
//!
//! Federation paths must continue to ship `Event.raw` verbatim — federation
//! peers verify the reference hash against those exact bytes.
//!
//! See `docs/event-view-conversions.md` for the rationale and the call-site
//! migration plan.

use ruma::events::{
    AnyStrippedStateEvent, AnySyncStateEvent, AnySyncTimelineEvent, AnyTimelineEvent,
};
use ruma::serde::Raw;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::Event;

/// Failure mode for the `TryFrom<&Event>` impls into state-shaped client
/// views: an `Event` whose `state_key` is `None` cannot be represented as a
/// state event. The infallible `From<&Event>` impls (timeline targets) can't
/// fail; [`strip_state_event`] is intrinsically infallible (it just removes
/// non-canonical fields from a JSON value) — see its doc for why.
#[derive(Debug, Error)]
pub enum StateEventConversionError {
    #[error("event of type {event_type} has no state_key — cannot represent as a state event")]
    NotAStateEvent { event_type: String },
}

/// Policy for `room_id` placement when enriching a wire event for the CSAPI.
///
/// `OnlyOnCreate` matches Synapse's `/sync` shape: `room_id` is redundant on
/// non-create events because the response is already keyed by room id under
/// `rooms.join.<id>.…`. `Always` is for the full-event endpoints where the
/// event is delivered standalone (`/_matrix/client/v3/rooms/{}/event/{id}`).
#[derive(Copy, Clone)]
enum IncludeRoomId {
    OnlyOnCreate,
    Always,
}

/// Parse `Event.raw` once, merge `event_id` (always) and `room_id` (per the
/// `IncludeRoomId` policy). Strips a stray `room_id` on non-create events
/// when policy is `OnlyOnCreate`.
///
/// The two `.expect`s are justified by `Event` invariants: `parse_event`
/// already validated that `raw` is a JSON object, and re-serialising a
/// `serde_json::Map` back through `to_raw_value` is type-system-guaranteed
/// infallible.
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

/// `/sync` `timeline.events` (v3) and v5 timeline. Both message and state
/// events are accepted (the umbrella type covers both); the conversion is
/// infallible by construction.
impl From<&Event> for Raw<AnySyncTimelineEvent> {
    fn from(ev: &Event) -> Self {
        Raw::from_json(enrich_for_client(ev, IncludeRoomId::OnlyOnCreate))
    }
}

/// Full event view for `/_matrix/client/v3/rooms/{}/event/{eventId}` and
/// similar endpoints that deliver an event standalone. Always carries
/// `event_id` *and* `room_id`.
impl From<&Event> for Raw<AnyTimelineEvent> {
    fn from(ev: &Event) -> Self {
        Raw::from_json(enrich_for_client(ev, IncludeRoomId::Always))
    }
}

/// `/sync` `state.events` (v3) and v5 `required_state`. Fails when the event
/// has no `state_key` — only state events are representable as state events.
impl TryFrom<&Event> for Raw<AnySyncStateEvent> {
    type Error = StateEventConversionError;
    fn try_from(ev: &Event) -> Result<Self, Self::Error> {
        if ev.state_key.is_none() {
            return Err(StateEventConversionError::NotAStateEvent {
                event_type: ev.event_type.clone(),
            });
        }
        Ok(Raw::from_json(enrich_for_client(
            ev,
            IncludeRoomId::OnlyOnCreate,
        )))
    }
}

/// `invite_state` / `knock_state` (MSC1772 stripped form). Keeps only the
/// four canonical fields: `type`, `state_key`, `sender`, `content`.
impl TryFrom<&Event> for Raw<AnyStrippedStateEvent> {
    type Error = StateEventConversionError;
    fn try_from(ev: &Event) -> Result<Self, Self::Error> {
        let state_key =
            ev.state_key
                .as_deref()
                .ok_or_else(|| StateEventConversionError::NotAStateEvent {
                    event_type: ev.event_type.clone(),
                })?;
        let content: Value = serde_json::from_str(ev.content.get())
            .expect("Event.content is canonical JSON by parse_event invariant");
        let stripped = Value::Object(Map::from_iter([
            ("type".to_string(), Value::String(ev.event_type.clone())),
            (
                "state_key".to_string(),
                Value::String(state_key.to_string()),
            ),
            ("sender".to_string(), Value::String(ev.sender.to_string())),
            ("content".to_string(), content),
        ]));
        Ok(Raw::from_json(
            to_raw_value(&stripped).expect("fixed-shape JSON → RawValue is infallible"),
        ))
    }
}

/// Strip a `Raw<AnySyncStateEvent>` down to MSC1772 stripped form (`type`,
/// `state_key`, `sender`, `content`). Free function because the orphan
/// rule forbids `impl TryFrom<Raw<_>> for Raw<_>` — both types are foreign.
///
/// Infallible: keeps whichever of the four canonical fields are present at
/// the root. Non-object / missing-field inputs yield `{}`. `Raw<T>` doesn't
/// validate bytes against `T` at construction, so structural validation is
/// the caller's job, via `.deserialize::<AnyStrippedStateEvent>()`.
pub fn strip_state_event(raw: &Raw<AnySyncStateEvent>) -> Raw<AnyStrippedStateEvent> {
    let parsed: Value = serde_json::from_str(raw.json().get())
        .expect("Raw<…> bytes are valid JSON by RawValue invariant");
    let mut stripped = Map::new();
    for field in ["type", "state_key", "sender", "content"] {
        if let Some(v) = parsed.get(field) {
            stripped.insert(field.to_string(), v.clone());
        }
    }
    Raw::from_json(
        to_raw_value(&Value::Object(stripped)).expect("fixed-shape JSON → RawValue is infallible"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_id::base_version_event_id;
    use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
    use serde_json::json;

    /// Build a test `Event` with the supplied raw JSON. event_id is derived
    /// from `base_version_event_id` to keep tests honest with the production
    /// invariant. For create events the caller passes `is_create=true` and
    /// we mint a `room_id` from the event_id (sigil swap), matching the
    /// production builder.
    fn build_event(
        event_type: &str,
        state_key: Option<&str>,
        sender: &str,
        raw_json: Value,
    ) -> Event {
        let raw = to_raw_value(&raw_json).expect("raw");
        let event_id = base_version_event_id(&raw).expect("event id");
        let content_value = raw_json
            .get("content")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));
        let content = to_raw_value(&content_value).expect("content");
        let room_id: OwnedRoomId = if event_type == "m.room.create" {
            let suffix = &event_id.as_str()[1..];
            OwnedRoomId::try_from(format!("!{suffix}")).expect("room_id sigil swap")
        } else {
            raw_json
                .get("room_id")
                .and_then(Value::as_str)
                .map(|s| OwnedRoomId::try_from(s).expect("room_id"))
                .expect("non-create raw must carry room_id")
        };
        Event {
            event_id,
            room_id,
            sender: OwnedUserId::try_from(sender).expect("sender"),
            event_type: event_type.to_string(),
            state_key: state_key.map(String::from),
            origin_server_ts: 1_000,
            content,
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            rejected: false,
            soft_failed: false,
            auth_events: Vec::new(),
            raw,
        }
    }

    /// Minimal m.room.create event for tests. Wire bytes deliberately
    /// omit `event_id` and `room_id` per v12 / MSC4242.
    fn create_event() -> Event {
        build_event(
            "m.room.create",
            Some(""),
            "@alice:hs1",
            json!({
                "type": "m.room.create",
                "sender": "@alice:hs1",
                "state_key": "",
                "origin_server_ts": 1_000,
                "content": {"room_version": "org.matrix.msc4242.12"},
                "hashes": {"sha256": "AAAA"},
                "prev_events": [],
                "prev_state_events": [],
            }),
        )
    }

    /// Minimal m.room.message (non-state) event for tests. The wire bytes
    /// include `room_id`, so the OnlyOnCreate stripping path can be
    /// exercised.
    fn message_event(room_id: &OwnedRoomId) -> Event {
        build_event(
            "m.room.message",
            None,
            "@alice:hs1",
            json!({
                "type": "m.room.message",
                "sender": "@alice:hs1",
                "room_id": room_id.as_str(),
                "origin_server_ts": 1_001,
                "content": {"msgtype": "m.text", "body": "hi"},
                "hashes": {"sha256": "BBBB"},
                "prev_events": [],
                "prev_state_events": [],
            }),
        )
    }

    /// Minimal m.room.name state event for tests.
    fn name_state_event(room_id: &OwnedRoomId) -> Event {
        build_event(
            "m.room.name",
            Some(""),
            "@alice:hs1",
            json!({
                "type": "m.room.name",
                "sender": "@alice:hs1",
                "state_key": "",
                "room_id": room_id.as_str(),
                "origin_server_ts": 1_002,
                "content": {"name": "My Room"},
                "hashes": {"sha256": "CCCC"},
                "prev_events": [],
                "prev_state_events": [],
            }),
        )
    }

    fn parse_raw<T>(raw: &Raw<T>) -> Value {
        serde_json::from_str(raw.json().get()).expect("Raw<T> is valid JSON")
    }

    #[test]
    fn from_event_for_sync_timeline_injects_event_id() {
        let create = create_event();
        let raw: Raw<AnySyncTimelineEvent> = (&create).into();
        let v = parse_raw(&raw);
        assert_eq!(v["event_id"].as_str(), Some(create.event_id.as_str()));
    }

    #[test]
    fn from_event_for_sync_timeline_create_event_carries_room_id() {
        let create = create_event();
        let raw: Raw<AnySyncTimelineEvent> = (&create).into();
        let v = parse_raw(&raw);
        // Create event wire bytes lack room_id (derived from event_id via
        // sigil swap) — the conversion must inject it.
        assert_eq!(v["room_id"].as_str(), Some(create.room_id.as_str()));
    }

    #[test]
    fn from_event_for_sync_timeline_non_create_strips_room_id() {
        let create = create_event();
        let msg = message_event(&create.room_id);
        let raw: Raw<AnySyncTimelineEvent> = (&msg).into();
        let v = parse_raw(&raw);
        assert!(
            v.get("room_id").is_none(),
            "non-create timeline event strips room_id (Synapse-shape /sync): {v}",
        );
        // event_id still injected.
        assert_eq!(v["event_id"].as_str(), Some(msg.event_id.as_str()));
    }

    #[test]
    fn try_from_event_for_sync_state_err_when_no_state_key() {
        let create = create_event();
        let msg = message_event(&create.room_id);
        // m.room.message has no state_key.
        let err = Raw::<AnySyncStateEvent>::try_from(&msg).unwrap_err();
        let StateEventConversionError::NotAStateEvent { event_type } = err;
        assert_eq!(event_type, "m.room.message");
    }

    #[test]
    fn try_from_event_for_sync_state_ok_when_state_key_present() {
        let create = create_event();
        let name = name_state_event(&create.room_id);
        let raw = Raw::<AnySyncStateEvent>::try_from(&name).expect("name event is a state event");
        let v = parse_raw(&raw);
        assert_eq!(v["event_id"].as_str(), Some(name.event_id.as_str()));
        assert_eq!(v["type"].as_str(), Some("m.room.name"));
        assert_eq!(v["state_key"].as_str(), Some(""));
    }

    #[test]
    fn try_from_event_for_stripped_state_keeps_only_four_canonical_fields() {
        let create = create_event();
        let raw =
            Raw::<AnyStrippedStateEvent>::try_from(&create).expect("create event is a state event");
        let v = parse_raw(&raw);
        let obj = v.as_object().expect("object");
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["content", "sender", "state_key", "type"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "stripped state carries exactly the four canonical fields",
        );
        // All four fields populated from the Event struct's pre-parsed
        // scalars (not from raw), so server-computed values appear here too.
        assert_eq!(v["type"].as_str(), Some("m.room.create"));
        assert_eq!(v["state_key"].as_str(), Some(""));
        assert_eq!(v["sender"].as_str(), Some("@alice:hs1"));
        assert_eq!(
            v["content"]["room_version"].as_str(),
            Some("org.matrix.msc4242.12")
        );
    }

    #[test]
    fn try_from_event_for_stripped_state_err_when_no_state_key() {
        let create = create_event();
        let msg = message_event(&create.room_id);
        let err = Raw::<AnyStrippedStateEvent>::try_from(&msg).unwrap_err();
        assert!(matches!(
            err,
            StateEventConversionError::NotAStateEvent { event_type } if event_type == "m.room.message"
        ));
    }

    #[test]
    fn from_event_for_room_event_carries_both_ids() {
        let create = create_event();
        let msg = message_event(&create.room_id);
        let raw: Raw<AnyTimelineEvent> = (&msg).into();
        let v = parse_raw(&raw);
        assert_eq!(v["event_id"].as_str(), Some(msg.event_id.as_str()));
        assert_eq!(v["room_id"].as_str(), Some(msg.room_id.as_str()));
    }

    #[test]
    fn from_event_for_room_event_create_event_also_carries_both_ids() {
        let create = create_event();
        let raw: Raw<AnyTimelineEvent> = (&create).into();
        let v = parse_raw(&raw);
        assert_eq!(v["event_id"].as_str(), Some(create.event_id.as_str()));
        assert_eq!(v["room_id"].as_str(), Some(create.room_id.as_str()));
    }

    /// Timeline delivery must pass `unsigned` (e.g. `age`, `prev_content`,
    /// `redacted_because`) through verbatim — clients depend on it for
    /// rendering. Pinning both arms (message and state event reaching the
    /// timeline target) guards against a future "normalise raw before
    /// delivery" change accidentally stripping fields the spec keeps.
    #[test]
    fn from_event_for_sync_timeline_preserves_unsigned() {
        let create = create_event();
        let msg = build_event(
            "m.room.message",
            None,
            "@alice:hs1",
            json!({
                "type": "m.room.message",
                "sender": "@alice:hs1",
                "room_id": create.room_id.as_str(),
                "origin_server_ts": 1_001,
                "content": {"msgtype": "m.text", "body": "hi"},
                "unsigned": {"age": 17, "transaction_id": "txn-42"},
                "hashes": {"sha256": "BBBB"},
                "prev_events": [],
                "prev_state_events": [],
            }),
        );
        let raw: Raw<AnySyncTimelineEvent> = (&msg).into();
        let v = parse_raw(&raw);
        assert_eq!(v["unsigned"]["age"].as_u64(), Some(17));
        assert_eq!(v["unsigned"]["transaction_id"].as_str(), Some("txn-42"));
    }

    /// State-event delivery must preserve `unsigned` *and* `prev_content`
    /// (state-event-only field carrying the previous content of the state
    /// key). The TryFrom impl re-uses `enrich_for_client` which goes through
    /// the same parse-and-inject path as the timeline impl, but the spec
    /// distinguishes the two shapes — pin them independently so a future
    /// divergence is caught.
    #[test]
    fn try_from_event_for_sync_state_preserves_unsigned_and_prev_content() {
        let create = create_event();
        let name = build_event(
            "m.room.name",
            Some(""),
            "@alice:hs1",
            json!({
                "type": "m.room.name",
                "state_key": "",
                "sender": "@alice:hs1",
                "room_id": create.room_id.as_str(),
                "origin_server_ts": 1_002,
                "content": {"name": "New"},
                "prev_content": {"name": "Old"},
                "unsigned": {"age": 99},
                "hashes": {"sha256": "CCCC"},
                "prev_events": [],
                "prev_state_events": [],
            }),
        );
        let raw = Raw::<AnySyncStateEvent>::try_from(&name).expect("name event is a state event");
        let v = parse_raw(&raw);
        assert_eq!(v["unsigned"]["age"].as_u64(), Some(99));
        assert_eq!(v["prev_content"]["name"].as_str(), Some("Old"));
        assert_eq!(v["content"]["name"].as_str(), Some("New"));
    }

    #[test]
    fn strip_state_event_drops_non_canonical_fields() {
        let raw_in = Raw::<AnySyncStateEvent>::from_json(
            to_raw_value(&json!({
                "type": "m.room.name",
                "state_key": "",
                "sender": "@u:example.org",
                "content": {"name": "X"},
                "event_id": "$evt:example.org",
                "origin_server_ts": 1_700_000_000_000u64,
                "unsigned": {"age": 17},
                "prev_content": {"name": "Old"},
                "room_id": "!r:example.org",
            }))
            .unwrap(),
        );
        let stripped = strip_state_event(&raw_in);
        let v = parse_raw(&stripped);
        let obj = v.as_object().expect("object");
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["content", "sender", "state_key", "type"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "stripped event carries only the four canonical fields",
        );
        for dropped in [
            "event_id",
            "origin_server_ts",
            "unsigned",
            "prev_content",
            "room_id",
        ] {
            assert!(
                obj.get(dropped).is_none(),
                "stripped event must drop {dropped}: {v}",
            );
        }
    }

    /// `Raw<AnySyncStateEvent>` doesn't validate its input — it can wrap any
    /// JSON, including non-objects, via `Raw::from_json` or `cast_unchecked`.
    /// `strip_state_event` must remain well-defined for those degenerate
    /// inputs: an empty `{}` result is fine, panicking is not.
    #[test]
    fn strip_state_event_returns_empty_for_non_object_input() {
        for raw_in in [
            // JSON array.
            Raw::<AnySyncStateEvent>::from_json(to_raw_value(&json!([1, 2, 3])).unwrap()),
            // JSON string.
            Raw::<AnySyncStateEvent>::from_json(to_raw_value(&json!("not an event")).unwrap()),
            // JSON null.
            Raw::<AnySyncStateEvent>::from_json(to_raw_value(&Value::Null).unwrap()),
            // Empty JSON object.
            Raw::<AnySyncStateEvent>::from_json(to_raw_value(&json!({})).unwrap()),
        ] {
            let stripped = strip_state_event(&raw_in);
            let v = parse_raw(&stripped);
            assert_eq!(
                v,
                json!({}),
                "non-object / empty-object input yields empty stripped output: {v}",
            );
        }
    }

    /// Suppress the unused-import lint when the helper is left over after
    /// a future test refactor; also acts as a smoke test that the helper
    /// compiles in isolation.
    #[test]
    fn build_event_helper_smoke_test() {
        let create = create_event();
        assert_eq!(create.event_type, "m.room.create");
        assert!(create.event_id.as_str().starts_with('$'));
        assert!(create.room_id.as_str().starts_with('!'));
        let _ = OwnedEventId::try_from(create.event_id.as_str()).expect("valid event_id");
    }
}
