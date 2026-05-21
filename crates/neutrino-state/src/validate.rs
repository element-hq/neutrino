//! Phase 1: validation.
//!
//! Phase 1a — `parse_event`: pure-JSON wire format. No I/O.
//! Phase 1b — `validate_references`: existential checks that require provider
//! lookups (v12 rule 2 + MSC4242 `prev_state_events` triad).
//!
//! Every check is annotated inline with its spec citation. Anything that
//! requires resolved room state is deferred to phase 3 (`check_auth_rules`).

use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::provider::StateProvider;
use crate::{Event, FormatError, ReferenceError, RoomVersion};

const MAX_PREV_EVENTS: usize = 20;
const MAX_PREV_STATE_EVENTS: usize = 20;
const DEPTH_LIMIT: u64 = (1u64 << 53) - 1;

/// Parse a raw event JSON into an `Event`, applying every phase-1a check
/// (wire-format only; no provider lookups). Reference validation —
/// `prev_state_events` lookup, room-exists check — is `validate_references`.
///
/// `event_id` is provided by the caller: under v12 it derives from the event's
/// reference hash, which is computed by a separate event-building step. This
/// function focuses on validating the wire bytes.
///
/// On error, the `Event` is not constructed; the error variant points at the
/// first failed check (validation is not exhaustive — additional issues may
/// surface after fixing the first).
///
/// The `signatures` field is intentionally ignored. Per `CLAUDE.md` the
/// server runs on a trusted network and does not verify signatures.
pub fn parse_event(
    raw: Box<RawValue>,
    event_id: OwnedEventId,
    room_version: RoomVersion,
) -> Result<Event, FormatError> {
    // Only v12 is supported (see lib.rs `RoomVersion`). Argument retained so
    // future versions can be slotted in without an API break.
    let _ = room_version;

    let map: Map<String, Value> = serde_json::from_str(raw.get())?;

    // MSC4242: reject any event that includes auth_events on the wire.
    if map.contains_key("auth_events") {
        return Err(FormatError::AuthEventsPresent);
    }

    // PDU schema required fields.
    let event_type = required_string(&map, "type")?.to_owned();
    let is_create = event_type == "m.room.create";

    let sender_str = required_string(&map, "sender")?;
    let sender: OwnedUserId = sender_str.parse().map_err(|_| FormatError::MalformedId {
        field: "sender",
        value: sender_str.to_owned(),
    })?;

    let origin_server_ts = required_u64(&map, "origin_server_ts")?;
    let depth = required_u64(&map, "depth")?;
    // v12 PDU schema: depth "Must be less than the maximum value for an
    // integer (2^53 - 1)."
    if depth >= DEPTH_LIMIT {
        return Err(FormatError::DepthOutOfRange);
    }

    // Hashes: required, well-formed object of strings. Content hashes are
    // independent of signatures/keys — we keep this check even though we
    // don't verify the hash values themselves.
    let hashes = map
        .get("hashes")
        .ok_or(FormatError::MissingField("hashes"))?
        .as_object()
        .ok_or(FormatError::InvalidFieldType {
            field: "hashes",
            expected: "object",
        })?;
    for v in hashes.values() {
        if !v.is_string() {
            return Err(FormatError::InvalidFieldType {
                field: "hashes",
                expected: "{string: string}",
            });
        }
    }

    // content: required, object.
    let content_value = map
        .get("content")
        .ok_or(FormatError::MissingField("content"))?;
    let content_obj = content_value
        .as_object()
        .ok_or(FormatError::InvalidFieldType {
            field: "content",
            expected: "object",
        })?;
    let content_raw = serde_json::value::to_raw_value(content_value)?;

    // prev_events: required, array, ≤ MAX_PREV_EVENTS.
    let prev_events = parse_event_id_array(&map, "prev_events")?;
    if prev_events.len() > MAX_PREV_EVENTS {
        return Err(FormatError::TooManyPrevEvents);
    }

    // v12 rule 1.1: m.room.create must have no prev_events.
    if is_create && !prev_events.is_empty() {
        return Err(FormatError::CreateHasPrevEvents);
    }

    // prev_state_events: required (MSC4242), array, ≤ MAX_PREV_STATE_EVENTS.
    // MSC4242 also: m.room.create must not have any.
    let prev_state_events = if is_create {
        // Create events MUST NOT have prev_state_events. Treat the field as
        // absent or empty; any non-empty value is a reject.
        if let Some(v) = map.get("prev_state_events") {
            let arr = v.as_array().ok_or(FormatError::InvalidFieldType {
                field: "prev_state_events",
                expected: "array",
            })?;
            if !arr.is_empty() {
                return Err(FormatError::CreateHasPrevStateEvents);
            }
        }
        Vec::new()
    } else {
        let v = parse_event_id_array(&map, "prev_state_events")?;
        if v.len() > MAX_PREV_STATE_EVENTS {
            return Err(FormatError::TooManyPrevStateEvents);
        }
        v
    };

    // room_id:
    //   non-create — required, valid room id.
    //   create     — v12 rule 1.2 "If the event has a room_id, reject."
    //                Derived from event_id by sigil swap ($ → !).
    let room_id = if is_create {
        if map.contains_key("room_id") {
            return Err(FormatError::CreateHasRoomId);
        }
        derive_create_room_id(&event_id)?
    } else {
        let s = required_string(&map, "room_id")?;
        s.parse::<OwnedRoomId>()
            .map_err(|_| FormatError::MalformedId {
                field: "room_id",
                value: s.to_owned(),
            })?
    };

    // state_key (optional in PDU schema, required by some auth rules below).
    // Absent => None. Present must be a string — `null` is a wire-format
    // error, not equivalent to absent.
    let state_key = match map.get("state_key") {
        None => None,
        Some(v) => Some(
            v.as_str()
                .ok_or(FormatError::InvalidFieldType {
                    field: "state_key",
                    expected: "string",
                })?
                .to_owned(),
        ),
    };

    // v12 rule 9: "If the event has a `state_key` that starts with an `@` and
    // does not match the `sender`, reject."
    //
    // Rule 5 (m.room.member) is terminal for membership events and has its
    // own rules about sender / state_key (the state_key is the *target* user,
    // not the sender — that's how invites and kicks work). So rule 9 must
    // not apply to m.room.member events. Synapse's `event_auth.py` does the
    // same exclusion.
    if event_type != "m.room.member"
        && let Some(sk) = &state_key
        && sk.starts_with('@')
        && sk != sender.as_str()
    {
        return Err(FormatError::StateKeyAtSignSenderMismatch);
    }

    // Per-type checks.
    match event_type.as_str() {
        "m.room.create" => check_create(content_obj)?,
        "m.room.member" => check_member(content_obj, state_key.as_deref())?,
        "m.room.power_levels" => check_power_levels(content_obj)?,
        _ => {}
    }

    Ok(Event {
        event_id,
        room_id,
        sender,
        event_type,
        state_key,
        origin_server_ts,
        content: content_raw,
        prev_events,
        prev_state_events,
        raw,
    })
}

fn required_string<'a>(
    map: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, FormatError> {
    map.get(field)
        .ok_or(FormatError::MissingField(field))?
        .as_str()
        .ok_or(FormatError::InvalidFieldType {
            field,
            expected: "string",
        })
}

fn required_u64(map: &Map<String, Value>, field: &'static str) -> Result<u64, FormatError> {
    map.get(field)
        .ok_or(FormatError::MissingField(field))?
        .as_u64()
        .ok_or(FormatError::InvalidFieldType {
            field,
            expected: "unsigned integer",
        })
}

fn parse_event_id_array(
    map: &Map<String, Value>,
    field: &'static str,
) -> Result<Vec<OwnedEventId>, FormatError> {
    let arr = map
        .get(field)
        .ok_or(FormatError::MissingField(field))?
        .as_array()
        .ok_or(FormatError::InvalidFieldType {
            field,
            expected: "array",
        })?;
    arr.iter()
        .map(|v| {
            let s = v.as_str().ok_or(FormatError::InvalidFieldType {
                field,
                expected: "array of strings",
            })?;
            s.parse::<OwnedEventId>()
                .map_err(|_| FormatError::MalformedId {
                    field,
                    value: s.to_owned(),
                })
        })
        .collect()
}

/// Derive the room_id of a v12 room from the event_id of its create event by
/// swapping the `$` sigil for `!`.
fn derive_create_room_id(event_id: &OwnedEventId) -> Result<OwnedRoomId, FormatError> {
    let s = event_id.as_str();
    let derived = match s.strip_prefix('$') {
        Some(rest) => format!("!{rest}"),
        None => {
            return Err(FormatError::MalformedId {
                field: "event_id (for room_id derivation)",
                value: s.to_owned(),
            });
        }
    };
    derived
        .parse::<OwnedRoomId>()
        .map_err(|_| FormatError::MalformedId {
            field: "room_id (derived)",
            value: derived,
        })
}

fn check_create(content: &Map<String, Value>) -> Result<(), FormatError> {
    // v12 rule 1.3: "If `content.room_version` is present and is not a
    // recognised version, reject."
    if let Some(v) = content.get("room_version") {
        let s = v.as_str().ok_or(FormatError::InvalidFieldType {
            field: "content.room_version",
            expected: "string",
        })?;
        if s != "12" {
            return Err(FormatError::UnrecognisedRoomVersion(s.to_owned()));
        }
    }
    // v12 rule 1.4: additional_creators must be array of strings each
    // passing the same user ID validation as `sender`.
    if let Some(ac) = content.get("additional_creators") {
        let arr = ac
            .as_array()
            .ok_or(FormatError::InvalidAdditionalCreators)?;
        for entry in arr {
            let s = entry
                .as_str()
                .ok_or(FormatError::InvalidAdditionalCreators)?;
            s.parse::<OwnedUserId>()
                .map_err(|_| FormatError::InvalidAdditionalCreators)?;
        }
    }
    Ok(())
}

fn check_member(content: &Map<String, Value>, state_key: Option<&str>) -> Result<(), FormatError> {
    // v12 rule 5.1: "If there is no `state_key` property, or no `membership`
    // property in `content`, reject." — split into two variants for a tighter
    // error message.
    if state_key.is_none() {
        return Err(FormatError::MemberMissingStateKey);
    }
    if content.get("membership").and_then(Value::as_str).is_none() {
        return Err(FormatError::MemberMissingMembership);
    }
    // Per-value validation (membership ∈ {join, leave, invite, ban, knock})
    // intentionally NOT done here — `auth_rules::check_rule_5_member`'s
    // switch/case owns the value enumeration (rule 5.8 = catch-all reject as
    // `AuthError::Rule5_8_UnknownMembership`). Keeping it in one place avoids
    // an upfront-assert/per-arm-handler sync drift. See PLAN.md decisions
    // log for the rationale.
    Ok(())
}

fn check_power_levels(content: &Map<String, Value>) -> Result<(), FormatError> {
    // v12 rule 10.1.
    const INT_FIELDS: &[&str] = &[
        "users_default",
        "events_default",
        "state_default",
        "ban",
        "redact",
        "kick",
        "invite",
    ];
    for field in INT_FIELDS {
        if let Some(v) = content.get(*field)
            && !v.is_i64()
        {
            return Err(FormatError::PowerLevelsBadIntField(field));
        }
    }
    // v12 rule 10.2.
    for field in ["events", "notifications"] {
        if let Some(v) = content.get(field) {
            let obj = v
                .as_object()
                .ok_or(FormatError::PowerLevelsBadObjectField(field))?;
            for vv in obj.values() {
                if !vv.is_i64() {
                    return Err(FormatError::PowerLevelsBadObjectField(field));
                }
            }
        }
    }
    // v12 rule 10.3.
    if let Some(v) = content.get("users") {
        let obj = v.as_object().ok_or(FormatError::PowerLevelsBadUsers)?;
        for (k, vv) in obj {
            k.parse::<OwnedUserId>()
                .map_err(|_| FormatError::PowerLevelsBadUsers)?;
            if !vv.is_i64() {
                return Err(FormatError::PowerLevelsBadUsers);
            }
        }
    }
    Ok(())
}

/// Phase 1b: validate that everything this event refers to actually resolves.
///
/// Checks:
/// - **v12 rule 2**: the event's `room_id` is the event ID of an accepted
///   `m.room.create` event (with the sigil `!` instead of `$`).
/// - **MSC4242 prev_state_events triad**: each entry must exist in the store,
///   belong to the same room as this event, have a `state_key` (i.e. be a
///   state event), and not be rejected.
///
/// Create events bypass all checks: they introduce the room, they have no
/// `prev_state_events` (phase 1a F4), and they are the create event whose
/// existence rule 2 demands.
pub fn validate_references(
    event: &Event,
    provider: &dyn StateProvider,
) -> Result<(), ReferenceError> {
    if event.event_type == "m.room.create" {
        return Ok(());
    }

    // v12 rule 2.
    let derived_create_id = derive_create_event_id(&event.room_id)
        .ok_or_else(|| ReferenceError::UnknownRoom(event.room_id.clone()))?;
    let info = provider
        .get_event(&derived_create_id)
        .ok_or_else(|| ReferenceError::UnknownRoom(event.room_id.clone()))?;
    if info.rejected {
        return Err(ReferenceError::RoomRejected(event.room_id.clone()));
    }
    if info.event.event_type != "m.room.create" {
        return Err(ReferenceError::RoomTypeMismatch(derived_create_id));
    }

    // MSC4242 prev_state_events triad.
    for psid in &event.prev_state_events {
        let info = provider
            .get_event(psid)
            .ok_or_else(|| ReferenceError::PrevStateNotFound(psid.clone()))?;
        if info.rejected {
            return Err(ReferenceError::PrevStateRejected(psid.clone()));
        }
        if info.event.state_key.is_none() {
            return Err(ReferenceError::PrevStateNotStateEvent(psid.clone()));
        }
        if info.event.room_id != event.room_id {
            return Err(ReferenceError::PrevStateDifferentRoom(psid.clone()));
        }
    }

    Ok(())
}

/// Derive the create event's ID from a v12 room_id by swapping the `!` sigil
/// for `$`. Returns `None` if the room_id is somehow malformed (shouldn't
/// happen for a value that already passed `OwnedRoomId` parsing, but the
/// graceful fallback keeps `validate_references` panic-free).
fn derive_create_event_id(room_id: &OwnedRoomId) -> Option<OwnedEventId> {
    let rest = room_id.as_str().strip_prefix('!')?;
    format!("${rest}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn raw(v: Value) -> Box<RawValue> {
        serde_json::value::to_raw_value(&v).expect("test fixture")
    }

    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("test fixture event id")
    }

    /// Minimal non-create event with every required PDU field populated.
    fn base_event() -> Value {
        json!({
            "type": "m.room.message",
            "sender": "@alice:example.org",
            "room_id": "!room123:example.org",
            "content": { "msgtype": "m.text", "body": "hi" },
            "prev_events": ["$prev1:example.org"],
            "prev_state_events": [],
            "depth": 1,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc123" }
        })
    }

    /// Minimal v12 create event.
    fn base_create() -> Value {
        json!({
            "type": "m.room.create",
            "sender": "@alice:example.org",
            "content": { "room_version": "12" },
            "prev_events": [],
            "depth": 0,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc123" },
            "state_key": ""
        })
    }

    #[test]
    fn happy_path_message() {
        let ev = parse_event(raw(base_event()), eid("$ev1:example.org"), RoomVersion::V12)
            .expect("valid message");
        assert_eq!(ev.event_type, "m.room.message");
        assert_eq!(ev.prev_events.len(), 1);
        assert!(ev.state_key.is_none());
    }

    #[test]
    fn happy_path_create() {
        let ev = parse_event(
            raw(base_create()),
            eid("$create:example.org"),
            RoomVersion::V12,
        )
        .expect("valid create");
        assert_eq!(ev.event_type, "m.room.create");
        // F5 sister-check: room_id derived from event_id sigil swap.
        assert_eq!(ev.room_id.as_str(), "!create:example.org");
    }

    // ---------- F15: auth_events present ----------
    #[test]
    fn rejects_auth_events_on_wire() {
        let mut v = base_event();
        v["auth_events"] = json!(["$a:example.org"]);
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::AuthEventsPresent)
        ));
    }

    // ---------- F1 / F2: too many prev_(state_)events ----------
    #[test]
    fn rejects_prev_events_over_20() {
        let mut v = base_event();
        v["prev_events"] = json!(
            (0..21)
                .map(|i| format!("$p{i}:example.org"))
                .collect::<Vec<_>>()
        );
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::TooManyPrevEvents)
        ));
    }

    #[test]
    fn rejects_prev_state_events_over_20() {
        let mut v = base_event();
        v["prev_state_events"] = json!(
            (0..21)
                .map(|i| format!("$p{i}:example.org"))
                .collect::<Vec<_>>()
        );
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::TooManyPrevStateEvents)
        ));
    }

    // ---------- F3 / F4: create has prev_(state_)events ----------
    #[test]
    fn rejects_create_with_prev_events() {
        let mut v = base_create();
        v["prev_events"] = json!(["$prev:example.org"]);
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::CreateHasPrevEvents)
        ));
    }

    #[test]
    fn rejects_create_with_prev_state_events() {
        let mut v = base_create();
        v["prev_state_events"] = json!(["$prev:example.org"]);
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::CreateHasPrevStateEvents)
        ));
    }

    // ---------- F5: create has room_id ----------
    #[test]
    fn rejects_create_with_room_id() {
        let mut v = base_create();
        v["room_id"] = json!("!fake:example.org");
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::CreateHasRoomId)
        ));
    }

    // ---------- F6: unrecognised room_version ----------
    #[test]
    fn rejects_unrecognised_room_version() {
        let mut v = base_create();
        v["content"] = json!({ "room_version": "11" });
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::UnrecognisedRoomVersion(_))
        ));
    }

    #[test]
    fn accepts_create_without_room_version_field() {
        // Per Kegan's call on F6: "default value" handling is separate from
        // "is value valid". When room_version is absent we don't reject here.
        let mut v = base_create();
        v["content"] = json!({});
        parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12)
            .expect("create without content.room_version is permitted in phase 1");
    }

    // ---------- F7: additional_creators ----------
    #[test]
    fn rejects_additional_creators_non_array() {
        let mut v = base_create();
        v["content"] = json!({ "room_version": "12", "additional_creators": "@bob:example.org" });
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidAdditionalCreators)
        ));
    }

    #[test]
    fn rejects_additional_creators_with_bad_user_id() {
        let mut v = base_create();
        v["content"] = json!({ "room_version": "12", "additional_creators": ["not-a-user-id"] });
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidAdditionalCreators)
        ));
    }

    #[test]
    fn accepts_additional_creators_valid() {
        let mut v = base_create();
        v["content"] = json!({
            "room_version": "12",
            "additional_creators": ["@bob:example.org", "@carol:example.org"]
        });
        parse_event(raw(v), eid("$create:example.org"), RoomVersion::V12)
            .expect("valid additional_creators");
    }

    // ---------- F8: m.room.member missing parts ----------
    #[test]
    fn rejects_member_without_state_key() {
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["content"] = json!({ "membership": "join" });
        // state_key absent
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MemberMissingStateKey)
        ));
    }

    #[test]
    fn rejects_member_without_membership() {
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["state_key"] = json!("@alice:example.org");
        v["content"] = json!({});
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MemberMissingMembership)
        ));
    }

    // ---------- F10: rule 9 (@-prefixed state_key must match sender) ----------
    #[test]
    fn rejects_at_state_key_mismatch() {
        let mut v = base_event();
        v["type"] = json!("m.some.thing");
        v["state_key"] = json!("@bob:example.org");
        // sender = @alice — mismatch
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::StateKeyAtSignSenderMismatch)
        ));
    }

    #[test]
    fn accepts_at_state_key_matching_sender() {
        let mut v = base_event();
        v["type"] = json!("m.something");
        v["state_key"] = json!("@alice:example.org");
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12)
            .expect("state_key matches sender");
    }

    #[test]
    fn accepts_non_at_state_key() {
        let mut v = base_event();
        v["type"] = json!("m.room.topic");
        v["state_key"] = json!("");
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12).expect("empty state_key ok");
    }

    #[test]
    fn rule_9_does_not_apply_to_m_room_member() {
        // Invite of @bob by @alice: state_key=@bob, sender=@alice — normal
        // membership operation. Rule 9 would reject naively; rule 5 owns
        // m.room.member end-to-end so rule 9 must skip it.
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["state_key"] = json!("@bob:example.org");
        v["content"] = json!({ "membership": "invite" });
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12)
            .expect("m.room.member with different state_key/sender is valid");
    }

    // ---------- F11 / F12 / F13: power_levels content ----------
    #[test]
    fn rejects_power_levels_non_integer_int_field() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users_default": "high" });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::PowerLevelsBadIntField("users_default"))
        ));
    }

    #[test]
    fn rejects_power_levels_events_non_int_value() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "events": { "m.room.name": "yes" } });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::PowerLevelsBadObjectField("events"))
        ));
    }

    #[test]
    fn rejects_power_levels_users_bad_user_id() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users": { "not-a-user-id": 50 } });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::PowerLevelsBadUsers)
        ));
    }

    #[test]
    fn rejects_power_levels_users_non_int_value() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users": { "@alice:example.org": "boss" } });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::PowerLevelsBadUsers)
        ));
    }

    #[test]
    fn accepts_valid_power_levels() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({
            "users_default": 0,
            "events_default": 0,
            "state_default": 50,
            "ban": 50,
            "redact": 50,
            "kick": 50,
            "invite": 50,
            "users": { "@alice:example.org": 100 },
            "events": { "m.room.name": 50 },
            "notifications": { "room": 50 }
        });
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12).expect("valid power_levels");
    }

    // ---------- F14: malformed IDs ----------
    #[test]
    fn rejects_malformed_sender() {
        let mut v = base_event();
        v["sender"] = json!("not-a-user-id");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MalformedId {
                field: "sender",
                ..
            })
        ));
    }

    #[test]
    fn rejects_malformed_room_id_on_non_create() {
        let mut v = base_event();
        v["room_id"] = json!("not-a-room-id");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MalformedId {
                field: "room_id",
                ..
            })
        ));
    }

    #[test]
    fn rejects_malformed_prev_event_id() {
        let mut v = base_event();
        v["prev_events"] = json!(["not-an-id"]);
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MalformedId {
                field: "prev_events",
                ..
            })
        ));
    }

    // ---------- F16 / F17 / F18 / F19 / F20 / F22: required PDU fields ----------
    #[test]
    fn rejects_missing_type() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("type");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("type"))
        ));
    }

    #[test]
    fn rejects_missing_sender() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("sender");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("sender"))
        ));
    }

    #[test]
    fn rejects_missing_content() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("content");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("content"))
        ));
    }

    #[test]
    fn rejects_missing_depth() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("depth");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("depth"))
        ));
    }

    #[test]
    fn rejects_non_integer_depth() {
        let mut v = base_event();
        v["depth"] = json!("not a number");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidFieldType { field: "depth", .. })
        ));
    }

    #[test]
    fn rejects_depth_at_limit() {
        let mut v = base_event();
        v["depth"] = json!(DEPTH_LIMIT);
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::DepthOutOfRange)
        ));
    }

    #[test]
    fn accepts_depth_just_under_limit() {
        let mut v = base_event();
        v["depth"] = json!(DEPTH_LIMIT - 1);
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12).expect("depth ok");
    }

    #[test]
    fn rejects_missing_hashes() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("hashes");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("hashes"))
        ));
    }

    #[test]
    fn rejects_hashes_non_string_value() {
        let mut v = base_event();
        v["hashes"] = json!({ "sha256": 123 });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidFieldType {
                field: "hashes",
                ..
            })
        ));
    }

    #[test]
    fn rejects_missing_origin_server_ts() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("origin_server_ts");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("origin_server_ts"))
        ));
    }

    #[test]
    fn rejects_non_integer_origin_server_ts() {
        let mut v = base_event();
        v["origin_server_ts"] = json!("yesterday");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidFieldType {
                field: "origin_server_ts",
                ..
            })
        ));
    }

    #[test]
    fn rejects_state_key_null() {
        // null state_key is a wire-format error, not absent.
        let mut v = base_event();
        v["type"] = json!("m.room.topic");
        v["state_key"] = json!(null);
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::InvalidFieldType {
                field: "state_key",
                ..
            })
        ));
    }

    #[test]
    fn rejects_missing_prev_events() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("prev_events");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("prev_events"))
        ));
    }

    #[test]
    fn rejects_missing_prev_state_events_on_non_create() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("prev_state_events");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("prev_state_events"))
        ));
    }

    #[test]
    fn rejects_non_create_without_room_id() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("room_id");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12),
            Err(FormatError::MissingField("room_id"))
        ));
    }

    // ---------- signatures field is ignored ----------
    #[test]
    fn ignores_signatures_field_present() {
        let mut v = base_event();
        v["signatures"] = json!({ "example.org": { "ed25519:key": "sig" } });
        parse_event(raw(v), eid("$e:example.org"), RoomVersion::V12)
            .expect("signatures field accepted but not verified");
    }

    #[test]
    fn ignores_signatures_field_absent() {
        // base_event has no signatures field — still accepted.
        parse_event(raw(base_event()), eid("$e:example.org"), RoomVersion::V12)
            .expect("missing signatures accepted under trusted-network policy");
    }

    // =====================================================================
    // Phase 1b: validate_references
    // =====================================================================

    use crate::ReferenceError;
    use crate::provider::{EventInfo, StateProvider};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Default)]
    struct MockProvider {
        events: HashMap<OwnedEventId, EventInfo>,
    }

    impl MockProvider {
        fn insert(&mut self, info: EventInfo) {
            self.events.insert(info.event.event_id.clone(), info);
        }
    }

    impl StateProvider for MockProvider {
        fn get_event(&self, id: &ruma::EventId) -> Option<EventInfo> {
            self.events.get(id).cloned()
        }
    }

    fn make_event(json: Value, event_id: &str) -> Arc<Event> {
        Arc::new(parse_event(raw(json), eid(event_id), RoomVersion::V12).expect("test event valid"))
    }

    fn make_create(event_id: &str) -> Arc<Event> {
        make_event(base_create(), event_id)
    }

    fn make_message(room_id: &str, prev_state: Vec<&str>, event_id: &str) -> Arc<Event> {
        let mut v = base_event();
        v["room_id"] = json!(room_id);
        v["prev_state_events"] = json!(
            prev_state
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>()
        );
        make_event(v, event_id)
    }

    fn make_state_event(room_id: &str, event_id: &str) -> Arc<Event> {
        // A state event in `room_id` (m.room.topic with state_key "").
        let mut v = base_event();
        v["type"] = json!("m.room.topic");
        v["state_key"] = json!("");
        v["room_id"] = json!(room_id);
        v["content"] = json!({ "topic": "hello" });
        make_event(v, event_id)
    }

    #[test]
    fn refs_create_event_skips_all_checks() {
        let create = make_create("$create:example.org");
        let provider = MockProvider::default();
        validate_references(&create, &provider).expect("create event bypasses ref checks");
    }

    #[test]
    fn refs_happy_path_known_room() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        validate_references(&msg, &provider).expect("known room");
    }

    // v12 rule 2: unknown room
    #[test]
    fn refs_unknown_room_rejected() {
        let provider = MockProvider::default();
        let msg = make_message("!doesnotexist:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::UnknownRoom(_))
        ));
    }

    // v12 rule 2: create event is rejected
    #[test]
    fn refs_rejected_create_rejects_event() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: true,
        });
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::RoomRejected(_))
        ));
    }

    // v12 rule 2 defensive: derived id resolves to a non-create event.
    #[test]
    fn refs_non_create_at_derived_id_rejected() {
        let mut provider = MockProvider::default();
        // Store a non-create event at id "$create:example.org".
        provider.insert(EventInfo {
            event: make_state_event("!somewhere:example.org", "$create:example.org"),
            rejected: false,
        });
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::RoomTypeMismatch(_))
        ));
    }

    // MSC4242 triad: prev_state_event not in store
    #[test]
    fn refs_prev_state_not_found_rejected() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        let msg = make_message(
            "!create:example.org",
            vec!["$missing:example.org"],
            "$msg:example.org",
        );
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::PrevStateNotFound(_))
        ));
    }

    // MSC4242 triad: prev_state_event is rejected
    #[test]
    fn refs_prev_state_rejected_rejects_event() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        provider.insert(EventInfo {
            event: make_state_event("!create:example.org", "$rejected:example.org"),
            rejected: true,
        });
        let msg = make_message(
            "!create:example.org",
            vec!["$rejected:example.org"],
            "$msg:example.org",
        );
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::PrevStateRejected(_))
        ));
    }

    // MSC4242 triad: prev_state_event has no state_key (i.e. not a state event)
    #[test]
    fn refs_prev_state_non_state_event_rejected() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        // make_message() produces an m.room.message without state_key.
        let non_state = make_message("!create:example.org", vec![], "$msg-ref:example.org");
        provider.insert(EventInfo {
            event: non_state,
            rejected: false,
        });
        let msg = make_message(
            "!create:example.org",
            vec!["$msg-ref:example.org"],
            "$msg:example.org",
        );
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::PrevStateNotStateEvent(_))
        ));
    }

    // MSC4242 triad: prev_state_event belongs to a different room
    #[test]
    fn refs_prev_state_different_room_rejected() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        // A state event whose room_id is a different room.
        provider.insert(EventInfo {
            event: make_state_event("!other:example.org", "$other-state:example.org"),
            rejected: false,
        });
        let msg = make_message(
            "!create:example.org",
            vec!["$other-state:example.org"],
            "$msg:example.org",
        );
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::PrevStateDifferentRoom(_))
        ));
    }

    #[test]
    fn refs_multiple_prev_state_all_valid() {
        let mut provider = MockProvider::default();
        provider.insert(EventInfo {
            event: make_create("$create:example.org"),
            rejected: false,
        });
        provider.insert(EventInfo {
            event: make_state_event("!create:example.org", "$state1:example.org"),
            rejected: false,
        });
        provider.insert(EventInfo {
            event: make_state_event("!create:example.org", "$state2:example.org"),
            rejected: false,
        });
        let msg = make_message(
            "!create:example.org",
            vec!["$state1:example.org", "$state2:example.org"],
            "$msg:example.org",
        );
        validate_references(&msg, &provider).expect("all references valid");
    }
}
