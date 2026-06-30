//! Event-scoped validation: wire-format parsing and provider-free semantic
//! rules.
//!
//! `parse_event`: pure-JSON wire format (required fields, JSON
//! types, ID parsing). No I/O, no semantic-rule decisions.
//! `validate_pdu`: semantic rules that work off a parsed `Event`
//! and need no provider (count limits, create structural rules, rule 9,
//! per-type content shape).
//!
//! Reference validation (v12 rule 2 + the MSC4242 `prev_state_events` triad)
//! requires provider lookups against a single room's DAG, so it is room-scoped
//! and lives in `neutrino-state` (`validate::validate_references`).
//!
//! `validate_pdu` is split out of `parse_event` so downstream callers
//! (`RoomCore::apply`) don't have to take "you ran parse_event already" as
//! a precondition. Both `EventBuilder::build` / `from_wire` and
//! `RoomCore::apply` run validate_pdu explicitly; apply also runs it
//! defensively so a hand-constructed `Event` can't bypass the semantic
//! checks.
//!
//! Every check is annotated inline with its spec citation.

use ruma::canonical_json::CanonicalJsonError;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{Event, ROOM_VERSION_ID};

const MAX_PREV_EVENTS: usize = 20;
const MAX_PREV_STATE_EVENTS: usize = 20;

/// Errors raised by format validation — wire-format violations that
/// reject the event outright, before any state lookup happens.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    /// PDU schema: a required top-level field is absent.
    #[error("missing required field: {0}")]
    MissingField(&'static str),

    /// PDU schema: a field is present but the JSON value has the wrong shape.
    #[error("field `{field}` has wrong shape, expected {expected}")]
    InvalidFieldType {
        field: &'static str,
        expected: &'static str,
    },

    /// PDU schema: an event-id-like field could not be parsed as a Matrix ID.
    #[error("field `{field}` contains malformed id: {value}")]
    MalformedId { field: &'static str, value: String },

    /// PDU schema: the assembled event JSON could not be encoded as canonical
    /// JSON. Raised when caller-supplied `content` / `unsigned` contains a
    /// float, an out-of-range integer, or another value canonical JSON forbids.
    #[error("event JSON is not canonical-JSON encodable: {0}")]
    NonCanonical(CanonicalJsonError),

    /// MSC4242: `auth_events` is removed from the wire and must not be present.
    /// Cross-ref synapse `events/__init__.py`:
    /// `assert "auth_events" not in event_dict` for the MSC4242 event class.
    #[error("auth_events field is not permitted on the wire under MSC4242")]
    AuthEventsPresent,

    /// PDU schema: `prev_events` > 20 entries.
    #[error("prev_events exceeds 20 entries")]
    TooManyPrevEvents,

    /// MSC4242: `prev_state_events` > 20 entries.
    #[error("prev_state_events exceeds 20 entries")]
    TooManyPrevStateEvents,

    /// v12 rule 1.1: "If it has any `prev_events`, reject."
    #[error("m.room.create event has prev_events")]
    CreateHasPrevEvents,

    /// MSC4242: "If it has any `prev_state_events`, reject."
    #[error("m.room.create event has prev_state_events")]
    CreateHasPrevStateEvents,

    /// v12 rule 1.2: "If the event has a `room_id`, reject."
    #[error("m.room.create event has a room_id field")]
    CreateHasRoomId,

    /// v12 rule 1.3: "If `content.room_version` is present and is not a
    /// recognised version, reject."
    #[error("unrecognised room_version: {0}")]
    UnrecognisedRoomVersion(String),

    /// v12 rule 1.4: "If `additional_creators` is present in `content` and is
    /// not an array of strings where each string passes the same user ID
    /// validation applied to `sender`, reject."
    #[error("additional_creators is not an array of valid user ids")]
    InvalidAdditionalCreators,

    /// v12 rule 5.1: "If there is no `state_key` property, or no `membership`
    /// property in `content`, reject."
    #[error("m.room.member event missing state_key")]
    MemberMissingStateKey,

    /// v12 rule 5.1: "If there is no `state_key` property, or no `membership`
    /// property in `content`, reject."
    #[error("m.room.member event missing content.membership")]
    MemberMissingMembership,

    /// v12 rule 9: "If the event has a `state_key` that starts with an `@` and
    /// does not match the `sender`, reject."
    #[error("state_key starts with @ but does not match sender")]
    StateKeyAtSignSenderMismatch,

    /// v12 rule 10.1: "If any of the properties `users_default`,
    /// `events_default`, `state_default`, `ban`, `redact`, `kick`, or `invite`
    /// in `content` are present and not an integer, reject."
    #[error("power_levels field `{0}` is not an integer")]
    PowerLevelsBadIntField(&'static str),

    /// v12 rule 10.2: "If either of the properties `events` or `notifications`
    /// in `content` are present and not an object with values that are
    /// integers, reject."
    #[error("power_levels field `{0}` is not an object of integer values")]
    PowerLevelsBadObjectField(&'static str),

    /// v12 rule 10.3: "If the `users` property in `content` is not an object
    /// with keys that are valid user IDs with values that are integers,
    /// reject."
    #[error("power_levels users field is not {{valid-user-id: int}}")]
    PowerLevelsBadUsers,
}

/// Parse a raw event JSON into an `Event`. Wire-format only:
/// required field presence, JSON value types, ID parsing. Semantic rules
/// (count limits, create structural constraints, rule 9, per-type content
/// shape) belong to [`validate_pdu`]; reference validation belongs to
/// `neutrino-state`'s `validate::validate_references`.
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
    auth_events: Vec<OwnedEventId>,
) -> Result<Event, FormatError> {
    let map: Map<String, Value> = serde_json::from_str(raw.get())?;

    // MSC4242: reject any event that includes auth_events on the wire.
    // Wire-format: the field is forbidden by the protocol, regardless of
    // what the embedded value would mean.
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
    // `depth` is intentionally not parsed: this server uses
    // `origin_server_ts` for everything Synapse would use depth for
    // (backfill ordering etc.). v12 inbound events MAY include `depth` for
    // interop with non-MSC4242 senders; we ignore it.

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

    // content: required, object. Shape-only here; per-type content rules
    // (rules 1.3 / 1.4 / 5.1 / 10.1–10.3) are in `validate_pdu`.
    let content_value = map
        .get("content")
        .ok_or(FormatError::MissingField("content"))?;
    if !content_value.is_object() {
        return Err(FormatError::InvalidFieldType {
            field: "content",
            expected: "object",
        });
    }
    let content_raw = serde_json::value::to_raw_value(content_value)?;

    // prev_events: required, array of valid event ids. Length bound is a
    // semantic check (see `validate_pdu`).
    let prev_events = parse_event_id_array(&map, "prev_events")?;

    // prev_state_events: required for non-create events; optional for create
    // (wire-format allows absent or empty for create). The "create has no
    // prev_state_events / prev_events" semantic rules and the length bound
    // are in `validate_pdu`.
    let prev_state_events = match map.get("prev_state_events") {
        None if is_create => Vec::new(),
        None => return Err(FormatError::MissingField("prev_state_events")),
        Some(_) => parse_event_id_array(&map, "prev_state_events")?,
    };

    // room_id:
    //   non-create — required, valid room id.
    //   create     — v12 rule 1.2 ("If the event has a `room_id`, reject")
    //                is a wire-format rule: the field is forbidden on the
    //                wire and the room_id is derived from event_id post-hash
    //                ($ → !). Once Event is constructed the wire-presence
    //                signal is lost, so this check stays in parse_event.
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

    // state_key (optional in PDU schema, required by some auth rules later).
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
        auth_events,
        // Fresh from the wire-format pass — rejection and soft-fail are
        // downstream verdicts from auth-rule evaluation, not wire-format
        // properties.
        rejected: false,
        soft_failed: false,
        raw,
    })
}

/// Semantic checks that don't need a provider. Runs on a parsed
/// `Event` (output of [`parse_event`]).
///
/// Split out of [`parse_event`] so callers don't have to take "you ran
/// parse_event" as an implicit precondition for the semantic rules: a
/// hand-constructed `Event` that bypassed parse_event still has to satisfy
/// these checks before `RoomCore::apply` will accept it.
///
/// Checks:
/// - **MSC4242**: `prev_events` ≤ `MAX_PREV_EVENTS`,
///   `prev_state_events` ≤ `MAX_PREV_STATE_EVENTS`.
/// - **v12 rule 1.1**: `m.room.create` has no `prev_events`.
/// - **MSC4242**: `m.room.create` has no `prev_state_events`.
/// - **v12 rule 9**: state_key starting with `@` matches sender, except for
///   `m.room.member` (rule 5 owns that type end-to-end).
/// - **v12 rule 1.3 / 1.4**: create content's `room_version` is recognised
///   and `additional_creators` is `[valid_user_id, ...]`.
/// - **v12 rule 5.1**: member content has a `state_key` and a `membership`.
/// - **v12 rule 10.1 / 10.2 / 10.3**: power_levels content is well-formed
///   (numeric fields are ints, events/notifications are objects of ints,
///   users is `{valid_user_id: int}`).
///
/// `Event.content` is re-parsed as a JSON object so the per-type checks can
/// inspect it. parse_event guarantees content is an object, but the
/// re-parse means a caller that hand-constructed an `Event` with a
/// non-object content still gets a typed error here rather than a panic.
pub fn validate_pdu(event: &Event) -> Result<(), FormatError> {
    // MSC4242 bounds on DAG fan-in. Cheap up-front guard.
    if event.prev_events.len() > MAX_PREV_EVENTS {
        return Err(FormatError::TooManyPrevEvents);
    }
    if event.prev_state_events.len() > MAX_PREV_STATE_EVENTS {
        return Err(FormatError::TooManyPrevStateEvents);
    }

    let is_create = event.event_type == "m.room.create";
    if is_create {
        // v12 rule 1.1.
        if !event.prev_events.is_empty() {
            return Err(FormatError::CreateHasPrevEvents);
        }
        // MSC4242.
        if !event.prev_state_events.is_empty() {
            return Err(FormatError::CreateHasPrevStateEvents);
        }
    }

    // v12 rule 9: "If the event has a `state_key` that starts with an `@`
    // and does not match the `sender`, reject."
    //
    // Rule 5 (m.room.member) is terminal for membership events and has its
    // own rules about sender / state_key (the state_key is the *target*
    // user, not the sender — that's how invites and kicks work). So rule 9
    // must not apply to m.room.member events. Synapse's `event_auth.py`
    // does the same exclusion.
    if event.event_type != "m.room.member"
        && let Some(sk) = &event.state_key
        && sk.starts_with('@')
        && sk != event.sender.as_str()
    {
        return Err(FormatError::StateKeyAtSignSenderMismatch);
    }

    // Per-type content checks.
    match event.event_type.as_str() {
        "m.room.create" => check_create(&content_as_object(&event.content)?)?,
        "m.room.member" => check_member(
            &content_as_object(&event.content)?,
            event.state_key.as_deref(),
        )?,
        "m.room.power_levels" => check_power_levels(&content_as_object(&event.content)?)?,
        _ => {}
    }

    Ok(())
}

/// Re-parse `Event.content` as a JSON object. parse_event guarantees this
/// shape, but validate_pdu doesn't depend on that — a hand-constructed
/// `Event` with non-object content gets a typed error here, not a panic.
fn content_as_object(content: &RawValue) -> Result<Map<String, Value>, FormatError> {
    match serde_json::from_str(content.get())? {
        Value::Object(map) => Ok(map),
        _ => Err(FormatError::InvalidFieldType {
            field: "content",
            expected: "object",
        }),
    }
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
    // recognised version, reject." We accept exactly the MSC4242 unstable
    // identifier — see `ROOM_VERSION_ID` for why we don't compare against
    // ruma's `RoomVersionId::V12`.
    if let Some(v) = content.get("room_version") {
        let s = v.as_str().ok_or(FormatError::InvalidFieldType {
            field: "content.room_version",
            expected: "string",
        })?;
        if s != ROOM_VERSION_ID {
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
    // an upfront-assert/per-arm-handler sync drift.
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
            "content": { "room_version": ROOM_VERSION_ID },
            "prev_events": [],
            "depth": 0,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc123" },
            "state_key": ""
        })
    }

    #[test]
    fn happy_path_message() {
        let ev =
            parse_event(raw(base_event()), eid("$ev1:example.org"), vec![]).expect("valid message");
        assert_eq!(ev.event_type, "m.room.message");
        assert_eq!(ev.prev_events.len(), 1);
        assert!(ev.state_key.is_none());
    }

    #[test]
    fn happy_path_create() {
        let ev = parse_event(raw(base_create()), eid("$create:example.org"), vec![])
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::AuthEventsPresent)
        ));
    }

    // ---------- F1 / F2: too many prev_(state_)events (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_prev_events_over_20() {
        let mut v = base_event();
        v["prev_events"] = json!(
            (0..21)
                .map(|i| format!("$p{i}:example.org"))
                .collect::<Vec<_>>()
        );
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::TooManyPrevEvents)
        ));
    }

    #[test]
    fn validate_pdu_rejects_prev_state_events_over_20() {
        let mut v = base_event();
        v["prev_state_events"] = json!(
            (0..21)
                .map(|i| format!("$p{i}:example.org"))
                .collect::<Vec<_>>()
        );
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::TooManyPrevStateEvents)
        ));
    }

    // ---------- F3 / F4: create has prev_(state_)events (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_create_with_prev_events() {
        let mut v = base_create();
        v["prev_events"] = json!(["$prev:example.org"]);
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::CreateHasPrevEvents)
        ));
    }

    #[test]
    fn validate_pdu_rejects_create_with_prev_state_events() {
        let mut v = base_create();
        v["prev_state_events"] = json!(["$prev:example.org"]);
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::CreateHasPrevStateEvents)
        ));
    }

    // ---------- F5: create has room_id (parse_event — wire-only check) ----------
    #[test]
    fn rejects_create_with_room_id() {
        let mut v = base_create();
        v["room_id"] = json!("!fake:example.org");
        assert!(matches!(
            parse_event(raw(v), eid("$create:example.org"), vec![]),
            Err(FormatError::CreateHasRoomId)
        ));
    }

    // ---------- F6: unrecognised room_version (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_unrecognised_room_version() {
        let mut v = base_create();
        v["content"] = json!({ "room_version": "11" });
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::UnrecognisedRoomVersion(_))
        ));
    }

    #[test]
    fn validate_pdu_accepts_create_without_room_version_field() {
        // Per Kegan's call on F6: "default value" handling is separate from
        // "is value valid". When room_version is absent we don't reject.
        let mut v = base_create();
        v["content"] = json!({});
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("absent room_version is permitted");
    }

    // ---------- F7: additional_creators (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_additional_creators_non_array() {
        let mut v = base_create();
        v["content"] =
            json!({ "room_version": ROOM_VERSION_ID, "additional_creators": "@bob:example.org" });
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::InvalidAdditionalCreators)
        ));
    }

    #[test]
    fn validate_pdu_rejects_additional_creators_with_bad_user_id() {
        let mut v = base_create();
        v["content"] =
            json!({ "room_version": ROOM_VERSION_ID, "additional_creators": ["not-a-user-id"] });
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::InvalidAdditionalCreators)
        ));
    }

    #[test]
    fn validate_pdu_accepts_additional_creators_valid() {
        let mut v = base_create();
        v["content"] = json!({
            "room_version": ROOM_VERSION_ID,
            "additional_creators": ["@bob:example.org", "@carol:example.org"]
        });
        let ev = parse_event(raw(v), eid("$create:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("valid additional_creators");
    }

    // ---------- F8: m.room.member missing parts (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_member_without_state_key() {
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["content"] = json!({ "membership": "join" });
        // state_key absent
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::MemberMissingStateKey)
        ));
    }

    #[test]
    fn validate_pdu_rejects_member_without_membership() {
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["state_key"] = json!("@alice:example.org");
        v["content"] = json!({});
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::MemberMissingMembership)
        ));
    }

    // ---------- F10: rule 9 (@-prefixed state_key must match sender) (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_at_state_key_mismatch() {
        let mut v = base_event();
        v["type"] = json!("m.some.thing");
        v["state_key"] = json!("@bob:example.org");
        // sender = @alice — mismatch
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::StateKeyAtSignSenderMismatch)
        ));
    }

    #[test]
    fn validate_pdu_accepts_at_state_key_matching_sender() {
        let mut v = base_event();
        v["type"] = json!("m.something");
        v["state_key"] = json!("@alice:example.org");
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("state_key matches sender");
    }

    #[test]
    fn validate_pdu_accepts_non_at_state_key() {
        let mut v = base_event();
        v["type"] = json!("m.room.topic");
        v["state_key"] = json!("");
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("empty state_key ok");
    }

    #[test]
    fn validate_pdu_rule_9_does_not_apply_to_m_room_member() {
        // Invite of @bob by @alice: state_key=@bob, sender=@alice — normal
        // membership operation. Rule 9 would reject naively; rule 5 owns
        // m.room.member end-to-end so rule 9 must skip it.
        let mut v = base_event();
        v["type"] = json!("m.room.member");
        v["state_key"] = json!("@bob:example.org");
        v["content"] = json!({ "membership": "invite" });
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("m.room.member with different state_key/sender is valid");
    }

    // ---------- F11 / F12 / F13: power_levels content (validate_pdu) ----------
    #[test]
    fn validate_pdu_rejects_power_levels_non_integer_int_field() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users_default": "high" });
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::PowerLevelsBadIntField("users_default"))
        ));
    }

    #[test]
    fn validate_pdu_rejects_power_levels_events_non_int_value() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "events": { "m.room.name": "yes" } });
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::PowerLevelsBadObjectField("events"))
        ));
    }

    #[test]
    fn validate_pdu_rejects_power_levels_users_bad_user_id() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users": { "not-a-user-id": 50 } });
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::PowerLevelsBadUsers)
        ));
    }

    #[test]
    fn validate_pdu_rejects_power_levels_users_non_int_value() {
        let mut v = base_event();
        v["type"] = json!("m.room.power_levels");
        v["state_key"] = json!("");
        v["content"] = json!({ "users": { "@alice:example.org": "boss" } });
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        assert!(matches!(
            validate_pdu(&ev),
            Err(FormatError::PowerLevelsBadUsers)
        ));
    }

    #[test]
    fn validate_pdu_accepts_valid_power_levels() {
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
        let ev = parse_event(raw(v), eid("$e:example.org"), vec![]).expect("wire ok");
        validate_pdu(&ev).expect("valid power_levels");
    }

    // ---------- F14: malformed IDs ----------
    #[test]
    fn rejects_malformed_sender() {
        let mut v = base_event();
        v["sender"] = json!("not-a-user-id");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("type"))
        ));
    }

    #[test]
    fn rejects_missing_sender() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("sender");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("sender"))
        ));
    }

    #[test]
    fn rejects_missing_content() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("content");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("content"))
        ));
    }

    #[test]
    fn ignores_depth_field_when_present() {
        // `depth` is silently accepted on the wire for interop with non-MSC4242
        // senders but never parsed into the `Event` struct. Both presence
        // (any integer) and absence parse the same.
        let mut with_depth = base_event();
        with_depth["depth"] = json!(42);
        let mut without_depth = base_event();
        without_depth.as_object_mut().unwrap().remove("depth");

        parse_event(raw(with_depth), eid("$e:example.org"), vec![]).expect("depth tolerated");
        parse_event(raw(without_depth), eid("$e:example.org"), vec![]).expect("missing depth ok");
    }

    #[test]
    fn rejects_missing_hashes() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("hashes");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("hashes"))
        ));
    }

    #[test]
    fn rejects_hashes_non_string_value() {
        let mut v = base_event();
        v["hashes"] = json!({ "sha256": 123 });
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("origin_server_ts"))
        ));
    }

    #[test]
    fn rejects_non_integer_origin_server_ts() {
        let mut v = base_event();
        v["origin_server_ts"] = json!("yesterday");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
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
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("prev_events"))
        ));
    }

    #[test]
    fn rejects_missing_prev_state_events_on_non_create() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("prev_state_events");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("prev_state_events"))
        ));
    }

    #[test]
    fn rejects_non_create_without_room_id() {
        let mut v = base_event();
        v.as_object_mut().unwrap().remove("room_id");
        assert!(matches!(
            parse_event(raw(v), eid("$e:example.org"), vec![]),
            Err(FormatError::MissingField("room_id"))
        ));
    }

    // ---------- signatures field is ignored ----------
    #[test]
    fn ignores_signatures_field_present() {
        let mut v = base_event();
        v["signatures"] = json!({ "example.org": { "ed25519:key": "sig" } });
        parse_event(raw(v), eid("$e:example.org"), vec![])
            .expect("signatures field accepted but not verified");
    }

    #[test]
    fn ignores_signatures_field_absent() {
        // base_event has no signatures field — still accepted.
        parse_event(raw(base_event()), eid("$e:example.org"), vec![])
            .expect("missing signatures accepted under trusted-network policy");
    }
}
