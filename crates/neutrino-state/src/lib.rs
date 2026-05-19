use std::collections::HashMap;

use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::value::RawValue;
use thiserror::Error;

pub mod auth_events;
pub mod provider;
pub mod validate;

/// Room version supported by this state machine.
///
/// Only v12 is supported (see `CLAUDE.md`). MSC4242 state DAG semantics
/// (`prev_state_events`) are assumed throughout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomVersion {
    V12,
}

/// Resolved room state: one entry per `(event_type, state_key)` pair.
pub type StateMap<V> = HashMap<(String, String), V>;

/// Parsed view of a Matrix event plus the original canonical JSON.
///
/// Constructed by the format-validation pass (phase 1). Under MSC4242 the
/// wire format does not carry `auth_events`; the auth-events set is
/// calculated server-side from state-before-event when needed.
#[derive(Debug)]
pub struct Event {
    pub event_id: OwnedEventId,
    pub room_id: OwnedRoomId,
    pub sender: OwnedUserId,
    pub event_type: String,
    pub state_key: Option<String>,
    pub origin_server_ts: u64,
    pub content: Box<RawValue>,
    pub prev_events: Vec<OwnedEventId>,
    /// MSC4242: state-DAG parents of this event.
    pub prev_state_events: Vec<OwnedEventId>,
    /// Original full event JSON, preserved so the event can be re-emitted
    /// byte-for-byte and so redaction can rewrite it later.
    pub raw: Box<RawValue>,
}

/// Errors raised by format validation (phase 1) — wire-format violations that
/// reject the event outright, before any state lookup happens.
///
/// Variants are added by the phase that produces them.
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

    /// PDU schema: `depth` ≥ 2^53 − 1.
    #[error("depth out of range: must be < 2^53 - 1")]
    DepthOutOfRange,

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

/// Errors raised by the v12 authorization rules (phase 3).
///
/// Variants are added by the phase that produces them.
#[derive(Debug, Error)]
pub enum AuthError {}

/// Errors raised by reference validation (phase 1b) — the event's references
/// (room, `prev_state_events`) must exist in the store and meet their
/// preconditions.
#[derive(Debug, Error)]
pub enum ReferenceError {
    /// v12 rule 2: the event's `room_id` does not correspond to any known
    /// `m.room.create` event.
    #[error("room not known: no create event with id derived from {0}")]
    UnknownRoom(OwnedRoomId),

    /// v12 rule 2: the referenced create event exists but is rejected.
    #[error("room's create event is rejected: {0}")]
    RoomRejected(OwnedRoomId),

    /// v12 rule 2 (defensive): the event found at the derived create-event ID
    /// is not actually an `m.room.create` event. Should be impossible for a
    /// well-formed store but worth the explicit reject path.
    #[error("event at derived create id is not m.room.create: {0}")]
    RoomTypeMismatch(OwnedEventId),

    /// MSC4242: "If there are entries which were themselves rejected under
    /// the checks performed on receipt of a PDU, reject."
    #[error("prev_state_event is rejected: {0}")]
    PrevStateRejected(OwnedEventId),

    /// MSC4242: a `prev_state_events` entry is not present in the store.
    /// Synapse's recovery path is `/get_missing_events` with `state_dag: true`;
    /// here we surface a typed error and let the caller decide.
    #[error("prev_state_event not in store: {0}")]
    PrevStateNotFound(OwnedEventId),

    /// MSC4242: "If there are entries which do not have a `state_key`,
    /// reject." (i.e. the referenced event is not a state event)
    #[error("prev_state_event is not a state event: {0}")]
    PrevStateNotStateEvent(OwnedEventId),

    /// MSC4242: "If there are entries which do not belong in the same room,
    /// reject."
    #[error("prev_state_event belongs to a different room: {0}")]
    PrevStateDifferentRoom(OwnedEventId),
}

/// Top-level error type returned by the state machine.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Reference(#[from] ReferenceError),
    #[error(transparent)]
    Auth(#[from] AuthError),
}
