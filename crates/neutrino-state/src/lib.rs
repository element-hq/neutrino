use std::collections::{BTreeMap, HashMap};

use ruma::canonical_json::CanonicalJsonError;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use thiserror::Error;

pub mod auth_events;
pub mod auth_rules;
pub mod event_id;
pub mod provider;
pub mod room_core;
pub mod state_res;
pub mod validate;

#[cfg(test)]
pub(crate) mod test_utils {
    //! Shared helpers for `#[cfg(test)]` fixtures across the crate. Single
    //! definition of `next_ts()` so different test modules can't desync
    //! their counters or duplicate the AtomicU64 logic.
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Monotonically increasing test-fixture timestamp. Different test modules
    /// share the same counter — fine because fixtures don't pin literal
    /// values, only their relative ordering matters. Starts well past v12
    /// release so absolute values are plausible to a future debugger.
    pub fn next_ts() -> u64 {
        static TS: AtomicU64 = AtomicU64::new(1_700_000_000_000);
        TS.fetch_add(1, Ordering::Relaxed)
    }
}

// The canonical `Event` type and the `RoomVersion` enum live in
// `neutrino-common` so storage and state-machine code share them. See
// `event-id-design.md` for the rationale.
pub use neutrino_common::Event;

/// Resolved room state: one entry per `(event_type, state_key)` pair.
pub type StateMap<V> = HashMap<(String, String), V>;

/// A change to apply to the resolved current state, keyed by
/// `(event_type, state_key)`. `Some(id)` sets or replaces the pointer for
/// that key (the referenced event is already persisted); `None` removes the
/// key entirely. Emitted by `RoomCore::apply` as the delta between the old
/// and recomputed current state — the persist layer applies it verbatim. A
/// `BTreeMap` (not `HashMap`) so the iteration order the persist layer sees
/// is deterministic.
pub type StateDelta = BTreeMap<(String, String), Option<OwnedEventId>>;

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

/// Errors raised by the v12 authorization rules.
///
/// Variant names track the spec rule number (e.g. `Rule5_3_2_SenderMismatch`
/// names the 5.3.2 reject path). Spec rules 1, 2, 9, 10.1–10.3 are
/// not here — they raise `FormatError` / `ReferenceError` instead.
#[allow(non_camel_case_types)] // variant names track v12 spec rule numbers
#[derive(Debug, Error)]
pub enum AuthError {
    /// The room's `m.room.create` event could not be resolved while building
    /// the auth context — neither present in the supplied state nor fetchable
    /// from the provider (a corrupt store, or a `room_id` with no create). An
    /// event whose room create is unresolvable cannot be authorized. v12
    /// excludes create from `auth_events`, so `AuthContext::new` derives it
    /// from the `room_id`; this is the failure of that derivation.
    #[error("m.room.create for the room could not be resolved")]
    CreateUnavailable,

    /// `m.room.member` state_key did not parse as a Matrix user id (rule 9
    /// exempts `m.room.member` from the `@`-state_key format check, so rule 5
    /// is the first place this is detected). Raised by 5.4 / 5.5 / 5.6.
    #[error("m.room.member state_key `{state_key}` is not a valid user id")]
    InvalidMemberStateKey { state_key: String },

    /// Rule 4: federation cross-domain mismatch with `m.federate: false`.
    #[error(
        "rule 4: m.federate=false but sender domain `{sender_domain}` ≠ create domain `{create_domain}`"
    )]
    Rule4FederationDisallowed {
        sender_domain: String,
        create_domain: String,
    },

    /// Rule 5.3.2: join with `sender` ≠ `state_key`.
    #[error("rule 5.3.2: join sender does not match state_key")]
    Rule5_3_2_JoinSenderMismatch,
    /// Rule 5.3.3: join while banned.
    #[error("rule 5.3.3: join attempted while sender is banned")]
    Rule5_3_3_JoinWhileBanned,
    /// Rule 5.3.4: invite/knock join_rule but sender not invited or already in
    /// a state that permits join.
    #[error("rule 5.3.4: invite/knock join_rule disallows join from current membership")]
    Rule5_3_4_JoinNotInvited,
    /// Rule 5.3.5: restricted/knock_restricted join_rule and authoriser is
    /// invalid (not a joined member or lacks invite power).
    #[error("rule 5.3.5: restricted-join authoriser is invalid")]
    Rule5_3_5_RestrictedAuthoriserInvalid,
    /// Rule 5.3.7: catch-all for `join` rejection.
    #[error("rule 5.3.7: join not allowed by current join_rule")]
    Rule5_3_7_JoinNotAllowed,

    /// Rule 5.4.1.*: third-party invite path failed validation.
    #[error("rule 5.4.1: third-party invite invalid: {0}")]
    Rule5_4_1_ThirdPartyInviteInvalid(&'static str),
    /// Rule 5.4.2: invite sender not joined.
    #[error("rule 5.4.2: invite sender is not joined")]
    Rule5_4_2_InviteSenderNotJoined,
    /// Rule 5.4.3: invite target already joined or banned.
    #[error("rule 5.4.3: invite target is already joined or banned")]
    Rule5_4_3_InviteTargetJoinedOrBanned,
    /// Rule 5.4.5: invite sender power below the invite level.
    #[error("rule 5.4.5: invite sender power {sender} below invite level {needed}")]
    Rule5_4_5_InvitePowerInsufficient { sender: i64, needed: i64 },

    /// Rule 5.5.1: self-leave from a membership that isn't invite/join/knock.
    #[error("rule 5.5.1: self-leave from invalid current membership")]
    Rule5_5_1_SelfLeaveInvalid,
    /// Rule 5.5.2: leave (kick) sender not joined.
    #[error("rule 5.5.2: leave sender is not joined")]
    Rule5_5_2_LeaveSenderNotJoined,
    /// Rule 5.5.3: unban attempted but sender's power below ban level.
    #[error("rule 5.5.3: unban requires power {needed}, sender has {sender}")]
    Rule5_5_3_UnbanInsufficient { sender: i64, needed: i64 },
    /// Rule 5.5.5: kick not permitted — sender lacks kick power or target
    /// outranks sender. Payload distinguishes the two conjuncts.
    #[error(
        "rule 5.5.5: kick not permitted (sender power {sender}, target power {target}, kick level {kick_level})"
    )]
    Rule5_5_5_KickNotAllowed {
        sender: i64,
        target: i64,
        kick_level: i64,
    },

    /// Rule 5.6.1: ban sender not joined.
    #[error("rule 5.6.1: ban sender is not joined")]
    Rule5_6_1_BanSenderNotJoined,
    /// Rule 5.6.3: ban not permitted — sender lacks ban power or target
    /// outranks sender. Payload distinguishes the two conjuncts.
    #[error(
        "rule 5.6.3: ban not permitted (sender power {sender}, target power {target}, ban level {ban_level})"
    )]
    Rule5_6_3_BanNotAllowed {
        sender: i64,
        target: i64,
        ban_level: i64,
    },

    /// Rule 5.7.1: knock attempted on a non-knockable room.
    #[error("rule 5.7.1: knock attempted on a non-knockable room")]
    Rule5_7_1_KnockNotKnockable,
    /// Rule 5.7.2: knock sender ≠ state_key.
    #[error("rule 5.7.2: knock sender does not match state_key")]
    Rule5_7_2_KnockSenderMismatch,
    /// Rule 5.7.4: knock from an invalid current membership (ban / invite / join).
    #[error("rule 5.7.4: knock from invalid current membership")]
    Rule5_7_4_KnockFromInvalidMembership,

    /// Rule 5.8: membership is not one of the recognised values.
    #[error("rule 5.8: unknown membership `{0}`")]
    Rule5_8_UnknownMembership(String),

    /// Rule 6: non-member event from a non-joined sender.
    #[error("rule 6: sender is not joined")]
    Rule6_SenderNotJoined,

    /// Rule 7: third-party invite below invite level.
    #[error("rule 7: third_party_invite below invite level (sender {sender}, needed {needed})")]
    Rule7_ThirdPartyInviteInsufficient { sender: i64, needed: i64 },

    /// Rule 8: sender lacks required power for this event type.
    #[error("rule 8: event `{event_type}` requires power {required}, sender has {sender}")]
    Rule8_RequiredPowerInsufficient {
        event_type: String,
        required: i64,
        sender: i64,
    },

    /// Rule 10.4: power_levels lists a creator in `users`.
    #[error("rule 10.4: power_levels.users names a creator (`{0}`)")]
    Rule10_4_CreatorInUsers(OwnedUserId),
    /// Rule 10.6.1: default-level current value above sender.
    #[error("rule 10.6.1: default `{property}` current value {value} above sender power {sender}")]
    Rule10_6_1_CurrentDefaultAboveSender {
        property: String,
        value: i64,
        sender: i64,
    },
    /// Rule 10.6.2: default-level new value above sender.
    #[error("rule 10.6.2: default `{property}` new value {value} above sender power {sender}")]
    Rule10_6_2_NewDefaultAboveSender {
        property: String,
        value: i64,
        sender: i64,
    },
    /// Rule 10.7: events/notifications entry change/removal above sender.
    #[error("rule 10.7: `{property}.{key}` current value {value} above sender power {sender}")]
    Rule10_7_CurrentEventEntryAboveSender {
        property: String,
        key: String,
        value: i64,
        sender: i64,
    },
    /// Rule 10.8: events/notifications entry addition/change above sender.
    #[error("rule 10.8: `{property}.{key}` new value {value} above sender power {sender}")]
    Rule10_8_NewEventEntryAboveSender {
        property: String,
        key: String,
        value: i64,
        sender: i64,
    },
    /// Rule 10.9: users entry change/removal at or above sender (non-self).
    #[error("rule 10.9: `users.{user}` current value {value} ≥ sender power {sender}")]
    Rule10_9_CurrentUsersEntryAtOrAboveSender {
        user: OwnedUserId,
        value: i64,
        sender: i64,
    },
    /// Rule 10.10: users entry addition/change above sender.
    #[error("rule 10.10: `users.{user}` new value {value} above sender power {sender}")]
    Rule10_10_NewUsersEntryAboveSender {
        user: OwnedUserId,
        value: i64,
        sender: i64,
    },
}

/// Errors raised by reference validation — the event's references
/// (room, `prev_state_events`) must exist in the store and meet their
/// preconditions.
#[derive(Debug, Error)]
pub enum ReferenceError {
    /// The event's `room_id` is not a well-formed v12 room id, so no create
    /// event id can be derived from it. A property of the event itself, not
    /// of the store: re-applying after backfill cannot change the outcome,
    /// so this is a DROP (non-retryable, never persisted), distinct from
    /// [`Self::UnknownRoom`] (create simply not fetched yet — retryable).
    #[error("malformed room_id, cannot derive create event id: {0}")]
    MalformedRoomId(OwnedRoomId),

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

    /// The store failed to serve a lookup while resolving references (SQL /
    /// hydration fault). Not a verdict about the event — surfaced as a fault
    /// so a transient error is never mistaken for an unknown room or a
    /// missing `prev_state_event`.
    #[error("event lookup failed during reference validation: {0}")]
    Lookup(#[from] StateResError),
}

/// Errors raised by state resolution and the state-DAG orchestration.
#[derive(Debug, Error)]
pub enum StateResError {
    /// An event referenced by state-res or DAG walks is not in the store.
    /// The reference may be from a seed set, an `auth_events` traversal,
    /// or a `prev_state_events` walk. Project invariant: every event we
    /// know about must have its complete ancestry (auth chain AND
    /// state-DAG predecessors) locally resolvable; a missing entry
    /// indicates corruption or a write-path bug, never a normal backfill
    /// boundary (we don't federate backfill — every event is authored
    /// locally or arrives with its full chain).
    #[error("state-res references unknown event: {0}")]
    MissingEvent(OwnedEventId),
    /// Storage-side fault while serving a provider lookup — SQL driver
    /// error, malformed row, JSON serialisation, anything that isn't a
    /// missing-event signal. In-memory providers never produce this; the
    /// SQLite-backed provider surfaces driver / hydration faults here so
    /// state-res can bubble them up the call stack rather than panic.
    #[error("state-res internal error: {0}")]
    Internal(String),
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
    #[error(transparent)]
    StateRes(#[from] StateResError),
    /// `RoomCore::apply_pdu` was called with an event whose `room_id` differs
    /// from the `RoomCore`'s tracked room. Caller dispatched to the wrong
    /// per-room state machine; nothing on this `RoomCore` is mutated.
    #[error("event room_id `{actual}` does not match RoomCore room_id `{expected}`")]
    RoomMismatch {
        expected: OwnedRoomId,
        actual: OwnedRoomId,
    },
}

impl CoreError {
    /// True when the failure means "we lack data" rather than "the event is
    /// bad": missing ancestry (a `prev_state_events` entry or auth-chain link
    /// not yet in the store, or an unknown room) or a transient storage fault.
    /// The federation `/send` handler should backfill the gap and re-apply
    /// rather than drop the event.
    ///
    /// This is the RETRY half of the disposition split documented on
    /// [`RoomCore::apply_pdu`](crate::room_core::RoomCore::apply_pdu). It is
    /// deliberately distinct from REJECT (an evaluable event that fails a
    /// rule — returned as `Ok` with `Event.rejected`, never as an `Err`) and
    /// from DROP (`RoomMismatch` / `Format`, a malformed-or-misrouted event
    /// that is neither retryable nor persisted).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            CoreError::Reference(
                ReferenceError::PrevStateNotFound(_)
                    | ReferenceError::UnknownRoom(_)
                    | ReferenceError::Lookup(_),
            ) | CoreError::StateRes(_)
        )
    }
}

#[cfg(test)]
mod core_error_tests {
    use super::*;

    fn eid() -> OwnedEventId {
        // v12 event id: `$` + base64url hash, no `:server` suffix.
        "$Fw7pQdLu79h74bsZabn1UKXoXo7-q5M-cOwQxQxfh2c"
            .parse()
            .expect("event id")
    }

    fn rid() -> OwnedRoomId {
        // v12 room id: `!` + the create event's hash, no `:server` suffix.
        "!Fw7pQdLu79h74bsZabn1UKXoXo7-q5M-cOwQxQxfh2c"
            .parse()
            .expect("room id")
    }

    #[test]
    fn retryable_covers_missing_ancestry_and_faults() {
        // RETRY: we lack data — backfill and re-apply.
        assert!(CoreError::Reference(ReferenceError::PrevStateNotFound(eid())).is_retryable());
        assert!(CoreError::Reference(ReferenceError::UnknownRoom(rid())).is_retryable());
        assert!(
            CoreError::Reference(ReferenceError::Lookup(StateResError::MissingEvent(eid())))
                .is_retryable()
        );
        assert!(CoreError::StateRes(StateResError::MissingEvent(eid())).is_retryable());
        assert!(CoreError::StateRes(StateResError::Internal("db".into())).is_retryable());
    }

    #[test]
    fn non_retryable_covers_reject_and_drop_classes() {
        // REJECT-class references (bad data, not missing) and DROP (misrouted)
        // are not retryable — re-applying would not change the verdict.
        assert!(!CoreError::Reference(ReferenceError::PrevStateRejected(eid())).is_retryable());
        assert!(
            !CoreError::Reference(ReferenceError::PrevStateNotStateEvent(eid())).is_retryable()
        );
        assert!(
            !CoreError::Reference(ReferenceError::PrevStateDifferentRoom(eid())).is_retryable()
        );
        assert!(
            !CoreError::RoomMismatch {
                expected: rid(),
                actual: rid(),
            }
            .is_retryable()
        );
    }
}
