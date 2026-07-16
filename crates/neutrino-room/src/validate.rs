//! Room-scoped reference validation.
//!
//! `validate_references`: existential checks that require provider lookups
//! against a single room's DAG (v12 rule 2 + the MSC4242 `prev_state_events`
//! triad).
//!
//! Wire-format parsing (`parse_event`) and the provider-free semantic rules
//! (`validate_pdu`) are event-scoped and live in
//! [`neutrino_event::validate`]. `RoomCore::apply` runs `validate_pdu`
//! (event-scoped) and then `validate_references` (room-scoped) before the auth
//! checks.

use ruma::{OwnedEventId, OwnedRoomId};

use crate::provider::StateProvider;
use crate::{Event, ReferenceError};

/// Validate that everything this event refers to actually resolves.
///
/// Checks:
/// - **v12 rule 2**: the event's `room_id` is the event ID of an accepted
///   `m.room.create` event (with the sigil `!` instead of `$`).
/// - **MSC4242 prev_state_events triad**: each entry must exist in the store,
///   belong to the same room as this event, have a `state_key` (i.e. be a
///   state event), and not be rejected.
///
/// Create events bypass all checks: they introduce the room, they have no
/// `prev_state_events` (wire-format rule), and they are the create event whose
/// existence rule 2 demands.
pub fn validate_references(
    event: &Event,
    provider: &dyn StateProvider,
) -> Result<(), ReferenceError> {
    if event.event_type == "m.room.create" {
        return Ok(());
    }

    // v12 rule 2.
    require_room_grounded(&event.room_id, provider)?;

    // MSC4242 prev_state_events triad.
    for psid in &event.prev_state_events {
        let ps = provider
            .get_event(psid)?
            .ok_or_else(|| ReferenceError::PrevStateNotFound(psid.clone()))?;
        if ps.rejected {
            return Err(ReferenceError::PrevStateRejected(psid.clone()));
        }
        if ps.state_key.is_none() {
            return Err(ReferenceError::PrevStateNotStateEvent(psid.clone()));
        }
        if ps.room_id != event.room_id {
            return Err(ReferenceError::PrevStateDifferentRoom(psid.clone()));
        }
    }

    Ok(())
}

/// Ground a non-create event's `room_id` to its `m.room.create` event and
/// validate it (v12 rule 2). Shared by [`validate_references`] and the
/// `RoomCore::apply_pdu` rejected short-circuit so the two cannot drift: both
/// must reject an event whose room's create is missing, rejected, or
/// malformed, and both must treat an unfetched create as retryable.
///
/// Errors mirror the rule-2 dispositions: [`ReferenceError::MalformedRoomId`]
/// (room_id yields no create id — DROP), [`ReferenceError::UnknownRoom`]
/// (create not fetched yet — RETRY), [`ReferenceError::RoomRejected`] (the
/// create is itself rejected), and [`ReferenceError::RoomTypeMismatch`] (the
/// event at the derived id is not a well-formed `m.room.create` — wrong type
/// or a non-empty `state_key`).
pub(crate) fn require_room_grounded(
    room_id: &OwnedRoomId,
    provider: &dyn StateProvider,
) -> Result<(), ReferenceError> {
    let create_id = derive_create_event_id(room_id)
        .ok_or_else(|| ReferenceError::MalformedRoomId(room_id.clone()))?;
    let create = provider
        .get_event(&create_id)?
        .ok_or_else(|| ReferenceError::UnknownRoom(room_id.clone()))?;
    if create.rejected {
        return Err(ReferenceError::RoomRejected(room_id.clone()));
    }
    // A well-formed v12 create is `m.room.create` with `state_key == ""`.
    if create.event_type != "m.room.create" || create.state_key.as_deref() != Some("") {
        return Err(ReferenceError::RoomTypeMismatch(create_id));
    }
    Ok(())
}

/// Derive the create event's ID from a v12 room_id by swapping the `!` sigil
/// for `$`. Returns `None` if the room_id is somehow malformed (shouldn't
/// happen for a value that already passed `OwnedRoomId` parsing, but the
/// graceful fallback keeps `validate_references` panic-free).
pub(crate) fn derive_create_event_id(room_id: &OwnedRoomId) -> Option<OwnedEventId> {
    let rest = room_id.as_str().strip_prefix('!')?;
    format!("${rest}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReferenceError;
    use crate::provider::InMemoryStateProvider;
    use neutrino_event::ROOM_VERSION_ID;
    use neutrino_event::validate::parse_event;
    use ruma::OwnedEventId;
    use serde_json::value::RawValue;
    use serde_json::{Value, json};
    use std::sync::Arc;

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

    /// Helper around `InMemoryStateProvider::insert` for the validate tests.
    /// `validate_references` only calls `get_event`, so the embedded
    /// `Event.auth_events` is irrelevant here (left empty by `parse_event`
    /// callers). `rejected` is set on the `Event` before insertion.
    fn insert_event(provider: &mut InMemoryStateProvider, mut event: Event, rejected: bool) {
        event.rejected = rejected;
        provider.insert(Arc::new(event));
    }

    fn make_event(json: Value, event_id: &str) -> Event {
        parse_event(raw(json), eid(event_id), vec![]).expect("test event valid")
    }

    fn make_create(event_id: &str) -> Event {
        make_event(base_create(), event_id)
    }

    fn make_message(room_id: &str, prev_state: Vec<&str>, event_id: &str) -> Event {
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

    fn make_state_event(room_id: &str, event_id: &str) -> Event {
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
        let provider = InMemoryStateProvider::new();
        validate_references(&create, &provider).expect("create event bypasses ref checks");
    }

    #[test]
    fn refs_happy_path_known_room() {
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        validate_references(&msg, &provider).expect("known room");
    }

    // v12 rule 2: unknown room
    #[test]
    fn refs_unknown_room_rejected() {
        let provider = InMemoryStateProvider::new();
        let msg = make_message("!doesnotexist:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::UnknownRoom(_))
        ));
    }

    // v12 rule 2: create event is rejected
    #[test]
    fn refs_rejected_create_rejects_event() {
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), true);
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::RoomRejected(_))
        ));
    }

    // v12 rule 2 defensive: derived id resolves to a non-create event.
    #[test]
    fn refs_non_create_at_derived_id_rejected() {
        let mut provider = InMemoryStateProvider::new();
        // Store a non-create event at id "$create:example.org".
        insert_event(
            &mut provider,
            make_state_event("!somewhere:example.org", "$create:example.org"),
            false,
        );
        let msg = make_message("!create:example.org", vec![], "$msg:example.org");
        assert!(matches!(
            validate_references(&msg, &provider),
            Err(ReferenceError::RoomTypeMismatch(_))
        ));
    }

    // MSC4242 triad: prev_state_event not in store
    #[test]
    fn refs_prev_state_not_found_rejected() {
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
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
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
        insert_event(
            &mut provider,
            make_state_event("!create:example.org", "$rejected:example.org"),
            true,
        );
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
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
        // make_message() produces an m.room.message without state_key.
        let non_state = make_message("!create:example.org", vec![], "$msg-ref:example.org");
        insert_event(&mut provider, non_state, false);
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
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
        // A state event whose room_id is a different room.
        insert_event(
            &mut provider,
            make_state_event("!other:example.org", "$other-state:example.org"),
            false,
        );
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
        let mut provider = InMemoryStateProvider::new();
        insert_event(&mut provider, make_create("$create:example.org"), false);
        insert_event(
            &mut provider,
            make_state_event("!create:example.org", "$state1:example.org"),
            false,
        );
        insert_event(
            &mut provider,
            make_state_event("!create:example.org", "$state2:example.org"),
            false,
        );
        let msg = make_message(
            "!create:example.org",
            vec!["$state1:example.org", "$state2:example.org"],
            "$msg:example.org",
        );
        validate_references(&msg, &provider).expect("all references valid");
    }
}
