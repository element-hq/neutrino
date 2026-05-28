//! Phase 6: per-room state machine orchestration.
//!
//! `RoomCore::apply(event, provider)` is the keystone — it wires together
//! everything from Phases 1–5 to integrate a single incoming event:
//!
//! 1. Reference validation (`validate::validate_references`, Phase 1b).
//! 2. State-before-event via `state_res::state_before_for_new_event` over
//!    the event's `prev_state_events`.
//! 3. Auth check against state-before-event (`auth_rules::check_auth_rules`,
//!    Phase 3). Failure → hard reject (error, no effects).
//! 4. If the event is a state event AND accepted: update forward extremities,
//!    recompute current_state by state-resolving across the new FE set.
//! 5. Second auth check against the (possibly updated) current_state.
//!    Failure → mark soft-failed. State update is NOT undone (matches synapse
//!    `_check_for_soft_fail`).
//! 6. Emit `Vec<Effect>` describing what storage and federation should do.
//!
//! Format validation (Phase 1a / `parse_event`) is upstream of `apply` — by
//! the time the caller has an `Event` value, the wire shape is already known
//! good (events come from `EventBuilder::build()` or `Event::from_wire`,
//! both of which run `parse_event` internally).

use std::collections::BTreeSet;
use std::sync::Arc;

use neutrino_common::Event;
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};

use crate::auth_rules::check_auth_rules;
use crate::provider::StateProvider;
use crate::state_res;
use crate::validate;
use crate::{CoreError, StateMap, StateResError};

/// Side-effects emitted by `RoomCore::apply`. The caller (storage and
/// federation layers) interprets them sequentially in emission order.
///
/// Acceptance path (event passes both reference validation and the
/// state-before-event auth check) emits at minimum `Persist`; state events
/// additionally emit `UpdateCurrentState`. Soft-fail (auth-against-
/// current-state failure on an otherwise-accepted event) emits
/// `MarkSoftFailed`. Federation outbox enqueue is deferred until a sender
/// implementation lands.
#[derive(Debug, Clone)]
pub enum Effect {
    /// Persist the event to storage. Always emitted for accepted events.
    Persist(Arc<Event>),
    /// Mark a previously-persisted event as soft-failed. Soft-failed events
    /// stay in storage and DO NOT regress the state update they triggered,
    /// but they're not relayed to clients (Matrix soft-fail semantics).
    MarkSoftFailed(OwnedEventId),
    /// Replace `current_state` with this resolved state map. Emitted only
    /// when the accepted event is a state event.
    UpdateCurrentState(StateMap<OwnedEventId>),
    /// Enqueue the event for federation send to `destinations`. Currently
    /// emitted with an empty `destinations` list — the federation outbox
    /// wiring is part of the SSAPI work and will populate this from the
    /// post-update current_state.
    #[allow(dead_code)] // not yet emitted by `apply`; placeholder for SSAPI work
    EnqueueOutbox {
        event: Arc<Event>,
        destinations: Vec<OwnedServerName>,
    },
}

/// Per-room state machine state: forward extremities of the state DAG and
/// the current resolved state. `apply` mutates both as it accepts events.
///
/// Cheap to clone — events are `Arc`-shared. State groups are deferred per
/// the project plan; until they land, `current_state` is materialised
/// directly as `StateMap<Arc<Event>>` for fast auth-check lookups.
#[derive(Debug, Clone)]
pub struct RoomCore {
    pub(crate) room_id: OwnedRoomId,
    pub(crate) state_forward_extremities: BTreeSet<OwnedEventId>,
    pub(crate) current_state: Arc<StateMap<Arc<Event>>>,
}

impl RoomCore {
    /// Create a fresh `RoomCore` for `room_id`. Starts with empty forward
    /// extremities and empty current_state — the first `apply` call is
    /// expected to be the room's create event.
    pub fn new(room_id: OwnedRoomId) -> Self {
        Self {
            room_id,
            state_forward_extremities: BTreeSet::new(),
            current_state: Arc::new(StateMap::new()),
        }
    }

    /// The room this state machine is tracking.
    pub fn room_id(&self) -> &ruma::RoomId {
        &self.room_id
    }

    /// Current forward extremities of the state DAG. Read-only view; mutation
    /// is the exclusive responsibility of `apply`.
    pub fn state_forward_extremities(&self) -> &BTreeSet<OwnedEventId> {
        &self.state_forward_extremities
    }

    /// Current resolved state. Read-only; mutate only through `apply`.
    pub fn current_state(&self) -> &Arc<StateMap<Arc<Event>>> {
        &self.current_state
    }

    /// Integrate a single event into the room.
    ///
    /// Returns the list of effects (in emission order) the caller should
    /// process. On hard-reject paths (`ReferenceError`, `AuthError`, etc.)
    /// returns `Err(CoreError)` and emits no effects — the event must not
    /// be persisted.
    ///
    /// Note: `apply` does NOT insert the event into `provider`. The caller
    /// is expected to honour `Effect::Persist` by writing through to storage
    /// (which doubles as the provider for subsequent `apply` calls).
    pub fn apply(
        &mut self,
        event: Event,
        provider: &dyn StateProvider,
    ) -> Result<Vec<Effect>, CoreError> {
        let event = Arc::new(event);

        // (1) Reference validation: rooms exist, prev_state_events are state
        // events in the same room, not rejected.
        validate::validate_references(&event, provider)?;

        // (2) State-before-event by state-resolving over state-after of each
        // prev_state_event. Empty for create events (no prev_state_events).
        let state_before_ids = state_res::state_before_for_new_event(&event, provider)?;
        let state_before_events = materialize_state(&state_before_ids, provider)?;

        // (3) Auth against state-before-event. Hard reject → propagate error.
        check_auth_rules(&event, &state_before_events)?;

        let mut effects: Vec<Effect> = vec![Effect::Persist(event.clone())];

        // (4) State-event acceptance path: compute new FEs + new current_state
        // into LOCAL bindings. Only commit to `self` after every fallible call
        // has succeeded — keeps RoomCore consistent on the error path (no
        // partial mutation if e.g. `recompute_current_state` errors after FE
        // mutation). See review I1.
        let is_state_event = event.state_key.is_some();
        let auth_check_state: StateMap<Arc<Event>> = if is_state_event {
            let mut new_fes = self.state_forward_extremities.clone();
            for parent in &event.prev_state_events {
                new_fes.remove(parent);
            }
            new_fes.insert(event.event_id.clone());

            let new_current_ids = recompute_current_state(&new_fes, provider, Some(event.clone()))?;
            let new_current_events =
                materialize_state_with_local(&new_current_ids, provider, Some(event.clone()))?;

            // Commit. All fallible work above is done.
            self.state_forward_extremities = new_fes;
            self.current_state = Arc::new(new_current_events.clone());
            effects.push(Effect::UpdateCurrentState(new_current_ids));
            new_current_events
        } else {
            (*self.current_state).clone()
        };

        // (5) Second auth check against the (possibly updated) current_state.
        // Failure → soft-fail. Does NOT undo the state update (synapse parity).
        if check_auth_rules(&event, &auth_check_state).is_err() {
            effects.push(Effect::MarkSoftFailed(event.event_id.clone()));
        }

        // (6) Effects emitted; caller persists.
        Ok(effects)
    }
}

/// Materialise a `StateMap<OwnedEventId>` into a `StateMap<Arc<Event>>` by
/// looking up each event_id in `provider`. Errors if any id is missing
/// (project closure invariant).
fn materialize_state(
    ids: &StateMap<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<StateMap<Arc<Event>>, CoreError> {
    let mut out = StateMap::new();
    for (key, id) in ids {
        let info = provider
            .get_event(id)
            .ok_or_else(|| CoreError::StateRes(StateResError::MissingAuthEvent(id.clone())))?;
        out.insert(key.clone(), info.event);
    }
    Ok(out)
}

/// Like `materialize_state` but treats `local_event` as resolvable in
/// addition to the provider — needed during `apply` because the new event
/// hasn't been persisted yet but may appear as a value in the recomputed
/// current_state (when it's a state event and is the latest FE for its key).
fn materialize_state_with_local(
    ids: &StateMap<OwnedEventId>,
    provider: &dyn StateProvider,
    local_event: Option<Arc<Event>>,
) -> Result<StateMap<Arc<Event>>, CoreError> {
    let mut out = StateMap::new();
    for (key, id) in ids {
        if let Some(local) = &local_event
            && &local.event_id == id
        {
            out.insert(key.clone(), local.clone());
            continue;
        }
        let info = provider
            .get_event(id)
            .ok_or_else(|| CoreError::StateRes(StateResError::MissingAuthEvent(id.clone())))?;
        out.insert(key.clone(), info.event);
    }
    Ok(out)
}

/// Recompute current_state by state-resolving across state-after of each
/// forward extremity. If `local_event` is provided, it's treated as
/// resolvable when it appears as an FE (used during `apply` before the
/// event is persisted).
fn recompute_current_state(
    forward_extremities: &BTreeSet<OwnedEventId>,
    provider: &dyn StateProvider,
    local_event: Option<Arc<Event>>,
) -> Result<StateMap<OwnedEventId>, CoreError> {
    if forward_extremities.is_empty() {
        return Ok(StateMap::new());
    }
    let mut state_sets: Vec<StateMap<OwnedEventId>> = Vec::with_capacity(forward_extremities.len());
    for fe in forward_extremities {
        let (state_after, fe_event) = if let Some(local) = &local_event
            && &local.event_id == fe
        {
            // The local (about-to-be-persisted) event is an FE. Compute
            // state-before via its prev_state_events directly.
            let sb = state_res::state_before_for_new_event(local, provider)?;
            (sb, local.clone())
        } else {
            let sb = state_res::state_before(fe, provider)?;
            let info = provider
                .get_event(fe)
                .ok_or_else(|| CoreError::StateRes(StateResError::MissingAuthEvent(fe.clone())))?;
            (sb, info.event)
        };
        let mut state_after = state_after;
        if let Some(sk) = &fe_event.state_key {
            state_after.insert(
                (fe_event.event_type.clone(), sk.clone()),
                fe_event.event_id.clone(),
            );
        }
        state_sets.push(state_after);
    }
    let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
    Ok(state_res::resolve_state(&refs, provider)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_id::EventBuilder;
    use crate::provider::{EventInfo, InMemoryStateProvider};
    use crate::test_utils::next_ts;
    use neutrino_common::ROOM_VERSION_ID;
    use neutrino_common::event_id::room_id_from_create;
    use serde_json::json;

    /// Build a create event by `creator` (no additional_creators, federate=true).
    fn create_event(creator: &str) -> Event {
        create_event_with(creator, true)
    }

    /// Build a create event with explicit `m.federate` flag.
    fn create_event_with(creator: &str, federate: bool) -> Event {
        let mut content = json!({ "room_version": ROOM_VERSION_ID });
        if !federate {
            content["m.federate"] = json!(false);
        }
        EventBuilder::new(creator.parse().expect("user"), "m.room.create".to_owned())
            .state_key(String::new())
            .content(content)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid create")
    }

    /// Build an `m.room.member` event. `prev_state_events` and `auth_events`
    /// share the same list for simplicity (apply only consults
    /// `prev_state_events` for state-before-event; auth_events is opaque to
    /// it). `prev_events` also matches so the rule-5.3.1 self-join shape
    /// (`prev_events == [create_id]`) can be expressed when the caller
    /// passes just the create id.
    fn member_event(
        sender: &str,
        target: &str,
        membership: &str,
        prev_state: Vec<OwnedEventId>,
        room: &ruma::RoomId,
    ) -> Event {
        EventBuilder::new(sender.parse().expect("user"), "m.room.member".to_owned())
            .room_id(room.to_owned())
            .state_key(target.to_owned())
            .content(json!({ "membership": membership }))
            .auth_events(prev_state.clone())
            .prev_events(prev_state.clone())
            .prev_state_events(prev_state)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid member")
    }

    /// Build an `m.room.power_levels` event.
    fn pl_event(
        sender: &str,
        content: serde_json::Value,
        prev_state: Vec<OwnedEventId>,
        room: &ruma::RoomId,
    ) -> Event {
        EventBuilder::new(
            sender.parse().expect("user"),
            "m.room.power_levels".to_owned(),
        )
        .room_id(room.to_owned())
        .state_key(String::new())
        .content(content)
        .auth_events(prev_state.clone())
        .prev_events(prev_state.clone())
        .prev_state_events(prev_state)
        .origin_server_ts(next_ts())
        .build()
        .expect("valid pl")
    }

    /// Drive a complete create → alice_join setup through `apply`. Returns
    /// the room, provider (with both events inserted), and the create +
    /// alice_join ids.
    fn alice_creates_and_joins(
        federate: bool,
    ) -> (
        RoomCore,
        InMemoryStateProvider,
        OwnedEventId,
        OwnedEventId,
        ruma::OwnedRoomId,
    ) {
        let create = Arc::new(create_event_with("@alice:example.org", federate));
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());
        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");
        let join = Arc::new(alice_join_event(&create_id, &room_id));
        let join_id = join.event_id.clone();
        room.apply((*join).clone(), &provider).expect("alice join");
        insert(&mut provider, join.clone());
        (room, provider, create_id, join_id, room_id)
    }

    /// Build alice's m.room.member join event, post-create, with the
    /// rule-5.3.1 shape: `prev_events == prev_state_events == [create_id]`.
    fn alice_join_event(create_id: &OwnedEventId, room: &ruma::RoomId) -> Event {
        EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room.to_owned())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join")
    }

    fn insert(provider: &mut InMemoryStateProvider, event: Arc<Event>) {
        provider.insert(EventInfo {
            event,
            rejected: false,
        });
    }

    // ----- apply (happy path) -----

    #[test]
    fn apply_create_event_initializes_room_state() {
        let create = create_event("@alice:example.org");
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let provider = InMemoryStateProvider::new();
        let mut room = RoomCore::new(room_id);

        let effects = room
            .apply(create.clone(), &provider)
            .expect("create accepted");

        assert_eq!(effects.len(), 2);
        assert!(matches!(&effects[0], Effect::Persist(e) if e.event_id == create_id));
        match &effects[1] {
            Effect::UpdateCurrentState(state) => {
                assert_eq!(state.len(), 1);
                assert_eq!(
                    state.get(&("m.room.create".to_string(), String::new())),
                    Some(&create_id)
                );
            }
            other => panic!("expected UpdateCurrentState, got {other:?}"),
        }
        // FE = the create event; current_state has only the create event.
        assert_eq!(room.state_forward_extremities.len(), 1);
        assert!(room.state_forward_extremities.contains(&create_id));
        assert_eq!(room.current_state.len(), 1);
    }

    #[test]
    fn apply_alice_join_updates_state_and_forward_extremities() {
        // Start: create already in room + provider. Apply alice's join.
        let create = Arc::new(create_event("@alice:example.org"));
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());

        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");

        // Caller honours Persist by also storing alice_join into the provider
        // — for these tests we insert manually before calling apply for the
        // next event.
        let join = alice_join_event(&create_id, &room_id);
        let join_id = join.event_id.clone();
        let effects = room.apply(join.clone(), &provider).expect("join accepted");
        insert(&mut provider, Arc::new(join));

        assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::UpdateCurrentState(_)))
        );
        // FE was {create_id} (parent removed because alice_join lists it in
        // prev_state_events), now {join_id}.
        assert_eq!(room.state_forward_extremities.len(), 1);
        assert!(room.state_forward_extremities.contains(&join_id));
        // current_state now has create + alice's member.
        assert_eq!(room.current_state.len(), 2);
        assert!(
            room.current_state
                .contains_key(&("m.room.create".to_string(), String::new()))
        );
        assert!(room.current_state.contains_key(&(
            "m.room.member".to_string(),
            "@alice:example.org".to_string()
        )));
    }

    #[test]
    fn apply_message_event_persists_but_does_not_update_state() {
        // Build a happy-path room: create + alice_join in both provider and
        // RoomCore. Then apply alice's m.room.message.
        let create = Arc::new(create_event("@alice:example.org"));
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());

        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");

        let join = Arc::new(alice_join_event(&create_id, &room_id));
        let join_id = join.event_id.clone();
        room.apply((*join).clone(), &provider).expect("join");
        insert(&mut provider, join.clone());

        let msg = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.message".to_owned(),
        )
        .room_id(room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "hi" }))
        .auth_events(vec![create_id.clone(), join_id.clone()])
        .prev_events(vec![join_id.clone()])
        .prev_state_events(vec![join_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid message");

        let pre_fe = room.state_forward_extremities.clone();
        let pre_state_len = room.current_state.len();
        let effects = room.apply(msg, &provider).expect("message accepted");

        // Message events emit Persist but NOT UpdateCurrentState.
        assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::UpdateCurrentState(_))),
            "non-state event must not update current_state"
        );
        // FEs unchanged.
        assert_eq!(room.state_forward_extremities, pre_fe);
        // current_state unchanged.
        assert_eq!(room.current_state.len(), pre_state_len);
    }

    // ----- apply (hard reject paths) -----

    #[test]
    fn apply_unknown_room_reference_errors() {
        // Build an alice_join referencing a create event that the provider
        // doesn't know. validate_references → ReferenceError::UnknownRoom.
        let phantom_create = create_event("@alice:example.org");
        let room_id = room_id_from_create(&phantom_create.event_id);
        let create_id = phantom_create.event_id.clone();
        // Provider has nothing — the create is NOT inserted.
        let provider = InMemoryStateProvider::new();
        let mut room = RoomCore::new(room_id.clone());

        let join = alice_join_event(&create_id, &room_id);
        let err = room.apply(join, &provider).expect_err("unknown room");
        assert!(matches!(err, CoreError::Reference(_)));
    }

    #[test]
    fn apply_auth_failure_does_not_mutate_room_core() {
        // Topic by a non-joined sender → rule 6 fails.
        let create = Arc::new(create_event("@alice:example.org"));
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());

        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");
        let snapshot_fe = room.state_forward_extremities.clone();
        let snapshot_state_len = room.current_state.len();

        // Bob hasn't joined; sending a topic should fail rule 6.
        let bob_topic = EventBuilder::new(
            "@bob:example.org".parse().expect("user"),
            "m.room.topic".to_owned(),
        )
        .room_id(room_id)
        .state_key(String::new())
        .content(json!({ "topic": "hi" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid topic");

        let err = room.apply(bob_topic, &provider).expect_err("auth fails");
        assert!(matches!(err, CoreError::Auth(_)));
        // RoomCore must not have mutated on the hard-reject path.
        assert_eq!(room.state_forward_extremities, snapshot_fe);
        assert_eq!(room.current_state.len(), snapshot_state_len);
    }

    // ----- apply: auth-rule integration (rules 4 / 5.4 / 5.5 / 5.6 / 8 / 10.4) -----

    #[test]
    fn rule_4_cross_domain_self_join_rejected_when_federate_false() {
        // Create has m.federate=false. bob@other.org attempts to self-join.
        // Rule 4 fires (sender_domain != create_domain && !federate) before
        // any rule-5 path → CoreError::Auth.
        let create = Arc::new(create_event_with("@alice:here.org", false));
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());
        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");

        let bob_self_join = member_event(
            "@bob:other.org",
            "@bob:other.org",
            "join",
            vec![create_id],
            &room_id,
        );
        let err = room.apply(bob_self_join, &provider).expect_err("rule 4");
        assert!(matches!(
            err,
            CoreError::Auth(crate::AuthError::Rule4FederationDisallowed { .. })
        ));
    }

    #[test]
    fn rule_5_4_invite_by_joined_creator_accepted() {
        let (mut room, provider, _create_id, alice_join_id, room_id) =
            alice_creates_and_joins(true);
        let invite = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            vec![alice_join_id],
            &room_id,
        );
        let effects = room.apply(invite, &provider).expect("invite accepted");
        assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::UpdateCurrentState(_)))
        );
        // bob's invite-state should now appear in current_state.
        assert!(
            room.current_state
                .contains_key(&("m.room.member".to_string(), "@bob:example.org".to_string()))
        );
    }

    #[test]
    fn rule_5_4_2_invite_by_non_joined_sender_rejected() {
        // charlie has never joined the room. charlie tries to invite bob —
        // rule 5.4.2 ("invite sender is not joined") fires.
        let (mut room, provider, _create_id, alice_join_id, room_id) =
            alice_creates_and_joins(true);
        let invite = member_event(
            "@charlie:example.org",
            "@bob:example.org",
            "invite",
            vec![alice_join_id],
            &room_id,
        );
        let err = room.apply(invite, &provider).expect_err("rule 5.4.2");
        assert!(matches!(
            err,
            CoreError::Auth(crate::AuthError::Rule5_4_2_InviteSenderNotJoined)
        ));
    }

    #[test]
    fn rule_5_5_creator_kicks_joined_user_accepted() {
        // Setup: create + alice_join + bob_invite + bob_join, then alice
        // kicks bob (m.room.member, sender=alice, target=bob, leave).
        // Rule 5 dispatches to 5.5 (leave); alice (creator → MAX power)
        // satisfies the kick conjuncts → accepted.
        let (mut room, mut provider, _, alice_join_id, room_id) = alice_creates_and_joins(true);
        let invite = Arc::new(member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            vec![alice_join_id.clone()],
            &room_id,
        ));
        let invite_id = invite.event_id.clone();
        room.apply((*invite).clone(), &provider).expect("invite");
        insert(&mut provider, invite.clone());

        let bob_join = Arc::new(member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            vec![invite_id.clone()],
            &room_id,
        ));
        let bob_join_id = bob_join.event_id.clone();
        room.apply((*bob_join).clone(), &provider)
            .expect("bob join");
        insert(&mut provider, bob_join.clone());

        let kick = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "leave",
            vec![bob_join_id],
            &room_id,
        );
        let effects = room.apply(kick, &provider).expect("kick accepted");
        assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
        // bob's membership entry now references the kick event.
        let bob_member_key = ("m.room.member".to_string(), "@bob:example.org".to_string());
        let bob_now = room
            .current_state
            .get(&bob_member_key)
            .expect("bob membership in state");
        // The kick is the latest member event for bob; verify content.
        let content: serde_json::Value =
            serde_json::from_str(bob_now.content.get()).expect("content");
        assert_eq!(
            content.get("membership").and_then(|v| v.as_str()),
            Some("leave")
        );
    }

    #[test]
    fn rule_5_6_creator_bans_joined_user_accepted() {
        let (mut room, mut provider, _, alice_join_id, room_id) = alice_creates_and_joins(true);
        let invite = Arc::new(member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            vec![alice_join_id.clone()],
            &room_id,
        ));
        let invite_id = invite.event_id.clone();
        room.apply((*invite).clone(), &provider).expect("invite");
        insert(&mut provider, invite.clone());

        let bob_join = Arc::new(member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            vec![invite_id],
            &room_id,
        ));
        let bob_join_id = bob_join.event_id.clone();
        room.apply((*bob_join).clone(), &provider)
            .expect("bob join");
        insert(&mut provider, bob_join.clone());

        let ban = member_event(
            "@alice:example.org",
            "@bob:example.org",
            "ban",
            vec![bob_join_id],
            &room_id,
        );
        let effects = room.apply(ban, &provider).expect("ban accepted");
        assert!(effects.iter().any(|e| matches!(e, Effect::Persist(_))));
        let bob_now = room
            .current_state
            .get(&("m.room.member".to_string(), "@bob:example.org".to_string()))
            .expect("bob membership in state");
        let content: serde_json::Value =
            serde_json::from_str(bob_now.content.get()).expect("content");
        assert_eq!(
            content.get("membership").and_then(|v| v.as_str()),
            Some("ban")
        );
    }

    #[test]
    fn rule_8_pl_event_by_low_power_sender_rejected() {
        // bob joins, then attempts to send a PL event. Default users_default
        // is 0, state_default is 50. bob's power = 0 < 50 → rule 8 rejects.
        let (mut room, mut provider, _, alice_join_id, room_id) = alice_creates_and_joins(true);
        let invite = Arc::new(member_event(
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            vec![alice_join_id.clone()],
            &room_id,
        ));
        let invite_id = invite.event_id.clone();
        room.apply((*invite).clone(), &provider).expect("invite");
        insert(&mut provider, invite.clone());

        let bob_join = Arc::new(member_event(
            "@bob:example.org",
            "@bob:example.org",
            "join",
            vec![invite_id],
            &room_id,
        ));
        let bob_join_id = bob_join.event_id.clone();
        room.apply((*bob_join).clone(), &provider)
            .expect("bob join");
        insert(&mut provider, bob_join.clone());

        // Bob attempts to set a PL.
        let bob_pl = pl_event(
            "@bob:example.org",
            json!({ "users_default": 99 }),
            vec![bob_join_id],
            &room_id,
        );
        let err = room.apply(bob_pl, &provider).expect_err("rule 8");
        assert!(matches!(
            err,
            CoreError::Auth(crate::AuthError::Rule8_RequiredPowerInsufficient { .. })
        ));
    }

    #[test]
    fn rule_10_4_pl_listing_creator_in_users_rejected() {
        // alice (creator) sends a PL listing herself in `users`. Rule 10.4
        // rejects: "power_levels.users names a creator".
        let (mut room, provider, _, alice_join_id, room_id) = alice_creates_and_joins(true);
        let bad_pl = pl_event(
            "@alice:example.org",
            json!({ "users": { "@alice:example.org": 50 } }),
            vec![alice_join_id],
            &room_id,
        );
        let err = room.apply(bad_pl, &provider).expect_err("rule 10.4");
        assert!(matches!(
            err,
            CoreError::Auth(crate::AuthError::Rule10_4_CreatorInUsers(_))
        ));
    }
}
