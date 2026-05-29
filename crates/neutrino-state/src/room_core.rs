//! Phase 6: per-room state machine orchestration.
//!
//! `RoomCore::apply(event, provider)` integrates a single incoming event
//! against the receipt-of-PDU checks the spec defines at
//! <https://spec.matrix.org/v1.18/server-server-api/#checks-performed-on-receipt-of-a-pdu>.
//! Reading the spec's numbered list against our implementation:
//!
//! - **Step 1** (PDU schema / required fields): wire-format part is
//!   upstream of `apply` (`validate::parse_event`, Phase 1a — enforced by
//!   `EventBuilder::build()` / `Event::from_wire`). The semantic rules
//!   that don't need a provider (count limits, create structural rules,
//!   rule 9, per-type content shape) run inside `apply` via
//!   `validate::validate_pdu` (Phase 1b). Running validate_pdu here
//!   defensively means a caller that hand-constructs an `Event` (bypassing
//!   the builder/wire path) still can't smuggle a semantically-malformed
//!   event past us.
//! - **Step 2** (signature verification): intentionally skipped per
//!   `CLAUDE.md` — trusted-network policy.
//! - **Step 3** (auth check against the event's auth_events): under
//!   MSC4242 the wire-level `auth_events` field is gone; the equivalent
//!   check is "auth against state-before-event", which we derive from
//!   `prev_state_events` via `state_res::state_at_heads`.
//! - **Step 4** (auth check against state-at-event): removed under
//!   MSC4242 / state DAGs — step 3 subsumes it.
//! - **Step 5** (soft-fail check against current room state): we run
//!   `check_auth_rules` against `current_state` for **non-state events
//!   only**. State events are not soft-failed (matches synapse's behaviour
//!   in `_check_for_soft_fail`). A soft-failed event is still persisted
//!   (Matrix soft-fail semantics) — the `soft_failed` flag on
//!   `Effect::Persist` lets storage keep it out of client timelines.
//! - **Step 6** (state-set check): removed under MSC4242 / state DAGs.
//!
//! Local additions on top of the spec list: reference validation (Phase
//! 1c, `validate::validate_references`) runs before the auth checks to
//! ensure the room exists and `prev_state_events` are well-formed.
//!
//! ## What `apply` mutates
//!
//! On a hard-rejected event (room_id mismatch, reference invalid, auth
//! fails against state-before-event), `apply` returns `Err(CoreError)`
//! and does NOT mutate `RoomCore`. On acceptance:
//! - **state events**: forward extremities are updated (parents in
//!   `prev_state_events` are removed, the new event is inserted), then
//!   `current_state` is recomputed by state-resolving across the new FE
//!   set. Mutation is committed atomically after every fallible call has
//!   succeeded.
//! - **non-state events**: no FE or `current_state` mutation; only the
//!   soft-fail check runs against the existing `current_state`.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use neutrino_common::Event;
use ruma::{EventId, OwnedEventId, OwnedRoomId};

use crate::auth_rules::check_auth_rules;
use crate::provider::StateProvider;
use crate::state_res;
use crate::validate;
use crate::{CoreError, StateMap, StateResError};

/// Provider wrapper that transparently resolves a single `local_event` —
/// the event currently being applied — while delegating everything else to
/// the inner provider. Lets `apply` reuse the same `state_at_heads` /
/// `resolve_state` code paths whether the FE under consideration is an
/// already-persisted event or the not-yet-persisted local one.
struct ProviderWithLocal<'a> {
    inner: &'a dyn StateProvider,
    local_event: Arc<Event>,
}

impl StateProvider for ProviderWithLocal<'_> {
    fn get_event(&self, id: &EventId) -> Option<Arc<Event>> {
        if id == self.local_event.event_id {
            // The local event is in-flight — by definition not rejected
            // yet (apply returns Err on hard-reject, never reaches this
            // resolver), so we hand it back as-is.
            return Some(self.local_event.clone());
        }
        self.inner.get_event(id)
    }

    fn auth_chain(
        &self,
        seeds: &HashSet<OwnedEventId>,
    ) -> Result<HashSet<OwnedEventId>, StateResError> {
        // If the local event isn't among the seeds, the inner provider's
        // closure already covers everything — no need to think about the
        // local event at all.
        let local_id = &self.local_event.event_id;
        if !seeds.contains(local_id) {
            return self.inner.auth_chain(seeds);
        }
        // The local event's `auth_events` parents are all already-persisted
        // events (every event's auth chain is locally resolvable — project
        // invariant). Swap the local id out of the seed set for those parents
        // and let the inner provider's optimised impl walk the rest. Then add
        // the local event back to the result (the seeds-included property).
        let mut new_seeds: HashSet<OwnedEventId> =
            seeds.iter().filter(|id| *id != local_id).cloned().collect();
        for parent in &self.local_event.auth_events {
            new_seeds.insert(parent.clone());
        }
        let mut chain = self.inner.auth_chain(&new_seeds)?;
        chain.insert(local_id.clone());
        Ok(chain)
    }
}

/// Side-effects emitted by `RoomCore::apply`. The caller (storage and
/// federation layers) interprets them sequentially in emission order.
#[derive(Debug, Clone)]
pub enum Effect {
    /// Persist the event to storage. `soft_failed = true` means the event
    /// passed reference + state-before auth but failed the soft-fail check
    /// against `current_state`; storage keeps it but it must not be relayed
    /// to clients (Matrix soft-fail semantics).
    Persist {
        event: Arc<Event>,
        soft_failed: bool,
    },
    /// Replace `current_state` with this resolved state map. Emitted only
    /// when the accepted event is a state event.
    UpdateCurrentState(StateMap<OwnedEventId>),
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

    /// Integrate a single event into the room. Returns the list of effects
    /// (in emission order) the caller should process. On hard-reject paths
    /// returns `Err(CoreError)` and emits no effects — the event must not
    /// be persisted. If `event` is already a forward extremity of this
    /// `RoomCore`, returns `Ok(vec![])` (idempotent no-op).
    ///
    /// **Assumed but verified**: `event` is the output of
    /// `EventBuilder::build()` or `Event::from_wire` — both of which run
    /// `validate::parse_event` (wire format) and `validate::validate_pdu`
    /// (semantic rules) before yielding an `Event`. `apply` re-runs
    /// `validate_pdu` defensively so a hand-constructed `Event` that
    /// bypassed those constructors still can't smuggle a semantically-
    /// malformed event past the auth pipeline. Wire-format checks
    /// (`parse_event`) are NOT re-run — they require the raw JSON bytes,
    /// which `Event` doesn't expose in structured form.
    ///
    /// Note: `apply` does NOT insert the event into `provider`. The caller
    /// is expected to honour `Effect::Persist` by writing through to storage
    /// (which doubles as the provider for subsequent `apply` calls).
    pub fn apply(
        &mut self,
        event: Event,
        provider: &dyn StateProvider,
    ) -> Result<Vec<Effect>, CoreError> {
        // C1: room_id sanity. A RoomCore is per-room; an event whose room_id
        // doesn't match has no business mutating it, even if the other room
        // exists in `provider`.
        if event.room_id != self.room_id {
            return Err(CoreError::RoomMismatch {
                expected: self.room_id.clone(),
                actual: event.room_id.clone(),
            });
        }

        let event = Arc::new(event);

        // C3: idempotency. If the event is already a forward extremity, this
        // `RoomCore` has already integrated it. Re-running the pipeline would
        // emit a duplicate `Persist` and recompute the same `current_state`.
        if self.state_forward_extremities.contains(&event.event_id) {
            return Ok(Vec::new());
        }

        // Phase 1b: semantic rules (no provider). Defence-in-depth — the
        // builder / wire-parser already ran this, but we re-run so a hand-
        // constructed `Event` doesn't bypass it.
        validate::validate_pdu(&event)?;

        // Phase 1c: reference validation (provider lookups) — local
        // addition on top of the spec's PDU-receipt checks.
        validate::validate_references(&event, provider)?;

        // Shared cache for state-before walks: state_before(event) and
        // (for state events) state_at_heads(new_fes) overlap heavily on the
        // old FEs' subgraphs. One cache amortises the duplicate work.
        let mut cache = state_res::StateBeforeCache::new();

        // Spec step 3: auth against state-before-event.
        let state_before_ids =
            state_res::state_at_heads(&event.prev_state_events, provider, &mut cache)?;
        let state_before_events = materialize_state(&state_before_ids, provider)?;
        check_auth_rules(&event, &state_before_events)?;

        let is_state_event = event.state_key.is_some();
        if is_state_event {
            // State-event acceptance: compute new FEs + new current_state into
            // LOCAL bindings, only commit to `self` after every fallible call
            // has returned `Ok`. Atomic on the error path.
            //
            // The new event isn't in `provider` yet — wrap with
            // `ProviderWithLocal` so the existing `state_res` paths transparently
            // resolve `event.event_id` to the in-flight `event`.
            let wrapper = ProviderWithLocal {
                inner: provider,
                local_event: event.clone(),
            };

            let mut new_fes = self.state_forward_extremities.clone();
            for parent in &event.prev_state_events {
                new_fes.remove(parent);
            }
            new_fes.insert(event.event_id.clone());

            let new_fes_slice: Vec<OwnedEventId> = new_fes.iter().cloned().collect();
            let new_current_ids = state_res::state_at_heads(&new_fes_slice, &wrapper, &mut cache)?;
            let new_current_events = materialize_state(&new_current_ids, &wrapper)?;

            // Commit. All fallible work above is done.
            self.state_forward_extremities = new_fes;
            self.current_state = Arc::new(new_current_events);

            // State events are not soft-failed (synapse parity).
            Ok(vec![
                Effect::Persist {
                    event,
                    soft_failed: false,
                },
                Effect::UpdateCurrentState(new_current_ids),
            ])
        } else {
            // Spec step 5: soft-fail against current_state. Non-state events
            // only. State events that pass step 3 are accepted as-is.
            let soft_failed = check_auth_rules(&event, &self.current_state).is_err();
            Ok(vec![Effect::Persist { event, soft_failed }])
        }
    }
}

/// Materialise a `StateMap<OwnedEventId>` into a `StateMap<Arc<Event>>` by
/// looking up each event_id in `provider`. The error case is unreachable in
/// practice — every id here came out of `state_res::state_at_heads` /
/// `resolve_state`, which already demanded the same provider to resolve
/// them. We surface `MissingEvent` rather than panicking because corruption
/// is recoverable as an error; a panic would tear down the whole apply
/// pipeline.
fn materialize_state(
    ids: &StateMap<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<StateMap<Arc<Event>>, CoreError> {
    let mut out = StateMap::new();
    for (key, id) in ids {
        let info = provider
            .get_event(id)
            .ok_or_else(|| CoreError::StateRes(StateResError::MissingEvent(id.clone())))?;
        out.insert(key.clone(), info);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_id::EventBuilder;
    use crate::provider::InMemoryStateProvider;
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
        provider.insert(event);
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
        assert!(matches!(
            &effects[0],
            Effect::Persist { event, soft_failed: false } if event.event_id == create_id
        ));
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

        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Persist {
                soft_failed: false,
                ..
            }
        )));
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
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Persist {
                soft_failed: false,
                ..
            }
        )));
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

    #[test]
    fn apply_foreign_room_id_rejected() {
        // C1: an event whose room_id doesn't match the RoomCore's room_id is
        // refused upfront, even if the foreign room is well-formed and
        // present in the provider.
        let our_create = Arc::new(create_event("@alice:example.org"));
        let our_room_id = room_id_from_create(&our_create.event_id);
        let other_create = Arc::new(create_event("@alice:example.org"));
        let other_room_id = room_id_from_create(&other_create.event_id);
        assert_ne!(our_room_id, other_room_id);

        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, our_create.clone());
        insert(&mut provider, other_create.clone());

        let mut room = RoomCore::new(our_room_id.clone());
        room.apply((*our_create).clone(), &provider).expect("ours");

        // Build a member event in the OTHER room.
        let foreign_join = alice_join_event(&other_create.event_id, &other_room_id);
        let err = room.apply(foreign_join, &provider).expect_err("wrong room");
        assert!(matches!(err, CoreError::RoomMismatch { .. }));
    }

    #[test]
    fn apply_event_already_in_forward_extremities_is_noop() {
        // C3: re-applying a state event that is already a forward extremity
        // returns an empty effects list and does not mutate the RoomCore.
        let create = Arc::new(create_event("@alice:example.org"));
        let room_id = room_id_from_create(&create.event_id);
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, create.clone());
        let mut room = RoomCore::new(room_id.clone());
        room.apply((*create).clone(), &provider).expect("create");

        let pre_fe = room.state_forward_extremities.clone();
        let pre_state = room.current_state.clone();

        // Re-apply the same create event.
        let effects = room.apply((*create).clone(), &provider).expect("noop");
        assert!(
            effects.is_empty(),
            "expected empty effects, got {effects:?}"
        );
        assert_eq!(room.state_forward_extremities, pre_fe);
        assert!(Arc::ptr_eq(&room.current_state, &pre_state));
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
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Persist {
                soft_failed: false,
                ..
            }
        )));
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
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Persist {
                soft_failed: false,
                ..
            }
        )));
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
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Persist {
                soft_failed: false,
                ..
            }
        )));
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
