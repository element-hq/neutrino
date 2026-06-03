//! Property-based tests.
//!
//! The 69 case tests in `src/` are the spec-anchored documentation — each one
//! corresponds to a quoted "reject" / "MUST" clause. The properties below
//! supplement those by sweeping inputs that hand-written cases couldn't
//! efficiently cover:
//!
//! 1. `auth_event_keys_never_includes_create` — universal version of the
//!    `v12_omits_create_event_key` case test: holds for *every* event.
//! 2. `calculate_auth_events_is_strict_pass_through_filter` — couples the
//!    selector to the lookup: every key returned by `auth_event_keys` that
//!    resolves in `state` must appear in `result`, and no other ids may.
//! 3. `calculate_auth_events_excludes_create_even_when_in_state` —
//!    universal version of the create-exclusion case test: v12 must drop
//!    `(m.room.create, "")` from any input state.
//!
//! `parse_event_rejects_any_missing_required_field` is kept here as a plain
//! `#[test]` that loops over `REQUIRED_FIELDS` — sweeping a fixed array
//! doesn't need proptest entropy.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use neutrino_common::ROOM_VERSION_ID;
use neutrino_common::event_id::room_id_from_create;
use neutrino_state::auth_events::{auth_event_keys, calculate_auth_events};
use neutrino_state::auth_rules::check_auth_rules;
use neutrino_state::event_id::EventBuilder;
use neutrino_state::provider::{InMemoryStateProvider, StateProvider};
use neutrino_state::room_core::{Effect, RoomCore};
use neutrino_state::state_res::{
    auth_chain_difference, conflicted_subgraph, iterative_auth_checks, power_of_sender,
    resolve_state, reverse_topological_power_sort, separate,
};
use neutrino_state::validate::parse_event;
use neutrino_state::{Event, FormatError, StateMap};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{TestCaseError, TestRunner};
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, room_id};
use serde_json::value::RawValue;
use serde_json::{Value, json};

// ---------- helpers ----------

fn raw(v: Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(&v).expect("fixture")
}

fn eid(s: &str) -> OwnedEventId {
    s.parse().expect("event id")
}

/// Minimal valid non-create event with every required PDU field populated.
fn base_message() -> Value {
    json!({
        "type": "m.room.message",
        "sender": "@alice:example.org",
        "room_id": "!room:example.org",
        "content": { "msgtype": "m.text", "body": "hi" },
        "prev_events": [],
        "prev_state_events": [],
        "depth": 1,
        "origin_server_ts": 1_700_000_000_000_u64,
        "hashes": { "sha256": "abc" }
    })
}

const REQUIRED_FIELDS: &[&str] = &[
    "type",
    "sender",
    "content",
    "origin_server_ts",
    "prev_events",
    "prev_state_events",
    "room_id",
    "hashes",
];

/// Removing any required top-level field from an otherwise-valid event
/// produces `FormatError::MissingField` naming exactly that field. Plain
/// enumeration — proptest isn't the right tool for sweeping a fixed list.
#[test]
fn parse_event_rejects_any_missing_required_field() {
    for &field in REQUIRED_FIELDS {
        let mut v = base_message();
        v.as_object_mut().expect("object").remove(field);
        let result = parse_event(raw(v), eid("$e:example.org"), vec![]);
        match result {
            Err(FormatError::MissingField(f)) => {
                assert_eq!(f, field, "field {field} produced wrong MissingField");
            }
            other => panic!("expected MissingField({field}), got {other:?}"),
        }
    }
}

// ---------- strategies ----------

/// "localpart" that's safe across user-id / event-id parsers (lowercase ASCII).
fn arb_localpart() -> impl Strategy<Value = String> {
    "[a-z]{1,8}"
}

fn arb_user_id() -> impl Strategy<Value = String> {
    arb_localpart().prop_map(|s| format!("@{s}:example.org"))
}

/// v12 event id: `$` + 43 chars of URL-safe unpadded base64 (the encoded
/// SHA-256 reference hash). Synthetic for tests — we don't compute a real
/// hash, just shape the string the way ruma will parse a v12 event id.
fn arb_event_id() -> impl Strategy<Value = OwnedEventId> {
    "\\$[A-Za-z0-9_-]{43}".prop_filter_map("parseable as OwnedEventId", |s| s.parse().ok())
}

/// Variable-length `prev_events`. Cap chosen low — anything more just
/// churns the shrinker without improving coverage.
fn arb_prev_events() -> impl Strategy<Value = Vec<OwnedEventId>> {
    prop::collection::vec(arb_event_id(), 0..=3)
}

/// Variable-length `prev_state_events`. Phase 1b validates these against a
/// provider — only the wire shape matters here, so any ids do.
fn arb_prev_state_events() -> impl Strategy<Value = Vec<OwnedEventId>> {
    prop::collection::vec(arb_event_id(), 0..=3)
}

/// Strategy for an `m.room.message` event with varied sender and ancestry.
/// `EventBuilder` computes the event_id from the canonical bytes, so the
/// strategy no longer threads in a separate `arb_event_id`.
fn arb_message_event() -> impl Strategy<Value = Event> {
    (
        arb_user_id(),
        arb_prev_events(),
        arb_prev_state_events(),
        // origin_server_ts: keep the fixture-ts to disambiguate otherwise-
        // identical events (v11 redaction strips the body, so two messages
        // with the same sender/prevs/ts would hash to the same id).
        1u64..1_000_000_000_000_000u64,
    )
        .prop_filter_map("valid message", |(sender, prevs, prev_states, ts)| {
            let sender: OwnedUserId = sender.parse().ok()?;
            EventBuilder::new(sender, "m.room.message".to_owned())
                .room_id(room_id!("!room:example.org").to_owned())
                .content(json!({ "msgtype": "m.text", "body": "hi" }))
                .prev_events(prevs)
                .prev_state_events(prev_states)
                .origin_server_ts(ts)
                .build()
                .ok()
        })
}

/// Strategy for an `m.room.member` event with varied sender/target and a
/// membership chosen by cases: when `sender == target` the wire-impossible
/// memberships `invite` and `ban` are excluded by construction — no
/// `prop_filter` rejection cycle.
fn arb_member_event() -> impl Strategy<Value = Event> {
    (arb_user_id(), arb_user_id())
        .prop_flat_map(|(sender, target)| {
            let memberships: Vec<&'static str> = if sender == target {
                vec!["join", "leave", "knock"]
            } else {
                vec!["join", "leave", "invite", "ban", "knock"]
            };
            (
                Just(sender),
                Just(target),
                proptest::sample::select(memberships),
                arb_prev_events(),
                arb_prev_state_events(),
                1u64..1_000_000_000_000_000u64,
            )
        })
        .prop_filter_map(
            "valid member",
            |(sender, target, membership, prevs, prev_states, ts)| {
                let sender: OwnedUserId = sender.parse().ok()?;
                EventBuilder::new(sender, "m.room.member".to_owned())
                    .room_id(room_id!("!room:example.org").to_owned())
                    .state_key(target)
                    .content(json!({ "membership": membership }))
                    .prev_events(prevs)
                    .prev_state_events(prev_states)
                    .origin_server_ts(ts)
                    .build()
                    .ok()
            },
        )
}

/// Strategy for an `m.room.create` event. Create events carry no `room_id`
/// (derived from `event_id`) and no ancestry (rule 1.1 / MSC4242).
fn arb_create_event() -> impl Strategy<Value = Event> {
    (arb_user_id(), 1u64..1_000_000_000_000_000u64).prop_filter_map(
        "valid create",
        |(sender, ts)| {
            let sender: OwnedUserId = sender.parse().ok()?;
            EventBuilder::new(sender, "m.room.create".to_owned())
                .state_key(String::new())
                .content(json!({ "room_version": ROOM_VERSION_ID }))
                .origin_server_ts(ts)
                .build()
                .ok()
        },
    )
}

/// Arbitrary `Event` — mix of message, member, and create events. Senders,
/// targets, memberships, and ancestry all vary.
fn arb_event() -> impl Strategy<Value = Event> {
    prop_oneof![arb_message_event(), arb_member_event(), arb_create_event(),]
}

/// Arbitrary `StateMap<OwnedEventId>` — keys are arbitrary `(type, state_key)`
/// tuples, values are arbitrary event IDs. Not constrained to "well-formed"
/// state — the properties under test only care about lookup behaviour.
fn arb_state_map() -> impl Strategy<Value = StateMap<OwnedEventId>> {
    prop::collection::hash_map(
        ("[a-z.]{1,20}", "[a-zA-Z@:_-]{0,30}"),
        arb_event_id(),
        0..20,
    )
}

// ---------- properties ----------

proptest! {
    /// Universal v12 invariant: `auth_event_keys` never asks for
    /// `m.room.create`. Holds for any event — create events themselves
    /// return an empty Vec, non-create events must exclude the key.
    #[test]
    fn auth_event_keys_never_includes_create(event in arb_event()) {
        let keys = auth_event_keys(&event);
        prop_assert!(
            !keys.iter().any(|(t, _)| t == "m.room.create"),
            "v12 must not request m.room.create"
        );
    }

    /// `calculate_auth_events` is a strict pass-through filter: the
    /// returned ids are exactly the values that `auth_event_keys` requests
    /// and that resolve in `state` — no fabrication, no dropping.
    ///
    /// A no-op `vec![]` implementation would have satisfied "no fabrication"
    /// on its own; the set equality below couples the property to the real
    /// selection behaviour.
    #[test]
    fn calculate_auth_events_is_strict_pass_through_filter(
        event in arb_event(),
        state in arb_state_map(),
    ) {
        let result: HashSet<_> = calculate_auth_events(&event, &state).into_iter().collect();
        let expected: HashSet<_> = auth_event_keys(&event)
            .into_iter()
            .filter_map(|k| state.get(&k).cloned())
            .collect();
        prop_assert_eq!(result, expected);
    }

    /// v12: even when the state map carries a `(m.room.create, "")` entry,
    /// `calculate_auth_events` never includes that id in its output.
    #[test]
    fn calculate_auth_events_excludes_create_even_when_in_state(
        event in arb_event(),
        state in arb_state_map(),
        create_id in arb_event_id(),
    ) {
        let mut state = state;
        // Avoid the (astronomically rare) collision where `create_id` is
        // already a value at some other key — keeps the property
        // unconditional rather than probabilistic.
        state.retain(|_, v| v != &create_id);
        state.insert(("m.room.create".to_string(), String::new()), create_id.clone());

        let result = calculate_auth_events(&event, &state);
        prop_assert!(
            !result.contains(&create_id),
            "v12 must exclude m.room.create from auth_events even when present in state"
        );
    }
}

// ---------- auth_rules sanity floor ----------

/// Combined strategy: a create event, its computed event_id, and 0..10
/// other state events with arbitrary keys. The create event is built via
/// `EventBuilder`, so its `event_id` is the canonical reference hash —
/// returned to call-site strategies so they can thread it into
/// `prev_events` / `prev_state_events` of the event-under-test (rule
/// 5.3.1: self-join immediately after create).
fn arb_state_with_create()
-> impl Strategy<Value = (OwnedEventId, OwnedUserId, StateMap<Arc<Event>>)> {
    (
        arb_user_id(),
        1u64..1_000_000_000_000_000u64,
        prop::collection::vec(arb_event().prop_map(Arc::new), 0..10),
    )
        .prop_filter_map("valid create + state", |(sender_str, ts, events)| {
            let sender: OwnedUserId = sender_str.parse().ok()?;
            let create_event = EventBuilder::new(sender, "m.room.create".to_owned())
                .state_key(String::new())
                .content(json!({ "room_version": ROOM_VERSION_ID }))
                .origin_server_ts(ts)
                .build()
                .ok()?;
            let create_id = create_event.event_id.clone();
            let creator_uid = create_event.sender.clone();
            let create = Arc::new(create_event);
            let mut state: StateMap<Arc<Event>> = StateMap::new();
            // Extras first; explicit create wins at `("m.room.create", "")`
            // so the strategy can't desynchronise create_id from the
            // state's create event when `arb_event` samples another
            // `m.room.create` into the extras.
            for ev in events {
                let key = (
                    ev.event_type.clone(),
                    ev.state_key.clone().unwrap_or_default(),
                );
                state.insert(key, ev);
            }
            state.insert(("m.room.create".to_string(), String::new()), create);
            Some((create_id, creator_uid, state))
        })
}

proptest! {
    /// Sanity floor: `check_auth_rules` must not panic for any combination of
    /// an arbitrary event and a state map carrying a valid `m.room.create`.
    /// (`AuthContext::new` enforces the create-event invariant via panic per
    /// the post-`validate_references` contract, so a create-absent state map
    /// is out of scope.) Guards the `.expect()`s inside `check_rule_5_member`
    /// against future drift of Phase 1a's state_key / content.membership
    /// invariants, and any future panics elsewhere in the dispatcher.
    #[test]
    fn check_auth_rules_never_panics(
        event in arb_event(),
        (_, _, state) in arb_state_with_create(),
    ) {
        let _ = check_auth_rules(&event, &state, &InMemoryStateProvider::new());
    }

    /// Rule 5.3.1: a self-join event whose only ancestry (in both DAGs) is
    /// the create event is always allowed, regardless of additional state.
    /// The create event is built via `EventBuilder` so its `event_id` is
    /// the canonical reference hash — and the join is built from that same
    /// id, keeping the two synchronised by construction.
    #[test]
    fn rule_5_3_1_self_join_after_create_always_allowed(
        sender_str in arb_user_id(),
        extras in prop::collection::vec(arb_event().prop_map(Arc::new), 0..10),
        create_ts in 1u64..1_000_000_000_000_000u64,
        join_ts in 1u64..1_000_000_000_000_000u64,
    ) {
        let sender: OwnedUserId = sender_str.parse().expect("sender");
        let create_event = EventBuilder::new(sender.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .origin_server_ts(create_ts)
            .build()
            .expect("create event valid");
        let create_id = create_event.event_id.clone();
        let create_room_id = create_event.room_id.clone();
        let creator_uid: OwnedUserId = create_event.sender.clone();
        let mut state: StateMap<Arc<Event>> = StateMap::new();
        // Insert extras first; the explicit create below must win at
        // `("m.room.create", "")` so the proptest can't desynchronise the
        // create_id from the state's create event by sampling another
        // `m.room.create` into `extras`.
        for ev in extras {
            let key = (
                ev.event_type.clone(),
                ev.state_key.clone().unwrap_or_default(),
            );
            state.insert(key, ev);
        }
        state.insert(
            ("m.room.create".to_string(), String::new()),
            Arc::new(create_event),
        );

        let join = EventBuilder::new(creator_uid.clone(), "m.room.member".to_owned())
            .room_id(create_room_id)
            .state_key(creator_uid.as_str().to_owned())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![create_id.clone()])
            .prev_state_events(vec![create_id.clone()])
            .origin_server_ts(join_ts)
            .build()
            .expect("self-join valid");
        prop_assert!(check_auth_rules(&join, &state, &InMemoryStateProvider::new()).is_ok());
    }
}

// ============== Phase 4a: state_res ==============
//
// Properties on `separate`, `conflicted_subgraph`, `auth_chain_difference`.
// Strategies generate unrestricted state maps and providers (auth chains may
// reference unknown events, may contain cycles); the algorithms must hold
// under that.

/// Placeholder `Arc<Event>` built via `EventBuilder` with caller-supplied
/// `auth_events`. The state-res functions don't look at the event body —
/// only `event_id` and `auth_events` — so a minimal valid event suffices,
/// and the computed event_id is what callers index by.
/// Seed the create event for the placeholder room so `power_of_sender` can
/// resolve the creator. The id is forced to that room's derived create id;
/// only the create's id/sender/content matter.
fn seed_placeholder_create(provider: &mut InMemoryStateProvider) {
    let mut create = EventBuilder::new(
        "@alice:example.org"
            .parse::<OwnedUserId>()
            .expect("user id"),
        "m.room.create".to_owned(),
    )
    .state_key(String::new())
    .content(json!({ "room_version": ROOM_VERSION_ID }))
    .origin_server_ts(1_699_999_999_999)
    .build()
    .expect("create");
    // `placeholder_arc_event` builds events in this room; the create id is its
    // room_id with the `!` sigil swapped for `$` (v12 room-id derivation).
    let room = room_id!("!room:example.org").to_owned();
    let create_id = format!("${}", room.as_str().strip_prefix('!').expect("room sigil"));
    create.event_id = create_id.parse().expect("create id");
    create.room_id = room;
    provider.insert(Arc::new(create));
}

fn placeholder_arc_event(ts: u64, auth_events: Vec<OwnedEventId>) -> Arc<Event> {
    Arc::new(
        EventBuilder::new(
            "@alice:example.org"
                .parse::<OwnedUserId>()
                .expect("user id"),
            "m.room.placeholder".to_owned(),
        )
        .room_id(room_id!("!room:example.org").to_owned())
        .state_key(String::new())
        .content(json!({}))
        .auth_events(auth_events)
        .origin_server_ts(ts)
        .build()
        .expect("placeholder event"),
    )
}

/// Arbitrary `InMemoryStateProvider` that is **closed** under `auth_events`
/// — every id any event references via its `auth_events` is itself in the
/// provider. Cycles are not constructed (we only point at *earlier* events
/// in the build order); the DFS in `auth_chain` dedupes via a visited set
/// regardless.
///
/// Closure is the project invariant for state-res input: a referenced-but-
/// unknown event is an error (`StateResError::MissingEvent`), not a
/// silent backfill boundary. The strategy reflects that.
///
/// Yields the provider **and the list of known ids** (in build order, so
/// any event references only ids earlier in the list).
fn arb_provider_with_ids() -> impl Strategy<Value = (InMemoryStateProvider, Vec<OwnedEventId>)> {
    // `n` events; for each, the indices (in the build order) of its
    // auth_events parents. Parents are constrained to earlier indices so
    // every reference resolves locally without needing back-patching.
    (1usize..=15)
        .prop_flat_map(|n| {
            (
                Just(n),
                prop::collection::vec(prop::collection::vec(0usize..n, 0..4), n),
            )
        })
        .prop_map(|(n, parents_per_event)| {
            let mut provider = InMemoryStateProvider::new();
            seed_placeholder_create(&mut provider);
            let mut ids: Vec<OwnedEventId> = Vec::with_capacity(n);
            for (i, parent_indices) in parents_per_event.iter().enumerate() {
                // Parents must reference earlier indices to avoid forward
                // references (and self-loops); clamp the sampled index
                // accordingly.
                let parents: Vec<OwnedEventId> = parent_indices
                    .iter()
                    .filter_map(|j| if *j < i { Some(ids[*j].clone()) } else { None })
                    .collect();
                let ts = 1_700_000_000_000 + (i as u64);
                let event = placeholder_arc_event(ts, parents);
                ids.push(event.event_id.clone());
                provider.insert(event);
            }
            (provider, ids)
        })
}

/// Build a deterministic state set whose values are drawn from `ids`
/// (which is yielded by `arb_provider_with_ids` and therefore known to
/// the matching provider). `offset` lets tests vary the set across
/// iterations without needing another `prop_flat_map` binding.
fn state_set_from_ids(ids: &[OwnedEventId]) -> StateMap<OwnedEventId> {
    state_set_from_ids_offset(ids, 0)
}

fn state_set_from_ids_offset(ids: &[OwnedEventId], offset: usize) -> StateMap<OwnedEventId> {
    let mut m = StateMap::new();
    let n = 3.min(ids.len());
    for i in 0..n {
        let idx = (i + offset) % ids.len();
        m.insert(("m.room.x".to_string(), format!("k{i}")), ids[idx].clone());
    }
    m
}

/// Helper for properties that need to compute the auth chain of an event id
/// from outside the algorithm under test, so the property can compare.
/// Reads auth_events off the `Event` directly rather than going through
/// `provider.auth_chain`, so the property genuinely cross-checks the impl.
fn auth_chain_of(seed: &OwnedEventId, provider: &dyn StateProvider) -> HashSet<OwnedEventId> {
    let mut chain: HashSet<OwnedEventId> = HashSet::new();
    let mut stack = vec![seed.clone()];
    while let Some(id) = stack.pop() {
        if chain.insert(id.clone()) {
            let parents: Vec<OwnedEventId> = provider
                .get_event(&id)
                .ok()
                .flatten()
                .map(|info| info.auth_events.clone())
                .unwrap_or_default();
            for parent in &parents {
                if !chain.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }
    }
    chain
}

/// Build the same arbitrary `StateMap<OwnedEventId>` shape as `arb_state_map`
/// up above, but accessible to the state_res properties below — the existing
/// strategy is fine to reuse via `arb_state_map`.
fn arb_state_set() -> impl Strategy<Value = StateMap<OwnedEventId>> {
    arb_state_map()
}

fn arb_state_sets() -> impl Strategy<Value = Vec<StateMap<OwnedEventId>>> {
    prop::collection::vec(arb_state_set(), 0..5)
}

proptest! {
    // ----- separate -----

    /// No fabricated event ids: every event id appearing in the output came
    /// from at least one input state set.
    #[test]
    fn separate_no_fabricated_event_ids(state_sets in arb_state_sets()) {
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out = separate(&refs);
        let input_ids: HashSet<OwnedEventId> = state_sets
            .iter()
            .flat_map(|s| s.values().cloned())
            .collect();
        for id in out.unconflicted.values() {
            prop_assert!(input_ids.contains(id));
        }
        for vals in out.conflicted.values() {
            for id in vals {
                prop_assert!(input_ids.contains(id));
            }
        }
    }

    /// No fabricated keys: every key in the output came from at least one
    /// input state set.
    #[test]
    fn separate_no_fabricated_keys(state_sets in arb_state_sets()) {
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out = separate(&refs);
        let input_keys: HashSet<(String, String)> = state_sets
            .iter()
            .flat_map(|s| s.keys().cloned())
            .collect();
        for k in out.unconflicted.keys() {
            prop_assert!(input_keys.contains(k));
        }
        for k in out.conflicted.keys() {
            prop_assert!(input_keys.contains(k));
        }
    }

    /// Total bucketing: every input `(key, event_id)` pair lands in either
    /// the unconflicted bucket at that key OR the conflicted value-set at
    /// that key. Captures Kegan's "each event is bucketed" property.
    #[test]
    fn separate_every_input_pair_in_output(state_sets in arb_state_sets()) {
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out = separate(&refs);
        let input_pairs: HashSet<((String, String), OwnedEventId)> = state_sets
            .iter()
            .flat_map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())))
            .collect();
        for (k, v) in input_pairs {
            let in_unconflicted = out.unconflicted.get(&k) == Some(&v);
            let in_conflicted = out
                .conflicted
                .get(&k)
                .is_some_and(|set| set.contains(&v));
            prop_assert!(
                in_unconflicted || in_conflicted,
                "pair ({:?}, {}) not in output",
                k,
                v
            );
        }
    }

    /// A key is in `unconflicted` iff every input state set has that key
    /// with the same value. (Definitional, but testable as a sweep.)
    #[test]
    fn separate_unconflicted_iff_all_sets_agree(state_sets in arb_state_sets()) {
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out = separate(&refs);
        let all_keys: HashSet<(String, String)> = state_sets
            .iter()
            .flat_map(|s| s.keys().cloned())
            .collect();
        for key in all_keys {
            let values_per_set: Vec<Option<&OwnedEventId>> =
                refs.iter().map(|s| s.get(&key)).collect();
            let present_in_all = values_per_set.iter().all(|v| v.is_some());
            let distinct: HashSet<&OwnedEventId> =
                values_per_set.iter().flatten().copied().collect();
            let should_be_unconflicted = present_in_all && distinct.len() == 1;
            prop_assert_eq!(
                out.unconflicted.contains_key(&key),
                should_be_unconflicted,
                "key {:?} unconflicted classification wrong",
                key
            );
        }
    }

    /// Single input state set: every key is unconflicted (no other set to
    /// disagree).
    #[test]
    fn separate_single_input_all_unconflicted(s in arb_state_set()) {
        let refs = vec![&s];
        let out = separate(&refs);
        prop_assert!(out.conflicted.is_empty());
        prop_assert_eq!(out.unconflicted.len(), s.len());
        for (k, v) in &s {
            prop_assert_eq!(out.unconflicted.get(k), Some(v));
        }
    }

    /// N identical inputs: output equals the input on unconflicted, conflicted
    /// is empty.
    #[test]
    fn separate_identical_inputs(s in arb_state_set(), n in 1usize..6) {
        let cloned: Vec<StateMap<OwnedEventId>> = (0..n).map(|_| s.clone()).collect();
        let refs: Vec<&StateMap<OwnedEventId>> = cloned.iter().collect();
        let out = separate(&refs);
        prop_assert!(out.conflicted.is_empty());
        prop_assert_eq!(out.unconflicted.len(), s.len());
    }

    /// Order-independence: reversing the input state-sets vec yields the
    /// same output (both `unconflicted` and `conflicted`).
    #[test]
    fn separate_order_independent(state_sets in arb_state_sets()) {
        let refs_forward: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out_forward = separate(&refs_forward);
        let mut reversed = state_sets.clone();
        reversed.reverse();
        let refs_reverse: Vec<&StateMap<OwnedEventId>> = reversed.iter().collect();
        let out_reverse = separate(&refs_reverse);
        prop_assert_eq!(out_forward.unconflicted, out_reverse.unconflicted);
        prop_assert_eq!(out_forward.conflicted.len(), out_reverse.conflicted.len());
        for (k, vs) in &out_forward.conflicted {
            prop_assert_eq!(out_reverse.conflicted.get(k), Some(vs));
        }
    }

    /// State sets whose key spaces are pairwise *disjoint* produce all
    /// conflicted output (every key is missing from at least one other set).
    /// Captures Kegan's intuition about disjoint sets, in the form that
    /// actually matches the spec.
    #[test]
    fn separate_disjoint_key_spaces_all_conflicted(s1 in arb_state_set(), s2 in arb_state_set()) {
        // Rebuild s2 with a guaranteed-distinct key prefix so the two sets
        // share no keys at all.
        let s2_disjoint: StateMap<OwnedEventId> = s2
            .into_iter()
            .map(|((t, sk), v)| ((format!("disjoint.{t}"), sk), v))
            .collect();
        // s1 may contain a "disjoint." prefix accidentally; filter those out.
        let s1_filtered: StateMap<OwnedEventId> = s1
            .into_iter()
            .filter(|((t, _), _)| !t.starts_with("disjoint."))
            .collect();
        if s1_filtered.is_empty() && s2_disjoint.is_empty() {
            // Vacuous — nothing to check.
            return Ok(());
        }
        let refs = vec![&s1_filtered, &s2_disjoint];
        let out = separate(&refs);
        prop_assert!(
            out.unconflicted.is_empty(),
            "disjoint key spaces should produce zero unconflicted entries; got {:?}",
            out.unconflicted
        );
        prop_assert_eq!(
            out.conflicted.len(),
            s1_filtered.len() + s2_disjoint.len()
        );
    }

    // ----- conflicted_subgraph -----

    /// Output contains every seed (we picked include-endpoints).
    #[test]
    fn subgraph_contains_seeds(
        (provider, ids) in arb_provider_with_ids(),
        n_seeds in 0usize..10,
    ) {
        let seeds: HashSet<OwnedEventId> =
            ids.iter().take(n_seeds.min(ids.len())).cloned().collect();
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        for s in &seeds {
            prop_assert!(sg.contains(s));
        }
    }

    /// Empty seeds produce empty output.
    #[test]
    fn subgraph_empty_seeds_empty_output((provider, _ids) in arb_provider_with_ids()) {
        let sg = conflicted_subgraph(&HashSet::new(), &provider).unwrap();
        prop_assert!(sg.is_empty());
    }

    /// Output is closed under `Event.auth_events`: for every id in the
    /// output, every event in its auth chain is also in the output.
    #[test]
    fn subgraph_closed_under_auth_events(
        (provider, ids) in arb_provider_with_ids(),
        n_seeds in 1usize..6,
    ) {
        let seeds: HashSet<OwnedEventId> =
            ids.iter().take(n_seeds.min(ids.len())).cloned().collect();
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        for id in &sg {
            let parents: Vec<OwnedEventId> = provider
                .get_event(id)
                .ok()
                .flatten()
                .map(|info| info.auth_events.clone())
                .unwrap_or_default();
            for parent in &parents {
                prop_assert!(
                    sg.contains(parent),
                    "subgraph missing parent {} of {}",
                    parent,
                    id
                );
            }
        }
    }

    /// No fabricated ids: every id in the output is either a seed or a
    /// member of some seed's auth-chain transitive closure.
    #[test]
    fn subgraph_no_fabricated_ids(
        (provider, ids) in arb_provider_with_ids(),
        n_seeds in 1usize..6,
    ) {
        let seeds: HashSet<OwnedEventId> =
            ids.iter().take(n_seeds.min(ids.len())).cloned().collect();
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        let mut allowed: HashSet<OwnedEventId> = HashSet::new();
        for seed in &seeds {
            allowed.extend(auth_chain_of(seed, &provider));
        }
        for id in &sg {
            prop_assert!(
                allowed.contains(id),
                "subgraph contains unreachable id {}",
                id
            );
        }
    }

    // ----- auth_chain_difference -----

    /// Zero or one state sets: empty output. (Empty short-circuits;
    /// single set: chain difference is well-defined as empty because
    /// "in some but not all chains" is vacuous for a single chain.)
    #[test]
    fn acd_zero_or_one_state_set_empty(
        (provider, ids) in arb_provider_with_ids(),
    ) {
        prop_assert!(auth_chain_difference(&[], &provider).unwrap().is_empty());
        let s = state_set_from_ids(&ids);
        prop_assert!(auth_chain_difference(&[&s], &provider).unwrap().is_empty());
    }

    /// N identical state sets: empty output (chains are identical →
    /// intersection equals union → difference empty).
    #[test]
    fn acd_identical_state_sets_empty(
        (provider, ids) in arb_provider_with_ids(),
        n in 2usize..5,
    ) {
        let s = state_set_from_ids(&ids);
        let cloned: Vec<StateMap<OwnedEventId>> = (0..n).map(|_| s.clone()).collect();
        let refs: Vec<&StateMap<OwnedEventId>> = cloned.iter().collect();
        prop_assert!(auth_chain_difference(&refs, &provider).unwrap().is_empty());
    }

    /// `diff == union(chains) \ intersection(chains)`. Both sides computed
    /// externally and compared as sets — this single equality subsumes the
    /// earlier subset-of-union and disjoint-from-intersection properties,
    /// which together did not pin equality.
    #[test]
    fn acd_equals_union_minus_intersection(
        (provider, ids) in arb_provider_with_ids(),
        n in 2usize..5,
    ) {
        let state_sets: Vec<StateMap<OwnedEventId>> =
            (0..n).map(|k| state_set_from_ids_offset(&ids, k)).collect();
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let diff = auth_chain_difference(&refs, &provider).unwrap();
        let chains: Vec<HashSet<OwnedEventId>> = refs
            .iter()
            .map(|s| {
                let mut chain: HashSet<OwnedEventId> = HashSet::new();
                for v in s.values() {
                    chain.extend(auth_chain_of(v, &provider));
                }
                chain
            })
            .collect();
        let union: HashSet<OwnedEventId> = chains.iter().flatten().cloned().collect();
        let intersection: HashSet<OwnedEventId> = chains
            .iter()
            .skip(1)
            .fold(chains[0].clone(), |acc, c| {
                acc.intersection(c).cloned().collect()
            });
        let expected: HashSet<OwnedEventId> =
            union.difference(&intersection).cloned().collect();
        prop_assert_eq!(diff, expected);
    }

    /// Order-independence: reversing the state-sets vec yields the same
    /// output.
    #[test]
    fn acd_order_independent(
        (provider, ids) in arb_provider_with_ids(),
        n in 2usize..5,
    ) {
        let state_sets: Vec<StateMap<OwnedEventId>> =
            (0..n).map(|k| state_set_from_ids_offset(&ids, k)).collect();
        let refs_forward: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let out_forward = auth_chain_difference(&refs_forward, &provider).unwrap();
        let mut reversed = state_sets.clone();
        reversed.reverse();
        let refs_reverse: Vec<&StateMap<OwnedEventId>> = reversed.iter().collect();
        let out_reverse = auth_chain_difference(&refs_reverse, &provider).unwrap();
        prop_assert_eq!(out_forward, out_reverse);
    }
}

// ============== Adversarial provider shapes ==============
//
// The strategies above generate random auth_event_ids maps with no structural
// constraints. These additional strategies generate specific pathological
// shapes — deep linear chains, dense diamonds, cycles — to surface bugs in
// graph traversal (premature termination, missed branches, infinite loops on
// cycles) that random-shape properties wouldn't reliably catch.

/// Linear chain of distinct event ids: ids[0] → ids[1] → ... → ids[n-1].
/// `ids[0]` is the head (deepest descendant); `ids[n-1]` is the tail (no
/// parents). Events are built tail-first so each parent reference resolves
/// to a real, already-computed event_id; ids are distinct by virtue of
/// distinct `origin_server_ts` values.
fn arb_linear_chain(
    min: usize,
    max: usize,
) -> impl Strategy<Value = (InMemoryStateProvider, Vec<OwnedEventId>)> {
    (min..=max).prop_map(|n| {
        let mut provider = InMemoryStateProvider::new();
        // Build tail (no parents) first, walking back to the head. Each
        // event's only parent is the next one along, whose id we just
        // computed.
        let mut tail_to_head: Vec<OwnedEventId> = Vec::with_capacity(n);
        for i in (0..n).rev() {
            let parents = tail_to_head
                .last()
                .cloned()
                .map(|p| vec![p])
                .unwrap_or_default();
            let ts = 1_700_000_000_000 + (i as u64);
            let event = placeholder_arc_event(ts, parents);
            tail_to_head.push(event.event_id.clone());
            provider.insert(event);
        }
        // Caller expects head-first order (ids[0] is the deepest descendant).
        tail_to_head.reverse();
        (provider, tail_to_head)
    })
}

/// Four-node diamond:
/// ```text
///     root
///    /    \
///  left  right
///    \    /
///    bottom
/// ```
/// `root` has two parents (`left`, `right`), each of which has the single
/// parent `bottom`. Build bottom-up so each parent reference resolves to a
/// real computed event_id; corners are distinct by construction (each
/// carries a unique origin_server_ts and a unique parent set).
fn arb_diamond() -> impl Strategy<Value = (InMemoryStateProvider, [OwnedEventId; 4])> {
    Just(()).prop_map(|()| {
        let mut provider = InMemoryStateProvider::new();
        let bottom_ev = placeholder_arc_event(1, vec![]);
        let bottom = bottom_ev.event_id.clone();
        // left and right both carry `bottom` as their only parent — they'd
        // hash to the same id under v11 redaction (the body is stripped) if
        // their other inputs matched. Disambiguate via distinct ts.
        let left_ev = placeholder_arc_event(2, vec![bottom.clone()]);
        let left = left_ev.event_id.clone();
        let right_ev = placeholder_arc_event(3, vec![bottom.clone()]);
        let right = right_ev.event_id.clone();
        let root_ev = placeholder_arc_event(4, vec![left.clone(), right.clone()]);
        let root = root_ev.event_id.clone();
        for ev in [bottom_ev, left_ev, right_ev, root_ev] {
            provider.insert(ev);
        }
        (provider, [root, left, right, bottom])
    })
}

proptest! {
    /// Subgraph of the head of a linear chain equals the whole chain — no
    /// premature termination, no off-by-one.
    #[test]
    fn subgraph_traverses_linear_chain_completely(
        (provider, ids) in arb_linear_chain(1, 30),
    ) {
        let mut seeds = HashSet::new();
        seeds.insert(ids[0].clone());
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        let expected: HashSet<OwnedEventId> = ids.into_iter().collect();
        prop_assert_eq!(sg, expected);
    }

    /// Subgraph of the diamond root visits both branches and converges on
    /// the shared bottom — catches "only walk first parent" bugs.
    #[test]
    fn subgraph_visits_both_branches_of_diamond(
        (provider, corners) in arb_diamond(),
    ) {
        let [root, left, right, bottom] = corners;
        let mut seeds = HashSet::new();
        seeds.insert(root.clone());
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        let expected: HashSet<OwnedEventId> =
            [root, left, right, bottom].into_iter().collect();
        prop_assert_eq!(sg, expected);
    }

    /// Two state sets reference the heads of two disjoint linear chains. The
    /// diff is the full union of both chains (no events shared → no events
    /// in the intersection → everything is in the difference).
    #[test]
    fn acd_disjoint_chains_full_union(
        (provider_a, ids_a) in arb_linear_chain(1, 10),
        (provider_b, ids_b) in arb_linear_chain(1, 10),
    ) {
        // Merge the two providers manually so the chains are disjoint but
        // share no ids. Skip if the random ids happened to overlap.
        let a_set: HashSet<&OwnedEventId> = ids_a.iter().collect();
        if ids_b.iter().any(|id| a_set.contains(id)) {
            return Ok(());
        }
        let mut merged = InMemoryStateProvider::new();
        // Copy the existing events directly from each source provider into
        // the merged one — we need the event_ids to match the ids we just
        // collected (rebuilding via `placeholder_arc_event` would compute
        // fresh ids and the test invariants would no longer hold).
        for id in &ids_a {
            if let Ok(Some(event)) = provider_a.get_event(id) {
                merged.insert(event);
            }
        }
        for id in &ids_b {
            if let Ok(Some(event)) = provider_b.get_event(id) {
                merged.insert(event);
            }
        }

        let mut s1 = StateMap::new();
        s1.insert(("m.room.name".to_string(), String::new()), ids_a[0].clone());
        let mut s2 = StateMap::new();
        s2.insert(("m.room.name".to_string(), String::new()), ids_b[0].clone());
        let refs = vec![&s1, &s2];
        let diff = auth_chain_difference(&refs, &merged).unwrap();
        let expected: HashSet<OwnedEventId> = ids_a.into_iter().chain(ids_b).collect();
        prop_assert_eq!(diff, expected);
    }

    /// Two state sets reference distinct heads of a *shared* linear chain
    /// (one is a deeper descendant of the other). The diff includes the
    /// events between them but excludes the common tail — the tail is in
    /// both auth chains, hence in the intersection.
    #[test]
    fn acd_overlapping_chains_excludes_shared_tail(
        (provider, ids) in arb_linear_chain(3, 20),
        head_choice in 0usize..20,
        tail_choice in 0usize..20,
    ) {
        // Two state sets pointing into the same chain at different depths.
        let len = ids.len();
        let head_idx = head_choice % len;
        let tail_idx = tail_choice % len;
        if head_idx == tail_idx {
            return Ok(());
        }
        let (deeper, shallower) = if head_idx < tail_idx {
            (head_idx, tail_idx)
        } else {
            (tail_idx, head_idx)
        };

        let mut s1 = StateMap::new();
        s1.insert(("m.room.name".to_string(), String::new()), ids[deeper].clone());
        let mut s2 = StateMap::new();
        s2.insert(("m.room.name".to_string(), String::new()), ids[shallower].clone());
        let refs = vec![&s1, &s2];
        let diff = auth_chain_difference(&refs, &provider).unwrap();

        // chain(deeper) = ids[deeper..]
        // chain(shallower) = ids[shallower..]
        // intersection = ids[shallower..] (shorter tail)
        // diff = ids[deeper..shallower]
        let expected: HashSet<OwnedEventId> =
            ids[deeper..shallower].iter().cloned().collect();
        prop_assert_eq!(diff, expected);
    }
}

// ====================================================================
// Phase 4b: power_of_sender / reverse_topological_power_sort / IAC
// ====================================================================

/// Monotonic per-call ts for prop fixtures. Distinct from `arb_*`'s ts ranges
/// (which start at 1_700_000_000_000) — using a separate base avoids ts
/// collisions between the strategy-generated events and these builder-emitted
/// fixtures, so v11 redaction never collapses two distinct fixtures to the
/// same event_id.
fn prop_ts() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static T: AtomicU64 = AtomicU64::new(1_800_000_000_000);
    T.fetch_add(1, Ordering::Relaxed)
}

fn prop_build_create(creator: &str, additional_creators: &[String], federate: bool) -> Event {
    let mut content = json!({ "room_version": ROOM_VERSION_ID });
    if !additional_creators.is_empty() {
        content["additional_creators"] = json!(additional_creators);
    }
    if !federate {
        content["m.federate"] = json!(false);
    }
    EventBuilder::new(
        creator.parse::<OwnedUserId>().expect("creator user id"),
        "m.room.create".to_owned(),
    )
    .state_key(String::new())
    .content(content)
    .origin_server_ts(prop_ts())
    .build()
    .expect("valid create")
}

fn prop_build_msg(room_id: &RoomId, sender: &str, auth: Vec<OwnedEventId>) -> Event {
    EventBuilder::new(
        sender.parse::<OwnedUserId>().expect("sender user id"),
        "m.room.message".to_owned(),
    )
    .room_id(room_id.to_owned())
    .content(json!({ "msgtype": "m.text", "body": "hi" }))
    .auth_events(auth)
    .origin_server_ts(prop_ts())
    .build()
    .expect("valid message")
}

fn prop_build_pl(room_id: &RoomId, sender: &str, content: Value, auth: Vec<OwnedEventId>) -> Event {
    EventBuilder::new(
        sender.parse::<OwnedUserId>().expect("sender user id"),
        "m.room.power_levels".to_owned(),
    )
    .room_id(room_id.to_owned())
    .state_key(String::new())
    .content(content)
    .auth_events(auth)
    .origin_server_ts(prop_ts())
    .build()
    .expect("valid power_levels")
}

/// 1–`max` distinct user IDs (HashSet collapses duplicates from `arb_user_id`).
/// The min is set to 1 so callers always have at least one user to designate
/// as the room creator.
fn arb_distinct_user_ids(max: usize) -> impl Strategy<Value = Vec<String>> {
    prop::collection::hash_set(arb_user_id(), 1..=max).prop_map(|set| set.into_iter().collect())
}

/// PL `users` map: at most 5 entries, integer power levels in `[-100, 100]`.
fn arb_pl_users() -> impl Strategy<Value = HashMap<String, i64>> {
    prop::collection::hash_map(arb_user_id(), -100i64..=100, 0..5)
}

/// Render a `users` map plus `users_default` into a v12 `m.room.power_levels`
/// content payload. Tests vary only the bits power_of_sender consults.
fn pl_content(users: &HashMap<String, i64>, users_default: i64) -> Value {
    let mut users_json = serde_json::Map::new();
    for (u, v) in users {
        users_json.insert(u.clone(), json!(v));
    }
    json!({ "users_default": users_default, "users": users_json })
}

proptest! {
    /// Property 1: creators (the create event's sender + everyone in
    /// `additional_creators`) always get `i64::MAX`, regardless of what the
    /// PL's `users` map or `users_default` would otherwise assign them.
    /// Spec v12: creators "cannot be demoted to a lower power level, even
    /// through m.room.power_levels".
    #[test]
    fn power_of_sender_creator_always_max(
        creators in arb_distinct_user_ids(4),
        pl_users in arb_pl_users(),
        pl_users_default in -100i64..=100,
        creator_idx in 0usize..32,
    ) {
        let primary = creators[0].clone();
        let additional: Vec<String> = creators[1..].to_vec();

        let create = prop_build_create(&primary, &additional, true);
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        provider.insert(Arc::new(create));

        // PL authored by the primary creator (anyone in `creators` could
        // author it — the choice doesn't affect power_of_sender for senders).
        let pl = prop_build_pl(&room_id, &primary, pl_content(&pl_users, pl_users_default), vec![create_id.clone()]);
        let pl_id = pl.event_id.clone();
        provider.insert(Arc::new(pl));

        // Pick any creator as the sender.
        let sender = &creators[creator_idx % creators.len()];
        let msg = prop_build_msg(&room_id, sender, vec![create_id, pl_id]);
        prop_assert_eq!(power_of_sender(&msg, &provider).unwrap(), i64::MAX);
    }

    /// Property 2: a PL marked `rejected: true` in the provider is treated
    /// identically to "no PL at all". For any pair (sender, PL content) the
    /// power computed with a rejected PL must equal the power computed with
    /// no PL referenced from auth_events.
    #[test]
    fn power_of_sender_rejected_pl_equals_no_pl(
        pl_users in arb_pl_users(),
        pl_users_default in -100i64..=100,
        sender in arb_user_id(),
    ) {
        let creator = "@creator:example.org";

        // Setup A: create + rejected PL referenced in the message's auth_events.
        let create_a = prop_build_create(creator, &[], true);
        let room_id_a = room_id_from_create(&create_a.event_id);
        let create_id_a = create_a.event_id.clone();
        let mut provider_a = InMemoryStateProvider::new();
        provider_a.insert(Arc::new(create_a));
        let pl = prop_build_pl(&room_id_a, creator, pl_content(&pl_users, pl_users_default), vec![create_id_a.clone()]);
        let pl_id = pl.event_id.clone();
        provider_a.insert({
            let mut pl = pl;
            pl.rejected = true;
            Arc::new(pl)
        });
        let msg_a = prop_build_msg(&room_id_a, &sender, vec![create_id_a, pl_id]);

        // Setup B: create only — no PL in auth_events.
        let create_b = prop_build_create(creator, &[], true);
        let room_id_b = room_id_from_create(&create_b.event_id);
        let create_id_b = create_b.event_id.clone();
        let mut provider_b = InMemoryStateProvider::new();
        provider_b.insert(Arc::new(create_b));
        let msg_b = prop_build_msg(&room_id_b, &sender, vec![create_id_b]);

        let power_a = power_of_sender(&msg_a, &provider_a).unwrap();
        let power_b = power_of_sender(&msg_b, &provider_b).unwrap();
        prop_assert_eq!(power_a, power_b);
    }

    /// Property 3: for a non-creator sender, power equals the PL's `users`
    /// lookup if present, else `users_default`. Couples the impl to the
    /// `PowerLevels::user_power` semantics at the property level.
    #[test]
    fn power_of_sender_non_creator_matches_pl_lookup(
        pl_users in arb_pl_users(),
        pl_users_default in -100i64..=100,
        sender in arb_user_id(),
    ) {
        let creator = "@creator:example.org";
        // Filter out the case where the random sender happens to land on
        // "@creator:example.org" — that's property 1's domain.
        prop_assume!(sender != creator);

        let create = prop_build_create(creator, &[], true);
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        provider.insert(Arc::new(create));

        let pl = prop_build_pl(&room_id, creator, pl_content(&pl_users, pl_users_default), vec![create_id.clone()]);
        let pl_id = pl.event_id.clone();
        provider.insert(Arc::new(pl));

        let msg = prop_build_msg(&room_id, &sender, vec![create_id, pl_id]);
        let power = power_of_sender(&msg, &provider).unwrap();
        let expected = pl_users.get(&sender).copied().unwrap_or(pl_users_default);
        prop_assert_eq!(power, expected);
    }

    /// Property 4: `reverse_topological_power_sort` output is a permutation
    /// of its input (no fabrication, no loss). Catches "drop event on cycle
    /// detection" or "duplicate emission" bugs at the algorithm level.
    #[test]
    fn sort_output_is_permutation_of_input(
        (provider, ids) in arb_provider_with_ids(),
        take in 0usize..16,
    ) {
        let n = take.min(ids.len());
        let subset: HashSet<OwnedEventId> = ids.iter().take(n).cloned().collect();
        let sorted = reverse_topological_power_sort(&subset, &provider).unwrap();
        prop_assert_eq!(sorted.len(), subset.len());
        let sorted_set: HashSet<OwnedEventId> = sorted.into_iter().collect();
        prop_assert_eq!(sorted_set, subset);
    }

    /// Property 5: for every (parent, child) pair where parent ∈
    /// child.auth_events and both are in the input subset, parent's index in
    /// the output is strictly less than child's. The defining
    /// reverse-topological invariant — case tests pin specific shapes, this
    /// generalises to every subset of every closed-under-auth_events provider.
    #[test]
    fn sort_parents_come_before_children(
        (provider, ids) in arb_provider_with_ids(),
        take in 0usize..16,
    ) {
        let n = take.min(ids.len());
        let subset: HashSet<OwnedEventId> = ids.iter().take(n).cloned().collect();
        let sorted = reverse_topological_power_sort(&subset, &provider).unwrap();
        let pos: HashMap<OwnedEventId, usize> = sorted
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        for eid in &subset {
            let info = provider
                .get_event(eid)
                .expect("lookup ok")
                .expect("subset event in provider");
            for parent in &info.auth_events {
                if subset.contains(parent) {
                    let p_idx = pos[parent];
                    let e_idx = pos[eid];
                    prop_assert!(
                        p_idx < e_idx,
                        "parent {parent} (idx {p_idx}) must come before child {eid} (idx {e_idx})"
                    );
                }
            }
        }
    }

    /// Property 7: every key written by IAC is either present in the initial
    /// state or matches `(event_type, state_key)` of some event in `sorted`.
    /// "No fabricated keys".
    ///
    /// Strategy: multiple m.room.create events (each by a distinct creator)
    /// — every create accepts under rule 1.5, so the writeback branch fires
    /// non-vacuously. All accepted writes land under the same key
    /// `("m.room.create", "")` which is a legitimate sorted-event key.
    #[test]
    fn iac_output_keys_only_from_initial_or_sorted(
        creators in arb_distinct_user_ids(4),
        initial in arb_state_set(),
    ) {
        let mut provider = InMemoryStateProvider::new();
        let mut sorted: Vec<OwnedEventId> = Vec::new();
        let mut allowed_keys: HashSet<(String, String)> = initial.keys().cloned().collect();

        for c in &creators {
            let create = prop_build_create(c, &[], true);
            let id = create.event_id.clone();
            allowed_keys.insert((create.event_type.clone(), create.state_key.clone().unwrap_or_default()));
            sorted.push(id);
            provider.insert(Arc::new(create));
        }

        let resolved = iterative_auth_checks(&sorted, initial, &provider).unwrap();
        for k in resolved.keys() {
            prop_assert!(
                allowed_keys.contains(k),
                "fabricated key in resolved state: {k:?}"
            );
        }
    }

    /// Property 9: if every event in `sorted` is provider-marked
    /// `rejected: true`, IAC's output equals the initial state byte-for-byte.
    /// Pins the early-skip branch at the property level — sweeps arbitrary
    /// graph shapes via `arb_provider_with_ids`.
    #[test]
    fn iac_all_rejected_sorted_yields_initial_state(
        (provider, ids) in arb_provider_with_ids(),
        initial in arb_state_set(),
    ) {
        // Re-insert every event in the provider with `rejected: true`. The
        // strategy returned them all as accepted; we override.
        let mut rejected_provider = InMemoryStateProvider::new();
        for id in &ids {
            let info = provider
                .get_event(id)
                .expect("lookup ok")
                .expect("provider returned its own id");
            rejected_provider.insert({
                let mut e = (*info).clone();
                e.rejected = true;
                Arc::new(e)
            });
        }
        let resolved = iterative_auth_checks(&ids, initial.clone(), &rejected_provider).unwrap();
        prop_assert_eq!(resolved, initial);
    }

    /// Phase 4c property: `resolve_state` is the identity on a single state
    /// set. With one input, `separate()` makes everything unconflicted, the
    /// conflict path is empty, and the final overlay restores the input.
    #[test]
    fn resolve_state_identity_for_single_state_set(
        creators in arb_distinct_user_ids(3),
    ) {
        let creator = &creators[0];
        let create = prop_build_create(creator, &[], true);
        let create_id = create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        provider.insert(Arc::new(create));
        let mut s = StateMap::new();
        s.insert(("m.room.create".to_string(), String::new()), create_id);
        let resolved = resolve_state(&[&s], &provider).unwrap();
        prop_assert_eq!(resolved, s);
    }

    /// Phase 4c property: when N state sets each carry a distinct
    /// `m.room.create` event (no other entries), `resolve_state` picks one
    /// of the candidates. Pinning *which* one is the algorithm's job — the
    /// property only guarantees the resolved value comes from the input
    /// candidates (no fabrication).
    #[test]
    fn resolve_state_with_only_create_events_picks_one_candidate(
        creators in arb_distinct_user_ids(4),
    ) {
        let mut provider = InMemoryStateProvider::new();
        let mut create_ids: Vec<OwnedEventId> = Vec::new();
        for c in &creators {
            let create = prop_build_create(c, &[], true);
            create_ids.push(create.event_id.clone());
            provider.insert(Arc::new(create));
        }
        let state_sets: Vec<StateMap<OwnedEventId>> = create_ids
            .iter()
            .map(|id| {
                let mut s = StateMap::new();
                s.insert(("m.room.create".to_string(), String::new()), id.clone());
                s
            })
            .collect();
        let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
        let resolved = resolve_state(&refs, &provider).unwrap();
        let picked = resolved
            .get(&("m.room.create".to_string(), String::new()))
            .expect("create resolved");
        prop_assert!(
            create_ids.contains(picked),
            "resolved create id {picked} not among input candidates"
        );
    }
}

// ---------- RoomCore state-DAG forks & merges: "every event accepted" ----------
//
// A *structural* generator for state-DAG forks and merges driven through
// `RoomCore::apply_pdu`. Alice is the sole sender and the room creator, so she
// holds implicit maximum power (v12 rule 10.4): every state event (topic) and
// message event she authors passes auth, and state events are never
// soft-failed. The oracle is therefore trivial — `apply_pdu` must *accept*
// every generated event (none `rejected`, none `soft_failed`). Any rejection
// is unambiguously a generator flaw (broken `prev_events` / `prev_state_events`
// linkage, a topological-order violation, a timestamp collision yielding a
// duplicate event_id, or head-set mistracking) rather than an auth-engine
// verdict — exactly what we want to surface first, before the auth-aware
// generator (which needs a faithful shadow model) is layered on top.
//
// The generator tracks the two head-sets the same way `apply_pdu` does —
// accepting an event E updates a head-set as `(heads \ E.prevs) ∪ {E}`. A fork
// is two events naming the same parent in `prev_state_events`; a merge is one
// event naming two parents. Timestamps are a pure function of build order
// (`base + counter`) so event_ids are distinct and reproducible, and every
// event is built exactly once. Construction is split from application
// (`build_dag` → `apply_dag`) so the same DAG can be replayed in many
// topological orders; build order is one such order, by construction.
//
// Two properties run over the generated DAGs: every event is accepted (the
// structural-generator oracle), and the result is independent of application
// order (`apply_dag` in random topological orders converges on the same
// resolved state and head-sets — `state_at_heads` is a pure function of the DAG
// and the final head-sets are "events unreferenced by any applied event").

const FORK_MERGE_SENDER: &str = "@alice:example.org";
const FORK_MERGE_TS_BASE: u64 = 1_700_000_000_000;

/// One generator step. `Extend`/`Fork` carry a head selector applied modulo the
/// live state-head count; `Merge` collapses *all* live state heads into one
/// event (falling back to a linear extend when fewer than two exist), so a merge
/// off ≥3 concurrent heads produces a genuine multi-head merge.
#[derive(Debug, Clone)]
enum DagOp {
    Extend(usize),
    Fork(usize),
    Merge,
    Message,
}

fn dag_op() -> impl Strategy<Value = DagOp> {
    prop_oneof![
        3 => any::<u8>().prop_map(|i| DagOp::Extend(i as usize)),
        3 => any::<u8>().prop_map(|i| DagOp::Fork(i as usize)),
        3 => Just(DagOp::Merge),
        2 => Just(DagOp::Message),
    ]
}

/// `(heads \ remove) ∪ {add}`, preserving order and avoiding duplicates —
/// mirrors the head-set update `apply_pdu` performs on acceptance.
fn heads_after(
    heads: &[OwnedEventId],
    remove: &[OwnedEventId],
    add: &OwnedEventId,
) -> Vec<OwnedEventId> {
    let mut out: Vec<OwnedEventId> = heads
        .iter()
        .filter(|h| !remove.contains(h))
        .cloned()
        .collect();
    if !out.contains(add) {
        out.push(add.clone());
    }
    out
}

fn build_create() -> Event {
    EventBuilder::new(
        FORK_MERGE_SENDER.parse().expect("user"),
        "m.room.create".to_owned(),
    )
    .state_key(String::new())
    .content(json!({ "room_version": ROOM_VERSION_ID }))
    .origin_server_ts(FORK_MERGE_TS_BASE)
    .build()
    .expect("create builds")
}

/// Alice's self-join, rule-5.3.1 shape: `prev_events == prev_state_events ==
/// [create_id]`. `auth_events` is left for `apply_pdu` to compute. Thin
/// `FORK_MERGE_SENDER` specialisation of the general `adv_state_event`.
fn build_join(room: &RoomId, create_id: &OwnedEventId, ts: u64) -> Event {
    adv_state_event(
        room,
        FORK_MERGE_SENDER,
        "m.room.member",
        FORK_MERGE_SENDER,
        json!({ "membership": "join" }),
        ts,
        vec![create_id.clone()],
    )
}

/// A topic state event on the given state heads (used for both `prev_events`
/// and `prev_state_events` so the timeline DAG mirrors the state DAG through
/// state events). `topic` content is redaction-stripped, so distinct ids rely
/// on the monotonic `ts`.
fn build_topic(room: &RoomId, ts: u64, prev: Vec<OwnedEventId>) -> Event {
    adv_state_event(
        room,
        FORK_MERGE_SENDER,
        "m.room.topic",
        "",
        json!({ "topic": format!("t{ts}") }),
        ts,
        prev,
    )
}

/// A message on the current timeline heads. Carries the state heads in
/// `prev_state_events` so state-before-event resolves to the live state and
/// the soft-fail check passes (alice is joined).
fn build_message(
    room: &RoomId,
    ts: u64,
    timeline_heads: Vec<OwnedEventId>,
    state_heads: Vec<OwnedEventId>,
) -> Event {
    adv_message_event(room, FORK_MERGE_SENDER, ts, timeline_heads, state_heads)
}

/// The verdict `apply_pdu` reaches on an event, in build order. Drives the
/// generalised `expected_heads` oracle: only an `Accepted` event becomes a
/// forward extremity or drops its parents from the head-sets. A `Rejected`
/// event mutates no head-set (apply_pdu returns before any advance); a
/// `SoftFailed` (non-state) event is persisted but never becomes a timeline
/// head and never drops its `prev_events` parents (synapse#5269). State events
/// never soft-fail, so `SoftFailed` only ever tags a non-state event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Accepted,
    Rejected,
    SoftFailed,
}

/// A built DAG: the room id and the events in build order (which is itself a
/// valid topological order over `prev_events ∪ prev_state_events`). Each stored
/// event is the *persisted* form, so its `rejected` / `soft_failed` flags carry
/// the verdict the generator observed — `verdict(i)` recovers it via `classify`
/// rather than storing a parallel array (which would risk length-desync). For
/// the creator-only generator the stored events are pre-apply (flags false), so
/// every verdict is `Accepted`.
struct Dag {
    room_id: OwnedRoomId,
    events: Vec<Event>,
}

impl Dag {
    /// The verdict for event `i`, derived from its persisted flags.
    fn verdict(&self, i: usize) -> Verdict {
        classify(&self.events[i])
    }
}

/// Builds a creator-only fork/merge DAG from a recipe — purely, with no
/// `RoomCore`. Tracks the two head-sets via `heads_after` so each event lands
/// on the right parents; `apply_pdu`'s head bookkeeping is mirrored here, not
/// consulted. Separating construction from application is what lets the same
/// DAG be replayed in many orders.
struct DagBuilder {
    room_id: OwnedRoomId,
    state_heads: Vec<OwnedEventId>,
    timeline_heads: Vec<OwnedEventId>,
    ts: u64,
    events: Vec<Event>,
}

impl DagBuilder {
    fn new() -> Self {
        let create = build_create();
        let room_id = room_id_from_create(&create.event_id);
        let create_id = create.event_id.clone();
        let mut builder = DagBuilder {
            room_id,
            state_heads: Vec::new(),
            timeline_heads: Vec::new(),
            ts: FORK_MERGE_TS_BASE,
            events: vec![create],
        };
        let join_ts = builder.next_ts();
        let join = build_join(&builder.room_id, &create_id, join_ts);
        let join_id = join.event_id.clone();
        builder.events.push(join);
        builder.state_heads = vec![join_id.clone()];
        builder.timeline_heads = vec![join_id];
        builder
    }

    fn next_ts(&mut self) -> u64 {
        self.ts += 1;
        self.ts
    }

    /// Append a topic state event on `prev` (a subset of the state heads) and
    /// advance both head-sets exactly as `apply_pdu` does for an accepted state
    /// event.
    fn push_state_event(&mut self, prev: Vec<OwnedEventId>) {
        let ts = self.next_ts();
        let event = build_topic(&self.room_id, ts, prev.clone());
        let id = event.event_id.clone();
        self.events.push(event);
        self.state_heads = heads_after(&self.state_heads, &prev, &id);
        self.timeline_heads = heads_after(&self.timeline_heads, &prev, &id);
    }

    fn run_op(&mut self, op: DagOp) {
        let len = self.state_heads.len();
        match op {
            DagOp::Extend(i) => {
                let tip = self.state_heads[i % len].clone();
                self.push_state_event(vec![tip]);
            }
            DagOp::Fork(i) => {
                let tip = self.state_heads[i % len].clone();
                self.push_state_event(vec![tip.clone()]);
                self.push_state_event(vec![tip]);
            }
            DagOp::Merge => {
                if len >= 2 {
                    // Merge every live state head, so a merge off ≥3 concurrent
                    // heads is a genuine multi-head merge (≥3 prev_state_events).
                    let heads = self.state_heads.clone();
                    self.push_state_event(heads);
                } else {
                    let tip = self.state_heads[0].clone();
                    self.push_state_event(vec![tip]);
                }
            }
            DagOp::Message => {
                let ts = self.next_ts();
                let event = build_message(
                    &self.room_id,
                    ts,
                    self.timeline_heads.clone(),
                    self.state_heads.clone(),
                );
                let id = event.event_id.clone();
                self.events.push(event);
                // A message advances only the timeline DAG, and references every
                // timeline head, so that head-set collapses to just the message.
                let prev = self.timeline_heads.clone();
                self.timeline_heads = heads_after(&self.timeline_heads, &prev, &id);
            }
        }
    }

    fn finish(self) -> Dag {
        let DagBuilder {
            room_id,
            state_heads,
            timeline_heads,
            events,
            ..
        } = self;
        // The creator-only generator is all-accepted by construction (alice is
        // omnipotent and always joined): the stored events are pre-apply, so
        // their flags are false and `verdict(i)` is uniformly `Accepted` —
        // `expected_heads` then reduces to the original "FE = unreferenced
        // event" rule.
        let dag = Dag { room_id, events };
        // Cross-check the builder's own running head bookkeeping (`heads_after`,
        // used only to pick parents) against the structural oracle. Without this
        // a `heads_after` bug would silently emit a different-but-valid DAG that
        // both RoomCore and `expected_heads` still agree on, masking it — there
        // are three implementations of the FE rule and P2 alone only cross-checks
        // two of them.
        let (expected_timeline, expected_state) = expected_heads(&dag);
        let builder_timeline: BTreeSet<OwnedEventId> = timeline_heads.into_iter().collect();
        let builder_state: BTreeSet<OwnedEventId> = state_heads.into_iter().collect();
        assert_eq!(
            builder_timeline, expected_timeline,
            "DagBuilder timeline heads diverged from the raw-DAG oracle"
        );
        assert_eq!(
            builder_state, expected_state,
            "DagBuilder state heads diverged from the raw-DAG oracle"
        );
        dag
    }
}

fn build_dag(ops: Vec<DagOp>) -> Dag {
    let mut builder = DagBuilder::new();
    for op in ops {
        builder.run_op(op);
    }
    builder.finish()
}

/// The order-invariant result of applying a DAG: the resolved state and both
/// head-sets. These are pure functions of the DAG, so any difference across
/// application orders is a bug.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    current_state: StateMap<OwnedEventId>,
    state_fes: BTreeSet<OwnedEventId>,
    timeline_fes: BTreeSet<OwnedEventId>,
}

/// Apply `dag.events` in `order` to a fresh `RoomCore`, asserting every event
/// is accepted (not rejected, not soft-failed) and that the folded
/// `UpdateCurrentState` deltas reconstruct the resolved current_state. Inserts
/// the accepted (auth_events-stamped) form into the provider so later
/// `auth_chain` walks resolve. Returns the resolved state and both head-sets.
fn apply_dag(dag: &Dag, order: &[usize]) -> Result<Outcome, TestCaseError> {
    let mut room = RoomCore::new(dag.room_id.clone());
    let mut provider = InMemoryStateProvider::new();
    let mut accumulated: StateMap<OwnedEventId> = StateMap::new();
    for &i in order {
        let event = dag.events[i].clone();
        let id = event.event_id.clone();
        let effects = room.apply_pdu(event, &provider).map_err(|e| {
            TestCaseError::fail(format!("apply_pdu errored on {id} (broken ancestry?): {e}"))
        })?;
        let persisted = persist_effect(&effects).ok_or_else(|| {
            TestCaseError::fail(format!("apply_pdu emitted no Persist effect for {id}"))
        })?;
        for effect in effects {
            if let Effect::UpdateCurrentState(delta) = effect {
                for (key, value) in delta {
                    match value {
                        Some(eid) => {
                            accumulated.insert(key, eid);
                        }
                        None => {
                            accumulated.remove(&key);
                        }
                    }
                }
            }
        }
        prop_assert!(
            !persisted.rejected,
            "creator-authored event {id} was rejected — generator flaw"
        );
        prop_assert!(
            !persisted.soft_failed,
            "creator-authored event {id} was soft-failed — generator flaw"
        );
        provider.insert(persisted);
    }
    let current_state: StateMap<OwnedEventId> = room
        .current_state()
        .iter()
        .map(|(k, v)| (k.clone(), v.event_id.clone()))
        .collect();
    prop_assert_eq!(
        &accumulated,
        &current_state,
        "accumulated UpdateCurrentState deltas diverged from the resolved current_state"
    );
    Ok(Outcome {
        current_state,
        state_fes: room.state_forward_extremities().clone(),
        timeline_fes: room.forward_extremities().clone(),
    })
}

/// A random topological order of the DAG over `prev_events ∪ prev_state_events`
/// edges — every parent emitted before its child. Both edge kinds matter: if a
/// child were applied before a parent, `apply_pdu`'s head removal (`heads \
/// prevs`) would miss that parent and the final head-sets would diverge. Kahn's
/// algorithm with the ready-set pick driven by `entropy` (cycled if short).
fn random_topo_order(dag: &Dag, entropy: &[usize]) -> Vec<usize> {
    let n = dag.events.len();
    let mut index_of: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, e) in dag.events.iter().enumerate() {
        index_of.insert(e.event_id.as_str(), i);
    }
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indegree = vec![0usize; n];
    for (i, e) in dag.events.iter().enumerate() {
        let mut parents: HashSet<usize> = HashSet::new();
        for p in e.prev_events.iter().chain(e.prev_state_events.iter()) {
            if let Some(&j) = index_of.get(p.as_str()) {
                parents.insert(j);
            }
        }
        indegree[i] = parents.len();
        for j in parents {
            children[j].push(i);
        }
    }
    let mut ready: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    let mut step = 0usize;
    while !ready.is_empty() {
        let pick = if entropy.is_empty() {
            0
        } else {
            entropy[step % entropy.len()] % ready.len()
        };
        let node = ready.swap_remove(pick);
        order.push(node);
        step += 1;
        for &c in &children[node] {
            indegree[c] -= 1;
            if indegree[c] == 0 {
                ready.push(c);
            }
        }
    }
    order
}

/// Recompute both head-sets straight from the raw DAG structure, independent of
/// `RoomCore` (P2's oracle). A *timeline* forward extremity is an accepted event
/// named in no *accepted* event's `prev_events`; a *state* forward extremity is
/// an accepted state event named in no *accepted state* event's
/// `prev_state_events`. Only state events advance the state DAG — a message
/// carries `prev_state_events` but never supersedes a state head — so message
/// references are excluded from the state side.
///
/// Verdict-aware (mirrors `apply_pdu`'s head bookkeeping exactly): a `Rejected`
/// event mutates no head-set, so it is neither a head nor does it drop its
/// parents; a `SoftFailed` event (non-state only) is likewise neither a timeline
/// head nor drops its `prev_events` parents (synapse#5269 — the parents stay
/// extremities until a *non*-soft-failed successor references them). Only an
/// `Accepted` event is head-eligible and removes the parents it names. On the
/// creator-only profile every verdict is `Accepted`, so this reduces to the
/// plain "FE = unreferenced event" rule.
fn expected_heads(dag: &Dag) -> (BTreeSet<OwnedEventId>, BTreeSet<OwnedEventId>) {
    let mut referenced_timeline: HashSet<&str> = HashSet::new();
    let mut referenced_state: HashSet<&str> = HashSet::new();
    for e in &dag.events {
        // Only an accepted event advances the head-sets, and only an advancing
        // event drops the parents it references.
        if classify(e) != Verdict::Accepted {
            continue;
        }
        for p in &e.prev_events {
            referenced_timeline.insert(p.as_str());
        }
        if e.state_key.is_some() {
            for p in &e.prev_state_events {
                referenced_state.insert(p.as_str());
            }
        }
    }
    let mut timeline_fes = BTreeSet::new();
    let mut state_fes = BTreeSet::new();
    for e in &dag.events {
        if classify(e) != Verdict::Accepted {
            continue;
        }
        if !referenced_timeline.contains(e.event_id.as_str()) {
            timeline_fes.insert(e.event_id.clone());
        }
        if e.state_key.is_some() && !referenced_state.contains(e.event_id.as_str()) {
            state_fes.insert(e.event_id.clone());
        }
    }
    (timeline_fes, state_fes)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A creator-only fork/merge DAG: every generated event must be accepted by
    /// `apply_pdu`, and folding the emitted `UpdateCurrentState` deltas must
    /// reconstruct the resolved current_state.
    #[test]
    fn creator_only_fork_merge_dag_accepts_every_event(
        ops in prop::collection::vec(dag_op(), 1..14),
    ) {
        let dag = build_dag(ops);
        let build_order: Vec<usize> = (0..dag.events.len()).collect();
        let outcome = apply_dag(&dag, &build_order)?;
        // The create event and alice's membership are always resolved.
        prop_assert!(
            outcome
                .current_state
                .contains_key(&("m.room.create".to_string(), String::new())),
            "create event missing from current_state"
        );
        prop_assert!(
            outcome
                .current_state
                .contains_key(&("m.room.member".to_string(), FORK_MERGE_SENDER.to_string())),
            "alice's membership missing from current_state"
        );
        // P2 — head-set bookkeeping: RoomCore's tracked forward extremities must
        // match the sets recomputed directly from the raw DAG structure. P1
        // proves the head-sets are *consistent* across orders; this proves they
        // are *correct*. The oracle is order-independent, so asserting it on the
        // build order plus P1 establishes it for every order.
        let (expected_timeline, expected_state) = expected_heads(&dag);
        prop_assert_eq!(
            &outcome.timeline_fes,
            &expected_timeline,
            "timeline forward extremities diverged from the raw-DAG oracle"
        );
        prop_assert_eq!(
            &outcome.state_fes,
            &expected_state,
            "state forward extremities diverged from the raw-DAG oracle"
        );

        // P5 — message-event invariants. A non-state event never touches
        // current_state or the state DAG: it appears in neither current_state
        // nor the state forward extremities, only (possibly) the timeline ones.
        // Equivalently, every timeline FE that is *not* a state FE is a message.
        // (The reverse direction — a state FE that is not a timeline FE — is
        // expected and is *not* a message: a state head a later message
        // referenced in `prev_events` stays a state head but drops out of the
        // timeline heads.) The "soft-failed" half of the divergence rule is
        // vacuous here — nothing soft-fails in the creator-only profile.
        let message_ids: HashSet<&str> = dag
            .events
            .iter()
            .filter(|e| e.state_key.is_none())
            .map(|e| e.event_id.as_str())
            .collect();
        for id in outcome.current_state.values() {
            prop_assert!(
                !message_ids.contains(id.as_str()),
                "message event {id} leaked into current_state"
            );
        }
        for id in &outcome.state_fes {
            prop_assert!(
                !message_ids.contains(id.as_str()),
                "message event {id} leaked into the state forward extremities"
            );
        }
        for id in outcome.timeline_fes.difference(&outcome.state_fes) {
            prop_assert!(
                message_ids.contains(id.as_str()),
                "timeline FE {id} diverges from the state FEs but is not a message"
            );
        }
    }

    /// Order-independence: the same fork/merge DAG applied in different
    /// topological orders must yield identical resolved state and head-sets.
    /// State resolution is a pure function of the DAG and the final head-sets
    /// are "events unreferenced by any applied event", so order cannot matter —
    /// any divergence is a real state-res or head-tracking bug.
    #[test]
    fn creator_only_fork_merge_order_independent(
        ops in prop::collection::vec(dag_op(), 1..14),
        entropies in prop::collection::vec(
            prop::collection::vec(any::<u8>().prop_map(|b| b as usize), 1..40),
            2..5,
        ),
    ) {
        let dag = build_dag(ops);
        let build_order: Vec<usize> = (0..dag.events.len()).collect();
        let baseline = apply_dag(&dag, &build_order)?;
        for entropy in &entropies {
            let order = random_topo_order(&dag, entropy);
            prop_assert_eq!(order.len(), dag.events.len(), "topo order dropped events");
            let outcome = apply_dag(&dag, &order)?;
            prop_assert_eq!(
                &outcome,
                &baseline,
                "fork/merge result depended on application order"
            );
        }
    }
}

/// Coverage corpus (non-shrinking generator-saturation guard). NOT a property
/// test: it asserts the `dag_op()` strategy *actually produces* the interesting
/// DAG shapes often enough, across a large deterministic sample, that the
/// properties above are not passing vacuously on degenerate linear chains.
///
/// A property test asserts "invariant P holds on every case"; this asserts
/// "across the sample, each shape appears in at least a floor fraction of
/// cases". It is non-shrinking on purpose — a shrunk single DAG lacking a shape
/// tells you nothing; the signal is the aggregate count. The floors are set
/// well below the measured rates so a generator tweak that *erodes* (not just
/// eliminates) coverage trips them, while leaving slack against benign drift.
///
/// Buckets covered now (creator-only profile):
/// - a real merge (a state event whose `prev_state_events` names ≥2 heads),
/// - a multi-head merge (≥3 heads in one event),
/// - a DAG containing a message (drives the P5 timeline/state divergence).
///
/// TODO(P4 shadow model): the remaining PLAN-listed shapes — a merge resolving a
/// power-level conflict, a rejected event with a dependent child, and a
/// soft-failed message — require the multi-user adversarial generator and cannot
/// occur in the creator-only profile (alice is omnipotent and always joined, so
/// nothing rejects or soft-fails). Add their floors when that generator lands.
#[test]
fn fork_merge_generator_coverage_corpus() {
    const SAMPLE: usize = 2000;
    let strat = prop::collection::vec(dag_op(), 1..14);
    let mut runner = TestRunner::deterministic();

    let (mut real_merge, mut multihead_merge, mut with_message) = (0usize, 0usize, 0usize);
    for _ in 0..SAMPLE {
        let ops = strat
            .new_tree(&mut runner)
            .expect("strategy produces a value")
            .current();
        let dag = build_dag(ops);
        let mut saw_real_merge = false;
        let mut saw_multihead = false;
        let mut saw_message = false;
        for e in &dag.events {
            if e.state_key.is_none() {
                saw_message = true;
                continue;
            }
            let distinct: HashSet<&str> = e.prev_state_events.iter().map(|p| p.as_str()).collect();
            if distinct.len() >= 2 {
                saw_real_merge = true;
            }
            if distinct.len() >= 3 {
                saw_multihead = true;
            }
        }
        real_merge += usize::from(saw_real_merge);
        multihead_merge += usize::from(saw_multihead);
        with_message += usize::from(saw_message);
    }

    // Floors (deterministic sample, so these are reproducible). Measured rates
    // at authoring time were far higher — real_merge 54%, multi-head 25%,
    // message 69% — so these guard against erosion, not just disappearance.
    let floor = |pct: usize| SAMPLE * pct / 100;
    assert!(
        real_merge >= floor(30),
        "real (≥2-head) merges only in {real_merge}/{SAMPLE} DAGs — generator coverage eroded"
    );
    assert!(
        multihead_merge >= floor(5),
        "multi-head (≥3-head) merges only in {multihead_merge}/{SAMPLE} DAGs — generator coverage eroded"
    );
    assert!(
        with_message >= floor(30),
        "messages only in {with_message}/{SAMPLE} DAGs — P5 divergence under-exercised"
    );
}

// ======================================================================
// Phase 1: multi-user shadow-auth adversarial generator
// ======================================================================
//
// Lifts the creator-only generator to a bounded user pool with real
// membership / power-level transitions and deliberately unauthorised events,
// so the DAGs now contain genuine rejects, soft-fails and power-level conflict
// merges. The construction strategy (settled in PLAN.md) is to drive a *real*
// `RoomCore` in build order during construction: heads are tracked from
// reality (advance only on the real verdict, mirroring `apply_pdu`'s
// drop-rejected-from-heads), the verdict of every event is recorded as it is
// applied, and the output is still a pure replayable `Dag` (now carrying its
// per-event `Verdict`s). The shadow model below is therefore only a *yield*
// helper for intent selection — a wrong guess lowers the accepted-event yield
// but never corrupts the DAG, because the verdict comes from `apply_pdu`, not
// the shadow.
//
// NOTE on order-(in)dependence (relevant to the Phase 2 properties, recorded
// here so the next session sees it): `reject` is order-independent (it is a
// pure function of the event's `prev_state_events` ancestry), but `soft-fail`
// is NOT — `apply_pdu` checks soft-fail against the room's *resolved*
// `current_state` at apply time (room_core.rs:433), which depends on which
// concurrent events have been applied. So `current_state` and the *state*
// forward extremities are order-independent, but the *timeline* forward
// extremities and soft-fail verdicts are not. Phase 2's order-independence
// property must therefore assert order-invariance only over
// {current_state, state_fes, reject-verdicts}, not over the timeline side.

/// The user pool. Index 0 (alice) is the room creator — omnipotent under v12
/// implicit power, and the sender of the create + initial join. The others
/// start with no power (users_default 0) and must be invited / promoted to do
/// anything privileged, which is exactly what drives the reject / soft-fail
/// coverage.
const ADV_POOL: [&str; 4] = [
    "@alice:example.org",
    "@bob:example.org",
    "@carol:example.org",
    "@dave:example.org",
];
const ADV_CREATOR: usize = 0;
/// Fork-width cap (concurrent state heads) — keeps DAGs shallow so shrunk
/// counterexamples stay readable and shadow desync stays bounded.
const ADV_MAX_FORK_WIDTH: usize = 4;
/// Soft cap on total events per DAG (checked per-op, so a compound op may
/// overshoot slightly).
const ADV_MAX_EVENTS: usize = 20;

/// The verdict `apply_pdu` reaches on a persisted event.
fn classify(event: &Event) -> Verdict {
    if event.rejected {
        Verdict::Rejected
    } else if event.soft_failed {
        Verdict::SoftFailed
    } else {
        Verdict::Accepted
    }
}

/// The persisted event from one `apply_pdu` effect list (the last `Persist`
/// wins, matching `apply_pdu`'s single-`Persist` contract; the `Arc` clone is
/// cheap). `None` only on the idempotency short-circuit (empty effects) — every
/// caller treats that as a failure. Borrows so `apply_dag` can still fold the
/// `UpdateCurrentState` deltas from the same list. Shared by the build path
/// (`AdvCtx::apply`) and both replay paths (`apply_dag` / `apply_adv_dag`).
fn persist_effect(effects: &[Effect]) -> Option<Arc<Event>> {
    effects.iter().rev().find_map(|effect| match effect {
        Effect::Persist { event } => Some(event.clone()),
        _ => None,
    })
}

/// The order-INVARIANT slice of a verdict vector: rejected-or-not, per event.
/// A reject is a pure function of the event's `prev_state_events` ancestry, so
/// this projection is identical across application orders — unlike the full
/// verdict, which distinguishes `SoftFailed` (order-dependent: checked against
/// the live resolved `current_state`, room_core.rs:433).
fn reject_projection(verdicts: &[Verdict]) -> Vec<bool> {
    verdicts.iter().map(|v| *v == Verdict::Rejected).collect()
}

/// Auth-relevant projection of the resolved `current_state` — the shadow model.
/// Coverage device only (see the module note): used to *realise* an intent into
/// a concrete sender/target the model believes is valid (or, for the
/// `Unauthorised` path, one it believes fails). Defaults mirror
/// `auth_rules::PowerLevels::parse`.
struct Shadow {
    members: HashMap<String, String>,
    pl_users: HashMap<String, i64>,
    users_default: i64,
    events_default: i64,
    state_default: i64,
    ban: i64,
    kick: i64,
    invite: i64,
}

impl Shadow {
    fn from_state(state: &StateMap<Arc<Event>>) -> Self {
        let mut shadow = Shadow {
            members: HashMap::new(),
            pl_users: HashMap::new(),
            users_default: 0,
            events_default: 0,
            state_default: 50,
            ban: 50,
            kick: 50,
            invite: 0,
        };
        for ((etype, sk), ev) in state.iter() {
            let content: Value = serde_json::from_str(ev.content.get()).unwrap_or(Value::Null);
            match etype.as_str() {
                "m.room.member" => {
                    if let Some(m) = content.get("membership").and_then(Value::as_str) {
                        shadow.members.insert(sk.clone(), m.to_owned());
                    }
                }
                "m.room.power_levels" => {
                    if let Some(users) = content.get("users").and_then(Value::as_object) {
                        for (u, v) in users {
                            if let Some(n) = v.as_i64() {
                                shadow.pl_users.insert(u.clone(), n);
                            }
                        }
                    }
                    let get = |key: &str, def: i64| {
                        content.get(key).and_then(Value::as_i64).unwrap_or(def)
                    };
                    shadow.users_default = get("users_default", 0);
                    shadow.events_default = get("events_default", 0);
                    shadow.state_default = get("state_default", 50);
                    shadow.ban = get("ban", 50);
                    shadow.kick = get("kick", 50);
                    shadow.invite = get("invite", 0);
                }
                _ => {}
            }
        }
        shadow
    }

    /// Implicit-infinite for the creator (v12), else the user's explicit level
    /// or `users_default`.
    fn power(&self, user: &str) -> i64 {
        if user == ADV_POOL[ADV_CREATOR] {
            i64::MAX
        } else {
            self.pl_users
                .get(user)
                .copied()
                .unwrap_or(self.users_default)
        }
    }

    fn joined(&self, user: &str) -> bool {
        self.members.get(user).map(String::as_str) == Some("join")
    }

    /// A joined user, biased by `seed`; falls back to the creator (always
    /// joined after the initial self-join) so realisation never stalls.
    fn pick_joined(&self, seed: usize) -> &'static str {
        let joined: Vec<&'static str> = ADV_POOL
            .iter()
            .copied()
            .filter(|u| self.joined(u))
            .collect();
        if joined.is_empty() {
            ADV_POOL[ADV_CREATOR]
        } else {
            joined[seed % joined.len()]
        }
    }

    /// The highest-power joined user (the creator if joined) — the actor most
    /// likely to be authorised for a kick/ban.
    fn strongest_joined(&self) -> &'static str {
        ADV_POOL
            .iter()
            .copied()
            .filter(|u| self.joined(u))
            .max_by_key(|u| self.power(u))
            .unwrap_or(ADV_POOL[ADV_CREATOR])
    }

    /// A full `m.room.power_levels` content equal to the current one with
    /// `edits` applied to the `users` map. The creator is omnipotent regardless
    /// of this map, so it always retains the authority to make the next edit.
    fn pl_content(&self, edits: &[(&str, i64)]) -> Value {
        let mut users = self.pl_users.clone();
        for (target, level) in edits {
            users.insert((*target).to_owned(), *level);
        }
        json!({
            "users": users,
            "users_default": self.users_default,
            "events_default": self.events_default,
            "state_default": self.state_default,
            "ban": self.ban,
            "kick": self.kick,
            "invite": self.invite,
        })
    }
}

/// One state-event intent. The head-selection dimension (extend / fork / merge)
/// is orthogonal and lives in `AdvOp`.
#[derive(Debug, Clone)]
enum StateIntent {
    Join,
    Invite,
    Leave,
    Kick,
    Ban,
    Unban,
    Topic,
    Name,
    PlPromote,
    PlDemote,
}

/// Realise an intent into `(sender, event_type, state_key, content)` using the
/// shadow's view of who is joined / powerful. Never fails: a poorly-matched
/// actor just yields a reject, which is wanted.
fn realise(
    intent: &StateIntent,
    shadow: &Shadow,
    a: usize,
    b: usize,
) -> (&'static str, &'static str, String, Value) {
    let non_creator = ADV_POOL[1 + b % 3];
    match intent {
        StateIntent::Join => {
            let u = ADV_POOL[a % 4];
            (
                u,
                "m.room.member",
                u.to_owned(),
                json!({ "membership": "join" }),
            )
        }
        StateIntent::Invite => {
            let s = shadow.pick_joined(a);
            (
                s,
                "m.room.member",
                non_creator.to_owned(),
                json!({ "membership": "invite" }),
            )
        }
        StateIntent::Leave => {
            let u = shadow.pick_joined(a);
            (
                u,
                "m.room.member",
                u.to_owned(),
                json!({ "membership": "leave" }),
            )
        }
        StateIntent::Kick => {
            let s = shadow.strongest_joined();
            (
                s,
                "m.room.member",
                non_creator.to_owned(),
                json!({ "membership": "leave" }),
            )
        }
        StateIntent::Ban => {
            let s = shadow.strongest_joined();
            (
                s,
                "m.room.member",
                non_creator.to_owned(),
                json!({ "membership": "ban" }),
            )
        }
        StateIntent::Unban => {
            let s = shadow.strongest_joined();
            (
                s,
                "m.room.member",
                non_creator.to_owned(),
                json!({ "membership": "leave" }),
            )
        }
        StateIntent::Topic => {
            let s = shadow.pick_joined(a);
            (
                s,
                "m.room.topic",
                String::new(),
                json!({ "topic": format!("t{a}-{b}") }),
            )
        }
        StateIntent::Name => {
            let s = shadow.pick_joined(a);
            (
                s,
                "m.room.name",
                String::new(),
                json!({ "name": format!("n{a}-{b}") }),
            )
        }
        StateIntent::PlPromote => {
            let level = 25 + (a as i64 % 3) * 25; // 25 / 50 / 75
            (
                ADV_POOL[ADV_CREATOR],
                "m.room.power_levels",
                String::new(),
                shadow.pl_content(&[(non_creator, level)]),
            )
        }
        StateIntent::PlDemote => (
            ADV_POOL[ADV_CREATOR],
            "m.room.power_levels",
            String::new(),
            shadow.pl_content(&[(non_creator, 0)]),
        ),
    }
}

fn adv_state_event(
    room: &RoomId,
    sender: &str,
    event_type: &str,
    state_key: &str,
    content: Value,
    ts: u64,
    prev: Vec<OwnedEventId>,
) -> Event {
    EventBuilder::new(
        sender.parse().expect("valid user id"),
        event_type.to_owned(),
    )
    .room_id(room.to_owned())
    .state_key(state_key.to_owned())
    .content(content)
    .prev_events(prev.clone())
    .prev_state_events(prev)
    .origin_server_ts(ts)
    .build()
    .expect("adversarial state event builds")
}

fn adv_message_event(
    room: &RoomId,
    sender: &str,
    ts: u64,
    timeline_prev: Vec<OwnedEventId>,
    state_prev: Vec<OwnedEventId>,
) -> Event {
    EventBuilder::new(
        sender.parse().expect("valid user id"),
        "m.room.message".to_owned(),
    )
    .room_id(room.to_owned())
    .content(json!({ "msgtype": "m.text", "body": format!("m{ts}") }))
    .prev_events(timeline_prev)
    .prev_state_events(state_prev)
    .origin_server_ts(ts)
    .build()
    .expect("adversarial message builds")
}

/// A generator op. Head-selection (Extend / Fork / Merge / ConflictFork) is
/// orthogonal to the state intent; `Cascade`, `Message`, `Unauthorised` and
/// `StaleMessage` are self-contained shapes targeting specific coverage
/// buckets.
#[derive(Debug, Clone)]
enum AdvOp {
    /// One event on a single chosen state head.
    Extend {
        intent: StateIntent,
        head: u8,
        a: u8,
        b: u8,
    },
    /// Two events on the *same* chosen head — widens the state DAG by one head
    /// (degrades to a single Extend when already at the fork-width cap).
    Fork {
        intent: StateIntent,
        head: u8,
        a: u8,
        b: u8,
    },
    /// One event referencing up to `ADV_MAX_FORK_WIDTH` live state heads.
    Merge { intent: StateIntent, a: u8, b: u8 },
    /// Two `m.room.power_levels` siblings off one head editing the *same* user
    /// to different levels — a guaranteed power-level conflict that a later
    /// `Merge` resolves via the mainline sort.
    ConflictFork { a: u8 },
    /// A state event whose `prev_state_events` names the most recent rejected
    /// event, forcing a reference-rejection (rejection cascade). Falls back to
    /// creating a fresh reject when none exists yet.
    Cascade,
    /// A deliberately unauthorised state event (a non-creator setting a name) —
    /// usually rejected, since the sender starts at power 0 (below the
    /// `state_default` of 50); it only slips through if an earlier `PlPromote`
    /// happened to raise that user.
    Unauthorised { a: u8, b: u8 },
    /// A message on the current timeline heads (soft-fails if its sender is not
    /// joined in the resolved current_state).
    Message { a: u8 },
    /// A scripted soft-fail: ensure a victim is joined, kick them, then have
    /// them send a message whose `prev_state_events` are the pre-kick heads —
    /// authorised against state-before-event but soft-failed against the
    /// post-kick current_state.
    StaleMessage { a: u8 },
}

fn state_intent() -> impl Strategy<Value = StateIntent> {
    prop_oneof![
        Just(StateIntent::Join),
        Just(StateIntent::Invite),
        Just(StateIntent::Leave),
        Just(StateIntent::Kick),
        Just(StateIntent::Ban),
        Just(StateIntent::Unban),
        Just(StateIntent::Topic),
        Just(StateIntent::Name),
        Just(StateIntent::PlPromote),
        Just(StateIntent::PlDemote),
    ]
}

fn adv_op() -> impl Strategy<Value = AdvOp> {
    prop_oneof![
        4 => (state_intent(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(intent, head, a, b)| AdvOp::Extend { intent, head, a, b }),
        3 => (state_intent(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(intent, head, a, b)| AdvOp::Fork { intent, head, a, b }),
        3 => (state_intent(), any::<u8>(), any::<u8>())
            .prop_map(|(intent, a, b)| AdvOp::Merge { intent, a, b }),
        2 => any::<u8>().prop_map(|a| AdvOp::ConflictFork { a }),
        2 => Just(AdvOp::Cascade),
        2 => (any::<u8>(), any::<u8>()).prop_map(|(a, b)| AdvOp::Unauthorised { a, b }),
        2 => any::<u8>().prop_map(|a| AdvOp::Message { a }),
        2 => any::<u8>().prop_map(|a| AdvOp::StaleMessage { a }),
    ]
}

/// Construction state: a real `RoomCore` driven in build order, plus the
/// accumulating event list and the most-recent reject (for the cascade
/// primitive). Owns everything so `build_adv_dag` can move the parts straight
/// into the `Dag`.
struct AdvCtx {
    room_id: OwnedRoomId,
    room: RoomCore,
    provider: InMemoryStateProvider,
    events: Vec<Event>,
    ts: u64,
    last_rejected: Option<OwnedEventId>,
}

impl AdvCtx {
    fn new(room_id: OwnedRoomId) -> Self {
        AdvCtx {
            room: RoomCore::new(room_id.clone()),
            room_id,
            provider: InMemoryStateProvider::new(),
            events: Vec::new(),
            ts: FORK_MERGE_TS_BASE,
            last_rejected: None,
        }
    }

    fn next_ts(&mut self) -> u64 {
        self.ts += 1;
        self.ts
    }

    fn state_heads(&self) -> Vec<OwnedEventId> {
        self.room
            .state_forward_extremities()
            .iter()
            .cloned()
            .collect()
    }

    fn timeline_heads(&self) -> Vec<OwnedEventId> {
        self.room.forward_extremities().iter().cloned().collect()
    }

    fn shadow(&self) -> Shadow {
        Shadow::from_state(self.room.current_state())
    }

    /// Apply one event through the real `RoomCore`, store the persisted form,
    /// and track the most-recent reject. `apply_pdu` only errors on a
    /// programming fault (room mismatch / missing ancestry) — impossible here,
    /// since every reference is a head or a recorded reject already in the
    /// provider — so an error is a generator bug and panics loudly.
    fn apply(&mut self, event: Event) {
        let id = event.event_id.clone();
        let effects = self
            .room
            .apply_pdu(event, &self.provider)
            .expect("adversarial build: apply_pdu must not error (ancestry always present)");
        let persisted = persist_effect(&effects)
            .expect("adversarial build: every applied event yields a Persist");
        if classify(&persisted) == Verdict::Rejected {
            self.last_rejected = Some(id);
        }
        self.events.push((*persisted).clone());
        self.provider.insert(persisted);
    }

    fn emit_state(
        &mut self,
        sender: &str,
        event_type: &str,
        state_key: &str,
        content: Value,
        prev: Vec<OwnedEventId>,
    ) {
        let ts = self.next_ts();
        let event = adv_state_event(
            &self.room_id,
            sender,
            event_type,
            state_key,
            content,
            ts,
            prev,
        );
        self.apply(event);
    }

    fn emit_message(
        &mut self,
        sender: &str,
        timeline_prev: Vec<OwnedEventId>,
        state_prev: Vec<OwnedEventId>,
    ) {
        let ts = self.next_ts();
        let event = adv_message_event(&self.room_id, sender, ts, timeline_prev, state_prev);
        self.apply(event);
    }

    fn run_op(&mut self, op: AdvOp) {
        let state_heads = self.state_heads();
        if state_heads.is_empty() {
            return; // unreachable: create + alice's join are always present
        }
        let shadow = self.shadow();
        match op {
            AdvOp::Extend { intent, head, a, b } => {
                let prev = vec![state_heads[head as usize % state_heads.len()].clone()];
                let (s, et, sk, c) = realise(&intent, &shadow, a as usize, b as usize);
                self.emit_state(s, et, &sk, c, prev);
            }
            AdvOp::Fork { intent, head, a, b } => {
                let tip = state_heads[head as usize % state_heads.len()].clone();
                if state_heads.len() >= ADV_MAX_FORK_WIDTH {
                    let (s, et, sk, c) = realise(&intent, &shadow, a as usize, b as usize);
                    self.emit_state(s, et, &sk, c, vec![tip]);
                } else {
                    let (s1, et1, sk1, c1) = realise(&intent, &shadow, a as usize, b as usize);
                    self.emit_state(s1, et1, &sk1, c1, vec![tip.clone()]);
                    let (s2, et2, sk2, c2) =
                        realise(&intent, &shadow, a as usize + 1, b as usize + 1);
                    self.emit_state(s2, et2, &sk2, c2, vec![tip]);
                }
            }
            AdvOp::Merge { intent, a, b } => {
                let width = state_heads.len().min(ADV_MAX_FORK_WIDTH);
                let prev = state_heads[..width].to_vec();
                let (s, et, sk, c) = realise(&intent, &shadow, a as usize, b as usize);
                self.emit_state(s, et, &sk, c, prev);
            }
            AdvOp::ConflictFork { a } => {
                let target = ADV_POOL[1 + a as usize % 3];
                let creator = ADV_POOL[ADV_CREATOR];
                if state_heads.len() >= ADV_MAX_FORK_WIDTH {
                    let c = shadow.pl_content(&[(target, 50)]);
                    self.emit_state(
                        creator,
                        "m.room.power_levels",
                        "",
                        c,
                        vec![state_heads[0].clone()],
                    );
                } else {
                    let tip = state_heads[a as usize % state_heads.len()].clone();
                    let c_lo = shadow.pl_content(&[(target, 30)]);
                    self.emit_state(creator, "m.room.power_levels", "", c_lo, vec![tip.clone()]);
                    let c_hi = shadow.pl_content(&[(target, 70)]);
                    self.emit_state(creator, "m.room.power_levels", "", c_hi, vec![tip]);
                }
            }
            AdvOp::Cascade => match self.last_rejected.clone() {
                Some(rejected) => {
                    let mut prev = state_heads;
                    if !prev.contains(&rejected) {
                        prev.push(rejected);
                    }
                    self.emit_state(
                        ADV_POOL[ADV_CREATOR],
                        "m.room.topic",
                        "",
                        json!({ "topic": "cascade" }),
                        prev,
                    );
                }
                None => {
                    // No reject to chain off yet — manufacture one so a later
                    // Cascade has a parent (a power-0 non-creator sets a name).
                    self.emit_state(
                        ADV_POOL[1],
                        "m.room.name",
                        "",
                        json!({ "name": "unauth" }),
                        vec![state_heads[0].clone()],
                    );
                }
            },
            AdvOp::Unauthorised { a, b } => {
                let sender = ADV_POOL[1 + a as usize % 3];
                let prev = vec![state_heads[b as usize % state_heads.len()].clone()];
                self.emit_state(sender, "m.room.name", "", json!({ "name": "unauth" }), prev);
            }
            AdvOp::Message { a } => {
                let sender = ADV_POOL[a as usize % 4];
                self.emit_message(sender, self.timeline_heads(), state_heads);
            }
            AdvOp::StaleMessage { a } => {
                let victim = ADV_POOL[1 + a as usize % 3];
                let creator = ADV_POOL[ADV_CREATOR];
                if !shadow.joined(victim) {
                    self.emit_state(
                        creator,
                        "m.room.member",
                        victim,
                        json!({ "membership": "invite" }),
                        self.state_heads(),
                    );
                    self.emit_state(
                        victim,
                        "m.room.member",
                        victim,
                        json!({ "membership": "join" }),
                        self.state_heads(),
                    );
                }
                // Pre-kick heads: the victim is joined in the state they resolve
                // to. Capture before the kick advances the heads.
                let pre_kick = self.state_heads();
                self.emit_state(
                    creator,
                    "m.room.member",
                    victim,
                    json!({ "membership": "leave" }),
                    pre_kick.clone(),
                );
                // The message is authorised against its (pre-kick) state-before
                // but soft-failed against the post-kick current_state.
                self.emit_message(victim, self.timeline_heads(), pre_kick);
            }
        }
    }
}

/// Build an adversarial DAG from a recipe by driving a real `RoomCore` in build
/// order. Always seeds the create + alice's self-join (both accepted), then
/// runs the ops until the event cap. Returns a pure, replayable `Dag` whose
/// stored events carry their persisted verdict flags.
fn build_adv_dag(ops: Vec<AdvOp>) -> Dag {
    let create = build_create();
    let create_id = create.event_id.clone();
    let room_id = room_id_from_create(&create.event_id);
    let mut ctx = AdvCtx::new(room_id);
    ctx.apply(create);
    let join_ts = ctx.next_ts();
    let join = build_join(&ctx.room_id, &create_id, join_ts);
    ctx.apply(join);
    for op in ops {
        if ctx.events.len() >= ADV_MAX_EVENTS {
            break;
        }
        ctx.run_op(op);
    }
    let AdvCtx {
        room_id, events, ..
    } = ctx;
    Dag { room_id, events }
}

/// The order-sensitive result of applying an adversarial DAG: resolved state,
/// both head-sets, and each event's *replay* verdict (indexed parallel to
/// `dag.events`). The verdicts here are an independent re-measurement from a
/// fresh `apply_pdu` pass — NOT derived from the stored `dag` events — which is
/// what lets `adv_build_order_reproduces_verdicts_and_heads` check that replay
/// reproduces the build-time verdicts rather than restating them.
struct AdvOutcome {
    current_state: StateMap<OwnedEventId>,
    state_fes: BTreeSet<OwnedEventId>,
    timeline_fes: BTreeSet<OwnedEventId>,
    verdicts: Vec<Verdict>,
}

/// Apply `dag.events` in `order` to a fresh `RoomCore`, returning the resolved
/// state, both head-sets, and each event's replay verdict (indexed parallel to
/// `dag.events`). Unlike `apply_dag` this asserts nothing about acceptance —
/// the adversarial DAG contains rejects and soft-fails by design.
fn apply_adv_dag(dag: &Dag, order: &[usize]) -> Result<AdvOutcome, TestCaseError> {
    let mut room = RoomCore::new(dag.room_id.clone());
    let mut provider = InMemoryStateProvider::new();
    let mut verdict_of: HashMap<&str, Verdict> = HashMap::new();
    for &i in order {
        let event = dag.events[i].clone();
        let id = event.event_id.clone();
        let effects = room
            .apply_pdu(event, &provider)
            .map_err(|e| TestCaseError::fail(format!("apply_pdu errored on {id}: {e}")))?;
        let persisted = persist_effect(&effects)
            .ok_or_else(|| TestCaseError::fail(format!("apply_pdu emitted no Persist for {id}")))?;
        verdict_of.insert(dag.events[i].event_id.as_str(), classify(&persisted));
        provider.insert(persisted);
    }
    let verdicts = dag
        .events
        .iter()
        .map(|e| verdict_of[e.event_id.as_str()])
        .collect();
    let current_state: StateMap<OwnedEventId> = room
        .current_state()
        .iter()
        .map(|(k, v)| (k.clone(), v.event_id.clone()))
        .collect();
    Ok(AdvOutcome {
        current_state,
        state_fes: room.state_forward_extremities().clone(),
        timeline_fes: room.forward_extremities().clone(),
        verdicts,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Phase 1 generator-soundness property: the verdicts recorded during
    /// construction (a RoomCore in build order) must be exactly reproduced by
    /// replaying the *persisted* events on a fresh RoomCore in the same order,
    /// and the resulting head-sets must match the verdict-aware structural
    /// oracle. This pins the generator + the verdict-aware `expected_heads`
    /// end-to-end without yet asserting order-independence (Phase 2).
    #[test]
    fn adv_build_order_reproduces_verdicts_and_heads(
        ops in prop::collection::vec(adv_op(), 1..16),
    ) {
        let dag = build_adv_dag(ops);
        let build_order: Vec<usize> = (0..dag.events.len()).collect();
        let replay = apply_adv_dag(&dag, &build_order)?;
        // Build-time verdicts come from the stored persisted flags; the replay
        // verdicts are an independent re-measurement — so this is a genuine
        // reproduction check, not a restatement.
        let build_verdicts: Vec<Verdict> = (0..dag.events.len()).map(|i| dag.verdict(i)).collect();
        prop_assert_eq!(
            &replay.verdicts,
            &build_verdicts,
            "replaying the DAG in build order produced different verdicts than construction"
        );
        let (expected_timeline, expected_state) = expected_heads(&dag);
        prop_assert_eq!(
            &replay.timeline_fes,
            &expected_timeline,
            "timeline forward extremities diverged from the verdict-aware oracle"
        );
        prop_assert_eq!(
            &replay.state_fes,
            &expected_state,
            "state forward extremities diverged from the verdict-aware oracle"
        );
        // (Rejection isolation — a rejected event never reaching current_state —
        // is asserted by `adv_rejection_cascade_and_isolation` below.)
    }
}

/// Coverage corpus for the adversarial generator — the multi-user analogue of
/// `fork_merge_generator_coverage_corpus`. Asserts the four PLAN-listed shapes
/// the creator-only profile could not reach now appear in a floor fraction of a
/// deterministic sample, so the (future) rejection / soft-fail / conflict-merge
/// properties cannot pass vacuously.
#[test]
fn adv_generator_coverage_corpus() {
    const SAMPLE: usize = 2000;
    let strat = prop::collection::vec(adv_op(), 1..16);
    let mut runner = TestRunner::deterministic();

    let (mut multihead, mut pl_conflict_merge, mut rejected_with_child, mut soft_failed_msg) =
        (0usize, 0usize, 0usize, 0usize);
    for _ in 0..SAMPLE {
        let ops = strat
            .new_tree(&mut runner)
            .expect("strategy produces a value")
            .current();
        let dag = build_adv_dag(ops);
        let idx_of: HashMap<&str, usize> = dag
            .events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.event_id.as_str(), i))
            .collect();

        let mut saw_multihead = false;
        let mut saw_pl_conflict = false;
        for e in &dag.events {
            if e.state_key.is_none() {
                continue;
            }
            let distinct: HashSet<&str> = e.prev_state_events.iter().map(|p| p.as_str()).collect();
            if distinct.len() >= 3 {
                saw_multihead = true;
            }
            // A merge resolving a power-level conflict: ≥2 of its parents are
            // accepted m.room.power_levels events (the ConflictFork siblings).
            if distinct.len() >= 2 {
                let pl_parents = distinct
                    .iter()
                    .filter_map(|p| idx_of.get(*p))
                    .filter(|&&i| {
                        dag.events[i].event_type == "m.room.power_levels"
                            && dag.verdict(i) == Verdict::Accepted
                    })
                    .count();
                if pl_parents >= 2 {
                    saw_pl_conflict = true;
                }
            }
        }

        let rejected_ids: HashSet<&str> = dag
            .events
            .iter()
            .filter(|e| classify(e) == Verdict::Rejected)
            .map(|e| e.event_id.as_str())
            .collect();
        let saw_rejected_child = dag.events.iter().any(|e| {
            e.prev_state_events
                .iter()
                .any(|p| rejected_ids.contains(p.as_str()))
        });
        // Require the soft-failed event to be a message (non-state) — production
        // only soft-fails non-state events, so this also documents the bucket.
        let saw_soft_fail = dag
            .events
            .iter()
            .any(|e| e.state_key.is_none() && classify(e) == Verdict::SoftFailed);

        multihead += usize::from(saw_multihead);
        pl_conflict_merge += usize::from(saw_pl_conflict);
        rejected_with_child += usize::from(saw_rejected_child);
        soft_failed_msg += usize::from(saw_soft_fail);
    }

    // Floors (deterministic sample → reproducible). Measured rates at authoring
    // time were higher — multihead 31%, pl_conflict 29%, rejected_with_child
    // 36%, soft_failed_msg 42% — and the floors sit at roughly half of those, so
    // they catch disappearance and gross erosion (not fine drift).
    let floor = |pct: usize| SAMPLE * pct / 100;
    assert!(
        multihead >= floor(15),
        "multi-head (≥3-head) merges only in {multihead}/{SAMPLE} DAGs — coverage eroded"
    );
    assert!(
        pl_conflict_merge >= floor(12),
        "power-level conflict merges only in {pl_conflict_merge}/{SAMPLE} DAGs — coverage eroded"
    );
    assert!(
        rejected_with_child >= floor(18),
        "rejected-with-dependent-child only in {rejected_with_child}/{SAMPLE} DAGs — coverage eroded"
    );
    assert!(
        soft_failed_msg >= floor(20),
        "soft-failed messages only in {soft_failed_msg}/{SAMPLE} DAGs — coverage eroded"
    );
}

/// Load-bearing invariant: the adversarial generator seeds the create + join
/// via `build_create`/`build_join` (which use `FORK_MERGE_SENDER`) while
/// `Shadow::power` treats `ADV_POOL[ADV_CREATOR]` as the omnipotent creator. If
/// those two identities diverged, the seeded creator would not be the shadow's
/// omnipotent user and the generator would mis-realise every intent.
#[test]
fn adv_creator_is_fork_merge_sender() {
    assert_eq!(ADV_POOL[ADV_CREATOR], FORK_MERGE_SENDER);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Phase 2 — order-independence of the order-invariant quantities, plus
    /// rejection determinism. `current_state`, the state forward extremities,
    /// and each event's *reject-or-not* verdict are pure functions of the DAG:
    /// state events never soft-fail, and a reject depends only on
    /// `prev_state_events` ancestry — so all three must be identical across
    /// every topological application order. Applied in build order (baseline)
    /// plus 2–4 random topo orders.
    ///
    /// The timeline forward extremities and the `SoftFailed`-vs-`Accepted`
    /// distinction are deliberately NOT asserted order-invariant: soft-fail is
    /// checked against the room's live resolved `current_state` at apply time
    /// (room_core.rs:433), which legitimately depends on which concurrent events
    /// have been applied. That is real Matrix soft-fail semantics, not a bug.
    #[test]
    fn adv_order_independent_invariants(
        ops in prop::collection::vec(adv_op(), 1..16),
        entropies in prop::collection::vec(
            prop::collection::vec(any::<u8>().prop_map(|b| b as usize), 1..40),
            2..5,
        ),
    ) {
        let dag = build_adv_dag(ops);
        let build_order: Vec<usize> = (0..dag.events.len()).collect();
        let baseline = apply_adv_dag(&dag, &build_order)?;
        let baseline_rejects = reject_projection(&baseline.verdicts);
        for entropy in &entropies {
            let order = random_topo_order(&dag, entropy);
            prop_assert_eq!(order.len(), dag.events.len(), "topo order dropped events");
            let outcome = apply_adv_dag(&dag, &order)?;
            prop_assert_eq!(
                &outcome.current_state,
                &baseline.current_state,
                "current_state depended on application order"
            );
            prop_assert_eq!(
                &outcome.state_fes,
                &baseline.state_fes,
                "state forward extremities depended on application order"
            );
            prop_assert_eq!(
                &reject_projection(&outcome.verdicts),
                &baseline_rejects,
                "reject verdicts depended on application order"
            );
        }
    }

    /// Phase 2 — rejection cascade + isolation. Cascade: any event whose
    /// `prev_state_events` names a rejected event is itself rejected
    /// (`validate::validate_references` → `PrevStateRejected`); this is a
    /// structural property of the DAG, so it is checked directly on the stored
    /// verdicts. Isolation: a rejected event never appears in the resolved
    /// `current_state` (`apply_pdu` returns before any state commit on the
    /// reject path); checked against an applied outcome.
    #[test]
    fn adv_rejection_cascade_and_isolation(
        ops in prop::collection::vec(adv_op(), 1..16),
    ) {
        let dag = build_adv_dag(ops);
        let verdict_by_id: HashMap<&str, Verdict> = dag
            .events
            .iter()
            .map(|e| (e.event_id.as_str(), classify(e)))
            .collect();

        // Cascade.
        for (i, e) in dag.events.iter().enumerate() {
            let names_rejected = e
                .prev_state_events
                .iter()
                .any(|p| verdict_by_id.get(p.as_str()) == Some(&Verdict::Rejected));
            if names_rejected {
                prop_assert_eq!(
                    dag.verdict(i),
                    Verdict::Rejected,
                    "event {} names a rejected prev_state_event but was not itself rejected",
                    e.event_id
                );
            }
        }

        // Isolation.
        let build_order: Vec<usize> = (0..dag.events.len()).collect();
        let outcome = apply_adv_dag(&dag, &build_order)?;
        let rejected: HashSet<&str> = dag
            .events
            .iter()
            .filter(|e| classify(e) == Verdict::Rejected)
            .map(|e| e.event_id.as_str())
            .collect();
        for id in outcome.current_state.values() {
            prop_assert!(
                !rejected.contains(id.as_str()),
                "rejected event {id} leaked into current_state"
            );
        }
    }
}
