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

use std::collections::HashSet;
use std::sync::Arc;

use neutrino_common::ROOM_VERSION_ID;
use neutrino_state::auth_events::{auth_event_keys, calculate_auth_events};
use neutrino_state::auth_rules::check_auth_rules;
use neutrino_state::provider::{EventInfo, InMemoryStateProvider, StateProvider};
use neutrino_state::state_res::{auth_chain_difference, conflicted_subgraph, separate};
use neutrino_state::validate::parse_event;
use neutrino_state::{Event, FormatError, StateMap};
use proptest::prelude::*;
use ruma::{OwnedEventId, OwnedUserId};
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
    "depth",
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

fn ids_as_json(ids: &[OwnedEventId]) -> Value {
    json!(ids.iter().map(|e| e.as_str()).collect::<Vec<_>>())
}

/// Strategy for an `m.room.message` event with varied sender and ancestry.
fn arb_message_event() -> impl Strategy<Value = Event> {
    (
        arb_user_id(),
        arb_event_id(),
        arb_prev_events(),
        arb_prev_state_events(),
    )
        .prop_filter_map("valid message", |(sender, id, prevs, prev_states)| {
            let mut v = base_message();
            v["sender"] = json!(sender);
            v["prev_events"] = ids_as_json(&prevs);
            v["prev_state_events"] = ids_as_json(&prev_states);
            parse_event(raw(v), id, vec![]).ok()
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
                arb_event_id(),
                arb_prev_events(),
                arb_prev_state_events(),
            )
        })
        .prop_filter_map(
            "valid member",
            |(sender, target, membership, id, prevs, prev_states)| {
                let mut v = base_message();
                v["type"] = json!("m.room.member");
                v["sender"] = json!(sender);
                v["state_key"] = json!(target);
                v["content"] = json!({ "membership": membership });
                v["prev_events"] = ids_as_json(&prevs);
                v["prev_state_events"] = ids_as_json(&prev_states);
                parse_event(raw(v), id, vec![]).ok()
            },
        )
}

/// Strategy for an `m.room.create` event. Create events carry no `room_id`
/// (derived from `event_id`) and no ancestry (rule 1.1 / MSC4242).
fn arb_create_event() -> impl Strategy<Value = Event> {
    (arb_user_id(), arb_event_id()).prop_filter_map("valid create", |(sender, id)| {
        let mut v = base_message();
        v["type"] = json!("m.room.create");
        v["sender"] = json!(sender);
        v["content"] = json!({ "room_version": ROOM_VERSION_ID });
        v["state_key"] = json!("");
        let obj = v.as_object_mut()?;
        obj.remove("room_id");
        obj.remove("prev_state_events");
        // prev_events stays [] from base_message — rule 1.1.
        parse_event(raw(v), id, vec![]).ok()
    })
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

/// Combined strategy: a create event_id, the create event built around it,
/// and 0..10 other state events with arbitrary keys. The create_id and
/// create event are coupled inside one `prop_filter_map` so the proptest
/// shrinker can't desynchronise them — without this coupling, shrinking
/// `create_id` independently would leave the `Event` carrying its original
/// id, and the 5.3.1 property below would never see matching ancestry.
///
/// Returning the `create_id` lets call-site strategies thread it into
/// `prev_events` / `prev_state_events` of the event-under-test, so rule
/// 5.3.1 (self-join immediately after create) is actually reachable.
fn arb_state_with_create()
-> impl Strategy<Value = (OwnedEventId, OwnedUserId, StateMap<Arc<Event>>)> {
    (
        arb_event_id(),
        arb_user_id(),
        prop::collection::vec(arb_event().prop_map(Arc::new), 0..10),
    )
        .prop_filter_map("valid create + state", |(create_id, sender_str, events)| {
            let v = json!({
                "type": "m.room.create",
                "sender": sender_str,
                "content": { "room_version": ROOM_VERSION_ID },
                "prev_events": [],
                "depth": 0,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" },
                "state_key": ""
            });
            let create_event = parse_event(raw(v), create_id.clone(), vec![]).ok()?;
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
        let _ = check_auth_rules(&event, &state);
    }

    /// Rule 5.3.1: a self-join event whose only ancestry (in both DAGs) is
    /// the create event is always allowed, regardless of additional state.
    /// Everything is built inside the test body from primitive inputs so the
    /// shrinker can't desynchronise the create_id from the create event's
    /// stored `event_id`.
    #[test]
    fn rule_5_3_1_self_join_after_create_always_allowed(
        create_id in arb_event_id(),
        sender_str in arb_user_id(),
        extras in prop::collection::vec(arb_event().prop_map(Arc::new), 0..10),
        join_id in arb_event_id(),
    ) {
        prop_assume!(create_id != join_id);

        let create_v = json!({
            "type": "m.room.create",
            "sender": sender_str,
            "content": { "room_version": ROOM_VERSION_ID },
            "prev_events": [],
            "depth": 0,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc" },
            "state_key": ""
        });
        let create_event = parse_event(raw(create_v), create_id.clone(), vec![])
            .expect("create event valid wire format");
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

        // `Event::room_id` is derived inline by `parse_event` from the
        // create event's id (`$X...` → `!X...`), so the join must carry the
        // matching `room_id` to pass phase 1a.
        let derived_room_id = format!("!{}", &create_id.as_str()[1..]);
        let join_v = json!({
            "type": "m.room.member",
            "sender": creator_uid.as_str(),
            "state_key": creator_uid.as_str(),
            "room_id": derived_room_id,
            "content": { "membership": "join" },
            "prev_events": [create_id.as_str()],
            "prev_state_events": [create_id.as_str()],
            "depth": 1,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc" }
        });
        let join = parse_event(raw(join_v), join_id, vec![])
            .expect("self-join event valid wire format");
        prop_assert!(check_auth_rules(&join, &state).is_ok());
    }
}

// ============== Phase 4a: state_res ==============
//
// Properties on `separate`, `conflicted_subgraph`, `auth_chain_difference`.
// Strategies generate unrestricted state maps and providers (auth chains may
// reference unknown events, may contain cycles); the algorithms must hold
// under that.

/// Placeholder `Arc<Event>` for an arbitrary event id with caller-supplied
/// `auth_events`. The state-res functions don't look at the event body —
/// only `event_id` and `auth_events` — so a minimal valid event with the
/// auth list attached suffices.
fn placeholder_arc_event(id: &OwnedEventId, auth_events: Vec<OwnedEventId>) -> Arc<Event> {
    Arc::new(
        parse_event(
            raw(json!({
                "type": "m.room.placeholder",
                "sender": "@alice:example.org",
                "room_id": "!room:example.org",
                "content": {},
                "prev_events": [],
                "prev_state_events": [],
                "depth": 1,
                "origin_server_ts": 1_700_000_000_000_u64,
                "hashes": { "sha256": "abc" },
                "state_key": ""
            })),
            id.clone(),
            auth_events,
        )
        .expect("placeholder event"),
    )
}

/// Arbitrary `InMemoryStateProvider` that is **closed** under `auth_events`
/// — every id any event references via its `auth_events` is itself in the
/// provider. Cycles are allowed (the DFS in `auth_chain` dedupes via a
/// visited set, so cyclic input still terminates).
///
/// Closure is the project invariant for state-res input: a referenced-but-
/// unknown event is an error (`StateResError::MissingAuthEvent`), not a
/// silent backfill boundary. The strategy reflects that.
///
/// Yields the provider **and the list of known ids** so callers can draw
/// seeds / state values from the keyset — input seeds outside this keyset
/// would (correctly) blow up `auth_chain` with `MissingAuthEvent` and aren't
/// what most properties want to test.
fn arb_provider_with_ids() -> impl Strategy<Value = (InMemoryStateProvider, Vec<OwnedEventId>)> {
    prop::collection::hash_set(arb_event_id(), 1..=15).prop_flat_map(|ids| {
        let len = ids.len();
        let ids_vec: Vec<OwnedEventId> = ids.into_iter().collect();
        (
            Just(ids_vec.clone()),
            prop::collection::vec(prop::collection::vec(0usize..len, 0..4), len),
        )
            .prop_map(|(ids, parents_per_event)| {
                let mut provider = InMemoryStateProvider::new();
                for (id, parent_indices) in ids.iter().zip(&parents_per_event) {
                    let parents: Vec<OwnedEventId> =
                        parent_indices.iter().map(|i| ids[*i].clone()).collect();
                    let event = placeholder_arc_event(id, parents);
                    provider.insert(EventInfo {
                        event,
                        rejected: false,
                    });
                }
                (provider, ids)
            })
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
                .map(|info| info.event.auth_events.clone())
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
                .map(|info| info.event.auth_events.clone())
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
/// parents). Distinct ids enforced via `prop_filter`.
fn arb_linear_chain(
    min: usize,
    max: usize,
) -> impl Strategy<Value = (InMemoryStateProvider, Vec<OwnedEventId>)> {
    prop::collection::vec(arb_event_id(), min..=max)
        .prop_filter("distinct ids", |ids| {
            ids.iter().collect::<HashSet<_>>().len() == ids.len()
        })
        .prop_map(|ids| {
            let mut provider = InMemoryStateProvider::new();
            for (i, id) in ids.iter().enumerate() {
                let parents = if i + 1 < ids.len() {
                    vec![ids[i + 1].clone()]
                } else {
                    vec![]
                };
                provider.insert(EventInfo {
                    event: placeholder_arc_event(id, parents),
                    rejected: false,
                });
            }
            (provider, ids)
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
/// parent `bottom`. Distinct ids enforced.
fn arb_diamond() -> impl Strategy<Value = (InMemoryStateProvider, [OwnedEventId; 4])> {
    (
        arb_event_id(),
        arb_event_id(),
        arb_event_id(),
        arb_event_id(),
    )
        .prop_filter("distinct corners", |(r, l, ri, b)| {
            let mut set: HashSet<&OwnedEventId> = HashSet::new();
            set.insert(r);
            set.insert(l);
            set.insert(ri);
            set.insert(b);
            set.len() == 4
        })
        .prop_map(|(root, left, right, bottom)| {
            let mut provider = InMemoryStateProvider::new();
            provider.insert(EventInfo {
                event: placeholder_arc_event(&root, vec![left.clone(), right.clone()]),
                rejected: false,
            });
            provider.insert(EventInfo {
                event: placeholder_arc_event(&left, vec![bottom.clone()]),
                rejected: false,
            });
            provider.insert(EventInfo {
                event: placeholder_arc_event(&right, vec![bottom.clone()]),
                rejected: false,
            });
            provider.insert(EventInfo {
                event: placeholder_arc_event(&bottom, vec![]),
                rejected: false,
            });
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
        // Pull each event's auth_events off its placeholder Event in the
        // source provider, then rebuild a new event in the merged provider
        // with the same auth_events embedded.
        let copy_event_auth = |id: &OwnedEventId, src: &InMemoryStateProvider| -> Vec<OwnedEventId> {
            src.get_event(id)
                .map(|info| info.event.auth_events.clone())
                .unwrap_or_default()
        };
        for id in &ids_a {
            let parents = copy_event_auth(id, &provider_a);
            merged.insert(EventInfo {
                event: placeholder_arc_event(id, parents),
                rejected: false,
            });
        }
        for id in &ids_b {
            let parents = copy_event_auth(id, &provider_b);
            merged.insert(EventInfo {
                event: placeholder_arc_event(id, parents),
                rejected: false,
            });
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
