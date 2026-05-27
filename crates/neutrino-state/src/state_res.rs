//! Phase 4: state resolution v2.1 (v12).
//!
//! This phase composes a `StateMap<OwnedEventId>` from a list of input state
//! maps (state-after each of an event's `prev_state_events`, or state-after
//! each forward extremity when computing current state). Under MSC4242 the
//! input shape is smaller and well-defined; the algorithm itself is unchanged
//! from synapse `state/v2.py`, modulo two v12 deltas:
//!
//! - The full conflicted set includes a **conflicted subgraph** — events
//!   reachable backwards via `auth_events` from any conflicted state event.
//!   v2 didn't have this.
//! - Iterative auth checks pass 1 starts from the **empty state**, not from
//!   the unconflicted state. v2 started from unconflicted.
//!
//! Phase 4b adds the reverse-topological power sort and the iterative auth
//! checks loop. Phase 4c will add mainline ordering, IAC pass 2, and the
//! `resolve_state` top-level entry point.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use ruma::OwnedEventId;

use crate::auth_events::auth_event_keys;
use crate::auth_rules::{AuthContext, check_auth_rules};
use crate::provider::StateProvider;
use crate::{Event, StateMap, StateResError};

// ----------------- separate -----------------

/// Output of `separate`: state map keys partitioned into those everybody
/// agrees on and those that differ.
#[derive(Debug, Default)]
pub struct Separated {
    /// Keys present with the **same value** in every input state set.
    pub unconflicted: StateMap<OwnedEventId>,
    /// Keys with differing values, or absent from at least one input. The
    /// value set is every distinct `OwnedEventId` that appears at that key
    /// across the input state sets.
    pub conflicted: HashMap<(String, String), HashSet<OwnedEventId>>,
}

/// Split the input state maps into unconflicted and conflicted portions.
///
/// A key is **unconflicted** iff every input state set carries that key and
/// the value is identical across all of them. A key is **conflicted** if it
/// is missing from at least one input set OR the values differ. Conflicted
/// values are collected into a `HashSet<OwnedEventId>` for downstream
/// processing.
///
/// Pure function, allocation only. Matches synapse `state/v2.py::_separate`.
pub fn separate(state_sets: &[&StateMap<OwnedEventId>]) -> Separated {
    let mut out = Separated::default();
    if state_sets.is_empty() {
        return out;
    }

    let mut all_keys: HashSet<(String, String)> = HashSet::new();
    for set in state_sets {
        for key in set.keys() {
            all_keys.insert(key.clone());
        }
    }

    for key in all_keys {
        let mut values: HashSet<OwnedEventId> = HashSet::new();
        let mut present_in_all = true;
        for set in state_sets {
            match set.get(&key) {
                Some(v) => {
                    values.insert(v.clone());
                }
                None => present_in_all = false,
            }
        }
        if present_in_all && values.len() == 1 {
            let v = values.into_iter().next().expect("len == 1");
            out.unconflicted.insert(key, v);
        } else {
            out.conflicted.insert(key, values);
        }
    }
    out
}

// ----------------- conflicted_subgraph -----------------

/// v12 (state-res v2.1) addition: events reachable backwards from any seed
/// event via the `auth_events` graph. The subgraph **includes the seeds
/// themselves** (per our pick on the spec's "include or exclude endpoints"
/// ambiguity — simpler, no special-casing).
///
/// `seeds` is typically the union of all event IDs across the conflicted
/// values produced by `separate`. The traversal is delegated to
/// `provider.auth_chain` — in-memory does DFS, SQLite will do a recursive
/// CTE.
pub fn conflicted_subgraph(
    seeds: &HashSet<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<HashSet<OwnedEventId>, StateResError> {
    provider.auth_chain(seeds)
}

// ----------------- auth_chain_difference -----------------

/// Events that appear in *some* but not *all* auth chains across the input
/// state sets — i.e. `(∪ chains) \ (∩ chains)` where each chain is the
/// transitive backwards closure of an entire state set via
/// `provider.auth_chain`.
///
/// This is the v2 quantity (synapse `_get_auth_chain_difference`); the v2.1
/// conflicted subgraph is a *separate* set computed by `conflicted_subgraph`.
/// `resolve_state` unions them.
///
/// One state set or zero state sets produces the empty set (no disagreement
/// to surface).
pub fn auth_chain_difference(
    state_sets: &[&StateMap<OwnedEventId>],
    provider: &dyn StateProvider,
) -> Result<HashSet<OwnedEventId>, StateResError> {
    if state_sets.len() < 2 {
        return Ok(HashSet::new());
    }

    let chains: Vec<HashSet<OwnedEventId>> = state_sets
        .iter()
        .map(|set| {
            let seeds: HashSet<OwnedEventId> = set.values().cloned().collect();
            provider.auth_chain(&seeds)
        })
        .collect::<Result<_, _>>()?;

    let union: HashSet<OwnedEventId> = chains.iter().flatten().cloned().collect();
    let mut intersection = chains[0].clone();
    for c in chains.iter().skip(1) {
        intersection = intersection.intersection(c).cloned().collect();
    }
    Ok(union.difference(&intersection).cloned().collect())
}

// ----------------- power_of_sender -----------------

/// Effective power level of `event.sender` *at the time of `event`*, computed
/// by inspecting `event.auth_events` directly (spec v12 state-res §reverse
/// topological power ordering: "looking at their respective `auth_event`s").
///
/// Synapse parity (`state/v2.py::_get_power_level_for_sender`):
/// - Walk `event.auth_events`; pick out the create event and the latest
///   `m.room.power_levels` event referenced directly.
/// - If a PL event is found, use it. If no PL is found but a create event is,
///   the only signal is creator-set membership → `i64::MAX` for creators,
///   `0` for non-creators (no PL ⇒ `users_default` is 0).
/// - If neither create nor PL is found (an invariant violation by the caller —
///   our `calculate_auth_events` always emits a create reference for
///   non-create events), return `0`.
///
/// `m.room.create` events have no `auth_events` and are authored by the
/// creator → MAX.
///
/// Reuses `AuthContext` so creator detection (sender + `additional_creators`)
/// and PL parsing stay defined in exactly one place.
pub fn power_of_sender(event: &Event, provider: &dyn StateProvider) -> Result<i64, StateResError> {
    if event.event_type == "m.room.create" {
        return Ok(i64::MAX);
    }

    let mut state: StateMap<Arc<Event>> = HashMap::new();
    for aid in &event.auth_events {
        let info = provider
            .get_event(aid)
            .ok_or_else(|| StateResError::MissingAuthEvent(aid.clone()))?;
        if info.rejected {
            continue;
        }
        match info.event.event_type.as_str() {
            "m.room.create" => {
                state.insert(("m.room.create".to_owned(), String::new()), info.event);
            }
            "m.room.power_levels" => {
                state.insert(
                    ("m.room.power_levels".to_owned(), String::new()),
                    info.event,
                );
            }
            _ => {}
        }
    }

    if !state.contains_key(&("m.room.create".to_owned(), String::new())) {
        return Ok(0);
    }
    let ctx = AuthContext::new(&state);
    Ok(ctx.user_power(&event.sender))
}

// ----------------- reverse_topological_power_sort -----------------

/// Reverse-topological power sort over the auth subgraph induced by `events`.
///
/// Output order: parents (events others depend on via `auth_events`) come
/// before their dependents. Ties broken by **higher power first**, then by
/// **lower `origin_server_ts`**, then by **lower `event_id`** — matching
/// synapse `state/v2.py::_reverse_topological_power_sort`'s heap key
/// `(-power_level, origin_server_ts, event_id)`.
///
/// Outdegree-restricted Kahn's: `outdegree(e) = |e.auth_events ∩ events|`. The
/// algorithm short-circuits if a cycle traps any subset — surfaced as a
/// shorter-than-input output rather than an error (cycles can't form in a
/// hash-derived auth chain, so this is a defensive observation, not a path
/// the caller is expected to hit).
pub fn reverse_topological_power_sort(
    events: &HashSet<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<Vec<OwnedEventId>, StateResError> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    // Resolve each input event and its power once.
    let mut event_map: HashMap<OwnedEventId, Arc<Event>> = HashMap::with_capacity(events.len());
    let mut event_to_pl: HashMap<OwnedEventId, i64> = HashMap::with_capacity(events.len());
    for eid in events {
        let info = provider
            .get_event(eid)
            .ok_or_else(|| StateResError::MissingAuthEvent(eid.clone()))?;
        let pl = power_of_sender(&info.event, provider)?;
        event_map.insert(eid.clone(), info.event);
        event_to_pl.insert(eid.clone(), pl);
    }

    // outdegree[e] = how many of e.auth_events are in `events`.
    // children_of[p] = events e where p ∈ e.auth_events ∩ events.
    let mut outdegree: HashMap<OwnedEventId, usize> = HashMap::with_capacity(events.len());
    let mut children_of: HashMap<OwnedEventId, Vec<OwnedEventId>> = HashMap::new();
    for eid in events {
        let ev = &event_map[eid];
        let mut deg = 0usize;
        for parent in &ev.auth_events {
            if events.contains(parent) {
                deg += 1;
                children_of
                    .entry(parent.clone())
                    .or_default()
                    .push(eid.clone());
            }
        }
        outdegree.insert(eid.clone(), deg);
    }

    // Max-heap on (power, Reverse(ts), Reverse(event_id)) → pop yields
    // (highest power, lowest ts, lowest event_id). Synapse equivalent is a
    // min-heap on (-power, ts, event_id).
    type Key = (i64, Reverse<u64>, Reverse<OwnedEventId>);
    let mut heap: BinaryHeap<Key> = BinaryHeap::new();
    let key_for = |eid: &OwnedEventId, event_map: &HashMap<OwnedEventId, Arc<Event>>| -> Key {
        let ev = &event_map[eid];
        (
            event_to_pl[eid],
            Reverse(ev.origin_server_ts),
            Reverse(eid.clone()),
        )
    };
    for eid in events {
        if outdegree[eid] == 0 {
            heap.push(key_for(eid, &event_map));
        }
    }

    let mut sorted = Vec::with_capacity(events.len());
    while let Some((_, _, Reverse(eid))) = heap.pop() {
        if let Some(child_list) = children_of.get(&eid) {
            for child in child_list {
                let deg = outdegree
                    .get_mut(child)
                    .expect("child registered during outdegree pass");
                *deg -= 1;
                if *deg == 0 {
                    heap.push(key_for(child, &event_map));
                }
            }
        }
        sorted.push(eid);
    }
    Ok(sorted)
}

// ----------------- iterative_auth_checks -----------------

/// IAC: walk `sorted` in order, accept-or-reject each event against an
/// `auth_events` map built from the event's own `auth_events` overlaid with
/// the current `resolved` state for keys the event needs (`auth_event_keys`).
///
/// Synapse parity (`state/v2.py::_iterative_auth_checks`):
/// 1. Build per-event auth map from `event.auth_events` (skipping rejected).
/// 2. Overlay entries from `resolved` for each key in
///    `auth_events::auth_event_keys(event)` — earlier accepted events in this
///    pass override the as-shipped auth_events. This is the iterative step.
/// 3. Skip events the provider has pre-marked rejected.
/// 4. Run `check_auth_rules`; on `Ok`, write the event into `resolved` if it's
///    a state event. Message events leave `resolved` unchanged.
///
/// `initial_state` is **empty** for v12 IAC pass 1 (v2.1 divergence), passed
/// through unchanged for IAC pass 2 (phase 4c). This function is agnostic to
/// which pass is running.
pub fn iterative_auth_checks(
    sorted: &[OwnedEventId],
    initial_state: StateMap<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<StateMap<OwnedEventId>, StateResError> {
    let mut resolved = initial_state;

    for eid in sorted {
        let info = provider
            .get_event(eid)
            .ok_or_else(|| StateResError::MissingAuthEvent(eid.clone()))?;
        if info.rejected {
            continue;
        }
        let event = info.event;

        let mut auth_map: StateMap<Arc<Event>> = HashMap::new();
        for aid in &event.auth_events {
            let parent = provider
                .get_event(aid)
                .ok_or_else(|| StateResError::MissingAuthEvent(aid.clone()))?;
            if parent.rejected {
                continue;
            }
            let key = (
                parent.event.event_type.clone(),
                parent.event.state_key.clone().unwrap_or_default(),
            );
            auth_map.insert(key, parent.event);
        }

        // Iterative step: overlay resolved-state entries for the keys this
        // event actually consults.
        for key in auth_event_keys(&event) {
            if let Some(rs_id) = resolved.get(&key) {
                let info = provider
                    .get_event(rs_id)
                    .ok_or_else(|| StateResError::MissingAuthEvent(rs_id.clone()))?;
                if !info.rejected {
                    auth_map.insert(key, info.event);
                }
            }
        }

        if check_auth_rules(&event, &auth_map).is_ok()
            && let Some(sk) = &event.state_key
        {
            resolved.insert((event.event_type.clone(), sk.clone()), eid.clone());
        }
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::event_id::EventBuilder;
    use crate::provider::{EventInfo, InMemoryStateProvider};
    use crate::test_utils::next_ts;
    use ruma::{room_id, user_id};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("event id")
    }

    /// Construct a placeholder state event with caller-supplied auth chain
    /// and return the computed event_id paired with the `Arc<Event>`.
    /// State-res only consults `event_id` + `auth_events`, so the rest of
    /// the event shape is fixed.
    fn placeholder(auth_chain: Vec<OwnedEventId>) -> (OwnedEventId, Arc<Event>) {
        let ev = EventBuilder::new(
            user_id!("@alice:example.org").to_owned(),
            "m.room.placeholder".to_owned(),
        )
        .room_id(room_id!("!room:example.org").to_owned())
        .state_key(String::new())
        .content(json!({}))
        .auth_events(auth_chain)
        .origin_server_ts(next_ts())
        .build()
        .expect("placeholder event");
        let id = ev.event_id.clone();
        (id, Arc::new(ev))
    }

    /// Register a labelled placeholder in `bag` and the provider. The label
    /// is just a string the test uses to refer back to the computed id —
    /// auth_chain is supplied via labels referenced earlier in the same
    /// graph.
    fn insert(
        provider: &mut InMemoryStateProvider,
        bag: &mut HashMap<&'static str, OwnedEventId>,
        label: &'static str,
        auth_labels: &[&'static str],
    ) {
        let auth_chain: Vec<OwnedEventId> = auth_labels
            .iter()
            .map(|l| {
                bag.get(l)
                    .expect("auth label must be inserted first")
                    .clone()
            })
            .collect();
        let (id, ev) = placeholder(auth_chain);
        bag.insert(label, id);
        provider.insert(EventInfo {
            event: ev,
            rejected: false,
        });
    }

    /// Build a state map from `(type, state_key, label)` triples — the label
    /// is resolved against the `bag` populated by previous `insert` calls.
    fn state_from_labels(
        bag: &HashMap<&'static str, OwnedEventId>,
        entries: &[(&str, &str, &str)],
    ) -> StateMap<OwnedEventId> {
        let mut m = StateMap::new();
        for (t, sk, label) in entries {
            let id = bag.get(label).expect("label registered").clone();
            m.insert(((*t).to_string(), (*sk).to_string()), id);
        }
        m
    }

    /// Build a state map from `(type, state_key, event_id)` triples — used
    /// by tests that need to introduce ids the provider doesn't know.
    fn state(entries: &[(&str, &str, &str)]) -> StateMap<OwnedEventId> {
        let mut m = StateMap::new();
        for (t, sk, id) in entries {
            m.insert(((*t).to_string(), (*sk).to_string()), eid(id));
        }
        m
    }

    // ----- separate -----

    #[test]
    fn separate_empty_input() {
        let out = separate(&[]);
        assert!(out.unconflicted.is_empty());
        assert!(out.conflicted.is_empty());
    }

    #[test]
    fn separate_identical_sets_all_unconflicted() {
        // `separate` is purely structural — the event_ids needn't reference
        // events the provider knows about. Use synthetic ids for brevity.
        let s1 = state(&[
            ("m.room.power_levels", "", "$pl:example.org"),
            ("m.room.name", "", "$name:example.org"),
        ]);
        let s2 = s1.clone();
        let out = separate(&[&s1, &s2]);
        assert_eq!(out.unconflicted.len(), 2);
        assert!(out.conflicted.is_empty());
    }

    #[test]
    fn separate_differing_value_is_conflicted() {
        let s1 = state(&[("m.room.name", "", "$n1:example.org")]);
        let s2 = state(&[("m.room.name", "", "$n2:example.org")]);
        let out = separate(&[&s1, &s2]);
        assert!(out.unconflicted.is_empty());
        let values = out
            .conflicted
            .get(&("m.room.name".to_string(), String::new()))
            .expect("key present");
        assert_eq!(values.len(), 2);
        assert!(values.contains(&eid("$n1:example.org")));
        assert!(values.contains(&eid("$n2:example.org")));
    }

    #[test]
    fn separate_absent_in_some_set_is_conflicted() {
        // Key present in one set, absent in another → conflicted.
        let s1 = state(&[
            ("m.room.power_levels", "", "$pl:example.org"),
            ("m.room.topic", "", "$topic:example.org"),
        ]);
        let s2 = state(&[("m.room.power_levels", "", "$pl:example.org")]);
        let out = separate(&[&s1, &s2]);
        // power_levels — present in both with same value → unconflicted.
        assert!(
            out.unconflicted
                .contains_key(&("m.room.power_levels".to_string(), String::new()))
        );
        // topic — absent in s2 → conflicted (with only one value present).
        let topic_vals = out
            .conflicted
            .get(&("m.room.topic".to_string(), String::new()))
            .expect("topic in conflicted");
        assert_eq!(topic_vals.len(), 1);
    }

    // ----- conflicted_subgraph -----

    #[test]
    fn conflicted_subgraph_errors_on_unknown_seed() {
        // Strict closure invariant: a seed that isn't in the provider is
        // an error, not a "include seed and stop walking" fallback. We
        // never lose track of an event we know about, and we never
        // reference one we don't.
        let provider = InMemoryStateProvider::new();
        let mut seeds = HashSet::new();
        seeds.insert(eid("$a:example.org"));
        let err = conflicted_subgraph(&seeds, &provider).expect_err("unknown seed");
        assert!(matches!(err, StateResError::MissingAuthEvent(_)));
    }

    #[test]
    fn conflicted_subgraph_walks_auth_event_ids_transitively() {
        // a → b → c (a's auth_events = [b], b's auth_events = [c]).
        // Insert leaves first so each id is known before being referenced.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "c", &[]);
        insert(&mut provider, &mut bag, "b", &["c"]);
        insert(&mut provider, &mut bag, "a", &["b"]);
        let mut seeds = HashSet::new();
        seeds.insert(bag["a"].clone());
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        let expected: HashSet<_> = [bag["a"].clone(), bag["b"].clone(), bag["c"].clone()]
            .into_iter()
            .collect();
        assert_eq!(sg, expected);
    }

    #[test]
    fn conflicted_subgraph_unions_multiple_seed_chains() {
        // a → b; c → d. Seeds {a, c} → {a, b, c, d}.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "b", &[]);
        insert(&mut provider, &mut bag, "a", &["b"]);
        insert(&mut provider, &mut bag, "d", &[]);
        insert(&mut provider, &mut bag, "c", &["d"]);
        let seeds: HashSet<_> = [bag["a"].clone(), bag["c"].clone()].into_iter().collect();
        let sg = conflicted_subgraph(&seeds, &provider).unwrap();
        assert_eq!(sg.len(), 4);
    }

    // ----- auth_chain_difference -----

    #[test]
    fn auth_chain_difference_zero_or_one_state_set_is_empty() {
        let provider = InMemoryStateProvider::new();
        assert!(auth_chain_difference(&[], &provider).unwrap().is_empty());
        // One state set short-circuits before any chain walk happens, so
        // even an event the provider doesn't know is fine here.
        let s1 = state(&[("m.room.name", "", "$n:example.org")]);
        assert!(auth_chain_difference(&[&s1], &provider).unwrap().is_empty());
    }

    #[test]
    fn auth_chain_difference_empty_state_set_returns_other_chain() {
        // [empty, non_empty] → diff = full chain of non_empty.
        // Empty state set's chain is empty; intersection with empty = empty;
        // difference = union of all other chains.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "b", &[]);
        insert(&mut provider, &mut bag, "a", &["b"]);
        let empty = StateMap::<OwnedEventId>::new();
        let s2 = state_from_labels(&bag, &[("m.room.name", "", "a")]);
        let diff = auth_chain_difference(&[&empty, &s2], &provider).unwrap();
        let expected: HashSet<_> = [bag["a"].clone(), bag["b"].clone()].into_iter().collect();
        assert_eq!(diff, expected);
    }

    #[test]
    fn auth_chain_difference_identical_chains_empty() {
        // Two state sets share the same single event whose auth chain is X→Y.
        // Both chains are {X, Y}; difference is empty.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "y", &[]);
        insert(&mut provider, &mut bag, "x", &["y"]);
        let s1 = state_from_labels(&bag, &[("m.room.name", "", "x")]);
        let s2 = state_from_labels(&bag, &[("m.room.name", "", "x")]);
        assert!(
            auth_chain_difference(&[&s1, &s2], &provider)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn auth_chain_difference_isolated_per_set_event_in_difference() {
        // s1 carries event A (auth chain {A}); s2 carries event B (auth chain
        // {B}). Union = {A, B}, intersection = {}. Difference = {A, B}.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "a", &[]);
        insert(&mut provider, &mut bag, "b", &[]);
        let s1 = state_from_labels(&bag, &[("m.room.name", "", "a")]);
        let s2 = state_from_labels(&bag, &[("m.room.name", "", "b")]);
        let diff = auth_chain_difference(&[&s1, &s2], &provider).unwrap();
        let expected: HashSet<_> = [bag["a"].clone(), bag["b"].clone()].into_iter().collect();
        assert_eq!(diff, expected);
    }

    #[test]
    fn auth_chain_difference_shared_ancestor_not_in_difference() {
        // s1: event A with chain {A, common}; s2: event B with chain {B, common}.
        // Union = {A, B, common}; intersection = {common}; difference = {A, B}.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "common", &[]);
        insert(&mut provider, &mut bag, "a", &["common"]);
        insert(&mut provider, &mut bag, "b", &["common"]);
        let s1 = state_from_labels(&bag, &[("m.room.name", "", "a")]);
        let s2 = state_from_labels(&bag, &[("m.room.name", "", "b")]);
        let diff = auth_chain_difference(&[&s1, &s2], &provider).unwrap();
        assert!(diff.contains(&bag["a"]));
        assert!(diff.contains(&bag["b"]));
        assert!(!diff.contains(&bag["common"]));
    }

    #[test]
    fn auth_chain_difference_three_way_with_partial_overlap() {
        // Three state sets:
        //   s1 -> A with chain {A, X}
        //   s2 -> B with chain {B, X}
        //   s3 -> A with chain {A, X}     (same as s1)
        // Intersection across all three = {X} (only X is in all chains; A is
        // missing from s2's chain, B is missing from s1/s3's chains).
        // Difference = {A, B}.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "x", &[]);
        insert(&mut provider, &mut bag, "a", &["x"]);
        insert(&mut provider, &mut bag, "b", &["x"]);
        let s1 = state_from_labels(&bag, &[("m.room.name", "", "a")]);
        let s2 = state_from_labels(&bag, &[("m.room.name", "", "b")]);
        let s3 = state_from_labels(&bag, &[("m.room.name", "", "a")]);
        let diff = auth_chain_difference(&[&s1, &s2, &s3], &provider).unwrap();
        let expected: HashSet<_> = [bag["a"].clone(), bag["b"].clone()].into_iter().collect();
        assert_eq!(diff, expected);
    }

    // ===== Phase 4b: power_of_sender / reverse_topological_power_sort / iterative_auth_checks =====

    use neutrino_common::ROOM_VERSION_ID;
    use ruma::RoomId;

    /// Stable room_id for non-create test events. The auth rules don't
    /// cross-check the room_id against the create event's derived id, so a
    /// fixed string is fine for unit tests.
    fn test_room() -> &'static RoomId {
        room_id!("!room:example.org")
    }

    /// Build an `m.room.create` event. `additional_creators` is a slice of
    /// user-id strings; passing an empty slice omits the field. `federate =
    /// false` writes `m.federate: false` into content; `true` omits it (the
    /// default).
    fn build_create(creator: &str, additional_creators: &[&str], federate: bool) -> Event {
        let mut content = json!({ "room_version": ROOM_VERSION_ID });
        if !additional_creators.is_empty() {
            content["additional_creators"] = json!(
                additional_creators
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            );
        }
        if !federate {
            content["m.federate"] = json!(false);
        }
        EventBuilder::new(
            creator.parse().expect("creator"),
            "m.room.create".to_owned(),
        )
        .state_key(String::new())
        .content(content)
        .origin_server_ts(next_ts())
        .build()
        .expect("valid create")
    }

    /// `m.room.power_levels` event with caller-supplied content + auth chain.
    fn build_power_levels(
        sender: &str,
        content: serde_json::Value,
        auth: Vec<OwnedEventId>,
    ) -> Event {
        EventBuilder::new(
            sender.parse().expect("sender"),
            "m.room.power_levels".to_owned(),
        )
        .room_id(test_room().to_owned())
        .state_key(String::new())
        .content(content)
        .auth_events(auth)
        .origin_server_ts(next_ts())
        .build()
        .expect("valid power_levels")
    }

    /// `m.room.message` event with caller-supplied auth chain.
    fn build_message(sender: &str, auth: Vec<OwnedEventId>) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.message".to_owned())
            .room_id(test_room().to_owned())
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .auth_events(auth)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid message")
    }

    /// `m.room.topic` state event with caller-supplied auth chain — used as a
    /// non-member state event for rule 4 / rule 8 paths.
    fn build_topic(sender: &str, auth: Vec<OwnedEventId>) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.topic".to_owned())
            .room_id(test_room().to_owned())
            .state_key(String::new())
            .content(json!({ "topic": "hi" }))
            .auth_events(auth)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid topic")
    }

    /// Insert an event into the provider as `rejected: false` and return its
    /// id.
    fn put(provider: &mut InMemoryStateProvider, ev: Event) -> OwnedEventId {
        let id = ev.event_id.clone();
        provider.insert(EventInfo {
            event: Arc::new(ev),
            rejected: false,
        });
        id
    }

    /// Insert an event into the provider as `rejected: true` (skipped by
    /// downstream consumers).
    fn put_rejected(provider: &mut InMemoryStateProvider, ev: Event) -> OwnedEventId {
        let id = ev.event_id.clone();
        provider.insert(EventInfo {
            event: Arc::new(ev),
            rejected: true,
        });
        id
    }

    // ----- power_of_sender -----

    #[test]
    fn power_of_sender_create_event_is_max() {
        let provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        // The create event's auth_events is empty per spec; power_of_sender
        // short-circuits to MAX.
        assert_eq!(power_of_sender(&create, &provider).unwrap(), i64::MAX);
    }

    #[test]
    fn power_of_sender_creator_with_no_pl_in_auth() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        // alice messages the room before any PL exists. Her power = MAX
        // (she's the creator).
        let msg = build_message("@alice:example.org", vec![create_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), i64::MAX);
    }

    #[test]
    fn power_of_sender_additional_creator_is_max() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &["@bob:example.org"], true);
        let create_id = put(&mut provider, create);
        let msg = build_message("@bob:example.org", vec![create_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), i64::MAX);
    }

    #[test]
    fn power_of_sender_non_creator_no_pl_returns_zero() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let msg = build_message("@charlie:example.org", vec![create_id]);
        // No PL in auth_events, charlie isn't a creator → users_default (0).
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 0);
    }

    #[test]
    fn power_of_sender_uses_explicit_users_entry() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let pl = build_power_levels(
            "@alice:example.org",
            json!({ "users": { "@charlie:example.org": 42 } }),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let msg = build_message("@charlie:example.org", vec![create_id, pl_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 42);
    }

    #[test]
    fn power_of_sender_falls_back_to_users_default() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let pl = build_power_levels(
            "@alice:example.org",
            json!({ "users_default": 33 }),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let msg = build_message("@charlie:example.org", vec![create_id, pl_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 33);
    }

    #[test]
    fn power_of_sender_skips_rejected_pl_in_auth_events() {
        // If the PL referenced in auth_events is marked rejected, fall through
        // to the no-PL path. Matches synapse's `if ev.rejected_reason is None`
        // filter (we don't model `rejected` as a soft-fail here, just as
        // "treat as absent").
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let pl = build_power_levels(
            "@alice:example.org",
            json!({ "users": { "@charlie:example.org": 99 } }),
            vec![create_id.clone()],
        );
        let pl_id = put_rejected(&mut provider, pl);
        let msg = build_message("@charlie:example.org", vec![create_id, pl_id]);
        // Rejected PL is skipped → charlie falls back to 0 (no PL, non-creator).
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 0);
    }

    // ----- reverse_topological_power_sort -----

    #[test]
    fn sort_empty_input_returns_empty() {
        let provider = InMemoryStateProvider::new();
        let out = reverse_topological_power_sort(&HashSet::new(), &provider).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn sort_chain_orders_parent_before_child() {
        // c (no auth) → b (auth = [c]) → a (auth = [b]). Sort yields [c, b, a].
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "c", &[]);
        insert(&mut provider, &mut bag, "b", &["c"]);
        insert(&mut provider, &mut bag, "a", &["b"]);
        let events: HashSet<_> = [bag["a"].clone(), bag["b"].clone(), bag["c"].clone()]
            .into_iter()
            .collect();
        let sorted = reverse_topological_power_sort(&events, &provider).unwrap();
        assert_eq!(
            sorted,
            vec![bag["c"].clone(), bag["b"].clone(), bag["a"].clone()]
        );
    }

    #[test]
    fn sort_higher_power_first_for_same_outdegree() {
        // Two events, both outdegree 0. alice = creator (MAX); charlie = 0.
        // Output order: alice first.
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let alice_msg = build_message("@alice:example.org", vec![create_id.clone()]);
        let charlie_msg = build_message("@charlie:example.org", vec![create_id.clone()]);
        let alice_id = put(&mut provider, alice_msg);
        let charlie_id = put(&mut provider, charlie_msg);
        let events: HashSet<_> = [alice_id.clone(), charlie_id.clone()].into_iter().collect();
        // The sort set excludes the create event so both events have
        // outdegree 0 in the restricted graph.
        let sorted = reverse_topological_power_sort(&events, &provider).unwrap();
        assert_eq!(sorted, vec![alice_id, charlie_id]);
    }

    #[test]
    fn sort_ts_tiebreak_when_power_equal() {
        // Two messages by the same sender (same power). next_ts() guarantees
        // strictly monotonic origin_server_ts, so the *earlier* event must
        // come first in the sort (synapse heap key: ts ascending).
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let early = build_message("@charlie:example.org", vec![create_id.clone()]);
        let late = build_message("@charlie:example.org", vec![create_id.clone()]);
        assert!(
            early.origin_server_ts < late.origin_server_ts,
            "next_ts() must be monotonic for this test"
        );
        let early_id = put(&mut provider, early);
        let late_id = put(&mut provider, late);
        let events: HashSet<_> = [late_id.clone(), early_id.clone()].into_iter().collect();
        let sorted = reverse_topological_power_sort(&events, &provider).unwrap();
        assert_eq!(sorted, vec![early_id, late_id]);
    }

    #[test]
    fn sort_diamond_orders_root_first_and_top_last() {
        //     top  (auth = [a, b])
        //     / \
        //    a   b   (each auth = [d])
        //     \ /
        //      d  (no auth)
        // Expected: d first, then {a, b} in tiebreak order, then top.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "d", &[]);
        insert(&mut provider, &mut bag, "a", &["d"]);
        insert(&mut provider, &mut bag, "b", &["d"]);
        insert(&mut provider, &mut bag, "top", &["a", "b"]);
        let events: HashSet<_> = [
            bag["a"].clone(),
            bag["b"].clone(),
            bag["d"].clone(),
            bag["top"].clone(),
        ]
        .into_iter()
        .collect();
        let sorted = reverse_topological_power_sort(&events, &provider).unwrap();
        // Don't pin a/b order (both have power 0 from placeholder + same
        // sender; id-tiebreak depends on hash-derived ids). Pin only the
        // structural property: d first, top last.
        assert_eq!(sorted.len(), 4);
        assert_eq!(sorted[0], bag["d"]);
        assert_eq!(sorted[3], bag["top"]);
        let middle: HashSet<_> = sorted[1..3].iter().cloned().collect();
        let expected_middle: HashSet<_> =
            [bag["a"].clone(), bag["b"].clone()].into_iter().collect();
        assert_eq!(middle, expected_middle);
    }

    #[test]
    fn sort_excludes_out_of_set_auth_parents_from_outdegree() {
        // The outdegree restriction must only count parents that are in the
        // input set. Otherwise events with parents outside the set would
        // never reach outdegree 0 and the sort would stall.
        let mut provider = InMemoryStateProvider::new();
        let mut bag = HashMap::new();
        insert(&mut provider, &mut bag, "outside", &[]);
        insert(&mut provider, &mut bag, "inside", &["outside"]);
        let events: HashSet<_> = [bag["inside"].clone()].into_iter().collect();
        let sorted = reverse_topological_power_sort(&events, &provider).unwrap();
        assert_eq!(sorted, vec![bag["inside"].clone()]);
    }

    // ----- iterative_auth_checks -----

    #[test]
    fn iac_empty_sorted_returns_initial_state() {
        let provider = InMemoryStateProvider::new();
        let mut initial = StateMap::new();
        initial.insert(
            ("m.room.name".to_string(), String::new()),
            eid("$preexisting:example.org"),
        );
        let out = iterative_auth_checks(&[], initial.clone(), &provider).unwrap();
        assert_eq!(out, initial);
    }

    #[test]
    fn iac_accepts_create_event_and_writes_to_resolved() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put(&mut provider, create);
        let out =
            iterative_auth_checks(std::slice::from_ref(&create_id), StateMap::new(), &provider)
                .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out.get(&("m.room.create".to_string(), String::new())),
            Some(&create_id)
        );
    }

    #[test]
    fn iac_skips_pre_rejected_event() {
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:example.org", &[], true);
        let create_id = put_rejected(&mut provider, create);
        let out =
            iterative_auth_checks(std::slice::from_ref(&create_id), StateMap::new(), &provider)
                .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn iac_auth_rule_failure_does_not_apply_event() {
        // create with m.federate=false. A topic from a different domain
        // fails rule 4 → not written into resolved.
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:here.org", &[], false);
        let create_id = put(&mut provider, create);
        let topic = build_topic("@bob:there.org", vec![create_id.clone()]);
        let topic_id = put(&mut provider, topic);
        let out = iterative_auth_checks(&[create_id.clone(), topic_id], StateMap::new(), &provider)
            .unwrap();
        // Only create made it in; topic was rule-4 rejected.
        assert_eq!(out.len(), 1);
        assert!(
            out.contains_key(&("m.room.create".to_string(), String::new())),
            "create event applied"
        );
        assert!(
            !out.contains_key(&("m.room.topic".to_string(), String::new())),
            "rule-4-rejected topic must not be in resolved state"
        );
    }

    #[test]
    fn iac_accepted_non_state_event_leaves_resolved_unchanged() {
        // alice sends a message. It passes auth (creator, rule 6 self-joined
        // is bypassed because alice is creator and has implicit join; in
        // practice we also need alice's member event in auth, but the v12
        // rule 6 check uses ctx.membership which falls back to None — so the
        // message would be rejected by rule 6. Use a more permissive setup:
        // a non-member, non-message *state* event by the creator is the only
        // case that mechanically auth-passes without alice's member event.
        // Switch to topic by alice — passes rule 8 (creator → MAX power),
        // but it IS a state event so we can't observe "state unchanged".
        //
        // So: directly verify that the loop runs without crashing on a
        // non-state event and the resolved state grows only by state-event
        // acceptances, by constructing a setup that has both a message and
        // a state event in the sort.
        let mut provider = InMemoryStateProvider::new();
        let create = build_create("@alice:here.org", &[], true);
        let create_id = put(&mut provider, create);
        // alice's message will fail rule 6 (no member event in auth), but
        // that's fine — the point of this test is that a non-state-event
        // acceptance OR rejection doesn't pollute `resolved`.
        let msg = build_message("@alice:here.org", vec![create_id.clone()]);
        let msg_id = put(&mut provider, msg);
        let out = iterative_auth_checks(&[create_id, msg_id], StateMap::new(), &provider).unwrap();
        // Whether the message accepts or rejects, `resolved` must only
        // contain the create event — m.room.message has no state_key.
        assert_eq!(out.len(), 1);
        assert!(out.contains_key(&("m.room.create".to_string(), String::new())));
    }

    #[test]
    fn iac_propagates_missing_auth_event_error() {
        // Build a topic referencing a create_id that is NOT in the provider.
        // Phase-4b IAC errors loudly per the project invariant ("every event
        // we know about has its complete auth chain locally resolvable").
        let mut provider = InMemoryStateProvider::new();
        let fake_create_id: OwnedEventId = "$nonexistent:example.org".parse().unwrap();
        let topic = build_topic("@alice:example.org", vec![fake_create_id.clone()]);
        let topic_id = put(&mut provider, topic);
        let err = iterative_auth_checks(&[topic_id], StateMap::new(), &provider).unwrap_err();
        assert!(matches!(err, StateResError::MissingAuthEvent(_)));
    }
}
