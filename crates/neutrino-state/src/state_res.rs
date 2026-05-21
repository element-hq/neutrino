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
//! Phase 4a (this file as of now): `separate`, `conflicted_subgraph`,
//! `auth_chain_difference`. Phase 4b/4c will add the sorters, IAC, mainline,
//! and the `resolve_state` top-level entry point.

use std::collections::{HashMap, HashSet};

use ruma::OwnedEventId;

use crate::StateMap;
use crate::provider::StateProvider;

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
/// values produced by `separate`. The output joins `seeds` plus everything
/// transitively reachable via `provider.auth_event_ids`.
pub fn conflicted_subgraph(
    seeds: &HashSet<OwnedEventId>,
    provider: &dyn StateProvider,
) -> HashSet<OwnedEventId> {
    let mut visited: HashSet<OwnedEventId> = HashSet::new();
    let mut stack: Vec<OwnedEventId> = seeds.iter().cloned().collect();
    while let Some(id) = stack.pop() {
        if visited.insert(id.clone()) {
            for parent in provider.auth_event_ids(&id).iter() {
                if !visited.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }
    }
    visited
}

// ----------------- auth_chain_difference -----------------

/// Events that appear in *some* but not *all* auth chains across the input
/// state sets — i.e. `(∪ chains) \ (∩ chains)` where each chain is the
/// transitive backwards closure of an entire state set via
/// `provider.auth_event_ids`.
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
) -> HashSet<OwnedEventId> {
    if state_sets.len() < 2 {
        return HashSet::new();
    }

    let chains: Vec<HashSet<OwnedEventId>> = state_sets
        .iter()
        .map(|set| {
            let mut chain: HashSet<OwnedEventId> = HashSet::new();
            let mut stack: Vec<OwnedEventId> = set.values().cloned().collect();
            while let Some(id) = stack.pop() {
                if chain.insert(id.clone()) {
                    for parent in provider.auth_event_ids(&id).iter() {
                        if !chain.contains(parent) {
                            stack.push(parent.clone());
                        }
                    }
                }
            }
            chain
        })
        .collect();

    let union: HashSet<OwnedEventId> = chains.iter().flatten().cloned().collect();
    let mut intersection = chains[0].clone();
    for c in chains.iter().skip(1) {
        intersection = intersection.intersection(c).cloned().collect();
    }
    union.difference(&intersection).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{EventInfo, InMemoryStateProvider};
    use crate::validate::parse_event;
    use crate::{Event, RoomVersion};
    use serde_json::value::RawValue;
    use serde_json::{Value, json};
    use std::sync::Arc;

    fn raw(v: Value) -> Box<RawValue> {
        serde_json::value::to_raw_value(&v).expect("fixture")
    }
    fn eid(s: &str) -> OwnedEventId {
        s.parse().expect("event id")
    }

    /// Construct a minimal `Event` with a given event_id and (optionally) a
    /// state_key. Other fields are placeholder — state-res only consults
    /// event_id + auth_event_ids in 4a.
    fn make_event(id: &str, state_key: Option<&str>) -> Arc<Event> {
        let mut v = json!({
            "type": "m.room.placeholder",
            "sender": "@alice:example.org",
            "room_id": "!room:example.org",
            "content": {},
            "prev_events": [],
            "prev_state_events": [],
            "depth": 1,
            "origin_server_ts": 1_700_000_000_000_u64,
            "hashes": { "sha256": "abc" }
        });
        if let Some(sk) = state_key {
            v["state_key"] = json!(sk);
        }
        Arc::new(parse_event(raw(v), eid(id), RoomVersion::V12).expect("valid"))
    }

    fn insert(provider: &mut InMemoryStateProvider, id: &str, auth_chain: &[&str]) {
        let info = EventInfo {
            event: make_event(id, Some("")),
            rejected: false,
        };
        provider.insert(info, auth_chain.iter().map(|s| eid(s)).collect());
    }

    /// Build a state map from `(type, state_key, event_id)` triples.
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
    fn conflicted_subgraph_includes_seeds_even_with_no_parents() {
        let provider = InMemoryStateProvider::new();
        // Provider has no events / no auth_event_ids. Subgraph = seeds only.
        let mut seeds = HashSet::new();
        seeds.insert(eid("$a:example.org"));
        seeds.insert(eid("$b:example.org"));
        let sg = conflicted_subgraph(&seeds, &provider);
        assert_eq!(sg, seeds);
    }

    #[test]
    fn conflicted_subgraph_walks_auth_event_ids_transitively() {
        // a → b → c (a's auth_events = [b], b's auth_events = [c]).
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$a:example.org", &["$b:example.org"]);
        insert(&mut provider, "$b:example.org", &["$c:example.org"]);
        insert(&mut provider, "$c:example.org", &[]);
        let mut seeds = HashSet::new();
        seeds.insert(eid("$a:example.org"));
        let sg = conflicted_subgraph(&seeds, &provider);
        let expected: HashSet<_> = [
            eid("$a:example.org"),
            eid("$b:example.org"),
            eid("$c:example.org"),
        ]
        .into_iter()
        .collect();
        assert_eq!(sg, expected);
    }

    #[test]
    fn conflicted_subgraph_unions_multiple_seed_chains() {
        // a → b; c → d. Seeds {a, c} → {a, b, c, d}.
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$a:example.org", &["$b:example.org"]);
        insert(&mut provider, "$b:example.org", &[]);
        insert(&mut provider, "$c:example.org", &["$d:example.org"]);
        insert(&mut provider, "$d:example.org", &[]);
        let seeds: HashSet<_> = [eid("$a:example.org"), eid("$c:example.org")]
            .into_iter()
            .collect();
        let sg = conflicted_subgraph(&seeds, &provider);
        assert_eq!(sg.len(), 4);
    }

    // ----- auth_chain_difference -----

    #[test]
    fn auth_chain_difference_zero_or_one_state_set_is_empty() {
        let provider = InMemoryStateProvider::new();
        assert!(auth_chain_difference(&[], &provider).is_empty());
        let s1 = state(&[("m.room.name", "", "$n:example.org")]);
        assert!(auth_chain_difference(&[&s1], &provider).is_empty());
    }

    #[test]
    fn auth_chain_difference_empty_state_set_returns_other_chain() {
        // [empty, non_empty] → diff = full chain of non_empty.
        // Empty state set's chain is empty; intersection with empty = empty;
        // difference = union of all other chains.
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$a:example.org", &["$b:example.org"]);
        insert(&mut provider, "$b:example.org", &[]);
        let empty = StateMap::<OwnedEventId>::new();
        let s2 = state(&[("m.room.name", "", "$a:example.org")]);
        let diff = auth_chain_difference(&[&empty, &s2], &provider);
        let expected: HashSet<_> = [eid("$a:example.org"), eid("$b:example.org")]
            .into_iter()
            .collect();
        assert_eq!(diff, expected);
    }

    #[test]
    fn auth_chain_difference_identical_chains_empty() {
        // Two state sets share the same single event whose auth chain is X→Y.
        // Both chains are {X, Y}; difference is empty.
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$x:example.org", &["$y:example.org"]);
        insert(&mut provider, "$y:example.org", &[]);
        let s1 = state(&[("m.room.name", "", "$x:example.org")]);
        let s2 = state(&[("m.room.name", "", "$x:example.org")]);
        assert!(auth_chain_difference(&[&s1, &s2], &provider).is_empty());
    }

    #[test]
    fn auth_chain_difference_isolated_per_set_event_in_difference() {
        // s1 carries event A (auth chain {A}); s2 carries event B (auth chain
        // {B}). Union = {A, B}, intersection = {}. Difference = {A, B}.
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$a:example.org", &[]);
        insert(&mut provider, "$b:example.org", &[]);
        let s1 = state(&[("m.room.name", "", "$a:example.org")]);
        let s2 = state(&[("m.room.name", "", "$b:example.org")]);
        let diff = auth_chain_difference(&[&s1, &s2], &provider);
        let expected: HashSet<_> = [eid("$a:example.org"), eid("$b:example.org")]
            .into_iter()
            .collect();
        assert_eq!(diff, expected);
    }

    #[test]
    fn auth_chain_difference_shared_ancestor_not_in_difference() {
        // s1: event A with chain {A, common}; s2: event B with chain {B, common}.
        // Union = {A, B, common}; intersection = {common}; difference = {A, B}.
        let mut provider = InMemoryStateProvider::new();
        insert(&mut provider, "$a:example.org", &["$common:example.org"]);
        insert(&mut provider, "$b:example.org", &["$common:example.org"]);
        insert(&mut provider, "$common:example.org", &[]);
        let s1 = state(&[("m.room.name", "", "$a:example.org")]);
        let s2 = state(&[("m.room.name", "", "$b:example.org")]);
        let diff = auth_chain_difference(&[&s1, &s2], &provider);
        assert!(diff.contains(&eid("$a:example.org")));
        assert!(diff.contains(&eid("$b:example.org")));
        assert!(!diff.contains(&eid("$common:example.org")));
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
        insert(&mut provider, "$a:example.org", &["$x:example.org"]);
        insert(&mut provider, "$b:example.org", &["$x:example.org"]);
        insert(&mut provider, "$x:example.org", &[]);
        let s1 = state(&[("m.room.name", "", "$a:example.org")]);
        let s2 = state(&[("m.room.name", "", "$b:example.org")]);
        let s3 = state(&[("m.room.name", "", "$a:example.org")]);
        let diff = auth_chain_difference(&[&s1, &s2, &s3], &provider);
        let expected: HashSet<_> = [eid("$a:example.org"), eid("$b:example.org")]
            .into_iter()
            .collect();
        assert_eq!(diff, expected);
    }
}
