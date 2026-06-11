//! State resolution v2.1 (v12).
//!
//! This module composes a `StateMap<OwnedEventId>` from a list of input state
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
//! The pipeline is the reverse-topological power sort, the iterative auth
//! checks loop, mainline ordering, and IAC pass 2, all wrapped by the
//! `resolve_state` top-level entry point.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use ruma::{EventId, OwnedEventId};

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
            .get_event(aid)?
            .ok_or_else(|| StateResError::MissingEvent(aid.clone()))?;
        if info.rejected {
            continue;
        }
        match info.event_type.as_str() {
            "m.room.create" => {
                state.insert(("m.room.create".to_owned(), String::new()), info);
            }
            "m.room.power_levels" => {
                state.insert(("m.room.power_levels".to_owned(), String::new()), info);
            }
            _ => {}
        }
    }

    // AuthContext resolves the create event itself (v12 excludes it from
    // auth_events, so it may be absent from the mini-state built above) —
    // deriving it from the room_id and fetching via the provider.
    let ctx = AuthContext::new(&event.room_id, &state, provider)?;
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
            .get_event(eid)?
            .ok_or_else(|| StateResError::MissingEvent(eid.clone()))?;
        let pl = power_of_sender(&info, provider)?;
        event_map.insert(eid.clone(), info);
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
/// through unchanged for IAC pass 2. This function is agnostic to
/// which pass is running.
///
/// **Caller contract**: every event_id reachable from this run — every entry
/// in `sorted`, every id in their `event.auth_events`, and every value in
/// `initial_state` — must be in `provider`. A missing lookup raises
/// `StateResError::MissingEvent`. In particular: if `initial_state` is
/// not empty (i.e. IAC pass 2), the caller must ensure every
/// value in it is provider-known. Pass 1 sidesteps this by passing
/// `StateMap::new()`.
pub fn iterative_auth_checks(
    sorted: &[OwnedEventId],
    initial_state: StateMap<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<StateMap<OwnedEventId>, StateResError> {
    let mut resolved = initial_state;

    // The room's create event is the same for every event in this run (state-
    // res is per-room) but v12 keeps create out of `auth_events`, so the auth
    // map built below never carries it and each `AuthContext` would otherwise
    // re-derive + re-fetch it. Resolve it once and seed it into every auth map;
    // `AuthContext` still resolves create itself when this misses, so the seed
    // is a pure optimisation, not a correctness dependency.
    let mut create: Option<Arc<Event>> = None;

    for eid in sorted {
        let info = provider
            .get_event(eid)?
            .ok_or_else(|| StateResError::MissingEvent(eid.clone()))?;
        if info.rejected {
            continue;
        }
        let event = info;

        if create.is_none()
            && let Some(create_id) = crate::validate::derive_create_event_id(&event.room_id)
        {
            create = provider.get_event(&create_id)?;
        }

        let mut auth_map: StateMap<Arc<Event>> = HashMap::new();
        for aid in &event.auth_events {
            let parent = provider
                .get_event(aid)?
                .ok_or_else(|| StateResError::MissingEvent(aid.clone()))?;
            if parent.rejected {
                continue;
            }
            // calculate_auth_events only selects state events into auth_events;
            // a None state_key here means upstream (storage or calculate_auth_events)
            // produced a malformed auth chain. Surface loudly rather than
            // inserting under `(type, "")` and masking the corruption.
            let sk = parent
                .state_key
                .clone()
                .expect("auth_events entries are state events (state_key present)");
            let key = (parent.event_type.clone(), sk);
            auth_map.insert(key, parent);
        }

        // Iterative step: overlay resolved-state entries for the keys this
        // event actually consults.
        for key in auth_event_keys(&event) {
            if let Some(rs_id) = resolved.get(&key) {
                let rs_info = provider
                    .get_event(rs_id)?
                    .ok_or_else(|| StateResError::MissingEvent(rs_id.clone()))?;
                if !rs_info.rejected {
                    auth_map.insert(key, rs_info);
                }
            }
        }

        if let Some(create) = &create {
            auth_map
                .entry(("m.room.create".to_owned(), String::new()))
                .or_insert_with(|| create.clone());
        }

        if check_auth_rules(&event, &auth_map, provider).is_ok()
            && let Some(sk) = &event.state_key
        {
            resolved.insert((event.event_type.clone(), sk.clone()), eid.clone());
        }
    }

    Ok(resolved)
}

// ----------------- is_power_event / split_power_events -----------------

/// Whether `event` is a "power event" — one of the event types that affects
/// the room's power structure and therefore goes through the
/// reverse-topological power sort in `resolve_state`. The complement set
/// (non-power events) goes through mainline ordering.
///
/// Per spec v12 §state resolution + MSC4242 author guidance, the power-event
/// set is:
/// - `m.room.create` (creator membership is a power source — v12 addition
///   over v2.0's predicate set; non-default-creator demotion requires this).
/// - `m.room.join_rules` (state_key `""`).
/// - `m.room.power_levels` (state_key `""`).
/// - `m.room.member` with `content.membership` ∈ {`leave`, `ban`} AND
///   `sender != state_key` (i.e. kicks and bans — self-leaves, joins,
///   invites, and knocks are not power events).
pub fn is_power_event(event: &Event) -> bool {
    match event.event_type.as_str() {
        "m.room.create" => true,
        "m.room.join_rules" | "m.room.power_levels" => event.state_key.as_deref() == Some(""),
        "m.room.member" => {
            let Some(state_key) = event.state_key.as_deref() else {
                return false;
            };
            if event.sender.as_str() == state_key {
                return false;
            }
            let content: serde_json::Value =
                serde_json::from_str(event.content.get()).unwrap_or(serde_json::Value::Null);
            let membership = content.get("membership").and_then(|v| v.as_str());
            matches!(membership, Some("leave") | Some("ban"))
        }
        _ => false,
    }
}

/// Partition `events` into `(power, non_power)` using `is_power_event`.
/// Every id must resolve through the provider; missing ids → `MissingEvent`.
pub fn split_power_events(
    events: &HashSet<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<(HashSet<OwnedEventId>, HashSet<OwnedEventId>), StateResError> {
    let mut power = HashSet::new();
    let mut non_power = HashSet::new();
    for eid in events {
        let info = provider
            .get_event(eid)?
            .ok_or_else(|| StateResError::MissingEvent(eid.clone()))?;
        if is_power_event(&info) {
            power.insert(eid.clone());
        } else {
            non_power.insert(eid.clone());
        }
    }
    Ok((power, non_power))
}

/// Step 1 of the v2 algorithm: the set of events that go through the
/// reverse-topological power sort.
///
/// Spec v11/v12 §state resolution step 1: "Select the set X of all power events
/// that appear in the full conflicted set. For each such power event P, enlarge
/// X by adding the events in the auth chain of P which also belong to the full
/// conflicted set." Synapse parity: `_add_event_and_auth_chain_to_graph` walks
/// each power event's `auth_events` and pulls in any ancestor that is in the
/// full conflicted set.
///
/// The complement (`full_conflicted \ <this set>`) is what `resolve_state`
/// hands to the mainline sort in step 3 — so this enlargement is *not* a
/// no-op: it moves the conflicted auth-chain ancestors of power events (e.g. a
/// contested membership a power_levels event depends on) out of the mainline
/// pass and into the power-ordered pass, where the spec requires them.
fn power_sort_set(
    full_conflicted: &HashSet<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<HashSet<OwnedEventId>, StateResError> {
    let (power_seed, _) = split_power_events(full_conflicted, provider)?;
    if power_seed.is_empty() {
        return Ok(power_seed);
    }
    // `auth_chain` returns the transitive backwards closure including the
    // seeds; intersecting with `full_conflicted` keeps the power events plus
    // their in-conflict auth-chain ancestors and drops everything else.
    let closure = provider.auth_chain(&power_seed)?;
    Ok(closure.intersection(full_conflicted).cloned().collect())
}

// ----------------- mainline -----------------

/// Walk the m.room.power_levels chain backwards starting at `seed_pl_id`.
///
/// **Caller contract**: `seed_pl_id`, if `Some`, MUST reference an
/// `m.room.power_levels` event with `state_key == ""` (the canonical PL
/// state-key). The function does not verify this — it pushes `seed_pl_id`
/// onto the chain unconditionally and walks `auth_events` from there. A
/// non-PL seed produces a meaningless chain that breaks `mainline_position`
/// indexing downstream. In practice `resolve_state` produces the seed via
/// `after_pass_1.get(("m.room.power_levels", ""))`, which IAC only writes
/// for accepted PL events; external callers must enforce this themselves.
///
/// Each step inspects the current PL's `auth_events` for a parent PL
/// reference. Returns `[current_pl, prev_pl, prev_prev_pl, ...]` head-first
/// (most recent first), terminating when a PL has no PL ancestor in its
/// auth_events.
///
/// `None` seed → empty mainline. A `None` here is normal: it means IAC pass 1
/// produced no PL in the resolved state (e.g. the room has never set
/// power_levels). The mainline-position function treats every event as
/// "depth == 0" in that case (all equal, ts/id tiebreak only).
///
/// The walk is **transitive** across PLs (PL → prev PL → prev-prev PL) but
/// performs only a **single linear scan** of each PL's `auth_events` vec
/// looking for a PL parent — first-match-wins. No transitive walk inside a
/// single PL's auth_events (we never look at a non-PL parent's own
/// auth_events while building the mainline).
///
/// Synapse parity: `state/v2.py::_get_mainline_chain`.
pub fn mainline(
    seed_pl_id: Option<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<Vec<OwnedEventId>, StateResError> {
    let mut chain = Vec::new();
    let mut current = seed_pl_id;
    while let Some(pl_id) = current {
        chain.push(pl_id.clone());
        let info = provider
            .get_event(&pl_id)?
            .ok_or_else(|| StateResError::MissingEvent(pl_id.clone()))?;
        // Find the previous PL in this PL's auth_events. Single-step lookup
        // (no transitive walk): under MSC4242 v12, `calculate_auth_events`
        // emits the immediately-prior PL directly in `auth_events`.
        let mut next: Option<OwnedEventId> = None;
        for aid in &info.auth_events {
            let parent = provider
                .get_event(aid)?
                .ok_or_else(|| StateResError::MissingEvent(aid.clone()))?;
            if parent.rejected {
                continue;
            }
            if parent.event_type == "m.room.power_levels" && parent.state_key.as_deref() == Some("")
            {
                next = Some(aid.clone());
                break;
            }
        }
        current = next;
    }
    Ok(chain)
}

// ----------------- mainline_position -----------------

/// Mainline depth of `event_id`: the index in `mainline_map` of the nearest
/// PL ancestor reachable from `event_id` through its `auth_events` chain.
/// `mainline_map` indexes the mainline oldest→newest with a 1-based offset
/// (oldest PL → 1, newest PL → `mainline_len`), so a LARGER index = closer to
/// the resolved PL and sorts LATER (wins last-write-wins in IAC pass 2).
///
/// If no PL ancestor is found (or the closest PL isn't in the mainline),
/// returns `0` — strictly below every in-mainline index, so such events sort
/// FIRST among non-power events. Synapse parity:
/// `state/v2.py::_get_mainline_depth` (the `return 0` default, reserved by the
/// 1-based mainline indexing).
///
/// The walk follows each event's `m.room.power_levels` reference in
/// `auth_events`, recursing into that PL's own auth_events if its id isn't
/// in `mainline_map`. Visited set guards against cycles (which can't form
/// in a hash-derived auth chain, but the defence is cheap).
pub fn mainline_position(
    event_id: &OwnedEventId,
    mainline_map: &HashMap<OwnedEventId, usize>,
    provider: &dyn StateProvider,
) -> Result<usize, StateResError> {
    let mut current = event_id.clone();
    let mut visited: HashSet<OwnedEventId> = HashSet::new();
    loop {
        if let Some(&depth) = mainline_map.get(&current) {
            return Ok(depth);
        }
        if !visited.insert(current.clone()) {
            // Defensive cycle break — shouldn't trigger under hash-derived ids.
            return Ok(0);
        }
        let info = provider
            .get_event(&current)?
            .ok_or_else(|| StateResError::MissingEvent(current.clone()))?;
        let mut next: Option<OwnedEventId> = None;
        for aid in &info.auth_events {
            let parent = provider
                .get_event(aid)?
                .ok_or_else(|| StateResError::MissingEvent(aid.clone()))?;
            if parent.rejected {
                continue;
            }
            if parent.event_type == "m.room.power_levels" && parent.state_key.as_deref() == Some("")
            {
                next = Some(aid.clone());
                break;
            }
        }
        match next {
            Some(n) => current = n,
            None => return Ok(0),
        }
    }
}

// ----------------- mainline_sort -----------------

/// Sort `events` ascending by `(mainline_position, origin_server_ts, event_id)`.
///
/// `resolved_pl_id` is the PL event_id selected by IAC pass 1 (typically
/// `pass_1.get(("m.room.power_levels", ""))`). If `None`, the mainline is
/// empty and every event gets depth 0 — the sort collapses to
/// `(origin_server_ts, event_id)` ascending.
///
/// `mainline()` returns the PL chain newest-first; the index map is built over
/// its REVERSE (oldest→newest) with a 1-based offset, so the oldest PL gets
/// index 1 and the newest (resolved) PL gets the highest index. Ascending sort
/// then places events anchored at newer power_levels LAST, where IAC pass 2's
/// last-write-wins makes them win — and reserves index 0 for the no-PL-ancestor
/// default so those events sort first. Synapse parity:
/// `state/v2.py::_mainline_sort` + the `{ev: i + 1 for i, ev in
/// enumerate(reversed(mainline))}` indexing. The output is the order IAC pass 2
/// processes the non-power conflict set.
pub fn mainline_sort(
    events: &HashSet<OwnedEventId>,
    resolved_pl_id: Option<OwnedEventId>,
    provider: &dyn StateProvider,
) -> Result<Vec<OwnedEventId>, StateResError> {
    let chain = mainline(resolved_pl_id, provider)?;
    let mainline_map: HashMap<OwnedEventId, usize> = chain
        .iter()
        .rev()
        .enumerate()
        .map(|(i, id)| (id.clone(), i + 1))
        .collect();

    // (depth, ts, id) → ascending; OwnedEventId implements Ord.
    let mut decorated: Vec<(usize, u64, OwnedEventId)> = Vec::with_capacity(events.len());
    for eid in events {
        let info = provider
            .get_event(eid)?
            .ok_or_else(|| StateResError::MissingEvent(eid.clone()))?;
        let depth = mainline_position(eid, &mainline_map, provider)?;
        decorated.push((depth, info.origin_server_ts, eid.clone()));
    }
    decorated.sort();
    Ok(decorated.into_iter().map(|(_, _, id)| id).collect())
}

// ----------------- state_before / state_at_heads -----------------

/// Memoisation table for `state_at_heads` / `state_before` walks. Threaded
/// through both calls in `RoomCore::apply` so the overlapping
/// `prev_state_events` ↔ forward-extremity subgraphs are walked once.
pub type StateBeforeCache = HashMap<OwnedEventId, Arc<StateMap<OwnedEventId>>>;

/// Resolved state at the merge of `heads` — equivalently, state-before an
/// imaginary event whose `prev_state_events` are `heads`.
///
/// Spec semantics (v12 + MSC4242): for `heads = [H1, H2, ...]`, returns
/// `resolve_state(state_after(H1), state_after(H2), ...)`, where
/// `state_after(H) = state_before(H) ∪ {(H.type, H.state_key) → H.id}` if H
/// is itself a state event (message events leave state unchanged).
///
/// `cache` is consulted (and populated) for every event_id whose
/// `state_before` is computed during the walk. Pass a fresh cache for a
/// one-shot call; share one across multiple calls within the same `apply`
/// to amortise the overlap.
///
/// **Errors**: any unknown id traversed (a head, or any
/// `prev_state_events` ancestor) raises `StateResError::MissingEvent`,
/// per the strict-closure invariant.
pub fn state_at_heads(
    heads: &[OwnedEventId],
    provider: &dyn StateProvider,
    cache: &mut StateBeforeCache,
) -> Result<StateMap<OwnedEventId>, StateResError> {
    if heads.is_empty() {
        return Ok(StateMap::new());
    }
    let mut state_sets: Vec<StateMap<OwnedEventId>> = Vec::with_capacity(heads.len());
    for head in heads {
        let state_before_head = state_before_inner(head, provider, cache)?;
        let head_info = provider
            .get_event(head)?
            .ok_or_else(|| StateResError::MissingEvent(head.clone()))?;
        let mut state_after = (*state_before_head).clone();
        if let Some(sk) = &head_info.state_key {
            state_after.insert((head_info.event_type.clone(), sk.clone()), head.clone());
        }
        state_sets.push(state_after);
    }
    let refs: Vec<&StateMap<OwnedEventId>> = state_sets.iter().collect();
    resolve_state(&refs, provider)
}

/// Compute state-before-`event_id`: `state_at_heads(event.prev_state_events)`
/// after a `get_event` lookup. Root case (no `prev_state_events`, i.e. a
/// create event) returns the empty map.
///
/// Convenience wrapper that allocates its own cache; for `apply`'s
/// back-to-back walks, prefer `state_at_heads` with a shared
/// `StateBeforeCache`.
pub fn state_before(
    event_id: &EventId,
    provider: &dyn StateProvider,
) -> Result<StateMap<OwnedEventId>, StateResError> {
    let mut cache = StateBeforeCache::new();
    let arc = state_before_inner(event_id, provider, &mut cache)?;
    Ok((*arc).clone())
}

fn state_before_inner(
    event_id: &EventId,
    provider: &dyn StateProvider,
    cache: &mut StateBeforeCache,
) -> Result<Arc<StateMap<OwnedEventId>>, StateResError> {
    let owned: OwnedEventId = event_id.to_owned();
    if let Some(cached) = cache.get(&owned) {
        return Ok(cached.clone());
    }

    let info = provider
        .get_event(event_id)?
        .ok_or_else(|| StateResError::MissingEvent(owned.clone()))?;

    // Root: no prev_state_events (create event) → empty state-before.
    if info.prev_state_events.is_empty() {
        let empty = Arc::new(StateMap::new());
        cache.insert(owned, empty.clone());
        return Ok(empty);
    }

    let resolved = state_at_heads(&info.prev_state_events, provider, cache)?;
    let arc = Arc::new(resolved);
    cache.insert(owned, arc.clone());
    Ok(arc)
}

// ----------------- resolve_state -----------------

/// Resolve a list of state sets into a single `StateMap<OwnedEventId>` per
/// the v12 / state-res v2.1 algorithm.
///
/// Steps (spec v12 §state resolution):
/// 1. `separate` the state sets into unconflicted + conflicted.
/// 2. Compute the **full conflicted set** = `auth_chain_difference ∪
///    conflicted_subgraph ∪ conflicted-state-values` (v2.1 adds the subgraph;
///    v2 used just the diff + conflicted values).
/// 3. Compute the **step-1 set** via `power_sort_set`: the power events in the
///    full conflicted set plus their auth-chain ancestors that are also in it.
///    The complement is the mainline set.
/// 4. Reverse-topological power sort over the step-1 set.
/// 5. **IAC pass 1**: iterative auth checks from the **empty** state (v2.1
///    divergence from v2.0's "from unconflicted").
/// 6. Mainline sort over non-power events, anchored on pass-1's resolved PL.
/// 7. **IAC pass 2**: iterative auth checks seeded with pass-1's result.
/// 8. Overlay the unconflicted state onto pass-2's result. Keys are disjoint
///    by construction (unconflicted = keys where every input state set
///    agrees; the conflict path only touches keys with disagreement), but
///    the spec mandates "add entries from the unconflicted state map" so
///    unconflicted wins on the unlikely-collision path.
pub fn resolve_state(
    state_sets: &[&StateMap<OwnedEventId>],
    provider: &dyn StateProvider,
) -> Result<StateMap<OwnedEventId>, StateResError> {
    // (1) Separate.
    let Separated {
        unconflicted,
        conflicted,
    } = separate(state_sets);

    // (2) Full conflicted set.
    let conflicted_values: HashSet<OwnedEventId> = conflicted.values().flatten().cloned().collect();
    let auth_diff = auth_chain_difference(state_sets, provider)?;
    let subgraph = conflicted_subgraph(&conflicted_values, provider)?;
    let full_conflicted: HashSet<OwnedEventId> = auth_diff
        .into_iter()
        .chain(subgraph)
        .chain(conflicted_values)
        .collect();

    // (3) Step 1 set: power events PLUS their in-conflict auth-chain ancestors
    // (spec step 1's "enlarge X"). The remainder goes to the mainline sort.
    let power_events = power_sort_set(&full_conflicted, provider)?;
    let non_power_events: HashSet<OwnedEventId> =
        full_conflicted.difference(&power_events).cloned().collect();

    // (4) Reverse-topological power sort + (5) IAC pass 1 from empty.
    let sorted_power = reverse_topological_power_sort(&power_events, provider)?;
    let after_pass_1 = iterative_auth_checks(&sorted_power, StateMap::new(), provider)?;

    // (6) Mainline sort over non-power events.
    let resolved_pl = after_pass_1
        .get(&("m.room.power_levels".to_string(), String::new()))
        .cloned();
    let sorted_non_power = mainline_sort(&non_power_events, resolved_pl, provider)?;

    // (7) IAC pass 2 seeded with pass-1's result.
    let after_pass_2 = iterative_auth_checks(&sorted_non_power, after_pass_1, provider)?;

    // (8) Overlay unconflicted (disjoint by construction; unconflicted wins
    // on the unlikely-collision path per spec step 7).
    let mut final_state = after_pass_2;
    for (k, v) in unconflicted {
        final_state.insert(k, v);
    }
    Ok(final_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::event_id::EventBuilder;
    use crate::provider::InMemoryStateProvider;
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

    /// Seed the create event for the placeholder room into `provider` so
    /// `power_of_sender` (via `AuthContext`) can resolve the creator. The id is
    /// forced to that room's derived create id; power-of-sender only consults
    /// the create's id, sender, and content. Needed by the reverse-topological-
    /// power-sort tests, which otherwise build a room with no create — an
    /// unrealistic shape that used to silently yield power 0 for everyone.
    fn seed_placeholder_create(provider: &mut InMemoryStateProvider) {
        let mut create = EventBuilder::new(
            user_id!("@alice:example.org").to_owned(),
            "m.room.create".to_owned(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .origin_server_ts(next_ts())
        .build()
        .expect("create");
        // The placeholder helpers build events in this room; the create id is
        // its room_id's derived create id (sigil swap).
        let room = room_id!("!room:example.org").to_owned();
        create.event_id = crate::validate::derive_create_event_id(&room).expect("derive create id");
        create.room_id = room;
        provider.insert(Arc::new(create));
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
        provider.insert(ev);
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
        assert!(matches!(err, StateResError::MissingEvent(_)));
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

    // ===== power_of_sender / reverse_topological_power_sort / iterative_auth_checks =====

    use neutrino_common::ROOM_VERSION_ID;
    use neutrino_common::event_id::room_id_from_create;
    use ruma::{OwnedRoomId, RoomId};

    /// Build an `m.room.create` event. `additional_creators` is a slice of
    /// user-id strings; passing an empty slice omits the field. `federate =
    /// false` writes `m.federate: false` into content; `true` omits it.
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

    /// One-shot test-room setup: builds a create event, derives its room_id
    /// via the v12 sigil swap (`!` ← `$`), inserts the create into a fresh
    /// in-memory provider, and returns the bundle.
    ///
    /// All non-create events built by `room_msg`, `room_pl`, `room_topic`
    /// thread this derived room_id through (no synthetic `!room:example.org`
    /// — the v12 spec only admits sigil-swapped event-id-derived room_ids).
    fn setup(
        creator: &str,
        additional_creators: &[&str],
        federate: bool,
    ) -> (InMemoryStateProvider, OwnedEventId, OwnedRoomId) {
        let create = build_create(creator, additional_creators, federate);
        let room_id = room_id_from_create(&create.event_id);
        let mut provider = InMemoryStateProvider::new();
        let create_id = put(&mut provider, create);
        (provider, create_id, room_id)
    }

    /// Sugar for `setup("@alice:example.org", &[], true)`.
    fn setup_default() -> (InMemoryStateProvider, OwnedEventId, OwnedRoomId) {
        setup("@alice:example.org", &[], true)
    }

    /// `m.room.power_levels` event in a known room with caller-supplied
    /// content + auth chain.
    fn room_pl(
        room_id: &RoomId,
        sender: &str,
        content: serde_json::Value,
        auth: Vec<OwnedEventId>,
    ) -> Event {
        EventBuilder::new(
            sender.parse().expect("sender"),
            "m.room.power_levels".to_owned(),
        )
        .room_id(room_id.to_owned())
        .state_key(String::new())
        .content(content)
        .auth_events(auth)
        .origin_server_ts(next_ts())
        .build()
        .expect("valid power_levels")
    }

    /// `m.room.message` event in a known room with caller-supplied auth chain.
    fn room_msg(room_id: &RoomId, sender: &str, auth: Vec<OwnedEventId>) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.message".to_owned())
            .room_id(room_id.to_owned())
            .content(json!({ "msgtype": "m.text", "body": "hi" }))
            .auth_events(auth)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid message")
    }

    /// `m.room.topic` state event in a known room — used as a non-member
    /// state event for rule 4 / rule 8 paths.
    fn room_topic(room_id: &RoomId, sender: &str, auth: Vec<OwnedEventId>) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.topic".to_owned())
            .room_id(room_id.to_owned())
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
        provider.insert(Arc::new(ev));
        id
    }

    /// Insert an event into the provider as `rejected: true` (skipped by
    /// downstream consumers).
    fn put_rejected(provider: &mut InMemoryStateProvider, mut ev: Event) -> OwnedEventId {
        ev.rejected = true;
        let id = ev.event_id.clone();
        provider.insert(Arc::new(ev));
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
        let (provider, create_id, room_id) = setup_default();
        // alice messages the room before any PL exists. Her power = MAX
        // (she's the creator).
        let msg = room_msg(&room_id, "@alice:example.org", vec![create_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), i64::MAX);
    }

    #[test]
    fn power_of_sender_additional_creator_is_max() {
        let (provider, create_id, room_id) =
            setup("@alice:example.org", &["@bob:example.org"], true);
        let msg = room_msg(&room_id, "@bob:example.org", vec![create_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), i64::MAX);
    }

    #[test]
    fn power_of_sender_non_creator_no_pl_returns_zero() {
        let (provider, create_id, room_id) = setup_default();
        let msg = room_msg(&room_id, "@charlie:example.org", vec![create_id]);
        // No PL in auth_events, charlie isn't a creator → users_default (0).
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 0);
    }

    #[test]
    fn power_of_sender_uses_explicit_users_entry() {
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users": { "@charlie:example.org": 42 } }),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let msg = room_msg(&room_id, "@charlie:example.org", vec![create_id, pl_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 42);
    }

    #[test]
    fn power_of_sender_falls_back_to_users_default() {
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 33 }),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let msg = room_msg(&room_id, "@charlie:example.org", vec![create_id, pl_id]);
        assert_eq!(power_of_sender(&msg, &provider).unwrap(), 33);
    }

    #[test]
    fn power_of_sender_skips_rejected_pl_in_auth_events() {
        // If the PL referenced in auth_events is marked rejected, fall through
        // to the no-PL path. Matches synapse's `if ev.rejected_reason is None`
        // filter (we don't model `rejected` as a soft-fail here, just as
        // "treat as absent").
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users": { "@charlie:example.org": 99 } }),
            vec![create_id.clone()],
        );
        let pl_id = put_rejected(&mut provider, pl);
        let msg = room_msg(&room_id, "@charlie:example.org", vec![create_id, pl_id]);
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
        seed_placeholder_create(&mut provider);
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
        let (mut provider, create_id, room_id) = setup_default();
        let alice_msg = room_msg(&room_id, "@alice:example.org", vec![create_id.clone()]);
        let charlie_msg = room_msg(&room_id, "@charlie:example.org", vec![create_id]);
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
        let (mut provider, create_id, room_id) = setup_default();
        let early = room_msg(&room_id, "@charlie:example.org", vec![create_id.clone()]);
        let late = room_msg(&room_id, "@charlie:example.org", vec![create_id]);
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
        seed_placeholder_create(&mut provider);
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
        // Don't pin a/b order (both have equal power — same creator sender —
        // so id-tiebreak decides, and that depends on hash-derived ids). Pin
        // only the structural property: d first, top last.
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
        seed_placeholder_create(&mut provider);
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
        let (mut provider, create_id, room_id) = setup("@alice:here.org", &[], false);
        let topic = room_topic(&room_id, "@bob:there.org", vec![create_id.clone()]);
        let topic_id = put(&mut provider, topic);
        let out =
            iterative_auth_checks(&[create_id, topic_id], StateMap::new(), &provider).unwrap();
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
        // Sort = [create, alice_message]. The message has no state_key — IAC
        // must not surface it in `resolved` regardless of whether
        // check_auth_rules accepts the message under this minimal setup.
        let (mut provider, create_id, room_id) = setup_default();
        let msg = room_msg(&room_id, "@alice:example.org", vec![create_id.clone()]);
        let msg_id = put(&mut provider, msg);
        let out = iterative_auth_checks(&[create_id, msg_id], StateMap::new(), &provider).unwrap();
        // `resolved` must only contain the create event — m.room.message has no state_key.
        assert_eq!(out.len(), 1);
        assert!(out.contains_key(&("m.room.create".to_string(), String::new())));
    }

    #[test]
    fn iac_propagates_missing_auth_event_error() {
        // Build a topic referencing a create_id that is NOT in the provider.
        // The phantom create event is built but never inserted, giving us a
        // v12-shaped room_id without populating the auth chain. IAC
        // errors loudly per the project invariant ("every event we know about
        // has its complete auth chain locally resolvable").
        let phantom_create = build_create("@alice:example.org", &[], true);
        let phantom_room = room_id_from_create(&phantom_create.event_id);
        let fake_create_id = phantom_create.event_id.clone();
        let mut provider = InMemoryStateProvider::new();
        let topic = room_topic(&phantom_room, "@alice:example.org", vec![fake_create_id]);
        let topic_id = put(&mut provider, topic);
        let err = iterative_auth_checks(&[topic_id], StateMap::new(), &provider).unwrap_err();
        assert!(matches!(err, StateResError::MissingEvent(_)));
    }

    // ===== mainline ordering / resolve_state =====

    /// Build an `m.room.join_rules` event with a chosen `join_rule`.
    fn room_join_rules(
        room_id: &RoomId,
        sender: &str,
        join_rule: &str,
        auth: Vec<OwnedEventId>,
    ) -> Event {
        EventBuilder::new(
            sender.parse().expect("sender"),
            "m.room.join_rules".to_owned(),
        )
        .room_id(room_id.to_owned())
        .state_key(String::new())
        .content(json!({ "join_rule": join_rule }))
        .auth_events(auth)
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join_rules")
    }

    /// Build an `m.room.member` event with chosen target/membership.
    fn room_member(
        room_id: &RoomId,
        sender: &str,
        target: &str,
        membership: &str,
        auth: Vec<OwnedEventId>,
    ) -> Event {
        EventBuilder::new(sender.parse().expect("sender"), "m.room.member".to_owned())
            .room_id(room_id.to_owned())
            .state_key(target.to_owned())
            .content(json!({ "membership": membership }))
            .auth_events(auth)
            .origin_server_ts(next_ts())
            .build()
            .expect("valid member")
    }

    // ----- is_power_event -----

    #[test]
    fn is_power_event_create_is_power() {
        let create = build_create("@alice:example.org", &[], true);
        assert!(is_power_event(&create));
    }

    #[test]
    fn is_power_event_pl_is_power() {
        let (_, create_id, room_id) = setup_default();
        let pl = room_pl(&room_id, "@alice:example.org", json!({}), vec![create_id]);
        assert!(is_power_event(&pl));
    }

    #[test]
    fn is_power_event_join_rules_is_power() {
        let (_, create_id, room_id) = setup_default();
        let jr = room_join_rules(&room_id, "@alice:example.org", "public", vec![create_id]);
        assert!(is_power_event(&jr));
    }

    #[test]
    fn is_power_event_self_leave_is_not_power() {
        let (_, create_id, room_id) = setup_default();
        let self_leave = room_member(
            &room_id,
            "@alice:example.org",
            "@alice:example.org",
            "leave",
            vec![create_id],
        );
        assert!(!is_power_event(&self_leave));
    }

    #[test]
    fn is_power_event_kick_is_power() {
        let (_, create_id, room_id) = setup_default();
        let kick = room_member(
            &room_id,
            "@alice:example.org",
            "@bob:example.org",
            "leave",
            vec![create_id],
        );
        assert!(is_power_event(&kick));
    }

    #[test]
    fn is_power_event_ban_is_power() {
        let (_, create_id, room_id) = setup_default();
        let ban = room_member(
            &room_id,
            "@alice:example.org",
            "@bob:example.org",
            "ban",
            vec![create_id],
        );
        assert!(is_power_event(&ban));
    }

    #[test]
    fn is_power_event_invite_is_not_power() {
        let (_, create_id, room_id) = setup_default();
        let invite = room_member(
            &room_id,
            "@alice:example.org",
            "@bob:example.org",
            "invite",
            vec![create_id],
        );
        assert!(!is_power_event(&invite));
    }

    #[test]
    fn is_power_event_self_join_is_not_power() {
        let (_, create_id, room_id) = setup_default();
        let join = room_member(
            &room_id,
            "@alice:example.org",
            "@alice:example.org",
            "join",
            vec![create_id],
        );
        assert!(!is_power_event(&join));
    }

    #[test]
    fn is_power_event_message_is_not_power() {
        let (_, create_id, room_id) = setup_default();
        let msg = room_msg(&room_id, "@alice:example.org", vec![create_id]);
        assert!(!is_power_event(&msg));
    }

    #[test]
    fn is_power_event_topic_is_not_power() {
        let (_, create_id, room_id) = setup_default();
        let topic = room_topic(&room_id, "@alice:example.org", vec![create_id]);
        assert!(!is_power_event(&topic));
    }

    // ----- split_power_events -----

    #[test]
    fn split_power_events_partitions_correctly() {
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let topic = room_topic(&room_id, "@alice:example.org", vec![create_id.clone()]);
        let topic_id = put(&mut provider, topic);
        let events: HashSet<_> = [create_id.clone(), pl_id.clone(), topic_id.clone()]
            .into_iter()
            .collect();
        let (power, non_power) = split_power_events(&events, &provider).unwrap();
        assert_eq!(power, HashSet::from([create_id, pl_id]));
        assert_eq!(non_power, HashSet::from([topic_id]));
    }

    // ----- power_sort_set (spec step 1 auth-chain enlargement) -----

    #[test]
    fn power_sort_set_pulls_in_conflicted_auth_ancestor_of_power_event() {
        // Spec step 1: "For each power event P, enlarge X by adding the events
        // in the auth chain of P which also belong to the full conflicted set."
        // Here a contested self-join (NOT a power event) is the sender
        // membership a contested power_levels event depends on. It MUST land in
        // the power-sort set, not the mainline set — the bug routed it to the
        // mainline because the split was done purely by `is_power_event`.
        let (mut provider, create_id, room_id) = setup_default();
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let join_id = put(&mut provider, alice_join);
        // power_levels whose auth chain includes the self-join above.
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 1 }),
            vec![create_id.clone(), join_id.clone()],
        );
        let pl_id = put(&mut provider, pl);

        let full: HashSet<OwnedEventId> = [create_id.clone(), join_id.clone(), pl_id.clone()]
            .into_iter()
            .collect();

        // The naive `is_power_event` split (the old behaviour) excludes the
        // self-join — proving the enlargement below is load-bearing, not a
        // no-op.
        let (naive_power, naive_non_power) = split_power_events(&full, &provider).unwrap();
        assert!(
            !naive_power.contains(&join_id),
            "a self-join is not itself a power event"
        );
        assert!(naive_non_power.contains(&join_id));

        // The step-1 set pulls the self-join in via the power_levels auth chain.
        let step1 = power_sort_set(&full, &provider).unwrap();
        assert!(
            step1.contains(&join_id),
            "conflicted auth-ancestor of a power event must join the power-sort set"
        );
        assert!(step1.contains(&pl_id));
        // And the mainline complement no longer carries it.
        let mainline_set: HashSet<OwnedEventId> = full.difference(&step1).cloned().collect();
        assert!(!mainline_set.contains(&join_id));
    }

    #[test]
    fn power_sort_set_empty_when_no_power_events() {
        // No power events in the conflicted set → nothing to anchor an auth
        // chain on → the step-1 set is empty (everything goes to the mainline).
        let (mut provider, create_id, room_id) = setup_default();
        let topic = room_topic(&room_id, "@alice:example.org", vec![create_id]);
        let topic_id = put(&mut provider, topic);
        let full: HashSet<OwnedEventId> = [topic_id].into_iter().collect();
        assert!(power_sort_set(&full, &provider).unwrap().is_empty());
    }

    // ----- mainline -----

    #[test]
    fn mainline_none_seed_returns_empty() {
        let provider = InMemoryStateProvider::new();
        assert!(mainline(None, &provider).unwrap().is_empty());
    }

    #[test]
    fn mainline_single_pl_returns_just_that_pl() {
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(&room_id, "@alice:example.org", json!({}), vec![create_id]);
        let pl_id = put(&mut provider, pl);
        let chain = mainline(Some(pl_id.clone()), &provider).unwrap();
        assert_eq!(chain, vec![pl_id]);
    }

    #[test]
    fn mainline_walks_pl_chain_head_first() {
        // PL chain: pl_v1 → pl_v2 → pl_v3. pl_v3's auth_events references pl_v2;
        // pl_v2's auth_events references pl_v1. Seed at pl_v3, expect [v3, v2, v1].
        let (mut provider, create_id, room_id) = setup_default();
        let pl_v1 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_v1_id = put(&mut provider, pl_v1);
        let pl_v2 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({"users_default": 1}),
            vec![create_id.clone(), pl_v1_id.clone()],
        );
        let pl_v2_id = put(&mut provider, pl_v2);
        let pl_v3 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({"users_default": 2}),
            vec![create_id, pl_v2_id.clone()],
        );
        let pl_v3_id = put(&mut provider, pl_v3);
        let chain = mainline(Some(pl_v3_id.clone()), &provider).unwrap();
        assert_eq!(chain, vec![pl_v3_id, pl_v2_id, pl_v1_id]);
    }

    #[test]
    fn mainline_terminates_when_no_prev_pl_in_auth() {
        // First PL has no PL ancestor (only create); walk terminates after one step.
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(&room_id, "@alice:example.org", json!({}), vec![create_id]);
        let pl_id = put(&mut provider, pl);
        let chain = mainline(Some(pl_id.clone()), &provider).unwrap();
        assert_eq!(chain, vec![pl_id]);
    }

    #[test]
    fn mainline_skips_rejected_pl_ancestor() {
        // pl_v2 → pl_v1 (rejected). The chain finds no non-rejected PL parent
        // for pl_v2 → terminates after pl_v2.
        let (mut provider, create_id, room_id) = setup_default();
        let pl_v1 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_v1_id = put_rejected(&mut provider, pl_v1);
        let pl_v2 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({"users_default": 1}),
            vec![create_id, pl_v1_id],
        );
        let pl_v2_id = put(&mut provider, pl_v2);
        let chain = mainline(Some(pl_v2_id.clone()), &provider).unwrap();
        assert_eq!(chain, vec![pl_v2_id]);
    }

    // ----- mainline_position -----

    #[test]
    fn mainline_position_event_with_pl_in_mainline_returns_index() {
        let (mut provider, create_id, room_id) = setup_default();
        let pl_v1 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_v1_id = put(&mut provider, pl_v1);
        let pl_v2 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({"users_default": 1}),
            vec![create_id.clone(), pl_v1_id.clone()],
        );
        let pl_v2_id = put(&mut provider, pl_v2);
        // Event authored under pl_v1 (auth_events references pl_v1, not pl_v2).
        let topic = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id, pl_v1_id.clone()],
        );
        let topic_id = put(&mut provider, topic);
        let chain = mainline(Some(pl_v2_id.clone()), &provider).unwrap();
        // Mainline indexed oldest→newest, 1-based: pl_v1 at depth 1, pl_v2 at
        // depth 2 (newest = highest = wins).
        let map: HashMap<_, _> = chain
            .iter()
            .rev()
            .enumerate()
            .map(|(i, id)| (id.clone(), i + 1))
            .collect();
        // topic is anchored at pl_v1 → depth 1.
        let depth = mainline_position(&topic_id, &map, &provider).unwrap();
        assert_eq!(depth, 1);
    }

    #[test]
    fn mainline_position_event_with_no_pl_ancestor_returns_zero() {
        // Event whose auth_events contains create only — no PL ancestor. Synapse
        // reserves depth 0 for this case (the 1-based mainline indexing), so the
        // event sorts FIRST among non-power events.
        let (mut provider, create_id, room_id) = setup_default();
        let pl = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_id = put(&mut provider, pl);
        let topic = room_topic(&room_id, "@alice:example.org", vec![create_id]);
        let topic_id = put(&mut provider, topic);
        let chain = mainline(Some(pl_id), &provider).unwrap();
        let map: HashMap<_, _> = chain
            .iter()
            .rev()
            .enumerate()
            .map(|(i, id)| (id.clone(), i + 1))
            .collect();
        let depth = mainline_position(&topic_id, &map, &provider).unwrap();
        assert_eq!(depth, 0);
    }

    // ----- mainline_sort -----

    #[test]
    fn mainline_sort_empty_events_returns_empty() {
        let provider = InMemoryStateProvider::new();
        let out = mainline_sort(&HashSet::new(), None, &provider).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn mainline_sort_ts_ascending_when_no_pl() {
        // No PL anywhere → mainline is empty → every event has depth 0 →
        // sort is purely (ts, event_id) ascending.
        let (mut provider, create_id, room_id) = setup_default();
        let early = room_topic(&room_id, "@alice:example.org", vec![create_id.clone()]);
        let late = room_topic(&room_id, "@alice:example.org", vec![create_id]);
        let early_id = put(&mut provider, early);
        let late_id = put(&mut provider, late);
        let events: HashSet<_> = [late_id.clone(), early_id.clone()].into_iter().collect();
        let sorted = mainline_sort(&events, None, &provider).unwrap();
        assert_eq!(sorted, vec![early_id, late_id]);
    }

    #[test]
    fn mainline_sort_depth_orders_before_ts() {
        // Two events anchored at different power_levels. Timestamps OPPOSE depth
        // so the test isolates depth's effect: the event under the newer
        // (resolved) PL is given the EARLIER ts, so by ts alone it would sort
        // first. Mainline depth must override that — the newer-anchored event
        // gets the higher depth and sorts LAST, where IAC pass 2's
        // last-write-wins makes it win.
        let (mut provider, create_id, room_id) = setup_default();
        let pl_v1 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({}),
            vec![create_id.clone()],
        );
        let pl_v1_id = put(&mut provider, pl_v1);
        let pl_v2 = room_pl(
            &room_id,
            "@alice:example.org",
            json!({"users_default": 1}),
            vec![create_id.clone(), pl_v1_id.clone()],
        );
        let pl_v2_id = put(&mut provider, pl_v2);
        // EARLIER-ts topic under pl_v2 (newer PL, depth 2 → should sort LAST).
        let topic_under_v2 = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id.clone(), pl_v2_id.clone()],
        );
        let topic_under_v2_id = put(&mut provider, topic_under_v2);
        // LATER-ts topic under pl_v1 (older PL, depth 1 → should sort FIRST).
        let topic_under_v1 = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id, pl_v1_id.clone()],
        );
        let topic_under_v1_id = put(&mut provider, topic_under_v1);
        let events: HashSet<_> = [topic_under_v1_id.clone(), topic_under_v2_id.clone()]
            .into_iter()
            .collect();
        let sorted = mainline_sort(&events, Some(pl_v2_id), &provider).unwrap();
        // Depth 1 (older anchor) sorts before depth 2 (newer anchor), despite
        // topic_under_v2 having the earlier ts. Newest-anchored is LAST = wins.
        assert_eq!(sorted, vec![topic_under_v1_id, topic_under_v2_id]);
    }

    // ----- resolve_state -----

    #[test]
    fn resolve_state_single_state_set_returns_it() {
        // With a single state set, separate() makes everything unconflicted;
        // resolve_state returns that state set's entries.
        let (provider, create_id, _room_id) = setup_default();
        let mut s = StateMap::new();
        s.insert(
            ("m.room.create".to_string(), String::new()),
            create_id.clone(),
        );
        let out = resolve_state(&[&s], &provider).unwrap();
        assert_eq!(out, s);
    }

    #[test]
    fn resolve_state_no_conflict_keys_yields_union() {
        // Two state sets with the same create-event entry → unconflicted →
        // output equals the shared input.
        let (provider, create_id, _room_id) = setup_default();
        let mut s1 = StateMap::new();
        s1.insert(
            ("m.room.create".to_string(), String::new()),
            create_id.clone(),
        );
        let s2 = s1.clone();
        let out = resolve_state(&[&s1, &s2], &provider).unwrap();
        assert_eq!(out, s1);
    }

    #[test]
    fn resolve_state_conflicting_pls_picks_one() {
        // Two state sets disagree on the PL. Both PLs are authored by alice
        // (the creator); alice's join is in each PL's auth_events so rule 6
        // ("sender's current membership state must be join") passes during
        // IAC pass 1. Reverse-topological power sort processes both PLs;
        // last-write-wins, with the later origin_server_ts going last.
        let (mut provider, create_id, room_id) = setup_default();
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let alice_join_id = put(&mut provider, alice_join);

        let pl_early = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 1 }),
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let pl_early_id = put(&mut provider, pl_early);
        let pl_late = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 2 }),
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let pl_late_id = put(&mut provider, pl_late);

        // s1 chose pl_early, s2 chose pl_late.
        let mut s1 = StateMap::new();
        s1.insert(
            ("m.room.create".to_string(), String::new()),
            create_id.clone(),
        );
        s1.insert(
            (
                "m.room.member".to_string(),
                "@alice:example.org".to_string(),
            ),
            alice_join_id.clone(),
        );
        s1.insert(
            ("m.room.power_levels".to_string(), String::new()),
            pl_early_id.clone(),
        );
        let mut s2 = StateMap::new();
        s2.insert(
            ("m.room.create".to_string(), String::new()),
            create_id.clone(),
        );
        s2.insert(
            (
                "m.room.member".to_string(),
                "@alice:example.org".to_string(),
            ),
            alice_join_id,
        );
        s2.insert(
            ("m.room.power_levels".to_string(), String::new()),
            pl_late_id.clone(),
        );

        let out = resolve_state(&[&s1, &s2], &provider).unwrap();
        // pl_late has later origin_server_ts → wins.
        assert_eq!(
            out.get(&("m.room.power_levels".to_string(), String::new())),
            Some(&pl_late_id)
        );
        // create is unconflicted, preserved unchanged.
        assert_eq!(
            out.get(&("m.room.create".to_string(), String::new())),
            Some(&create_id)
        );
    }

    #[test]
    fn resolve_state_newer_pl_anchored_non_power_event_wins() {
        // Regression: mainline sort direction. Two conflicting NON-power events
        // (topics) anchored at different power_levels. The topic under the
        // newer (resolved) PL must win, EVEN THOUGH it has the earlier ts —
        // mainline depth dominates ts, and the newer anchor sorts last so IAC
        // pass 2's last-write-wins picks it. The buggy inverted indexing made
        // the older-anchored (later-ts) topic win instead.
        let (mut provider, create_id, room_id) = setup_default();
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let alice_join_id = put(&mut provider, alice_join);

        // pl_old then pl_new — pl_new has the later ts so it wins IAC pass 1 and
        // becomes the resolved PL the mainline is anchored on.
        let pl_old = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 1 }),
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let pl_old_id = put(&mut provider, pl_old);
        let pl_new = room_pl(
            &room_id,
            "@alice:example.org",
            json!({ "users_default": 2 }),
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let pl_new_id = put(&mut provider, pl_new);

        // EARLIER-ts topic under pl_new (depth 2 → sorts last → must win).
        let topic_new = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id.clone(), alice_join_id.clone(), pl_new_id.clone()],
        );
        let topic_new_id = put(&mut provider, topic_new);
        // LATER-ts topic under pl_old (depth 1 → sorts first → must lose).
        let topic_old = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id.clone(), alice_join_id.clone(), pl_old_id.clone()],
        );
        let topic_old_id = put(&mut provider, topic_old);

        let member_key = (
            "m.room.member".to_string(),
            "@alice:example.org".to_string(),
        );
        let create_key = ("m.room.create".to_string(), String::new());
        let pl_key = ("m.room.power_levels".to_string(), String::new());
        let topic_key = ("m.room.topic".to_string(), String::new());

        // s1 chose pl_old + topic_old, s2 chose pl_new + topic_new.
        let mut s1 = StateMap::new();
        s1.insert(create_key.clone(), create_id.clone());
        s1.insert(member_key.clone(), alice_join_id.clone());
        s1.insert(pl_key.clone(), pl_old_id);
        s1.insert(topic_key.clone(), topic_old_id);
        let mut s2 = StateMap::new();
        s2.insert(create_key, create_id);
        s2.insert(member_key, alice_join_id);
        s2.insert(pl_key, pl_new_id.clone());
        s2.insert(topic_key.clone(), topic_new_id.clone());

        let out = resolve_state(&[&s1, &s2], &provider).unwrap();
        // Resolved PL is pl_new (later ts), and the topic anchored at it wins
        // despite its earlier ts.
        assert_eq!(
            out.get(&("m.room.power_levels".to_string(), String::new())),
            Some(&pl_new_id)
        );
        assert_eq!(out.get(&topic_key), Some(&topic_new_id));
    }

    // ----- state_before -----

    #[test]
    fn state_before_create_event_is_empty() {
        let (provider, create_id, _) = setup_default();
        let out = state_before(&create_id, &provider).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn state_before_unknown_event_errors() {
        let provider = InMemoryStateProvider::new();
        let phantom = build_create("@alice:example.org", &[], true);
        let err = state_before(&phantom.event_id, &provider).unwrap_err();
        assert!(matches!(err, StateResError::MissingEvent(_)));
    }

    #[test]
    fn state_before_linear_state_chain_overlays_each_event() {
        // create → alice_join (state) → topic (state).
        // state_before(topic) should contain create + alice_join.
        let (mut provider, create_id, room_id) = setup_default();
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let alice_join_id = put(&mut provider, alice_join);
        let topic = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.topic".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key(String::new())
        .content(json!({ "topic": "hi" }))
        .auth_events(vec![create_id.clone(), alice_join_id.clone()])
        .prev_events(vec![alice_join_id.clone()])
        .prev_state_events(vec![alice_join_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid topic");
        let topic_id = put(&mut provider, topic);

        let out = state_before(&topic_id, &provider).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(
            out.get(&("m.room.create".to_string(), String::new())),
            Some(&create_id)
        );
        assert_eq!(
            out.get(&(
                "m.room.member".to_string(),
                "@alice:example.org".to_string()
            )),
            Some(&alice_join_id)
        );
    }

    #[test]
    fn state_before_message_event_in_chain_does_not_appear_in_state() {
        // create → alice_join (state) → message (NOT state) → topic (state).
        // state_before(topic) should NOT contain the message event.
        let (mut provider, create_id, room_id) = setup_default();
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let alice_join_id = put(&mut provider, alice_join);
        let msg = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.message".to_owned(),
        )
        .room_id(room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "hi" }))
        .auth_events(vec![create_id.clone(), alice_join_id.clone()])
        .prev_events(vec![alice_join_id.clone()])
        .prev_state_events(vec![alice_join_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid msg");
        let msg_id = put(&mut provider, msg);
        let topic = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.topic".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key(String::new())
        .content(json!({ "topic": "hi" }))
        .auth_events(vec![create_id.clone(), alice_join_id.clone()])
        .prev_events(vec![msg_id.clone()])
        // topic's prev_state_events points past msg to alice_join (msg is not a state event).
        .prev_state_events(vec![alice_join_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid topic");
        let topic_id = put(&mut provider, topic);

        let out = state_before(&topic_id, &provider).unwrap();
        // No m.room.message entry — message events leave state untouched.
        assert!(!out.contains_key(&("m.room.message".to_string(), String::new())));
        assert!(out.contains_key(&(
            "m.room.member".to_string(),
            "@alice:example.org".to_string()
        )));
    }

    #[test]
    fn resolve_state_unconflicted_keys_survive() {
        // s1 has (create, topic_v1); s2 has (create, topic_v2). Topic
        // conflicts, create doesn't. Resolve picks one topic; create is
        // preserved.
        let (mut provider, create_id, room_id) = setup_default();
        // alice joins (so subsequent topic events pass rule 6).
        let alice_join = EventBuilder::new(
            "@alice:example.org".parse().expect("user"),
            "m.room.member".to_owned(),
        )
        .room_id(room_id.clone())
        .state_key("@alice:example.org".to_owned())
        .content(json!({ "membership": "join" }))
        .auth_events(vec![create_id.clone()])
        .prev_events(vec![create_id.clone()])
        .prev_state_events(vec![create_id.clone()])
        .origin_server_ts(next_ts())
        .build()
        .expect("valid join");
        let alice_join_id = put(&mut provider, alice_join);
        let topic_v1 = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let topic_v1_id = put(&mut provider, topic_v1);
        let topic_v2 = room_topic(
            &room_id,
            "@alice:example.org",
            vec![create_id.clone(), alice_join_id.clone()],
        );
        let topic_v2_id = put(&mut provider, topic_v2);

        let mut s1 = StateMap::new();
        s1.insert(
            ("m.room.create".to_string(), String::new()),
            create_id.clone(),
        );
        s1.insert(
            (
                "m.room.member".to_string(),
                "@alice:example.org".to_string(),
            ),
            alice_join_id.clone(),
        );
        s1.insert(
            ("m.room.topic".to_string(), String::new()),
            topic_v1_id.clone(),
        );
        let mut s2 = s1.clone();
        s2.insert(
            ("m.room.topic".to_string(), String::new()),
            topic_v2_id.clone(),
        );

        let out = resolve_state(&[&s1, &s2], &provider).unwrap();
        // create + alice's member are unconflicted, preserved as-is.
        assert_eq!(
            out.get(&("m.room.create".to_string(), String::new())),
            Some(&create_id)
        );
        assert_eq!(
            out.get(&(
                "m.room.member".to_string(),
                "@alice:example.org".to_string()
            )),
            Some(&alice_join_id)
        );
        // Topic is conflicted; resolved value is one of the two candidates.
        let resolved_topic = out
            .get(&("m.room.topic".to_string(), String::new()))
            .expect("topic resolved");
        assert!(resolved_topic == &topic_v1_id || resolved_topic == &topic_v2_id);
    }
}
