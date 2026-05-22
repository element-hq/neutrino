//! Read-side interface the state machine uses to look up events, plus the
//! in-memory implementation used by tests and (until storage lands) by
//! Phase 6 `apply`.
//!
//! The trait carries:
//! - `get_event(id)`: used by `validate::validate_references` (Phase 1b) and
//!   anywhere downstream that needs the event body / rejection flag.
//! - `auth_chain(seeds)`: used by Phase 4 state resolution. Returns the
//!   transitive backwards closure of the seeds through their `auth_events`
//!   (including the seeds themselves). Implementations are free to choose
//!   their traversal strategy — in-memory does a stack-based DFS; a future
//!   SQLite-backed provider will use a recursive CTE.
//!
//! Under MSC4242 `auth_events` is **not** on the wire; the server calculates
//! it once at insert time and stores it on the `Event` struct. The provider
//! reads from there — there's no separate `auth_event_ids` map any more.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use neutrino_common::Event;
use ruma::{EventId, OwnedEventId};

/// View of an event known to the store, with its rejection status.
///
/// Rejection is tracked separately from the event body because state res
/// must still observe the existence of a rejected event (e.g. when deciding
/// not to walk its auth chain), but downstream callers usually need to
/// branch on the flag.
#[derive(Debug, Clone)]
pub struct EventInfo {
    pub event: Arc<Event>,
    pub rejected: bool,
}

/// Lookup interface the state machine uses to read events.
pub trait StateProvider {
    /// Look up an event by ID, returning `None` if the store does not know it.
    fn get_event(&self, id: &EventId) -> Option<EventInfo>;

    /// Transitive backwards closure of `seeds` via `Event.auth_events`.
    ///
    /// The result includes every `seed` that resolves to a known event (so
    /// downstream set operations don't have to special-case the seed
    /// themselves). Seeds that aren't in the store are silently dropped —
    /// state-res treats them as backfill boundaries.
    ///
    /// In-memory impls do a stack-based DFS reading `event.auth_events`
    /// per step. SQLite-backed impls collapse the walk to a single recursive
    /// CTE join against `event_edges WHERE edge_type = 'auth'` — see
    /// `event-id-design.md` §"What event_edges is doing".
    fn auth_chain(&self, seeds: &HashSet<OwnedEventId>) -> HashSet<OwnedEventId>;
}

/// In-memory `StateProvider`. Public (not test-cfg) — Phase 6 `apply` uses
/// it as the storage-free fallback, and unit tests use it directly.
#[derive(Debug, Default)]
pub struct InMemoryStateProvider {
    events: HashMap<OwnedEventId, EventInfo>,
}

impl InMemoryStateProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an event into the provider. `auth_events` lives on the
    /// `Event` itself (MSC4242 server-side metadata) — the provider
    /// reads them from there.
    pub fn insert(&mut self, info: EventInfo) {
        let id = info.event.event_id.clone();
        self.events.insert(id, info);
    }
}

impl StateProvider for InMemoryStateProvider {
    fn get_event(&self, id: &EventId) -> Option<EventInfo> {
        self.events.get(id).cloned()
    }

    fn auth_chain(&self, seeds: &HashSet<OwnedEventId>) -> HashSet<OwnedEventId> {
        let mut visited: HashSet<OwnedEventId> = HashSet::new();
        let mut stack: Vec<OwnedEventId> = seeds.iter().cloned().collect();
        while let Some(id) = stack.pop() {
            if !visited.insert(id.clone()) {
                continue;
            }
            // Pull this event's auth_events off the `Event` struct.
            // Unknown id → no parents to walk (federation backfill
            // boundary; the seed itself is still in `visited`).
            if let Some(info) = self.events.get(&id) {
                for parent in &info.event.auth_events {
                    if !visited.contains(parent) {
                        stack.push(parent.clone());
                    }
                }
            }
        }
        visited
    }
}
