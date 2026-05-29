//! Read-side interface the state machine uses to look up events, plus the
//! in-memory implementation used by tests and (until storage lands) by
//! Phase 6 `apply`.
//!
//! The trait carries:
//! - `get_event(id)`: used by `validate::validate_references` (Phase 1b) and
//!   anywhere downstream that needs the event body / rejection flag.
//! - `auth_chain(seeds)`: used by Phase 4 state resolution. Returns the
//!   transitive backwards closure of the seeds through their `auth_events`
//!   (including the seeds themselves). **Errors** if any event in the
//!   closure isn't in the store — every event we know about must have its
//!   complete auth chain locally (no federation auth-chain backfill).
//!
//! Under MSC4242 `auth_events` is **not** on the wire; the server calculates
//! it once at insert time and stores it on the `Event` struct. The provider
//! reads from there — there's no separate `auth_event_ids` map any more.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use neutrino_common::Event;
use ruma::{EventId, OwnedEventId};

use crate::StateResError;

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
    /// Returns every id reachable backwards from the seeds (seeds included
    /// in the result). **Errors** with `StateResError::MissingEvent` if
    /// any id — seed or transitively discovered — isn't in the store.
    ///
    /// The project invariant: every event we know about has its **complete**
    /// auth chain locally resolvable. We don't federate auth chains; every
    /// event is authored locally or arrives with its full chain. A missing
    /// entry indicates corruption or a write-path bug, never a normal
    /// backfill boundary — surface it loudly rather than walking around it.
    ///
    /// In-memory impls do a stack-based DFS reading `event.auth_events`
    /// per step. SQLite-backed impls collapse the walk to a single recursive
    /// CTE join against `event_edges WHERE edge_type = 'auth'` — see
    /// `event-id-design.md` §"What event_edges is doing".
    fn auth_chain(
        &self,
        seeds: &HashSet<OwnedEventId>,
    ) -> Result<HashSet<OwnedEventId>, StateResError>;
}

/// In-memory `StateProvider`. Public (not test-cfg) — Phase 6 `apply` uses
/// it as the storage-free fallback, and unit tests use it directly. Clone
/// is cheap: events are `Arc`-shared via `EventInfo`.
#[derive(Debug, Default, Clone)]
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

    fn auth_chain(
        &self,
        seeds: &HashSet<OwnedEventId>,
    ) -> Result<HashSet<OwnedEventId>, StateResError> {
        let mut visited: HashSet<OwnedEventId> = HashSet::new();
        let mut stack: Vec<OwnedEventId> = seeds.iter().cloned().collect();
        while let Some(id) = stack.pop() {
            if !visited.insert(id.clone()) {
                continue;
            }
            // Strict closure invariant: every id we visit (seed or
            // transitively discovered) must resolve. Missing => error.
            let info = self
                .events
                .get(&id)
                .ok_or_else(|| StateResError::MissingEvent(id.clone()))?;
            for parent in &info.event.auth_events {
                if !visited.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }
        Ok(visited)
    }
}
