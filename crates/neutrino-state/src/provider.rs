//! Read-side interface the state machine uses to look up events, plus the
//! in-memory implementation used by tests and (until storage lands) by
//! `RoomCore::apply`.
//!
//! The trait carries:
//! - `get_event(id)`: used by `validate::validate_references` and
//!   anywhere downstream that needs the event body / rejection flag. Rejection
//!   lives on `Event.rejected` (server-computed, co-located on the struct),
//!   so the trait surface is just `Option<Arc<Event>>`.
//! - `auth_chain(seeds)`: used by state resolution. Returns the
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

/// Lookup interface the state machine uses to read events.
pub trait StateProvider {
    /// Look up an event by ID.
    ///
    /// - `Ok(Some(event))` — found. Rejection status is on `Event.rejected`.
    /// - `Ok(None)` — the store genuinely does not know the id. Callers map
    ///   this to their own semantic verdict (`MissingEvent`, `UnknownRoom`,
    ///   `PrevStateNotFound`, …).
    /// - `Err(StateResError::Internal)` — the lookup itself failed (SQL /
    ///   hydration fault). This must propagate as a fault, never be conflated
    ///   with "absent": a transient DB error is not a verdict about the event.
    ///   In-memory impls never return `Err`.
    fn get_event(&self, id: &EventId) -> Result<Option<Arc<Event>>, StateResError>;

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

    /// Resolved **state-after** `event_id`, read directly from the temporal
    /// state-group index (`room_state`) without walking `prev_state_events`.
    ///
    /// - `Ok(Some(map))` — the room is tracked from its create event and the
    ///   interval query reconstructed its state-after. The map is the full
    ///   `StateMap` (every `(type, state_key)` live at `event_id`), including
    ///   `event_id`'s own slot if it is a state event.
    /// - `Ok(None)` — no index for this event (untracked room, or an impl with
    ///   no state-group table). The caller falls back to the recursive
    ///   [`state_at_heads`](crate::state_res::state_at_heads) walk, so this is
    ///   a performance hint, never a correctness signal.
    /// - `Err(StateResError::Internal)` — the lookup itself faulted.
    ///
    /// The default returns `Ok(None)`: in-memory and test providers have no
    /// index, so they always drive the recursive path. Only the SQLite-backed
    /// provider overrides this.
    fn state_after(
        &self,
        _event_id: &EventId,
    ) -> Result<Option<crate::StateMap<OwnedEventId>>, StateResError> {
        Ok(None)
    }
}

/// In-memory `StateProvider`. Public (not test-cfg) — `RoomCore::apply` uses
/// it as the storage-free fallback, and unit tests use it directly. Clone
/// is cheap: events are `Arc`-shared.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStateProvider {
    events: HashMap<OwnedEventId, Arc<Event>>,
}

impl InMemoryStateProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an event into the provider. `auth_events` and `rejected` live
    /// on the `Event` itself (server-computed metadata) — the provider
    /// reads them from there.
    pub fn insert(&mut self, event: Arc<Event>) {
        let id = event.event_id.clone();
        self.events.insert(id, event);
    }
}

impl StateProvider for InMemoryStateProvider {
    fn get_event(&self, id: &EventId) -> Result<Option<Arc<Event>>, StateResError> {
        Ok(self.events.get(id).cloned())
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
            let event = self
                .events
                .get(&id)
                .ok_or_else(|| StateResError::MissingEvent(id.clone()))?;
            for parent in &event.auth_events {
                if !visited.contains(parent) {
                    stack.push(parent.clone());
                }
            }
        }
        Ok(visited)
    }
}
