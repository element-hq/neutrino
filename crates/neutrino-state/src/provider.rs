//! Read-side interface the state machine uses to look up events, plus the
//! in-memory implementation used by tests and (until storage lands) by
//! Phase 6 `apply`.
//!
//! The trait grows as state resolution lands; today it carries:
//! - `get_event(id)`: used by `validate::validate_references` (Phase 1b) and
//!   anywhere downstream that needs the event body / rejection flag.
//! - `auth_event_ids(id)`: used by Phase 4 state resolution to walk the
//!   precomputed auth chain. Under MSC4242 these are calculated server-side
//!   at insert time (by `auth_events::calculate_auth_events`) and stored.

use std::collections::HashMap;
use std::sync::Arc;

use ruma::{EventId, OwnedEventId};

use crate::Event;

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

    /// Precomputed auth_events of `id` — under MSC4242, `auth_events` is not
    /// on the wire; the server calculates it once at insert time via
    /// `auth_events::calculate_auth_events` and stores it alongside the
    /// event. Returns the empty vec if `id` is unknown.
    fn auth_event_ids(&self, id: &EventId) -> Vec<OwnedEventId>;
}

/// In-memory `StateProvider`. Public (not test-cfg) — Phase 6 `apply` uses
/// it as the storage-free fallback, and unit tests use it directly.
#[derive(Debug, Default)]
pub struct InMemoryStateProvider {
    events: HashMap<OwnedEventId, EventInfo>,
    auth_event_ids: HashMap<OwnedEventId, Vec<OwnedEventId>>,
}

impl InMemoryStateProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an event into the provider with its precomputed auth_events.
    ///
    /// `auth_events` is the result of running
    /// `auth_events::calculate_auth_events(event, state_before_event)` at the
    /// time this event was accepted. The provider stores it verbatim — it
    /// does not recompute or validate.
    pub fn insert(&mut self, info: EventInfo, auth_events: Vec<OwnedEventId>) {
        let id = info.event.event_id.clone();
        self.events.insert(id.clone(), info);
        self.auth_event_ids.insert(id, auth_events);
    }
}

impl StateProvider for InMemoryStateProvider {
    fn get_event(&self, id: &EventId) -> Option<EventInfo> {
        self.events.get(id).cloned()
    }

    fn auth_event_ids(&self, id: &EventId) -> Vec<OwnedEventId> {
        self.auth_event_ids.get(id).cloned().unwrap_or_default()
    }
}
