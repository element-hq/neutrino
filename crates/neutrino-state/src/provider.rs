//! Read-side interface the state machine uses to look up events.
//!
//! Implementations are expected to be `Sync` and cheap to query. The trait
//! grows as later phases land; today it carries only what
//! `validate::validate_references` (Phase 1b) needs. Phase 5 (in-memory
//! provider) will extend it for state resolution.

use std::sync::Arc;

use ruma::EventId;

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
}
