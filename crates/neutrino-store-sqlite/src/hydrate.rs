//! Row → `StoredEvent` / `StoredPdu` hydration.
//!
//! Single source of truth for the column shape expected by any SELECT
//! returning events. Per design doc §3: keeping this in one place means
//! the SELECTs in `store/events.rs`, `store/state.rs`, `store/dag.rs`, and
//! `store/outbox.rs` all agree on what they're projecting, and a schema
//! change touches one file instead of seven.
//!
//! Skeleton: the actual hydration is filled in during tasks #4 / #5. The
//! constant below names the canonical column list so call sites can use it
//! directly via `format!`/string concat once implementation begins.

/// Canonical event-row column list. Order matters — `stored_event_from_row`
/// (once implemented) will index by position to avoid per-row HashMap
/// allocations.
#[allow(dead_code)]
pub(crate) const EVENT_COLUMNS: &str =
    "event_id, room_id, event_type, state_key, sender, origin_server_ts, json";
