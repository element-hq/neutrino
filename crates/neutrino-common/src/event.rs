//! Canonical parsed-event type, shared across all neutrino crates.
//!
//! `Event` is the single source of truth for a Matrix v12 PDU after format
//! validation has run. It carries:
//!
//! - The pre-parsed scalar fields, for indexing and fast access in state-res
//!   and storage queries.
//! - `prev_events` and `prev_state_events` (MSC4242) extracted from the wire.
//! - `auth_events`, which is **not** on the v12 wire under MSC4242 but is
//!   calculated server-side and co-located here for state resolution.
//! - `event_id`, which is **never** on the v12 wire — it is computed from
//!   the reference hash of `raw` (see PR 2 / `event-id-design.md`).
//! - `raw`, the canonical wire bytes of the event.
//!
//! See `event-id-design.md` §"Co-location pattern" for the five
//! server-computed fields that live on the struct but not in `raw`:
//! `event_id`, `room_id` (create events only), `auth_events`, `rejected`,
//! `soft_failed`.

use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::value::RawValue;

/// Parsed view of a Matrix v12 PDU.
///
/// Constructed by the format-validation pass (`neutrino-state::validate`)
/// and by the server-side event builder (`neutrino-state::event_id`,
/// landing in PR 2). Round-trips through `Event.raw` byte-for-byte.
#[derive(Debug, Clone)]
pub struct Event {
    /// Computed from `reference_hash(raw)`. Never on the wire in v12.
    pub event_id: OwnedEventId,

    /// For `m.room.create`: derived from `event_id` via sigil swap.
    /// For all other events: copied from `raw.room_id`.
    pub room_id: OwnedRoomId,

    pub sender: OwnedUserId,
    pub event_type: String,
    pub state_key: Option<String>,
    pub origin_server_ts: u64,

    /// `RawValue` view into `raw["content"]`.
    pub content: Box<RawValue>,

    /// DAG ancestors.
    pub prev_events: Vec<OwnedEventId>,

    /// MSC4242: state-DAG ancestors.
    pub prev_state_events: Vec<OwnedEventId>,

    /// Calculated server-side via the auth-events selection algorithm.
    /// **Not** on the v12 wire under MSC4242 — co-located here for
    /// state-res traversal.
    pub auth_events: Vec<OwnedEventId>,

    /// Whether this event was rejected by auth-rule evaluation. The state
    /// machine still observes the event's existence (e.g. to skip its
    /// auth-chain in state-res, or reject a child that references it via
    /// `prev_state_events`), but downstream callers typically branch on
    /// the flag. Server-computed, not on the v12 wire.
    pub rejected: bool,

    /// Whether this event was soft-failed: it passed auth against
    /// state-before-event but failed the auth rules against the room's
    /// *current* state (Matrix soft-fail semantics). Such an event is still
    /// persisted and observed by the state machine, but it never becomes a
    /// forward extremity and must not be relayed to clients. Only non-state
    /// events are soft-failed (state events that pass step 3 are accepted
    /// as-is). Server-computed, not on the v12 wire.
    pub soft_failed: bool,

    /// Canonical wire bytes — what gets hashed, federated, and stored.
    pub raw: Box<RawValue>,
}
