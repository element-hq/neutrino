use std::collections::HashMap;

use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::value::RawValue;
use thiserror::Error;

/// Room version supported by this state machine.
///
/// Only v12 is supported (see `CLAUDE.md`). MSC4242 state DAG semantics
/// (`prev_state_events`) are assumed throughout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomVersion {
    V12,
}

/// Resolved room state: one entry per `(event_type, state_key)` pair.
pub type StateMap<V> = HashMap<(String, String), V>;

/// Parsed view of a Matrix event plus the original canonical JSON.
///
/// Constructed by the format-validation pass (phase 1). All optional fields
/// from the wire format that aren't present become `None` / empty.
#[derive(Debug)]
pub struct Event {
    pub event_id: OwnedEventId,
    pub room_id: OwnedRoomId,
    pub sender: OwnedUserId,
    pub event_type: String,
    pub state_key: Option<String>,
    pub origin_server_ts: u64,
    pub content: Box<RawValue>,
    pub prev_events: Vec<OwnedEventId>,
    pub auth_events: Vec<OwnedEventId>,
    /// MSC4242: state-DAG parents of this event.
    pub prev_state_events: Vec<OwnedEventId>,
    /// Original full event JSON, preserved so the event can be re-emitted
    /// byte-for-byte and so redaction can rewrite it later.
    pub raw: Box<RawValue>,
}

/// Errors raised by format validation (phase 1) — wire-format violations that
/// reject the event outright, before any state lookup happens.
///
/// Variants are added by the phase that produces them.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("invalid JSON: {0}")]
    InvalidJson(String),
}

/// Errors raised by the v12 authorization rules (phase 3).
///
/// Variants are added by the phase that produces them.
#[derive(Debug, Error)]
pub enum AuthError {}

/// Top-level error type returned by the state machine.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Auth(#[from] AuthError),
}
