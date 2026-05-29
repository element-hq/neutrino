//! Test fixtures for integration tests. Mirrors `src/tests.rs` —
//! integration tests are a separate crate and can't see `pub(crate)` items.
//!
//! Helpers compute the event_id via [`compute_event_id`] so events round-trip
//! through `EventStore::persist_event`'s debug-build hash check (PR 2 / B4).
//! Callers must capture the returned event's `event_id` if they need it for
//! assertions — there is no caller-supplied id parameter.
//!
//! Each integration-test binary that includes this module via `mod common`
//! only links the helpers it actually calls, so unused warnings fire for the
//! ones it doesn't. The allow below covers that — these helpers are shared
//! across multiple test files.

#![allow(dead_code)]

use lazy_static::lazy_static;
use neutrino_common::Event;
use neutrino_common::ROOM_VERSION_ID;
use neutrino_common::event_id::compute_event_id;
use neutrino_store_sqlite::SqliteStore;
use ruma::{EventId, OwnedEventId, RoomId, UserId, room_id, user_id};
use serde_json::{Value, json, value::RawValue};

// Canonical IDs.

lazy_static! {
    pub static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
    pub static ref BOB_ROOM_ID: &'static RoomId = room_id!("!r2:example.com");
    pub static ref ALICE_USER_ID: &'static UserId = user_id!("@alice:example.com");
    pub static ref BOB_USER_ID: &'static UserId = user_id!("@bob:example.com");
}

// Store fixtures.

pub async fn store() -> SqliteStore {
    SqliteStore::open_in_memory().await.unwrap()
}

/// Open a fresh in-memory store and create [`ALICE_ROOM_ID`] with a single
/// create event owned by [`ALICE_USER_ID`]. Returns the store paired with
/// the create event so tests can reference its computed `event_id`.
pub async fn store_with_room_and_create() -> (SqliteStore, Event) {
    use neutrino_store::RoomStore;
    let s = store().await;
    let create = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
    s.create_room(&create, &[]).await.unwrap();
    (s, create)
}

/// Build a v12-shaped `Event` and compute its event_id via the reference-hash
/// pipeline. All callers go through this — the only caller-controlled inputs
/// are the structural fields; `event_id` is derived from `raw`.
///
/// `origin_server_ts` is exposed so tests that need distinct event_ids for
/// otherwise-identical events can disambiguate via the timestamp.
#[allow(clippy::too_many_arguments)]
pub fn make_event(
    room_id: &RoomId,
    sender: &UserId,
    event_type: &str,
    state_key: Option<&str>,
    content: Value,
    origin_server_ts: u64,
    prev_events: &[&EventId],
    prev_state_events: &[&EventId],
) -> Event {
    let prev_event_strs: Vec<&str> = prev_events.iter().map(|e| e.as_str()).collect();
    let prev_state_strs: Vec<&str> = prev_state_events.iter().map(|e| e.as_str()).collect();

    let mut obj = serde_json::Map::new();
    obj.insert(
        "room_id".to_owned(),
        Value::String(room_id.as_str().to_owned()),
    );
    obj.insert(
        "sender".to_owned(),
        Value::String(sender.as_str().to_owned()),
    );
    obj.insert("type".to_owned(), Value::String(event_type.to_owned()));
    if let Some(sk) = state_key {
        obj.insert("state_key".to_owned(), Value::String(sk.to_owned()));
    }
    obj.insert("content".to_owned(), content.clone());
    obj.insert("origin_server_ts".to_owned(), Value::from(origin_server_ts));
    obj.insert(
        "prev_events".to_owned(),
        Value::Array(
            prev_event_strs
                .iter()
                .map(|s| Value::String((*s).to_owned()))
                .collect(),
        ),
    );
    obj.insert(
        "prev_state_events".to_owned(),
        Value::Array(
            prev_state_strs
                .iter()
                .map(|s| Value::String((*s).to_owned()))
                .collect(),
        ),
    );

    let json_str = serde_json::to_string(&Value::Object(obj)).unwrap();
    let raw = RawValue::from_string(json_str).unwrap();
    let event_id = compute_event_id(&raw).expect("test fixture must compute event_id");
    let content_raw = serde_json::value::to_raw_value(&content).unwrap();

    let prev_events_owned: Vec<OwnedEventId> =
        prev_events.iter().map(|e| (*e).to_owned()).collect();
    let prev_state_owned: Vec<OwnedEventId> =
        prev_state_events.iter().map(|e| (*e).to_owned()).collect();

    Event {
        event_id,
        room_id: room_id.to_owned(),
        event_type: event_type.to_owned(),
        state_key: state_key.map(str::to_owned),
        sender: sender.to_owned(),
        origin_server_ts,
        content: content_raw,
        prev_events: prev_events_owned,
        prev_state_events: prev_state_owned,
        auth_events: Vec::new(),
        rejected: false,
        raw,
    }
}

pub fn create_event(room_id: &RoomId, sender: &UserId) -> Event {
    make_event(
        room_id,
        sender,
        "m.room.create",
        Some(""),
        json!({"creator": sender.as_str(), "room_version": ROOM_VERSION_ID}),
        0,
        &[],
        &[],
    )
}

pub fn member_join(room_id: &RoomId, user_id: &UserId) -> Event {
    make_event(
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "join"}),
        0,
        &[],
        &[],
    )
}

pub fn member_leave(room_id: &RoomId, user_id: &UserId) -> Event {
    make_event(
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "leave"}),
        0,
        &[],
        &[],
    )
}

pub fn name_event(room_id: &RoomId, sender: &UserId, name: &str) -> Event {
    make_event(
        room_id,
        sender,
        "m.room.name",
        Some(""),
        json!({"name": name}),
        0,
        &[],
        &[],
    )
}

/// Message event. `ts` disambiguates otherwise-identical messages (same
/// room, sender, body) so each gets a unique event_id under the reference
/// hash. Tests that loop and want distinct ids pass `i as u64`.
pub fn message_with_ts(room_id: &RoomId, sender: &UserId, body: &str, ts: u64) -> Event {
    make_event(
        room_id,
        sender,
        "m.room.message",
        None,
        json!({"body": body, "msgtype": "m.text"}),
        ts,
        &[],
        &[],
    )
}

/// Shorthand for `message_with_ts(.., 0)`. Callers that need multiple
/// distinct messages in the same test must use `message_with_ts` directly.
pub fn message(room_id: &RoomId, sender: &UserId, body: &str) -> Event {
    message_with_ts(room_id, sender, body, 0)
}
