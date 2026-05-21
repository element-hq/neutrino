//! Test fixtures for integration tests. Mirrors `src/tests.rs` —
//! integration tests are a separate crate and can't see `pub(crate)` items.
//!
//! Helpers take ruma's typed reference types (`&EventId`, `&RoomId`,
//! `&UserId`) so callers must produce a valid ID at compile time via the
//! `event_id!` / `room_id!` / `user_id!` macros.
//!
//! Each integration-test binary that includes this module via `mod common`
//! only links the helpers it actually calls, so unused warnings fire for the
//! ones it doesn't. The allow below covers that — these helpers are shared
//! across multiple test files.

#![allow(dead_code)]

use lazy_static::lazy_static;
use neutrino_store::StoredEvent;
use neutrino_store_sqlite::SqliteStore;
use ruma::{EventId, RoomId, UserId, event_id, room_id, user_id};
use serde_json::{Value, json, value::RawValue};

// Canonical IDs.

lazy_static! {
    pub static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
    pub static ref BOB_ROOM_ID: &'static RoomId = room_id!("!r2:example.com");
    pub static ref ALICE_USER_ID: &'static UserId = user_id!("@alice:example.com");
    pub static ref BOB_USER_ID: &'static UserId = user_id!("@bob:example.com");
    pub static ref CREATE_EVENT_ID: &'static EventId = event_id!("$create:example.com");
}

// Store fixtures.

pub async fn store() -> SqliteStore {
    SqliteStore::open_in_memory().await.unwrap()
}

/// Open a fresh in-memory store and create [`ALICE_ROOM_ID`] with a single
/// create event ([`CREATE_EVENT_ID`]) owned by [`ALICE_USER_ID`].
pub async fn store_with_room() -> SqliteStore {
    use neutrino_store::RoomStore;
    let s = store().await;
    s.create_room(
        &create_event(*CREATE_EVENT_ID, *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();
    s
}

pub fn make_event(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    event_type: &str,
    state_key: Option<&str>,
    content: Value,
) -> StoredEvent {
    let json_val = json!({
        "event_id": event_id.as_str(),
        "room_id": room_id.as_str(),
        "sender": sender.as_str(),
        "type": event_type,
        "state_key": state_key,
        "content": content,
        "origin_server_ts": 0,
        "prev_events": [],
        "prev_state_events": [],
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    let json = RawValue::from_string(json_str).unwrap();

    StoredEvent {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: event_type.to_owned(),
        state_key: state_key.map(str::to_owned),
        sender: sender.to_owned(),
        origin_server_ts: 0,
        json,
    }
}

pub fn create_event(event_id: &EventId, room_id: &RoomId, sender: &UserId) -> StoredEvent {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.create",
        Some(""),
        json!({"creator": sender.as_str(), "room_version": "12"}),
    )
}

pub fn member_join(event_id: &EventId, room_id: &RoomId, user_id: &UserId) -> StoredEvent {
    make_event(
        event_id,
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "join"}),
    )
}

pub fn member_leave(event_id: &EventId, room_id: &RoomId, user_id: &UserId) -> StoredEvent {
    make_event(
        event_id,
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "leave"}),
    )
}

pub fn name_event(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    name: &str,
) -> StoredEvent {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.name",
        Some(""),
        json!({"name": name}),
    )
}

pub fn message(event_id: &EventId, room_id: &RoomId, sender: &UserId, body: &str) -> StoredEvent {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.message",
        None,
        json!({"body": body, "msgtype": "m.text"}),
    )
}
