//! Test fixtures for integration tests. Mirrors `src/tests.rs` —
//! integration tests are a separate crate and can't see `pub(crate)` items.
//!
//! Helpers take ruma's typed reference types (`&EventId`, `&RoomId`,
//! `&UserId`) so callers must produce a valid ID at compile time via the
//! `event_id!` / `room_id!` / `user_id!` macros.

#![allow(dead_code)] // not every integration test uses every helper

use neutrino_store::StoredEvent;
use neutrino_store_sqlite::SqliteStore;
use ruma::{EventId, RoomId, UserId};
use serde_json::{Value, json, value::RawValue};

pub async fn store() -> SqliteStore {
    SqliteStore::open_in_memory().await.unwrap()
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
