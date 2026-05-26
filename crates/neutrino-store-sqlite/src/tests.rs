//! Shared test fixtures for in-crate unit tests.
//!
//! Not every fixture is used by every submodule's tests, so individual
//! helpers can look "unused" to dead-code analysis. The allow below covers
//! that — these are shared helpers across multiple `mod tests` blocks.

#![allow(dead_code)]

use deadpool_sqlite::rusqlite::params;
use lazy_static::lazy_static;
use neutrino_common::Event;
use neutrino_common::ROOM_VERSION_ID;
use ruma::{EventId, RoomId, UserId, event_id, room_id, user_id};
use serde_json::{Value, json, value::RawValue};

use crate::SqliteStore;
use crate::error::Error;
use crate::row::EventRow;

// Canonical IDs.

lazy_static! {
    pub(crate) static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
    pub(crate) static ref BOB_ROOM_ID: &'static RoomId = room_id!("!r2:example.com");
    pub(crate) static ref ALICE_USER_ID: &'static UserId = user_id!("@alice:example.com");
    pub(crate) static ref BOB_USER_ID: &'static UserId = user_id!("@bob:example.com");
    pub(crate) static ref CREATE_EVENT_ID: &'static EventId = event_id!("$create:example.com");
}

// Store fixtures.

pub(crate) async fn store() -> SqliteStore {
    SqliteStore::open_in_memory().await.unwrap()
}

/// Open a fresh in-memory store and create [`ALICE_ROOM_ID`] with a single
/// create event ([`CREATE_EVENT_ID`]) owned by [`ALICE_USER_ID`]. Convenience
/// for tests that need a room to exist but don't care about its contents.
pub(crate) async fn store_with_room() -> SqliteStore {
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

/// Build a `Event` from a JSON value supplied by the caller. The
/// `prev_events` / `prev_state_events` arrays default to empty.
pub(crate) fn make_event(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    event_type: &str,
    state_key: Option<&str>,
    content: Value,
) -> Event {
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
    let raw = RawValue::from_string(json_str).unwrap();
    let content_raw = serde_json::value::to_raw_value(&content).unwrap();

    Event {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: event_type.to_owned(),
        state_key: state_key.map(str::to_owned),
        sender: sender.to_owned(),
        origin_server_ts: 0,
        content: content_raw,
        prev_events: Vec::new(),
        prev_state_events: Vec::new(),
        auth_events: Vec::new(),
        raw,
    }
}

/// Construct a `Event` with caller-supplied raw JSON (for tests that
/// need to exercise the deserializer error path).
pub(crate) fn make_event_with_raw_json(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    event_type: &str,
    state_key: Option<&str>,
    raw_json: &str,
) -> Event {
    let raw = RawValue::from_string(raw_json.to_owned()).unwrap();
    // The fixture used by the deserializer-error tests intentionally
    // passes malformed JSON; fall back to `{}` for `content` so the
    // struct can be built and the storage layer's own validation
    // exercises the parse failure path.
    let parsed: serde_json::Value =
        serde_json::from_str(raw_json).unwrap_or(serde_json::Value::Object(Default::default()));
    let content_value = parsed
        .get("content")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));
    let content = serde_json::value::to_raw_value(&content_value).unwrap();
    Event {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: event_type.to_owned(),
        state_key: state_key.map(str::to_owned),
        sender: sender.to_owned(),
        origin_server_ts: 0,
        content,
        prev_events: Vec::new(),
        prev_state_events: Vec::new(),
        auth_events: Vec::new(),
        raw,
    }
}

pub(crate) fn create_event(event_id: &EventId, room_id: &RoomId, sender: &UserId) -> Event {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.create",
        Some(""),
        json!({"creator": sender.as_str(), "room_version": ROOM_VERSION_ID}),
    )
}

pub(crate) fn member_join(event_id: &EventId, room_id: &RoomId, user_id: &UserId) -> Event {
    make_event(
        event_id,
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "join"}),
    )
}

pub(crate) fn member_leave(event_id: &EventId, room_id: &RoomId, user_id: &UserId) -> Event {
    make_event(
        event_id,
        room_id,
        user_id,
        "m.room.member",
        Some(user_id.as_str()),
        json!({"membership": "leave"}),
    )
}

/// Generic membership helper with caller-controlled sender/target —
/// invites and bans want `sender != target` (the actor and the affected
/// user are different); knocks always have `sender == target`.
pub(crate) fn member_event(
    event_id: &EventId,
    room_id: &RoomId,
    target: &UserId,
    sender: &UserId,
    membership: &str,
) -> Event {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.member",
        Some(target.as_str()),
        json!({"membership": membership}),
    )
}

pub(crate) fn name_event(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    name: &str,
) -> Event {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.name",
        Some(""),
        json!({"name": name}),
    )
}

pub(crate) fn message(event_id: &EventId, room_id: &RoomId, sender: &UserId, body: &str) -> Event {
    make_event(
        event_id,
        room_id,
        sender,
        "m.room.message",
        None,
        json!({"body": body, "msgtype": "m.text"}),
    )
}

/// Insert a room + its `m.room.create` event, bypassing the `RoomStore`
/// trait's JSON validation. Useful when a test wants the room (FK target)
/// to exist but doesn't want — or doesn't have access to — a working
/// `RoomStore` impl.
///
/// The create event goes through [`EventRow::write_into_tx`], the same
/// path `RoomStore::create_room` uses, so observable surfaces consistent
/// with a real `create_room` call include `events_after`, `room_messages`,
/// `event_edges`, and `current_state` (the create event lands as the
/// `(room, "m.room.create", "")` state row). The watch is NOT advanced —
/// tests subscribe-after-setup, and never rely on the watch for the
/// create event.
pub(crate) async fn setup_room(
    s: &SqliteStore,
    room_id: &RoomId,
    user_id: &UserId,
    create_event_id: &EventId,
) {
    let ce = create_event(create_event_id, room_id, user_id);
    let row = EventRow::from(&ce).to_owned();
    let room_id = room_id.to_owned();

    s.run_write(move |conn| -> Result<(), Error> {
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO rooms (room_id, room_version) VALUES (?, ?)",
            params![room_id.as_str(), ROOM_VERSION_ID],
        )?;
        row.write_into_tx(&tx)?;
        tx.commit()?;
        Ok(())
    })
    .await
    .unwrap();
}

/// Message event with caller-supplied `prev_events`. Used by the DAG
/// tests to build specific chain / branch / cycle shapes.
pub(crate) fn message_with_prev(
    event_id: &EventId,
    room_id: &RoomId,
    sender: &UserId,
    body: &str,
    prev_events: &[&EventId],
) -> Event {
    let prev_event_strs: Vec<&str> = prev_events.iter().map(|e| e.as_str()).collect();
    let json_val = json!({
        "event_id": event_id.as_str(),
        "room_id": room_id.as_str(),
        "sender": sender.as_str(),
        "type": "m.room.message",
        "state_key": Option::<String>::None,
        "content": {"body": body, "msgtype": "m.text"},
        "origin_server_ts": 0,
        "prev_events": prev_event_strs,
        "prev_state_events": [],
    });
    let json_str = serde_json::to_string(&json_val).unwrap();
    let raw = RawValue::from_string(json_str).unwrap();
    let content_raw =
        serde_json::value::to_raw_value(&json!({"body": body, "msgtype": "m.text"})).unwrap();
    let prev = prev_events
        .iter()
        .map(|e| (*e).to_owned())
        .collect::<Vec<_>>();

    Event {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: "m.room.message".to_owned(),
        state_key: None,
        sender: sender.to_owned(),
        origin_server_ts: 0,
        content: content_raw,
        prev_events: prev,
        prev_state_events: Vec::new(),
        auth_events: Vec::new(),
        raw,
    }
}
