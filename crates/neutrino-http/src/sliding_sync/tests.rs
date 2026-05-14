use std::collections::BTreeMap;
use std::sync::Arc;

use ruma::api::client::sync::sync_events::v5::{Request, request};
use ruma::events::StateEventType;
use ruma::{OwnedRoomId, UInt, event_id, room_id, user_id};

use super::mock::{MockStore, make_event};
use super::{SyncError, SyncState, handle};

fn list_with(timeline_limit: u32, required: Vec<(StateEventType, &str)>) -> request::List {
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(99u32))];
    list.room_details.timeline_limit = UInt::from(timeline_limit);
    list.room_details.required_state = required
        .into_iter()
        .map(|(t, k)| (t, k.to_string()))
        .collect();
    list
}

#[tokio::test]
async fn initial_sync_with_no_lists_returns_empty_rooms_and_fresh_pos() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let req = Request::new();
    let resp = handle(&state, user, req).await.unwrap();

    assert!(resp.rooms.is_empty(), "no rooms when no lists or subs");
    assert!(resp.lists.is_empty(), "no list results when no lists");
    let pos: u64 = resp.pos.parse().expect("pos is u64");
    assert!(pos >= 1, "pos advances past 0 on initial sync");
}

#[tokio::test]
async fn initial_sync_with_list_returns_joined_rooms_and_calls_storage() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room_a = room_id!("!room-a:example.org");
    let room_b = room_id!("!room-b:example.org");

    store.join_user(user, room_a);
    store.join_user(user, room_b);

    let ev_a = make_event(
        event_id!("$ev-a-1:example.org"),
        room_a,
        "m.room.message",
        None,
        user,
        1000,
        serde_json::json!({"body": "hello a", "msgtype": "m.text"}),
    );
    let ev_b = make_event(
        event_id!("$ev-b-1:example.org"),
        room_b,
        "m.room.message",
        None,
        user,
        2000,
        serde_json::json!({"body": "hello b", "msgtype": "m.text"}),
    );
    store.add_event(ev_a);
    store.add_event(ev_b);

    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    assert_eq!(resp.rooms.len(), 2, "both joined rooms returned");
    assert!(resp.rooms.contains_key(room_a));
    assert!(resp.rooms.contains_key(room_b));

    let a_result = resp.rooms.get(room_a).unwrap();
    assert_eq!(
        a_result.initial,
        Some(true),
        "initial=true on first emission"
    );
    assert_eq!(a_result.timeline.len(), 1, "one message event in timeline");

    let b_result = resp.rooms.get(room_b).unwrap();
    assert_eq!(b_result.timeline.len(), 1);
    assert_eq!(
        b_result.bump_stamp,
        Some(UInt::from(2000u32)),
        "bump_stamp = origin_server_ts of latest event"
    );

    let list_result = resp.lists.get("all").unwrap();
    assert_eq!(list_result.count, UInt::from(2u32));
}

#[tokio::test]
async fn required_state_filters_current_state() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);

    let create = make_event(
        event_id!("$create:example.org"),
        room,
        "m.room.create",
        Some(""),
        user,
        100,
        serde_json::json!({"creator": user.as_str(), "room_version": "12"}),
    );
    let join = make_event(
        event_id!("$join:example.org"),
        room,
        "m.room.member",
        Some(user.as_str()),
        user,
        200,
        serde_json::json!({"membership": "join"}),
    );
    let name = make_event(
        event_id!("$name:example.org"),
        room,
        "m.room.name",
        Some(""),
        user,
        300,
        serde_json::json!({"name": "Alice's room"}),
    );
    store.add_event(create);
    store.add_event(join);
    store.add_event(name);

    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert(
        "all".to_string(),
        list_with(
            10,
            vec![
                (StateEventType::RoomName, ""),
                (StateEventType::RoomCreate, ""),
            ],
        ),
    );
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    let room_result = resp.rooms.get(room).unwrap();
    let state_types: Vec<String> = room_result
        .required_state
        .iter()
        .map(|raw| raw.get_field::<String>("type").unwrap().unwrap())
        .collect();
    assert_eq!(state_types.len(), 2, "two state events emitted");
    assert!(state_types.contains(&"m.room.name".to_string()));
    assert!(state_types.contains(&"m.room.create".to_string()));
    assert!(
        !state_types.contains(&"m.room.member".to_string()),
        "member state not included since not in required_state"
    );
}

#[tokio::test]
async fn unknown_pos_returns_error() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.pos = Some("9999".to_string());
    let res = handle(&state, user, req).await;
    assert!(matches!(res, Err(SyncError::UnknownPos)));
}

#[tokio::test]
async fn second_sync_with_correct_pos_succeeds() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let resp1 = handle(&state, user, Request::new()).await.unwrap();
    let pos1 = resp1.pos.clone();

    let mut req2 = Request::new();
    req2.pos = Some(pos1);
    let resp2 = handle(&state, user, req2).await;
    assert!(resp2.is_ok(), "valid pos accepted on second call");
}

#[tokio::test]
async fn invited_rooms_are_candidates() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let invited_room: &ruma::RoomId = room_id!("!invited:example.org");
    store.invite_user(user, invited_room);

    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    let owned: OwnedRoomId = invited_room.to_owned();
    assert!(
        resp.rooms.contains_key(&owned),
        "invited room appears in candidates"
    );
}
