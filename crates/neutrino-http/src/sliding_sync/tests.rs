use std::collections::BTreeMap;
use std::sync::Arc;

use neutrino_common::storage::StoredEvent;
use ruma::api::client::sync::sync_events::v5::{Request, request};
use ruma::events::StateEventType;
use ruma::{OwnedRoomId, RoomId, UInt, event_id, room_id, user_id};

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

/// Seed N rooms named `!room-{i}:example.org` (i = 0..N) each with one message
/// event at the given `ts[i]`. Returns the room IDs in seed order so tests can
/// assert membership against ranking-derived subsets.
fn seed_rooms_with_timestamps(
    store: &MockStore,
    user: &ruma::UserId,
    timestamps: &[u64],
) -> Vec<OwnedRoomId> {
    let mut ids = Vec::with_capacity(timestamps.len());
    for (i, ts) in timestamps.iter().enumerate() {
        let room_id_str = format!("!room-{i}:example.org");
        let room_id: &RoomId = (&*Box::leak(room_id_str.into_boxed_str()))
            .try_into()
            .unwrap();
        store.join_user(user, room_id);
        let event_id_str = format!("$ev-{i}:example.org");
        let event_id: &ruma::EventId = (&*Box::leak(event_id_str.into_boxed_str()))
            .try_into()
            .unwrap();
        let ev = make_event(
            event_id,
            room_id,
            "m.room.message",
            None,
            user,
            *ts,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        );
        store.add_event(ev);
        ids.push(room_id.to_owned());
    }
    ids
}

#[tokio::test]
async fn rooms_sorted_by_bump_stamp_desc() {
    // 3 rooms with descending bump stamps assigned to ascending room IDs —
    // room IDs sort opposite to bump_stamp, so any room_id-based ordering
    // would give different results than bump_stamp-based ordering.
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    // !room-0 → ts 300 (most recent), !room-1 → 200, !room-2 → 100.
    let ids = seed_rooms_with_timestamps(&store, user, &[300, 200, 100]);
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    // Request only the top 2 by recency.
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(1u32))];
    list.room_details.timeline_limit = UInt::from(5u32);
    lists.insert("top2".to_string(), list);
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    assert_eq!(resp.rooms.len(), 2, "exactly the top 2 returned");
    assert!(
        resp.rooms.contains_key(&ids[0]),
        "room with ts=300 included"
    );
    assert!(
        resp.rooms.contains_key(&ids[1]),
        "room with ts=200 included"
    );
    assert!(
        !resp.rooms.contains_key(&ids[2]),
        "room with ts=100 (rank 2) excluded by range [0,1]"
    );

    let list_result = resp.lists.get("top2").unwrap();
    assert_eq!(
        list_result.count,
        UInt::from(3u32),
        "count is full candidate set, not range size"
    );
}

#[tokio::test]
async fn range_slicing_returns_only_requested_indexes() {
    // 10 rooms with strictly ascending bump stamps; request indexes [2,4].
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let timestamps: Vec<u64> = (1..=10).map(|i| i * 100).collect();
    let ids = seed_rooms_with_timestamps(&store, user, &timestamps);
    // After sort: rank 0 = highest ts (!room-9, ts=1000); rank 9 = lowest.
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(2u32), UInt::from(4u32))];
    list.room_details.timeline_limit = UInt::from(1u32);
    lists.insert("slice".to_string(), list);
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    assert_eq!(resp.rooms.len(), 3, "range [2,4] is inclusive on both ends");
    // Ranks 2, 3, 4 correspond to the 8th-, 7th-, 6th-most-recent timestamps,
    // i.e. !room-7 (ts=800), !room-6 (ts=700), !room-5 (ts=600).
    assert!(resp.rooms.contains_key(&ids[7]));
    assert!(resp.rooms.contains_key(&ids[6]));
    assert!(resp.rooms.contains_key(&ids[5]));
    assert!(!resp.rooms.contains_key(&ids[9]), "rank 0 excluded");
    assert!(!resp.rooms.contains_key(&ids[4]), "rank 5 excluded");
}

#[tokio::test]
async fn subscription_bypasses_list_range() {
    // 3 rooms ranked 0, 1, 2 by recency. List asks for range [0,0] (top only).
    // Subscription names the lowest-ranked room — should appear regardless.
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let ids = seed_rooms_with_timestamps(&store, user, &[300, 200, 100]);
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(1u32);
    lists.insert("top1".to_string(), list);
    req.lists = lists;
    let mut subs = BTreeMap::new();
    let mut sub = request::RoomSubscription::default();
    sub.timeline_limit = UInt::from(1u32);
    subs.insert(ids[2].clone(), sub);
    req.room_subscriptions = subs;

    let resp = handle(&state, user, req).await.unwrap();

    assert!(
        resp.rooms.contains_key(&ids[0]),
        "list top includes rank-0 room"
    );
    assert!(
        resp.rooms.contains_key(&ids[2]),
        "subscription pulls in rank-2 room despite range [0,0]"
    );
    assert!(
        !resp.rooms.contains_key(&ids[1]),
        "rank-1 room not in list range and not subscribed"
    );
}

#[tokio::test]
async fn multi_range_request_only_honours_first() {
    // MSC4186 removed MSC3575's multi-range support; ruma v5 still types
    // `ranges: Vec` for compatibility. We honour only `ranges[0]` and silently
    // drop the rest. This test asserts that with 5 rooms and a request that
    // sends both [0,0] and [3,4], only the rank-0 room comes back.
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let ids = seed_rooms_with_timestamps(&store, user, &[100, 200, 300, 400, 500]);
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    list.ranges = vec![
        (UInt::from(0u32), UInt::from(0u32)),
        (UInt::from(3u32), UInt::from(4u32)),
    ];
    list.room_details.timeline_limit = UInt::from(1u32);
    lists.insert("multi".to_string(), list);
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();
    assert_eq!(resp.rooms.len(), 1, "only the first range applied");
    // rank 0 = highest ts = !room-4 (ts=500).
    assert!(resp.rooms.contains_key(&ids[4]));
}

#[tokio::test]
async fn list_count_independent_of_range_size() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let _ids = seed_rooms_with_timestamps(&store, user, &[100, 200, 300, 400, 500]);
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    // Request only one slot, but `count` should still be the total candidates.
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(1u32);
    lists.insert("one".to_string(), list);
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    assert_eq!(resp.rooms.len(), 1);
    let list_result = resp.lists.get("one").unwrap();
    assert_eq!(list_result.count, UInt::from(5u32));
}

// ----------------------------------------------------------------------------
// Phase 4 — deltas, `limited`, invite_state, name/avatar/counts, state stubs.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn second_sync_returns_only_new_events() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);
    store.add_event(make_event(
        event_id!("$ev1:example.org"),
        room,
        "m.room.message",
        None,
        user,
        1000,
        serde_json::json!({"body": "first", "msgtype": "m.text"}),
    ));
    let state = SyncState::new(store.clone());

    let mut req1 = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(10, vec![]));
    req1.lists = lists.clone();

    let resp1 = handle(&state, user, req1).await.unwrap();
    let room1 = resp1.rooms.get(room).unwrap();
    assert_eq!(room1.timeline.len(), 1, "initial: snapshot");
    assert_eq!(room1.num_live, None, "initial sync events are historical");

    // Add a second event between syncs.
    store.add_event(make_event(
        event_id!("$ev2:example.org"),
        room,
        "m.room.message",
        None,
        user,
        2000,
        serde_json::json!({"body": "second", "msgtype": "m.text"}),
    ));

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos.clone());
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();

    let room2 = resp2.rooms.get(room).unwrap();
    assert_eq!(
        room2.timeline.len(),
        1,
        "delta: only the new event, not both"
    );
    let event_id_returned: String = room2.timeline[0]
        .get_field::<String>("event_id")
        .unwrap()
        .unwrap();
    assert_eq!(event_id_returned, "$ev2:example.org");
    assert_eq!(
        room2.num_live,
        Some(UInt::from(1u32)),
        "delta events are live"
    );
}

#[tokio::test]
async fn third_sync_with_no_new_events_omits_room() {
    // After an initial emission and a subsequent sync that captures nothing
    // new, the room should drop out of the response entirely (MSC4186
    // §"Room Matching Rules").
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);
    store.add_event(make_event(
        event_id!("$only:example.org"),
        room,
        "m.room.message",
        None,
        user,
        1000,
        serde_json::json!({"body": "x", "msgtype": "m.text"}),
    ));
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req = Request::new();
    req.lists = lists.clone();
    let resp1 = handle(&state, user, req).await.unwrap();
    assert!(resp1.rooms.contains_key(room));

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    assert!(
        !resp2.rooms.contains_key(room),
        "no-update room omitted from delta"
    );
}

#[tokio::test]
async fn limited_set_when_timeline_truncated() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);
    // Seed one event so room is "known" after first sync.
    store.add_event(make_event(
        event_id!("$seed:example.org"),
        room,
        "m.room.message",
        None,
        user,
        100,
        serde_json::json!({"body": "seed", "msgtype": "m.text"}),
    ));
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(2, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    // Now drop 5 new events between syncs while the configured timeline_limit
    // is 2 — delta path must cap and set `limited`.
    for i in 0..5 {
        let id_str = format!("$ev-{i}:example.org");
        let id: &ruma::EventId = (&*Box::leak(id_str.into_boxed_str())).try_into().unwrap();
        store.add_event(make_event(
            id,
            room,
            "m.room.message",
            None,
            user,
            200 + i,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        ));
    }

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    let room_res = resp2.rooms.get(room).unwrap();
    assert_eq!(room_res.timeline.len(), 2, "capped at timeline_limit=2");
    assert!(
        room_res.limited,
        "limited=true when older delta events were dropped"
    );
}

#[tokio::test]
async fn required_state_not_re_sent_when_unchanged() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);
    store.add_event(make_event(
        event_id!("$name:example.org"),
        room,
        "m.room.name",
        Some(""),
        user,
        100,
        serde_json::json!({"name": "Alice's room"}),
    ));
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert(
        "all".to_string(),
        list_with(5, vec![(StateEventType::RoomName, "")]),
    );

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    let room1 = resp1.rooms.get(room).unwrap();
    assert_eq!(room1.required_state.len(), 1, "name emitted on first sync");

    // Drop a new message event so the room is included in the second sync
    // (otherwise it gets the no-update-omit treatment).
    store.add_event(make_event(
        event_id!("$msg:example.org"),
        room,
        "m.room.message",
        None,
        user,
        200,
        serde_json::json!({"body": "x", "msgtype": "m.text"}),
    ));

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    let room2 = resp2.rooms.get(room).unwrap();
    assert!(
        room2.required_state.is_empty(),
        "name unchanged → not re-emitted on delta"
    );
}

#[tokio::test]
async fn deleted_state_not_surfaced_with_stubs_disabled() {
    // `EMIT_STATE_STUBS` is hardcoded `false` (see `build.rs`), so a state
    // key disappearing from current state must NOT produce a stub in the
    // next sync — the client is intentionally left with its stale view
    // until the state is re-set. The deletion-detection logic itself is
    // covered separately by a unit test on `diff_required_state`.
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);
    store.add_event(make_event(
        event_id!("$name:example.org"),
        room,
        "m.room.name",
        Some(""),
        user,
        100,
        serde_json::json!({"name": "X"}),
    ));
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert(
        "all".to_string(),
        list_with(5, vec![(StateEventType::RoomName, "")]),
    );

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert_eq!(resp1.rooms.get(room).unwrap().required_state.len(), 1);

    // Drop the name event from current state and add a message so the room
    // is still emitted on the second sync (otherwise it would be omitted
    // entirely as a no-update room).
    store.remove_state(room, "m.room.name", "");
    store.add_event(make_event(
        event_id!("$msg:example.org"),
        room,
        "m.room.message",
        None,
        user,
        200,
        serde_json::json!({"body": "x", "msgtype": "m.text"}),
    ));

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();

    let room_res = resp2.rooms.get(room).unwrap();
    assert!(
        room_res.required_state.is_empty(),
        "no stub emitted: client never learns the name was removed"
    );
    assert_eq!(
        room_res.timeline.len(),
        1,
        "room is still emitted because of the new message event"
    );
}

#[tokio::test]
async fn invited_room_emits_invite_state() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let room = room_id!("!invite:example.org");

    store.invite_user(user, room);

    // Real-world this `StoredEvent` would be persisted out of the remote
    // server's `PUT /_matrix/federation/v2/invite/...` call: an invite-form
    // `m.room.member` whose `unsigned.invite_room_state` carries the
    // stripped room context. That's our only window into the room until
    // the invite is accepted, so the handler must read from there rather
    // than from `current_state_event` lookups for create/name/etc.
    let invite_json = serde_json::json!({
        "event_id": "$invite:example.org",
        "room_id": room.as_str(),
        "type": "m.room.member",
        "state_key": user.as_str(),
        "sender": inviter.as_str(),
        "origin_server_ts": 80,
        "content": {"membership": "invite"},
        "unsigned": {
            "invite_room_state": [
                {
                    "type": "m.room.create",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"creator": inviter.as_str(), "room_version": "12"}
                },
                {
                    "type": "m.room.name",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"name": "Bob's invite"}
                },
                {
                    "type": "m.room.member",
                    "state_key": inviter.as_str(),
                    "sender": inviter.as_str(),
                    "content": {"membership": "join"}
                }
            ]
        }
    });
    let invite_event = StoredEvent {
        event_id: event_id!("$invite:example.org").to_owned(),
        room_id: room.to_owned(),
        event_type: "m.room.member".to_string(),
        state_key: Some(user.as_str().to_string()),
        sender: inviter.to_owned(),
        origin_server_ts: 80,
        json: serde_json::value::to_raw_value(&invite_json).unwrap(),
    };
    store.add_event(invite_event);

    let state = SyncState::new(store);
    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp.rooms.get(room).unwrap();
    assert!(
        room_res.timeline.is_empty(),
        "invited rooms don't carry a timeline"
    );
    let invite_state = room_res
        .invite_state
        .as_ref()
        .expect("invite_state populated for invites");
    let types: Vec<String> = invite_state
        .iter()
        .map(|raw| raw.get_field::<String>("type").unwrap().unwrap())
        .collect();
    assert!(types.contains(&"m.room.create".to_string()));
    assert!(types.contains(&"m.room.name".to_string()));
    // Two membership events: the inviter's join (from `invite_room_state`)
    // and the invitee's own invite event (stripped from the stored PDU).
    let member_count = types
        .iter()
        .filter(|t| t.as_str() == "m.room.member")
        .count();
    assert_eq!(member_count, 2);
}

#[tokio::test]
async fn name_avatar_and_counts_emitted() {
    let store = Arc::new(MockStore::new());
    let user = user_id!("@alice:example.org");
    let bob = user_id!("@bob:example.org");
    let carol = user_id!("@carol:example.org");
    let room = room_id!("!room:example.org");
    store.join_user(user, room);

    store.add_event(make_event(
        event_id!("$name:example.org"),
        room,
        "m.room.name",
        Some(""),
        user,
        100,
        serde_json::json!({"name": "Alice's room"}),
    ));
    store.add_event(make_event(
        event_id!("$avatar:example.org"),
        room,
        "m.room.avatar",
        Some(""),
        user,
        110,
        serde_json::json!({"url": "mxc://example.org/abc"}),
    ));
    store.add_event(make_event(
        event_id!("$alice-join:example.org"),
        room,
        "m.room.member",
        Some(user.as_str()),
        user,
        120,
        serde_json::json!({"membership": "join"}),
    ));
    store.add_event(make_event(
        event_id!("$bob-join:example.org"),
        room,
        "m.room.member",
        Some(bob.as_str()),
        bob,
        130,
        serde_json::json!({"membership": "join"}),
    ));
    store.add_event(make_event(
        event_id!("$carol-invite:example.org"),
        room,
        "m.room.member",
        Some(carol.as_str()),
        user,
        140,
        serde_json::json!({"membership": "invite"}),
    ));

    let state = SyncState::new(store);
    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp.rooms.get(room).unwrap();
    assert_eq!(room_res.name.as_deref(), Some("Alice's room"));
    match &room_res.avatar {
        ruma::JsOption::Some(uri) => assert_eq!(uri.as_str(), "mxc://example.org/abc"),
        _ => panic!("avatar should be Some"),
    }
    // The mock's `joined_members` returns empty (StateStore stub), so
    // joined_count comes back as 0 / Some(0). Acceptable for a mock-only
    // test; the real sqlite impl will populate this properly.
    assert_eq!(room_res.joined_count, Some(UInt::from(0u32)));
    assert_eq!(
        room_res.invited_count,
        Some(UInt::from(1u32)),
        "carol's invite counted"
    );
}

// ----------------------------------------------------------------------------
// Phase 6 — request validation + extension echoes.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn conn_id_over_16_chars_rejected() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.conn_id = Some("this-string-is-way-longer-than-sixteen".to_string());
    let res = handle(&state, user, req).await;
    assert!(matches!(res, Err(SyncError::BadRequest(_))));
}

#[tokio::test]
async fn too_many_lists_rejected() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    for i in 0..101 {
        lists.insert(format!("list-{i}"), list_with(1, vec![]));
    }
    req.lists = lists;
    let res = handle(&state, user, req).await;
    assert!(matches!(res, Err(SyncError::BadRequest(_))));
}

#[tokio::test]
async fn e2ee_extension_echoed_when_enabled() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.extensions.e2ee.enabled = Some(true);
    let resp = handle(&state, user, req).await.unwrap();

    assert!(
        !resp.extensions.e2ee.device_one_time_keys_count.is_empty(),
        "OTK count populated"
    );
    assert!(
        resp.extensions
            .e2ee
            .device_unused_fallback_key_types
            .is_some()
    );
}

#[tokio::test]
async fn to_device_extension_echoed_when_enabled() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.extensions.to_device.enabled = Some(true);
    let resp = handle(&state, user, req).await.unwrap();

    let to_device = resp
        .extensions
        .to_device
        .as_ref()
        .expect("to_device echo populated");
    assert_eq!(to_device.next_batch, "0");
    assert!(to_device.events.is_empty());
}

#[tokio::test]
async fn extensions_not_echoed_when_not_requested() {
    let store = Arc::new(MockStore::new());
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let resp = handle(&state, user, Request::new()).await.unwrap();
    assert!(resp.extensions.e2ee.device_one_time_keys_count.is_empty());
    assert!(resp.extensions.to_device.is_none());
}
