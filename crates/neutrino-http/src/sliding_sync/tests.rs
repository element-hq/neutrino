use std::collections::BTreeMap;
use std::sync::Arc;

use neutrino_common::ROOM_VERSION_ID;
use neutrino_store::{EventStore, RoomStore, StoredEvent};
use neutrino_store_sqlite::SqliteStore;
use ruma::api::client::sync::sync_events::v5::{Request, request};
use ruma::events::StateEventType;
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, UInt, UserId, event_id, room_id, user_id};
use serde_json::Value;
use tempfile::NamedTempFile;

use super::{SyncError, SyncState, handle};

// -----------------------------------------------------------------------------
// Test fixtures.
//
// Each test owns its own file-backed `SqliteStore`. `open_in_memory` uses
// SQLite's shared-cache backing under a per-store UUID URI, which is fine for
// single-task tests but unsafe for the concurrent reader+writer workloads the
// long-poll tests drive (the shared-cache lock manager doesn't honour
// `busy_timeout`). A `NamedTempFile` per test gives every case a private DB
// that auto-deletes on drop.
// -----------------------------------------------------------------------------

/// Open a fresh file-backed `SqliteStore`. The returned `NamedTempFile` must be
/// kept alive for the test's lifetime — its `Drop` removes the underlying file.
async fn fresh_store() -> (Arc<SqliteStore>, NamedTempFile) {
    let tmp = NamedTempFile::new().expect("create tempfile");
    let store = SqliteStore::open(tmp.path())
        .await
        .expect("open SqliteStore on tempfile");
    (Arc::new(store), tmp)
}

/// Build a `StoredEvent` whose JSON field is exactly the caller-supplied
/// `Value`. Lower-level helper — most tests want `make_event` (which
/// constructs the standard PDU wrapper); `make_event_from_json` is the
/// escape hatch when tests need to set `unsigned.invite_room_state` or
/// otherwise control the full body.
fn build_stored_event(
    event_id: &EventId,
    room_id: &RoomId,
    event_type: &str,
    state_key: Option<&str>,
    sender: &UserId,
    origin_server_ts: u64,
    json: Value,
) -> StoredEvent {
    let json = serde_json::value::to_raw_value(&json).expect("to_raw_value");
    StoredEvent {
        event_id: event_id.to_owned(),
        room_id: room_id.to_owned(),
        event_type: event_type.to_string(),
        state_key: state_key.map(String::from),
        sender: sender.to_owned(),
        origin_server_ts,
        json,
    }
}

/// Build a `StoredEvent` whose `json` field is a flat object with the
/// standard PDU keys. Tests pass `content` separately so they don't have
/// to construct the wrapper themselves.
fn make_event(
    event_id: &EventId,
    room_id: &RoomId,
    event_type: &str,
    state_key: Option<&str>,
    sender: &UserId,
    origin_server_ts: u64,
    content: Value,
) -> StoredEvent {
    let json = serde_json::json!({
        "event_id": event_id.as_str(),
        "room_id": room_id.as_str(),
        "type": event_type,
        "state_key": state_key,
        "sender": sender.as_str(),
        "origin_server_ts": origin_server_ts,
        "content": content,
    });
    build_stored_event(
        event_id,
        room_id,
        event_type,
        state_key,
        sender,
        origin_server_ts,
        json,
    )
}

/// Same as `make_event` but the caller supplies the full JSON body — used
/// when a test needs `unsigned.invite_room_state` or other top-level keys
/// the standard wrapper doesn't expose.
fn make_event_from_json(
    event_id: &EventId,
    room_id: &RoomId,
    event_type: &str,
    state_key: Option<&str>,
    sender: &UserId,
    origin_server_ts: u64,
    json: Value,
) -> StoredEvent {
    build_stored_event(
        event_id,
        room_id,
        event_type,
        state_key,
        sender,
        origin_server_ts,
        json,
    )
}

/// Deterministic per-room "stem" for generated event IDs — tests construct
/// multiple rooms in one store and need their create / join event IDs to
/// avoid colliding.
fn room_stem(room_id: &RoomId) -> String {
    room_id
        .as_str()
        .trim_start_matches('!')
        .split(':')
        .next()
        .unwrap_or("room")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-')
        .collect()
}

/// Build a minimal v12 `m.room.create` event for `room_id` with `creator` as
/// sender. Event ID is derived from the room's localpart so a test that calls
/// `setup_*` for two rooms in the same store doesn't collide.
fn create_event_for(room_id: &RoomId, creator: &UserId) -> StoredEvent {
    let stem = room_stem(room_id);
    let id_str = format!("$create-{stem}:example.org");
    let id: OwnedEventId = id_str.try_into().expect("create event id parses");
    make_event(
        &id,
        room_id,
        "m.room.create",
        Some(""),
        creator,
        0,
        serde_json::json!({"creator": creator.as_str(), "room_version": ROOM_VERSION_ID}),
    )
}

/// Open `room_id` in the store with `creator` as the create-event sender, no
/// initial events beyond create. Tests building an invited-only room go
/// through this so the FK on the events table is satisfied — production sees
/// the create event arrive over federation alongside the invite; the test
/// simulates that arrival.
async fn setup_room(store: &SqliteStore, room_id: &RoomId, creator: &UserId) {
    let create = create_event_for(room_id, creator);
    store
        .create_room(&create, &[])
        .await
        .expect("create_room succeeds");
}

/// Open `room_id` and immediately add a `member=join` event for `user`. This
/// is the closest analog to the old `InMemoryStore::join_user` helper, and
/// is what most tests want: a room the test user is a member of.
async fn setup_joined_room(store: &SqliteStore, room_id: &RoomId, user: &UserId) {
    let create = create_event_for(room_id, user);
    let stem = room_stem(room_id);
    let member_id_str = format!("$join-{stem}-{}:example.org", user.localpart());
    let member_id: OwnedEventId = member_id_str.try_into().expect("member id parses");
    let join = make_event(
        &member_id,
        room_id,
        "m.room.member",
        Some(user.as_str()),
        user,
        0,
        serde_json::json!({"membership": "join"}),
    );
    store
        .create_room(&create, std::slice::from_ref(&join))
        .await
        .expect("create_room+join succeeds");
}

/// Persist any pre-built event via the trait surface. Panics on error so the
/// failure is visible at the call site — every test in this module wants
/// "this seeding succeeded" as a precondition.
async fn seed(store: &SqliteStore, ev: &StoredEvent) {
    store
        .persist_event(ev, &[])
        .await
        .expect("persist_event succeeds");
}

/// Seed an `m.room.member` event with caller-controlled sender/target/
/// membership. Used by the kick / ban / self-leave / knock tests where the
/// sender-vs-target distinction matters.
async fn seed_member(
    store: &SqliteStore,
    event_id: &EventId,
    room: &RoomId,
    target: &UserId,
    sender: &UserId,
    membership: &str,
    ts: u64,
) {
    let ev = make_event(
        event_id,
        room,
        "m.room.member",
        Some(target.as_str()),
        sender,
        ts,
        serde_json::json!({"membership": membership}),
    );
    seed(store, &ev).await;
}

// -----------------------------------------------------------------------------
// Common request shapes.
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Phase 3 — initial sync, candidate rooms, range slicing.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn initial_sync_with_no_lists_returns_empty_rooms_and_fresh_pos() {
    let (store, _tmp) = fresh_store().await;
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room_a = room_id!("!room-a:example.org");
    let room_b = room_id!("!room-b:example.org");

    setup_joined_room(&store, room_a, user).await;
    setup_joined_room(&store, room_b, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$ev-a-1:example.org"),
            room_a,
            "m.room.message",
            None,
            user,
            1000,
            serde_json::json!({"body": "hello a", "msgtype": "m.text"}),
        ),
    )
    .await;
    seed(
        &store,
        &make_event(
            event_id!("$ev-b-1:example.org"),
            room_b,
            "m.room.message",
            None,
            user,
            2000,
            serde_json::json!({"body": "hello b", "msgtype": "m.text"}),
        ),
    )
    .await;

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
    // Timeline contains: create + member-join + message (3 events).
    assert!(
        !a_result.timeline.is_empty(),
        "timeline includes at least the message event"
    );

    let b_result = resp.rooms.get(room_b).unwrap();
    assert!(!b_result.timeline.is_empty());
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    // `setup_joined_room` writes create + member-join via `create_room`.
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            300,
            serde_json::json!({"name": "Alice's room"}),
        ),
    )
    .await;

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
    let (store, _tmp) = fresh_store().await;
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.pos = Some("9999".to_string());
    let res = handle(&state, user, req).await;
    assert!(matches!(res, Err(SyncError::UnknownPos)));
}

#[tokio::test]
async fn second_sync_with_correct_pos_succeeds() {
    let (store, _tmp) = fresh_store().await;
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let invited_room: &RoomId = room_id!("!invited:example.org");
    // Production gets the create event for an invited room over federation
    // alongside the invite. Tests simulate that arrival by writing the create
    // before the invite member event.
    setup_room(&store, invited_room, inviter).await;
    seed_member(
        &store,
        event_id!("$invite:example.org"),
        invited_room,
        user,
        inviter,
        "invite",
        100,
    )
    .await;

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

/// Seed N rooms named `!room-{i}:example.org` each with `(create, join, msg)`
/// where the message's `origin_server_ts` is `ts[i]`. Returns the room IDs in
/// seed order so tests can assert membership against ranking-derived subsets.
async fn seed_rooms_with_timestamps(
    store: &SqliteStore,
    user: &UserId,
    timestamps: &[u64],
) -> Vec<OwnedRoomId> {
    let mut ids = Vec::with_capacity(timestamps.len());
    for (i, ts) in timestamps.iter().enumerate() {
        let room_id: OwnedRoomId = format!("!room-{i}:example.org").try_into().unwrap();
        setup_joined_room(store, &room_id, user).await;
        let event_id: OwnedEventId = format!("$ev-{i}:example.org").try_into().unwrap();
        let ev = make_event(
            &event_id,
            &room_id,
            "m.room.message",
            None,
            user,
            *ts,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        );
        seed(store, &ev).await;
        ids.push(room_id);
    }
    ids
}

#[tokio::test]
async fn rooms_sorted_by_bump_stamp_desc() {
    // 3 rooms with descending bump stamps assigned to ascending room IDs —
    // room IDs sort opposite to bump_stamp, so any room_id-based ordering
    // would give different results than bump_stamp-based ordering.
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let ids = seed_rooms_with_timestamps(&store, user, &[300, 200, 100]).await;
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let timestamps: Vec<u64> = (1..=10).map(|i| i * 100).collect();
    let ids = seed_rooms_with_timestamps(&store, user, &timestamps).await;
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
    assert!(resp.rooms.contains_key(&ids[7]));
    assert!(resp.rooms.contains_key(&ids[6]));
    assert!(resp.rooms.contains_key(&ids[5]));
    assert!(!resp.rooms.contains_key(&ids[9]), "rank 0 excluded");
    assert!(!resp.rooms.contains_key(&ids[4]), "rank 5 excluded");
}

#[tokio::test]
async fn subscription_bypasses_list_range() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let ids = seed_rooms_with_timestamps(&store, user, &[300, 200, 100]).await;
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let ids = seed_rooms_with_timestamps(&store, user, &[100, 200, 300, 400, 500]).await;
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
    assert!(resp.rooms.contains_key(&ids[4]));
}

#[tokio::test]
async fn invited_room_bump_stamp_uses_invitee_member_event() {
    // For an invited room we don't take the latest event in the room — we use
    // the invitee's own `m.room.member` event ts. Seed an invited room where
    // a later inviter-side member event would otherwise inflate bump_stamp.
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let invited = room_id!("!invited:example.org");
    setup_room(&store, invited, inviter).await;

    seed_member(
        &store,
        event_id!("$invite:example.org"),
        invited,
        user,
        inviter,
        "invite",
        500,
    )
    .await;
    // A later member event in the same room — inviter's own state. Without
    // the per-membership branch this would inflate bump_stamp to 1000.
    seed_member(
        &store,
        event_id!("$inviter:example.org"),
        invited,
        inviter,
        inviter,
        "join",
        1000,
    )
    .await;

    let state = SyncState::new(store);
    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();
    let owned: OwnedRoomId = invited.to_owned();
    let room = resp.rooms.get(&owned).expect("invited room emitted");
    assert_eq!(
        room.bump_stamp,
        Some(UInt::from(500u32)),
        "bump_stamp comes from the invitee's m.room.member event ts (500), \
         not the room's most recent event (1000)"
    );
}

#[tokio::test]
async fn list_count_independent_of_range_size() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let _ids = seed_rooms_with_timestamps(&store, user, &[100, 200, 300, 400, 500]).await;
    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(1u32);
    lists.insert("one".to_string(), list);
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();

    assert_eq!(resp.rooms.len(), 1);
    let list_result = resp.lists.get("one").unwrap();
    assert_eq!(list_result.count, UInt::from(5u32));
}

// -----------------------------------------------------------------------------
// Phase 4 — deltas, `limited`, invite_state, name/avatar/counts, state stubs.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn second_sync_returns_only_new_events() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$ev1:example.org"),
            room,
            "m.room.message",
            None,
            user,
            1000,
            serde_json::json!({"body": "first", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut req1 = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(10, vec![]));
    req1.lists = lists.clone();

    let resp1 = handle(&state, user, req1).await.unwrap();
    let room1 = resp1.rooms.get(room).unwrap();
    assert!(
        !room1.timeline.is_empty(),
        "initial: snapshot includes the seed message"
    );
    assert_eq!(room1.num_live, None, "initial sync events are historical");

    // Add a second event between syncs.
    seed(
        &store,
        &make_event(
            event_id!("$ev2:example.org"),
            room,
            "m.room.message",
            None,
            user,
            2000,
            serde_json::json!({"body": "second", "msgtype": "m.text"}),
        ),
    )
    .await;

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos.clone());
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();

    let room2 = resp2.rooms.get(room).unwrap();
    assert_eq!(
        room2.timeline.len(),
        1,
        "delta: only the new event, not the earlier snapshot"
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$only:example.org"),
            room,
            "m.room.message",
            None,
            user,
            1000,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        ),
    )
    .await;
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(2, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    for i in 0..5 {
        let id_str = format!("$ev-{i}:example.org");
        let id: OwnedEventId = id_str.try_into().unwrap();
        seed(
            &store,
            &make_event(
                &id,
                room,
                "m.room.message",
                None,
                user,
                200 + i,
                serde_json::json!({"body": "x", "msgtype": "m.text"}),
            ),
        )
        .await;
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
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            100,
            serde_json::json!({"name": "Alice's room"}),
        ),
    )
    .await;
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

    seed(
        &store,
        &make_event(
            event_id!("$msg:example.org"),
            room,
            "m.room.message",
            None,
            user,
            200,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        ),
    )
    .await;

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

/// `EMIT_STATE_STUBS=false` skips wire emission of removed-state markers. The
/// deletion-detection logic itself is covered by
/// `unit_tests::diff_required_state_detects_deletion` in `build.rs`;
/// re-exercising it end-to-end through the trait surface needs synthetic
/// state removal, which `EventStore::persist_event` doesn't provide — every
/// trait-level path overwrites state rather than removing it. Tracked as a
/// gap rather than ported, since the behaviour is testable at the unit
/// level.
#[tokio::test]
#[ignore = "trait surface has no way to remove a current_state row; covered by build.rs unit test"]
async fn deleted_state_not_surfaced_with_stubs_disabled() {}

/// Seed an invited-room scenario: an `m.room.member` event with
/// `membership = "invite"` for `user`, carrying the canonical pieces of
/// stripped state inside `unsigned.invite_room_state`. Mirrors what would
/// come in from a federation `/invite` call. Caller is responsible for
/// having called `setup_room` (or `setup_joined_room`) first so the
/// events-table FK to `rooms(room_id)` resolves.
async fn seed_invite(
    store: &SqliteStore,
    room: &RoomId,
    user: &UserId,
    inviter: &UserId,
    invite_event_id: &EventId,
    room_name: &str,
    ts: u64,
) {
    let invite_json = serde_json::json!({
        "event_id": invite_event_id.as_str(),
        "room_id": room.as_str(),
        "type": "m.room.member",
        "state_key": user.as_str(),
        "sender": inviter.as_str(),
        "origin_server_ts": ts,
        "content": {"membership": "invite"},
        "unsigned": {
            "invite_room_state": [
                {
                    "type": "m.room.create",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"creator": inviter.as_str(), "room_version": ROOM_VERSION_ID}
                },
                {
                    "type": "m.room.name",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"name": room_name}
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
    let invite_event = make_event_from_json(
        invite_event_id,
        room,
        "m.room.member",
        Some(user.as_str()),
        inviter,
        ts,
        invite_json,
    );
    seed(store, &invite_event).await;
}

#[tokio::test]
async fn invited_room_emits_invite_state() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let room = room_id!("!invite:example.org");
    setup_room(&store, room, inviter).await;

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
                    "content": {"creator": inviter.as_str(), "room_version": ROOM_VERSION_ID}
                },
                {
                    "type": "m.room.name",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"name": "Bob's invite"}
                },
                {
                    "type": "m.room.avatar",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"url": "mxc://example.org/invite-avatar"}
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
    let invite_event = make_event_from_json(
        event_id!("$invite:example.org"),
        room,
        "m.room.member",
        Some(user.as_str()),
        inviter,
        80,
        invite_json,
    );
    seed(&store, &invite_event).await;

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
    assert!(types.contains(&"m.room.avatar".to_string()));
    let member_count = types
        .iter()
        .filter(|t| t.as_str() == "m.room.member")
        .count();
    assert_eq!(member_count, 2);

    assert_eq!(
        room_res.name.as_deref(),
        Some("Bob's invite"),
        "name lifted from invite_room_state"
    );
    match &room_res.avatar {
        ruma::JsOption::Some(uri) => assert_eq!(uri.as_str(), "mxc://example.org/invite-avatar"),
        _ => panic!("avatar should be Some, lifted from invite_room_state"),
    }
}

#[tokio::test]
async fn fresh_invite_emitted_while_existing_invite_pending() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let room_a = room_id!("!a:example.org");
    let room_b = room_id!("!b:example.org");

    setup_room(&store, room_a, inviter).await;
    seed_invite(
        &store,
        room_a,
        user,
        inviter,
        event_id!("$invite-a:example.org"),
        "Room A",
        100,
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    let a_first = resp1
        .rooms
        .get(room_a)
        .expect("A in first response")
        .invite_state
        .as_ref()
        .expect("invite_state populated on first emission");
    assert!(!a_first.is_empty());

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists.clone();
    let resp2 = handle(&state, user, req2).await.unwrap();
    assert!(
        !resp2.rooms.contains_key(room_a),
        "A no longer in response — invite_state already delivered"
    );

    setup_room(&store, room_b, inviter).await;
    seed_invite(
        &store,
        room_b,
        user,
        inviter,
        event_id!("$invite-b:example.org"),
        "Room B",
        200,
    )
    .await;

    let mut req3 = Request::new();
    req3.pos = Some(resp2.pos);
    req3.lists = lists;
    let resp3 = handle(&state, user, req3).await.unwrap();

    let b_invite_state = resp3
        .rooms
        .get(room_b)
        .expect("B in third response")
        .invite_state
        .as_ref()
        .expect("invite_state populated for the new invite");
    let b_types: Vec<String> = b_invite_state
        .iter()
        .map(|raw| raw.get_field::<String>("type").unwrap().unwrap())
        .collect();
    assert!(b_types.contains(&"m.room.name".to_string()));
    assert!(b_types.iter().any(|t| t == "m.room.member"));

    assert!(
        !resp3.rooms.contains_key(room_a),
        "A's invite is still pending but not re-emitted"
    );
}

#[tokio::test]
async fn name_avatar_and_counts_emitted() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let bob = user_id!("@bob:example.org");
    let carol = user_id!("@carol:example.org");
    let room = room_id!("!room:example.org");
    // `setup_joined_room` covers create + alice-join.
    setup_joined_room(&store, room, user).await;

    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            100,
            serde_json::json!({"name": "Alice's room"}),
        ),
    )
    .await;
    seed(
        &store,
        &make_event(
            event_id!("$avatar:example.org"),
            room,
            "m.room.avatar",
            Some(""),
            user,
            110,
            serde_json::json!({"url": "mxc://example.org/abc"}),
        ),
    )
    .await;
    seed_member(
        &store,
        event_id!("$bob-join:example.org"),
        room,
        bob,
        bob,
        "join",
        130,
    )
    .await;
    seed_member(
        &store,
        event_id!("$carol-invite:example.org"),
        room,
        carol,
        user,
        "invite",
        140,
    )
    .await;

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
    assert_eq!(room_res.joined_count, Some(UInt::from(2u32)));
    assert_eq!(
        room_res.invited_count,
        Some(UInt::from(1u32)),
        "carol's invite counted"
    );
}

// -----------------------------------------------------------------------------
// Phase 6 — request validation + extension echoes.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn conn_id_over_16_chars_rejected() {
    let (store, _tmp) = fresh_store().await;
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.conn_id = Some("this-string-is-way-longer-than-sixteen".to_string());
    let res = handle(&state, user, req).await;
    assert!(matches!(res, Err(SyncError::BadRequest(_))));
}

#[tokio::test]
async fn too_many_lists_rejected() {
    let (store, _tmp) = fresh_store().await;
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
    let (store, _tmp) = fresh_store().await;
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
    let (store, _tmp) = fresh_store().await;
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
    let (store, _tmp) = fresh_store().await;
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let resp = handle(&state, user, Request::new()).await.unwrap();
    assert!(resp.extensions.e2ee.device_one_time_keys_count.is_empty());
    assert!(resp.extensions.to_device.is_none());
}

// -----------------------------------------------------------------------------
// Phase 5 — long-poll loop + retry idempotency.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn initial_sync_ignores_timeout() {
    let (store, _tmp) = fresh_store().await;
    let state = SyncState::new(store);
    let user = user_id!("@alice:example.org");

    let mut req = Request::new();
    req.timeout = Some(std::time::Duration::from_secs(10));

    let start = std::time::Instant::now();
    let _resp = handle(&state, user, req).await.unwrap();
    assert!(
        start.elapsed() < std::time::Duration::from_millis(500),
        "initial sync should return immediately"
    );
}

#[tokio::test]
async fn long_poll_returns_empty_after_timeout() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    req2.timeout = Some(std::time::Duration::from_millis(80));

    let start = std::time::Instant::now();
    let resp2 = handle(&state, user, req2).await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed >= std::time::Duration::from_millis(60),
        "should have waited the full timeout, got {elapsed:?}"
    );
    assert!(resp2.rooms.is_empty(), "no rooms when nothing changed");
}

#[tokio::test]
async fn long_poll_wakes_on_new_event() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state_arc = Arc::new(SyncState::new(store.clone()));

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state_arc, user, req1).await.unwrap();

    let store_for_task = store.clone();
    let waker_user = user.to_owned();
    let waker_room = room.to_owned();
    let waker = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        seed(
            &store_for_task,
            &make_event(
                event_id!("$late:example.org"),
                &waker_room,
                "m.room.message",
                None,
                &waker_user,
                200,
                serde_json::json!({"body": "late", "msgtype": "m.text"}),
            ),
        )
        .await;
    });

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    req2.timeout = Some(std::time::Duration::from_millis(300));

    let start = std::time::Instant::now();
    let resp2 = handle(&state_arc, user, req2).await.unwrap();
    let elapsed = start.elapsed();
    waker.await.unwrap();

    assert!(
        elapsed < std::time::Duration::from_millis(250),
        "should return promptly after the event arrives, got {elapsed:?}"
    );
    assert_eq!(
        resp2.rooms.get(room).unwrap().timeline.len(),
        1,
        "the late event is in the timeline"
    );
}

#[tokio::test]
async fn retry_with_same_pos_returns_cached_response() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$ev:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "a", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos.clone());
    req2.lists = lists.clone();
    let resp2 = handle(&state, user, req2).await.unwrap();
    let pos_after_second = resp2.pos.clone();

    let mut retry = Request::new();
    retry.pos = Some(resp1.pos.clone());
    retry.lists = lists;
    let retry_resp = handle(&state, user, retry).await.unwrap();

    assert_eq!(
        retry_resp.pos, pos_after_second,
        "retry returns the same pos as the original"
    );
    assert_eq!(
        retry_resp.rooms.len(),
        resp2.rooms.len(),
        "retry returns the same set of rooms"
    );
}

#[tokio::test]
async fn stale_pos_returns_unknown_pos_after_advancing() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let state = SyncState::new(store);

    let resp1 = handle(&state, user, Request::new()).await.unwrap();

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos.clone());
    let resp2 = handle(&state, user, req2).await.unwrap();

    let mut req3 = Request::new();
    req3.pos = Some(resp2.pos.clone());
    let _resp3 = handle(&state, user, req3).await.unwrap();

    let mut stale = Request::new();
    stale.pos = Some(resp1.pos);
    let res = handle(&state, user, stale).await;
    assert!(
        matches!(res, Err(SyncError::UnknownPos)),
        "stale pos returns UnknownPos once we've moved past the cached request"
    );
}

#[tokio::test]
async fn retry_does_not_consume_pending_events() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$first:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "first", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos.clone());
    req2.lists = lists.clone();
    let resp2 = handle(&state, user, req2).await.unwrap();

    seed(
        &store,
        &make_event(
            event_id!("$second:example.org"),
            room,
            "m.room.message",
            None,
            user,
            200,
            serde_json::json!({"body": "second", "msgtype": "m.text"}),
        ),
    )
    .await;

    let mut retry = Request::new();
    retry.pos = Some(resp1.pos.clone());
    retry.lists = lists.clone();
    let retry_resp = handle(&state, user, retry).await.unwrap();
    assert_eq!(
        retry_resp.rooms.get(room).map(|r| r.timeline.len()),
        resp2.rooms.get(room).map(|r| r.timeline.len()),
        "retry mirrors the cached response — no late event leakage"
    );

    let mut req3 = Request::new();
    req3.pos = Some(resp2.pos.clone());
    req3.lists = lists;
    let resp3 = handle(&state, user, req3).await.unwrap();
    assert_eq!(
        resp3.rooms.get(room).unwrap().timeline.len(),
        1,
        "the late event is still available on the proper next sync"
    );
}

// -----------------------------------------------------------------------------
// Phase 7 — ported from Synapse `tests/rest/client/sliding_sync/`.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn required_state_wildcard_matches_everything() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            110,
            serde_json::json!({"name": "X"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(5u32);
    list.room_details.required_state = vec![(StateEventType::from("*"), "*".to_string())];
    lists.insert("all".to_string(), list);
    let mut req = Request::new();
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();
    let types: Vec<String> = resp
        .rooms
        .get(room)
        .unwrap()
        .required_state
        .iter()
        .map(|raw| raw.get_field::<String>("type").unwrap().unwrap())
        .collect();
    assert_eq!(types.len(), 3, "all 3 state events emitted");
    assert!(types.contains(&"m.room.create".to_string()));
    assert!(types.contains(&"m.room.name".to_string()));
    assert!(types.contains(&"m.room.member".to_string()));
}

#[tokio::test]
async fn required_state_wildcard_state_key_returns_all_keys_of_type() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let bob = user_id!("@bob:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed_member(
        &store,
        event_id!("$bob:example.org"),
        room,
        bob,
        bob,
        "join",
        110,
    )
    .await;
    // Different event type — must NOT come back when the rule targets members.
    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            120,
            serde_json::json!({"name": "X"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(5u32);
    list.room_details.required_state = vec![(StateEventType::RoomMember, "*".to_string())];
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list);
    let mut req = Request::new();
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();
    let raws = &resp.rooms.get(room).unwrap().required_state;
    assert_eq!(raws.len(), 2, "both members emitted, name skipped");
    for raw in raws {
        assert_eq!(
            raw.get_field::<String>("type").unwrap().unwrap(),
            "m.room.member"
        );
    }
}

#[tokio::test]
async fn required_state_wildcard_event_type_matches_specific_state_key() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$name:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            110,
            serde_json::json!({"name": "X"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut list = request::List::default();
    list.ranges = vec![(UInt::from(0u32), UInt::from(0u32))];
    list.room_details.timeline_limit = UInt::from(5u32);
    list.room_details.required_state = vec![(StateEventType::from("*"), String::new())];
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list);
    let mut req = Request::new();
    req.lists = lists;

    let resp = handle(&state, user, req).await.unwrap();
    let raws = &resp.rooms.get(room).unwrap().required_state;
    assert_eq!(raws.len(), 2, "create + name (both state_key=\"\")");
    let types: Vec<String> = raws
        .iter()
        .map(|raw| raw.get_field::<String>("type").unwrap().unwrap())
        .collect();
    assert!(types.contains(&"m.room.create".to_string()));
    assert!(types.contains(&"m.room.name".to_string()));
    assert!(
        !types.contains(&"m.room.member".to_string()),
        "member has non-empty state_key"
    );
}

#[tokio::test]
async fn initial_sync_sets_limited_true_when_room_has_more_events_than_limit() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    for i in 0..5 {
        let id_str = format!("$ev-{i}:example.org");
        let id: OwnedEventId = id_str.try_into().unwrap();
        seed(
            &store,
            &make_event(
                &id,
                room,
                "m.room.message",
                None,
                user,
                (i + 1) * 100,
                serde_json::json!({"body": "x", "msgtype": "m.text"}),
            ),
        )
        .await;
    }
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(2, vec![]));
    let mut req = Request::new();
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp.rooms.get(room).unwrap();
    assert_eq!(room_res.timeline.len(), 2);
    assert!(
        room_res.limited,
        "limited=true: more history exists beyond the timeline window"
    );
    assert!(
        room_res.prev_batch.is_some(),
        "prev_batch token issued for backpagination"
    );
}

#[tokio::test]
async fn initial_sync_sets_limited_false_when_all_events_fit() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$only:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "x", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(10, vec![]));
    let mut req = Request::new();
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp.rooms.get(room).unwrap();
    // create + join + only = 3 events. timeline_limit=10 so they all fit.
    assert!(
        !room_res.limited,
        "limited=false: every event in the room fits the window"
    );
}

#[tokio::test]
async fn newly_joined_room_emits_initial_snapshot_on_incremental_sync() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let existing = room_id!("!existing:example.org");
    let fresh = room_id!("!fresh:example.org");
    setup_joined_room(&store, existing, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$existing:example.org"),
            existing,
            "m.room.message",
            None,
            user,
            50,
            serde_json::json!({"body": "old", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert!(resp1.rooms.contains_key(existing));
    assert!(!resp1.rooms.contains_key(fresh));

    setup_joined_room(&store, fresh, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$f1:example.org"),
            fresh,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "a", "msgtype": "m.text"}),
        ),
    )
    .await;
    seed(
        &store,
        &make_event(
            event_id!("$f2:example.org"),
            fresh,
            "m.room.message",
            None,
            user,
            110,
            serde_json::json!({"body": "b", "msgtype": "m.text"}),
        ),
    )
    .await;

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();

    let fresh_room = resp2
        .rooms
        .get(fresh)
        .expect("freshly-joined room appears in delta sync");
    assert_eq!(
        fresh_room.initial,
        Some(true),
        "initial=true on first emission"
    );
    assert!(
        fresh_room.timeline.len() >= 2,
        "full snapshot of timeline includes both new messages"
    );
}

#[tokio::test]
async fn empty_room_still_emitted_on_initial_sync() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!empty:example.org");
    // `setup_joined_room` already writes create + member-join (the minimum to
    // make the room visible). "Empty" here means no additional content.
    setup_joined_room(&store, room, user).await;
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req = Request::new();
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp
        .rooms
        .get(room)
        .expect("empty room still appears so the client knows about it");
    assert_eq!(room_res.initial, Some(true));
}

#[tokio::test]
async fn name_change_propagates_on_incremental_sync() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$name-1:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            100,
            serde_json::json!({"name": "Old Name"}),
        ),
    )
    .await;
    let state = SyncState::new(store.clone());

    let mut lists = BTreeMap::new();
    lists.insert(
        "all".to_string(),
        list_with(5, vec![(StateEventType::RoomName, "")]),
    );
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert_eq!(
        resp1.rooms.get(room).unwrap().name.as_deref(),
        Some("Old Name")
    );

    seed(
        &store,
        &make_event(
            event_id!("$name-2:example.org"),
            room,
            "m.room.name",
            Some(""),
            user,
            200,
            serde_json::json!({"name": "New Name"}),
        ),
    )
    .await;

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    let room_res = resp2
        .rooms
        .get(room)
        .expect("room emitted because its state changed");
    assert_eq!(room_res.name.as_deref(), Some("New Name"));
    assert_eq!(
        room_res.required_state.len(),
        1,
        "single state diff: the new m.room.name"
    );
}

#[tokio::test]
async fn invited_room_emits_name_and_avatar_from_stripped_state() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let inviter = user_id!("@bob:example.org");
    let room = room_id!("!invite:example.org");
    setup_room(&store, room, inviter).await;

    let invite_json = serde_json::json!({
        "event_id": "$invite:example.org",
        "room_id": room.as_str(),
        "type": "m.room.member",
        "state_key": user.as_str(),
        "sender": inviter.as_str(),
        "origin_server_ts": 100,
        "content": {"membership": "invite"},
        "unsigned": {
            "invite_room_state": [
                {
                    "type": "m.room.name",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"name": "Bob's Place"}
                },
                {
                    "type": "m.room.avatar",
                    "state_key": "",
                    "sender": inviter.as_str(),
                    "content": {"url": "mxc://example.org/avatar"}
                }
            ]
        }
    });
    let invite_event = make_event_from_json(
        event_id!("$invite:example.org"),
        room,
        "m.room.member",
        Some(user.as_str()),
        inviter,
        100,
        invite_json,
    );
    seed(&store, &invite_event).await;

    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req = Request::new();
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    let room_res = resp.rooms.get(room).unwrap();
    assert_eq!(room_res.name.as_deref(), Some("Bob's Place"));
    match &room_res.avatar {
        ruma::JsOption::Some(uri) => assert_eq!(uri.as_str(), "mxc://example.org/avatar"),
        _ => panic!("avatar should be populated from the stripped state"),
    }
    assert_eq!(room_res.joined_count, None);
    assert_eq!(room_res.invited_count, None);
}

// -----------------------------------------------------------------------------
// MSC4186 §"Rooms included in the server list" — kicked / banned / left /
// knocked. See `build::include_room_per_msc4186` and the storage trait
// `StateStore::rooms_with_membership`.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn knocked_room_appears_in_candidates() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let knocker = user_id!("@knock-host:example.org");
    let room = room_id!("!knock:example.org");
    setup_room(&store, room, knocker).await;
    seed_member(
        &store,
        event_id!("$knock:example.org"),
        room,
        user,
        user,
        "knock",
        100,
    )
    .await;

    let state = SyncState::new(store);

    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    assert!(
        resp.rooms.contains_key(room),
        "knocked rooms are always included per MSC4186"
    );
}

#[tokio::test]
async fn kicked_room_appears_in_candidates_even_on_fresh_connection() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let kicker = user_id!("@bob:example.org");
    let room = room_id!("!kicked:example.org");
    setup_room(&store, room, kicker).await;
    // Member event with sender != target — that's a kick. The kicker's own
    // join also needs to exist for an authentic-looking pre-kick state.
    seed_member(
        &store,
        event_id!("$kicker-join:example.org"),
        room,
        kicker,
        kicker,
        "join",
        50,
    )
    .await;
    seed_member(
        &store,
        event_id!("$alice-join:example.org"),
        room,
        user,
        user,
        "join",
        60,
    )
    .await;
    seed_member(
        &store,
        event_id!("$kick:example.org"),
        room,
        user,
        kicker,
        "leave",
        100,
    )
    .await;

    let state = SyncState::new(store);
    let mut req = Request::new();
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    req.lists = lists;
    let resp = handle(&state, user, req).await.unwrap();

    assert!(
        resp.rooms.contains_key(room),
        "kick (sender ≠ user) → always included, even on first sync"
    );
}

#[tokio::test]
async fn self_left_room_only_appears_if_previously_emitted() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!leave:example.org");

    // Step 1: user is joined. Sync once to put the room into conn.sent.
    setup_joined_room(&store, room, user).await;
    let state = SyncState::new(store.clone());
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert!(resp1.rooms.contains_key(room), "join is included initially");

    // Step 2: user self-leaves. They should still see the room because the
    // conn previously emitted it.
    seed_member(
        &store,
        event_id!("$self-leave:example.org"),
        room,
        user,
        user,
        "leave",
        200,
    )
    .await;
    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    assert!(
        resp2.rooms.contains_key(room),
        "self-leave keeps the room visible because it was previously emitted"
    );

    // Step 3: a brand-new connection (no `pos`) does NOT see the
    // self-leave-only room — it was never emitted on this conn.
    let new_state = SyncState::new(store);
    let mut req3 = Request::new();
    let mut lists2 = BTreeMap::new();
    lists2.insert("all".to_string(), list_with(5, vec![]));
    req3.lists = lists2;
    let resp3 = handle(&new_state, user, req3).await.unwrap();
    assert!(
        !resp3.rooms.contains_key(room),
        "fresh connection skips self-left rooms with no prior emission"
    );
}

#[tokio::test]
async fn banned_room_only_appears_if_previously_emitted() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let banner = user_id!("@bob:example.org");
    let room = room_id!("!ban:example.org");
    setup_room(&store, room, banner).await;
    seed_member(
        &store,
        event_id!("$banner-join:example.org"),
        room,
        banner,
        banner,
        "join",
        50,
    )
    .await;
    seed_member(
        &store,
        event_id!("$ban:example.org"),
        room,
        user,
        banner,
        "ban",
        100,
    )
    .await;
    let state = SyncState::new(store);
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert!(
        !resp1.rooms.contains_key(room),
        "ban with no prior emission is not included (we can't prove previous-join)"
    );
}

#[tokio::test]
async fn banned_room_remains_visible_after_being_emitted_while_joined() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let banner = user_id!("@bob:example.org");
    let room = room_id!("!ban:example.org");

    // Join → sync to register the room with the conn → get banned → next sync
    // must still show the room because conn.sent recorded it.
    setup_joined_room(&store, room, user).await;
    let state = SyncState::new(store.clone());
    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert!(resp1.rooms.contains_key(room));

    seed_member(
        &store,
        event_id!("$ban:example.org"),
        room,
        user,
        banner,
        "ban",
        200,
    )
    .await;
    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    assert!(
        resp2.rooms.contains_key(room),
        "ban after a prior emission stays visible (approximates MSC4186's 'previously joined')"
    );
}

// -----------------------------------------------------------------------------
// Concurrent same-conn requests — cancel-older-on-newer semantics.
//
// MSC3575/4186 forbids concurrent requests with the same conn_id. The
// embedded server cancels the in-flight long-poll rather than queueing the
// newcomer (which would otherwise block for up to 30 s) or rejecting it.
// See `MSC4186-gaps.md` for the spec deviation note.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_long_poll_is_cancelled_by_newer_request() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = Arc::new(SyncState::new(store));

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let conn_id = Some("c1".to_string());

    let mut req1 = Request::new();
    req1.conn_id = conn_id.clone();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    // Spawn the long-poll. 10 s timeout is well past the test's deadline so
    // a "without cancellation" regression would hang the test out instead of
    // silently passing.
    let state_a = state.clone();
    let lists_a = lists.clone();
    let conn_id_a = conn_id.clone();
    let resp1_pos = resp1.pos.clone();
    let user_owned = user.to_owned();
    let a_started = std::time::Instant::now();
    let a = tokio::spawn(async move {
        let mut req = Request::new();
        req.conn_id = conn_id_a;
        req.pos = Some(resp1_pos);
        req.lists = lists_a;
        req.timeout = Some(std::time::Duration::from_secs(10));
        handle(&state_a, &user_owned, req).await
    });

    // Give A time to subscribe to its cancel signal and enter the select.
    // Without this, B's bump can race A's subscribe and the test becomes
    // timing-dependent.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut req_b = Request::new();
    req_b.conn_id = conn_id.clone();
    req_b.pos = Some(resp1.pos.clone());
    req_b.lists = lists.clone();
    let resp_b = handle(&state, user, req_b).await.unwrap();

    let resp_a = a.await.unwrap().unwrap();
    let elapsed = a_started.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "cancelled long-poll should return well before its 10s timeout, got {elapsed:?}"
    );
    // B was a byte-identical retry of A's request (same pos, same body).
    // A's cancellation wrote the idempotency cache on its way out, so B
    // hits the cache and gets the same response A returned.
    assert_eq!(
        resp_a.pos, resp_b.pos,
        "B's cache hit returns the response A wrote on cancellation"
    );
    assert!(resp_a.rooms.is_empty(), "cancelled A returns empty rooms");
}

#[tokio::test]
async fn initial_sync_cancels_prior_entrys_long_poll() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = Arc::new(SyncState::new(store));

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let conn_id = Some("c1".to_string());

    let mut req1 = Request::new();
    req1.conn_id = conn_id.clone();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    let state_a = state.clone();
    let lists_a = lists.clone();
    let conn_id_a = conn_id.clone();
    let resp1_pos = resp1.pos.clone();
    let user_owned = user.to_owned();
    let a_started = std::time::Instant::now();
    let a = tokio::spawn(async move {
        let mut req = Request::new();
        req.conn_id = conn_id_a;
        req.pos = Some(resp1_pos);
        req.lists = lists_a;
        req.timeout = Some(std::time::Duration::from_secs(10));
        handle(&state_a, &user_owned, req).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Initial sync (pos=None) on the same conn_id. `registry.create`
    // cancels the prior entry on the way to inserting the replacement,
    // which is what should wake A.
    let mut req_b = Request::new();
    req_b.conn_id = conn_id.clone();
    req_b.lists = lists.clone();
    let resp_b = handle(&state, user, req_b).await.unwrap();
    assert!(
        resp_b.rooms.contains_key(room),
        "initial sync re-emits the joined room"
    );

    let resp_a = a.await.unwrap().unwrap();
    let elapsed = a_started.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "orphan long-poll should wake when prior entry is replaced, got {elapsed:?}"
    );
    // The orphan A still completes successfully — its response goes to the
    // (cancelled-by-client) HTTP connection. We only assert it returned;
    // the body is empty because no events arrived during A's brief window.
    assert!(resp_a.rooms.is_empty());
}

#[tokio::test]
async fn second_concurrent_request_proceeds_after_first_cancelled() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    seed(
        &store,
        &make_event(
            event_id!("$seed:example.org"),
            room,
            "m.room.message",
            None,
            user,
            100,
            serde_json::json!({"body": "seed", "msgtype": "m.text"}),
        ),
    )
    .await;
    let state = Arc::new(SyncState::new(store.clone()));

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));
    let conn_id = Some("c1".to_string());

    let mut req1 = Request::new();
    req1.conn_id = conn_id.clone();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();

    let state_a = state.clone();
    let lists_a = lists.clone();
    let conn_id_a = conn_id.clone();
    let resp1_pos = resp1.pos.clone();
    let user_owned = user.to_owned();
    let a = tokio::spawn(async move {
        let mut req = Request::new();
        req.conn_id = conn_id_a;
        req.pos = Some(resp1_pos);
        req.lists = lists_a;
        req.timeout = Some(std::time::Duration::from_secs(10));
        handle(&state_a, &user_owned, req).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // B differs from A in its body (different timeline_limit), so B's
    // retry-cache key won't match A's cached entry — B has to process
    // fresh. Because A's cancellation advanced `conn.pos`, B's
    // `req1.resp.pos` no longer matches; the test instead drives B with
    // the post-cancel pos that A wrote into the cache. To find it, we
    // use a third request: send the byte-identical retry first (which
    // cache-hits A's cancel response and gives us its pos), then issue
    // B against that pos.
    //
    // This proves the chain: cancellation completes, cache is queryable,
    // newer non-cached requests proceed against the advanced state.
    let mut probe = Request::new();
    probe.conn_id = conn_id.clone();
    probe.pos = Some(resp1.pos.clone());
    probe.lists = lists.clone();
    let probe_resp = handle(&state, user, probe).await.unwrap();

    let resp_a = a.await.unwrap().unwrap();
    assert_eq!(
        probe_resp.pos, resp_a.pos,
        "byte-identical retry hits the cancelled A's cached response"
    );

    // Now a body-differing request at A's post-cancel pos.
    let mut lists_b = lists.clone();
    lists_b.insert("all".to_string(), list_with(50, vec![]));
    let mut req_b = Request::new();
    req_b.conn_id = conn_id.clone();
    req_b.pos = Some(probe_resp.pos.clone());
    req_b.lists = lists_b;
    req_b.timeout = Some(std::time::Duration::from_millis(100));
    let resp_b = handle(&state, user, req_b).await.unwrap();
    assert_ne!(
        resp_b.pos, probe_resp.pos,
        "fresh processing advances pos past the cache-hit pos"
    );
}

#[tokio::test]
async fn initial_sync_anchors_high_water_at_store_head() {
    let (store, _tmp) = fresh_store().await;
    let user = user_id!("@alice:example.org");
    let room = room_id!("!room:example.org");
    setup_joined_room(&store, room, user).await;
    // > EVENTS_PER_SYNC_LIMIT (1000) is required to trip the bug. We seed
    // 1100 events past the create + member-join already written by setup.
    for i in 0..1100u64 {
        let id: OwnedEventId = format!("$e{i}:example.org").try_into().unwrap();
        seed(
            &store,
            &make_event(
                &id,
                room,
                "m.room.message",
                None,
                user,
                100 + i,
                serde_json::json!({"body": "x", "msgtype": "m.text"}),
            ),
        )
        .await;
    }
    let state = SyncState::new(store);

    let mut lists = BTreeMap::new();
    lists.insert("all".to_string(), list_with(5, vec![]));

    let mut req1 = Request::new();
    req1.lists = lists.clone();
    let resp1 = handle(&state, user, req1).await.unwrap();
    assert!(
        resp1.rooms.contains_key(room),
        "initial sync emits the room"
    );

    let mut req2 = Request::new();
    req2.pos = Some(resp1.pos);
    req2.lists = lists;
    let resp2 = handle(&state, user, req2).await.unwrap();
    assert!(
        !resp2.rooms.contains_key(room),
        "second sync with no new events omits room — high-water is at store head"
    );
}
