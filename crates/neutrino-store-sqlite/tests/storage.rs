//! Integration tests covering interactions across `RoomStore`,
//! `EventStore`, `StateStore`, and `FederationOutbox`. Per-method unit
//! tests live inline in `src/store/{events,rooms,state,dag,outbox,inbox}.rs`.

mod common;

use neutrino_store::{EventStore, FederationOutbox, RoomStore, StateStore, StreamPos};
use ruma::{event_id, server_name};

use common::{create_event, member_join, member_leave, message, name_event, store};

use crate::common::{ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, BOB_USER_ID};

// R1: create_room with a single create event → all observable surfaces agree.
#[tokio::test]
async fn create_room_with_create_only() {
    let s = store().await;
    assert_eq!(s.room_count().await.unwrap(), 0);

    let ce = create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    s.create_room(&ce, &[]).await.unwrap();

    assert_eq!(s.room_count().await.unwrap(), 1);

    let version = s.get_room_version(*ALICE_ROOM_ID).await.unwrap();
    assert!(version.is_some(), "expected Some(RoomVersionId), got None");

    let events = s.events_after(StreamPos(0), 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1.event_type, "m.room.create");
}

// R2: create_room with initial events → all visible via events_after, ascending order.
#[tokio::test]
async fn create_room_with_initial_events() {
    let s = store().await;
    let ce = create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    let join = member_join(event_id!("$m1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    s.create_room(&ce, &[join]).await.unwrap();

    let events = s.events_after(StreamPos(0), 10).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].1.event_type, "m.room.create");
    assert_eq!(events[1].1.event_type, "m.room.member");
    // Stream pos strictly increasing.
    assert!(events[0].0 < events[1].0);
}

// R6: create_room with N events fires the watch exactly once, advancing
// to the final stream_pos (not N intermediate notifications).
#[tokio::test]
async fn create_room_fires_watch_once() {
    let s = store().await;
    let mut receiver = s.subscribe();
    assert_eq!(*receiver.borrow(), StreamPos(0));

    let ce = create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    let join = member_join(event_id!("$m1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    s.create_room(&ce, &[join]).await.unwrap();

    // Wait for the first change notification.
    receiver.changed().await.unwrap();
    let pos_after = *receiver.borrow();
    // 2 events committed → final stream_pos = 2.
    assert_eq!(pos_after, StreamPos(2));

    // No further change expected (the batch advanced the watch once).
    let timeout =
        tokio::time::timeout(std::time::Duration::from_millis(50), receiver.changed()).await;
    assert!(timeout.is_err(), "watch fired more than once for one batch");
}

// E1: create_room + persist_event → event visible via get_events.
#[tokio::test]
async fn persist_event_visible_after_create_room() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();
    let msg = message(
        event_id!("$msg1:example.com"),
        *ALICE_ROOM_ID,
        *ALICE_USER_ID,
        "hello",
    );
    s.persist_event(&msg, &[]).await.unwrap();

    let fetched = s
        .get_events(&[event_id!("$msg1:example.com")])
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].event_id.as_str(), "$msg1:example.com");
}

// E10: subscribe → persist → watch advances.
#[tokio::test]
async fn persist_event_advances_watch() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let mut receiver = s.subscribe();
    let pos_before = *receiver.borrow();

    let msg = message(
        event_id!("$msg1:example.com"),
        *ALICE_ROOM_ID,
        *ALICE_USER_ID,
        "hello",
    );
    s.persist_event(&msg, &[]).await.unwrap();

    receiver.changed().await.unwrap();
    let pos_after = *receiver.borrow();
    assert!(
        pos_after > pos_before,
        "expected watch to advance: before={:?}, after={:?}",
        pos_before,
        pos_after
    );
}

// E11: two concurrent persists → watch ends at the max stream_pos (never
// goes backward).
#[tokio::test]
async fn persist_event_watch_monotonic() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c1:example.com"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let s1 = s.clone();
    let s2 = s.clone();
    let h1 = tokio::spawn(async move {
        let m = message(
            event_id!("$msg1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "a",
        );
        s1.persist_event(&m, &[]).await.unwrap();
    });
    let h2 = tokio::spawn(async move {
        let m = message(
            event_id!("$msg2:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "b",
        );
        s2.persist_event(&m, &[]).await.unwrap();
    });
    h1.await.unwrap();
    h2.await.unwrap();

    let receiver = s.subscribe();
    let final_pos = *receiver.borrow();
    // 1 create + 2 messages → final stream_pos = 3.
    assert_eq!(final_pos, StreamPos(3));
}

// X1–X10: cross-trait scenarios deferred from task #4 (need StateStore and
// FederationOutbox impls that landed in #5).

// X1: persisting a state event updates current state observable via StateStore.
#[tokio::test]
async fn persist_state_event_updates_current_state() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();
    s.persist_event(
        &name_event(
            event_id!("$n:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "Test Room",
        ),
        &[],
    )
    .await
    .unwrap();

    let got = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
        .await
        .unwrap()
        .expect("name event in current state");
    assert_eq!(got.event_id.as_str(), "$n:e");
}

// X2: persisting a non-state event doesn't touch current_state.
#[tokio::test]
async fn persist_non_state_event_leaves_state_untouched() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();
    let before = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();

    s.persist_event(
        &message(event_id!("$m:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "hi"),
        &[],
    )
    .await
    .unwrap();

    let after = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
    assert_eq!(before.len(), after.len());
    // Same key set (no new state events).
    for k in before.keys() {
        assert!(after.contains_key(k));
    }
}

// X3: a later state event with the same key supersedes the earlier one.
#[tokio::test]
async fn persist_supersedes_prior_state() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[name_event(
            event_id!("$n1:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "first",
        )],
    )
    .await
    .unwrap();
    s.persist_event(
        &name_event(event_id!("$n2:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "second"),
        &[],
    )
    .await
    .unwrap();

    let got = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
        .await
        .unwrap()
        .expect("name event present");
    assert_eq!(got.event_id.as_str(), "$n2:e");
}

// X4: persisting an `m.room.member` join surfaces the user in joined_members.
#[tokio::test]
async fn persist_member_join_appears_in_joined_members() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mja:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();
    s.persist_event(
        &member_join(event_id!("$mjb:e"), *ALICE_ROOM_ID, *BOB_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let members = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
    assert_eq!(members.len(), 2);
    assert!(members.contains_key(*ALICE_USER_ID));
    assert!(members.contains_key(*BOB_USER_ID));
}

// X5: persist_event with N destinations populates N outbox rows visible via
// FederationOutbox::pending_pdus.
#[tokio::test]
async fn persist_event_with_destinations_appears_in_outbox() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let d1 = server_name!("a.example.com");
    let d2 = server_name!("b.example.com");
    let d3 = server_name!("c.example.com");

    s.persist_event(
        &message(event_id!("$m:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "hi"),
        &[d1, d2, d3],
    )
    .await
    .unwrap();

    // Each destination has one PDU pending.
    for d in [d1, d2, d3] {
        let pdus = s.pending_pdus(d).await.unwrap();
        assert_eq!(
            pdus.len(),
            1,
            "destination {} should have 1 pdu",
            d.as_str()
        );
        assert_eq!(pdus[0].event_id.as_str(), "$m:e");
    }
    let mut destinations = s.pending_destinations().await.unwrap();
    destinations.sort_by_key(|d| d.as_str().to_owned());
    assert_eq!(destinations.len(), 3);
}

// X6: create_room never writes outbox rows (no remote members yet).
#[tokio::test]
async fn create_room_creates_no_outbox_entries() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mj:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();
    assert!(s.pending_destinations().await.unwrap().is_empty());
}

// X7: initial member event passed to create_room flows through to
// joined_members.
#[tokio::test]
async fn create_room_initial_join_appears_in_joined_members() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mj:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();
    let members = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
    assert_eq!(members.len(), 1);
    assert!(members.contains_key(*ALICE_USER_ID));
}

// X8: alice joined to two distinct rooms → joined_rooms returns both.
#[tokio::test]
async fn joined_rooms_after_multiple_creates() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c1:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mj1:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();
    s.create_room(
        &create_event(event_id!("$c2:e"), *BOB_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mj2:e"),
            *BOB_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();

    let mut rooms = s.joined_rooms(*ALICE_USER_ID).await.unwrap();
    rooms.sort_by_key(|r| r.as_str().to_owned());
    assert_eq!(rooms.len(), 2);
    assert_eq!(rooms[0].as_str(), ALICE_ROOM_ID.as_str());
    assert_eq!(rooms[1].as_str(), BOB_ROOM_ID.as_str());
}

// X9: remove_pdus drops the destination from pending_destinations once its
// last PDU is gone.
#[tokio::test]
async fn remove_pdus_makes_destination_disappear() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let d = server_name!("a.example.com");
    s.persist_event(
        &message(event_id!("$m:e"), *ALICE_ROOM_ID, *ALICE_USER_ID, "hi"),
        &[d],
    )
    .await
    .unwrap();
    assert_eq!(s.pending_destinations().await.unwrap().len(), 1);

    s.remove_pdus(d, &[event_id!("$m:e")]).await.unwrap();
    assert!(s.pending_destinations().await.unwrap().is_empty());
}

// X10: alice leaves the room → joined_rooms no longer includes it.
#[tokio::test]
async fn member_left_no_longer_in_joined_rooms() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            event_id!("$mj:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.joined_rooms(*ALICE_USER_ID).await.unwrap().len(), 1);

    s.persist_event(
        &member_leave(event_id!("$ml:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();
    assert!(s.joined_rooms(*ALICE_USER_ID).await.unwrap().is_empty());
}

// X11: a forward-extension `persist_event` for the same
// `(room, type, state_key)` as one of `create_room`'s `initial_events`
// overwrites `current_state` — the unconditional UPSERT in
// `row::write_into_tx` is last-writer-wins. The two writes are
// sequenced via `await`, which is sufficient to pin commit order
// regardless of writer-pool shape. Complements X13, which pins the
// inverse for `persist_historical_event` (doesn't regress
// current_state).
#[tokio::test]
async fn persist_event_after_create_room_overwrites_initial_state() {
    let s = store().await;
    let initial_member_id = event_id!("$mj_initial:e");
    let later_member_id = event_id!("$mj_later:e");

    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[member_join(
            initial_member_id,
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
        )],
    )
    .await
    .unwrap();

    // Sanity: current_state reflects the initial join.
    let initial = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.member", ALICE_USER_ID.as_str())
        .await
        .unwrap()
        .expect("initial member state present after create_room");
    assert_eq!(initial.event_id.as_str(), initial_member_id.as_str());

    // Forward persist_event for the same state key — UPSERT overwrites.
    s.persist_event(
        &member_leave(later_member_id, *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    let after = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.member", ALICE_USER_ID.as_str())
        .await
        .unwrap()
        .expect("member state still present");
    assert_eq!(
        after.event_id.as_str(),
        later_member_id.as_str(),
        "forward persist_event must overwrite create_room's initial state row"
    );
}

// X12: `persist_historical_event` writes a state event without
// touching `current_state`. The trait post-condition is explicit on
// this — historical events feed history (`events`, `event_edges`,
// `room_messages`) but must not regress the resolved current state.
//
// Cross-trait scenario (EventStore writes ↔ StateStore reads), so it
// lives in `tests/storage.rs` rather than the per-trait unit suite.
#[tokio::test]
async fn persist_historical_event_does_not_update_current_state() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    // No name state event yet → current_state for that key is empty.
    assert!(
        s.current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap()
            .is_none()
    );

    let n = name_event(
        event_id!("$n_hist:e"),
        *ALICE_ROOM_ID,
        *ALICE_USER_ID,
        "historical",
    );
    s.persist_historical_event(&n).await.unwrap();

    // Event is in the store …
    let got = s.get_events(&[event_id!("$n_hist:e")]).await.unwrap();
    assert_eq!(got.len(), 1, "historical event must be in events table");

    // … but current_state remains empty.
    assert!(
        s.current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap()
            .is_none(),
        "persist_historical_event must not upsert current_state"
    );
}

// X13: cardinal regression test for the persist split. A forward-
// extension `persist_event` sets `current_state`; a subsequent
// `persist_historical_event` for the *same* `(room, type, state_key)`
// must NOT regress it. This is the test that flags any future change
// that re-conflates the two paths or revives the unconditional
// UPSERT.
#[tokio::test]
async fn persist_historical_event_does_not_regress_current_state() {
    let s = store().await;
    s.create_room(
        &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .unwrap();

    // Forward extension: alice joins.
    let join = member_join(event_id!("$join:e"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    s.persist_event(&join, &[]).await.unwrap();

    let before = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.member", ALICE_USER_ID.as_str())
        .await
        .unwrap()
        .expect("current state should be the join event");
    assert_eq!(before.event_id.as_str(), "$join:e");

    // Historical write: an older leave event arrives via backfill.
    let leave = member_leave(event_id!("$old_leave:e"), *ALICE_ROOM_ID, *ALICE_USER_ID);
    s.persist_historical_event(&leave).await.unwrap();

    // current_state still points at the forward join — not regressed.
    let after = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.member", ALICE_USER_ID.as_str())
        .await
        .unwrap()
        .expect("current state still present");
    assert_eq!(
        after.event_id.as_str(),
        "$join:e",
        "historical leave must not regress current_state from the forward join"
    );

    // And the historical event itself is in the events table for the
    // backfill / DAG-walk surface.
    let got = s.get_events(&[event_id!("$old_leave:e")]).await.unwrap();
    assert_eq!(
        got.len(),
        1,
        "historical leave should be in events for backfill / DAG walks"
    );
}
