//! Restart durability tests — verify that an `SqliteStore` reopened
//! at the same file path observes everything the prior process
//! committed. The design doc
//! (`docs/2026-05-14-sqlite-storage-backend.md` §2 "Restart & crash
//! semantics") makes load-bearing claims about WAL atomicity, watch
//! seeding from `MAX(stream_pos)`, and outbox redelivery on restart;
//! the integration test surface had no coverage of those before, so a
//! regression that broke any of them would only have shown up in
//! production.
//!
//! File-backed via `tempfile::NamedTempFile` — `open_in_memory` is
//! pointless here because the in-memory DB is gone the moment the
//! first store drops.

mod common;

use std::str::FromStr;
use std::time::Duration;

use neutrino_common::ROOM_VERSION_ID;
use neutrino_store::{
    EventStore, FederationInbox, FederationOutbox, RoomStore, StateStore, StreamPos,
};
use neutrino_store_sqlite::SqliteStore;
use ruma::{RoomVersionId, event_id, server_name};
use tempfile::NamedTempFile;

use common::{
    ALICE_ROOM_ID, ALICE_USER_ID, CREATE_EVENT_ID, create_event, member_join, message, name_event,
};

/// Wait for `deadpool-sync` to actually close the previous store's
/// connections before reopening the same path.
///
/// `SyncWrapper::Drop` fires `sqlite3_close` onto tokio's blocking pool via
/// `spawn_blocking_background` — no `JoinHandle`, so we can't `await` it.
/// On a constrained runtime (current-thread tokio, low core count) the
/// immediate reopen can race against the still-running close on the same
/// path and stall inside SQLite's WAL recovery, surfacing as an indefinite
/// test hang. Forcing a blocking-pool round-trip + a brief sleep gives the
/// queued close tasks time to release POSIX SHM locks before the reopen's
/// `Connection::open` competes for them. Same race the 2026-05-21 schema
/// tests hit; they sidestepped it by moving their raw `Connection::open`
/// calls into `spawn_blocking` (which by happenstance bridged the same
/// gap). This helper is the explicit, documented version of that bridge.
async fn settle_close_race() {
    tokio::task::spawn_blocking(|| {})
        .await
        .expect("blocking-pool round-trip");
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Comprehensive restart-survival test: populate every durable trait
/// surface (events, current_state, outbox, federation_txns), drop the
/// store, reopen at the same path, and
/// assert every observable persists. A single fat test rather than
/// one per surface — the setup cost dominates and each assertion's
/// failure message ("name event missing", "outbox empty", etc.)
/// pinpoints the broken invariant on its own.
#[tokio::test]
async fn restart_preserves_all_state() {
    let file = NamedTempFile::new().expect("tempfile");
    let path = file.path();

    let dest = server_name!("matrix.org");
    let origin = server_name!("origin.example.com");

    // ---- Process 1: populate every durable surface. ----
    let final_pos = {
        let s = SqliteStore::open(path).await.expect("first open");

        // create_room writes the create event + one initial member event.
        s.create_room(
            &create_event(*CREATE_EVENT_ID, *ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(
                event_id!("$m_alice:e"),
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
            )],
        )
        .await
        .expect("create room");

        // A state event — exercises the `current_state` upsert path.
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
        .expect("persist name event");

        // A non-state event with a federation destination — exercises
        // the outbox.
        let msg_id = event_id!("$msg:e");
        s.persist_event(
            &message(msg_id, *ALICE_ROOM_ID, *ALICE_USER_ID, "hello"),
            &[dest],
        )
        .await
        .expect("persist message");

        // FederationInbox dedup record — first call returns false.
        let already = s
            .record_federation_txn(origin, "txn-Z")
            .await
            .expect("record federation txn");
        assert!(!already, "first federation txn record must return false");

        // Snapshot the final stream_pos via the watch; the reopen
        // should land on this exact value.
        let pos = *s.subscribe().borrow();
        assert_eq!(
            pos,
            StreamPos(4),
            "expected 4 events committed (create + member + name + message)"
        );
        pos
    };

    // ---- Process 2: reopen and verify. ----
    settle_close_race().await;
    let s = SqliteStore::open(path).await.expect("reopen");

    // Watch must seed from MAX(stream_pos) — the durability contract
    // for cross-restart subscriber wake-up.
    assert_eq!(
        *s.subscribe().borrow(),
        final_pos,
        "watch must seed from MAX(stream_pos) on restart"
    );

    // Room version round-trips.
    let v = s
        .get_room_version(*ALICE_ROOM_ID)
        .await
        .expect("get_room_version");
    assert_eq!(v, Some(RoomVersionId::from_str(ROOM_VERSION_ID).unwrap()));

    // All four events readable via the stream.
    let events = s
        .events_after(StreamPos(0), 100)
        .await
        .expect("events_after");
    assert_eq!(events.len(), 4, "expected 4 events to survive restart");
    let ids: Vec<&str> = events.iter().map(|(_, e)| e.event_id.as_str()).collect();
    for expected in ["$create:example.com", "$m_alice:e", "$n:e", "$msg:e"] {
        assert!(
            ids.contains(&expected),
            "event {expected} missing after restart"
        );
    }

    // Current state survives — the name event and the member event
    // are both reachable via their respective StateStore queries.
    let name = s
        .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
        .await
        .expect("current_state_event")
        .expect("name state row present after restart");
    assert_eq!(name.event_id.as_str(), "$n:e");

    let members = s
        .joined_members(*ALICE_ROOM_ID)
        .await
        .expect("joined_members");
    assert_eq!(members.len(), 1);
    assert!(
        members.contains_key(*ALICE_USER_ID),
        "alice's join membership must survive restart"
    );

    // Federation outbox survives — `pending_destinations` enumerates
    // the destination and `pending_pdus` returns the queued message.
    let destinations = s
        .pending_destinations()
        .await
        .expect("pending_destinations");
    assert_eq!(destinations.len(), 1);
    assert_eq!(destinations[0].as_str(), "matrix.org");

    let pdus = s.pending_pdus(dest).await.expect("pending_pdus");
    assert_eq!(pdus.len(), 1);
    assert_eq!(pdus[0].event_id.as_str(), "$msg:e");

    // Federation inbox dedup record survives — replaying the same
    // (origin, txn_id) must return true (already seen). This is the
    // "outbox redelivery on restart is safe because
    // record_federation_txn is idempotent on the remote" property
    // from the design doc.
    let already = s
        .record_federation_txn(origin, "txn-Z")
        .await
        .expect("record federation txn (replay)");
    assert!(
        already,
        "federation txn record must dedup across restart (replay returned false)"
    );
}

/// Empty-DB edge case: open a fresh file, drop without writing
/// anything, reopen, and assert the watch seeds at `StreamPos(0)`.
/// Exercises the `COALESCE(MAX(stream_pos), 0)` branch of the watch
/// seeding query — distinct from the post-write path covered above.
#[tokio::test]
async fn restart_empty_db_seeds_watch_at_zero() {
    let file = NamedTempFile::new().expect("tempfile");
    let path = file.path();

    {
        let _ = SqliteStore::open(path).await.expect("first open");
    }

    settle_close_race().await;
    let s = SqliteStore::open(path).await.expect("reopen");
    assert_eq!(
        *s.subscribe().borrow(),
        StreamPos(0),
        "empty events table must seed watch at StreamPos(0)"
    );
}
