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
use ruma::{RoomVersionId, server_name};
use tempfile::{NamedTempFile, TempDir};

use common::{ALICE_ROOM_ID, ALICE_USER_ID, create_event, member_join, message, name_event};

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

    // Pre-compute the event ids we'll cross-check after restart.
    let create_ev = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
    let member_ev = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
    let name_ev = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "Test Room");
    let msg_ev = message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hello");

    let create_id = create_ev.event_id.clone();
    let member_id = member_ev.event_id.clone();
    let name_id = name_ev.event_id.clone();
    let msg_id = msg_ev.event_id.clone();

    // ---- Process 1: populate every durable surface. ----
    let final_pos = {
        let s = SqliteStore::open(path).await.expect("first open");

        // create_room writes the create event + one initial member event.
        s.create_room(&create_ev, &[member_ev])
            .await
            .expect("create room");

        // A state event — exercises the `current_state` upsert path.
        s.persist_event(&name_ev, &[])
            .await
            .expect("persist name event");

        // A non-state event with a federation destination — exercises
        // the outbox.
        s.persist_event(&msg_ev, &[dest])
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
    for expected in [
        create_id.as_str(),
        member_id.as_str(),
        name_id.as_str(),
        msg_id.as_str(),
    ] {
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
    assert_eq!(name.event_id.as_str(), name_id.as_str());

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

    let pdus = s
        .pending_pdus(dest, usize::MAX)
        .await
        .expect("pending_pdus");
    assert_eq!(pdus.len(), 1);
    assert_eq!(pdus[0].event_id.as_str(), msg_id.as_str());

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

/// `open_in_dir` owns the `<dir>/neutrino.db` layout: it creates a missing
/// directory (parents included), places the database at the documented
/// filename, and a reopen at the same directory observes prior writes. Pins
/// the dir-creation + filename invariants the configurable-storage-dir
/// feature relies on — the HTTP round-trip tests exercise persistence but
/// never assert the on-disk file name or that a missing dir is created.
#[tokio::test]
async fn open_in_dir_creates_dir_and_persists_at_neutrino_db() {
    let root = TempDir::new().expect("tempdir");
    // A nested, not-yet-existing path — exercises create_dir_all's parents.
    let dir = root.path().join("does/not/exist/yet");

    let create_ev = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
    let member_ev = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);

    {
        let s = SqliteStore::open_in_dir(&dir).await.expect("open_in_dir");
        s.create_room(&create_ev, &[member_ev])
            .await
            .expect("create room");
    }

    assert!(
        dir.join("neutrino.db").exists(),
        "store must create the directory and name the DB <dir>/neutrino.db"
    );

    settle_close_race().await;

    // Reopening the same directory must observe the prior write — the watch
    // seeds from MAX(stream_pos), so a non-zero position proves persistence.
    let s = SqliteStore::open_in_dir(&dir).await.expect("reopen_in_dir");
    assert!(
        *s.subscribe().borrow() > StreamPos(0),
        "writes before drop must survive reopening the same storage dir"
    );
}

/// On unix, a directory `open_in_dir` creates is owner-only (`0o700`) — the
/// DB holds plaintext message history, so no other UID should be able to
/// traverse in and read it (or its WAL sidecars). Defense-in-depth on top of
/// the Android per-UID sandbox; also protects the dev binary writing to cwd.
#[cfg(unix)]
#[tokio::test]
async fn open_in_dir_creates_owner_only_directory() {
    use std::os::unix::fs::PermissionsExt;

    let root = TempDir::new().expect("tempdir");
    let dir = root.path().join("created/by/us");

    let _s = SqliteStore::open_in_dir(&dir).await.expect("open_in_dir");

    let mode = std::fs::metadata(&dir)
        .expect("stat storage dir")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o700,
        "storage dir we create must be owner-only (0o700), got {:o}",
        mode & 0o777
    );
}
