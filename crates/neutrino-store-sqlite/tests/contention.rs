//! Contention tests — verify the read/write pool split survives
//! concurrent access. Public-API subset of the design doc §4 contention
//! surface (`docs/2026-05-18-read-write-pool-split.md`).
//!
//! Tests requiring internals not exposed to integration tests — raw
//! reader/writer pool-connection accessors, deliberately-slow closures
//! to pin reader/writer non-blocking, `PRAGMA query_only` readbacks —
//! are out of scope at this boundary. They'd need either a `pub` /
//! feature-gated test hook on `SqliteStore`, or to live as
//! `#[cfg(test)]` unit tests next to the impl.
//!
//! All tests run on the multi-threaded tokio runtime so concurrent
//! futures actually race rather than interleaving on a single worker.

mod common;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use neutrino_store::{EventStore, RoomStore, StorageError, StreamPos};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedEventId, event_id};
use tempfile::NamedTempFile;

use common::{ALICE_ROOM_ID, ALICE_USER_ID, CREATE_EVENT_ID, create_event, message};

/// Bootstrap a file-backed `SqliteStore` on a fresh tempfile, plus the
/// create event for [`ALICE_ROOM_ID`]. The returned [`NamedTempFile`]
/// keeps the on-disk file alive for the test's lifetime; it's unlinked
/// when the binding drops.
///
/// Contention tests must use a file-backed store, not
/// [`SqliteStore::open_in_memory`]. The in-memory backing uses
/// `mode=memory&cache=shared` so two pools can rendezvous on the same
/// DB, but that turns on SQLite's in-process shared-cache table-level
/// lock manager — readers and writers serialise there, and the lock
/// manager doesn't invoke `busy_handler`, so `busy_timeout` can't
/// absorb the conflict. `SQLITE_LOCKED_SHAREDCACHE` surfaces directly
/// (see sqlite.org/sharedcache.html). File-backed `open(path)` uses
/// the regular WAL file-locking instead, which gives true concurrent
/// reader/writer semantics — what the contention surface needs.
async fn store_with_room_on_tempfile() -> (SqliteStore, NamedTempFile) {
    let file = NamedTempFile::new().expect("create tempfile");
    let s = SqliteStore::open(file.path())
        .await
        .expect("open store on tempfile");
    s.create_room(
        &create_event(*CREATE_EVENT_ID, *ALICE_ROOM_ID, *ALICE_USER_ID),
        &[],
    )
    .await
    .expect("bootstrap room");
    (s, file)
}

/// C1: N concurrent `persist_event` calls all succeed. None surface
/// `SQLITE_BUSY` / "database is locked" — the writer pool serialises
/// at the application layer (`pool.get().await` FIFO) so two writers
/// never race for the SQLite write lock. This is the canonical witness
/// that the split is doing its job; if it fails, either the writer
/// pool isn't size-1 or a write got routed to the reader pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_never_busy() {
    let (s, _tempfile) = store_with_room_on_tempfile().await;
    let n = 32_usize;

    let handles: Vec<_> = (0..n)
        .map(|i| {
            let s = s.clone();
            tokio::spawn(async move {
                let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
                let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
                s.persist_event(&m, &[]).await
            })
        })
        .collect();

    for h in handles {
        match h.await.unwrap() {
            Ok(()) => {}
            Err(StorageError::Internal(msg))
                if msg.contains("database is locked") || msg.contains("BUSY") =>
            {
                panic!("SQLITE_BUSY-class error under concurrent writes: {msg}");
            }
            Err(e) => panic!("unexpected error from persist_event: {e:?}"),
        }
    }
}

/// C2: every concurrent writer commits to a *distinct* `stream_pos` —
/// no lost commits, no duplicates, no gaps in the (0, n] range. The
/// `events.stream_pos AUTOINCREMENT` schema + writer-pool serialisation
/// is what gives this; the test pins both together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_produce_unique_stream_positions() {
    let (s, _tempfile) = store_with_room_on_tempfile().await;
    let n = 32_usize;

    let handles: Vec<_> = (0..n)
        .map(|i| {
            let s = s.clone();
            tokio::spawn(async move {
                let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
                let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
                s.persist_event(&m, &[]).await.unwrap();
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }

    // The room bootstrap contributes 1 create event at stream_pos = 1,
    // then the N concurrent writers occupy 2..=N+1 in some order.
    let events = s.events_after(StreamPos(0), n + 100).await.unwrap();
    assert_eq!(events.len(), n + 1, "lost commits under contention");
    let positions: HashSet<u64> = events.iter().map(|(sp, _)| sp.0).collect();
    assert_eq!(positions.len(), n + 1, "duplicate stream_pos under contention");
    let max = positions.iter().copied().max().unwrap();
    let min = positions.iter().copied().min().unwrap();
    assert_eq!(min, 1, "stream_pos doesn't start at 1");
    assert_eq!(max, n as u64 + 1, "stream_pos has gaps");
}

/// C3: the `subscribe()` watch is strictly monotonic under concurrent
/// writes. The `send_if_modified` max-guard inside `notify_watch` is
/// what makes this true; if a future change drops the guard, this
/// test catches the regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_monotonic_under_concurrent_writes() {
    let (s, _tempfile) = store_with_room_on_tempfile().await;
    // Subscribe *before* spawning writers — TOCTOU avoidance per the
    // trait `subscribe` contract.
    let mut rx = s.subscribe();
    let mut last = *rx.borrow();

    let n = 16_usize;
    let writers: Vec<_> = (0..n)
        .map(|i| {
            let s = s.clone();
            tokio::spawn(async move {
                let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
                let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
                s.persist_event(&m, &[]).await.unwrap();
            })
        })
        .collect();
    for h in writers {
        h.await.unwrap();
    }

    // Drain any remaining watch updates. `tokio::watch` collapses
    // multiple updates between observations, so we may see fewer than
    // N changes — but every observed change must be `>= last`.
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.changed()).await {
            Ok(Ok(())) => {
                let cur = *rx.borrow();
                assert!(
                    cur >= last,
                    "watch went backwards under contention: {last:?} -> {cur:?}"
                );
                last = cur;
            }
            // Settled (timed out with no new value) or sender dropped.
            _ => break,
        }
    }
    // Final value must reflect every committed write — `n` new events
    // on top of the create event = StreamPos(n + 1).
    assert_eq!(last, StreamPos(n as u64 + 1), "watch missed commits");
}

/// C4: mixed concurrent readers + writers — both make progress within
/// a wall-clock deadline. WAL gives readers consistent snapshots
/// regardless of writer activity, and the reader pool isn't serialised
/// by the writer mutex, so neither side should block the other.
///
/// Not as precise as the design-doc §4 tests 7-9 (which would pin
/// `≤ 500ms` for a writer-during-long-read deadline), but at the public
/// API boundary we don't have the "slow read"/"slow write" hooks
/// needed for those — this is the cheap deadlock-class regression
/// guard instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_reads_and_writes_make_progress() {
    let (s, _tempfile) = store_with_room_on_tempfile().await;
    let n_writers = 8_usize;
    let n_readers = 8_usize;

    let mut handles = Vec::with_capacity(n_writers + n_readers);
    for i in 0..n_writers {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let id: OwnedEventId = format!("$m{i}:example.com").try_into().unwrap();
            let m = message(&id, *ALICE_ROOM_ID, *ALICE_USER_ID, "hi");
            s.persist_event(&m, &[]).await.unwrap();
        }));
    }
    for _ in 0..n_readers {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            // Read path is `run_read`; if writers were serialising
            // readers behind a shared mutex, these would queue behind
            // each persist_event and the deadline would blow.
            let _ = s.events_after(StreamPos(0), 100).await.unwrap();
        }));
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    for h in handles {
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::timeout(remaining, h)
            .await
            .expect("contention deadlock: task did not complete within 5s")
            .unwrap();
    }

    // Sanity check: every writer's commit is observable post-settle.
    let events = s.events_after(StreamPos(0), 100).await.unwrap();
    assert_eq!(events.len(), n_writers + 1, "writes lost during mixed contention");
}

/// C5: re-`persist_event` of the same `event_id` is rejected (the
/// schema's `UNIQUE(event_id)` constraint surfaces as
/// `InvalidInput`), and that rejection is durable under contention —
/// 32 concurrent attempts to insert the same event yield exactly one
/// success and N-1 `InvalidInput`s, with no `BUSY`-class errors and
/// no duplicate row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_persist_event_under_contention_is_unique() {
    let (s, _tempfile) = store_with_room_on_tempfile().await;
    let n = 32_usize;
    let dup_id = event_id!("$dup:example.com");

    let handles: Vec<_> = (0..n)
        .map(|_| {
            let s = s.clone();
            tokio::spawn(async move {
                let m = message(dup_id, *ALICE_ROOM_ID, *ALICE_USER_ID, "dup");
                s.persist_event(&m, &[]).await
            })
        })
        .collect();

    let mut ok = 0_usize;
    let mut invalid = 0_usize;
    for h in handles {
        match h.await.unwrap() {
            Ok(()) => ok += 1,
            Err(StorageError::InvalidInput(_)) => invalid += 1,
            Err(StorageError::Internal(msg))
                if msg.contains("database is locked") || msg.contains("BUSY") =>
            {
                panic!("SQLITE_BUSY-class error under duplicate contention: {msg}");
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert_eq!(ok, 1, "expected exactly one successful insert; got {ok}");
    assert_eq!(invalid, n - 1, "expected {} InvalidInputs; got {invalid}", n - 1);

    // Exactly one row landed.
    let events = s.events_after(StreamPos(0), 100).await.unwrap();
    let matching: Vec<_> = events
        .iter()
        .filter(|(_, e)| e.event_id.as_str() == dup_id.as_str())
        .collect();
    assert_eq!(matching.len(), 1, "duplicate row landed under contention");
}
