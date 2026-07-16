#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! SQLite implementation of the `neutrino-store::StorageBackend` trait.
//!
//! See `docs/2026-05-14-sqlite-storage-backend.md` for the original design
//! (driver choice, version gate, watch notification, restart semantics,
//! error mapping). The reader/writer pool split — supersedes 05-14 §2
//! "Pool initialization" and the uniform `run<F>` helper — is specified
//! in `docs/2026-05-18-read-write-pool-split.md`.

use std::path::Path;
use std::time::Duration;

use deadpool_sqlite::{Config, Hook, HookError, Pool, Runtime, rusqlite::Connection};
use neutrino_room::provider::StateProvider;
use neutrino_store::{StorageError, StreamPos, WithStateProvider};
use tokio::sync::watch;

/// Wall-clock ceiling for any single `run_write` call. The writer pool is
/// size-1 and the underlying `spawn_blocking` thread cannot be cancelled,
/// so a hung closure would otherwise stall every subsequent writer for
/// the lifetime of the process (pool-split doc §6). The timeout doesn't
/// *unstall* the writer — the blocking thread keeps running and the
/// connection is still held — but it surfaces the failure to the caller
/// as `Internal("write timed out")` instead of letting the await hang
/// forever. 30 s is well past any expected SQLite write under embedded
/// load while still being short enough to catch a genuine deadlock /
/// runaway loop in development.
///
/// Tests override to a short duration so the timeout arm is reachable
/// without burning 30 s of CI time per case.
#[cfg(not(test))]
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

mod error;
mod row;
mod schema;
mod store;

pub use crate::store::SqliteStateProvider;

#[cfg(test)]
mod tests;

use crate::error::Error;

/// SQLite-backed `StorageBackend`.
///
/// Constructed via [`SqliteStore::open_in_dir`] (production — a storage
/// directory), [`SqliteStore::open`] (file-backed, by exact path) or
/// [`SqliteStore::open_in_memory`] (tests). Cheap to clone — both inner
/// pools are `Arc`-shared.
#[derive(Debug, Clone)]
pub struct SqliteStore {
    /// Reader pool — multiple concurrent readers, each with
    /// `PRAGMA query_only = ON`. WAL gives readers consistent snapshots
    /// regardless of writer activity.
    reader_pool: Pool,
    /// Writer pool, `max_size = 1`. The pool itself is the application-
    /// layer write mutex: `pool.get().await` is an async FIFO queue, so
    /// concurrent writers serialise here rather than racing the SQLite
    /// write lock and surfacing as `SQLITE_BUSY`.
    writer_pool: Pool,
    /// One sender per store. Receivers handed out via `subscribe()` /
    /// `EventStore::subscribe` see the `StreamPos` of the most recently
    /// committed `persist_event`. Seeded at open from `MAX(stream_pos)`;
    /// monotonically advanced by `persist_event` via `send_if_modified`
    /// from inside the `spawn_blocking` closure (05-14 §2).
    watch_tx: watch::Sender<StreamPos>,
    /// Whether client-visible reads (`events_after` for `/sync`,
    /// `room_messages` for `/messages`) exclude soft-failed rows. `true` in
    /// production; the convergence harness sets it `false` (see
    /// [`SqliteStore::client_hides_soft_failed`]). Copied on `clone`; set once
    /// at startup, before the store is shared.
    hide_soft_failed: bool,
}

/// Database filename within a storage directory. The on-disk layout — this
/// file plus its `-wal`/`-shm` sidecars — is a storage-implementation detail
/// owned here, not by callers handing us a directory.
const DB_FILENAME: &str = "neutrino.db";

/// Create the storage directory itself — *not* its parents, which are the
/// caller's responsibility. Returns whether we actually created it (vs. it
/// already existing), so [`secure_storage_dir`] knows whether it owns the
/// directory's permissions. A missing parent surfaces as an error naming the
/// path so the caller knows to create it first.
async fn create_storage_dir(dir: &Path) -> Result<bool, StorageError> {
    match tokio::fs::create_dir(dir).await {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(Error::Internal(format!(
            "creating storage dir {} (create its parent dirs first): {e}",
            dir.display()
        ))
        .into()),
    }
}

/// Keep the plaintext DB owner-private on unix: clamp `neutrino.db` and its
/// `-wal`/`-shm` sidecars to `0o600`, and — only when we created it — the
/// directory to `0o700`. We never tighten a directory the host handed us
/// (`created == false`); that mode is its owner's choice. Sidecar absence is
/// tolerated: they exist once WAL is engaged (the schema bundle is a write),
/// but a future journal-mode change shouldn't make this brittle.
#[cfg(unix)]
async fn secure_storage_dir(dir: &Path, created: bool) -> Result<(), StorageError> {
    use std::os::unix::fs::PermissionsExt;

    if created {
        tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|e| Error::Internal(format!("securing storage dir {}: {e}", dir.display())))?;
    }

    let db = dir.join(DB_FILENAME);
    let wal = dir.join(format!("{DB_FILENAME}-wal"));
    let shm = dir.join(format!("{DB_FILENAME}-shm"));
    for path in [db, wal, shm] {
        match tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(Error::Internal(format!("securing {}: {e}", path.display())).into());
            }
        }
    }
    Ok(())
}

impl SqliteStore {
    /// Open a file-backed store at `path`. Creates the schema bundle on
    /// first open (per `schema::ensure_schema`'s version gate).
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let cfg = Config::new(path.as_ref().to_path_buf());
        Self::build(cfg, /* reader_size */ 4).await
    }

    /// Open a store rooted at a storage *directory*: the directory itself is
    /// created (tolerating it already existing) and the database lives at
    /// `<dir>/neutrino.db`. This is the constructor production code should
    /// use — the host passes a directory it owns (e.g. Android's
    /// `context.filesDir`) and stays unaware of the database filename and
    /// WAL sidecar layout.
    ///
    /// We create only the final directory, *not* its parents — a missing
    /// parent surfaces as an error pointing the caller at it. (Creating the
    /// whole chain would force our owner-only mode onto intermediate dirs the
    /// host may share with other processes.)
    ///
    /// On unix the plaintext DB is kept owner-private: [`secure_storage_dir`]
    /// clamps `neutrino.db` and its `-wal`/`-shm` sidecars to `0o600`, and the
    /// directory to `0o700` *only when we created it* — a pre-existing host
    /// dir (Android's per-UID `filesDir`, a `./data` from a prior run) keeps
    /// the mode its owner chose.
    pub async fn open_in_dir(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let dir = dir.as_ref();
        let created = create_storage_dir(dir).await?;
        let store = Self::open(dir.join(DB_FILENAME)).await?;
        #[cfg(unix)]
        secure_storage_dir(dir, created).await?;
        #[cfg(not(unix))]
        let _ = created;
        Ok(store)
    }

    /// Open a private in-memory store. Per pool-split doc §5: two
    /// separate pools on `:memory:` would each see a private DB, so
    /// readers would never observe writes. Use the shared-cache URI with
    /// a per-store UUID so both pools attach to the same in-memory DB
    /// while staying isolated across tests.
    ///
    /// # Concurrency caveat
    ///
    /// The shared-cache backing engages SQLite's in-process shared-cache
    /// lock manager, which serialises reader/writer transactions and
    /// *does not* invoke `busy_handler` (sqlite.org/sharedcache.html).
    /// `SQLITE_LOCKED_SHAREDCACHE` therefore surfaces past
    /// `PRAGMA busy_timeout = 5000` and shows up as panics on the
    /// reader side when a writer holds an in-flight transaction.
    /// **Safe for single-task and single-worker tests; not safe for
    /// concurrent reader+writer workloads.** Use file-backed
    /// [`SqliteStore::open_in_dir`] on a `tempfile::TempDir` for any test
    /// that exercises the concurrent reader/writer surface — a `TempDir`
    /// reaps the DB *and* its WAL `-wal`/`-shm` sidecars on drop, which a
    /// bare `NamedTempFile` would orphan. See
    /// `docs/2026-05-18-read-write-pool-split.md` §5 for the full
    /// rationale.
    pub async fn open_in_memory() -> Result<Self, StorageError> {
        let name = uuid::Uuid::new_v4();
        let uri = format!("file:neutrino-store-{name}?mode=memory&cache=shared");
        let cfg = Config::new(uri);
        Self::build(cfg, /* reader_size */ 1).await
    }

    async fn build(cfg: Config, reader_size: usize) -> Result<Self, StorageError> {
        let reader_pool = build_pool(cfg.clone(), reader_size, /* query_only */ true)?;
        let writer_pool = build_pool(cfg, /* max_size */ 1, /* query_only */ false)?;

        // Schema bundle is a write — run it against the writer pool.
        // Must happen after PRAGMAs are applied via the post_create hook,
        // since `ensure_schema` executes statements that depend on FK
        // enforcement being on.
        let writer = writer_pool.get().await.map_err(Error::Pool)?;
        writer
            .interact(schema::ensure_schema)
            .await
            .map_err(Error::Interact)?
            .map_err(StorageError::from)?;
        drop(writer);

        // Seed the stream watch from MAX(stream_pos) via the reader pool
        // — read-only, no need to hold the single writer connection.
        // New subscribers boot against this value after a restart — see
        // 05-14 §2 "Restart & crash semantics".
        let reader = reader_pool.get().await.map_err(Error::Pool)?;
        let initial: i64 = reader
            .interact(|conn| {
                conn.query_row(
                    "SELECT COALESCE(MAX(stream_pos), 0) FROM events",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .await
            .map_err(Error::Interact)?
            .map_err(Error::Sqlite)?;

        let (watch_tx, _) = watch::channel(StreamPos(initial as u64));

        Ok(Self {
            reader_pool,
            writer_pool,
            watch_tx,
            // Production default: soft-failed events are hidden from clients.
            hide_soft_failed: true,
        })
    }

    /// Set whether client-visible reads hide soft-failed events (default
    /// `true`). The convergence harness passes `false` so soft-failed events
    /// stay visible in `/messages` / `/sync`: soft-fail is order-dependent and
    /// server-local, so hiding it diverges client timelines even when the
    /// state DAG converges. Consuming builder — call once at startup, before
    /// the store is cloned/shared.
    #[must_use]
    pub fn client_hides_soft_failed(mut self, hide: bool) -> Self {
        self.hide_soft_failed = hide;
        self
    }

    /// SQL fragment excluding soft-failed rows from a client-visible read, or
    /// empty when the client-side filter is off (see
    /// [`client_hides_soft_failed`](Self::client_hides_soft_failed)).
    fn soft_failed_filter(&self) -> &'static str {
        if self.hide_soft_failed {
            " AND soft_failed = 0"
        } else {
            ""
        }
    }

    /// Public sibling of `EventStore::subscribe` — useful for callers that
    /// hold an `SqliteStore` directly and don't need the trait surface.
    /// Same receiver semantics: subscribe-before-query to avoid TOCTOU.
    pub fn subscribe(&self) -> watch::Receiver<StreamPos> {
        self.watch_tx.subscribe()
    }

    /// Advance the stream watch under the max-guard. Called *inside* the
    /// `spawn_blocking` closure after `tx.commit()?` per design doc §2 —
    /// `spawn_blocking` closures are non-cancellable, so a committed event
    /// can never be stranded without notification. Associated function
    /// (not `&self`) because the calling closure must be `'static` and
    /// cannot borrow the store.
    pub(crate) fn notify_watch(watch_tx: &watch::Sender<StreamPos>, new_pos: i64) {
        let new_pos = StreamPos(new_pos as u64);
        watch_tx.send_if_modified(|cur| {
            if new_pos > *cur {
                *cur = new_pos;
                true
            } else {
                false
            }
        });
    }

    /// Wake stream-watch subscribers *without* advancing the cursor. An OOB
    /// invite (or its removal) is not a room event — it carries no
    /// `StreamPos` — but it IS new data the sliding-sync long-poll must
    /// surface promptly rather than after its full timeout. `send_modify`
    /// bumps the watch's internal version (so `changed()` fires) while
    /// leaving the `StreamPos` value untouched, which preserves both the
    /// head read in `build_response` and `notify_watch`'s monotonic
    /// `new_pos > cur` guard for real events. Same `'static`-closure reason
    /// for being an associated fn as [`Self::notify_watch`].
    pub(crate) fn notify_watch_changed(watch_tx: &watch::Sender<StreamPos>) {
        watch_tx.send_modify(|_| {});
    }

    /// Run a read-only closure on a connection from the reader pool.
    /// Mis-routed writes (any `INSERT`/`UPDATE`/`DELETE`/`CREATE`/`DROP`)
    /// fail with `SQLITE_READONLY` because the reader pool sets
    /// `PRAGMA query_only = ON` on every connection — this is the
    /// enforcement primitive that catches a method silently bypassing
    /// the writer-serialisation point.
    pub(crate) async fn run_read<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let obj = self.reader_pool.get().await.map_err(Error::Pool)?;
        let inner = obj.interact(f).await.map_err(Error::Interact)?;
        inner.map_err(StorageError::from)
    }

    /// Run a write closure on the single writer-pool connection.
    /// `writer_pool.get().await` is the FIFO mutex — concurrent writers
    /// queue here rather than racing the SQLite write lock. Read-
    /// modify-write closures stay on this connection so they see their
    /// own uncommitted state inside the transaction.
    ///
    /// Bounded by [`WRITE_TIMEOUT`]: a hung closure surfaces as
    /// `Internal("write timed out")` instead of an indefinite hang on
    /// the await. The timeout covers the *entire* call — both the FIFO
    /// queue wait and the blocking closure — so a writer queued behind
    /// a hung one will also surface a timeout rather than wait forever.
    pub(crate) async fn run_write<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let fut = async {
            let obj = self.writer_pool.get().await.map_err(Error::Pool)?;
            let inner = obj.interact(f).await.map_err(Error::Interact)?;
            inner.map_err(StorageError::from)
        };
        match tokio::time::timeout(WRITE_TIMEOUT, fut).await {
            Ok(res) => res,
            Err(_) => Err(StorageError::Internal(format!(
                "write timed out after {WRITE_TIMEOUT:?}"
            ))),
        }
    }
}

/// The connection-bridging primitive the per-room actor drives `RoomCore::apply`
/// through: `run_read` and `SqliteStateProvider` are `pub(crate)`/connection-
/// bound, so the engine can't build a provider itself — but it owns the state
/// machine. `f` runs the apply (a read: immutable events + auth chains, no write
/// transaction). Keeping the apply types (`RoomCore` / `Effect` / `CoreError`)
/// inside `R` means this crate never names them — storage stays ignorant of the
/// state machine, knowing only the `StateProvider` trait it implements. See
/// [`WithStateProvider`] for the contract.
impl WithStateProvider for SqliteStore {
    async fn with_state_provider<F, R>(&self, f: F) -> Result<R, StorageError>
    where
        F: for<'a> FnOnce(&'a dyn StateProvider) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.run_read(move |conn| {
            let provider = crate::store::SqliteStateProvider::new(conn);
            Ok(f(&provider))
        })
        .await
    }
}

/// Construct one pool with the shared per-connection PRAGMA set. The
/// reader and writer pools diverge by exactly one PRAGMA (`query_only`).
fn build_pool(cfg: Config, max_size: usize, query_only: bool) -> Result<Pool, Error> {
    cfg.builder(Runtime::Tokio1)
        .map_err(|never: std::convert::Infallible| -> Error {
            // `cfg.builder(...)` returns `Result<_, Infallible>` — the error case
            // is unrepresentable, so a closing match on the empty enum is the
            // statically-checked way to unwrap without `.unwrap()` / `.expect()`.
            match never {}
        })?
        .max_size(max_size)
        .post_create(Hook::async_fn(move |obj, _| {
            Box::pin(async move {
                obj.interact(move |conn| schema::apply_connection_pragmas(conn, query_only))
                    .await
                    .map_err(|e| HookError::Message(format!("interact: {e}").into()))?
                    .map_err(|e| HookError::Message(format!("pragmas: {e}").into()))?;
                Ok(())
            })
        }))
        .build()
        .map_err(Error::Build)
}

#[cfg(test)]
mod timeout_tests {
    use std::time::{Duration, Instant};

    use neutrino_store::StorageError;

    use crate::{SqliteStore, WRITE_TIMEOUT, error::Error};

    /// A `run_write` closure that exceeds [`WRITE_TIMEOUT`] surfaces as
    /// `Internal("write timed out")` instead of hanging the caller
    /// indefinitely. Sleeps inside `spawn_blocking` (legal — that's its
    /// whole point), bypassing the pool's recycle path; the test asserts
    /// the wall-clock cost is bounded by the timeout, not the closure's
    /// sleep duration.
    #[tokio::test]
    async fn run_write_times_out_on_long_closure() {
        let s = SqliteStore::open_in_memory().await.unwrap();
        let sleep_for = WRITE_TIMEOUT * 3;
        let start = Instant::now();
        let result: Result<(), StorageError> = s
            .run_write(move |_conn| -> Result<(), Error> {
                std::thread::sleep(sleep_for);
                Ok(())
            })
            .await;
        let elapsed = start.elapsed();
        match result {
            Err(StorageError::Internal(msg)) if msg.contains("write timed out") => {}
            other => panic!("expected Internal(\"write timed out\"); got {other:?}"),
        }
        assert!(
            elapsed < WRITE_TIMEOUT + Duration::from_secs(1),
            "run_write returned after {elapsed:?}; expected ≈ {WRITE_TIMEOUT:?}"
        );
    }
}
