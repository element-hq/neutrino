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

use deadpool_sqlite::{Config, Hook, HookError, Pool, Runtime, rusqlite::Connection};
use neutrino_store::{StorageError, StreamPos};
use tokio::sync::watch;

mod error;
mod row;
mod schema;
mod store;

#[cfg(test)]
mod tests;

use crate::error::Error;

/// SQLite-backed `StorageBackend`.
///
/// Constructed via [`SqliteStore::open`] (file-backed) or
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
}

impl SqliteStore {
    /// Open a file-backed store at `path`. Creates the schema bundle on
    /// first open (per `schema::ensure_schema`'s version gate).
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let cfg = Config::new(path.as_ref().to_path_buf());
        Self::build(cfg, /* reader_size */ 4).await
    }

    /// Open a private in-memory store. Per pool-split doc §5: two
    /// separate pools on `:memory:` would each see a private DB, so
    /// readers would never observe writes. Use the shared-cache URI with
    /// a per-store UUID so both pools attach to the same in-memory DB
    /// while staying isolated across tests.
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
        })
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
    pub(crate) async fn run_write<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let obj = self.writer_pool.get().await.map_err(Error::Pool)?;
        let inner = obj.interact(f).await.map_err(Error::Interact)?;
        inner.map_err(StorageError::from)
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
