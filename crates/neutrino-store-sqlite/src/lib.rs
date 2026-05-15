#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

//! SQLite implementation of the `neutrino-store::StorageBackend` trait.
//!
//! See `docs/2026-05-14-sqlite-storage-backend.md` for the full design
//! rationale (driver choice, pool shape, version gate, watch notification,
//! restart semantics, error mapping).

use std::path::Path;

use deadpool_sqlite::{Config, Hook, HookError, Pool, Runtime, rusqlite::Connection};
use neutrino_store::{StorageError, StreamPos};
use tokio::sync::watch;

mod error;
mod hydrate;
mod schema;
mod store;

use crate::error::Error;

/// SQLite-backed `StorageBackend`.
///
/// Constructed via [`SqliteStore::open`] (file-backed) or
/// [`SqliteStore::open_in_memory`] (tests). Cheap to clone — the inner
/// pool is `Arc`-shared.
#[derive(Clone)]
pub struct SqliteStore {
    pool: Pool,
    /// One sender per store. Receivers handed out via `subscribe()` /
    /// `EventStore::subscribe` see the `StreamPos` of the most recently
    /// committed `persist_event`. Seeded at open from `MAX(stream_pos)`;
    /// monotonically advanced by `persist_event` via `send_if_modified`
    /// from inside the `spawn_blocking` closure (design doc §2).
    #[allow(dead_code)]
    watch_tx: watch::Sender<StreamPos>,
}

impl SqliteStore {
    /// Open a file-backed store at `path`. Creates the schema bundle on
    /// first open (per `schema::ensure_schema`'s version gate).
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let cfg = Config::new(path.as_ref().to_path_buf());
        Self::build(cfg, /* max_size */ 4).await
    }

    /// Open a private in-memory store. Pool size 1 per design doc §2
    /// "`open_in_memory` topology" — each test gets an isolated DB with
    /// no `cache=shared` cross-test contamination.
    pub async fn open_in_memory() -> Result<Self, StorageError> {
        let cfg = Config::new(":memory:");
        Self::build(cfg, /* max_size */ 1).await
    }

    async fn build(cfg: Config, max_size: usize) -> Result<Self, StorageError> {
        let pool = cfg
            .builder(Runtime::Tokio1)
            .map_err(|never: std::convert::Infallible| -> Error {
                // `cfg.builder(...)` returns `Result<_, Infallible>` — the error case
                // is unrepresentable, so a closing match on the empty enum is the
                // statically-checked way to unwrap without `.unwrap()` / `.expect()`.
                match never {}
            })?
            .max_size(max_size)
            .post_create(Hook::async_fn(|obj, _| {
                Box::pin(async move {
                    obj.interact(|conn| schema::apply_connection_pragmas(conn))
                        .await
                        .map_err(|e| HookError::Message(format!("interact: {e}").into()))?
                        .map_err(|e| HookError::Message(format!("pragmas: {e}").into()))?;
                    Ok(())
                })
            }))
            .build()
            .map_err(Error::Build)?;

        // Run schema bundle (version-gated). Must happen after PRAGMAs are
        // applied via the post_create hook, since `ensure_schema` may
        // execute statements that depend on FK enforcement being on.
        let obj = pool.get().await.map_err(Error::Pool)?;
        obj.interact(schema::ensure_schema)
            .await
            .map_err(Error::Interact)?
            .map_err(StorageError::from)?;

        // Seed the stream watch from MAX(stream_pos). New subscribers boot
        // against this value after a restart — see design doc §2 "Restart
        // & crash semantics".
        let initial: i64 = obj
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

        Ok(Self { pool, watch_tx })
    }

    /// Public sibling of `EventStore::subscribe` — useful for callers that
    /// hold an `SqliteStore` directly and don't need the trait surface.
    /// Same receiver semantics: subscribe-before-query to avoid TOCTOU.
    pub fn subscribe(&self) -> watch::Receiver<StreamPos> {
        self.watch_tx.subscribe()
    }

    /// Acquire a pooled connection and run `f` inside `interact` (which
    /// internally uses `spawn_blocking`). The closure runs on a blocking
    /// worker so synchronous rusqlite calls don't block the async runtime.
    ///
    /// The closure must be `'static`; capture by move and avoid borrowing
    /// `self`. Errors propagate as `StorageError` via the local `Error`
    /// type's `From` impls (see `error.rs`).
    #[allow(dead_code)]
    pub(crate) async fn run<F, T>(&self, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut Connection) -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let obj = self.pool.get().await.map_err(Error::Pool)?;
        let inner = obj.interact(f).await.map_err(Error::Interact)?;
        inner.map_err(StorageError::from)
    }
}
