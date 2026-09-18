use deadpool_sqlite::rusqlite::Connection;

use crate::error::Error;

const SCHEMA_SQL: &str = include_str!("schema.sql");

/// The schema version this build expects. Bumped whenever `schema.sql` changes
/// shape, so a store written by an older build is refused at open (loudly, and
/// before any query hits a column that does not exist) rather than breaking
/// mid-request. One constant so the stamp and the gate cannot drift.
pub(crate) const SCHEMA_VERSION: i64 = 3;

/// Version-gate the database against the bundled schema. Authoritative
/// open-time check per design doc §2 "Open path: version gate & schema bundle".
///
/// | `user_version`     | Action                                          |
/// |--------------------|-------------------------------------------------|
/// | `0`                | Fresh DB. Apply `journal_mode = WAL`, then run  |
/// |                    | the schema bundle + version stamp in one txn.   |
/// | `SCHEMA_VERSION`   | Already at current schema. Skip the bundle.     |
/// | other              | Refuse to open — defensive bail.                |
///
/// There is deliberately **no migration path**: a store written by an older
/// schema is refused, not upgraded, and the fix is to delete it. Every
/// deployment is under our control, so a wipe is cheaper than carrying
/// migration code for versions no one is running.
///
/// Atomicity: the schema DDL and the version stamp run inside
/// the same transaction, so a mid-bundle failure rolls both back. The
/// next open sees `user_version = 0` and re-runs the (non-`IF NOT
/// EXISTS`) bundle, which either succeeds or fails loudly on a
/// colliding pre-existing table. Migrations are likewise one transaction
/// each, so a failure leaves the old version stamped and retryable.
///
/// `journal_mode = WAL` cannot be inside the transaction — SQLite
/// forbids journal-mode changes while a transaction is open — so it
/// runs against the bare connection first. WAL state is persisted in
/// the DB file, so applying it before a bundle that ultimately rolls
/// back is harmless: the next open finds the DB already in WAL mode
/// and skips re-applying it (the PRAGMA is idempotent).
pub(crate) fn ensure_schema(conn: &mut Connection) -> Result<(), Error> {
    let v: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    match v {
        0 => {
            // Journal mode is persisted in the DB file but can't sit
            // inside a transaction — set it first against the bare
            // connection.
            conn.execute_batch("PRAGMA journal_mode = WAL")?;
            // DDL + version stamp atomic.
            let tx = conn.transaction()?;
            tx.execute_batch(SCHEMA_SQL)?;
            tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
            tx.commit()?;
            Ok(())
        }
        v if v == SCHEMA_VERSION => Ok(()),
        other => Err(Error::Internal(format!("unknown schema version: {other}"))),
    }
}

/// Per-connection PRAGMAs applied via the deadpool `post_create` hook on
/// every connection check-out. Per design doc §2 "Pool initialization".
///
/// `foreign_keys` enforcement is the critical one — it's per-connection,
/// not persisted in the DB file, and defaults to OFF. The hook makes
/// forgetting impossible.
///
/// `journal_mode = WAL` is NOT applied here — it's persisted in the DB
/// file and only needs setting once at open time (via `schema.sql`).
///
/// `query_only` flips between reader (ON) and writer (OFF) per the
/// read/write pool split doc §1 — runtime enforcement that a mis-routed
/// write on a reader connection fails fast with `SQLITE_READONLY`.
pub(crate) fn apply_connection_pragmas(conn: &Connection, query_only: bool) -> Result<(), Error> {
    // Tuning values (journal_size_limit / mmap_size / cache_size) adapted
    // from https://fractaledmind.com/2023/09/07/enhancing-rails-sqlite-fine-tuning/
    // — embedded workload so values are conservative; revisit if profiling
    // shows memory pressure or page-cache thrash.
    conn.execute_batch(
        "
        PRAGMA foreign_keys       = ON;
        PRAGMA synchronous        = NORMAL;
        PRAGMA busy_timeout       = 5000;
        PRAGMA trusted_schema     = OFF;
        PRAGMA journal_size_limit = 67108864;
        PRAGMA mmap_size          = 134217728;
        PRAGMA cache_size         = 2000;
        ",
    )?;
    if query_only {
        conn.execute_batch("PRAGMA query_only = ON;")?;
    }
    // Skip the close-time checkpoint + WAL delete. SQLite 3.51.0–3.51.1
    // (bundled by libsqlite3-sys 0.36.0) has a lock-order inversion between
    // `sqlite3WalClose`'s last-closer probe (`unixLock(EXCLUSIVE)` holds
    // pInode->pLockMutex, wants unixBigLock via unixIsSharingShmNode) and a
    // concurrent `unixClose` (holds unixBigLock, wants pLockMutex). deadpool-
    // sync closes each pooled connection on a detached blocking task at store
    // drop, so the writer + reader closes race and can ABBA-deadlock, wedging
    // the process (observed as CI test hangs). Fixed upstream in SQLite
    // 3.51.2; this flag removes the only reachable inversion arm until a
    // deadpool-sqlite release bundles a fixed SQLite. Auto-checkpoint during
    // normal operation is unaffected; the WAL is simply recovered on next
    // open instead of deleted at close.
    conn.set_db_config(
        deadpool_sqlite::rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
        true,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use deadpool_sqlite::rusqlite::Connection;
    use neutrino_store::StorageError;
    use tempfile::TempDir;

    use crate::SqliteStore;

    /// Exercises the `other => Err(Internal(_))` arm of the version
    /// gate. Open the store once to install the schema, mutate
    /// `user_version` on the bare file to a value the gate doesn't
    /// recognise, then re-open and assert the refusal.
    ///
    /// Raw `Connection::open` / `pragma_update` / `query_row` calls are
    /// wrapped in `tokio::task::spawn_blocking` so the
    /// `#[tokio::test]` current-thread runtime stays free to poll
    /// deadpool's pool-drop cleanup tasks that fire when the previous
    /// `SqliteStore` went out of scope. Calling rusqlite synchronously
    /// on the worker thread blocked the runtime in CI (the `Connection`
    /// drops scheduled via Pool drop couldn't make progress while the
    /// worker was inside `Connection::open`), surfacing as an indefinite
    /// hang on the two schema tests.
    #[tokio::test]
    async fn ensure_schema_refuses_unknown_user_version() {
        // TempDir (not NamedTempFile): these tests raw-open the DB file to
        // poke `user_version`, and TempDir's recursive drop also reaps the
        // WAL `-wal`/`-shm` sidecars a NamedTempFile would orphan.
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("schema_test.db");

        // First open installs the schema at the current version.
        {
            let _ = SqliteStore::open(&path).await.expect("first open");
        }

        // Bypass the store and rewrite user_version directly.
        let path_for_bump = path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&path_for_bump).expect("raw open");
            conn.pragma_update(None, "user_version", 999_i64)
                .expect("bump user_version");
        })
        .await
        .expect("blocking task panicked");

        let err = SqliteStore::open(&path)
            .await
            .expect_err("second open must refuse unknown schema version");
        assert!(
            matches!(err, StorageError::Internal(_)),
            "expected Internal, got {err:?}"
        );
    }

    /// Atomicity test for the schema-bundle transaction. Pre-seed the
    /// target file with a `rooms` table so the bundle's
    /// `CREATE TABLE rooms` fails partway through, then assert that
    /// `user_version` is still `0` afterwards — the transaction rolled
    /// back the version stamp along with everything else, so a
    /// follow-up open re-enters the `0 => …` arm instead of
    /// short-circuiting on the current-version arm with a partial schema.
    #[tokio::test]
    async fn ensure_schema_rolls_back_on_mid_bundle_failure() {
        // TempDir (not NamedTempFile): these tests raw-open the DB file to
        // poke `user_version`, and TempDir's recursive drop also reaps the
        // WAL `-wal`/`-shm` sidecars a NamedTempFile would orphan.
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("schema_test.db");

        // Pre-existing colliding `rooms` table. `CREATE TABLE rooms (…)`
        // in the bundle will fail with "table rooms already exists",
        // aborting the bundle's transaction.
        let path_for_seed = path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&path_for_seed).expect("raw open");
            conn.execute_batch("CREATE TABLE rooms (junk TEXT)")
                .expect("pre-seed colliding table");
        })
        .await
        .expect("blocking task panicked");

        let err = SqliteStore::open(&path)
            .await
            .expect_err("schema bundle must fail on the colliding table");
        // "table already exists" is SQLITE_ERROR, not a constraint
        // violation, so per `error.rs` it surfaces as Internal.
        assert!(
            matches!(err, StorageError::Internal(_)),
            "expected Internal, got {err:?}"
        );

        // Version stamp is part of the rolled-back txn, so the
        // file must still be at user_version = 0. The pre-existing
        // table survives (the txn rolled back, it didn't drop anything
        // that was there before). Both checks run on a single
        // `Connection` inside one `spawn_blocking` task — same
        // rationale as the doc-comment on the test above.
        let path_for_check = path.clone();
        let (version, rooms_count): (i64, i64) = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&path_for_check).expect("raw reopen");
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .expect("read user_version");
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema \
                     WHERE type = 'table' AND name = 'rooms'",
                    [],
                    |r| r.get(0),
                )
                .expect("check rooms table");
            (version, exists)
        })
        .await
        .expect("blocking task panicked");
        assert_eq!(
            version, 0,
            "user_version must roll back to 0 on bundle failure"
        );
        assert_eq!(
            rooms_count, 1,
            "pre-existing rooms table must survive rollback"
        );
    }
}
