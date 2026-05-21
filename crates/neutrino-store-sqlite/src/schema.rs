use deadpool_sqlite::rusqlite::Connection;

use crate::error::Error;

const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Version-gate the database against the bundled V1 schema. Authoritative
/// open-time check per design doc §2 "Open path: version gate & schema bundle".
///
/// | `user_version` | Action                                              |
/// |----------------|-----------------------------------------------------|
/// | `0`            | Fresh DB. Apply `journal_mode = WAL`, then run the  |
/// |                | schema bundle + `user_version = 1` inside one txn.  |
/// | `1`            | Already at current schema. Skip the bundle.         |
/// | other          | Unknown version. Refuse to open — defensive bail.   |
///
/// Atomicity: the schema DDL and the `user_version = 1` stamp run inside
/// the same transaction, so a mid-bundle failure rolls both back. The
/// next open sees `user_version = 0` and re-runs the (non-`IF NOT
/// EXISTS`) bundle, which either succeeds or fails loudly on a
/// colliding pre-existing table.
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
            tx.execute_batch("PRAGMA user_version = 1")?;
            tx.commit()?;
            Ok(())
        }
        1 => Ok(()),
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use deadpool_sqlite::rusqlite::Connection;
    use neutrino_store::StorageError;
    use tempfile::NamedTempFile;

    use crate::SqliteStore;

    /// Exercises the `other => Err(Internal(_))` arm of the version
    /// gate. Open the store once to install the schema, mutate
    /// `user_version` on the bare file to a value the gate doesn't
    /// recognise, then re-open and assert the refusal.
    #[tokio::test]
    async fn ensure_schema_refuses_unknown_user_version() {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.path();

        // First open installs the schema, leaving user_version = 1.
        {
            let _ = SqliteStore::open(path).await.expect("first open");
        }

        // Bypass the store and rewrite user_version directly.
        {
            let conn = Connection::open(path).expect("raw open");
            conn.pragma_update(None, "user_version", 999_i64)
                .expect("bump user_version");
        }

        let err = SqliteStore::open(path)
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
    /// short-circuiting on the `1 => Ok(())` arm with a partial schema.
    #[tokio::test]
    async fn ensure_schema_rolls_back_on_mid_bundle_failure() {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.path();

        // Pre-existing colliding `rooms` table. `CREATE TABLE rooms (…)`
        // in the bundle will fail with "table rooms already exists",
        // aborting the bundle's transaction.
        {
            let conn = Connection::open(path).expect("raw open");
            conn.execute_batch("CREATE TABLE rooms (junk TEXT)")
                .expect("pre-seed colliding table");
        }

        let err = SqliteStore::open(path)
            .await
            .expect_err("schema bundle must fail on the colliding table");
        // "table already exists" is SQLITE_ERROR, not a constraint
        // violation, so per `error.rs` it surfaces as Internal.
        assert!(
            matches!(err, StorageError::Internal(_)),
            "expected Internal, got {err:?}"
        );

        // Version stamp is part of the rolled-back txn, so the
        // file must still be at user_version = 0.
        let conn = Connection::open(path).expect("raw reopen");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .expect("read user_version");
        assert_eq!(
            version, 0,
            "user_version must roll back to 0 on bundle failure"
        );
        // The pre-existing table survives (the txn rolled back, it
        // didn't drop anything that was there before). Its presence is
        // *also* what makes the next open re-fail — exactly the
        // "fails loudly on the colliding table" property the design
        // doc calls out.
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema \
                 WHERE type = 'table' AND name = 'rooms'",
                [],
                |r| r.get(0),
            )
            .expect("check rooms table");
        assert_eq!(exists, 1, "pre-existing rooms table must survive rollback");
    }
}
