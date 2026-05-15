use deadpool_sqlite::rusqlite::Connection;

use crate::error::Error;

const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Version-gate the database against the bundled V1 schema. Authoritative
/// open-time check per design doc §2 "Open path: version gate & schema bundle".
///
/// | `user_version` | Action                                              |
/// |----------------|-----------------------------------------------------|
/// | `0`            | Fresh DB. Execute `schema.sql` (which itself sets   |
/// |                | `journal_mode=WAL` and bumps `user_version=1`).     |
/// | `1`            | Already at current schema. Skip the bundle.         |
/// | other          | Unknown version. Refuse to open — defensive bail.   |
///
/// `schema.sql` includes `PRAGMA journal_mode = WAL` which SQLite forbids
/// inside an active transaction. We therefore run `execute_batch` outside
/// any txn wrapper. Individual `CREATE TABLE` statements still succeed or
/// fail atomically; a partial-bundle failure leaves `user_version` at `0`,
/// and a subsequent open re-runs the plain (non-`IF NOT EXISTS`) bundle,
/// which fails loudly on the colliding table — exactly the design-doc
/// "fails loudly" failure mode.
pub(crate) fn ensure_schema(conn: &mut Connection) -> Result<(), Error> {
    let v: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    match v {
        0 => {
            conn.execute_batch(SCHEMA_SQL)?;
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
pub(crate) fn apply_connection_pragmas(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys   = ON;
        PRAGMA synchronous    = NORMAL;
        PRAGMA busy_timeout   = 5000;
        PRAGMA trusted_schema = OFF;
        ",
    )?;
    Ok(())
}
