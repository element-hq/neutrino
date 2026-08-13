use deadpool_sqlite::{BuildError, InteractError, PoolError, rusqlite};
use neutrino_store::StorageError;
use rusqlite::ErrorCode;
use thiserror::Error;

/// Aggregates every error source the SQLite backend can observe (driver, pool,
/// JSON crack), and the error type a closure handed to
/// [`SqliteStore::run_read`](crate::SqliteStore::run_read) /
/// [`run_write`](crate::SqliteStore::run_write) returns. The single
/// `From<Error> for StorageError` impl in this file is the canonical mapping
/// point — design doc §3, the "single point of variant-selection".
///
/// Orphan rule: `StorageError` lives in `neutrino-store`, which does not (and
/// must not) depend on `rusqlite`/`deadpool`. We can't write
/// `From<rusqlite::Error> for StorageError` there, so this local enum is the
/// only place those `From` impls can live.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(transparent)]
    Pool(#[from] PoolError),

    #[error(transparent)]
    Build(#[from] BuildError),

    #[error(transparent)]
    Interact(#[from] InteractError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    InvalidInput(String),

    #[error("{0}")]
    Internal(String),
}

impl From<Error> for StorageError {
    fn from(e: Error) -> Self {
        match &e {
            // ConstraintViolation (FK, CHECK, NOT NULL, UNIQUE) ⇒ caller passed
            // something the schema rejects. Per design doc §3 mapping table.
            Error::Sqlite(rusqlite::Error::SqliteFailure(f, _))
                if matches!(f.code, ErrorCode::ConstraintViolation) =>
            {
                StorageError::InvalidInput(e.to_string())
            }
            // QueryReturnedNoRows is not an error at the storage layer — call
            // sites should map it to Ok(None) / empty vec before it reaches
            // here. If it bubbles up, it's a programming bug in the impl.
            Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) => {
                StorageError::Internal(e.to_string())
            }
            // Local discriminator variants — used when the impl needs to
            // signal InvalidInput without going through a SqliteFailure
            // (e.g. malformed event JSON).
            Error::InvalidInput(msg) => StorageError::InvalidInput(msg.clone()),
            Error::Internal(msg) => StorageError::Internal(msg.clone()),
            // Everything else maps to Internal: pool / interact / driver /
            // JSON deserialisation of rows we wrote ourselves.
            _ => StorageError::Internal(e.to_string()),
        }
    }
}
