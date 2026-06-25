//! `IdentityStore` impl on [`crate::SqliteStore`].
//!
//! Persists the server's node secret (32 raw bytes) in the single-row
//! `node_identity` table. The secret seeds the server's stable identity (from
//! which its federation `server_name` is derived when unconfigured), so it must
//! survive restarts — stored once, on first start, and read back thereafter.
//!
//! The store does NOT generate the secret: SQLite's `randomblob` is not a
//! guaranteed CSPRNG, so the caller supplies a freshly-generated seed (from a
//! Rust CSPRNG) and the store keeps the first one written.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::params;
use neutrino_store::{IdentityStore, StorageError};

use crate::{SqliteStore, error::Error};

#[async_trait]
impl IdentityStore for SqliteStore {
    async fn get_or_create_node_secret(
        &self,
        fresh_seed: [u8; 32],
    ) -> Result<[u8; 32], StorageError> {
        self.run_write(move |conn| -> Result<[u8; 32], Error> {
            // First-write-wins, atomic in one write txn: store the caller's seed
            // if the row is absent, otherwise keep the existing secret. `INSERT
            // OR IGNORE` on the `id = 0` PK makes the first secret stable across
            // restarts — a later call (with a different `fresh_seed`) reads back
            // the original.
            conn.execute(
                "INSERT OR IGNORE INTO node_identity (id, secret) VALUES (0, ?)",
                params![fresh_seed.as_slice()],
            )?;
            let secret: Vec<u8> =
                conn.query_row("SELECT secret FROM node_identity WHERE id = 0", [], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?;
            // The `length(secret) = 32` CHECK guarantees this, but convert
            // fallibly rather than panic on a corrupt row.
            secret.try_into().map_err(|v: Vec<u8>| {
                Error::Internal(format!("node secret must be 32 bytes, got {}", v.len()))
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::IdentityStore;
    use tempfile::TempDir;

    use crate::SqliteStore;

    /// The first seed written wins (a later call with a different seed reads back
    /// the original), and it survives a reopen — the persisted identity is stable
    /// across restarts, which is the whole point of storing it.
    #[tokio::test]
    async fn node_secret_is_first_write_wins_and_stable() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("identity.db");
        let seed_a = [1u8; 32];
        let seed_b = [2u8; 32];

        let stored = {
            let store = SqliteStore::open(&path).await.expect("open");
            let first = store
                .get_or_create_node_secret(seed_a)
                .await
                .expect("create");
            assert_eq!(first, seed_a, "first seed is stored");
            let second = store
                .get_or_create_node_secret(seed_b)
                .await
                .expect("read back");
            assert_eq!(second, seed_a, "first write wins; a later seed is ignored");
            first
        };
        // Reopen the same file: a second start must see the same secret.
        let store = SqliteStore::open(&path).await.expect("reopen");
        let again = store
            .get_or_create_node_secret(seed_b)
            .await
            .expect("reopen read");
        assert_eq!(again, stored, "secret is stable across restarts");
    }
}
