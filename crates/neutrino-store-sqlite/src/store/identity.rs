//! `IdentityStore` impl on [`crate::SqliteStore`].
//!
//! Persists the server's identity facts in the key/value `server_identity`
//! table: the node secret (`key = 'secret'`, 32 raw bytes) and the local user's
//! display name (`key = 'displayname'`, text). Both must survive restarts.
//!
//! The store does NOT generate the secret: SQLite's `randomblob` is not a
//! guaranteed CSPRNG, so the caller supplies a freshly-generated seed (from a
//! Rust CSPRNG) and the store keeps the first one written.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_store::{IdentityStore, StorageError};

use crate::{SqliteStore, error::Error};

/// The single key under which the 32-byte node secret is stored.
const KEY_SECRET: &str = "secret";
/// The single key under which the local user's display name is stored.
const KEY_DISPLAYNAME: &str = "displayname";

#[async_trait]
impl IdentityStore for SqliteStore {
    async fn get_or_create_node_secret(
        &self,
        fresh_seed: [u8; 32],
    ) -> Result<[u8; 32], StorageError> {
        self.run_write(move |conn| -> Result<[u8; 32], Error> {
            // First-write-wins, atomic in one write txn: store the caller's seed
            // if the row is absent, otherwise keep the existing secret. `INSERT
            // OR IGNORE` on the `key` PK makes the first secret stable across
            // restarts — a later call (with a different `fresh_seed`) reads back
            // the original.
            conn.execute(
                "INSERT OR IGNORE INTO server_identity (key, value) VALUES (?, ?)",
                params![KEY_SECRET, fresh_seed.as_slice()],
            )?;
            let secret: Vec<u8> = conn.query_row(
                "SELECT value FROM server_identity WHERE key = ?",
                params![KEY_SECRET],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            // The `length = 32` CHECK guarantees this, but convert fallibly
            // rather than panic on a corrupt row.
            secret.try_into().map_err(|v: Vec<u8>| {
                Error::Internal(format!("node secret must be 32 bytes, got {}", v.len()))
            })
        })
        .await
    }

    async fn get_display_name(&self) -> Result<Option<String>, StorageError> {
        self.run_read(move |conn| -> Result<Option<String>, Error> {
            Ok(conn
                .query_row(
                    "SELECT value FROM server_identity WHERE key = ?",
                    params![KEY_DISPLAYNAME],
                    |row| row.get::<_, String>(0),
                )
                .optional()?)
        })
        .await
    }

    async fn set_display_name(&self, name: &str) -> Result<(), StorageError> {
        let name = name.to_owned();
        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "INSERT INTO server_identity (key, value) VALUES (?, ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![KEY_DISPLAYNAME, name],
            )?;
            Ok(())
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

    /// Display name is `None` until set, round-trips, is overwritten by a later
    /// set (not appended), and survives a reopen.
    #[tokio::test]
    async fn display_name_round_trips_and_persists() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("identity.db");

        {
            let store = SqliteStore::open(&path).await.expect("open");
            assert_eq!(
                store.get_display_name().await.expect("get"),
                None,
                "unset display name reads back as None"
            );
            store.set_display_name("Alice").await.expect("set");
            assert_eq!(
                store.get_display_name().await.expect("get"),
                Some("Alice".to_string())
            );
            store.set_display_name("Bob").await.expect("overwrite");
            assert_eq!(
                store.get_display_name().await.expect("get"),
                Some("Bob".to_string()),
                "a later set replaces, not appends"
            );
        }
        // Reopen: the display name persists across restarts.
        let store = SqliteStore::open(&path).await.expect("reopen");
        assert_eq!(
            store.get_display_name().await.expect("reopen read"),
            Some("Bob".to_string())
        );
    }

    /// The secret and display name share the table without colliding.
    #[tokio::test]
    async fn secret_and_display_name_coexist() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("identity.db");
        let store = SqliteStore::open(&path).await.expect("open");
        let secret = store
            .get_or_create_node_secret([7u8; 32])
            .await
            .expect("secret");
        store.set_display_name("Carol").await.expect("set");
        assert_eq!(
            store
                .get_or_create_node_secret([9u8; 32])
                .await
                .expect("re"),
            secret,
            "setting the display name must not disturb the secret"
        );
        assert_eq!(
            store.get_display_name().await.expect("get"),
            Some("Carol".to_string())
        );
    }
}
