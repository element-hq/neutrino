//! `FederationInbox` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::params;
use neutrino_store::{FederationInbox, StorageError};
use ruma::ServerName;

use crate::{SqliteStore, error::Error};

#[async_trait]
impl FederationInbox for SqliteStore {
    async fn record_federation_txn(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<bool, StorageError> {
        let origin = origin.to_owned();
        let txn_id = txn_id.to_owned();

        self.run_write(move |conn| -> Result<bool, Error> {
            // INSERT OR IGNORE + `conn.changes() == 0` ⇒ row already
            // existed, return `true`. Per design doc §3 + trait
            // post-condition: "returns true if it was already recorded".
            conn.execute(
                "INSERT OR IGNORE INTO federation_txns (origin, txn_id) VALUES (?, ?)",
                params![origin.as_str(), txn_id],
            )?;
            Ok(conn.changes() == 0)
        })
        .await
    }
}
