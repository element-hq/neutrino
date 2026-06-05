//! `FederationInbox` impl on `SqliteStore`.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::params;
use neutrino_store::{FederationInbox, StorageError};
use ruma::ServerName;

use crate::{SqliteStore, error::Error};

#[async_trait]
impl FederationInbox for SqliteStore {
    async fn federation_txn_seen(
        &self,
        origin: &ServerName,
        txn_id: &str,
    ) -> Result<bool, StorageError> {
        let origin = origin.to_owned();
        let txn_id = txn_id.to_owned();

        self.run_read(move |conn| -> Result<bool, Error> {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM federation_txns WHERE origin = ? AND txn_id = ?)",
                params![origin.as_str(), txn_id],
                |row| row.get(0),
            )
            .map_err(Error::from)
        })
        .await
    }

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

#[cfg(test)]
mod tests {
    use neutrino_store::FederationInbox;
    use ruma::server_name;

    use crate::tests::store;

    // I1: first record returns false (was not recorded).
    #[tokio::test]
    async fn record_federation_txn_first_returns_false() {
        let s = store().await;
        let already = s
            .record_federation_txn(server_name!("matrix.org"), "txn1")
            .await
            .unwrap();
        assert!(!already);
    }

    // I2: second record with the same key returns true (was recorded).
    #[tokio::test]
    async fn record_federation_txn_second_returns_true() {
        let s = store().await;
        s.record_federation_txn(server_name!("matrix.org"), "txn1")
            .await
            .unwrap();
        let already = s
            .record_federation_txn(server_name!("matrix.org"), "txn1")
            .await
            .unwrap();
        assert!(already);
    }

    // I3: independent (origin, txn_id) pairs don't interfere.
    #[tokio::test]
    async fn record_federation_txn_independent_keys() {
        let s = store().await;
        let origin_a = server_name!("a.example.com");
        let origin_b = server_name!("b.example.com");

        // First time for each pair → all three return false.
        assert!(!s.record_federation_txn(origin_a, "txn1").await.unwrap());
        assert!(!s.record_federation_txn(origin_b, "txn1").await.unwrap()); // diff origin, same txn
        assert!(!s.record_federation_txn(origin_a, "txn2").await.unwrap()); // same origin, diff txn

        // Repeat → all three return true.
        assert!(s.record_federation_txn(origin_a, "txn1").await.unwrap());
        assert!(s.record_federation_txn(origin_b, "txn1").await.unwrap());
        assert!(s.record_federation_txn(origin_a, "txn2").await.unwrap());
    }

    // I5: `federation_txn_seen` reports membership without recording.
    #[tokio::test]
    async fn federation_txn_seen_does_not_record() {
        let s = store().await;
        let origin = server_name!("matrix.org");
        // Not yet recorded — seen is false, and checking does not record it.
        assert!(!s.federation_txn_seen(origin, "txn1").await.unwrap());
        assert!(!s.federation_txn_seen(origin, "txn1").await.unwrap());
        // Recording it then makes seen true.
        assert!(!s.record_federation_txn(origin, "txn1").await.unwrap());
        assert!(s.federation_txn_seen(origin, "txn1").await.unwrap());
    }

    // I4: SQL injection defence. A malicious txn_id is stored verbatim
    // (not interpreted), so the literal string is dedup'd on retry and
    // the `federation_txns` table is intact afterwards.
    #[tokio::test]
    async fn record_federation_txn_sql_injection_safe() {
        let s = store().await;
        let origin = server_name!("matrix.org");
        let malicious = "'; DROP TABLE federation_txns; --";

        // First call — stored verbatim.
        assert!(!s.record_federation_txn(origin, malicious).await.unwrap());
        // Second call — table still exists, the literal string round-trips.
        assert!(s.record_federation_txn(origin, malicious).await.unwrap());
        // An unrelated txn_id still works — table was not dropped.
        assert!(!s.record_federation_txn(origin, "normal_txn").await.unwrap());
    }
}
