#![cfg(feature = "testing")]

use blew::{Central, CentralConfig};

#[tokio::test]
#[ignore = "requires a real BLE adapter with TCC authorization"]
async fn with_config_accepts_restore_identifier() {
    let config = CentralConfig {
        restore_identifier: Some("com.example.test".into()),
    };
    let _central = Central::with_config(config).await.unwrap();
}

#[test]
fn central_config_default_has_no_restore_identifier() {
    let c = CentralConfig::default();
    assert!(c.restore_identifier.is_none());
}
