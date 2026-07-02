#![cfg(feature = "testing")]

use blew::testing::{MockCentral, MockPeripheral};
use std::time::Duration;

#[tokio::test]
async fn central_wait_ready_returns_immediately_when_already_powered() {
    let central = MockCentral::new_powered();
    central.wait_ready(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn central_wait_ready_waits_for_power_on_event() {
    let central = MockCentral::new_unpowered();
    let handle = central.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.mock_emit_adapter_state(true);
    });
    central.wait_ready(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn central_wait_ready_times_out_if_never_powered() {
    let central = MockCentral::new_unpowered();
    let err = central
        .wait_ready(Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(matches!(err, blew::error::BlewError::Timeout));
}

#[tokio::test]
async fn peripheral_wait_ready_returns_immediately_when_already_powered() {
    let peripheral = MockPeripheral::new_powered();
    peripheral.wait_ready(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn peripheral_wait_ready_waits_for_power_on_event() {
    let peripheral = MockPeripheral::new_unpowered();
    let handle = peripheral.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.mock_emit_adapter_state(true);
    });
    peripheral.wait_ready(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn peripheral_wait_ready_times_out_if_never_powered() {
    let peripheral = MockPeripheral::new_unpowered();
    let err = peripheral
        .wait_ready(Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(matches!(err, blew::error::BlewError::Timeout));
}
