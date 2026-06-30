//! Integration tests for the L2CAP data path using blew mock backends.

#![allow(clippy::unwrap_used)]

use blew::testing::{MockErrorKind, MockL2capPolicy, MockLink};

#[tokio::test]
async fn mock_peripheral_advertises_psm_when_l2cap_supported() {
    use blew::peripheral::backend::PeripheralBackend;
    let (_central_ep, periph_ep) = MockLink::pair();
    let (psm, _stream) = periph_ep.peripheral.l2cap_listener().await.unwrap();
    assert_eq!(psm.value(), 0x1001);
}

#[tokio::test]
async fn mock_l2cap_full_round_trip() {
    use blew::central::backend::CentralBackend;
    use blew::peripheral::backend::PeripheralBackend;
    use blew::types::DeviceId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::StreamExt;

    let (central_ep, periph_ep) = MockLink::pair();
    let (psm, mut listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();

    let device_id = DeviceId::from("mock-peripheral");
    let open_fut = central_ep.central.open_l2cap_channel(&device_id, psm);
    let accept_fut = listener.next();
    let (central_side, accepted) = tokio::join!(open_fut, accept_fut);
    let mut central_side = central_side.unwrap();
    let (_accepted_device_id, mut periph_side) = accepted.unwrap().unwrap();

    central_side.write_all(b"hello").await.unwrap();
    let mut buf = [0_u8; 5];
    periph_side.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
}

#[tokio::test]
async fn l2cap_open_error_policy_forces_fallback() {
    use blew::central::backend::CentralBackend;
    use blew::peripheral::backend::PeripheralBackend;
    use blew::types::DeviceId;

    let policy = MockL2capPolicy {
        open_error: Some(MockErrorKind::NotSupported),
        ..Default::default()
    };
    let (central_ep, periph_ep) = MockLink::pair_with_policy(policy);
    let (psm, _listener) = periph_ep.peripheral.l2cap_listener().await.unwrap();
    let device_id = DeviceId::from("mock-peripheral");
    let result = central_ep.central.open_l2cap_channel(&device_id, psm).await;
    assert!(result.is_err(), "open should fail under policy");
}

#[tokio::test]
async fn l2cap_listener_error_policy_means_no_psm() {
    use blew::peripheral::backend::PeripheralBackend;

    let policy = MockL2capPolicy {
        listener_error: Some(MockErrorKind::NotSupported),
        ..Default::default()
    };
    let (_central_ep, periph_ep) = MockLink::pair_with_policy(policy);
    let result = periph_ep.peripheral.l2cap_listener().await;
    assert!(result.is_err(), "listener should fail under policy");
}
