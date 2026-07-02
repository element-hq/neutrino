//! BLE peripheral / advertiser example.
//!
//! Registers a simple GATT service with one read-write characteristic, starts
//! advertising, handles ATT requests for 30 seconds, then shuts down.
//!
//! ```sh
//! cargo run --example advertise -p blew
//! ```

use blew::Peripheral;
use blew::gatt::props::{AttributePermissions, CharacteristicProperties};
use blew::gatt::service::{GattCharacteristic, GattService};
use blew::peripheral::{AdvertisingConfig, PeripheralRequest, PeripheralStateEvent};
use tokio_stream::StreamExt as _;
use uuid::Uuid;

const SVC_UUID: Uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abc);
const CHAR_UUID: Uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abd);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Explicit type annotation required -- Rust can't infer the default type param.
    let peripheral: Peripheral = Peripheral::new().await?;

    peripheral
        .add_service(&GattService {
            uuid: SVC_UUID,
            primary: true,
            characteristics: vec![GattCharacteristic {
                uuid: CHAR_UUID,
                properties: CharacteristicProperties::READ | CharacteristicProperties::WRITE,
                permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                // CoreBluetooth crashes if a writable characteristic has a static value.
                value: vec![],
                descriptors: vec![],
            }],
        })
        .await?;

    let mut requests = peripheral.take_requests().expect("requests already taken");
    let mut state = peripheral.state_events();

    peripheral
        .start_advertising(&AdvertisingConfig {
            local_name: "blew-example".into(),
            service_uuids: vec![SVC_UUID],
        })
        .await?;
    println!("Advertising as \"blew-example\" for 30 s ... (Ctrl-C to stop early)");

    let state_task = tokio::spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                PeripheralStateEvent::AdapterStateChanged { powered } => {
                    println!("  adapter state: powered={powered}");
                }
                PeripheralStateEvent::SubscriptionChanged {
                    client_id,
                    char_uuid,
                    subscribed,
                } => {
                    println!(
                        "  subscription: client={client_id}  char={char_uuid}  \
                         subscribed={subscribed}"
                    );
                }
            }
        }
    });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);

    loop {
        match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(Some(request)) => match request {
                PeripheralRequest::Read {
                    client_id,
                    char_uuid,
                    offset,
                    responder,
                    ..
                } => {
                    println!("  read:  client={client_id}  char={char_uuid}  offset={offset}");
                    let reply = b"hello from blew".to_vec();
                    responder.respond(reply);
                }
                PeripheralRequest::Write {
                    client_id,
                    char_uuid,
                    value,
                    responder,
                    ..
                } => {
                    println!(
                        "  write: client={client_id}  char={char_uuid}  \
                         value={:?}",
                        String::from_utf8_lossy(&value)
                    );
                    if let Some(r) = responder {
                        r.success();
                    }
                }
            },
            Ok(None) => break, // request stream closed
            Err(_) => break,   // 30 s elapsed
        }
    }

    state_task.abort();
    peripheral.stop_advertising().await?;
    println!("Done.");
    Ok(())
}
