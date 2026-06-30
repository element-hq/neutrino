//! BLE peripheral (server) example.
//!
//! Advertises a single GATT service and handles read/write requests from
//! connected centrals. Also pushes a notification every 2 seconds to any
//! subscribed centrals.
//!
//! Run with:
//!   cargo run --example peripheral -p iroh-ble-transport
//!
//! Then run `cargo run --example central -p iroh-ble-transport` on the same or another
//! machine to connect to it.

use iroh_ble_transport::{
    AdvertisingConfig, AttributePermissions, CharacteristicProperties, DeviceId,
    GattCharacteristic, GattService, Peripheral, PeripheralRequest, PeripheralStateEvent,
};
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time;
use tokio_stream::StreamExt as _;
use uuid::{Uuid, uuid};

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
}

// Must match the central example.
const SERVICE_UUID: Uuid = uuid!("12345678-0000-1000-8000-00805f9b34fb");
const CHAR_UUID: Uuid = uuid!("12345678-0001-1000-8000-00805f9b34fb");
const DEVICE_NAME: &str = "iroh-ble-example";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let peripheral: Peripheral = Peripheral::new().await?;
    let mut state_events = peripheral.state_events();
    let mut requests = peripheral
        .take_requests()
        .expect("fresh Peripheral: take_requests must succeed on first call");

    // CoreBluetooth initialises asynchronously.
    if !peripheral.is_powered().await? {
        println!("Waiting for Bluetooth adapter...");
        while let Some(event) = state_events.next().await {
            match event {
                PeripheralStateEvent::AdapterStateChanged { powered: true } => break,
                PeripheralStateEvent::AdapterStateChanged { powered: false } => {
                    eprintln!("Bluetooth is powered off. Please enable Bluetooth and retry.");
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    peripheral
        .add_service(&GattService {
            uuid: SERVICE_UUID,
            primary: true,
            characteristics: vec![GattCharacteristic {
                uuid: CHAR_UUID,
                properties: CharacteristicProperties::READ
                    | CharacteristicProperties::WRITE
                    | CharacteristicProperties::NOTIFY,
                permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                // CoreBluetooth crashes if a writable characteristic has a static value.
                value: vec![],
                descriptors: vec![],
            }],
        })
        .await?;

    peripheral
        .start_advertising(&AdvertisingConfig {
            local_name: DEVICE_NAME.to_string(),
            service_uuids: vec![SERVICE_UUID],
        })
        .await?;

    println!("Advertising as '{DEVICE_NAME}'. Waiting for connections...");
    println!("Press Ctrl+C to stop.\n");

    let mut counter: u32 = 0;
    let mut notify_interval = time::interval(Duration::from_secs(2));
    notify_interval.tick().await; // skip the immediate first tick

    let subscribers: Mutex<HashSet<DeviceId>> = Mutex::new(HashSet::new());

    loop {
        tokio::select! {
            event = state_events.next() => {
                match event {
                    Some(event) => handle_state_event(event, &subscribers),
                    None => break,
                }
            }

            req = requests.next() => {
                match req {
                    Some(req) => handle_request(req),
                    None => break,
                }
            }

            _ = notify_interval.tick() => {
                counter += 1;
                let value = format!("count={counter}").into_bytes();
                let targets: Vec<DeviceId> = subscribers.lock().unwrap().iter().cloned().collect();
                if targets.is_empty() {
                    continue;
                }
                println!("-> notify: count={counter} (to {} subscribers)", targets.len());
                for device_id in targets {
                    if let Err(e) = peripheral
                        .notify_characteristic(&device_id, CHAR_UUID, value.clone())
                        .await
                    {
                        eprintln!("notify error for {device_id}: {e}");
                    }
                }
            }

            _ = shutdown_signal() => {
                println!("\nStopping advertisement...");
                peripheral.stop_advertising().await?;
                break;
            }
        }
    }

    Ok(())
}

fn handle_state_event(event: PeripheralStateEvent, subscribers: &Mutex<HashSet<DeviceId>>) {
    match event {
        PeripheralStateEvent::AdapterStateChanged { powered } => {
            println!("Adapter powered: {powered}");
        }

        PeripheralStateEvent::SubscriptionChanged {
            client_id,
            char_uuid,
            subscribed,
        } => {
            let action = if subscribed {
                "subscribed to"
            } else {
                "unsubscribed from"
            };
            println!("<- {client_id} {action} {char_uuid}");
            if char_uuid == CHAR_UUID {
                let mut set = subscribers.lock().unwrap();
                if subscribed {
                    set.insert(client_id);
                } else {
                    set.remove(&client_id);
                }
            }
        }
    }
}

fn handle_request(req: PeripheralRequest) {
    match req {
        PeripheralRequest::Read {
            client_id,
            char_uuid,
            offset,
            responder,
            ..
        } => {
            println!("<- read  from {client_id}  char={char_uuid}  offset={offset}");
            responder.respond(b"hello from peripheral".to_vec());
        }

        PeripheralRequest::Write {
            client_id,
            char_uuid,
            value,
            responder,
            ..
        } => {
            let text = String::from_utf8_lossy(&value);
            println!("<- write from {client_id}  char={char_uuid}  value={text:?}");
            if let Some(r) = responder {
                r.success();
            }
        }
    }
}
