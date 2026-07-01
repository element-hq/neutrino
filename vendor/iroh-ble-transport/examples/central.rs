//! BLE central (client) example.
//!
//! Scans for a peripheral advertising the example service UUID (or optionally
//! filtered by name), connects to it, reads the target characteristic, then
//! subscribes to notifications and prints them as they arrive.
//!
//! Run with:
//!   cargo run --example central -p iroh-ble-transport
//!   cargo run --example central -p iroh-ble-transport -- --name my-device
//!
//! Start `cargo run --example peripheral -p iroh-ble-transport` first so there is something to connect to.

use iroh_ble_transport::{Central, CentralEvent, DeviceId, ScanFilter};
use std::{env, time::Duration};
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

// Must match the peripheral example.
const SERVICE_UUID: Uuid = uuid!("12345678-0000-1000-8000-00805f9b34fb");
const CHAR_UUID: Uuid = uuid!("12345678-0001-1000-8000-00805f9b34fb");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let name_filter: Option<String> = {
        let args: Vec<String> = env::args().collect();
        args.windows(2)
            .find(|w| w[0] == "--name")
            .map(|w| w[1].clone())
    };

    if let Some(ref name) = name_filter {
        println!("Scanning for device named '{name}'...");
    } else {
        println!("Scanning for service {SERVICE_UUID}...");
    }
    println!("Press Ctrl+C to stop.\n");

    let central: Central = Central::new().await?;
    let mut events = central.events();

    central
        .start_scan(ScanFilter {
            services: vec![SERVICE_UUID],
            ..Default::default()
        })
        .await?;

    let device_id = tokio::select! {
        id = wait_for_device(&mut events, &name_filter) => id?,
        _ = time::sleep(Duration::from_secs(15)) => {
            eprintln!("No matching device found within 15 s.");
            return Ok(());
        }
        _ = shutdown_signal() => return Ok(()),
    };

    central.stop_scan().await?;
    println!("Connecting to {device_id}...");
    central.connect(&device_id).await?;

    while let Some(event) = events.next().await {
        if let CentralEvent::DeviceConnected { device_id: ref id } = event
            && *id == device_id
        {
            println!("Connected to {device_id}\n");
            break;
        }
    }

    let services = central.discover_services(&device_id).await?;
    println!("Discovered {} service(s):", services.len());
    for svc in &services {
        println!("  service {}  (primary={})", svc.uuid, svc.primary);
        for c in &svc.characteristics {
            println!("    char {}  props={:?}", c.uuid, c.properties);
        }
    }
    println!();

    let value = central.read_characteristic(&device_id, CHAR_UUID).await?;
    println!("Read:  {}", String::from_utf8_lossy(&value));

    let msg = b"hello from central".to_vec();
    central
        .write_characteristic(
            &device_id,
            CHAR_UUID,
            msg,
            iroh_ble_transport::WriteType::WithResponse,
        )
        .await?;
    println!("Wrote: \"hello from central\"");

    central
        .subscribe_characteristic(&device_id, CHAR_UUID)
        .await?;
    println!("Subscribed. Waiting for notifications... (Ctrl+C to stop)\n");

    loop {
        tokio::select! {
            event = events.next() => {
                match event {
                    Some(CentralEvent::CharacteristicNotification {
                        device_id: ref id,
                        char_uuid,
                        ref value,
                    }) if *id == device_id => {
                        println!(
                            "notify  char={}  value={}",
                            char_uuid,
                            String::from_utf8_lossy(value),
                        );
                    }

                    Some(CentralEvent::DeviceDisconnected { device_id: ref id, .. })
                        if *id == device_id =>
                    {
                        println!("Device disconnected.");
                        break;
                    }

                    Some(_) => {}
                    None => break,
                }
            }

            _ = shutdown_signal() => {
                println!("\nDisconnecting...");
                let _ = central.unsubscribe_characteristic(&device_id, CHAR_UUID).await;
                let _ = central.disconnect(&device_id).await;
                break;
            }
        }
    }

    Ok(())
}

async fn wait_for_device(
    events: &mut (impl tokio_stream::Stream<Item = CentralEvent> + Unpin),
    name_filter: &Option<String>,
) -> Result<DeviceId, Box<dyn std::error::Error>> {
    while let Some(event) = events.next().await {
        if let CentralEvent::DeviceDiscovered(device) = event {
            let display_name = device.name.as_deref().unwrap_or("<unknown>");
            println!(
                "Found: {}  name={display_name:?}  rssi={:?}",
                device.id, device.rssi
            );

            let matches = match name_filter {
                Some(filter) => device.name.as_deref() == Some(filter.as_str()),
                None => true, // take the first device advertising our service UUID
            };

            if matches {
                return Ok(device.id);
            }
        }
    }
    Err("event stream closed".into())
}
