//! L2CAP CoC echo server.
//!
//! Publishes an L2CAP CoC channel (Apple assigns the PSM dynamically), then
//! exposes that PSM via a readable GATT characteristic so the client example
//! can discover it.  Every incoming byte is echoed back.
//!
//! Run this first, then run `l2cap_client` on a second device.
//!
//! ```sh
//! cargo run --example l2cap_server -p blew
//! ```

use blew::Peripheral;
use blew::gatt::props::{AttributePermissions, CharacteristicProperties};
use blew::gatt::service::{GattCharacteristic, GattService};
use blew::peripheral::{AdvertisingConfig, PeripheralRequest};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt as _;
use uuid::Uuid;

// Must match the client example.
const SVC_UUID: Uuid = Uuid::from_u128(0x13b0c000_b0b0_1234_5678_000000000001);
// Holds the L2CAP PSM as 2 bytes LE; served dynamically via ReadRequest.
const PSM_CHAR_UUID: Uuid = Uuid::from_u128(0x13b0c000_b0b0_1234_5678_000000000002);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let peripheral: Peripheral = Peripheral::new().await?;

    peripheral
        .add_service(&GattService {
            uuid: SVC_UUID,
            primary: true,
            characteristics: vec![GattCharacteristic {
                uuid: PSM_CHAR_UUID,
                properties: CharacteristicProperties::READ,
                permissions: AttributePermissions::READ,
                value: vec![],
                descriptors: vec![],
            }],
        })
        .await?;

    let mut requests = peripheral.take_requests().expect("requests already taken");

    // On Apple the OS assigns a dynamic PSM.
    let (psm, mut l2cap_channels) = peripheral.l2cap_listener().await?;
    let psm_bytes = psm.value().to_le_bytes().to_vec();
    println!("L2CAP PSM assigned: {}", psm.value());

    peripheral
        .start_advertising(&AdvertisingConfig {
            local_name: "blew-l2cap".into(),
            service_uuids: vec![SVC_UUID],
        })
        .await?;
    println!("Advertising as \"blew-l2cap\" ... (Ctrl-C to stop)");

    loop {
        tokio::select! {
            request = requests.next() => {
                let Some(request) = request else { break };
                match request {
                    PeripheralRequest::Read { char_uuid, responder, .. }
                        if char_uuid == PSM_CHAR_UUID =>
                    {
                        responder.respond(psm_bytes.clone());
                    }
                    _ => {}
                }
            }
            incoming = l2cap_channels.next() => {
                let Some(result) = incoming else { break };
                match result {
                    Ok((device_id, mut ch)) => {
                        println!("  L2CAP connection accepted from {device_id} -- echoing ...");
                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 4096];
                            let mut total_bytes: usize = 0;
                            let mut start: Option<Instant> = None;
                            loop {
                                match ch.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        if start.is_none() {
                                            start = Some(Instant::now());
                                        }
                                        total_bytes += n;
                                        if ch.write_all(&buf[..n]).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            if let Some(start) = start {
                                let elapsed = start.elapsed();
                                let kbps = (total_bytes as f64 / 1024.0) / elapsed.as_secs_f64();
                                println!(
                                    "  L2CAP connection closed -- received {} KB in {:.2?}  ->  {:.1} KB/s ({:.1} kbps)",
                                    total_bytes / 1024,
                                    elapsed,
                                    kbps,
                                    kbps * 8.0,
                                );
                            } else {
                                println!("  L2CAP connection closed.");
                            }
                        });
                    }
                    Err(e) => eprintln!("  L2CAP accept error: {e}"),
                }
            }
        }
    }

    peripheral.stop_advertising().await?;
    println!("Done.");
    Ok(())
}
