//! L2CAP CoC echo client.
//!
//! Scans for the `l2cap_server` example by its service UUID, connects,
//! reads the PSM from the GATT characteristic, opens an L2CAP CoC channel,
//! sends a test message, prints the echoed reply, then runs a speed test.
//!
//! Start the server first on a separate device (same-machine BLE does not
//! work on macOS/CoreBluetooth):
//!
//! ```sh
//! # device A:
//! cargo run --example l2cap_server -p blew
//!
//! # device B:
//! cargo run --example l2cap_client -p blew
//! ```

use blew::Central;
use blew::central::{CentralEvent, ScanFilter};
use blew::l2cap::Psm;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt as _;
use uuid::Uuid;

const SPEEDTEST_BYTES: usize = 256 * 1024;
const CHUNK_SIZE: usize = 4096;

// Must match the server example.
const SVC_UUID: Uuid = Uuid::from_u128(0x13b0c000_b0b0_1234_5678_000000000001);
const PSM_CHAR_UUID: Uuid = Uuid::from_u128(0x13b0c000_b0b0_1234_5678_000000000002);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let central: Central = Central::new().await?;

    let mut events = central.events();

    central
        .start_scan(ScanFilter {
            services: vec![SVC_UUID],
            ..Default::default()
        })
        .await?;
    println!("Scanning for \"blew-l2cap\" server ...");

    // BlueZ can surface cached devices that don't match the filter,
    // so check the advertised service list explicitly.
    let device_id = loop {
        match events.next().await {
            Some(CentralEvent::DeviceDiscovered(d)) => {
                if !d.services.contains(&SVC_UUID) {
                    println!(
                        "Skipping: {}  ({}) -- no matching service UUID",
                        d.name.as_deref().unwrap_or("<unnamed>"),
                        d.id
                    );
                    continue;
                }
                println!(
                    "Found: {}  ({})",
                    d.name.as_deref().unwrap_or("<unnamed>"),
                    d.id
                );
                break d.id;
            }
            Some(_) => {}
            None => return Err("event stream closed before device found".into()),
        }
    };

    central.stop_scan().await?;

    println!("Connecting ...");
    central.connect(&device_id).await?;
    println!("Connected.");

    central.discover_services(&device_id).await?;

    let psm_data = central
        .read_characteristic(&device_id, PSM_CHAR_UUID)
        .await?;
    if psm_data.len() < 2 {
        return Err(format!(
            "PSM characteristic too short ({} bytes, expected 2)",
            psm_data.len()
        )
        .into());
    }
    let psm = Psm(u16::from_le_bytes([psm_data[0], psm_data[1]]));
    println!("PSM = {}", psm.value());

    println!("Opening L2CAP channel ...");
    let mut ch = central.open_l2cap_channel(&device_id, psm).await?;
    println!("L2CAP channel open.");

    let msg = b"Hello from blew L2CAP!";
    println!("Sending:  {:?}", std::str::from_utf8(msg).unwrap());
    ch.write_all(msg).await?;

    let mut buf = vec![0u8; CHUNK_SIZE];
    let n = ch.read(&mut buf).await?;
    println!(
        "Received: {:?}",
        std::str::from_utf8(&buf[..n]).unwrap_or("<binary>")
    );

    // Send and receive concurrently to keep the pipe full.
    println!(
        "\nSpeed test: {} KB in {} B chunks ...",
        SPEEDTEST_BYTES / 1024,
        CHUNK_SIZE
    );
    let (mut reader, mut writer) = tokio::io::split(ch);

    let start = Instant::now();

    let sender = async move {
        let chunk = vec![0x55u8; CHUNK_SIZE];
        let mut remaining = SPEEDTEST_BYTES;
        while remaining > 0 {
            let n = remaining.min(CHUNK_SIZE);
            writer.write_all(&chunk[..n]).await?;
            remaining -= n;
        }
        Ok::<_, std::io::Error>(())
    };

    let receiver = async move {
        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut remaining = SPEEDTEST_BYTES;
        while remaining > 0 {
            let n = reader.read(&mut buf).await?;
            remaining = remaining.saturating_sub(n);
        }
        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(sender, receiver)?;
    let elapsed = start.elapsed();

    let kbps = (SPEEDTEST_BYTES as f64 / 1024.0) / elapsed.as_secs_f64();
    println!(
        "Round-trip: {} KB in {:.2?}  ->  {:.1} KB/s ({:.1} kbps)",
        SPEEDTEST_BYTES / 1024,
        elapsed,
        kbps,
        kbps * 8.0,
    );
    println!("(Data travels twice -- server echoes; divide by 2 for unidirectional estimate.)");

    println!("\nDone.");
    Ok(())
}
