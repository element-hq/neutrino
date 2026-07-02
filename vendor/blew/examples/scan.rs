//! BLE scanner example.
//!
//! Starts a scan for all nearby peripherals, prints each discovery for
//! 10 seconds, then stops.
//!
//! ```sh
//! cargo run --example scan -p blew
//! ```

use blew::Central;
use blew::central::{CentralEvent, ScanFilter};
use tokio_stream::StreamExt as _;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Explicit type annotation required -- Rust can't infer the default type param.
    let central: Central = Central::new().await?;

    let mut events = central.events();

    central.start_scan(ScanFilter::default()).await?;
    println!("Scanning for 10 s ... (Ctrl-C to stop early)");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        match tokio::time::timeout_at(deadline, events.next()).await {
            Ok(Some(CentralEvent::DeviceDiscovered(device))) => {
                let name = device.name.as_deref().unwrap_or("<unnamed>");
                let rssi = device
                    .rssi
                    .map(|r| format!("{r} dBm"))
                    .unwrap_or_else(|| "?".into());
                println!("  {:<40} {:>8}  {}", device.id, rssi, name);
            }
            Ok(Some(_)) => {}  // ignore connect/disconnect etc.
            Ok(None) => break, // event stream closed
            Err(_) => break,   // 10 s elapsed
        }
    }

    central.stop_scan().await?;
    println!("Done.");
    Ok(())
}
