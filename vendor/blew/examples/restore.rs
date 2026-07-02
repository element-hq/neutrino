//! iOS state-restoration pattern.
//!
//! Demonstrates the correct launch sequence for apps that want the OS to hand back
//! preserved BLE state when it relaunches them in the background. Only Apple targets
//! expose `take_restored()`, so this example is Apple-only; other platforms get a
//! no-op stub.
//!
//! On iOS this would run synchronously from
//! `application:didFinishLaunchingWithOptions:`. On macOS, which shares the API but
//! not the backgrounding semantics, it's equivalent to a plain peripheral startup.
//!
//! Prerequisites for real state restoration on iOS:
//!
//! 1. Info.plist must declare `UIBackgroundModes` with `bluetooth-peripheral` (and/or
//!    `bluetooth-central`).
//! 2. The restore identifier must be stable across launches — hard-code it or load it
//!    before the BLE stack comes up.
//! 3. Construct `Central`/`Peripheral` as early as possible during app launch.
//!
//! ```sh
//! cargo run --example restore -p blew
//! ```
//!
//! See the crate-level `State restoration` rustdoc for the full contract.

#[cfg(not(target_vendor = "apple"))]
fn main() {
    println!("restore.rs is Apple-only; target_vendor != \"apple\" on this build.");
}

#[cfg(target_vendor = "apple")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use blew::central::{Central, CentralConfig};
    use blew::gatt::props::{AttributePermissions, CharacteristicProperties};
    use blew::gatt::service::{GattCharacteristic, GattService};
    use blew::peripheral::{AdvertisingConfig, Peripheral, PeripheralConfig};
    use uuid::Uuid;

    // The restore identifier must be stable across launches; the OS matches on it.
    const CENTRAL_RESTORE_ID: &str = "org.example.blew.central.restore";
    const PERIPHERAL_RESTORE_ID: &str = "org.example.blew.peripheral.restore";
    const SVC_UUID: Uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abc);
    const CHAR_UUID: Uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abd);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // --- Central side ---

    let central: Central = Central::with_config(CentralConfig {
        restore_identifier: Some(CENTRAL_RESTORE_ID.into()),
    })
    .await?;

    // Drain the restored peripheral list BEFORE issuing any new scans/connects.
    // On iOS this is `Some(..)` only when the OS relaunched the app; on other
    // platforms it is always `None`.
    match central.take_restored() {
        Some(devices) => {
            println!(
                "central restored by OS: {} preserved peripheral(s)",
                devices.len()
            );
            for device in &devices {
                println!(
                    "  - {} ({})",
                    device.name.as_deref().unwrap_or("<unknown>"),
                    device.id,
                );
            }
            // Your app would re-attach to these (e.g. resume a protocol handshake,
            // re-subscribe to notifications) instead of scanning again.
        }
        None => {
            println!("central: fresh launch (no restoration)");
        }
    }

    // --- Peripheral side ---

    let peripheral: Peripheral = Peripheral::with_config(PeripheralConfig {
        restore_identifier: Some(PERIPHERAL_RESTORE_ID.into()),
    })
    .await?;

    match peripheral.take_restored() {
        Some(service_uuids) => {
            println!(
                "peripheral restored by OS: {} preserved service(s)",
                service_uuids.len()
            );
            for uuid in &service_uuids {
                println!("  - {uuid}");
            }
            // The OS re-registered these services on the manager for us. Do NOT call
            // `add_service` for them again. Advertising, if it was active at
            // termination, has already resumed automatically.
            //
            // L2CAP channels are NOT restored — republish via `l2cap_listener()`.
        }
        None => {
            println!("peripheral: fresh launch (no restoration)");
            peripheral
                .add_service(&GattService {
                    uuid: SVC_UUID,
                    primary: true,
                    characteristics: vec![GattCharacteristic {
                        uuid: CHAR_UUID,
                        properties: CharacteristicProperties::READ
                            | CharacteristicProperties::WRITE,
                        permissions: AttributePermissions::READ | AttributePermissions::WRITE,
                        value: vec![],
                        descriptors: vec![],
                    }],
                })
                .await?;
            peripheral
                .start_advertising(&AdvertisingConfig {
                    local_name: "blew-restore".into(),
                    service_uuids: vec![SVC_UUID],
                })
                .await?;
        }
    }

    println!("Ready. (In a real iOS app this task would be the long-lived main loop.)");
    Ok(())
}
