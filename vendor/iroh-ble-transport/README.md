[![iroh-ble-transport Crates.io](https://img.shields.io/crates/v/iroh-ble-transport)](https://crates.io/crates/iroh-ble-transport)
[![iroh-ble-transport Docs.rs](https://img.shields.io/docsrs/iroh-ble-transport)](https://docs.rs/iroh-ble-transport)

# `iroh-ble-transport`

🔥 **Warning:** 🔥 This library is experimental. A best effort will be made to resolve
bugs and follow semantic versioning, but there are no guarantees. Do not rely on it
until it has been sufficiently field-tested.

`iroh-ble-transport` is a Rust crate providing BLE (Bluetooth Low Energy) as a
custom transport for the [Iroh](https://github.com/n0-computer/iroh)
peer-to-peer networking stack. Devices act as both central (scanner/client) and
peripheral (advertiser/server) simultaneously to discover and connect to each
other over BLE.

Built on top of [blew](https://github.com/mcginty/blew), a cross-platform BLE
library for Rust.

## Requirements

- Rust **1.95** or newer

## High-level functionality

- Each node advertises a GATT service whose UUID encodes its public key prefix
- Peers discover each other from advertising packets without connecting
- Connections start on GATT, then upgrade to L2CAP when available
- If L2CAP setup fails or times out, the connection falls back to GATT

## Supported Platforms

| Platform | Backend |
|----------|---------|
| macOS / iOS | CoreBluetooth (via `objc2`) |
| Linux | BlueZ (via `bluer`) |
| Android | JNI + Kotlin (via `jni` and `ndk-context`) |

## Usage

```rust
use iroh_ble_transport::BleTransport;
use iroh::Endpoint;

let transport = BleTransport::new().await?;
let endpoint = Endpoint::builder()
    .add_custom_transport(transport)
    .bind()
    .await?;
```

## Examples

```sh
# Echo + speed test (listener)
cargo run --example iroh_ble -p iroh-ble-transport

# Echo + speed test (dialer)
cargo run --example iroh_ble -p iroh-ble-transport -- <endpoint-id>
```

## Demo App

There's a Tauri-based demo group chat app to show off Iroh BLE connectivity in
`demos/iroh-ble-chat`. More details on this app will follow, but please do not
use it for anything sensitive, it is not secure or in any way was written as a
toy solely to demonstrate the underlying transport. The app starts with
debug-event capture disabled by default so tester logs stay low-noise unless
explicitly expanded.

## License

This project is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE).

Commercial licenses are available for use cases where the AGPL is not suitable.
Contact [me@jakebot.org](mailto:me@jakebot.org) for details.
