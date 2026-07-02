[![Crates.io](https://img.shields.io/crates/v/blew.svg)](https://crates.io/crates/blew)
[![Docs.rs](https://docs.rs/blew/badge.svg)](https://docs.rs/blew)

# `blew`

🔥 Warning 🔥 This library is in alpha state and is subject to change without
backwards-compatibility until otherwise noted.

`blew` is a BLE (Bluetooth Low Energy) Rust library focused on enabling
peer-to-peer applications, and was built as the backend for
[`iroh-ble-transport`](https://github.com/mcginty/iroh-ble-transport).

It differs from other libraries in that it implements both Central *and*
Peripheral modes, and aims to provide support for macOS, iOS, Android, and
Linux. It is also `async`-only and requires a Tokio runtime.

It also supports opportunistic L2CAP which provides a lower-level socket-type
interface that tends to be significantly faster than using GATT for data
transfer.

`blew` is intended to support as many concurrent L2CAP channels as the device
and platform can sustain. Backend implementations should therefore optimize for
high L2CAP concurrency, not just single-channel correctness.

For Tauri users, see the companion crate
[`tauri-plugin-blew`](https://crates.io/crates/tauri-plugin-blew) that vastly
simplifies the necessary setup of the Kotlin/JNI glue to enable Android
functionality.

## Supported Platforms

| Platform | Backend | Central | Peripheral | L2CAP |
|----------|---------|---------|------------|-------|
| macOS / iOS | CoreBluetooth (via `objc2`) | Yes | Yes | Yes |
| Linux | BlueZ (via `bluer`) | Yes | Yes | Yes |
| Android | JNI + Kotlin (via `jni` and `ndk-context`) | Yes | Yes | Yes |

## Examples

```sh
cargo run --example scan -p blew          # scan for 10s, print discoveries
cargo run --example advertise -p blew     # advertise a GATT service, handle reads/writes
cargo run --example l2cap_server -p blew  # peripheral: publish L2CAP CoC, echo data
cargo run --example l2cap_client -p blew  # central: scan, connect, open L2CAP, send data
cargo run --example restore -p blew       # iOS state-restoration launch sequence
```

The `integration_peripheral` / `integration_central` pair is a scripted
real-hardware smoke test -- run them on two separate hosts to verify the
backend end-to-end. See the top-level README for details.

## Alternative Libraries

This library was customized primarily to be used for Iroh, and there are plenty
of great alternatives out there! I'm sure I'm missing some, but to name a few
that are actively maintained:

- [`btleplug`](https://github.com/deviceplug/btleplug)
- [`bluest`](https://github.com/alexmoon/bluest)
- [`ble-peripheral-rust`](https://github.com/rohitsangwan01/ble-peripheral-rust)

## License

This project is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE).

Commercial licenses are available for use cases where the AGPL is not suitable.
Contact [me@jakebot.org](mailto:me@jakebot.org) for details.
