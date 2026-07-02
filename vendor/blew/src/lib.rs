//! `blew` is a cross-platform BLE central + peripheral library.
//!
//! Supports iOS, macOS, Linux, and Android. Each role is initialised
//! independently, and in its simplest form:
//!
//! ```rust
//! # async fn example() -> blew::error::BlewResult<()> {
//! use blew::central::{Central, ScanFilter};
//! use blew::peripheral::Peripheral;
//!
//! let central: Central = Central::new().await?;
//! central.start_scan(ScanFilter::default()).await?;
//!
//! let peripheral: Peripheral = Peripheral::new().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Getting started
//!
//! Construct [`Central`] or [`Peripheral`] with an explicit type annotation:
//!
//! ```no_run
//! # async fn example() -> blew::error::BlewResult<()> {
//! use blew::central::Central;
//! let central: Central = Central::new().await?;
//! # Ok(()) }
//! ```
//!
//! The annotation is required because Rust's default type-parameter inference does not fire on
//! method calls. If you see compiler error E0283 ("type annotations needed"), you forgot the
//! `: Central`.
//!
//! # State restoration
//!
//! **iOS only.** On macOS, Linux, and Android the restoration configuration is inert —
//! `Central::with_config` / `Peripheral::with_config` accept the same types but behave
//! identically to `new()` there, so applications can call them unconditionally. The
//! `take_restored()` drain methods are only exposed on Apple targets; non-Apple code
//! does not need to call them.
//!
//! State restoration lets iOS relaunch your app in the background when BLE events arrive
//! after it has been suspended or jetsam-killed. When it works, the system hands back the
//! previously-connected peripherals (central) or published services (peripheral), along
//! with the subscriber list and any in-flight advertising state — no re-scan, no
//! re-advertise, no re-discover-services. Used correctly it can keep a BLE session alive
//! across hours or days of background time. Used incorrectly it silently does nothing, or
//! worse, diverges from what the OS expects and causes connection drops. The rules below
//! are non-negotiable.
//!
//! ## Prerequisites
//!
//! 1. **Info.plist `UIBackgroundModes`** must include `bluetooth-central` (for [`Central`])
//!    and/or `bluetooth-peripheral` (for [`Peripheral`]). Without these the OS will not
//!    preserve or restore state and `with_config` behaves like `new()`.
//! 2. **The restore identifier must be stable across launches.** Hard-code it or load it
//!    from a place that is available before the BLE stack comes up (e.g. a build-time
//!    constant). If the identifier differs from the previous launch, the OS discards the
//!    preserved state.
//! 3. **Construct the role in `application:didFinishLaunchingWithOptions:`** — that is, at
//!    the earliest possible point in app startup, *synchronously* from the launch
//!    callback. If you construct it later, the delegate misses the `willRestoreState:`
//!    callback and preserved state is dropped.
//!
//! ## Runtime contract
//!
//! The `willRestoreState:` callback fires *during* manager construction, before
//! [`Central::with_config`] / [`Peripheral::with_config`] returns, so subscribers attached
//! afterwards would miss it. Instead, the payload is buffered and handed out once via
//! [`Central::take_restored`] / [`Peripheral::take_restored`]. Call it immediately after
//! `with_config`, before issuing any new work (scanning, connecting, adding services,
//! advertising) — otherwise the app's state can diverge from the OS's in ways that look
//! like random disconnects.
//!
//! Typical shape (iOS):
//!
//! ```ignore
//! use blew::central::{Central, CentralConfig};
//!
//! let central: Central = Central::with_config(CentralConfig {
//!     restore_identifier: Some("com.example.app.central".into()),
//!     ..Default::default()
//! }).await?;
//!
//! if let Some(devices) = central.take_restored() {
//!     // Re-hydrate app-level state from the restored peripherals before
//!     // kicking off any new scans or connects.
//!     for _dev in devices { /* ... */ }
//! }
//! ```
//!
//! ## What is *not* restored
//!
//! - **L2CAP CoC channels.** The OS does not persist L2CAP sockets across termination.
//!   Any side that had a channel open must re-establish it — the central via
//!   [`Central::open_l2cap_channel`], the peripheral via
//!   [`Peripheral::l2cap_listener`]. The restored-state callbacks do not carry PSM info.
//! - **Subscribers.** Peripheral subscribers are not carried across restoration; the
//!   `SubscriptionChanged` events replay as clients re-subscribe.
//! - **GATT service discovery cache** (central side) is preserved, but discovered
//!   characteristics will be re-emitted by your own `discover_services` flow if you call
//!   it again. Skip redundant discovery by using the restored peripherals directly.
//!
//! ## Background runtime caveats
//!
//! - Background scanning is degraded: you must pass a non-empty service-UUID filter, the
//!   OS forces duplicate suppression on, and the scan duty cycle is coalesced with other
//!   apps. Scans with no filter are silently ignored in the background.
//! - Background advertising strips `local_name` from the advertisement and moves service
//!   UUIDs into the overflow area. Peers that are not explicitly scanning for your
//!   service UUID will not see you.
//! - Repeated crashes during a background relaunch cause iOS to *disable* background BLE
//!   for your app until the user relaunches it from the foreground. Treat
//!   `willRestoreState:` as a stability-critical code path.

pub mod central;
pub mod error;
pub mod gatt;
pub mod l2cap;
pub mod peripheral;
pub mod platform;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod types;
pub mod util;

pub use central::{Central, CentralConfig, DisconnectCause};
pub use error::{BlewError, BlewResult};
pub use l2cap::L2capChannel;
pub use peripheral::Peripheral;
pub use types::{BleDevice, DeviceId};
