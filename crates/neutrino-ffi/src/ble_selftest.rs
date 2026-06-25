// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! P0b on-device BLE smoke test (feature `ble` only).
//!
//! This is **not** part of the homeserver. It is a throwaway de-risking
//! entrypoint the host app calls directly so we can answer two questions on real
//! Android hardware, before building any of the real relay (P3):
//!
//! 1. Does `blew`'s Android Bluetooth backend work at all? — proven by a QUIC
//!    **stream** echo (what the upstream `iroh_ble` example exercises).
//! 2. Do QUIC **unreliable datagrams** survive the BLE custom transport? — the
//!    relay bets on datagrams (one IP packet per datagram); the example only
//!    uses streams, so this is an open risk. Proven (or refuted) here.
//!
//! All output goes through `tracing` (target `neutrino::ble_selftest`) so it
//! reaches logcat via the host's subscriber — there is no return value to
//! inspect across FFI; read the log.
//!
//! Usage from the host, on two devices:
//! - device A: `ble_smoke_test(None)` → advertises + accepts; logs its node id.
//! - device B: `ble_smoke_test(Some("<node id from A's log>"))` → dials A, runs the
//!   stream + datagram round-trips, logs PASS/FAIL per medium.
//!
//! The app must hold the Android BLE runtime permissions (BLUETOOTH_SCAN /
//! _CONNECT / _ADVERTISE) before calling this.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use iroh::endpoint::presets::N0DisableRelay;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_ble_transport::transport::BleTransport;
use iroh_ble_transport::{Central, Peripheral};

const TARGET: &str = "neutrino::ble_selftest";

/// ALPN for the self-test (distinct from the eventual relay ALPN).
const SELFTEST_ALPN: &[u8] = b"neutrino-ble/selftest/0";

/// Small payloads — BLE is low-bandwidth and a datagram must fit the path MTU.
const STREAM_MSG: &[u8] = b"neutrino ble stream echo";
const DGRAM_MSG: &[u8] = b"neutrino ble datagram echo";

/// How long the dialer scans/retries before giving up.
const DIAL_BUDGET: Duration = Duration::from_secs(60);
/// Per-connect-attempt timeout while scanning.
const CONNECT_ATTEMPT: Duration = Duration::from_secs(5);
/// Bound on a single stream/datagram round-trip once connected.
const ROUNDTRIP_TIMEOUT: Duration = Duration::from_secs(20);

/// Run the BLE smoke test on a dedicated runtime thread and return immediately.
/// `remote` = `None` → listener (advertise + accept + echo); `Some(node_id)` →
/// dialer (connect + stream test + datagram test). Results are logged, not
/// returned (see module docs).
#[uniffi::export]
pub fn ble_smoke_test(remote: Option<String>) {
    // Route panics through tracing so they reach logcat. Without this, a panic on
    // this dedicated thread (e.g. blew's `Central::new()` panicking with "JVM not
    // initialized" when the Android JNI glue is missing) only hits stderr, which
    // does not surface on Android — leaving the run looking like a silent hang.
    install_panic_logger();
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name("ble-selftest")
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!(target: TARGET, "failed to build runtime: {e}");
                return;
            }
        };
        rt.block_on(async {
            if let Err(e) = run(remote).await {
                tracing::error!(target: TARGET, "ble selftest failed: {e}");
            }
        });
    });
}

/// Install (once) a panic hook that logs the panic message + location through
/// `tracing`, so failures on the self-test thread are visible in logcat.
fn install_panic_logger() {
    use std::sync::Once;
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            tracing::error!(target: TARGET, "panic: {info}");
            prev(info);
        }));
    });
}

/// Build a BLE-only iroh endpoint (IP transports cleared, so this genuinely
/// exercises Bluetooth). Mirrors the upstream `iroh_ble` example. Each BLE
/// constructor is logged so a hang/panic pinpoints the offending step.
async fn ble_endpoint(secret: SecretKey) -> Result<Endpoint, Box<dyn std::error::Error>> {
    let public = secret.public();
    // Bootstrap blew's Android JNI layer (no-op off Android). Without it,
    // `Central::new()` panics "JVM not initialized" — see `ble_android`.
    crate::ble_android::ensure_initialised()
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    tracing::info!(target: TARGET, "initialising BLE transport (central + peripheral)…");
    let central = Arc::new(Central::new().await?);
    tracing::info!(target: TARGET, "central up");
    let peripheral = Arc::new(Peripheral::new().await?);
    tracing::info!(target: TARGET, "peripheral up");
    let transport = BleTransport::new(public, central, peripheral).await?;
    tracing::info!(target: TARGET, "ble transport up");
    let lookup = transport.address_lookup();
    let transport: Arc<dyn iroh::endpoint::transports::CustomTransport> = Arc::new(transport);

    let ep = Endpoint::builder(N0DisableRelay)
        .add_custom_transport(transport)
        .address_lookup(lookup)
        .secret_key(secret)
        .clear_ip_transports()
        .alpns(vec![SELFTEST_ALPN.to_vec()])
        .bind()
        .await?;
    Ok(ep)
}

async fn run(remote: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    match remote {
        None => run_listener().await,
        Some(id) => run_dialer(&id).await,
    }
}

/// Advertise and accept connections; echo back both a stream and a datagram per
/// connection so a dialer can verify either medium.
async fn run_listener() -> Result<(), Box<dyn std::error::Error>> {
    let secret = SecretKey::generate();
    let ep = ble_endpoint(secret).await?;
    tracing::info!(
        target: TARGET,
        node_id = %ep.id(),
        "BLE listener up — run ble_smoke_test(Some(\"<this node id>\")) on the other device",
    );

    while let Some(incoming) = ep.accept().await {
        tokio::spawn(async move {
            let conn = match incoming.await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(target: TARGET, "accept failed: {e}");
                    return;
                }
            };
            let peer = conn.remote_id();
            tracing::info!(target: TARGET, %peer, "accepted BLE connection; echoing");

            // Stream echo (proves the backend).
            match conn.accept_bi().await {
                Ok((mut send, mut recv)) => match recv.read_to_end(4096).await {
                    Ok(msg) => {
                        let _ = send.write_all(&msg).await;
                        let _ = send.finish();
                        tracing::info!(target: TARGET, bytes = msg.len(), "echoed stream");
                    }
                    Err(e) => tracing::warn!(target: TARGET, "stream read failed: {e}"),
                },
                Err(e) => tracing::warn!(target: TARGET, "accept_bi failed: {e}"),
            }

            // Datagram echo (proves the relay's chosen primitive over BLE).
            match conn.read_datagram().await {
                Ok(dgram) => match conn.send_datagram(dgram) {
                    Ok(()) => tracing::info!(target: TARGET, "echoed datagram"),
                    Err(e) => {
                        tracing::warn!(target: TARGET, "send_datagram failed (BLE may not carry datagrams): {e}")
                    }
                },
                Err(e) => {
                    tracing::warn!(target: TARGET, "read_datagram failed (BLE may not carry datagrams): {e}")
                }
            }

            // Hold the connection open briefly so echoes flush.
            let _ = tokio::time::timeout(ROUNDTRIP_TIMEOUT, conn.closed()).await;
        });
    }
    Ok(())
}

/// Dial the listener over BLE, then run the stream and datagram round-trips,
/// logging PASS/FAIL for each.
async fn run_dialer(remote: &str) -> Result<(), Box<dyn std::error::Error>> {
    let remote_id: EndpointId = remote.parse()?;
    let secret = SecretKey::generate();
    let ep = ble_endpoint(secret).await?;
    tracing::info!(target: TARGET, peer = %remote_id.fmt_short(), "scanning for peer over BLE…");

    let conn = {
        let deadline = tokio::time::Instant::now() + DIAL_BUDGET;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err("peer not found over BLE within budget".into());
            }
            let addr = EndpointAddr::from(remote_id);
            match tokio::time::timeout(CONNECT_ATTEMPT, ep.connect(addr, SELFTEST_ALPN)).await {
                Ok(Ok(conn)) => break conn,
                Ok(Err(e)) => tracing::debug!(target: TARGET, "connect attempt failed: {e}"),
                Err(_) => {} // attempt timed out; keep scanning
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    tracing::info!(target: TARGET, peer = %remote_id.fmt_short(), "connected over BLE");

    // 1. Stream round-trip — the primary backend check.
    match stream_roundtrip(&conn).await {
        Ok(()) => tracing::info!(target: TARGET, "PASS: BLE stream echo"),
        Err(e) => tracing::error!(target: TARGET, "FAIL: BLE stream echo: {e}"),
    }

    // 2. Datagram round-trip — the relay's chosen primitive.
    match datagram_roundtrip(&conn).await {
        Ok(()) => {
            tracing::info!(target: TARGET, "PASS: BLE datagram echo (relay can use datagrams)")
        }
        Err(e) => tracing::error!(
            target: TARGET,
            "FAIL: BLE datagram echo: {e} — relay must fall back to framed streams over BLE"
        ),
    }

    conn.close(0u32.into(), b"selftest done");
    ep.close().await;
    Ok(())
}

async fn stream_roundtrip(
    conn: &iroh::endpoint::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(STREAM_MSG).await?;
    send.finish()?;
    let echoed = tokio::time::timeout(ROUNDTRIP_TIMEOUT, recv.read_to_end(4096)).await??;
    if echoed == STREAM_MSG {
        Ok(())
    } else {
        Err(format!("echo mismatch: {} bytes", echoed.len()).into())
    }
}

async fn datagram_roundtrip(
    conn: &iroh::endpoint::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    if conn.max_datagram_size().unwrap_or(0) == 0 {
        return Err("path advertises no datagram support".into());
    }
    conn.send_datagram(Bytes::from_static(DGRAM_MSG))?;
    let echoed = tokio::time::timeout(ROUNDTRIP_TIMEOUT, conn.read_datagram()).await??;
    if echoed.as_ref() == DGRAM_MSG {
        Ok(())
    } else {
        Err(format!("echo mismatch: {} bytes", echoed.len()).into())
    }
}
