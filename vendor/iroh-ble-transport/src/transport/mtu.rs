//! ATT MTU → reliable-protocol chunk size resolution.
//!
//! `resolve_chunk_size` polls `BleInterface::mtu` with a bounded deadline,
//! rejects readings at or below the BLE spec default (23), caps the result
//! at `MAX_CHUNK_SIZE`, and falls back to `DEFAULT_FALLBACK_MTU` on timeout.
//! See `docs/superpowers/specs/2026-04-14-dynamic-mtu-chunk-size-design.md`
//! for the full rationale.

use std::time::Duration;

use blew::DeviceId;
use tokio::time::Instant;

use crate::transport::interface::BleInterface;

/// Hard ceiling on per-fragment payload size. 509 = MTU(512) - 3 (ATT PDU
/// overhead). This is the audited envelope across Apple, Linux, and Android
/// today; lifting it requires re-auditing each backend for silent truncation.
pub const MAX_CHUNK_SIZE: usize = 509;

/// Smallest ATT MTU treated as a real, post-negotiation reading. Per BLE
/// Core Spec Vol 3 Part F §3.2.8, the default ATT_MTU is exactly 23, so any
/// strictly-greater value provably implies an exchange completed. Readings
/// at or below 23 collapse all "I don't know" paths (Android blew's
/// `unwrap_or(23)`, Linux blew's `23_u16` stub, pre-negotiation queries).
pub const MIN_SANE_MTU: u16 = 24;

/// Fallback ATT MTU assumed when the platform cannot give us a sane reading
/// within the deadline. Produces a chunk size of 509 — today's static cap.
/// This is an explicit gamble that any peer we reach is one of our supported
/// platforms that negotiates ≥ 512; mistakes are caught loudly by the
/// fragment canary.
pub const DEFAULT_FALLBACK_MTU: u16 = 512;

/// Max time to wait after `StartDataPipe` for the platform's MTU reading to
/// become sane. 3 s comfortably clears the peripheral-side race where the
/// central's `requestMtu` → `onMtuChanged` dance lands after our first poll,
/// while still being short enough that a broken peer limps along on the
/// fallback instead of wedging the pipe.
pub const MTU_READY_DEADLINE: Duration = Duration::from_secs(3);

/// Interval between polls while waiting for the MTU to become sane.
pub const MTU_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// ATT PDU overhead on every write/notify (1-byte opcode + 2-byte handle).
pub const ATT_OVERHEAD: usize = 3;

/// Max reassembled QUIC datagram size in bytes. Applied uniformly to the
/// GATT (reliable) and L2CAP paths so both surface the same datagram-size
/// ceiling to iroh. 1472 matches iroh's `initial_mtu(1200)` with headroom
/// for QUIC overhead — see `docs/superpowers/specs/` for the rationale.
pub const MAX_DATAGRAM_SIZE: usize = 1472;

/// Resolve a chunk size for `device_id` by polling `iface.mtu()` until it
/// crosses `MIN_SANE_MTU` or `MTU_READY_DEADLINE` elapses. The returned
/// value is clamped to `MAX_CHUNK_SIZE` and is the over-the-wire fragment
/// size (including the 2-byte reliable header and 1-byte canary).
pub async fn resolve_chunk_size(iface: &dyn BleInterface, device_id: &DeviceId) -> usize {
    let deadline = Instant::now() + MTU_READY_DEADLINE;
    let mtu = loop {
        let m = iface.mtu(device_id).await;
        if m >= MIN_SANE_MTU {
            break m;
        }
        if Instant::now() >= deadline {
            // Some backends (Linux blew stub, platforms where negotiation
            // never completes) leave us at 23 forever. The fragment canary
            // catches mis-sized fallbacks loudly, so this is an expected
            // path rather than an operator-actionable warning.
            tracing::info!(
                device = %device_id,
                last_seen = m,
                "MTU never reached MIN_SANE_MTU, falling back"
            );
            break DEFAULT_FALLBACK_MTU;
        }
        tokio::time::sleep(MTU_POLL_INTERVAL).await;
    };
    let chunk_size = (mtu as usize)
        .saturating_sub(ATT_OVERHEAD)
        .min(MAX_CHUNK_SIZE);
    tracing::info!(
        device = %device_id,
        mtu,
        chunk_size,
        "resolved pipe chunk size"
    );
    chunk_size
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::transport::test_util::MockBleInterface;

    fn device() -> DeviceId {
        DeviceId::from("mtu-test-device")
    }

    #[tokio::test(start_paused = true)]
    async fn returns_fallback_when_platform_stays_at_23() {
        let mock = MockBleInterface::new();
        mock.set_mtu_default(23);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, DEFAULT_FALLBACK_MTU as usize - ATT_OVERHEAD);
    }

    #[tokio::test(start_paused = true)]
    async fn returns_real_mtu_minus_overhead_when_reading_is_sane_immediately() {
        let mock = MockBleInterface::new();
        mock.set_mtu_default(200);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, 200 - ATT_OVERHEAD);
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_late_but_sane_reading() {
        let mock = MockBleInterface::new();
        mock.push_mtu(23);
        mock.push_mtu(23);
        mock.push_mtu(512);
        mock.set_mtu_default(512);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, 512 - ATT_OVERHEAD);
    }

    #[tokio::test(start_paused = true)]
    async fn clamps_large_mtu_to_max_chunk_size() {
        let mock = MockBleInterface::new();
        mock.set_mtu_default(517);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, MAX_CHUNK_SIZE);
    }

    #[tokio::test(start_paused = true)]
    async fn trusts_small_real_mtu_over_fallback() {
        let mock = MockBleInterface::new();
        mock.set_mtu_default(50);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, 50 - ATT_OVERHEAD);
    }

    #[tokio::test(start_paused = true)]
    async fn floor_edge_24_is_trusted() {
        let mock = MockBleInterface::new();
        mock.set_mtu_default(24);
        let iface: &dyn BleInterface = &mock;
        let size = resolve_chunk_size(iface, &device()).await;
        assert_eq!(size, 24 - ATT_OVERHEAD);
    }
}
