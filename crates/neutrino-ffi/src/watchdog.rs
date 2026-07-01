//! Executor-stall watchdog.
//!
//! The embedded server runs on a `current_thread` Tokio runtime (see
//! [`crate::start`]), so any task that fails to yield — or any starvation of the
//! single executor thread — stalls *every* task at once, including the C-S
//! `/sync` long-poll timers. Such a stall is otherwise invisible: the server
//! just goes quiet (no error, no panic). This watchdog makes it loud so a
//! recurrence can be root-caused from the logs instead of guessed at.
//!
//! Mechanism: a lightweight in-runtime task records a monotonic heartbeat every
//! [`BEAT_INTERVAL`]; a dedicated OS thread — deliberately *off* the runtime, so
//! it keeps running even when the executor is wedged — checks the heartbeat
//! every [`CHECK_INTERVAL`] and logs a `WARN` whenever it has gone stale beyond
//! [`STALL_THRESHOLD`]. The stall decision ([`evaluate`]) is a pure function, so
//! the policy is unit-tested without threads, sleeps, or a runtime.
//!
//! Lifetime: the watchdog thread holds only a [`Weak`] to the heartbeat cell,
//! which the in-runtime task owns. When the runtime shuts down the task is
//! cancelled, the last strong ref drops, and the thread's next `upgrade()`
//! returns `None` and it exits — so restarting the server never leaks a thread.

use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How often the in-runtime task records a heartbeat.
const BEAT_INTERVAL: Duration = Duration::from_millis(500);
/// How often the off-runtime watchdog thread checks the heartbeat.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Heartbeat age beyond which the executor is considered stalled. Comfortably
/// above [`BEAT_INTERVAL`] so ordinary scheduling jitter never trips it, and
/// below the client's 30 s `/sync` long-poll so a stall that would eat a
/// long-poll is caught well before it does.
const STALL_THRESHOLD: Duration = Duration::from_secs(3);

/// Pure stall decision. Given the current monotonic time, the last recorded
/// beat, and the threshold (all in ms since a shared origin), returns
/// `Some(lag_ms)` when the executor has been silent longer than the threshold,
/// else `None`. `saturating_sub` guards against a last-beat that (spuriously)
/// reads ahead of `now` under relaxed-atomic reordering.
fn evaluate(now_ms: u64, last_beat_ms: u64, threshold_ms: u64) -> Option<u64> {
    let lag = now_ms.saturating_sub(last_beat_ms);
    (lag > threshold_ms).then_some(lag)
}

/// Start the watchdog. Must be called from *inside* the server runtime (e.g. at
/// the top of `rt.block_on`) so the heartbeat task lands on that runtime.
///
/// Spawns two things: an in-runtime task bumping the heartbeat, and an
/// off-runtime OS thread that logs on stall. Both stop when the runtime shuts
/// down (see the module-level lifetime note). A failure to spawn the OS thread
/// is logged and swallowed — the watchdog is diagnostics, never a startup gate.
pub(crate) fn spawn() {
    let origin = Instant::now();
    let last_beat_ms = std::sync::Arc::new(AtomicU64::new(0));

    // In-runtime beater. `interval`'s first tick fires immediately, so the
    // heartbeat is primed at ~0ms without a separate priming write.
    let beat = last_beat_ms.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(BEAT_INTERVAL);
        loop {
            tick.tick().await;
            beat.store(origin.elapsed().as_millis() as u64, Ordering::Relaxed);
        }
    });

    // Off-runtime watchdog. Weak ref → exits once the runtime (and thus the
    // beater's strong ref) is gone.
    let beat = std::sync::Arc::downgrade(&last_beat_ms);
    if let Err(e) = std::thread::Builder::new()
        .name("neutrino-watchdog".into())
        .spawn(move || watchdog_loop(origin, beat))
    {
        tracing::warn!(error = %e, "neutrino: failed to spawn executor-stall watchdog thread");
    }
}

/// The off-runtime check loop. Sleeps [`CHECK_INTERVAL`], reads the heartbeat,
/// and logs on a stall — escalating only when the lag exceeds the worst already
/// reported for the current stall, so a single stall yields one growing report
/// rather than a per-second flood. Returns when the heartbeat's owner is gone.
fn watchdog_loop(origin: Instant, beat: Weak<AtomicU64>) {
    let threshold_ms = STALL_THRESHOLD.as_millis() as u64;
    let mut reported_lag_ms = 0u64;
    loop {
        std::thread::sleep(CHECK_INTERVAL);
        let Some(beat) = beat.upgrade() else {
            return; // runtime gone → watchdog's job is done
        };
        let now_ms = origin.elapsed().as_millis() as u64;
        match evaluate(now_ms, beat.load(Ordering::Relaxed), threshold_ms) {
            Some(lag) if lag > reported_lag_ms => {
                reported_lag_ms = lag;
                tracing::warn!(
                    lag_ms = lag,
                    "neutrino executor stalled: no heartbeat for {lag}ms — tasks are not \
                     being polled (single-thread starvation or a blocking call on the \
                     runtime thread)"
                );
            }
            Some(_) => {}                // still stalled; already reported this peak
            None => reported_lag_ms = 0, // recovered → re-arm
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_below_threshold_is_not_a_stall() {
        // 200ms of lag, 3s threshold → healthy.
        assert_eq!(evaluate(1_000, 800, 3_000), None);
    }

    #[test]
    fn lag_exactly_at_threshold_is_not_a_stall() {
        // Boundary: the comparison is strictly `>`, so exactly threshold is OK.
        assert_eq!(evaluate(4_000, 1_000, 3_000), None);
    }

    #[test]
    fn lag_just_over_threshold_is_a_stall() {
        assert_eq!(evaluate(4_001, 1_000, 3_000), Some(3_001));
    }

    #[test]
    fn large_lag_reports_the_full_lag() {
        // The reported value is the actual lag, not just "stalled".
        assert_eq!(evaluate(60_000, 1_000, 3_000), Some(59_000));
    }

    #[test]
    fn beat_ahead_of_now_saturates_to_zero_not_a_stall() {
        // Relaxed atomics could momentarily surface a beat that reads ahead of
        // our `now`; that must never be mistaken for a giant stall.
        assert_eq!(evaluate(500, 1_000, 3_000), None);
    }

    #[test]
    fn heartbeat_beat_then_read_shows_no_stall() {
        // End-to-end over the atomic cell (no runtime/threads): record a beat at
        // `now`, read at the same `now` → zero lag → healthy.
        let cell = AtomicU64::new(0);
        cell.store(10_000, Ordering::Relaxed);
        assert_eq!(evaluate(10_000, cell.load(Ordering::Relaxed), 3_000), None);
        // …and 4s later with no further beat → stalled.
        assert_eq!(
            evaluate(14_000, cell.load(Ordering::Relaxed), 3_000),
            Some(4_000)
        );
    }
}
