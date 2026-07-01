//! Emits `PeerCommand::Tick` at a fixed cadence so the registry can advance
//! its timers.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::transport::peer::PeerCommand;

// TODO(perf): gate this pump on peer presence. Today we tick every 250 ms
// forever, even when the registry has zero peers and nothing to time out.
// A peer-presence notify (set when `peers` becomes non-empty, cleared when
// it drains) would let idle apps suspend the pump entirely; wake it back
// up the moment the first Advertised/Inbound* command arrives.
pub const TICK_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_watchdog(inbox: mpsc::Sender<PeerCommand>) {
    let mut interval = tokio::time::interval(TICK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if inbox.send(PeerCommand::Tick(Instant::now())).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn tick_fires_periodically() {
        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(run_watchdog(tx));
        // Advance time and collect at least 2 ticks
        tokio::time::advance(TICK_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_ok(), "expected first tick");

        tokio::time::advance(TICK_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_ok(), "expected second tick");
    }
}
