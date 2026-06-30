//! Shared runtime utilities: federation constants, backoff/jitter, the
//! transaction-id source, and the stage-then-poke ingestion primitive.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use neutrino_common::Event;
use neutrino_store::{StagingStore, StorageError};
use rand::Rng;
use ruma::{OwnedRoomId, RoomId, ServerName};
use tokio::sync::mpsc;

/// Max PDUs per federation transaction. The inbound `/send` handler rejects a
/// transaction carrying more than this; the outbound sender chunks to it. One
/// constant so the two halves can't drift.
pub const MAX_PDUS_PER_TXN: usize = 50;

/// Backoff floor after a transient failure (outbound delivery, inbound
/// staging). Shared so the two retry loops can't drift.
pub(crate) const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Backoff ceiling. The exponential sequence (1, 2, 4, 8, … s) is clamped here.
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);

/// Double the backoff ceiling, clamped at [`BACKOFF_CAP`].
pub(crate) fn next_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(BACKOFF_CAP)
}

/// Full jitter: a uniform random duration in `[0, ceiling]`. Spreads retries
/// (and startup) so a fleet of senders / a gap-fill loop doesn't thunder a
/// recovering peer in lockstep.
pub(crate) fn jitter(ceiling: Duration) -> Duration {
    let max_ms = ceiling.as_millis() as u64;
    if max_ms == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rand::rng().random_range(0..=max_ms))
}

/// Milliseconds since the Unix epoch, for the federation transaction
/// `origin_server_ts`. Saturates to 0 on a pre-epoch clock — never panics (no
/// `unwrap` on `SystemTime`). Shared by the inbound `backfill` response and the
/// outbound `client`.
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Monotonic transaction-id source: `{startup_prefix}-{counter}`. The prefix
/// (a process-startup timestamp, supplied by the caller) keeps ids unique
/// across restarts; the counter keeps them unique within a run. Receivers
/// dedup on `(origin, txn_id)` via `FederationInbox::record_federation_txn`.
pub struct TxnIdGen {
    prefix: u64,
    counter: AtomicU64,
}

impl TxnIdGen {
    pub fn new(prefix: u64) -> Self {
        Self {
            prefix,
            counter: AtomicU64::new(0),
        }
    }

    pub fn next_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}", self.prefix, n)
    }
}

/// Durably stage `events` for `room_id` (skipping any cross-room event a peer
/// slipped in), then poke the inbound worker to drain them. The poke is awaited
/// (not `try_send`) so a single fresh-room ingest can't be silently dropped and
/// left to stall.
pub async fn stage_and_poke(
    store: &impl StagingStore,
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    origin: &ServerName,
    room_id: &RoomId,
    events: &[Event],
) -> Result<(), StorageError> {
    for ev in events {
        if ev.room_id != *room_id {
            continue; // never stage a cross-room event a peer slipped in
        }
        store
            .stage_pdu(origin, &ev.room_id, &ev.event_id, &ev.raw)
            .await?;
    }
    if worker_poke.send(room_id.to_owned()).await.is_err() {
        tracing::warn!(%room_id, "worker poke failed; staged events will drain on the next poke or restart");
    }
    Ok(())
}
