//! Shared runtime utilities: federation constants, backoff/jitter, the
//! transaction-id source, room-version resolution, and the stage-then-poke
//! ingestion primitive.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use neutrino_event::{Event, RoomVersion, RoomVersions};
use neutrino_store::{RoomStore, StagingStore, StorageError};
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

/// The version of `room_id`, as this build understands it.
///
/// The persisted `rooms.room_version` resolved through the registry: naming an
/// event requires knowing its room's version, and this is the one place the
/// runtime turns a room id into one. `None` means we cannot name events in this
/// room right now — either the row is gone / storage faulted, or the room is of
/// a version this build does not speak (a peer whose medium declares one we
/// lack). Both are logged; both mean the caller must not guess a version, since
/// naming an event under the wrong one silently invents a different event.
pub async fn room_version(
    store: &impl RoomStore,
    versions: &RoomVersions,
    room_id: &RoomId,
) -> Option<Arc<RoomVersion>> {
    let stored = match store.get_room_version(room_id).await {
        Ok(Some(v)) => v,
        Ok(None) => {
            tracing::warn!(%room_id, "no room version on record for room");
            return None;
        }
        Err(e) => {
            tracing::warn!(%room_id, error = %e, "reading room version failed");
            return None;
        }
    };
    let resolved = versions.get(stored.as_str()).cloned();
    if resolved.is_none() {
        tracing::warn!(%room_id, version = %stored, "room is of an unsupported room version");
    }
    resolved
}

/// The version that names an inbound wire event, resolved the only two ways an
/// event can say: a create declares its own ([`RoomVersionKeys::declared`]),
/// anything else is named by the version of the room it is in.
///
/// `None` means the event cannot be named and must be dropped: an unknown room
/// (we are not in it), a version this build does not speak, or a create
/// declaring one we lack. Callers holding a room id already should use
/// [`room_version`] directly rather than re-reading the bytes.
pub async fn room_version_for_wire(
    store: &impl RoomStore,
    versions: &RoomVersions,
    raw: &serde_json::value::RawValue,
) -> Option<Arc<RoomVersion>> {
    let keys = neutrino_event::room_version_keys(raw);
    match (keys.room_id, keys.declared) {
        // In a room: the room's persisted version is authoritative. Both keys
        // at once means a create carrying a `room_id` — malformed under v12, and
        // `from_wire` refuses it once named; a non-create cannot declare a
        // version at all (`room_version_keys` gates on `type`).
        (Some(room_id), _) => room_version(store, versions, &room_id).await,
        // A create, declaring the version it creates the room under.
        (None, Some(declared)) => {
            let resolved = versions.get(&declared).cloned();
            if resolved.is_none() {
                tracing::warn!(version = %declared, "create event declares an unsupported room version");
            }
            resolved
        }
        // A create declaring nothing: v12 rule 1.3 permits the field to be
        // absent, and the base version is what an absent declaration means.
        (None, None) => Some(versions.base().clone()),
    }
}
