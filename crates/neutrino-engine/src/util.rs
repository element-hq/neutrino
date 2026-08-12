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
use thiserror::Error;
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

/// Why a room's version could not be resolved.
///
/// The distinction is load-bearing, not decorative: naming an event requires its
/// room's version, so a caller that cannot get one must either give up on the
/// event or try again later — and picking the wrong one of those loses events
/// (dropping a PDU a retry would have applied) or leaks them (retrying one that
/// can never apply). [`is_retryable`](Self::is_retryable) is the whole question.
#[derive(Debug, Error)]
pub enum VersionError {
    /// No row for this room: we are not in it. Terminal — nothing about this
    /// server will change that, so an event for it can never be applied.
    #[error("no version on record for this room")]
    UnknownRoom,

    /// The room exists but its `rooms.room_version` is one this build does not
    /// speak (a peer whose medium declares a version we lack). Terminal until
    /// the build changes.
    #[error("unsupported room version {0}")]
    Unsupported(String),

    /// Reading the store failed. Retryable: the version is on disk, we just
    /// could not read it this time.
    #[error("reading room version: {0}")]
    Fault(#[from] StorageError),
}

impl VersionError {
    /// Whether trying again later can succeed. `false` means the caller must
    /// stop — retrying forever on a terminal failure is how staged rows and
    /// per-room tasks leak.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Fault(_))
    }
}

/// The version of `room_id`, as this build understands it.
///
/// The persisted `rooms.room_version` resolved through the registry: naming an
/// event requires knowing its room's version, and this is the one place the
/// runtime turns a room id into one. Every failure is a [`VersionError`] so the
/// caller can tell "try again" from "give up" — never guess a version, since
/// naming an event under the wrong one silently invents a different event.
pub async fn room_version(
    store: &impl RoomStore,
    versions: &RoomVersions,
    room_id: &RoomId,
) -> Result<Arc<RoomVersion>, VersionError> {
    let stored = store
        .get_room_version(room_id)
        .await?
        .ok_or(VersionError::UnknownRoom)?;
    versions
        .get(stored.as_str())
        .cloned()
        .ok_or_else(|| VersionError::Unsupported(stored.as_str().to_owned()))
}

/// The version that names an inbound wire event, resolved the only two ways an
/// event can say: a create declares its own ([`RoomVersionKeys::declared`]),
/// anything else is named by the version of the room it is in.
///
/// Callers holding a room id already should use [`room_version`] directly rather
/// than re-reading the bytes.
pub async fn room_version_for_wire(
    store: &impl RoomStore,
    versions: &RoomVersions,
    raw: &serde_json::value::RawValue,
) -> Result<Arc<RoomVersion>, VersionError> {
    let keys = neutrino_event::room_version_keys(raw);
    match (keys.room_id, keys.declared) {
        // In a room: the room's persisted version is authoritative. Both keys
        // at once means a create carrying a `room_id` — malformed under v12, and
        // `from_wire` refuses it once named; a non-create cannot declare a
        // version at all (`room_version_keys` gates on `type`).
        (Some(room_id), _) => room_version(store, versions, &room_id).await,
        // A create, declaring the version it creates the room under.
        (None, Some(declared)) => versions
            .get(&declared)
            .cloned()
            .ok_or(VersionError::Unsupported(declared)),
        // A create declaring nothing: v12 rule 1.3 permits the field to be
        // absent, and the base version is what an absent declaration means.
        (None, None) => Ok(versions.base().clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neutrino_event::event_builder::EventBuilder;
    use neutrino_event::{ROOM_VERSION_ID, RoomVersion};
    use neutrino_store::RoomStore;
    use neutrino_store_sqlite::SqliteStore;
    use ruma::OwnedUserId;
    use serde_json::json;

    /// A second room version, so a store can hold a room this build's registry
    /// does not know about — the only way to reach `Unsupported`.
    fn other_version() -> RoomVersion {
        RoomVersion {
            id: "org.matrix.neutrino.test.other",
            rules: ruma::room_version_rules::RoomVersionRules::V12,
            ids: std::sync::Arc::new(neutrino_event::event_id::ReferenceHashIds),
            extra_redaction_keys: &["prev_state_events"],
        }
    }

    /// Create a room under `version` and hand back the store and its room id.
    async fn store_with_room(version: &std::sync::Arc<RoomVersion>) -> (SqliteStore, OwnedRoomId) {
        let store = SqliteStore::open_in_memory().await.expect("open store");
        let alice: OwnedUserId = "@alice:example.org".parse().expect("alice");
        let create = EventBuilder::new(alice, "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": version.id }))
            .version(Some(version.clone()))
            .build()
            .expect("build create");
        let room_id = create.room_id.clone();
        store.create_room(&create, &[]).await.expect("create_room");
        (store, room_id)
    }

    #[tokio::test]
    async fn resolves_a_room_of_a_known_version() {
        let versions = RoomVersions::base_only();
        let (store, room_id) = store_with_room(neutrino_event::base_version()).await;
        let got = room_version(&store, &versions, &room_id)
            .await
            .expect("base version resolves");
        assert_eq!(got.id, ROOM_VERSION_ID);
    }

    /// A room we are not in. Terminal: no retry can conjure the row, so a caller
    /// that retries would spin forever (and a caller that stages for it would
    /// leak rows).
    #[tokio::test]
    async fn a_room_we_do_not_have_is_terminal() {
        let store = SqliteStore::open_in_memory().await.expect("open store");
        let room_id: OwnedRoomId = "!nope:example.org".parse().expect("room id");
        let err = room_version(&store, &RoomVersions::base_only(), &room_id)
            .await
            .expect_err("unknown room");
        assert!(matches!(err, VersionError::UnknownRoom));
        assert!(!err.is_retryable(), "must not be retried");
    }

    /// A room on disk whose version this build does not speak — a store written
    /// by a build whose medium declared a version we lack. Also terminal: only a
    /// different build could read it.
    #[tokio::test]
    async fn a_room_of_an_unsupported_version_is_terminal() {
        let other = std::sync::Arc::new(other_version());
        let (store, room_id) = store_with_room(&other).await;

        // Same store, read by a registry that only knows the base version.
        let err = room_version(&store, &RoomVersions::base_only(), &room_id)
            .await
            .expect_err("unsupported version");
        assert!(matches!(&err, VersionError::Unsupported(v) if v == other.id));
        assert!(!err.is_retryable(), "must not be retried");

        // A registry that *does* know it resolves the same row.
        let versions = RoomVersions::new(Some(other_version())).expect("distinct ids");
        assert_eq!(
            room_version(&store, &versions, &room_id)
                .await
                .expect("resolves")
                .id,
            other.id
        );
    }

    /// Two rooms of different versions in ONE store, each resolving to its own —
    /// the property the registry exists for.
    #[tokio::test]
    async fn one_store_holds_rooms_of_several_versions() {
        let other = std::sync::Arc::new(other_version());
        let versions = RoomVersions::new(Some(other_version())).expect("distinct ids");
        let (store, other_room) = store_with_room(&other).await;

        // A base-version room in the same store.
        let alice: OwnedUserId = "@alice:example.org".parse().expect("alice");
        let base = neutrino_event::base_version();
        let create = EventBuilder::new(alice, "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": base.id }))
            .version(Some(base.clone()))
            .build()
            .expect("build create");
        let base_room = create.room_id.clone();
        store.create_room(&create, &[]).await.expect("create_room");

        assert_eq!(
            room_version(&store, &versions, &base_room)
                .await
                .expect("base room")
                .id,
            ROOM_VERSION_ID
        );
        assert_eq!(
            room_version(&store, &versions, &other_room)
                .await
                .expect("other room")
                .id,
            other.id
        );
    }
}
