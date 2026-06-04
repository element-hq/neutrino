//! Closing a received PDU's missing state-DAG ancestry by fetching it from a
//! peer into the pre-auth staging cache.
//!
//! `RoomCore::apply_pdu` returns a *retryable* [`CoreError`](neutrino_state::CoreError)
//! when an event's `prev_state_events` ancestry (the auth-relevant state DAG,
//! MSC4242) doesn't reach `m.room.create` in our store. We must *authorise*
//! every PDU — concurrency reorders operations, so even a trusted peer's event
//! can be invalid by DAG position — and an un-vetted event must never get a
//! stream position or surface in any read / state-res path. So fetched ancestry
//! is parked in a pre-auth staging cache rather than persisted as history.
//!
//! This module owns the *fetch-into-staging* half ([`fill_state_ancestry`]).
//! The *apply* half is the inbound worker's drain loop ([`crate::federation::worker`]):
//! once the gap is staged, the worker re-reads the room's staged rows,
//! toposorts them, and applies each through the per-room actor — staged
//! ancestry and freshly-received PDUs flow through the *same* loop, so there is
//! no separate "promote" step.

use neutrino_common::Event;
use neutrino_state::event_id::from_wire;
use neutrino_store::{RoomStore, StagingStore};
use ruma::{EventId, OwnedEventId, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::client::FederationClientError;

/// Initial `limit` for the first gap-fill request; doubled each round (MSC4242
/// recommends exponentially increasing the limit until all ancestry is seen).
const INITIAL_GAPFILL_LIMIT: u32 = 10;

/// Fetches missing state-DAG ancestry from a peer to close a gap, via
/// `POST origin/_matrix/federation/v1/get_missing_events` with `state_dag: true`
/// (MSC4242). The production impl is
/// [`crate::federation::client::ReqwestFetcher`]; tests inject a stub. Held on
/// `AppState` as an `Arc<dyn MissingEventsFetcher>`.
#[async_trait::async_trait]
pub(crate) trait MissingEventsFetcher: Send + Sync {
    /// Walk back from `latest` (stopping at `earliest`) up to `limit` events,
    /// returning opaque PDU bytes oldest-first. `Ok(empty)` means the peer gave
    /// us nothing new (the caller treats it as an unfillable gap); `Err` is a
    /// transport/HTTP failure reaching the peer.
    async fn fetch(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        earliest: &[OwnedEventId],
        limit: u32,
    ) -> Result<Vec<Box<RawJsonValue>>, FederationClientError>;
}

/// Fetch `event`'s missing state-DAG ancestry into the staging cache until it
/// is grounded (every `prev_state_events` path reaches an event we hold).
///
/// Each round recomputes the gap over `events ∪ staged_events`; an empty
/// `missing` frontier means done. Otherwise we ask the peer, passing
/// `latest = event + the staged frontier` (so the peer walks down *through*
/// what we've cached without re-sending it) and `earliest = our state-DAG
/// forward extremities` (the committed bottom boundary). The newly fetched
/// events are staged, not applied — the worker's drain loop applies them.
///
/// Returns `Ok(())` once the ancestry is fully staged. Returns `Err(reason)` on
/// any terminal condition for *this* attempt: the peer has nothing new (an
/// empty result, or a round that re-sends only events we already hold — both
/// "unfillable"), a peer transport/HTTP failure, or a storage fault. The caller
/// (the worker) backs the PDU off and retries later; staged ancestry from a
/// partial round is left durable, so a retry resumes from it.
///
/// The loop is **unbounded** in rounds: grounding requires the *whole* state-DAG
/// ancestry to `m.room.create`, however deep (inherent to MSC4242 — like
/// fetching a full auth chain), so a real chain is walked to completion. It can
/// only stop early by *grounding* or by the peer running out of new events; a
/// trusted peer never feeds an infinite distinct chain.
pub(crate) async fn fill_state_ancestry<F: MissingEventsFetcher + ?Sized>(
    store: &neutrino_store_sqlite::SqliteStore,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
) -> Result<(), String> {
    let room_id = &event.room_id;
    let earliest = state_dag_boundary(store, room_id).await;
    let mut limit = INITIAL_GAPFILL_LIMIT;

    loop {
        let heads: Vec<&EventId> = event.prev_state_events.iter().map(|e| e.as_ref()).collect();
        let gap = store
            .ancestry_gap(room_id, &heads)
            .await
            .map_err(|e| e.to_string())?;
        if gap.missing.is_empty() {
            return Ok(());
        }

        // `latest` = the event plus the staged boundary. The peer excludes
        // these from its result but walks *through* them, so it returns only
        // the frontier below our cache — the "ask for 1-4, not 5-99" property.
        let mut latest = Vec::with_capacity(gap.staged.len() + 1);
        latest.push(event.event_id.clone());
        latest.extend(gap.staged);

        let fetched = match fetcher
            .fetch(origin, room_id, &latest, &earliest, limit)
            .await
        {
            Ok(fetched) if fetched.is_empty() => {
                return Err("missing ancestry, gap unfillable: peer returned no events".to_owned());
            }
            Ok(fetched) => fetched,
            Err(e) => return Err(format!("peer fetch failed: {e}")),
        };

        // Stage under each event's *computed* id (`from_wire` derives it from
        // the reference hash and yields canonical bytes, so id ↔ bytes
        // round-trip). An unkeyable PDU is dropped. A peer can return events for
        // any room; only stage ones in *this* room — a foreign-room event is
        // never reachable by this room's `ancestry_gap` walk, so staging it
        // would be unreachable junk that nothing ever drains.
        let mut staged_new = 0usize;
        for raw in fetched {
            if let Ok(ancestor) = from_wire(raw, Vec::new()) {
                if ancestor.room_id != *room_id {
                    continue;
                }
                if store
                    .stage_pdu(origin, room_id, &ancestor.event_id, &ancestor.raw)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    staged_new += 1;
                }
            }
        }

        // No-progress guard (the loop's only non-grounding terminator besides an
        // empty fetch): a round that staged nothing new means the peer re-sent
        // only what we already hold, so it can't ground this gap. Bail; the PDU
        // backs off and a later retry (or a peer that has more) resumes from the
        // durable staged prefix.
        if staged_new == 0 {
            return Err("missing ancestry, gap unfillable: peer returned no new events".to_owned());
        }
        limit = limit.saturating_mul(2);
    }
}

/// The room's state-DAG forward extremities — the committed bottom boundary
/// (`earliest_events`) for a state-DAG gap-fill walk. Best-effort: empty if the
/// room is unknown or the lookup faults.
async fn state_dag_boundary(
    store: &neutrino_store_sqlite::SqliteStore,
    room_id: &RoomId,
) -> Vec<OwnedEventId> {
    match store.forward_extremities(room_id).await {
        Ok(Some((_timeline, state))) => state.into_iter().collect(),
        _ => Vec::new(),
    }
}
