//! Closing a received PDU's missing state-DAG ancestry by fetching it from a
//! peer into the pre-auth staging cache.
//!
//! `RoomCore::apply_pdu` returns a *retryable* [`CoreError`](neutrino_room::CoreError)
//! when an event's `prev_state_events` ancestry (the auth-relevant state DAG,
//! MSC4242) doesn't reach `m.room.create` in our store. We must *authorise*
//! every PDU — concurrency reorders operations, so even a trusted peer's event
//! can be invalid by DAG position — and an un-vetted event must never get a
//! stream position or surface in any read / state-res path. So fetched ancestry
//! is parked in a pre-auth staging cache rather than persisted as history.
//!
//! Timeline ancestry (`prev_events`) is not needed to authorise a PDU, so it is
//! never walked to completion here: a dangling timeline parent becomes a
//! backward extremity for `/backfill`. But when a PDU needs gap-filling at all,
//! one bounded timeline-DAG round runs first, so a peer that answers state-DAG
//! walks only for events in its own timeline (the common resident) is asked
//! about the PDU's immediate timeline parent before the state walk starts.
//!
//! This module owns the *fetch-into-staging* half ([`fill_state_ancestry`]).
//! The *apply* half is the inbound worker's drain loop ([`crate::worker`]):
//! once the gap is staged, the worker re-reads the room's staged rows,
//! toposorts them, and applies each through the per-room actor — staged
//! ancestry and freshly-received PDUs flow through the *same* loop, so there is
//! no separate "promote" step.

use neutrino_event::{Event, EventPolicy};
use neutrino_store::StorageBackend;
use ruma::{EventId, OwnedEventId, RoomId, ServerName};

use crate::ports::{MissingEventsFetcher, MissingEventsQuery, TransportError};
use crate::util::room_version;

/// Initial `limit` for the first gap-fill request; doubled each round (MSC4242
/// recommends exponentially increasing the limit until all ancestry is seen).
const INITIAL_GAPFILL_LIMIT: u32 = 10;

/// Fetch `event`'s missing state-DAG ancestry into the staging cache until it
/// is grounded (every `prev_state_events` path reaches an event we hold),
/// preceded by one bounded timeline-DAG round for a missing `prev_events`
/// parent ([`fill_timeline_parents`]).
///
/// Each round recomputes the gap over `events ∪ staged_events`; an empty
/// `missing` set means done. Otherwise we ask the peer, passing `latest = the
/// frontier` (the staged events, and `event` itself, whose parents are
/// missing) and `earliest = our state-DAG forward extremities` (the committed
/// bottom boundary), so each round fetches only the events directly below what
/// we hold. The newly fetched events are staged, not applied — the worker's
/// drain loop applies them.
///
/// Returns `Ok(true)` once the ancestry is fully staged *and this call staged at
/// least one new event* — i.e. it made progress, so the worker should re-drain
/// and retry the PDU immediately. Returns `Ok(false)` when the ancestry was
/// *already* grounded on entry and nothing was fetched: the triggering
/// `apply_pdu` was retryable for a reason other than a real state-DAG gap (a
/// transient state-res / storage fault, or a not-yet-known room), so the worker
/// must back off rather than spin. Returns `Err` on a failure of *this*
/// attempt, classified for the worker ([`GapFillError`]):
/// [`Unfillable`](GapFillError::Unfillable) when the peer answered but cannot
/// supply a path to `m.room.create` (an empty result, or a round that re-sends
/// only events we already hold) — the PDU is rejected, per MSC4242's "SHOULD
/// reject the `/send`"; [`Transient`](GapFillError::Transient) on a peer
/// transport/HTTP failure or a storage fault — the PDU backs off and retries.
///
/// Every ancestor this stages is tagged `fetched_for = event` (see
/// `staged_events.fetched_for`), so the worker never treats it as a gap-fill
/// root of its own — a partially-fetched, ungrounded ancestry must not be
/// promoted into independent PDUs that each re-walk the peer. Fetched rows
/// live and die with `event`: on `Transient` they stay staged, so the retry's
/// `ancestry_gap` resumes from the staged frontier rather than refetching; on
/// `Unfillable` the worker deletes them together with `event`
/// (`StagingStore::unstage_with_fetched`).
///
/// The loop is **unbounded** in rounds: grounding requires the *whole* state-DAG
/// ancestry to `m.room.create`, however deep (inherent to MSC4242 — like
/// fetching a full auth chain), so a real chain is walked to completion. It can
/// only stop early by *grounding* or by the peer running out of new events; a
/// trusted peer never feeds an infinite distinct chain.
/// Why a [`fill_state_ancestry`] attempt failed, and hence what the worker does
/// with the PDU: reject it, or back it off and retry.
#[derive(Debug)]
pub(crate) enum GapFillError {
    /// The peer answered, but cannot supply a path from the PDU's
    /// `prev_state_events` to `m.room.create` — nothing new, or a 4xx refusal.
    /// The PDU can never be authorised from this peer; MSC4242 says to reject it.
    Unfillable(String),
    /// Reaching the peer failed (transport / 5xx), or our own storage
    /// faulted. Says nothing about the DAG; retry later.
    Transient(String),
}

impl std::fmt::Display for GapFillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unfillable(reason) => write!(f, "gap unfillable: {reason}"),
            Self::Transient(reason) => write!(f, "transient: {reason}"),
        }
    }
}

pub(crate) async fn fill_state_ancestry<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
    policy: &EventPolicy,
) -> Result<bool, GapFillError> {
    let room_id = &event.room_id;
    // The version every fetched ancestor is named under. A failure here is a
    // storage fault (or a not-yet-known room), so it is transient.
    let version = room_version(store, &policy.versions, room_id)
        .await
        .map_err(|e| GapFillError::Transient(e.to_string()))?;
    let (timeline_boundary, earliest) = boundaries(store, room_id).await;
    let mut limit = INITIAL_GAPFILL_LIMIT;
    // Whether any round staged a new event. `false` at a grounded exit means the
    // retryable verdict wasn't a real gap (transient fault) — the signal the
    // worker uses to back off instead of immediately retrying (which would spin).
    let mut made_progress = fill_timeline_parents(
        store,
        origin,
        event,
        fetcher,
        policy,
        &version,
        &timeline_boundary,
    )
    .await;

    loop {
        let heads: Vec<&EventId> = event.prev_state_events.iter().map(|e| e.as_ref()).collect();
        let gap = store
            .ancestry_gap(room_id, &heads)
            .await
            .map_err(|e| GapFillError::Transient(e.to_string()))?;
        if gap.missing.is_empty() {
            return Ok(made_progress);
        }

        // `event` is not a walk head (the walk starts at its parents), so it
        // joins the frontier itself when one of those parents is missing.
        let mut latest = gap.frontier;
        if event
            .prev_state_events
            .iter()
            .any(|p| gap.missing.contains(p))
        {
            latest.push(event.event_id.clone());
        }

        let fetched = match fetcher
            .fetch(MissingEventsQuery {
                origin,
                room_id,
                latest: &latest,
                earliest: &earliest,
                limit,
                // Gap-fill walks the state DAG (the ancestry `apply_pdu` needs)
                // and only ever wants ancestry, never the heads themselves.
                state_dag: true,
                include_latest_events: false,
            })
            .await
        {
            Ok(fetched) if fetched.is_empty() => {
                return Err(GapFillError::Unfillable(
                    "peer returned no events".to_owned(),
                ));
            }
            Ok(fetched) => fetched,
            // A 4xx is the peer's final word for this request (no such room,
            // endpoint unsupported): nothing a retry can change.
            Err(e @ TransportError::Status(400..=499)) => {
                return Err(GapFillError::Unfillable(format!("peer refused: {e}")));
            }
            Err(e) => return Err(GapFillError::Transient(format!("peer fetch failed: {e}"))),
        };

        let staged_new = stage_fetched(store, policy, &version, origin, event, fetched)
            .await
            .map_err(GapFillError::Transient)?;

        // No-progress guard (the loop's only non-grounding terminator besides an
        // empty fetch): a round that staged nothing new means the peer re-sent
        // only what we already hold, so it can't ground this gap. Bail; the
        // worker rejects the PDU and deletes the ancestry fetched for it.
        if staged_new == 0 {
            return Err(GapFillError::Unfillable(
                "peer returned no new events".to_owned(),
            ));
        }
        made_progress = true;
        limit = limit.saturating_mul(2);
    }
}

/// One bounded timeline-DAG round: if any of `event.prev_events` is held
/// nowhere (neither committed nor staged), ask the peer once for `event`'s
/// timeline ancestry at the initial limit and stage what comes back. Never
/// recursive — the timeline parents are not needed to authorise `event`, so a
/// peer failure or an empty answer just skips the round. Returns whether it
/// staged anything new.
async fn fill_timeline_parents<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
    policy: &EventPolicy,
    version: &std::sync::Arc<neutrino_event::RoomVersion>,
    timeline_boundary: &[OwnedEventId],
) -> bool {
    let room_id = &event.room_id;
    // `ancestry_gap` classifies its heads too: a head in neither table is in
    // `missing`. Only the heads matter here, not what lies below a staged one.
    let heads: Vec<&EventId> = event.prev_events.iter().map(|e| e.as_ref()).collect();
    let parent_missing = match store.ancestry_gap(room_id, &heads).await {
        Ok(gap) => event.prev_events.iter().any(|p| gap.missing.contains(p)),
        Err(e) => {
            tracing::warn!(%room_id, error = %e, "gapfill: timeline parent lookup failed");
            false
        }
    };
    if !parent_missing {
        return false;
    }
    let fetched = match fetcher
        .fetch(MissingEventsQuery {
            origin,
            room_id,
            latest: std::slice::from_ref(&event.event_id),
            earliest: timeline_boundary,
            limit: INITIAL_GAPFILL_LIMIT,
            state_dag: false,
            include_latest_events: false,
        })
        .await
    {
        Ok(fetched) => fetched,
        Err(e) => {
            tracing::warn!(%room_id, error = %e, "gapfill: timeline round failed; continuing with the state walk");
            return false;
        }
    };
    match stage_fetched(store, policy, version, origin, event, fetched).await {
        Ok(staged_new) => staged_new > 0,
        Err(e) => {
            tracing::warn!(%room_id, error = %e, "gapfill: staging timeline ancestry failed; continuing with the state walk");
            false
        }
    }
}

/// Stage a peer's `get_missing_events` answer on behalf of `root` (each row
/// tagged `fetched_for = root`), returning how many rows were new.
///
/// Each event is staged under its *computed* id (`from_wire` derives it from
/// the reference hash and yields canonical bytes, so id ↔ bytes round-trip). An
/// unkeyable PDU is dropped. A peer can return events for any room; only ones
/// in *this* room are staged — a foreign-room event is never reachable by this
/// room's `ancestry_gap` walk, so staging it would be unreachable junk that
/// nothing ever drains. Both `Wire` variants are staged: a `Rejected` ancestor
/// is exactly the cascade terminator — the worker persists it rejected and the
/// descendant's reference check ends via `PrevStateRejected`. A drop-class
/// ancestor (`Err`) is never staged, so a round that fetches only those stages
/// nothing and the caller's no-progress terminator declares the gap unfillable.
/// Rows are staged under `origin` — the peer that referenced them, not their
/// true author — so if an ancestor itself later needs gap-filling we ask the
/// same peer (it vouched for the reference); `origin` is otherwise unused in
/// the trusted mesh.
async fn stage_fetched(
    store: &impl StorageBackend,
    policy: &EventPolicy,
    version: &std::sync::Arc<neutrino_event::RoomVersion>,
    origin: &ServerName,
    root: &Event,
    fetched: Vec<Box<serde_json::value::RawValue>>,
) -> Result<usize, String> {
    let room_id = &root.room_id;
    let mut staged_new = 0usize;
    for raw in fetched {
        let Ok(wire) = policy.admit_wire(raw, version).await else {
            continue;
        };
        if let neutrino_event::Wire::Rejected(ev, defect) = &wire {
            tracing::warn!(event_id = %ev.event_id, %defect, "gapfill: staging malformed ancestor as rejected");
        }
        let ancestor = wire.into_event();
        if ancestor.room_id != *room_id {
            continue;
        }
        if store
            .stage_pdu(
                origin,
                room_id,
                &ancestor.event_id,
                &ancestor.raw,
                Some(&root.event_id),
            )
            .await
            .map_err(|e| e.to_string())?
        {
            staged_new += 1;
        }
    }
    Ok(staged_new)
}

/// The room's `(timeline, state)` forward extremities — the committed bottom
/// boundaries (`earliest_events`) for the timeline round and the state-DAG walk
/// respectively. Best-effort: both empty if the room is unknown or the lookup
/// faults.
async fn boundaries(
    store: &impl StorageBackend,
    room_id: &RoomId,
) -> (Vec<OwnedEventId>, Vec<OwnedEventId>) {
    match store.forward_extremities(room_id).await {
        Ok(Some((timeline, state))) => {
            (timeline.into_iter().collect(), state.into_iter().collect())
        }
        _ => (Vec::new(), Vec::new()),
    }
}
