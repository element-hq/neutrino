//! Closing a received PDU's missing ancestry by fetching it from a peer into
//! the pre-auth staging cache.
//!
//! Two fills, one pipeline:
//!
//! - [`fill_state_ancestry`] — **mandatory, unbounded**. `RoomCore::apply_pdu`
//!   returns a *retryable* [`CoreError`](neutrino_room::CoreError) when an
//!   event's `prev_state_events` ancestry (the auth-relevant state DAG,
//!   MSC4242) doesn't reach `m.room.create` in our store. We must *authorise*
//!   every PDU — concurrency reorders operations, so even a trusted peer's
//!   event can be invalid by DAG position — and an un-vetted event must never
//!   get a stream position or surface in any read / state-res path.
//! - [`fill_timeline_gap`] — **best-effort, one shot**. Transitive delivery of
//!   missed *messages*: a live-pushed PDU whose `prev_events` we don't hold
//!   asks its origin once for the recent timeline gap. State events sit in the
//!   timeline DAG too, so the common few-missed-events case pulls the missed
//!   state along in this single request and the state fill above never needs a
//!   round-trip of its own. Failure never blocks the event — a dangling
//!   `prev_events` edge is valid federation state (`/messages` backfill can
//!   still reach the tail).
//!
//! In both cases fetched ancestry is parked in the pre-auth staging cache
//! rather than persisted as history. This module owns the *fetch-into-staging*
//! half; the *apply* half is the inbound worker's drain loop ([`crate::worker`]):
//! once the gap is staged, the worker re-reads the room's staged rows,
//! toposorts them, and applies each through the per-room actor — staged
//! ancestry and freshly-received PDUs flow through the *same* loop, so there is
//! no separate "promote" step.

use neutrino_event::Event;
use neutrino_event::event_builder::from_wire;
use neutrino_store::{StagedKind, StorageBackend};
use ruma::{EventId, OwnedEventId, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;

use crate::ports::{MissingEventsFetcher, MissingEventsQuery};

/// Initial `limit` for the first gap-fill request; doubled each round (MSC4242
/// recommends exponentially increasing the limit until all ancestry is seen).
const INITIAL_GAPFILL_LIMIT: u32 = 10;

/// `limit` for the single best-effort timeline gap-fill round: the common case
/// is a handful of missed events, and anything deeper is deliberately left to
/// `/messages`-driven backfill rather than walked to completion.
const TIMELINE_GAPFILL_LIMIT: u32 = 10;

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
/// Returns `Ok(true)` once the ancestry is fully staged *and this call staged at
/// least one new event* — i.e. it made progress, so the worker should re-drain
/// and retry the PDU immediately. Returns `Ok(false)` when the ancestry was
/// *already* grounded on entry and nothing was fetched: the triggering
/// `apply_pdu` was retryable for a reason other than a real state-DAG gap (a
/// transient state-res / storage fault, or a not-yet-known room), so the worker
/// must back off rather than spin. Returns `Err(reason)` on a terminal failure
/// for *this* attempt: the peer has nothing new (an empty result, or a round
/// that re-sends only events we already hold — both "unfillable"), a peer
/// transport/HTTP failure, or a storage fault. In every non-`Ok(true)` case the
/// worker backs the PDU off; staged ancestry from a partial round is durable, so
/// a later retry resumes from it.
///
/// The loop is **unbounded** in rounds: grounding requires the *whole* state-DAG
/// ancestry to `m.room.create`, however deep (inherent to MSC4242 — like
/// fetching a full auth chain), so a real chain is walked to completion. It can
/// only stop early by *grounding* or by the peer running out of new events; a
/// trusted peer never feeds an infinite distinct chain.
pub(crate) async fn fill_state_ancestry<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
) -> Result<bool, String> {
    let room_id = &event.room_id;
    let earliest = state_dag_boundary(store, room_id).await;
    let mut limit = INITIAL_GAPFILL_LIMIT;
    // Whether any round staged a new event. `false` at a grounded exit means the
    // retryable verdict wasn't a real gap (transient fault) — the signal the
    // worker uses to back off instead of immediately retrying (which would spin).
    let mut made_progress = false;

    loop {
        let heads: Vec<&EventId> = event.prev_state_events.iter().map(|e| e.as_ref()).collect();
        let gap = store
            .ancestry_gap(room_id, &heads)
            .await
            .map_err(|e| e.to_string())?;
        if gap.missing.is_empty() {
            return Ok(made_progress);
        }

        // `latest` = the event plus the staged boundary. The peer excludes
        // these from its result but walks *through* them, so it returns only
        // the frontier below our cache — the "ask for 1-4, not 5-99" property.
        let mut latest = Vec::with_capacity(gap.staged.len() + 1);
        latest.push(event.event_id.clone());
        latest.extend(gap.staged);

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
                return Err("missing ancestry, gap unfillable: peer returned no events".to_owned());
            }
            Ok(fetched) => fetched,
            Err(e) => return Err(format!("peer fetch failed: {e}")),
        };

        let staged_new = stage_fetched(store, origin, room_id, fetched).await?.len();

        // No-progress guard (the loop's only non-grounding terminator besides an
        // empty fetch): a round that staged nothing new means the peer re-sent
        // only what we already hold, so it can't ground this gap. Bail; the PDU
        // backs off and a later retry (or a peer that has more) resumes from the
        // durable staged prefix.
        if staged_new == 0 {
            return Err("missing ancestry, gap unfillable: peer returned no new events".to_owned());
        }
        made_progress = true;
        limit = limit.saturating_mul(2);
    }
}

/// Best-effort, single-round fetch of `event`'s missing *timeline* ancestry
/// (transitive message delivery). One `get_missing_events` walking
/// `prev_events` back from `event` down to our timeline forward extremities,
/// capped at [`TIMELINE_GAPFILL_LIMIT`] — no recursion, no growing rounds.
/// Returns how many fetched events were newly staged; `0` (peer had nothing
/// new) and `Err` (peer/storage fault) are both "apply the event as-is" to the
/// caller — unlike the state fill, this never blocks an event.
///
/// Skipped for an unknown room (`forward_extremities` is `None`): there is no
/// boundary to walk to, and the worker drops unknown-room PDUs at apply anyway.
pub(crate) async fn fill_timeline_gap<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
) -> Result<usize, String> {
    let room_id = &event.room_id;
    let Ok(Some((timeline, _state))) = store.forward_extremities(room_id).await else {
        return Ok(0);
    };
    let earliest: Vec<OwnedEventId> = timeline.into_iter().collect();
    let latest = [event.event_id.clone()];

    let fetched = fetcher
        .fetch(MissingEventsQuery {
            origin,
            room_id,
            latest: &latest,
            earliest: &earliest,
            limit: TIMELINE_GAPFILL_LIMIT,
            // The point of this fill: walk the timeline DAG. Missed state
            // events ride along (they are timeline events too), so the state
            // fill usually never needs its own request.
            state_dag: false,
            // The triggering event is already staged; only its ancestry is wanted.
            include_latest_events: false,
        })
        .await
        .map_err(|e| format!("peer fetch failed: {e}"))?;

    Ok(stage_fetched(store, origin, room_id, fetched).await?.len())
}

/// Stage a `get_missing_events` response: each PDU is keyed under its
/// *computed* id (`from_wire` derives it from the reference hash and yields
/// canonical bytes, so id ↔ bytes round-trip); an unkeyable PDU is dropped. A
/// peer can return events for any room; only events in `room_id` are staged —
/// a foreign-room event is never reachable by this room's drain or
/// `ancestry_gap` walk, so staging it would be unreachable junk.
///
/// Rows are staged under `origin` — the peer that returned them, not their
/// true author. That's deliberate: if a staged event itself later needs
/// gap-filling we ask the same peer (it vouched for the reference), and
/// `origin` is otherwise unused (no signature checks in the trusted mesh). At
/// worst a wrong peer delays grounding, which the no-progress terminator and a
/// later redelivery resolve. Kind is always [`StagedKind::Fetched`]: a pulled
/// event must never trigger a further timeline fetch of its own.
///
/// Returns the ids of the newly staged rows (already-held events dedup to
/// nothing inside `stage_pdu`).
pub(crate) async fn stage_fetched(
    store: &impl StorageBackend,
    origin: &ServerName,
    room_id: &RoomId,
    fetched: Vec<Box<RawJsonValue>>,
) -> Result<Vec<OwnedEventId>, String> {
    let mut staged = Vec::new();
    for raw in fetched {
        let Ok(ev) = from_wire(raw, Vec::new()) else {
            continue;
        };
        if ev.room_id != *room_id {
            continue;
        }
        if store
            .stage_pdu(origin, room_id, &ev.event_id, StagedKind::Fetched, &ev.raw)
            .await
            .map_err(|e| e.to_string())?
        {
            staged.push(ev.event_id);
        }
    }
    Ok(staged)
}

/// The room's state-DAG forward extremities — the committed bottom boundary
/// (`earliest_events`) for a state-DAG gap-fill walk. Best-effort: empty if the
/// room is unknown or the lookup faults.
async fn state_dag_boundary(store: &impl StorageBackend, room_id: &RoomId) -> Vec<OwnedEventId> {
    match store.forward_extremities(room_id).await {
        Ok(Some((_timeline, state))) => state.into_iter().collect(),
        _ => Vec::new(),
    }
}
