//! Forward-extremity reconciliation (anti-entropy).
//!
//! Federation fan-out picks an event's recipients from a point-in-time snapshot
//! of the room's joined set, so a server that is concurrently (re)joining can be
//! omitted and then never told about an event — a permanent divergence between
//! servers that both believe they are synced. To close that gap, peers advertise
//! their per-room forward extremities on every `/send` (request *and* response,
//! so a single transaction reconciles both ends). On seeing an advertised head we
//! do not hold, we fetch it — together with any ancestry between it and our own
//! heads — in a single `get_missing_events` with `include_latest_events`, stage
//! it into the pre-auth cache, and poke the inbound worker to auth + apply it.
//!
//! No new apply path: staging + the worker drain are exactly the inbound `/send`
//! pipeline (the inbound `/send` handler / [`crate::worker`]), and the
//! state-DAG gap-fill (`gapfill`) grounds anything deeper than
//! one fetch. Reconciliation only ever *adds work to that pipeline*; it grants no
//! trust — a fetched event is auth-checked and state-resolved like any other PDU.

use std::collections::BTreeSet;

use neutrino_event::event_builder::from_wire;
use neutrino_store::{StateStore, StorageBackend};
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, ServerName};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::ports::{ForwardExtremities, MissingEventsFetcher, MissingEventsQuery};

/// Whether `server` has a joined member in `room_id`. A store-backed predicate
/// (not an `X-Matrix` check): the advertised heads a peer sends are
/// attacker-controllable, so reconciliation only honours an advertisement from a
/// peer that actually shares the room. Also gates the read-scoped federation
/// handlers (`backfill`, `get_missing_events`) in `neutrino-http`.
pub async fn server_in_room(
    store: &impl StateStore,
    room_id: &RoomId,
    server: &ServerName,
) -> Result<bool, neutrino_store::StorageError> {
    Ok(store
        .joined_members(room_id)
        .await?
        .keys()
        .any(|user| user.server_name() == server))
}

/// Initial `limit` for a reconciliation fetch. We only need the advertised
/// head(s) staged — the worker's state-DAG gap-fill grounds anything deeper — so
/// a modest page suffices; it is not the whole-ancestry budget.
const RECONCILE_LIMIT: u32 = 50;

/// The first few event ids as a loggable list — a debugging aid on the
/// anti-entropy log lines. The set is usually ≤ 3 (a room's forward extremities),
/// so this is bounded; the cap guards a pathologically forked DAG.
fn sample_ids(ids: &[OwnedEventId]) -> Vec<&str> {
    ids.iter().take(3).map(|e| e.as_str()).collect()
}

/// This server's current forward extremities for `room_id`, in the wire shape,
/// for advertising to a peer. Empty if the room is unknown or the lookup faults
/// (best-effort: a missing advertisement just means no reconciliation this round).
pub async fn local_extremities(
    store: &impl StorageBackend,
    room_id: &RoomId,
) -> ForwardExtremities {
    match store.forward_extremities(room_id).await {
        Ok(Some((timeline, state))) => ForwardExtremities {
            timeline: timeline.into_iter().collect(),
            state: state.into_iter().collect(),
        },
        _ => ForwardExtremities::default(),
    }
}

/// Reconcile our view of `room_id` against `advertised`: fetch any advertised
/// head we lack (plus the ancestry between it and our heads) via one
/// `get_missing_events` with `include_latest_events`, stage it, and poke the
/// worker to auth + apply. Best-effort — logs and returns on any peer/storage
/// fault; the next advertisement retries. A no-op when we hold every advertised
/// head (the common, converged case): only set-membership checks, no peer call.
pub async fn reconcile_room<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    fetcher: &F,
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    peer: &ServerName,
    room_id: &RoomId,
    advertised: &ForwardExtremities,
) {
    if advertised.is_empty() {
        return;
    }
    // Only honour advertisements from a peer that actually shares this room. The
    // advertised heads are attacker-controllable, so without this a peer could
    // induce us to fetch from it for any room we host — even one it isn't in.
    // (The fetched events are still independently auth-checked by the worker, so
    // this is a fetch-amplification scope, not an integrity gate.)
    match server_in_room(store, room_id, peer).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: peer-membership check failed");
            return;
        }
    }
    // Only reconcile rooms we actually host — `forward_extremities` is `None` for
    // an unknown room.
    let Ok(Some((our_timeline, our_state))) = store.forward_extremities(room_id).await else {
        return;
    };
    let our_timeline: Vec<OwnedEventId> = our_timeline.into_iter().collect();
    let our_state: Vec<OwnedEventId> = our_state.into_iter().collect();

    // A head we already fetched is *staged* pending the worker (and stays staged
    // for the whole duration of a partition, until its ancestry can be grounded).
    // Treat staged-but-not-yet-applied events as present too, or every subsequent
    // transaction would re-detect the same head as "unknown" and re-fetch it — a
    // `get_missing_events` per transaction while it drains. With this, each
    // genuinely-missing head triggers at most one fetch.
    let staged_ids: BTreeSet<OwnedEventId> = match store.staged_for_room(room_id).await {
        Ok(rows) => rows.into_iter().map(|p| p.event_id).collect(),
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: listing staged events failed");
            BTreeSet::new()
        }
    };

    // State heads first (the auth-relevant DAG), then timeline heads. Each walks
    // the matching DAG so the fetched ancestry is the kind `apply_pdu` needs.
    let (mut staged, _) = fetch_unknown(
        store,
        fetcher,
        peer,
        room_id,
        &advertised.state,
        &our_state,
        &staged_ids,
        true,
        RECONCILE_LIMIT,
    )
    .await;
    let (staged_timeline, _) = fetch_unknown(
        store,
        fetcher,
        peer,
        room_id,
        &advertised.timeline,
        &our_timeline,
        &staged_ids,
        false,
        RECONCILE_LIMIT,
    )
    .await;
    staged.extend(staged_timeline);

    // Hand the staged events to the inbound worker (toposort + auth + apply).
    // Best-effort poke: a full channel means the worker already has the room
    // queued, and its drain still picks these rows up.
    if !staged.is_empty() {
        info!(
            target: "neutrino_http",
            %peer,
            %room_id,
            count = staged.len(),
            events = ?sample_ids(&staged),
            "anti-entropy: reconciled a divergence — staged events from a peer's advertised heads, poking worker",
        );
        let _ = worker_poke.try_send(room_id.to_owned());
    }
}

/// Batch size per `get_missing_events` round of a timeline-prev gap-fill. Small
/// on purpose: the fetch often rides the same congested pipe the gap came from,
/// and each round's response must fit comfortably in one Q-Block transfer.
const PREV_GAP_LIMIT: u32 = 10;

/// Rounds per trigger. A gap deeper than `PREV_GAP_MAX_ROUNDS × PREV_GAP_LIMIT`
/// events is deliberately left unfilled — that's history, and paging history is
/// `/messages` backfill's job (or a later anti-entropy round); an inbound
/// transaction must not turn into an unbounded walk of a peer's DAG.
const PREV_GAP_MAX_ROUNDS: u32 = 3;

/// Fetch the missing `prev_events` of a just-received transaction's PDUs from
/// the transaction's origin. The origin referenced these ids (they are its own
/// events' parents), so it provably holds them — asking it directly closes a
/// timeline gap in seconds instead of waiting for the events' author to push
/// them through a possibly-wedged pipe of its own, which is how a one-line
/// message once took two minutes to arrive.
///
/// Bounded on both axes ([`PREV_GAP_LIMIT`], [`PREV_GAP_MAX_ROUNDS`]): each
/// round fetches one small page walking the timeline DAG down from the current
/// frontier, the next round's frontier is the prevs of what was just staged,
/// and after the round budget whatever is still missing stays a gap. Prevs we
/// already hold (or have staged) cost only the membership checks — the common
/// linear-chat case where a PDU's prev is simply our current head is a cheap
/// no-op. Same trust model as [`reconcile_room`]: fetched events are staged
/// pre-auth and the worker auth-checks them like any PDU.
pub async fn fill_timeline_prev_gap<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    fetcher: &F,
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    peer: &ServerName,
    room_id: &RoomId,
    prevs: Vec<OwnedEventId>,
) {
    if prevs.is_empty() {
        return;
    }
    // Same fetch-amplification scope guard as `reconcile_room`: PDU contents
    // (and so their prev ids) are attacker-controllable.
    match server_in_room(store, room_id, peer).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "prev-gap: peer-membership check failed");
            return;
        }
    }
    let Ok(Some((our_timeline, _))) = store.forward_extremities(room_id).await else {
        return;
    };
    let earliest: Vec<OwnedEventId> = our_timeline.into_iter().collect();

    let mut heads = prevs;
    for round in 0..PREV_GAP_MAX_ROUNDS {
        let staged_ids: BTreeSet<OwnedEventId> = match store.staged_for_room(room_id).await {
            Ok(rows) => rows.into_iter().map(|p| p.event_id).collect(),
            Err(e) => {
                warn!(%peer, %room_id, error = %e, "prev-gap: listing staged events failed");
                return;
            }
        };
        let (staged, staged_prevs) = fetch_unknown(
            store,
            fetcher,
            peer,
            room_id,
            &heads,
            &earliest,
            &staged_ids,
            false,
            PREV_GAP_LIMIT,
        )
        .await;
        if staged.is_empty() {
            // Grounded (every head held/staged), or the peer had nothing new,
            // or the fetch failed — in all cases there is no next frontier.
            return;
        }
        info!(
            target: "neutrino_http",
            %peer,
            %room_id,
            round,
            count = staged.len(),
            events = ?sample_ids(&staged),
            "prev-gap: staged missing prev_events from the transaction's origin, poking worker",
        );
        let _ = worker_poke.try_send(room_id.to_owned());
        heads = staged_prevs;
    }
    // Round budget exhausted with a live frontier: leave the rest unfilled.
    info!(
        %peer, %room_id,
        frontier = heads.len(),
        "prev-gap: round budget exhausted; leaving deeper history to backfill",
    );
}

/// Fetch the advertised `heads` we don't already hold — with their ancestry down
/// to `earliest` — in a single `get_missing_events` (`include_latest_events` so
/// the heads themselves come back, not just their ancestors), and stage them.
/// Returns `(staged, staged_prevs)`: the ids of the events it newly staged, and
/// those events' `prev_events` — the next-deeper timeline frontier, which
/// [`fill_timeline_prev_gap`] feeds back in as the next round's heads.
#[allow(clippy::too_many_arguments)]
async fn fetch_unknown<F: MissingEventsFetcher + ?Sized>(
    store: &impl StorageBackend,
    fetcher: &F,
    peer: &ServerName,
    room_id: &RoomId,
    heads: &[OwnedEventId],
    earliest: &[OwnedEventId],
    staged_ids: &BTreeSet<OwnedEventId>,
    state_dag: bool,
    limit: u32,
) -> (Vec<OwnedEventId>, Vec<OwnedEventId>) {
    if heads.is_empty() {
        return (Vec::new(), Vec::new());
    }
    // `get_events` returns only the events we hold, so it doubles as an existence
    // filter: an advertised head it omits is one we are missing. A head already
    // staged (`staged_ids`) is in the pipeline too, so it isn't "unknown".
    let head_refs: Vec<&EventId> = heads.iter().map(|e| e.as_ref()).collect();
    let committed: BTreeSet<OwnedEventId> = match store.get_events(&head_refs).await {
        Ok(events) => events.into_iter().map(|e| e.event_id).collect(),
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: looking up advertised heads failed");
            return (Vec::new(), Vec::new());
        }
    };
    let unknown: Vec<OwnedEventId> = heads
        .iter()
        .filter(|h| !committed.contains(*h) && !staged_ids.contains(*h))
        .cloned()
        .collect();
    if unknown.is_empty() {
        return (Vec::new(), Vec::new());
    }
    info!(
        target: "neutrino_http",
        %peer,
        %room_id,
        dag = if state_dag { "state" } else { "timeline" },
        count = unknown.len(),
        events = ?sample_ids(&unknown),
        "anti-entropy: peer advertised forward extremities we lack; fetching",
    );

    let fetched = match fetcher
        .fetch(MissingEventsQuery {
            origin: peer,
            room_id,
            latest: &unknown,
            earliest,
            limit,
            state_dag,
            include_latest_events: true,
        })
        .await
    {
        Ok(fetched) => fetched,
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: fetching advertised heads failed");
            return (Vec::new(), Vec::new());
        }
    };

    let mut staged_new: Vec<OwnedEventId> = Vec::new();
    let mut staged_prevs: Vec<OwnedEventId> = Vec::new();
    for raw in fetched {
        // Derive each event's id from its bytes (`from_wire`); an unkeyable PDU is
        // dropped. Only stage events for *this* room — a foreign-room event is
        // never reachable by this room's drain, so it would be junk.
        let Ok(ev) = from_wire(raw, Vec::new()) else {
            continue;
        };
        if ev.room_id != *room_id {
            continue;
        }
        match store.stage_pdu(peer, room_id, &ev.event_id, &ev.raw).await {
            Ok(true) => {
                staged_prevs.extend(ev.prev_events.iter().cloned());
                staged_new.push(ev.event_id);
            }
            Ok(false) => {}
            Err(e) => {
                warn!(%peer, %room_id, error = %e, "reconcile: staging a fetched event failed")
            }
        }
    }
    (staged_new, staged_prevs)
}
