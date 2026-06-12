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
//! pipeline (`crate::federation::send` / `crate::federation::worker`), and the
//! state-DAG gap-fill (`crate::federation::gapfill`) grounds anything deeper than
//! one fetch. Reconciliation only ever *adds work to that pipeline*; it grants no
//! trust — a fetched event is auth-checked and state-resolved like any other PDU.

use std::collections::BTreeSet;

use neutrino_state::event_id::from_wire;
use neutrino_store::{EventStore, RoomStore, StagingStore};
use neutrino_store_sqlite::SqliteStore;
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, ServerName};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::federation::gapfill::{MissingEventsFetcher, MissingEventsQuery};

/// Initial `limit` for a reconciliation fetch. We only need the advertised
/// head(s) staged — the worker's state-DAG gap-fill grounds anything deeper — so
/// a modest page suffices; it is not the whole-ancestry budget.
const RECONCILE_LIMIT: u32 = 50;

/// A room's advertised forward extremities — the anti-entropy wire shape carried
/// on `/send` requests and responses. Both head-sets are advertised: the timeline
/// DAG heads and the state DAG heads (MSC4242), so a peer can tell message-DAG
/// divergence from state-DAG divergence and walk the right edges to reconcile.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct ForwardExtremities {
    #[serde(default)]
    pub timeline: Vec<OwnedEventId>,
    #[serde(default)]
    pub state: Vec<OwnedEventId>,
}

impl ForwardExtremities {
    pub(crate) fn is_empty(&self) -> bool {
        self.timeline.is_empty() && self.state.is_empty()
    }
}

/// The first few event ids as a loggable list — a debugging aid on the
/// anti-entropy log lines. The set is usually ≤ 3 (a room's forward extremities),
/// so this is bounded; the cap guards a pathologically forked DAG.
fn sample_ids(ids: &[OwnedEventId]) -> Vec<&str> {
    ids.iter().take(3).map(|e| e.as_str()).collect()
}

/// This server's current forward extremities for `room_id`, in the wire shape,
/// for advertising to a peer. Empty if the room is unknown or the lookup faults
/// (best-effort: a missing advertisement just means no reconciliation this round).
pub(crate) async fn local_extremities(store: &SqliteStore, room_id: &RoomId) -> ForwardExtremities {
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
pub(crate) async fn reconcile_room<F: MissingEventsFetcher + ?Sized>(
    store: &SqliteStore,
    fetcher: &F,
    worker_poke: &mpsc::Sender<OwnedRoomId>,
    peer: &ServerName,
    room_id: &RoomId,
    advertised: &ForwardExtremities,
) {
    if advertised.is_empty() {
        return;
    }
    // Only reconcile rooms we actually host — `forward_extremities` is `None` for
    // an unknown room, so this also scopes us to rooms we share with the peer.
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
    let mut staged = fetch_unknown(
        store,
        fetcher,
        peer,
        room_id,
        &advertised.state,
        &our_state,
        &staged_ids,
        true,
    )
    .await;
    staged.extend(
        fetch_unknown(
            store,
            fetcher,
            peer,
            room_id,
            &advertised.timeline,
            &our_timeline,
            &staged_ids,
            false,
        )
        .await,
    );

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

/// Fetch the advertised `heads` we don't already hold — with their ancestry down
/// to `earliest` — in a single `get_missing_events` (`include_latest_events` so
/// the heads themselves come back, not just their ancestors), and stage them.
/// Returns the ids of the events it newly staged.
#[allow(clippy::too_many_arguments)]
async fn fetch_unknown<F: MissingEventsFetcher + ?Sized>(
    store: &SqliteStore,
    fetcher: &F,
    peer: &ServerName,
    room_id: &RoomId,
    heads: &[OwnedEventId],
    earliest: &[OwnedEventId],
    staged_ids: &BTreeSet<OwnedEventId>,
    state_dag: bool,
) -> Vec<OwnedEventId> {
    if heads.is_empty() {
        return Vec::new();
    }
    // `get_events` returns only the events we hold, so it doubles as an existence
    // filter: an advertised head it omits is one we are missing. A head already
    // staged (`staged_ids`) is in the pipeline too, so it isn't "unknown".
    let head_refs: Vec<&EventId> = heads.iter().map(|e| e.as_ref()).collect();
    let committed: BTreeSet<OwnedEventId> = match store.get_events(&head_refs).await {
        Ok(events) => events.into_iter().map(|e| e.event_id).collect(),
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: looking up advertised heads failed");
            return Vec::new();
        }
    };
    let unknown: Vec<OwnedEventId> = heads
        .iter()
        .filter(|h| !committed.contains(*h) && !staged_ids.contains(*h))
        .cloned()
        .collect();
    if unknown.is_empty() {
        return Vec::new();
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
            limit: RECONCILE_LIMIT,
            state_dag,
            include_latest_events: true,
        })
        .await
    {
        Ok(fetched) => fetched,
        Err(e) => {
            warn!(%peer, %room_id, error = %e, "reconcile: fetching advertised heads failed");
            return Vec::new();
        }
    };

    let mut staged_new: Vec<OwnedEventId> = Vec::new();
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
            Ok(true) => staged_new.push(ev.event_id),
            Ok(false) => {}
            Err(e) => {
                warn!(%peer, %room_id, error = %e, "reconcile: staging a fetched event failed")
            }
        }
    }
    staged_new
}
