//! The inbound staging worker: drains pre-auth staged PDUs into the per-room
//! actor, off the HTTP request path.
//!
//! The inbound `/send` handler (in `neutrino-http`) durably *stages*
//! each received PDU and returns 200 immediately. This worker does the actual
//! integration asynchronously, so a slow auth / gap-fill round-trip can't block
//! the response and a PDU is never lost (presence in `staged_events` = pending;
//! it is unstaged only once durably applied).
//!
//! ## Shape (mirrors the outbound [`sender`](crate::sender) pool)
//!
//! - **One task per room.** A room's PDUs are integrated serially through the
//!   per-room actor, so there is never overlapping work (or duplicate gap-fill
//!   fetches) for one room. The task is long-lived: it drains, then parks on a
//!   per-room [`Notify`] until poked again (it never exits on its own — the
//!   set of rooms is bounded by the rooms we are in).
//! - **A supervisor task** owns discovery. On startup it enumerates
//!   [`neutrino_store::StagingStore::staged_rooms`] (restart recovery — staged rows survive a
//!   crash) and spawns a task per room. Then it serves an in-process **poke**
//!   channel: the `/send` handler sends the room id of each freshly-staged PDU,
//!   and the supervisor either spawns that room's task or wakes the running one.
//!   When the poke channel closes (the owning `AppState` was dropped) the
//!   supervisor aborts every room task and stops.
//!
//! The poke is in-process (not the storage watch): staged rows carry no stream
//! position, and reusing the watch would spuriously wake sliding-sync. Restart
//! is covered by the startup enumeration.
//!
//! ## Drain pass
//!
//! Read the room's staged rows, toposort by `prev_events ∪ prev_state_events`,
//! and apply each oldest-first through [`RoomRegistry::apply_pdu`]:
//!
//! - **Ok** (accepted / soft-failed / rejected — federation persists rejects):
//!   unstage it.
//! - **Retryable** (missing `prev_state_events` ancestry): fetch the gap into
//!   staging (`fill_state_ancestry`) — those fetched rows are the *same kind*
//!   of staged row, so the next drain pass toposorts and applies them ahead of
//!   this PDU. There is no separate "promote" step. On an unfillable gap / peer
//!   failure, back the PDU off.
//! - **Non-retryable** (malformed / misrouted — can never apply): drop it
//!   (unstage + log).
//! - **UnknownRoom / storage fault**: back off and retry.
//!
//! ## Backoff
//!
//! In-memory, per staged event (a wedged PDU is *skipped* during its backoff
//! window, never dequeued, so it doesn't block eligible siblings or fresh
//! arrivals). Full-jitter exponential, shared with the outbound sender
//! (`BACKOFF_BASE` → `BACKOFF_CAP`).
//! It never permanently gives up (never-lose); a restart clears the map and
//! re-drains (the "kick it by restarting" path).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use neutrino_event::Event;
use neutrino_event::event_builder::from_wire;
use neutrino_store::{StagedPdu, StorageBackend, WithStateProvider};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, RoomId};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::gapfill::fill_state_ancestry;
use crate::ports::MissingEventsFetcher;
use crate::room_actor::{RoomActorError, RoomRegistry};
use crate::util::{BACKOFF_BASE, jitter, next_backoff};

/// Buffer for the in-process poke channel. A poke is just a room id; the
/// supervisor coalesces duplicates (re-reading the room's staged rows each
/// time), so a full buffer only means a poke is briefly delayed, never lost
/// (the room is already known to have work). Generous so a burst of inbound
/// transactions never blocks the handler on `try_send`.
const POKE_BUFFER: usize = 256;

/// Shared, cheaply-cloned handles a worker task needs. Each field is an `Arc`
/// (or a `Copy` `Duration`), so cloning into a spawned task is near-free.
struct WorkerCtx<S> {
    store: Arc<S>,
    registry: Arc<RoomRegistry<S>>,
    fetcher: Arc<dyn MissingEventsFetcher>,
    /// Backoff floor; [`BACKOFF_BASE`] in production, near-zero in tests so the
    /// retry path runs without real delays.
    backoff_base: Duration,
}

// Hand-written so the bound is `S` (not `S: Clone`) — `Arc`s + a `Copy`
// `Duration` clone regardless of `S`. `#[derive(Clone)]` would demand `S: Clone`.
impl<S> Clone for WorkerCtx<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            registry: self.registry.clone(),
            fetcher: self.fetcher.clone(),
            backoff_base: self.backoff_base,
        }
    }
}

/// A running room task plus the handle used to wake it (a fresh poke) or stop
/// it (supervisor shutdown).
struct RoomTask {
    notify: Arc<Notify>,
    handle: JoinHandle<()>,
}

/// Spawn the inbound staging worker. Returns the poke [`mpsc::Sender`] the
/// `/send` handler uses to signal freshly-staged work; dropping every clone of
/// it (i.e. dropping the owning `AppState`) shuts the worker down.
///
/// Must be called from within a Tokio runtime (it spawns the supervisor task) —
/// every caller is on an async path (`serve`, the test routers).
pub fn spawn<S: StorageBackend + WithStateProvider + 'static>(
    store: Arc<S>,
    registry: Arc<RoomRegistry<S>>,
    fetcher: Arc<dyn MissingEventsFetcher>,
) -> mpsc::Sender<OwnedRoomId> {
    spawn_with(store, registry, fetcher, BACKOFF_BASE)
}

/// Inner spawn with the backoff floor made explicit, so tests can drive the
/// full supervisor → task path without real backoff delays.
fn spawn_with<S: StorageBackend + WithStateProvider + 'static>(
    store: Arc<S>,
    registry: Arc<RoomRegistry<S>>,
    fetcher: Arc<dyn MissingEventsFetcher>,
    backoff_base: Duration,
) -> mpsc::Sender<OwnedRoomId> {
    let (tx, rx) = mpsc::channel(POKE_BUFFER);
    let ctx = WorkerCtx {
        store,
        registry,
        fetcher,
        backoff_base,
    };
    tokio::spawn(supervise(ctx, rx));
    tx
}

/// Discovery loop: ensure every room with staged PDUs has a running drain task.
/// Enumerates the staging backlog once on startup (restart recovery), then runs
/// off in-process pokes until the channel closes (shutdown).
async fn supervise<S: StorageBackend + WithStateProvider + 'static>(
    ctx: WorkerCtx<S>,
    mut poke_rx: mpsc::Receiver<OwnedRoomId>,
) {
    let mut rooms: HashMap<OwnedRoomId, RoomTask> = HashMap::new();

    // Restart recovery: drain whatever was left staged by a previous run.
    match ctx.store.staged_rooms().await {
        Ok(list) => {
            for room in list {
                ensure_running(&mut rooms, &ctx, room);
            }
        }
        Err(e) => error!(error = %e, "enumerating staged rooms on startup"),
    }

    // Serve pokes. `recv` yields `None` when every sender is dropped (the
    // `AppState` holding the poke sender is gone) — our shutdown signal.
    while let Some(room) = poke_rx.recv().await {
        ensure_running(&mut rooms, &ctx, room);
    }

    info!("inbound staging worker poke channel closed; stopping");
    for (_, task) in rooms.drain() {
        task.handle.abort();
    }
}

/// Spawn a drain task for `room` if none is running, else wake the running one.
fn ensure_running<S: StorageBackend + WithStateProvider + 'static>(
    rooms: &mut HashMap<OwnedRoomId, RoomTask>,
    ctx: &WorkerCtx<S>,
    room: OwnedRoomId,
) {
    if let Some(task) = rooms.get(&room) {
        // Already draining → wake it so it re-reads the (now larger) backlog.
        // A stored permit covers the race where the task is between "drained
        // empty" and `notified().await`.
        task.notify.notify_one();
        return;
    }
    let notify = Arc::new(Notify::new());
    debug!(%room, "spawning inbound drain task");
    let handle = tokio::spawn(run_room(ctx.clone(), room.clone(), notify.clone()));
    rooms.insert(room, RoomTask { notify, handle });
}

/// In-memory backoff for one staged event: the earliest [`Instant`] it is
/// eligible to be retried, and the current ceiling (doubles each failure).
struct Backoff {
    next: Instant,
    ceiling: Duration,
}

/// Drain one room forever: apply eligible staged PDUs, then block until poked
/// (or until a backing-off PDU becomes eligible). Owns the room's backoff map.
async fn run_room<S: StorageBackend + WithStateProvider + 'static>(
    ctx: WorkerCtx<S>,
    room: OwnedRoomId,
    notify: Arc<Notify>,
) {
    let mut backoff: HashMap<OwnedEventId, Backoff> = HashMap::new();
    loop {
        let rows = match ctx.store.staged_for_room(&room).await {
            Ok(r) => r,
            Err(e) => {
                // A storage read fault is transient; pause briefly then retry.
                error!(%room, error = %e, "reading staged rows");
                sleep_or_poke(ctx.backoff_base, &notify).await;
                continue;
            }
        };

        if rows.is_empty() {
            // Fully drained: forget any backoff state and park until poked.
            backoff.clear();
            notify.notified().await;
            continue;
        }

        // Drop backoff entries for events that are no longer staged (applied or
        // dropped), so the map stays bounded by the live backlog.
        let present: HashSet<&OwnedEventId> = rows.iter().map(|p| &p.event_id).collect();
        backoff.retain(|id, _| present.contains(id));

        // Eligible = not currently in a backoff window. A backing-off PDU is
        // skipped (left staged), never dequeued, so it can't block its siblings.
        let now = Instant::now();
        let eligible: Vec<StagedPdu> = rows
            .into_iter()
            .filter(|p| backoff.get(&p.event_id).is_none_or(|b| b.next <= now))
            .collect();

        if eligible.is_empty() {
            // Everything left is backing off: sleep until the soonest becomes
            // eligible (or a poke brings fresh work).
            let soonest = backoff.values().map(|b| b.next).min();
            let wait = soonest
                .map(|t| t.saturating_duration_since(Instant::now()))
                .unwrap_or(ctx.backoff_base);
            sleep_or_poke(wait, &notify).await;
            continue;
        }

        // Apply parents before children that are staged together. Rows whose
        // bytes no longer parse are junk (they passed `from_wire` when staged,
        // so this is defensive) — unstage them rather than spin forever.
        for staged in toposort(parse_or_drop(&ctx, &eligible).await) {
            process_one(&ctx, &room, staged, &mut backoff).await;
        }
    }
}

/// Parse each eligible staged row to a [`Staged`]; a row whose bytes no longer
/// round-trip through `from_wire` is unstaged and skipped.
async fn parse_or_drop<S: StorageBackend + WithStateProvider + 'static>(
    ctx: &WorkerCtx<S>,
    eligible: &[StagedPdu],
) -> Vec<Staged> {
    let mut out = Vec::with_capacity(eligible.len());
    for p in eligible {
        match from_wire(p.raw.clone(), Vec::new()) {
            // Both variants proceed: a `Wire::Rejected` event carries
            // `rejected = true` and `apply_pdu` short-circuits it to a
            // rejected persist (the cascade terminator).
            Ok(wire) => {
                if let neutrino_event::Wire::Rejected(ev, defect) = &wire {
                    tracing::warn!(event_id = %ev.event_id, %defect, "staging malformed PDU for rejected persist");
                }
                out.push(Staged {
                    event: wire.into_event(),
                    origin: p.origin.clone(),
                });
            }
            Err(e) => {
                warn!(event_id = %p.event_id, error = %e, "dropping unparseable staged PDU");
                unstage(ctx, &p.event_id).await;
            }
        }
    }
    out
}

/// One staged event paired with the server it arrived from (the gap-fill fetch
/// target). The `Event` carries the DAG pointers `toposort` orders by.
struct Staged {
    event: Event,
    origin: OwnedServerName,
}

/// Integrate one staged PDU through the actor, updating `backoff` per the
/// drain-pass disposition (see the module docs).
async fn process_one<S: StorageBackend + WithStateProvider + 'static>(
    ctx: &WorkerCtx<S>,
    room: &RoomId,
    staged: Staged,
    backoff: &mut HashMap<OwnedEventId, Backoff>,
) {
    let id = staged.event.event_id.clone();
    match ctx.registry.apply_pdu(room, staged.event.clone()).await {
        // Terminal: accepted, soft-failed, or rejected (federation persists
        // rejects). Either way it's integrated — drop it from staging.
        Ok(()) => {
            unstage(ctx, &id).await;
            backoff.remove(&id);
        }
        // Retryable verdict. `fill_state_ancestry` returns `Ok(true)` if it
        // staged a real missing-ancestry gap — clear the backoff so the next
        // pass applies the now-staged ancestry ahead of this PDU. `Ok(false)`
        // means the ancestry was already grounded and nothing was fetched, i.e.
        // the retryable verdict was a transient state-res / storage fault, not a
        // gap — back off rather than spin re-applying. `Err` (unfillable / peer
        // failure) also backs off.
        Err(RoomActorError::Apply(e)) if e.is_retryable() => {
            match fill_state_ancestry(&*ctx.store, &staged.origin, &staged.event, &*ctx.fetcher)
                .await
            {
                Ok(true) => {
                    backoff.remove(&id);
                }
                Ok(false) => {
                    debug!(%id, "retryable apply with no gap to fill; backing off");
                    bump_backoff(backoff, id, ctx.backoff_base);
                }
                Err(reason) => {
                    warn!(%id, reason, "gap-fill failed; backing off");
                    bump_backoff(backoff, id, ctx.backoff_base);
                }
            }
        }
        // Non-retryable verdict: the event is malformed or misrouted and can
        // never apply. Drop it so it neither blocks the queue nor retries.
        Err(RoomActorError::Apply(e)) => {
            warn!(%id, error = %e, "dropping un-appliable staged PDU");
            unstage(ctx, &id).await;
            backoff.remove(&id);
        }
        // We have no record of this room. A federation `/send` only arrives for
        // rooms we've joined (the join handshake establishes room state before
        // live events flow), so an unknown room is anomalous, not a pre-create
        // race. Drop the PDU rather than retry forever — otherwise an
        // unauthenticated peer could accumulate un-drainable staged rows + a
        // permanent per-room task by naming nonexistent rooms.
        Err(RoomActorError::UnknownRoom) => {
            warn!(%id, %room, "dropping staged PDU for unknown room");
            unstage(ctx, &id).await;
            backoff.remove(&id);
        }
        // Storage / actor faults: transient, back off and retry. (`Rejected`
        // and `Build` never arise on the `apply_pdu` path.)
        Err(other) => {
            warn!(%id, error = %other, "applying staged PDU failed; backing off");
            bump_backoff(backoff, id, ctx.backoff_base);
        }
    }
}

/// Delete `id` from staging, logging (not propagating) a removal fault — a
/// surviving row is harmless (re-applied idempotently next pass).
async fn unstage<S: StorageBackend + WithStateProvider + 'static>(
    ctx: &WorkerCtx<S>,
    id: &OwnedEventId,
) {
    if let Err(e) = ctx.store.unstage_events(&[id.as_ref()]).await {
        error!(%id, error = %e, "unstaging processed PDU");
    }
}

/// Push `id`'s next-eligible time out by a full-jittered backoff and double its
/// ceiling. A first failure seeds the entry at `base`.
fn bump_backoff(backoff: &mut HashMap<OwnedEventId, Backoff>, id: OwnedEventId, base: Duration) {
    let entry = backoff.entry(id).or_insert(Backoff {
        next: Instant::now(),
        ceiling: base,
    });
    let wait = jitter(entry.ceiling);
    entry.next = Instant::now() + wait;
    entry.ceiling = next_backoff(entry.ceiling);
}

/// Wait for `dur`, or wake early if poked. Used both for the transient-fault
/// pause and the all-backing-off sleep.
async fn sleep_or_poke(dur: Duration, notify: &Notify) {
    tokio::select! {
        _ = tokio::time::sleep(dur) => {}
        _ = notify.notified() => {}
    }
}

/// Topologically sort a room's staged batch so a PDU is applied after any of
/// its parents staged alongside it. Edges are intra-batch `prev_events` ∪
/// `prev_state_events`; events whose parents are outside the batch (committed,
/// or still missing) sort first. Kahn's algorithm; any residual cycle
/// (impossible in a valid DAG) appends the remainder in arrival order.
fn toposort(items: Vec<Staged>) -> Vec<Staged> {
    let ids: HashSet<OwnedEventId> = items.iter().map(|s| s.event.event_id.clone()).collect();

    let mut indegree: Vec<usize> = Vec::with_capacity(items.len());
    let mut children: HashMap<OwnedEventId, Vec<usize>> = HashMap::new();
    for (idx, s) in items.iter().enumerate() {
        let parents: HashSet<&OwnedEventId> = s
            .event
            .prev_events
            .iter()
            .chain(s.event.prev_state_events.iter())
            .filter(|p| ids.contains(*p))
            .collect();
        indegree.push(parents.len());
        for p in parents {
            children.entry(p.clone()).or_default().push(idx);
        }
    }

    let mut ready: Vec<usize> = (0..items.len()).filter(|&i| indegree[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(items.len());
    let mut emitted = vec![false; items.len()];
    while let Some(idx) = ready.pop() {
        order.push(idx);
        emitted[idx] = true;
        if let Some(kids) = children.get(&items[idx].event.event_id) {
            for &k in kids {
                indegree[k] -= 1;
                if indegree[k] == 0 {
                    ready.push(k);
                }
            }
        }
    }
    for (i, done) in emitted.iter().enumerate() {
        if !done {
            order.push(i);
        }
    }

    let mut slots: Vec<Option<Staged>> = items.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| slots[i].take()).collect()
}

#[cfg(test)]
mod tests {
    use neutrino_event::ROOM_VERSION_ID;
    use neutrino_event::event_builder::EventBuilder;
    use ruma::{OwnedUserId, server_name};
    use serde_json::json;

    use super::*;

    fn staged(event: Event) -> Staged {
        Staged {
            event,
            origin: server_name!("example.org").to_owned(),
        }
    }

    /// A linear chain create → a → b → c (each `prev_*` points at the previous),
    /// returned oldest-first; `create` is the grounded root, not part of a batch.
    fn chain() -> (Event, Event, Event) {
        let alice: OwnedUserId = "@alice:example.org".parse().unwrap();
        let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .build()
            .unwrap();
        let room_id = create.room_id.clone();
        let on = |prev: &Event, body: &str| {
            EventBuilder::new(alice.clone(), "m.room.message".to_owned())
                .room_id(room_id.clone())
                .content(json!({ "msgtype": "m.text", "body": body }))
                .prev_events(vec![prev.event_id.clone()])
                .prev_state_events(vec![prev.event_id.clone()])
                .build()
                .unwrap()
        };
        let a = on(&create, "a");
        let b = on(&a, "b");
        let c = on(&b, "c");
        (a, b, c)
    }

    #[test]
    fn toposort_orders_parents_before_children() {
        let (a, b, c) = chain();
        let (a_id, b_id, c_id) = (a.event_id.clone(), b.event_id.clone(), c.event_id.clone());

        // Feed in a shuffled, child-before-parent order.
        let ordered = toposort(vec![staged(c), staged(a), staged(b)]);
        let ids: Vec<OwnedEventId> = ordered.into_iter().map(|s| s.event.event_id).collect();
        let pos = |id: &OwnedEventId| ids.iter().position(|x| x == id).unwrap();

        assert_eq!(ids.len(), 3);
        assert!(pos(&a_id) < pos(&b_id), "a must precede b: {ids:?}");
        assert!(pos(&b_id) < pos(&c_id), "b must precede c: {ids:?}");
    }
}
