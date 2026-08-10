//! Outbound federation delivery: the per-destination sender pool.
//!
//! Drains the durable `outbox` (populated atomically with each accepted event
//! — see `EventStore::persist_resolved_event`) and PUTs transactions to peers
//! via `neutrino-http`'s `FederationClient`. This is where the "events MUST eventually be sent
//! / never lost / retry on restart" clause of the Server-Server `/send` task
//! lives.
//!
//! ## Shape
//!
//! - **One task per destination.** It drains that destination's pending PDUs in
//!   causal (insertion) order, chunking to [`MAX_PDUS_PER_TXN`] per transaction,
//!   and removes a batch from the outbox only after the peer returns 2xx. When
//!   the destination drains empty it blocks on a [`watch::Receiver<StreamPos>`]
//!   clone (advanced by every persist) and re-drains on the next wake-up.
//! - **A supervisor task** owns the discovery side: on startup and on every
//!   watch advance it enumerates [`neutrino_store::FederationOutbox::pending_destinations`] and
//!   spawns a task for any destination not already running. Idle destination
//!   tasks stay alive (bounded by the size of the mesh) rather than being
//!   reaped and respawned.
//!
//! ## Flood control on startup
//!
//! On a restart with a large backlog, every destination would otherwise fire at
//! once. Two bounds prevent that thundering herd:
//!
//! - **Startup jitter.** Destinations present in the *first* enumeration round
//!   wait a random `[0, config.startup_jitter]` before their first drain. Spreads
//!   the restart burst. Destinations discovered *later* (a live send to a new
//!   peer) drain immediately — no added latency on the common path.
//! - **A global send semaphore.** At most `NEUTRINO_OUTBOUND_CONCURRENCY` (default
//!   `DEFAULT_OUTBOUND_CONCURRENCY`, min 1) transactions are in flight across
//!   all destinations at once. A backing-off destination holds no permit, so it
//!   never starves a healthy one.
//!
//! ## Failure handling
//!
//! - **4xx** from a peer = the transaction was malformed/rejected at the
//!   envelope level (the inbound handler only 4xxs the whole transaction, never
//!   an individual PDU). Retrying won't help, so the batch is dropped from the
//!   outbox and we move on.
//! - **5xx / transport / unreachable** = transient. Retry the same batch after
//!   an exponential backoff (full jitter, doubling `BACKOFF_BASE` → capped at
//!   `BACKOFF_CAP`); never remove until a 2xx. Backoff state is in-memory
//!   only, so a restart re-enumerates and retries within the startup jitter
//!   window — restarting the app is a way to "kick" a stuck destination.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use neutrino_store::{StorageBackend, StreamPos};
#[cfg(test)]
use neutrino_store_sqlite::SqliteStore;
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use neutrino_event::{EventPolicy, now_ms};

use crate::ports::{FederationTransport, ForwardExtremities, MissingEventsFetcher, TransportError};
use crate::reconcile;
use crate::util::{BACKOFF_BASE, MAX_PDUS_PER_TXN, TxnIdGen, jitter, next_backoff};

/// Shared, cheaply-cloneable handles every sender task needs. Bundled so the
/// per-destination signatures stay readable as the pool grows (mirrors
/// `worker::WorkerCtx`).
struct SenderCtx<S> {
    store: Arc<S>,
    transport: Arc<dyn FederationTransport>,
    idgen: Arc<TxnIdGen>,
    send_slots: Arc<Semaphore>,
    /// Anti-entropy: peer fetcher for reconciling against the forward extremities
    /// a peer advertises on a transaction *response*. Shared with the inbound
    /// worker/handler (see `AppState`).
    fetcher: Arc<dyn MissingEventsFetcher>,
    /// Deployment-wide event policy from the medium's declared link trust.
    policy: EventPolicy,
    /// Poke the inbound worker after reconciliation stages fetched events.
    worker_poke: mpsc::Sender<OwnedRoomId>,
}

// Hand-written so the bound is `S` (not `S: Clone`) — every field is an `Arc`/
// channel that clones regardless of `S`. `#[derive(Clone)]` would spuriously
// demand `S: Clone`.
impl<S> Clone for SenderCtx<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            transport: self.transport.clone(),
            idgen: self.idgen.clone(),
            send_slots: self.send_slots.clone(),
            fetcher: self.fetcher.clone(),
            policy: self.policy.clone(),
            worker_poke: self.worker_poke.clone(),
        }
    }
}

/// Spawn the outbound sender pool. Returns the supervisor's `JoinHandle` so
/// the caller can await it after the shutdown token fires.
///
/// Subscribes to the persist watch *before* the first enumeration so a
/// destination added concurrently with startup can't be missed (the watch will
/// have advanced, waking the supervisor's first `changed()`). Tests pass a zero
/// `startup_jitter_max` to run the full supervisor → task path without the wait.
#[allow(clippy::too_many_arguments)]
pub fn spawn<S: StorageBackend + 'static>(
    store: Arc<S>,
    transport: Arc<dyn FederationTransport>,
    concurrency: usize,
    startup_jitter_max: Duration,
    shutdown: CancellationToken,
    kick_rx: watch::Receiver<()>,
    fetcher: Arc<dyn MissingEventsFetcher>,
    policy: EventPolicy,
    worker_poke: mpsc::Sender<OwnedRoomId>,
) -> JoinHandle<()> {
    let watch_rx = store.subscribe();
    // Process-startup prefix keeps txn ids unique across restarts; the
    // in-memory counter keeps them unique within a run.
    let idgen = Arc::new(TxnIdGen::new(now_ms()));
    // `Semaphore::new(0)` would block every send forever; clamp here so the
    // "≥ 1" invariant holds regardless of how `Config` was constructed (the
    // `from_env` path already clamps, but the field is `pub`).
    let send_slots = Arc::new(Semaphore::new(concurrency.max(1)));
    let ctx = SenderCtx {
        store,
        transport,
        idgen,
        send_slots,
        fetcher,
        policy,
        worker_poke,
    };
    tokio::spawn(supervise(
        ctx,
        watch_rx,
        startup_jitter_max,
        shutdown,
        kick_rx,
    ))
}

/// Discovery loop: ensure every destination with pending PDUs has a running
/// sender task. Runs until the shutdown token is cancelled or the store's watch
/// sender is dropped. On exit, aborts all per-destination child tasks.
async fn supervise<S: StorageBackend + 'static>(
    ctx: SenderCtx<S>,
    mut watch_rx: watch::Receiver<StreamPos>,
    startup_jitter_max: Duration,
    shutdown: CancellationToken,
    kick_rx: watch::Receiver<()>,
) {
    let mut running: HashMap<OwnedServerName, JoinHandle<()>> = HashMap::new();
    let mut first_round = true;
    loop {
        // Union the outbox destinations with destinations owed only an
        // anti-entropy advertisement, so a quiescent peer with no pending PDUs
        // but a standing advertisement obligation still gets a sender task.
        match destinations_needing_a_task(&ctx).await {
            Ok(dests) => {
                for dest in dests {
                    running.entry(dest.clone()).or_insert_with(|| {
                        // Only the startup backlog gets jittered; a destination
                        // discovered mid-run is a single live send, not a flood.
                        let initial_delay = if first_round {
                            jitter(startup_jitter_max)
                        } else {
                            Duration::ZERO
                        };
                        debug!(%dest, ?initial_delay, "spawning outbound sender task");
                        tokio::spawn(run_destination(
                            ctx.clone(),
                            dest,
                            watch_rx.clone(),
                            kick_rx.clone(),
                            initial_delay,
                        ))
                    });
                }
            }
            Err(e) => error!(error = %e, "enumerating outbound destinations"),
        }
        first_round = false;
        // Wait for the next persist (which may have added a new destination) or
        // for the shutdown signal. `Err` on `changed()` means the watch sender
        // was dropped — the store is gone, so the pool shuts down with it.
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,                      // shutdown requested
            res = watch_rx.changed() => if res.is_err() { break },  // store gone
        }
    }
    info!(
        "outbound supervisor stopping; aborting {} child task(s)",
        running.len()
    );
    for (_, handle) in running.drain() {
        handle.abort();
    }
}

/// Every destination that needs a running sender task: the union of those with
/// pending outbox PDUs and those owed an anti-entropy advertisement. A peer in
/// only the second set has fallen quiet with a standing obligation — the task
/// it gets here drains that obligation (an empty-`pdus` advertisement) and then
/// idles like any other.
async fn destinations_needing_a_task<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
) -> Result<BTreeSet<OwnedServerName>, neutrino_store::StorageError> {
    let mut dests: BTreeSet<OwnedServerName> = ctx
        .store
        .pending_destinations()
        .await?
        .into_iter()
        .collect();
    dests.extend(ctx.store.advertisement_destinations().await?);
    Ok(dests)
}

/// Drain a single destination forever: deliver pending PDUs, then block until
/// the next persist wakes us. Owns its own backoff state.
async fn run_destination<S: StorageBackend + 'static>(
    ctx: SenderCtx<S>,
    dest: OwnedServerName,
    mut watch_rx: watch::Receiver<StreamPos>,
    mut kick_rx: watch::Receiver<()>,
    initial_delay: Duration,
) {
    // Baseline the kick signal: only a `KickBackoff` sent *after* this task
    // starts should shortcut a backoff. (A just-spawned destination is at base
    // and about to drain anyway, so a kick racing spawn is harmless to miss.)
    kick_rx.borrow_and_update();

    if !initial_delay.is_zero() {
        tokio::time::sleep(initial_delay).await;
    }

    let mut backoff = BACKOFF_BASE;
    loop {
        // One transaction's worth, in causal order. `pending_pdus` caps the
        // load at `MAX_PDUS_PER_TXN`, so a huge backlog never lands in memory
        // at once — we drain it a batch at a time across loop iterations.
        let batch = match ctx.store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await {
            Ok(b) => b,
            Err(e) => {
                // A storage read fault is transient; back off and retry.
                error!(%dest, error = %e, "reading pending PDUs");
                if sleep_backoff(&mut backoff, &mut kick_rx).await {
                    backoff = BACKOFF_BASE;
                }
                continue;
            }
        };

        if !batch.is_empty() {
            if deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await {
                backoff = BACKOFF_BASE;
                continue;
            }
            // Shutdown (semaphore closed). Stop the task.
            return;
        }

        // No PDUs pending. Drain any anti-entropy advertisement obligations
        // before idling — a quiescent peer owed an advertisement reaches us
        // only here (a normal `/send` would have cleared it already). Read
        // faults are transient: back off and retry, same as the PDU read.
        let adv_rooms = match ctx.store.pending_advertisements(&dest).await {
            Ok(r) => r,
            Err(e) => {
                error!(%dest, error = %e, "reading pending advertisements");
                if sleep_backoff(&mut backoff, &mut kick_rx).await {
                    backoff = BACKOFF_BASE;
                }
                continue;
            }
        };
        if !adv_rooms.is_empty() {
            if send_advertisement(&ctx, &dest, &adv_rooms, &mut backoff, &mut kick_rx).await {
                backoff = BACKOFF_BASE;
                continue;
            }
            return;
        }

        // Fully drained (no PDUs, no advertisements): reset backoff and idle
        // until the next persist wakes us.
        backoff = BACKOFF_BASE;
        if watch_rx.changed().await.is_err() {
            return;
        }
    }
}

/// The terminal result of attempting one transaction, after transient (5xx /
/// transport) failures have been retried in place under one reused `txn_id`.
enum SendOutcome {
    /// Peer returned 2xx; carries the forward extremities it advertised back.
    Delivered(BTreeMap<OwnedRoomId, ForwardExtremities>),
    /// Peer returned 4xx — the transaction envelope was rejected; retrying it
    /// can't help, so the caller drops the work that produced it.
    Rejected,
    /// The global send semaphore was closed (shutdown); the caller must stop.
    Shutdown,
}

/// Send one transaction (`pdus` + advertised `our_fes`) to `dest` under a fixed
/// `txn_id`, retrying transient failures in place with backoff until the peer
/// returns a terminal status. The shared core of [`deliver_batch`] and
/// [`send_advertisement`] — an advertisement is just this with an empty `pdus`
/// array. The caller owns `txn_id` (so a post-success durable-write fault can
/// re-call this under the *same* id, which the peer dedups on `(origin, txnId)`)
/// and decides what `Delivered` / `Rejected` mean for its durable state.
async fn send_transaction_with_retry<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
    dest: &ServerName,
    txn_id: &str,
    pdus: &[Box<RawJsonValue>],
    our_fes: &BTreeMap<OwnedRoomId, ForwardExtremities>,
    backoff: &mut Duration,
    kick_rx: &mut watch::Receiver<()>,
) -> SendOutcome {
    let mut attempt = 0u32;
    loop {
        // One INFO line per attempt under the `neutrino_http` target (same as the
        // inbound request log). Retries reuse `txn_id`, so a peer being retried
        // (e.g. while partitioned) shows as repeating lines with a climbing
        // `attempt`; `pdus = 0` marks an anti-entropy advertisement. The `/send`
        // path is spelled out so it surfaces when filtering on `_matrix/federation/`.
        attempt += 1;
        info!(
            target: "neutrino_http",
            %dest,
            txn = %txn_id,
            pdus = pdus.len(),
            rooms = our_fes.len(),
            attempt,
            "outbound PUT /_matrix/federation/v1/send",
        );
        // Hold a global permit only around the network call — released before
        // any backoff sleep, so a slow peer can't pin a concurrency slot.
        let send_result = match ctx.send_slots.acquire().await {
            Ok(_permit) => {
                ctx.transport
                    .send_transaction(dest, txn_id, pdus, our_fes)
                    .await
            }
            // The semaphore is never closed in normal operation; an error here
            // means shutdown.
            Err(_) => return SendOutcome::Shutdown,
        };

        match send_result {
            Ok(peer_fes) => return SendOutcome::Delivered(peer_fes),
            // 4xx: the peer rejected the envelope. Retrying is futile.
            Err(TransportError::Status(code)) if (400..500).contains(&code) => {
                warn!(%dest, code, pdus = pdus.len(), "peer rejected transaction (4xx)");
                return SendOutcome::Rejected;
            }
            // 5xx / transport / URL: transient. Back off and retry under the same
            // txn_id.
            Err(e) => {
                warn!(%dest, error = %e, backoff = ?backoff, "transaction delivery failed; will retry");
                if sleep_backoff(backoff, kick_rx).await {
                    *backoff = BACKOFF_BASE;
                }
            }
        }
    }
}

/// The event ids a transaction carrying `batch` tells its peer about by itself:
/// each event in it, plus the `prev_state_events` those events force the peer to
/// gap-fill in order to auth them. Fed to [`reconcile::strip_known`] so heads the
/// peer will hold anyway are left off the wire; see that function for why
/// `prev_events` is excluded.
fn covered_by(batch: &[neutrino_event::Event]) -> BTreeSet<OwnedEventId> {
    batch
        .iter()
        .flat_map(|e| {
            std::iter::once(e.event_id.clone()).chain(e.prev_state_events.iter().cloned())
        })
        .collect()
}

/// Deliver one outbox batch to `dest`: a transaction carrying the batch's PDUs
/// and our forward extremities. On a 2xx the batch is removed from the outbox
/// and — because the transaction carried our heads — any standing advertisement
/// obligation for the rooms it covered is cleared (the piggyback IS the
/// advertisement). A 4xx drops the batch but leaves the obligation (our heads
/// never landed, so the duty stands). A post-2xx `remove_pdus` fault re-sends
/// under the same txn id (the peer dedups). Returns `false` only on shutdown.
async fn deliver_batch<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
    dest: &ServerName,
    batch: &[neutrino_event::Event],
    backoff: &mut Duration,
    kick_rx: &mut watch::Receiver<()>,
) -> bool {
    let pdus: Vec<Box<RawJsonValue>> = batch.iter().map(|e| e.raw.clone()).collect();
    let ids: Vec<&EventId> = batch.iter().map(|e| &*e.event_id).collect();

    // Our forward extremities for every room in the batch, so the peer can
    // reconcile against us. Computed once (reused across retries — it's a hint).
    let mut our_fes: BTreeMap<OwnedRoomId, ForwardExtremities> = BTreeMap::new();
    let rooms: BTreeSet<OwnedRoomId> = batch.iter().map(|e| e.room_id.clone()).collect();
    for room in &rooms {
        let fes = reconcile::local_extremities(&*ctx.store, room).await;
        if !fes.is_empty() {
            our_fes.insert(room.clone(), fes);
        }
    }
    // What actually goes on the wire is only the part this transaction doesn't
    // already tell the peer — commonly nothing at all (a message sits on our own
    // head, so the peer derives the identical head-sets by applying the PDUs), in
    // which case the field is omitted outright. `our_fes` stays whole below: it is
    // the obligation ledger, not the advertisement. The two are different facts —
    // the peer ends up *holding* a stripped head, which discharges the duty to
    // advertise it more strongly than telling it the id would.
    let advert = reconcile::strip_known(&our_fes, &covered_by(batch));

    let txn_id = ctx.idgen.next_id();
    loop {
        match send_transaction_with_retry(ctx, dest, &txn_id, &pdus, &advert, backoff, kick_rx)
            .await
        {
            SendOutcome::Shutdown => return false,
            // 4xx: drop the batch (retrying won't help), but DO NOT clear the
            // advertisement obligation — our heads never landed, so it stands. A
            // removal fault re-sends (and re-4xxs) under the same txn id.
            SendOutcome::Rejected => match ctx.store.remove_pdus(dest, &ids).await {
                Ok(()) => return true,
                Err(e) => {
                    error!(%dest, error = %e, "removing rejected PDUs from outbox");
                    if sleep_backoff(backoff, kick_rx).await {
                        *backoff = BACKOFF_BASE;
                    }
                }
            },
            SendOutcome::Delivered(peer_fes) => {
                // Before the outbox rows go: once they're gone this batch is
                // unrecoverable, so a mark written after a crashed removal
                // would be a mark we could never derive again. Recording first
                // can only ever duplicate (the re-send re-marks the same event,
                // which the store's max-guard absorbs).
                record_deliveries(ctx, dest, batch).await;
                match ctx.store.remove_pdus(dest, &ids).await {
                    Ok(()) => {
                        // The transaction carried `our_fes`, so this 2xx satisfied any
                        // standing advertisement obligation for the rooms it covered.
                        if !our_fes.is_empty() {
                            let room_refs: Vec<&RoomId> =
                                our_fes.keys().map(AsRef::as_ref).collect();
                            if let Err(e) = ctx.store.remove_advertisements(dest, &room_refs).await
                            {
                                warn!(%dest, error = %e, "clearing satisfied advertisement obligations");
                            }
                        }
                        // Reconcile against the heads the peer advertised back. Spawned
                        // so a peer round-trip doesn't stall this destination's drain.
                        spawn_reconcile(ctx, dest, peer_fes);
                        return true;
                    }
                    Err(e) => {
                        // Rows survive a removal fault; back off and re-send under the
                        // same txn id (the peer dedups) rather than hot-looping.
                        error!(%dest, error = %e, "removing delivered PDUs from outbox");
                        if sleep_backoff(backoff, kick_rx).await {
                            *backoff = BACKOFF_BASE;
                        }
                    }
                }
            }
        }
    }
}

/// Mark, per room in the delivered batch, the newest event `dest` has now
/// acknowledged holding — the raw material sync turns into delivery receipts.
///
/// One mark per room rather than per event: the mark means "everything up to
/// here", exactly as a read receipt does, and the batch is drained in causal
/// order so the last event of a room in the batch is its newest. `dest`
/// acknowledged the *transaction*, so every event in it is covered.
///
/// Best-effort by design, in both directions:
/// - A write fault is logged and swallowed. Federation delivery must never
///   stall on a receipt, and the mark is self-repairing — the next delivery to
///   this destination re-marks the room from wherever it got to.
/// - The peer 2xx'd a *durably staged* transaction, not an applied one, so a
///   marked event can still be dropped later at auth. This is delivery, not
///   acceptance; see `DeliveryStore`.
async fn record_deliveries<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
    dest: &ServerName,
    batch: &[neutrino_event::Event],
) {
    let mut newest: BTreeMap<&RoomId, &EventId> = BTreeMap::new();
    for event in batch {
        newest.insert(&event.room_id, &event.event_id);
    }
    let ts = now_ms();
    for (room, event_id) in newest {
        if let Err(e) = ctx.store.record_delivery(dest, room, event_id, ts).await {
            warn!(%dest, %room, error = %e, "recording federation delivery mark");
        }
    }
}

/// Deliver one anti-entropy advertisement to `dest`: an empty-`pdus` transaction
/// carrying our current forward extremities for the rooms `dest` is owed (MSC
/// anti-entropy-extension). On a 2xx the obligation rows are cleared and the
/// peer's response heads are reconciled (symmetric exchange); a 4xx drops the
/// obligation for what we tried (a malformed empty-`pdus` envelope retrying can't
/// fix); a transient error retries, leaving the durable rows so a restart
/// re-sends. Returns `false` only on shutdown.
///
/// Only the rooms we actually advertise are cleared on success. Rooms whose
/// current extremities read back empty (unknown / no heads — not expected for a
/// room we just applied a join to) carry nothing to advertise; their obligation
/// rows are cleared up-front so a junk row can't wedge the drain.
async fn send_advertisement<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
    dest: &ServerName,
    rooms: &[OwnedRoomId],
    backoff: &mut Duration,
    kick_rx: &mut watch::Receiver<()>,
) -> bool {
    let mut our_fes: BTreeMap<OwnedRoomId, ForwardExtremities> = BTreeMap::new();
    for room in rooms {
        let fes = reconcile::local_extremities(&*ctx.store, room).await;
        if !fes.is_empty() {
            our_fes.insert(room.clone(), fes);
        }
    }

    // Nothing advertisable for any owed room — clear the (junk) obligations and
    // move on rather than re-reading the same rows forever.
    if our_fes.is_empty() {
        let room_refs: Vec<&RoomId> = rooms.iter().map(AsRef::as_ref).collect();
        if let Err(e) = ctx.store.remove_advertisements(dest, &room_refs).await {
            warn!(%dest, error = %e, "clearing un-advertisable advertisement obligations");
        }
        return true;
    }

    // Clear only the rooms we actually advertise; an owed room with no heads is
    // left for the junk-clear branch above to reap on a later pass.
    let advertised: Vec<&RoomId> = our_fes.keys().map(AsRef::as_ref).collect();
    let empty_pdus: Vec<Box<RawJsonValue>> = Vec::new();
    let txn_id = ctx.idgen.next_id();
    loop {
        match send_transaction_with_retry(
            ctx,
            dest,
            &txn_id,
            &empty_pdus,
            &our_fes,
            backoff,
            kick_rx,
        )
        .await
        {
            SendOutcome::Shutdown => return false,
            // 4xx on an empty-`pdus` envelope shouldn't happen against a conforming
            // peer; retrying won't help, so drop the obligation for what we tried.
            SendOutcome::Rejected => {
                if let Err(e) = ctx.store.remove_advertisements(dest, &advertised).await {
                    warn!(%dest, error = %e, "clearing rejected advertisement obligations");
                }
                return true;
            }
            SendOutcome::Delivered(peer_fes) => {
                match ctx.store.remove_advertisements(dest, &advertised).await {
                    Ok(()) => {
                        spawn_reconcile(ctx, dest, peer_fes);
                        return true;
                    }
                    Err(e) => {
                        // Rows survive a clear fault; back off and re-send under the
                        // same txn id (the peer dedups) rather than hot-looping.
                        error!(%dest, error = %e, "clearing advertisement obligations after send");
                        if sleep_backoff(backoff, kick_rx).await {
                            *backoff = BACKOFF_BASE;
                        }
                    }
                }
            }
        }
    }
}

/// Spawn a best-effort reconciliation task per room the peer advertised in its
/// transaction response. Fire-and-forget: a healed link's divergence is closed
/// off the outbox's hot path (see [`reconcile::reconcile_room`]).
fn spawn_reconcile<S: StorageBackend + 'static>(
    ctx: &SenderCtx<S>,
    dest: &ServerName,
    peer_fes: BTreeMap<OwnedRoomId, ForwardExtremities>,
) {
    for (room, heads) in peer_fes {
        let store = ctx.store.clone();
        let fetcher = ctx.fetcher.clone();
        let policy = ctx.policy.clone();
        let worker_poke = ctx.worker_poke.clone();
        let dest = dest.to_owned();
        tokio::spawn(async move {
            reconcile::reconcile_room(
                &*store,
                &*fetcher,
                &policy,
                &worker_poke,
                &dest,
                &room,
                &heads,
            )
            .await;
        });
    }
}

/// Sleep for a full-jittered interval in `[0, *backoff]`, then advance the
/// backoff ceiling toward [`BACKOFF_CAP`](crate::util::BACKOFF_CAP).
///
/// Returns `true` if a `KickBackoff` (`kick_rx` advanced — the host signalled
/// connectivity restored) interrupts the wait: the caller resets `*backoff` to
/// base and retries immediately, and the ceiling is left unadvanced. A kick that
/// landed while the caller was mid-send is caught here too — `watch` retains the
/// unobserved pulse, so `changed()` resolves at once. An `Err` from
/// `changed()` means the kick sender was dropped (teardown, this task is about
/// to be aborted); fall back to a normal backoff so we never busy-loop on a
/// closed channel.
async fn sleep_backoff(backoff: &mut Duration, kick_rx: &mut watch::Receiver<()>) -> bool {
    let wait = jitter(*backoff);
    tokio::select! {
        biased;
        res = kick_rx.changed() => {
            if res.is_ok() {
                return true;
            }
            tokio::time::sleep(wait).await;
            *backoff = next_backoff(*backoff);
            false
        }
        _ = tokio::time::sleep(wait) => {
            *backoff = next_backoff(*backoff);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use neutrino_event::ROOM_VERSION_ID;
    use neutrino_event::event_builder::EventBuilder;
    use neutrino_store::{EventStore, FederationOutbox, RoomStore};
    use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId};
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::util::BACKOFF_CAP;

    /// A shutdown token that never fires — for tests exercising non-shutdown paths.
    fn no_shutdown() -> CancellationToken {
        CancellationToken::new()
    }

    /// A kick receiver whose sender is dropped immediately — no kick ever
    /// arrives, so the sender behaves exactly as it did before `KickBackoff`
    /// (a dropped sender makes `sleep_backoff` fall back to a normal backoff).
    fn no_kick() -> watch::Receiver<()> {
        watch::channel(()).1
    }

    /// A no-op fetcher for the sender tests: these stubs respond with no
    /// `forward_extremities`, so reconciliation never fires and the fetcher is
    /// never called — but the sender pool now requires one.
    struct NoFetcher;
    #[async_trait::async_trait]
    impl MissingEventsFetcher for NoFetcher {
        async fn fetch(
            &self,
            _q: crate::ports::MissingEventsQuery<'_>,
        ) -> Result<Vec<Box<RawJsonValue>>, TransportError> {
            Ok(Vec::new())
        }
    }
    fn null_fetcher() -> Arc<dyn MissingEventsFetcher> {
        Arc::new(NoFetcher)
    }

    /// A worker-poke sender whose receiver is dropped — `try_send` just fails,
    /// which reconciliation tolerates. Reconciliation never fires in these tests
    /// anyway (see [`NoFetcher`]).
    fn null_poke() -> mpsc::Sender<OwnedRoomId> {
        mpsc::channel(1).0
    }

    /// In-process [`FederationTransport`] stub — the sender pool's outbound port,
    /// not an HTTP peer. The runtime is tested against transport *outcomes*
    /// (deliver / 4xx / 5xx / unreachable) directly; the on-the-wire Matrix
    /// round-trip is covered by the `e2e_lb_*` + `neutrino` federation tests.
    ///
    /// The first `fail_until` calls to a non-`dead` destination return
    /// `Status(fail_status)`; the rest succeed and record their PDUs +
    /// advertised forward extremities. A destination in `dead` always fails
    /// `Transient` (an unreachable peer). `attempts`/`txns` count every
    /// recorded (non-`dead`) call (to assert retry + txn-id reuse).
    #[derive(Default)]
    struct Stub {
        /// Decoded PDUs of every accepted transaction (one `Vec` per txn).
        accepted: Mutex<Vec<Vec<Value>>>,
        /// The advertised `forward_extremities` of every accepted transaction,
        /// as JSON — lets anti-entropy tests assert an advertisement carried our
        /// heads (`fes[0].get(room).is_some()`).
        fes: Mutex<Vec<Value>>,
        txns: Mutex<Vec<String>>,
        attempts: AtomicU64,
        fail_until: u64,
        fail_status: u16,
        /// Destinations that are unreachable (always `Transient`).
        dead: Mutex<BTreeSet<OwnedServerName>>,
    }

    #[async_trait::async_trait]
    impl FederationTransport for Stub {
        async fn send_transaction(
            &self,
            dest: &ServerName,
            txn_id: &str,
            pdus: &[Box<RawJsonValue>],
            forward_extremities: &BTreeMap<OwnedRoomId, ForwardExtremities>,
        ) -> Result<BTreeMap<OwnedRoomId, ForwardExtremities>, TransportError> {
            if self.dead.lock().unwrap().contains(dest) {
                return Err(TransportError::Transient("unreachable".to_owned()));
            }
            self.txns.lock().unwrap().push(txn_id.to_owned());
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_until {
                return Err(TransportError::Status(self.fail_status));
            }
            let decoded: Vec<Value> = pdus
                .iter()
                .map(|p| serde_json::from_str(p.get()).expect("stub pdu is valid JSON"))
                .collect();
            self.fes
                .lock()
                .unwrap()
                .push(serde_json::to_value(forward_extremities).unwrap());
            self.accepted.lock().unwrap().push(decoded);
            // The peer reports no forward extremities, so reconciliation never fires.
            Ok(BTreeMap::new())
        }
    }

    /// A fixed destination name. The stub ignores reachability, so any valid
    /// `ServerName` works as an outbox target.
    fn peer() -> OwnedServerName {
        "peer.test".parse().unwrap()
    }

    /// Open a store, create a room, and enqueue `n` linked message events to
    /// `dest` in the outbox. Returns the store + tempfile guard + the event ids
    /// in causal order. `create_room` itself enqueues nothing (no destinations),
    /// so the outbox holds exactly the `n` messages.
    async fn store_with_outbox(
        dest: &ServerName,
        n: usize,
    ) -> (Arc<SqliteStore>, TempDir, OwnedRoomId, Vec<OwnedEventId>) {
        let tempfile = TempDir::new().unwrap();
        let store = Arc::new(SqliteStore::open_in_dir(tempfile.path()).await.unwrap());
        let sender: OwnedUserId = "@alice:local.test".parse().unwrap();

        let create = EventBuilder::new(sender.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .build()
            .unwrap();
        let room_id = create.room_id.clone();
        let create_id = create.event_id.clone();
        let join = EventBuilder::new(sender.clone(), "m.room.member".to_owned())
            .room_id(room_id.clone())
            .state_key(sender.as_str().to_owned())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![create_id.clone()])
            .prev_state_events(vec![create_id.clone()])
            .build()
            .unwrap();
        let mut prev = join.event_id.clone();
        store.create_room(&create, &[join]).await.unwrap();

        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let ev = EventBuilder::new(sender.clone(), "m.room.message".to_owned())
                .room_id(room_id.clone())
                .content(json!({ "msgtype": "m.text", "body": format!("msg {i}") }))
                .prev_events(vec![prev.clone()])
                .origin_server_ts(1_700_000_000_000 + i as u64)
                .build()
                .unwrap();
            store.persist_event(&ev, &[dest]).await.unwrap();
            prev = ev.event_id.clone();
            ids.push(ev.event_id);
        }
        (store, tempfile, room_id, ids)
    }

    /// Poll until `dest`'s outbox is empty, or panic after ~10s.
    async fn wait_drained(store: &SqliteStore, dest: &ServerName) {
        for _ in 0..500 {
            if store
                .pending_pdus(dest, usize::MAX)
                .await
                .unwrap()
                .is_empty()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("outbox for {dest} not drained within timeout");
    }

    /// Enqueue a durable advertisement obligation for `dest` + `room` by
    /// persisting a non-state event with `advertise_to = [dest]` (and no outbox
    /// destination). The event also advances the timeline head, so our heads
    /// differ from `dest`'s notional join point — exactly the divergence an
    /// advertisement reconciles. Mirrors what `persist_resolved_event` does on a
    /// real joined-set-growth trigger.
    async fn enqueue_advertisement(store: &SqliteStore, room: &RoomId, dest: &ServerName) {
        use neutrino_store::RoomStore;
        let alice: OwnedUserId = "@alice:local.test".parse().unwrap();
        let (timeline, state) = store.forward_extremities(room).await.unwrap().unwrap();
        let prev = timeline.iter().next().expect("room has a head").clone();
        let msg = EventBuilder::new(alice, "m.room.message".to_owned())
            .room_id(room.to_owned())
            .content(json!({ "msgtype": "m.text", "body": "advertise-me" }))
            .prev_events(vec![prev])
            .origin_server_ts(1_700_000_000_123)
            .build()
            .unwrap();
        let new_timeline: BTreeSet<OwnedEventId> = [msg.event_id.clone()].into_iter().collect();
        store
            .persist_resolved_event(&msg, &new_timeline, &state, &BTreeMap::new(), &[], &[dest])
            .await
            .unwrap();
    }

    /// Build a message on the room's heads exactly as `RoomCore::build_local_event`
    /// would: `prev_events` = the timeline heads, `prev_state_events` = the state
    /// heads. Nothing is persisted — the caller commits it (and decides the
    /// resulting head-set), so two of these built from one snapshot are genuine
    /// concurrent siblings.
    ///
    /// `ts` is what makes two such siblings distinct events: the reference hash is
    /// taken over the *redacted* event, and redaction strips a message's `content`
    /// entirely, so two messages agreeing on metadata share one event id however
    /// much their bodies differ.
    async fn message_on_heads(
        store: &SqliteStore,
        room: &RoomId,
        body: &str,
        ts: u64,
    ) -> neutrino_event::Event {
        use neutrino_store::RoomStore;
        let alice: OwnedUserId = "@alice:local.test".parse().unwrap();
        let (timeline, state) = store.forward_extremities(room).await.unwrap().unwrap();
        EventBuilder::new(alice, "m.room.message".to_owned())
            .room_id(room.to_owned())
            .content(json!({ "msgtype": "m.text", "body": body }))
            .prev_events(timeline.iter().cloned().collect())
            .prev_state_events(state.iter().cloned().collect())
            .origin_server_ts(ts)
            .build()
            .unwrap()
    }

    /// Commit `event` with `heads` as the room's timeline head-set (state heads
    /// unchanged), enqueued for `dests`. The explicit head-set is what lets a test
    /// model a fork: commit two siblings and name both as heads.
    async fn commit_with_heads(
        store: &SqliteStore,
        event: &neutrino_event::Event,
        heads: &[OwnedEventId],
        dests: &[&ServerName],
    ) {
        use neutrino_store::RoomStore;
        let (_, state) = store
            .forward_extremities(&event.room_id)
            .await
            .unwrap()
            .unwrap();
        let timeline: BTreeSet<OwnedEventId> = heads.iter().cloned().collect();
        store
            .persist_resolved_event(event, &timeline, &state, &BTreeMap::new(), dests, &[])
            .await
            .unwrap();
    }

    /// Poll until `dest` has no pending advertisement obligation, or panic ~10s.
    async fn wait_adv_drained(store: &SqliteStore, dest: &ServerName) {
        for _ in 0..500 {
            if store.pending_advertisements(dest).await.unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("advertisement obligation for {dest} not drained within timeout");
    }

    /// A `SenderCtx` for calling `deliver_batch` / `send_advertisement` directly
    /// (deterministic, no supervisor/pool timing). One global send permit.
    fn test_ctx(
        store: Arc<SqliteStore>,
        transport: Arc<dyn FederationTransport>,
    ) -> SenderCtx<SqliteStore> {
        SenderCtx {
            store,
            transport,
            idgen: Arc::new(TxnIdGen::new(now_ms())),
            send_slots: Arc::new(Semaphore::new(1)),
            fetcher: null_fetcher(),
            policy: EventPolicy::trusted_network(),
            worker_poke: null_poke(),
        }
    }

    /// A backoff receiver whose sender is dropped — no kick ever arrives, so
    /// `sleep_backoff` falls back to a normal timed backoff.
    fn test_backoff() -> watch::Receiver<()> {
        let (_tx, mut rx) = watch::channel(());
        rx.borrow_and_update();
        rx
    }

    // No startup jitter in tests — exercise the full path without the wait.
    const NO_JITTER: Duration = Duration::ZERO;

    /// The `content.body` of a wire PDU. v12 PDUs carry no `event_id` (it's the
    /// reference hash, computed not transmitted), so identity/order is checked
    /// via the per-message body we set in `store_with_outbox`.
    fn body_of(pdu: &Value) -> String {
        pdu["content"]["body"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    #[tokio::test]
    async fn delivers_pending_and_drains_outbox() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 3).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_drained(&store, &dest).await;

        // Every event was delivered, in causal order, across the transaction(s).
        let delivered: Vec<String> = stub
            .accepted
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .map(body_of)
            .collect();
        let expected: Vec<String> = (0..3).map(|i| format!("msg {i}")).collect();
        assert_eq!(delivered, expected);
    }

    #[tokio::test]
    async fn retries_after_transient_5xx() {
        // First request 500s, then succeeds — the batch must survive and retry.
        let stub = Arc::new(Stub {
            fail_until: 1,
            fail_status: 500,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_drained(&store, &dest).await;

        assert!(
            stub.attempts.load(Ordering::SeqCst) >= 2,
            "should have retried"
        );
        let accepted = stub.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(body_of(&accepted[0][0]), "msg 0");
    }

    #[tokio::test]
    async fn drops_batch_on_4xx_without_retry() {
        // A permanent 400 must drop the batch (not loop forever).
        let stub = Arc::new(Stub {
            fail_until: u64::MAX,
            fail_status: 400,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_drained(&store, &dest).await; // dropped → outbox empties

        // Exactly one attempt: the 4xx was not retried.
        assert_eq!(stub.attempts.load(Ordering::SeqCst), 1);
        assert!(stub.accepted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn chunks_backlog_over_the_50_pdu_cap() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 60).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_drained(&store, &dest).await;

        let accepted = stub.accepted.lock().unwrap();
        // 60 split as 50 + 10.
        assert_eq!(accepted.len(), 2);
        assert_eq!(accepted[0].len(), MAX_PDUS_PER_TXN);
        assert_eq!(accepted[1].len(), 10);
        // Order preserved across the chunk boundary.
        let delivered: Vec<String> = accepted.iter().flatten().map(body_of).collect();
        let expected: Vec<String> = (0..60).map(|i| format!("msg {i}")).collect();
        assert_eq!(delivered, expected);
    }

    #[tokio::test]
    async fn dead_peer_does_not_block_healthy_one() {
        let stub = Arc::new(Stub::default());
        let healthy = peer();
        let dead: OwnedServerName = "dead.test".parse().unwrap();
        stub.dead.lock().unwrap().insert(dead.clone());

        // One store, both destinations enqueued.
        let (store, _tmp, room_id, _ids) = store_with_outbox(&healthy, 2).await;
        let sender: OwnedUserId = "@alice:local.test".parse().unwrap();
        // Add a message destined for the dead peer too.
        let ev = EventBuilder::new(sender, "m.room.message".to_owned())
            .room_id(room_id)
            .content(json!({ "msgtype": "m.text", "body": "to-dead" }))
            .origin_server_ts(1_800_000_000_000)
            .build()
            .unwrap();
        store.persist_event(&ev, &[&dead]).await.unwrap();

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));

        // The healthy peer drains despite the dead peer retrying forever.
        wait_drained(&store, &healthy).await;
        assert!(
            !store
                .pending_pdus(&dead, usize::MAX)
                .await
                .unwrap()
                .is_empty(),
            "dead peer's PDU should still be pending"
        );
    }

    #[tokio::test]
    async fn discovers_destination_added_after_startup() {
        // Pool starts against an empty outbox; a later persist must wake it.
        let stub = Arc::new(Stub::default());
        let dest = peer();
        // n=0: a real room exists but the outbox starts empty.
        let (store, _tmp, room_id, _ids) = store_with_outbox(&dest, 0).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        // Give the supervisor a moment to reach its idle `changed()` await.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A later persist must wake the supervisor and spawn the task.
        let sender: OwnedUserId = "@alice:local.test".parse().unwrap();
        let ev = EventBuilder::new(sender, "m.room.message".to_owned())
            .room_id(room_id)
            .content(json!({ "msgtype": "m.text", "body": "later" }))
            .origin_server_ts(1_900_000_000_000)
            .build()
            .unwrap();
        store.persist_event(&ev, &[&dest]).await.unwrap();

        wait_drained(&store, &dest).await;
        let accepted = stub.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(body_of(&accepted[0][0]), "later");
    }

    // Anti-entropy: a destination owed only an advertisement (no pending PDUs)
    // gets a sender task that delivers an empty-`pdus` transaction carrying our
    // forward extremities, then clears the obligation. The obligation is seeded
    // into the store *before* the pool starts, so this also covers crash-safety:
    // a durable obligation persisted in one run is re-sent by a fresh pool.
    #[tokio::test]
    async fn sends_advertisement_for_quiescent_obligation() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        // n=0: a real room, empty outbox — the only work is the advertisement.
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 0).await;
        enqueue_advertisement(&store, &room, &dest).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_adv_drained(&store, &dest).await;

        // Exactly one transaction, with no PDUs and our heads for the room.
        let accepted = stub.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1, "one advertisement transaction");
        assert!(accepted[0].is_empty(), "an advertisement carries no PDUs");
        let fes = stub.fes.lock().unwrap();
        assert!(
            fes[0].get(room.as_str()).is_some(),
            "advertisement carried our forward extremities for the room: {:?}",
            fes[0]
        );
    }

    // Anti-entropy: a normal FE-carrying `/send` to a destination satisfies a
    // standing advertisement obligation for the rooms it covers — the piggyback
    // IS the advertisement, so no separate advertisement transaction is sent.
    #[tokio::test]
    async fn covering_send_clears_advertisement_obligation() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        // One pending PDU for dest, plus a standing advertisement obligation for
        // the same room.
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 1).await;
        enqueue_advertisement(&store, &room, &dest).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));
        wait_drained(&store, &dest).await;
        wait_adv_drained(&store, &dest).await;

        // The PDU batch carried our heads and cleared the obligation, so there
        // is exactly one transaction (the batch), not a second advertisement.
        let accepted = stub.accepted.lock().unwrap();
        assert_eq!(
            accepted.len(),
            1,
            "only the PDU batch, no extra advertisement"
        );
        assert_eq!(
            accepted[0].len(),
            1,
            "the batch carried the one pending PDU"
        );
    }

    // Byte thrift, common case: a batch that already tells the peer our heads (the
    // message IS our timeline head, and its `prev_state_events` ARE our state
    // heads) carries no `forward_extremities` at all — the peer derives the
    // identical head-sets by applying the PDU. The standing obligation is still
    // discharged: the peer ends up holding those heads, which is stronger than
    // being told their ids, so no second (empty-`pdus`) advertisement is sent.
    #[tokio::test]
    async fn omits_forward_extremities_the_batch_already_covers() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 0).await;
        // Obligation first, so the event below lands on top of it and its head
        // stops being an extremity — otherwise the batch wouldn't cover our heads.
        enqueue_advertisement(&store, &room, &dest).await;
        let msg = message_on_heads(&store, &room, "on-our-head", 1_700_000_000_456).await;
        commit_with_heads(&store, &msg, std::slice::from_ref(&msg.event_id), &[&dest]).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let batch = store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        assert!(deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await);

        // Checked before the stub locks are taken, so no guard is held across it.
        assert!(
            store
                .pending_advertisements(&dest)
                .await
                .unwrap()
                .is_empty(),
            "a covering batch still discharges the obligation, filtered or not"
        );

        let accepted = stub.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1, "one transaction");
        assert_eq!(accepted[0].len(), 1, "carrying the one pending PDU");
        let fes = stub.fes.lock().unwrap();
        assert_eq!(
            fes[0],
            json!({}),
            "every head is covered by the batch, so nothing is advertised: {:?}",
            fes[0]
        );
    }

    // The flip side: a concurrent extremity the batch does NOT carry is exactly
    // what the advertisement exists for, so it survives — while the covered heads
    // beside it are stripped.
    #[tokio::test]
    async fn advertises_only_the_heads_the_batch_leaves_out() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 0).await;
        // Two siblings built from the same snapshot — neither references the other,
        // so both are genuine concurrent extremities. Only `msg` is enqueued for
        // `dest`, so `fork` is a head this batch cannot tell `dest` about.
        let elsewhere: OwnedServerName = "other.test".parse().unwrap();
        let fork = message_on_heads(&store, &room, "concurrent-fork", 1_700_000_000_456).await;
        let msg = message_on_heads(&store, &room, "on-our-head", 1_700_000_000_789).await;
        let heads = [msg.event_id.clone(), fork.event_id.clone()];
        commit_with_heads(&store, &fork, &heads, &[&elsewhere]).await;
        commit_with_heads(&store, &msg, &heads, &[&dest]).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let batch = store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        assert!(deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await);

        assert_eq!(
            batch.iter().map(|e| e.event_id.clone()).collect::<Vec<_>>(),
            vec![msg.event_id.clone()],
            "the batch carries only our own event, not the fork"
        );
        let fes = stub.fes.lock().unwrap();
        assert_eq!(
            fes[0][room.as_str()]["timeline"],
            json!([fork.event_id.as_str()]),
            "the uncovered fork head is advertised, the batch's own head is not: {:?}",
            fes[0]
        );
        assert_eq!(
            fes[0][room.as_str()]["state"],
            json!([]),
            "state heads are all covered by the PDU's prev_state_events: {:?}",
            fes[0]
        );
    }

    // Anti-entropy 4xx handling, advertisement side: a 4xx on an advertisement is
    // terminal — the obligation is dropped (a malformed empty-pdus envelope can't
    // be fixed by retrying), not retried forever. Driven through `send_advertisement`
    // directly so there's no pool-timing nondeterminism.
    #[tokio::test]
    async fn advertisement_4xx_drops_obligation() {
        let stub = Arc::new(Stub {
            fail_until: u64::MAX,
            fail_status: 400,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 0).await;
        enqueue_advertisement(&store, &room, &dest).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let rooms = store.pending_advertisements(&dest).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        let proceeded = send_advertisement(&ctx, &dest, &rooms, &mut backoff, &mut kick_rx).await;

        assert!(proceeded, "a 4xx is terminal (returns true), not shutdown");
        assert!(
            stub.attempts.load(Ordering::SeqCst) >= 1,
            "the peer was contacted"
        );
        assert!(
            store
                .pending_advertisements(&dest)
                .await
                .unwrap()
                .is_empty(),
            "a 4xx drops the advertisement obligation rather than looping forever"
        );
    }

    // Anti-entropy 4xx handling, PDU-batch side: a
    // 4xx on a batch drops the batch from the outbox but must NOT clear a standing
    // advertisement obligation — our forward extremities never landed, so the duty
    // still stands. Driven through `deliver_batch` directly.
    #[tokio::test]
    async fn batch_4xx_keeps_advertisement_obligation() {
        let stub = Arc::new(Stub {
            fail_until: u64::MAX,
            fail_status: 400,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 1).await;
        enqueue_advertisement(&store, &room, &dest).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let batch = store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        let delivered = deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await;

        assert!(delivered, "a 4xx drops the batch (returns true)");
        assert!(
            store
                .pending_pdus(&dest, usize::MAX)
                .await
                .unwrap()
                .is_empty(),
            "the rejected batch is removed from the outbox"
        );
        assert_eq!(
            store.pending_advertisements(&dest).await.unwrap().len(),
            1,
            "a batch 4xx must leave the advertisement obligation intact (heads never landed)"
        );
    }

    /// The delivery marks a store holds, as `(room, destination, event_id)`
    /// triples — enough to assert both "who was told what" and "how many rows".
    async fn marks(store: &SqliteStore) -> Vec<(String, String, String)> {
        use neutrino_store::{DeliveryPos, DeliveryStore};
        store
            .deliveries_since(DeliveryPos(0))
            .await
            .unwrap()
            .into_iter()
            .map(|d| {
                (
                    d.room_id.to_string(),
                    d.destination.to_string(),
                    d.event_id.to_string(),
                )
            })
            .collect()
    }

    /// A 2xx marks the batch's newest event per room as delivered to that peer
    /// — one mark, not one per event, since the mark means "up to here".
    #[tokio::test]
    async fn delivery_records_the_batch_high_water_mark() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, room, ids) = store_with_outbox(&dest, 3).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let batch = store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        assert!(deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await);

        assert_eq!(
            marks(&store).await,
            vec![(
                room.to_string(),
                dest.to_string(),
                ids[2].to_string() // the newest of the three, not all three
            )]
        );
    }

    /// A 4xx drops the batch without marking anything: the peer rejected the
    /// envelope, so it holds none of it. Claiming delivery here would tick a
    /// receipt for an event that never landed.
    #[tokio::test]
    async fn rejected_batch_records_no_delivery() {
        let stub = Arc::new(Stub {
            fail_until: u64::MAX,
            fail_status: 400,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let batch = store.pending_pdus(&dest, MAX_PDUS_PER_TXN).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        assert!(deliver_batch(&ctx, &dest, &batch, &mut backoff, &mut kick_rx).await);

        assert!(
            marks(&store).await.is_empty(),
            "a rejected transaction delivered nothing"
        );
    }

    /// An advertisement carries no PDUs, so a 2xx on one says nothing about any
    /// event and must leave the marks alone.
    #[tokio::test]
    async fn advertisement_records_no_delivery() {
        let stub = Arc::new(Stub::default());
        let dest = peer();
        let (store, _tmp, room, _ids) = store_with_outbox(&dest, 0).await;
        enqueue_advertisement(&store, &room, &dest).await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let rooms = store.pending_advertisements(&dest).await.unwrap();
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        assert!(send_advertisement(&ctx, &dest, &rooms, &mut backoff, &mut kick_rx).await);

        assert!(
            marks(&store).await.is_empty(),
            "an empty-pdus advertisement delivers no event"
        );
    }

    /// Two peers are marked independently, each at the event *it* was sent —
    /// a peer must never inherit another peer's progress.
    #[tokio::test]
    async fn marks_are_per_destination() {
        let stub = Arc::new(Stub::default());
        let one = peer();
        let two: OwnedServerName = "other.test".parse().unwrap();
        // `one` is sent both messages; `two` only the second.
        let (store, _tmp, room, ids) = store_with_outbox(&one, 1).await;
        let second = message_on_heads(&store, &room, "second", 1_700_000_001_000).await;
        commit_with_heads(
            &store,
            &second,
            std::slice::from_ref(&second.event_id),
            &[&two],
        )
        .await;

        let ctx = test_ctx(store.clone(), stub.clone());
        let mut backoff = BACKOFF_BASE;
        let mut kick_rx = test_backoff();
        for dest in [&one, &two] {
            let batch = store.pending_pdus(dest, MAX_PDUS_PER_TXN).await.unwrap();
            assert!(deliver_batch(&ctx, dest, &batch, &mut backoff, &mut kick_rx).await);
        }

        let mut got = marks(&store).await;
        got.sort();
        let mut want = vec![
            (room.to_string(), one.to_string(), ids[0].to_string()),
            (
                room.to_string(),
                two.to_string(),
                second.event_id.to_string(),
            ),
        ];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn next_backoff_doubles_then_caps() {
        let mut b = BACKOFF_BASE;
        let mut seq = vec![b];
        for _ in 0..20 {
            b = next_backoff(b);
            seq.push(b);
        }
        // Doubling: 1, 2, 4, 8, … seconds.
        assert_eq!(seq[0], Duration::from_secs(1));
        assert_eq!(seq[1], Duration::from_secs(2));
        assert_eq!(seq[2], Duration::from_secs(4));
        assert_eq!(seq[3], Duration::from_secs(8));
        // Eventually pinned at the cap and never exceeding it.
        assert_eq!(*seq.last().unwrap(), BACKOFF_CAP);
        assert!(seq.iter().all(|d| *d <= BACKOFF_CAP));
    }

    #[test]
    fn jitter_stays_within_ceiling() {
        let ceiling = Duration::from_secs(8);
        for _ in 0..1000 {
            assert!(jitter(ceiling) <= ceiling);
        }
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    }

    /// A `KickBackoff` (network restored) interrupts an in-progress backoff
    /// sleep: `sleep_backoff` returns `true` immediately and leaves the ceiling
    /// untouched, so the caller resets to base and retries now instead of
    /// waiting out a long (up to [`BACKOFF_CAP`]) backoff. A pulse sent before we
    /// poll is observed by the `biased` select's first arm, so this is
    /// deterministic without any real sleep.
    #[tokio::test]
    async fn kick_interrupts_backoff_and_preserves_ceiling() {
        let (tx, mut rx) = watch::channel(());
        rx.borrow_and_update(); // baseline; only a later pulse counts as a kick
        let mut backoff = BACKOFF_CAP; // a long ceiling we must NOT wait out
        tx.send_modify(|_| {}); // network restored: kick pending before poll
        let kicked = sleep_backoff(&mut backoff, &mut rx).await;
        assert!(kicked, "a pending kick must interrupt the backoff sleep");
        assert_eq!(
            backoff, BACKOFF_CAP,
            "a kick must not advance the backoff ceiling"
        );
    }

    /// With no kick, `sleep_backoff` waits out the jittered interval and advances
    /// the ceiling toward the cap — the pre-existing behaviour. `_tx` is kept
    /// alive so `changed()` stays pending (a dropped sender would resolve `Err`).
    #[tokio::test]
    async fn backoff_sleep_without_kick_advances_ceiling() {
        let (_tx, mut rx) = watch::channel(());
        rx.borrow_and_update();
        let mut backoff = Duration::from_millis(10);
        let kicked = sleep_backoff(&mut backoff, &mut rx).await;
        assert!(!kicked, "no kick: must report a normal timed backoff");
        assert_eq!(
            backoff,
            Duration::from_millis(20),
            "a timed backoff must double the ceiling"
        );
    }

    /// Cancelling the shutdown token makes the supervisor break out of its loop,
    /// abort its per-destination children, and RETURN — even with a live store and
    /// a forever-retrying dead-peer child task.
    #[tokio::test]
    async fn supervisor_returns_on_shutdown() {
        let dead: OwnedServerName = "dead.test".parse().unwrap();
        let stub = Arc::new(Stub::default());
        stub.dead.lock().unwrap().insert(dead.clone());
        let (store, _tmp, _room, _ids) = store_with_outbox(&dead, 1).await;
        let shutdown = CancellationToken::new();

        let supervisor = spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            shutdown.clone(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        );

        // Let the supervisor enumerate the dead peer and spawn its (forever-retrying)
        // child before we signal shutdown, so we exercise the drain+abort path.
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(5), supervisor)
            .await
            .expect("supervisor must return promptly after shutdown, not hang")
            .expect("supervisor task must not panic");
    }

    /// A permanent 5xx must keep the batch in the outbox — never lost —
    /// and reuse the SAME txn_id on every retry, so the receiver dedups.
    #[tokio::test]
    async fn permanent_5xx_keeps_batch_and_reuses_txn_id() {
        let stub = Arc::new(Stub {
            fail_until: u64::MAX,
            fail_status: 500,
            ..Stub::default()
        });
        let dest = peer();
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        drop(spawn(
            store.clone(),
            stub.clone(),
            2,
            NO_JITTER,
            no_shutdown(),
            no_kick(),
            null_fetcher(),
            EventPolicy::trusted_network(),
            null_poke(),
        ));

        // Wait for at least two attempts — proves it retried after the first 5xx.
        for _ in 0..500 {
            if stub.attempts.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let attempts = stub.attempts.load(Ordering::SeqCst);
        assert!(attempts >= 2, "expected retries, got {attempts}");

        // A 5xx never removes the batch, and nothing was accepted.
        assert!(
            !store
                .pending_pdus(&dest, usize::MAX)
                .await
                .unwrap()
                .is_empty(),
            "a 5xx must leave the batch pending"
        );
        assert!(stub.accepted.lock().unwrap().is_empty());

        // Every retry carried one and the same txn_id.
        let txns = stub.txns.lock().unwrap();
        assert!(txns.len() >= 2);
        assert!(
            txns.iter().all(|t| t == &txns[0]),
            "txn_id must be reused across retries: {txns:?}"
        );
    }
}
