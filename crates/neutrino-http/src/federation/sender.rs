//! Outbound federation delivery: the per-destination sender pool.
//!
//! Drains the durable `outbox` (populated atomically with each accepted event
//! — see `EventStore::persist_resolved_event`) and PUTs transactions to peers
//! via [`FederationClient`]. This is where the "events MUST eventually be sent
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
//!   watch advance it enumerates [`FederationOutbox::pending_destinations`] and
//!   spawns a task for any destination not already running. Idle destination
//!   tasks stay alive (bounded by the size of the mesh) rather than being
//!   reaped and respawned.
//!
//! ## Flood control on startup (per Kegan, 2026-06-04)
//!
//! On a restart with a large backlog, every destination would otherwise fire at
//! once. Two bounds prevent that thundering herd:
//!
//! - **Startup jitter.** Destinations present in the *first* enumeration round
//!   wait a random `[0, STARTUP_JITTER_MAX]` before their first drain. Spreads
//!   the restart burst. Destinations discovered *later* (a live send to a new
//!   peer) drain immediately — no added latency on the common path.
//! - **A global send semaphore.** At most `NEUTRINO_OUTBOUND_CONCURRENCY` (default
//!   [`DEFAULT_OUTBOUND_CONCURRENCY`], min 1) transactions are in flight across
//!   all destinations at once. A backing-off destination holds no permit, so it
//!   never starves a healthy one.
//!
//! ## Failure handling (per Kegan, 2026-06-04)
//!
//! - **4xx** from a peer = the transaction was malformed/rejected at the
//!   envelope level (the inbound handler only 4xxs the whole transaction, never
//!   an individual PDU). Retrying won't help, so the batch is dropped from the
//!   outbox and we move on.
//! - **5xx / transport / unreachable** = transient. Retry the same batch after
//!   an exponential backoff (full jitter, doubling [`BACKOFF_BASE`] → capped at
//!   [`BACKOFF_CAP`]); never remove until a 2xx. Backoff state is in-memory
//!   only, so a restart re-enumerates and retries within the startup jitter
//!   window — restarting the app is a way to "kick" a stuck destination.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use neutrino_store::{FederationOutbox, StreamPos};
use neutrino_store_sqlite::SqliteStore;
use rand::Rng;
use ruma::{EventId, OwnedServerName, ServerName};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::{Semaphore, watch};
use tracing::{debug, error, info, warn};

use crate::federation::client::{FederationClient, FederationClientError, TxnIdGen};
use crate::federation::now_ms;

/// Max PDUs per `/send` transaction. The inbound handler 400s a transaction
/// carrying more than this (`send.rs`), so chunk on the way out.
const MAX_PDUS_PER_TXN: usize = 50;

/// Backoff floor after a transient delivery failure.
const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Backoff ceiling. The exponential sequence (1, 2, 4, 8, … s) is clamped here.
const BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);

/// Default cap on transactions in flight across all destinations. Overridable
/// via `NEUTRINO_OUTBOUND_CONCURRENCY` (clamped to ≥1).
const DEFAULT_OUTBOUND_CONCURRENCY: usize = 2;

/// Upper bound on the random delay a startup-present destination waits before
/// its first drain. Generous on purpose — spreads a fleet of restart-time
/// retries so they don't flood the network in lockstep.
const STARTUP_JITTER_MAX: Duration = Duration::from_secs(30);

/// Spawn the outbound sender pool. Returns immediately; the supervisor and
/// per-destination tasks run detached for the lifetime of the process.
///
/// Subscribes to the persist watch *before* the first enumeration so a
/// destination added concurrently with startup can't be missed (the watch will
/// have advanced, waking the supervisor's first `changed()`).
pub(crate) fn spawn(store: Arc<SqliteStore>, origin: String) {
    spawn_with(store, origin, outbound_concurrency(), STARTUP_JITTER_MAX);
}

/// Inner spawn with the flood-control bounds made explicit, so tests can run
/// the full supervisor → task path with zero startup jitter.
fn spawn_with(
    store: Arc<SqliteStore>,
    origin: String,
    concurrency: usize,
    startup_jitter_max: Duration,
) {
    let watch_rx = store.subscribe();
    let client = Arc::new(FederationClient::new(origin));
    // Process-startup prefix keeps txn ids unique across restarts; the
    // in-memory counter keeps them unique within a run.
    let idgen = Arc::new(TxnIdGen::new(now_ms()));
    let send_slots = Arc::new(Semaphore::new(concurrency));
    tokio::spawn(supervise(
        store,
        client,
        idgen,
        send_slots,
        watch_rx,
        startup_jitter_max,
    ));
}

/// Discovery loop: ensure every destination with pending PDUs has a running
/// sender task. Runs until the store's watch sender is dropped (shutdown).
async fn supervise(
    store: Arc<SqliteStore>,
    client: Arc<FederationClient>,
    idgen: Arc<TxnIdGen>,
    send_slots: Arc<Semaphore>,
    mut watch_rx: watch::Receiver<StreamPos>,
    startup_jitter_max: Duration,
) {
    let mut running: HashSet<OwnedServerName> = HashSet::new();
    let mut first_round = true;
    loop {
        match store.pending_destinations().await {
            Ok(dests) => {
                for dest in dests {
                    // `insert` returns false if already present → don't respawn.
                    if running.insert(dest.clone()) {
                        // Only the startup backlog gets jittered; a destination
                        // discovered mid-run is a single live send, not a flood.
                        let initial_delay = if first_round {
                            jitter(startup_jitter_max)
                        } else {
                            Duration::ZERO
                        };
                        debug!(%dest, ?initial_delay, "spawning outbound sender task");
                        tokio::spawn(run_destination(
                            store.clone(),
                            client.clone(),
                            idgen.clone(),
                            send_slots.clone(),
                            dest,
                            watch_rx.clone(),
                            initial_delay,
                        ));
                    }
                }
            }
            Err(e) => error!(error = %e, "enumerating outbox destinations"),
        }
        first_round = false;
        // Wait for the next persist (which may have added a new destination).
        // `Err` means the watch sender was dropped — the store is gone, so the
        // pool shuts down with it.
        if watch_rx.changed().await.is_err() {
            info!("persist watch closed; outbound supervisor stopping");
            return;
        }
    }
}

/// Drain a single destination forever: deliver pending PDUs, then block until
/// the next persist wakes us. Owns its own backoff state.
async fn run_destination(
    store: Arc<SqliteStore>,
    client: Arc<FederationClient>,
    idgen: Arc<TxnIdGen>,
    send_slots: Arc<Semaphore>,
    dest: OwnedServerName,
    mut watch_rx: watch::Receiver<StreamPos>,
    initial_delay: Duration,
) {
    if !initial_delay.is_zero() {
        tokio::time::sleep(initial_delay).await;
    }

    let mut backoff = BACKOFF_BASE;
    loop {
        let pending = match store.pending_pdus(&dest).await {
            Ok(p) => p,
            Err(e) => {
                // A storage read fault is transient; back off and retry.
                error!(%dest, error = %e, "reading pending PDUs");
                sleep_backoff(&mut backoff).await;
                continue;
            }
        };

        if pending.is_empty() {
            // Fully drained: reset backoff and idle until the next persist.
            backoff = BACKOFF_BASE;
            if watch_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        // One transaction's worth, in causal order.
        let chunk = &pending[..pending.len().min(MAX_PDUS_PER_TXN)];
        let pdus: Vec<Box<RawJsonValue>> = chunk.iter().map(|e| e.raw.clone()).collect();
        let ids: Vec<&EventId> = chunk.iter().map(|e| &*e.event_id).collect();
        let txn_id = idgen.next_id();

        // Hold a global permit only around the network call — released before
        // any backoff sleep, so a slow peer can't pin a concurrency slot.
        let send_result = match send_slots.acquire().await {
            Ok(_permit) => client.send_transaction(&dest, &txn_id, &pdus).await,
            // The semaphore is never closed in normal operation; an error here
            // means shutdown, so stop the task.
            Err(_) => return,
        };

        match send_result {
            Ok(()) => {
                drop_batch(&store, &dest, &ids).await;
                // Reset and loop immediately to drain any remaining chunks.
                backoff = BACKOFF_BASE;
            }
            // 4xx: the peer rejected the transaction envelope. Retrying is
            // futile — drop the batch and carry on with the next.
            Err(FederationClientError::Status(code)) if (400..500).contains(&code) => {
                warn!(%dest, code, pdus = ids.len(), "peer rejected transaction (4xx); dropping batch");
                drop_batch(&store, &dest, &ids).await;
                backoff = BACKOFF_BASE;
            }
            // 5xx / transport / URL: transient. Keep the batch, back off, retry.
            Err(e) => {
                warn!(%dest, error = %e, backoff = ?backoff, "transaction delivery failed; will retry");
                sleep_backoff(&mut backoff).await;
            }
        }
    }
}

/// Remove a delivered (or dropped-as-4xx) batch from the outbox. A storage
/// fault here is logged but not fatal: the rows survive and the next drain
/// re-attempts removal (delivery is idempotent on the receiver via txn dedup).
async fn drop_batch(store: &SqliteStore, dest: &ServerName, ids: &[&EventId]) {
    if let Err(e) = store.remove_pdus(dest, ids).await {
        error!(%dest, error = %e, "removing delivered PDUs from outbox");
    }
}

/// Sleep for a full-jittered interval in `[0, *backoff]`, then advance the
/// backoff ceiling toward [`BACKOFF_CAP`].
async fn sleep_backoff(backoff: &mut Duration) {
    let wait = jitter(*backoff);
    tokio::time::sleep(wait).await;
    *backoff = next_backoff(*backoff);
}

/// Double the backoff ceiling, clamped at [`BACKOFF_CAP`].
fn next_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(BACKOFF_CAP)
}

/// Full jitter: a uniform random duration in `[0, ceiling]`. Spreads retries
/// (and startup) so a fleet of senders doesn't thunder a recovering peer in
/// lockstep.
fn jitter(ceiling: Duration) -> Duration {
    let max_ms = ceiling.as_millis() as u64;
    if max_ms == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rand::rng().random_range(0..=max_ms))
}

/// Resolve the configured outbound concurrency from the environment.
fn outbound_concurrency() -> usize {
    parse_concurrency(
        std::env::var("NEUTRINO_OUTBOUND_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

/// Parse + clamp the concurrency knob: a valid `usize ≥ 1`, else the default.
fn parse_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_OUTBOUND_CONCURRENCY)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::{
        Json, Router,
        extract::Path,
        http::StatusCode,
        response::IntoResponse,
        routing::put,
    };
    use neutrino_common::ROOM_VERSION_ID;
    use neutrino_state::event_id::EventBuilder;
    use neutrino_store::{EventStore, RoomStore};
    use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId};
    use serde_json::{Value, json};
    use tempfile::NamedTempFile;

    use super::*;

    /// Stub federation peer. `fail_until` requests return `fail_status`; the
    /// rest 200 and have their `pdus` array recorded (one entry per accepted
    /// transaction). `attempts` counts *every* request, success or not.
    #[derive(Default)]
    struct Stub {
        accepted: Mutex<Vec<Vec<Value>>>,
        attempts: AtomicU64,
        fail_until: u64,
        fail_status: u16,
    }

    /// Bind a stub peer on an ephemeral port and return its `ServerName`
    /// (`127.0.0.1:{port}`) — exactly what the sender resolves to `http://…`.
    async fn spawn_peer(stub: Arc<Stub>) -> OwnedServerName {
        let app = Router::new().route(
            "/_matrix/federation/v1/send/{txn}",
            put(move |Path(_txn): Path<String>, Json(body): Json<Value>| {
                let stub = stub.clone();
                async move {
                    let n = stub.attempts.fetch_add(1, Ordering::SeqCst);
                    if n < stub.fail_until {
                        return (StatusCode::from_u16(stub.fail_status).unwrap(), "fail")
                            .into_response();
                    }
                    let pdus = body["pdus"].as_array().cloned().unwrap_or_default();
                    stub.accepted.lock().unwrap().push(pdus);
                    Json(json!({ "pdus": {} })).into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// A `ServerName` for a port nothing listens on → every connect refuses.
    async fn dead_peer() -> OwnedServerName {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// Open a store, create a room, and enqueue `n` linked message events to
    /// `dest` in the outbox. Returns the store + tempfile guard + the event ids
    /// in causal order. `create_room` itself enqueues nothing (no destinations),
    /// so the outbox holds exactly the `n` messages.
    async fn store_with_outbox(
        dest: &ServerName,
        n: usize,
    ) -> (
        Arc<SqliteStore>,
        NamedTempFile,
        OwnedRoomId,
        Vec<OwnedEventId>,
    ) {
        let tempfile = NamedTempFile::new().unwrap();
        let store = Arc::new(SqliteStore::open(tempfile.path()).await.unwrap());
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
            if store.pending_pdus(dest).await.unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("outbox for {dest} not drained within timeout");
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
        let dest = spawn_peer(stub.clone()).await;
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 3).await;

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);
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
        let dest = spawn_peer(stub.clone()).await;
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);
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
        let dest = spawn_peer(stub.clone()).await;
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 1).await;

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);
        wait_drained(&store, &dest).await; // dropped → outbox empties

        // Exactly one attempt: the 4xx was not retried.
        assert_eq!(stub.attempts.load(Ordering::SeqCst), 1);
        assert!(stub.accepted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn chunks_backlog_over_the_50_pdu_cap() {
        let stub = Arc::new(Stub::default());
        let dest = spawn_peer(stub.clone()).await;
        let (store, _tmp, _room, _ids) = store_with_outbox(&dest, 60).await;

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);
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
        let healthy_stub = Arc::new(Stub::default());
        let healthy = spawn_peer(healthy_stub.clone()).await;
        let dead = dead_peer().await;

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

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);

        // The healthy peer drains despite the dead peer retrying forever.
        wait_drained(&store, &healthy).await;
        assert!(
            !store.pending_pdus(&dead).await.unwrap().is_empty(),
            "dead peer's PDU should still be pending"
        );
    }

    #[tokio::test]
    async fn discovers_destination_added_after_startup() {
        // Pool starts against an empty outbox; a later persist must wake it.
        let stub = Arc::new(Stub::default());
        let dest = spawn_peer(stub.clone()).await;
        // n=0: a real room exists but the outbox starts empty.
        let (store, _tmp, room_id, _ids) = store_with_outbox(&dest, 0).await;

        spawn_with(store.clone(), "local.test".to_owned(), 2, NO_JITTER);
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

    #[test]
    fn parse_concurrency_clamps_and_defaults() {
        assert_eq!(parse_concurrency(None), DEFAULT_OUTBOUND_CONCURRENCY);
        assert_eq!(
            parse_concurrency(Some("garbage")),
            DEFAULT_OUTBOUND_CONCURRENCY
        );
        assert_eq!(parse_concurrency(Some("")), DEFAULT_OUTBOUND_CONCURRENCY);
        // Zero is meaningless for a semaphore → floored to 1.
        assert_eq!(parse_concurrency(Some("0")), 1);
        assert_eq!(parse_concurrency(Some("1")), 1);
        assert_eq!(parse_concurrency(Some("5")), 5);
    }
}
