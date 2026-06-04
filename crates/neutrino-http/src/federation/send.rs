//! `PUT /_matrix/federation/v1/send/{txnId}` — inbound federation transaction.
//!
//! A transaction is an envelope of up to 50 PDUs (plus EDUs, which this server
//! stubs out — they are deserialized for shape validation and dropped). Each
//! PDU is a fully-formed v12 event; we parse it via
//! [`neutrino_state::event_id::from_wire`] (which derives the event_id from the
//! reference hash, verifies/redacts on content-hash mismatch, and runs the
//! format + semantic validators), then integrate it through the per-room actor
//! ([`RoomRegistry::apply_pdu`]).
//!
//! ## Trust model
//!
//! No X-Matrix auth and no signature verification — the same trusted-mesh
//! stance as `/get_missing_events` and `/backfill`. The transaction's `origin`
//! field is taken at face value for txn deduplication.
//!
//! ## Per-PDU isolation
//!
//! The spec response is a per-event result map
//! (`{ "pdus": { "$id": {} | { "error": … } } }`): a single bad PDU never
//! fails the whole transaction. We toposort the batch by intra-batch
//! `prev_events` / `prev_state_events` so a parent is applied before a child
//! that arrived in the same transaction.
//!
//! ## Gap-filling
//!
//! `apply_pdu` returns a *retryable* [`CoreError`](neutrino_state::CoreError)
//! when an event's ancestry (a `prev_state_events` entry / auth-chain link) is
//! absent from the store. Under MSC4242 the completeness condition is
//! structural: the state DAG must walk back to `m.room.create`. That is exactly
//! what makes `apply_pdu` succeed vs. return retryable, so we loop:
//!
//! 1. **Success** — `apply_pdu` → `Ok`: the closure to create resolved.
//! 2. **No progress** — a fetch round yields zero new events ⇒ the gap is
//!    unfillable ⇒ that PDU gets an `error`, others still commit.
//! 3. **Safety bound** — [`MAX_GAPFILL_ROUNDS`] caps the loop against a buggy
//!    peer feeding an unbounded chain.
//!
//! The outbound fetch ([`MissingEventsFetcher`]) needs an HTTP client +
//! server-name resolution (the deferred outbound-federation work), so it is
//! behind a trait. Until that lands the wired impl is [`NoFetcher`], which
//! always reports "no progress" — so a transaction whose PDUs arrive with
//! complete ancestry (the common in-order case) is handled end-to-end today,
//! and the reqwest-backed fetcher drops in later with no handler change.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_common::Event;
use neutrino_state::event_id::from_wire;
use neutrino_store::{EventStore, FederationInbox, RoomStore};
use ruma::{OwnedEventId, OwnedServerName, RoomId, ServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::FedError;
use crate::room_actor::{RoomActorError, RoomRegistry};
use crate::{AppState, lock_app};

/// Spec maximum PDUs per transaction
/// (<https://spec.matrix.org/v1.18/server-server-api/#transactions>).
const MAX_PDUS: usize = 50;
/// Initial `limit` for the first gap-fill request; doubled each round (MSC4242
/// recommends exponentially increasing the limit until all ancestry is seen).
const INITIAL_GAPFILL_LIMIT: u32 = 10;
/// Hard cap on gap-fill rounds for a single PDU. A backstop against a buggy or
/// adversarial peer feeding an unbounded ancestry chain.
const MAX_GAPFILL_ROUNDS: u32 = 10;

/// Inbound federation transaction body.
///
/// Hand-rolled rather than using `ruma::api::federation` — that crate's
/// `federation-api` feature on our pinned ruma version depends on an
/// unpublished sub-crate. Mirrors the wire-verbatim approach already used by
/// `backfill.rs` / `get_missing_events.rs`: PDUs are opaque `RawValue`s.
#[derive(Deserialize)]
pub(crate) struct TransactionBody {
    /// The sending server's name. Trusted at face value (no X-Matrix auth) and
    /// used only for transaction deduplication + as the gap-fill fetch target.
    origin: OwnedServerName,
    /// Required by the spec; parsed for shape validation then ignored — this
    /// server stores no per-transaction timestamp.
    #[serde(rename = "origin_server_ts")]
    _origin_server_ts: u64,
    /// The events to integrate. Optional in the wire format; a missing key is
    /// an empty transaction.
    #[serde(default)]
    pdus: Vec<Box<RawJsonValue>>,
    /// EDUs are out of scope (no presence/typing/receipts/E2EE on this server).
    /// Deserialized for shape validation, then dropped — stubbed per CLAUDE.md.
    #[serde(default, rename = "edus")]
    _edus: Vec<Box<RawJsonValue>>,
}

/// Per-PDU processing result. An empty object is success; `error` carries a
/// human-readable reason on failure (spec `PduProcessingResult`).
#[derive(Serialize, Default)]
struct PduResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Transaction response body: `{ "pdus": { "$id": {} | { "error": … } } }`.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    pdus: BTreeMap<String, PduResult>,
}

/// Federation `/send/{txnId}` handler.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path(txn_id): Path<String>,
    body: Result<Json<serde_json::Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ResponseBody>, FedError> {
    // Route JSON-edge failures (bad content-type, invalid JSON, shape mismatch)
    // through 400 M_INVALID_PARAM, matching the other federation handlers.
    let body_value = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;
    let body: TransactionBody = serde_json::from_value(body_value)
        .map_err(|_| FedError::BadRequest("body shape does not match the spec"))?;

    if body.pdus.len() > MAX_PDUS {
        return Err(FedError::BadRequest("transaction exceeds 50 PDUs"));
    }

    let (store, registry) = {
        let app = lock_app(&state);
        (app.store.clone(), app.room_registry.clone())
    };

    // Transaction-level idempotency: a re-sent transaction is acknowledged
    // without reprocessing. We do not persist per-PDU results, so a duplicate
    // gets an empty results map — acceptable since every PDU it carried was
    // already integrated (or recorded as rejected) the first time.
    if store
        .record_federation_txn(&body.origin, &txn_id)
        .await
        .map_err(FedError::Storage)?
    {
        return Ok(Json(ResponseBody {
            pdus: BTreeMap::new(),
        }));
    }

    let mut pdus = BTreeMap::new();
    let fetcher = NoFetcher;

    // Parse every PDU up front. `from_wire` derives the event_id from the
    // reference hash, so a PDU that fails it is unkeyable (malformed, or no
    // derivable id) and cannot appear in the result map — it is silently
    // dropped, matching Synapse's log-and-skip for such events.
    //
    // De-duplicate by event_id: a peer may legally repeat the same PDU bytes
    // in one transaction, and `toposort`'s indegree bookkeeping is keyed by
    // event_id, so two array slots sharing an id would double-decrement a
    // child's indegree (a `usize` underflow → panic in debug). Keeping the
    // first occurrence is sufficient — `apply_pdu` is idempotent anyway.
    let mut parsed: Vec<Event> = Vec::new();
    let mut seen: HashSet<OwnedEventId> = HashSet::new();
    for raw in body.pdus {
        if let Ok(event) = from_wire(raw, Vec::new())
            && seen.insert(event.event_id.clone())
        {
            parsed.push(event);
        }
    }

    // Apply parents before children that arrived in the same transaction.
    for event in toposort(parsed) {
        let id = event.event_id.to_string();
        let result =
            match apply_with_gapfill(&registry, &store, &body.origin, event, &fetcher).await {
                Ok(()) => PduResult::default(),
                Err(error) => PduResult { error: Some(error) },
            };
        pdus.insert(id, result);
    }

    Ok(Json(ResponseBody { pdus }))
}

/// Integrate one PDU, gap-filling missing ancestry on a retryable verdict.
///
/// Returns `Ok(())` for any terminal *integration* outcome — accepted,
/// soft-failed, rejected, or idempotent re-delivery — since `apply_pdu` already
/// persists rejects (federation policy). Returns `Err(reason)` only when the
/// PDU could not be evaluated: an unfillable ancestry gap, an unknown room, a
/// malformed/misrouted event (non-retryable `CoreError`), or a storage fault.
async fn apply_with_gapfill<F: MissingEventsFetcher + ?Sized>(
    registry: &RoomRegistry,
    store: &neutrino_store_sqlite::SqliteStore,
    origin: &ServerName,
    event: Event,
    fetcher: &F,
) -> Result<(), String> {
    let room_id = event.room_id.clone();
    let mut rounds = 0u32;
    let mut limit = INITIAL_GAPFILL_LIMIT;

    loop {
        match registry.apply_pdu(&room_id, event.clone()).await {
            // (1) Success: closure to create resolved (or idempotent no-op).
            Ok(()) => return Ok(()),
            Err(RoomActorError::Apply(e)) if e.is_retryable() => {
                // (3) Safety bound.
                if rounds >= MAX_GAPFILL_ROUNDS {
                    return Err(format!("gap-fill exhausted after {rounds} rounds: {e}"));
                }
                rounds += 1;

                let earliest = known_boundary(store, &room_id).await;
                let want = vec![event.event_id.clone()];
                let fetched = fetcher
                    .fetch(origin, &room_id, &want, &earliest, limit)
                    .await;

                // (2) No progress: the peer can't close the gap.
                if fetched.is_empty() {
                    return Err(format!("missing ancestry, gap unfillable: {e}"));
                }

                // Persist fetched ancestry as historical context so the next
                // apply's auth-chain walk resolves it. (These are not
                // auth-checked here — trusted-mesh posture, same as
                // `/get_missing_events`/`/backfill`.)
                for raw in fetched {
                    if let Ok(ancestor) = from_wire(raw, Vec::new()) {
                        let _ = store.persist_historical_event(&ancestor).await;
                    }
                }
                limit = limit.saturating_mul(2);
            }
            // DROP (non-retryable CoreError: malformed / misrouted), or we
            // simply don't have the room — neither is fillable here.
            Err(RoomActorError::Apply(e)) => return Err(e.to_string()),
            Err(RoomActorError::UnknownRoom) => return Err("unknown room".to_owned()),
            Err(other) => return Err(other.to_string()),
        }
    }
}

/// The events we already have, used as the gap-fill walk boundary
/// (`earliest_events`). Best-effort: the room's timeline forward extremities,
/// or empty if the room is unknown / the lookup faults.
async fn known_boundary(
    store: &neutrino_store_sqlite::SqliteStore,
    room_id: &RoomId,
) -> Vec<OwnedEventId> {
    match store.forward_extremities(room_id).await {
        Ok(Some((timeline, _state))) => timeline.into_iter().collect(),
        _ => Vec::new(),
    }
}

/// Topologically sort a batch so a PDU is applied after any of its parents that
/// arrived in the same transaction. Edges are intra-batch `prev_events` ∪
/// `prev_state_events`; events whose parents are outside the batch (already in
/// the store, or missing) sort first. Kahn's algorithm; any residual cycle
/// (impossible in a valid DAG) appends the remainder in arrival order.
fn toposort(events: Vec<Event>) -> Vec<Event> {
    let ids: HashSet<OwnedEventId> = events.iter().map(|e| e.event_id.clone()).collect();

    // index → in-batch parent ids; and a child-list for propagation.
    let mut indegree: Vec<usize> = Vec::with_capacity(events.len());
    let mut children: HashMap<OwnedEventId, Vec<usize>> = HashMap::new();
    for event in &events {
        let parents: BTreeSet<&OwnedEventId> = event
            .prev_events
            .iter()
            .chain(event.prev_state_events.iter())
            .filter(|p| ids.contains(*p))
            .collect();
        indegree.push(parents.len());
        let idx = indegree.len() - 1;
        for p in parents {
            children.entry(p.clone()).or_default().push(idx);
        }
    }

    let mut ready: Vec<usize> = (0..events.len()).filter(|&i| indegree[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(events.len());
    let mut emitted = vec![false; events.len()];
    while let Some(idx) = ready.pop() {
        order.push(idx);
        emitted[idx] = true;
        if let Some(kids) = children.get(&events[idx].event_id) {
            for &k in kids {
                indegree[k] -= 1;
                if indegree[k] == 0 {
                    ready.push(k);
                }
            }
        }
    }
    // Residual cycle guard: append anything left in arrival order.
    for (i, done) in emitted.iter().enumerate() {
        if !done {
            order.push(i);
        }
    }

    // Reorder by `order`. `Option::take` lets us move each event out exactly
    // once without cloning.
    let mut slots: Vec<Option<Event>> = events.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| slots[i].take()).collect()
}

/// Fetches missing ancestry from a peer to close a state-DAG gap.
///
/// The real impl (reqwest `POST origin/_matrix/federation/v1/get_missing_events`
/// with `state_dag: true`) lands with the outbound-federation work; until then
/// [`NoFetcher`] is wired in and every retryable PDU resolves to "no progress".
#[async_trait::async_trait]
pub(crate) trait MissingEventsFetcher: Send + Sync {
    /// Walk back from `latest` (stopping at `earliest`) up to `limit` events,
    /// returning opaque PDU bytes oldest-first. An empty result means the peer
    /// gave us nothing new — the caller treats that as an unfillable gap.
    async fn fetch(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        earliest: &[OwnedEventId],
        limit: u32,
    ) -> Vec<Box<RawJsonValue>>;
}

/// No-op fetcher: never fills a gap. The wired impl until the outbound HTTP
/// client lands.
pub(crate) struct NoFetcher;

#[async_trait::async_trait]
impl MissingEventsFetcher for NoFetcher {
    async fn fetch(
        &self,
        _origin: &ServerName,
        _room_id: &RoomId,
        _latest: &[OwnedEventId],
        _earliest: &[OwnedEventId],
        _limit: u32,
    ) -> Vec<Box<RawJsonValue>> {
        Vec::new()
    }
}
