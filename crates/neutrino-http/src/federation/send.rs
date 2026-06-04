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
//! when an event's `prev_state_events` ancestry (the auth-relevant state DAG,
//! MSC4242) doesn't reach `m.room.create` in our store. We must *authorise*
//! every PDU — concurrency reorders operations, so even a trusted peer's event
//! can be invalid by DAG position — and an un-vetted event must never get a
//! stream position or surface in any read / state-res path. So fetched ancestry
//! is parked in a pre-auth staging cache rather than persisted as history, then
//! promoted through the actor (which auths it) only once it is grounded. See
//! [`apply_with_gapfill`] for the loop:
//!
//! 1. **Fetch** the missing frontier into staging, walking `events ∪
//!    staged_events` over `prev_state_events` to ask the peer (`state_dag: true`)
//!    only for what we still lack ([`fill_state_ancestry`]).
//! 2. **Promote** the now-grounded staged subgraph through the actor
//!    ([`promote_staged_ancestry`]) — auth + stream positions happen here.
//! 3. **Re-apply** the PDU against its committed ancestry.
//!
//! The fetch is behind a [`MissingEventsFetcher`] trait
//! ([`crate::federation::client::ReqwestFetcher`] in production; a stub in
//! tests), held on `AppState`. A transaction whose PDUs arrive with complete
//! ancestry (the common in-order case) skips all of this on the fast path.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_common::Event;
use neutrino_state::event_id::from_wire;
use neutrino_store::{FederationInbox, RoomStore, StagingStore};
use ruma::{EventId, OwnedEventId, OwnedServerName, RoomId, ServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::FedError;
use crate::federation::client::FederationClientError;
use crate::room_actor::{RoomActorError, RoomRegistry};
use crate::{AppState, lock_app};

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

    if body.pdus.len() > super::MAX_PDUS_PER_TXN {
        return Err(FedError::BadRequest("transaction exceeds 50 PDUs"));
    }

    let (store, registry, fetcher) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.room_registry.clone(),
            app.fetcher.clone(),
        )
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
            match apply_with_gapfill(&registry, &store, &body.origin, event, &*fetcher).await {
                Ok(()) => PduResult::default(),
                Err(error) => PduResult { error: Some(error) },
            };
        pdus.insert(id, result);
    }

    Ok(Json(ResponseBody { pdus }))
}

/// Integrate one PDU, gap-filling missing *state-DAG* ancestry on a retryable
/// verdict.
///
/// `apply_pdu` returns retryable when `event`'s `prev_state_events` ancestry
/// (the auth-relevant DAG, MSC4242) doesn't reach `m.room.create` in our store.
/// We fill it through a pre-auth staging cache rather than persisting ancestry
/// as committed history: events fetched from a peer are NOT yet authorised
/// (concurrency reorders operations, so a trusted peer's event can still be
/// invalid by DAG position), and an un-vetted event must never get a stream
/// position or surface in any read / state-res path. So:
///
/// 1. **Fetch** the missing ancestry into staging ([`fill_state_ancestry`]):
///    walk `events ∪ staged_events` via `prev_state_events` to find the still-
///    missing frontier, ask the peer (`state_dag: true`) only for that, repeat
///    until grounded. The peer is told to skip what we've staged so each round
///    requests only the new frontier.
/// 2. **Promote** ([`promote_staged_ancestry`]): once grounded, apply the staged
///    subgraph through the per-room actor oldest-first — auth, state-res, and
///    stream positions happen here — then unstage it.
/// 3. **Re-apply** `event` against its now-committed ancestry.
///
/// Returns `Ok(())` for any terminal *integration* outcome of `event` —
/// accepted, soft-failed, rejected, or idempotent re-delivery (`apply_pdu`
/// persists rejects per federation policy). Returns `Err(reason)` when the PDU
/// could not be evaluated: an unfillable gap, a peer fetch failure, an unknown
/// room, a malformed/misrouted event, or a storage fault. Staged ancestry from
/// a failed round is left durable — a later inbound retry resumes from it.
async fn apply_with_gapfill<F: MissingEventsFetcher + ?Sized>(
    registry: &RoomRegistry,
    store: &neutrino_store_sqlite::SqliteStore,
    origin: &ServerName,
    event: Event,
    fetcher: &F,
) -> Result<(), String> {
    let room_id = event.room_id.clone();

    // Fast path: ancestry already present (the common in-order case).
    match registry.apply_pdu(&room_id, event.clone()).await {
        Ok(()) => return Ok(()),
        Err(RoomActorError::Apply(e)) if e.is_retryable() => { /* gap-fill below */ }
        Err(RoomActorError::Apply(e)) => return Err(e.to_string()),
        Err(RoomActorError::UnknownRoom) => return Err("unknown room".to_owned()),
        Err(other) => return Err(other.to_string()),
    }

    fill_state_ancestry(store, origin, &event, fetcher).await?;
    promote_staged_ancestry(registry, store, &event).await?;

    // Ancestry committed: a fresh apply now evaluates `event` for real.
    match registry.apply_pdu(&room_id, event).await {
        Ok(()) => Ok(()),
        Err(RoomActorError::Apply(e)) => Err(e.to_string()),
        Err(RoomActorError::UnknownRoom) => Err("unknown room".to_owned()),
        Err(other) => Err(other.to_string()),
    }
}

/// Fetch `event`'s missing state-DAG ancestry into the staging cache until it
/// is grounded (every `prev_state_events` path reaches an event we hold).
///
/// Each round recomputes the gap over `events ∪ staged_events`; an empty
/// `missing` frontier means done. Otherwise we ask the peer, passing
/// `latest = event + the staged frontier` (so the peer walks down *through*
/// what we've cached without re-sending it) and `earliest = our state-DAG
/// forward extremities` (the committed bottom boundary). `Ok(empty)` ⇒ the peer
/// has nothing new ⇒ unfillable; `Err` ⇒ peer unreachable/erred. Both are
/// terminal for this attempt (no in-request retry — inbound `/send` stays
/// synchronous; a federation resend re-enters here and resumes from staging).
async fn fill_state_ancestry<F: MissingEventsFetcher + ?Sized>(
    store: &neutrino_store_sqlite::SqliteStore,
    origin: &ServerName,
    event: &Event,
    fetcher: &F,
) -> Result<(), String> {
    let room_id = &event.room_id;
    let heads: Vec<&EventId> = event.prev_state_events.iter().map(|e| e.as_ref()).collect();
    let earliest = state_dag_boundary(store, room_id).await;
    let mut rounds = 0u32;
    let mut limit = INITIAL_GAPFILL_LIMIT;

    loop {
        let gap = store
            .ancestry_gap(room_id, &heads)
            .await
            .map_err(|e| e.to_string())?;
        if gap.missing.is_empty() {
            return Ok(());
        }
        if rounds >= MAX_GAPFILL_ROUNDS {
            return Err(format!(
                "gap-fill exhausted after {rounds} rounds; {} ancestry event(s) still missing",
                gap.missing.len()
            ));
        }
        rounds += 1;

        // `latest` = the event plus the staged boundary. The peer excludes
        // these from its result but walks *through* them, so it returns only
        // the frontier below our cache — the "ask for 1-4, not 5-99" property.
        let mut latest = Vec::with_capacity(gap.staged.len() + 1);
        latest.push(event.event_id.clone());
        latest.extend(gap.staged);

        match fetcher
            .fetch(origin, room_id, &latest, &earliest, limit)
            .await
        {
            Ok(fetched) if fetched.is_empty() => {
                return Err("missing ancestry, gap unfillable: peer returned no events".to_owned());
            }
            Ok(fetched) => {
                // Stage under each event's *computed* id (`from_wire` derives it
                // from the reference hash and yields canonical bytes, so id ↔
                // bytes round-trip). An unkeyable PDU is dropped.
                for raw in fetched {
                    if let Ok(ancestor) = from_wire(raw, Vec::new()) {
                        store
                            .stage_event(&ancestor.room_id, &ancestor.event_id, &ancestor.raw)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
                limit = limit.saturating_mul(2);
            }
            Err(e) => return Err(format!("peer fetch failed: {e}")),
        }
    }
}

/// Promote `event`'s now-grounded staged ancestry through the actor, then drop
/// it from staging.
///
/// The staged subgraph is applied oldest-first (toposorted by
/// `prev_events ∪ prev_state_events`) so each event's ancestry is committed
/// before it. `apply_pdu` is the authority — it auths, resolves, mints the
/// stream position, and persists (accept *or* reject, per federation policy);
/// each event is unstaged only after it commits. A retryable verdict here means
/// the ancestry is still incomplete (a deeper gap or concurrent change) — bail
/// and leave the remainder staged for a later retry.
async fn promote_staged_ancestry(
    registry: &RoomRegistry,
    store: &neutrino_store_sqlite::SqliteStore,
    event: &Event,
) -> Result<(), String> {
    let room_id = &event.room_id;
    let heads: Vec<&EventId> = event.prev_state_events.iter().map(|e| e.as_ref()).collect();
    let gap = store
        .ancestry_gap(room_id, &heads)
        .await
        .map_err(|e| e.to_string())?;
    if gap.staged.is_empty() {
        // Nothing was fetched — `event`'s ancestry was already committed (the
        // retryable verdict was a transient lookup fault, not a real gap).
        return Ok(());
    }

    let staged_refs: Vec<&EventId> = gap.staged.iter().map(|e| e.as_ref()).collect();
    let raws = store
        .staged_raw(&staged_refs)
        .await
        .map_err(|e| e.to_string())?;
    let ancestors: Vec<Event> = raws
        .into_iter()
        .filter_map(|raw| from_wire(raw, Vec::new()).ok())
        .collect();

    for ancestor in toposort(ancestors) {
        let id = ancestor.event_id.clone();
        match registry.apply_pdu(room_id, ancestor).await {
            Ok(()) => {
                store
                    .unstage_events(&[id.as_ref()])
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Err(RoomActorError::Apply(e)) if e.is_retryable() => {
                return Err(format!("promotion stalled, ancestry incomplete: {e}"));
            }
            Err(RoomActorError::Apply(e)) => return Err(e.to_string()),
            Err(RoomActorError::UnknownRoom) => return Err("unknown room".to_owned()),
            Err(other) => return Err(other.to_string()),
        }
    }
    Ok(())
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
