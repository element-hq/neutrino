//! `PUT /_matrix/federation/v1/send/{txnId}` — inbound federation transaction.
//!
//! A transaction is an envelope of up to 50 PDUs (plus EDUs, which this server
//! stubs out — they are deserialized for shape validation and dropped). Each
//! PDU is a fully-formed v12 event; we parse it via
//! [`neutrino_event::event_builder::from_wire`] (which derives the event_id from the
//! reference hash, verifies/redacts on content-hash mismatch, and runs the
//! format + semantic validators).
//!
//! ## Stage-then-async
//!
//! The handler does **not** integrate PDUs synchronously. It durably **stages**
//! each parsed PDU into the pre-auth `staged_events` table (keyed by the
//! event_id it just computed) and returns 200 immediately. The background
//! worker ([`neutrino_engine::worker`]) toposorts, auth-checks, gap-fills, and
//! persists each room's staged PDUs off the request path. This keeps the
//! response off the auth + peer-backfill round-trips, and means a PDU is
//! durably accepted before it is acknowledged — `RoomCore`'s persisted-check
//! makes the eventual (re-)application idempotent, so the handler's job is
//! *durable accept*, not full processing.
//!
//! The per-PDU result map is therefore optimistic: a successfully-staged PDU
//! gets `{}` (the spec's `error` field is optional and senders ignore it). A
//! PDU dropped because its room is at the staging cap carries an `error`.
//!
//! ## Trust model
//!
//! Requires an `X-Matrix` header (network-attested origin — see
//! [`crate::federation::auth`]). Signatures, on a signed deployment, are NOT
//! checked here: the inbound worker re-admits every staged PDU under the
//! deployment policy and is the sole authority on the staged→applied path, so
//! ingress parses on faith and lets the worker drop any bad-signature row.
//! The transaction's `origin` field is cross-checked against the header origin
//! (rejected on mismatch), then used for txn deduplication and as the worker's
//! gap-fill fetch target.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use neutrino_store::{FederationInbox, StagingStore};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;
use tracing::warn;

use crate::federation::{FedError, auth};
use crate::{AppState, lock_app};
use neutrino_engine::{ForwardExtremities, reconcile};

/// Inbound federation transaction body.
///
/// Hand-rolled rather than using `ruma::api::federation` — that crate's
/// `federation-api` feature on our pinned ruma version depends on an
/// unpublished sub-crate. Mirrors the wire-verbatim approach already used by
/// `backfill.rs` / `get_missing_events.rs`: PDUs are opaque `RawValue`s.
#[derive(Deserialize)]
pub(crate) struct TransactionBody {
    /// The sending server's name. Required to equal the network-attested
    /// `X-Matrix` origin (rejected on mismatch), then used for transaction
    /// deduplication + as the worker's gap-fill fetch target (recorded as the
    /// staged row's `origin`).
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
    /// Anti-entropy: the sender's per-room forward extremities. Optional; a peer
    /// that has not implemented forward-extremity reconciliation omits it and the
    /// transaction behaves exactly as before. For each advertised room we hold,
    /// any head we are missing is fetched + reconciled (off the response path).
    #[serde(default)]
    forward_extremities: BTreeMap<OwnedRoomId, ForwardExtremities>,
}

/// Per-PDU processing result. An empty object is success; `error` carries a
/// human-readable reason on failure (spec `PduProcessingResult`).
#[derive(Serialize, Default)]
struct PduResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Transaction response body: `{ "pdus": { "$id": {} | { "error": … } } }`,
/// plus the anti-entropy `forward_extremities` advertisement (this server's
/// per-room heads, so the *sender* can reconcile against us from the response —
/// a single transaction reconciles both directions). Omitted when empty, so a
/// peer that does not implement reconciliation sees an unchanged response shape.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    pdus: BTreeMap<String, PduResult>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    forward_extremities: BTreeMap<OwnedRoomId, ForwardExtremities>,
}

/// Federation `/send/{txnId}` handler. Stages the transaction's PDUs and pokes
/// the background worker; integration happens asynchronously.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path(txn_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<serde_json::Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ResponseBody>, FedError> {
    // Route JSON-edge failures (bad content-type, invalid JSON, shape mismatch)
    // through 400 M_INVALID_PARAM, matching the other federation handlers.
    let body_value = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;
    let body: TransactionBody = serde_json::from_value(body_value)
        .map_err(|_| FedError::BadRequest("body shape does not match the spec"))?;

    if body.pdus.len() > neutrino_engine::MAX_PDUS_PER_TXN {
        return Err(FedError::BadRequest("transaction exceeds 50 PDUs"));
    }

    let (store, worker_poke, fetcher, security, our_name) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.worker_poke.clone(),
            app.fetcher.clone(),
            app.security.clone(),
            app.config.server_name.clone(),
        )
    };

    // Authenticate the sender via its `X-Matrix` header. The header origin is
    // network-attested; `body.origin` is self-asserted. Require them to agree so
    // the authenticated identity governs txn dedup, the staged gap-fill target,
    // and reconciliation — a peer can't claim one origin in the envelope and
    // another at the network layer.
    let origin = auth::authenticated_origin(&headers, &our_name)?;
    if origin != body.origin {
        return Err(FedError::Unauthorized(
            "X-Matrix origin does not match the transaction origin",
        ));
    }

    // Cheap whole-transaction dedup: a re-sent transaction we've already fully
    // staged is acknowledged without re-staging. This is a read-only *check* —
    // the matching *record* happens only after staging succeeds (below), so a
    // mid-stage fault never marks the txn done and a resend re-stages.
    if store
        .federation_txn_seen(&body.origin, &txn_id)
        .await
        .map_err(FedError::Storage)?
    {
        // A duplicate (already-staged) transaction: ack without re-staging. We
        // skip the anti-entropy advertisement here to keep the dedup path cheap —
        // reconciliation rides organic (non-duplicate) traffic, of which a healthy
        // mesh has plenty.
        return Ok(Json(ResponseBody {
            pdus: BTreeMap::new(),
            forward_extremities: BTreeMap::new(),
        }));
    }

    // Parse + dedup by event_id, then durably stage each PDU. A PDU that fails
    // `from_wire` is unkeyable (no derivable id) and cannot appear in the
    // result map — silently dropped, matching Synapse's log-and-skip. The
    // worker does the toposort/auth/gap-fill, so the handler does not order or
    // apply anything here.
    let mut pdus = BTreeMap::new();
    let mut seen: HashSet<OwnedEventId> = HashSet::new();
    let mut touched: BTreeSet<OwnedRoomId> = BTreeSet::new();
    // Event ids this transaction proves its sender already holds, so the response
    // advertisement below can leave them off the wire (see
    // `reconcile::strip_known`). Accumulated as we parse: no second pass, and no
    // re-derivation of the parent lists.
    let mut sender_holds: BTreeSet<OwnedEventId> = BTreeSet::new();
    // Stays true only if every keyable PDU was durably staged. A storage fault
    // on any one keeps the txn *unrecorded* so the peer's resend re-stages it
    // (never-lose). An unkeyable/malformed PDU is an intentional drop, not a
    // failure — it would fail identically on every resend, so it must not block
    // recording.
    let mut all_staged = true;
    for raw in body.pdus {
        // Parse only — signatures are NOT verified here. The inbound worker
        // (`parse_or_drop` → `apply_pdu`) is the sole authority on the
        // staged→applied path and re-admits every row under the deployment
        // policy, so a bad-signature PDU that reaches staging is dropped there
        // before it can apply; verifying at ingress too would just double the
        // ed25519 work on the happy path (every legitimate PDU is validly
        // signed). `admit_on_faith` runs the parse without the signature check
        // (content-hash verify/redact + semantic classification still run).
        // Drop-class PDUs (`Err`) are unkeyable and never enter the system;
        // `Wire::Rejected` ones are staged like any other — the worker persists
        // them rejected (the cascade terminator).
        let event = match neutrino_event::event_builder::from_wire(raw, Vec::new())
            .map(|uw| uw.admit_on_faith())
        {
            Ok(neutrino_event::Wire::Valid(ev)) => ev,
            Ok(neutrino_event::Wire::Rejected(ev, defect)) => {
                tracing::warn!(event_id = %ev.event_id, %defect, "/send: staging malformed PDU as rejected");
                ev
            }
            Err(_) => continue,
        };
        // The sender holds this event (it sent it) and its state-DAG parents (it
        // could not have applied the event without grounding them). Its *timeline*
        // parents only if it authored the event: a relayed PDU may reference
        // `prev_events` the relaying server never fetched and does not hold, and a
        // missing timeline parent is never gap-filled.
        sender_holds.insert(event.event_id.clone());
        sender_holds.extend(event.prev_state_events.iter().cloned());
        if event.sender.server_name() == &*body.origin {
            sender_holds.extend(event.prev_events.iter().cloned());
        }
        if !seen.insert(event.event_id.clone()) {
            continue;
        }
        let id = event.event_id.to_string();
        let result = match store
            .stage_pdu(&body.origin, &event.room_id, &event.event_id, &event.raw)
            .await
        {
            // Staged (newly, or already present from an earlier delivery) — in
            // both cases it is pending in the room, so poke the worker.
            Ok(_) => {
                touched.insert(event.room_id.clone());
                PduResult::default()
            }
            // A storage write fault is a server-side problem; surface it on this
            // PDU, keep staging the rest, and leave the txn unrecorded.
            Err(e) => {
                warn!(event_id = %id, error = %e, "staging PDU failed");
                all_staged = false;
                PduResult {
                    error: Some(e.to_string()),
                }
            }
        };
        pdus.insert(id, result);
    }

    // Record the transaction as processed only now that its PDUs are durably
    // staged (and only if all of them are) — the never-lose ordering.
    if all_staged {
        store
            .record_federation_txn(&body.origin, &txn_id)
            .await
            .map_err(FedError::Storage)?;
    }

    // Poke the worker once per touched room, *after* the rows are committed.
    // Best-effort: a full buffer means the worker already has pending pokes, and
    // its next drain (or startup enumeration) still picks the room up.
    for room in &touched {
        let _ = worker_poke.try_send(room.clone());
    }

    // Anti-entropy. Advertise our own forward extremities back to the sender (so
    // it can reconcile against us from this response), for every room it
    // advertised plus every room this transaction touched — minus the heads the
    // transaction itself proves the sender already holds, which is commonly all of
    // them (our heads are still the pre-batch ones, i.e. exactly what its PDUs
    // reference, since staging is asynchronous). An empty-`pdus` advertisement
    // strips nothing, so a peer asking to be reconciled always gets our heads.
    let advertised = body.forward_extremities;
    let mut resp_rooms: BTreeSet<OwnedRoomId> = touched;
    resp_rooms.extend(advertised.keys().cloned());
    let mut ours = BTreeMap::new();
    for room in &resp_rooms {
        let fes = reconcile::local_extremities(&*store, room).await;
        if !fes.is_empty() {
            ours.insert(room.clone(), fes);
        }
    }
    let forward_extremities = reconcile::strip_known(&ours, &sender_holds);

    // Reconcile our view against the heads the sender advertised: fire-and-forget
    // so the 200 isn't blocked on peer round-trips. Each task fetches any
    // advertised head we lack and stages it for the worker.
    for (room, heads) in advertised {
        let store = store.clone();
        let fetcher = fetcher.clone();
        let security = security.clone();
        let worker_poke = worker_poke.clone();
        let origin = body.origin.clone();
        tokio::spawn(async move {
            reconcile::reconcile_room(
                &*store,
                &*fetcher,
                &security,
                &worker_poke,
                &origin,
                &room,
                &heads,
            )
            .await;
        });
    }

    Ok(Json(ResponseBody {
        pdus,
        forward_extremities,
    }))
}
