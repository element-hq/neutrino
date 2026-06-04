//! `PUT /_matrix/federation/v1/send/{txnId}` — inbound federation transaction.
//!
//! A transaction is an envelope of up to 50 PDUs (plus EDUs, which this server
//! stubs out — they are deserialized for shape validation and dropped). Each
//! PDU is a fully-formed v12 event; we parse it via
//! [`neutrino_state::event_id::from_wire`] (which derives the event_id from the
//! reference hash, verifies/redacts on content-hash mismatch, and runs the
//! format + semantic validators).
//!
//! ## Stage-then-async
//!
//! The handler does **not** integrate PDUs synchronously. It durably **stages**
//! each parsed PDU into the pre-auth `staged_events` table (keyed by the
//! event_id it just computed) and returns 200 immediately. The background
//! worker ([`crate::federation::worker`]) toposorts, auth-checks, gap-fills, and
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
//! No X-Matrix auth and no signature verification — the same trusted-mesh
//! stance as `/get_missing_events` and `/backfill`. The transaction's `origin`
//! field is taken at face value for txn deduplication and as the worker's
//! gap-fill fetch target.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_state::event_id::from_wire;
use neutrino_store::{FederationInbox, StagingStore};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;
use tracing::warn;

use crate::federation::FedError;
use crate::{AppState, lock_app};

/// Inbound federation transaction body.
///
/// Hand-rolled rather than using `ruma::api::federation` — that crate's
/// `federation-api` feature on our pinned ruma version depends on an
/// unpublished sub-crate. Mirrors the wire-verbatim approach already used by
/// `backfill.rs` / `get_missing_events.rs`: PDUs are opaque `RawValue`s.
#[derive(Deserialize)]
pub(crate) struct TransactionBody {
    /// The sending server's name. Trusted at face value (no X-Matrix auth) and
    /// used for transaction deduplication + as the worker's gap-fill fetch
    /// target (recorded as the staged row's `origin`).
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

/// Federation `/send/{txnId}` handler. Stages the transaction's PDUs and pokes
/// the background worker; integration happens asynchronously.
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

    let (store, worker_poke) = {
        let app = lock_app(&state);
        (app.store.clone(), app.worker_poke.clone())
    };

    // Cheap whole-transaction dedup: a re-sent transaction whose PDUs were
    // already staged (or staged-then-processed) is acknowledged without
    // re-staging. Staging and application are both idempotent, so this is an
    // optimization, not a correctness gate — its main value is avoiding the
    // re-staging of PDUs the worker has already processed and unstaged.
    if store
        .record_federation_txn(&body.origin, &txn_id)
        .await
        .map_err(FedError::Storage)?
    {
        return Ok(Json(ResponseBody {
            pdus: BTreeMap::new(),
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
    for raw in body.pdus {
        let Ok(event) = from_wire(raw, Vec::new()) else {
            continue;
        };
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
            // PDU and carry on staging the rest of the batch.
            Err(e) => {
                warn!(event_id = %id, error = %e, "staging PDU failed");
                PduResult {
                    error: Some(e.to_string()),
                }
            }
        };
        pdus.insert(id, result);
    }

    // Poke the worker once per touched room, *after* the rows are committed.
    // Best-effort: a full buffer means the worker already has pending pokes, and
    // its next drain (or startup enumeration) still picks the room up.
    for room in touched {
        let _ = worker_poke.try_send(room);
    }

    Ok(Json(ResponseBody { pdus }))
}
