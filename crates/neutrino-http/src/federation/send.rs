//! `PUT /_matrix/federation/v1/send/{txnId}` — inbound federation transaction.
//!
//! A transaction is an envelope of up to 50 PDUs (plus EDUs, which this server
//! stubs out — they are deserialized for shape validation and dropped). Each
//! PDU is a fully-formed v12 event; we parse it via
//! [`neutrino_event::event_builder::from_wire`] (which derives the event_id
//! under the room's version, verifies/redacts on content-hash mismatch, and
//! runs the format + semantic validators).
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
//! The header origin is what drives txn deduplication and the worker's gap-fill
//! fetch target. The transaction's own `origin` field is optional (our sender
//! omits it); when a peer does send one it is cross-checked against the header
//! origin and a mismatch is rejected.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use neutrino_event::{EventPolicy, Wire};
use neutrino_store::{FederationInbox, OobMembershipStore, StagingStore, StorageError};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName};
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
    /// The sending server's name, as self-asserted by the envelope. Optional:
    /// our own sender omits it (redundant with the network-attested `X-Matrix`
    /// origin — see [`crate::federation::client`] and
    /// <https://github.com/matrix-org/matrix-spec/issues/374>), while a real
    /// Matrix peer still sends it. When present it MUST equal the header origin
    /// (rejected on mismatch); the header origin is what actually drives txn
    /// dedup, staging and the gap-fill fetch target either way.
    #[serde(default)]
    origin: Option<OwnedServerName>,
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

/// Out-of-band membership update: an `m.room.member` leave/ban for a local
/// user we hold an out-of-band stub for (the inviter rescinding an invite). It
/// cannot be applied — there is no room state here — so it is named under the
/// stub's room version, admitted under the deployment policy, and stored over
/// the stub for sync to surface. `Ok(None)` = not such a PDU (no stub for the
/// target, another type/membership, or a sender the origin does not own) —
/// stage it like any other. `Ok(Some(id))` = consumed, whether stored or
/// dropped (malformed, or not naming the stub it supersedes).
async fn apply_oob_membership(
    store: &impl OobMembershipStore,
    policy: &EventPolicy,
    origin: &ServerName,
    our_name: &str,
    room_id: &RoomId,
    raw: &RawJsonValue,
) -> Result<Option<OwnedEventId>, StorageError> {
    #[derive(Deserialize)]
    struct Member {
        r#type: String,
        state_key: Option<String>,
        content: Content,
    }
    #[derive(Deserialize)]
    struct Content {
        membership: Option<String>,
    }
    let Ok(member) = serde_json::from_str::<Member>(raw.get()) else {
        return Ok(None);
    };
    if member.r#type != "m.room.member"
        || !matches!(member.content.membership.as_deref(), Some("leave" | "ban"))
    {
        return Ok(None);
    }
    let Some(user) = member
        .state_key
        .as_deref()
        .and_then(|k| k.parse::<OwnedUserId>().ok())
        .filter(|u| u.server_name().as_str() == our_name)
    else {
        return Ok(None);
    };
    let Some(stub) = store.get_oob_membership(room_id, &user).await? else {
        return Ok(None);
    };
    let Some(version) = policy.versions.get(&stub.room_version).cloned() else {
        warn!(%room_id, version = %stub.room_version, "/send: out-of-band stub names a version this build does not speak");
        return Ok(None);
    };
    let event = match policy.admit_wire(raw.to_owned(), &version).await {
        Ok(Wire::Valid(ev)) => ev,
        Ok(Wire::Rejected(ev, defect)) => {
            warn!(event_id = %ev.event_id, %defect, "/send: dropping malformed out-of-band membership");
            return Ok(Some(ev.event_id));
        }
        Err(_) => return Ok(None),
    };
    // With no room state to auth against, the network-attested origin is the
    // only anchor: the rescinding server must be the one delivering it.
    if event.sender.server_name() != origin || event.room_id != room_id {
        return Ok(None);
    }
    // MSC4242 out-of-band events: the update MUST name the membership it
    // supersedes in `prev_state_events` — the only tie between this event and
    // the stub, since we cannot compute its auth events. Without it a stale
    // rescission could be replayed to cancel a newer invite. Consumed, not
    // staged: it is addressed to a stub, so there is nothing else to do with it.
    if !event.prev_state_events.contains(&stub.event.event_id) {
        warn!(event_id = %event.event_id, stub = %stub.event.event_id, "/send: dropping out-of-band membership that does not name the membership it supersedes");
        return Ok(Some(event.event_id));
    }
    store
        .put_oob_membership(room_id, &user, &event, &stub.room_version)
        .await?;
    Ok(Some(event.event_id))
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

    let (store, worker_poke, fetcher, policy, our_name) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.worker_poke.clone(),
            app.fetcher.clone(),
            app.policy.clone(),
            app.config.server_name.clone(),
        )
    };

    // Authenticate the sender via its `X-Matrix` header. The header origin is
    // network-attested and is the *only* identity used below (txn dedup, the
    // staged gap-fill target, reconciliation). A self-asserted `body.origin`, if
    // the peer sent one, must agree — a peer can't claim one origin in the
    // envelope and another at the network layer.
    let origin = auth::authenticated_origin(&headers, &our_name)?;
    if body.origin.is_some_and(|claimed| claimed != origin) {
        return Err(FedError::Unauthorized(
            "X-Matrix origin does not match the transaction origin",
        ));
    }

    // Cheap whole-transaction dedup: a re-sent transaction we've already fully
    // staged is acknowledged without re-staging. This is a read-only *check* —
    // the matching *record* happens only after staging succeeds (below), so a
    // mid-stage fault never marks the txn done and a resend re-stages.
    if store
        .federation_txn_seen(&origin, &txn_id)
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
    // One version lookup per distinct room in the transaction, not per PDU: a
    // transaction commonly carries several events for one room. Only *decided*
    // outcomes are cached (a resolved version, or `None` for a terminal refusal
    // that no retry can change); a storage fault is deliberately not cached, so
    // it neither poisons the rest of the transaction nor is mistaken for a
    // refusal.
    let mut versions: HashMap<Option<OwnedRoomId>, Option<Arc<neutrino_event::RoomVersion>>> =
        HashMap::new();
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
        // A PDU can only be named under its room's version, so resolve that
        // first: the room it claims (the common case) or, for a create, the
        // version the create declares.
        //
        // A *terminal* refusal (we are not in that room, or we do not speak its
        // version) is a drop, exactly as an unparseable PDU is dropped — guessing
        // a version would invent a different event. A *storage fault* is not:
        // the version is on disk and a resend can succeed, so the PDU is left
        // unstaged AND the transaction is left unrecorded (`all_staged = false`),
        // which is what makes the peer resend it. Dropping on a fault would lose
        // the event for good, since the txn-dedup would swallow the resend.
        let keys = neutrino_event::room_version_keys(&raw);
        // A leave/ban for a local user we hold an out-of-band stub for (the
        // inviter rescinding) is stored over the stub, not staged: there is no
        // room state here to apply it against.
        if let Some(room_id) = &keys.room_id {
            match apply_oob_membership(&*store, &policy, &origin, &our_name, room_id, &raw).await {
                Ok(Some(event_id)) => {
                    pdus.insert(event_id.to_string(), PduResult::default());
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(%room_id, error = %e, "/send: out-of-band membership store fault; leaving the transaction unrecorded so the peer resends");
                    all_staged = false;
                    continue;
                }
            }
        }
        let cached = versions.get(&keys.room_id).cloned();
        let version = match cached {
            Some(decided) => decided,
            None => match neutrino_engine::room_version_for_wire(&*store, &policy.versions, &raw)
                .await
            {
                Ok(v) => {
                    versions.insert(keys.room_id.clone(), Some(v.clone()));
                    Some(v)
                }
                Err(e) if e.is_retryable() => {
                    warn!(room_id = ?keys.room_id, error = %e, "/send: cannot name this room's events; leaving the transaction unrecorded so the peer resends");
                    all_staged = false;
                    continue;
                }
                Err(e) => {
                    warn!(room_id = ?keys.room_id, error = %e, "/send: dropping PDU we can never name");
                    versions.insert(keys.room_id.clone(), None);
                    None
                }
            },
        };
        let Some(version) = version else {
            continue;
        };
        let event = match neutrino_event::event_builder::from_wire(raw, Vec::new(), &version)
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
        if event.sender.server_name() == &*origin {
            sender_holds.extend(event.prev_events.iter().cloned());
        }
        if !seen.insert(event.event_id.clone()) {
            continue;
        }
        let id = event.event_id.to_string();
        // `None`: a `/send` PDU is staged in its own right — a gap-fill root
        // (and, if we only held it as fetched ancestry so far, this promotes it).
        let result = match store
            .stage_pdu(&origin, &event.room_id, &event.event_id, &event.raw, None)
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
            .record_federation_txn(&origin, &txn_id)
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
        let policy = policy.clone();
        let worker_poke = worker_poke.clone();
        let origin = origin.clone();
        tokio::spawn(async move {
            reconcile::reconcile_room(
                &*store,
                &*fetcher,
                &policy,
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
