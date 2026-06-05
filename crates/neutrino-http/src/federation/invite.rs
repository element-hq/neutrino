//! `PUT /_matrix/federation/v2/invite/{roomId}/{eventId}` — inbound federated
//! invite, where **we host the invited user**.
//!
//! A remote resident server invites one of our local users to a room. The
//! request body is the **v2 envelope** `{ event, room_version, invite_room_state }`
//! (the v2 endpoint wraps the PDU; v1's bare event is not accepted). The invite
//! is **out-of-band membership**: in the common case we hold no state for the
//! room (no `m.room.create`, no auth chain), so the event cannot go through
//! `apply_pdu` (there is nothing to auth it against). Two dispositions:
//!
//! - **Room we do NOT host** (the common case): merge the envelope's
//!   `invite_room_state` into the event's `unsigned`, then store it via
//!   [`InviteStore::put_invite`]. That stripped state is what sync renders the
//!   room from.
//! - **Room we already host** (the inviting server may not realise we're
//!   resident — *not* an error, per the 2026-06-05 decision): stage the event
//!   and let the per-room worker integrate it through `apply_pdu` like any
//!   inbound PDU (auth + state-res + persist), so it becomes normal
//!   `current_state` rather than a redundant out-of-band stub.
//!
//! Response is `{ event }` — our copy of the invite event, verbatim. In a
//! signatures world this is where the resident signature would be added.
//!
//! Trusted mesh: no X-Matrix auth, no signature verification (same stance as
//! `/send` and `/send_join`). The inviting server is taken as the invite
//! event's `sender` domain — used as the staged row's gap-fill fetch target.

use axum::{
    Json,
    extract::rejection::JsonRejection,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_state::event_id::from_wire;
use neutrino_store::{InviteStore, RoomStore, StateStore};
use ruma::events::AnyStrippedStateEvent;
use ruma::serde::Raw;
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;
use serde_json::{Value, json};

use crate::federation::client::{FederationClient, FederationClientError};
use crate::federation::{FedError, stage_and_poke};
use crate::room_actor::RoomActorError;
use crate::{AppState, error_response, lock_app};

/// v2 `/invite` request envelope: the PDU plus the room version and an optional
/// stripped `invite_room_state` for the invitee to render the room. The v2
/// endpoint wraps the event in this envelope (v1 sent the bare event); we parse
/// the envelope and merge `invite_room_state` into the event's `unsigned` so the
/// sync invite path (which reads `unsigned.invite_room_state`) surfaces it.
#[derive(Deserialize)]
pub(crate) struct InviteRequestBody {
    event: Box<RawJsonValue>,
    room_version: String,
    #[serde(default)]
    invite_room_state: Vec<Box<RawJsonValue>>,
}

/// `/invite/v2` response: `{ event }` — our copy of the invite event.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    event: Box<RawJsonValue>,
}

/// Federation `/invite/v2` handler.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, event_id)): Path<(String, String)>,
    body: Result<Json<InviteRequestBody>, JsonRejection>,
) -> Result<Json<ResponseBody>, FedError> {
    let body = body
        .map_err(|_| FedError::BadRequest("body is not the v2 invite envelope"))?
        .0;
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;

    // Version negotiation: we only host v12 + MSC4242.
    if body.room_version != ROOM_VERSION_ID {
        return Err(FedError::IncompatibleRoomVersion(
            ROOM_VERSION_ID.to_owned(),
        ));
    }

    // Merge the envelope's `invite_room_state` into the event's `unsigned` (the
    // sync invite path reads it from there). `unsigned` is outside the reference
    // hash, so this leaves the computed event_id unchanged.
    let raw = merge_invite_room_state(body.event, body.invite_room_state);

    // Parse + compute the event id (also runs the format + semantic validators).
    // `auth_events` start empty — they are never on the v12 wire.
    let event =
        from_wire(raw, Vec::new()).map_err(|_| FedError::BadRequest("malformed invite event"))?;

    // Structural validation (spec §invite). No signature / sender-on-origin
    // checks (trusted mesh, no X-Matrix origin header).
    if event.event_id.as_str() != event_id {
        return Err(FedError::BadRequest(
            "event_id in path does not match the event",
        ));
    }
    if event.room_id != room_id {
        return Err(FedError::BadRequest(
            "room_id in path does not match the event",
        ));
    }
    if event.event_type != "m.room.member" {
        return Err(FedError::BadRequest("event is not an m.room.member event"));
    }
    if event.content_str("membership").as_deref() != Some("invite") {
        return Err(FedError::BadRequest("membership is not invite"));
    }
    // The invited user is the `state_key`; it MUST be one of our local users —
    // we have no business storing an invite addressed to another server's user.
    let invited: OwnedUserId = event
        .state_key
        .as_deref()
        .ok_or(FedError::BadRequest("invite is missing a state_key"))?
        .parse()
        .map_err(|_| FedError::BadRequest("state_key is not a valid user id"))?;

    let (store, worker_poke, our_server) = {
        let app = lock_app(&state);
        (
            app.store.clone(),
            app.worker_poke.clone(),
            app.config.server_name.clone(),
        )
    };

    if our_server != invited.server_name().as_str() {
        return Err(FedError::BadRequest(
            "invited user is not local to this server",
        ));
    }

    // Keep the wire bytes for the response before either path moves `event`.
    let event_raw = event.raw.clone();

    if store.room_exists(&room_id).await? {
        // We host the room: integrate the invite as a normal inbound PDU. The
        // worker auth-checks + state-resolves + persists it (gap-filling its
        // ancestry if needed), so it lands in `current_state`. Origin = the
        // invite's sender domain (the worker's gap-fill fetch target).
        let origin = event.sender.server_name().to_owned();
        stage_and_poke(
            &*store,
            &worker_poke,
            &origin,
            &room_id,
            std::slice::from_ref(&event),
        )
        .await?;
    } else {
        // Out-of-band: no room state to auth against. Store the stub so sync can
        // surface the invite from its `unsigned.invite_room_state`.
        store.put_invite(&room_id, &invited, &event).await?;
    }

    Ok(Json(ResponseBody { event: event_raw }))
}

// ── Outbound: we are the resident inviting a remote user (Milestone B3) ───────

/// Current-state types worth giving the invitee to render the room with
/// (spec `unsigned.invite_room_state`). A curated subset; the inviter's own
/// member event is added separately so the invitee sees who invited them.
const INVITE_ROOM_STATE_TYPES: &[&str] = &[
    "m.room.create",
    "m.room.join_rules",
    "m.room.canonical_alias",
    "m.room.avatar",
    "m.room.name",
    "m.room.encryption",
    "m.room.topic",
];

/// Outbound CSAPI `/invite` of a **remote** user: **federate-then-persist,
/// atomic, non-durable** (the client is the retry mechanism — no outbox). Build
/// a candidate invite off current heads (not persisted) → `PUT /invite/v2` to
/// the invitee's server → on 200, commit the returned event through
/// `apply_resident` (persist + distribute to the *other* room servers; the
/// invitee already has it from the handshake, and invites are excluded from
/// transaction broadcast) → 200. Any failure persists nothing, so 200 OK ⟺ the
/// invitee server acked AND it's persisted AND propagating.
pub(crate) async fn federated_invite(
    state: &AppState,
    sender: OwnedUserId,
    room_id: &RoomId,
    target: &UserId,
    reason: Option<String>,
) -> Response {
    let (store, registry, own_server) = {
        let app = lock_app(state);
        (
            app.store.clone(),
            app.room_registry.clone(),
            app.config.server_name.clone(),
        )
    };

    // 1. Build the candidate on the room's current heads — read-only, NOT
    //    persisted. Auth (can the inviter invite?) is deferred to the commit
    //    (`apply_resident`); a build failure / unknown room surfaces now.
    let mut content = json!({ "membership": "invite" });
    if let Some(r) = &reason {
        content["reason"] = json!(r);
    }
    let candidate = match registry
        .build_event(
            room_id,
            sender.clone(),
            "m.room.member".to_owned(),
            Some(target.to_string()),
            content,
        )
        .await
    {
        Ok(ev) => ev,
        Err(RoomActorError::UnknownRoom) => {
            return error_response(StatusCode::NOT_FOUND, "M_NOT_FOUND", "Not a known room");
        }
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    // 2. Federate to the invitee's server (the target's domain — a v12 room id
    //    carries no server) as the v2 envelope: the bare candidate event +
    //    room_version + stripped `invite_room_state` (the peer merges the latter
    //    into the event's `unsigned` for its sync to render).
    let irs = build_invite_room_state(&*store, room_id, &sender).await;
    let client = FederationClient::new(own_server);
    let returned = match client
        .invite(
            target.server_name(),
            room_id,
            &candidate.event_id,
            &candidate.raw,
            ROOM_VERSION_ID,
            &irs,
        )
        .await
    {
        Ok(resp) => resp.event,
        // The invitee server refused (e.g. 403 from its own checks). Map 403 →
        // 403, anything else → 502; persist nothing either way.
        Err(FederationClientError::Status(code)) => {
            let (status, errcode) = if code == StatusCode::FORBIDDEN.as_u16() {
                (StatusCode::FORBIDDEN, "M_FORBIDDEN")
            } else {
                (StatusCode::BAD_GATEWAY, "M_UNKNOWN")
            };
            return error_response(status, errcode, "invitee server rejected the invite");
        }
        Err(_) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "M_UNKNOWN",
                "could not reach the invitee's server",
            );
        }
    };

    // 3. Commit through the resident apply path (persist + distribute). Validate
    //    the peer returned *our* event (same reference hash) — it can't swap in a
    //    different one. `unsigned.invite_room_state` rides along harmlessly (it
    //    is outside the hash and never read for a remote member).
    let returned_event = match from_wire(returned, Vec::new()) {
        Ok(ev) if ev.event_id == candidate.event_id => ev,
        _ => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "M_UNKNOWN",
                "invitee server returned an unexpected event",
            );
        }
    };
    match registry.apply_resident(room_id, returned_event).await {
        Ok(()) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(RoomActorError::Rejected) => error_response(
            StatusCode::FORBIDDEN,
            "M_FORBIDDEN",
            "you do not have permission to invite this user",
        ),
        Err(RoomActorError::UnknownRoom) => {
            error_response(StatusCode::NOT_FOUND, "M_NOT_FOUND", "Not a known room")
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Build the stripped `invite_room_state` from current room state: the curated
/// rendering types plus the inviter's own member event, each reduced to a
/// StrippedStateEvent (`type` / `state_key` / `sender` / `content`). Re-stripped
/// here on egress rather than trusting any stored stripped form.
async fn build_invite_room_state(
    store: &impl StateStore,
    room_id: &RoomId,
    inviter: &UserId,
) -> Vec<Value> {
    let Ok(state) = store.current_room_state(room_id).await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for ((event_type, state_key), ev) in &state {
        let keep = INVITE_ROOM_STATE_TYPES.contains(&event_type.as_str())
            || (event_type == "m.room.member" && state_key.as_str() == inviter.as_str());
        if !keep {
            continue;
        }
        if let Ok(stripped) = Raw::<AnyStrippedStateEvent>::try_from(ev)
            && let Ok(v) = serde_json::to_value(&stripped)
        {
            out.push(v);
        }
    }
    out
}

/// Merge the v2 envelope's `invite_room_state` into the event's
/// `unsigned.invite_room_state`, so the sync invite path (which reads it from
/// `unsigned`) surfaces it. `unsigned` is outside the reference hash, so this
/// does not change the computed event id. An empty `irs` is a no-op — the event
/// is returned untouched (preserving any `unsigned` the sender already set).
/// Any JSON fault degrades to the original event (the invite still works, just
/// without a room preview) rather than failing the request.
fn merge_invite_room_state(
    event: Box<RawJsonValue>,
    irs: Vec<Box<RawJsonValue>>,
) -> Box<RawJsonValue> {
    if irs.is_empty() {
        return event;
    }
    let Ok(mut v) = serde_json::from_str::<Value>(event.get()) else {
        return event;
    };
    let Some(obj) = v.as_object_mut() else {
        return event;
    };
    let arr: Vec<Value> = irs
        .iter()
        .filter_map(|r| serde_json::from_str::<Value>(r.get()).ok())
        .collect();
    let unsigned = obj
        .entry("unsigned")
        .or_insert_with(|| Value::Object(Default::default()));
    if let Some(uobj) = unsigned.as_object_mut() {
        uobj.insert("invite_room_state".to_owned(), Value::Array(arr));
    }
    RawJsonValue::from_string(v.to_string()).unwrap_or(event)
}
