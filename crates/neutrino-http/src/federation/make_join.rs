//! `GET /_matrix/federation/v1/make_join/{roomId}/{userId}` — the first half of
//! the federated join handshake, where **we are the resident** server.
//!
//! We return an unsigned `m.room.member` (`membership: join`) *template* built
//! on the room's current heads. The joining server completes it (filling
//! `origin_server_ts`, recomputing the reference-hash event id — we add no
//! signature, trusted mesh) and sends it back via `send_join`.
//!
//! Per MSC4242 the template carries `prev_events` (timeline forward
//! extremities) and `prev_state_events` (state-DAG forward extremities) but
//! **no `auth_events`** — those are server-computed at apply time, so the
//! joining server neither has nor needs the state DAG to produce a valid id.
//!
//! Trusted-mesh deviations match the sibling federation handlers: no X-Matrix
//! auth, no signature, no `join_authorised_via_users_server` (restricted rooms
//! are out of scope — a `restricted`/`knock` join rule is refused 403 here).

use axum::{
    Json,
    extract::{Path, RawQuery, State},
};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_store::{RoomStore, StateStore};
use ruma::{OwnedRoomId, OwnedUserId};
use serde::Serialize;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::FedError;
use crate::room_actor::RoomActorError;
use crate::{AppState, lock_app};

/// `make_join` response: the membership-event `template` plus the room's
/// version. (MSC4242 `omit_members` / `partial_state` is out of scope, so no
/// extra fields.)
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    /// The unsigned template event, wire-verbatim (`Event.raw`).
    event: Box<RawJsonValue>,
    room_version: String,
}

/// Federation `/make_join` handler.
///
/// 1. Parse `room_id` (400 JSON) + `user_id` (400 JSON).
/// 2. Negotiate the room version: the room must exist (else 404) and our
///    version must appear in the requester's `?ver=` list (else 400
///    `M_INCOMPATIBLE_ROOM_VERSION`).
/// 3. Join-rules pre-check against current state (banned ⇒ 403; invite-only and
///    not invited ⇒ 403; `public` ⇒ allow; `restricted`/`knock` ⇒ 403, out of
///    scope). Fail-fast only — `send_join`'s apply is authoritative.
/// 4. Build the `m.room.member`/`join` template on the current heads (no
///    persist) and return it with `room_version`.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path((room_id, user_id)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<ResponseBody>, FedError> {
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;
    let user_id = OwnedUserId::try_from(user_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid user_id"))?;

    let (store, registry) = {
        let app = lock_app(&state);
        (app.store.clone(), app.room_registry.clone())
    };

    // (2) — room must exist and be our version.
    if store.get_room_version(&room_id).await?.is_none() {
        return Err(FedError::RoomNotFound);
    }
    if !ver_includes_ours(raw_query.as_deref()) {
        return Err(FedError::IncompatibleRoomVersion(
            ROOM_VERSION_ID.to_owned(),
        ));
    }

    // (3) — join-rules gate.
    check_can_join(&*store, &room_id, &user_id).await?;

    // (4) — build the template on the current heads (read-only).
    let template = registry
        .build_event(
            &room_id,
            user_id.clone(),
            "m.room.member".to_owned(),
            Some(user_id.to_string()),
            serde_json::json!({ "membership": "join" }),
        )
        .await
        .map_err(map_build_err)?;

    Ok(Json(ResponseBody {
        event: template.raw.clone(),
        room_version: ROOM_VERSION_ID.to_owned(),
    }))
}

/// True if the requester's repeated `?ver=` query includes our room version.
/// A wholly-absent `ver` defaults to `["1"]` per spec, which never matches our
/// `org.matrix.msc4242.12`, so an absent `ver` is (correctly) incompatible.
fn ver_includes_ours(raw: Option<&str>) -> bool {
    let Some(raw) = raw else {
        return false;
    };
    raw.split('&').any(|pair| {
        let (key, val) = pair.split_once('=').unwrap_or((pair, ""));
        key == "ver" && val == ROOM_VERSION_ID
    })
}

/// Join-rules pre-check. Reads the user's current member event once (ban +
/// invite) and the room's join rule. Out-of-scope rules (`restricted`,
/// `knock`) are refused.
async fn check_can_join(
    store: &impl StateStore,
    room_id: &ruma::RoomId,
    user_id: &ruma::UserId,
) -> Result<(), FedError> {
    let member = store
        .current_state_event(room_id, "m.room.member", user_id.as_str())
        .await?;
    let membership = member.as_ref().and_then(|e| e.content_str("membership"));
    if membership.as_deref() == Some("ban") {
        return Err(FedError::Forbidden("user is banned from the room"));
    }

    let join_rule = store
        .current_state_event(room_id, "m.room.join_rules", "")
        .await?
        .and_then(|e| e.content_str("join_rule"))
        .unwrap_or_else(|| "invite".to_owned());

    match join_rule.as_str() {
        "public" => Ok(()),
        // Default and invite-only: a currently-invited user may join — and so
        // may an already-`join`ed user (a re-`make_join`, e.g. after the
        // joining server lost its DB), matching auth rule 5.3.4 which the
        // authoritative apply at `send_join` enforces.
        "invite" => {
            if matches!(membership.as_deref(), Some("invite") | Some("join")) {
                Ok(())
            } else {
                Err(FedError::Forbidden("you are not invited to this room"))
            }
        }
        // restricted / knock are out of scope for this server.
        _ => Err(FedError::Forbidden(
            "this room's join rule is not supported",
        )),
    }
}

/// Map a `build_event` actor error onto the HTTP layer. `UnknownRoom` is a 404
/// (the room vanished between the version check and the build — a race);
/// anything else is internal. Shared with `make_leave` (same template-build
/// path).
pub(crate) fn map_build_err(err: RoomActorError) -> FedError {
    match err {
        RoomActorError::UnknownRoom => FedError::RoomNotFound,
        RoomActorError::Storage(e) => FedError::Storage(e),
        _ => FedError::Internal("could not build membership template"),
    }
}
