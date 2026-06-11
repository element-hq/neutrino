//! Legacy `/_matrix/client/v3/sync` stub layered over the MSC4186
//! sliding-sync handler.
//!
//! The pure translation helpers (`parse_legacy_query`,
//! `synthesize_v5_request`, `translate_response`) live in
//! [`translate`]. This module's [`handle`] is the axum entrypoint
//! that ties them together — extracts state, calls into
//! `sliding_sync::handle`, maps errors and shapes the response.
//! See `docs/legacy-sync-stub.md` for the design.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use neutrino_store::{Membership, StorageBackend};
use ruma::{OwnedRoomId, OwnedUserId};

use crate::{AppState, error_response, lock_app};
use crate::{
    legacy_sync::translate::{parse_legacy_query, synthesize_v5_request, translate_response},
    sliding_sync::{self, SyncError, SyncState},
};

pub mod translate;

/// Legacy `GET /_matrix/client/v3/sync` handler.
///
/// Mirrors the MSC4186 wrapper in `lib.rs::sync` exactly in terms of state
/// extraction (clone `sync_state` + `user_id` out of the std-mutex'd
/// `AppState` so we don't hold a `!Send` lock across `.await`) and error
/// mapping (`UnknownPos` → 400 M_UNKNOWN_POS, `BadRequest` → 400
/// M_INVALID_PARAM, `Storage` / `EventConversion` → 500 M_UNKNOWN).
pub(crate) async fn handle(
    state: State<AppState>,
    crate::AuthUser(user_id): crate::AuthUser,
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    let sync_state = lock_app(&state.0).sync_state.clone();

    let legacy_query = parse_legacy_query(&query.0);
    let req = synthesize_v5_request(&legacy_query);

    // Snapshot the user's room memberships **before** invoking sliding_sync
    // so the bucketing reflects the same point-in-time the v5 call observes.
    // See `docs/legacy-sync-stub.md` §"Per-room bucketing".
    let memberships = match fetch_memberships(&sync_state, &user_id).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    // Legacy `since` tokens are durable: a client may sync from any past token
    // forever. Sliding-sync's `pos`, which we map `since` onto, is the opposite
    // — a single-cursor per-connection value that rejects anything but the
    // last-issued one with `UnknownPos` (a v5-only reconnect signal). So on an
    // unknown/stale token we don't 400; we fall back to a full initial sync,
    // which returns current state under a fresh token. (Stale tokens collapse to
    // "state now" rather than a true cumulative delta — see docs/legacy-sync-stub.md.)
    let resp = match sliding_sync::handle(&sync_state, &user_id, req).await {
        Err(SyncError::UnknownPos) => {
            let mut initial = synthesize_v5_request(&legacy_query);
            initial.pos = None;
            sliding_sync::handle(&sync_state, &user_id, initial).await
        }
        other => other,
    };

    match resp {
        Ok(v5_resp) => {
            let body = translate_response(v5_resp, &memberships);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(SyncError::UnknownPos) => {
            // Unreachable in practice: an initial sync (pos = None) never raises
            // this. Kept as a defensive mapping.
            error_response(StatusCode::BAD_REQUEST, "M_UNKNOWN_POS", "Unknown position")
        }
        Err(SyncError::BadRequest(msg)) => {
            error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", msg)
        }
        Err(SyncError::Storage(e)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
        Err(SyncError::EventConversion(e)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Query the store for the user's current memberships across every
/// `Membership` variant and collect into a `BTreeMap` for O(log n)
/// lookup by `translate_response`'s bucketing loop.
///
/// Out-of-band invites (federated invites for rooms we don't host, stored
/// outside `current_state`) are unioned in as `Invite` so `translate_response`
/// buckets those rooms into `rooms.invite` explicitly rather than via its
/// missing-from-map `invite_state` fallback (which warns). In-room membership
/// wins on overlap (`or_insert` keeps the `rooms_with_membership` value).
async fn fetch_memberships<S: StorageBackend>(
    sync_state: &SyncState<S>,
    user_id: &OwnedUserId,
) -> Result<BTreeMap<OwnedRoomId, Membership>, neutrino_store::StorageError> {
    let all: BTreeSet<Membership> = [
        Membership::Join,
        Membership::Invite,
        Membership::Knock,
        Membership::Leave,
        Membership::Ban,
    ]
    .into_iter()
    .collect();
    let rows = sync_state
        .store
        .rooms_with_membership(user_id, &all)
        .await?;
    let mut map: BTreeMap<OwnedRoomId, Membership> = rows.into_iter().collect();
    for room_id in sync_state.store.invited_oob_rooms(user_id).await? {
        map.entry(room_id).or_insert(Membership::Invite);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use neutrino_common::Event;
    use neutrino_common::event_id::compute_event_id;
    use neutrino_store::{InviteStore, Membership};
    use neutrino_store_sqlite::SqliteStore;
    use ruma::{RoomId, UserId, room_id, user_id};
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::fetch_memberships;
    use crate::legacy_sync::translate::{
        parse_legacy_query, synthesize_v5_request, translate_response,
    };
    use crate::sliding_sync::{self, SyncState};

    /// A shutdown token that never fires — for tests exercising non-shutdown paths.
    fn no_shutdown() -> tokio_util::sync::CancellationToken {
        tokio_util::sync::CancellationToken::new()
    }

    async fn fresh_store() -> (Arc<SqliteStore>, TempDir) {
        let tmp = TempDir::new().expect("create tempfile");
        let store = SqliteStore::open_in_dir(tmp.path())
            .await
            .expect("open store");
        (Arc::new(store), tmp)
    }

    /// An out-of-band invite `m.room.member` event (with stripped
    /// `unsigned.invite_room_state`) as it arrives over `/invite/v2`. Built by
    /// hand so the raw carries `unsigned` verbatim — `put_invite` stores it and
    /// `get_invite` hydrates it through `EventRow` without redaction.
    fn oob_invite(room: &RoomId, invited: &UserId, inviter: &UserId, name: &str) -> Event {
        let body: Value = json!({
            "room_id": room.as_str(),
            "type": "m.room.member",
            "state_key": invited.as_str(),
            "sender": inviter.as_str(),
            "origin_server_ts": 80u64,
            "content": {"membership": "invite"},
            "hashes": {"sha256": "abcDEF0123456789"},
            "prev_events": [],
            "prev_state_events": [],
            "unsigned": {"invite_room_state": [
                {"type": "m.room.name", "state_key": "", "sender": inviter.as_str(),
                 "content": {"name": name}},
                {"type": "m.room.member", "state_key": inviter.as_str(),
                 "sender": inviter.as_str(), "content": {"membership": "join"}}
            ]}
        });
        let raw = serde_json::value::to_raw_value(&body).unwrap();
        let event_id = compute_event_id(&raw).unwrap();
        let content = serde_json::value::to_raw_value(body.get("content").unwrap()).unwrap();
        Event {
            event_id,
            room_id: room.to_owned(),
            event_type: "m.room.member".to_owned(),
            state_key: Some(invited.as_str().to_owned()),
            sender: inviter.to_owned(),
            origin_server_ts: 80,
            content,
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            auth_events: Vec::new(),
            rejected: false,
            soft_failed: false,
            raw,
        }
    }

    /// The legacy read-path: an OOB invite must be classified `Invite` by
    /// `fetch_memberships` and bucketed under `rooms.invite` with its
    /// `invite_state` by the full v5→v3 composition — no `current_state` for
    /// the room exists.
    #[tokio::test]
    async fn oob_invite_buckets_into_legacy_rooms_invite() {
        let (store, _tmp) = fresh_store().await;
        let user = user_id!("@alice:example.org");
        let inviter = user_id!("@bob:other.example.org");
        let room = room_id!("!remote:other.example.org");
        store
            .put_invite(room, user, &oob_invite(room, user, inviter, "Remote Room"))
            .await
            .unwrap();

        let sync_state = SyncState::new(store, no_shutdown());
        let owned_user = user.to_owned();

        let memberships = fetch_memberships(&sync_state, &owned_user).await.unwrap();
        assert_eq!(
            memberships.get(room),
            Some(&Membership::Invite),
            "OOB invite classified as Invite for bucketing"
        );

        let req = synthesize_v5_request(&parse_legacy_query(&HashMap::new()));
        let v5 = sliding_sync::handle(&sync_state, user, req).await.unwrap();
        let body = translate_response(v5, &memberships);

        let invite = body
            .pointer("/rooms/invite")
            .and_then(|v| v.as_object())
            .expect("rooms.invite present");
        let room_obj = invite
            .get(room.as_str())
            .expect("OOB invite room bucketed under rooms.invite");
        let events = room_obj
            .pointer("/invite_state/events")
            .and_then(|v| v.as_array())
            .expect("invite_state.events present");
        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.pointer("/type").and_then(|t| t.as_str()))
            .collect();
        assert!(
            types.contains(&"m.room.name"),
            "stripped invite_room_state passed through to v3 invite_state"
        );
    }
}
