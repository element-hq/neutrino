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

use self::translate::{parse_legacy_query, synthesize_v5_request, translate_response};
use super::sliding_sync::{self, SyncError, SyncState};
use crate::{AppState, error_response, lock_app};

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
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    let (sync_state, user_id_str) = {
        let app = lock_app(&state.0);
        (app.sync_state.clone(), app.config.user_id())
    };

    let user_id: OwnedUserId = match user_id_str.as_str().try_into() {
        Ok(u) => u,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

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

    match sliding_sync::handle(&sync_state, &user_id, req).await {
        Ok(v5_resp) => {
            let body = translate_response(v5_resp, &memberships);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(SyncError::UnknownPos) => {
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
    Ok(rows.into_iter().collect())
}
