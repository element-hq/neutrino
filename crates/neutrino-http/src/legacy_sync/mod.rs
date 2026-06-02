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
