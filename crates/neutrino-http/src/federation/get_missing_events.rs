//! `POST /_matrix/federation/v1/get_missing_events/{roomId}`.
//!
//! Pure read over `DagStore::missing_events`. See `docs/get-missing-events.md`
//! for the design — the algorithm comments below cross-reference the seven
//! steps from the design doc's §Handler/Algorithm.

use axum::{
    Json,
    extract::{Path, State},
};
use neutrino_store::{DagStore, RoomStore};
use ruma::OwnedRoomId;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::FedError;
use crate::{AppState, lock_app};

/// Spec default for `limit` when the client omits it.
const DEFAULT_LIMIT: u32 = 10;
/// Hard cap on the number of events returned. The v1.18 spec sets *no* maximum
/// on `limit` (the field is documented only as "Defaults to 10":
/// <https://spec.matrix.org/v1.18/server-server-api/#post_matrixfederationv1get_missing_eventsroomid>).
/// This 20-event ceiling matches Synapse's `min(limit, 20)` in
/// `_get_missing_events` and bounds per-request work. Saturating cap, not a 400.
const MAX_LIMIT: u32 = 20;

/// Body of the federation request.
///
/// ruma's `#[request]` macro on `fed_v1::Request` generates an
/// `IncomingRequest` impl rather than a plain `Deserialize`, and the
/// `room_id` field is annotated `#[ruma_api(path)]` — it doesn't live in the
/// JSON body. We deserialize only the body half here and combine with the
/// path-extracted `room_id` in the handler. Mirrors the `SyncRequestBody`
/// pattern in `lib.rs::build_sync_request`.
#[derive(Deserialize)]
pub(crate) struct RequestBody {
    /// Optional. Saturates to [`DEFAULT_LIMIT`] when missing, capped at
    /// [`MAX_LIMIT`]. No `#[serde(default)]` — serde already deserializes a
    /// missing `Option` field to `None`.
    limit: Option<u32>,
    /// Parsed for spec compliance (so a malformed `min_depth` still 400s
    /// via serde) then dropped on the floor — Neutrino stores no depth
    /// column, per the 2026-05-22 PLAN.md decision. See
    /// `docs/get-missing-events.md` §"Trust model & spec deviations".
    /// Typed `i64` (not `u64`) because the spec defines `depth` as a
    /// signed integer; using `u64` would reject negative values at serde
    /// and contradict the accept-but-ignore intent. The underscore prefix
    /// tells `dead_code` the field is intentionally unused after
    /// deserialization. No `#[serde(default)]` — a missing `Option` field
    /// already deserializes to `None`.
    #[serde(rename = "min_depth")]
    _min_depth: Option<i64>,
    /// Boundary the requester already has; never appears in the response.
    /// Required per the spec (same as `latest_events`): a body omitting it
    /// 400s at deserialization, not silently treated as empty.
    earliest_events: Vec<ruma::OwnedEventId>,
    /// Events the requester wants to walk back from. Required and non-empty.
    latest_events: Vec<ruma::OwnedEventId>,
}

/// Serializable mirror of `ruma::api::federation::event::get_missing_events::v1::Response`.
///
/// ruma's `#[response]` macro on the federation crate emits an
/// `OutgoingResponse` impl that knows about HTTP-level concerns (status
/// codes, headers) but does not derive plain `Serialize`. We hand-roll a
/// view that emits the JSON body the federation spec actually wants —
/// just an `events` array of opaque PDUs.
///
/// Doubles as the *outbound* deserialize target — the federation client
/// (`client::FederationClient::get_missing_events`) parses a peer's response
/// into this same type. `#[serde(default)]` lets a `{}` body (no `events`
/// key) decode to an empty vec ("no progress").
#[derive(Serialize, Deserialize)]
pub(crate) struct ResponseBody {
    #[serde(default)]
    pub(crate) events: Vec<Box<RawJsonValue>>,
}

/// Federation `/get_missing_events` handler.
///
/// Algorithm (cross-ref `docs/get-missing-events.md` §Handler/Algorithm):
/// 1. Reject empty `latest_events` (400 M_INVALID_PARAM).
/// 2. 404 if room is unknown (pre-checked via `RoomStore::room_exists`).
/// 3. Clamp `limit`: default 10, max 20.
/// 4. Drop `_min_depth` (wire field `min_depth`) on the floor — Neutrino
///    has no depth column. See `RequestBody._min_depth` doc.
/// 5. Call `DagStore::missing_events`.
/// 6. Build response from `Event.raw` verbatim (no enrichment), reversed to
///    oldest-first.
/// 7. Storage errors → 500 M_UNKNOWN via `FedError::Storage`.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
    body: Result<Json<serde_json::Value>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<ResponseBody>, FedError> {
    // Parse the path-extracted `room_id` manually so malformed IDs surface
    // as 400 M_INVALID_PARAM JSON, not axum's default plain-text 400.
    // Mirrors the `members` handler precedent in `lib.rs`.
    let room_id: OwnedRoomId = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;

    // Take the raw JSON Value through `Json<Value>` so any malformed body
    // — invalid JSON, wrong content-type, missing required fields when we
    // deserialize below — surfaces as 400 M_INVALID_PARAM, not axum's
    // default 422 for shape mismatches. Mirrors how `lib.rs::sync`
    // routes JSON-edge failures through `error_response`.
    let body_value = body
        .map_err(|_| FedError::BadRequest("body is not valid JSON"))?
        .0;
    let body: RequestBody = serde_json::from_value(body_value)
        .map_err(|_| FedError::BadRequest("body shape does not match the spec"))?;

    // (1)
    if body.latest_events.is_empty() {
        return Err(FedError::BadRequest("latest_events must not be empty"));
    }

    let store = {
        let app = lock_app(&state);
        app.store.clone()
    };

    // (2) — `missing_events` surfaces an unknown room as
    // `StorageError::InvalidInput` (→ 500), so we pre-check existence via
    // `RoomStore::room_exists` and map absence to the spec-required 404.
    if !store.room_exists(&room_id).await? {
        return Err(FedError::RoomNotFound);
    }

    // (3) — saturating cap, not a 400.
    let limit = body.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;

    // (4) — `_min_depth` (wire field `min_depth`) deliberately ignored.

    // (5)
    let latest: Vec<&ruma::EventId> = body.latest_events.iter().map(|id| id.as_ref()).collect();
    let earliest: Vec<&ruma::EventId> = body.earliest_events.iter().map(|id| id.as_ref()).collect();
    let events = store
        .missing_events(&room_id, &latest, &earliest, limit)
        .await?;

    // (6) — wire bytes verbatim, oldest-first. `missing_events` walks back
    // from `latest`, so it yields newest-first; reverse to the topological
    // (oldest-first) order federation receivers expect — matches Synapse,
    // which reverses its walk before responding. The reference hash that
    // produced each event_id was computed over `event.raw`, so peers MUST
    // receive those exact bytes for the event_id to round-trip.
    let events: Vec<Box<RawJsonValue>> = events.into_iter().rev().map(|e| e.raw).collect();

    Ok(Json(ResponseBody { events }))
}
