//! Own-user receipts, read markers and room account data. All of it stays on
//! this server and reaches the client through sliding sync.

use axum::{
    Json,
    body::Bytes,
    extract::{FromRequest, FromRequestParts, Path, Request, State},
    http::StatusCode,
    response::IntoResponse,
};
use neutrino_store::StorageError;
use ruma::api::IncomingRequest;
use ruma::api::client::config::set_room_account_data;
use ruma::api::client::read_marker::set_read_marker;
use ruma::api::client::receipt::create_receipt::{self, v3::ReceiptType};
use ruma::events::RoomAccountDataEventType;
use ruma::events::fully_read::FullyReadEventContent;
use ruma::events::receipt::ReceiptThread;
use ruma::{EventId, MilliSecondsSinceUnixEpoch, RoomId, UserId};
use serde_json::json;
use serde_json::value::to_raw_value;

use crate::{AppState, AuthUser, Store, error_response, lock_app};

const FULLY_READ: &str = "m.fully_read";

/// A ruma request type, parsed from the path segments and body.
pub(crate) struct RumaRequest<T>(pub T);

impl<S: Send + Sync, T: IncomingRequest> FromRequest<S> for RumaRequest<T> {
    type Rejection = axum::response::Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = req.into_parts();
        let Path(path_args) = Path::<Vec<String>>::from_request_parts(&mut parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let body = Bytes::from_request(Request::from_parts(parts.clone(), body), state)
            .await
            .map_err(IntoResponse::into_response)?;
        T::try_from_http_request(axum::http::Request::from_parts(parts, body), &path_args)
            .map(Self)
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "M_BAD_JSON", &e.to_string()))
    }
}

/// # `POST /_matrix/client/v3/rooms/{roomId}/receipt/{receiptType}/{eventId}`
///
/// Sets the user's receipt or fully-read marker at an event.
pub(crate) async fn post_receipt<S: Store>(
    state: State<AppState<S>>,
    AuthUser(user): AuthUser,
    RumaRequest(req): RumaRequest<create_receipt::v3::Request>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let written = match req.receipt_type {
        ReceiptType::FullyRead if req.thread != ReceiptThread::Unthreaded => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                "thread_id is not allowed on m.fully_read",
            );
        }
        ReceiptType::FullyRead => set_fully_read(&*store, &user, &req.room_id, &req.event_id).await,
        ReceiptType::Read | ReceiptType::ReadPrivate => {
            record_receipt(
                &*store,
                &user,
                &req.room_id,
                req.receipt_type.as_str(),
                req.thread.as_str().unwrap_or_default(),
                &req.event_id,
            )
            .await
        }
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                "unknown receipt type",
            );
        }
    };
    respond(written)
}

/// # `POST /_matrix/client/v3/rooms/{roomId}/read_markers`
///
/// Sets the user's fully-read marker and/or read receipts in a room.
pub(crate) async fn post_read_markers<S: Store>(
    state: State<AppState<S>>,
    AuthUser(user): AuthUser,
    RumaRequest(req): RumaRequest<set_read_marker::v3::Request>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let written = async {
        if let Some(event_id) = &req.fully_read {
            set_fully_read(&*store, &user, &req.room_id, event_id).await?;
        }
        for (receipt_type, event_id) in [
            (ReceiptType::Read, &req.read_receipt),
            (ReceiptType::ReadPrivate, &req.private_read_receipt),
        ] {
            if let Some(event_id) = event_id {
                let receipt_type = receipt_type.as_str();
                record_receipt(&*store, &user, &req.room_id, receipt_type, "", event_id).await?;
            }
        }
        Ok(())
    }
    .await;
    respond(written)
}

/// # `PUT /_matrix/client/v3/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Sets some per-room account data for the user.
pub(crate) async fn put_room_account_data<S: Store>(
    state: State<AppState<S>>,
    AuthUser(user): AuthUser,
    RumaRequest(req): RumaRequest<set_room_account_data::v3::Request>,
) -> axum::response::Response {
    // The marker only moves through the receipt routes, which keep it forward-only.
    if req.event_type == RoomAccountDataEventType::FullyRead {
        return error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "M_BAD_JSON",
            "m.fully_read is set through /read_markers",
        );
    }
    if req
        .data
        .deserialize_as::<serde_json::Map<String, serde_json::Value>>()
        .is_err()
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "M_NOT_JSON",
            "content must be a JSON object",
        );
    }
    let store = lock_app(&state.0).store.clone();
    let data_type = req.event_type.to_string();
    respond(
        store
            .set_room_account_data(&user, &req.room_id, &data_type, req.data.json())
            .await,
    )
}

async fn record_receipt<S: Store>(
    store: &S,
    user: &UserId,
    room: &RoomId,
    receipt_type: &str,
    thread: &str,
    event_id: &EventId,
) -> Result<(), StorageError> {
    let ts = MilliSecondsSinceUnixEpoch::now().get().into();
    store
        .record_receipt(room, user, receipt_type, thread, event_id, ts)
        .await
}

/// Move `user`'s `m.fully_read` marker to `event_id`, unless the marker is
/// already at or after it. An event not in `room` leaves the marker alone.
async fn set_fully_read<S: Store>(
    store: &S,
    user: &UserId,
    room: &RoomId,
    event_id: &EventId,
) -> Result<(), StorageError> {
    let Some(new_pos) = store.event_stream_pos(room, event_id).await? else {
        return Ok(());
    };
    if let Some(current) = store.room_account_data(user, room, FULLY_READ).await?
        && let Ok(current) = serde_json::from_str::<FullyReadEventContent>(current.get())
        && let Some(current_pos) = store.event_stream_pos(room, &current.event_id).await?
        && current_pos >= new_pos
    {
        return Ok(());
    }
    let content = to_raw_value(&FullyReadEventContent::new(event_id.to_owned()))
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    store
        .set_room_account_data(user, room, FULLY_READ, &content)
        .await
}

fn respond(written: Result<(), StorageError>) -> axum::response::Response {
    match written {
        Ok(()) => Json(json!({})).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}
