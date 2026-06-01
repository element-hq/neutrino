//! Room Client-Server endpoints: createRoom, members, and event send.
//!
//! These are the real CSAPI handlers (not stubs) — they read and write room
//! state through the [`StorageBackend`](neutrino_store) trait.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_state::event_id::EventBuilder;
use neutrino_store::{EventStore, RoomStore, StateStore};
use ruma::{OwnedRoomId, OwnedUserId};
use serde_json::{Value, json};

use crate::{AppState, error_response, lock_app};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/_matrix/client/v3/createRoom", post(create_room))
        .route("/_matrix/client/v3/rooms/{room_id}/members", get(members))
        .route(
            "/_matrix/client/v3/rooms/{room_id}/send/{type}/{msg_id}",
            put(put_event),
        )
}

pub(crate) async fn create_room(
    state: State<AppState>,
    body: Json<Value>,
) -> axum::response::Response {
    let (store, user_id) = {
        let app = lock_app(&state.0);
        (app.store.clone(), app.config.user_id())
    };

    let sender: OwnedUserId = match user_id.parse() {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    // v12 / MSC4242: the creator is implicit (taken from `sender`); the
    // explicit `content.creator` field v11 carried is deprecated. The
    // builder computes the create event's event_id from the reference
    // hash, and `parse_event` derives `room_id` from it via the sigil swap.
    let create = match EventBuilder::new(sender.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
    {
        Ok(ev) => ev,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let room_id = create.room_id.clone();
    let create_event_id = create.event_id.clone();

    // Self-join. References the create event as both `prev_events` (DAG
    // parent) and `prev_state_events` (state-DAG parent, MSC4242).
    let join = match EventBuilder::new(sender.clone(), "m.room.member".to_owned())
        .room_id(room_id.clone())
        .state_key(sender.as_str().to_owned())
        .content(json!({ "membership": "join", "displayname": "Alice" }))
        .prev_events(vec![create_event_id.clone()])
        .prev_state_events(vec![create_event_id.clone()])
        .build()
    {
        Ok(ev) => ev,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    let mut initial = vec![join];

    if let Some(n) = body.0.pointer("/name").and_then(|v| v.as_str()) {
        let name = match EventBuilder::new(sender, "m.room.name".to_owned())
            .room_id(room_id.clone())
            .state_key(String::new())
            .content(json!({ "name": n }))
            .prev_events(vec![initial[0].event_id.clone()])
            .prev_state_events(vec![create_event_id.clone()])
            .build()
        {
            Ok(ev) => ev,
            Err(e) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                );
            }
        };
        initial.push(name);
    }

    // SqliteStore requires `create_room` to register the room before any
    // `persist_event` calls succeed. The create event lands via the trait's
    // dedicated path; member-join + (optional) name come through alongside
    // as `initial_events` so the whole thing is one transaction.
    if let Err(e) = store.create_room(&create, &initial).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        );
    }

    (StatusCode::OK, Json(json!({"room_id": room_id}))).into_response()
}

pub(crate) async fn members(
    state: State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let rid = match ruma::OwnedRoomId::try_from(room_id.as_str()) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };
    let map = match store
        .current_state_events_of_type(&rid, "m.room.member")
        .await
    {
        Ok(m) => m,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    // Per spec (https://spec.matrix.org/v1.18/client-server-api/#get_matrixclientv3roomsroomidmembers)
    // the default response includes members of every membership; filtering
    // is opt-in via `membership` / `not_membership` query params (which we
    // don't honour — see PLAN.md non-goals).
    let chunk: Vec<Value> = map
        .into_values()
        .filter_map(|ev| serde_json::from_str::<Value>(ev.raw.get()).ok())
        .collect();
    (StatusCode::OK, Json(json!({"chunk": chunk}))).into_response()
}

pub(crate) async fn put_event(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type, _msg_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    let (store, user_id) = {
        let app = lock_app(&state.0);
        (app.store.clone(), app.config.user_id())
    };

    let sender: OwnedUserId = match user_id.parse() {
        Ok(s) => s,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let parsed_room_id: OwnedRoomId = match room_id.parse() {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    // `prev_events` intentionally empty for now — wiring the DAG against the
    // room's current head is state-machine work (PLAN.md Phase 6) and not
    // in scope here. Matches the pre-B6 behaviour of this handler.
    let event = match EventBuilder::new(sender, event_type)
        .room_id(parsed_room_id)
        .content(body.0)
        .build()
    {
        Ok(ev) => ev,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_BAD_JSON", &e.to_string());
        }
    };
    let event_id = event.event_id.clone();
    if let Err(e) = store.persist_event(&event, &[]).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        );
    }

    (StatusCode::OK, Json(json!({"event_id": event_id}))).into_response()
}
