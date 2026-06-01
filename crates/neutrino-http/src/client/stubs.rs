//! Canned Client-Server API responses: login, registration, capabilities,
//! profile, account-data, and other endpoints the embedded client probes but
//! that carry no real server state.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use neutrino_common::Config;
use serde_json::{Value, json};
use tracing::info;

use crate::{AppState, lock_app};

pub(crate) fn routes(config: &Config) -> Router<AppState> {
    let user_id = config.user_id();
    Router::new()
        .route("/_matrix/client/versions", get(versions))
        .route(
            "/_matrix/client/{version}/login",
            get(get_login).post(post_login),
        )
        .route("/_matrix/client/{version}/register", post(post_register))
        .route(
            &format!("/_matrix/client/v3/profile/{}", user_id),
            get(profile),
        )
        .route(
            &format!(
                "/_matrix/client/v3/user/{}/account_data/{{account_data_type}}",
                user_id
            ),
            get(get_account_data),
        )
        .route("/_matrix/client/v3/room_keys/version", get(get_room_keys))
        .route("/_matrix/client/v3/pushers/set", post(pushers_set))
        .route("/_matrix/client/v3/capabilities", get(get_capabilities))
}

pub(crate) async fn versions() -> Json<Value> {
    Json(json!({
        "unstable_features": {
            "org.matrix.simplified_msc3575": true,
            "org.matrix.msc4222": true,
        },
        "versions": ["v1.16"]
    }))
}

pub(crate) async fn get_login() -> Json<Value> {
    Json(json!({
        "flows": [
            {
                "type": "m.login.password"
            }
        ],
    }))
}

pub(crate) async fn post_register(
    state: State<AppState>,
    body: Json<Value>,
) -> (StatusCode, Json<Value>) {
    // No `auth` block — initiate UIA so the client knows which flows to attempt.
    if body.0.get("auth").is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "flows": [{"stages": ["m.login.dummy"]}],
                "params": {},
                "session": "neutrino-register-session",
            })),
        );
    }

    let app = lock_app(&state.0);
    let device_id = body
        .0
        .pointer("/device_id")
        .and_then(|v| v.as_str())
        .unwrap_or("DEVICEID")
        .to_string();

    (
        StatusCode::OK,
        Json(json!({
            "user_id": app.config.user_id(),
            "access_token": "syt_1234567890abcdef",
            "home_server": app.config.server_name,
            "device_id": device_id,
        })),
    )
}

pub(crate) async fn post_login(state: State<AppState>) -> Json<Value> {
    info!("Logged in");

    let app = lock_app(&state.0);
    let user_id = app.config.user_id();
    let server_name = app.config.server_name.clone();

    Json(json!({
        "user_id": user_id,
        "access_token": "syt_1234567890abcdef",
        "home_server": server_name,
        "device_id": "DEVICEID"
    }))
}

pub(crate) async fn profile() -> Json<Value> {
    Json(json!({
        "displayname": "Alice",
    }))
}

pub(crate) async fn get_account_data(
    axum::extract::Path(_account_data_type): axum::extract::Path<String>,
) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
             "errcode": "M_NOT_FOUND",
              "error": "No current backup version"
        })),
    )
}

pub(crate) async fn get_room_keys() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
             "errcode": "M_NOT_FOUND",
              "error": "No current backup version"
        })),
    )
}

pub(crate) async fn pushers_set() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({})))
}

pub(crate) async fn get_capabilities() -> Json<Value> {
    Json(json!({
        "capabilities": {
            "m.room_versions": {
                "default": "12",
                "available": { "12": "stable" }
            }
        }
    }))
}
