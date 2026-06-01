//! End-to-end encryption key stubs (`/_matrix/client/v3/keys/*`).
//!
//! E2EE is not implemented (see CLAUDE.md); these handlers stash and echo the
//! client's uploaded key blobs so the client application keeps functioning.

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use tracing::info;

use crate::{AppState, lock_app};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/_matrix/client/v3/keys/query", post(keys_query))
        .route("/_matrix/client/v3/keys/upload", post(keys_upload))
        .route(
            "/_matrix/client/v3/keys/device_signing/upload",
            post(device_signing_upload),
        )
        .route(
            "/_matrix/client/v3/keys/signatures/upload",
            post(signatures_upload),
        )
}

pub(crate) async fn keys_query(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received query: {:?}", body.0);

    if let Some(keys) = &lock_app(&state.0).keys {
        info!(
            "Returning stored keys: {}",
            serde_json::to_string(&keys).unwrap_or_default()
        );
        Json(keys.clone())
    } else {
        Json(json!({
            "device_keys": {},
        }))
    }
}

pub(crate) async fn keys_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received keys upload: {:?}", body.0);

    let mut app = lock_app(&state.0);
    let body = body.0;

    if app.keys.is_none()
        && let Some(device_keys) = body.pointer("/device_keys")
    {
        let user_id = app.config.user_id();
        app.keys = Some(json!({
            "device_keys": {
                user_id: { "DEVICEID": device_keys.clone() }
            }
        }));
    }

    Json(json!({
      "one_time_key_counts": {
        "signed_curve25519": 100
      }
    }))
}

pub(crate) async fn device_signing_upload(
    state: State<AppState>,
    body: Json<Value>,
) -> Json<Value> {
    let mut app = lock_app(&state.0);

    let mut body = body.0;
    if let Some(obj) = body.as_object_mut() {
        obj.remove("auth");
    }

    // Merge the (auth-stripped) cross-signing keys into the stored blob.
    // No-op unless a prior `keys_upload` created `app.keys` as an object and
    // the body is itself an object — a malformed body must not panic the
    // stub handler.
    if let Some(keys) = app.keys.as_mut().and_then(Value::as_object_mut)
        && let Some(body_obj) = body.as_object()
    {
        keys.extend(body_obj.clone());
    }

    Json(json!({}))
}

pub(crate) async fn signatures_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received signatures upload: {:?}", body.0);
    let mut app = lock_app(&state.0);
    let user_id = app.config.user_id();

    // Extract the uploaded signatures map. Absent/malformed path → nothing to
    // merge; return the stub without touching stored keys.
    let sigs = body
        .pointer(&format!("/{0}/DEVICEID/signatures/{0}", user_id))
        .and_then(Value::as_object)
        .cloned();

    if let Some(sigs) = sigs
        && let Some(keys) = &mut app.keys
    {
        info!(
            "Adding signatures to stored keys {:?}",
            serde_json::to_string(keys).unwrap_or_default()
        );
        if let Some(target) = keys
            .pointer_mut(&format!(
                "/device_keys/{0}/DEVICEID/signatures/{0}",
                user_id
            ))
            .and_then(Value::as_object_mut)
        {
            target.extend(sigs);
        }
    }

    Json(json!({}))
}
