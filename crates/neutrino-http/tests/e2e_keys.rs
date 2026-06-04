//! E2EE keys-endpoint stubs are reached over loopback by the embedded client.
//! They must never panic on a malformed body — a `.unwrap()` on a missing JSON
//! pointer would tear down the request (CLAUDE.md: no `.unwrap()` in handler
//! code). These tests POST deliberately-malformed bodies to each keys endpoint
//! and assert a clean 2xx stub response instead of a 500 from a panic.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
        ..Default::default()
    }
}

async fn post(app: &axum::Router, path: &str, body: &Value) -> StatusCode {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

const UPLOAD: &str = "/_matrix/client/v3/keys/upload";
const DEVICE_SIGNING: &str = "/_matrix/client/v3/keys/device_signing/upload";
const SIGNATURES: &str = "/_matrix/client/v3/keys/signatures/upload";
const QUERY: &str = "/_matrix/client/v3/keys/query";

/// `keys_upload` without a `device_keys` field used to `.unwrap()` the missing
/// pointer and panic.
#[tokio::test]
async fn keys_upload_without_device_keys_does_not_panic() {
    let app = router(config()).await.expect("router init");
    let status = post(&app, UPLOAD, &json!({ "one_time_keys": {} })).await;
    assert!(status.is_success(), "got {status} (panic → 500?)");
}

/// Body that isn't even an object.
#[tokio::test]
async fn device_signing_upload_with_non_object_body_does_not_panic() {
    let app = router(config()).await.expect("router init");
    let status = post(&app, DEVICE_SIGNING, &json!("not-an-object")).await;
    assert!(status.is_success(), "got {status} (panic → 500?)");
}

/// `signatures_upload` whose body lacks the `<user>/DEVICEID/signatures/<user>`
/// path used to `.unwrap()` through three missing pointers.
#[tokio::test]
async fn signatures_upload_without_expected_path_does_not_panic() {
    let app = router(config()).await.expect("router init");
    let status = post(&app, SIGNATURES, &json!({ "wrong": "shape" })).await;
    assert!(status.is_success(), "got {status} (panic → 500?)");
}

/// device_signing then signatures, both before any successful `keys_upload`,
/// so `app.keys` is `None` — the handlers must no-op rather than unwrap.
#[tokio::test]
async fn signing_and_signatures_before_upload_do_not_panic() {
    let app = router(config()).await.expect("router init");
    let user = config().user_id();

    let well_formed_signing = json!({ "master_key": { "keys": {} }, "auth": {} });
    assert!(
        post(&app, DEVICE_SIGNING, &well_formed_signing)
            .await
            .is_success()
    );

    let well_formed_sigs = json!({
        user.clone(): {
            "DEVICEID": { "signatures": { user.clone(): { "ed25519:DEVICEID": "sig" } } }
        }
    });
    assert!(post(&app, SIGNATURES, &well_formed_sigs).await.is_success());
}

/// Happy path still works: a well-formed upload is stored and queryable.
#[tokio::test]
async fn well_formed_upload_round_trips_through_query() {
    let app = router(config()).await.expect("router init");
    let user = config().user_id();

    let upload = json!({
        "device_keys": {
            "user_id": user,
            "device_id": "DEVICEID",
            "algorithms": ["m.megolm.v1.aes-sha2"],
            "keys": { "ed25519:DEVICEID": "abc" }
        }
    });
    assert!(post(&app, UPLOAD, &upload).await.is_success());

    // Query echoes the stored device_keys.
    let req = Request::builder()
        .method("POST")
        .uri(QUERY)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({ "device_keys": {} })).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(resp.status().is_success());
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        body.pointer(&format!("/device_keys/{user}/DEVICEID"))
            .is_some(),
        "stored device_keys should be echoed back, got {body}"
    );
}
