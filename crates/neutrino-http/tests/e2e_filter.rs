//! End-to-end test for the `POST /user/{userId}/filter` + `GET …/filter/{id}`
//! stub.
//!
//! matrix-rust-sdk syncs via MSC4186 sliding sync, which does its own
//! filtering; neutrino's legacy `/v3/sync` exists only to run Complement, and
//! its translator reads-and-discards `?filter=`. So the filter API never needs
//! to store or apply anything — it exists purely so Complement's `CreateFilter`
//! (called before the `TestSync/*` tranche) gets a `filter_id` instead of a
//! 404. This pins that no-op contract: POST returns a `filter_id`, GET returns
//! a JSON object.

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

/// Build a router over a throwaway storage directory the test owns. The
/// returned `TempDir` MUST be held for the lifetime of the router — dropping
/// it deletes the database directory.
async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().expect("create storage tempdir");
    let mut cfg = config();
    cfg.storage_dir = tmp.path().to_path_buf();
    let app = router(cfg).await.expect("router");
    (app, tmp)
}

#[tokio::test]
async fn post_filter_returns_a_filter_id() {
    let (app, _tmp) = test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/_matrix/client/v3/user/@alice:example.org/filter")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "room": { "timeline": { "limit": 10 } } }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert!(
        body.get("filter_id").and_then(Value::as_str).is_some(),
        "response carries a string filter_id: {body}"
    );
}

#[tokio::test]
async fn get_filter_returns_a_json_object() {
    let (app, _tmp) = test_router().await;

    let req = Request::builder()
        .method("GET")
        .uri("/_matrix/client/v3/user/@alice:example.org/filter/0")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert!(body.is_object(), "GET filter returns a JSON object: {body}");
}
