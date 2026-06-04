//! End-to-end test for the `/_matrix/client/versions` handler.
//!
//! Verifies the advertised unstable features so a regression in either
//! advertisement (sliding-sync or MSC4222) is caught here rather than
//! manifesting as client-visible behavior changes (e.g. a client that
//! depends on the MSC4222 flag to know it can request `state_after`).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
use neutrino_http::router;
use serde_json::Value;
use tower::ServiceExt;

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn versions_advertises_msc4222_and_sliding_sync() {
    let app = router(config()).await.expect("router init");

    let req = Request::builder()
        .method("GET")
        .uri("/_matrix/client/versions")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        body.pointer("/unstable_features/org.matrix.msc4222")
            .and_then(|v| v.as_bool()),
        Some(true),
        "MSC4222 advertised: {body}"
    );
    assert_eq!(
        body.pointer("/unstable_features/org.matrix.simplified_msc3575")
            .and_then(|v| v.as_bool()),
        Some(true),
        "sliding-sync advertisement preserved: {body}"
    );

    let versions = body
        .get("versions")
        .and_then(|v| v.as_array())
        .expect("versions is an array");
    assert!(
        versions.iter().any(|v| v.as_str() == Some("v1.16")),
        "versions contains v1.16: {versions:?}"
    );
}
