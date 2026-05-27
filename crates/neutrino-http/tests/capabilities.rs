//! End-to-end test for the `/_matrix/client/v3/capabilities` stub.
//!
//! Complement's `CSAPI.GetDefaultRoomVersion` (client.go:521) calls this
//! endpoint before many tests run. A 404 here cascades into Fatal'd parent
//! tests (e.g. TestRoomCreationReportsEventsToMyself) before any subtest
//! executes — pin the shape so allowlisted tests keep working.

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
    }
}

#[tokio::test]
async fn capabilities_advertises_room_version_12() {
    let app = router(config()).await.expect("router init");

    let req = Request::builder()
        .method("GET")
        .uri("/_matrix/client/v3/capabilities")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        body.pointer("/capabilities/m.room_versions/default")
            .and_then(|v| v.as_str()),
        Some("12"),
        "default room version is 12: {body}"
    );
    assert_eq!(
        body.pointer("/capabilities/m.room_versions/available/12")
            .and_then(|v| v.as_str()),
        Some("stable"),
        "room version 12 advertised as stable: {body}"
    );
}
