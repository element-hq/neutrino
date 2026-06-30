//! Proves the embedded DB persists across an `AppState`/router drop+reopen on
//! the same `storage_dir` — the core guarantee of the configurable-storage work.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_ctl::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt; // oneshot

fn config_in(dir: &std::path::Path) -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
        storage_dir: dir.to_path_buf(),
        ..Default::default()
    }
}

async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(match body {
            Some(b) => Body::from(serde_json::to_vec(b).unwrap()),
            None => Body::empty(),
        })
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn room_survives_restart_on_same_storage_dir() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let cfg = config_in(tmp.path());

    // First boot: create a room.
    let app1 = router(cfg.clone()).await.expect("router boot 1");
    let (status, body) = send(
        &app1,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "createRoom failed: {body}");
    let room_id = body["room_id"].as_str().expect("room_id").to_string();
    drop(app1); // close the pools so the DB file is fully released

    // Second boot on the SAME directory: the room must still be there.
    let app2 = router(cfg).await.expect("router boot 2");
    let (status, state) = send(
        &app2,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state"),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "state read after restart failed: {state}"
    );
    let has_create = state
        .as_array()
        .expect("state array")
        .iter()
        .any(|e| e["type"] == "m.room.create");
    assert!(has_create, "m.room.create missing after restart: {state}");
}
