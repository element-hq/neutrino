//! End-to-end tests for the testing-only multi-user identity shim. Compiled
//! and run only with `--features multi-user-shim`:
//!   cargo test -p neutrino-http --features multi-user-shim --test e2e_multi_user
//!
//! Proves: distinct per-user tokens; events + sync attributed to the token's
//! user; spec-correct 401 on missing/unknown tokens.
#![cfg(feature = "multi-user-shim")]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use neutrino_common::Config;
use neutrino_http::router;
use serde_json::{Value, json};
use tower::ServiceExt;

const SYNC_PATH: &str = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync";

fn config() -> Config {
    Config {
        server_name: "example.org".to_string(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: "alice".to_string(),
    }
}

/// Send a request with an optional Bearer token and JSON body.
async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let req = builder
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

/// Register a user via the two-step UIA stub; return (user_id, access_token).
async fn register(app: &axum::Router, username: &str) -> (String, String) {
    let _ = send(
        app,
        "POST",
        "/_matrix/client/v3/register",
        None,
        &json!({ "username": username }),
    )
    .await;
    let (status, body) = send(
        app,
        "POST",
        "/_matrix/client/v3/register",
        None,
        &json!({ "username": username, "auth": { "type": "m.login.dummy" } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register body: {body}");
    (
        body["user_id"].as_str().unwrap().to_owned(),
        body["access_token"].as_str().unwrap().to_owned(),
    )
}

fn sync_body() -> Value {
    json!({
        "lists": { "all": { "ranges": [[0, 99]], "timeline_limit": 5, "required_state": [] } }
    })
}

#[tokio::test]
async fn register_two_users_yields_distinct_tokens() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    assert_eq!(alice_id, "@alice:example.org");
    assert_eq!(bob_id, "@bob:example.org");
    assert_ne!(alice_tok, bob_tok, "tokens must differ");
}

#[tokio::test]
async fn createroom_and_sync_are_attributed_to_the_token_user() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    let (s, a_room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{a_room}");
    let alice_room = a_room["room_id"].as_str().unwrap().to_owned();

    let (s, b_room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b_room}");
    let bob_room = b_room["room_id"].as_str().unwrap().to_owned();

    assert_ne!(alice_room, bob_room);

    let (s, alice_sync) = send(&app, "POST", SYNC_PATH, Some(&alice_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{alice_sync}");
    let alice_rooms = alice_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(
        alice_rooms.contains_key(&alice_room),
        "alice should see her room: {alice_sync}"
    );
    assert!(
        !alice_rooms.contains_key(&bob_room),
        "alice must NOT see bob's room: {alice_sync}"
    );

    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    let bob_rooms = bob_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(
        bob_rooms.contains_key(&bob_room),
        "bob should see his room: {bob_sync}"
    );
    assert!(
        !bob_rooms.contains_key(&alice_room),
        "bob must NOT see alice's room: {bob_sync}"
    );
}

#[tokio::test]
async fn missing_token_is_401_missing() {
    let app = router(config()).await.expect("router init");
    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        None,
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["errcode"], "M_MISSING_TOKEN");
}

#[tokio::test]
async fn unknown_token_is_401_unknown() {
    let app = router(config()).await.expect("router init");
    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some("syt_bogus"),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["errcode"], "M_UNKNOWN_TOKEN");
}
