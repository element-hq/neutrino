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
        ..Default::default()
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

/// A successful `/login` must mint a token that actually resolves — i.e. the
/// returned token is usable on a subsequent authenticated request, not a
/// hardcoded constant that was never stored.
#[tokio::test]
async fn login_mints_a_resolvable_token() {
    let app = router(config()).await.expect("router init");
    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/login",
        None,
        &json!({ "type": "m.login.password", "identifier": { "user": "bob" } }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["user_id"], "@bob:example.org");
    let token = body["access_token"].as_str().unwrap().to_owned();

    // The minted token must be honoured by an authed endpoint.
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&token),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "minted token must resolve: {room}");
}

/// A `/login` identifier that cannot form a valid user id must surface a 400,
/// never a 200 carrying an unregistered token (which would 401 on the next
/// authenticated request). A localpart that overflows the 255-byte MXID limit
/// is rejected regardless of ruma's (lenient) charset grammar.
#[tokio::test]
async fn login_with_malformed_identifier_is_400() {
    let app = router(config()).await.expect("router init");
    let oversized = "a".repeat(300);
    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/login",
        None,
        &json!({ "type": "m.login.password", "identifier": { "user": oversized } }),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["errcode"], "M_INVALID_USERNAME");
}

/// Joining a public room needs no prior invite: the join is authorised by the
/// `public` join rule, and the room then appears in the joiner's sync.
#[tokio::test]
async fn join_public_room_without_invite_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["room_id"], room_id, "join echoes the room id");

    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    let rooms = bob_sync["rooms"].as_object().cloned().unwrap_or_default();
    assert!(
        rooms.contains_key(&room_id),
        "bob should see the joined room: {bob_sync}"
    );
}

/// The global `POST /_matrix/client/v3/join/{roomIdOrAlias}` endpoint (the one
/// Complement's `MustJoinRoom` uses) joins by room id and echoes `{room_id}`,
/// just like the room-scoped `/rooms/{id}/join`. The `server_name` query param
/// is accepted and ignored (single-server).
#[tokio::test]
async fn global_join_by_room_id_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/join/{room_id}?server_name=example.org"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["room_id"], room_id, "global join echoes the room id");
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("join"),
        "bob should be joined after the global-join call"
    );
}

/// The global join endpoint resolves room ids only. A syntactically valid room
/// *alias* (`#…`) is unresolvable (we have no room directory), so it is reported
/// as 404 `M_NOT_FOUND` — "unknown", not the 400 "malformed" a room-id parse
/// would give — matching Synapse's "No such room alias".
#[tokio::test]
async fn global_join_by_alias_is_404() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;

    // `%23` is `#`, the alias sigil; axum percent-decodes it before the handler.
    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/join/%23somealias:example.org",
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["errcode"], json!("M_NOT_FOUND"), "{body}");
}

/// Joining a room the caller is already in is idempotent: the second call
/// still `200`s with the room id, but emits no new `m.room.member` event, so
/// only one join is present in the timeline (Synapse parity — a no-op re-join
/// reuses the existing membership event rather than stacking a duplicate).
#[tokio::test]
async fn repeated_global_join_is_idempotent() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let join = || async {
        send(
            &app,
            "POST",
            &format!("/_matrix/client/v3/join/{room_id}"),
            Some(&bob_tok),
            &json!({}),
        )
        .await
    };
    let (s1, b1) = join().await;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    let (s2, b2) = join().await;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    assert_eq!(
        b2["room_id"], room_id,
        "idempotent re-join still echoes the room id"
    );

    // Read the full timeline (generous limit) and count bob's join events.
    let body = json!({
        "lists": { "all": { "ranges": [[0, 99]], "timeline_limit": 50, "required_state": [] } }
    });
    let (s, sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &body).await;
    assert_eq!(s, StatusCode::OK, "{sync}");
    let joins = sync["rooms"][&room_id]["timeline"]
        .as_array()
        .map(|tl| {
            tl.iter()
                .filter(|ev| {
                    ev["type"] == json!("m.room.member")
                        && ev["state_key"] == json!(bob_id)
                        && ev["content"]["membership"] == json!("join")
                })
                .count()
        })
        .unwrap_or_default();
    assert_eq!(
        joins, 1,
        "exactly one join event after two join calls: {sync}"
    );
}

/// A membership POST with a genuinely empty body (0 bytes) sent with an
/// `application/json` content-type must still succeed. `Option<Json<_>>` would
/// 400 this (axum runs serde on zero bytes); the `OptionalBody` extractor treats
/// it as "no body". Regression for the Complement bare `POST …/join` failure.
#[tokio::test]
async fn global_join_with_empty_body_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Empty body + application/json — exactly what Complement's bare POST sends.
    let req = Request::builder()
        .method("POST")
        .uri(format!("/_matrix/client/v3/join/{room_id}"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {bob_tok}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "empty-body join should succeed"
    );
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("join"),
    );
}

/// Kicking a user who is not in the room (no member event, or already `leave`)
/// is `403 M_FORBIDDEN`, not a silent no-op `leave` (Synapse parity).
#[tokio::test]
async fn kick_user_not_in_room_is_forbidden() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, _bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/kick"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["errcode"], json!("M_FORBIDDEN"), "{body}");
}

/// `PUT /state/{type}/` with a trailing slash (empty state key) is accepted —
/// the spec marks the trailing slash optional for an empty state key, and
/// clients use it (Complement sets `m.room.power_levels` this way).
#[tokio::test]
async fn put_state_trailing_slash_empty_key_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "PUT",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.topic/"),
        Some(&alice_tok),
        &json!({ "topic": "hello" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert!(body["event_id"].is_string(), "{body}");
}

/// `GET /state/m.room.member/{user}` returns the event *content* by default
/// (top-level `membership`); `?format=event` returns the full event (with
/// `room_id`, `sender`, nested `content`).
#[tokio::test]
async fn get_state_member_content_and_format_event() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.member/{alice_id}"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["membership"],
        json!("join"),
        "content-only shape: {body}"
    );
    // Content-only must NOT carry the event envelope (distinguishes it from
    // `?format=event`).
    assert!(
        body.get("room_id").is_none(),
        "content has no room_id: {body}"
    );
    assert!(
        body.get("sender").is_none(),
        "content has no sender: {body}"
    );

    let (s, ev) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.member/{alice_id}?format=event"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{ev}");
    assert_eq!(ev["room_id"], json!(room_id), "{ev}");
    assert_eq!(ev["sender"], json!(alice_id), "{ev}");
    assert_eq!(ev["content"]["membership"], json!("join"), "{ev}");

    // Unknown `format` is rejected (Synapse parity: enum {content, event}).
    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.member/{alice_id}?format=bogus"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["errcode"], json!("M_INVALID_PARAM"), "{body}");
}

/// A `(type, state_key)` with no current state event is `404 M_NOT_FOUND`.
#[tokio::test]
async fn get_state_unknown_key_is_404() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.name"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["errcode"], json!("M_NOT_FOUND"), "{body}");
}

/// `GET /state` returns a bare array of full state events; a freshly created
/// room contains at least `m.room.create` and the creator's `m.room.member`.
#[tokio::test]
async fn get_state_all_lists_current_state() {
    let app = router(config()).await.expect("router init");
    let (alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let events = body.as_array().expect("bare array of state events");
    assert!(
        events
            .iter()
            .any(|e| e["type"] == json!("m.room.create") && e["room_id"] == json!(room_id)),
        "state contains m.room.create with room_id: {body}"
    );
    assert!(
        events.iter().any(|e| e["type"] == json!("m.room.member")
            && e["state_key"] == json!(alice_id)
            && e["content"]["membership"] == json!("join")),
        "state contains the creator's join member event: {body}"
    );
}

/// A state event written with the empty key round-trips through the
/// trailing-slash GET form (`…/state/{type}/`) — the spec's optional trailing
/// slash, which axum routes separately. Exercises the empty-key *success* path.
#[tokio::test]
async fn get_state_trailing_slash_empty_key_round_trips() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "PUT",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.topic"),
        Some(&alice_tok),
        &json!({ "topic": "round trip" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // GET via the trailing-slash empty-key form.
    let (s, body) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/state/m.room.topic/"),
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["topic"], json!("round trip"), "{body}");
}

/// A syntactically invalid room id is `400 M_INVALID_PARAM` (both the full-state
/// and single-event read paths share this guard; this hits `get_state_all`).
#[tokio::test]
async fn get_state_malformed_room_id_is_400() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (s, body) = send(
        &app,
        "GET",
        "/_matrix/client/v3/rooms/not-a-room-id/state",
        Some(&alice_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["errcode"], json!("M_INVALID_PARAM"), "{body}");
}

/// A joined user leaving themselves moves their membership to `leave`.
#[tokio::test]
async fn self_leave_sets_membership_to_leave() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    // GET /members reflects bob's membership as `leave`.
    let (s, members) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/members"),
        None,
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{members}");
    let membership = members["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["state_key"] == json!(bob_id))
        .and_then(|ev| ev["content"]["membership"].as_str());
    assert_eq!(membership, Some("leave"), "{members}");
}

/// Read the current `m.room.member` membership of `user_id` in `room_id` via
/// the unauthenticated `GET /members` endpoint.
async fn member_membership(app: &axum::Router, room_id: &str, user_id: &str) -> Option<String> {
    let (s, members) = send(
        app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/members"),
        None,
        &json!({}),
    )
    .await;
    // A non-OK /members response means the read itself failed; surfacing that
    // as `None` (no membership) would silently mask a broken endpoint and let
    // membership assertions pass against a 404/500. Fail loudly instead.
    assert_eq!(s, StatusCode::OK, "GET /members failed: {members}");
    members["chunk"].as_array()?.iter().find_map(|ev| {
        if ev["state_key"] == json!(user_id) {
            ev["content"]["membership"].as_str().map(str::to_owned)
        } else {
            None
        }
    })
}

/// In an invite-only room, an invited user can see the room as an invite, then
/// join; after joining `GET /members` reports their membership as `join`.
#[tokio::test]
async fn invite_then_join_makes_room_visible() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Alice invites bob.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/invite"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    // Bob's sync surfaces the room (as an invite).
    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    assert!(
        bob_sync["rooms"]
            .as_object()
            .map(|r| r.contains_key(&room_id))
            .unwrap_or(false),
        "bob should see the invited room: {bob_sync}"
    );

    // Bob joins.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let membership = member_membership(&app, &room_id, &bob_id).await;
    assert_eq!(membership.as_deref(), Some("join"));
}

/// Joining an invite-only room with no prior invite is rejected.
#[tokio::test]
async fn join_invite_only_without_invite_is_403() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["errcode"], "M_FORBIDDEN");
}

/// A room creator (power 50 ≥ default kick level) can kick a joined member;
/// the target's membership becomes `leave`.
#[tokio::test]
async fn kick_sets_target_membership_to_leave() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("join")
    );

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/kick"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id, "reason": "spam" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("leave")
    );
}

/// Banning a member sets `ban` and blocks rejoin; unbanning returns them to
/// `leave` and lets them join again (public room).
#[tokio::test]
async fn ban_blocks_rejoin_until_unban() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Bob joins, then alice bans him.
    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/ban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("ban")
    );

    // A banned user cannot rejoin.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");

    // Alice unbans bob → membership back to leave.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/unban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("leave")
    );

    // Bob can join the public room again.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("join")
    );
}

/// `/createRoom` with an `invite` list emits a creator-authored invite member
/// event per listed user, so the invitee sees the room in sync without any
/// explicit `/invite` call and `GET /members` reports them as `invite`.
#[tokio::test]
async fn createroom_invite_list_invites_listed_users() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "private_chat", "invite": [bob_id] }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{room}");
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("invite"),
        "bob should be invited by createRoom"
    );

    let (s, bob_sync) = send(&app, "POST", SYNC_PATH, Some(&bob_tok), &sync_body()).await;
    assert_eq!(s, StatusCode::OK, "{bob_sync}");
    assert!(
        bob_sync["rooms"]
            .as_object()
            .map(|r| r.contains_key(&room_id))
            .unwrap_or(false),
        "bob should see the invited room in sync: {bob_sync}"
    );
}

/// `/leave` is a no-op success when the caller is not in the room. The spec
/// only defines leaving from invite/join/knock; for a user who never joined,
/// the auth rules would reject the self-leave event (rule 5.5.1), so the
/// handler must short-circuit to 200 rather than surface a 403.
#[tokio::test]
async fn leave_when_not_in_room_succeeds() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    // Bob never joined; leaving still succeeds and writes no member event.
    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/leave"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(member_membership(&app, &room_id, &bob_id).await, None);
}

/// `/leave` for a room that was never created is `404 M_NOT_FOUND`, not a
/// 200 no-op. Synapse 404s ("Not a known room") when the server is not in the
/// room and the caller has no local membership (`room_member.py:1135-1152`);
/// the no-op success is only for a room that exists but the caller never joined.
#[tokio::test]
async fn leave_on_nonexistent_room_is_404() {
    let app = router(config()).await.expect("router init");
    let (_bob_id, bob_tok) = register(&app, "bob").await;

    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/rooms/!nonexistent:example.org/leave",
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["errcode"], json!("M_NOT_FOUND"), "{body}");
}

/// `/unban` for a room that was never created is `404 M_NOT_FOUND`, not
/// `M_BAD_STATE`. In Synapse unban is internally a leave (`room_member.py:842`),
/// so it hits the same not-a-known-room 404 before the unban-specific bad-state
/// check is ever reached.
#[tokio::test]
async fn unban_on_nonexistent_room_is_404() {
    let app = router(config()).await.expect("router init");
    let (bob_id, _bob_tok) = register(&app, "bob").await;
    let (_alice_id, alice_tok) = register(&app, "alice").await;

    let (s, body) = send(
        &app,
        "POST",
        "/_matrix/client/v3/rooms/!nonexistent:example.org/unban",
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["errcode"], json!("M_NOT_FOUND"), "{body}");
}

/// `/unban` on a user who is not currently banned is rejected with 403
/// `M_BAD_STATE` and, crucially, does NOT mutate their membership. Synapse
/// raises `SynapseError(403, ..., errcode=BAD_STATE)` here
/// (`room_member.py:1000-1006`). Without the precheck the handler would emit a
/// bare `leave`, which the auth rules accept as a *kick* of a joined member — a
/// destructive wrong action.
#[tokio::test]
async fn unban_non_banned_member_is_rejected_and_does_not_kick() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    let (s, _) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/join"),
        Some(&bob_tok),
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/unban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id }),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["errcode"], json!("M_BAD_STATE"), "{body}");
    // Bob is still joined — the unban must not have kicked him.
    assert_eq!(
        member_membership(&app, &room_id, &bob_id).await.as_deref(),
        Some("join"),
        "unban of a non-banned user must not change membership"
    );
}

/// A `reason` supplied to `/unban` is copied onto the resulting member event,
/// per the spec's optional `reason` parameter.
#[tokio::test]
async fn unban_propagates_reason() {
    let app = router(config()).await.expect("router init");
    let (_alice_id, alice_tok) = register(&app, "alice").await;
    let (bob_id, bob_tok) = register(&app, "bob").await;

    let (_s, room) = send(
        &app,
        "POST",
        "/_matrix/client/v3/createRoom",
        Some(&alice_tok),
        &json!({ "preset": "public_chat" }),
    )
    .await;
    let room_id = room["room_id"].as_str().unwrap().to_owned();

    for (tok, path, body) in [
        (&bob_tok, "join", json!({})),
        (&alice_tok, "ban", json!({ "user_id": bob_id })),
    ] {
        let (s, b) = send(
            &app,
            "POST",
            &format!("/_matrix/client/v3/rooms/{room_id}/{path}"),
            Some(tok),
            &body,
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{b}");
    }

    let (s, body) = send(
        &app,
        "POST",
        &format!("/_matrix/client/v3/rooms/{room_id}/unban"),
        Some(&alice_tok),
        &json!({ "user_id": bob_id, "reason": "appeal granted" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let (s, members) = send(
        &app,
        "GET",
        &format!("/_matrix/client/v3/rooms/{room_id}/members"),
        None,
        &json!({}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{members}");
    let reason = members["chunk"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["state_key"] == json!(bob_id))
        .and_then(|ev| ev["content"]["reason"].as_str());
    assert_eq!(reason, Some("appeal granted"), "{members}");
}
