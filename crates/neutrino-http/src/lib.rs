use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use neutrino_common::Config;
use neutrino_sqlite::Store;
use rand::{Rng, distr::Alphanumeric};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Debug)]
struct App {
    store: Store,
    keys: Option<Value>,
    config: Config,
}

#[derive(Debug, Clone)]
pub struct AppState(Arc<Mutex<App>>);

impl AppState {
    fn new(config: Config) -> Self {
        let app = App {
            store: Store::open_in_memory(),
            keys: None,
            config,
        };
        AppState(Arc::new(Mutex::new(app)))
    }
}

pub async fn serve(listener: TcpListener, config: Config) -> Result<(), std::io::Error> {
    axum::serve(listener, router(config)).await?;
    Ok(())
}

fn router(config: Config) -> Router {
    let state = AppState::new(config.clone());
    let user_id = config.user_id();

    Router::new()
        .route("/", get(root))
        .route("/_matrix/client/versions", get(versions))
        .route(
            "/_matrix/client/{version}/login",
            get(get_login).post(post_login),
        )
        .route("/_matrix/client/{version}/register", post(post_register))
        .route(
            "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
            post(sync),
        )
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
        .route("/_matrix/client/v3/createRoom", post(create_room))
        .route("/_matrix/client/v3/rooms/{room_id}/members", get(members))
        .route(
            "/_matrix/client/v3/rooms/{room_id}/send/{type}/{msg_id}",
            put(put_event),
        )
        .route("/_matrix/client/v3/pushers/set", post(pushers_set))
        .fallback(default_fallback)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn mint_id(prefix: char, server_name: &str, len: usize) -> String {
    let chars: String = rand::rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(|c| c as char)
        .collect();
    format!("{}{}:{}", prefix, chars, server_name)
}

async fn root() -> &'static str {
    "Hello, World!"
}

async fn versions() -> Json<Value> {
    Json(json!({
        "unstable_features": {"org.matrix.simplified_msc3575": true},
        "versions": ["v1.16"]
    }))
}

async fn get_login() -> Json<Value> {
    Json(json!({
        "flows": [
            {
                "type": "m.login.password"
            }
        ],
    }))
}

async fn post_register(state: State<AppState>, body: Json<Value>) -> (StatusCode, Json<Value>) {
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

    let app = state.0.0.lock().unwrap();
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

async fn post_login(state: State<AppState>) -> Json<Value> {
    info!("Logged in");

    let app = state.0.0.lock().unwrap();
    let user_id = app.config.user_id();
    let server_name = app.config.server_name.clone();

    Json(json!({
        "user_id": user_id,
        "access_token": "syt_1234567890abcdef",
        "home_server": server_name,
        "device_id": "DEVICEID"
    }))
}

#[derive(Serialize)]
struct SyncRoom {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    initial: Option<bool>,
    required_state: Vec<Value>,
    timeline: Vec<Value>,
    membership: String,
}

async fn sync(
    state: State<AppState>,
    query: Query<HashMap<String, String>>,
    body: Json<Value>,
) -> Json<Value> {
    let pos: i64 = query.get("pos").map(|s| s.parse().unwrap()).unwrap_or(0);
    let timeout: i64 = query
        .get("timeout")
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);

    info!("Received sync with pos: {}, {:?}", pos, body.0);

    let mut lists = HashMap::<String, Value>::new();
    if let Some(lists_json) = body.0.pointer("/lists").and_then(|v| v.as_object()) {
        let app = state.0.0.lock().unwrap();
        let count = app.store.count_distinct_rooms();
        for list_name in lists_json.keys() {
            lists.insert(list_name.clone(), json!({"count": count}));
        }
    }

    let user_id = state.0.0.lock().unwrap().config.user_id();

    if !query.contains_key("pos") {
        return Json(json!({
            "pos": pos.to_string(),
            "lists": lists,
            "rooms": {},
            "extensions": {
                "e2ee": {
                    "device_one_time_keys_count": {
                        "signed_curve25519": 100
                    },
                    "device_lists": {
                        "changed": [user_id],
                        "left": []
                    },
                    "device_unused_fallback_key_types": [
                        "signed_curve25519"
                    ]
                },
                "to_device": {"next_batch": "12345"}
            }
        }));
    }

    loop {
        if !lists.is_empty() {
            let app = state.0.0.lock().unwrap();
            let rows = app.store.events_after(pos);

            let mut max_pos = pos;
            let mut events_by_room: HashMap<String, Vec<Value>> = HashMap::new();
            for (stream_ordering, room_id, event) in rows {
                max_pos = stream_ordering;
                events_by_room.entry(room_id).or_default().push(event);
            }

            if !events_by_room.is_empty() {
                info!(
                    "Returning {} rooms with events up to pos {}",
                    events_by_room.len(),
                    max_pos
                );

                let mut rooms_json = HashMap::new();
                for (room_id, events) in events_by_room {
                    let mut entry = SyncRoom {
                        name: None,
                        initial: None,
                        required_state: vec![],
                        timeline: vec![],
                        membership: "join".to_string(),
                    };

                    for event in events {
                        if event.get("type") == Some(&Value::String("m.room.create".to_string())) {
                            entry.initial = Some(true);
                        }

                        if event.get("type") == Some(&Value::String("m.room.name".to_string())) {
                            entry.name = event
                                .pointer("/content/name")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                        }

                        if event.get("state_key").is_some() {
                            entry.required_state.push(event.clone());
                        }

                        entry.timeline.push(event);
                    }

                    rooms_json.insert(room_id, entry);
                }

                let room_count = app.store.count_distinct_rooms();

                for list in lists.values_mut() {
                    list.as_object_mut()
                        .unwrap()
                        .insert("count".to_string(), json!(room_count));
                }

                return Json(json!({
                    "pos": max_pos.to_string(),
                    "lists": lists,
                    "rooms": rooms_json,
                    "extensions": {
                        "e2ee": {
                            "device_one_time_keys_count": {
                                "signed_curve25519": 100
                            },
                        },
                        "to_device": {"next_batch": "12345"}
                    }
                }));
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        if timeout == 0 {
            info!("No new events, returning empty sync");
            return Json(json!({
                "pos": pos.to_string(),
                "lists": lists,
                "rooms": {},
                "extensions": {
                    "e2ee": {
                        "device_one_time_keys_count": {
                            "signed_curve25519": 100
                        },
                    },
                    "to_device": {"next_batch": "12345"}
                }
            }));
        }
    }
}

async fn keys_query(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received query: {:?}", body.0);

    if let Some(keys) = &state.0.0.lock().unwrap().keys {
        info!(
            "Returning stored keys: {}",
            serde_json::to_string(&keys).unwrap()
        );
        Json(keys.clone())
    } else {
        Json(json!({
            "device_keys": {},
        }))
    }
}

async fn keys_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received keys upload: {:?}", body.0);

    let mut app = state.0.0.lock().unwrap();
    let body = body.0;

    if app.keys.is_none() {
        let user_id = app.config.user_id();
        app.keys = Some(json!({
            "device_keys": {
                user_id: { "DEVICEID": body.pointer("/device_keys").unwrap().clone() }
            }
        }));
    }

    Json(json!({
      "one_time_key_counts": {
        "signed_curve25519": 100
      }
    }))
}

async fn device_signing_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    let mut app = state.0.0.lock().unwrap();

    let mut body = body.0;
    body.as_object_mut().unwrap().remove("auth");

    if let Some(keys) = &mut app.keys {
        keys.as_object_mut()
            .unwrap()
            .extend(body.as_object().unwrap().clone());
    }

    Json(json!({}))
}

async fn signatures_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received signatures upload: {:?}", body.0);
    let mut app = state.0.0.lock().unwrap();
    let user_id = app.config.user_id();

    let sigs = body
        .pointer(&format!("/{0}/DEVICEID/signatures/{0}", user_id))
        .unwrap()
        .as_object()
        .unwrap()
        .clone();

    if let Some(keys) = &mut app.keys {
        info!(
            "Adding signatures to stored keys {:?}",
            serde_json::to_string(keys).unwrap()
        );
        keys.pointer_mut(&format!(
            "/device_keys/{0}/DEVICEID/signatures/{0}",
            user_id
        ))
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(sigs);
    }

    Json(json!({}))
}

async fn profile() -> Json<Value> {
    Json(json!({
        "displayname": "Alice",
    }))
}

async fn get_account_data(
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

async fn get_room_keys() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
             "errcode": "M_NOT_FOUND",
              "error": "No current backup version"
        })),
    )
}

async fn create_room(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    let mut app = state.0.0.lock().unwrap();
    let server_name = app.config.server_name.clone();
    let user_id = app.config.user_id();
    let room_id = mint_id('!', &server_name, 7);

    let create_event = json!({
        "type": "m.room.create",
        "state_key": "",
        "sender": user_id,
        "room_id": room_id,
        "content": {
            "creator": user_id,
            "room_version": "12"
        },
        "origin_server_ts": 0,
        "event_id": mint_id('$', &server_name, 10),
    });

    let join_event = json!({
        "type": "m.room.member",
        "state_key": user_id,
        "sender": user_id,
        "room_id": room_id,
        "content": {
            "membership": "join",
            "displayname": "Alice"
        },
        "origin_server_ts": 0,
        "event_id": mint_id('$', &server_name, 10),
    });

    let mut events = vec![create_event, join_event];

    if let Some(n) = body.0.pointer("/name").and_then(|v| v.as_str()) {
        let name_event = json!({
            "type": "m.room.name",
            "state_key": "",
            "sender": user_id,
            "room_id": room_id,
            "content": {
                "name": n
            },
            "origin_server_ts": 0,
            "event_id": mint_id('$', &server_name, 10),
        });
        events.push(name_event);
    }

    app.store.insert_events(&events);

    Json(json!({
        "room_id": room_id,
    }))
}

async fn members(
    state: State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
) -> Json<Value> {
    let app = state.0.0.lock().unwrap();
    let members = app.store.members_of(&room_id);

    Json(json!({
        "chunk": members
    }))
}

async fn put_event(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type, _msg_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> Json<Value> {
    let mut app = state.0.0.lock().unwrap();
    let server_name = app.config.server_name.clone();
    let user_id = app.config.user_id();

    let event_id = mint_id('$', &server_name, 10);

    app.store.insert_events(&[json!({
        "room_id": room_id,
        "type": event_type,
        "sender": user_id,
        "event_id": event_id,
        "content": body.0,
        "origin_server_ts": 0,
    })]);

    Json(json!({
        "event_id": event_id,
    }))
}

async fn pushers_set() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({})))
}

async fn default_fallback(request: axum::extract::Request) -> (StatusCode, &'static str) {
    info!(
        uri = %request.uri(),
        method = %request.method(),
        "received request to unknown route"
    );

    (
        StatusCode::NOT_FOUND,
        "The requested resource was not found.",
    )
}
