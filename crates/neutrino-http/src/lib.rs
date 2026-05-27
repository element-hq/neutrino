use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};
use neutrino_common::{Config, ROOM_VERSION_ID};
use neutrino_store::{Event, EventStore, RoomStore, StateStore, StorageError};
use neutrino_store_sqlite::SqliteStore;
use rand::{Rng, distr::Alphanumeric};
use ruma::api::client::sync::sync_events::v5;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

mod legacy_sync;
mod sliding_sync;

use sliding_sync::{SyncError, SyncState};

struct App {
    store: Arc<SqliteStore>,
    sync_state: Arc<SyncState<SqliteStore>>,
    keys: Option<Value>,
    config: Config,
    /// Kept alive for the lifetime of the server; `NamedTempFile::drop`
    /// removes the underlying db file. Held here so the path stays valid
    /// for as long as `store` is in use.
    _db_tempfile: NamedTempFile,
}

#[derive(Clone)]
pub struct AppState(Arc<Mutex<App>>);

/// Lock `App`, recovering from `PoisonError` by taking the inner value.
/// `App`'s fields hold no invariants that can be broken by a panic
/// mid-write (each field is independently meaningful), so the poison
/// flag carries no useful signal — `.unwrap()` would crash every
/// subsequent request once any handler ever panicked under the lock.
fn lock_app(state: &AppState) -> std::sync::MutexGuard<'_, App> {
    state.0.lock().unwrap_or_else(|e| e.into_inner())
}

/// Errors `AppState::new` (and therefore `router` / `serve`) can surface.
/// Distinct from `std::io::Error` because the failure modes are storage,
/// not networking.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("creating db tempfile: {0}")]
    Tempfile(#[from] std::io::Error),
    #[error("opening sqlite store: {0}")]
    Store(#[from] StorageError),
}

impl AppState {
    async fn new(config: Config) -> Result<Self, StartupError> {
        // File-backed SQLite on a tempfile. `SqliteStore::open_in_memory`
        // exists but its shared-cache mode is unsafe for the concurrent
        // reader+writer workloads sliding-sync long-polls drive — see
        // the `open_in_memory` doc-comment.
        let tempfile = NamedTempFile::new()?;
        let store = Arc::new(SqliteStore::open(tempfile.path()).await?);
        let sync_state = Arc::new(SyncState::new(store.clone()));
        let app = App {
            store,
            sync_state,
            keys: None,
            config,
            _db_tempfile: tempfile,
        };
        Ok(AppState(Arc::new(Mutex::new(app))))
    }
}

pub async fn serve(listener: TcpListener, config: Config) -> Result<(), StartupError> {
    let router = router(config).await?;
    axum::serve(listener, router)
        .await
        .map_err(StartupError::Tempfile)?;
    Ok(())
}

pub async fn router(config: Config) -> Result<Router, StartupError> {
    let state = AppState::new(config.clone()).await?;
    let user_id = config.user_id();

    let router = Router::new()
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
        .route("/_matrix/client/v3/sync", get(legacy_sync::handle))
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
        .route("/_matrix/client/v3/capabilities", get(get_capabilities))
        .fallback(default_fallback)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    Ok(router)
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
        "unstable_features": {
            "org.matrix.simplified_msc3575": true,
            "org.matrix.msc4222": true,
        },
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

async fn post_login(state: State<AppState>) -> Json<Value> {
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

/// MSC4186 sliding-sync entrypoint. The actual work is in
/// `sliding_sync::handle`; this wrapper handles the HTTP/JSON edge:
/// - assembles a `v5::Request` from the JSON body plus query string (`pos`,
///   `timeout` live on the URL per ruma's annotations);
/// - clones the `Arc<SyncState>` out from under the std-mutex'd `AppState`
///   so we don't hold a `!Send` lock across `.await`;
/// - maps `SyncError` to the spec's HTTP / errcode shape.
async fn sync(
    state: State<AppState>,
    query: Query<HashMap<String, String>>,
    body: Json<Value>,
) -> axum::response::Response {
    let body_value = body.0;
    let (sync_state, user_id_str) = {
        let app = lock_app(&state.0);
        (app.sync_state.clone(), app.config.user_id())
    };

    let user_id: ruma::OwnedUserId = match user_id_str.as_str().try_into() {
        Ok(u) => u,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    let req = match build_sync_request(&query.0, body_value) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, "M_BAD_JSON", &e.to_string()),
    };

    match sliding_sync::handle(&sync_state, &user_id, req).await {
        Ok(resp) => (StatusCode::OK, Json(SyncResponseWire::from(resp))).into_response(),
        Err(SyncError::UnknownPos) => {
            error_response(StatusCode::BAD_REQUEST, "M_UNKNOWN_POS", "Unknown position")
        }
        Err(SyncError::BadRequest(msg)) => {
            error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", msg)
        }
        Err(SyncError::Storage(e)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Build a `v5::Request` from the JSON body plus the `pos` and `timeout`
/// query parameters. The body fields (`conn_id`, `txn_id`, `lists`,
/// `room_subscriptions`, `extensions`) come from JSON; the query fields
/// override whatever was in the body.
///
/// Ruma's `#[request]` macro doesn't derive plain `Deserialize` on
/// `v5::Request` (it generates an `IncomingRequest` impl meant for
/// reconstructing the full HTTP request shape). The inner field types DO
/// derive `Deserialize`, so we go through a thin wrapper that mirrors only
/// the body-side fields and copy them onto a fresh `v5::Request`.
fn build_sync_request(
    query: &HashMap<String, String>,
    body: Value,
) -> Result<v5::Request, serde_json::Error> {
    let body_typed: SyncRequestBody =
        if body.is_null() || matches!(&body, Value::Object(m) if m.is_empty()) {
            SyncRequestBody::default()
        } else {
            serde_json::from_value(body)?
        };

    let mut req = v5::Request::new();
    req.conn_id = body_typed.conn_id;
    req.txn_id = body_typed.txn_id;
    req.lists = body_typed.lists;
    req.room_subscriptions = body_typed.room_subscriptions;
    req.extensions = body_typed.extensions;

    if let Some(p) = query.get("pos") {
        req.pos = Some(p.clone());
    }
    if let Some(t) = query.get("timeout")
        && let Ok(ms) = t.parse::<u64>()
    {
        req.timeout = Some(Duration::from_millis(ms));
    }
    Ok(req)
}

/// Deserializable mirror of the *body* half of `v5::Request`. The query
/// fields (`pos`, `timeout`, `set_presence`) are handled separately.
#[derive(Default, Deserialize)]
struct SyncRequestBody {
    #[serde(default)]
    conn_id: Option<String>,
    #[serde(default)]
    txn_id: Option<String>,
    #[serde(default)]
    lists: BTreeMap<String, v5::request::List>,
    #[serde(default)]
    room_subscriptions: BTreeMap<OwnedRoomId, v5::request::RoomSubscription>,
    #[serde(default)]
    extensions: v5::request::Extensions,
}

/// Serializable mirror of `v5::Response`. Same trick — ruma's `#[response]`
/// macro doesn't derive plain `Serialize` on the outer type, but its inner
/// field types do.
#[derive(Serialize)]
struct SyncResponseWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    txn_id: Option<String>,
    pos: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    lists: BTreeMap<String, v5::response::List>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    rooms: BTreeMap<OwnedRoomId, v5::response::Room>,
    #[serde(skip_serializing_if = "extensions_is_empty")]
    extensions: v5::response::Extensions,
}

impl From<v5::Response> for SyncResponseWire {
    fn from(r: v5::Response) -> Self {
        Self {
            txn_id: r.txn_id,
            pos: r.pos,
            lists: r.lists,
            rooms: r.rooms,
            extensions: r.extensions,
        }
    }
}

fn extensions_is_empty(e: &v5::response::Extensions) -> bool {
    e.to_device.is_none()
        && e.e2ee.device_lists.is_empty()
        && e.e2ee.device_one_time_keys_count.is_empty()
        && e.e2ee.device_unused_fallback_key_types.is_none()
}

fn error_response(status: StatusCode, errcode: &str, error: &str) -> axum::response::Response {
    (status, Json(json!({"errcode": errcode, "error": error}))).into_response()
}

async fn keys_query(state: State<AppState>, body: Json<Value>) -> Json<Value> {
    info!("Received query: {:?}", body.0);

    if let Some(keys) = &lock_app(&state.0).keys {
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

    let mut app = lock_app(&state.0);
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
    let mut app = lock_app(&state.0);

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
    let mut app = lock_app(&state.0);
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

async fn create_room(state: State<AppState>, body: Json<Value>) -> axum::response::Response {
    let (store, server_name, user_id) = {
        let app = lock_app(&state.0);
        (
            app.store.clone(),
            app.config.server_name.clone(),
            app.config.user_id(),
        )
    };

    let room_id = mint_id('!', &server_name, 7);

    // v12 / MSC4242: the creator is implicit (taken from `sender`); the
    // explicit `content.creator` field that v11 carried is deprecated.
    let create_event = json!({
        "type": "m.room.create",
        "state_key": "",
        "sender": user_id,
        "room_id": room_id,
        "content": {
            "room_version": ROOM_VERSION_ID
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

    // SqliteStore requires `create_room` to register the room before any
    // `persist_event` calls succeed. The create event lands via the trait's
    // dedicated path; member-join + (optional) name come through alongside
    // as `initial_events` so the whole thing is one transaction.
    let mut stored: Vec<Event> = match events.iter().map(stored_event_from_json).collect() {
        Some(v) => v,
        None => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                "failed to build create_room PDUs",
            );
        }
    };
    let create = stored.remove(0);
    if let Err(e) = store.create_room(&create, &stored).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        );
    }

    (StatusCode::OK, Json(json!({"room_id": room_id}))).into_response()
}

async fn members(
    state: State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let rid = match ruma::OwnedRoomId::try_from(room_id.as_str()) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };
    let map = match store
        .current_state_events_of_type(&rid, "m.room.member")
        .await
    {
        Ok(m) => m,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    // Per spec (https://spec.matrix.org/v1.18/client-server-api/#get_matrixclientv3roomsroomidmembers)
    // the default response includes members of every membership; filtering
    // is opt-in via `membership` / `not_membership` query params (which we
    // don't honour — see PLAN.md non-goals).
    let chunk: Vec<Value> = map
        .into_values()
        .filter_map(|ev| serde_json::from_str::<Value>(ev.raw.get()).ok())
        .collect();
    (StatusCode::OK, Json(json!({"chunk": chunk}))).into_response()
}

async fn put_event(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type, _msg_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    let (store, server_name, user_id) = {
        let app = lock_app(&state.0);
        (
            app.store.clone(),
            app.config.server_name.clone(),
            app.config.user_id(),
        )
    };

    let event_id = mint_id('$', &server_name, 10);

    let pdu = json!({
        "room_id": room_id,
        "type": event_type,
        "sender": user_id,
        "event_id": event_id,
        "content": body.0,
        "origin_server_ts": 0,
    });
    let Some(stored) = stored_event_from_json(&pdu) else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            "failed to build PDU",
        );
    };
    if let Err(e) = store.persist_event(&stored, &[]).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        );
    }

    (StatusCode::OK, Json(json!({"event_id": event_id}))).into_response()
}

/// Parse a hand-built PDU `serde_json::Value` into a `Event`. Used by
/// the legacy CSAPI write handlers (`createRoom`, `put_event`). Returns
/// `None` on any required-field-missing path — callers built the PDU
/// themselves, so a malformed value is a programmer error, not a client
/// error.
///
/// This is **transitional code**. PR 2 introduces `EventBuilder::build`
/// which assembles the wire JSON, computes the reference hash, and
/// constructs the `Event` end-to-end. Once that lands, both CSAPI write
/// handlers migrate to it and this helper plus `mint_id` are deleted.
fn stored_event_from_json(value: &Value) -> Option<Event> {
    let event_id: OwnedEventId = value.get("event_id")?.as_str()?.try_into().ok()?;
    let room_id: OwnedRoomId = value.get("room_id")?.as_str()?.try_into().ok()?;
    let event_type = value.get("type")?.as_str()?.to_string();
    let state_key = value
        .get("state_key")
        .and_then(|s| s.as_str())
        .map(String::from);
    let sender: OwnedUserId = value.get("sender")?.as_str()?.try_into().ok()?;
    let origin_server_ts = value
        .get("origin_server_ts")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let raw: Box<RawValue> = serde_json::value::to_raw_value(value).ok()?;
    // Mirror what the storage layer's row hydration does: pull `content`,
    // `prev_events`, `prev_state_events` out of the raw value. Failures
    // surface as `None` (programmer error in the caller's hand-built
    // PDU). auth_events is empty — the legacy write path is server-
    // authored and these are wired up properly by `EventBuilder` in PR 2.
    let content = value
        .get("content")
        .and_then(|c| serde_json::value::to_raw_value(c).ok())?;
    let prev_events = value
        .get("prev_events")
        .map(|arr| {
            arr.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().and_then(|s| OwnedEventId::try_from(s).ok()))
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();
    let prev_state_events = value
        .get("prev_state_events")
        .map(|arr| {
            arr.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().and_then(|s| OwnedEventId::try_from(s).ok()))
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();
    Some(Event {
        event_id,
        room_id,
        event_type,
        state_key,
        sender,
        origin_server_ts,
        content,
        prev_events,
        prev_state_events,
        auth_events: Vec::new(),
        raw,
    })
}

async fn pushers_set() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({})))
}

async fn get_capabilities() -> Json<Value> {
    Json(json!({
        "capabilities": {
            "m.room_versions": {
                "default": "12",
                "available": { "12": "stable" }
            }
        }
    }))
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
