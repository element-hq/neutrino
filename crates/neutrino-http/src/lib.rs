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
use neutrino_common::{Config, Event, ROOM_VERSION_ID};
use neutrino_state::event_id::EventBuilder;
use neutrino_state::provider::InMemoryStateProvider;
use neutrino_state::room_core::{Effect, RoomCore};
use neutrino_state::{CoreError, FormatError};
use neutrino_store::{RoomStore, StateStore, StorageError};
use neutrino_store_sqlite::SqliteStore;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
use ruma::{OwnedRoomId, OwnedUserId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;
use tracing::info;

mod federation;
mod legacy_sync;
mod membership;
mod messages;
mod room_actor;
mod sliding_sync;

#[cfg(feature = "multi-user-shim")]
mod multi_user;

use federation::client::{FederationClient, ReqwestFetcher};
use federation::gapfill::MissingEventsFetcher;
use room_actor::{RoomActorError, RoomRegistry};
use sliding_sync::{SyncError, SyncState};

struct App {
    store: Arc<SqliteStore>,
    /// Per-room state-machine actors. CSAPI writes go through here so they
    /// are DAG-linked, auth-checked, and state-resolved.
    room_registry: Arc<RoomRegistry>,
    /// In-process poke to the inbound staging worker. The `/send` handler sends
    /// the room id of each freshly-staged PDU; the worker spawns or wakes that
    /// room's drain task. Best-effort (`try_send`): a full buffer just means the
    /// worker is already aware the room has work. Dropping the owning `AppState`
    /// drops this sender, which shuts the worker down (see `federation::worker`).
    /// INVARIANT: this is the *only* long-lived holder of the poke sender — the
    /// worker tasks must never hold a clone, or the channel would never close
    /// and the worker (plus its `store`/`registry` `Arc`s) would leak.
    worker_poke: mpsc::Sender<OwnedRoomId>,
    sync_state: Arc<SyncState<SqliteStore>>,
    keys: Option<Value>,
    config: Config,
    /// Kept alive for the lifetime of the server; `NamedTempFile::drop`
    /// removes the underlying db file. Held here so the path stays valid
    /// for as long as `store` is in use.
    _db_tempfile: NamedTempFile,
    /// Testing-only access-token → user map (multi-user shim). See
    /// `multi_user`. Absent from the production single-user build.
    #[cfg(feature = "multi-user-shim")]
    user_tokens: Arc<Mutex<multi_user::UserTokens>>,
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

/// Per-request caller identity. Yields the authenticated user.
///
/// - feature `multi-user-shim` ON: resolves `Authorization: Bearer <token>`
///   against the in-memory token map; 401 on missing/unknown.
/// - feature OFF: ignores any token and yields the single configured user
///   (`config.user_id()`), exactly matching today's single-user behaviour.
pub struct AuthUser(pub OwnedUserId);

impl axum::extract::FromRequestParts<AppState> for AuthUser {
    type Rejection = axum::response::Response;

    #[cfg(feature = "multi-user-shim")]
    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let tokens = lock_app(state).user_tokens.clone();
        match multi_user::resolve(&parts.headers, &tokens) {
            Ok(user) => Ok(AuthUser(user)),
            Err(multi_user::TokenError::Missing) => Err(error_response(
                StatusCode::UNAUTHORIZED,
                "M_MISSING_TOKEN",
                "Missing access token",
            )),
            Err(multi_user::TokenError::Unknown) => Err(error_response(
                StatusCode::UNAUTHORIZED,
                "M_UNKNOWN_TOKEN",
                "Unrecognised access token",
            )),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let user_id = lock_app(state).config.user_id();
        match user_id.parse() {
            Ok(u) => Ok(AuthUser(u)),
            Err(e) => Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            )),
        }
    }
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
        Ok(Self::from_store(config, store, tempfile))
    }

    /// Build an `AppState` around an already-open `SqliteStore`. Used by
    /// the e2e tests in `src/federation/tests.rs` to seed events via the
    /// storage trait *before* the router is mounted — `DagStore::missing_events`
    /// needs specific multi-event DAG shapes (gaps, branches) that are
    /// simplest to construct directly through the trait rather than via the
    /// CSAPI write path. The caller passes the tempfile guard so the file
    /// stays alive for the lifetime of the router.
    pub(crate) fn from_store(
        config: Config,
        store: Arc<SqliteStore>,
        tempfile: NamedTempFile,
    ) -> Self {
        // Production gap-fill fetcher: a reqwest client resolving peers as
        // `http://{server_name}` (trusted mesh). Built here rather than shared
        // with the sender pool — a second connection pool is cheap, and the two
        // clients target different peer sets (inbound gap-fill origins vs
        // outbound destinations). It's moved straight into the worker below
        // (which owns the only `Arc<dyn MissingEventsFetcher>`), so `App` holds
        // no fetcher field.
        let client = Arc::new(FederationClient::new(config.server_name.clone()));
        let fetcher: Arc<dyn MissingEventsFetcher> = Arc::new(ReqwestFetcher::new(client));
        Self::from_store_with_fetcher(config, store, tempfile, fetcher)
    }

    /// Like [`AppState::from_store`] but with an explicit gap-fill `fetcher`.
    /// The federation gap-fill tests inject a deterministic stub here instead
    /// of the reqwest client (which would otherwise reach the network).
    fn from_store_with_fetcher(
        config: Config,
        store: Arc<SqliteStore>,
        tempfile: NamedTempFile,
        fetcher: Arc<dyn MissingEventsFetcher>,
    ) -> Self {
        let sync_state = Arc::new(SyncState::new(store.clone()));
        let room_registry = Arc::new(RoomRegistry::new(store.clone(), config.server_name.clone()));
        // Spawn the inbound staging worker bound to this store/registry/fetcher.
        // It runs wherever the router does (production `serve` and the e2e
        // tests), enumerates any leftover staged rows on startup, and stops when
        // this `AppState` is dropped (the `worker_poke` sender drops with it).
        let worker_poke = federation::worker::spawn(store.clone(), room_registry.clone(), fetcher);
        let app = App {
            store,
            room_registry,
            worker_poke,
            sync_state,
            keys: None,
            config,
            _db_tempfile: tempfile,
            #[cfg(feature = "multi-user-shim")]
            user_tokens: Arc::new(Mutex::new(multi_user::UserTokens::new())),
        };
        AppState(Arc::new(Mutex::new(app)))
    }

    /// The shared storage handle. Used by `serve` to wire the outbound
    /// federation sender pool to the same `SqliteStore` the router serves from.
    fn store(&self) -> Arc<SqliteStore> {
        lock_app(self).store.clone()
    }

    /// This homeserver's name, sent as the `origin` on outbound transactions.
    fn server_name(&self) -> String {
        lock_app(self).config.server_name.clone()
    }

    /// The configured cap on concurrent outbound federation transactions.
    fn outbound_concurrency(&self) -> usize {
        lock_app(self).config.outbound_concurrency
    }
}

pub async fn serve(listener: TcpListener, config: Config) -> Result<(), StartupError> {
    let state = AppState::new(config).await?;
    // Start draining the federation outbox before serving. Outbox rows survive
    // restarts, so this is also the "retry on restart" path — startup
    // enumeration resumes delivery of anything left undelivered.
    federation::sender::spawn(
        state.store(),
        state.server_name(),
        state.outbound_concurrency(),
    );
    let router = build_router(state);
    axum::serve(listener, router)
        .await
        .map_err(StartupError::Tempfile)?;
    Ok(())
}

pub async fn router(config: Config) -> Result<Router, StartupError> {
    let state = AppState::new(config).await?;
    Ok(build_router(state))
}

/// Test-only constructor that mounts the same router over an externally-
/// provided `SqliteStore`. The tempfile guard keeps the underlying db
/// file alive — drop it (e.g. when the test scope ends) and the file is
/// removed.
///
/// Used by `src/federation/tests.rs` to seed events via the
/// `StorageBackend` trait directly before the HTTP layer observes them —
/// the DAG-walk tests need arbitrary chain/gap shapes that are simplest to
/// construct through the trait rather than via the CSAPI write path.
#[cfg(test)]
pub(crate) fn router_with_store(
    config: Config,
    store: Arc<SqliteStore>,
    tempfile: NamedTempFile,
) -> Router {
    let state = AppState::from_store(config, store, tempfile);
    build_router(state)
}

/// Like [`router_with_store`] but with an injected gap-fill `fetcher`. The
/// inbound `/send` gap-fill tests use this to supply a deterministic
/// [`MissingEventsFetcher`] stub (the default reqwest fetcher would reach the
/// network for an unreachable test `origin`).
#[cfg(test)]
pub(crate) fn router_with_store_and_fetcher(
    config: Config,
    store: Arc<SqliteStore>,
    tempfile: NamedTempFile,
    fetcher: Arc<dyn MissingEventsFetcher>,
) -> Router {
    let state = AppState::from_store_with_fetcher(config, store, tempfile, fetcher);
    build_router(state)
}

fn build_router(state: AppState) -> Router {
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
        .route("/_matrix/client/v3/profile/{user_id}", get(profile))
        .route(
            "/_matrix/client/v3/user/{user_id}/account_data/{account_data_type}",
            get(get_account_data),
        )
        .route("/_matrix/client/v3/room_keys/version", get(get_room_keys))
        .route("/_matrix/client/v3/createRoom", post(create_room))
        .route("/_matrix/client/v3/rooms/{room_id}/members", get(members))
        .route(
            "/_matrix/client/v3/rooms/{room_id}/messages",
            get(messages::get_messages),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/send/{type}/{msg_id}",
            put(put_event),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state",
            get(get_state_all),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}/{state_key}",
            put(put_state).get(get_state_event),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}",
            put(put_state_empty_key).get(get_state_event_empty_key),
        )
        // Empty state key may be sent with a trailing slash; the spec marks it
        // optional ("when an empty string, the trailing slash on this endpoint
        // is optional"), and clients (e.g. Complement setting power_levels) use
        // it. axum treats `…/state/{type}/` as a path distinct from
        // `…/state/{type}`, so it needs its own route.
        .route(
            "/_matrix/client/v3/rooms/{room_id}/state/{type}/",
            put(put_state_empty_key).get(get_state_event_empty_key),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/join",
            post(membership::join),
        )
        .route(
            "/_matrix/client/v3/join/{room_id_or_alias}",
            post(membership::join_by_id_or_alias),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/leave",
            post(membership::leave),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/invite",
            post(membership::invite),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/kick",
            post(membership::kick),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/ban",
            post(membership::ban),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/unban",
            post(membership::unban),
        )
        .route("/_matrix/client/v3/pushers/set", post(pushers_set))
        .route("/_matrix/client/v3/capabilities", get(get_capabilities))
        .route(
            "/_matrix/federation/v1/get_missing_events/{room_id}",
            post(federation::get_missing_events::handle),
        )
        .route(
            "/_matrix/federation/v1/send/{txn_id}",
            put(federation::send::handle),
        )
        .route(
            "/_matrix/federation/v1/backfill/{room_id}",
            get(federation::backfill::handle),
        )
        .route(
            "/_matrix/federation/v1/make_join/{room_id}/{user_id}",
            get(federation::make_join::handle),
        )
        .route(
            "/_matrix/federation/v2/send_join/{room_id}/{event_id}",
            put(federation::send_join::handle),
        )
        .route(
            "/_matrix/federation/v2/invite/{room_id}/{event_id}",
            put(federation::invite::handle),
        )
        .fallback(default_fallback)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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

    let device_id = body
        .0
        .pointer("/device_id")
        .and_then(|v| v.as_str())
        .unwrap_or("DEVICEID")
        .to_string();

    #[cfg(feature = "multi-user-shim")]
    {
        let (tokens, server_name, default_user_id) = {
            let app = lock_app(&state.0);
            (
                app.user_tokens.clone(),
                app.config.server_name.clone(),
                app.config.user_id(),
            )
        };
        // The UIA flow is stateless — this shim stores no per-session state, so
        // the client must resend `username` on the auth-completion request (as
        // Complement does); absent here, `provision` falls back to the default
        // user. `localpart_of` lets a full MXID through too, matching `/login`.
        let requested = body
            .0
            .pointer("/username")
            .and_then(|v| v.as_str())
            .map(localpart_of);
        match multi_user::provision(
            &tokens,
            &server_name,
            &default_user_id,
            requested.as_deref(),
        ) {
            Ok((user_id, token)) => (
                StatusCode::OK,
                Json(json!({
                    "user_id": user_id,
                    "access_token": token,
                    "home_server": server_name,
                    "device_id": device_id,
                })),
            ),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "errcode": "M_INVALID_USERNAME", "error": e })),
            ),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    {
        let app = lock_app(&state.0);
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
}

async fn post_login(
    state: State<AppState>,
    #[cfg(feature = "multi-user-shim")] body: Json<Value>,
) -> (StatusCode, Json<Value>) {
    info!("Logged in");

    #[cfg(feature = "multi-user-shim")]
    {
        let (tokens, server_name, default_user_id) = {
            let app = lock_app(&state.0);
            (
                app.user_tokens.clone(),
                app.config.server_name.clone(),
                app.config.user_id(),
            )
        };
        let requested = body
            .0
            .pointer("/identifier/user")
            .or_else(|| body.0.pointer("/user"))
            .and_then(|v| v.as_str())
            .map(localpart_of);
        match multi_user::provision(
            &tokens,
            &server_name,
            &default_user_id,
            requested.as_deref(),
        ) {
            Ok((user_id, token)) => (
                StatusCode::OK,
                Json(json!({
                    "user_id": user_id,
                    "access_token": token,
                    "home_server": server_name,
                    "device_id": "DEVICEID",
                })),
            ),
            // Mirror `/register`: a malformed identifier is a 400, not a 200
            // carrying a token that was never inserted into the map (which would
            // then 401 on the very next authenticated request).
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "errcode": "M_INVALID_USERNAME", "error": e })),
            ),
        }
    }

    #[cfg(not(feature = "multi-user-shim"))]
    {
        let app = lock_app(&state.0);
        (
            StatusCode::OK,
            Json(json!({
                "user_id": app.config.user_id(),
                "access_token": "syt_1234567890abcdef",
                "home_server": app.config.server_name,
                "device_id": "DEVICEID",
            })),
        )
    }
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
    AuthUser(user_id): AuthUser,
    query: Query<HashMap<String, String>>,
    body: Json<Value>,
) -> axum::response::Response {
    let body_value = body.0;
    let sync_state = lock_app(&state.0).sync_state.clone();

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
        Err(SyncError::EventConversion(e)) => error_response(
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

/// Extract the localpart from a login identifier that may be a full MXID
/// (`@bob:server`) or already a bare localpart (`bob`).
#[cfg(feature = "multi-user-shim")]
fn localpart_of(identifier: &str) -> String {
    if let Some(rest) = identifier.strip_prefix('@') {
        rest.split_once(':')
            .map(|(lp, _)| lp)
            .unwrap_or(rest)
            .to_owned()
    } else {
        identifier.to_owned()
    }
}

fn error_response(status: StatusCode, errcode: &str, error: &str) -> axum::response::Response {
    (status, Json(json!({"errcode": errcode, "error": error}))).into_response()
}

async fn keys_query(state: State<AppState>, body: Json<Value>) -> Json<Value> {
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

async fn keys_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
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

async fn device_signing_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
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

async fn signatures_upload(state: State<AppState>, body: Json<Value>) -> Json<Value> {
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

async fn profile(axum::extract::Path(_user_id): axum::extract::Path<String>) -> Json<Value> {
    Json(json!({
        "displayname": "Alice",
    }))
}

async fn get_account_data(
    axum::extract::Path((_user_id, _account_data_type)): axum::extract::Path<(String, String)>,
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

async fn create_room(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    body: Json<Value>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();

    // Build the spec-mandated initial-state batch (create → join →
    // power_levels → join_rules, plus name/topic when requested). Each event
    // is built on the running heads, server-side `auth_events` selected, and
    // verified through `RoomCore::apply` before it's persisted — see
    // `build_initial_events`. Any failure here is a server bug (the events are
    // server-authored), so it maps to 500.
    let (create, initial) = match build_initial_events(&sender, &body.0) {
        Ok(batch) => batch,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let room_id = create.room_id.clone();

    // SqliteStore requires `create_room` to register the room before any
    // `persist_event` calls succeed. The create event lands via the trait's
    // dedicated path; the rest of the batch comes through alongside as
    // `initial_events` so the whole thing is one transaction. The chain is
    // linear, so `create_room`'s `last()` forward-extremity seeding is correct.
    if let Err(e) = store.create_room(&create, &initial).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        );
    }

    (StatusCode::OK, Json(json!({"room_id": room_id}))).into_response()
}

/// Error building the createRoom initial-state batch. Every event is
/// server-authored, so any failure is an internal bug rather than client
/// input — all variants surface as 500.
#[derive(Debug, thiserror::Error)]
enum CreateRoomError {
    #[error("building initial event: {0}")]
    Build(#[from] FormatError),
    #[error("initial event rejected by auth rules: {0}")]
    Apply(#[from] CoreError),
    /// `apply_pdu` produced no `Persist` effect. createRoom events are
    /// server-authored on valid heads, so they neither reject nor no-op —
    /// unreachable in practice, surfaced rather than panicked on.
    #[error("initial event produced no persist effect")]
    NotApplied,
}

/// Pull the persisted (`auth_events`-stamped) event out of `apply_pdu`'s
/// effects. createRoom always accepts its own server-authored events, so the
/// `Persist` is always present; its absence is an internal bug.
fn persisted_event(effects: Vec<Effect>) -> Result<Arc<Event>, CreateRoomError> {
    effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Persist { event } => Some(event),
            Effect::UpdateCurrentState(_) => None,
        })
        .ok_or(CreateRoomError::NotApplied)
}

/// Build the spec-mandated initial-state sequence for a new room, returning
/// the create event and the ordered tail (join → power_levels → join_rules →
/// history_visibility, then optional name/topic). Drives a transient
/// `RoomCore` + in-memory provider so every event is built on the real heads,
/// carries server-computed `auth_events`, and is auth-checked via `apply`
/// before it's persisted. The chain is linear (each event sits on the single
/// current head), so the last event is the sole head of both DAGs.
///
/// `join_rules` is taken from the request's `preset` (or `visibility` when no
/// preset is given — see [`join_rule_for`]); `history_visibility` is `shared`,
/// which every standard preset agrees on. Aliases, guest access, and arbitrary
/// `initial_state` / `power_level_content_override` overrides are not honoured.
fn build_initial_events(
    sender: &OwnedUserId,
    body: &Value,
) -> Result<(Event, Vec<Event>), CreateRoomError> {
    // create is special: no parents, room_id derived from its own event_id.
    let create = EventBuilder::new(sender.clone(), "m.room.create".to_owned())
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()?;

    let mut room = RoomCore::new(create.room_id.clone());
    let mut provider = InMemoryStateProvider::new();
    room.apply_pdu(create.clone(), &provider)?;
    provider.insert(Arc::new(create.clone()));

    let mut initial: Vec<Event> = Vec::new();
    let mut add =
        |event_type: &str, state_key: &str, content: Value| -> Result<(), CreateRoomError> {
            let ev = room.build_local_event(
                sender.clone(),
                event_type.to_owned(),
                Some(state_key.to_owned()),
                content,
            )?;
            // apply_pdu is the sole authority for `auth_events`, stamping them
            // onto the event it hands back via `Persist` — persist *that*, not
            // the pre-apply build output (which has empty auth_events).
            let stored = persisted_event(room.apply_pdu(ev, &provider)?)?;
            provider.insert(stored.clone());
            initial.push((*stored).clone());
            Ok(())
        };

    add(
        "m.room.member",
        sender.as_str(),
        json!({ "membership": "join" }),
    )?;
    add("m.room.power_levels", "", default_power_levels())?;
    add(
        "m.room.join_rules",
        "",
        json!({ "join_rule": join_rule_for(body) }),
    )?;
    add(
        "m.room.history_visibility",
        "",
        json!({ "history_visibility": "shared" }),
    )?;
    if let Some(n) = body.pointer("/name").and_then(|v| v.as_str()) {
        add("m.room.name", "", json!({ "name": n }))?;
    }
    if let Some(t) = body.pointer("/topic").and_then(|v| v.as_str()) {
        add("m.room.topic", "", json!({ "topic": t }))?;
    }

    // Honour the request's `invite` list (the membership follow-up to the
    // multi-user shim): emit one invite member event per well-formed, non-self
    // target, authored by the creator — who is joined with implicit MAX power,
    // so rule 5.4 accepts it. Malformed entries are skipped rather than failing
    // room creation (test server, best-effort). `is_direct` is propagated onto
    // the invite content when the request sets it.
    if let Some(invitees) = body.pointer("/invite").and_then(Value::as_array) {
        let is_direct = body.pointer("/is_direct").and_then(Value::as_bool) == Some(true);
        for entry in invitees {
            let Some(target) = entry.as_str() else {
                continue;
            };
            if target == sender.as_str() || OwnedUserId::try_from(target).is_err() {
                continue;
            }
            let mut content = json!({ "membership": "invite" });
            if is_direct {
                content["is_direct"] = json!(true);
            }
            add("m.room.member", target, content)?;
        }
    }

    Ok((create, initial))
}

/// Spec-default `m.room.power_levels` content for a new room. Room v12 makes
/// the creator implicitly all-powerful (and rule 10.4 forbids naming a creator
/// in `users`), so `users` is left empty rather than pinning the creator at a
/// numeric level.
fn default_power_levels() -> Value {
    json!({
        "ban": 50,
        "events": {
            "m.room.name": 50,
            "m.room.power_levels": 100,
            "m.room.history_visibility": 100,
            "m.room.canonical_alias": 50,
            "m.room.tombstone": 100,
            "m.room.server_acl": 100,
        },
        "events_default": 0,
        "invite": 0,
        "kick": 50,
        "redact": 50,
        "state_default": 50,
        "users": {},
        "users_default": 0,
        "notifications": { "room": 50 },
    })
}

/// Resolve the `join_rule` for a new room from the createRoom request, per
/// <https://spec.matrix.org/v1.18/client-server-api/#post_matrixclientv3createroom>.
/// An explicit `preset` wins; otherwise it's derived from `visibility`
/// (`public` ⇒ `public_chat`, else `private_chat`). Only `public_chat` opens
/// the room (`public`); `private_chat` / `trusted_private_chat` (and any
/// unrecognised preset) stay invite-only. The `trusted_private_chat`
/// invitee-power bump is not modelled, though the `invite` list itself is
/// honoured by [`build_initial_events`].
fn join_rule_for(body: &Value) -> &'static str {
    let is_public = match body.pointer("/preset").and_then(Value::as_str) {
        Some(preset) => preset == "public_chat",
        None => body.pointer("/visibility").and_then(Value::as_str) == Some("public"),
    };
    if is_public { "public" } else { "invite" }
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

/// `PUT /rooms/{room}/send/{type}/{txn}` — a message (non-state) event.
async fn put_event(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type, _msg_id)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(&state.0, sender, room_id, event_type, None, body.0).await
}

/// `PUT /rooms/{room}/state/{type}/{stateKey}` — a state event.
async fn put_state(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type, state_key)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(
        &state.0,
        sender,
        room_id,
        event_type,
        Some(state_key),
        body.0,
    )
    .await
}

/// `PUT /rooms/{room}/state/{type}` — a state event with the empty state key
/// (the common case for `m.room.name`, `m.room.topic`, …).
async fn put_state_empty_key(
    state: State<AppState>,
    AuthUser(sender): AuthUser,
    axum::extract::Path((room_id, event_type)): axum::extract::Path<(String, String)>,
    body: Json<Value>,
) -> axum::response::Response {
    send_via_actor(
        &state.0,
        sender,
        room_id,
        event_type,
        Some(String::new()),
        body.0,
    )
    .await
}

/// `GET /rooms/{room}/state` — every current state event, as a bare array of
/// full (enriched) events. No auth/visibility gating (embedded trusted surface;
/// matches `members`).
async fn get_state_all(
    state: State<AppState>,
    axum::extract::Path(room_id): axum::extract::Path<String>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    let rid = match OwnedRoomId::try_from(room_id.as_str()) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };
    let map = match store.current_room_state(&rid).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    let events: Vec<Raw<AnyTimelineEvent>> =
        map.values().map(Raw::<AnyTimelineEvent>::from).collect();
    (StatusCode::OK, Json(events)).into_response()
}

/// `GET /rooms/{room}/state/{type}/{stateKey}` — the current state event. The
/// default response is the event `content`; `?format=event` returns the full
/// enriched event.
async fn get_state_event(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type, state_key)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    state_event_response(&state.0, &room_id, &event_type, &state_key, &query.0).await
}

/// `GET /rooms/{room}/state/{type}` (and the trailing-slash form) — as
/// [`get_state_event`] with the empty state key.
async fn get_state_event_empty_key(
    state: State<AppState>,
    axum::extract::Path((room_id, event_type)): axum::extract::Path<(String, String)>,
    query: Query<HashMap<String, String>>,
) -> axum::response::Response {
    state_event_response(&state.0, &room_id, &event_type, "", &query.0).await
}

async fn state_event_response(
    state: &AppState,
    room_id: &str,
    event_type: &str,
    state_key: &str,
    query: &HashMap<String, String>,
) -> axum::response::Response {
    let store = lock_app(state).store.clone();
    let rid = match OwnedRoomId::try_from(room_id) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };
    // `format` is the spec enum {content, event}; reject anything else with 400
    // (Synapse parses it with `allowed_values=["content","event"]`) rather than
    // silently treating an unknown value as the default.
    let format = query.get("format").map(String::as_str).unwrap_or("content");
    if format != "content" && format != "event" {
        return error_response(
            StatusCode::BAD_REQUEST,
            "M_INVALID_PARAM",
            &format!("Unknown format: {format}"),
        );
    }
    let event = match store.current_state_event(&rid, event_type, state_key).await {
        Ok(Some(e)) => e,
        Ok(None) => {
            return error_response(StatusCode::NOT_FOUND, "M_NOT_FOUND", "Event not found.");
        }
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };
    if format == "event" {
        (StatusCode::OK, Json(Raw::<AnyTimelineEvent>::from(&event))).into_response()
    } else {
        match serde_json::from_str::<Value>(event.content.get()) {
            Ok(content) => (StatusCode::OK, Json(content)).into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            ),
        }
    }
}

/// Shared body for the CSAPI write endpoints: build + apply + persist the
/// event through the room's actor (DAG-linked, auth-checked, state-resolved)
/// and return `{ event_id }`. `state_key = None` for a message event,
/// `Some(_)` for a state event.
async fn send_via_actor(
    state: &AppState,
    sender: OwnedUserId,
    room_id: String,
    event_type: String,
    state_key: Option<String>,
    content: Value,
) -> axum::response::Response {
    let registry = lock_app(state).room_registry.clone();
    let parsed_room_id: OwnedRoomId = match room_id.parse() {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    match registry
        .send_event(&parsed_room_id, sender, event_type, state_key, content)
        .await
    {
        Ok(event) => (StatusCode::OK, Json(json!({ "event_id": event.event_id }))).into_response(),
        Err(e) => room_actor_response(e),
    }
}

/// Map a [`RoomActorError`] to a CSAPI error response.
fn room_actor_response(e: RoomActorError) -> axum::response::Response {
    let (status, code) = match &e {
        RoomActorError::UnknownRoom => (StatusCode::NOT_FOUND, "M_NOT_FOUND"),
        RoomActorError::Build(_) => (StatusCode::BAD_REQUEST, "M_BAD_JSON"),
        RoomActorError::Apply(_) | RoomActorError::Rejected => {
            (StatusCode::FORBIDDEN, "M_FORBIDDEN")
        }
        RoomActorError::Storage(_) | RoomActorError::NotApplied | RoomActorError::ActorGone => {
            (StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN")
        }
    };
    error_response(status, code, &e.to_string())
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

#[cfg(test)]
mod tests {
    use super::join_rule_for;
    use serde_json::json;

    #[test]
    fn join_rule_explicit_preset_wins_over_visibility() {
        // An explicit preset overrides visibility entirely.
        assert_eq!(
            join_rule_for(&json!({ "preset": "public_chat", "visibility": "private" })),
            "public"
        );
        assert_eq!(
            join_rule_for(&json!({ "preset": "private_chat", "visibility": "public" })),
            "invite"
        );
        assert_eq!(
            join_rule_for(&json!({ "preset": "trusted_private_chat" })),
            "invite"
        );
    }

    #[test]
    fn join_rule_derived_from_visibility_when_no_preset() {
        assert_eq!(join_rule_for(&json!({ "visibility": "public" })), "public");
        assert_eq!(join_rule_for(&json!({ "visibility": "private" })), "invite");
    }

    #[test]
    fn join_rule_defaults_to_invite() {
        // No preset, no visibility ⇒ private (invite-only), and an
        // unrecognised preset is treated conservatively as invite-only.
        assert_eq!(join_rule_for(&json!({})), "invite");
        assert_eq!(
            join_rule_for(&json!({ "preset": "weird_preset" })),
            "invite"
        );
    }
}
