use std::sync::{Arc, Mutex};

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use neutrino_common::Config;
use neutrino_store::StorageError;
use neutrino_store_sqlite::SqliteStore;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

mod client;
mod federation;

use client::sliding_sync::SyncState;

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
        Ok(Self::from_store(config, store, tempfile))
    }

    /// Build an `AppState` around an already-open `SqliteStore`. Used by
    /// the e2e tests in `src/federation/tests.rs` to seed events via the
    /// storage trait *before* the router is mounted — `DagStore::missing_events`
    /// needs a non-flat DAG to walk, and the CSAPI `/send` endpoint
    /// currently writes events with empty `prev_events` (Phase 6 will
    /// wire those up). The caller passes the tempfile guard so the file
    /// stays alive for the lifetime of the router.
    fn from_store(config: Config, store: Arc<SqliteStore>, tempfile: NamedTempFile) -> Self {
        let sync_state = Arc::new(SyncState::new(store.clone()));
        let app = App {
            store,
            sync_state,
            keys: None,
            config,
            _db_tempfile: tempfile,
        };
        AppState(Arc::new(Mutex::new(app)))
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
    Ok(build_router(config, state))
}

/// Test-only constructor that mounts the same router over an externally-
/// provided `SqliteStore`. The tempfile guard keeps the underlying db
/// file alive — drop it (e.g. when the test scope ends) and the file is
/// removed.
///
/// Used by `src/federation/tests.rs` to seed events via the
/// `StorageBackend` trait directly before the HTTP layer observes them;
/// the CSAPI `/send` path currently writes flat DAGs (Phase 6 will fix
/// this), which prevents the DAG-walk tests from exercising
/// `DagStore::missing_events` over a real chain.
#[cfg(test)]
pub(crate) fn router_with_store(
    config: Config,
    store: Arc<SqliteStore>,
    tempfile: NamedTempFile,
) -> Router {
    let state = AppState::from_store(config.clone(), store, tempfile);
    build_router(config, state)
}

fn build_router(config: Config, state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .merge(client::routes(&config))
        .merge(federation::routes())
        .fallback(default_fallback)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn root() -> &'static str {
    "Hello, World!"
}

fn error_response(status: StatusCode, errcode: &str, error: &str) -> axum::response::Response {
    (status, Json(json!({"errcode": errcode, "error": error}))).into_response()
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
