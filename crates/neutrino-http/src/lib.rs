use std::{
    collections::{BTreeMap, HashMap},
    ops::ControlFlow,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
    routing::{get, post, put},
};
use neutrino_ctl::{Command, Config, DEFAULT_DISPLAY_NAME, DiscoveryRegistry};
use neutrino_event::event_builder::EventBuilder;
use neutrino_event::{Event, EventPolicy, FormatError};
use neutrino_room::CoreError;
use neutrino_room::provider::InMemoryStateProvider;
use neutrino_room::room_core::{Effect, RoomCore};
use neutrino_store::{IdentityStore, RoomStore, StateStore, StorageError};
use neutrino_store_sqlite::SqliteStore;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
use ruma::{OwnedRoomId, OwnedUserId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::trace::TraceLayer;
use tracing::{Span, error, info, info_span, warn};

mod federation;
mod legacy_sync;
mod membership;
mod messages;
mod sliding_sync;

#[cfg(feature = "multi-user-shim")]
mod multi_user;

use federation::client::{FederationClient, ReqwestFetcher};
use neutrino_engine::{MissingEventsFetcher, RoomActorError, RoomRegistry};
use sliding_sync::{SyncError, SyncState};

struct App {
    store: Arc<SqliteStore>,
    /// Per-room state-machine actors. CSAPI writes go through here so they
    /// are DAG-linked, auth-checked, and state-resolved.
    room_registry: Arc<RoomRegistry<SqliteStore>>,
    /// In-process poke to the inbound staging worker. The `/send` handler sends
    /// the room id of each freshly-staged PDU; the worker spawns or wakes that
    /// room's drain task. Best-effort (`try_send`): a full buffer just means the
    /// worker is already aware the room has work. Dropping the owning `AppState`
    /// drops this sender, which shuts the worker down (see `neutrino_engine::worker`).
    /// INVARIANT: this is the *only* long-lived holder of the poke sender — the
    /// worker tasks must never hold a clone, or the channel would never close
    /// and the worker (plus its `store`/`registry` `Arc`s) would leak.
    worker_poke: mpsc::Sender<OwnedRoomId>,
    /// Peer fetcher for `get_missing_events`, shared with the inbound worker's
    /// gap-fill and the `/send` handler's anti-entropy reconciliation. The
    /// outbound sender pool is handed a clone too (see `serve`), so a healed
    /// link reconciles divergence from a transaction's forward-extremity
    /// exchange. Held behind a trait object so tests inject a deterministic stub.
    fetcher: Arc<dyn MissingEventsFetcher>,
    /// Shared outbound federation client, built once at startup from
    /// `config.server_name`/`config.federation_proxy`. Reused by the `/messages`
    /// backward-underflow backfill path (`messages.rs`) so each back-page reuses
    /// the same connection pool rather than rebuilding a reqwest client per
    /// round. (`fetcher` also wraps a `FederationClient`, but only as a type-
    /// erased `MissingEventsFetcher`, which exposes no `backfill` method.)
    fed_client: Arc<FederationClient>,
    sync_state: Arc<SyncState<SqliteStore>>,
    keys: Option<Value>,
    /// Out-of-band peer discovery (BLE mesh): the host pushes the set of
    /// currently-visible peers here and the user-directory search handler reads
    /// it. Shared (`Arc`) so the host can hold a write handle while the router
    /// reads — see [`AppState::discovery`].
    discovery: Arc<DiscoveryRegistry>,
    /// Publishes the local user's display name whenever it changes (via `PUT
    /// /profile/.../displayname`), so the BLE transport can re-advertise it.
    /// `None` off the embedded path (dev binary / tests): nothing re-advertises.
    display_name_tx: Option<watch::Sender<String>>,
    /// The deployment's event policy (composed at the composition root): one
    /// value carrying the ingress admission mode, the signer for
    /// locally-authored events, and the room versions this build speaks.
    policy: EventPolicy,
    config: Config,
    /// Latching cancellation signal shared with long-polls and the outbound
    /// federation sender. Fired once by [`AppState::begin_shutdown`]; after
    /// that, every `cancelled().await` on any clone resolves immediately.
    shutdown: CancellationToken,
    /// "Kick" signal shared with the outbound sender's per-destination tasks.
    /// [`AppState::kick_backoff`] pulses it; each task watches a clone and, on a
    /// change, resets its retry backoff to base and retries immediately. The
    /// host pulses it (via `Command::KickBackoff`) when device connectivity is
    /// restored, so a destination that backed off while offline doesn't wait out
    /// a long backoff before reconnecting.
    ///
    /// Carries `()`: receivers only observe *that* it changed (via `changed()` /
    /// `borrow_and_update()`), never a value — `send_modify` always notifies, so
    /// a no-op mutation is a pulse.
    ///
    /// Why `watch` rather than the `shutdown` `CancellationToken` precedent
    /// above: a kick is a repeatable edge, not a one-shot latch. And unlike
    /// `Notify::notify_waiters`, `watch` *retains* a pulse that lands while a
    /// task is mid-send (between backoff sleeps), so it isn't lost. The store's
    /// `subscribe()` (`StreamPos`) watch can't carry it either: an offline task
    /// is parked in the delivery retry loop with PDUs still queued, not on that
    /// idle stream-position arm, so a position bump wouldn't reach it.
    kick_backoff: watch::Sender<()>,
    /// In-flight outbound federated joins, keyed by (room, user). The dance
    /// runs in a detached task; a client that times out and retries `/join`
    /// re-attaches to the running dance instead of restarting the handshake —
    /// over a slow link the send_join transfer outlives the client's HTTP
    /// timeout, and a restart discards the transfer's progress while its
    /// retransmissions keep competing for the link. See `federation::join`.
    joins: HashMap<(OwnedRoomId, OwnedUserId), federation::join::JoinWatch>,
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
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("startup i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("opening sqlite store: {0}")]
    Store(#[from] StorageError),
    #[error("invalid federation_proxy: {0}")]
    InvalidFederationProxy(String),
}

impl AppState {
    /// Validate `Config` fields that must be well-formed before serving. A
    /// configured federation proxy must parse as a proxy URL — bail at startup
    /// rather than silently falling back to direct federation: in a sidecar
    /// deployment "direct" means sending plain JSON to a peer's CBOR-only ingress
    /// port, which breaks the whole mesh. Fail loudly.
    fn validate_config(config: &Config) -> Result<(), StartupError> {
        if let Some(url) = config.federation_proxy.as_deref() {
            reqwest::Proxy::all(url)
                .map_err(|e| StartupError::InvalidFederationProxy(format!("{url}: {e}")))?;
        }
        Ok(())
    }

    async fn new(config: Config) -> Result<Self, StartupError> {
        Self::validate_config(&config)?;
        // Persistent file-backed store rooted at the configured directory;
        // the store owns the `<dir>/neutrino.db` layout and creates the dir
        // if missing. (`open_in_memory` exists but its shared-cache mode is
        // unsafe for the concurrent reader+writer workloads sliding-sync
        // long-polls drive — see that method's doc-comment.)
        let store = Arc::new(SqliteStore::open_in_dir(&config.storage_dir).await?);
        Ok(Self::from_store(config, store))
    }

    /// Build an `AppState` around an already-open `SqliteStore`. Used by
    /// the e2e tests in `src/federation/tests.rs` to seed events via the
    /// storage trait *before* the router is mounted — `DagStore::missing_events`
    /// needs specific multi-event DAG shapes (gaps, branches) that are
    /// simplest to construct directly through the trait rather than via the
    /// CSAPI write path.
    pub(crate) fn from_store(config: Config, store: Arc<SqliteStore>) -> Self {
        Self::from_store_with_discovery(
            config,
            store,
            Arc::new(DiscoveryRegistry::new()),
            EventPolicy::trusted_network(),
        )
    }

    /// Like [`AppState::from_store`] but with a caller-owned discovery registry
    /// and event policy. The production path ([`serve`]) injects the
    /// registry the host holds a write handle to (so its BLE-discovery callback
    /// and the router read the same set); [`from_store`] passes a fresh empty
    /// one (and [`EventPolicy::trusted_network()`]) for tests/the dev binary.
    pub(crate) fn from_store_with_discovery(
        config: Config,
        store: Arc<SqliteStore>,
        discovery: Arc<DiscoveryRegistry>,
        policy: EventPolicy,
    ) -> Self {
        let client = Arc::new(FederationClient::new(
            config.server_name.clone(),
            config.federation_proxy.as_deref(),
        ));
        let fetcher: Arc<dyn MissingEventsFetcher> = Arc::new(ReqwestFetcher::new(client));
        Self::from_store_with_fetcher(config, store, fetcher, discovery, policy)
    }

    /// Like [`AppState::from_store`] but with an explicit gap-fill `fetcher`
    /// (and discovery registry). The federation gap-fill tests inject a
    /// deterministic stub fetcher here instead of the reqwest client (which
    /// would otherwise reach the network).
    fn from_store_with_fetcher(
        config: Config,
        store: Arc<SqliteStore>,
        fetcher: Arc<dyn MissingEventsFetcher>,
        discovery: Arc<DiscoveryRegistry>,
        policy: EventPolicy,
    ) -> Self {
        let shutdown = CancellationToken::new();
        let mut sync_state = SyncState::new(store.clone(), shutdown.clone());
        sync_state.delivery_receipts = config.delivery_receipts;
        let sync_state = Arc::new(sync_state);
        let room_registry = Arc::new(RoomRegistry::new(
            store.clone(),
            config.server_name.clone(),
            policy.clone(),
        ));
        // Spawn the inbound staging worker bound to this store/registry/fetcher.
        // It runs wherever the router does (production `serve` and the e2e
        // tests), enumerates any leftover staged rows on startup, and stops when
        // this `AppState` is dropped (the `worker_poke` sender drops with it).
        let worker_poke = neutrino_engine::worker::spawn(
            store.clone(),
            room_registry.clone(),
            fetcher.clone(),
            policy.clone(),
        );
        // Receivers are taken later via `subscribe_kick` (one per destination
        // task); the initial receiver is dropped — `send_modify` notifies any
        // live receivers and is a no-op when there are none.
        let (kick_backoff, _) = watch::channel(());
        // Outbound federation client, built once here and shared (rather than
        // rebuilt per back-page in `messages.rs`'s backfill path).
        let fed_client = Arc::new(FederationClient::new(
            config.server_name.clone(),
            config.federation_proxy.as_deref(),
        ));
        let app = App {
            store,
            room_registry,
            worker_poke,
            fetcher,
            fed_client,
            sync_state,
            keys: None,
            discovery,
            display_name_tx: None,
            policy,
            config,
            shutdown,
            kick_backoff,
            joins: HashMap::new(),
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

    /// The shared out-of-band discovery registry. The host writes the visible
    /// peer set through this handle (via the FFI); the user-directory search
    /// handler reads it.
    pub fn discovery(&self) -> Arc<DiscoveryRegistry> {
        lock_app(self).discovery.clone()
    }

    /// The deployment-wide event policy, shared by every federation-ingress
    /// path and every event-authoring site.
    fn policy(&self) -> EventPolicy {
        lock_app(self).policy.clone()
    }

    /// The local event signer (`None` on a trusted network) — derived from
    /// the same policy value the ingress admission uses.
    fn signer(&self) -> Option<Arc<neutrino_event::EventSigner>> {
        lock_app(self).policy.signer().cloned()
    }

    /// The shared `get_missing_events` fetcher, for the outbound sender pool's
    /// anti-entropy reconciliation (a peer's forward extremities arrive on a
    /// transaction *response*, which the sender — not a handler — processes).
    fn fetcher(&self) -> Arc<dyn MissingEventsFetcher> {
        lock_app(self).fetcher.clone()
    }

    /// A clone of the inbound worker poke, for the outbound sender pool: after
    /// reconciliation stages fetched events it pokes the worker to apply them.
    fn worker_poke(&self) -> mpsc::Sender<OwnedRoomId> {
        lock_app(self).worker_poke.clone()
    }

    /// The configured cap on concurrent outbound federation transactions.
    fn outbound_concurrency(&self) -> usize {
        lock_app(self).config.outbound_concurrency
    }

    /// The configured `neutrino-lb` egress proxy URL, if any. Threaded into
    /// every outbound `FederationClient` so all federation routes through it.
    fn federation_proxy(&self) -> Option<String> {
        lock_app(self).config.federation_proxy.clone()
    }

    /// The configured startup jitter for the outbound sender pool.
    fn startup_jitter(&self) -> Duration {
        lock_app(self).config.startup_jitter
    }

    /// Signal every shutdown-aware subsystem (long-polls, the outbound federation
    /// sender) to wind down. Idempotent (`CancellationToken::cancel` is a no-op
    /// once already cancelled, and takes `&self`). Called by the command dispatcher
    /// on the terminal command or when the last `NeutrinoHandle` is dropped.
    fn begin_shutdown(&self) {
        lock_app(self).shutdown.cancel();
    }

    /// A clone of the shutdown token, for a subsystem spawned from `serve`.
    /// Every clone shares one cancellation state, so a later `begin_shutdown`
    /// is observed by all of them via `cancelled().await`.
    fn subscribe_shutdown(&self) -> CancellationToken {
        lock_app(self).shutdown.clone()
    }

    /// Reset every outbound destination's retry backoff to base and retry now,
    /// by pulsing the shared kick signal. Idempotent in effect: each
    /// destination task collapses any number of bumps into a single reset.
    /// Called by the command dispatcher on `Command::KickBackoff`.
    fn kick_backoff(&self) {
        // `send_modify` always notifies, even on a no-op mutation, and never
        // errors on zero receivers (vs `send`). A destination spawned later
        // baselines at the current version in `run_destination`, so only a pulse
        // after it subscribes interrupts its backoff — which is correct, since a
        // fresh task is already at base.
        lock_app(self).kick_backoff.send_modify(|_| {});
    }

    /// A clone of the kick signal receiver, for a sender task spawned from
    /// `serve`. Mirrors [`AppState::subscribe_shutdown`].
    fn subscribe_kick(&self) -> watch::Receiver<()> {
        lock_app(self).kick_backoff.subscribe()
    }
}

// The composition seam: every parameter is one injected policy/handle from the
// entrypoint, and bundling them into a struct would just move the list.
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    listener: TcpListener,
    config: Config,
    store: Arc<SqliteStore>,
    commands: mpsc::UnboundedReceiver<Command>,
    // Out-of-band peer discovery registry. The embedding host holds a clone and
    // pushes the visible peer set into it; the user-directory search handler
    // reads it. Non-embedded callers (the dev binary / e2e tests) pass a fresh
    // empty one — discovery is a no-op without the BLE side channel.
    discovery: Arc<DiscoveryRegistry>,
    // Publishes the local display name on change so the BLE transport can
    // re-advertise. `None` off the embedded path (no BLE advert to update).
    display_name_tx: Option<watch::Sender<String>>,
    // The event policy, composed by the composition root from the app's
    // `trusted_network` config and the medium's declared room version: one
    // value carrying the ingress admission mode, the local signer, and the
    // room versions this build speaks.
    policy: EventPolicy,
) -> Result<(), StartupError> {
    // The caller (the entrypoint) opens the store, resolves the server identity
    // from it, and hands the live handle in — so we build state around it rather
    // than re-opening the same DB.
    AppState::validate_config(&config)?;
    let state = AppState::from_store_with_discovery(config, store, discovery, policy);
    lock_app(&state).display_name_tx = display_name_tx;
    // Start draining the federation outbox before serving. Outbox rows survive
    // restarts, so this is also the "retry on restart" path — startup
    // enumeration resumes delivery of anything left undelivered.
    let transport = Arc::new(FederationClient::new(
        state.server_name(),
        state.federation_proxy().as_deref(),
    ));
    let sender_task = neutrino_engine::sender::spawn(
        state.store(),
        transport,
        state.outbound_concurrency(),
        state.startup_jitter(),
        state.subscribe_shutdown(),
        state.subscribe_kick(),
        state.fetcher(),
        state.policy(),
        state.worker_poke(),
    );
    let router = build_router(&state);
    // `dispatch` resolves on a terminal command or when every sender is dropped,
    // which drives axum's graceful shutdown — in-flight requests drain first.
    // `state` is threaded in so server-directed commands can act on internals.
    axum::serve(listener, router)
        .with_graceful_shutdown(dispatch(commands, state))
        .await
        .map_err(StartupError::Io)?;
    // dispatch fired the token before resolving, so the supervisor has already
    // begun winding down and aborting its children; await it to confirm teardown.
    let _ = sender_task.await;
    Ok(())
}

/// Dispatch out-of-band control commands until a terminal one (`Shutdown`)
/// arrives, or until every sender is dropped — the embedding host released the
/// last `NeutrinoHandle`. Resolving this future is what triggers the server's
/// graceful shutdown in [`serve`].
///
/// This is the permanent home for command handling: a server-directed command
/// (e.g. `Command::KickBackoff`, which resets the federation sender's backoff)
/// acts on internals reachable only from inside `serve`, and is added as one
/// more arm in [`handle`] returning [`ControlFlow::Continue`] to keep the loop
/// running. `state` is the handle to those internals — every *terminal* command
/// fires the shutdown token before returning.
async fn dispatch(mut commands: mpsc::UnboundedReceiver<Command>, state: AppState) {
    while let Some(command) = commands.recv().await {
        if handle(command, &state).is_break() {
            return; // Shutdown already signalled inside handle
        }
    }
    // Channel closed: last NeutrinoHandle dropped — treat as a shutdown request.
    state.begin_shutdown();
}

/// Apply a single control command, returning whether the dispatch loop should
/// stop ([`ControlFlow::Break`]) or keep running ([`ControlFlow::Continue`]).
/// Splitting this out of [`dispatch`] keeps per-command behaviour
/// unit-testable, and the `Continue` path is what makes the loop a genuine loop.
fn handle(command: Command, state: &AppState) -> ControlFlow<()> {
    match command {
        Command::Shutdown => {
            state.begin_shutdown();
            ControlFlow::Break(())
        }
        // Non-terminal: kick the outbound sender's backoff and keep dispatching.
        // Must NOT fire the shutdown token (that would end `serve`).
        Command::KickBackoff => {
            state.kick_backoff();
            ControlFlow::Continue(())
        }
    }
}

pub async fn router(config: Config) -> Result<Router, StartupError> {
    let state = AppState::new(config).await?;
    Ok(build_router(&state))
}

/// Test-only constructor that mounts the same router over an externally-
/// provided `SqliteStore`. Used by `src/federation/tests.rs` to seed events
/// via the `StorageBackend` trait directly before the HTTP layer observes
/// them — the DAG-walk tests need arbitrary chain/gap shapes that are
/// simplest to construct through the trait rather than via the CSAPI write
/// path.
#[cfg(test)]
pub(crate) fn router_with_store(config: Config, store: Arc<SqliteStore>) -> Router {
    let state = AppState::from_store(config, store);
    build_router(&state)
}

/// Like [`router_with_store`] but with an injected gap-fill `fetcher`. The
/// inbound `/send` gap-fill tests use this to supply a deterministic
/// [`MissingEventsFetcher`] stub (the default reqwest fetcher would reach the
/// network for an unreachable test `origin`).
#[cfg(test)]
pub(crate) fn router_with_store_and_fetcher(
    config: Config,
    store: Arc<SqliteStore>,
    fetcher: Arc<dyn MissingEventsFetcher>,
) -> Router {
    let state = AppState::from_store_with_fetcher(
        config,
        store,
        fetcher,
        Arc::new(DiscoveryRegistry::new()),
        EventPolicy::trusted_network(),
    );
    build_router(&state)
}

fn build_router(state: &AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/_matrix/client/versions", get(versions))
        .route("/_matrix/key/v2/server", get(server_keys))
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
            "/_matrix/client/v3/profile/{user_id}/displayname",
            get(get_display_name).put(put_display_name),
        )
        .route(
            "/_matrix/client/v3/user_directory/search",
            post(user_directory_search),
        )
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
        .route(
            "/_matrix/federation/v1/make_leave/{room_id}/{user_id}",
            get(federation::make_leave::handle),
        )
        .route(
            "/_matrix/federation/v2/send_leave/{room_id}/{event_id}",
            put(federation::send_leave::handle),
        )
        .fallback(default_fallback)
        // Log one INFO line per request (method + path) and one per response
        // (status + latency), with 5xx surfaced at ERROR. We emit these under our
        // own `neutrino_http` target (rather than `tower_http`) so they sit
        // alongside the rest of the crate's logs under the `neutrino_http=info`
        // env-filter (see `neutrino-main::platform`).
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::extract::Request| {
                    info_span!(
                        target: "neutrino_http",
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                })
                .on_request(|_request: &axum::extract::Request, _span: &Span| {
                    info!(target: "neutrino_http", "started processing request");
                })
                .on_response(
                    |response: &axum::response::Response, latency: Duration, _span: &Span| {
                        info!(
                            target: "neutrino_http",
                            status = response.status().as_u16(),
                            latency = ?latency,
                            "finished processing request",
                        );
                    },
                )
                .on_failure(
                    |error: ServerErrorsFailureClass, latency: Duration, _span: &Span| {
                        error!(
                            target: "neutrino_http",
                            %error,
                            latency = ?latency,
                            "response failed",
                        );
                    },
                ),
        )
        // Outermost: log requests whose handler future is dropped before a
        // response is written (client hung up — routine for /sync long-polls
        // aborted when the client stops its sync loop). `TraceLayer` above
        // only logs written responses, so without this an aborted long-poll
        // leaves a dangling "started processing request" line that is
        // indistinguishable from a hung handler.
        .layer(axum::middleware::from_fn(log_aborted_requests))
        // `AppState` is `Arc`-backed; this clone is the router's instance, the
        // caller keeps the other (e.g. `serve` hands its instance to `dispatch`).
        .with_state(state.clone())
}

async fn log_aborted_requests(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut guard = AbortLogGuard {
        method: request.method().clone(),
        uri: request.uri().clone(),
        start: Instant::now(),
        completed: false,
    };
    let response = next.run(request).await;
    guard.completed = true;
    response
}

/// Armed for the lifetime of a request; fires from `Drop` iff the response
/// never completed, which only happens when the request future is dropped
/// (client disconnected). Captures method/uri itself because the drop runs
/// outside the `TraceLayer` request span.
struct AbortLogGuard {
    method: Method,
    uri: Uri,
    start: Instant,
    completed: bool,
}

impl Drop for AbortLogGuard {
    fn drop(&mut self) {
        if !self.completed {
            info!(
                target: "neutrino_http",
                method = %self.method,
                uri = %self.uri,
                latency = ?self.start.elapsed(),
                "request aborted by client before a response was written",
            );
        }
    }
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

/// `GET /_matrix/key/v2/server` — this server's signing key, as signed JSON
/// (spec §"Retrieving server keys"). Only meaningful on a signed
/// deployment (`trusted_network = false`): a trusted network has no signing keys at
/// all, so the endpoint answers 404 there. No rotation mechanics — the key IS
/// the node identity, `old_verify_keys` is permanently empty, and
/// `valid_until_ts` is a rolling window that only paces peer re-fetches.
async fn server_keys(state: State<AppState>) -> axum::response::Response {
    let (signer, server_name) = {
        let app = lock_app(&state.0);
        (app.policy.signer().cloned(), app.config.server_name.clone())
    };
    let Some(signer) = signer else {
        return error_response(
            StatusCode::NOT_FOUND,
            "M_NOT_FOUND",
            "this deployment runs on a trusted network and has no signing keys",
        );
    };
    // 30 days: far enough that node-id peers (who never fetch) are unaffected
    // and DNS-named peers re-fetch at a lazy cadence.
    const VALIDITY_WINDOW_MS: u64 = 30 * 24 * 60 * 60 * 1000;
    let response = json!({
        "server_name": server_name,
        "valid_until_ts": neutrino_event::now_ms() + VALIDITY_WINDOW_MS,
        "verify_keys": {
            neutrino_event::SIGNING_KEY_ID: {
                "key": neutrino_event::event_id::b64_unpadded(&signer.public_key()),
            }
        },
        "old_verify_keys": {},
    });
    // Sign the response (appendices "Signing JSON"). The value tree above is
    // canonical-JSON-safe by construction (strings + ints only), so the
    // conversion cannot fail; refuse loudly rather than serve unsigned keys
    // if that invariant is ever broken.
    let Ok(ruma::CanonicalJsonValue::Object(mut obj)) =
        ruma::CanonicalJsonValue::try_from(response)
    else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            "key response is not canonical JSON",
        );
    };
    signer.sign_json(&mut obj);
    (StatusCode::OK, Json(obj)).into_response()
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

    info!(
        %user_id,
        query = %serde_json::to_string(&query.0).unwrap_or_default(),
        body = %body_value,
        "sync request"
    );

    let req = match build_sync_request(&query.0, body_value) {
        Ok(r) => r,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, "M_BAD_JSON", &e.to_string()),
    };

    // Identifying fields for the backstop diagnostic, captured before `req`
    // is moved into `handle`.
    let pos = query.0.get("pos").cloned();
    let conn_id = req.conn_id.clone().unwrap_or_default();

    // Wrap the handler in an absolute backstop deadline. A healthy long-poll
    // returns well within `BACKSTOP_TIMEOUT`; if it doesn't, the handler is
    // wedged (see the const's doc + the decisions log). The outer timer's
    // waker is registered with the time driver independently of any inner
    // await, so it fires even when the inner wakers are lost; on fire we drop
    // `handle` (which frees the conn lock) and return a retryable error rather
    // than hang the client's serial sync loop forever.
    let handled = tokio::time::timeout(
        sliding_sync::BACKSTOP_TIMEOUT,
        sliding_sync::handle(&sync_state, &user_id, req),
    )
    .await;

    match handled {
        Ok(Ok(resp)) => {
            let wire = SyncResponseWire::from(resp);
            info!(
                %user_id,
                body = %serde_json::to_string(&wire).unwrap_or_default(),
                "sync response"
            );
            (StatusCode::OK, Json(wire)).into_response()
        }
        Ok(Err(SyncError::UnknownPos)) => {
            error_response(StatusCode::BAD_REQUEST, "M_UNKNOWN_POS", "Unknown position")
        }
        Ok(Err(SyncError::BadRequest(msg))) => {
            error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", msg)
        }
        Ok(Err(SyncError::Storage(e))) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
        Ok(Err(SyncError::EventConversion(e))) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
        Err(_elapsed) => {
            error!(
                %user_id,
                conn_id,
                pos = pos.as_deref().unwrap_or("<initial>"),
                backstop_secs = sliding_sync::BACKSTOP_TIMEOUT.as_secs(),
                "sliding-sync handler exceeded backstop deadline: the long-poll is \
                 wedged (not an executor stall — other requests are being served). \
                 Dropping the handler to free the conn lock; returning 504 so the \
                 client's sync loop recovers."
            );
            dump_wedged_tasks().await;
            error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "M_UNKNOWN",
                "sync handler backstop deadline exceeded",
            )
        }
    }
}

/// Log a full task dump when the sliding-sync backstop fires, to pin the exact
/// await the wedged handler is parked on.
///
/// Real dumps need `--cfg tokio_unstable` **and** `--features task-dump` (which
/// enables tokio's `taskdump`, pulling in `backtrace`) — off by default so
/// normal builds stay lean. To capture backtraces on the next repro, build the
/// FFI lib with e.g.
/// `RUSTFLAGS="--cfg tokio_unstable" cargo build -p neutrino-ffi --features task-dump`.
/// Backtraces are only usable with frame pointers (`-C force-frame-pointers=yes`).
#[cfg(all(tokio_unstable, feature = "task-dump"))]
async fn dump_wedged_tasks() {
    // `Handle::dump` re-polls every task in a tracing mode and can fail to
    // terminate if a worker is blocked, so the tokio docs recommend pairing it
    // with an explicit timeout.
    match tokio::time::timeout(
        Duration::from_secs(5),
        tokio::runtime::Handle::current().dump(),
    )
    .await
    {
        Ok(dump) => {
            for task in dump.tasks().iter() {
                error!(task_id = %task.id(), trace = %task.trace(), "wedged-sync taskdump");
            }
        }
        Err(_) => error!("wedged-sync taskdump timed out (a runtime worker is blocked)"),
    }
}

/// Fallback when task dumps aren't compiled in — see the `cfg`-enabled variant.
#[cfg(not(all(tokio_unstable, feature = "task-dump")))]
async fn dump_wedged_tasks() {
    error!(
        "task dump unavailable: rebuild with RUSTFLAGS=\"--cfg tokio_unstable\" and \
         --features task-dump to capture task backtraces when the sync backstop fires"
    );
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
    // Ruma's own emptiness check, not a local one: it covers every extension
    // field it knows about, so a newly-populated extension (receipts) can't be
    // silently dropped here by a predicate that predates it.
    #[serde(skip_serializing_if = "v5::response::Extensions::is_empty")]
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

/// Resolve a user's display name: the local user's from the persistent
/// [`IdentityStore`] (falling back to [`DEFAULT_DISPLAY_NAME`]), a discovered
/// peer's from the [`DiscoveryRegistry`] (matched on the user id's
/// `server_name`). `None` only for an unknown remote user.
async fn resolve_display_name(state: &AppState, user_id: &str) -> Option<String> {
    let (is_self, store, discovery) = {
        let app = lock_app(state);
        (
            user_id == app.config.user_id(),
            app.store.clone(),
            app.discovery.clone(),
        )
    };
    if is_self {
        return Some(local_display_name(&store).await);
    }
    user_id
        .rsplit_once(':')
        .and_then(|(_, server_name)| discovery.get(server_name))
        .map(|peer| peer.display_name)
}

/// The local user's stored display name, falling back to
/// [`DEFAULT_DISPLAY_NAME`] when never set. This is the server-wide display
/// name both reported for the local user's profile and embedded into the
/// `m.room.member` events the server authors for the local user.
pub(crate) async fn local_display_name(store: &SqliteStore) -> String {
    store
        .get_display_name()
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| DEFAULT_DISPLAY_NAME.to_string())
}

/// Set `content.displayname` on an `m.room.member` content object. The server
/// is authoritative for its local users' profile, so the member events it
/// authors for a local user carry the server-wide display name (a no-op if
/// `content` is not a JSON object).
pub(crate) fn set_member_displayname(content: &mut Value, name: &str) {
    if let Some(obj) = content.as_object_mut() {
        obj.insert("displayname".to_owned(), Value::String(name.to_owned()));
    }
}

/// `GET /_matrix/client/v3/profile/{user_id}` and the `/displayname` keyed
/// variant. An unknown remote user yields an empty profile (`{}`) — the spec
/// permits an absent `displayname`, and there is no profile store to 404 a
/// never-seen peer against.
async fn profile(
    state: State<AppState>,
    axum::extract::Path(user_id): axum::extract::Path<String>,
) -> Json<Value> {
    match resolve_display_name(&state.0, &user_id).await {
        Some(dn) => Json(json!({ "displayname": dn })),
        None => Json(json!({})),
    }
}

/// `GET /_matrix/client/v3/profile/{user_id}/displayname` — the keyed variant
/// of [`profile`], returning only the `displayname` field.
async fn get_display_name(
    state: State<AppState>,
    axum::extract::Path(user_id): axum::extract::Path<String>,
) -> Json<Value> {
    match resolve_display_name(&state.0, &user_id).await {
        Some(dn) => Json(json!({ "displayname": dn })),
        None => Json(json!({})),
    }
}

#[derive(serde::Deserialize)]
struct SetDisplayNameRequest {
    #[serde(default)]
    displayname: String,
}

/// `PUT /_matrix/client/v3/profile/{user_id}/displayname`
/// (https://spec.matrix.org/v1.18/client-server-api/#put_matrixclientv3profileuseriddisplayname).
/// Persists the local user's display name in the [`IdentityStore`]. The embedded
/// server is single-user, so the path `user_id` is the local user by
/// construction; the name is stored verbatim.
async fn put_display_name(
    state: State<AppState>,
    axum::extract::Path(_user_id): axum::extract::Path<String>,
    Json(req): Json<SetDisplayNameRequest>,
) -> axum::response::Response {
    let store = lock_app(&state.0).store.clone();
    match store.set_display_name(&req.displayname).await {
        Ok(()) => {
            // Signal the BLE transport (if any) to re-advertise the new name.
            if let Some(tx) = &lock_app(&state.0).display_name_tx {
                let _ = tx.send(req.displayname);
            }
            Json(json!({})).into_response()
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "M_UNKNOWN",
            &e.to_string(),
        ),
    }
}

/// Default result cap when the client omits `limit`
/// (https://spec.matrix.org/v1.18/client-server-api/#post_matrixclientv3user_directorysearch).
fn default_search_limit() -> usize {
    10
}

#[derive(serde::Deserialize)]
struct SearchRequest {
    search_term: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

/// `POST /_matrix/client/v3/user_directory/search`.
///
/// The embedded server has no Matrix-level user directory: it answers from the
/// out-of-band [`DiscoveryRegistry`] (peers seen over the BLE mesh). Each peer's
/// user id is rebuilt as `@{localpart}:{server_name}` from its stored fields; a
/// peer whose advertised data doesn't form a syntactically valid id is skipped
/// rather than failing the whole search.
async fn user_directory_search(
    state: State<AppState>,
    Json(req): Json<SearchRequest>,
) -> Json<Value> {
    let mut matches = state.discovery().search(&req.search_term);
    // The spec caps the list at `limit` and flags whether that truncated it.
    let limited = matches.len() > req.limit;
    matches.truncate(req.limit);
    let results: Vec<Value> = matches
        .into_iter()
        .filter_map(|(server_name, peer)| {
            let user_id: OwnedUserId = format!("@{}:{}", peer.localpart, server_name)
                .parse()
                .ok()?;
            Some(json!({ "user_id": user_id, "display_name": peer.display_name }))
        })
        .collect();
    Json(json!({ "results": results, "limited": limited }))
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
    let (store, own_server, policy) = {
        let app = lock_app(&state.0);
        (
            app.store.clone(),
            app.config.server_name.clone(),
            app.policy.clone(),
        )
    };

    // Build the spec-mandated initial-state batch (create → join →
    // power_levels → join_rules, plus name/topic when requested). Each event
    // is built on the running heads, server-side `auth_events` selected, and
    // verified through `RoomCore::apply` before it's persisted — see
    // `build_initial_events`. Any failure here is a server bug (the events are
    // server-authored), so it maps to 500. Only *local* invitees are baked into
    // the batch; remote invitees are federated separately below.
    let display_name = local_display_name(&store).await;
    let (create, initial) =
        match build_initial_events(&sender, &body.0, &own_server, &display_name, &policy) {
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

    // Federate each *remote* invitee via the dedicated `/invite` handshake — a
    // remote user's server isn't in the room yet, so it's not in the joined-set
    // that transaction fan-out reaches; only `federated_invite` (PUT
    // /federation/v2/invite) can deliver the invite. Local invitees already rode
    // the initial batch. Best-effort: the room is persisted, so a failed invite
    // is logged and left for the client to retry rather than unwinding the room.
    for target in invite_targets(&sender, &body.0) {
        if target.server_name().as_str() == own_server {
            continue;
        }
        let resp = crate::federation::invite::federated_invite(
            &state.0,
            sender.clone(),
            &room_id,
            &target,
            None,
        )
        .await;
        if !resp.status().is_success() {
            warn!(%room_id, invitee = %target, status = %resp.status(), "createRoom: failed to federate invite to remote invitee");
        }
    }

    (StatusCode::OK, Json(json!({"room_id": room_id}))).into_response()
}

/// The validated `invite` targets from a createRoom body, minus the creator and
/// malformed entries (best-effort — a bad entry is skipped, not a room-creation
/// failure). Shared by [`build_initial_events`] (bakes *local* invitees into the
/// initial batch) and [`create_room`] (federates *remote* invitees as a
/// follow-up), so the two split the same list on the same rules.
fn invite_targets(sender: &OwnedUserId, body: &Value) -> Vec<OwnedUserId> {
    let Some(invitees) = body.pointer("/invite").and_then(Value::as_array) else {
        return Vec::new();
    };
    invitees
        .iter()
        .filter_map(Value::as_str)
        .filter(|t| *t != sender.as_str())
        .filter_map(|t| OwnedUserId::try_from(t).ok())
        .collect()
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
    own_server: &str,
    display_name: &str,
    policy: &neutrino_event::EventPolicy,
) -> Result<(Event, Vec<Event>), CreateRoomError> {
    // The version this room is created under — the medium's if it declared one,
    // else the base. It is stamped into `content.room_version` (which is how
    // every peer learns it) and names every event below.
    let version = policy.versions.default_for_new_rooms().clone();

    // create is special: no parents, room_id derived from its own event_id.
    let create = EventBuilder::new(sender.clone(), "m.room.create".to_owned(), version.clone())
        .state_key(String::new())
        .content(json!({ "room_version": version.id }))
        .signer(policy.signer().cloned())
        .build()?;

    let mut room = RoomCore::new(create.room_id.clone(), version);
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
                policy.signer(),
            )?;
            // apply_pdu is the sole authority for `auth_events`, stamping them
            // onto the event it hands back via `Persist` — persist *that*, not
            // the pre-apply build output (which has empty auth_events).
            let stored = persisted_event(room.apply_pdu(ev, &provider)?)?;
            provider.insert(stored.clone());
            initial.push((*stored).clone());
            Ok(())
        };

    let mut join_content = json!({ "membership": "join" });
    set_member_displayname(&mut join_content, display_name);
    add("m.room.member", sender.as_str(), join_content)?;
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

    // Honour the request's `invite` list, but only for *local* invitees: emit one
    // invite member event per local target, authored by the creator — who is
    // joined with implicit MAX power, so rule 5.4 accepts it. Remote invitees
    // cannot ride the initial batch (their server isn't in the room yet, so
    // nothing federates the event to them); `create_room` delivers those via the
    // dedicated `/invite` handshake instead. `is_direct` is propagated onto the
    // invite content when the request sets it.
    let is_direct = body.pointer("/is_direct").and_then(Value::as_bool) == Some(true);
    for target in invite_targets(sender, body) {
        if target.server_name().as_str() != own_server {
            continue;
        }
        let mut content = json!({ "membership": "invite" });
        if is_direct {
            content["is_direct"] = json!(true);
        }
        // Local invitees only reach this branch (remote ones are skipped
        // above), so their displayname is our server-wide name.
        set_member_displayname(&mut content, display_name);
        add("m.room.member", target.as_str(), content)?;
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
    // don't honour — member filtering is out of scope).
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
        RoomActorError::Storage(_)
        | RoomActorError::NotApplied
        | RoomActorError::ActorGone
        // A room on our own disk whose version this build cannot speak: a
        // server-side condition, not something the client got wrong.
        | RoomActorError::UnsupportedRoomVersion(_) => {
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

async fn default_fallback() -> (StatusCode, &'static str) {
    // The 404 status on the response is sufficient; no log line needed.
    (
        StatusCode::NOT_FOUND,
        "The requested resource was not found.",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        AppState, Command, Config, ControlFlow, DiscoveryRegistry, Event, EventPolicy, OwnedUserId,
        SqliteStore, StatusCode, TcpListener, Value, build_initial_events, build_router, dispatch,
        handle, invite_targets, join_rule_for, mpsc,
    };
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    /// The wire mirror must keep an extensions object that carries *only*
    /// receipts. It previously used a hand-rolled emptiness predicate that knew
    /// about to-device and e2ee alone, so a receipts-only response serialised
    /// with the whole extensions object dropped — the synthesised receipt was
    /// built and then silently discarded at the JSON edge.
    #[test]
    fn wire_keeps_a_receipts_only_extensions_object() {
        use ruma::api::client::sync::sync_events::v5;

        let mut resp = v5::Response::new("1".to_owned());
        resp.extensions.receipts.rooms.insert(
            ruma::room_id!("!r:example.org").to_owned(),
            ruma::serde::Raw::new(&json!({ "type": "m.receipt", "content": {} }))
                .expect("raw receipt")
                .cast_unchecked(),
        );

        let wire = serde_json::to_value(super::SyncResponseWire::from(resp)).expect("serialise");
        assert!(
            wire["extensions"]["receipts"]["rooms"]
                .get("!r:example.org")
                .is_some(),
            "receipts must survive the wire mirror: {wire}"
        );
    }

    fn test_config(tmp: &TempDir) -> Config {
        Config {
            server_name: "127.0.0.1".to_string(),
            bind_addr: "127.0.0.1:0".to_string(),
            localpart: "alice".to_string(),
            storage_dir: tmp.path().to_path_buf(),
            ..Default::default()
        }
    }

    /// A throwaway `AppState` over a fresh temp-dir store. `dispatch` takes a
    /// state handle (for future server-directed commands); these tests only
    /// exercise the lifecycle arms, so the state is constructed but unused.
    async fn test_state() -> (AppState, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let store = Arc::new(
            SqliteStore::open_in_dir(tmp.path())
                .await
                .expect("open store"),
        );
        let state = AppState::from_store(test_config(&tmp), store);
        (state, tmp)
    }

    /// `/_matrix/key/v2/server`: on a signed deployment it serves the node
    /// key under `ed25519:1` with a signature block; on a trusted network
    /// (no signer) it answers 404 — there are no keys to serve, by design.
    #[tokio::test]
    async fn server_keys_served_iff_signed_deployment() {
        use tower::ServiceExt;
        let secret = [7u8; 32];
        let signer = neutrino_event::EventSigner::new(&secret, "127.0.0.1");
        let expected_key = neutrino_event::event_id::b64_unpadded(&signer.public_key());

        let request = || {
            axum::http::Request::builder()
                .uri("/_matrix/key/v2/server")
                .body(axum::body::Body::empty())
                .expect("request")
        };

        // Signed deployment: key + signature present.
        let tmp = TempDir::new().expect("tempdir");
        let store = Arc::new(SqliteStore::open_in_dir(tmp.path()).await.expect("open"));
        let state = AppState::from_store_with_discovery(
            test_config(&tmp),
            store,
            Arc::new(DiscoveryRegistry::new()),
            EventPolicy::new(
                neutrino_event::EventSecurity::Signed {
                    signer: Arc::new(signer),
                    resolver: Arc::new(neutrino_event::NodeIdKeyResolver),
                },
                Arc::new(neutrino_event::RoomVersions::base_only()),
            ),
        );
        let response = build_router(&state)
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let v: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["server_name"], "127.0.0.1");
        assert_eq!(
            v["verify_keys"][neutrino_event::SIGNING_KEY_ID]["key"],
            json!(expected_key)
        );
        assert!(
            v["signatures"]["127.0.0.1"][neutrino_event::SIGNING_KEY_ID].is_string(),
            "key response must be signed JSON"
        );
        assert!(v["valid_until_ts"].is_u64());

        // Trusted network: no keys exist.
        let (state, _tmp) = test_state().await;
        let response = build_router(&state)
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn router_bails_on_invalid_federation_proxy() {
        // A malformed proxy must fail startup, not silently degrade to direct
        // federation (which would break the CBOR sidecar mesh).
        let tmp = TempDir::new().expect("tempdir");
        let mut config = test_config(&tmp);
        config.federation_proxy = Some("not a url".to_string());
        let err = super::router(config)
            .await
            .expect_err("invalid federation_proxy must abort startup");
        assert!(
            matches!(err, super::StartupError::InvalidFederationProxy(_)),
            "got {err:?}"
        );
    }

    /// Captures event messages so the abort-guard tests can assert on what was
    /// (and wasn't) logged without pulling in `tracing-subscriber`.
    struct CapturingSubscriber(Arc<std::sync::Mutex<Vec<String>>>);

    impl tracing::Subscriber for CapturingSubscriber {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct MessageVisitor(String);
            impl tracing::field::Visit for MessageVisitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.0.lock().expect("capture lock").push(visitor.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn dropped_request_logs_abort_and_completed_request_does_not() {
        use tower::ServiceExt;

        let messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(CapturingSubscriber(messages.clone()));

        let router = axum::Router::new()
            .route(
                "/hang",
                axum::routing::get(std::future::pending::<&'static str>),
            )
            .route("/ok", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(super::log_aborted_requests));
        let req = |path: &str| {
            axum::extract::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .expect("request")
        };

        // A request that completes must not fire the guard.
        let response = router.clone().oneshot(req("/ok")).await.expect("response");
        assert_eq!(response.status(), super::StatusCode::OK);
        assert!(
            messages
                .lock()
                .expect("capture lock")
                .iter()
                .all(|m| !m.contains("aborted")),
            "completed request must not log an abort"
        );

        // Dropping an in-flight request (client hung up mid long-poll) must.
        let mut in_flight = Box::pin(router.oneshot(req("/hang")));
        tokio::time::timeout(Duration::from_millis(50), &mut in_flight)
            .await
            .expect_err("/hang must still be in flight");
        drop(in_flight);
        assert!(
            messages
                .lock()
                .expect("capture lock")
                .iter()
                .any(|m| m.contains("request aborted by client before a response was written")),
            "dropped request must log an abort"
        );
    }

    #[tokio::test]
    async fn user_directory_search_matches_discovered_peers() {
        use neutrino_ctl::DiscoveredPeer;

        let (state, _tmp) = test_state().await;
        let reg = state.discovery();
        let mk = |dn: &str| DiscoveredPeer {
            localpart: "n".to_string(),
            display_name: dn.to_string(),
            last_seen_ms: 0,
        };
        reg.upsert("nodealice".to_string(), mk("Alice"));
        reg.upsert("nodebob".to_string(), mk("Bob"));

        let body = super::user_directory_search(
            axum::extract::State(state.clone()),
            axum::Json(super::SearchRequest {
                search_term: "ali".to_string(),
                limit: 10,
            }),
        )
        .await
        .0;

        assert_eq!(body["limited"], serde_json::json!(false));
        let results = body["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1);
        // user_id is rebuilt from the peer's localpart + server_name (the key).
        assert_eq!(results[0]["user_id"], serde_json::json!("@n:nodealice"));
        assert_eq!(results[0]["display_name"], serde_json::json!("Alice"));
    }

    #[tokio::test]
    async fn user_directory_search_honours_limit_and_flags_truncation() {
        use neutrino_ctl::DiscoveredPeer;

        let (state, _tmp) = test_state().await;
        let reg = state.discovery();
        for i in 0..3 {
            reg.upsert(
                format!("node{i}"),
                DiscoveredPeer {
                    localpart: "n".to_string(),
                    display_name: format!("Peer {i}"),
                    last_seen_ms: 0,
                },
            );
        }

        let body = super::user_directory_search(
            axum::extract::State(state.clone()),
            axum::Json(super::SearchRequest {
                search_term: "peer".to_string(),
                limit: 2,
            }),
        )
        .await
        .0;

        assert_eq!(body["limited"], serde_json::json!(true));
        assert_eq!(body["results"].as_array().expect("results").len(), 2);
    }

    #[tokio::test]
    async fn profile_default_then_persisted_display_name() {
        let (state, _tmp) = test_state().await;
        // test_config: localpart "alice", server_name "127.0.0.1".
        let self_id = "@alice:127.0.0.1".to_string();

        // Unset → product default "Neutrino".
        let body = super::profile(
            axum::extract::State(state.clone()),
            axum::extract::Path(self_id.clone()),
        )
        .await
        .0;
        assert_eq!(body["displayname"], json!("Neutrino"));

        // Persist via PUT, then GET reflects it (and survives in the store).
        super::put_display_name(
            axum::extract::State(state.clone()),
            axum::extract::Path(self_id.clone()),
            axum::Json(super::SetDisplayNameRequest {
                displayname: "Zaphod".to_string(),
            }),
        )
        .await;
        let body = super::profile(
            axum::extract::State(state.clone()),
            axum::extract::Path(self_id),
        )
        .await
        .0;
        assert_eq!(body["displayname"], json!("Zaphod"));
    }

    #[tokio::test]
    async fn put_display_name_persists_and_pulses_readvertise() {
        use neutrino_store::IdentityStore;
        let (state, _tmp) = test_state().await;
        // Wire a display-name watch as the embedded path (serve) would.
        let (tx, mut rx) = super::watch::channel(String::new());
        super::lock_app(&state).display_name_tx = Some(tx);

        super::put_display_name(
            axum::extract::State(state.clone()),
            axum::extract::Path("@alice:127.0.0.1".to_string()),
            axum::Json(super::SetDisplayNameRequest {
                displayname: "Ford".to_string(),
            }),
        )
        .await;

        // Persisted in the store …
        let store = super::lock_app(&state).store.clone();
        let stored = store.get_display_name().await.expect("get");
        assert_eq!(stored, Some("Ford".to_string()));
        // … and pulsed to the re-advertise watch.
        assert!(rx.has_changed().expect("sender alive"));
        assert_eq!(*rx.borrow_and_update(), "Ford");
    }

    #[tokio::test]
    async fn profile_resolves_peer_from_registry_and_unknown_is_empty() {
        use neutrino_ctl::DiscoveredPeer;
        let (state, _tmp) = test_state().await;
        state.discovery().upsert(
            "peernode".to_string(),
            DiscoveredPeer {
                localpart: "n".to_string(),
                display_name: "Trillian".to_string(),
                last_seen_ms: 0,
            },
        );

        let body = super::profile(
            axum::extract::State(state.clone()),
            axum::extract::Path("@n:peernode".to_string()),
        )
        .await
        .0;
        assert_eq!(body["displayname"], json!("Trillian"));

        // An unknown remote user → empty profile.
        let body = super::profile(
            axum::extract::State(state.clone()),
            axum::extract::Path("@x:unknownserver".to_string()),
        )
        .await
        .0;
        assert_eq!(body, json!({}));
    }

    #[tokio::test]
    async fn handle_shutdown_breaks() {
        // The only terminal command today. When a non-terminal variant lands it
        // returns ControlFlow::Continue, and this stays the per-command oracle.
        let (state, _tmp) = test_state().await;
        assert_eq!(handle(Command::Shutdown, &state), ControlFlow::Break(()));
    }

    #[tokio::test]
    async fn handle_kick_backoff_continues_and_pulses_kick() {
        // Non-terminal: the dispatch loop must keep running (Continue, not Break)
        // and must NOT fire the shutdown token. The observable effect is that the
        // shared kick signal changes, so destination tasks see the kick.
        let (state, _tmp) = test_state().await;
        let kick_rx = state.subscribe_kick();
        assert_eq!(
            handle(Command::KickBackoff, &state),
            ControlFlow::Continue(())
        );
        assert!(
            kick_rx.has_changed().expect("kick sender is alive"),
            "KickBackoff must pulse the shared kick signal"
        );
    }

    #[tokio::test]
    async fn dispatch_returns_on_shutdown() {
        let (state, _tmp) = test_state().await;
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Command::Shutdown).unwrap();
        tokio::time::timeout(Duration::from_secs(1), dispatch(rx, state))
            .await
            .expect("dispatch must return promptly on Shutdown, not hang");
    }

    #[tokio::test]
    async fn dispatch_returns_when_all_senders_dropped() {
        let (state, _tmp) = test_state().await;
        let (tx, rx) = mpsc::unbounded_channel::<Command>();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), dispatch(rx, state))
            .await
            .expect("dispatch must return promptly when the channel closes, not hang");
    }

    #[tokio::test]
    async fn serve_stops_when_shutdown_command_sent() {
        // End-to-end: a Shutdown command must actually drive `serve` to return
        // via the graceful-shutdown wiring — not just the dispatch helper in
        // isolation. A regression that dropped/mis-wired the receiver would hang
        // here and trip the timeout.
        //
        // We seed an outbox row destined for a dead peer so the sender supervisor
        // spawns a live per-destination task. The key regression this pins is that
        // a live sender task does NOT block `serve` from returning after
        // `Command::Shutdown`. (That the supervisor actually aborts its children
        // is pinned by `sender::tests::supervisor_returns_on_shutdown`.)
        use neutrino_event::ROOM_VERSION_ID;
        use neutrino_event::event_builder::EventBuilder;
        use neutrino_store::{EventStore, RoomStore};

        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);

        // Open the store and seed one outbox row to a dead peer (nothing listens
        // on this port after we drop the listener).
        let dead_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind dead");
        let dead_port = dead_listener.local_addr().expect("addr").port();
        drop(dead_listener);
        let dead_peer: ruma::OwnedServerName = format!("127.0.0.1:{dead_port}")
            .parse()
            .expect("parse server name");

        let store = Arc::new(
            SqliteStore::open_in_dir(tmp.path())
                .await
                .expect("open store"),
        );
        // Build a minimal room so we can persist a message event.
        let sender: ruma::OwnedUserId = "@alice:127.0.0.1".parse().expect("user id");
        let create = EventBuilder::new(
            sender.clone(),
            "m.room.create".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .state_key(String::new())
        .content(serde_json::json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
        let room_id = create.room_id.clone();
        store.create_room(&create, &[]).await.expect("create_room");
        // Persist a message event to the dead peer's outbox — this is what makes
        // the sender supervisor spawn a per-destination task that keeps retrying.
        let msg = EventBuilder::new(
            sender,
            "m.room.message".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .room_id(room_id)
        .content(serde_json::json!({ "msgtype": "m.text", "body": "hi" }))
        .prev_events(vec![create.event_id])
        .build()
        .expect("build msg");
        store
            .persist_event(&msg, &[&*dead_peer])
            .await
            .expect("persist_event");

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let (tx, rx) = mpsc::unbounded_channel();
        let server = tokio::spawn(super::serve(
            listener,
            config,
            store,
            rx,
            Arc::new(super::DiscoveryRegistry::new()),
            None,
            EventPolicy::trusted_network(),
        ));

        // Give the sender supervisor a moment to discover the dead peer and
        // spawn its per-destination task (which will be retrying the dead peer).
        tokio::time::sleep(Duration::from_millis(50)).await;

        tx.send(Command::Shutdown).expect("send shutdown");
        let joined = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve must return after a Shutdown command even with a live sender task");
        joined
            .expect("serve task must not panic")
            .expect("serve must return Ok after graceful shutdown");
    }

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

    #[test]
    fn invite_targets_skips_self_and_malformed() {
        let sender: OwnedUserId = "@alice:127.0.0.1".parse().expect("user id");
        let body = json!({
            "invite": [
                "@bob:127.0.0.1",
                "@alice:127.0.0.1", // self — skipped
                "not-a-user-id",    // malformed — skipped
                "@carol:remote.example",
                42,                 // wrong type — skipped
            ]
        });
        let targets: Vec<String> = invite_targets(&sender, &body)
            .into_iter()
            .map(|u| u.to_string())
            .collect();
        assert_eq!(targets, ["@bob:127.0.0.1", "@carol:remote.example"]);
    }

    #[test]
    fn build_initial_events_bakes_local_invite_only_remote_deferred() {
        // A DM-style createRoom with one local and one remote invitee: the local
        // invite member event rides the initial batch (with `is_direct`), the
        // remote one does not — `create_room` federates it via `/invite` instead.
        let sender: OwnedUserId = "@alice:127.0.0.1".parse().expect("user id");
        let body = json!({
            "is_direct": true,
            "invite": ["@bob:127.0.0.1", "@carol:remote.example"],
        });
        let (_create, initial) = build_initial_events(
            &sender,
            &body,
            "127.0.0.1",
            "Alice",
            &EventPolicy::trusted_network(),
        )
        .expect("build initial events");

        // The creator's own join carries the server-wide display name.
        let join = initial
            .iter()
            .find(|e| {
                e.event_type == "m.room.member"
                    && e.content_str("membership").as_deref() == Some("join")
            })
            .expect("creator join present");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(join.content.get())
                .expect("join content")
                .pointer("/displayname"),
            Some(&json!("Alice")),
        );

        let invites: Vec<&Event> = initial
            .iter()
            .filter(|e| {
                e.event_type == "m.room.member"
                    && e.content_str("membership").as_deref() == Some("invite")
            })
            .collect();
        assert_eq!(invites.len(), 1, "only the local invitee is baked in");
        let bob = invites[0];
        assert_eq!(bob.state_key.as_deref(), Some("@bob:127.0.0.1"));
        let content: serde_json::Value =
            serde_json::from_str(bob.content.get()).expect("content json");
        assert_eq!(content.pointer("/is_direct"), Some(&json!(true)));
        assert_eq!(
            content.pointer("/displayname"),
            Some(&json!("Alice")),
            "the local invitee carries the server-wide display name"
        );
        assert!(
            !initial
                .iter()
                .any(|e| e.state_key.as_deref() == Some("@carol:remote.example")),
            "the remote invitee must not appear in the initial batch"
        );
    }
}
