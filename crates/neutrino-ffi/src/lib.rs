uniffi::setup_scaffolding!("neutrino");

#[cfg(feature = "ble")]
mod ble_android;
// The iroh-backed datagram link: implements `neutrino_main::DatagramLink` over an
// iroh QUIC endpoint (keyed by 32-byte node ids). Built in `start` and injected
// into the entrypoint via a `FederationLinkFactory`.
mod relay_transport;
mod watchdog;

use relay_transport::{IrohTransport, RELAY_BIND};

/// FFI-facing server configuration. Mirrors `neutrino_ctl::Config` so EX
/// Android can fully configure the embedded homeserver. Kept here (not on the
/// common `Config`) so UniFFI stays out of the common crates — see the
/// crate-structure rule in CLAUDE.md. All fields are required; defaults live
/// in `Config::default`/`from_env` for the dev binary.
#[derive(uniffi::Record)]
pub struct NeutrinoConfig {
    pub bind_addr: String,
    pub localpart: String,
    /// Absolute path to a writable directory the host owns (e.g. Android's
    /// `context.filesDir`). The DB is `<storage_dir>/neutrino.db`.
    pub storage_dir: String,
    pub outbound_concurrency: u32,
    /// When set, runs the in-process `neutrino-lb` CoAP low-bandwidth sidecar.
    /// This is the public federation port the ingress binds — peers'
    /// `server_name` resolves to `host(bind_addr):lb_federation_port`. The
    /// egress is an internal loopback port the server allocates. `None` = direct
    /// federation (no in-process sidecar).
    pub lb_federation_port: Option<u16>,
}

impl From<NeutrinoConfig> for neutrino_main::Config {
    fn from(c: NeutrinoConfig) -> Self {
        neutrino_main::Config {
            // The embedded server has no operator-set name: it always derives its
            // identity (a node id) from the persisted secret. An empty
            // `server_name` triggers that derivation in the entrypoint; the host
            // reads the result back via `NeutrinoHandle::server_name()`.
            server_name: String::new(),
            bind_addr: c.bind_addr,
            localpart: c.localpart,
            storage_dir: std::path::PathBuf::from(c.storage_dir),
            // Floor-to-1 invariant lives on `Config`, shared with `from_env`.
            outbound_concurrency: neutrino_main::Config::clamp_outbound_concurrency(
                c.outbound_concurrency as usize,
            ),
            lb_federation_port: c.lb_federation_port,
            // `federation_proxy` is internal/derived (set by neutrino-main when
            // the sidecar runs) and startup jitter isn't FFI-exposed; both take
            // their `Config::default()` values.
            ..Default::default()
        }
    }
}

/// FFI-facing control command. Mirrors `neutrino_ctl::Command` (re-exported
/// as `neutrino_main::Command`) so EX Android can drive the embedded server.
/// Kept here, not on the common `Command`, so UniFFI stays out of the common
/// crates — the same split as `NeutrinoConfig` / `Config`.
#[derive(uniffi::Enum)]
pub enum Command {
    Shutdown,
    KickBackoff,
}

impl From<Command> for neutrino_main::Command {
    fn from(c: Command) -> Self {
        match c {
            Command::Shutdown => neutrino_main::Command::Shutdown,
            Command::KickBackoff => neutrino_main::Command::KickBackoff,
        }
    }
}

/// Fixed localpart for every embedded peer's user: user ids are
/// `@n:{node_id}`. The discovery registry is localpart-agnostic — this is the
/// embedded host's convention, applied by the BLE transport's discovery drain
/// (see `relay_transport`) where the node id is known.
#[cfg(feature = "ble")]
const DISCOVERY_LOCALPART: &str = "n";

#[derive(uniffi::Object)]
pub struct NeutrinoHandle {
    tx: tokio::sync::mpsc::UnboundedSender<neutrino_main::Command>,
    /// The server identity (its resolved `server_name`/node id), published by the
    /// entrypoint once resolved. Read by `server_name()`; `None` until booted.
    identity: tokio::sync::watch::Receiver<Option<String>>,
}

#[uniffi::export]
impl NeutrinoHandle {
    /// Push a control command to the running server. Fire-and-forget: returns
    /// immediately and never blocks — the channel is unbounded, so the sync
    /// `UnboundedSender::send` never awaits, which is what lets this be called
    /// safely from the FFI/JNI thread (a bounded channel would force either an
    /// `async fn` across the boundary or a backpressure-drop policy). A send
    /// after the server has already stopped (receiver dropped) is a silent
    /// no-op.
    pub fn command(&self, command: Command) {
        let _ = self.tx.send(command.into());
    }

    /// Gracefully stop the server. Convenience for `command(Command::Shutdown)`;
    /// preserves the pre-existing FFI method so existing Android callers keep
    /// working unchanged.
    pub fn shutdown(&self) {
        self.command(Command::Shutdown);
    }

    /// Reset outbound retry backoff and retry now. Convenience for
    /// `command(Command::KickBackoff)`; the host calls this when device
    /// connectivity is restored so backed-off destinations reconnect promptly.
    pub fn kick_backoff(&self) {
        self.command(Command::KickBackoff);
    }

    /// The server's resolved federation name — its derived node id, or `None`
    /// until the server has booted and resolved its identity (so the host can
    /// distinguish "not ready yet" from a value rather than racing on an empty
    /// string). Since the embedded server no longer takes a configured name,
    /// this is how the host learns the name to build user ids
    /// (`@localpart:server_name`).
    pub fn server_name(&self) -> Option<String> {
        self.identity.borrow().clone()
    }
}

/// Spawn an owned Tokio runtime and begin polling the server entrypoint with
/// the supplied configuration. Returns a handle for pushing control commands
/// (including `Shutdown`) into the running server. When the server stops
/// (via `Shutdown` or channel close) the runtime is dropped on its OS thread:
/// the executor stops and async tasks are cancelled at their next await point,
/// but joining the blocking pool waits for any in-flight SQLite write to finish
/// — those `spawn_blocking` closures are non-cancellable (`neutrino-store-sqlite`
/// `WRITE_TIMEOUT` surfaces a hung write to its awaiter but does not stop the
/// closure). Normal writes are sub-millisecond, so teardown is effectively
/// immediate; only a runaway closure can delay this thread's exit.
#[uniffi::export]
pub fn start(config: NeutrinoConfig) -> NeutrinoHandle {
    // Route logs to logcat before anything that can fail: a runtime-build error
    // or an `entrypoint` error below must be visible, not written to a stderr that
    // Android discards. Idempotent (entrypoint calls it too).
    neutrino_main::init_tracing();
    // In this build reqwest's TLS backend is unified to rustls (pulled in by iroh)
    // but with no default crypto provider, so building the federation client would
    // panic ("No rustls crypto provider is configured"). The crypto provider is a
    // process-global the embedding host must install; do it here, before the server
    // (or iroh) builds any client. Idempotent: `install_default` returns `Err` if a
    // provider is already set, which we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let config: neutrino_main::Config = config.into();
    // The entrypoint publishes the resolved server name here once identity
    // resolution completes; `server_name()` reads it back.
    let (handoff_tx, handoff_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    // Shared out-of-band discovery registry: the BLE transport writes the peers
    // it scans into it, and the homeserver reads it for user-directory search.
    // One handle for the homeserver (read by user-directory search), one for the
    // BLE transport (which writes scanned peers into it — see the drain task).
    let discovery_for_server = std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new());
    let discovery_for_link = discovery_for_server.clone();
    // Build the federation datagram link from the resolved node secret: an iroh
    // QUIC endpoint, addressed by 32-byte node id, carrying the sidecar's
    // CoAP/CBOR wire over BLE (no OS socket / TUN / virtual IPs). The entrypoint
    // calls this once it has resolved the secret, then injects the link into the
    // lb sidecar. A bind failure is widened to `entrypoint`'s boxed error and
    // fails startup loudly (logged on the runtime thread below).
    let link_factory: neutrino_main::FederationLinkFactory = Box::new(move |secret, name_rx| {
        Box::pin(async move {
            let transport =
                IrohTransport::bind(&secret, RELAY_BIND, name_rx, discovery_for_link).await?;
            Ok(transport as std::sync::Arc<dyn neutrino_main::DatagramLink>)
        })
    });
    std::thread::spawn(move || {
        // Neutrino owns its runtime. current_thread = parity with the previous
        // async-compat global (also current_thread); all DB work is offloaded to
        // the blocking pool, so the executor is I/O-bound. enable_all() = I/O +
        // time drivers (TcpListener, reqwest, tokio::time).
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name("neutrino")
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!(error = %e, "neutrino: failed to build the server runtime");
                return;
            }
        };
        rt.block_on(async {
            // Loudly surface any stall of this single-threaded executor (a task
            // that never yields, or a blocking call on the runtime thread) — it
            // would otherwise silently freeze every task, including /sync.
            watchdog::spawn();
            // The command receiver is threaded into the server; a `Shutdown`
            // command (or every `NeutrinoHandle` being dropped, which closes the
            // channel) drives `serve`'s graceful shutdown and returns here.
            if let Err(e) = neutrino_main::entrypoint(
                config,
                rx,
                Some(handoff_tx),
                Some(link_factory),
                Some(discovery_for_server),
            )
            .await
            {
                tracing::error!(error = %e, "neutrino: server entrypoint exited with an error");
            }
        });
        // `rt` drops here on this OS thread (sync context — safe): the executor
        // stops and async tasks are cancelled at their next await point. Joining
        // the blocking pool, however, waits for any in-flight SQLite write to
        // finish — `spawn_blocking` closures are non-cancellable (the store's
        // `WRITE_TIMEOUT` only surfaces a hung write to its awaiter, it does not
        // stop the closure), so a runaway write can delay this thread's exit.
        // Normal writes are sub-millisecond, so in practice teardown is immediate.
    });
    NeutrinoHandle {
        tx,
        identity: handoff_rx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutrino_config_maps_to_internal_config() {
        let nc = NeutrinoConfig {
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 0, // must clamp to 1
            lb_federation_port: Some(8448),
        };
        let cfg: neutrino_main::Config = nc.into();
        // Dropped from the FFI surface → empty, which triggers identity derivation.
        assert_eq!(cfg.server_name, "");
        assert_eq!(cfg.bind_addr, "127.0.0.1:8008");
        assert_eq!(cfg.localpart, "alice");
        assert_eq!(cfg.storage_dir, std::path::PathBuf::from("/data/neutrino"));
        assert_eq!(cfg.outbound_concurrency, 1);
        assert_eq!(cfg.lb_federation_port, Some(8448));
        // `federation_proxy` is internal/derived, never set from the FFI surface.
        assert_eq!(cfg.federation_proxy, None);
    }

    #[test]
    fn shutdown_enqueues_shutdown_command() {
        // The FFI producer side: shutdown() -> command(Shutdown) -> `From`
        // conversion -> tx.send must land a `Shutdown` on the channel that
        // `serve`'s dispatch loop drains. (The dispatch/teardown side is tested
        // in neutrino-http, where that logic now lives.)
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = NeutrinoHandle {
            tx,
            identity: tokio::sync::watch::channel::<Option<String>>(None).1,
        };
        handle.shutdown();
        assert_eq!(rx.try_recv().unwrap(), neutrino_main::Command::Shutdown);
    }

    #[test]
    fn kick_backoff_enqueues_kick_command() {
        // The FFI producer side for the non-terminal kick: kick_backoff() ->
        // command(KickBackoff) -> `From` -> tx.send lands a `KickBackoff` on the
        // channel `serve`'s dispatch loop drains.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = NeutrinoHandle {
            tx,
            identity: tokio::sync::watch::channel::<Option<String>>(None).1,
        };
        handle.kick_backoff();
        assert_eq!(rx.try_recv().unwrap(), neutrino_main::Command::KickBackoff);
    }

    #[test]
    fn neutrino_config_passes_through_nonzero_concurrency() {
        // Guards against a regression where the clamp emits a constant 1
        // rather than flooring: a non-zero value must pass through unchanged.
        let nc = NeutrinoConfig {
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
            lb_federation_port: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
