uniffi::setup_scaffolding!("neutrino");

mod watchdog;

/// FFI-facing server configuration. Mirrors `neutrino_ctl::Config` so EX
/// Android can fully configure the embedded homeserver. Kept here (not on the
/// common `Config`) so UniFFI stays out of the common crates — see the
/// crate-structure rule in CLAUDE.md. Fields are required unless marked with a
/// `uniffi` default; the dev binary's defaults live in
/// `Config::default`/`from_env`.
#[derive(uniffi::Record)]
pub struct NeutrinoConfig {
    pub bind_addr: String,
    pub localpart: String,
    /// The homeserver's federation `server_name` — the domain in
    /// `@localpart:server_name`. `None` (or an empty string) lets the server
    /// derive a stable name from its node identity, which is the embedded
    /// default; the host then reads the result back via
    /// `NeutrinoHandle::server_name()`. Set a concrete value to pin a specific
    /// name: it is recorded on first start and every later start under a
    /// different name is refused (see the server-name identity guard).
    pub server_name: Option<String>,
    /// Absolute path to a writable directory the host owns (e.g. Android's
    /// `context.filesDir`). The DB is `<storage_dir>/neutrino.db`.
    pub storage_dir: String,
    pub outbound_concurrency: u32,
    /// Whether the network is trusted and hence whether we can drop signatures
    pub trusted_network: bool,
    /// When set, runs the in-process `neutrino-lb` CoAP low-bandwidth sidecar.
    /// This is the public federation port the ingress binds — peers'
    /// `server_name` resolves to `host(bind_addr):lb_federation_port`. The
    /// egress is an internal loopback port the server allocates. `None` = direct
    /// federation (no in-process sidecar).
    pub lb_federation_port: Option<u16>,
    /// Absolute path to a directory the server writes rotating `neutrino.*` log
    /// files into, on top of logcat. Android's logcat is a small ring buffer
    /// that also drops lines from chatty UIDs, so a bug report filed minutes
    /// after a failure has already lost it; point this at the host's bug-report
    /// log directory to keep a day of history that the report can upload. The
    /// server creates the directory if missing. `None` = logcat only.
    #[uniffi(default = None)]
    pub log_dir: Option<String>,
}

impl From<NeutrinoConfig> for neutrino_main::Config {
    fn from(c: NeutrinoConfig) -> Self {
        neutrino_main::Config {
            // Pass the host's choice through: `None`/empty derives the identity
            // (a node id) from the persisted secret in the entrypoint — the
            // embedded default, read back via `NeutrinoHandle::server_name()`; a
            // concrete value is used verbatim (and pinned on first start).
            server_name: c.server_name.unwrap_or_default(),
            bind_addr: c.bind_addr,
            localpart: c.localpart,
            storage_dir: std::path::PathBuf::from(c.storage_dir),
            // Floor-to-1 invariant lives on `Config`, shared with `from_env`.
            outbound_concurrency: neutrino_main::Config::clamp_outbound_concurrency(
                c.outbound_concurrency as usize,
            ),
            lb_federation_port: c.lb_federation_port,
            log_dir: c.log_dir.map(std::path::PathBuf::from),
            // Soft-fail hides events that fail auth against *current* state on
            // arrival. In this trusted P2P mesh that mostly catches messages
            // delivered late after a partition (the sender has since left) —
            // hiding them silently drops legitimately-sent messages from the
            // user's timeline, and the verdict is order-dependent so peers
            // disagree on which. Turn the client-side filter off so the
            // embedded client sees the full DAG (the same knob the convergence
            // rig uses; the verdict is still computed and stored).
            enable_soft_failure: false,
            trusted_network: c.trusted_network,
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

/// Failure arming a pcap capture (see `NeutrinoHandle::start_capture`).
#[derive(Debug, uniffi::Error)]
pub enum CaptureError {
    /// The capture file could not be opened/created at the given path. Field is
    /// named `reason` (not `message`) to avoid colliding with `Throwable.message`
    /// in the generated Kotlin exception.
    Io { reason: String },
}

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CaptureError::Io { reason } => write!(f, "pcap capture I/O error: {reason}"),
        }
    }
}

impl std::error::Error for CaptureError {}

/// A peer the embedded server has discovered out of band (over the federation
/// medium's scan), for the host to render in a
/// Settings directory. Host-facing projection of
/// `neutrino_ctl::DiscoveredPeer` plus its `server_name` key; the localpart is
/// omitted (the host builds user ids itself from `server_name`).
#[derive(uniffi::Record)]
pub struct DiscoveredPeer {
    /// The peer's `server_name` (its 64-char hex node id).
    pub server_name: String,
    /// The display name the peer advertised.
    pub display_name: String,
    /// Wall-clock milliseconds of the scan snapshot that last saw this peer.
    /// Uniform across all peers in a snapshot (see `discovered_peers`).
    pub last_seen_ms: u64,
}

#[derive(uniffi::Object)]
pub struct NeutrinoHandle {
    tx: tokio::sync::mpsc::UnboundedSender<neutrino_main::Command>,
    /// The server identity (its resolved `server_name`/node id), published by the
    /// entrypoint once resolved. Read by `server_name()`; `None` until booted.
    identity: tokio::sync::watch::Receiver<Option<String>>,
    /// The fatal error the server exited with, if any (e.g. a bind failure, a
    /// `server_name`/trust-domain mismatch against existing data, or a config
    /// validation error). Published by the boot thread when the entrypoint
    /// returns `Err`; read by `last_error()` so the host can raise a dialog.
    /// `None` while the server is starting or running normally.
    last_error: tokio::sync::watch::Receiver<Option<String>>,
    /// Out-of-band discovery registry, written by the BLE transport's scan drain
    /// and read back by `discovered_peers()`. Shared (same `Arc`) with the
    /// homeserver's user-directory search.
    discovery: std::sync::Arc<neutrino_main::DiscoveryRegistry>,
    /// Runtime-toggleable pcap capture of federation HTTP/JSON (a debug tap).
    /// Same `Arc` as the one wrapping the transport link; the Settings toggle
    /// drives it via `start_capture`/`stop_capture`.
    capture: std::sync::Arc<neutrino_main::CaptureControl>,
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
    /// string). Since the embedded server takes no configured name, this is
    /// how the host learns the name to build user ids
    /// (`@localpart:server_name`).
    pub fn server_name(&self) -> Option<String> {
        self.identity.borrow().clone()
    }

    /// The fatal error the server exited with, or `None` while it is starting or
    /// running normally. The host polls this alongside `server_name()`: a
    /// `Some(server_name)` means "ready", a `Some(last_error)` means "refused to
    /// start — show this message in a dialog". A non-blocking in-memory read
    /// (like `server_name`), safe to call from the FFI/JNI thread. Startup
    /// failures that used to only reach logcat (identity mismatch, bad config,
    /// bind failure) are now observable by the host.
    pub fn last_error(&self) -> Option<String> {
        self.last_error.borrow().clone()
    }

    /// A single-shot snapshot of every peer discovered over the BLE mesh, sorted
    /// by `(display_name, server_name)`. Not live — the host re-calls to refresh.
    /// A non-blocking in-memory read (like `server_name`), so it is safe to call
    /// from the FFI/JNI thread. Empty on a build without BLE discovery, or before
    /// the first scan has landed any peers.
    pub fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        self.discovery
            .all()
            .into_iter()
            .map(|(server_name, peer)| DiscoveredPeer {
                server_name,
                display_name: peer.display_name,
                last_seen_ms: peer.last_seen_ms,
            })
            .collect()
    }

    /// Start mirroring every federation HTTP request/response into a
    /// Wireshark-readable pcap at `path` (an absolute path in host-owned
    /// storage, e.g. app-specific external storage so it is `adb pull`-able).
    /// The capture is HTTP/JSON — what a `tcpdump -i lo` on a desktop would
    /// have shown of the two loopback legs, which Android cannot capture
    /// itself. Errors if the file can't be opened. Calling it while already
    /// capturing rotates to the new file. A non-blocking control call, safe
    /// from the FFI/JNI thread.
    pub fn start_capture(&self, path: String) -> Result<(), CaptureError> {
        self.capture.start(&path).map_err(|e| CaptureError::Io {
            reason: e.to_string(),
        })
    }

    /// Stop capturing and flush + close the file before returning, so it is
    /// immediately ready to `adb pull`. Returns whether a capture was running.
    /// Idempotent.
    pub fn stop_capture(&self) -> bool {
        self.capture.stop()
    }

    /// Whether a capture is currently running — drives the Settings toggle state.
    pub fn is_capturing(&self) -> bool {
        self.capture.is_active()
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
    start_with(config, None)
}

/// The composition seam for out-of-tree federation media. Same as [`start`]
/// but with an injected [`neutrino_main::FederationLinkFactory`]: a downstream
/// crate that provides a concrete [`neutrino_main::DatagramLink`] (e.g. the
/// iroh/BLE medium) calls this from its own `#[uniffi::export]`ed entrypoint,
/// and its cdylib carries both crates' scaffolding. Deliberately NOT
/// uniffi-exported — the factory is a Rust trait-object seam and cannot (and
/// need not) cross the FFI. See `LinkContext` in neutrino-main for the
/// contract an injected medium must uphold.
pub fn start_with(
    config: NeutrinoConfig,
    link_factory: Option<neutrino_main::FederationLinkFactory>,
) -> NeutrinoHandle {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    // Converted first only to read `log_dir` off it; the conversion is pure
    // field mapping and cannot fail, so it does not weaken the guarantee below.
    let config: neutrino_main::Config = config.into();
    // Route logs to logcat (and the host's log directory, if it named one)
    // before anything that can fail: a runtime-build error or an `entrypoint`
    // error below must be visible, not written to a stderr that Android
    // discards. Idempotent (entrypoint calls it too, with the same directory).
    neutrino_main::init_tracing(config.log_dir.as_deref());
    // Runtime-toggleable pcap capture of the federation conversation (a debug
    // tap; see `neutrino_lb::capture`). One handle is threaded into the lb
    // sidecar's HTTP/JSON edges, an identical clone lives on `NeutrinoHandle`
    // for the Settings toggle (`start_capture`/`stop_capture`). Off until the
    // host arms it. The tap is transport-independent, so it records on every
    // build — including the LAN/UDP one, which has no injected link.
    let capture = neutrino_main::CaptureControl::new();
    let capture_for_lb = capture.clone();
    // The entrypoint publishes the resolved server name here once identity
    // resolution completes; `server_name()` reads it back.
    let (handoff_tx, handoff_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    // Fatal-error handoff: the boot thread publishes the entrypoint's error
    // message here so the host can read it back via `last_error()` and raise a
    // dialog, rather than the failure only reaching logcat.
    let (error_tx, error_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    // Shared out-of-band discovery registry: the federation medium
    // writes the peers it discovers into it, and the homeserver
    // reads it for user-directory search. One handle for the homeserver, one
    // reader-side handle for the FFI directory listing (`discovered_peers`);
    // an injected medium receives the same `Arc` via its `LinkContext`.
    let discovery_for_server = std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new());
    let discovery_for_handle = discovery_for_server.clone();
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
                link_factory,
                Some(discovery_for_server),
                Some(capture_for_lb),
            )
            .await
            {
                tracing::error!(error = %e, "neutrino: server entrypoint exited with an error");
                // Surface the message to the host (dialog box); a send error just
                // means the handle was dropped, so nothing is listening.
                let _ = error_tx.send(Some(e.to_string()));
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
        last_error: error_rx,
        discovery: discovery_for_handle,
        capture,
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
            server_name: None,
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 0, // must clamp to 1
            lb_federation_port: Some(8448),
            trusted_network: true,
            log_dir: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        // `None` from the FFI surface → empty, which triggers identity derivation.
        assert_eq!(cfg.server_name, "");
        assert_eq!(cfg.bind_addr, "127.0.0.1:8008");
        assert_eq!(cfg.localpart, "alice");
        assert_eq!(cfg.storage_dir, std::path::PathBuf::from("/data/neutrino"));
        assert_eq!(cfg.outbound_concurrency, 1);
        assert_eq!(cfg.lb_federation_port, Some(8448));
        // `federation_proxy` is internal/derived, never set from the FFI surface.
        assert_eq!(cfg.federation_proxy, None);
        // No directory asked for → platform sink only, no file sink.
        assert_eq!(cfg.log_dir, None);
    }

    #[test]
    fn neutrino_config_passes_through_log_dir() {
        // The host's log directory reaches `Config` as a path, which is what
        // `init_tracing` needs to install the rotating file sink.
        let nc = NeutrinoConfig {
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            server_name: None,
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
            lb_federation_port: None,
            trusted_network: true,
            log_dir: Some("/data/cache/logs".to_string()),
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(
            cfg.log_dir,
            Some(std::path::PathBuf::from("/data/cache/logs"))
        );
    }

    #[test]
    fn neutrino_config_passes_through_configured_server_name() {
        // A concrete name from the host is used verbatim (not derived); the
        // server pins it on first start. `None` (the case above) derives instead.
        let nc = NeutrinoConfig {
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            server_name: Some("hs.example".to_string()),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
            lb_federation_port: None,
            trusted_network: true,
            log_dir: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.server_name, "hs.example");
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
            last_error: tokio::sync::watch::channel::<Option<String>>(None).1,
            discovery: std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new()),
            capture: neutrino_main::CaptureControl::new(),
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
            last_error: tokio::sync::watch::channel::<Option<String>>(None).1,
            discovery: std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new()),
            capture: neutrino_main::CaptureControl::new(),
        };
        handle.kick_backoff();
        assert_eq!(rx.try_recv().unwrap(), neutrino_main::Command::KickBackoff);
    }

    #[test]
    fn discovered_peers_maps_registry_snapshot() {
        let discovery = std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new());
        discovery.upsert(
            "node_bob".to_string(),
            neutrino_main::DiscoveredPeer {
                localpart: "n".to_string(),
                display_name: "Bob".to_string(),
                last_seen_ms: 42,
            },
        );
        discovery.upsert(
            "node_alice".to_string(),
            neutrino_main::DiscoveredPeer {
                localpart: "n".to_string(),
                display_name: "Alice".to_string(),
                last_seen_ms: 7,
            },
        );
        let handle = NeutrinoHandle {
            tx: tokio::sync::mpsc::unbounded_channel().0,
            identity: tokio::sync::watch::channel::<Option<String>>(None).1,
            last_error: tokio::sync::watch::channel::<Option<String>>(None).1,
            discovery,
            capture: neutrino_main::CaptureControl::new(),
        };
        let peers = handle.discovered_peers();
        // Sorted by (display_name, server_name); localpart is dropped, other
        // fields carried through verbatim.
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].server_name, "node_alice");
        assert_eq!(peers[0].display_name, "Alice");
        assert_eq!(peers[0].last_seen_ms, 7);
        assert_eq!(peers[1].server_name, "node_bob");
        assert_eq!(peers[1].last_seen_ms, 42);
    }

    #[test]
    fn last_error_reads_back_published_message() {
        // The boot thread publishes the entrypoint's fatal error on this watch
        // channel; `last_error()` reads the latest value so the host can raise a
        // dialog. `None` before anything fails, `Some(msg)` after.
        let (error_tx, error_rx) = tokio::sync::watch::channel::<Option<String>>(None);
        let handle = NeutrinoHandle {
            tx: tokio::sync::mpsc::unbounded_channel().0,
            identity: tokio::sync::watch::channel::<Option<String>>(None).1,
            last_error: error_rx,
            discovery: std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new()),
            capture: neutrino_main::CaptureControl::new(),
        };
        assert_eq!(handle.last_error(), None, "no error before boot fails");
        error_tx
            .send(Some("server_name mismatch".to_string()))
            .expect("publish");
        assert_eq!(
            handle.last_error(),
            Some("server_name mismatch".to_string()),
            "the published fatal error is observable by the host"
        );
    }

    #[test]
    fn discovered_peers_empty_registry() {
        let handle = NeutrinoHandle {
            tx: tokio::sync::mpsc::unbounded_channel().0,
            identity: tokio::sync::watch::channel::<Option<String>>(None).1,
            last_error: tokio::sync::watch::channel::<Option<String>>(None).1,
            discovery: std::sync::Arc::new(neutrino_main::DiscoveryRegistry::new()),
            capture: neutrino_main::CaptureControl::new(),
        };
        assert!(handle.discovered_peers().is_empty());
    }

    #[test]
    fn neutrino_config_passes_through_nonzero_concurrency() {
        // Guards against a regression where the clamp emits a constant 1
        // rather than flooring: a non-zero value must pass through unchanged.
        let nc = NeutrinoConfig {
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            server_name: None,
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
            lb_federation_port: None,
            trusted_network: true,
            log_dir: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
