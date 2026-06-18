uniffi::setup_scaffolding!("neutrino");

/// FFI-facing server configuration. Mirrors `neutrino_common::Config` so EX
/// Android can fully configure the embedded homeserver. Kept here (not on the
/// common `Config`) so UniFFI stays out of the common crates — see the
/// crate-structure rule in CLAUDE.md. All fields are required; defaults live
/// in `Config::default`/`from_env` for the dev binary.
#[derive(uniffi::Record)]
pub struct NeutrinoConfig {
    pub server_name: String,
    pub bind_addr: String,
    pub localpart: String,
    /// Absolute path to a writable directory the host owns (e.g. Android's
    /// `context.filesDir`). The DB is `<storage_dir>/neutrino.db`.
    pub storage_dir: String,
    pub outbound_concurrency: u32,
    /// Optional `neutrino-lb` egress proxy URL (e.g. `http://127.0.0.1:8009`).
    /// `None` = direct federation.
    pub federation_proxy: Option<String>,
    /// When set, runs a `neutrino-lb` sidecar in-process: this is the public
    /// federation port the ingress binds (what peers' `server_name` resolve to).
    /// Requires `federation_proxy` (the egress) and a loopback `bind_addr`.
    /// `None` = no in-process sidecar.
    pub lb_ingress_bind: Option<String>,
}

impl From<NeutrinoConfig> for neutrino_main::Config {
    fn from(c: NeutrinoConfig) -> Self {
        neutrino_main::Config {
            server_name: c.server_name,
            bind_addr: c.bind_addr,
            localpart: c.localpart,
            storage_dir: std::path::PathBuf::from(c.storage_dir),
            // Floor-to-1 invariant lives on `Config`, shared with `from_env`.
            outbound_concurrency: neutrino_main::Config::clamp_outbound_concurrency(
                c.outbound_concurrency as usize,
            ),
            federation_proxy: c.federation_proxy,
            lb_ingress_bind: c.lb_ingress_bind,
            // Startup jitter isn't an FFI-exposed tunable; take the default.
            ..Default::default()
        }
    }
}

/// FFI-facing control command. Mirrors `neutrino_common::Command` (re-exported
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

#[derive(uniffi::Object)]
pub struct NeutrinoHandle {
    tx: tokio::sync::mpsc::UnboundedSender<neutrino_main::Command>,
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
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let config: neutrino_main::Config = config.into();
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
                eprintln!("failed to build runtime: {e}");
                return;
            }
        };
        rt.block_on(async {
            // The command receiver is threaded into the server; a `Shutdown`
            // command (or every `NeutrinoHandle` being dropped, which closes the
            // channel) drives `serve`'s graceful shutdown and returns here.
            if let Err(e) = neutrino_main::entrypoint(config, rx).await {
                eprintln!("entrypoint exited: {e}");
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
    NeutrinoHandle { tx }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutrino_config_maps_to_internal_config() {
        let nc = NeutrinoConfig {
            server_name: "hs.example".to_string(),
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 0, // must clamp to 1
            federation_proxy: Some("http://127.0.0.1:8009".to_owned()),
            lb_ingress_bind: Some("0.0.0.0:8448".to_owned()),
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.server_name, "hs.example");
        assert_eq!(cfg.bind_addr, "127.0.0.1:8008");
        assert_eq!(cfg.localpart, "alice");
        assert_eq!(cfg.storage_dir, std::path::PathBuf::from("/data/neutrino"));
        assert_eq!(cfg.outbound_concurrency, 1);
        assert_eq!(
            cfg.federation_proxy,
            Some("http://127.0.0.1:8009".to_owned())
        );
        assert_eq!(cfg.lb_ingress_bind, Some("0.0.0.0:8448".to_owned()));
    }

    #[test]
    fn shutdown_enqueues_shutdown_command() {
        // The FFI producer side: shutdown() -> command(Shutdown) -> `From`
        // conversion -> tx.send must land a `Shutdown` on the channel that
        // `serve`'s dispatch loop drains. (The dispatch/teardown side is tested
        // in neutrino-http, where that logic now lives.)
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = NeutrinoHandle { tx };
        handle.shutdown();
        assert_eq!(rx.try_recv().unwrap(), neutrino_main::Command::Shutdown);
    }

    #[test]
    fn kick_backoff_enqueues_kick_command() {
        // The FFI producer side for the non-terminal kick: kick_backoff() ->
        // command(KickBackoff) -> `From` -> tx.send lands a `KickBackoff` on the
        // channel `serve`'s dispatch loop drains.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = NeutrinoHandle { tx };
        handle.kick_backoff();
        assert_eq!(rx.try_recv().unwrap(), neutrino_main::Command::KickBackoff);
    }

    #[test]
    fn neutrino_config_passes_through_nonzero_concurrency() {
        // Guards against a regression where the clamp emits a constant 1
        // rather than flooring: a non-zero value must pass through unchanged.
        let nc = NeutrinoConfig {
            server_name: "hs.example".to_string(),
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
            federation_proxy: None,
            lb_ingress_bind: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
