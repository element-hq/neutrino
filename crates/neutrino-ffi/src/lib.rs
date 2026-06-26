uniffi::setup_scaffolding!("neutrino");

#[cfg(feature = "ble")]
mod ble_android;
#[cfg(feature = "ble")]
mod ble_selftest;
// The iroh-backed relay: transport, assembly, and the host-TUN `PacketIo`. Wired
// into the live tunnel by `tunnel::Tunnel` (driven by `start_tunnel`).
mod relay_stack;
mod relay_transport;
mod tun_io;
mod tunnel;

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
            server_name: c.server_name,
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
    /// The TUN packet-capture tunnel. Independent of the command channel above:
    /// the VPN is toggled on/off (each toggle a fresh fd) separately from the
    /// homeserver's lifetime, so it has its own start/stop. Idle by default.
    tunnel: tunnel::Tunnel,
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

    /// Take ownership of an established TUN file descriptor and start the packet
    /// relay over it: IP packets the host writes into the tunnel are carried over
    /// the wire (iroh) to the destination node, and inbound packets are injected
    /// back (see [`tunnel`]). Non-blocking: spawns the relay on the server runtime
    /// and returns immediately.
    ///
    /// `tun_fd` MUST come from `VpnService.Builder.establish()` with ownership
    /// transferred to native code — on the Kotlin side, pass
    /// `ParcelFileDescriptor.detachFd()`, NOT `.fd`. It MUST also be set non-blocking
    /// before being handed over (`Os.fcntlInt(fd, F_SETFL, O_NONBLOCK)`); the relay's
    /// `AsyncFd` reader relies on this so a read never stalls the server executor.
    /// This crate owns and closes the fd from then on; the host must not close it (and
    /// must not keep the `ParcelFileDescriptor` that produced it open).
    ///
    /// `mtu` is the tunnel MTU; currently advisory (clamping packets to the iroh
    /// datagram limit is a relay-layer concern not yet wired — the host sets it).
    ///
    /// The relay runs on the server runtime, so it is cancelled automatically when
    /// the homeserver shuts down: a tunnel cannot outlive its homeserver. Calling
    /// this before the runtime exists is a no-op that closes the fd; calling it
    /// before the server *identity* is resolved is fine — the relay task waits for
    /// it (holding the fd) rather than dropping the tunnel. Safe to call across
    /// repeated VPN toggles: each call installs a fresh fd, replacing any
    /// still-running relay. Pair every `start_tunnel` with a [`Self::stop_tunnel`].
    pub fn start_tunnel(&self, tun_fd: i32, mtu: u32) {
        self.tunnel.start(tun_fd, mtu);
    }

    /// Stop the tunnel reader and close the fd. Idempotent: a call when no tunnel is
    /// running is a no-op. Called on VPN toggle-off and teardown.
    pub fn stop_tunnel(&self) {
        self.tunnel.stop();
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
    // Published once the runtime is built so `start_tunnel` can spawn its reader
    // onto this runtime — which is what ties the tunnel's lifetime to the server's.
    let runtime: std::sync::Arc<std::sync::OnceLock<tokio::runtime::Handle>> =
        std::sync::Arc::new(std::sync::OnceLock::new());
    let runtime_publisher = std::sync::Arc::clone(&runtime);
    // The entrypoint publishes {node secret, shared route table} here once the
    // server identity is resolved; the relay task awaits it (so a `start_tunnel`
    // that races ahead of identity resolution waits rather than losing the tunnel).
    let (handoff_tx, handoff_rx) =
        tokio::sync::watch::channel::<Option<neutrino_main::TunnelHandoff>>(None);
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
        // Publish the handle before blocking so a concurrent `start_tunnel` can spawn
        // onto this runtime. Tasks spawned from the FFI thread are polled by this
        // `block_on`; when it returns, `rt` drops and any reader task is cancelled.
        let _ = runtime_publisher.set(rt.handle().clone());
        rt.block_on(async {
            // The command receiver is threaded into the server; a `Shutdown`
            // command (or every `NeutrinoHandle` being dropped, which closes the
            // channel) drives `serve`'s graceful shutdown and returns here.
            if let Err(e) = neutrino_main::entrypoint(config, rx, Some(handoff_tx)).await {
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
    NeutrinoHandle {
        tx,
        tunnel: tunnel::Tunnel::new(runtime, handoff_rx),
    }
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
            lb_federation_port: Some(8448),
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.server_name, "hs.example");
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
            tunnel: tunnel::Tunnel::new(
                std::sync::Arc::new(std::sync::OnceLock::new()),
                tokio::sync::watch::channel::<Option<neutrino_main::TunnelHandoff>>(None).1,
            ),
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
            tunnel: tunnel::Tunnel::new(
                std::sync::Arc::new(std::sync::OnceLock::new()),
                tokio::sync::watch::channel::<Option<neutrino_main::TunnelHandoff>>(None).1,
            ),
        };
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
            lb_federation_port: None,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
