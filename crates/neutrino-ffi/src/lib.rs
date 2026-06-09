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
}

impl From<Command> for neutrino_main::Command {
    fn from(c: Command) -> Self {
        match c {
            Command::Shutdown => neutrino_main::Command::Shutdown,
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
    /// immediately, never blocks. A send after the server has already stopped
    /// (the receiver was dropped) is a silent no-op.
    pub fn command(&self, command: Command) {
        let _ = self.tx.send(command.into());
    }

    /// Gracefully stop the server. Convenience for `command(Command::Shutdown)`;
    /// preserves the pre-existing FFI method so existing Android callers keep
    /// working unchanged.
    pub fn shutdown(&self) {
        self.command(Command::Shutdown);
    }
}

/// Spawn the Tokio runtime and begin polling the server entrypoint with the
/// supplied configuration. Returns a handle for pushing control commands
/// (including `Shutdown`) into the running server.
#[uniffi::export]
pub fn start(config: NeutrinoConfig) -> NeutrinoHandle {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let config: neutrino_main::Config = config.into();
    std::thread::spawn(move || {
        let rt = async_compat::get_runtime_handle();
        rt.block_on(async {
            tokio::select! {
                res = neutrino_main::entrypoint(config) => {
                    if let Err(e) = res {
                        eprintln!("entrypoint exited: {e}");
                    }
                },
                _ = drain_until_shutdown(rx) => {},
            }
        });
    });
    NeutrinoHandle { tx }
}

/// Drain control commands until a `Shutdown` arrives, or until the channel
/// closes (every `NeutrinoHandle` dropped). Returning resolves the `select!`
/// arm in `start`, which drops the entrypoint future and tears down the
/// runtime — the same lifecycle the old shutdown oneshot drove.
///
/// Only lifecycle commands are handled here. When a server-directed variant is
/// added (e.g. the Approach A `NetworkReachable` backoff kick), the receiver
/// will instead be threaded into `neutrino_main::entrypoint` → `serve`, and the
/// exhaustive `match` below will force that wiring decision at compile time.
async fn drain_until_shutdown(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<neutrino_main::Command>,
) {
    // `never_loop` fires because every current arm of the match returns.
    // The loop is intentional: future non-Shutdown variants (e.g. NetworkReachable)
    // will `continue` rather than `return`, and the exhaustive match will force
    // that decision at compile time.
    #[allow(clippy::never_loop)]
    loop {
        match rx.recv().await {
            None => return, // channel closed — all senders dropped
            Some(neutrino_main::Command::Shutdown) => return,
        }
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
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.server_name, "hs.example");
        assert_eq!(cfg.bind_addr, "127.0.0.1:8008");
        assert_eq!(cfg.localpart, "alice");
        assert_eq!(cfg.storage_dir, std::path::PathBuf::from("/data/neutrino"));
        assert_eq!(cfg.outbound_concurrency, 1);
    }

    #[tokio::test]
    async fn drain_returns_on_shutdown() {
        // A Shutdown command must end the drain loop (which resolves the
        // select! arm in `start` and tears the runtime down).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<neutrino_main::Command>();
        tx.send(neutrino_main::Command::Shutdown).unwrap();
        drain_until_shutdown(rx).await; // hangs (test times out) if it never returns
    }

    #[tokio::test]
    async fn drain_returns_when_all_senders_dropped() {
        // Dropping every NeutrinoHandle closes the channel; the drain must end,
        // preserving the old behaviour where dropping the handle shut the server.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<neutrino_main::Command>();
        drop(tx);
        drain_until_shutdown(rx).await;
    }

    #[test]
    fn command_shutdown_maps_to_internal() {
        let internal: neutrino_main::Command = Command::Shutdown.into();
        assert_eq!(internal, neutrino_main::Command::Shutdown);
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
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
