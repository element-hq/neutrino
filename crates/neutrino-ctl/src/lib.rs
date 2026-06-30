//! Server control plane: configuration and out-of-band control commands.
//!
//! This is the broadest-scoped crate in the tree (whole-server lifecycle) and
//! deliberately depends on nothing — it sits at the base alongside
//! `neutrino-common` (event-scoped types) so every layer above can read the
//! server's [`Config`] and accept host-pushed [`Command`]s without pulling in
//! Matrix data types.

mod discovery;
pub use discovery::{DiscoveredPeer, DiscoveryRegistry};

use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8008";
const DEFAULT_SERVER_NAME: &str = "localhost";
const DEFAULT_LOCALPART: &str = "alice";
/// Default cap on concurrent in-flight outbound federation transactions.
const DEFAULT_OUTBOUND_CONCURRENCY: usize = 2;
/// Default upper bound on the random startup delay before a freshly-started
/// sender first drains its outbox backlog — spreads a fleet's restart-time
/// retries so they don't flood the network in lockstep. Tests set this to 0.
const DEFAULT_STARTUP_JITTER_MS: u64 = 30_000;
/// Default storage directory: a `data/` subdirectory of the process's working
/// directory rather than the cwd itself, so the server never has to clamp the
/// permissions of (or scatter its DB sidecars across) a directory it doesn't
/// own. The dev binary lands here; Android always overrides it over the FFI.
const DEFAULT_STORAGE_DIR: &str = "./data";

#[derive(Debug, Clone)]
pub struct Config {
    /// The homeserver's federation name (the domain in `@user:server_name`).
    /// **Empty string means "derive it"**: at startup the entrypoint replaces an
    /// empty `server_name` with one derived from the persisted node secret (a
    /// stable per-install identity); a non-empty value is used verbatim. The dev
    /// binary / `from_env` default to a concrete name, so the derive path is for
    /// callers (e.g. the embedded host) that deliberately leave this empty.
    pub server_name: String,
    pub bind_addr: String,
    pub localpart: String,
    /// Display name advertised for this server's user, returned by `/profile`
    /// for the local user and broadcast over the discovery side channel. Empty
    /// by default (the dev binary); the embedded host sets it at startup.
    pub display_name: String,
    /// Max outbound federation transactions in flight across all destinations
    /// at once (the sender pool's global concurrency bound). Always ≥ 1.
    pub outbound_concurrency: usize,
    /// Directory the embedded SQLite database lives in (`<dir>/neutrino.db`).
    /// Defaults to `./data`; Android supplies its app files dir over the FFI.
    /// The server creates this directory if missing, but not its parents —
    /// those are the caller's responsibility (see `SqliteStore::open_in_dir`).
    pub storage_dir: PathBuf,
    /// Outbound federation proxy URL (the `neutrino-lb` egress). **Internal /
    /// derived — not operator-set.** `neutrino-main` fills this in when it runs
    /// the in-process sidecar (see `lb_federation_port`), pointing it at the
    /// loopback egress it allocates; `neutrino-http` reads it to route outbound
    /// federation through the egress. `None` = direct federation (the default).
    pub federation_proxy: Option<String>,
    /// When set, `neutrino-main` runs a `neutrino-lb` sidecar **in-process**
    /// alongside the homeserver (the embedded-on-mobile target), with the CoAP
    /// low-bandwidth wire. This is the public federation port peers'
    /// `server_name` resolves to: the ingress binds `host(bind_addr):port`
    /// (only the port differs from `bind_addr`). The egress is an internal
    /// loopback port `neutrino-main` allocates, and the upstream is `bind_addr`
    /// (which must be loopback-reachable). `None` = direct federation, no
    /// in-process sidecar (the default).
    pub lb_federation_port: Option<u16>,
    /// Upper bound on the random delay a freshly-started outbound sender waits
    /// before its first outbox drain (thundering-herd guard on restart). Default
    /// 30s; tests set it to 0 so post-restart redelivery is immediate.
    pub startup_jitter: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            localpart: DEFAULT_LOCALPART.to_string(),
            display_name: String::new(),
            outbound_concurrency: DEFAULT_OUTBOUND_CONCURRENCY,
            storage_dir: PathBuf::from(DEFAULT_STORAGE_DIR),
            federation_proxy: None,
            lb_federation_port: None,
            startup_jitter: Duration::from_millis(DEFAULT_STARTUP_JITTER_MS),
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_name: std::env::var("NEUTRINO_SERVER_NAME")
                .unwrap_or_else(|_| DEFAULT_SERVER_NAME.to_string()),
            bind_addr: std::env::var("NEUTRINO_BIND_ADDR")
                .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string()),
            outbound_concurrency: parse_outbound_concurrency(
                std::env::var("NEUTRINO_OUTBOUND_CONCURRENCY")
                    .ok()
                    .as_deref(),
            ),
            storage_dir: storage_dir_from(std::env::var("NEUTRINO_STORAGE_DIR").ok().as_deref()),
            // `federation_proxy` is internal/derived (set by neutrino-main when
            // the in-process sidecar runs), not an environment knob.
            federation_proxy: None,
            lb_federation_port: std::env::var("NEUTRINO_LB_FEDERATION_PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok()),
            startup_jitter: std::env::var("NEUTRINO_STARTUP_JITTER_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map_or_else(
                    || Duration::from_millis(DEFAULT_STARTUP_JITTER_MS),
                    Duration::from_millis,
                ),
            // `localpart` (and any future non-env field) defaults from `Default`,
            // so the value lives in exactly one place.
            ..Default::default()
        }
    }

    pub fn user_id(&self) -> String {
        format!("@{}:{}", self.localpart, self.server_name)
    }

    /// Floor an outbound-concurrency value to 1 — zero is meaningless for the
    /// sender semaphore. The single home of that invariant: both `from_env`
    /// and the FFI `From<NeutrinoConfig>` route their input through here so
    /// the floor can't drift between entry points.
    pub fn clamp_outbound_concurrency(n: usize) -> usize {
        n.max(1)
    }
}

/// Out-of-band control commands the embedding host (Android, over FFI) pushes
/// into a running server. Fire-and-forget: senders never block and never
/// receive a reply. The UniFFI-facing mirror and the conversion into this type
/// live in `neutrino-ffi`, keeping UniFFI out of the common crates — the same
/// split used for [`Config`] / `NeutrinoConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Gracefully stop the server. The FFI layer owns the Tokio runtime, so
    /// once the entrypoint returns the runtime is dropped and all its threads
    /// are reclaimed — this is the real embedded-runtime teardown.
    Shutdown,
    /// Reset every outbound destination's retry backoff to base and retry
    /// immediately. The host sends this when device connectivity is restored, so
    /// a destination that backed off while offline reconnects promptly instead
    /// of waiting out a long (up to the backoff cap) retry delay. Non-terminal:
    /// the server keeps running.
    KickBackoff,
}

/// Resolve the storage directory: the env value if present, else the
/// [`DEFAULT_STORAGE_DIR`] (`./data`, resolved lazily at open time so this
/// stays infallible).
fn storage_dir_from(raw: Option<&str>) -> PathBuf {
    raw.map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_DIR))
}

/// Parse + clamp the outbound-concurrency env value: a valid `usize ≥ 1`, else
/// the default. The floor lives in [`Config::clamp_outbound_concurrency`].
fn parse_outbound_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .map(Config::clamp_outbound_concurrency)
        .unwrap_or(DEFAULT_OUTBOUND_CONCURRENCY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_dir_from_defaults_to_data() {
        assert_eq!(
            storage_dir_from(None),
            std::path::PathBuf::from(DEFAULT_STORAGE_DIR)
        );
        assert_eq!(
            storage_dir_from(Some("/data/neutrino")),
            std::path::PathBuf::from("/data/neutrino")
        );
    }

    #[test]
    fn default_config_storage_dir_is_data() {
        assert_eq!(
            Config::default().storage_dir,
            std::path::PathBuf::from(DEFAULT_STORAGE_DIR)
        );
    }

    #[test]
    fn federation_proxy_defaults_to_none() {
        assert_eq!(Config::default().federation_proxy, None);
    }

    #[test]
    fn parse_outbound_concurrency_clamps_and_defaults() {
        assert_eq!(
            parse_outbound_concurrency(None),
            DEFAULT_OUTBOUND_CONCURRENCY
        );
        assert_eq!(
            parse_outbound_concurrency(Some("garbage")),
            DEFAULT_OUTBOUND_CONCURRENCY
        );
        assert_eq!(
            parse_outbound_concurrency(Some("")),
            DEFAULT_OUTBOUND_CONCURRENCY
        );
        assert_eq!(parse_outbound_concurrency(Some("0")), 1);
        assert_eq!(parse_outbound_concurrency(Some("1")), 1);
        assert_eq!(parse_outbound_concurrency(Some("5")), 5);
    }
}
