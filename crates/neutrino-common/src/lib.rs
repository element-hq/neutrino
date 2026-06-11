use std::path::PathBuf;

pub mod event;
pub mod event_id;
pub mod event_view;
pub use event::Event;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8008";
const DEFAULT_SERVER_NAME: &str = "localhost";
const DEFAULT_LOCALPART: &str = "alice";
/// Default cap on concurrent in-flight outbound federation transactions.
const DEFAULT_OUTBOUND_CONCURRENCY: usize = 2;
/// Default storage directory: a `data/` subdirectory of the process's working
/// directory rather than the cwd itself, so the server never has to clamp the
/// permissions of (or scatter its DB sidecars across) a directory it doesn't
/// own. The dev binary lands here; Android always overrides it over the FFI.
const DEFAULT_STORAGE_DIR: &str = "./data";

/// Wire identifier for the only room version this server speaks.
///
/// MSC4242 (State DAGs) is layered on top of Matrix room version 12. The MSC
/// has not been merged into the spec yet, so the wire form is the unstable
/// `org.matrix.msc4242.12`, not the bare `"12"` ruma uses for the merged v12.
/// Stored verbatim in `rooms.room_version`, emitted in the `m.room.create`
/// event's `content.room_version`, and validated against on every inbound
/// create event.
///
/// We can't use `ruma::RoomVersionId::V12` for this — ruma doesn't model
/// MSC4242, so `RoomVersionId::from_str(ROOM_VERSION_ID)` parses as
/// `RoomVersionId::Custom("org.matrix.msc4242.12")`. Compare against this
/// string directly instead.
pub const ROOM_VERSION_ID: &str = "org.matrix.msc4242.12";

#[derive(Debug, Clone)]
pub struct Config {
    pub server_name: String,
    pub bind_addr: String,
    pub localpart: String,
    /// Max outbound federation transactions in flight across all destinations
    /// at once (the sender pool's global concurrency bound). Always ≥ 1.
    pub outbound_concurrency: usize,
    /// Directory the embedded SQLite database lives in (`<dir>/neutrino.db`).
    /// Defaults to `./data`; Android supplies its app files dir over the FFI.
    /// The server creates this directory if missing, but not its parents —
    /// those are the caller's responsibility (see `SqliteStore::open_in_dir`).
    pub storage_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            localpart: DEFAULT_LOCALPART.to_string(),
            outbound_concurrency: DEFAULT_OUTBOUND_CONCURRENCY,
            storage_dir: PathBuf::from(DEFAULT_STORAGE_DIR),
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
