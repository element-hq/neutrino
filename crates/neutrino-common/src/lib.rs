pub mod event;
pub mod event_id;
pub mod event_view;
pub use event::Event;

use std::path::PathBuf;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8008";
const DEFAULT_SERVER_NAME: &str = "localhost";
const DEFAULT_LOCALPART: &str = "alice";
/// Default cap on concurrent in-flight outbound federation transactions.
const DEFAULT_OUTBOUND_CONCURRENCY: usize = 2;

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
    /// Directory the SQLite database lives in. A server always needs one, so
    /// this is required (not optional) — the platform bindings (`neutrino`,
    /// `neutrino-ffi`) are responsible for supplying a value; `Default` leaves
    /// it empty as a placeholder those bindings overwrite.
    pub storage_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_name: DEFAULT_SERVER_NAME.to_string(),
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            localpart: DEFAULT_LOCALPART.to_string(),
            outbound_concurrency: DEFAULT_OUTBOUND_CONCURRENCY,
            storage_path: std::env::current_dir().unwrap(),
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
            // The host binary's default: a stable directory under the system
            // temp dir when `NEUTRINO_STORAGE_PATH` is unset (it survives
            // restarts — unlike a tempfile — but stays out of the cwd).
            storage_path: std::env::var_os("NEUTRINO_STORAGE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("neutrino")),
            // `localpart` (and any future non-env field) defaults from `Default`,
            // so the value lives in exactly one place.
            ..Default::default()
        }
    }

    pub fn user_id(&self) -> String {
        format!("@{}:{}", self.localpart, self.server_name)
    }
}

/// Parse + clamp the outbound-concurrency env value: a valid `usize ≥ 1`, else
/// the default. Zero is meaningless for a semaphore, so it floors to 1.
fn parse_outbound_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1))
        .unwrap_or(DEFAULT_OUTBOUND_CONCURRENCY)
}

#[cfg(test)]
mod tests {
    use super::*;

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
