pub mod event;
pub use event::Event;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8008";
const DEFAULT_SERVER_NAME: &str = "localhost";
const DEFAULT_LOCALPART: &str = "alice";

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
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_name: std::env::var("NEUTRINO_SERVER_NAME")
                .unwrap_or_else(|_| DEFAULT_SERVER_NAME.to_string()),
            bind_addr: std::env::var("NEUTRINO_BIND_ADDR")
                .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string()),
            localpart: DEFAULT_LOCALPART.to_string(),
        }
    }

    pub fn user_id(&self) -> String {
        format!("@{}:{}", self.localpart, self.server_name)
    }
}
