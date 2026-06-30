pub mod event;
pub mod event_builder;
pub mod event_id;
pub mod event_view;
pub mod validate;
pub use event::Event;
pub use validate::FormatError;

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

/// Milliseconds since the Unix epoch, for the federation transaction
/// `origin_server_ts`. Saturates to 0 on a pre-epoch clock — never panics (no
/// `unwrap` on `SystemTime`). Shared by the inbound `backfill` response, the
/// outbound federation client, and the engine's transaction-id source.
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
