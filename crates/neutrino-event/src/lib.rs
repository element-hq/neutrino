pub mod event;
pub mod event_builder;
pub mod event_id;
pub mod event_view;
pub mod room_version;
pub mod sign;
pub mod validate;
pub use event::Event;
pub use event_builder::{RoomVersionKeys, UnverifiedWire, Wire, room_version_keys};
pub use room_version::{RegistryError, RoomVersion, RoomVersions, base_version};
pub use sign::{
    CoSignError, EventPolicy, EventSecurity, EventSigner, KeyResolveError, KeyResolver,
    NodeIdKeyResolver, SIGNING_KEY_ID, VerifyError, verify_event_signature, verify_event_signed_by,
};
pub use validate::{FormatError, SemanticVerdict, semantic_verdict};

/// Wire identifier of the **base** room version — the one every build
/// understands and creates rooms under unless its federation medium declares
/// another (see [`RoomVersions`]).
///
/// MSC4242 (State DAGs) is layered on top of Matrix room version 12. The MSC
/// has not been merged into the spec yet, so the wire form is the unstable
/// `org.matrix.msc4242.12`, not the bare `"12"` ruma uses for the merged v12.
/// Stored verbatim in `rooms.room_version` and emitted in the `m.room.create`
/// event's `content.room_version`.
///
/// Nothing compares against this in production any more — which version an
/// event belongs to is resolved through [`RoomVersions`], because a store may
/// legitimately hold rooms of several. It remains the base version's
/// [`RoomVersion::id`] (and what test fixtures stamp).
///
/// We can't use `ruma::RoomVersionId::V12` for this — ruma doesn't model
/// MSC4242, so `RoomVersionId::from_str(ROOM_VERSION_ID)` parses as
/// `RoomVersionId::Custom("org.matrix.msc4242.12")`.
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
