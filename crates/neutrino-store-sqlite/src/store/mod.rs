//! Trait implementations on [`crate::SqliteStore`]. One file per sub-trait
//! so each method's pre/post conditions map 1:1 to a file boundary.

mod dag;
mod deliveries;
mod events;
mod identity;
mod inbox;
mod oob_memberships;
mod outbox;
mod read_state;
mod rooms;
mod staging;
mod state;
mod state_provider;

pub(crate) use events::maintain_room_state;
pub(crate) use read_state::max_read_state_pos;
pub use state_provider::SqliteStateProvider;
