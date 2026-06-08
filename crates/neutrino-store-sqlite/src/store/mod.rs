//! Trait implementations on [`crate::SqliteStore`]. One file per sub-trait
//! so each method's pre/post conditions map 1:1 to a file boundary.

mod dag;
mod events;
mod inbox;
mod invites;
mod outbox;
mod rooms;
mod staging;
mod state;
mod state_provider;

pub use state_provider::SqliteStateProvider;
