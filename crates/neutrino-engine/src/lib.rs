//! The room runtime: per-room state-machine actors, the inbound staging worker,
//! and the outbound federation delivery pool — plus the ports through which it
//! reaches the network.
//!
//! The runtime drives the network only through the traits in [`ports`], so it
//! stays ignorant of the concrete transport (`reqwest`, the low-bandwidth
//! proxy). `neutrino-http` provides the transport implementations, composes the
//! runtime, and exposes it over the HTTP APIs.
//!
//! Backend-agnostic: every component is generic over `S: StorageBackend`
//! (plus `WithStateProvider` where it drives an apply), so production names no
//! concrete store. Only the tests bind `neutrino-store-sqlite` (a dev-dep).

mod gapfill;
pub mod ports;
pub mod reconcile;
mod room_actor;
pub mod sender;
mod util;
pub mod worker;

pub use ports::{
    FederationTransport, ForwardExtremities, MissingEventsFetcher, MissingEventsQuery,
    TransportError,
};
pub use room_actor::{RoomActorError, RoomRegistry};
pub use util::{
    MAX_PDUS_PER_TXN, TxnIdGen, VersionError, room_version, room_version_for_wire, stage_and_poke,
};
