//! Engine ports — the dependency-inversion seams between the room runtime and
//! its outbound I/O.
//!
//! The runtime (per-room actors, the inbound staging worker, the outbound
//! delivery pool) drives the network only through the traits defined here, so
//! it stays ignorant of the concrete transport (`reqwest`, the low-bandwidth
//! proxy). `neutrino-http` provides the implementations.
//!
//! Phase 1 of the `neutrino-engine` extraction: the ports + the types that
//! cross them live here; the runtime code that consumes them still lives in
//! `neutrino-http` and moves in a later phase.

mod ports;

pub use ports::{
    FederationTransport, ForwardExtremities, MissingEventsFetcher, MissingEventsQuery,
    TransportError,
};
