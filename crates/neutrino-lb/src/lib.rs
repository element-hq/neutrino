//! neutrino-lb: a sidecar that transcodes Server-Server federation bodies
//! between JSON (local side) and CBOR (wire side). See
//! `docs/superpowers/specs/2026-06-15-neutrino-lb-cbor-proxy-design.md`.

pub mod codec;

use std::net::SocketAddr;

/// Runtime configuration for the sidecar.
#[derive(Debug, Clone)]
pub struct LbConfig {
    /// Public federation port (what peers' `server_name` resolve to). The
    /// ingress reverse proxy binds here.
    pub ingress_bind: SocketAddr,
    /// Loopback port the egress forward proxy binds to. `neutrino-http`'s
    /// `federation_proxy` config points here.
    pub egress_bind: SocketAddr,
    /// Base URL of the local `neutrino-http` (loopback), e.g.
    /// `http://127.0.0.1:8008`. The ingress forwards transcoded requests here.
    pub upstream: String,
}
