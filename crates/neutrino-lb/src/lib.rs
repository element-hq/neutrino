//! neutrino-lb: a sidecar that transcodes Server-Server federation bodies
//! between JSON (local side) and CBOR (wire side). See
//! `docs/superpowers/specs/2026-06-15-neutrino-lb-cbor-proxy-design.md`.

pub mod codec;
pub mod egress;
mod error;
mod headers;
pub mod ingress;
pub mod transport;

pub use error::LbError;

use std::net::SocketAddr;
use std::time::Duration;

/// Connect timeout for the proxy's outbound HTTP hops (egress→peer,
/// ingress→loopback upstream). Mirrors `neutrino-http`'s `FederationClient`:
/// without it, a black-holing peer would leak an in-flight request on every
/// sender retry, since the homeserver's own timeout only bounds the loopback
/// hop to the egress, not the real network leg.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Total request timeout for the proxy's outbound HTTP hops.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

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

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::ingress::IngressHandler;
use crate::transport::WireServer;
use crate::transport::http::{HttpWireClient, HttpWireServer};

/// Run both proxy halves until `shutdown` fires. Egress forwards local→wire
/// (JSON→CBOR); ingress serves wire→local (CBOR→JSON→loopback upstream).
/// Returns when both halves have wound down.
pub async fn serve(config: LbConfig, shutdown: CancellationToken) -> Result<(), LbError> {
    // v1 wire transport: HTTP+CBOR. The CoAP/UDP swap replaces these two lines.
    let wire_client = Arc::new(HttpWireClient::new());
    let wire_server = HttpWireServer::new(config.ingress_bind);

    let ingress_handler = Arc::new(IngressHandler::new(config.upstream.clone()));

    let egress = egress::serve(config.egress_bind, wire_client, shutdown.clone());
    let ingress = wire_server.serve(ingress_handler, shutdown.clone());

    // Both run for the process lifetime; surface whichever errors first.
    tokio::select! {
        r = egress => r.map_err(LbError::from),
        r = ingress => r.map_err(LbError::from),
    }
}

impl LbConfig {
    /// Build from environment, mirroring `neutrino_common::Config::from_env`.
    /// `NEUTRINO_LB_INGRESS_BIND` (default `0.0.0.0:8448`),
    /// `NEUTRINO_LB_EGRESS_BIND` (default `127.0.0.1:8009`),
    /// `NEUTRINO_LB_UPSTREAM` (default `http://127.0.0.1:8008`).
    pub fn from_env() -> Result<Self, LbError> {
        let ingress_bind = std::env::var("NEUTRINO_LB_INGRESS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8448".to_owned())
            .parse()
            .map_err(|e| {
                LbError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("NEUTRINO_LB_INGRESS_BIND: {e}"),
                ))
            })?;
        let egress_bind = std::env::var("NEUTRINO_LB_EGRESS_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8009".to_owned())
            .parse()
            .map_err(|e| {
                LbError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("NEUTRINO_LB_EGRESS_BIND: {e}"),
                ))
            })?;
        let upstream = std::env::var("NEUTRINO_LB_UPSTREAM")
            .unwrap_or_else(|_| "http://127.0.0.1:8008".to_owned());
        Ok(Self {
            ingress_bind,
            egress_bind,
            upstream,
        })
    }
}
