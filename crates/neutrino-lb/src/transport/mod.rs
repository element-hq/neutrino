//! The wire seam. `WireClient` (egress sender) and `WireServer` + `WireHandler`
//! (ingress receiver) abstract the hop between two sidecars. v1 ships one
//! HTTP+CBOR implementation in `http`; a CoAP/UDP implementation will live
//! beside it and be selected in `crate::serve` without changing egress/ingress.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::Method;
use tokio_util::sync::CancellationToken;

pub mod coap;
pub mod http;

/// Upper bound on a single assembled wire body, applied on every leg that
/// hands a body to the transcode/handler. The public federation port is the
/// one truly network-exposed surface, and a body is buffered whole,
/// CBOR-decoded, *and* re-serialized — so an unbounded body is a
/// memory-exhaustion / OOM risk (fatal on the embedded-on-mobile target). A
/// generous cap that no legitimate federation body approaches. The HTTP
/// transport enforces it while buffering; the CoAP transport enforces it on the
/// assembled (post-reassembly) body — see the OOM note in `transport::coap`.
pub(crate) const MAX_WIRE_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("wire transport error: {0}")]
    Transport(String),
    #[error("wire server error: {0}")]
    Serve(String),
}

/// A federation request ready for the wire. `body` is already CBOR (empty for a
/// bodyless GET). `dest` is the peer `server_name` (== host:port for the v1 HTTP
/// transport); it is unused on the ingress side.
#[derive(Debug, Clone)]
pub struct WireRequest {
    pub dest: String,
    pub method: Method,
    /// Path plus query, e.g. `/_matrix/federation/v1/send/123?foo=bar`.
    pub path: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// A federation response from the wire. `body` is CBOR.
#[derive(Debug, Clone)]
pub struct WireResponse {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
}

/// Egress side: send a CBOR request to a peer and return its CBOR response.
#[async_trait]
pub trait WireClient: Send + Sync {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError>;
}

/// Ingress side: turn an inbound CBOR request into a CBOR response. The proxy
/// logic (`crate::ingress`) implements this; the `WireServer` drives it.
#[async_trait]
pub trait WireHandler: Send + Sync + 'static {
    async fn handle(&self, req: WireRequest) -> WireResponse;
}

/// Ingress side: own the wire-facing listener and dispatch each inbound request
/// to `handler` until `shutdown` fires.
#[async_trait]
pub trait WireServer: Send + Sync {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError>;
}

/// Maps a destination `server_name` (the request authority) to the address the
/// wire client should actually dial. The default ([`DirectResolver`]) is
/// identity — dial the authority verbatim, as on a direct-LAN network. The
/// embedded tunnel build injects a resolver that maps a peer's `server_name` to
/// its virtual IP (and registers the route so the relay can carry the traffic),
/// keeping that mapping out of the transport itself — the wire client still
/// just dials whatever `dest` it is handed.
pub trait DestinationResolver: Send + Sync + std::fmt::Debug {
    /// Rewrite a destination authority (`host` or `host:port`) into the address
    /// to dial.
    fn resolve(&self, authority: &str) -> String;
}

/// Identity [`DestinationResolver`]: dial the authority unchanged. Used whenever
/// no tunnel resolver is configured (desktop / direct-LAN federation).
#[derive(Debug, Default, Clone)]
pub struct DirectResolver;

impl DestinationResolver for DirectResolver {
    fn resolve(&self, authority: &str) -> String {
        authority.to_owned()
    }
}
