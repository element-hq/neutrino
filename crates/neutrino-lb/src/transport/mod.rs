//! The wire seam. `WireClient` (egress sender) and `WireServer` + `WireHandler`
//! (ingress receiver) abstract the hop between two sidecars. v1 ships one
//! HTTP+CBOR implementation in `http`; a CoAP/UDP implementation will live
//! beside it and be selected in `crate::serve` without changing egress/ingress.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::Method;
use tokio_util::sync::CancellationToken;

pub mod http;

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
