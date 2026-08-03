//! The wire seam. `WireClient` (egress sender) and `WireServer` + `WireHandler`
//! (ingress receiver) abstract the hop between two sidecars. An HTTP+CBOR
//! implementation lives in `http` and a CoAP/UDP one in `coap`; `crate::serve`
//! selects between them without changing egress/ingress.

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

/// CoAP content-format for `application/cbor`, and the default for every
/// [`WireRequest`]/[`WireResponse`] `content_format`.
pub const CBOR_CONTENT_FORMAT: u16 = 60;

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
    /// How `body` is encoded, as a CoAP content-format codepoint. Almost always
    /// [`CBOR_CONTENT_FORMAT`]; a [`LinkCodec`](crate::LinkCodec) that rewrites
    /// the body into some other encoding sets this so the far side knows what it
    /// received, and so a capture is dissectable as the thing it actually is.
    /// It rides the wire as the CoAP Content-Format option (the HTTP transport
    /// has no equivalent and ignores it).
    pub content_format: u16,
}

/// A federation response from the wire. `body` is CBOR.
#[derive(Debug, Clone)]
pub struct WireResponse {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
    /// How `body` is encoded — see [`WireRequest::content_format`].
    pub content_format: u16,
}

// Hand-written rather than derived: `content_format` defaults to CBOR, not to
// `u16::default()`, which is `0` — a real content-format (text/plain) and the
// wrong one. Tests fill the field with `..Default::default()`.
impl Default for WireRequest {
    fn default() -> Self {
        Self {
            dest: String::new(),
            method: Method::GET,
            path: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            content_format: CBOR_CONTENT_FORMAT,
        }
    }
}

impl Default for WireResponse {
    fn default() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
            content_format: CBOR_CONTENT_FORMAT,
        }
    }
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
/// embedded datagram build injects a resolver that maps a peer's `server_name`
/// to its bare 64-char hex node id (what the datagram egress dials by), keeping
/// that mapping out of the transport itself — the wire client still just dials
/// whatever `dest` it is handed.
///
/// `resolve` is a **pure** mapping with no side effects: it just rewrites the
/// authority, nothing more.
pub trait DestinationResolver: Send + Sync {
    /// Map a destination authority (`host` or `host:port`) to the address to
    /// dial. Takes ownership so the identity case can hand the string straight
    /// back without re-allocating.
    fn resolve(&self, authority: String) -> String;
}

/// Identity [`DestinationResolver`]: dial the authority unchanged. Used whenever
/// no tunnel resolver is configured (desktop / direct-LAN federation).
#[derive(Debug, Default, Clone)]
pub struct DirectResolver;

impl DestinationResolver for DirectResolver {
    fn resolve(&self, authority: String) -> String {
        authority
    }
}

#[cfg(test)]
mod resolver_tests {
    use super::*;

    #[test]
    fn direct_resolver_is_identity() {
        let r = DirectResolver;
        assert_eq!(r.resolve("peer.example".to_owned()), "peer.example");
        assert_eq!(
            r.resolve("peer.example:8448".to_owned()),
            "peer.example:8448"
        );
        // IPv6 bracket form must round-trip untouched.
        assert_eq!(
            r.resolve("[2001:db8::1]:8448".to_owned()),
            "[2001:db8::1]:8448"
        );
    }
}
