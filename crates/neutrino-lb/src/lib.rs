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
pub use transport::coap::capture::{CaptureControl, PcapCaptureLink};
pub use transport::coap::datagram::DatagramLink;
pub use transport::{DestinationResolver, DirectResolver};

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

/// Install rustls' ring crypto provider as the process default, once.
///
/// `iroh` (in the ffi build) unifies reqwest's TLS backend onto rustls with NO
/// default crypto provider, and `cargo test --workspace` feature-unifies that
/// onto every crate's reqwest — so any process that builds a `reqwest::Client`
/// panics at build time ("No rustls crypto provider is configured") unless a
/// provider is installed first. Every reqwest-client constructor on the
/// federation path calls this; `neutrino-ffi::start` installs the same provider.
/// Idempotent: `install_default` returns `Err` if one is already set (we ignore
/// it), and the `Once` keeps repeat calls from every construction site cheap.
pub fn install_crypto_provider() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// RFC 9177 §6.2 NON-mode Q-Block timing knobs, exposed on `LbConfig` without
/// leaking coap-rs's `QBlockConfig`, whose extra CON-mode fields (`probing_rate`,
/// `nstart`, `non_probing_wait`) are unread on the NON path. Mapped to coap-rs at
/// transport construction.
///
/// RFC 9177 lets peers choose their timing independently — the 4.08 missing-block
/// exchange is self-describing on the wire. coap-rs's drive model is stricter:
/// the sender only lingers to service inbound 4.08s for a bounded window after
/// its burst, so in practice both ends of a mesh should use comparable values, or
/// a slow recovery can fall outside that window. (Follow-up: make the client
/// linger until the exchange completes rather than for a fixed window.)
///
/// That sender linger is coap-rs's `non_receive_timeout * (non_max_retransmit +
/// 2)` — an implementation quantity, *not* an RFC constant (the RFC's own
/// receiver delay grows exponentially with the per-payload retry count, so it can
/// exceed this linear figure). `CoapWireClient::with_qblock` sizes the
/// total-exchange timeout to cover this linger automatically, so raising these
/// knobs no longer risks a recovery being killed mid-exchange by `REQUEST_TIMEOUT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QBlockTuning {
    /// Blocks sent per burst before the inter-burst congestion delay
    /// (`MAX_PAYLOADS`, default 10).
    pub max_payloads: u32,
    /// Base inter-burst delay (`NON_TIMEOUT`, default 2 s).
    pub non_timeout: Duration,
    /// Receiver wait-for-gaps base before a missing-block request
    /// (`NON_RECEIVE_TIMEOUT`, default 4 s).
    pub non_receive_timeout: Duration,
    /// Max missing-block recovery rounds (`NON_MAX_RETRANSMIT`, default 4).
    pub non_max_retransmit: u32,
}

impl Default for QBlockTuning {
    fn default() -> Self {
        Self {
            max_payloads: 10,
            non_timeout: Duration::from_secs(2),
            non_receive_timeout: Duration::from_secs(4),
            non_max_retransmit: 4,
        }
    }
}

impl QBlockTuning {
    /// Map to coap-rs's `QBlockConfig`, leaving its CON-mode fields
    /// (`probing_rate`, `nstart`, `non_probing_wait`) and the NON field
    /// `non_partial_timeout` (a partial-body hold time, not yet wired in coap-rs
    /// v1) at their defaults — none are read on the NON send/serve path today.
    pub(crate) fn to_qblock_config(self) -> coap::qblock::QBlockConfig {
        coap::qblock::QBlockConfig {
            max_payloads: self.max_payloads,
            non_timeout: self.non_timeout,
            non_receive_timeout: self.non_receive_timeout,
            non_max_retransmit: self.non_max_retransmit,
            ..Default::default()
        }
    }
}

/// Which wire transport the sidecar pair uses. Both peers must agree on the
/// transport; the size knobs are local tunings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireKind {
    /// v1 HTTP+CBOR over TCP (default; debuggable with ordinary tooling).
    #[default]
    Http,
    /// v2 CoAP+CBOR over UDP (low-bandwidth link).
    ///
    /// `block1_size` caps the per-request (Block1) datagram *payload*; `None`
    /// uses coap-rs's 1024 B default. `max_message_size` is this node's total
    /// framed-message budget (payload + CoAP options), set on the server's
    /// `BlockHandler`; `None` uses coap-rs's ~1152 B default. It bounds **both**
    /// the largest inbound request the node accepts in one datagram *and* the
    /// outbound Block2 (response) fragment size — it is not response-only.
    ///
    /// The two are coupled and must be coordinated mesh-wide:
    /// `block1_size + option_overhead ≤ max_message_size`, or a peer whose
    /// request blocks exceed our budget triggers a server down-negotiation that
    /// the coap-rs client cannot handle (it errors). Leave both `None` (current
    /// default, 1024 < 1152) unless tuning for a specific link MTU.
    Coap {
        block1_size: Option<usize>,
        max_message_size: Option<usize>,
    },
    /// v2 CoAP+CBOR over UDP using RFC 9177 Q-Block NON-mode robust transfer.
    ///
    /// Like `Coap`, but the request/response bodies travel as non-confirmable
    /// Q-Block bursts (up to `MAX_PAYLOADS` blocks unacked) with missing-block
    /// recovery, instead of CON stop-and-wait — the throughput win on lossy
    /// serial/radio links. `block1_size` caps the per-burst block payload (`None`
    /// = coap-rs's 1024 B default); `qblock` carries the RFC 9177 §6.2 timing.
    /// No `max_message_size` knob this cut (Block2 follows the szx default).
    CoapQBlock {
        block1_size: Option<usize>,
        qblock: QBlockTuning,
    },
}

/// Runtime configuration for the sidecar.
#[derive(Clone)]
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
    /// Wire transport for the inter-sidecar hop. Defaults to `Http`.
    pub wire: WireKind,
    /// How a destination `server_name` is turned into the address the egress
    /// dials. `None` = direct dial (the authority verbatim), which is the
    /// desktop / direct-LAN behaviour. The embedded datagram build supplies a
    /// resolver that maps `server_name` → its 64-char hex node id.
    pub resolver: Option<Arc<dyn DestinationResolver>>,
    /// In-process federation transport. When `Some`, the CoAP wire runs over this
    /// datagram link (the embedded/iroh build) instead of a UDP socket — keyed by
    /// 32-byte node id, so `ingress_bind` is unused on this path and the resolver
    /// must yield a 64-char hex node id as the egress `dest`. `None` = UDP socket
    /// (dev / direct-LAN), the existing behaviour.
    pub link: Option<Arc<dyn DatagramLink>>,
}

// Hand-written so `DestinationResolver` needn't be `Debug` just to satisfy a
// derive: a trait object can't print anything useful anyway, so the resolver
// field shows only whether one is configured.
impl std::fmt::Debug for LbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LbConfig")
            .field("ingress_bind", &self.ingress_bind)
            .field("egress_bind", &self.egress_bind)
            .field("upstream", &self.upstream)
            .field("wire", &self.wire)
            .field("resolver", &self.resolver.as_ref().map(|_| "<configured>"))
            .field("link", &self.link.as_ref().map(|_| "<configured>"))
            .finish()
    }
}

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::ingress::IngressHandler;
use crate::transport::coap::datagram::{Hub, IrohCoapWireClient, IrohCoapWireServer};
use crate::transport::coap::{CoapWireClient, CoapWireServer};
use crate::transport::http::{HttpWireClient, HttpWireServer};
use crate::transport::{WireClient, WireError, WireServer};

/// Run both proxy halves until `shutdown` fires. Egress forwards local→wire
/// (JSON→CBOR); ingress serves wire→local (CBOR→JSON→loopback upstream).
/// Returns when both halves have wound down. The wire transport (HTTP or CoAP)
/// is chosen by `config.wire`; egress/ingress are identical across both.
pub async fn serve(config: LbConfig, shutdown: CancellationToken) -> Result<(), LbError> {
    let ingress_handler = Arc::new(IngressHandler::new(config.upstream.clone()));
    // Direct dial unless the embedding host injected a tunnel resolver.
    let resolver = config
        .resolver
        .clone()
        .unwrap_or_else(|| Arc::new(DirectResolver));
    // When the embedding host injected a datagram link (the iroh build), the CoAP
    // wire runs over it instead of a UDP socket; the framing config still comes
    // from `config.wire`. Selected purely by injection — `None` keeps the existing
    // UDP socket path 100% intact.
    if let Some(link) = config.link.clone() {
        return serve_over_link(link, config, ingress_handler, resolver, shutdown).await;
    }
    match config.wire {
        WireKind::Http => {
            let wire_client: Arc<dyn WireClient> = Arc::new(HttpWireClient::new());
            let wire_server = HttpWireServer::new(config.ingress_bind);
            run_pair(
                config.egress_bind,
                wire_client,
                wire_server,
                ingress_handler,
                resolver,
                shutdown,
            )
            .await
        }
        WireKind::Coap {
            block1_size,
            max_message_size,
        } => {
            let wire_client: Arc<dyn WireClient> =
                Arc::new(CoapWireClient::with_block1_size(block1_size));
            let wire_server =
                CoapWireServer::with_max_message_size(config.ingress_bind, max_message_size);
            run_pair(
                config.egress_bind,
                wire_client,
                wire_server,
                ingress_handler,
                resolver,
                shutdown,
            )
            .await
        }
        WireKind::CoapQBlock {
            block1_size,
            qblock,
        } => {
            let cfg = qblock.to_qblock_config();
            let wire_client: Arc<dyn WireClient> =
                Arc::new(CoapWireClient::with_qblock(block1_size, cfg.clone()));
            let wire_server = CoapWireServer::with_qblock(config.ingress_bind, cfg);
            run_pair(
                config.egress_bind,
                wire_client,
                wire_server,
                ingress_handler,
                resolver,
                shutdown,
            )
            .await
        }
    }
}

/// Run the proxy with the CoAP wire over an injected [`DatagramLink`] (the
/// embedded/iroh build). One [`Hub`] multiplexes both directions over the link
/// and is shared between the egress client and the ingress server; the framing
/// (CON / Q-Block, sizes) still comes from `config.wire`. The egress forward
/// proxy on `egress_bind` and the resolver are transport-independent, so this
/// reuses [`run_pair`] unchanged. `ingress_bind` is unused on this path (the link
/// is keyed by node id, not an IP/port).
async fn serve_over_link(
    link: Arc<dyn DatagramLink>,
    config: LbConfig,
    ingress_handler: Arc<IngressHandler>,
    resolver: Arc<dyn DestinationResolver>,
    shutdown: CancellationToken,
) -> Result<(), LbError> {
    let hub = Hub::new(link);
    match config.wire {
        WireKind::CoapQBlock {
            block1_size,
            qblock,
        } => {
            let cfg = qblock.to_qblock_config();
            let wire_client: Arc<dyn WireClient> = Arc::new(IrohCoapWireClient::with_qblock(
                hub.clone(),
                block1_size,
                cfg.clone(),
            ));
            let wire_server = IrohCoapWireServer::with_qblock(hub, cfg);
            run_pair(
                config.egress_bind,
                wire_client,
                wire_server,
                ingress_handler,
                resolver,
                shutdown,
            )
            .await
        }
        WireKind::Coap {
            block1_size,
            max_message_size,
        } => {
            let wire_client: Arc<dyn WireClient> =
                Arc::new(IrohCoapWireClient::new(hub.clone(), block1_size));
            let wire_server = IrohCoapWireServer::new(hub, max_message_size);
            run_pair(
                config.egress_bind,
                wire_client,
                wire_server,
                ingress_handler,
                resolver,
                shutdown,
            )
            .await
        }
        // A link with the HTTP wire is a wiring bug: the datagram path is CoAP-only
        // (the link carries CoAP datagrams keyed by node id, not TCP/HTTP).
        WireKind::Http => Err(LbError::from(WireError::Serve(
            "datagram link requires a CoAP wire kind".to_owned(),
        ))),
    }
}

/// Run the egress forward proxy and the `wire_server` ingress concurrently until
/// `shutdown` fires; surface whichever half errors first.
async fn run_pair<S: WireServer>(
    egress_bind: SocketAddr,
    wire_client: Arc<dyn WireClient>,
    wire_server: S,
    ingress_handler: Arc<IngressHandler>,
    resolver: Arc<dyn DestinationResolver>,
    shutdown: CancellationToken,
) -> Result<(), LbError> {
    let egress = egress::serve(egress_bind, wire_client, resolver, shutdown.clone());
    let ingress = wire_server.serve(ingress_handler, shutdown.clone());
    tokio::select! {
        r = egress => r.map_err(LbError::from),
        r = ingress => r.map_err(LbError::from),
    }
}

#[cfg(test)]
mod serve_selection_tests {
    use super::*;
    use std::net::SocketAddr;

    fn cfg(wire: WireKind) -> LbConfig {
        let free = |p: &str| -> SocketAddr {
            let s = std::net::UdpSocket::bind(p).unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        };
        LbConfig {
            ingress_bind: free("127.0.0.1:0"),
            egress_bind: free("127.0.0.1:0"),
            upstream: "http://127.0.0.1:1".to_owned(),
            wire,
            resolver: None,
            link: None,
        }
    }

    /// Test-only [`DatagramLink`] that never yields inbound traffic and accepts
    /// every send: the Hub's drain task parks on `recv`, so the serve-over-link
    /// path comes up and winds down on cancel exactly like the UDP arms, without a
    /// real transport.
    struct ParkedLink;

    #[async_trait::async_trait]
    impl DatagramLink for ParkedLink {
        async fn send(&self, _dst: [u8; 32], _datagram: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        async fn recv(&self) -> Option<([u8; 32], Vec<u8>)> {
            std::future::pending().await
        }
    }

    // The link arm must build the CoAP wire over the injected link (no UDP bind)
    // and wind down on cancel — proves the link branch is wired, mirroring
    // `coap_serve_binds_and_shuts_down` for the datagram path.
    #[tokio::test]
    async fn link_serve_comes_up_and_shuts_down() {
        let mut config = cfg(WireKind::CoapQBlock {
            block1_size: None,
            qblock: QBlockTuning::default(),
        });
        config.link = Some(Arc::new(ParkedLink));
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move { serve(config, server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "link serve did not wind down");
    }

    // A link configured with the HTTP wire is a wiring bug and must be rejected
    // (the datagram path is CoAP-only).
    #[tokio::test]
    async fn link_with_http_wire_is_rejected() {
        let mut config = cfg(WireKind::Http);
        config.link = Some(Arc::new(ParkedLink));
        let err = serve(config, CancellationToken::new()).await;
        assert!(
            matches!(err, Err(LbError::Wire(_))),
            "HTTP wire over a link must error, got {err:?}"
        );
    }

    #[test]
    fn wirekind_defaults_to_http() {
        assert!(matches!(WireKind::default(), WireKind::Http));
    }

    // The Coap arm must build and bind a UDP listener, then wind down on cancel
    // (proves the match arm is wired, not just that the enum exists).
    #[tokio::test]
    async fn coap_serve_binds_and_shuts_down() {
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            serve(
                cfg(WireKind::Coap {
                    block1_size: None,
                    max_message_size: None,
                }),
                server_token,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "coap serve did not wind down");
    }

    // The CoapQBlock arm must build and bind a UDP listener, then wind down on
    // cancel (proves the match arm is wired, not just that the enum exists).
    #[tokio::test]
    async fn coap_qblock_serve_binds_and_shuts_down() {
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move {
            serve(
                cfg(WireKind::CoapQBlock {
                    block1_size: None,
                    qblock: QBlockTuning::default(),
                }),
                server_token,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "coap qblock serve did not wind down");
    }
}

#[cfg(test)]
mod qblock_tuning_tests {
    use super::*;
    use std::time::Duration;

    // Defaults are the RFC 9177 §6.2 NON constants, matching coap-rs/libcoap.
    #[test]
    fn defaults_are_rfc9177_non_constants() {
        let t = QBlockTuning::default();
        assert_eq!(t.max_payloads, 10);
        assert_eq!(t.non_timeout, Duration::from_secs(2));
        assert_eq!(t.non_receive_timeout, Duration::from_secs(4));
        assert_eq!(t.non_max_retransmit, 4);
    }

    // Mapping copies the NON knobs through to the coap-rs config verbatim.
    #[test]
    fn maps_non_knobs_to_coap_config() {
        let t = QBlockTuning {
            max_payloads: 7,
            non_timeout: Duration::from_millis(500),
            non_receive_timeout: Duration::from_millis(900),
            non_max_retransmit: 2,
        };
        let c = t.to_qblock_config();
        assert_eq!(c.max_payloads, 7);
        assert_eq!(c.non_timeout, Duration::from_millis(500));
        assert_eq!(c.non_receive_timeout, Duration::from_millis(900));
        assert_eq!(c.non_max_retransmit, 2);
    }
}
