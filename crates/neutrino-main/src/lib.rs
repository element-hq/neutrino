mod platform;
mod resolver;

use std::net::SocketAddr;
use std::sync::Arc;

pub use neutrino_ctl::{Command, Config, DiscoveredPeer, DiscoveryRegistry};
pub use neutrino_event::{KeyResolver, NodeIdKeyResolver};
pub use neutrino_lb::{
    CaptureControl, CodecError, DatagramLink, LinkAddr, LinkCodec, LinkProfile, PcapCaptureLink,
    WireRequest, WireResponse,
};

use std::future::Future;
use std::pin::Pin;

use neutrino_store::IdentityStore;
use neutrino_store_sqlite::SqliteStore;
use rand::RngCore;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// What a federation medium's factory returns: the datagram link plus the
/// medium's key-resolution *capability* — how `(server_name, key_id)` maps to
/// a verify key in the medium's namespace (node-id names decode to their own
/// key via [`neutrino_event::NodeIdKeyResolver`]; a DNS medium would fetch
/// `/_matrix/key/v2/server`; `None` = the medium has no key story). Whether
/// signing actually happens is NOT the medium's call — that is the app's
/// `trusted_network` config; the capability is only consumed when the app
/// opts into signed mode. Wire-level facts (MTU, connection authentication)
/// ride [`DatagramLink::profile`].
pub struct FederationLink {
    pub link: std::sync::Arc<dyn DatagramLink>,
    pub key_resolver: Option<Arc<dyn neutrino_event::KeyResolver>>,
}

/// Result of building the federation datagram link from the node secret.
pub type DatagramLinkResult = Result<FederationLink, Box<dyn std::error::Error + Send + Sync>>;

/// Everything an injected federation medium needs from the server, packed by
/// [`entrypoint`] once identity resolution completes. This is the whole
/// contract between the homeserver and an out-of-tree [`DatagramLink`]
/// implementation — nothing may be smuggled in by closure capture, so the
/// medium and the server cannot disagree on which registry (or channel) they
/// share.
///
/// Contract for implementors:
/// - Link addresses are medium-defined bytes: a peer's address is its
///   federation `server_name` as the egress resolver renders it (UTF-8,
///   verbatim, any length). On the node-id medium that is the lowercase 64-hex
///   of the peer's ed25519 public key — and the local node id MUST then be the
///   public key derived from `secret` (the server's derived `server_name` is
///   its lowercase hex). Other media (e.g. mDNS LAN names) define their own
///   naming; the datagram path never interprets the bytes.
/// - The source address a [`DatagramLink::recv`] tags each datagram with MUST
///   identify the sending peer as strongly as the medium can
///   (`authenticates_connections` declares how strongly). Events carry no
///   signatures on a trusted network, so that tag is what the ingress binds a
///   request's claimed `X-Matrix origin` to (exact byte equality — names are
///   canonical, lowercase for hex media).
/// - The medium declares facts and capabilities, never policy: CoAP
///   fragmentation is sized from [`DatagramLink::profile`]'s `max_datagram`,
///   `authenticates_connections` states whether the hop is trusted, and the
///   optional [`FederationLink::key_resolver`] says how keys resolve in the
///   medium's namespace. Whether events are signed/verified is the app's
///   `trusted_network` config (see `SecurityConfig` in [`entrypoint`]).
pub struct LinkContext {
    /// The persisted 32-byte node secret the server's identity is derived from.
    pub secret: [u8; 32],
    /// Current + future local display name: the medium advertises the current
    /// value for peer discovery and re-advertises on change.
    pub display_name: watch::Receiver<String>,
    /// Shared out-of-band discovery registry. The medium writes the peers it
    /// discovers into it, keyed by each peer's federation `server_name` (on
    /// the node-id medium: the lowercase hex node id); user-directory search
    /// and the host's peer list read the same set.
    pub discovery: Arc<DiscoveryRegistry>,
    /// Command fan-in back into the server. A medium pulses
    /// [`Command::KickBackoff`] when a peer (re)appears so destinations that
    /// backed off while that peer was unreachable retry promptly.
    pub commands: tokio::sync::mpsc::UnboundedSender<Command>,
}

/// Builds the federation datagram link once the node secret is resolved. The
/// embedded build injects one (see [`LinkContext`] for the contract); the dev
/// binary passes `None` (plain UDP federation). Async because binding the
/// transport is async.
pub type FederationLinkFactory =
    Box<dyn FnOnce(LinkContext) -> Pin<Box<dyn Future<Output = DatagramLinkResult> + Send>> + Send>;

/// Install the tracing subscriber that routes our logs to the platform sink
/// (logcat on Android). Idempotent. [`entrypoint`] calls this itself, but an
/// embedding host (the FFI) should also call it *before* spawning the server
/// runtime so that a failure to even build the runtime — or any error returned
/// from `entrypoint` — is logged rather than written to a stderr nothing reads.
pub fn init_tracing() {
    platform::init_tracing();
}

pub async fn entrypoint(
    mut config: Config,
    commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
    // Published once identity resolution completes so an embedding host (the
    // ffi/Android layer) can read the server's resolved federation name (its node
    // id) back off the watch channel. The node secret flows to the host via
    // `link_factory`, not here, so this carries only the identity string;
    // non-embedded callers (the dev binary) pass `None`.
    handoff: Option<watch::Sender<Option<String>>>,
    link_factory: Option<FederationLinkFactory>,
    // Out-of-band peer discovery registry. The embedding host (ffi) supplies the
    // `Arc` it keeps a write handle to, so its BLE-discovery callback and the
    // homeserver's user-directory search read the same set. Non-embedded callers
    // (the dev binary / tests) pass `None` and a fresh empty registry is used.
    discovery: Option<Arc<DiscoveryRegistry>>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let discovery = discovery.unwrap_or_else(|| Arc::new(DiscoveryRegistry::new()));

    // Open the store once, here, and resolve the server's stable identity from
    // it before anything reads `config`. The same handle is threaded into the
    // homeserver (`serve`) so the database is opened exactly once.
    let store = Arc::new(
        SqliteStore::open_in_dir(&config.storage_dir)
            .await?
            .client_hides_soft_failed(config.enable_soft_failure),
    );
    let secret = resolve_server_identity(&mut config, &store).await?;

    // The local display name, sourced from the store (the client sets it via
    // `PUT /profile/.../displayname`). A `watch` carries it to two consumers: the
    // http PUT handler updates it (below, injected into `serve`), and the BLE
    // transport advertises + re-advertises it (via `link_factory`). Seeded with
    // the persisted value, or the product default when never set.
    let display_name = store
        .get_display_name()
        .await?
        .unwrap_or_else(|| neutrino_ctl::DEFAULT_DISPLAY_NAME.to_string());
    let (display_name_tx, display_name_rx) = watch::channel(display_name);

    // Command fan-in. The host's receiver is forwarded into an internal channel
    // so an injected medium can also push commands (`KickBackoff` on peer
    // reappearance) — the medium gets a sender clone via `LinkContext`, and
    // `serve` drains the merged stream. When the host side closes (every handle
    // dropped), forward a final `Shutdown`: the medium's sender keeps the
    // internal channel open, so the old close-to-shutdown contract must ride an
    // explicit command through the indirection.
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn({
        let cmd_tx = cmd_tx.clone();
        let mut host = commands;
        async move {
            while let Some(c) = host.recv().await {
                if cmd_tx.send(c).is_err() {
                    return; // server side gone — nothing left to forward to
                }
            }
            let _ = cmd_tx.send(Command::Shutdown);
        }
    });

    // Build the federation datagram link from the resolved secret (the embedded
    // build injects a factory; the dev binary passes `None` for plain UDP
    // federation). A failed transport bind must fail startup loudly — propagate
    // with `?` rather than silently falling back to UDP.
    // The factory's error is `Send + Sync` (so it can cross the ffi task
    // boundary); widen it to this function's plain `Box<dyn Error>` on the way
    // out, since the two boxed-trait-object types don't auto-convert via `?`.
    let (link, key_resolver) = match link_factory {
        Some(factory) => {
            let FederationLink { link, key_resolver } = factory(LinkContext {
                secret,
                display_name: display_name_rx,
                discovery: discovery.clone(),
                commands: cmd_tx,
            })
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
            (Some(link), key_resolver)
        }
        // No injected medium: plain UDP, which authenticates nothing and has
        // no key-resolution namespace of its own.
        None => (None, None),
    };

    // Compose the deployment's security configuration from its two
    // independently-owned facts: the transport declares whether the hop is
    // authenticated (its LinkProfile; plain UDP authenticates nothing), the
    // app declares whether origins are trusted on faith (`trusted_network`).
    // All four combinations are valid deployments — e.g. sigs=on/auth=off is
    // a future anonymous store-and-forward relay, sigs=off/auth=on is
    // today's BLE mesh — so nothing is refused here; the composition is
    // logged so a deployment's posture is one grep away.
    let link_profile = link.as_ref().map(|l| l.profile()).unwrap_or_default();
    let security_config = SecurityConfig {
        authenticate_connections: link
            .as_ref()
            .map(|l| l.profile().authenticates_connections)
            .unwrap_or(false),
        sign_messages: !config.trusted_network,
    };
    tracing::info!(?security_config, "federation security");
    let security = event_security(&security_config, key_resolver, &secret, &config.server_name)?;

    // A store's events belong to exactly one trust domain: unsigned events
    // can never serve a signed deployment (nothing to verify) and vice versa.
    // First start records the mode; a later start under the other mode is
    // refused — wipe the database to switch.
    let domain = match security_config.sign_messages {
        true => "signed",
        false => "transitive",
    };
    let recorded = store.get_or_create_trust_domain(domain).await?;
    if recorded != domain {
        return Err(format!(
            "store at {} belongs to trust domain {recorded:?} but the medium declares \
             {domain:?}; signed and unsigned event histories cannot mix — delete the \
             database to switch modes",
            config.storage_dir.display()
        )
        .into());
    }

    // When embedded, publish the resolved server name to the host over the
    // handoff (the host reads it back off the watch channel). The node secret
    // reached the host via `link_factory` above; outbound federation is addressed
    // by node id over the datagram link, so there is no route table to wire.
    if let Some(handoff) = &handoff {
        let _ = handoff.send(Some(config.server_name.clone()));
    }

    // Embedded low-bandwidth sidecar: when `lb_federation_port` is set we run a
    // `neutrino-lb` proxy in-process beside the homeserver (the embedded-on-
    // mobile target). The ingress owns the public federation port peers
    // reach (`host(bind_addr):lb_federation_port`) and forwards inbound
    // federation to the homeserver's loopback; the homeserver routes its
    // outbound federation back through the egress, an internal loopback port we
    // allocate here and wire in via `federation_proxy`.
    //
    // Allocate + derive (and so validate) the sidecar config *before* binding
    // the listener: an illegal in-process combo (a non-loopback `bind_addr`)
    // then fails fast without first claiming the public port.
    let lb_config = match config.lb_federation_port {
        Some(port) => {
            let egress_bind = alloc_loopback_egress()?;
            config.federation_proxy = Some(format!("http://{egress_bind}"));
            Some(build_lb_config(
                &config,
                port,
                egress_bind,
                link.clone(),
                link_profile,
            )?)
        }
        None => None,
    };

    // Bind through `canonical_loopback` so a `localhost` bind lands on IPv4
    // deterministically — the same family the ingress upstream targets. Binding
    // the raw `localhost` lets the resolver pick `::1` while the upstream dials
    // `127.0.0.1` (or vice versa), which is a connect-refused 502 on the loopback
    // hop. `config.bind_addr` is left raw for `neutrino_http::serve`.
    let listener = tokio::net::TcpListener::bind(&canonical_loopback(&config.bind_addr)).await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    match lb_config {
        Some(lb_config) => {
            tracing::info!(
                ingress = %lb_config.ingress_bind,
                egress = %lb_config.egress_bind,
                upstream = %lb_config.upstream,
                "starting in-process neutrino-lb sidecar"
            );
            let shutdown = CancellationToken::new();
            let lb = neutrino_lb::serve(lb_config, shutdown.clone());
            let hs = neutrino_http::serve(
                listener,
                config,
                store,
                cmd_rx,
                discovery,
                Some(display_name_tx),
                security,
            );
            tokio::pin!(lb, hs);
            tokio::select! {
                // The homeserver owns the command channel, so it drives the
                // lifecycle: when it winds down, stop the sidecar and join it.
                r = &mut hs => {
                    shutdown.cancel();
                    let _ = (&mut lb).await;
                    match &r {
                        Ok(()) => tracing::info!("homeserver stopped; shut down sidecar"),
                        Err(e) => tracing::error!(error = %e, "homeserver exited with an error"),
                    }
                    r?;
                }
                // The sidecar runs until `shutdown`, so returning here means it
                // stopped on its own — always a failure (it has no clean early
                // exit). Surface it loudly; dropping `hs` then stops the homeserver,
                // which is why a sidecar bind/serve failure takes the whole server
                // down.
                r = &mut lb => {
                    match &r {
                        Ok(()) => tracing::error!(
                            "in-process neutrino-lb sidecar returned unexpectedly; stopping homeserver"
                        ),
                        Err(e) => tracing::error!(
                            error = %e,
                            "in-process neutrino-lb sidecar failed; stopping homeserver"
                        ),
                    }
                    r?;
                }
            }
        }
        None => {
            neutrino_http::serve(
                listener,
                config,
                store,
                cmd_rx,
                discovery,
                Some(display_name_tx),
                security,
            )
            .await?
        }
    }
    Ok(())
}

/// Ensure the server has a stable identity before serving.
///
/// The server is identified by a persistent 32-byte secret, generated once on
/// first start from a CSPRNG and kept across restarts, so the server names
/// itself deterministically. The secret is persisted **unconditionally** — it
/// is the server's identity seed, which lower layers (the transport that maps
/// this name onto a route) consume independently of whether a `server_name` was
/// configured. When the caller supplies no `server_name`, derive one from the
/// secret here; a configured name is kept verbatim.
///
/// Operates on the caller-opened `store` (threaded on into `serve`), so this
/// identity bootstrap stays out of the request layer (`neutrino-http`, which is
/// agnostic to how its name is chosen) without re-opening the database.
async fn resolve_server_identity(
    config: &mut Config,
    store: &SqliteStore,
) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut seed = [0u8; 32];
    rand::rng().fill_bytes(&mut seed);
    let secret = store.get_or_create_node_secret(seed).await?;
    if config.server_name.is_empty() {
        config.server_name = server_identity_from_secret(&secret);
    }
    // Pin the effective name to the store on first start and refuse to boot if a
    // later start disagrees. The name is baked into every stored event's
    // sender/origin, so booting existing data under a different name forks the
    // server's identity: the new name is a member of nothing, its rooms are
    // orphaned, and peers can no longer route to it. First write wins, like the
    // node secret and trust domain — wipe the database to change the name.
    let recorded = store.get_or_create_server_name(&config.server_name).await?;
    if recorded != config.server_name {
        let msg = format!(
            "server_name mismatch: this database was created as {recorded:?} but the server \
             is starting as {:?}. The name is baked into every stored event, so changing it \
             would fork the server's identity and orphan its rooms — delete the database to \
             start fresh under the new name.",
            config.server_name
        );
        tracing::error!("{msg}");
        return Err(msg.into());
    }
    Ok(secret)
}

/// Derive a stable, collision-resistant server identity from its secret: the
/// lowercase-hex fingerprint of the secret's ed25519 public key. This is a
/// general public-key identity; how the name maps onto a network route is the
/// transport's concern, not this function's.
fn server_identity_from_secret(secret: &[u8; 32]) -> String {
    let signing = ed25519_dalek::SigningKey::from_bytes(secret);
    hex::encode(signing.verifying_key().to_bytes())
}

/// The deployment's security configuration — Kegan's two fundamental bools,
/// each set by its own authority and composed only here:
///
/// - `authenticate_connections` ("trust the hop") comes from the pluggable
///   transport: only it knows whether it authenticates who each datagram
///   came from.
/// - `sign_messages` ("trust the origin", inverted) comes from the app
///   configuration: only the operator knows whether the network is trusted.
///
/// All four combinations are valid; this struct exists to make the composed
/// posture explicit (and greppable in the startup log) rather than to gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SecurityConfig {
    authenticate_connections: bool,
    sign_messages: bool,
}

/// Realise a [`SecurityConfig`] as the [`neutrino_event::EventSecurity`]
/// value the http/engine stack runs under. Signed mode needs the medium's
/// key-resolution capability — the one composition that can fail: signing
/// without any way to resolve peers' keys is a configuration error, not a
/// policy choice.
fn event_security(
    config: &SecurityConfig,
    key_resolver: Option<Arc<dyn neutrino_event::KeyResolver>>,
    secret: &[u8; 32],
    server_name: &str,
) -> Result<neutrino_event::EventSecurity, String> {
    if !config.sign_messages {
        return Ok(neutrino_event::EventSecurity::TrustedNetwork);
    }
    let resolver = key_resolver.ok_or(
        "trusted_network = false requires a key resolver, but the federation          medium provides none (FederationLink.key_resolver) — inbound events          could never be verified",
    )?;
    Ok(neutrino_event::EventSecurity::Signed {
        signer: Arc::new(neutrino_event::EventSigner::new(secret, server_name)),
        resolver,
    })
}

/// Derive the in-process sidecar's [`neutrino_lb::LbConfig`] from the homeserver
/// `Config`, the chosen public federation port, the already-allocated loopback
/// `egress_bind`, and the link's declared [`LinkProfile`] (the default profile
/// on the UDP path). The ingress reuses `bind_addr`'s host with the port
/// replaced by `fed_port` (only the port differs — see [`ingress_bind_for`]);
/// the upstream is the homeserver's own `bind_addr` (which must be
/// loopback-reachable). Errors if `bind_addr` is a concrete non-loopback
/// address (see [`upstream_url`]) or the profile's MTU is too small to frame a
/// block.
fn build_lb_config(
    config: &Config,
    fed_port: u16,
    egress_bind: SocketAddr,
    link: Option<Arc<dyn DatagramLink>>,
    profile: LinkProfile,
) -> Result<neutrino_lb::LbConfig, Box<dyn std::error::Error>> {
    Ok(neutrino_lb::LbConfig {
        ingress_bind: ingress_bind_for(&config.bind_addr, fed_port),
        egress_bind,
        upstream: upstream_url(&config.bind_addr)?,
        // Q-Block sized to the medium's declared MTU. Only the block-option
        // framing is reserved — what else rides each block is controlled by
        // the medium's LinkCodec, and an over-MTU datagram is the link's to
        // detect and log loudly (see `WireKind::coap_qblock_for_mtu`).
        wire: neutrino_lb::WireKind::coap_qblock_for_mtu(profile.max_datagram)?,
        // The in-process sidecar is the embedded/datagram-link target: map a
        // peer's node-id `server_name` to its bare 64-char hex node id so the
        // datagram egress dials the peer over the link directly. Dormant for a
        // non-node `server_name` (dev/named servers pass through to direct dial).
        resolver: Some(Arc::new(resolver::NodeIdResolver::new())),
        // The injected federation transport (the embedded build) — when `Some`,
        // the sidecar's CoAP wire runs over it instead of a UDP socket. `None` for
        // dev/LAN keeps the UDP path.
        link,
    })
}

/// The public federation ingress bind: the port peers reach this node on (the
/// UDP/LAN path; on the embedded datagram-link path inbound federation arrives
/// over the link, not this socket, but the `LbConfig` still carries the field).
///
/// An unspecified `bind_addr` (the embedded `0.0.0.0`/`::` case) binds
/// IPv6-unspecified `[::]` so a dual-stack listener accepts both families; a
/// *concrete* host is kept verbatim (dev/test bind a specific loopback); a non-IP
/// authority (`localhost:8008`, the offline fallback) also takes `[::]` so the
/// listener still comes up (an offline device has no peers regardless).
fn ingress_bind_for(bind_addr: &str, fed_port: u16) -> SocketAddr {
    match bind_addr.parse::<SocketAddr>() {
        Ok(addr) if !addr.ip().is_unspecified() => SocketAddr::new(addr.ip(), fed_port),
        _ => SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), fed_port),
    }
}

/// Pin the loopback hostname `localhost` to `127.0.0.1`. `localhost` resolves to
/// BOTH `127.0.0.1` and `::1`, and the homeserver's listener bind and the ingress
/// upstream resolve it independently — so they can land on different families,
/// and a listener bound to one while the ingress dials the other is a
/// connect-refused `502` (the "Empty Room" invite failure; see the
/// `upstream_address_family_mismatch_yields_bad_gateway` test). Forcing IPv4 on
/// both keeps them in lockstep. Numeric addresses and other hostnames pass
/// through unchanged.
fn canonical_loopback(authority: &str) -> String {
    match authority.rsplit_once(':') {
        Some((host, port)) if host.eq_ignore_ascii_case("localhost") => {
            format!("127.0.0.1:{port}")
        }
        _ => authority.to_owned(),
    }
}

/// Allocate an ephemeral loopback port for the sidecar egress (the homeserver's
/// outbound forward proxy). Probe-bind `127.0.0.1:0`, read the assigned address,
/// and release it; the egress listener re-binds it. The egress is loopback by
/// construction, so its unauthenticated open forward proxy stays off the network
/// without an explicit guard. (The brief probe→re-bind window is the same
/// free-port pattern the e2e tests use.)
fn alloc_loopback_egress() -> Result<SocketAddr, String> {
    let probe = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("allocating sidecar egress port: {e}"))?;
    probe
        .local_addr()
        .map_err(|e| format!("reading sidecar egress port: {e}"))
}

/// The loopback URL the ingress uses to reach the co-located homeserver. The
/// homeserver runs in the same process, so the ingress must reach it over
/// loopback — and in-process mode therefore requires a loopback-reachable
/// `bind_addr`:
/// - a loopback address (`127.0.0.1`, `[::1]`) is used verbatim;
/// - an unspecified bind (`0.0.0.0`, used so CSAPI can be port-published in a
///   container) still listens on loopback, so it is rewritten to it;
/// - a concrete *non*-loopback address (e.g. a LAN interface) is **rejected**:
///   the homeserver would listen only there, so a loopback rewrite would miss
///   it and a verbatim URL would send the ingress→upstream hop off the loopback
///   path — exposing the unauthenticated CSAPI on the network. Fail loudly
///   rather than silently going off-box.
/// - the loopback hostname `localhost` is pinned to `127.0.0.1` (see
///   [`canonical_loopback`]) so the hop lands on the same family the listener
///   binds; any other non-IP authority (`hostname:port`) can't be classified
///   without resolution, so it is trusted verbatim.
fn upstream_url(bind_addr: &str) -> Result<String, String> {
    let bind_addr = canonical_loopback(bind_addr);
    match bind_addr.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_loopback() => Ok(format!("http://{bind_addr}")),
        Ok(addr) if addr.ip().is_unspecified() => {
            let host = if addr.is_ipv6() { "[::1]" } else { "127.0.0.1" };
            Ok(format!("http://{host}:{}", addr.port()))
        }
        Ok(addr) => Err(format!(
            "bind_addr {addr} is a concrete non-loopback address; in-process \
             low-bandwidth mode (lb_federation_port set) requires a loopback or \
             unspecified bind_addr so the sidecar ingress reaches the homeserver \
             over loopback and the unauthenticated CSAPI stays off the network"
        )),
        Err(_) => Ok(format!("http://{bind_addr}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(bind: &str) -> Config {
        Config {
            bind_addr: bind.to_owned(),
            ..Default::default()
        }
    }

    const EGRESS: &str = "127.0.0.1:9999";

    fn egress() -> SocketAddr {
        EGRESS.parse().unwrap()
    }

    // The public federation ingress takes the federation port; for the embedded
    // case `bind_addr` is unspecified (`0.0.0.0`), so the ingress binds IPv6
    // unspecified (`[::]:<fed port>`) — a dual-stack listener that accepts both
    // families. The egress is supplied (allocated by `entrypoint`); the wire is CoAP.
    #[test]
    fn build_lb_config_derives_ingress_from_bind_addr_and_port() {
        let c = cfg("0.0.0.0:8008");
        let lb = build_lb_config(&c, 8448, egress(), None, LinkProfile::default())
            .expect("valid lb config");
        assert_eq!(lb.ingress_bind, "[::]:8448".parse().unwrap());
        assert_eq!(lb.egress_bind, egress());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
        // The default profile derives the SZX-max 1024 B Q-Block1 size: no
        // per-request option budget is reserved any more — the medium's
        // LinkCodec controls what rides each block, and an over-MTU datagram
        // is the link's to detect and log loudly.
        assert!(matches!(
            lb.wire,
            neutrino_lb::WireKind::CoapQBlock {
                block1_size: Some(1024),
                ..
            }
        ));
    }

    // A medium with a constrained MTU shrinks the Q-Block1 size; an MTU that
    // cannot frame a block refuses startup rather than stalling on first send.
    #[test]
    fn build_lb_config_sizes_blocks_from_link_profile() {
        let c = cfg("0.0.0.0:8008");
        let small = LinkProfile {
            max_datagram: 700,
            ..LinkProfile::default()
        };
        let lb = build_lb_config(&c, 8448, egress(), None, small).expect("valid lb config");
        assert!(matches!(
            lb.wire,
            neutrino_lb::WireKind::CoapQBlock {
                block1_size: Some(512),
                ..
            }
        ));
        let tiny = LinkProfile {
            max_datagram: 16,
            ..LinkProfile::default()
        };
        assert!(build_lb_config(&c, 8448, egress(), None, tiny).is_err());
    }

    // SecurityConfig → EventSecurity realisation: trusted network is
    // signature-free regardless of resolver availability; signed mode needs
    // the medium's resolver capability and derives the signer from the node
    // secret so its name/key match the server identity.
    #[test]
    fn event_security_composes_from_security_config() {
        let secret = [7u8; 32];
        let name = server_identity_from_secret(&secret);
        let resolver: Arc<dyn neutrino_event::KeyResolver> =
            Arc::new(neutrino_event::NodeIdKeyResolver);

        // trusted_network = true → TrustedNetwork, resolver irrelevant.
        let cfg = SecurityConfig {
            authenticate_connections: true,
            sign_messages: false,
        };
        let sec = event_security(&cfg, None, &secret, &name).expect("composes");
        assert!(matches!(sec, neutrino_event::EventSecurity::TrustedNetwork));
        assert!(sec.signer().is_none(), "trusted network must not sign");

        // trusted_network = false → Signed, with identity symmetry: the
        // signer's key IS the node identity.
        let cfg = SecurityConfig {
            // sigs=on / connection-auth=off is legal (anonymous
            // store-and-forward relays) — must compose, not refuse.
            authenticate_connections: false,
            sign_messages: true,
        };
        let sec = event_security(&cfg, Some(resolver), &secret, &name).expect("composes");
        let signer = sec.signer().expect("signed mode must sign");
        assert_eq!(signer.server_name(), name);
        assert_eq!(hex::encode(signer.public_key()), name);

        // Signed mode without any key-resolution capability is the one
        // configuration error: inbound events could never be verified.
        assert!(event_security(&cfg, None, &secret, &name).is_err());
    }

    // A concrete loopback `bind_addr` keeps its host; only the port becomes the
    // federation port.
    #[test]
    fn build_lb_config_ingress_reuses_concrete_host() {
        let c = cfg("127.0.0.1:8008");
        let lb = build_lb_config(&c, 8448, egress(), None, LinkProfile::default())
            .expect("valid lb config");
        assert_eq!(lb.ingress_bind, "127.0.0.1:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A `localhost:port` authority (the offline/dev fallback): the public ingress
    // still can't be classified, so it falls back to IPv6 unspecified (`[::]`; no
    // peer can reach an offline device regardless), but the loopback upstream is
    // pinned to IPv4 `127.0.0.1` so the ingress→homeserver hop can't land on the
    // wrong family (see `canonical_loopback`).
    #[test]
    fn build_lb_config_ingress_falls_back_to_unspecified_for_hostname() {
        let c = cfg("localhost:8008");
        let lb = build_lb_config(&c, 8448, egress(), None, LinkProfile::default())
            .expect("valid lb config");
        assert_eq!(lb.ingress_bind, "[::]:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // `localhost` is pinned to IPv4 loopback for the upstream — it otherwise
    // resolves to both `::1` and `127.0.0.1`, and dialing the family the
    // homeserver did not bind is a connect-refused 502.
    #[test]
    fn upstream_url_pins_localhost_to_ipv4_loopback() {
        assert_eq!(
            upstream_url("localhost:8008").unwrap(),
            "http://127.0.0.1:8008"
        );
        // Case-insensitive host, and the port is preserved.
        assert_eq!(
            upstream_url("LocalHost:443").unwrap(),
            "http://127.0.0.1:443"
        );
    }

    // Numeric addresses and non-`localhost` hostnames are untouched by the pin.
    #[test]
    fn canonical_loopback_only_rewrites_localhost() {
        assert_eq!(canonical_loopback("localhost:8008"), "127.0.0.1:8008");
        assert_eq!(canonical_loopback("127.0.0.1:8008"), "127.0.0.1:8008");
        assert_eq!(canonical_loopback("0.0.0.0:8008"), "0.0.0.0:8008");
        assert_eq!(canonical_loopback("[::1]:8008"), "[::1]:8008");
        assert_eq!(canonical_loopback("[::]:8008"), "[::]:8008");
        assert_eq!(canonical_loopback("example.com:8008"), "example.com:8008");
    }

    // An unspecified bind is loopback-rewritten for the upstream so the ingress
    // reaches the co-located homeserver over loopback.
    #[test]
    fn build_lb_config_upstream_loopbacks_an_unspecified_bind() {
        let c = cfg("0.0.0.0:8008");
        let lb = build_lb_config(&c, 80, egress(), None, LinkProfile::default())
            .expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A concrete non-loopback `bind_addr` is rejected: the homeserver would
    // listen only there, so the ingress→upstream loopback hop would miss it (and
    // a verbatim URL would expose the unauthenticated CSAPI on the network).
    #[test]
    fn build_lb_config_rejects_non_loopback_bind_addr() {
        let c = cfg("192.168.1.5:8008");
        assert!(build_lb_config(&c, 8448, egress(), None, LinkProfile::default()).is_err());
    }

    // The self-allocated egress is always a loopback address: it is an
    // unauthenticated open forward proxy that must stay off the network, and
    // binding loopback by construction is what removes the need for a guard.
    #[test]
    fn alloc_loopback_egress_is_loopback() {
        let addr = alloc_loopback_egress().expect("allocate egress");
        assert!(addr.ip().is_loopback(), "egress {addr} must be loopback");
    }

    #[test]
    fn server_identity_from_secret_is_deterministic_64_hex() {
        let secret = [3u8; 32];
        let a = server_identity_from_secret(&secret);
        assert_eq!(a, server_identity_from_secret(&secret), "deterministic");
        assert_eq!(a.len(), 64, "64-char hex public-key fingerprint");
        assert!(
            a.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        );
    }

    #[test]
    fn server_identity_from_distinct_secrets_differ() {
        // Guards against a degenerate impl that ignores the secret (e.g. returns
        // a constant): different secrets MUST yield different identities, else
        // every install would name itself identically.
        assert_ne!(
            server_identity_from_secret(&[3u8; 32]),
            server_identity_from_secret(&[4u8; 32]),
        );
    }

    fn cfg_in(dir: &tempfile::TempDir, server_name: &str) -> Config {
        Config {
            server_name: server_name.to_owned(),
            storage_dir: dir.path().to_path_buf(),
            ..Default::default()
        }
    }

    async fn open(dir: &tempfile::TempDir) -> SqliteStore {
        SqliteStore::open_in_dir(dir.path())
            .await
            .expect("open store")
    }

    #[tokio::test]
    async fn resolve_identity_derives_name_when_empty() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let store = open(&tmp).await;
        let mut config = cfg_in(&tmp, "");
        resolve_server_identity(&mut config, &store)
            .await
            .expect("resolve");
        assert_eq!(config.server_name.len(), 64, "derived a 64-char identity");
    }

    #[tokio::test]
    async fn resolve_identity_keeps_configured_name() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let store = open(&tmp).await;
        let mut config = cfg_in(&tmp, "hs.example");
        resolve_server_identity(&mut config, &store)
            .await
            .expect("resolve");
        assert_eq!(config.server_name, "hs.example", "configured name kept");
    }

    #[tokio::test]
    async fn resolve_identity_is_stable_across_restart() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        // Resolve, then re-open the store (a "restart") and resolve again: the
        // persisted secret yields the same derived identity — this is why the
        // secret is stored, not regenerated.
        let mut first = cfg_in(&tmp, "");
        resolve_server_identity(&mut first, &open(&tmp).await)
            .await
            .expect("first");
        let mut second = cfg_in(&tmp, "");
        resolve_server_identity(&mut second, &open(&tmp).await)
            .await
            .expect("second");
        assert_eq!(first.server_name, second.server_name);
    }

    #[tokio::test]
    async fn resolve_identity_refuses_name_change_on_existing_store() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        // First boot pins the configured name into the store.
        let mut first = cfg_in(&tmp, "hs.example");
        resolve_server_identity(&mut first, &open(&tmp).await)
            .await
            .expect("first boot pins the name");
        // A later boot under a different name must be refused, not silently
        // accepted — that would fork the server identity against existing data.
        let mut second = cfg_in(&tmp, "other.example");
        let err = resolve_server_identity(&mut second, &open(&tmp).await)
            .await
            .expect_err("name change must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("hs.example"), "error names the recorded value");
        assert!(
            msg.contains("other.example"),
            "error names the attempted value"
        );
    }

    #[tokio::test]
    async fn resolve_identity_derived_name_survives_across_restart() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        // The derived name is a pure function of the persisted secret, so the
        // guard is a no-op on the embedded (empty-name) path: a second boot
        // recomputes the identical name and passes.
        let mut first = cfg_in(&tmp, "");
        resolve_server_identity(&mut first, &open(&tmp).await)
            .await
            .expect("first");
        let mut second = cfg_in(&tmp, "");
        resolve_server_identity(&mut second, &open(&tmp).await)
            .await
            .expect("derived name re-passes the guard");
        assert_eq!(first.server_name, second.server_name);
    }
}
