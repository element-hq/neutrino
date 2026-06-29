mod platform;
mod resolver;

use std::net::SocketAddr;
use std::sync::Arc;

pub use neutrino_common::{Command, Config};
pub use neutrino_lb::DatagramLink;

use std::future::Future;
use std::pin::Pin;

use neutrino_store::IdentityStore;
use neutrino_store_sqlite::SqliteStore;
use rand::RngCore;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Result of building the federation datagram link from the node secret.
pub type DatagramLinkResult =
    Result<std::sync::Arc<dyn DatagramLink>, Box<dyn std::error::Error + Send + Sync>>;

/// Builds the federation datagram link once the node secret is resolved. ffi
/// supplies one that binds an iroh transport; the dev binary passes `None`
/// (plain UDP federation). Async because binding the transport is async.
pub type FederationLinkFactory =
    Box<dyn FnOnce([u8; 32]) -> Pin<Box<dyn Future<Output = DatagramLinkResult> + Send>> + Send>;

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
) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // Open the store once, here, and resolve the server's stable identity from
    // it before anything reads `config`. The same handle is threaded into the
    // homeserver (`serve`) so the database is opened exactly once.
    let store = Arc::new(SqliteStore::open_in_dir(&config.storage_dir).await?);
    let secret = resolve_server_identity(&mut config, &store).await?;

    // Build the federation datagram link from the resolved secret (the embedded
    // iroh build injects a factory; the dev binary passes `None` for plain UDP
    // federation). A failed transport bind must fail startup loudly — propagate
    // with `?` rather than silently falling back to UDP.
    // The factory's error is `Send + Sync` (so it can cross the ffi task
    // boundary); widen it to this function's plain `Box<dyn Error>` on the way
    // out, since the two boxed-trait-object types don't auto-convert via `?`.
    let link = match link_factory {
        Some(factory) => Some(
            factory(secret)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { e })?,
        ),
        None => None,
    };

    // When embedded, publish the resolved server name to the host over the
    // handoff (the host reads it back off the watch channel). The node secret
    // reached the host via `link_factory` above; outbound federation is addressed
    // by node id over the datagram link, so there is no route table to wire.
    if let Some(handoff) = &handoff {
        let _ = handoff.send(Some(config.server_name.clone()));
    }

    // Embedded low-bandwidth sidecar: when `lb_federation_port` is set we run a
    // `neutrino-lb` proxy in-process beside the homeserver (the embedded-on-
    // mobile target — the in-process analogue of the legacy `DendriteService`
    // owning the monolith). The ingress owns the public federation port peers
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
            Some(build_lb_config(&config, port, egress_bind, link.clone())?)
        }
        None => None,
    };

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
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
            let hs = neutrino_http::serve(listener, config, store, commands);
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
                // down (and, before this log existed, did so silently).
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
        None => neutrino_http::serve(listener, config, store, commands).await?,
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

/// Derive the in-process sidecar's [`neutrino_lb::LbConfig`] from the homeserver
/// `Config`, the chosen public federation port, and the already-allocated
/// loopback `egress_bind`. The ingress reuses `bind_addr`'s host with the port
/// replaced by `fed_port` (only the port differs — see [`ingress_bind_for`]);
/// the upstream is the homeserver's own `bind_addr` (which must be
/// loopback-reachable). Errors if `bind_addr` is a concrete non-loopback
/// address (see [`upstream_url`]).
fn build_lb_config(
    config: &Config,
    fed_port: u16,
    egress_bind: SocketAddr,
    link: Option<Arc<dyn DatagramLink>>,
) -> Result<neutrino_lb::LbConfig, Box<dyn std::error::Error>> {
    Ok(neutrino_lb::LbConfig {
        ingress_bind: ingress_bind_for(&config.bind_addr, fed_port),
        egress_bind,
        upstream: upstream_url(&config.bind_addr)?,
        wire: neutrino_lb::WireKind::CoapQBlock {
            // 512 B per Q-Block1 chunk, NOT coap-rs's 1024 default. Each block's
            // serialized PDU also carries the request's options — the federation
            // path (`/_matrix/federation/v2/invite/!room.../$event...`, long with
            // room+event ids) plus forwarded headers and the Q-Block/Size/
            // Request-Tag options — and coap-lite caps a serialized message at
            // `Packet::MAX_SIZE` (1280 B). A 1024 B block + those options exceeds
            // 1280, so `build_block`'s `to_bytes()` fails on the very first block
            // and the send silently stalls (coap-rs drops the error). 512 leaves
            // ample room for options under 1280 and under the iroh datagram MTU.
            block1_size: Some(512),
            qblock: neutrino_lb::QBlockTuning::default(),
        },
        // The in-process sidecar is the embedded/datagram-link target: map a
        // peer's node-id `server_name` to its bare 64-char hex node id so the
        // datagram egress dials the peer's iroh endpoint directly. Dormant for a
        // non-node `server_name` (dev/named servers pass through to direct dial).
        resolver: Some(Arc::new(resolver::NodeIdResolver::new())),
        // The injected federation transport (iroh, embedded build) — when `Some`,
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
/// - a non-IP authority (`hostname:port`) can't be classified without
///   resolution, so it is trusted verbatim.
fn upstream_url(bind_addr: &str) -> Result<String, String> {
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
        let lb = build_lb_config(&c, 8448, egress(), None).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "[::]:8448".parse().unwrap());
        assert_eq!(lb.egress_bind, egress());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
        assert!(matches!(lb.wire, neutrino_lb::WireKind::CoapQBlock { .. }));
    }

    // A concrete loopback `bind_addr` keeps its host; only the port becomes the
    // federation port.
    #[test]
    fn build_lb_config_ingress_reuses_concrete_host() {
        let c = cfg("127.0.0.1:8008");
        let lb = build_lb_config(&c, 8448, egress(), None).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "127.0.0.1:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A non-IP authority (`localhost:port`, the offline/dev fallback) can't be
    // classified, so the ingress falls back to IPv6 unspecified (`[::]`; no peer
    // can reach an offline device regardless) and the upstream is trusted verbatim.
    #[test]
    fn build_lb_config_ingress_falls_back_to_unspecified_for_hostname() {
        let c = cfg("localhost:8008");
        let lb = build_lb_config(&c, 8448, egress(), None).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "[::]:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://localhost:8008");
    }

    // An unspecified bind is loopback-rewritten for the upstream so the ingress
    // reaches the co-located homeserver over loopback.
    #[test]
    fn build_lb_config_upstream_loopbacks_an_unspecified_bind() {
        let c = cfg("0.0.0.0:8008");
        let lb = build_lb_config(&c, 80, egress(), None).expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A concrete non-loopback `bind_addr` is rejected: the homeserver would
    // listen only there, so the ingress→upstream loopback hop would miss it (and
    // a verbatim URL would expose the unauthenticated CSAPI on the network).
    #[test]
    fn build_lb_config_rejects_non_loopback_bind_addr() {
        let c = cfg("192.168.1.5:8008");
        assert!(build_lb_config(&c, 8448, egress(), None).is_err());
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
}
