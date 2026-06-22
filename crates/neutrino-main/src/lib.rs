mod platform;

use std::net::SocketAddr;

pub use neutrino_common::{Command, Config};

use tokio_util::sync::CancellationToken;

pub async fn entrypoint(
    mut config: Config,
    commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
) -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

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
            Some(build_lb_config(&config, port, egress_bind)?)
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
            let hs = neutrino_http::serve(listener, config, commands);
            tokio::pin!(lb, hs);
            tokio::select! {
                // The homeserver owns the command channel, so it drives the
                // lifecycle: when it winds down, stop the sidecar and join it.
                r = &mut hs => {
                    shutdown.cancel();
                    let _ = (&mut lb).await;
                    r?;
                }
                // The sidecar runs until `shutdown`, so returning here means it
                // failed. Surface the error; dropping `hs` stops the homeserver.
                r = &mut lb => { r?; }
            }
        }
        None => neutrino_http::serve(listener, config, commands).await?,
    }
    Ok(())
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
) -> Result<neutrino_lb::LbConfig, Box<dyn std::error::Error>> {
    Ok(neutrino_lb::LbConfig {
        ingress_bind: ingress_bind_for(&config.bind_addr, fed_port),
        egress_bind,
        upstream: upstream_url(&config.bind_addr)?,
        wire: neutrino_lb::WireKind::Coap {
            block1_size: None,
            max_message_size: None,
        },
    })
}

/// The public federation ingress bind: `bind_addr`'s host with the port set to
/// `fed_port` (peers reach `host(bind_addr):fed_port`). A non-IP authority
/// (e.g. `localhost:8008`, the offline/dev fallback) can't be classified, so
/// fall back to the unspecified IPv4 address — an offline device has no peers,
/// and the listener must still come up.
fn ingress_bind_for(bind_addr: &str, fed_port: u16) -> SocketAddr {
    match bind_addr.parse::<SocketAddr>() {
        Ok(addr) => SocketAddr::new(addr.ip(), fed_port),
        Err(_) => SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), fed_port),
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

    // The public federation ingress derives its host from `bind_addr` (only the
    // port changes to the federation port); the egress is supplied (allocated by
    // `entrypoint`); the wire is CoAP. The Android LAN case binds `0.0.0.0`, so
    // the ingress is `0.0.0.0:<fed port>`.
    #[test]
    fn build_lb_config_derives_ingress_from_bind_addr_and_port() {
        let c = cfg("0.0.0.0:8008");
        let lb = build_lb_config(&c, 8448, egress()).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "0.0.0.0:8448".parse().unwrap());
        assert_eq!(lb.egress_bind, egress());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
        assert!(matches!(lb.wire, neutrino_lb::WireKind::Coap { .. }));
    }

    // A concrete loopback `bind_addr` keeps its host; only the port becomes the
    // federation port.
    #[test]
    fn build_lb_config_ingress_reuses_concrete_host() {
        let c = cfg("127.0.0.1:8008");
        let lb = build_lb_config(&c, 8448, egress()).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "127.0.0.1:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A non-IP authority (`localhost:port`, the offline/dev fallback) can't be
    // classified, so the ingress falls back to the unspecified IPv4 address (no
    // peer can reach an offline device regardless) and the upstream is trusted
    // verbatim.
    #[test]
    fn build_lb_config_ingress_falls_back_to_unspecified_for_hostname() {
        let c = cfg("localhost:8008");
        let lb = build_lb_config(&c, 8448, egress()).expect("valid lb config");
        assert_eq!(lb.ingress_bind, "0.0.0.0:8448".parse().unwrap());
        assert_eq!(lb.upstream, "http://localhost:8008");
    }

    // An unspecified bind is loopback-rewritten for the upstream so the ingress
    // reaches the co-located homeserver over loopback.
    #[test]
    fn build_lb_config_upstream_loopbacks_an_unspecified_bind() {
        let c = cfg("0.0.0.0:8008");
        let lb = build_lb_config(&c, 80, egress()).expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A concrete non-loopback `bind_addr` is rejected: the homeserver would
    // listen only there, so the ingress→upstream loopback hop would miss it (and
    // a verbatim URL would expose the unauthenticated CSAPI on the network).
    #[test]
    fn build_lb_config_rejects_non_loopback_bind_addr() {
        let c = cfg("192.168.1.5:8008");
        assert!(build_lb_config(&c, 8448, egress()).is_err());
    }

    // The self-allocated egress is always a loopback address: it is an
    // unauthenticated open forward proxy that must stay off the network, and
    // binding loopback by construction is what removes the need for a guard.
    #[test]
    fn alloc_loopback_egress_is_loopback() {
        let addr = alloc_loopback_egress().expect("allocate egress");
        assert!(addr.ip().is_loopback(), "egress {addr} must be loopback");
    }
}
