mod platform;

use std::net::SocketAddr;

pub use neutrino_common::{Command, Config};

use tokio_util::sync::CancellationToken;

pub async fn entrypoint(
    config: Config,
    commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
) -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

    // Embedded low-bandwidth sidecar: when `lb_ingress_bind` is set we run a
    // `neutrino-lb` proxy in-process beside the homeserver (the embedded-on-
    // mobile target — the in-process analogue of the legacy `DendriteService`
    // owning the monolith). The homeserver routes its outbound federation
    // through the egress (`federation_proxy`); the ingress owns the public port
    // peers reach and forwards inbound federation to the homeserver's loopback.
    //
    // Derive (and so validate) the sidecar config *before* binding the listener:
    // an illegal in-process combo (no egress, or a non-loopback `bind_addr`)
    // then fails fast without first claiming the public port.
    let lb_config = config
        .lb_ingress_bind
        .as_deref()
        .map(|ingress| build_lb_config(&config, ingress))
        .transpose()?;

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
/// `Config`. The egress address is taken from `federation_proxy` (the homeserver
/// routes outbound there, and the sidecar binds it); the upstream is the
/// homeserver's own `bind_addr` (which must be loopback so the ingress reaches
/// it). Errors if `lb_ingress_bind` is set without a `federation_proxy` egress,
/// if either address fails to parse, or if `bind_addr` is a concrete
/// non-loopback address (see [`upstream_url`]).
fn build_lb_config(
    config: &Config,
    ingress: &str,
) -> Result<neutrino_lb::LbConfig, Box<dyn std::error::Error>> {
    let proxy = config
        .federation_proxy
        .as_deref()
        .ok_or("lb_ingress_bind is set but federation_proxy (the egress URL) is not")?;
    let ingress_bind: SocketAddr = ingress
        .parse()
        .map_err(|e| format!("lb_ingress_bind {ingress:?}: {e}"))?;
    let egress_bind = egress_bind_from_proxy(proxy)?;
    Ok(neutrino_lb::LbConfig {
        ingress_bind,
        egress_bind,
        upstream: upstream_url(&config.bind_addr)?,
    })
}

/// The `host:port` the egress must bind, extracted from a `federation_proxy`
/// URL. That value is validated at startup as a `reqwest::Proxy::all` URL
/// (`AppState::new`), which accepts an optional `scheme://` and a trailing
/// path/slash/query — so mirror that here (strip the scheme and anything from
/// the first path/query/fragment delimiter) rather than assuming a bare
/// `http://host:port`. This keeps the two validators in agreement: a proxy URL
/// that passed startup won't then fail to start the in-process sidecar. Still
/// aborts (returns `Err`) if what remains isn't a bindable `SocketAddr` (e.g. a
/// hostname needing resolution, which the egress listener can't bind directly).
fn egress_bind_from_proxy(proxy: &str) -> Result<SocketAddr, String> {
    let after_scheme = proxy.split_once("://").map_or(proxy, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    let addr: SocketAddr = authority
        .parse()
        .map_err(|e| format!("federation_proxy egress {authority:?}: {e}"))?;
    // The egress is an unauthenticated open forward proxy (no dest allowlist, no
    // request-body cap) that only ever serves the co-located homeserver over
    // loopback. A non-loopback bind (a routable interface, or an unspecified
    // `0.0.0.0`/`[::]`) would expose that relay on the network, so reject it —
    // symmetric with the `bind_addr`/upstream loopback guard in `upstream_url`.
    if !addr.ip().is_loopback() {
        return Err(format!(
            "federation_proxy egress {addr} is not a loopback address; the \
             in-process sidecar egress must bind loopback so its unauthenticated \
             forward proxy stays off the network"
        ));
    }
    Ok(addr)
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
             low-bandwidth mode (lb_ingress_bind set) requires a loopback or \
             unspecified bind_addr so the sidecar ingress reaches the homeserver \
             over loopback and the unauthenticated CSAPI stays off the network"
        )),
        Err(_) => Ok(format!("http://{bind_addr}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(bind: &str, proxy: Option<&str>) -> Config {
        Config {
            bind_addr: bind.to_owned(),
            federation_proxy: proxy.map(str::to_owned),
            ..Default::default()
        }
    }

    #[test]
    fn build_lb_config_derives_binds_and_upstream() {
        let c = cfg("127.0.0.1:8008", Some("http://127.0.0.1:8009"));
        let lb = build_lb_config(&c, "0.0.0.0:8448").expect("valid lb config");
        assert_eq!(lb.ingress_bind, "0.0.0.0:8448".parse().unwrap());
        assert_eq!(lb.egress_bind, "127.0.0.1:8009".parse().unwrap());
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    #[test]
    fn build_lb_config_requires_federation_proxy() {
        let c = cfg("127.0.0.1:8008", None);
        assert!(build_lb_config(&c, "0.0.0.0:8448").is_err());
    }

    #[test]
    fn build_lb_config_rejects_unparseable_ingress() {
        let c = cfg("127.0.0.1:8008", Some("http://127.0.0.1:8009"));
        assert!(build_lb_config(&c, "not-an-addr").is_err());
    }

    // The egress derivation must accept every `federation_proxy` form that
    // `reqwest::Proxy::all` accepts at startup, so a value that passed startup
    // validation can't then abort the in-process sidecar.
    #[test]
    fn egress_bind_reflects_proxy_all_accepted_forms() {
        let want: SocketAddr = "127.0.0.1:8009".parse().unwrap();
        // Trailing slash, trailing path, bare authority (no scheme), and a
        // non-http scheme are all valid `Proxy::all` inputs.
        for proxy in [
            "http://127.0.0.1:8009",
            "http://127.0.0.1:8009/",
            "http://127.0.0.1:8009/path",
            "127.0.0.1:8009",
            "https://127.0.0.1:8009",
        ] {
            assert_eq!(
                egress_bind_from_proxy(proxy).unwrap(),
                want,
                "egress derivation failed for {proxy:?}"
            );
        }
        // IPv6 authority round-trips too.
        assert_eq!(
            egress_bind_from_proxy("http://[::1]:8009/").unwrap(),
            "[::1]:8009".parse().unwrap()
        );
    }

    // The abort is kept: a proxy URL whose authority isn't a bindable
    // SocketAddr (e.g. a hostname) still fails loudly rather than going direct.
    #[test]
    fn build_lb_config_rejects_unbindable_egress_authority() {
        let c = cfg("127.0.0.1:8008", Some("http://localhost:8009"));
        assert!(build_lb_config(&c, "0.0.0.0:8448").is_err());
        assert!(egress_bind_from_proxy("http://nope").is_err());
    }

    // The egress local-in port only ever receives the co-located homeserver's
    // own outbound requests over loopback, and it is an unauthenticated open
    // forward proxy (no dest allowlist, no request-body cap). Binding it to a
    // non-loopback address (a routable interface, or `0.0.0.0`) would expose
    // that relay on the network, so a non-loopback `federation_proxy` egress is
    // rejected — symmetric with the `bind_addr`/upstream loopback guard.
    #[test]
    fn build_lb_config_rejects_non_loopback_egress() {
        // Unspecified (`0.0.0.0` / `[::]`) — would bind all interfaces.
        assert!(egress_bind_from_proxy("http://0.0.0.0:8009").is_err());
        assert!(egress_bind_from_proxy("http://[::]:8009").is_err());
        // A concrete routable address.
        assert!(egress_bind_from_proxy("http://192.168.1.5:8009").is_err());
        let c = cfg("127.0.0.1:8008", Some("http://0.0.0.0:8009"));
        assert!(build_lb_config(&c, "0.0.0.0:8448").is_err());
    }

    #[test]
    fn build_lb_config_upstream_loopbacks_an_unspecified_bind() {
        // A container binds 0.0.0.0 (so CSAPI can be published) but the
        // co-located ingress must still reach the homeserver over loopback.
        let c = cfg("0.0.0.0:8008", Some("http://127.0.0.1:8009"));
        let lb = build_lb_config(&c, "0.0.0.0:80").expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }

    // A concrete non-loopback `bind_addr` in in-process mode is rejected: the
    // homeserver would listen only on that interface, so rewriting the upstream
    // to loopback would miss it, and using it verbatim sends the ingress→upstream
    // hop off the loopback path (exposing CSAPI on the network). Fail loudly
    // rather than silently going off-box.
    #[test]
    fn build_lb_config_rejects_non_loopback_bind_addr() {
        let c = cfg("192.168.1.5:8008", Some("http://127.0.0.1:8009"));
        assert!(build_lb_config(&c, "0.0.0.0:8448").is_err());
    }

    // A loopback `bind_addr` is used verbatim (it's already loopback-reachable).
    #[test]
    fn build_lb_config_accepts_loopback_bind_addr() {
        let c = cfg("127.0.0.1:8008", Some("http://127.0.0.1:8009"));
        let lb = build_lb_config(&c, "0.0.0.0:8448").expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }
}
