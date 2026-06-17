mod platform;

use std::net::SocketAddr;

pub use neutrino_common::{Command, Config};

use tokio_util::sync::CancellationToken;

pub async fn entrypoint(
    config: Config,
    commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
) -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("listening on {}", listener.local_addr()?);

    // Embedded low-bandwidth sidecar: when `lb_ingress_bind` is set we run a
    // `neutrino-lb` proxy in-process beside the homeserver (the embedded-on-
    // mobile target — the in-process analogue of the legacy `DendriteService`
    // owning the monolith). The homeserver routes its outbound federation
    // through the egress (`federation_proxy`); the ingress owns the public port
    // peers reach and forwards inbound federation to the homeserver's loopback.
    match config.lb_ingress_bind.as_deref() {
        Some(ingress) => {
            let lb_config = build_lb_config(&config, ingress)?;
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
/// or if either address fails to parse.
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
        upstream: upstream_url(&config.bind_addr),
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
    authority
        .parse::<SocketAddr>()
        .map_err(|e| format!("federation_proxy egress {authority:?}: {e}"))
}

/// The loopback URL the ingress uses to reach the co-located homeserver. When
/// `bind_addr` binds all interfaces (e.g. `0.0.0.0:8008` in a container, needed
/// so CSAPI can be port-published), the ingress still reaches it over loopback;
/// any concrete host is used verbatim.
fn upstream_url(bind_addr: &str) -> String {
    match bind_addr.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_unspecified() => {
            let host = if addr.is_ipv6() { "[::1]" } else { "127.0.0.1" };
            format!("http://{host}:{}", addr.port())
        }
        _ => format!("http://{bind_addr}"),
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

    #[test]
    fn build_lb_config_upstream_loopbacks_an_unspecified_bind() {
        // A container binds 0.0.0.0 (so CSAPI can be published) but the
        // co-located ingress must still reach the homeserver over loopback.
        let c = cfg("0.0.0.0:8008", Some("http://127.0.0.1:8009"));
        let lb = build_lb_config(&c, "0.0.0.0:80").expect("valid lb config");
        assert_eq!(lb.upstream, "http://127.0.0.1:8008");
    }
}
