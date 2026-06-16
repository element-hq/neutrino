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
    let egress = proxy.strip_prefix("http://").unwrap_or(proxy);
    let ingress_bind: SocketAddr = ingress
        .parse()
        .map_err(|e| format!("lb_ingress_bind {ingress:?}: {e}"))?;
    let egress_bind: SocketAddr = egress
        .parse()
        .map_err(|e| format!("federation_proxy egress {egress:?}: {e}"))?;
    Ok(neutrino_lb::LbConfig {
        ingress_bind,
        egress_bind,
        upstream: format!("http://{}", config.bind_addr),
    })
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
}
