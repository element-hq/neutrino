//! Dev binary: run the sidecar standalone, configured from the environment.

use neutrino_lb::{LbConfig, serve};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), neutrino_lb::LbError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = LbConfig::from_env()?;
    tracing::info!(
        ingress = %config.ingress_bind,
        egress = %config.egress_bind,
        upstream = %config.upstream,
        "neutrino-lb starting"
    );
    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        sig.cancel();
    });
    serve(config, shutdown).await
}
