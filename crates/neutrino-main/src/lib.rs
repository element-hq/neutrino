mod platform;

use neutrino_common::Config;

/// Run the server with the supplied [`Config`]. Initialises tracing, binds the
/// listener, and serves until shutdown. The caller owns config construction:
/// the host binary (`crates/neutrino`) builds it from the environment, the FFI
/// layer (`neutrino-ffi`) from values the embedding app passes in.
pub async fn entrypoint(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

    let bind_addr = config.bind_addr.clone();
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    tracing::info!("listening on {}", listener.local_addr()?);
    neutrino_http::serve(listener, config).await?;
    Ok(())
}
