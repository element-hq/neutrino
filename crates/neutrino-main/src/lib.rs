mod platform;

use neutrino_common::Config;

pub async fn entrypoint() -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

    let config = Config::from_env();
    let bind_addr = config.bind_addr.clone();

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    tracing::info!("listening on {}", listener.local_addr()?);
    neutrino_http::serve(listener, config).await?;
    Ok(())
}
