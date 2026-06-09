mod platform;

pub use neutrino_common::Config;

pub async fn entrypoint(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    platform::init_tracing();

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;

    tracing::info!("listening on {}", listener.local_addr()?);
    neutrino_http::serve(listener, config).await?;
    Ok(())
}
