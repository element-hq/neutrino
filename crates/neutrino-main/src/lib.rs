mod platform;

use neutrino_common::Config;

pub async fn entrypoint() {
    platform::init_tracing();

    let config = Config::from_env();
    let bind_addr = config.bind_addr.clone();

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();

    tracing::info!("listening on {}", listener.local_addr().unwrap());
    neutrino_http::serve(listener, config).await.unwrap();
}
