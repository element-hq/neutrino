mod platform;

use neutrino_common::Config;

pub async fn entrypoint() {
    platform::init_tracing();

    let config = Config::from_env();
    let bind_addr = config.bind_addr.clone();

    let app = neutrino_http::router(config);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
