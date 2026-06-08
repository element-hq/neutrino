//! Local entrypoint for running the server directly on the host during development
//! (as opposed to building the embedded/Android target).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    neutrino_main::entrypoint(neutrino_common::Config::from_env()).await
}
