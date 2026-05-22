//! Local entrypoint for running the server directly on the host during development
//! (as opposed to building the embedded/Android target).

#[tokio::main]
async fn main() {
    neutrino_main::entrypoint().await;
}
