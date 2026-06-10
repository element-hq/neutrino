//! Local entrypoint for running the server directly on the host during development
//! (as opposed to building the embedded/Android target).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The local dev binary has no out-of-band command source, so it holds the
    // sender open and runs until the process is signalled; `entrypoint` needs a
    // receiver to thread into the server's command dispatch. (A Ctrl-C handler
    // sending `Command::Shutdown` here would give graceful local shutdown.)
    let (_commands_tx, commands_rx) = tokio::sync::mpsc::unbounded_channel();
    neutrino_main::entrypoint(neutrino_main::Config::from_env(), commands_rx).await
}
