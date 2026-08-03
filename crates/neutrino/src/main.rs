//! Local entrypoint for running the server directly on the host during development
//! (as opposed to building the embedded/Android target).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("neutrino: startup");
    // The local dev binary has no out-of-band command source over FFI, so it
    // holds the sender open and runs until the process is signalled; `entrypoint`
    // needs a receiver to thread into the server's command dispatch.
    let (commands_tx, commands_rx) = tokio::sync::mpsc::unbounded_channel();
    // On Unix, SIGUSR2 maps to `Command::KickBackoff`: the federation test rig
    // sends it after healing a partition so the outbound sender retries
    // immediately instead of waiting out its backoff (capped at 15 min). The
    // embedded/Android host drives commands over FFI and never uses this path.
    #[cfg(unix)]
    spawn_kick_on_sigusr2(commands_tx.clone());
    // Hold the sender open for the process lifetime. An empty but never-closed
    // channel keeps the dispatch loop alive; a *closed* channel is read as a
    // shutdown request. (A Ctrl-C handler sending `Command::Shutdown` here would
    // give graceful local shutdown.)
    let _commands_tx = commands_tx;
    // The dev binary runs the homeserver directly with no embedded relay/TUN, so
    // no tunnel handoff and no injected federation link (plain UDP federation).
    neutrino_main::entrypoint(
        neutrino_main::Config::from_env(),
        commands_rx,
        None,
        None,
        None,
        None,
    )
    .await
}

/// Translate SIGUSR2 into [`neutrino_main::Command::KickBackoff`] for the local
/// dev binary. The federation test rig sends SIGUSR2 to a container after
/// healing a network partition so that destination's outbound sender resets its
/// backoff and retries immediately, rather than waiting out the (up to 15 min)
/// backoff cap.
#[cfg(unix)]
fn spawn_kick_on_sigusr2(commands_tx: tokio::sync::mpsc::UnboundedSender<neutrino_main::Command>) {
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut sigusr2 = match signal(SignalKind::user_defined2()) {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!(
                    "neutrino: failed to install SIGUSR2 handler ({err}); KickBackoff-via-signal disabled"
                );
                return;
            }
        };
        // `recv()` yields once per delivered signal, so loop: each heal kicks
        // again. `None` means the runtime is shutting down — stop listening.
        while sigusr2.recv().await.is_some() {
            eprintln!("neutrino: SIGUSR2 received, sending Command::KickBackoff");
            // A send error means the dispatch loop is already gone (server
            // stopping); nothing left to kick, so end the task.
            if commands_tx
                .send(neutrino_main::Command::KickBackoff)
                .is_err()
            {
                break;
            }
        }
    });
}
