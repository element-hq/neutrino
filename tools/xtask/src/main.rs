//! Developer tasks that don't fit a plain `cargo` invocation
//! ([xtask pattern](https://github.com/matklad/cargo-xtask)). Run
//! `cargo xtask <command>` from the workspace root. Android artifact packaging
//! lives in the out-of-tree medium repositories that compose this workspace.

mod complement;
#[cfg(test)]
mod layers;
mod sh;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// Developer tasks for the Neutrino workspace.
#[derive(Parser)]
#[command(name = "xtask", about = "Neutrino dev tasks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the Complement suite against the neutrino image.
    Complement(complement::ComplementArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Complement(args) => complement::run(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask: {err}");
            ExitCode::FAILURE
        }
    }
}
