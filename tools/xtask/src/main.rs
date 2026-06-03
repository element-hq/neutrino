mod compile;
mod complement;
mod publish;
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
    /// Build the Android shared libraries and generate the Kotlin bindings.
    Compile(compile::CompileArgs),
    /// Build the bindings and publish the AAR (local Maven or GitHub Packages).
    Publish(publish::PublishArgs),
    /// Run the Complement suite against the neutrino image.
    Complement(complement::ComplementArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Compile(args) => compile::run(&args),
        Command::Publish(args) => publish::run(&args),
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
