use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to spawn `{program}`: {source}")]
    Spawn {
        program: String,
        source: std::io::Error,
    },
    #[error("`{program}` exited with {code}")]
    Status { program: String, code: String },
    #[error("{0}")]
    Other(String),
}

/// Absolute path to the workspace root (the directory holding the root
/// `Cargo.toml`). Derived from this crate's manifest dir (`tools/xtask`),
/// so commands are CWD-independent.
pub fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .nth(2)
        .unwrap_or(manifest)
        .to_path_buf()
}

/// A subprocess invocation: program + args + cwd + extra env. `run` streams
/// the child's stdio and fails on a non-zero exit (mirrors `set -e`).
pub struct Cmd {
    program: String,
    args: Vec<String>,
    cwd: PathBuf,
    envs: Vec<(String, String)>,
}

impl Cmd {
    pub fn new(program: impl Into<String>, cwd: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: cwd.into(),
            envs: Vec::new(),
        }
    }

    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }

    pub fn args<I, S>(mut self, it: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(it.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.envs.push((k.into(), v.into()));
        self
    }

    pub fn run(self) -> Result<(), Error> {
        eprintln!("$ {} {}", self.program, self.args.join(" "));
        let status = Command::new(&self.program)
            .args(&self.args)
            .current_dir(&self.cwd)
            .envs(self.envs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .status()
            .map_err(|source| Error::Spawn {
                program: self.program.clone(),
                source,
            })?;
        if !status.success() {
            return Err(Error::Status {
                program: self.program,
                code: status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".into()),
            });
        }
        Ok(())
    }
}
