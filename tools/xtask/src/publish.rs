use std::path::Path;
use std::process::Command;

use clap::Args;

use crate::compile::{self, CompileArgs};
use crate::sh::{self, Cmd};

const FALLBACK_VERSION: &str = "0.1.0-SNAPSHOT";

#[derive(Args)]
pub struct PublishArgs {
    /// Publish to the local Maven repository instead of GitHub Packages.
    #[arg(long)]
    pub local: bool,
    /// Explicit version. Defaults to the current git tag, else a snapshot.
    #[arg(long)]
    pub version: Option<String>,
}

/// Strip a leading `v` from a tag name (`v0.1.0` -> `0.1.0`).
pub fn normalize_tag(tag: &str) -> String {
    tag.strip_prefix('v').unwrap_or(tag).to_string()
}

/// Resolve the version to publish: explicit flag wins, else the git tag,
/// else the snapshot fallback.
pub fn resolve_version(explicit: Option<&str>, git_tag: Option<&str>) -> String {
    if let Some(v) = explicit {
        return v.to_string();
    }
    match git_tag {
        Some(tag) if !tag.is_empty() => normalize_tag(tag),
        _ => FALLBACK_VERSION.to_string(),
    }
}

/// `git describe --tags --exact-match`, or None when HEAD is not a tag.
fn current_git_tag(root: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .current_dir(root)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let tag = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if tag.is_empty() { None } else { Some(tag) }
}

pub fn run(args: &PublishArgs) -> Result<(), sh::Error> {
    let root = sh::workspace_root();

    // Always rebuild the artifacts before publishing (all default ABIs).
    compile::run(&CompileArgs::default())?;

    let tag = current_git_tag(&root);
    let version = resolve_version(args.version.as_deref(), tag.as_deref());

    let task = if args.local {
        ":bindings:publishToMavenLocal"
    } else {
        ":bindings:publish"
    };

    Cmd::new("./gradlew", &root)
        .arg(task)
        .arg(format!("-PneutrinoVersion={version}"))
        .run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_version_wins() {
        assert_eq!(resolve_version(Some("9.9.9"), Some("v1.0.0")), "9.9.9");
    }

    #[test]
    fn tag_leading_v_is_stripped() {
        assert_eq!(resolve_version(None, Some("v0.1.0")), "0.1.0");
        assert_eq!(resolve_version(None, Some("0.2.0")), "0.2.0");
    }

    #[test]
    fn no_tag_falls_back_to_snapshot() {
        assert_eq!(resolve_version(None, None), FALLBACK_VERSION);
        assert_eq!(resolve_version(None, Some("")), FALLBACK_VERSION);
    }
}
