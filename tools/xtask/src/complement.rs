use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use clap::Args;

use crate::sh::{self, Cmd};

const DEFAULT_REF: &str = "main";
const DEFAULT_IMAGE: &str = "neutrino:complement";

#[derive(Args)]
pub struct ComplementArgs {
    /// Run the in-repo tests (./complement/tests/...) instead of the allowlist.
    #[arg(long = "in-repo")]
    pub in_repo: bool,
    /// Extra args forwarded verbatim to `go test` (e.g. -run Foo -v).
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub extra: Vec<String>,
}

/// Allowlist entries: non-blank, non-`#` lines, trimmed.
pub fn parse_allowlist(contents: &str) -> Vec<String> {
    contents
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// True when the forwarded args contain an explicit `-run` selection, which
/// bypasses the allowlist (matches the old script's debug escape hatch).
pub fn is_adhoc_run(extra: &[String]) -> bool {
    extra.iter().any(|a| a == "-run")
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn image_exists(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build the complement image unless SKIP_IMAGE_BUILD is set and it already
/// exists (CI pre-builds it with buildx layer caching).
fn ensure_image(root: &Path, image: &str) -> Result<(), sh::Error> {
    if std::env::var_os("SKIP_IMAGE_BUILD").is_some() && image_exists(image) {
        eprintln!("Using pre-built {image} (SKIP_IMAGE_BUILD set)");
        return Ok(());
    }
    eprintln!("Building {image}...");
    Cmd::new("docker", root)
        .args(["build", "-f", "docker/complement/Dockerfile", "-t"])
        .arg(image)
        .arg(".")
        .run()
}

/// Resolve a complement checkout: COMPLEMENT_DIR if set, else fetch the
/// matrix-org/complement archive for COMPLEMENT_REF into ./complement-<ref>/.
fn ensure_complement_checkout(root: &Path) -> Result<PathBuf, sh::Error> {
    if let Some(dir) = std::env::var_os("COMPLEMENT_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let reff = env_or("COMPLEMENT_REF", DEFAULT_REF);
    let dir = root.join(format!("complement-{reff}"));
    if dir.is_dir() {
        return Ok(dir);
    }
    eprintln!(
        "Fetching matrix-org/complement@{reff} into {}...",
        dir.display()
    );
    std::fs::create_dir_all(&dir)
        .map_err(|e| sh::Error::Other(format!("creating {}: {e}", dir.display())))?;
    let url = format!("https://github.com/matrix-org/complement/archive/{reff}.tar.gz");
    let tarball = dir.join("complement.tar.gz");
    Cmd::new("wget", root)
        .args(["-q", "-O"])
        .arg(tarball.to_string_lossy().into_owned())
        .arg(url)
        .run()?;
    Cmd::new("tar", root)
        .arg("-xzf")
        .arg(tarball.to_string_lossy().into_owned())
        .arg("--strip-components=1")
        .arg("-C")
        .arg(dir.to_string_lossy().into_owned())
        .run()?;
    let _ = std::fs::remove_file(&tarball);
    Ok(dir)
}

fn go_test(dir: &Path, image: &str) -> Cmd {
    Cmd::new("go", dir)
        .env("COMPLEMENT_BASE_IMAGE", image)
        .args(["test", "-v", "-timeout", "5m"])
}

pub fn run(args: &ComplementArgs) -> Result<(), sh::Error> {
    let root = sh::workspace_root();
    let image = env_or("IMAGE_TAG", DEFAULT_IMAGE);

    ensure_image(&root, &image)?;

    // In-repo neutrino-specific tests.
    if args.in_repo {
        eprintln!("Running in-repo complement tests...");
        return go_test(&root.join("complement"), &image)
            .args(args.extra.iter().cloned())
            .arg("./tests/...")
            .run();
    }

    let complement_dir = ensure_complement_checkout(&root)?;

    // Ad-hoc -run bypasses the allowlist (debug a single test).
    if is_adhoc_run(&args.extra) {
        eprintln!("Running ad-hoc test selection (allowlist bypassed)");
        return go_test(&complement_dir, &image)
            .args(args.extra.iter().cloned())
            .arg("./tests/csapi/...")
            .run();
    }

    let allowlist_path = root.join("complement/allowlist.txt");
    let contents = std::fs::read_to_string(&allowlist_path)
        .map_err(|e| sh::Error::Other(format!("reading {}: {e}", allowlist_path.display())))?;
    let entries = parse_allowlist(&contents);
    if entries.is_empty() {
        return Err(sh::Error::Other(
            "allowlist contains no enabled tests".into(),
        ));
    }

    // Run each entry as its own `go test -run`, aggregating exit codes (Go
    // splits -run on `/`, so entries can't be batched into one regex).
    let mut failed: Vec<String> = Vec::new();
    for entry in entries {
        eprintln!("\n=== Allowlist entry: {entry}");
        let res = go_test(&complement_dir, &image)
            .args(["-run", &entry])
            .args(args.extra.iter().cloned())
            .arg("./tests/csapi/...")
            .run();
        if res.is_err() {
            failed.push(entry);
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(sh::Error::Other(format!(
            "allowlist entries failed: {}",
            failed.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_skips_blanks_and_comments() {
        let input = "# header\n\nTestFoo\n  TestBar  \n# c\nTestBaz\n";
        assert_eq!(
            parse_allowlist(input),
            vec!["TestFoo", "TestBar", "TestBaz"]
        );
    }

    #[test]
    fn parse_empty_allowlist() {
        assert!(parse_allowlist("# only comments\n\n").is_empty());
    }

    #[test]
    fn adhoc_run_detected() {
        assert!(is_adhoc_run(&["-run".into(), "TestX".into()]));
        assert!(!is_adhoc_run(&["-v".into()]));
    }
}
