# xtask tooling + GitHub Packages Maven publish — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `scripts/compile.sh` and `scripts/complement.sh` with a Rust `cargo xtask` CLI, and publish the UniFFI Kotlin bindings to GitHub Packages (Maven) on `v*` tag pushes.

**Architecture:** A new zero-product-impact `tools/xtask` crate (clap CLI) orchestrates existing tools (`cargo`, `cargo-ndk`, `gradlew`, `docker`, `go`, `wget`, `tar`, `git`) via `std::process::Command`. CI workflows become thin wrappers that call `cargo xtask <cmd>`. Gradle gains a GitHub Packages repo and a property-driven version.

**Tech Stack:** Rust 2024, clap 4 (derive), thiserror (both workspace deps), Gradle `maven-publish`, GitHub Actions.

> ⚠️ **Environment note:** in the current sandbox `/workspace/.git` is mounted **read-only**, so the `git commit` / `git add` steps below cannot be executed here — the human operator commits. Working-tree edits and `cargo` commands work normally. Run the commit steps wherever `.git` is writable.

> **Dependency note:** `xtask` uses `clap` (already added to `[workspace.dependencies]`) and `thiserror` (already a workspace dep, used per the project's "errors use thiserror, no anyhow" rule). No new external crates enter the lockfile.

---

## File Structure

**Create:**
- `tools/xtask/Cargo.toml` — crate manifest.
- `tools/xtask/src/main.rs` — clap `Cli`/`Command` + dispatch.
- `tools/xtask/src/sh.rs` — `Cmd` process-runner builder, `Error`, `workspace_root()`.
- `tools/xtask/src/compile.rs` — `compile` subcommand + `lib_filename()`.
- `tools/xtask/src/publish.rs` — `publish` subcommand + `resolve_version()`/`normalize_tag()`.
- `tools/xtask/src/complement.rs` — `complement` subcommand + `parse_allowlist()`/`is_adhoc_run()`.
- `.cargo/config.toml` — `xtask` cargo alias.
- `.github/workflows/release.yml` — tag-triggered publish.

**Modify:**
- `Cargo.toml` (root) — add `./tools/xtask` to `members`.
- `bindings/build.gradle.kts` — GitHub Packages repo + property-driven version.
- `.github/workflows/ci.yml` — `compile`/`complement` jobs call `cargo xtask`.
- `README.md` — Embedding section command.
- `complement/README.md` — invocation examples.

**Delete:**
- `scripts/compile.sh`, `scripts/complement.sh` (and `scripts/` if it ends up empty).

---

## Task 1: Scaffold the xtask crate and cargo alias

**Files:**
- Create: `tools/xtask/Cargo.toml`
- Create: `tools/xtask/src/main.rs`
- Create: `tools/xtask/src/sh.rs`
- Create: `.cargo/config.toml`
- Modify: `Cargo.toml` (root, `members` list)

- [ ] **Step 1: Add the crate to the workspace**

Edit root `Cargo.toml` `members`:

```toml
members = [
    "./crates/*",
    "./tools/uniffi-bindgen",
    "./tools/xtask"
]
```

- [ ] **Step 2: Write `tools/xtask/Cargo.toml`**

```toml
[package]
name = "xtask"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
clap = { workspace = true }
thiserror = { workspace = true }
```

- [ ] **Step 3: Write `tools/xtask/src/sh.rs`** (process runner + workspace root)

```rust
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
    manifest.ancestors().nth(2).unwrap_or(manifest).to_path_buf()
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
```

- [ ] **Step 4: Write `tools/xtask/src/main.rs`** (skeleton — subcommand modules added in later tasks)

```rust
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
```

- [ ] **Step 5: Write `.cargo/config.toml`** (alias)

```toml
[alias]
xtask = "run --quiet --package xtask --"
```

- [ ] **Step 6: Verify the workspace still resolves**

(`compile.rs` / `publish.rs` / `complement.rs` don't exist yet, so the crate won't build until Task 2–4. Just confirm cargo *sees* the new member.)

Run: `cargo metadata --no-deps --format-version 1 | grep -o '"name":"xtask"'`
Expected: `"name":"xtask"`

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml .cargo/config.toml tools/xtask/Cargo.toml tools/xtask/src/main.rs tools/xtask/src/sh.rs
git commit -m "build(xtask): scaffold xtask crate, alias and process runner"
```

---

## Task 2: `compile` subcommand

**Files:**
- Create: `tools/xtask/src/compile.rs`
- Test: inline `#[cfg(test)]` in `tools/xtask/src/compile.rs`

- [ ] **Step 1: Write the failing test + module**

Create `tools/xtask/src/compile.rs`:

```rust
use clap::Args;

use crate::sh::{self, Cmd};

/// Android ABIs cargo-ndk builds by default (matches the old compile.sh).
const DEFAULT_ABIS: &[&str] = &["armeabi-v7a", "arm64-v8a", "x86", "x86_64"];

#[derive(Args, Default)]
pub struct CompileArgs {
    /// Android ABI to build (repeatable). Defaults to all four.
    #[arg(short = 't', long = "target")]
    pub targets: Vec<String>,
}

/// Shared-library filename uniffi-bindgen loads, per host OS.
pub fn lib_filename() -> &'static str {
    if cfg!(target_os = "macos") {
        "libneutrino.dylib"
    } else {
        "libneutrino.so"
    }
}

pub fn run(args: &CompileArgs) -> Result<(), sh::Error> {
    let root = sh::workspace_root();
    let abis: Vec<&str> = if args.targets.is_empty() {
        DEFAULT_ABIS.to_vec()
    } else {
        args.targets.iter().map(String::as_str).collect()
    };

    // 1. Host build so uniffi-bindgen can load the cdylib.
    Cmd::new("cargo", &root).args(["build", "--release"]).run()?;

    // 2. Android targets via cargo-ndk → jniLibs.
    let mut ndk = Cmd::new("cargo", &root).args([
        "ndk",
        "-o",
        "./bindings/src/main/jniLibs",
        "--manifest-path",
        "./Cargo.toml",
    ]);
    for abi in &abis {
        ndk = ndk.args(["-t", abi]);
    }
    ndk.args(["build", "-p", "neutrino-ffi", "--release"]).run()?;

    // 3. Generate the Kotlin bindings from the host cdylib.
    let lib = format!("./target/release/{}", lib_filename());
    Cmd::new("cargo", &root)
        .args(["run", "--bin", "uniffi-bindgen", "generate", "--library"])
        .arg(lib)
        .args(["--language", "kotlin", "--out-dir", "./bindings/src/main/java"])
        .run()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lib_filename_matches_host() {
        if cfg!(target_os = "macos") {
            assert_eq!(lib_filename(), "libneutrino.dylib");
        } else {
            assert_eq!(lib_filename(), "libneutrino.so");
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it compiles and passes**

(`publish.rs` / `complement.rs` are still absent, so `cargo test -p xtask` won't link yet. Defer the run to Task 4 Step 4, where all three modules exist. For now just confirm this file is type-correct in isolation by checking the next task continues.)

- [ ] **Step 3: Commit**

```bash
git add tools/xtask/src/compile.rs
git commit -m "feat(xtask): compile subcommand (cargo-ndk + uniffi-bindgen)"
```

---

## Task 3: `publish` subcommand

**Files:**
- Create: `tools/xtask/src/publish.rs`
- Test: inline `#[cfg(test)]` in `tools/xtask/src/publish.rs`

- [ ] **Step 1: Write the module with tests**

Create `tools/xtask/src/publish.rs`:

```rust
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
```

- [ ] **Step 2: Commit**

```bash
git add tools/xtask/src/publish.rs
git commit -m "feat(xtask): publish subcommand (gradlew publish, tag-derived version)"
```

---

## Task 4: `complement` subcommand

**Files:**
- Create: `tools/xtask/src/complement.rs`
- Test: inline `#[cfg(test)]` in `tools/xtask/src/complement.rs`

- [ ] **Step 1: Write the module with tests**

Create `tools/xtask/src/complement.rs`:

```rust
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
    let contents = std::fs::read_to_string(&allowlist_path).map_err(|e| {
        sh::Error::Other(format!("reading {}: {e}", allowlist_path.display()))
    })?;
    let entries = parse_allowlist(&contents);
    if entries.is_empty() {
        return Err(sh::Error::Other(
            "allowlist contains no enabled tests".into(),
        ));
    }

    // Run each entry as its own `go test -run`, aggregating exit codes (Go
    // splits -run on `/`, so entries can't be batched into one regex).
    let mut overall = Ok(());
    for entry in entries {
        eprintln!("\n=== Allowlist entry: {entry}");
        let res = go_test(&complement_dir, &image)
            .args(["-run", &entry])
            .args(args.extra.iter().cloned())
            .arg("./tests/csapi/...")
            .run();
        if res.is_err() {
            overall = Err(sh::Error::Other(
                "one or more allowlist entries failed".into(),
            ));
        }
    }
    overall
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_skips_blanks_and_comments() {
        let input = "# header\n\nTestFoo\n  TestBar  \n# c\nTestBaz\n";
        assert_eq!(parse_allowlist(input), vec!["TestFoo", "TestBar", "TestBaz"]);
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
```

- [ ] **Step 2: Build the whole crate**

Run: `cargo build -p xtask`
Expected: compiles cleanly (all three subcommand modules now present).

- [ ] **Step 3: Clippy**

Run: `cargo clippy -p xtask -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Run the unit tests**

Run: `cargo test -p xtask`
Expected: PASS — `lib_filename_matches_host`, the three `resolve_version` tests, and the three complement tests.

- [ ] **Step 5: Smoke-test the CLI surface**

Run: `cargo xtask --help` then `cargo xtask complement --help`
Expected: top-level help lists `compile`, `publish`, `complement`; complement help shows `--in-repo` and a trailing `[EXTRA]...`.

- [ ] **Step 6: Commit**

```bash
git add tools/xtask/src/complement.rs
git commit -m "feat(xtask): complement subcommand (image build, allowlist loop)"
```

---

## Task 5: Gradle GitHub Packages publish target

**Files:**
- Modify: `bindings/build.gradle.kts`

- [ ] **Step 1: Make the publication version property-driven**

In `bindings/build.gradle.kts`, replace the hardcoded version line inside the `release` publication:

```kotlin
            artifactId = "bindings"
            version = "0.1.0"
```

with:

```kotlin
            artifactId = "bindings"
            version = (findProperty("neutrinoVersion") as String?) ?: "0.1.0-SNAPSHOT"
```

- [ ] **Step 2: Add the GitHub Packages repository**

In the same file, inside the top-level `publishing { ... }` block (a sibling of `publications { ... }`), add:

```kotlin
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/element-hq/neutrino")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
```

So the block reads `publishing { publications { ... } repositories { ... } }`.

- [ ] **Step 3: Verify (best-effort — no Gradle in the sandbox)**

If a JDK + Android SDK are available: `./gradlew :bindings:tasks --group publishing` should list `publishReleasePublicationToGitHubPackagesRepository` and `publishToMavenLocal`. Otherwise verify by inspection that the Kotlin parses (balanced braces, `repositories` is a sibling of `publications`).

- [ ] **Step 4: Commit**

```bash
git add bindings/build.gradle.kts
git commit -m "build(bindings): publish to GitHub Packages, version from property"
```

---

## Task 6: Release workflow (tag-triggered publish)

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Write `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags: ['v*']

permissions:
  contents: read
  packages: write

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5
      - uses: dtolnay/rust-toolchain@3c5f7ea28cd621ae0bf5283f0e981fb97b8a7af9
        with:
          toolchain: stable
          targets: armv7-linux-androideabi,aarch64-linux-android,i686-linux-android,x86_64-linux-android
      - uses: nttld/setup-ndk@2817442ee7c3346c1eb7d2ec60263c93206ba14f
        with:
          ndk-version: r27c
      - uses: actions/setup-java@99b8673ff64fbf99d8d325f52d9a5bdedb8483e9
        with:
          distribution: temurin
          java-version: '17'
      - uses: Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4
      - run: cargo install cargo-ndk
      - name: Publish bindings to GitHub Packages
        run: cargo xtask publish --version "${GITHUB_REF_NAME#v}"
        env:
          GITHUB_ACTOR: ${{ github.actor }}
          GITHUB_TOKEN: ${{ github.token }}
```

> Note: the version is passed explicitly from the tag (`${GITHUB_REF_NAME#v}`) so it does not depend on `git describe` / fetch-depth. The `actions/setup-java` SHA above is the v4 release pin — if the executor can't confirm it, pin to the current `actions/setup-java@v4` commit SHA from the Actions marketplace and keep the SHA-pinned style used across `ci.yml`.

- [ ] **Step 2: Validate YAML**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/release.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: tag-triggered release workflow publishing to GitHub Packages"
```

---

## Task 7: Migrate CI jobs to `cargo xtask`

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Replace the `compile` job body**

Replace the three inline steps (`cargo build --release`, `cargo ndk ...`, the `uniffi-bindgen` heredoc) with a single `cargo xtask compile` call (single ABI keeps the check fast). The `compile` job becomes:

```yaml
  compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5
      - uses: dtolnay/rust-toolchain@3c5f7ea28cd621ae0bf5283f0e981fb97b8a7af9
        with:
          targets: aarch64-linux-android
          toolchain: stable
      - uses: nttld/setup-ndk@2817442ee7c3346c1eb7d2ec60263c93206ba14f
        with:
          ndk-version: r27c
      - uses: Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4
      - run: cargo install cargo-ndk
      - run: cargo xtask compile -t arm64-v8a
```

- [ ] **Step 2: Switch the `complement` job to xtask**

The `complement` job currently ends with `- run: scripts/complement.sh`. It has Go + buildx but **no Rust toolchain**, which `cargo xtask` needs. Add a toolchain step and swap the run line. Replace the `complement` job's steps so they read:

```yaml
  complement:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5
      - uses: dtolnay/rust-toolchain@3c5f7ea28cd621ae0bf5283f0e981fb97b8a7af9
        with:
          toolchain: stable
      - uses: Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4
      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff
        with:
          go-version: stable
      - uses: docker/setup-buildx-action@8d2750c68a42422c14e847fe6c8ac0403b4cbd6f
      - uses: docker/build-push-action@10e90e3645eae34f1e60eeb005ba3a3d33f178e8
        with:
          context: .
          file: docker/complement/Dockerfile
          tags: neutrino:complement
          load: true
          cache-from: type=gha
          cache-to: type=gha,mode=max
      - run: cargo xtask complement
        env:
          SKIP_IMAGE_BUILD: "1"
```

(Only the trailing two lines changed versus today — the toolchain + rust-cache steps are added, and `scripts/complement.sh` → `cargo xtask complement`. The `docker/build-push-action` comment block above it in the current file may be kept.)

- [ ] **Step 3: Validate YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: route compile and complement jobs through cargo xtask"
```

---

## Task 8: Delete the bash scripts and update docs

**Files:**
- Delete: `scripts/compile.sh`, `scripts/complement.sh`
- Modify: `README.md`, `complement/README.md`

- [ ] **Step 1: Delete the scripts**

```bash
git rm scripts/compile.sh scripts/complement.sh
```

(If `scripts/` is now empty, it disappears with the files. Do not remove anything else under `scripts/` if other files exist.)

- [ ] **Step 2: Update `README.md` Embedding section**

Replace:

```
To build Neutrino for use inside Element X, run the `compile` script:

```
$ ./scripts/compile.sh
```
```

with:

```
To build Neutrino for use inside Element X, run:

```
$ cargo xtask publish --local
```
```

Leave the four-bullet "This will:" list beneath it unchanged — it still describes the steps accurately.

- [ ] **Step 3: Update `complement/README.md` invocation examples**

Replace each `bash scripts/complement.sh ...` form with the `cargo xtask complement ...` equivalent:

| Old | New |
| --- | --- |
| `bash scripts/complement.sh` | `cargo xtask complement` |
| `COMPLEMENT_REF=v0.x bash scripts/complement.sh` | `COMPLEMENT_REF=v0.x cargo xtask complement` |
| `COMPLEMENT_DIR=/path/to/complement bash scripts/complement.sh` | `COMPLEMENT_DIR=/path/to/complement cargo xtask complement` |
| `bash scripts/complement.sh -run TestVersionStructure` | `cargo xtask complement -- -run TestVersionStructure` |
| `bash scripts/complement.sh --in-repo` | `cargo xtask complement --in-repo` |

Also update the prose line that mentions the script is "Used both by `tests/` and by `scripts/complement.sh`" to reference `cargo xtask complement`. Note the `-run` example gains a `--` separator (clap passthrough). Leave the `complement/README.md:9` framework-version explanation otherwise intact.

- [ ] **Step 4: Verify no stale references remain in live docs**

Run: `grep -rn "scripts/compile.sh\|scripts/complement.sh" README.md complement/README.md`
Expected: no output. (Historical mentions in `LOG.md`, `PLAN.md`, `complement/VIABLE-TESTS.md`, and `CLAUDE.md` are intentionally left as-is.)

- [ ] **Step 5: Commit**

```bash
git add README.md complement/README.md scripts/
git commit -m "docs: replace compile.sh/complement.sh with cargo xtask"
```

---

## Task 9: Final verification + project bookkeeping

**Files:**
- Modify: `LOG.md` (append), `PLAN.md` (decisions log)

- [ ] **Step 1: Full check on the new crate**

Run: `cargo fmt --check && cargo clippy -p xtask -- -D warnings && cargo test -p xtask`
Expected: clean fmt, no clippy warnings, all xtask tests pass.

- [ ] **Step 2: Confirm the workspace still builds end-to-end**

Run: `cargo build --workspace`
Expected: success (xtask is included; no other crate affected).

- [ ] **Step 3: Append a 2-line summary to the bottom of `LOG.md`**

Append (oldest-first, so add at the end), with no rationale (rationale belongs in PLAN.md):

```
2026-06-03: Added tools/xtask crate (clap CLI) with compile/publish/complement subcommands, replacing scripts/compile.sh and scripts/complement.sh; added .cargo/config.toml alias. CI compile/complement jobs now call cargo xtask.
2026-06-03: bindings/build.gradle.kts publishes to GitHub Packages (maven.pkg.github.com/element-hq/neutrino) with a property-driven version; new .github/workflows/release.yml publishes on v* tags.
```

- [ ] **Step 4: Append a decision entry to the PLAN.md decisions log**

Add under the decisions log (preserve existing entries):

```
2026-06-03: Migrated dev tooling from bash (scripts/compile.sh, scripts/complement.sh) to a Rust `cargo xtask` crate, and set up GitHub Packages Maven publishing of the bindings on `v*` tags. Rationale: single source of truth runnable identically locally and in CI (CI is a thin wrapper around `cargo xtask`); tags give immutable, version-pinned releases. xtask is an orchestrator (shells out to cargo/cargo-ndk/gradlew/docker/go/wget/tar) — no download/extract logic reimplemented in Rust. Deps: clap + thiserror (both already workspace deps); no new external crates.
```

- [ ] **Step 5: Commit**

```bash
git add LOG.md PLAN.md
git commit -m "docs: log xtask migration and GitHub Packages publishing"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** xtask crate (T1–4), gradle GH Packages + version (T5), release workflow on tags (T6), CI thin-wrapper migration (T7), script deletion + doc updates (T8), bookkeeping (T9). All spec sections map to a task.
- **Type consistency:** `CompileArgs` derives `Default` (used by `publish::run`); `sh::Cmd`/`sh::Error` signatures match across all three subcommand modules; `go_test()` returns a `Cmd` builder that callers extend then `.run()`.
- **Known sandbox limits:** `cargo xtask compile/publish/complement` *runs* need NDK/Gradle/docker/go and a writable `.git`; they are verified in CI, not in this sandbox. Unit-tested logic (version derivation, allowlist parsing, lib filename, clap parsing) is fully runnable here.
