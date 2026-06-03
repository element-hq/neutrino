# xtask tooling + GitHub Packages Maven publish — design

**Date:** 2026-06-03
**Branch:** `kaylendog/tools/xtask`
**Goal:** Publish the Neutrino UniFFI Kotlin bindings to GitHub Packages (Maven)
on tag pushes, and migrate the existing `scripts/*.sh` dev tooling to a Rust
`xtask` model that is the single source of truth for build/publish/complement
flows (run identically locally and in CI).

## Decisions (locked with Skye, 2026-06-03)

1. Migrate **both** `scripts/compile.sh` and `scripts/complement.sh` to `xtask`.
2. Publishing is triggered by **git tags** (`v*`); the published version is
   derived from the tag.
3. **CI is a thin wrapper** — workflows call `cargo xtask <cmd>`; all build /
   publish / complement logic lives in `xtask`.
4. Use **clap** (workspace dep, `4.6.1` + `derive`, already added) for the CLI.

## Non-goals

- No signing / Maven Central publishing (GitHub Packages only, trusted network).
- No migration of the `fmt` / `clippy` / `test` CI jobs — those are plain `cargo`
  invocations, not scripts, and stay as-is (YAGNI; respects "move the scripts").
- No reimplementation of download/extract/docker logic in pure Rust — `xtask` is
  an **orchestrator** that shells out to existing tools (`cargo`, `cargo-ndk`,
  `gradlew`, `docker`, `go`, `wget`, `tar`, `git`). Keeps the dep surface to just
  `clap`.

## Components

### 1. `tools/xtask` crate (new)

- Lives at `tools/xtask` (next to `tools/uniffi-bindgen`); added to the workspace
  `members` list in the root `Cargo.toml`.
- Binary name `xtask`. Single external dep: `clap = { workspace = true }`.
- `.cargo/config.toml` (new, repo root):
  ```toml
  [alias]
  xtask = "run --quiet --package xtask --"
  ```
  so `cargo xtask <cmd>` resolves from anywhere in the workspace.
- Orchestration via `std::process::Command`. A small helper runs a command,
  streams stdio, and returns a `Result` that fails the process on non-zero exit
  (mirrors `set -euo pipefail`). All paths are resolved against the workspace root
  (located via `CARGO_MANIFEST_DIR` → `../..`), so commands work regardless of CWD.

#### Subcommands

`compile [-t/--target <abi>]...`
- Port of `compile.sh` build steps (no publish):
  1. `cargo build --release` (host target — required so uniffi-bindgen can load
     the `.so`/`.dylib`).
  2. `cargo ndk -o ./bindings/src/main/jniLibs --manifest-path ./Cargo.toml
     -t <abi>... build -p neutrino-ffi --release`.
     Default ABIs: `armeabi-v7a arm64-v8a x86 x86_64`. `-t` overrides (repeatable)
     so the CI check job can build a single ABI fast.
  3. `cargo run --bin uniffi-bindgen generate --library
     ./target/release/libneutrino.<so|dylib> --language kotlin --out-dir
     ./bindings/src/main/java`. Library extension chosen by `cfg!(target_os)`
     (`dylib` on macOS, else `so`) — replaces the `uname` shell branch.

`publish [--local] [--version <V>]`
- Runs `compile` (all default ABIs), then:
  - remote (default): `./gradlew :bindings:publish` → pushes to GitHub Packages.
  - `--local`: `./gradlew :bindings:publishToMavenLocal` (the old end-of-`compile.sh`
    behaviour, for local EX development).
- Version resolution: `--version` if given, else `git describe --tags --exact-match`
  with a leading `v` stripped, else fallback `0.1.0-SNAPSHOT`. Passed to Gradle as
  `-PneutrinoVersion=<V>`.

`complement [--in-repo] [-- <go test args>...]`
- Port of `complement.sh`. clap passthrough via
  `#[arg(trailing_var_arg = true, allow_hyphen_values = true)]` on a
  `Vec<String>` for the forwarded `go test` args.
- Preserves all current behaviour and env knobs:
  - Env: `COMPLEMENT_DIR`, `COMPLEMENT_REF` (default `main`), `IMAGE_TAG`
    (default `neutrino:complement`), `SKIP_IMAGE_BUILD`.
  - Image: skip build when `SKIP_IMAGE_BUILD` set **and** the image already exists
    (`docker image inspect`); else `docker build -f docker/complement/Dockerfile`.
  - `--in-repo`: `go test -v -timeout 5m <extra> ./tests/...` in `./complement`.
  - Upstream: fetch `matrix-org/complement@$REF` tarball into
    `./complement-$REF/` (via `wget` + `tar`) when `COMPLEMENT_DIR` unset;
    iterate `complement/allowlist.txt` line-by-line running `go test -run <entry>
    ./tests/csapi/...`; skip blank/`#` lines; aggregate exit codes.
  - Ad-hoc bypass: if forwarded args contain `-run`, skip the allowlist and run
    the selection directly (matches the current script).

#### Unit tests (the pure logic only)

- Version derivation: `v0.1.0` → `0.1.0`; `0.2.0` → `0.2.0`; empty → fallback.
- Allowlist parsing: blank lines and `#` comments skipped; entries trimmed.
- Library filename selection for the current `target_os`.
- clap arg parsing: `complement --in-repo -- -run Foo -v` collects passthrough
  args correctly; `compile -t arm64-v8a -t x86` yields both ABIs.

Orchestration that shells out to `cargo-ndk` / `docker` / `go` is **not** unit
tested (no NDK/docker in the dev sandbox); it is exercised by CI.

### 2. `bindings/build.gradle.kts` (edit)

- Add a publish target repository inside the existing `publishing {}` block:
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
- Replace the hardcoded `version = "0.1.0"` in the publication with:
  ```kotlin
  version = (findProperty("neutrinoVersion") as String?) ?: "0.1.0-SNAPSHOT"
  ```
- `publishToMavenLocal` continues to work unchanged (built into `maven-publish`).

### 3. `.github/workflows/release.yml` (new)

```yaml
on:
  push:
    tags: ['v*']
permissions:
  contents: read
  packages: write
```
Single job:
1. `actions/checkout`
2. `dtolnay/rust-toolchain@stable` with the four Android targets
   (`armv7-linux-androideabi`, `aarch64-linux-android`, `i686-linux-android`,
   `x86_64-linux-android`).
3. `nttld/setup-ndk` `r27c`.
4. `actions/setup-java` (Temurin 17) — Gradle needs a JDK.
5. `cargo install cargo-ndk`.
6. `cargo xtask publish` with `GITHUB_ACTOR` / `GITHUB_TOKEN: ${{ github.token }}`
   in `env`. Version derives from the pushed tag inside `xtask`.

Action SHAs pinned to match the style already used in `ci.yml`.

### 4. `.github/workflows/ci.yml` (edit — thin-wrapper migration)

- `compile` job: replace the inline `cargo build` + `cargo ndk` + bindgen steps
  with a single `cargo xtask compile -t arm64-v8a` (keeps the current fast
  single-ABI behaviour; toolchain still installs `aarch64-linux-android` + NDK).
- `complement` job: replace `- run: scripts/complement.sh` with
  `- run: cargo xtask complement`, keeping `env: SKIP_IMAGE_BUILD: "1"`.
- `fmt` / `clippy` / `test` jobs: unchanged.

### 5. Cleanup & doc updates

- Delete `scripts/compile.sh` and `scripts/complement.sh`. (If `scripts/` is left
  empty, remove it.)
- `README.md`: the "Embedding" section documents `./scripts/compile.sh` as
  build-bindings-and-install-to-local-Maven (it ends in `publishToMavenLocal`).
  That maps to `cargo xtask publish --local`; update the command and keep the
  bullet list describing the steps.
- `complement/README.md`: update the `bash scripts/complement.sh ...` invocations
  to `cargo xtask complement ...` (including the `COMPLEMENT_REF=`, `COMPLEMENT_DIR=`,
  `-run`, and `--in-repo` examples).
- Leave `LOG.md` / `PLAN.md` historical entries untouched (append-only history).
  `complement/VIABLE-TESTS.md` prose references to the script are descriptive
  history — out of scope to rewrite; may be touched only if trivially stale.
- `CLAUDE.md` references `compile.sh` (in the CI task description) but **must not
  be modified** per project rules; that line is descriptive of a past task.

## Docker note

`docker/complement/Dockerfile` already `COPY tools ./tools` and scopes its build
to `--bin neutrino`, so adding `tools/xtask` (zero deps beyond clap) does not
change the image build. No Dockerfile change required.

## Verification plan

- `cargo build -p xtask` compiles; `cargo clippy -p xtask -- -D warnings` clean;
  `cargo fmt` clean.
- `cargo test -p xtask` — the pure-logic unit tests pass.
- `cargo xtask --help` and per-subcommand `--help` render.
- `release.yml` / `ci.yml` are valid YAML; `build.gradle.kts` parses (visual
  review — no Android SDK/Gradle in the sandbox).
- **Not runnable in this sandbox** (no NDK, no docker, no JDK/Gradle): the full
  `compile`, `publish`, and `complement` runs. These are verified in CI. This
  limitation is stated explicitly rather than claimed as passing.

## Follow-ups (explicitly out of scope)

- Single-ABI vs all-ABI tuning of the CI compile check beyond `-t arm64-v8a`.
- Teaching `complement` to run non-csapi packages (`tests/msc4222/...`) — a
  pre-existing gap noted in `complement/VIABLE-TESTS.md`.
- Any signing / Maven Central path.
