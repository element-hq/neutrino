# xtask

Neutrino uses the [`xtask` pattern](https://github.com/matklad/cargo-xtask) for the developer
tasks that don't fit a plain `cargo` invocation. Run `cargo xtask <command>` from the workspace
root.

## `compile`

Builds the Android shared libraries and generates the Kotlin bindings, in three steps:

1. **Builds the server for your host** (`cargo build --release`) so `uniffi-bindgen` can load the
   resulting `cdylib` and generate bindings from it.
2. **Builds the Android shared libraries** with `cargo-ndk` into `bindings/src/main/jniLibs`, for
   all four supported ABIs (`armeabi-v7a`, `arm64-v8a`, `x86`, `x86_64`).
3. **Generates the Kotlin bindings** from the host `cdylib` into `bindings/src/main/java`.

Pass `-t <abi>` (repeatable) to restrict the Android targets - e.g. `cargo xtask compile -t
arm64-v8a` builds a single ABI while iterating, which is much faster than the full four-target
build.

## `publish`

Builds the bindings (always running `compile` first) and publishes the resulting `.aar`. The
version is resolved as: the `--version <v>` flag if given, else the exact git tag on `HEAD` with
any leading `v` stripped (`v0.1.0` → `0.1.0`), else the `0.1.0-SNAPSHOT` fallback.

With `--local`, it runs `./gradlew :bindings:publishToMavenLocal`, dropping the artifact into
`~/.m2`. Without it, it runs `./gradlew :bindings:publish` to publish to GitHub Packages
(`https://maven.pkg.github.com/element-hq/neutrino`), which requires `GITHUB_ACTOR` and
`GITHUB_TOKEN` in the environment for authentication.

The GitHub Packages path should not be run by hand: pushing a `v*` tag triggers the
[`release`](../../.github/workflows/release.yml) workflow, which builds all four Android ABIs and
runs `cargo xtask publish --version "${tag#v}"`.

## `complement`

Runs the [Complement](https://github.com/matrix-org/complement) suite against the
`neutrino:complement` Docker image (built on demand, unless `SKIP_IMAGE_BUILD` is set and the
image already exists). By default it runs each entry in `complement/allowlist.txt` as its own
`go test -run`, aggregating the results. Flags:

- `--in-repo` runs the in-repo neutrino-specific tests under `complement/tests/...` instead of the
  allowlist.
- trailing args are forwarded verbatim to `go test` (e.g. `cargo xtask complement -- -run TestFoo
  -v`); an explicit `-run` bypasses the allowlist for debugging a single test.
