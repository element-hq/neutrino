# Neutrino

A lightweight, embedded homeserver written in Rust.

## Overview

Neutrino is split into a number of different modules:

- `neutrino-common` contains the common type definitions and utilities used throughout the project.
- `neutrino-state` contains the room-state model, event authorisation rules, and state resolution.
- `neutrino-store` defines the `StorageBackend` storage trait that handlers go through.
- `neutrino-store-sqlite` is the SQLite implementation of that storage trait.
- `neutrino-http` contains the HTTP router for both the C2S and S2S Matrix APIs.
- `neutrino-main` contains a common entrypoint between the host development and Android-embedded
  servers.

Two further crates are involved in building binary versions of the server:

- `neutrino` contains the host development binary definition.
- `neutrino-ffi` contains bindings for Neutrino for use in Android, generated using UniFFI.

Neutrino makes use of the [`xtask` pattern](https://github.com/matklad/cargo-xtask) for compiling
and publishing the project's FFI bindings; see [`tools/xtask/README.md`](tools/xtask/README.md) for
the command reference.

## Building

### Prerequisites

To build Neutrino you'll need:

- A working Rust toolchain
- Android SDK + NDK (r27c is what CI uses)
- [`cargo-ndk`](https://github.com/bbqsrc/cargo-ndk)
- A JDK 17 (for the Gradle binding build — the bundled `./gradlew` wrapper handles Gradle itself)

### Development

You can run the homeserver locally during development to test against arbitrary clients:

```
$ cargo run --bin neutrino
```

By default, this starts the server on `http://localhost:3000`.

You can also reverse-proxy the development server into an Android emulator running the **mainline**
`element-x-android`:

```
$ adb reverse tcp:3000 tcp:3000
```

You can then use the same URL as above as the homeserver URL when logging in.

### Embedding

Element X Android consumes Neutrino as a pre-built UniFFI binding artifact published to a Maven
repository, rather than building the Rust from source. The
[`element-x-android-neutrino`](https://github.com/element-hq/element-x-android-neutrino) fork
resolves the bindings (coordinate `io.element.neutrino:bindings:<version>`) from **your local Maven
repository (`~/.m2`) first, falling back to GitHub Packages**.

For local development, publish to `~/.m2` and re-sync your `element-x-android-neutrino` checkout to
pick up the freshly-built bindings:

```
$ cargo xtask publish --local
```

The published version is taken from the git tag on `HEAD` with any leading `v` stripped - so on a
tagged commit (e.g. `v0.2.0`) it publishes as `0.2.0`, and on an untagged commit it falls back to
`0.1.0-SNAPSHOT`. Either way, make sure the fork's dependency declaration requests the matching
version (or pass `--version <v>` to override it).

Tagged production releases are published to
[GitHub Packages](https://github.com/orgs/element-hq/packages?repo_name=neutrino) by CI: pushing a
`v*` tag builds all four Android ABIs and publishes the bindings under the matching version. Since
`~/.m2` takes priority, **these are only resolved when no local build of a matching version is
present**.

For example, to iterate from the `v0.2.0` release towards `0.2.1`:

1. Publish a snapshot to `~/.m2`. A snapshot is an in-development build rather than a tagged commit,
   so pass the version explicitly (otherwise it falls back to `0.1.0-SNAPSHOT`):

   ```
   $ cargo xtask publish --local --version 0.2.1-SNAPSHOT
   ```

2. Set the matching version in the fork's `gradle/libs.versions.toml` (it has a dedicated `neutrino`
   version block):

   ```toml
   [versions]
   neutrino = "0.2.1-SNAPSHOT"
   ```

3. Re-sync your `element-x-android-neutrino` repository to pick up the bindings. This can be done
   via the far right toolbar buttons inside Android Studio.

4. Once `v0.2.1` is pushed as a tag on GitHub, CI builds all four ABIs and publishes `0.2.1` (no
   `-SNAPSHOT`) to GitHub Packages. Bump the catalog to `neutrino = "0.2.1"`, ensuring no local
   `0.2.1` build exists in `~/.m2`, the fork now resolves it from GitHub Packages.

The mechanics of both paths are documented in [`tools/xtask/README.md`](tools/xtask/README.md).
