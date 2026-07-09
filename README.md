# Neutrino

Neutrino is an experimental lightweight embedded Matrix homeserver written in Rust.

We're releasing the code at a **very early stage** for transparency, and it comes with some serious
caveats:

 1. Neutrino has been written specifically to experiment with P2P Matrix dialects.
 2. It is **not** intended to run as a standalone homeserver.
     * Instead, it's runnable from the P2P branch of Element X Android, embedded within the Android app.
 3. It is very deliberately **NOT SECURE** for use on the public internet.
     * Again, please DO NOT USE NEUTRINO ON UNTRUSTED NETWORKS (e.g. the internet) yet.
     * It does not yet put signatures on events, nor does it check them.
 4. It does not speak or interoperate with normal Matrix yet.
 5. It is deliberately very feature poor; it only supports joining/leaving rooms and sending messages.
     * It has no E2EE, or file transfer, or typing notifications, or read receipts, etc. etc.
 6. It is deliberately not at all optimised yet - e.g. it has no caching.
 7. It only implements the very latest room versions (Hydra Phase 2 and (in future) 3 - e.g. State DAGs: MSC4242)
 8. Gives us somewhere to evaluate Hydra and state resets in the harshest conditions (i.e. P2P)

 Separately, it's worth noting that:
  * Neutrino is not the future of Synapse or Synapse Pro, although we expect Neutrino and Synapse to share Rust code in future
  * This not a re-run of Dendrite (which was built as a horizontally scalable successor to Synapse, but in the end its learnings got subsumed into Synapse and Synapse Pro)
  * Instead, we want a dedicated embeddable minimal homeserver for P2P, rather than forcing a serverside homeserver to run clientside
  * We also want somewhere to experiment freely with new ideas (e.g. temporal state storage) which can get backported and/or shared with Synapse & Synapse Pro.
  * Neutrino development has been accelerated significantly via use of Claude Opus and Fable. We are very mindful of the ethical and environmental aspects of using LLMs, but have concluded that using big tech to accelerate decentralisation of communication is net positive in this instance.

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

2. Set the matching version in `element-x-android-neutrino`'s `gradle/libs.versions.toml` (it has a
   dedicated `neutrino` version block):

   ```toml
   [versions]
   neutrino = "0.2.1-SNAPSHOT"
   ```

3. Re-sync your EX Android fork to pick up the bindings. This can be done via the far right toolbar
   buttons inside Android Studio.

4. Once `v0.2.1` is pushed as a tag on GitHub, CI builds all four ABIs and publishes `0.2.1` (no
   `-SNAPSHOT`) to GitHub Packages. Bump the catalog to `neutrino = "0.2.1"`, ensuring no local
   `0.2.1` build exists in `~/.m2`, the fork now resolves it from GitHub Packages.

The mechanics of both paths are documented in [`tools/xtask/README.md`](tools/xtask/README.md).

## Releasing

This section outlines the release process for Neutrino and its Kotlin bindings. It does **not** cover how to prepare APKs of Element X Android with an embedded Neutrino homserver - see the [`element-x-android-neutrino`](https://github.com/element-hq/element-x-android-neutrino) repository for that information.

### 1. Compile Neutrino

This builds the current implementation and re-generates the Kotlin bindings if the FFI surface has changed.

```sh
cargo xtask compile
```

### 2. Commit Bindings

If the bindings were updated in the last step, these should be committed to ensure the bindings line up with the tag itself.

```sh
git add bindings
git commit -m "chore: Update bindings"
```

### 3. Tag and Push

Once the working tree is clean, you can tag with the latest version and push:
```sh
git tag v1.2.3
git push --tags
```

This will trigger the CI to perform an automatic release of the Neutrino bindings.
