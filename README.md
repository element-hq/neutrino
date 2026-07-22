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

Server operations:
- `neutrino-ctl` contains control plane operations e.g. configuration, shutdown commands.
- `neutrino-http` contains the HTTP router for both the C2S and S2S Matrix APIs.
- `neutrino-lb` contains the low bandwidth modifications which map HTTP to CoaP.
- `neutrino-testkit` contains a multi-federation test harness.

Matrix logic:
- `neutrino-engine` contains operations which work on a collection of rooms e.g. event ingestion pipeline, actor models
- `neutrino-room` contains operations which work in a single room e.g. state resolution.
- `neutrino-event` contains operations which work on a single event e.g. redaction/hashing algorithms, canonical JSON.

FFI/bindings/stand-alone:
- `neutrino-ffi` defines the FFI functions exposed to Android generated using UniFFI.
- `neutrino` defines the development binary.
- `neutrino-main` defines shared entrypoint code between binary/FFI.

Storage:
- `neutrino-store` defines the `StorageBackend` storage trait that handlers go through.
- `neutrino-store-sqlite` is the SQLite implementation of that storage trait.

Neutrino makes use of the [`xtask` pattern](https://github.com/matklad/cargo-xtask) for the
developer tasks that don't fit a plain `cargo` invocation; see
[`tools/xtask/README.md`](tools/xtask/README.md) for the command reference.

## Building

### Prerequisites

To build Neutrino you'll need a working Rust toolchain.

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

Neutrino is embedded in Element X Android as a pre-built UniFFI binding artifact (`.aar`), rather
than built from Rust source by the app. That artifact is produced and published by the
[`neutrino-iroh`](https://github.com/element-hq/neutrino-iroh) repository, which composes this
workspace over its iroh/BLE federation medium and ships a single `.aar` carrying the whole embedded
API. Binding generation, ABI builds, Maven publishing, and the tag-triggered release workflow all
live there — see that repository's README for the build and release process.

## License

Neutrino is licensed under the
[GNU Affero General Public License v3.0](./LICENSE-AGPL-3.0) (`AGPL-3.0-only`).
For discussion around alternative licensing please contact licensing@element.io
