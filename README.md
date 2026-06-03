# Neutrino

 A lightweight, embedded homeserver written in Rust.

 ## Building

 ### Prerequisites

 To build Neutrino you’ll need:

 - A working Rust toolchain
 - Android SDK
 - [`cargo-ndk`](https://github.com/bbqsrc/cargo-ndk)

 ### Development

 You can run the homeserver locally during development to test against arbitrary clients:

 ```
 $ cargo run --bin neutrino
 ```

 By default, this starts the server on `http://localhost:3000`.

 You can also reverse-proxy the development server into an Android emulator running the **mainline** `element-x-android`:

 ```
 $ adb reverse tcp:3000 tcp:3000
 ```

 You can then use the same URL as above as the homeserver URL when logging in.

 ### Embedding

 To build Neutrino for use inside Element X, run:

 ```
 $ cargo xtask publish --local
 ```

 This will:

 - Build the server for your host machine (so UniFFI can generate bindings from the debug symbols).
 - Build the server for the supported Android targets.
 - Build the bindings into an `.aar` using Gradle.
 - Publish the resulting archive to your local Maven repository.

 After that, re-sync your `element-x-android` fork to pick up the updated bindings.

## Contributing

Neutrino is split into a number of different modules:

- `neutrino-common` contains the common type definitions and utilities used throughout the project.
- `neutrino-http` contains the HTTP router for both the C2S and S2S Matrix APIs.
- `neutrino-main` contains a common entrypoint between the host development and Android-embedded servers.

Two of these are involved in actually building binaries of the server:

- `neutrino` contains the host development binary definition.
- `neutrino-ffi` contains bindings for Neutrino for use in Android, generated using UniFFI.
