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

 To build Neutrino for use inside Element X, run the `compile` script:

 ```
 $ ./scripts/compile.sh
 ```

 This will:

 - Build the server for your host machine (so UniFFI can generate bindings from the debug symbols).
 - Build the server for the supported Android targets.
 - Build the bindings into an `.aar` using Gradle.
 - Publish the resulting archive to your local Maven repository.

 After that, re-sync your `element-x-android` fork to pick up the updated bindings.

## Contributing

Neutrino is split into a number of different modules:

- `neutrino-common` contains the common type definitions and utilites used throughout the project.
- `neutrino-http` contains the HTTP router for both the C2S and S2S Matrix APIs.
- `neutrino-sqlite` contains the SQLite database table definitions and logic.
- `neutrino-main` contains a common entrypoint between the host development and Android-embedded servers.

Two of these are involved in actually building binaries of the server:

- `neutrino` contains the host development binary definition.
- `neutrino-ffi` contains bindings for Neutrino for use in Android, generated using UniFFI.
>>>>>>> f095723 (Initial commit)


## Running Claude

To run Claude on your local machine, modify `.claude-env-sample` and move to `.claude-env`, then:
```
docker build -t claude-neutrino -f Claude.Dockerfile .
docker run -it --rm -v /Users/kegan/github/neutrino:/workspace -v /Users/kegan/github/neutrino/.claude-matrix:/root/.claude --env-file ./.claude-env claude-neutrino
```
Add the MCP server then start claude with `--dangerously-load-development-channels`, the container will provide instructions.