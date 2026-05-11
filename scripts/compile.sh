set -euo pipefail

# Build using the default target, required for UniFFI to generate bindings.
cargo build --release

# Build for the various Android targets (including x86 for the emulator).
cargo ndk -o ./bindings/src/main/jniLibs \
        --manifest-path ./Cargo.toml \
        -t armeabi-v7a \
        -t arm64-v8a \
        -t x86 \
        -t x86_64 \
        build \
        -p neutrino-ffi --release

# Get the built shared library - might be broken on Linux.
LIB_EXT=so
case "$(uname -s)" in
  Darwin*) LIB_EXT=dylib ;;
  *) LIB_EXT=so ;;
esac

# Generate bindings.
cargo run --bin uniffi-bindgen generate \
    --library "./target/release/libneutrino.${LIB_EXT}" \
    --language kotlin \
    --out-dir ./bindings/src/main/java

# Publish for use in EX.
./gradlew :bindings:publishToMavenLocal
