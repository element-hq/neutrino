# syntax=docker/dockerfile:1.7

# cargo-chef splits the build into a planner stage that emits a dependency
# recipe and a cooker stage that compiles deps from that recipe. The deps
# layer is cached until Cargo.toml/Cargo.lock change.
FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /src

FROM chef AS planner
# Use the pruned Complement workspace manifest (excludes neutrino-ffi + the
# vendored BLE crates). `neutrino` — the only bin built here — doesn't use them,
# and excluding them keeps `blew`/`iroh-ble-transport` out of the cargo-chef
# recipe entirely, so cargo-chef's local-version normalisation can't trigger the
# `links = "blew"` collision. `vendor/coap-rs` is the one vendored crate that IS
# copied: neutrino-lb depends on it and the pruned manifest patches `coap` to
# this path (it has no `links` key, so it can't collide like `blew`).
COPY docker/complement/Cargo.toml ./Cargo.toml
COPY Cargo.lock ./Cargo.lock
COPY crates ./crates
COPY tools ./tools
COPY vendor/coap-rs ./vendor/coap-rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /src/recipe.json recipe.json
# Scope to the neutrino binary so we don't pull in tools/uniffi-bindgen, whose
# uniffi[cli] dep transitively compiles uniffi_bindgen
RUN cargo chef cook --release --recipe-path recipe.json --bin neutrino

COPY docker/complement/Cargo.toml ./Cargo.toml
COPY Cargo.lock ./Cargo.lock
COPY crates ./crates
COPY tools ./tools
COPY vendor/coap-rs ./vendor/coap-rs
RUN cargo build --release --bin neutrino --features multi-user-shim

# Runtime: nginx sidecar terminates TLS on 8448 and proxies plain HTTP on 8008.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        nginx \
        openssl \
        gettext-base \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/neutrino /usr/local/bin/neutrino
COPY docker/complement/conf/nginx.conf.template /etc/neutrino/nginx.conf.template
COPY docker/complement/conf/entrypoint.sh /entrypoint.sh

RUN mkdir -p /etc/neutrino \
    && chmod +x /entrypoint.sh

EXPOSE 8008 8448

ENTRYPOINT ["/entrypoint.sh"]
