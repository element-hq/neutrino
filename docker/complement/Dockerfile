# syntax=docker/dockerfile:1.7

# cargo-chef splits the build into a planner stage that emits a dependency
# recipe and a cooker stage that compiles deps from that recipe. The deps
# layer is cached until Cargo.toml/Cargo.lock change.
FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
# neutrino-ffi has a path dependency on the vendored iroh-ble-transport crate, so
# the workspace manifest can't be loaded without it — even though `neutrino` (the
# only bin built here) doesn't enable the `ble` feature that pulls it in.
COPY vendor ./vendor
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /src/recipe.json recipe.json
# Scope to the neutrino binary so we don't pull in tools/uniffi-bindgen, whose
# uniffi[cli] dep transitively compiles uniffi_bindgen
RUN cargo chef cook --release --recipe-path recipe.json --bin neutrino

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
COPY vendor ./vendor
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
