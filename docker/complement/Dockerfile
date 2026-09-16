# syntax=docker/dockerfile:1.7

# cargo-chef splits the build into a planner stage that emits a dependency
# recipe and a cooker stage that compiles deps from that recipe. The deps
# layer is cached until Cargo.toml/Cargo.lock change.
FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /src

FROM chef AS planner
# Use the pruned Complement workspace manifest (excludes neutrino-ffi, which
# `neutrino` — the only bin built here — doesn't use; keeps uniffi out of the
# cargo-chef recipe). `vendor/coap-rs` + `vendor/coap-lite` are copied:
# neutrino-lb reaches both in-house forks by path dep.
COPY docker/complement/Cargo.toml ./Cargo.toml
COPY Cargo.lock ./Cargo.lock
COPY crates ./crates
COPY tools ./tools
COPY vendor/coap-rs ./vendor/coap-rs
COPY vendor/coap-lite ./vendor/coap-lite
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /src/recipe.json recipe.json
# The recipe records `coap`/`coap-lite` as path dependencies (./vendor/* in
# the manifest), and `cargo chef cook` reads that path's Cargo.toml while
# reconstructing the skeleton — so it must be on disk before cook, not just
# before the final build. cook busts (and rebuilds coap) only when the vendored
# source changes, which is correct for a dependency.
COPY vendor/coap-rs ./vendor/coap-rs
COPY vendor/coap-lite ./vendor/coap-lite
# Scope to the neutrino binary so we don't pull in tools/uniffi-bindgen, whose
# uniffi[cli] dep transitively compiles uniffi_bindgen
RUN cargo chef cook --release --recipe-path recipe.json --bin neutrino

COPY docker/complement/Cargo.toml ./Cargo.toml
COPY Cargo.lock ./Cargo.lock
COPY crates ./crates
COPY tools ./tools
# Re-copy after cook alongside the real crate source: if cargo-chef stubbed the
# vendored source during cook, this restores it for the actual build.
COPY vendor/coap-rs ./vendor/coap-rs
COPY vendor/coap-lite ./vendor/coap-lite
RUN cargo build --release --bin neutrino --features multi-user-shim

# Outbound federation egress (see egress/main.go): neutrino's federation
# client is plaintext http:// with no TLS backend, so the entrypoint routes it
# through this proxy, which re-issues each request over HTTPS verified against
# the complement CA. Dependency-free Go, static binary.
FROM golang:1-bookworm AS egress
WORKDIR /src
COPY docker/complement/egress ./
RUN CGO_ENABLED=0 go build -trimpath -o /neutrino-egress .

# Runtime: nginx terminates TLS on 8448 and proxies plain HTTP on 8008; the
# egress handles the outbound side.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        nginx \
        openssl \
        gettext-base \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/neutrino /usr/local/bin/neutrino
COPY --from=egress /neutrino-egress /usr/local/bin/neutrino-egress
COPY docker/complement/conf/nginx.conf.template /etc/neutrino/nginx.conf.template
COPY docker/complement/conf/entrypoint.sh /entrypoint.sh

RUN mkdir -p /etc/neutrino /data \
    && chmod +x /entrypoint.sh

# Overridable (Complement forwards host env under COMPLEMENT_SHARE_ENV_PREFIX).
# Signed mode: events carry hashes + signatures and requests a signed X-Matrix,
# which Complement's gomatrixserverlib requires. Jitter is the pre-drain delay
# on the outbound sender: the 30s production default would stall every
# federated send past Complement's sync timeouts.
ENV NEUTRINO_TRUSTED_NETWORK=0 \
    NEUTRINO_STORAGE_DIR=/data \
    NEUTRINO_STARTUP_JITTER_MS=0

EXPOSE 8008 8448

ENTRYPOINT ["/entrypoint.sh"]
