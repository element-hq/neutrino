# syntax=docker/dockerfile:1.7
#
# Build the neutrino dev binary and ship it on a slim runtime.
# Build context is the repo root (see docker-compose.yml `build.context: ..`),
# so paths below are relative to the workspace root, not testrig/.
#
# cargo-chef splits the build into a planner stage that emits a dependency
# recipe and a builder stage that compiles deps from that recipe. The deps
# layer is cached until Cargo.toml/Cargo.lock change — so CI's `type=gha` cache
# turns the common (no-dep-change) run into a near-instant rebuild instead of a
# full cold compile.
FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=planner /src/recipe.json recipe.json
# Scope to the neutrino binary so we don't pull in tools/uniffi-bindgen, whose
# uniffi[cli] dep transitively compiles uniffi_bindgen.
RUN cargo chef cook --release --recipe-path recipe.json --bin neutrino
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
RUN cargo build --release --bin neutrino

FROM debian:bookworm-slim
# reqwest's default TLS backend links libssl; harmless if a rustls build is used.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/neutrino /usr/local/bin/neutrino
# Bind on :80 inside the container so federation's `http://{server_name}`
# (which defaults to port 80) reaches us without a `:port` in user/room ids.
ENV NEUTRINO_BIND_ADDR=0.0.0.0:80
EXPOSE 80
ENTRYPOINT ["neutrino"]
