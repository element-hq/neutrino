# Build the neutrino dev binary and ship it on a slim runtime.
# Build context is the repo root (see docker-compose.yml `build.context: ..`),
# so paths below are relative to the workspace root, not testrig/.
FROM rust:bookworm AS builder
WORKDIR /src
# Copy only what the workspace build needs — skips target/ and the pile of
# untracked scratch files in the repo root, so no .dockerignore is required.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
RUN cargo build --release -p neutrino

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
