#!/bin/sh
set -eu

: "${SERVER_NAME:?SERVER_NAME must be set by the complement harness}"

CERT_DIR=/etc/neutrino
mkdir -p "${CERT_DIR}"

# Mint a TLS certificate signed by the complement CA mounted at /complement/ca.
# The SAN matters: Go TLS clients (Complement's federation client, a peer's
# egress) ignore CN.
openssl genrsa -out "${CERT_DIR}/server.key" 2048
openssl req -new \
    -key "${CERT_DIR}/server.key" \
    -subj "/CN=${SERVER_NAME}" \
    -out /tmp/server.csr
printf 'subjectAltName=DNS:%s\n' "${SERVER_NAME}" > /tmp/server.ext
openssl x509 -req \
    -in /tmp/server.csr \
    -CA /complement/ca/ca.crt \
    -CAkey /complement/ca/ca.key \
    -set_serial 01 \
    -days 1 \
    -extfile /tmp/server.ext \
    -out "${CERT_DIR}/server.crt"

# Render the nginx config with the configured server name.
export SERVER_NAME
envsubst '${SERVER_NAME}' \
    < /etc/neutrino/nginx.conf.template \
    > /etc/nginx/nginx.conf

# Inbound: nginx on 8008 (plain) and 8448 (TLS). Outbound: the egress on
# loopback upgrades neutrino's plaintext federation requests to HTTPS.
nginx -g 'daemon off;' &
NGINX_PID=$!
EGRESS_LISTEN=127.0.0.1:18449 EGRESS_CA=/complement/ca/ca.crt /usr/local/bin/neutrino-egress &
EGRESS_PID=$!

# Forward signals so docker stop is clean.
trap 'kill -TERM ${NGINX_PID} ${EGRESS_PID} 2>/dev/null || true' TERM INT

# Hand the foreground to neutrino; it only ever touches loopback.
export NEUTRINO_SERVER_NAME="${SERVER_NAME}"
export NEUTRINO_BIND_ADDR="127.0.0.1:18008"
export NEUTRINO_FEDERATION_PROXY="http://127.0.0.1:18449"
exec /usr/local/bin/neutrino
