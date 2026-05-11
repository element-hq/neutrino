#!/bin/sh
set -eu

: "${SERVER_NAME:?SERVER_NAME must be set by the complement harness}"

CERT_DIR=/etc/neutrino
mkdir -p "${CERT_DIR}"

# Mint a TLS certificate signed by the complement CA mounted at /complement/ca.
openssl genrsa -out "${CERT_DIR}/server.key" 2048
openssl req -new \
    -key "${CERT_DIR}/server.key" \
    -subj "/CN=${SERVER_NAME}" \
    -out /tmp/server.csr
openssl x509 -req \
    -in /tmp/server.csr \
    -CA /complement/ca/ca.crt \
    -CAkey /complement/ca/ca.key \
    -set_serial 01 \
    -days 1 \
    -out "${CERT_DIR}/server.crt"

# Trust the complement CA system-wide so neutrino can talk to peers if needed.
cp /complement/ca/ca.crt /usr/local/share/ca-certificates/complement-ca.crt
update-ca-certificates

# Render the nginx config with the configured server name.
export SERVER_NAME
envsubst '${SERVER_NAME}' \
    < /etc/neutrino/nginx.conf.template \
    > /etc/nginx/nginx.conf

# Run nginx in the foreground via daemon off, in the background of this script.
nginx -g 'daemon off;' &
NGINX_PID=$!

# Forward signals to nginx so docker stop is clean.
trap 'kill -TERM ${NGINX_PID} 2>/dev/null || true' TERM INT

# Hand the foreground to neutrino. Nginx fronts 8008/8448; neutrino binds loopback.
export NEUTRINO_SERVER_NAME="${SERVER_NAME}"
export NEUTRINO_BIND_ADDR="127.0.0.1:18008"
exec /usr/local/bin/neutrino
