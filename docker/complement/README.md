# Complement image

`Dockerfile` builds an image suitable for use as `COMPLEMENT_BASE_IMAGE`. It
runs neutrino on a loopback port (`127.0.0.1:18008`) behind an nginx sidecar
that listens on `:8008` (plain HTTP) and `:8448` (TLS).

The TLS certificate is minted at container start by `conf/entrypoint.sh` from
the complement CA mounted at `/complement/ca`. Nginx config is rendered from
`conf/nginx.conf.template` via `envsubst` so `${SERVER_NAME}` is filled in
per-container.

Federation is out of scope; nginx terminates `:8448` only because complement
expects it to be reachable. No federation traffic is implemented.
