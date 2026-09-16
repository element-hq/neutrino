# Complement image

`Dockerfile` builds an image suitable for use as `COMPLEMENT_BASE_IMAGE`. It
runs neutrino on a loopback port (`127.0.0.1:18008`) with a proxy on each side
of federation:

- **Inbound** — nginx on `:8008` (plain HTTP, client-server) and `:8448`
  (TLS, server-server) proxies to neutrino. `conf/entrypoint.sh` mints the
  certificate at container start from the complement CA mounted at
  `/complement/ca`, with a SAN for `${SERVER_NAME}` (Go TLS clients ignore
  CN). Nginx config is rendered from `conf/nginx.conf.template` via `envsubst`.
- **Outbound** — neutrino's federation client is plaintext `http://` with no
  TLS backend, so the entrypoint sets `NEUTRINO_FEDERATION_PROXY` to
  `egress/main.go` on `127.0.0.1:18449`. neutrino carries the real destination
  in the request authority with a `~` host sentinel
  (`host.docker.internal~:12345`, `hs2~`); the egress strips it, defaults the
  port to 8448 and re-issues the request over HTTPS verified against the
  complement CA. Names resolve through `/etc/hosts` (the complement
  host-gateway alias) and Docker's embedded DNS (peer containers). nginx
  cannot play this role: its request-line parser rejects `~` in an
  absolute-URI host, which is what an HTTP proxy receives.

Runtime knobs (`ENV` in the Dockerfile, overridable — complement forwards host
env under `COMPLEMENT_SHARE_ENV_PREFIX`): `NEUTRINO_TRUSTED_NETWORK=0` (signed
mode: events carry `hashes`/`signatures`, requests a signed `X-Matrix`, and
peer keys are fetched from `/_matrix/key/v2/server` through the egress —
gomatrixserverlib refuses hash-less events, so trusted mode cannot federate
with complement), `NEUTRINO_STORAGE_DIR` (`/data`), `NEUTRINO_STARTUP_JITTER_MS`
(`0`; the production 30s pre-drain delay would stall federated sends past
complement's sync timeouts).

Known gaps: inbound `X-Matrix` signatures are not verified (origin is taken
from the header); `createRoom` ignores `room_version` (rooms are always
`org.matrix.msc4242.12`).

Running the MSC4242 suite from a complement checkout that has it:

```sh
docker build -f docker/complement/Dockerfile -t neutrino:complement .
cd /path/to/complement && COMPLEMENT_BASE_IMAGE=neutrino:complement \
    go test -v -count=1 ./tests/msc4242/...
```
