# neutrino-lb: low-bandwidth federation proxy (v1 — JSON↔CBOR over HTTP) — design

Date: 2026-06-15

## Problem

Neutrino federates server-to-server over plain HTTP+JSON (`FederationClient` in
`crates/neutrino-http/src/federation/client.rs` resolves `http://{server_name}` and
sends `.json(body)`). The end goal (per `CLAUDE.md` and the workspace blueprint, MSC3079)
is to federate over a low-bandwidth link: **CoAP + CBOR over UDP / serial**, mirroring the
legacy Dendrite `internal/lb` + `cmd/dendrite-serial/fedcoap` stack.

That end goal is two independent changes welded together: (a) encode bodies as CBOR instead
of JSON, and (b) carry requests over CoAP/UDP instead of HTTP/TCP. This spec delivers **(a)
only**, behind a clean transport seam, so that (b) can be added later without touching
`neutrino-http`. Doing CBOR first proves the proxy boundary end-to-end while still riding
HTTP/TCP (debuggable with ordinary tooling).

This is the open question `CLAUDE.md` flags ("CBOR could be done in HTTP layer?"): the answer
is **CBOR lives in `neutrino-lb`, not in `neutrino-http`'s HTTP layer.**

## Scope

**In scope (v1):**
- A new `neutrino-lb` crate: a standalone bidirectional HTTP proxy that transcodes
  federation request/response **bodies** between JSON (local side) and CBOR (wire side).
- One config field on `neutrino-http` to route outbound federation through the proxy.
- A `WireClient` / `WireServer` trait pair scoped to the **wire hop**, with a single v1
  HTTP+CBOR implementation behind them.

**Out of scope (explicit follow-ups, not now):**
- The integer-key / enum-key CBOR codec (Dendrite `cbor_codec.go` / `cbor_v1.go`) and
  single-byte CoAP path enums. v1 is an **opaque** `JSON value ⇄ CBOR bytes` transcode.
- The CoAP/UDP transport (the second `WireClient`/`WireServer` impl). Gated on a separate
  investigation of Rust CoAP libraries.
- Any change to `neutrino-http`'s axum Router, federation handlers, or route list.
- Signatures, EDUs, E2EE, pagination, auth (all out of scope per `CLAUDE.md`).

## Topology

Every node runs `neutrino-http` (homeserver) **behind** its own `neutrino-lb` sidecar.
`neutrino-lb` ingress owns the public federation port that `server_name` resolves to;
`neutrino-http` binds loopback only.

```
A:neutrino-http ──(http+json, loopback)──▶ A:neutrino-lb [EGRESS]
                                              ├─ local-in:  HTTP server (recv reqwest-proxied req)
                                              ├─ JSON→CBOR
                                              └─ wire-out:  WireClient::send ──┐
                                                                               │ (http+cbor wire; CoAP/UDP later)
                                                                               ▼
B:neutrino-lb [INGRESS] ◀── wire-in: WireServer (recv cbor) ───────────────────┘
   ├─ CBOR→JSON
   └─ local-out: HTTP client (reqwest) ──(http+json, loopback)──▶ B:neutrino-http /_matrix/federation/*
```

Responses retrace the path, transcoded the other way. Because the wire destination is just
`server_name` (which already resolves to the peer's public address, now its ingress port),
**no address-mapping table is needed** — egress forwards to `http://{dest}{path}` exactly as
`FederationClient` does today, only with a CBOR body. This mirrors Dendrite: the CoAP listener
owned the federation port and Dendrite sat behind it.

## Why a plain proxy (no Router surgery)

`neutrino-lb` is a **transparent** proxy: it forwards opaque `(method, path, query, headers)`
and rewrites only the body. It never enumerates federation routes, so there is nothing to keep
in sync with `neutrino-http`'s router. `neutrino-http`'s axum wiring and federation handlers
are **untouched**. The only `neutrino-http`-side code change in the whole design is one new
config field (below). The inbound side requires no `neutrino-http` code change at all — it just
binds loopback, a config value.

## Crate layout

New workspace member `crates/neutrino-lb` (library + thin dev binary). Modules:

- `codec` — opaque body transcode.
  - `json_to_cbor(&[u8]) -> Result<Vec<u8>, CodecError>`: parse to `serde_json::Value`, encode
    to CBOR via **ciborium**.
  - `cbor_to_json(&[u8]) -> Result<Vec<u8>, CodecError>`: decode CBOR to `ciborium::Value`
    (or `serde_json::Value` via serde), encode to JSON.
  - Round-trippable for all federation bodies. Empty body → empty body (GET requests).
- `egress` — the local→wire half (a **forward proxy**).
  - `local-in`: an axum service that accepts `neutrino-http`'s reqwest-proxied requests
    (absolute-URI request-target). Reads `dest` from the request's URI authority and `path`
    from its path-and-query.
  - Transcodes the request body JSON→CBOR, builds a `WireRequest`, calls `WireClient::send`,
    transcodes the `WireResponse` body CBOR→JSON, returns it to `neutrino-http`.
- `ingress` — the wire→local half (a **reverse proxy**).
  - Implements `WireHandler`: given a `WireRequest` (CBOR body), transcodes CBOR→JSON, forwards
    verbatim (method/path/headers) to the loopback upstream via reqwest, transcodes the response
    JSON→CBOR, returns a `WireResponse`. Path/method/headers pass through untouched.
  - Hosted by a `WireServer`, which owns the public-port listener.
- `transport` — the wire seam.
  - `WireRequest` / `WireResponse` / `WireError` (transport-neutral message types).
  - `WireClient` (egress sender) and `WireServer` + `WireHandler` (ingress receiver) traits.
  - `http` submodule: the v1 `HttpWireClient` (reqwest, `Content-Type: application/cbor`) and
    `HttpWireServer` (axum catch-all on the public port). The future CoAP/UDP impls live beside
    these and are selected at construction; nothing above `transport` changes.
- `config` — `LbConfig` (see below).
- `lib.rs` — `serve(LbConfig, CancellationToken) -> Result<(), LbError>` starting both halves;
  composed by `neutrino-ffi` (Android) or the dev binary.
- `main.rs` (dev bin) — parse config, run `serve` until ctrl-c.

### Transport seam (the part that becomes CoAP)

```rust
pub struct WireRequest {
    pub dest:    OwnedServerName,        // transport resolves this (host:port now; UDP/pubkey later)
    pub method:  http::Method,           // GET / PUT / POST
    pub path:    String,                 // "/_matrix/federation/v1/send/{txn}?..."
    pub headers: Vec<(String, Vec<u8>)>, // pass-through: Authorization/X-Matrix, etc.
    pub body:    bytes::Bytes,           // already CBOR (empty for GET)
}
pub struct WireResponse { pub status: u16, pub headers: Vec<(String, Vec<u8>)>, pub body: bytes::Bytes }

#[async_trait]
pub trait WireClient: Send + Sync {                     // EGRESS sender
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError>;
}
#[async_trait]
pub trait WireServer: Send + Sync {                     // INGRESS receiver (owns the listener)
    async fn serve(self, handler: Arc<dyn WireHandler>, shutdown: CancellationToken)
        -> Result<(), WireError>;
}
#[async_trait]
pub trait WireHandler: Send + Sync + 'static {          // ingress proxy logic implements this
    async fn handle(&self, req: WireRequest) -> WireResponse;
}
```

The accept-loop/framing lives in the `WireServer` impl; the transcode-and-forward-to-loopback
lives in the `WireHandler`. The handler never learns TCP-vs-UDP. The CoAP/UDP swap replaces only
the `transport::http` impls; `codec`, `egress`, `ingress` are byte-identical across it. MTU /
blockwise concerns (Dendrite hacked SZX to 64 KB for large `/send` and `send_join` bodies) are
isolated entirely inside the future CoAP `WireClient`/`WireServer`.

## neutrino-http changes (minimal)

1. **Config field.** Add `federation_proxy: Option<String>` (a base URL like
   `http://127.0.0.1:8009`) to `Config` in `neutrino-common`, with the matching `NeutrinoConfig`
   FFI mirror + `From` impl in `neutrino-ffi` (same split pattern as existing `Config`).
   Default `None` → current direct behaviour preserved (tests and non-lb deployments unchanged).

2. **FederationClient uses it.** `FederationClient::new` currently builds reqwest with
   `.no_proxy()`. When `federation_proxy` is `Some(url)`, build with
   `.proxy(reqwest::Proxy::all(url)?)` instead. For plain-HTTP requests reqwest then sends the
   full absolute-URI request (`PUT http://{dest}/path` + body) to the proxy. Both construction
   sites must honour the config: `AppState::from_store` (`lib.rs:189`) and
   `federation::sender::spawn_with` (`sender.rs:130`). Pass the proxy setting through alongside
   the existing `origin`.

No other `neutrino-http` change. URL construction in `client.rs` is unchanged; headers
(Authorization/X-Matrix) flow through reqwest as today.

## Deployment / addressing model

- `neutrino-http` binds loopback (e.g. `127.0.0.1:8008`) — a config value, no code change.
- `neutrino-lb` ingress (`WireServer`) binds the public federation port (what peers'
  `server_name` resolve to).
- `neutrino-lb` egress local-in server binds a loopback port; `neutrino-http`'s
  `federation_proxy` points at it.
- `neutrino-lb` ingress forwards to `neutrino-http`'s loopback bind (`LbConfig.upstream`).
- `LbConfig`: `ingress_bind: SocketAddr`, `egress_bind: SocketAddr`, `upstream: Url`, timeouts.

## Wire framing & content negotiation (v1 HTTP impl)

- `HttpWireClient` sends to `http://{dest}{path}`, copies method + pass-through headers, sets
  `Content-Type: application/cbor`, body = CBOR. `Content-Length` is recomputed (CBOR length ≠
  JSON length) by setting a fresh body.
- `HttpWireServer` accepts any method/path (axum `fallback`, **not** a route table), treats the
  body as CBOR, builds a `WireRequest`, calls the handler.
- Bodies are fully buffered (bounded; we need the whole body to transcode anyway).
- GET requests (`make_join`, `backfill`, `make_leave`) carry no request body — request
  transcode is a no-op; only the response body is transcoded.

## Error handling

- Per `CLAUDE.md`: `thiserror`, no `anyhow`, no `.unwrap()`/`.expect()` in proxy code.
- Transcode failure → `502` (bad upstream/peer payload).
- Wire/peer unreachable or upstream error → surfaced to `FederationClient` as an ordinary failed
  HTTP request, so `neutrino-http`'s **existing per-destination backoff and anti-entropy keep
  working unchanged** (the proxy is indistinguishable from "the remote" to the outbound sender).
- `LbError` / `WireError` / `CodecError` are distinct `thiserror` enums.

## Testing

- **Codec round-trip** (unit, `neutrino-lb`): `json → cbor → json` is identity for representative
  federation bodies (a `/send` transaction, a `send_join` event, an `invite` envelope, a
  `get_missing_events` request). Property test over arbitrary JSON values if cheap.
- **Egress + ingress integration** (`neutrino-lb`): spin a mock loopback upstream, an ingress, and
  an egress; send a federation PUT through egress; assert the upstream receives correct JSON with
  method/path/headers intact, and the response round-trips back as JSON.
- **End-to-end** (insert into existing federation e2e): stand up two `neutrino-http` instances each
  behind a `neutrino-lb`, exchange a real `/send`, and assert convergence still holds (reuse the
  `converge` rig where practical). May land as a follow-up test if the rig needs wiring work.
- Port any relevant Synapse/Complement tests only if they map (likely none — this is a transport
  concern). Direct-mode (`federation_proxy = None`) regression: existing federation tests must
  still pass untouched.

## New dependencies (need approval per CLAUDE.md)

`neutrino-lb` is a new crate; its deps:
- **`ciborium`** — CBOR codec. **New external dependency** (serde_cbor is unmaintained).
  Requires explicit approval.
- `axum`, `tokio`, `reqwest`, `bytes`, `thiserror`, `tracing`, `tokio-util` (CancellationToken),
  `serde_json`, `http`, `async-trait`, `ruma` (for `OwnedServerName`) — already in the workspace.

## Follow-up work (write-ups, not this task)

1. **Integer-key / enum-key CBOR codec** — port Dendrite `internal/lb` `cbor_codec.go` /
   `cbor_v1.go` (Matrix JSON string keys → integer enum keys) and the single-byte CoAP path
   enums (`coap_paths*.go`). Lands behind the same `codec` module surface. Pairs with CoAP.
2. **CoAP/UDP transport** — the second `WireClient`/`WireServer` impl, including blockwise/MTU
   handling. **Gated on an investigation of Rust CoAP libraries** (e.g. `coap-lite`, `coap`).

## Decisions log (to append to PLAN.md on implementation)

- `neutrino-lb` is a **standalone sidecar proxy**; CBOR transcode lives in it, not in
  `neutrino-http`'s HTTP layer.
- v1 is an **opaque** JSON↔CBOR transcode (no key remapping); integer-key codec deferred.
- Outbound handoff via **reqwest proxy mode** (one `federation_proxy` config field); no URL or
  Router change in `neutrino-http`.
- `WireClient`/`WireServer` traits are scoped to the **wire hop only**; local hops stay plain
  HTTP. The traits are the CoAP seam; v1 ships one HTTP+CBOR impl behind them.
- HTTP-server substrate: **axum (`fallback`) + reqwest**.
