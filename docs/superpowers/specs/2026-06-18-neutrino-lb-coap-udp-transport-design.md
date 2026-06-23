# neutrino-lb: CoAP/UDP wire transport (Layer B) — design

Date: 2026-06-18

## Problem

`neutrino-lb` v1 (see `2026-06-15-neutrino-lb-cbor-proxy-design.md`) transcodes
Server-Server federation bodies JSON↔CBOR, but still carries them over **HTTP/TCP**
between sidecars, behind a `WireClient`/`WireServer`/`WireHandler` trait seam built
expressly so the real transport could drop in later. The end goal (per `CLAUDE.md`
and the workspace blueprint, MSC3079) is to federate over a **low-bandwidth link**:
CoAP + CBOR over UDP, mirroring the legacy Dendrite `internal/lb` +
`cmd/dendrite-serial/fedcoap` stack.

That end goal is two independent layers:

- **Layer A — integer-key CBOR codec.** Replace the opaque `serde_json::Value`
  transcode in `codec.rs` with the key-remapping codec (Dendrite `cbor_codec.go` /
  `cbor_v1.go`): a string→int key table + the event-ID-as-raw-32-bytes trick.
  Transport-agnostic; changes *what* bytes go on the wire.
- **Layer B — CoAP/UDP transport.** A second `WireClient`/`WireServer` implementation
  carrying requests over CoAP/UDP instead of HTTP/TCP; changes *how* the bytes travel.

The two are decoupled and land iteratively. **This spec delivers Layer B only.** The
codec stays opaque; the CoAP transport never inspects the body, it just carries the
CBOR bytes `egress`/`ingress` hand it.

## Scope

**In scope:**
- A new `transport::coap` module: `CoapWireClient` (impl `WireClient`) and
  `CoapWireServer` (impl `WireServer`), a sibling of `transport::http`.
- The path↔code table (Dendrite v1 federation codes) with dynamic-segment overlay
  and a literal-path fallback.
- `WireRequest`/`WireResponse` ⇄ CoAP message mapping: method, headers, exact HTTP
  status, Content-Format.
- CoAP **blockwise transfer** (RFC 7959) so large `send_join`/`send_leave` state
  bodies survive a link with a small datagram MTU.
- A `WireKind { Http, Coap }` field on `LbConfig` (default `Http`) and a `match` in
  `crate::serve` to select the transport pair.

**Out of scope (explicit follow-ups, not now):**
- Layer A (the integer-key codec). The body stays an opaque CBOR blob here.
- SLIP / serial-link byte framing (the `cmd/test-coap/bridge` work). This iteration
  stops at a UDP socket.
- CoAP **OBSERVE** / streaming. Unlike Dendrite's `internal/lb`, this sidecar carries
  **only S-S federation request/response** — the Client-Server API (incl. `/sync`) is
  embedded locally and never crosses the wire, so there is no streaming resource to
  observe. This removes the gnarliest third of the Dendrite code.
- DTLS / any wire security (trusted-network assumption, per `CLAUDE.md`).
- Any change to `egress.rs`, `ingress.rs`, `codec.rs`, the `transport` traits, or
  `neutrino-http`.

## Design decisions (settled in brainstorming)

1. **CoAP-over-UDP only** this iteration; serial/SLIP is a later layer.
2. **Blockwise is in scope** — load-bearing, since a `send_join` response serializes
   the whole room state DAG and routinely exceeds one datagram.
3. **Reuse Dendrite's v1 federation path codes** for the routes we use, plus a
   literal-path fallback for anything unmapped.
4. **Approach 1**: lean on `coap-rs` for both the client and the server runtime, with
   `coap-lite` for message/option/path detail. A hand-rolled UDP server over
   `tokio::net::UdpSocket` + `coap-lite::BlockHandler` is the **named fallback** if
   verification shows `coap-rs`'s server-side blockwise is inadequate (see Open
   questions).

## Topology

Only the **inter-sidecar wire hop** changes. The local homeserver↔sidecar hops stay
loopback HTTP exactly as in v1 — the `WireClient`/`WireServer` seam is scoped to the
wire hop only.

```
A:neutrino-http ──(http+json, loopback)──▶ A:neutrino-lb [EGRESS]
                                              ├─ local-in:  HTTP server (recv reqwest-proxied req)
                                              ├─ JSON→CBOR (opaque codec, unchanged)
                                              └─ wire-out:  CoapWireClient::send ──┐
                                                                                   │  CoAP + CBOR over UDP (CON)
                                                                                   ▼
B:neutrino-lb [INGRESS] ◀── wire-in: CoapWireServer (UDP socket) ──────────────────┘
   ├─ map CoAP → WireRequest, call IngressHandler (unchanged)
   ├─ CBOR→JSON (opaque codec, unchanged)
   └─ local-out: HTTP client (reqwest) ──(http+json, loopback)──▶ B:neutrino-http /_matrix/federation/*
```

`dest` is still the peer `server_name` (host:port) — now a **UDP** port. `CoapWireClient`
dials `coap://{dest}`; `CoapWireServer` binds `ingress_bind` as a UDP socket. Both
sidecars must run the same `WireKind`. No address-mapping table is needed.

## Module layout

`crates/neutrino-lb/src/transport/coap/`:

- `mod.rs` — `CoapWireClient` + `CoapWireServer`; UDP socket lifecycle, per-`dest`
  client handling, the `serve` loop, shutdown wiring.
- `paths.rs` — the path↔code table + `encode_path` / `decode_path` (dynamic-segment
  overlay, query handling, literal-path fallback). Pure; unit-tested in isolation.
- `message.rs` — `WireRequest` ⇄ `coap_lite` request, `coap_lite` response ⇄
  `WireResponse`: method↔`RequestType`, headers↔options, HTTP-status carriage,
  Content-Format. Pure; unit-tested in isolation.

Selected in `crate::serve`:

```rust
// LbConfig
pub enum WireKind { Http, Coap }   // Default::default() == Http
pub wire: WireKind,
```

```rust
// crate::serve — branch on the kind, constructing concrete client+server in each
// arm. (Each arm builds its own egress+ingress futures and `tokio::select!`s them,
// exactly as serve() does today.) `WireServer::serve` takes `self` by value, so the
// server stays a concrete type per arm rather than a `Box<dyn WireServer>` — the
// egress already takes `Arc<dyn WireClient>`, so only the client is type-erased.
match config.wire {
    WireKind::Http => run_pair(HttpWireClient::new(), HttpWireServer::new(config.ingress_bind), config, shutdown).await,
    WireKind::Coap => run_pair(CoapWireClient::new(), CoapWireServer::new(config.ingress_bind), config, shutdown).await,
}
```

`neutrino-main` / FFI plumb the new field through later; the `Http` default keeps every
existing test and deployment unchanged.

## Request/response mapping

**Method.** HTTP `Method` ⇄ `coap_lite::RequestType`. GET/POST/PUT cover all nine
federation endpoints:

| Method | Endpoints |
|--------|-----------|
| GET  | `backfill`, `event`, `make_join`, `make_leave` |
| POST | `get_missing_events` |
| PUT  | `send`, `send_join` (v2), `send_leave` (v2), `invite` (v2) |

**Path** (`paths.rs`). The first Uri-Path segment is the Dendrite v1 code; dynamic
segments follow as further Uri-Path segments; query params become Uri-Query options.
The server reverses it back to the full HTTP path+query.

| Code | HTTP path |
|------|-----------|
| `z`  | `/_matrix/federation/v1/send/{txnId}` |
| `f1` | `/_matrix/federation/v1/backfill/{roomId}` |
| `f2` | `/_matrix/federation/v1/get_missing_events/{roomId}` |
| `f5` | `/_matrix/federation/v1/event/{eventId}` |
| `f6` | `/_matrix/federation/v1/make_join/{roomId}/{userId}` |
| `f8` | `/_matrix/federation/v2/send_join/{roomId}/{eventId}` |
| `fA` | `/_matrix/federation/v2/invite/{roomId}/{eventId}` |
| `fB` | `/_matrix/federation/v1/make_leave/{roomId}/{userId}` |
| `fD` | `/_matrix/federation/v2/send_leave/{roomId}/{eventId}` |

**Fallback:** a path matching no template is carried verbatim as Uri-Path segments, so
a not-yet-coded route still works (and round-trips losslessly).

**Headers.** Keep the existing allowlist (`is_forwardable`: `authorization` +
`x-matrix-*`). Each forwardable header travels as a CoAP option in the private-use
range (mirrors Dendrite's access-token/origin options); the server reconstructs the
header before calling `IngressHandler`. The body's Content-Format option is
`application/cbor`.

**HTTP status.** Federation cares about exact codes — 200 vs 403 vs 404 drive the
homeserver sender's retry-vs-give-up decision (a 4xx is dropped permanently, a 5xx is
retried). CoAP response codes are not 1:1 with HTTP, so rather than a lossy map we
carry the **exact `u16` HTTP status in a dedicated CoAP option**; the CoAP response
code itself only reflects the success/error class. `WireResponse.status` is therefore
exact, and the existing 2xx-vs-non-2xx body-decode rules in `egress`/`ingress` keep
working untouched.

## Blockwise, reliability, body caps

- **CON messages.** Requests are Confirmable; CoAP's ACK/retransmit is the per-message
  reliability layer over lossy UDP. Higher-level retries remain where they already
  live: the homeserver's outbound federation sender backoff. No retry logic is added
  in the sidecar.
- **Client blockwise** is automatic in `coap-rs`'s `UdpCoAPClient` (Block1 for a large
  request body, Block2 to reassemble a large response; configurable block size,
  default ~1024 B).
- **Server blockwise** via the lib where it suffices; the named fallback is
  `coap-lite::BlockHandler` driving assembly/segmentation on a hand-rolled UDP loop.
- **Body caps.** Reassembled bodies are bounded by `MAX_WIRE_BODY_BYTES` on **both**
  legs (the existing 64 MiB OOM guard — fatal to exceed on the embedded-on-mobile
  target). Over-cap aborts reassembly and surfaces as a transport error; we never
  buffer a partial body past the cap.

## Error handling

Transport failures — timeout, peer unreachable, over-cap, undecodable CoAP, missing
required option — surface as `WireError::Transport(..)`. This is the **existing**
contract: `egress` already maps a `WireClient` error to a retryable 502, and the
`ingress`/`CoapWireServer` path answers its 502-equivalent. The 2xx-vs-non-2xx
body-decode rules (which preserve a peer's 4xx/5xx status) live in `egress.rs` /
`ingress.rs` and are not touched here.

`CoapWireServer::serve` selects on the `CancellationToken` (same contract as
`HttpWireServer::serve`): on cancel it stops accepting, closes the UDP socket, and
returns `Ok(())`.

## Testing

- **`paths.rs` unit tests** — every code round-trips path→CoAP→path; dynamic-segment
  overlay (`{roomId}`/`{eventId}`/`{userId}`); query-param carriage; the literal-path
  fallback for an unmapped route.
- **`message.rs` unit tests** — method↔`RequestType`, forwardable-header↔option
  round-trip (and that non-allowlisted headers are dropped), exact HTTP-status
  carriage, Content-Format.
- **CoAP e2e** — a twin of `tests/e2e_lb_federation.rs` with both sidecars on
  `WireKind::Coap` over loopback UDP: two homeservers converge a real join + message.
- **Large-state blockwise e2e** — a body well over one datagram (a multi-block
  `send_join`-style payload) round-trips intact; plus an over-cap body is rejected
  rather than partially buffered.

## Open questions / verification gates for the plan

1. **`coap-rs` 0.27 server API + server-side blockwise.** docs.rs rate-limited the
   doc fetch during design; the plan's first step is to vendor the crate
   (`cargo fetch`) and read the real `Server` / `UdpCoAPClient` source to confirm:
   (a) the server handler shape and how it composes with `Arc<dyn WireHandler>` +
   `CancellationToken`; (b) whether the server drives Block1/Block2 automatically or
   needs `coap-lite::BlockHandler`. If inadequate → Approach 2 (hand-rolled UDP server)
   for the server half only; the client half stays on `coap-rs`.
2. **Private-use CoAP option numbers** for the forwarded headers and the HTTP-status
   carriage — choose numbers that don't collide with the options `coap-rs`/`coap-lite`
   already interpret (e.g. Uri-Path, Uri-Query, Content-Format, Block1/Block2).
3. **Block size vs. UDP datagram.** Default ~1024 B is safe for UDP; the serial layer
   (future) will revisit SZX. No change needed now.
