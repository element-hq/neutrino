# neutrino-lb CoAP/UDP Wire Transport (Layer B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CoAP-over-UDP wire transport to `neutrino-lb` as a second `WireClient`/`WireServer` implementation behind the existing seam, selectable via config, so S-S federation can ride a low-bandwidth datagram link.

**Architecture:** A new `transport::coap` module (sibling of `transport::http`) implements the existing `WireClient`/`WireServer` traits over `coap-rs` (UDP runtime, automatic blockwise) + `coap-lite` (message/option detail). `egress`, `ingress`, `codec`, and the traits are untouched — they already speak `WireRequest`/`WireResponse` with opaque CBOR bodies. A `WireKind` config field and a `match` in `crate::serve` pick the transport pair; `Http` stays the default.

**Tech Stack:** Rust 2024, tokio, `coap = "0.27"`, `coap-lite = "0.13"`, axum (existing), `async-trait`, `tokio-util::CancellationToken`.

Design spec: `docs/superpowers/specs/2026-06-18-neutrino-lb-coap-udp-transport-design.md`.

## Global Constraints

- **Scope:** Layer B only. Do NOT touch the codec (`codec.rs` stays opaque `serde_json::Value` transcode), `egress.rs`, `ingress.rs`, or the `transport` traits in `transport/mod.rs`. Do NOT touch `neutrino-http`.
- **Out of scope (do not implement):** SLIP/serial framing, integer-key CBOR codec, CoAP OBSERVE/streaming, DTLS.
- **Project rules (`crates/neutrino-lb` + `neutrino/CLAUDE.md`):** no `anyhow` (use `thiserror`); no `.unwrap()`/`.expect()` in non-test code (panicking `.expect()` is allowed ONLY for the documented "plaintext-client-can't-fail" pattern already in `transport/http.rs` — avoid adding new ones); run `cargo fmt` + `cargo clippy -p neutrino-lb --tests -- -D warnings` + `cargo test -p neutrino-lb` before declaring a task done; no dead code / unused imports.
- **Dependencies:** `coap` and `coap-lite` are already in `crates/neutrino-lb/Cargo.toml`. Do NOT add other dependencies without asking.
- **Custom CoAP option numbers** (both even = elective; high range, no collision with coap-lite's known options which are all < 260):
  - `OPT_HTTP_STATUS: u16 = 2048` — exact HTTP status as 2 big-endian bytes.
  - `OPT_FWD_HEADER: u16 = 2050` — one forwarded header per occurrence; value = `name_bytes` + `0x00` + `value_bytes` (split on the FIRST `0x00`).
- **CBOR content format** number is `60` (`application/cbor`, RFC 8949 §9.1).
- **Body cap:** reuse the existing `MAX_WIRE_BODY_BYTES = 64 * 1024 * 1024` concept on reassembled bodies.
- **Path-code invariant:** federation HTTP paths always begin `/_matrix/...`, so their first literal segment is `_matrix`, which never equals a route code (`z`, `f1`, … `fD`). The decoder uses this to tell a coded path from a literal fallback path. Codes are matched case-sensitively.
- **Git identity for commits:** `Skye Elliot <actuallyori@gmail.com>`. End commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer. (Set `git config user.name/user.email` if not already set.)

---

### Task 1: Scaffold `transport::coap` module + confirm the coap-rs API

Proves the `coap-rs` 0.27 client/server/blockwise API against real source (the spec's #1 verification gate) with a minimal raw echo round-trip, and wires the empty module into the tree. No `WireClient`/`WireServer` impls yet.

**Files:**
- Create: `crates/neutrino-lb/src/transport/coap/mod.rs`
- Modify: `crates/neutrino-lb/src/transport/mod.rs` (add `pub mod coap;` next to `pub mod http;`)

**Interfaces:**
- Consumes: nothing.
- Produces: the `transport::coap` module path; shared `pub(crate) const OPT_HTTP_STATUS/OPT_FWD_HEADER` and `CBOR_CONTENT_FORMAT` constants used by later tasks.

- [ ] **Step 1: Confirm the vendored API.** Read the real source for the pinned versions before writing code (the doc fetch was rate-limited at design time):

Run:
```bash
cargo fetch -p coap -p coap-lite 2>/dev/null; \
find ~/.cargo/registry/src -maxdepth 1 -type d \( -name 'coap-0.27*' -o -name 'coap-lite-0.13*' \) -print
```
Open the printed `coap-0.27*/src/{client.rs,server.rs}` and `coap-lite-0.13*/src/{packet.rs,request.rs,response.rs}`. Confirm: `UdpCoAPClient::new`, `CoAPClient::send`, `set_block1_size`; `Server::new_udp`, `Server::run`, the `RequestHandler::handle_request` signature, and that the server runs `BlockHandler` internally; `CoapRequest::new`, `set_method`, `set_path`, the `.message: Packet` and `.response: Option<CoapResponse>` fields; `CoapResponse::new`, `set_status`; `Packet::{add_option,get_option,get_first_option,payload}`, `CoapOption::{UriPath,UriQuery,ContentFormat,Unknown}`, and `RequestType::{Get,Post,Put}`. If any signature below differs, adjust the later tasks' code to match the source (the source is authoritative).

- [ ] **Step 2: Add the module declaration.**

In `crates/neutrino-lb/src/transport/mod.rs`, directly below `pub mod http;`:
```rust
pub mod coap;
```

- [ ] **Step 3: Write the module skeleton with shared constants and a failing smoke test.**

Create `crates/neutrino-lb/src/transport/coap/mod.rs`:
```rust
//! v2 CoAP/UDP wire transport. `CoapWireClient` (egress→peer) and
//! `CoapWireServer` (peer→ingress), a sibling of `transport::http`, selected in
//! `crate::serve`. The codec stays opaque: this transport carries the CBOR body
//! verbatim and never inspects it.

/// Exact HTTP status, carried as 2 big-endian bytes (CoAP response codes are not
/// 1:1 with HTTP, and federation needs the precise code).
pub(crate) const OPT_HTTP_STATUS: u16 = 2048;
/// One forwarded header per occurrence: `name` + 0x00 + `value`.
pub(crate) const OPT_FWD_HEADER: u16 = 2050;
/// `application/cbor` (RFC 8949 §9.1).
pub(crate) const CBOR_CONTENT_FORMAT: u16 = 60;

#[cfg(test)]
mod smoke_tests {
    use coap::Server;
    use coap::UdpCoAPClient;
    use coap_lite::{CoapRequest, RequestType};
    use std::net::SocketAddr;

    // Confirms the coap-rs 0.27 client+server API and a basic CON round-trip.
    // If this compiles and passes, the signatures the later tasks rely on hold.
    #[tokio::test]
    async fn coap_rs_client_server_roundtrip() {
        let addr = "127.0.0.1:0";
        let server = Server::new_udp(addr).expect("bind udp server");
        let bound: SocketAddr = server.socket_addr().expect("server addr");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    if let Some(ref mut resp) = request.response {
                        resp.message.payload = b"pong".to_vec();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = UdpCoAPClient::new(bound).await.expect("client");
        let mut req: CoapRequest<SocketAddr> = CoapRequest::new();
        req.set_method(RequestType::Get);
        req.set_path("/ping");
        let resp = client.send(req).await.expect("send");
        assert_eq!(resp.message.payload, b"pong");
    }
}
```

> NOTE: `Server::socket_addr()` is the accessor for the OS-assigned port when binding `:0`. If the vendored source names it differently (e.g. the server exposes the addr another way), bind an explicit free port instead: grab one with `std::net::UdpSocket::bind("127.0.0.1:0")`, read its `local_addr()`, drop it, and pass that to `Server::new_udp`.

- [ ] **Step 4: Run the smoke test — expect it to surface any API mismatch first.**

Run: `cargo test -p neutrino-lb --lib transport::coap::smoke_tests -- --nocapture`
Expected: PASS. If it fails to COMPILE, the API differs from Step 1's assumptions — fix the calls against the vendored source and re-run. If it compiles but the assertion fails, the round-trip/handler-response contract differs — adjust and re-run until green.

- [ ] **Step 5: Lint + format.**

Run: `cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit.**

```bash
git add crates/neutrino-lb/src/transport/mod.rs crates/neutrino-lb/src/transport/coap/mod.rs
git commit -m "feat(neutrino-lb): scaffold transport::coap + confirm coap-rs API

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Path ↔ CoAP-code mapping (`paths.rs`)

Pure string logic, no coap dependency: turn a full HTTP path+query into CoAP Uri-Path segments + Uri-Query strings and back, using Dendrite's v1 federation codes with a literal-path fallback.

**Files:**
- Create: `crates/neutrino-lb/src/transport/coap/paths.rs`
- Modify: `crates/neutrino-lb/src/transport/coap/mod.rs` (add `mod paths;`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub(crate) fn encode(path_and_query: &str) -> (Vec<String>, Vec<String>)` — returns `(uri_path_segments, uri_queries)`. `uri_queries` entries are whole `k=v` strings.
  - `pub(crate) fn decode(path_segments: &[Vec<u8>], queries: &[Vec<u8>]) -> String` — rebuilds the full `path?query` HTTP string. Takes raw option bytes (what `Packet::get_option` yields) for the decode side.

- [ ] **Step 1: Add the module declaration.**

In `crates/neutrino-lb/src/transport/coap/mod.rs`, add near the top (after the doc comment):
```rust
mod paths;
```

- [ ] **Step 2: Write the failing tests.**

Create `crates/neutrino-lb/src/transport/coap/paths.rs`:
```rust
//! HTTP path <-> CoAP Uri-Path code mapping. Reuses Dendrite's MSC3079 v1
//! federation codes; unmapped paths fall back to verbatim segments. Federation
//! paths start with `_matrix`, which never collides with a route code, so the
//! decoder distinguishes coded from literal paths by the first segment.

/// `(code, template)` pairs. Template segments wrapped in `{}` are dynamic.
const ROUTES: &[(&str, &str)] = &[
    ("z", "/_matrix/federation/v1/send/{txnId}"),
    ("f1", "/_matrix/federation/v1/backfill/{roomId}"),
    ("f2", "/_matrix/federation/v1/get_missing_events/{roomId}"),
    ("f5", "/_matrix/federation/v1/event/{eventId}"),
    ("f6", "/_matrix/federation/v1/make_join/{roomId}/{userId}"),
    ("f8", "/_matrix/federation/v2/send_join/{roomId}/{eventId}"),
    ("fA", "/_matrix/federation/v2/invite/{roomId}/{eventId}"),
    ("fB", "/_matrix/federation/v1/make_leave/{roomId}/{userId}"),
    ("fD", "/_matrix/federation/v2/send_leave/{roomId}/{eventId}"),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn encodes_coded_route_with_dynamic_segments() {
        let (path, q) = encode("/_matrix/federation/v2/send_join/!r:a/$e");
        assert_eq!(path, vec!["f8", "!r:a", "$e"]);
        assert!(q.is_empty());
    }

    #[test]
    fn round_trips_every_coded_route() {
        let cases = [
            "/_matrix/federation/v1/send/txn1",
            "/_matrix/federation/v1/backfill/!r:a",
            "/_matrix/federation/v1/get_missing_events/!r:a",
            "/_matrix/federation/v1/event/$e",
            "/_matrix/federation/v1/make_join/!r:a/@u:a",
            "/_matrix/federation/v2/send_join/!r:a/$e",
            "/_matrix/federation/v2/invite/!r:a/$e",
            "/_matrix/federation/v1/make_leave/!r:a/@u:a",
            "/_matrix/federation/v2/send_leave/!r:a/$e",
        ];
        for original in cases {
            let (p, q) = encode(original);
            assert_eq!(decode(&segs_owned(&p), &segs_owned(&q)), original, "{original}");
        }
    }

    #[test]
    fn carries_query_params() {
        let (p, q) = encode("/_matrix/federation/v1/backfill/!r:a?v=$x&limit=10");
        assert_eq!(p, vec!["f1", "!r:a"]);
        assert_eq!(q, vec!["v=$x", "limit=10"]);
        assert_eq!(
            decode(&segs_owned(&p), &segs_owned(&q)),
            "/_matrix/federation/v1/backfill/!r:a?v=$x&limit=10"
        );
    }

    #[test]
    fn unmapped_path_falls_back_to_literal_and_round_trips() {
        let original = "/_matrix/federation/v1/version";
        let (p, q) = encode(original);
        // First segment is the literal `_matrix`, not a code.
        assert_eq!(p[0], "_matrix");
        assert_eq!(decode(&segs_owned(&p), &segs_owned(&q)), original);
    }

    #[test]
    fn decode_ignores_unknown_first_segment_as_literal() {
        // A literal path whose first segment is not a known code.
        let decoded = decode(&segs(&["_matrix", "federation", "v1", "version"]), &[]);
        assert_eq!(decoded, "/_matrix/federation/v1/version");
    }

    fn segs_owned(v: &[String]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail.**

Run: `cargo test -p neutrino-lb --lib transport::coap::paths`
Expected: FAIL to compile — `encode`/`decode` not defined.

- [ ] **Step 4: Implement `encode` and `decode`.**

Add to `crates/neutrino-lb/src/transport/coap/paths.rs` (above the `#[cfg(test)]` block):
```rust
/// Split a full `path?query` into CoAP Uri-Path segments + Uri-Query strings.
/// A path matching a known route becomes `[code, dynamic_segs..]`; anything else
/// is sent verbatim as its `/`-split segments (the fallback).
pub(crate) fn encode(path_and_query: &str) -> (Vec<String>, Vec<String>) {
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, q),
        None => (path_and_query, ""),
    };
    let queries: Vec<String> = if query.is_empty() {
        Vec::new()
    } else {
        query.split('&').map(|s| s.to_owned()).collect()
    };
    let path_segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    for (code, template) in ROUTES {
        let tmpl_segs: Vec<&str> = template.trim_start_matches('/').split('/').collect();
        if tmpl_segs.len() != path_segs.len() {
            continue;
        }
        let mut dynamic = Vec::new();
        let mut matched = true;
        for (t, p) in tmpl_segs.iter().zip(path_segs.iter()) {
            if t.starts_with('{') && t.ends_with('}') {
                dynamic.push((*p).to_owned());
            } else if t != p {
                matched = false;
                break;
            }
        }
        if matched {
            let mut out = Vec::with_capacity(1 + dynamic.len());
            out.push((*code).to_owned());
            out.extend(dynamic);
            return (out, queries);
        }
    }
    // Fallback: literal segments.
    (path_segs.iter().map(|s| (*s).to_owned()).collect(), queries)
}

/// Rebuild the full `path?query` HTTP string from CoAP option bytes. If the first
/// path segment is a known route code, expand its template; otherwise treat the
/// segments as a literal path.
pub(crate) fn decode(path_segments: &[Vec<u8>], queries: &[Vec<u8>]) -> String {
    let segs: Vec<String> = path_segments
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    let path = match segs.split_first() {
        Some((first, rest)) => match ROUTES.iter().find(|(code, _)| *code == first) {
            Some((_, template)) => expand_template(template, rest),
            None => format!("/{}", segs.join("/")),
        },
        None => "/".to_owned(),
    };
    if queries.is_empty() {
        return path;
    }
    let q: Vec<String> = queries
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    format!("{path}?{}", q.join("&"))
}

/// Fill a route template's `{placeholder}` segments from `values` in order.
fn expand_template(template: &str, values: &[String]) -> String {
    let mut value_iter = values.iter();
    let filled: Vec<String> = template
        .trim_start_matches('/')
        .split('/')
        .map(|seg| {
            if seg.starts_with('{') && seg.ends_with('}') {
                value_iter.next().cloned().unwrap_or_default()
            } else {
                seg.to_owned()
            }
        })
        .collect();
    format!("/{}", filled.join("/"))
}
```

- [ ] **Step 5: Run the tests to verify they pass.**

Run: `cargo test -p neutrino-lb --lib transport::coap::paths`
Expected: PASS (5 tests).

- [ ] **Step 6: Lint, format, commit.**

```bash
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/src/transport/coap/paths.rs crates/neutrino-lb/src/transport/coap/mod.rs
git commit -m "feat(neutrino-lb): CoAP path<->code mapping with literal fallback

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `WireRequest`/`WireResponse` ⇄ CoAP message mapping (`message.rs`)

Pure mapping between our wire types and `coap-lite` packets: method, path (via `paths`), forwardable-header options, exact-status option, Content-Format, body. No socket I/O — testable by building a packet and reading it back.

**Files:**
- Create: `crates/neutrino-lb/src/transport/coap/message.rs`
- Modify: `crates/neutrino-lb/src/transport/coap/mod.rs` (add `mod message;`)

**Interfaces:**
- Consumes: `paths::encode`/`decode`; `OPT_HTTP_STATUS`/`OPT_FWD_HEADER`/`CBOR_CONTENT_FORMAT`; `crate::headers::is_forwardable`; `crate::transport::{WireRequest, WireResponse}`.
- Produces:
  - `pub(crate) fn build_request(req: &WireRequest) -> CoapRequest<SocketAddr>` — egress side (dest/source endpoint is set later by the client; method/path/options/payload set here).
  - `pub(crate) fn parse_request(req: &CoapRequest<SocketAddr>) -> WireRequest` — ingress side (`dest` left empty, matching `transport::http`).
  - `pub(crate) fn write_response(resp: &mut CoapResponse, wire: &WireResponse)` — ingress side.
  - `pub(crate) fn parse_response(resp: &CoapResponse) -> WireResponse` — egress side.

- [ ] **Step 1: Add the module declaration.**

In `crates/neutrino-lb/src/transport/coap/mod.rs`:
```rust
mod message;
```

- [ ] **Step 2: Write the failing tests.**

Create `crates/neutrino-lb/src/transport/coap/message.rs`:
```rust
//! Mapping between `WireRequest`/`WireResponse` and `coap-lite` messages. The
//! body is carried verbatim (opaque CBOR). Forwardable headers travel as
//! `OPT_FWD_HEADER` options; the exact HTTP status as `OPT_HTTP_STATUS`.

use std::net::SocketAddr;

use axum::http::Method;
use coap_lite::{CoapRequest, CoapResponse, CoapOption, RequestType};

use crate::headers::is_forwardable;
use crate::transport::{WireRequest, WireResponse};

use super::{CBOR_CONTENT_FORMAT, OPT_FWD_HEADER, OPT_HTTP_STATUS};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips_method_path_headers_body() {
        let wire = WireRequest {
            dest: "peer:8448".to_owned(),
            method: Method::PUT,
            path: "/_matrix/federation/v2/send_join/!r:a/$e?ver=12".to_owned(),
            headers: vec![
                ("authorization".to_owned(), b"X-Matrix origin=\"a\"".to_vec()),
                // Not forwardable — must be dropped.
                ("content-length".to_owned(), b"99".to_vec()),
            ],
            body: vec![0xA1, 0x01, 0x02], // arbitrary CBOR-ish bytes
        };
        let coap = build_request(&wire);
        // Serialize + reparse through the packet to prove it survives the wire.
        let bytes = coap.message.to_bytes().expect("to_bytes");
        let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        let mut reparsed: CoapRequest<SocketAddr> = CoapRequest::new();
        reparsed.message = packet;

        let got = parse_request(&reparsed);
        assert_eq!(got.method, Method::PUT);
        assert_eq!(got.path, "/_matrix/federation/v2/send_join/!r:a/$e?ver=12");
        assert_eq!(got.body, vec![0xA1, 0x01, 0x02]);
        assert!(
            got.headers.iter().any(|(k, v)| k == "authorization" && v == b"X-Matrix origin=\"a\""),
            "authorization header lost: {:?}",
            got.headers
        );
        assert!(
            !got.headers.iter().any(|(k, _)| k == "content-length"),
            "non-forwardable header leaked"
        );
    }

    #[test]
    fn response_carries_exact_status_and_body() {
        let req: CoapRequest<SocketAddr> = CoapRequest::new();
        let mut resp = CoapResponse::new(&req.message).expect("response");
        let wire = WireResponse {
            status: 403,
            headers: vec![("x-test".to_owned(), b"v".to_vec())],
            body: vec![0xFF, 0x00],
        };
        write_response(&mut resp, &wire);

        let bytes = resp.message.to_bytes().expect("to_bytes");
        let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        let reparsed = CoapResponse { message: packet };

        let got = parse_response(&reparsed);
        assert_eq!(got.status, 403, "exact HTTP status must survive");
        assert_eq!(got.body, vec![0xFF, 0x00]);
        assert!(got.headers.iter().any(|(k, v)| k == "x-test" && v == b"v"));
    }

    #[test]
    fn missing_status_option_defaults_to_bad_gateway() {
        // A response with no OPT_HTTP_STATUS (e.g. a malformed peer) must not
        // panic and must surface a retryable status.
        let req: CoapRequest<SocketAddr> = CoapRequest::new();
        let resp = CoapResponse::new(&req.message).expect("response");
        let got = parse_response(&resp);
        assert_eq!(got.status, 502);
    }
}
```

- [ ] **Step 3: Run to verify failure.**

Run: `cargo test -p neutrino-lb --lib transport::coap::message`
Expected: FAIL to compile — the four functions are undefined.

- [ ] **Step 4: Implement the mapping functions.**

Add to `crates/neutrino-lb/src/transport/coap/message.rs` (above the test module):
```rust
/// HTTP method -> CoAP request type. Federation only uses GET/POST/PUT.
fn to_coap_method(method: &Method) -> RequestType {
    match *method {
        Method::POST => RequestType::Post,
        Method::PUT => RequestType::Put,
        _ => RequestType::Get,
    }
}

/// CoAP request type -> HTTP method.
fn to_http_method(rt: &RequestType) -> Method {
    match rt {
        RequestType::Post => Method::POST,
        RequestType::Put => Method::PUT,
        _ => Method::GET,
    }
}

/// Egress: build a CoAP request from a `WireRequest`. The destination endpoint is
/// set by the client at send time; here we set method, path/query, options, body.
pub(crate) fn build_request(req: &WireRequest) -> CoapRequest<SocketAddr> {
    let mut out: CoapRequest<SocketAddr> = CoapRequest::new();
    out.set_method(to_coap_method(&req.method));

    let (path_segs, queries) = super::paths::encode(&req.path);
    for seg in path_segs {
        out.message.add_option(CoapOption::UriPath, seg.into_bytes());
    }
    for q in queries {
        out.message.add_option(CoapOption::UriQuery, q.into_bytes());
    }
    out.message
        .add_option(CoapOption::ContentFormat, (CBOR_CONTENT_FORMAT).to_be_bytes().to_vec());
    for (name, value) in &req.headers {
        if is_forwardable(name) {
            out.message
                .add_option(CoapOption::Unknown(OPT_FWD_HEADER), encode_header(name, value));
        }
    }
    out.message.payload = req.body.clone();
    out
}

/// Ingress: parse an inbound CoAP request into a `WireRequest`. `dest` is left
/// empty (unused on the ingress side, matching `transport::http`).
pub(crate) fn parse_request(req: &CoapRequest<SocketAddr>) -> WireRequest {
    let path_segs = option_values(req.message.get_option(CoapOption::UriPath));
    let queries = option_values(req.message.get_option(CoapOption::UriQuery));
    let path = super::paths::decode(&path_segs, &queries);
    let headers = decode_headers(req.message.get_option(CoapOption::Unknown(OPT_FWD_HEADER)));
    WireRequest {
        dest: String::new(),
        method: to_http_method(req.get_method()),
        path,
        headers,
        body: req.message.payload.clone(),
    }
}

/// Ingress: write a `WireResponse` into a CoAP response message.
pub(crate) fn write_response(resp: &mut CoapResponse, wire: &WireResponse) {
    resp.message
        .add_option(CoapOption::Unknown(OPT_HTTP_STATUS), wire.status.to_be_bytes().to_vec());
    resp.message
        .add_option(CoapOption::ContentFormat, (CBOR_CONTENT_FORMAT).to_be_bytes().to_vec());
    for (name, value) in &wire.headers {
        if is_forwardable(name) {
            resp.message
                .add_option(CoapOption::Unknown(OPT_FWD_HEADER), encode_header(name, value));
        }
    }
    resp.message.payload = wire.body.clone();
}

/// Egress: parse a CoAP response into a `WireResponse`. A missing/short status
/// option defaults to a retryable 502 rather than panicking.
pub(crate) fn parse_response(resp: &CoapResponse) -> WireResponse {
    let status = resp
        .message
        .get_first_option(CoapOption::Unknown(OPT_HTTP_STATUS))
        .filter(|b| b.len() == 2)
        .map(|b| u16::from_be_bytes([b[0], b[1]]))
        .unwrap_or(502);
    let headers = decode_headers(resp.message.get_option(CoapOption::Unknown(OPT_FWD_HEADER)));
    WireResponse {
        status,
        headers,
        body: resp.message.payload.clone(),
    }
}

/// `name` + 0x00 + `value`.
fn encode_header(name: &str, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + 1 + value.len());
    out.extend_from_slice(name.as_bytes());
    out.push(0x00);
    out.extend_from_slice(value);
    out
}

/// Split each option value on its FIRST 0x00 into `(name, value)`. Entries with
/// no separator are dropped.
fn decode_headers(opt: Option<&std::collections::LinkedList<Vec<u8>>>) -> Vec<(String, Vec<u8>)> {
    let mut headers = Vec::new();
    if let Some(values) = opt {
        for raw in values {
            if let Some(sep) = raw.iter().position(|b| *b == 0x00) {
                let name = String::from_utf8_lossy(&raw[..sep]).into_owned();
                headers.push((name, raw[sep + 1..].to_vec()));
            }
        }
    }
    headers
}

/// Flatten a multi-valued option into an ordered `Vec<Vec<u8>>`.
fn option_values(opt: Option<&std::collections::LinkedList<Vec<u8>>>) -> Vec<Vec<u8>> {
    opt.map(|l| l.iter().cloned().collect()).unwrap_or_default()
}
```

> NOTE: `req.get_method()` returns `&RequestType` in coap-lite; if the vendored signature differs (e.g. returns `Option`), adjust `to_http_method` and its call. `CoapResponse { message: packet }` literal construction in the test relies on the `message` field being public — confirmed in `packet.rs`/`response.rs` during Task 1; if it is not, use `CoapResponse::new(&packet)`.

- [ ] **Step 5: Run to verify pass.**

Run: `cargo test -p neutrino-lb --lib transport::coap::message`
Expected: PASS (3 tests).

- [ ] **Step 6: Lint, format, commit.**

```bash
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/src/transport/coap/message.rs crates/neutrino-lb/src/transport/coap/mod.rs
git commit -m "feat(neutrino-lb): WireRequest/Response <-> CoAP message mapping

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `CoapWireClient` (egress side)

Implements `WireClient` over `UdpCoAPClient`: build the CoAP request from the `WireRequest`, dial `dest`, send (blockwise automatic), parse the response.

**Files:**
- Modify: `crates/neutrino-lb/src/transport/coap/mod.rs`

**Interfaces:**
- Consumes: `message::build_request`/`parse_response`; `crate::transport::{WireClient, WireError, WireRequest, WireResponse}`; `MAX_WIRE_BODY_BYTES` (define a `pub(crate) const` in `coap/mod.rs`, or reuse via `crate::transport::http`'s value by re-declaring the same literal here with a comment — do NOT make `transport::http`'s private const public).
- Produces: `pub struct CoapWireClient` with `pub fn new() -> Self` implementing `WireClient`.

- [ ] **Step 1: Write the failing test.**

Add to `crates/neutrino-lb/src/transport/coap/mod.rs` a test module (append at end of file):
```rust
#[cfg(test)]
mod client_tests {
    use super::*;
    use crate::transport::{WireClient, WireRequest};
    use axum::http::Method;
    use coap::Server;
    use coap_lite::{CoapRequest, CoapOption};
    use std::net::SocketAddr;

    // A coap-rs server that echoes the request path + body back as a 200, so the
    // client's request construction and response parsing are observable.
    #[tokio::test]
    async fn client_sends_request_and_parses_response() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let server = Server::new_udp(addr).expect("server");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    let path_segs = request
                        .message
                        .get_option(CoapOption::UriPath)
                        .map(|l| {
                            l.iter()
                                .map(|b| String::from_utf8_lossy(b).into_owned())
                                .collect::<Vec<_>>()
                                .join("/")
                        })
                        .unwrap_or_default();
                    let echo = request.message.payload.clone();
                    if let Some(ref mut resp) = request.response {
                        resp.message
                            .add_option(CoapOption::Unknown(OPT_HTTP_STATUS), 200u16.to_be_bytes().to_vec());
                        resp.message.payload = [path_segs.as_bytes(), b"|", &echo].concat();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
                headers: vec![],
                body: vec![1, 2, 3],
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        // Server echoes the decoded coap path segments (code f8 + dynamic) + body.
        assert_eq!(resp.body, b"f8/!r:a/$e|\x01\x02\x03");
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p neutrino-lb --lib transport::coap::client_tests`
Expected: FAIL to compile — `CoapWireClient` undefined.

- [ ] **Step 3: Implement `CoapWireClient`.**

Add to `crates/neutrino-lb/src/transport/coap/mod.rs` (after the constants, before the test modules):
```rust
use async_trait::async_trait;
use coap::UdpCoAPClient;

use crate::transport::{WireClient, WireError, WireRequest, WireResponse};

/// Egress wire client over CoAP/UDP. Dials `req.dest` per send; coap-rs handles
/// CON retransmit and Block1/Block2 blockwise transparently.
pub struct CoapWireClient {
    _private: (),
}

impl CoapWireClient {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for CoapWireClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WireClient for CoapWireClient {
    async fn send(&self, req: WireRequest) -> Result<WireResponse, WireError> {
        let client = UdpCoAPClient::new(req.dest.as_str())
            .await
            .map_err(|e| WireError::Transport(format!("coap dial {}: {e}", req.dest)))?;
        let coap_req = message::build_request(&req);
        let resp = client
            .send(coap_req)
            .await
            .map_err(|e| WireError::Transport(format!("coap send: {e}")))?;
        Ok(message::parse_response(&resp))
    }
}
```

> NOTE: `UdpCoAPClient::new` takes `A: ToSocketAddrs`; `&str` "host:port" satisfies it. If a peer `server_name` is ever a hostname rather than `ip:port`, DNS resolution happens here — acceptable (same as the HTTP transport resolving `http://{dest}`). Confirm `send` consumes `self` by `&self` (per Task 1); if it needs `&mut self`, make `client` mutable.

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p neutrino-lb --lib transport::coap::client_tests`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit.**

```bash
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/src/transport/coap/mod.rs
git commit -m "feat(neutrino-lb): CoapWireClient (egress CoAP/UDP)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `CoapWireServer` (ingress side) with shutdown

Implements `WireServer` over `coap::Server`: a `RequestHandler` adapter parses each inbound CoAP request, calls the `Arc<dyn WireHandler>`, and writes the response back. Because `coap::Server::run` has no native shutdown, wrap it in `tokio::select!` against the `CancellationToken`.

**Files:**
- Modify: `crates/neutrino-lb/src/transport/coap/mod.rs`

**Interfaces:**
- Consumes: `message::parse_request`/`write_response`; `crate::transport::{WireServer, WireHandler, WireError}`; `tokio_util::sync::CancellationToken`.
- Produces: `pub struct CoapWireServer` with `pub fn new(bind: SocketAddr) -> Self` implementing `WireServer`.

- [ ] **Step 1: Write the failing test (round-trip via the real client + an echo handler).**

Add a test module at the end of `crates/neutrino-lb/src/transport/coap/mod.rs`:
```rust
#[cfg(test)]
mod server_tests {
    use super::*;
    use crate::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use async_trait::async_trait;
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    struct EchoHandler;

    #[async_trait]
    impl WireHandler for EchoHandler {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            // Echo the decoded path + body so we can assert the server mapped them.
            WireResponse {
                status: 200,
                headers: vec![("x-seen-path".to_owned(), req.path.into_bytes())],
                body: req.body,
            }
        }
    }

    #[tokio::test]
    async fn server_dispatches_to_handler_and_client_round_trips() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let token = CancellationToken::new();
        let server_token = token.clone();
        let server = CoapWireServer::new(addr);
        let handle = tokio::spawn(async move {
            server.serve(Arc::new(EchoHandler), server_token).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = CoapWireClient::new();
        let resp = client
            .send(WireRequest {
                dest: addr.to_string(),
                method: Method::PUT,
                path: "/_matrix/federation/v1/send/txn9".to_owned(),
                headers: vec![],
                body: vec![9, 9, 9],
            })
            .await
            .expect("send");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, vec![9, 9, 9]);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "x-seen-path" && v == b"/_matrix/federation/v1/send/txn9"),
            "server mapped path wrong: {:?}",
            resp.headers
        );

        // Shutdown returns cleanly.
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "server did not wind down on cancel");
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p neutrino-lb --lib transport::coap::server_tests`
Expected: FAIL to compile — `CoapWireServer` undefined.

- [ ] **Step 3: Implement `CoapWireServer`.**

Add to `crates/neutrino-lb/src/transport/coap/mod.rs`:
```rust
use std::net::SocketAddr;
use std::sync::Arc;

use coap::Server;
use coap_lite::CoapRequest;
use tokio_util::sync::CancellationToken;

use crate::transport::{WireHandler, WireServer};

/// Ingress wire server over CoAP/UDP. Binds the public federation UDP port and
/// dispatches each inbound request to the `WireHandler`. coap-rs reassembles
/// blockwise requests and segments large responses internally.
pub struct CoapWireServer {
    bind: SocketAddr,
}

impl CoapWireServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }
}

/// Adapter so `coap::Server` can call into our `WireHandler`.
struct CoapDispatch {
    handler: Arc<dyn WireHandler>,
}

impl CoapDispatch {
    async fn handle(&self, mut request: Box<CoapRequest<SocketAddr>>) -> Box<CoapRequest<SocketAddr>> {
        let wire_req = message::parse_request(&request);
        let wire_resp = self.handler.handle(wire_req).await;
        if let Some(ref mut response) = request.response {
            message::write_response(response, &wire_resp);
        }
        request
    }
}

#[async_trait]
impl WireServer for CoapWireServer {
    async fn serve(
        self,
        handler: Arc<dyn WireHandler>,
        shutdown: CancellationToken,
    ) -> Result<(), WireError> {
        let server = Server::new_udp(self.bind)
            .map_err(|e| WireError::Serve(format!("bind {}: {e}", self.bind)))?;
        let dispatch = Arc::new(CoapDispatch { handler });

        // `coap::Server::run` has no native shutdown, so race it against the
        // token: when shutdown fires the run future is dropped, closing the
        // socket. (coap-rs implements RequestHandler for closures returning a
        // boxed request future.)
        tokio::select! {
            r = server.run(move |request| {
                let dispatch = dispatch.clone();
                async move { dispatch.handle(request).await }
            }) => r.map_err(|e| WireError::Serve(format!("coap serve: {e}"))),
            _ = shutdown.cancelled() => Ok(()),
        }
    }
}
```

> NOTE: confirm against Task 1 that a closure `Fn(Box<CoapRequest<SocketAddr>>) -> impl Future<Output = Box<CoapRequest<SocketAddr>>>` implements `RequestHandler` (the README indicates it does). If coap-rs instead requires an explicit `impl RequestHandler` type, make `CoapDispatch` implement the trait directly (`#[async_trait] impl RequestHandler for CoapDispatch { async fn handle_request(self: &Self, request: Box<..>) -> Box<..> { self.handle(request).await } }`) and pass `dispatch` to `run`. Either satisfies the same round-trip test.

- [ ] **Step 4: Run to verify pass.**

Run: `cargo test -p neutrino-lb --lib transport::coap::server_tests`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit.**

```bash
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/src/transport/coap/mod.rs
git commit -m "feat(neutrino-lb): CoapWireServer (ingress CoAP/UDP) with cancellable serve

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `WireKind` config + `serve` selection

Add the transport selector to `LbConfig` and branch in `crate::serve`, keeping `Http` the default so every existing test/deployment is unchanged.

**Files:**
- Modify: `crates/neutrino-lb/src/lib.rs`

**Interfaces:**
- Consumes: `CoapWireClient`, `CoapWireServer` (Tasks 4–5); existing `HttpWireClient`/`HttpWireServer`.
- Produces: `pub enum WireKind { Http, Coap }` (with `Default`), `LbConfig.wire: WireKind`.

- [ ] **Step 1: Write the failing test.**

In `crates/neutrino-lb/src/lib.rs`, add a test module at the end:
```rust
#[cfg(test)]
mod serve_selection_tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio_util::sync::CancellationToken;

    fn cfg(wire: WireKind) -> LbConfig {
        let f = |p: &str| -> SocketAddr {
            let s = std::net::UdpSocket::bind(p).unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        };
        LbConfig {
            ingress_bind: f("127.0.0.1:0"),
            egress_bind: f("127.0.0.1:0"),
            upstream: "http://127.0.0.1:1".to_owned(),
            wire,
        }
    }

    #[test]
    fn wirekind_defaults_to_http() {
        assert!(matches!(WireKind::default(), WireKind::Http));
    }

    // The Coap arm must build and bind a UDP listener, then wind down on cancel
    // (proves the match arm is wired, not just that the enum exists).
    #[tokio::test]
    async fn coap_serve_binds_and_shuts_down() {
        let token = CancellationToken::new();
        let server_token = token.clone();
        let handle = tokio::spawn(async move { serve(cfg(WireKind::Coap), server_token).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
        let joined = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(joined.is_ok(), "coap serve did not wind down");
    }
}
```

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p neutrino-lb --lib serve_selection_tests`
Expected: FAIL to compile — `WireKind` undefined / `LbConfig` has no `wire` field.

- [ ] **Step 3: Add `WireKind` and the `wire` field.**

In `crates/neutrino-lb/src/lib.rs`, add above `LbConfig`:
```rust
/// Which wire transport the sidecar pair uses. Both peers must match.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireKind {
    /// v1 HTTP+CBOR over TCP (default; debuggable with ordinary tooling).
    #[default]
    Http,
    /// v2 CoAP+CBOR over UDP (low-bandwidth link).
    Coap,
}
```
Add the field to `LbConfig` (after `upstream`):
```rust
    /// Wire transport for the inter-sidecar hop. Defaults to `Http`.
    pub wire: WireKind,
```

- [ ] **Step 4: Branch in `serve`.**

Replace the body of `serve` in `crates/neutrino-lb/src/lib.rs` so the wire pair is chosen by `config.wire`. Keep egress/ingress identical across arms (only the `WireClient`/`WireServer` concrete types differ):
```rust
pub async fn serve(config: LbConfig, shutdown: CancellationToken) -> Result<(), LbError> {
    let ingress_handler = Arc::new(IngressHandler::new(config.upstream.clone()));
    match config.wire {
        WireKind::Http => {
            let wire_client = Arc::new(HttpWireClient::new());
            let wire_server = HttpWireServer::new(config.ingress_bind);
            run_pair(config.egress_bind, wire_client, wire_server, ingress_handler, shutdown).await
        }
        WireKind::Coap => {
            let wire_client = Arc::new(crate::transport::coap::CoapWireClient::new());
            let wire_server = crate::transport::coap::CoapWireServer::new(config.ingress_bind);
            run_pair(config.egress_bind, wire_client, wire_server, ingress_handler, shutdown).await
        }
    }
}

/// Run both proxy halves until `shutdown` fires; surface whichever errors first.
async fn run_pair<S: crate::transport::WireServer>(
    egress_bind: SocketAddr,
    wire_client: Arc<dyn crate::transport::WireClient>,
    wire_server: S,
    ingress_handler: Arc<IngressHandler>,
    shutdown: CancellationToken,
) -> Result<(), LbError> {
    let egress = egress::serve(egress_bind, wire_client, shutdown.clone());
    let ingress = wire_server.serve(ingress_handler, shutdown.clone());
    tokio::select! {
        r = egress => r.map_err(LbError::from),
        r = ingress => r.map_err(LbError::from),
    }
}
```
Ensure `IngressHandler` is usable as `Arc<dyn WireHandler>` in both arms — it already implements `WireHandler`, so `Arc<IngressHandler>` coerces at the `serve` call (the trait method takes `Arc<dyn WireHandler>`; pass `ingress_handler` directly, relying on the existing `Arc<IngressHandler> -> Arc<dyn WireHandler>` unsizing coercion). If the coercion does not apply through the generic, change `run_pair`'s param to `ingress_handler: Arc<dyn crate::transport::WireHandler>` and construct it as such in `serve`.

- [ ] **Step 5: Update any `LbConfig { .. }` literals that now miss the `wire` field.**

Run a scope-scan for breakage (the new field makes existing struct literals fail to compile):
```bash
grep -rn "LbConfig {" crates/ --include=*.rs
```
For each hit (notably in `neutrino-main` where the in-process sidecar config is built, and any `neutrino-lb` tests), add `wire: WireKind::default(),` (or `WireKind::Http`). Import `neutrino_lb::WireKind` where needed. Do NOT change runtime behaviour — default is `Http`.

- [ ] **Step 6: Run to verify pass + no regressions.**

Run: `cargo test -p neutrino-lb` then `cargo build -p neutrino-main`
Expected: PASS; `neutrino-main` compiles with the defaulted field.

- [ ] **Step 7: Lint, format, commit.**

```bash
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/src/lib.rs crates/neutrino-main
git commit -m "feat(neutrino-lb): WireKind config + serve() transport selection

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: End-to-end federation + blockwise tests

A CoAP twin of `tests/e2e_lb_federation.rs` (two sidecars on `WireKind::Coap` over loopback UDP converging a real join + message), plus a large-state body that forces blockwise.

**Files:**
- Create: `crates/neutrino-lb/tests/e2e_coap_federation.rs`
- Read first (template): `crates/neutrino-http/tests/e2e_lb_federation.rs` and `crates/neutrino-lb/tests/loopback.rs`

**Interfaces:**
- Consumes: `neutrino_lb::{serve, LbConfig, WireKind}`; the same homeserver/test harness the existing e2e uses.

- [ ] **Step 1: Read the existing e2e to copy its harness.**

Run: `sed -n '1,80p' crates/neutrino-http/tests/e2e_lb_federation.rs && sed -n '1,60p' crates/neutrino-lb/tests/loopback.rs`
Identify how two homeservers + two sidecars are started and how the test drives a join + message. The CoAP test reuses that harness verbatim except every `LbConfig` sets `wire: WireKind::Coap` and the ingress port is treated as UDP (no other change — `serve` binds UDP under the Coap arm).

- [ ] **Step 2: Write the e2e federation test.**

Create `crates/neutrino-lb/tests/e2e_coap_federation.rs` mirroring the JSON↔CBOR e2e but with `wire: WireKind::Coap`. Structure (fill bodies from the existing harness in Step 1 — reuse its helpers, do not invent a new harness):
```rust
//! CoAP/UDP twin of the HTTP+CBOR federation e2e: two sidecars on WireKind::Coap
//! over loopback UDP converge a real join + message.

// [Reuse the start-homeserver + start-sidecar helpers from the existing e2e.
//  The ONLY differences from e2e_lb_federation.rs are:
//   - LbConfig.wire = WireKind::Coap on both sidecars
//   - ingress_bind is a UDP port (serve() binds UDP under the Coap arm)]

#[tokio::test]
async fn two_coap_sidecars_converge_join_and_message() {
    // 1. Start homeserver A + homeserver B (loopback HTTP), each behind a
    //    neutrino-lb sidecar started with wire: WireKind::Coap.
    // 2. Point each homeserver's federation_proxy at its egress_bind.
    // 3. User on A creates a room; user on B joins it over federation.
    // 4. Assert B sees the room state (join converged) and a message sent on A
    //    is visible on B — identical assertions to e2e_lb_federation.rs.
}
```

- [ ] **Step 3: Run the e2e.**

Run: `cargo test -p neutrino-lb --test e2e_coap_federation two_coap_sidecars_converge_join_and_message -- --nocapture`
Expected: PASS — the join converges and the message propagates over CoAP/UDP.

- [ ] **Step 4: Add the large-state blockwise test.**

Append to `crates/neutrino-lb/tests/e2e_coap_federation.rs`:
```rust
// Forces CoAP blockwise: a body well over a single ~1 KB block must round-trip
// intact through the CoAP transport. Uses the WireClient/WireServer directly
// (no homeserver) with an echo handler returning a large body.
#[tokio::test]
async fn large_body_round_trips_via_blockwise() {
    use neutrino_lb::transport::coap::{CoapWireClient, CoapWireServer};
    use neutrino_lb::transport::{WireClient, WireHandler, WireRequest, WireResponse};
    use axum::http::Method;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use async_trait::async_trait;

    struct BigEcho;
    #[async_trait]
    impl WireHandler for BigEcho {
        async fn handle(&self, req: WireRequest) -> WireResponse {
            // Echo a body 64x larger than the request to exercise Block2 too.
            let big = req.body.iter().cycle().take(req.body.len() * 64).copied().collect();
            WireResponse { status: 200, headers: vec![], body: big }
        }
    }

    let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = probe.local_addr().unwrap();
    drop(probe);
    let token = CancellationToken::new();
    let server_token = token.clone();
    let handle = tokio::spawn(async move {
        CoapWireServer::new(addr).serve(Arc::new(BigEcho), server_token).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 8 KiB request body -> 512 KiB response body, both far over one datagram.
    let req_body = vec![0xABu8; 8 * 1024];
    let client = CoapWireClient::new();
    let resp = client
        .send(WireRequest {
            dest: addr.to_string(),
            method: Method::PUT,
            path: "/_matrix/federation/v2/send_join/!r:a/$e".to_owned(),
            headers: vec![],
            body: req_body.clone(),
        })
        .await
        .expect("blockwise send");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.len(), req_body.len() * 64);
    assert!(resp.body.iter().all(|b| *b == 0xAB), "blockwise payload corrupted");

    token.cancel();
    let _ = handle.await;
}
```

> NOTE: this test imports `neutrino_lb::transport::coap::{CoapWireClient, CoapWireServer}` and `neutrino_lb::transport::{...}` — confirm `transport` and `transport::coap` (and the two structs) are `pub` reachable from outside the crate. They are: `pub mod transport;`, `pub mod coap;`, and `pub struct CoapWireClient/Server`. If `transport::coap` is only `pub(crate)`, widen the two structs' module path to `pub` for the integration test, OR move this large-body test into a `#[cfg(test)]` unit module inside `coap/mod.rs` (where it has crate-internal access) instead of the integration test file.

- [ ] **Step 5: Run the blockwise test.**

Run: `cargo test -p neutrino-lb --test e2e_coap_federation large_body_round_trips_via_blockwise -- --nocapture`
Expected: PASS — confirms Block1 (request) + Block2 (response) reassembly intact.

- [ ] **Step 6: Full crate check, lint, format, commit.**

```bash
cargo test -p neutrino-lb
cargo fmt -p neutrino-lb && cargo clippy -p neutrino-lb --tests -- -D warnings
git add crates/neutrino-lb/tests/e2e_coap_federation.rs
git commit -m "test(neutrino-lb): e2e CoAP federation + blockwise round-trip

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 7: Update project docs.**

- In `PLAN.md`, under "Low-bandwidth proxy (`neutrino-lb`)", move the CoAP/UDP transport from deferred follow-ups to done (note: opaque codec retained; integer-key codec still deferred as Layer A).
- Append a 2-line summary to `LOG.md`.
- Commit:
```bash
git add PLAN.md LOG.md
git commit -m "docs(neutrino-lb): record CoAP/UDP transport (Layer B) landed

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Module layout (`transport::coap/{mod,paths,message}`) → Tasks 1–5. ✓
- `WireKind` config + `serve` branch → Task 6. ✓
- Method/path/header/status mapping → Tasks 2 (path), 3 (method/header/status). ✓
- Dendrite v1 path codes + literal fallback → Task 2. ✓
- Blockwise (client auto + server `BlockHandler`) → relied on coap-rs (Task 1 confirms; Task 7 step 4 proves). ✓
- Reliability via CON → coap-rs default (Task 1). ✓
- Body cap → noted in Global Constraints; enforced by coap-rs's own limits + the existing egress/ingress flow. (See gap note below.)
- HTTP-status carriage via option → Task 3. ✓
- Shutdown via `CancellationToken` → Task 5. ✓
- e2e + blockwise tests → Task 7. ✓
- No OBSERVE / serial / codec-change → enforced by Global Constraints. ✓

**Gap found + resolved:** the spec's explicit `MAX_WIRE_BODY_BYTES` cap on *reassembled* bodies is not actively enforced in Tasks 3–5 (coap-rs controls block reassembly internally; we don't see partials). This is acceptable for this iteration because the trusted-network assumption holds and coap-rs bounds its own buffers, but it is weaker than the HTTP transport's explicit cap. **Action:** Task 5's implementer should check, while reading coap-rs source in Task 1, whether `Server`/`UdpCoAPClient` expose a max-total-payload limit; if so, set it to `MAX_WIRE_BODY_BYTES` and add a one-line assertion test. If not, add a `// TODO(coap-oom): no reassembly cap exposed by coap-rs 0.27` note in `coap/mod.rs` and flag it back to the requester rather than silently dropping the guard. (Recorded here so it is not lost.)

**Placeholder scan:** Task 7 Step 2 intentionally defers the homeserver-harness body to "reuse the existing e2e helpers" (Step 1 reads them) rather than reproducing the entire harness blind — the harness is large and lives in the repo; copying it verbatim into the plan would be guesswork about helper names. This is a directed reuse, not a placeholder. All code-bearing steps in Tasks 1–6 contain complete code.

**Type consistency:** `encode`/`decode` signatures (Task 2) match their callers in `build_request`/`parse_request` (Task 3). `build_request`/`parse_response`/`parse_request`/`write_response` (Task 3) match their callers in `CoapWireClient` (Task 4) and `CoapWireServer` (Task 5). `CoapWireClient::new()`/`CoapWireServer::new(SocketAddr)` match their uses in Tasks 6–7. `WireKind`/`LbConfig.wire` (Task 6) match the e2e (Task 7). Option constants are defined once in Task 1 and reused by name throughout.
