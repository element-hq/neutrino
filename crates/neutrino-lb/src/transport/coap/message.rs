//! Mapping between `WireRequest`/`WireResponse` and `coap-lite` messages. The
//! body is carried verbatim (opaque CBOR). Forwardable headers travel as
//! `OPT_FWD_HEADER` options; the exact HTTP status as `OPT_HTTP_STATUS`; the
//! canonical X-Matrix credential is compacted to `origin,destination` under
//! `OPT_X_MATRIX_AUTH` and re-expanded to a full `authorization` header on
//! ingress.

use std::collections::LinkedList;
use std::net::SocketAddr;

use axum::http::Method;
use coap_lite::{CoapOption, CoapRequest, CoapResponse, RequestType};

use crate::headers::is_forwardable;
use crate::transport::{WireRequest, WireResponse};

use super::{CBOR_CONTENT_FORMAT, OPT_FWD_HEADER, OPT_HTTP_STATUS, OPT_X_MATRIX_AUTH};

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

/// CoAP content-format option value, minimally encoded (RFC 7252 §3.2): the
/// shortest big-endian form, so anything under 256 — including the CBOR default
/// — costs a single byte in every block.
fn content_format_bytes(format: u16) -> Vec<u8> {
    match format {
        0 => Vec::new(),
        f if f < 256 => vec![f as u8],
        f => f.to_be_bytes().to_vec(),
    }
}

/// Read back [`content_format_bytes`]. An absent or over-long option means the
/// peer said nothing usable, which is the CBOR default.
fn parse_content_format(raw: Option<&Vec<u8>>) -> u16 {
    match raw.map(Vec::as_slice) {
        Some([]) => 0,
        Some(&[b]) => u16::from(b),
        Some(&[hi, lo]) => u16::from_be_bytes([hi, lo]),
        _ => CBOR_CONTENT_FORMAT,
    }
}

/// Egress: build a CoAP request from a `WireRequest`. The destination endpoint is
/// set by the client at send time; here we set method, path/query, options, body.
pub(crate) fn build_request(req: &WireRequest) -> CoapRequest<SocketAddr> {
    let mut out: CoapRequest<SocketAddr> = CoapRequest::new();
    out.set_method(to_coap_method(&req.method));

    let (path_segs, queries) = super::paths::encode(&req.path);
    for seg in path_segs {
        out.message
            .add_option(CoapOption::UriPath, seg.into_bytes());
    }
    for q in queries {
        out.message.add_option(CoapOption::UriQuery, q.into_bytes());
    }
    out.message.add_option(
        CoapOption::ContentFormat,
        content_format_bytes(req.content_format),
    );
    for (name, value) in &req.headers {
        if !is_forwardable(name) {
            continue;
        }
        // The X-Matrix credential is the heavy always-present header, so its
        // canonical form travels as bare `origin,destination` under a dedicated
        // option instead of the full `name` + scheme/param/quote framing.
        if name.eq_ignore_ascii_case("authorization")
            && let Some(compact) = compact_xmatrix(value)
        {
            out.message
                .add_option(CoapOption::Unknown(OPT_X_MATRIX_AUTH), compact);
            continue;
        }
        out.message.add_option(
            CoapOption::Unknown(OPT_FWD_HEADER),
            encode_header(name, value),
        );
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
    let mut headers = decode_headers(req.message.get_option(CoapOption::Unknown(OPT_FWD_HEADER)));
    if let Some(auth) = req
        .message
        .get_first_option(CoapOption::Unknown(OPT_X_MATRIX_AUTH))
        .and_then(|raw| expand_xmatrix(raw))
    {
        // The compact credential is authoritative: drop any verbatim
        // `authorization` a peer also sent, so a second identity can't ride
        // past whichever gate reads the other copy (the ingress origin↔source
        // binding reads the first match — see `Hub::origin_binding_violation`).
        headers.retain(|(name, _)| !name.eq_ignore_ascii_case("authorization"));
        headers.push(auth);
    }
    WireRequest {
        dest: String::new(),
        method: to_http_method(req.get_method()),
        path,
        headers,
        body: req.message.payload.clone(),
        content_format: parse_content_format(
            req.message.get_first_option(CoapOption::ContentFormat),
        ),
    }
}

/// Ingress: write a `WireResponse` into a CoAP response message.
pub(crate) fn write_response(resp: &mut CoapResponse, wire: &WireResponse) {
    resp.message.add_option(
        CoapOption::Unknown(OPT_HTTP_STATUS),
        wire.status.to_be_bytes().to_vec(),
    );
    resp.message.add_option(
        CoapOption::ContentFormat,
        content_format_bytes(wire.content_format),
    );
    for (name, value) in &wire.headers {
        if is_forwardable(name) {
            resp.message.add_option(
                CoapOption::Unknown(OPT_FWD_HEADER),
                encode_header(name, value),
            );
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
        content_format: parse_content_format(
            resp.message.get_first_option(CoapOption::ContentFormat),
        ),
    }
}

/// Compact the canonical X-Matrix credential this project's federation client
/// sends — `X-Matrix origin="…",destination="…"` with no other auth-params (no
/// `key`/`sig`; trusted network, see neutrino-http's `federation::auth`) — to
/// bare `origin,destination` bytes. Server names contain no `,`/`"`, so the
/// join is unambiguous; `None` for any other shape (a signing peer's
/// `key`/`sig`, another scheme), which then travels verbatim as
/// `OPT_FWD_HEADER`.
fn compact_xmatrix(value: &[u8]) -> Option<Vec<u8>> {
    let params = std::str::from_utf8(value).ok()?.strip_prefix("X-Matrix ")?;
    let (origin, destination) = params.split_once(',')?;
    let origin = origin.strip_prefix("origin=\"")?.strip_suffix('"')?;
    let destination = destination
        .strip_prefix("destination=\"")?
        .strip_suffix('"')?;
    // A `,`/`"` inside a "value" means extra auth-params or a non-server-name
    // value — not the canonical form, and not representable compactly.
    if origin.contains([',', '"']) || destination.contains([',', '"']) {
        return None;
    }
    Some(format!("{origin},{destination}").into_bytes())
}

/// Re-synthesise the full `authorization` header from a compact
/// `origin,destination` option value. `None` (the request then asserts no
/// server identity) if the value is malformed, mirroring `decode_headers`
/// dropping separator-less entries.
fn expand_xmatrix(raw: &[u8]) -> Option<(String, Vec<u8>)> {
    let (origin, destination) = std::str::from_utf8(raw).ok()?.split_once(',')?;
    let value = format!("X-Matrix origin=\"{origin}\",destination=\"{destination}\"");
    Some(("authorization".to_owned(), value.into_bytes()))
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
fn decode_headers(opt: Option<&LinkedList<Vec<u8>>>) -> Vec<(String, Vec<u8>)> {
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
fn option_values(opt: Option<&LinkedList<Vec<u8>>>) -> Vec<Vec<u8>> {
    opt.map(|l| l.iter().cloned().collect()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire cost of the field: the option value must stay one byte for any
    /// codepoint under 256, because CoAP repeats options in EVERY block — a
    /// two-byte value is paid per block, not per message.
    #[test]
    fn content_format_under_256_costs_one_byte() {
        assert_eq!(content_format_bytes(CBOR_CONTENT_FORMAT).len(), 1);
        assert_eq!(content_format_bytes(59).len(), 1);
        assert_eq!(content_format_bytes(255).len(), 1);
        assert_eq!(content_format_bytes(0).len(), 0, "0 encodes empty");
        assert_eq!(content_format_bytes(65000).len(), 2, "the range to avoid");
    }

    #[test]
    fn content_format_encoding_round_trips() {
        for f in [0u16, 59, 60, 255, 256, 65000, 65535] {
            let bytes = content_format_bytes(f);
            assert_eq!(parse_content_format(Some(&bytes)), f, "format {f}");
        }
    }

    /// A peer that sets no content-format at all is talking CBOR — that is what
    /// every pre-field build of this proxy emitted.
    #[test]
    fn absent_content_format_reads_as_cbor() {
        assert_eq!(parse_content_format(None), CBOR_CONTENT_FORMAT);
    }

    /// A non-default format must survive serialization in both directions, or a
    /// codec's compressed body would be read back as plain CBOR.
    #[test]
    fn non_default_content_format_survives_the_wire() {
        let wire = WireRequest {
            path: "/_matrix/federation/v1/send/1".to_owned(),
            body: vec![0xde, 0xad],
            content_format: 59,
            ..Default::default()
        };
        let bytes = build_request(&wire).message.to_bytes().expect("to_bytes");
        let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        let mut reparsed: CoapRequest<SocketAddr> = CoapRequest::new();
        reparsed.message = packet;
        assert_eq!(parse_request(&reparsed).content_format, 59);

        let resp_wire = WireResponse {
            status: 200,
            body: vec![0xde, 0xad],
            content_format: 59,
            ..Default::default()
        };
        let req: CoapRequest<SocketAddr> = CoapRequest::new();
        let mut resp = CoapResponse::new(&req.message).expect("response");
        write_response(&mut resp, &resp_wire);
        let bytes = resp.message.to_bytes().expect("to_bytes");
        let mut reparsed = CoapResponse::new(&req.message).expect("response");
        reparsed.message = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        assert_eq!(parse_response(&reparsed).content_format, 59);
    }

    #[test]
    fn request_round_trips_method_path_headers_body() {
        let wire = WireRequest {
            dest: "peer:8448".to_owned(),
            method: Method::PUT,
            path: "/_matrix/federation/v2/send_join/!r:a/$e?ver=12".to_owned(),
            headers: vec![
                (
                    "authorization".to_owned(),
                    b"X-Matrix origin=\"a\"".to_vec(),
                ),
                // Not forwardable — must be dropped.
                ("content-length".to_owned(), b"99".to_vec()),
            ],
            body: vec![0xA1, 0x01, 0x02], // arbitrary CBOR-ish bytes
            ..Default::default()
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
            got.headers
                .iter()
                .any(|(k, v)| k == "authorization" && v == b"X-Matrix origin=\"a\""),
            "authorization header lost: {:?}",
            got.headers
        );
        assert!(
            !got.headers.iter().any(|(k, _)| k == "content-length"),
            "non-forwardable header leaked"
        );
    }

    // Serialize a built request through packet bytes and reparse, as the tests
    // below all need to prove survival across the actual wire encoding.
    fn wire_round_trip(wire: &WireRequest) -> (CoapRequest<SocketAddr>, WireRequest) {
        let coap = build_request(wire);
        let bytes = coap.message.to_bytes().expect("to_bytes");
        let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        let mut reparsed: CoapRequest<SocketAddr> = CoapRequest::new();
        reparsed.message = packet;
        let got = parse_request(&reparsed);
        (reparsed, got)
    }

    fn auth_request(auth_value: &[u8]) -> WireRequest {
        WireRequest {
            dest: "peer:8448".to_owned(),
            method: Method::PUT,
            path: "/_matrix/federation/v1/send/txn1".to_owned(),
            headers: vec![("authorization".to_owned(), auth_value.to_vec())],
            body: vec![],
            ..Default::default()
        }
    }

    // The canonical credential must travel as bare `origin,destination` under
    // OPT_X_MATRIX_AUTH — no header name, no scheme/param/quote framing — and
    // re-expand to the exact full header on ingress. This is the low-bandwidth
    // win: the option is re-sent in every block, so its size is ~the two names.
    #[test]
    fn canonical_xmatrix_compacts_on_wire_and_re_expands() {
        let full = br#"X-Matrix origin="a.example",destination="b.example""#;
        let (reparsed, got) = wire_round_trip(&auth_request(full));

        let compact = reparsed
            .message
            .get_first_option(CoapOption::Unknown(OPT_X_MATRIX_AUTH))
            .expect("compact auth option");
        assert_eq!(compact, b"a.example,b.example");
        assert!(
            reparsed
                .message
                .get_option(CoapOption::Unknown(OPT_FWD_HEADER))
                .is_none_or(|l| l.is_empty()),
            "canonical credential must not also travel verbatim"
        );
        assert_eq!(
            got.headers,
            vec![("authorization".to_owned(), full.to_vec())],
            "ingress must re-synthesise the exact full header"
        );
    }

    // A non-canonical credential (a real signing peer's key/sig) cannot be
    // represented compactly and must fall back to the verbatim path unchanged.
    #[test]
    fn xmatrix_with_key_and_sig_falls_back_to_verbatim() {
        let signed =
            br#"X-Matrix origin="a.example",destination="b.example",key="ed25519:1",sig="abc==""#;
        let (reparsed, got) = wire_round_trip(&auth_request(signed));

        assert!(
            reparsed
                .message
                .get_first_option(CoapOption::Unknown(OPT_X_MATRIX_AUTH))
                .is_none(),
            "a key/sig credential must not be lossily compacted"
        );
        assert_eq!(
            got.headers,
            vec![("authorization".to_owned(), signed.to_vec())]
        );
    }

    // A peer sending BOTH the compact option and a verbatim authorization must
    // end up with exactly one identity — the compact one — so the ingress
    // origin↔source binding and the upstream auth gate can't read different
    // origins from the same request.
    #[test]
    fn compact_auth_overrides_verbatim_authorization() {
        let mut coap: CoapRequest<SocketAddr> = CoapRequest::new();
        coap.set_method(RequestType::Put);
        coap.message.add_option(
            CoapOption::Unknown(OPT_FWD_HEADER),
            encode_header("authorization", br#"X-Matrix origin="evil.example""#),
        );
        coap.message.add_option(
            CoapOption::Unknown(OPT_X_MATRIX_AUTH),
            b"a.example,b.example".to_vec(),
        );
        let got = parse_request(&coap);
        assert_eq!(
            got.headers,
            vec![(
                "authorization".to_owned(),
                br#"X-Matrix origin="a.example",destination="b.example""#.to_vec()
            )]
        );
    }

    // A malformed compact option (no separator) asserts no identity at all —
    // the request proceeds headerless and the upstream auth gate 401s it.
    #[test]
    fn malformed_compact_auth_yields_no_authorization() {
        let mut coap: CoapRequest<SocketAddr> = CoapRequest::new();
        coap.set_method(RequestType::Get);
        coap.message
            .add_option(CoapOption::Unknown(OPT_X_MATRIX_AUTH), b"no-comma".to_vec());
        let got = parse_request(&coap);
        assert!(got.headers.is_empty(), "got {:?}", got.headers);
    }

    #[test]
    fn response_carries_exact_status_and_body() {
        let req: CoapRequest<SocketAddr> = CoapRequest::new();
        let mut resp = CoapResponse::new(&req.message).expect("response");
        let wire = WireResponse {
            status: 403,
            // `x-matrix-*` is the forwardable prefix; a non-allowlisted header
            // would be (correctly) dropped, so use one that must survive.
            headers: vec![("x-matrix-test".to_owned(), b"v".to_vec())],
            body: vec![0xFF, 0x00],
            ..Default::default()
        };
        write_response(&mut resp, &wire);

        let bytes = resp.message.to_bytes().expect("to_bytes");
        let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
        let reparsed = CoapResponse { message: packet };

        let got = parse_response(&reparsed);
        assert_eq!(got.status, 403, "exact HTTP status must survive");
        assert_eq!(got.body, vec![0xFF, 0x00]);
        assert!(
            got.headers
                .iter()
                .any(|(k, v)| k == "x-matrix-test" && v == b"v")
        );
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

    #[test]
    fn methods_round_trip_and_unknown_falls_back_to_get() {
        // GET/POST/PUT survive the CoAP method mapping; any other method is
        // intentionally coerced to GET (federation uses only these three — see
        // `to_coap_method`), a lossy fallback this pins explicitly.
        for (input, expected) in [
            (Method::GET, Method::GET),
            (Method::POST, Method::POST),
            (Method::PUT, Method::PUT),
            (Method::DELETE, Method::GET),
        ] {
            let wire = WireRequest {
                dest: String::new(),
                method: input.clone(),
                path: "/_matrix/federation/v1/send/txn1".to_owned(),
                headers: vec![],
                body: vec![],
                ..Default::default()
            };
            let coap = build_request(&wire);
            let bytes = coap.message.to_bytes().expect("to_bytes");
            let packet = coap_lite::Packet::from_bytes(&bytes).expect("from_bytes");
            let mut reparsed: CoapRequest<SocketAddr> = CoapRequest::new();
            reparsed.message = packet;
            let got = parse_request(&reparsed);
            assert_eq!(got.method, expected, "{input} should map to {expected}");
        }
    }
}
