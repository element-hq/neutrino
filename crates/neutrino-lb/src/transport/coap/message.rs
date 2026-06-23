//! Mapping between `WireRequest`/`WireResponse` and `coap-lite` messages. The
//! body is carried verbatim (opaque CBOR). Forwardable headers travel as
//! `OPT_FWD_HEADER` options; the exact HTTP status as `OPT_HTTP_STATUS`.

use std::collections::LinkedList;
use std::net::SocketAddr;

use axum::http::Method;
use coap_lite::{CoapOption, CoapRequest, CoapResponse, RequestType};

use crate::headers::is_forwardable;
use crate::transport::{WireRequest, WireResponse};

use super::{CBOR_CONTENT_FORMAT, OPT_FWD_HEADER, OPT_HTTP_STATUS};

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

/// CoAP content-format option value, minimally encoded (60 fits one byte).
fn content_format_bytes() -> Vec<u8> {
    vec![CBOR_CONTENT_FORMAT as u8]
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
    out.message
        .add_option(CoapOption::ContentFormat, content_format_bytes());
    for (name, value) in &req.headers {
        if is_forwardable(name) {
            out.message.add_option(
                CoapOption::Unknown(OPT_FWD_HEADER),
                encode_header(name, value),
            );
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
    resp.message.add_option(
        CoapOption::Unknown(OPT_HTTP_STATUS),
        wire.status.to_be_bytes().to_vec(),
    );
    resp.message
        .add_option(CoapOption::ContentFormat, content_format_bytes());
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
