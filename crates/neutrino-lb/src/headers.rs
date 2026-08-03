//! Header pass-through policy. The proxy forwards only the *semantic* Matrix
//! federation headers and drops everything else. An **allowlist** (not a
//! denylist) is used deliberately: the body is re-serialized JSON↔CBOR on every
//! hop, so any header a peer set describing the original body — a stale
//! `Content-Encoding`, a smuggled `Transfer-Encoding`/`Content-Length` — would
//! be a lie at the next hop. Listing what may pass, and dropping the rest,
//! means a header has to be explicitly understood to survive; framing headers
//! are recomputed per hop by the downstream HTTP client regardless.

/// Lowercase header names the proxy forwards verbatim. The only semantic header
/// this (signature-less, trusted-network) server uses is `authorization`: it
/// carries the `X-Matrix origin="…",destination="…"` credential the inbound
/// side reads to authenticate the origin (see `federation::auth`).
const ALLOWED: &[&str] = &["authorization"];

/// Lowercase prefixes the proxy forwards. Reserved for any future
/// low-bandwidth `X-Matrix-*` header; matches the `X-Matrix` auth scheme family.
const ALLOWED_PREFIXES: &[&str] = &["x-matrix"];

/// True if `name` (any case) may be forwarded verbatim to the next hop. Matrix
/// S2S *responses* carry no semantic headers, so on the response path this
/// forwards nothing.
pub fn is_forwardable(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ALLOWED.contains(&lower.as_str()) || ALLOWED_PREFIXES.iter().any(|p| lower.starts_with(p))
}

/// Extract the unquoted `origin` auth-param from an `X-Matrix origin="…",…`
/// Authorization value. `None` if the scheme prefix or `origin` is absent.
///
/// Two callers, one parser: the transport-layer identity binding
/// (`Hub::origin_binding_violation`) and the pcap capture's peer naming, which
/// is the only peer identity the ingress has — an inbound `WireRequest` carries
/// no source. Mirrors `neutrino_http::federation::auth`'s parse, kept here so
/// the Matrix-agnostic transport needn't depend on the http crate; it extracts
/// the bytes only — the http layer still owns the real auth policy.
pub fn xmatrix_origin(value: &str) -> Option<&str> {
    let params = value.strip_prefix("X-Matrix ")?;
    for part in params.split(',') {
        let Some((key, val)) = part.split_once('=') else {
            continue;
        };
        if key.trim() == "origin" {
            return Some(val.trim().trim_matches('"'));
        }
    }
    None
}

/// The raw `authorization` header value, if the list carries one. Split out
/// because the transport binding must tell "no header" (defer to the upstream
/// auth gate) from "header present but unparseable" (hard reject), a
/// distinction [`claimed_origin`] collapses.
pub fn authorization(headers: &[(String, Vec<u8>)]) -> Option<&[u8]> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.as_slice())
}

/// The claimed `X-Matrix` origin `server_name` from a header list, if any.
pub fn claimed_origin(headers: &[(String, Vec<u8>)]) -> Option<&str> {
    std::str::from_utf8(authorization(headers)?)
        .ok()
        .and_then(xmatrix_origin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_framing_and_hop_headers() {
        for h in ["Host", "content-length", "Content-Type", "Connection"] {
            assert!(!is_forwardable(h), "{h} must be stripped");
        }
    }

    #[test]
    fn forwards_authorization() {
        assert!(is_forwardable("Authorization"));
        assert!(is_forwardable("X-Matrix-Foo"));
    }

    // Allowlist: anything outside the Matrix auth headers is dropped — including
    // a peer-supplied header that would *lie* after the body is re-serialized
    // (e.g. a `Content-Encoding` describing the pre-transcode body, or a smuggled
    // framing header). A denylist would forward these by default.
    #[test]
    fn drops_unlisted_and_misleading_headers() {
        for h in [
            "Content-Encoding",
            "Transfer-Encoding",
            "X-Custom-Header",
            "User-Agent",
            "Cookie",
            "Forwarded",
        ] {
            assert!(!is_forwardable(h), "{h} must be dropped by the allowlist");
        }
    }
}
