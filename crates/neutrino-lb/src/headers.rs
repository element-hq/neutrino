//! Header pass-through policy. The proxy forwards semantic headers
//! (Authorization / X-Matrix, etc.) but never the framing/hop-by-hop headers,
//! which the downstream HTTP client recomputes for its own request.

/// Lowercase names the proxy must NOT copy through: the body framing headers
/// (re-set per hop after transcoding changes the length and media type) and
/// the connection-management hop-by-hop headers.
const STRIPPED: &[&str] = &[
    "host",
    "content-length",
    "content-type",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-connection",
    "upgrade",
];

/// True if `name` (any case) may be forwarded verbatim to the next hop.
pub fn is_forwardable(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    !STRIPPED.contains(&lower.as_str())
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
}
