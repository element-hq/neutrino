//! `X-Matrix` federation auth — a **network-attested** identity, NOT a
//! cryptographic proof.
//!
//! Real Matrix federation signs each request and the receiver verifies the
//! signature against the origin's published key. Neutrino has no signing key and
//! runs no signature checks (trusted mesh — see the crate root), so we can
//! neither produce nor verify a real `X-Matrix` header. Instead we carry only
//! `origin` + `destination` (no `key`/`sig`) and trust the `origin` **solely
//! because the network layer (mTLS / a trusted proxy / a private mesh)
//! authenticated the peer**. The absent signature field is deliberate and
//! self-documenting: a reader sees there is no signature, so this can never be
//! mistaken for verified federation auth.
//!
//! SECURITY: `origin` is an identity *hint* only. It drives transaction dedup,
//! gap-fill / reconcile fetch targeting, member-only read scoping, and
//! cross-checks of self-asserted fields (`body.origin`, an event's `sender`). It
//! MUST NOT bypass event-level auth / state resolution — a PDU from an
//! authenticated origin is still an untrusted event that must pass the room's
//! auth rules, exactly as before. If the network layer does NOT bind the peer to
//! its claimed origin, this header is as forgeable as any self-asserted field and
//! provides no protection.

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use ruma::OwnedServerName;

use crate::federation::FedError;

/// Parse the `origin` out of an `X-Matrix origin="…",destination="…"` header
/// *value*. Tolerant of optional surrounding quotes and of extra/unknown
/// auth-params (a real peer's `key`/`sig`, and our own `destination`, which we
/// ignore — see below). `None` if the scheme prefix is absent or `origin` is
/// missing / not a valid server name. Server names contain no `,` or `"`, so the
/// naive comma/quote handling is safe for them.
///
/// `destination` is intentionally **not** enforced. This project resolves a
/// server name directly to its address (`http://{server_name}`), so a peer's
/// `destination` is the address it dialled, which need not equal our configured
/// `server_name` (e.g. multi-homed, or an ephemeral-port test peer). There is no
/// virtual-hosting to protect, and the network layer already binds the peer, so
/// a destination check would add friction without security. We still *send*
/// `destination` outbound for wire-compatibility with real Matrix peers.
fn parse_origin(value: &str) -> Option<OwnedServerName> {
    let params = value.strip_prefix("X-Matrix ")?;
    let mut origin = None;
    for part in params.split(',') {
        // Skip a malformed auth-param rather than failing the whole header.
        let Some((key, val)) = part.split_once('=') else {
            continue;
        };
        if key.trim() == "origin" {
            origin = OwnedServerName::try_from(val.trim().trim_matches('"')).ok();
        }
    }
    origin
}

/// Authenticate an inbound federation request from its `X-Matrix` header and
/// return the network-attested origin server. Rejects (all 401 `M_UNAUTHORIZED`):
/// - a missing or unparseable header (no `origin`),
/// - an `origin` claiming to be us (a peer must not impersonate this server —
///   that would poison our own txn-dedup / reconcile identity namespace).
pub(crate) fn authenticated_origin(
    headers: &HeaderMap,
    our_name: &str,
) -> Result<OwnedServerName, FedError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(FedError::Unauthorized(
            "missing X-Matrix authorization header",
        ))?;
    let origin = parse_origin(value).ok_or(FedError::Unauthorized(
        "malformed X-Matrix authorization header",
    ))?;
    if origin.as_str() == our_name {
        return Err(FedError::Unauthorized(
            "X-Matrix origin must not claim to be this server",
        ));
    }
    Ok(origin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        h
    }

    #[test]
    fn parses_origin_ignoring_destination_key_and_sig() {
        // A real peer's header carries destination/key/sig too; we extract only
        // the origin and ignore the rest.
        let origin = parse_origin(
            r#"X-Matrix origin="a.example",destination="b.example",key="ed25519:1",sig="abc=="#,
        )
        .expect("parses");
        assert_eq!(origin.as_str(), "a.example");
        // origin alone is enough — destination is not required.
        assert_eq!(
            parse_origin(r#"X-Matrix origin="a.example""#)
                .unwrap()
                .as_str(),
            "a.example"
        );
    }

    #[test]
    fn parse_rejects_wrong_scheme_and_missing_origin() {
        assert!(parse_origin("Bearer token").is_none());
        assert!(parse_origin(r#"X-Matrix destination="b.example""#).is_none()); // no origin
        assert!(parse_origin("X-Matrix nonsense").is_none());
    }

    #[test]
    fn authenticated_origin_happy_path() {
        // destination need not match our name (we don't enforce it).
        let origin = authenticated_origin(
            &headers(r#"X-Matrix origin="a.example",destination="anything.example""#),
            "us.example",
        )
        .expect("ok");
        assert_eq!(origin.as_str(), "a.example");
    }

    #[test]
    fn authenticated_origin_rejects_missing_malformed_and_self() {
        // Missing header.
        assert!(matches!(
            authenticated_origin(&HeaderMap::new(), "us.example"),
            Err(FedError::Unauthorized(_))
        ));
        // Malformed (wrong scheme).
        assert!(matches!(
            authenticated_origin(&headers("Bearer x"), "us.example"),
            Err(FedError::Unauthorized(_))
        ));
        // Self-impersonation: origin claims to be us.
        assert!(matches!(
            authenticated_origin(
                &headers(r#"X-Matrix origin="us.example",destination="us.example""#),
                "us.example"
            ),
            Err(FedError::Unauthorized(_))
        ));
    }
}
