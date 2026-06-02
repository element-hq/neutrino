//! Testing-only multi-user identity shim. Compiled only under the
//! `multi-user-shim` cargo feature. Holds the in-memory access-token →
//! user map, a token minter, and the `AuthUser` extractor's resolution
//! logic. None of this ships in the single-user (Android/FFI) build.

#![cfg(feature = "multi-user-shim")]

use std::collections::HashMap;

use ruma::OwnedUserId;

/// In-memory map of opaque access token → the user it authenticates.
/// Ephemeral: lives in `App`, lost on restart (acceptable for tests).
pub(crate) type UserTokens = HashMap<String, OwnedUserId>;

/// Mint a fresh, unique access token of the Synapse-ish `syt_<random>`
/// shape. 32 random alphanumerics give ample collision resistance for a
/// test server.
pub(crate) fn mint_token() -> String {
    use rand::Rng;
    use rand::distr::Alphanumeric;
    let suffix: String = rand::rng()
        .sample_iter(Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    format!("syt_{suffix}")
}

use axum::http::HeaderMap;

/// Why a token failed to resolve. Maps to a 401 errcode at the HTTP edge.
pub(crate) enum TokenError {
    /// No `Authorization` header at all.
    Missing,
    /// Header present but malformed, or token not in the map.
    Unknown,
}

/// Resolve a request's `Authorization: Bearer <token>` against the token
/// map. `Ok(user)` on a hit; `Err` otherwise (the caller maps to 401).
pub(crate) fn resolve(
    headers: &HeaderMap,
    tokens: &std::sync::Mutex<UserTokens>,
) -> Result<OwnedUserId, TokenError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(TokenError::Missing)?;
    let value = header.to_str().map_err(|_| TokenError::Unknown)?;
    // RFC 7235 scheme matching is case-insensitive, but every Matrix client
    // sends exactly "Bearer " — we accept only that canonical form.
    let token = value.strip_prefix("Bearer ").ok_or(TokenError::Unknown)?;
    let map = tokens.lock().unwrap_or_else(|e| e.into_inner());
    map.get(token).cloned().ok_or(TokenError::Unknown)
}

/// Resolve a requested localpart to a full user id on this server, mint a
/// fresh token, store it, and return `(user_id, token)`. An absent/blank
/// localpart falls back to the configured default (single-user parity).
pub(crate) fn provision(
    tokens: &std::sync::Mutex<UserTokens>,
    server_name: &str,
    default_user_id: &str,
    requested_localpart: Option<&str>,
) -> Result<(OwnedUserId, String), String> {
    let user_id: OwnedUserId = match requested_localpart {
        Some(lp) if !lp.is_empty() => format!("@{lp}:{server_name}")
            .as_str()
            .try_into()
            .map_err(|e: ruma::IdParseError| e.to_string())?,
        _ => default_user_id
            .try_into()
            .map_err(|e: ruma::IdParseError| e.to_string())?,
    };
    let token = mint_token();
    tokens
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(token.clone(), user_id.clone());
    Ok((user_id, token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_token_has_prefix_and_is_unique() {
        let a = mint_token();
        let b = mint_token();
        assert!(a.starts_with("syt_"), "got {a}");
        assert!(a.len() > 4);
        assert_ne!(a, b, "two mints must differ");
    }

    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use std::sync::Mutex;

    #[test]
    fn provision_uses_localpart_and_stores_token() {
        let tokens = Mutex::new(UserTokens::new());
        let (user, token) =
            provision(&tokens, "example.org", "@alice:example.org", Some("bob")).unwrap();
        assert_eq!(user.as_str(), "@bob:example.org");
        assert_eq!(
            tokens.lock().unwrap().get(&token).map(|u| u.to_string()),
            Some("@bob:example.org".to_owned())
        );
    }

    #[test]
    fn provision_falls_back_to_default_when_absent() {
        let tokens = Mutex::new(UserTokens::new());
        let (user, _) = provision(&tokens, "example.org", "@alice:example.org", None).unwrap();
        assert_eq!(user.as_str(), "@alice:example.org");
    }

    #[test]
    fn resolve_hit_miss_and_missing() {
        let mut t = UserTokens::new();
        let alice: OwnedUserId = "@alice:example.org".try_into().unwrap();
        t.insert("syt_abc".to_owned(), alice.clone());
        let tokens = Mutex::new(t);

        // Hit.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("Bearer syt_abc"));
        assert_eq!(resolve(&h, &tokens).ok(), Some(alice));

        // Unknown token.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("Bearer nope"));
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Unknown)));

        // Missing header.
        let h = HeaderMap::new();
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Missing)));

        // Malformed (no Bearer prefix) → Unknown.
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("syt_abc"));
        assert!(matches!(resolve(&h, &tokens), Err(TokenError::Unknown)));
    }
}
