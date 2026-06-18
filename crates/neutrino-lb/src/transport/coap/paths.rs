//! HTTP path <-> CoAP Uri-Path code mapping. Reuses Dendrite's MSC3079 v1
//! federation codes; unmapped paths fall back to verbatim segments. Federation
//! paths start with `_matrix`, which never collides with a route code, so the
//! decoder distinguishes coded from literal paths by the first segment.

// `encode`/`decode` gain their non-test caller in message.rs (next task); the
// allow is removed there.
#![allow(dead_code)]

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

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn segs_owned(v: &[String]) -> Vec<Vec<u8>> {
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
            assert_eq!(
                decode(&segs_owned(&p), &segs_owned(&q)),
                original,
                "{original}"
            );
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
}
