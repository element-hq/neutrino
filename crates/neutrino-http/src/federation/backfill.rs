//! `GET /_matrix/federation/v1/backfill/{roomId}`.
//!
//! Walks the room's `prev_events` DAG backward from the `v` query-param
//! event IDs and returns up to `limit` PDUs, newest-first, wrapped in the
//! federation transaction envelope `{ origin, origin_server_ts, pdus }`.
//!
//! Reuses `DagStore::events_before` (the seeds-included reverse-chronological
//! priority-queue walk) rather than a dedicated storage method. Trusted-mesh
//! deviations match the `get_missing_events` sibling:
//! no X-Matrix auth, no signature verification, no history-visibility /
//! redaction filtering.

use axum::{
    Json,
    extract::{Path, RawQuery, State},
    http::HeaderMap,
};
use neutrino_store::{EventStore, RoomStore};
use ruma::{EventId, OwnedEventId, OwnedRoomId};
use serde::Serialize;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::{FedError, auth};
use crate::{AppState, lock_app};

/// Spec/Synapse default for `limit` when the requester omits it. Backfill's
/// `limit` is nominally required (ruma types it `UInt`), but a *missing* or
/// un-parseable `limit` defaults here rather than 400 — friendlier in the
/// trusted single-user mesh. An *explicit* `limit=0` is still rejected (see
/// the handler): that matches Synapse, whose backfill servlet does
/// `if not limit: return 400` — asking for zero events is a client bug, not
/// a valid empty backfill.
const DEFAULT_LIMIT: u32 = 10;
/// Hard cap on returned PDUs. Matches Synapse's `min(limit, 100)` in its
/// backfill handler; the v1.18 spec sets no maximum. Saturating, not a 400.
const MAX_LIMIT: u32 = 100;

/// Serializable mirror of `ruma::api::federation::backfill::get_backfill::v1::Response`.
///
/// ruma's `#[response]` macro emits an `OutgoingResponse` impl, not a plain
/// `Serialize`, so — as with the `get_missing_events` sibling — we hand-roll
/// the JSON body the federation spec wants: a transaction envelope of
/// `origin`, `origin_server_ts`, and the opaque `pdus`.
#[derive(Serialize)]
pub(crate) struct ResponseBody {
    origin: String,
    origin_server_ts: u64,
    pdus: Vec<Box<RawJsonValue>>,
}

/// Federation `/backfill` handler.
///
/// 1. Parse `room_id` manually (400 JSON, not axum's plain-text default).
/// 2. Reject an empty `v` (400) — nothing to walk back from.
/// 3. Reject an explicit `limit=0` (400) — Synapse parity (`if not limit`).
/// 4. 404 if the room is unknown (pre-checked via `RoomStore::room_exists`).
/// 5. Clamp `limit`: default 10 if absent, max 100 (Synapse parity).
/// 5. Pre-filter `v` to the seeds we actually hold (so an unknown seed is
///    skipped, not a 500 — `events_before` rejects unknown seeds).
/// 6. Walk back via `DagStore::events_before` (seeds included, newest-first).
/// 7. Ship `Event.raw` verbatim inside the transaction envelope.
pub(crate) async fn handle(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<ResponseBody>, FedError> {
    // (1)
    let room_id = OwnedRoomId::try_from(room_id.as_str())
        .map_err(|_| FedError::BadRequest("invalid room_id"))?;

    let (v_raw, limit_raw) = parse_backfill_query(raw_query.as_deref());

    // (2) — a present-but-blank `?v=` parses to no values (the parser drops
    // empty entries), so this also catches the blank case, not just the
    // wholly-absent one.
    if v_raw.is_empty() {
        return Err(FedError::BadRequest("missing v parameter"));
    }

    // (3) — explicit `limit=0` is a client bug, not a valid empty backfill;
    // reject it like Synapse's `if not limit`. A missing limit defaults (5).
    if limit_raw == Some(0) {
        return Err(FedError::BadRequest("limit must be a positive integer"));
    }

    let (store, origin) = {
        let app = lock_app(&state);
        (app.store.clone(), app.config.server_name.clone())
    };

    // Authenticate the caller (network-attested `X-Matrix` origin) — `origin`
    // here is *our* name (the response envelope's `origin`), used as the
    // expected `destination`.
    let caller = auth::authenticated_origin(&headers, &origin)?;

    // (4)
    if !store.room_exists(&room_id).await? {
        return Err(FedError::RoomNotFound);
    }

    // (4b) — member-only scoping: backfill is the timeline-DAG analogue of
    // `get_missing_events` and equally leaks history; only a server sharing the
    // room may walk it. 404 (above) precedes this 403 so an unknown room isn't
    // masked as a membership failure.
    if !auth::server_in_room(&store, &room_id, &caller).await? {
        return Err(FedError::Forbidden(
            "origin server is not a member of this room",
        ));
    }

    // (5) — default 10 if absent; saturating upper cap (explicit 0 already
    // rejected in (3)).
    let limit = limit_raw.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;

    // (6) — parse seed ids, dropping any that aren't syntactically valid
    // (a garbled seed is treated like an unknown one: skipped). Then filter
    // to the seeds we actually hold in this room via `get_events` (which
    // silently omits absent ids). `events_before` rejects a seed it doesn't
    // hold with InvalidInput → 500, but a peer may legitimately ask us to
    // backfill from an event we've never seen; skipping matches Synapse,
    // which only seeds its walk from events it has.
    let seeds: Vec<OwnedEventId> = v_raw
        .iter()
        .filter_map(|s| OwnedEventId::try_from(s.as_str()).ok())
        .collect();
    let seed_refs: Vec<&EventId> = seeds.iter().map(|id| id.as_ref()).collect();
    let known: Vec<OwnedEventId> = store
        .get_events(&seed_refs)
        .await?
        .into_iter()
        .filter(|e| e.room_id == room_id)
        .map(|e| e.event_id)
        .collect();

    // (7) + (8) — reverse-chronological (newest-first), seeds included: the
    // order `events_before` yields and the order Synapse's backfill returns.
    // No `.rev()` (unlike get_missing_events, which serves oldest-first).
    // Wire bytes verbatim so each event's reference hash round-trips on the
    // peer.
    let known_refs: Vec<&EventId> = known.iter().map(|id| id.as_ref()).collect();
    let pdus: Vec<Box<RawJsonValue>> =
        crate::federation::events_before_raw(&*store, &room_id, &known_refs, limit).await?;

    Ok(Json(ResponseBody {
        origin,
        origin_server_ts: crate::federation::now_ms(),
        pdus,
    }))
}

/// Parse the backfill query string into the repeated `v` event-id values
/// (percent-decoded, in wire order) and the optional `limit`.
///
/// axum's `Query<HashMap<_, _>>` (serde_urlencoded) collapses repeated keys,
/// so `?v=…&v=…` would lose all but one — hence the hand-rolled walk over
/// `RawQuery`. Unknown keys are ignored; a `limit` that doesn't parse as a
/// `u32` yields `None` (the caller defaults it). `v` values that are *empty*
/// (a blank `?v=`) or fail to percent-decode are dropped — so a request that
/// supplies no non-empty `v` yields an empty list, which the caller turns
/// into a 400, and an unknown/garbled non-empty seed is treated as "not
/// held" (skipped).
fn parse_backfill_query(raw: Option<&str>) -> (Vec<String>, Option<u32>) {
    let mut vs = Vec::new();
    let mut limit = None;
    let Some(raw) = raw else {
        return (vs, limit);
    };
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, val) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            // Drop empty values: a blank `?v=` is a missing seed, not a seed
            // that happens to be the empty string. The caller's empty-`v`
            // check then 400s instead of silently returning an empty walk.
            "v" => {
                if let Some(decoded) = percent_decode(val).filter(|d| !d.is_empty()) {
                    vs.push(decoded);
                }
            }
            // Last `limit` wins; a non-numeric value leaves it `None` so the
            // caller falls back to the default.
            "limit" => limit = percent_decode(val).and_then(|d| d.parse::<u32>().ok()),
            _ => {}
        }
    }
    (vs, limit)
}

/// Percent-decode a single `application/x-www-form-urlencoded` value: `%XX`
/// hex escapes and `+` → space. Returns `None` for any malformed `%` escape —
/// whether the two following characters aren't hex digits *or* the escape is
/// incomplete (a trailing `%`, or `%` followed by only one character) — and
/// for output that isn't valid UTF-8. Malformed input is rejected, not
/// silently passed through.
fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                // Require exactly two trailing hex digits; an incomplete
                // escape (no/one trailing char) or a non-hex digit makes
                // `?` short-circuit to `None`.
                let hi = bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16))?;
                let lo = bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16))?;
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_none_query_is_empty() {
        assert_eq!(parse_backfill_query(None), (Vec::new(), None));
    }

    #[test]
    fn parse_empty_query_is_empty() {
        assert_eq!(parse_backfill_query(Some("")), (Vec::new(), None));
    }

    #[test]
    fn parse_single_v_no_limit() {
        // `$` arrives percent-encoded as %24 on the wire (serde_html_form,
        // which is what ruma's client uses to build the query).
        let (vs, limit) = parse_backfill_query(Some("v=%24abc"));
        assert_eq!(vs, vec!["$abc".to_owned()]);
        assert_eq!(limit, None);
    }

    #[test]
    fn parse_multiple_v_preserves_order() {
        let (vs, _) = parse_backfill_query(Some("v=%24a&v=%24b&v=%24c"));
        assert_eq!(vs, vec!["$a".to_owned(), "$b".to_owned(), "$c".to_owned()]);
    }

    #[test]
    fn parse_limit_extracted() {
        let (_, limit) = parse_backfill_query(Some("v=%24a&limit=50"));
        assert_eq!(limit, Some(50));
    }

    #[test]
    fn parse_unparseable_limit_is_none() {
        let (_, limit) = parse_backfill_query(Some("v=%24a&limit=not-a-number"));
        assert_eq!(limit, None);
    }

    #[test]
    fn parse_ignores_unknown_keys() {
        let (vs, limit) = parse_backfill_query(Some("foo=bar&v=%24a&baz=qux"));
        assert_eq!(vs, vec!["$a".to_owned()]);
        assert_eq!(limit, None);
    }

    #[test]
    fn parse_blank_v_value_is_dropped() {
        // A present-but-blank `?v=` yields no seeds (the handler turns the
        // empty list into a 400) — it must not become an empty-string seed.
        let (vs, limit) = parse_backfill_query(Some("v=&limit=10"));
        assert!(vs.is_empty(), "blank v must be dropped: {vs:?}");
        assert_eq!(limit, Some(10));
        // A bare `?v` with no `=` is likewise empty and dropped.
        let (vs, _) = parse_backfill_query(Some("v&limit=10"));
        assert!(vs.is_empty(), "bare v must be dropped: {vs:?}");
        // A blank `v` alongside a real one keeps only the real seed.
        let (vs, _) = parse_backfill_query(Some("v=&v=%24a"));
        assert_eq!(vs, vec!["$a".to_owned()]);
    }

    #[test]
    fn parse_explicit_limit_zero_is_some_zero() {
        // The parser surfaces `limit=0` as `Some(0)` so the handler can
        // distinguish it from a missing limit and 400 (Synapse parity).
        let (_, limit) = parse_backfill_query(Some("v=%24a&limit=0"));
        assert_eq!(limit, Some(0));
    }

    #[test]
    fn percent_decode_handles_sigil_and_plus() {
        // %24 → '$', '+' → ' ' (form-encoding), unreserved chars untouched.
        assert_eq!(percent_decode("%24abc-_").as_deref(), Some("$abc-_"));
        assert_eq!(percent_decode("a+b").as_deref(), Some("a b"));
    }

    #[test]
    fn percent_decode_rejects_malformed_escape() {
        assert_eq!(percent_decode("%2g"), None);
    }

    #[test]
    fn percent_decode_rejects_incomplete_escape() {
        // A `%` with fewer than two trailing characters is malformed and
        // rejected (not passed through as a literal `%`).
        assert_eq!(percent_decode("ab%"), None);
        assert_eq!(percent_decode("a%2"), None);
    }
}
