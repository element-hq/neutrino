//! Pure translation helpers used by the legacy `/sync` stub.
//!
//! Three public functions:
//!
//! - [`parse_legacy_query`]: v3 query string → [`LegacyQuery`].
//! - [`synthesize_v5_request`]: [`LegacyQuery`] → [`v5::Request`].
//! - [`translate_response`]: [`v5::Response`] + membership map → v3 JSON.
//!
//! The handler that ties them together lives in
//! `legacy_sync` proper (phase 2); these helpers have no I/O, no awaits, and
//! no fallible logic on the happy path so they're easy to test.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use neutrino_store::Membership;
use ruma::OwnedRoomId;
use ruma::api::client::sync::sync_events::v5;
use ruma::events::StateEventType;
use serde_json::{Value, json};

/// Parsed v3 `/sync` query parameters.
///
/// Only the fields that influence the synthesized v5 request are kept;
/// `filter`, `full_state` and `set_presence` are read-and-discarded per
/// the design doc.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyQuery {
    /// Opaque continuation token. Passed straight to v5 `pos`.
    pub since: Option<String>,
    /// Long-poll timeout. Defaults to zero (no waiting) when absent or
    /// unparseable, matching legacy `/sync` semantics.
    pub timeout: Duration,
    /// `true` iff the client advertised MSC4222 awareness via either
    /// `use_state_after=true` or `org.matrix.msc4222.use_state_after=true`.
    /// The translator currently dual-emits regardless, but the flag is
    /// captured so a future strict-mode path can branch on it.
    pub use_state_after: bool,
}

/// Parse a v3 `/sync` query parameter map per the design doc's
/// "Query parameter mapping" table.
///
/// Behaviour:
/// - `since` → `LegacyQuery::since`. An empty string (`?since=`) is treated
///   the same as absent — both mean "initial sync". Legacy `/sync` clients
///   routinely send `since=` empty on the first request; passing `Some("")`
///   through to v5 would be rejected as an invalid `pos` by the u64 parser.
/// - `timeout` → ms → `Duration`. Missing or unparseable → `Duration::ZERO`.
/// - `use_state_after` / `org.matrix.msc4222.use_state_after` → either with
///   value `"true"` (case-insensitive) flips `use_state_after` to `true`.
/// - `filter`, `full_state`, `set_presence` → silently ignored.
pub fn parse_legacy_query(query: &HashMap<String, String>) -> LegacyQuery {
    let since = query.get("since").filter(|s| !s.is_empty()).cloned();
    let timeout = query
        .get("timeout")
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::ZERO);
    let use_state_after = is_truthy(query.get("use_state_after"))
        || is_truthy(query.get("org.matrix.msc4222.use_state_after"));
    LegacyQuery {
        since,
        timeout,
        use_state_after,
    }
}

fn is_truthy(value: Option<&String>) -> bool {
    value
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Build the synthesized [`v5::Request`] from a parsed legacy query.
///
/// Shape (per the design doc's "Synthesized into the v5 request" section):
/// - `pos` carries `query.since` verbatim,
/// - `timeout` is `Some(d)` only when the legacy query asked for a non-zero
///   wait (legacy `/sync` defaults to "return immediately"; the v5 handler
///   treats `None` and `Some(ZERO)` the same way, so the distinction matters
///   only for log readability),
/// - `conn_id = Some("__legacy__")` so legacy and any real sliding-sync
///   sessions never collide in the `ConnRegistry`,
/// - exactly one list named `"all"` covering the full window with a
///   wildcard `required_state`. `StateEventType::from("*")` is the
///   wildcard form already exercised by the `required_state_wildcard_*`
///   tests in `sliding_sync/tests.rs` (e.g.
///   `required_state_wildcard_matches_everything` and
///   `required_state_wildcard_event_type_matches_specific_state_key`)
///   which round-trip through the v5 type and are matched by
///   `required_state_matches` in `build.rs` — using the wildcard avoids
///   enumerating event types and matches what the existing tests
///   verify works.
pub fn synthesize_v5_request(query: &LegacyQuery) -> v5::Request {
    let mut req = v5::Request::new();
    req.pos = query.since.clone();
    req.timeout = if query.timeout.is_zero() {
        None
    } else {
        Some(query.timeout)
    };
    req.conn_id = Some(LEGACY_CONN_ID.to_string());

    let mut list = v5::request::List::default();
    list.ranges = vec![(ruma::UInt::from(0u32), ruma::UInt::MAX)];
    // Wildcard required_state: verified to round-trip cleanly through ruma's
    // `StateEventType` / `v5::Request` machinery — see the doc comment on
    // this function and the matching tests in `sliding_sync/tests.rs`.
    list.room_details.required_state = vec![(StateEventType::from("*"), "*".to_string())];
    list.room_details.timeline_limit = ruma::UInt::from(LEGACY_TIMELINE_LIMIT);

    let mut lists = BTreeMap::new();
    lists.insert(LEGACY_LIST_NAME.to_string(), list);
    req.lists = lists;

    req.room_subscriptions = BTreeMap::new();
    req.extensions = Default::default();

    req
}

/// The `conn_id` legacy syncs reserve for themselves. Distinct namespace so
/// the `ConnRegistry` never collides legacy and real sliding-sync sessions
/// under the same `(user_id, conn_id)` key.
const LEGACY_CONN_ID: &str = "__legacy__";

/// Name of the single synthesized list. Arbitrary — the v5 handler keys
/// `Conn::lists` on this name internally and we never read it back.
const LEGACY_LIST_NAME: &str = "all";

/// Per-room timeline cap for the synthesized list. Matches the doc's
/// recommendation and is large enough that single-user complement tests
/// don't lose recent events to truncation.
const LEGACY_TIMELINE_LIMIT: u32 = 50;

/// Translate a v5 sliding-sync response into a legacy `/sync` JSON payload.
///
/// `memberships` is the caller-supplied bucketing map keyed by `OwnedRoomId`
/// (built in phase 2 from `StorageBackend::rooms_with_membership` before
/// invoking the v5 handler).
///
/// Bucketing rules per MSC4186 + the design doc:
/// - `Membership::Invite` → `rooms.invite` with the v5 `invite_state` lifted
///   verbatim under `invite_state.events`. No timeline, no state.
/// - `Membership::Join` → `rooms.join` with `timeline`, `state`,
///   `org.matrix.msc4222.state_after`, plus empty `ephemeral` and
///   `account_data`.
/// - `Membership::Knock` → emitted under `rooms.knock` with
///   `knock_state.events` carrying **stripped** (`type` / `state_key` /
///   `sender` / `content`) projections of v5's `required_state`. The
///   upstream sliding-sync handler does **not** populate `invite_state`
///   for knock rooms (its `is_invited` check matches only `invite`), so
///   `required_state` is the right source field — but v3's `knock_state`
///   is defined as stripped state, not full state events, so we reshape
///   each entry via [`strip_state_event`] before emission.
/// - `Membership::Leave` / `Membership::Ban` → `rooms.leave` with the
///   same per-room shape as a joined room (state delta + final timeline
///   for the kick / leave event).
/// - Rooms in the v5 response without a `memberships` entry: log via
///   `tracing::warn!`, then probe `room.invite_state` as a tiebreaker. If
///   v5 supplied stripped invite state we bucket into `rooms.invite` so
///   those events aren't lost; otherwise we graceful-default to
///   `rooms.join`. Shouldn't normally happen because phase 2 prefills
///   `memberships` from the same query that drives candidate rooms, but
///   covers the race where a fresh invite lands between the pre-query
///   and the v5 call.
///
/// Per-room `prev_batch` is the empty string (no `/messages` impl); `limited`
/// is `false`. `state` and `org.matrix.msc4222.state_after` are dual-emitted
/// with identical contents — see `docs/legacy-sync-stub.md` §"Join/leave
/// room shape" for why.
pub fn translate_response(
    resp: v5::Response,
    memberships: &BTreeMap<OwnedRoomId, Membership>,
) -> Value {
    let mut join = serde_json::Map::new();
    let mut invite = serde_json::Map::new();
    let mut leave = serde_json::Map::new();
    let mut knock = serde_json::Map::new();

    for (room_id, room) in resp.rooms {
        let bucket = match memberships.get(&room_id) {
            Some(Membership::Invite) => Bucket::Invite,
            Some(Membership::Join) => Bucket::Join,
            Some(Membership::Knock) => Bucket::Knock,
            Some(Membership::Leave) | Some(Membership::Ban) => Bucket::Leave,
            None => {
                tracing::warn!(
                    room_id = %room_id,
                    "translate_response: room missing from memberships map; probing invite_state",
                );
                if room.invite_state.is_some() {
                    Bucket::Invite
                } else {
                    Bucket::Join
                }
            }
        };

        let key = room_id.to_string();
        match bucket {
            Bucket::Invite => {
                invite.insert(key, invite_room_shape(&room));
            }
            Bucket::Join => {
                join.insert(key, joined_room_shape(&room));
            }
            Bucket::Leave => {
                leave.insert(key, joined_room_shape(&room));
            }
            Bucket::Knock => {
                knock.insert(key, knock_room_shape(&room));
            }
        }
    }

    json!({
        "next_batch": resp.pos,
        "rooms": {
            "join": Value::Object(join),
            "invite": Value::Object(invite),
            "leave": Value::Object(leave),
            "knock": Value::Object(knock),
        },
        "presence": {"events": []},
        "account_data": {"events": []},
        "to_device": {"events": []},
        "device_lists": {"changed": [], "left": []},
        "device_one_time_keys_count": {},
    })
}

/// Which top-level v3 bucket a v5-response room maps to.
#[derive(PartialEq, Eq)]
enum Bucket {
    Join,
    Invite,
    Leave,
    Knock,
}

/// Shape for a joined or left room. Both buckets use the identical shape;
/// the difference is *which* outer key they live under.
fn joined_room_shape(room: &v5::response::Room) -> Value {
    let state_events: Vec<&ruma::serde::Raw<ruma::events::AnySyncStateEvent>> =
        room.required_state.iter().collect();
    json!({
        "timeline": {
            "events": room.timeline,
            "limited": false,
            "prev_batch": "",
        },
        "state": {
            "events": state_events,
        },
        "org.matrix.msc4222.state_after": {
            "events": state_events,
        },
        "ephemeral": {"events": []},
        "account_data": {"events": []},
    })
}

/// Shape for an invited room. v5's `invite_state` is already stripped state;
/// pass it through verbatim under `invite_state.events`.
fn invite_room_shape(room: &v5::response::Room) -> Value {
    let events: &[ruma::serde::Raw<ruma::events::AnyStrippedStateEvent>] =
        room.invite_state.as_deref().unwrap_or(&[]);
    json!({
        "invite_state": {
            "events": events,
        },
    })
}

/// Shape for a knocked room. The upstream sliding-sync handler does not
/// populate `invite_state` for knock rooms (its `is_invited` check matches
/// only `invite`), so we lift `required_state` instead — the user's own
/// knock member event (and any other state we requested via the wildcard)
/// lives there. Each entry is reshaped through [`strip_state_event`] so the
/// emitted `knock_state.events` carries only the four canonical stripped
/// fields, matching the v3 spec.
fn knock_room_shape(room: &v5::response::Room) -> Value {
    let events: Vec<Value> = room.required_state.iter().map(strip_state_event).collect();
    json!({
        "knock_state": {
            "events": events,
        },
    })
}

/// Strip a v5 `Raw<AnySyncStateEvent>` down to the v3 stripped-state shape
/// (`type`, `state_key`, `sender`, `content` and nothing else). Used to
/// build `rooms.knock.{id}.knock_state.events` — v5's `required_state`
/// carries full state events but the v3 spec defines `knock_state` as
/// stripped state only. Ruma has no general-purpose "strip a typed state
/// event" method (`Raw::cast` requires the concrete content type, which we
/// don't statically know for `AnySyncStateEvent`'s 50+ variants), so we
/// reshape the JSON directly. Missing fields are dropped silently — if a
/// malformed event lacks one of the four, the resulting stripped object is
/// still valid stripped state for whatever fields it does have.
fn strip_state_event(raw: &ruma::serde::Raw<ruma::events::AnySyncStateEvent>) -> serde_json::Value {
    let parsed: serde_json::Value = match serde_json::from_str(raw.json().get()) {
        Ok(v) => v,
        Err(_) => return serde_json::Value::Object(Default::default()),
    };
    let mut stripped = serde_json::Map::new();
    for field in ["type", "state_key", "sender", "content"] {
        if let Some(v) = parsed.get(field) {
            stripped.insert(field.to_string(), v.clone());
        }
    }
    serde_json::Value::Object(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruma::events::AnyStrippedStateEvent;
    use ruma::serde::Raw;
    use ruma::{OwnedRoomId, room_id};
    use serde_json::value::to_raw_value;

    // ----- parse_legacy_query --------------------------------------------

    fn query<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parse_query_empty() {
        let q = parse_legacy_query(&query([]));
        assert_eq!(q.since, None);
        assert_eq!(q.timeout, Duration::ZERO);
        assert!(!q.use_state_after);
    }

    #[test]
    fn parse_query_since_passthrough() {
        let q = parse_legacy_query(&query([("since", "s_token_42")]));
        assert_eq!(q.since.as_deref(), Some("s_token_42"));
    }

    #[test]
    fn parse_query_timeout_custom_ms() {
        let q = parse_legacy_query(&query([("timeout", "30000")]));
        assert_eq!(q.timeout, Duration::from_millis(30_000));
    }

    #[test]
    fn parse_query_timeout_unparseable_falls_back_to_zero() {
        // Garbage strings don't error — legacy /sync clients sometimes
        // send `timeout=` (empty) or non-numeric values; we treat
        // "couldn't parse" the same as "absent" so the request still
        // succeeds with the no-wait default.
        let q = parse_legacy_query(&query([("timeout", "not-a-number")]));
        assert_eq!(q.timeout, Duration::ZERO);
    }

    #[test]
    fn parse_query_use_state_after_plain() {
        let q = parse_legacy_query(&query([("use_state_after", "true")]));
        assert!(q.use_state_after);
    }

    #[test]
    fn parse_query_use_state_after_prefixed() {
        let q = parse_legacy_query(&query([("org.matrix.msc4222.use_state_after", "true")]));
        assert!(q.use_state_after);
    }

    #[test]
    fn parse_query_use_state_after_either_form_truthy() {
        // Both flags present and truthy → still true (no double-toggle).
        let q = parse_legacy_query(&query([
            ("use_state_after", "true"),
            ("org.matrix.msc4222.use_state_after", "true"),
        ]));
        assert!(q.use_state_after);
    }

    #[test]
    fn parse_query_use_state_after_false_when_absent_or_off() {
        let q = parse_legacy_query(&query([("use_state_after", "false")]));
        assert!(!q.use_state_after);
        let q = parse_legacy_query(&query([("use_state_after", "")]));
        assert!(!q.use_state_after);
    }

    #[test]
    fn parse_query_empty_since_treated_as_none() {
        // Legacy clients send `?since=` (empty) on initial sync. Empty
        // string must collapse to `None` — passing `Some("")` through
        // to v5 would be rejected as an invalid `pos` by its u64 parser.
        let q = parse_legacy_query(&query([("since", "")]));
        assert_eq!(q.since, None);
        // Non-empty values still pass through verbatim.
        let q = parse_legacy_query(&query([("since", "abc")]));
        assert_eq!(q.since.as_deref(), Some("abc"));
    }

    #[test]
    fn parse_query_filter_full_state_set_presence_ignored() {
        // These three params are listed in the design doc as "dropped" /
        // "noop". The parser must accept them without erroring and not
        // surface them in `LegacyQuery`.
        let q = parse_legacy_query(&query([
            ("filter", "0"),
            ("full_state", "true"),
            ("set_presence", "offline"),
        ]));
        assert_eq!(q.since, None);
        assert_eq!(q.timeout, Duration::ZERO);
        assert!(!q.use_state_after);
    }

    // ----- synthesize_v5_request -----------------------------------------

    #[test]
    fn synthesize_uses_legacy_conn_id() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        assert_eq!(req.conn_id.as_deref(), Some(LEGACY_CONN_ID));
    }

    #[test]
    fn synthesize_has_single_list_named_all() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        assert_eq!(req.lists.len(), 1);
        assert!(req.lists.contains_key("all"));
    }

    #[test]
    fn synthesize_list_ranges_and_timeline_limit() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        let list = req.lists.get("all").expect("list \"all\" present");
        assert_eq!(
            list.ranges,
            vec![(ruma::UInt::from(0u32), ruma::UInt::MAX)],
            "full window",
        );
        assert_eq!(
            list.room_details.timeline_limit,
            ruma::UInt::from(50u32),
            "timeline_limit fixed at 50 per design doc",
        );
    }

    #[test]
    fn synthesize_required_state_wildcard() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        let list = req.lists.get("all").expect("list present");
        assert_eq!(list.room_details.required_state.len(), 1);
        let (evt_type, state_key) = &list.room_details.required_state[0];
        assert_eq!(evt_type.to_string(), "*");
        assert_eq!(state_key, "*");
    }

    #[test]
    fn synthesize_passes_pos_through() {
        let q = LegacyQuery {
            since: Some("xyz".to_string()),
            ..Default::default()
        };
        let req = synthesize_v5_request(&q);
        assert_eq!(req.pos.as_deref(), Some("xyz"));
    }

    #[test]
    fn synthesize_timeout_none_when_zero() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        assert_eq!(req.timeout, None);
    }

    #[test]
    fn synthesize_timeout_some_when_nonzero() {
        let q = LegacyQuery {
            timeout: Duration::from_millis(12_345),
            ..Default::default()
        };
        let req = synthesize_v5_request(&q);
        assert_eq!(req.timeout, Some(Duration::from_millis(12_345)));
    }

    #[test]
    fn synthesize_room_subscriptions_empty() {
        let req = synthesize_v5_request(&LegacyQuery::default());
        assert!(req.room_subscriptions.is_empty());
    }

    // ----- translate_response --------------------------------------------

    /// Build a `v5::Response` with `pos = pos` and the supplied rooms.
    fn make_response(pos: &str, rooms: Vec<(OwnedRoomId, v5::response::Room)>) -> v5::Response {
        let mut resp = v5::Response::new(pos.to_string());
        for (room_id, room) in rooms {
            resp.rooms.insert(room_id, room);
        }
        resp
    }

    /// Build a minimal `v5::response::Room` carrying one timeline event
    /// (`type = "m.room.message"`) and one required_state event
    /// (`m.room.name`). Returns the room plus the JSON snippets so tests
    /// can assert on shape.
    fn joined_room() -> v5::response::Room {
        let mut r = v5::response::Room::new();
        r.timeline = vec![ruma::serde::Raw::from_json(
            to_raw_value(&json!({
                "type": "m.room.message",
                "event_id": "$msg:example.org",
                "content": {"body": "hi"},
            }))
            .unwrap(),
        )];
        r.required_state = vec![ruma::serde::Raw::from_json(
            to_raw_value(&json!({
                "type": "m.room.name",
                "state_key": "",
                "content": {"name": "Room"},
            }))
            .unwrap(),
        )];
        r
    }

    /// Build a minimal invite-shape `v5::response::Room`. The v5 type
    /// uses `Vec<Raw<AnyStrippedStateEvent>>` for invite_state, so we
    /// pre-strip the events.
    fn invite_room() -> v5::response::Room {
        let mut r = v5::response::Room::new();
        let stripped: Raw<AnyStrippedStateEvent> = Raw::from_json(
            to_raw_value(&json!({
                "type": "m.room.member",
                "state_key": "@u:example.org",
                "sender": "@inviter:example.org",
                "content": {"membership": "invite"},
            }))
            .unwrap(),
        );
        r.invite_state = Some(vec![stripped]);
        r
    }

    #[test]
    fn translate_empty_response_has_only_stubs() {
        let resp = make_response("pos1", vec![]);
        let v = translate_response(resp, &BTreeMap::new());

        assert_eq!(v["next_batch"], "pos1");
        assert_eq!(v["rooms"]["join"], json!({}));
        assert_eq!(v["rooms"]["invite"], json!({}));
        assert_eq!(v["rooms"]["leave"], json!({}));
        assert_eq!(v["rooms"]["knock"], json!({}));
        assert_eq!(v["presence"], json!({"events": []}));
        assert_eq!(v["account_data"], json!({"events": []}));
        assert_eq!(v["to_device"], json!({"events": []}));
        assert_eq!(v["device_lists"], json!({"changed": [], "left": []}));
        assert_eq!(v["device_one_time_keys_count"], json!({}));
    }

    #[test]
    fn translate_next_batch_equals_pos() {
        let resp = make_response("opaque-cursor-xyz", vec![]);
        let v = translate_response(resp, &BTreeMap::new());
        assert_eq!(v["next_batch"], "opaque-cursor-xyz");
    }

    #[test]
    fn translate_joined_room_buckets_to_join_with_dual_state() {
        let r = room_id!("!joined:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), joined_room())]);
        let mut memberships = BTreeMap::new();
        memberships.insert(r.clone(), Membership::Join);

        let v = translate_response(resp, &memberships);
        let join = &v["rooms"]["join"];
        let room_v = &join[r.as_str()];
        assert!(!room_v.is_null(), "room landed in rooms.join");
        assert!(v["rooms"]["invite"][r.as_str()].is_null());
        assert!(v["rooms"]["leave"][r.as_str()].is_null());

        // Both `state` and `state_after` are present with identical
        // contents (single name event).
        let state_events = &room_v["state"]["events"];
        let state_after_events = &room_v["org.matrix.msc4222.state_after"]["events"];
        assert_eq!(state_events, state_after_events);
        assert_eq!(state_events.as_array().map(|a| a.len()), Some(1));

        // Per-room shape sanity.
        assert_eq!(room_v["timeline"]["limited"], json!(false));
        assert_eq!(room_v["timeline"]["prev_batch"], json!(""));
        assert_eq!(
            room_v["timeline"]["events"].as_array().map(|a| a.len()),
            Some(1)
        );
        assert_eq!(room_v["ephemeral"], json!({"events": []}));
        assert_eq!(room_v["account_data"], json!({"events": []}));
    }

    #[test]
    fn translate_knock_buckets_to_knock() {
        // Knock rooms land in `rooms.knock` with `knock_state.events` carrying
        // **stripped** (`type` / `state_key` / `sender` / `content`)
        // projections of v5's `required_state`. The upstream handler doesn't
        // populate `invite_state` for knocks — its `is_invited` check matches
        // only `invite` — so `required_state` is the source field, but the v3
        // spec defines `knock_state` as stripped state, so the translator must
        // reshape each entry rather than passing the full state event through.
        let r = room_id!("!knocked:example.org").to_owned();

        // Build a room whose required_state event carries the full set of
        // server-side fields. Stripping must drop event_id, origin_server_ts,
        // unsigned, prev_content and room_id, keeping only the canonical four.
        let mut room = v5::response::Room::new();
        room.required_state = vec![ruma::serde::Raw::from_json(
            to_raw_value(&json!({
                "type": "m.room.member",
                "state_key": "@knocker:example.org",
                "sender": "@knocker:example.org",
                "content": {"membership": "knock"},
                "event_id": "$knock-evt:example.org",
                "origin_server_ts": 1_700_000_000_000u64,
                "unsigned": {"age": 42},
                "prev_content": {"membership": "leave"},
                "room_id": "!knocked:example.org",
            }))
            .unwrap(),
        )];
        let resp = make_response("p", vec![(r.clone(), room)]);
        let mut memberships = BTreeMap::new();
        memberships.insert(r.clone(), Membership::Knock);

        let v = translate_response(resp, &memberships);
        let room_v = &v["rooms"]["knock"][r.as_str()];
        assert!(!room_v.is_null(), "knock room landed in rooms.knock");
        assert!(
            v["rooms"]["join"][r.as_str()].is_null(),
            "knock room must not appear in rooms.join",
        );
        assert!(
            v["rooms"]["invite"][r.as_str()].is_null(),
            "knock room must not appear in rooms.invite",
        );
        assert!(
            v["rooms"]["leave"][r.as_str()].is_null(),
            "knock room must not appear in rooms.leave",
        );

        // `knock_state.events` length matches the v5 `required_state` length.
        let events = room_v["knock_state"]["events"]
            .as_array()
            .expect("knock_state.events is an array");
        assert_eq!(events.len(), 1);

        // The event must be stripped: only the four canonical fields, no
        // event_id / origin_server_ts / unsigned / prev_content / room_id.
        let evt = &events[0];
        let obj = evt.as_object().expect("event is a JSON object");
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["content", "sender", "state_key", "type"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "knock_state events carry only the four canonical stripped fields",
        );
        assert_eq!(evt["type"], "m.room.member");
        assert_eq!(evt["state_key"], "@knocker:example.org");
        assert_eq!(evt["sender"], "@knocker:example.org");
        assert_eq!(evt["content"], json!({"membership": "knock"}));
        for dropped in [
            "event_id",
            "origin_server_ts",
            "unsigned",
            "prev_content",
            "room_id",
        ] {
            assert!(
                evt.get(dropped).is_none(),
                "stripped event must not carry {dropped}",
            );
        }
    }

    #[test]
    fn strip_state_event_keeps_only_canonical_four_fields() {
        // A `Raw<AnySyncStateEvent>` carrying every server-side field a full
        // state event might have must come out the other side of
        // `strip_state_event` containing **exactly** the four canonical
        // stripped fields — type, state_key, sender, content — and nothing
        // else.
        let raw = ruma::serde::Raw::<ruma::events::AnySyncStateEvent>::from_json(
            to_raw_value(&json!({
                "type": "m.room.name",
                "state_key": "",
                "sender": "@creator:example.org",
                "content": {"name": "Hello"},
                "event_id": "$evt:example.org",
                "origin_server_ts": 1_700_000_000_000u64,
                "unsigned": {"age": 17, "prev_sender": "@x:y"},
                "prev_content": {"name": "Old"},
                "room_id": "!room:example.org",
            }))
            .unwrap(),
        );
        let stripped = strip_state_event(&raw);
        let obj = stripped.as_object().expect("stripped event is an object");
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["content", "sender", "state_key", "type"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "exactly the four canonical fields are kept",
        );
        assert_eq!(stripped["type"], "m.room.name");
        assert_eq!(stripped["state_key"], "");
        assert_eq!(stripped["sender"], "@creator:example.org");
        assert_eq!(stripped["content"], json!({"name": "Hello"}));
        for dropped in [
            "event_id",
            "origin_server_ts",
            "unsigned",
            "prev_content",
            "room_id",
        ] {
            assert!(
                stripped.get(dropped).is_none(),
                "stripped output must drop {dropped}",
            );
        }
    }

    #[test]
    fn strip_state_event_handles_partial_events() {
        // A malformed input missing some of the canonical fields must not
        // panic and must not synthesize the missing ones — only the fields
        // that were actually present pass through.
        let raw = ruma::serde::Raw::<ruma::events::AnySyncStateEvent>::from_json(
            to_raw_value(&json!({
                "type": "m.room.topic",
                "content": {"topic": "lol"},
            }))
            .unwrap(),
        );
        let stripped = strip_state_event(&raw);
        let obj = stripped.as_object().expect("stripped event is an object");
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["content", "type"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "only the fields that were present are kept",
        );
        assert_eq!(stripped["type"], "m.room.topic");
        assert_eq!(stripped["content"], json!({"topic": "lol"}));
        assert!(stripped.get("state_key").is_none());
        assert!(stripped.get("sender").is_none());
    }

    #[test]
    fn translate_unknown_membership_falling_back_does_not_land_in_knock() {
        // The None-branch fallback (missing memberships entry → probe
        // invite_state → join) must never spill into the knock bucket.
        // Cover both fallback sub-paths:
        //   (a) no invite_state → join bucket
        //   (b) invite_state present → invite bucket
        // Neither should populate rooms.knock.
        let r_join = room_id!("!stray-join:example.org").to_owned();
        let r_invite = room_id!("!stray-invite:example.org").to_owned();
        let resp = make_response(
            "p",
            vec![
                (r_join.clone(), joined_room()),
                (r_invite.clone(), invite_room()),
            ],
        );
        let v = translate_response(resp, &BTreeMap::new());

        // Knock bucket stays empty.
        assert_eq!(
            v["rooms"]["knock"].as_object().map(|o| o.len()),
            Some(0),
            "fallback path must not produce knock entries",
        );
        assert!(v["rooms"]["knock"][r_join.as_str()].is_null());
        assert!(v["rooms"]["knock"][r_invite.as_str()].is_null());

        // And the rooms land where the fallback says they should.
        assert!(!v["rooms"]["join"][r_join.as_str()].is_null());
        assert!(!v["rooms"]["invite"][r_invite.as_str()].is_null());
    }

    #[test]
    fn translate_invite_room_lifts_invite_state() {
        let r = room_id!("!invited:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), invite_room())]);
        let mut memberships = BTreeMap::new();
        memberships.insert(r.clone(), Membership::Invite);

        let v = translate_response(resp, &memberships);
        let room_v = &v["rooms"]["invite"][r.as_str()];
        assert!(!room_v.is_null(), "room landed in rooms.invite");
        assert!(v["rooms"]["join"][r.as_str()].is_null());
        assert!(v["rooms"]["leave"][r.as_str()].is_null());

        let events = room_v["invite_state"]["events"]
            .as_array()
            .expect("invite_state.events is an array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "m.room.member");
        assert_eq!(events[0]["content"]["membership"], "invite");
    }

    #[test]
    fn translate_leave_room_buckets_to_leave() {
        let r = room_id!("!left:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), joined_room())]);
        let mut memberships = BTreeMap::new();
        memberships.insert(r.clone(), Membership::Leave);

        let v = translate_response(resp, &memberships);
        assert!(!v["rooms"]["leave"][r.as_str()].is_null());
        assert!(v["rooms"]["join"][r.as_str()].is_null());
        assert!(v["rooms"]["invite"][r.as_str()].is_null());
    }

    #[test]
    fn translate_ban_buckets_to_leave() {
        let r = room_id!("!banned:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), joined_room())]);
        let mut memberships = BTreeMap::new();
        memberships.insert(r.clone(), Membership::Ban);

        let v = translate_response(resp, &memberships);
        assert!(!v["rooms"]["leave"][r.as_str()].is_null());
    }

    #[test]
    fn translate_multiple_rooms_correctly_bucketed() {
        let r_join = room_id!("!join:example.org").to_owned();
        let r_invite = room_id!("!invite:example.org").to_owned();
        let r_leave = room_id!("!leave:example.org").to_owned();
        let resp = make_response(
            "p",
            vec![
                (r_join.clone(), joined_room()),
                (r_invite.clone(), invite_room()),
                (r_leave.clone(), joined_room()),
            ],
        );
        let mut memberships = BTreeMap::new();
        memberships.insert(r_join.clone(), Membership::Join);
        memberships.insert(r_invite.clone(), Membership::Invite);
        memberships.insert(r_leave.clone(), Membership::Leave);

        let v = translate_response(resp, &memberships);
        assert!(!v["rooms"]["join"][r_join.as_str()].is_null());
        assert!(!v["rooms"]["invite"][r_invite.as_str()].is_null());
        assert!(!v["rooms"]["leave"][r_leave.as_str()].is_null());
        assert_eq!(v["rooms"]["join"].as_object().map(|o| o.len()), Some(1));
        assert_eq!(v["rooms"]["invite"].as_object().map(|o| o.len()), Some(1));
        assert_eq!(v["rooms"]["leave"].as_object().map(|o| o.len()), Some(1));
    }

    #[test]
    fn translate_unknown_membership_defaults_to_join() {
        // Phase 2 ought to prefill `memberships` for every room in the v5
        // response, but the helper must degrade gracefully (warn + put in
        // join) rather than drop the room or panic.
        let r = room_id!("!stray:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), joined_room())]);
        let v = translate_response(resp, &BTreeMap::new());
        assert!(
            !v["rooms"]["join"][r.as_str()].is_null(),
            "missing-from-memberships room falls into join bucket",
        );
    }

    #[test]
    fn translate_unknown_membership_with_invite_state_buckets_to_invite() {
        // Race: a fresh invite lands between the phase-2 pre-query and the
        // v5 call, so `memberships` lacks an entry but the v5 response
        // carries stripped invite_state. The helper must bucket into
        // `rooms.invite` and preserve the stripped events rather than
        // dropping them into a join-shaped envelope with empty timeline.
        let r = room_id!("!race-invite:example.org").to_owned();
        let resp = make_response("p", vec![(r.clone(), invite_room())]);
        // Note: empty memberships map — room is missing.
        let v = translate_response(resp, &BTreeMap::new());

        let room_v = &v["rooms"]["invite"][r.as_str()];
        assert!(
            !room_v.is_null(),
            "room with invite_state but no memberships entry landed in rooms.invite",
        );
        assert!(v["rooms"]["join"][r.as_str()].is_null());
        assert!(v["rooms"]["leave"][r.as_str()].is_null());

        let events = room_v["invite_state"]["events"]
            .as_array()
            .expect("invite_state.events is an array");
        assert_eq!(events.len(), 1, "stripped event preserved");
        assert_eq!(events[0]["type"], "m.room.member");
        assert_eq!(events[0]["content"]["membership"], "invite");
    }

    #[test]
    fn translate_stubs_always_present_and_well_shaped() {
        // Verified against the exact shape the design doc pins down.
        let resp = make_response("p", vec![]);
        let v = translate_response(resp, &BTreeMap::new());
        assert_eq!(v["presence"]["events"], json!([]));
        assert_eq!(v["account_data"]["events"], json!([]));
        assert_eq!(v["to_device"]["events"], json!([]));
        assert_eq!(v["device_lists"]["changed"], json!([]));
        assert_eq!(v["device_lists"]["left"], json!([]));
        assert_eq!(v["device_one_time_keys_count"], json!({}));
    }
}
