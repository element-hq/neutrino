use std::collections::{BTreeMap, BTreeSet};

use neutrino_common::storage::{Direction, StorageBackend};
use ruma::UInt;
use ruma::api::client::sync::sync_events::v5::response;
use ruma::api::client::sync::sync_events::v5::{Request, Response};
use ruma::events::{AnySyncStateEvent, AnySyncTimelineEvent, StateEventType};
use ruma::serde::Raw;
use ruma::{OwnedRoomId, UserId};

use super::conn::{Conn, ConnKey, ListCfg, RoomSent, SubCfg};
use super::{SyncError, SyncState};

/// Build one sliding-sync response.
///
/// Flow:
/// 1. Resolve or create the `Conn` for `(user_id, conn_id)`. Absent `pos` →
///    new conn. Present `pos` → must match the conn's most-recently-issued
///    token, else `UnknownPos` (forces the client to reconnect).
/// 2. Merge the request's list/sub configs into the conn (sticky update).
/// 3. Enumerate candidate rooms (joined ∪ invited; no filtering).
/// 4. For each room, fetch timeline + filtered current state and build a
///    `response::Room`.
/// 5. Record what we just sent in `conn.sent` so future deltas can diff.
/// 6. Bump the conn's pos token and return it.
pub(super) async fn build_response<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: Request,
) -> Result<Response, SyncError> {
    let key = ConnKey {
        user_id: user_id.to_owned(),
        conn_id: req.conn_id.clone().unwrap_or_default(),
    };

    let conn_arc = match req.pos {
        None => state.registry.create(key).await,
        Some(ref pos_str) => {
            let pos: u64 = pos_str.parse().map_err(|_| SyncError::UnknownPos)?;
            let conn_arc = state
                .registry
                .get(&key)
                .await
                .ok_or(SyncError::UnknownPos)?;
            // Reject stale tokens. Each response advances pos by one, and the
            // client must echo the most recent value back — anything else means
            // the client missed a response or is replaying an old one.
            if conn_arc.lock().await.last_stream_pos != pos {
                return Err(SyncError::UnknownPos);
            }
            conn_arc
        }
    };

    let mut conn = conn_arc.lock().await;

    apply_sticky(&mut conn, &req);

    let candidate_rooms = candidate_rooms(state, user_id).await?;
    let combined = combined_room_configs(&conn, &candidate_rooms);

    let mut rooms_response = BTreeMap::new();

    for room_id in combined.keys() {
        let combined_cfg = combined.get(room_id).expect("present");
        let is_initial = !conn.sent.contains_key(room_id);

        let room_result = build_room(state, room_id, combined_cfg, is_initial).await?;

        let sent = conn.sent.entry(room_id.clone()).or_default();
        update_sent(sent, &room_result);

        rooms_response.insert(room_id.clone(), room_result);
    }

    let lists_response = conn
        .lists
        .keys()
        .map(|name| {
            let mut list = response::List::default();
            list.count = UInt::try_from(candidate_rooms.len() as u64).unwrap_or(UInt::MIN);
            (name.clone(), list)
        })
        .collect();

    conn.last_stream_pos = conn.last_stream_pos.saturating_add(1);
    let pos_token = conn.last_stream_pos.to_string();

    let mut resp = Response::new(pos_token);
    resp.txn_id = req.txn_id;
    resp.lists = lists_response;
    resp.rooms = rooms_response;
    Ok(resp)
}

/// Merge per-request sticky params into the connection's stored state.
///
/// MSC4186 lists and subscriptions are sticky — a single appearance keeps them
/// active across subsequent requests. We `insert` (not `merge field-by-field`)
/// because each list/sub re-send fully replaces its prior value. Lists that
/// stop appearing in requests stay active forever in our impl; MSC4186 doesn't
/// require deletion semantics.
fn apply_sticky(conn: &mut Conn, req: &Request) {
    for (name, list) in &req.lists {
        let cfg = ListCfg {
            timeline_limit: u64::from(list.room_details.timeline_limit) as usize,
            required_state: list.room_details.required_state.clone(),
            ranges: list
                .ranges
                .iter()
                .map(|(a, b)| (u64::from(*a) as usize, u64::from(*b) as usize))
                .collect(),
            filters: list.filters.clone(),
        };
        conn.lists.insert(name.clone(), cfg);
    }

    for (room_id, sub) in &req.room_subscriptions {
        let cfg = SubCfg {
            timeline_limit: u64::from(sub.timeline_limit) as usize,
            required_state: sub.required_state.clone(),
        };
        conn.subs.insert(room_id.clone(), cfg);
    }
}

/// Resolved per-room request after merging every rule that mentions it.
/// MSC4186 §"Room Config Combination": when a room matches multiple lists or
/// is also a direct subscription, the server unions the required_state and
/// takes the max timeline_limit.
struct CombinedCfg {
    timeline_limit: usize,
    required_state: Vec<(StateEventType, String)>,
}

/// Every room the user can see for sync purposes.
///
/// Joined rooms come from current state; invited rooms from invite events. We
/// don't apply any of MSC4186's `is_dm`/`is_encrypted`/`spaces`/`tags`/etc.
/// filters — the embedded single-user server returns the full set (see PLAN.md
/// 2026-05-14). Sorting/dedup keeps the output deterministic for tests and for
/// future range slicing.
async fn candidate_rooms<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
) -> Result<Vec<OwnedRoomId>, SyncError> {
    let mut rooms = state.store.joined_rooms(user_id).await?;
    rooms.extend(state.store.invited_rooms(user_id).await?);
    rooms.sort();
    rooms.dedup();
    Ok(rooms)
}

/// For each candidate room, compute the effective request config by unioning
/// every list/subscription rule that applies to it. Since filters are ignored,
/// every list applies to every candidate; phase 3 will restrict by `ranges`.
fn combined_room_configs(conn: &Conn, rooms: &[OwnedRoomId]) -> BTreeMap<OwnedRoomId, CombinedCfg> {
    let mut out: BTreeMap<OwnedRoomId, CombinedCfg> = BTreeMap::new();

    for cfg in conn.lists.values() {
        for room_id in rooms {
            merge_into(&mut out, room_id, cfg.timeline_limit, &cfg.required_state);
        }
    }

    for (room_id, cfg) in &conn.subs {
        merge_into(&mut out, room_id, cfg.timeline_limit, &cfg.required_state);
    }

    out
}

fn merge_into(
    map: &mut BTreeMap<OwnedRoomId, CombinedCfg>,
    room_id: &OwnedRoomId,
    timeline_limit: usize,
    required_state: &[(StateEventType, String)],
) {
    let entry = map.entry(room_id.clone()).or_insert(CombinedCfg {
        timeline_limit: 0,
        required_state: Vec::new(),
    });
    if timeline_limit > entry.timeline_limit {
        entry.timeline_limit = timeline_limit;
    }
    let existing: BTreeSet<(StateEventType, String)> =
        entry.required_state.iter().cloned().collect();
    for pair in required_state {
        if !existing.contains(pair) {
            entry.required_state.push(pair.clone());
        }
    }
}

/// Produce one room's slice of the sync response.
///
/// `is_initial` controls the `initial: true` flag (MSC4186 §"Room Result"). It
/// does **not** currently affect what we return — every emission ships the full
/// timeline window and full filtered required_state. Phase 4 will use
/// `RoomSent` to suppress already-sent events on subsequent emissions.
///
/// Timeline is fetched Backward then reversed, since MSC4186 wants the most
/// recent events last in the array.
///
/// `bump_stamp` is set to the max `origin_server_ts` across the returned
/// timeline. The MSC notes bump_stamp "is not a timestamp" but treats the field
/// as opaque to clients (just sortable). Using `origin_server_ts` has the
/// pleasant property that federation-backfilled-old events don't bump the room
/// to the top of the list (their timestamps are old). Documented in PLAN.md
/// 2026-05-14.
async fn build_room<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &OwnedRoomId,
    cfg: &CombinedCfg,
    is_initial: bool,
) -> Result<response::Room, SyncError> {
    let mut room = response::Room::new();
    if is_initial {
        room.initial = Some(true);
    }

    let (mut events, prev_batch_token) = state
        .store
        .room_messages(room_id, None, Direction::Backward, cfg.timeline_limit)
        .await?;
    events.reverse();
    let mut max_origin_server_ts: u64 = 0;
    let timeline_raw: Vec<Raw<AnySyncTimelineEvent>> = events
        .iter()
        .map(|e| {
            if e.origin_server_ts > max_origin_server_ts {
                max_origin_server_ts = e.origin_server_ts;
            }
            Raw::<AnySyncTimelineEvent>::from_json(e.json.clone())
        })
        .collect();
    room.timeline = timeline_raw;
    room.prev_batch = prev_batch_token.map(|t| t.0.to_string());

    if !cfg.required_state.is_empty() {
        let current_state = state.store.current_room_state(room_id).await?;
        let state_raw: Vec<Raw<AnySyncStateEvent>> = current_state
            .iter()
            .filter(|((evt_type, state_key), _)| {
                required_state_matches(&cfg.required_state, evt_type, state_key)
            })
            .map(|(_, ev)| Raw::<AnySyncStateEvent>::from_json(ev.json.clone()))
            .collect();
        room.required_state = state_raw;
    }

    if max_origin_server_ts > 0 {
        room.bump_stamp = UInt::try_from(max_origin_server_ts).ok();
    }

    Ok(room)
}

/// MSC3575 §"Required State" matching: each `(event_type, state_key)` rule is
/// OR'd against the current state; `"*"` is a wildcard for either field. We
/// don't yet implement the special tokens `$LAZY`/`$ME` (no client we're
/// targeting sends them, and `$LAZY` only matters with lazy_members which is
/// out of scope).
fn required_state_matches(
    rules: &[(StateEventType, String)],
    evt_type: &str,
    state_key: &str,
) -> bool {
    for (rule_type, rule_key) in rules {
        let type_match = rule_type.to_string() == "*" || rule_type.to_string() == evt_type;
        let key_match = rule_key == "*" || rule_key == state_key;
        if type_match && key_match {
            return true;
        }
    }
    false
}

/// Record what was just emitted to this connection for this room.
///
/// Populates `RoomSent` from the `response::Room` we're about to return so that
/// future sync requests on the same connection can compute deltas without
/// re-sending old data. See `RoomSent`'s docs for unbounded-growth caveats.
///
/// We pull `event_id` out of the `Raw<...>` payload rather than threading the
/// `StoredEvent` through `build_room` because the response struct is the source
/// of truth for "what we actually serialised" — bugs in formatting would
/// otherwise drift silently between sent state and the wire.
fn update_sent(sent: &mut RoomSent, room: &response::Room) {
    for ev in &room.timeline {
        if let Ok(Some(id)) = ev.get_field::<ruma::OwnedEventId>("event_id") {
            sent.timeline_event_ids.push(id);
        }
    }
    for ev in &room.required_state {
        if let (Ok(Some(evt_type)), Ok(Some(state_key)), Ok(Some(event_id))) = (
            ev.get_field::<String>("type"),
            ev.get_field::<String>("state_key"),
            ev.get_field::<ruma::OwnedEventId>("event_id"),
        ) {
            sent.required_state_keys
                .insert((evt_type, state_key), event_id);
        }
    }
}
