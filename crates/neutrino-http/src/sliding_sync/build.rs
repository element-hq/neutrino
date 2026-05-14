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

struct CombinedCfg {
    timeline_limit: usize,
    required_state: Vec<(StateEventType, String)>,
}

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

fn update_sent(sent: &mut RoomSent, room: &response::Room) {
    for ev in &room.timeline {
        if let Ok(event_id) = ev.get_field::<ruma::OwnedEventId>("event_id") {
            if let Some(id) = event_id {
                sent.timeline_event_ids.push(id);
            }
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
