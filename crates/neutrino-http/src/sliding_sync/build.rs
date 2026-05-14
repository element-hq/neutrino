use std::collections::BTreeMap;
use std::sync::Arc;

use neutrino_common::storage::{Direction, StorageBackend, StoredEvent};
use ruma::UInt;
use ruma::api::client::sync::sync_events::v5::response;
use ruma::api::client::sync::sync_events::v5::{Request, Response};
use ruma::events::{AnySyncStateEvent, AnySyncTimelineEvent, StateEventType};
use ruma::serde::Raw;
use ruma::{OwnedRoomId, UserId};
use tokio::sync::Mutex;

use super::conn::{Conn, ConnKey, ConnRegistry, ListCfg, RoomSent, SubCfg};
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
    let conn_arc = resolve_conn(&state.registry, key, req.pos.as_deref()).await?;
    let mut conn = conn_arc.lock().await;

    apply_sticky(&mut conn, &req);

    let candidate_rooms = candidate_rooms(state, user_id).await?;
    let combined = combined_room_configs(&conn, &candidate_rooms);

    let mut rooms_response = BTreeMap::new();
    for (room_id, combined_cfg) in &combined {
        let is_initial = !conn.sent.contains_key(room_id);
        let (room_result, timeline_events, state_events) =
            build_room(state, room_id, combined_cfg, is_initial).await?;

        let sent = conn.sent.entry(room_id.clone()).or_default();
        update_sent(sent, &timeline_events, &state_events);

        rooms_response.insert(room_id.clone(), room_result);
    }

    // TODO(phase-3): once filters/ranges are honoured, `count` is the size of
    // the *filtered* candidate set, not the raw candidate set.
    let lists_response = conn
        .lists
        .keys()
        .map(|name| {
            let mut list = response::List::default();
            list.count = UInt::try_from(candidate_rooms.len() as u64).unwrap_or(UInt::MIN);
            (name.clone(), list)
        })
        .collect();

    conn.pos = conn.pos.saturating_add(1);
    let pos_token = conn.pos.to_string();

    let mut resp = Response::new(pos_token);
    resp.txn_id = req.txn_id;
    resp.lists = lists_response;
    resp.rooms = rooms_response;
    Ok(resp)
}

/// Either look up an existing connection (validating its pos) or allocate a
/// fresh one. Encapsulates the "is this a new sync or a continuation?" check
/// so `build_response` reads top-down.
async fn resolve_conn(
    registry: &ConnRegistry,
    key: ConnKey,
    req_pos: Option<&str>,
) -> Result<Arc<Mutex<Conn>>, SyncError> {
    let Some(pos_str) = req_pos else {
        return Ok(registry.create(key).await);
    };
    let pos: u64 = pos_str.parse().map_err(|_| SyncError::UnknownPos)?;
    let conn = registry.get(&key).await.ok_or(SyncError::UnknownPos)?;
    // Reject stale tokens. Each response advances pos by one, and the client
    // must echo the most recent value back — anything else means the client
    // missed a response or is replaying an old one.
    if conn.lock().await.pos != pos {
        return Err(SyncError::UnknownPos);
    }
    Ok(conn)
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
///
/// TODO(phase-3): sort by `bump_stamp` desc (recency) before range slicing.
/// TODO(phase-3): include kicked/banned rooms per MSC4186 §"Rooms included in
/// the server list", and previously-joined left rooms if the conn already saw
/// them.
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
/// every list/subscription rule that applies to it.
///
/// Since filters are ignored, every list applies to every candidate (and so
/// has the same effect as one big list for now). The union/max logic still
/// matters once explicit subscriptions are mixed in.
///
/// TODO(phase-3): apply each list's `ranges` to slice the candidate set
/// before mapping to configs.
fn combined_room_configs(conn: &Conn, rooms: &[OwnedRoomId]) -> BTreeMap<OwnedRoomId, CombinedCfg> {
    let mut out: BTreeMap<OwnedRoomId, CombinedCfg> = BTreeMap::new();

    let apply = |out: &mut BTreeMap<OwnedRoomId, CombinedCfg>,
                 room_id: &OwnedRoomId,
                 timeline_limit: usize,
                 required_state: &[(StateEventType, String)]| {
        let entry = out.entry(room_id.clone()).or_insert(CombinedCfg {
            timeline_limit: 0,
            required_state: Vec::new(),
        });
        entry.timeline_limit = entry.timeline_limit.max(timeline_limit);
        for pair in required_state {
            if !entry.required_state.contains(pair) {
                entry.required_state.push(pair.clone());
            }
        }
    };

    for cfg in conn.lists.values() {
        for room_id in rooms {
            apply(&mut out, room_id, cfg.timeline_limit, &cfg.required_state);
        }
    }
    for (room_id, cfg) in &conn.subs {
        apply(&mut out, room_id, cfg.timeline_limit, &cfg.required_state);
    }

    out
}

/// Produce one room's slice of the sync response.
///
/// Returns `(response::Room, timeline_events, state_events)` — the trailing
/// `StoredEvent` slices are handed to `update_sent` so it can record what we
/// emitted without re-parsing the JSON. We clone the events into the response
/// as `Raw<...>` and again into the sent-tracking vecs; cheap and keeps the
/// data flow obvious.
///
/// TODO(phase-4): when `is_initial == false`, diff against `conn.sent`:
///   - drop already-sent timeline events from the response
///   - drop unchanged `required_state` entries (compare by event_id)
///   - set `limited` correctly: true iff we elided older events the client
///     hasn't seen; phase 5 also flips it when the long-poll wakes between
///     batches.
/// TODO(phase-4): emit MSC4186 §StateStub `{type, state_key}` markers for
/// state keys present in `conn.sent.required_state_keys` but absent from
/// current state. (Blocked on ruma v5 — its `required_state` is typed as
/// `Vec<Raw<AnySyncStateEvent>>` with no stub variant; we'd serialise stubs
/// as raw JSON and `cast_unchecked()`.)
/// TODO(phase-4): handle `expanded_timeline` — set when the conn's
/// timeline_limit just grew and we're re-sending older events.
async fn build_room<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &OwnedRoomId,
    cfg: &CombinedCfg,
    is_initial: bool,
) -> Result<(response::Room, Vec<StoredEvent>, Vec<StoredEvent>), SyncError> {
    let mut room = response::Room::new();
    if is_initial {
        room.initial = Some(true);
    }

    // TODO(phase-4): for invited rooms emit `invite_state` (stripped state)
    // instead of timeline. MSC4186 §"Invite/Knock/Rejected Rooms".
    let (mut timeline_events, prev_batch_token) = state
        .store
        .room_messages(room_id, None, Direction::Backward, cfg.timeline_limit)
        .await?;
    timeline_events.reverse();

    let timeline_raw: Vec<Raw<AnySyncTimelineEvent>> = timeline_events
        .iter()
        .map(|e| Raw::<AnySyncTimelineEvent>::from_json(e.json.clone()))
        .collect();
    room.timeline = timeline_raw;
    room.prev_batch = prev_batch_token.map(|t| t.0.to_string());

    let mut state_events: Vec<StoredEvent> = Vec::new();
    if !cfg.required_state.is_empty() {
        let current_state = state.store.current_room_state(room_id).await?;
        for ((evt_type, state_key), ev) in current_state.iter() {
            if required_state_matches(&cfg.required_state, evt_type, state_key) {
                state_events.push(StoredEvent {
                    event_id: ev.event_id.clone(),
                    room_id: ev.room_id.clone(),
                    event_type: ev.event_type.clone(),
                    state_key: ev.state_key.clone(),
                    sender: ev.sender.clone(),
                    origin_server_ts: ev.origin_server_ts,
                    json: ev.json.clone(),
                });
            }
        }
        room.required_state = state_events
            .iter()
            .map(|e| Raw::<AnySyncStateEvent>::from_json(e.json.clone()))
            .collect();
    }

    // bump_stamp = max origin_server_ts across the returned timeline. Opaque
    // to the client per MSC4186 (just sortable); using origin_server_ts means
    // federation-backfilled-old events don't bump the room. See PLAN.md
    // 2026-05-14.
    //
    // TODO(phase-4): fall back to the room's create-event ts (or another
    // stable value) when the timeline is empty — currently fresh joins and
    // invites get `bump_stamp = None` and sort to the bottom.
    let max_ts = timeline_events.iter().map(|e| e.origin_server_ts).max();
    if let Some(ts) = max_ts {
        room.bump_stamp = UInt::try_from(ts).ok();
    }

    // TODO(phase-4): populate `name` and `avatar` from the m.room.name /
    // m.room.avatar state events. Without these the client falls back to
    // heroes (also unimplemented, see PLAN.md) so it'll show raw room IDs.
    // TODO(phase-4): populate `joined_count` / `invited_count` from the
    // state store's membership-indexed methods.

    Ok((room, timeline_events, state_events))
}

/// MSC3575 §"Required State" matching: each `(event_type, state_key)` rule is
/// OR'd against the current state; `"*"` is a wildcard for either field.
///
/// TODO(phase-4): implement the special tokens `$LAZY` / `$ME` — needed only if
/// a client we care about starts sending them. `$LAZY` is paired with
/// lazy_members which is explicitly out of scope per PLAN.md.
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
/// Takes the `StoredEvent`s directly (cheap clones from `build_room`) rather
/// than reaching into the serialised `Raw<...>` payloads. Keeps the data flow
/// readable at the cost of two extra clones.
///
/// TODO(phase-4): this populates `RoomSent` but `build_room` never reads it.
/// Once the delta path is implemented, this is the input that lets us suppress
/// already-sent events.
fn update_sent(
    sent: &mut RoomSent,
    timeline_events: &[StoredEvent],
    state_events: &[StoredEvent],
) {
    for ev in timeline_events {
        sent.timeline_event_ids.push(ev.event_id.clone());
    }
    for ev in state_events {
        if let Some(state_key) = &ev.state_key {
            sent.required_state_keys.insert(
                (ev.event_type.clone(), state_key.clone()),
                ev.event_id.clone(),
            );
        }
    }
}
