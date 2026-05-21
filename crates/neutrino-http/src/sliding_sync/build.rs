use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use neutrino_store::{Direction, StorageBackend, StoredEvent};
use ruma::UInt;
use ruma::api::client::sync::sync_events::v5::response;
use ruma::api::client::sync::sync_events::v5::{Request, Response};
use ruma::events::{AnySyncStateEvent, AnySyncTimelineEvent, StateEventType};
use ruma::serde::Raw;
use ruma::{OwnedRoomId, RoomId, UserId};
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
/// 3. Rank candidate rooms (joined ∪ invited) by recency.
/// 4. Apply each list's `ranges` to slice the ranked set; subscriptions bypass
///    ranges.
/// 5. For each selected room, fetch timeline + filtered current state and build
///    a `response::Room`.
/// 6. Record what we just sent in `conn.sent` so future deltas can diff.
/// 7. Bump the conn's pos token and return it.
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

    let ranked = candidate_rooms(state, user_id).await?;
    let combined = combined_room_configs(&conn, &ranked);

    let mut rooms_response = BTreeMap::new();
    for (room_id, combined_cfg) in &combined {
        let is_initial = !conn.sent.contains_key(room_id);
        let (room_result, timeline_events, state_events) =
            build_room(state, room_id, combined_cfg, is_initial).await?;

        let sent = conn.sent.entry(room_id.clone()).or_default();
        update_sent(sent, &timeline_events, &state_events);

        rooms_response.insert(room_id.clone(), room_result);
    }

    // `count` is the size of the filtered candidate set (before slicing by
    // range). Filters are no-ops in our impl, so it's just `ranked.len()`.
    let lists_response = conn
        .lists
        .keys()
        .map(|name| {
            let mut list = response::List::default();
            list.count = UInt::try_from(ranked.len() as u64).unwrap_or(UInt::MIN);
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
        // MSC4186 removed MSC3575's multi-range support; the wire field is now
        // a singular `range`. Ruma v5's `ranges: Vec` is a half-migrated
        // artefact — we honour only the first entry and silently drop any
        // others. Going strict-with-400 would break clients that haven't fully
        // migrated yet (see PLAN.md/MSC4186-gaps.md notes on ruma's dialect).
        let range = list
            .ranges
            .first()
            .map(|(a, b)| (u64::from(*a) as usize, u64::from(*b) as usize));
        let cfg = ListCfg {
            timeline_limit: u64::from(list.room_details.timeline_limit) as usize,
            required_state: list.room_details.required_state.clone(),
            range,
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

/// One candidate room plus its server-computed sort key.
///
/// `bump_stamp` uses `origin_server_ts` rather than stream position so that
/// federation-backfilled old events don't bump the room to the top (PLAN.md
/// 2026-05-14). The source depends on the user's membership:
/// - **Joined**: most recent event in the room (via `room_messages` Backward
///   limit 1), falling back to the `m.room.create` event ts.
/// - **Invited**: the invitee's own `m.room.member` event ts (i.e. the invite
///   itself) — we deliberately don't peek into the room's full timeline since
///   the user only cares about their own invite for sort purposes, and in
///   practice we may not have the rest of the room's events anyway.
///
/// Rooms with no resolvable source get 0 and sort to the bottom by room_id.
struct RankedRoom {
    room_id: OwnedRoomId,
    bump_stamp: u64,
}

/// Resolved per-room request after merging every rule that mentions it.
/// MSC4186 §"Room Config Combination": when a room matches multiple lists or
/// is also a direct subscription, the server unions the required_state and
/// takes the max timeline_limit. We also carry the pre-computed `bump_stamp`
/// so `build_room` doesn't recompute it.
struct CombinedCfg {
    timeline_limit: usize,
    required_state: Vec<(StateEventType, String)>,
    bump_stamp: u64,
}

/// Rank every room the user can see by recency.
///
/// Joined rooms come from current state; invited rooms from invite events.
/// We don't apply MSC4186's `is_dm`/`is_encrypted`/`spaces`/`tags`/etc.
/// filters — the embedded single-user server returns the full set (PLAN.md
/// 2026-05-14). Sorted by `bump_stamp` desc, tiebroken by `room_id` asc so
/// tests are deterministic.
///
/// TODO(phase-3+): include kicked/banned rooms per MSC4186 §"Rooms included in
/// the server list", and previously-joined left rooms if the conn already saw
/// them. Blocked on a new `StateStore` method like `rooms_with_membership` —
/// trait change, ask first.
async fn candidate_rooms<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
) -> Result<Vec<RankedRoom>, SyncError> {
    let joined = state.store.joined_rooms(user_id).await?;
    let invited = state.store.invited_rooms(user_id).await?;

    let mut seen: HashSet<OwnedRoomId> = HashSet::with_capacity(joined.len() + invited.len());
    let mut ranked: Vec<RankedRoom> = Vec::with_capacity(joined.len() + invited.len());

    for room_id in joined {
        if !seen.insert(room_id.clone()) {
            continue;
        }
        let bump_stamp = bump_stamp_for_joined(state, &room_id).await?;
        ranked.push(RankedRoom {
            room_id,
            bump_stamp,
        });
    }
    for room_id in invited {
        if !seen.insert(room_id.clone()) {
            continue;
        }
        let bump_stamp = bump_stamp_for_invited(state, &room_id, user_id).await?;
        ranked.push(RankedRoom {
            room_id,
            bump_stamp,
        });
    }

    ranked.sort_by(|a, b| {
        b.bump_stamp
            .cmp(&a.bump_stamp)
            .then_with(|| a.room_id.cmp(&b.room_id))
    });
    Ok(ranked)
}

/// Best-available recency stamp for a **joined** room. Peek the most recent
/// event (Backward, limit 1), else fall back to the create event. Returns 0
/// if neither exists — that room sorts to the bottom by room_id.
///
/// **Cost:** called once per candidate room from `candidate_rooms` on every
/// sync request, so this is `O(n)` storage round-trips where `n` is the
/// user's joined+invited room count. Acceptable for the embedded single-user
/// case (small `n`, no concurrency). Future optimisations if it becomes a
/// problem: (a) cache `(room_id → bump_stamp)` in `SyncState` and update on
/// `EventStore::subscribe()` wakeups; (b) add a batched
/// `StateStore::room_bump_stamps(rooms)` trait method; (c) maintain a
/// `bump_stamp` column on the rooms table updated transactionally on every
/// `persist_event`. (c) is the cleanest long-term; (a) is cheapest if storage
/// stays single-process. All are out of scope for phase 3.
async fn bump_stamp_for_joined<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &RoomId,
) -> Result<u64, SyncError> {
    let (events, _) = state
        .store
        .room_messages(room_id, None, Direction::Backward, 1)
        .await?;
    if let Some(ev) = events.first() {
        return Ok(ev.origin_server_ts);
    }
    let create = state
        .store
        .current_state_event(room_id, "m.room.create", "")
        .await?;
    Ok(create.map(|e| e.origin_server_ts).unwrap_or(0))
}

/// Best-available recency stamp for an **invited** room. For invited rooms we
/// haven't joined yet, the timeline and full room state aren't (necessarily)
/// replicated to us, but the invitee's own `m.room.member` event always is —
/// it's how we knew to add the room to the invited set in the first place.
/// Use its `origin_server_ts` as the bump stamp; that's "when this room
/// changed in a way relevant to the user".
async fn bump_stamp_for_invited<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &RoomId,
    user_id: &UserId,
) -> Result<u64, SyncError> {
    let member = state
        .store
        .current_state_event(room_id, "m.room.member", user_id.as_str())
        .await?;
    Ok(member.map(|e| e.origin_server_ts).unwrap_or(0))
}

/// Slice the ranked candidates by each list's `ranges`, union in subscriptions,
/// and merge per-room timeline/required_state configs.
///
/// Subscriptions bypass ranges: an explicitly subscribed room shows up even if
/// it's not in any list's window (MSC4186 §"Subscriptions"). A subscribed room
/// not in `ranked` (shouldn't happen with joined ∪ invited candidates, but
/// could once we add kicked/banned) gets `bump_stamp = 0`.
///
/// An empty `ranges` array means "no slice requested" — we treat that as the
/// full set `[0, len-1]` (defensive: ruma's `List` defaults to empty ranges).
fn combined_room_configs(conn: &Conn, ranked: &[RankedRoom]) -> BTreeMap<OwnedRoomId, CombinedCfg> {
    let mut out: BTreeMap<OwnedRoomId, CombinedCfg> = BTreeMap::new();
    let bump_by_room: HashMap<&OwnedRoomId, u64> =
        ranked.iter().map(|r| (&r.room_id, r.bump_stamp)).collect();

    let apply = |out: &mut BTreeMap<OwnedRoomId, CombinedCfg>,
                 room_id: &OwnedRoomId,
                 bump_stamp: u64,
                 timeline_limit: usize,
                 required_state: &[(StateEventType, String)]| {
        let entry = out.entry(room_id.clone()).or_insert(CombinedCfg {
            timeline_limit: 0,
            required_state: Vec::new(),
            bump_stamp,
        });
        entry.timeline_limit = entry.timeline_limit.max(timeline_limit);
        for pair in required_state {
            if !entry.required_state.contains(pair) {
                entry.required_state.push(pair.clone());
            }
        }
    };

    for cfg in conn.lists.values() {
        let Some((lo, hi)) = effective_range(cfg.range, ranked.len()) else {
            continue;
        };
        for room in ranked.iter().take(hi + 1).skip(lo) {
            apply(
                &mut out,
                &room.room_id,
                room.bump_stamp,
                cfg.timeline_limit,
                &cfg.required_state,
            );
        }
    }
    for (room_id, cfg) in &conn.subs {
        let bump_stamp = bump_by_room.get(room_id).copied().unwrap_or(0);
        apply(
            &mut out,
            room_id,
            bump_stamp,
            cfg.timeline_limit,
            &cfg.required_state,
        );
    }

    out
}

/// Normalise a list's `range` against the actual candidate count: clamp the
/// upper bound to `total - 1`, drop the range if it starts past the end, and
/// drop inverted ranges (`a > b`) since they describe an empty interval that
/// the slicing iterator would silently zero-out. `None` input (no `range`
/// field on the request) becomes the full window `[0, total-1]`. Zero
/// candidates → `None` (caller iterates zero times anyway).
///
/// We *drop* malformed ranges rather than 400'ing because phase 3 has no
/// request-validation step yet; phase 6 may upgrade this to a `BadRequest`.
///
/// Only one range is honoured per list (MSC4186 removed MSC3575's multi-range
/// support; see `apply_sticky`).
fn effective_range(range: Option<(usize, usize)>, total: usize) -> Option<(usize, usize)> {
    if total == 0 {
        return None;
    }
    let Some((a, b)) = range else {
        return Some((0, total - 1));
    };
    if a >= total || a > b {
        return None;
    }
    Some((a, b.min(total - 1)))
}

/// Produce one room's slice of the sync response.
///
/// Returns `(response::Room, timeline_events, state_events)` — the trailing
/// `StoredEvent` slices are handed to `update_sent` so it can record what we
/// emitted without re-parsing the JSON. We clone the events into the response
/// as `Raw<...>` and again into the sent-tracking vecs; cheap and keeps the
/// data flow obvious.
///
/// `cfg.bump_stamp` is already computed in `candidate_rooms`; we use it
/// directly rather than re-deriving it from the timeline window.
///
/// TODO(phase-4): when `is_initial == false`, diff against `conn.sent`:
/// drop already-sent timeline events, drop unchanged `required_state` entries
/// (compare by event_id), set `limited` correctly (true iff we elided older
/// events the client hasn't seen; phase 5 also flips it when the long-poll
/// wakes between batches).
///
/// TODO(phase-4): emit MSC4186 §StateStub `{type, state_key}` markers for
/// state keys present in `conn.sent.required_state_keys` but absent from
/// current state. Blocked on ruma v5 — its `required_state` is typed as
/// `Vec<Raw<AnySyncStateEvent>>` with no stub variant; we'd serialise stubs
/// as raw JSON and `cast_unchecked()`.
///
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

    if cfg.bump_stamp > 0 {
        room.bump_stamp = UInt::try_from(cfg.bump_stamp).ok();
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
        let rule_type = rule_type.to_string();
        let type_match = rule_type == "*" || rule_type == evt_type;
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
fn update_sent(sent: &mut RoomSent, timeline_events: &[StoredEvent], state_events: &[StoredEvent]) {
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

#[cfg(test)]
mod unit_tests {
    use super::effective_range;

    #[test]
    fn effective_range_none_input_full_window() {
        assert_eq!(effective_range(None, 5), Some((0, 4)));
    }

    #[test]
    fn effective_range_zero_total_returns_none() {
        assert_eq!(effective_range(Some((0, 4)), 0), None);
        assert_eq!(effective_range(None, 0), None);
    }

    #[test]
    fn effective_range_clamps_upper_bound() {
        assert_eq!(effective_range(Some((0, 99)), 3), Some((0, 2)));
    }

    #[test]
    fn effective_range_drops_fully_out_of_range() {
        // start ≥ total → drop.
        assert_eq!(effective_range(Some((10, 20)), 5), None);
    }

    #[test]
    fn effective_range_drops_inverted_range() {
        // a > b describes an empty interval; the slicing iterator would
        // silently zero-out. Drop explicitly so the malformed case is visible.
        assert_eq!(effective_range(Some((5, 2)), 10), None);
        // a == b is a valid single-element range, not inverted.
        assert_eq!(effective_range(Some((3, 3)), 10), Some((3, 3)));
    }
}
