use std::collections::{BTreeMap, HashMap, HashSet};

use neutrino_store::{Direction, StorageBackend, StoredEvent, StreamPos};
use ruma::UInt;
use ruma::api::client::sync::sync_events::v5::response;
use ruma::api::client::sync::sync_events::v5::{Request, Response};
use ruma::events::{
    AnyStrippedStateEvent, AnySyncStateEvent, AnySyncTimelineEvent, StateEventType,
};
use ruma::serde::Raw;
use ruma::{OwnedRoomId, RoomId, UserId};
use serde_json::value::RawValue;

use super::conn::{Conn, ListCfg, RoomSent, SubCfg};
use super::{SyncError, SyncState};

/// How many globally-new events we drain from the store per sync request.
/// Mobile-scale: the embedded server processes events at modest rates, and a
/// short-polling client requests every few seconds, so 1000 events per
/// request is generously over-sized in practice. If we ever hit this ceiling
/// the bookkeeping still advances correctly (we just process the oldest 1000
/// this sync and the rest next time), but `limited` won't get set globally —
/// only per-room when we cap that room's timeline at `timeline_limit`.
const EVENTS_PER_SYNC_LIMIT: usize = 1000;

/// Whether to surface MSC4186 §StateStub `{type, state_key}` markers when a
/// previously-sent state key disappears from current state.
///
/// **Set to `false`** because the practical failure mode of *not* informing
/// the client is mild — the client keeps its last view of that key until the
/// state is re-set to something new, at which point the diff path picks up
/// the change and emits the new event. Setting it `true` would surface
/// genuine deletions promptly, at the cost of pushing a JSON shape (`{"type",
/// "state_key"}` with no `content`) that not every client handles uniformly.
/// Flip when we have a confirmed need from a target client.
const EMIT_STATE_STUBS: bool = false;

/// Build one sliding-sync response into the supplied connection.
///
/// The caller is responsible for resolving the `Conn` (see
/// `mod::handle::resolve_conn`) and holding its lock for the duration of the
/// call. This split exists so that the long-poll loop in `handle` can call
/// `build_response` multiple times against the same locked conn, and so that
/// the idempotency-cache check can short-circuit before we ever enter this
/// path.
///
/// Flow:
/// 1. Merge the request's list/sub configs into the conn (sticky update).
/// 2. Fetch globally-new events via `events_after(last_event_stream_pos)`,
///    group by room. This drives the timeline delta path.
/// 3. Rank candidate rooms (joined ∪ invited) by recency, slice by ranges.
/// 4. For each selected room, build a `response::Room` — initial syncs get a
///    full snapshot via `room_messages`, deltas get only the new events.
///    Rooms with no updates (and already known) are omitted entirely.
/// 5. Record what we just sent in `conn.sent` so future deltas can diff.
/// 6. Bump `conn.pos` and `conn.last_event_stream_pos`, return.
pub(super) async fn build_response<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    req: &Request,
    conn: &mut Conn,
) -> Result<Response, SyncError> {
    let initial_sync = req.pos.is_none();
    apply_sticky(conn, req);

    // Drain new events since our high-water mark and group them by room.
    // On initial sync we skip the drain entirely — the per-room snapshot
    // comes from `room_messages`, not from `events_after` — and instead
    // anchor `last_event_stream_pos` at the store's current head via the
    // watch's borrowed value. Previously we drained up to
    // `EVENTS_PER_SYNC_LIMIT` and advanced only that far, which on a store
    // holding > 1000 events would have the next sync re-emit positions
    // 1001+ as "new" deltas the client already received in the snapshot.
    let (new_events_by_room, max_stream_pos) = if initial_sync {
        let head = state.store.subscribe().borrow().0;
        (HashMap::new(), head)
    } else {
        fetch_event_deltas(state, conn.last_event_stream_pos).await?
    };
    if max_stream_pos > conn.last_event_stream_pos {
        conn.last_event_stream_pos = max_stream_pos;
    }

    let ranked = candidate_rooms(state, user_id, conn).await?;
    let combined = combined_room_configs(conn, &ranked);

    let mut rooms_response = BTreeMap::new();
    for (room_id, combined_cfg) in &combined {
        let is_initial_for_room = !conn.sent.contains_key(room_id);
        let room_delta: &[StoredEvent] = new_events_by_room
            .get(room_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let sent_snapshot = conn.sent.get(room_id).cloned();

        let built = build_room(
            state,
            user_id,
            room_id,
            combined_cfg,
            is_initial_for_room,
            initial_sync,
            room_delta,
            sent_snapshot.as_ref(),
        )
        .await?;

        let Some((room_result, state_events, deleted_state_keys)) = built else {
            continue;
        };

        let sent = conn.sent.entry(room_id.clone()).or_default();
        update_sent(sent, &state_events, &deleted_state_keys);
        rooms_response.insert(room_id.clone(), room_result);
    }

    // `count` is the size of the filtered candidate set (before slicing by
    // range). Filters are no-ops in our impl, so it's just `ranked.len()`.
    let lists_response: BTreeMap<_, _> = conn
        .lists
        .keys()
        .map(|name| {
            let mut list = response::List::default();
            list.count = UInt::try_from(ranked.len() as u64).unwrap_or(UInt::MIN);
            (name.clone(), list)
        })
        .collect();

    // Remember each list's current timeline_limit for next-time
    // expanded_timeline detection (data tracked but not yet surfaced — ruma
    // v5 doesn't carry the `expanded_timeline` field; see MSC4186-gaps.md).
    let snapshot: Vec<(String, usize)> = conn
        .lists
        .iter()
        .map(|(n, cfg)| (n.clone(), cfg.timeline_limit))
        .collect();
    for (name, limit) in snapshot {
        conn.prev_list_timeline_limits.insert(name, limit);
    }

    // The pos token reflects what the conn *will* be advanced to once the
    // caller commits this response. We deliberately don't mutate `conn.pos`
    // here — the long-poll loop in `super::handle` may call this function
    // multiple times per request, but only the final response gets returned
    // to the client. Committing the bump there means each request consumes
    // exactly one pos value, and a mid-loop storage error leaves conn.pos
    // untouched so the client's last-good token still matches.
    let pos_token = conn.pos.saturating_add(1).to_string();

    let mut resp = Response::new(pos_token);
    resp.txn_id = req.txn_id.clone();
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

/// Drain everything in the event stream past `from_pos` and group by room.
///
/// Returns the per-room delta map plus the highest `StreamPos` seen so the
/// caller can advance the conn's high-water mark.
async fn fetch_event_deltas<S: StorageBackend>(
    state: &SyncState<S>,
    from_pos: u64,
) -> Result<(HashMap<OwnedRoomId, Vec<StoredEvent>>, u64), SyncError> {
    let events = state
        .store
        .events_after(StreamPos(from_pos), EVENTS_PER_SYNC_LIMIT)
        .await?;
    let mut max_pos = from_pos;
    let mut by_room: HashMap<OwnedRoomId, Vec<StoredEvent>> = HashMap::new();
    for (pos, ev) in events {
        if pos.0 > max_pos {
            max_pos = pos.0;
        }
        by_room.entry(ev.room_id.clone()).or_default().push(ev);
    }
    Ok((by_room, max_pos))
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
/// Applies MSC4186 §"Rooms included in the server list":
/// - **Join / invite / knock:** always included.
/// - **Kick** (`m.room.member.content.membership == "leave"`, `sender != user`):
///   always included so the client can render the notification.
/// - **Self-leave** (`membership == "leave"`, `sender == user`): included
///   only if we've previously emitted the room on this connection
///   (`conn.sent.contains_key`). MSC4186: "Left rooms, unless previously
///   sent to this connection".
/// - **Ban** (`membership == "ban"`): included if previously emitted on
///   this connection. The spec says "previously joined"; we approximate
///   with "previously emitted" because we don't keep a separate
///   per-conn history of historical join events. False negatives are
///   possible after a restart (the conn is fresh, so prior emissions are
///   lost) — documented in MSC4186-gaps.md.
///
/// We don't apply MSC4186's `is_dm`/`is_encrypted`/`spaces`/`tags`/etc.
/// filters — the embedded single-user server returns the full set (PLAN.md
/// 2026-05-14). Sorted by `bump_stamp` desc, tiebroken by `room_id` asc so
/// tests are deterministic.
async fn candidate_rooms<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    conn: &Conn,
) -> Result<Vec<RankedRoom>, SyncError> {
    // One round-trip for the union across all five MSC4186-eligible
    // memberships. The store hands back `(room, current_membership)`
    // pairs so we can branch on membership without a second lookup.
    let pairs = state
        .store
        .rooms_with_membership(user_id, &["join", "invite", "knock", "leave", "ban"])
        .await?;

    let mut included: Vec<(OwnedRoomId, String)> = Vec::with_capacity(pairs.len());
    for (room_id, membership) in pairs {
        if include_room_per_msc4186(state, user_id, &room_id, &membership, conn).await? {
            included.push((room_id, membership));
        }
    }
    included.sort_by(|a, b| a.0.cmp(&b.0));

    let mut ranked: Vec<RankedRoom> = Vec::with_capacity(included.len());
    for (room_id, membership) in included {
        // For rooms we've never joined (`invite`, `knock`) we don't have the
        // create event or a replicated timeline — fall back to the user's own
        // member-event ts. For everything else (join, leave/kick, ban) the
        // timeline + create are available, so the joined path applies.
        let bump_stamp = if membership == "invite" || membership == "knock" {
            bump_stamp_for_invited(state, &room_id, user_id).await?
        } else {
            bump_stamp_for_joined(state, &room_id).await?
        };
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

/// Decide whether to include a room with the given current membership,
/// per MSC4186 §"Rooms included in the server list". For `leave` we have
/// to look at the member event itself to distinguish a kick (always
/// include) from a self-leave (only if previously emitted on this conn).
async fn include_room_per_msc4186<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    room_id: &RoomId,
    membership: &str,
    conn: &Conn,
) -> Result<bool, SyncError> {
    match membership {
        "join" | "invite" | "knock" => Ok(true),
        "leave" => {
            let kicked = is_kick(state, room_id, user_id).await?;
            Ok(kicked || conn.sent.contains_key(room_id))
        }
        "ban" => Ok(conn.sent.contains_key(room_id)),
        _ => Ok(false),
    }
}

/// True iff the current `m.room.member` event for `user_id` in `room_id`
/// has a different sender than the target — i.e. somebody else flipped us
/// to "leave". Caller is responsible for only invoking this when the user's
/// current membership is already known to be "leave"; we don't re-parse the
/// JSON to check, since `StoredEvent.sender` is the typed canonical value
/// the storage layer already extracted.
async fn is_kick<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &RoomId,
    user_id: &UserId,
) -> Result<bool, SyncError> {
    let Some(ev) = state
        .store
        .current_state_event(room_id, "m.room.member", user_id.as_str())
        .await?
    else {
        return Ok(false);
    };
    Ok(ev.sender.as_str() != user_id.as_str())
}

/// Best-available recency stamp for a **joined** (or previously-joined: kick
/// / self-leave / ban) room. Peek the most recent event (Backward, limit 1),
/// else fall back to the create event. Returns 0 if neither exists — that
/// room sorts to the bottom by room_id.
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

/// Slice the ranked candidates by each list's `range`, union in subscriptions,
/// and merge per-room timeline/required_state configs.
///
/// Subscriptions bypass ranges: an explicitly subscribed room shows up even if
/// it's not in any list's window (MSC4186 §"Subscriptions"). A subscribed room
/// not in `ranked` (shouldn't happen with joined ∪ invited candidates, but
/// could once we add kicked/banned) gets `bump_stamp = 0`.
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
/// Returns `(response::Room, state_events, deleted_state_keys)`:
/// - `state_events` are the state events we just emitted (caller records them
///   in `conn.sent` for future diffing).
/// - `deleted_state_keys` are `(type, state_key)` pairs that were sent before
///   but are no longer current — caller removes them from `conn.sent`.
///
/// Returns `None` when the room has no updates worth sending (already known,
/// no new timeline events, no state changes) — MSC4186 §"Room Matching Rules"
/// excludes such rooms from the response.
///
/// **Invite rooms** (user's current membership is `invite`): we emit
/// `invite_state` instead of timeline, populated from a curated state set
/// (create + name + avatar + inviter member + receiver member). MSC4186
/// §"Invite/Knock/Rejected Rooms".
///
/// **Joined rooms, initial emission:** full snapshot via
/// `room_messages(.., Backward, timeline_limit)`. `limited = prev_batch.is_some()`.
///
/// **Joined rooms, delta emission:** timeline from `room_delta` (the
/// caller-supplied slice of `events_after`), capped at `timeline_limit`.
/// `limited = true` iff we capped.
#[allow(clippy::too_many_arguments)] // each arg is genuinely distinct
async fn build_room<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    room_id: &OwnedRoomId,
    cfg: &CombinedCfg,
    is_initial_for_room: bool,
    is_initial_sync: bool,
    room_delta: &[StoredEvent],
    sent_snapshot: Option<&RoomSent>,
) -> Result<Option<(response::Room, Vec<StoredEvent>, Vec<(String, String)>)>, SyncError> {
    let invited = is_invited(state, user_id, room_id).await?;

    if invited {
        return build_invite_room(state, user_id, room_id, cfg, is_initial_for_room).await;
    }

    let mut room = response::Room::new();
    if is_initial_for_room {
        room.initial = Some(true);
    }

    // ---- Timeline ----
    let (timeline_events, prev_batch_str, limited) = if is_initial_for_room {
        let (mut events, prev_batch_token) = state
            .store
            .room_messages(room_id, None, Direction::Backward, cfg.timeline_limit)
            .await?;
        events.reverse();
        let limited = prev_batch_token.is_some();
        let token = prev_batch_token.map(|t| t.0.to_string());
        (events, token, limited)
    } else {
        let total_new = room_delta.len();
        let (events, limited) = if total_new > cfg.timeline_limit {
            // Drop the older events that don't fit; client backfills via
            // `prev_batch` if it wants them.
            let start = total_new - cfg.timeline_limit;
            (room_delta[start..].iter().map(clone_event).collect(), true)
        } else {
            (room_delta.iter().map(clone_event).collect(), false)
        };
        (events, None, limited)
    };

    let timeline_raw: Vec<Raw<AnySyncTimelineEvent>> = timeline_events
        .iter()
        .map(|e| Raw::<AnySyncTimelineEvent>::from_json(e.json.clone()))
        .collect();
    room.timeline = timeline_raw;
    room.prev_batch = prev_batch_str;
    room.limited = limited;
    // `num_live`: MSC4186 = recent live events. Initial sync events are
    // historical from the client's POV; delta events are live.
    if !is_initial_for_room && !timeline_events.is_empty() {
        room.num_live = UInt::try_from(timeline_events.len() as u64).ok();
    }

    // ---- Required state diff ----
    let current_state = state.store.current_room_state(room_id).await?;
    let (state_events, mut deleted_state_keys) =
        diff_required_state(&current_state, &cfg.required_state, sent_snapshot);
    if !EMIT_STATE_STUBS {
        // Drop the list so the rest of the pipeline behaves as if no
        // deletions occurred: no stubs in `required_state`, no "deletion-only"
        // emissions, no `RoomSent` entries removed via `update_sent`.
        deleted_state_keys.clear();
    }

    if !state_events.is_empty() || !deleted_state_keys.is_empty() {
        let mut raw: Vec<Raw<AnySyncStateEvent>> = state_events
            .iter()
            .map(|e| Raw::<AnySyncStateEvent>::from_json(e.json.clone()))
            .collect();
        for (t, k) in &deleted_state_keys {
            raw.push(state_stub_raw(t, k));
        }
        room.required_state = raw;
    }

    // ---- Skip-or-emit decision ----
    // MSC4186 §"Room Matching Rules": a room only goes in the response if
    // it's new to the connection OR has updates since last return.
    let has_updates = is_initial_for_room
        || !timeline_events.is_empty()
        || !state_events.is_empty()
        || !deleted_state_keys.is_empty();
    if !has_updates {
        return Ok(None);
    }

    // ---- Name / avatar / counts / bump_stamp ----
    populate_room_metadata(state, room_id, &mut room, &current_state, cfg.bump_stamp).await?;

    // `num_live` for initial sync stays unset (the events are historical
    // from the client's perspective even though they're in the timeline).
    if is_initial_sync && is_initial_for_room {
        room.num_live = None;
    }

    Ok(Some((room, state_events, deleted_state_keys)))
}

/// Whether `user_id`'s current `m.room.member` event in `room_id` is `invite`.
/// A malformed event (parse failure, missing/typed-wrong `content.membership`)
/// degrades to `false` — a single bad row shouldn't take the whole sync down
/// with a 500.
async fn is_invited<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    room_id: &RoomId,
) -> Result<bool, SyncError> {
    let ev = state
        .store
        .current_state_event(room_id, "m.room.member", user_id.as_str())
        .await?;
    let Some(ev) = ev else { return Ok(false) };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(ev.json.get()) else {
        return Ok(false);
    };
    Ok(parsed
        .pointer("/content/membership")
        .and_then(|v| v.as_str())
        == Some("invite"))
}

/// MSC4186 §"Invite/Knock/Rejected Rooms": invited rooms return
/// `invite_state` (stripped state) instead of a timeline.
///
/// **Source of the stripped state.** Per the Matrix S2S spec, when a remote
/// server invites our user it calls `PUT /_matrix/federation/v2/invite/...`
/// with the invite `m.room.member` event carrying an
/// `unsigned.invite_room_state` array of already-stripped state events
/// (`m.room.create`, `m.room.name`, `m.room.avatar`, the inviter's
/// membership, etc.). That array is the *only* room context we have —
/// we have not joined the room, so we don't have its full state in the
/// DB. We pass those events through verbatim and additionally include a
/// freshly-stripped copy of the invite `m.room.member` event itself so the
/// client can show "Bob invited you" without parsing the array.
///
/// Once the invite is accepted, the full state arrives via federation and
/// the room enters the normal joined-room path.
async fn build_invite_room<S: StorageBackend>(
    state: &SyncState<S>,
    user_id: &UserId,
    room_id: &OwnedRoomId,
    cfg: &CombinedCfg,
    is_initial_for_room: bool,
) -> Result<Option<(response::Room, Vec<StoredEvent>, Vec<(String, String)>)>, SyncError> {
    if !is_initial_for_room {
        // The invite_room_state is fixed at invite time and doesn't change
        // until accept/reject (which would move the room out of
        // `invited_rooms`). Re-emitting it every sync would just retransmit
        // the same bytes.
        return Ok(None);
    }

    let mut room = response::Room::new();
    room.initial = Some(true);

    let invite_event = state
        .store
        .current_state_event(room_id, "m.room.member", user_id.as_str())
        .await?;

    let mut stripped: Vec<Raw<AnyStrippedStateEvent>> = Vec::new();
    if let Some(ev) = invite_event.as_ref() {
        stripped.extend(extract_invite_room_state(ev)?);
        // Include the invite membership itself so the client can render the
        // "you've been invited by …" preview without parsing the array.
        stripped.push(strip_state_event(ev)?);
        // Lift `m.room.name` / `m.room.avatar` out of `invite_room_state` to
        // the top-level fields. Without this the client sees only the
        // stripped array and falls back to heroes (unimplemented, PLAN.md)
        // → renders the raw room id. Member counts are intentionally left
        // `None` for invites (Synapse-parity, no pre-accept room-size leak).
        let (name, avatar) = lift_invite_metadata(ev);
        if let Some(n) = name {
            room.name = Some(n);
        }
        if let Some(url) = avatar {
            room.avatar = ruma::JsOption::Some(url.into());
        }
    }
    room.invite_state = Some(stripped);

    if cfg.bump_stamp > 0 {
        room.bump_stamp = UInt::try_from(cfg.bump_stamp).ok();
    }

    // Tracking-wise: report no `state_events` (we used stripped_state, which
    // doesn't feed the required_state diff path) and no deletions.
    Ok(Some((room, Vec::new(), Vec::new())))
}

/// Scan an invite event's `unsigned.invite_room_state` for the stripped
/// `m.room.name` / `m.room.avatar` entries (state_key="") and return their
/// `content.name` / `content.url` strings. Either or both may be absent if
/// the federating server didn't include them. A malformed
/// `invite_room_state` (parse failure or non-array) silently yields
/// `(None, None)` — better to render a name-less invite than to surface a
/// "couldn't read invite" error for what's basically a presentation
/// fallback. Member counts deliberately not populated — Synapse doesn't
/// expose them on invites either (no leakage of room size pre-accept).
fn lift_invite_metadata(invite_event: &StoredEvent) -> (Option<String>, Option<String>) {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(invite_event.json.get()) else {
        return (None, None);
    };
    let Some(arr) = parsed
        .pointer("/unsigned/invite_room_state")
        .and_then(|v| v.as_array())
    else {
        return (None, None);
    };
    let mut name = None;
    let mut avatar = None;
    for v in arr {
        let event_type = v.pointer("/type").and_then(|x| x.as_str());
        let state_key = v.pointer("/state_key").and_then(|x| x.as_str());
        if state_key != Some("") {
            continue;
        }
        match event_type {
            Some("m.room.name") if name.is_none() => {
                if let Some(n) = v.pointer("/content/name").and_then(|x| x.as_str()) {
                    name = Some(n.to_string());
                }
            }
            Some("m.room.avatar") if avatar.is_none() => {
                if let Some(u) = v.pointer("/content/url").and_then(|x| x.as_str()) {
                    avatar = Some(u.to_string());
                }
            }
            _ => {}
        }
    }
    (name, avatar)
}

/// Pull stripped state out of `unsigned.invite_room_state` on an
/// `m.room.member` invite event. The inviting server is required by the spec
/// to populate this array with stripped events (`type`, `state_key`,
/// `sender`, `content` only), so we pass them through as
/// `Raw<AnyStrippedStateEvent>` without re-stripping.
///
/// Returns an empty vec when `unsigned.invite_room_state` is missing or
/// not an array — better to surface a name-less invite than to fail the
/// whole sync over a single malformed event.
fn extract_invite_room_state(
    invite_event: &StoredEvent,
) -> Result<Vec<Raw<AnyStrippedStateEvent>>, SyncError> {
    let parsed: serde_json::Value = serde_json::from_str(invite_event.json.get())
        .map_err(|e| SyncError::Storage(neutrino_store::StorageError::Internal(e.to_string())))?;
    let Some(arr) = parsed
        .pointer("/unsigned/invite_room_state")
        .and_then(|v| v.as_array())
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let raw: Box<RawValue> = serde_json::value::to_raw_value(v).map_err(|e| {
            SyncError::Storage(neutrino_store::StorageError::Internal(e.to_string()))
        })?;
        out.push(Raw::<AnyStrippedStateEvent>::from_json(raw));
    }
    Ok(out)
}

/// Re-serialise a state event in MSC1772 stripped form: only `type`,
/// `state_key`, `sender`, `content`. Drops `event_id`, `room_id`,
/// `origin_server_ts`, and anything else a state PDU normally carries.
fn strip_state_event(ev: &StoredEvent) -> Result<Raw<AnyStrippedStateEvent>, SyncError> {
    let parsed: serde_json::Value = serde_json::from_str(ev.json.get())
        .map_err(|e| SyncError::Storage(neutrino_store::StorageError::Internal(e.to_string())))?;
    let content = parsed
        .get("content")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let stripped = serde_json::json!({
        "type": &ev.event_type,
        "state_key": ev.state_key.as_deref().unwrap_or(""),
        "sender": ev.sender.as_str(),
        "content": content,
    });
    let raw: Box<RawValue> = serde_json::value::to_raw_value(&stripped)
        .expect("fixed-shape JSON serialisation cannot fail");
    Ok(Raw::<AnyStrippedStateEvent>::from_json(raw))
}

/// Filter `current_state` through `required_state` rules and diff against
/// what we previously sent for this conn+room. Returns:
/// - `state_events`: new or changed entries the client hasn't seen.
/// - `deleted_state_keys`: keys we sent before but are no longer current.
fn diff_required_state(
    current_state: &HashMap<(String, String), StoredEvent>,
    required_state: &[(StateEventType, String)],
    sent_snapshot: Option<&RoomSent>,
) -> (Vec<StoredEvent>, Vec<(String, String)>) {
    let filtered: Vec<(&(String, String), &StoredEvent)> = current_state
        .iter()
        .filter(|((t, k), _)| required_state_matches(required_state, t, k))
        .collect();

    let mut state_events: Vec<StoredEvent> = Vec::new();
    for (key, ev) in &filtered {
        let already_sent = sent_snapshot
            .and_then(|s| s.required_state_keys.get(*key))
            .map(|prev_id| prev_id == &ev.event_id)
            .unwrap_or(false);
        if !already_sent {
            state_events.push(clone_event(ev));
        }
    }

    let mut deleted: Vec<(String, String)> = Vec::new();
    if let Some(sent) = sent_snapshot {
        let live: HashSet<(String, String)> = filtered
            .iter()
            .map(|((t, k), _)| (t.clone(), k.clone()))
            .collect();
        for key in sent.required_state_keys.keys() {
            // Only flag a deletion if the rule that produced it is still in
            // effect — otherwise we'd emit stubs for state the client no
            // longer cares about because required_state shrank.
            let (t, k) = key;
            if required_state_matches(required_state, t, k) && !live.contains(key) {
                deleted.push(key.clone());
            }
        }
    }

    (state_events, deleted)
}

/// MSC4186 §StateStub: emit `{"type": …, "state_key": …}` with no `content`
/// to tell the client a state key was deleted from the room. Ruma v5 types
/// `required_state` as `Vec<Raw<AnySyncStateEvent>>` with no stub variant, so
/// we hand-roll the JSON and lean on `Raw` being lazy about its phantom type.
fn state_stub_raw(event_type: &str, state_key: &str) -> Raw<AnySyncStateEvent> {
    let stub = serde_json::json!({
        "type": event_type,
        "state_key": state_key,
    });
    let raw: Box<RawValue> = serde_json::value::to_raw_value(&stub)
        .expect("serialising a fixed-shape JSON literal cannot fail");
    Raw::<AnySyncStateEvent>::from_json(raw)
}

/// Populate `name`, `avatar`, `joined_count`, `invited_count`, `bump_stamp`
/// from current state. Called for every emitted joined room.
async fn populate_room_metadata<S: StorageBackend>(
    state: &SyncState<S>,
    room_id: &RoomId,
    room: &mut response::Room,
    current_state: &HashMap<(String, String), StoredEvent>,
    bump_stamp: u64,
) -> Result<(), SyncError> {
    // Name.
    if let Some(ev) = current_state.get(&("m.room.name".to_string(), String::new())) {
        let parsed: serde_json::Value = serde_json::from_str(ev.json.get()).map_err(|e| {
            SyncError::Storage(neutrino_store::StorageError::Internal(e.to_string()))
        })?;
        if let Some(n) = parsed.pointer("/content/name").and_then(|v| v.as_str()) {
            room.name = Some(n.to_string());
        }
    }

    // Avatar. Ruma's `JsOption<OwnedMxcUri>` distinguishes set/unset/null.
    if let Some(ev) = current_state.get(&("m.room.avatar".to_string(), String::new())) {
        let parsed: serde_json::Value = serde_json::from_str(ev.json.get()).map_err(|e| {
            SyncError::Storage(neutrino_store::StorageError::Internal(e.to_string()))
        })?;
        if let Some(url) = parsed.pointer("/content/url").and_then(|v| v.as_str()) {
            let uri: ruma::OwnedMxcUri = url.into();
            room.avatar = ruma::JsOption::Some(uri);
        }
    }

    // joined_count via the indexed `joined_members` lookup.
    let joined = state.store.joined_members(room_id).await?;
    room.joined_count = UInt::try_from(joined.len() as u64).ok();

    // invited_count: scan `m.room.member` state for membership == "invite".
    // No indexed helper for this in the trait; fine for embedded scale.
    let members = state
        .store
        .current_state_events_of_type(room_id, "m.room.member")
        .await?;
    let invited_count = members
        .values()
        .filter(|ev| {
            serde_json::from_str::<serde_json::Value>(ev.json.get())
                .ok()
                .and_then(|v| {
                    v.pointer("/content/membership")
                        .and_then(|m| m.as_str())
                        .map(|m| m == "invite")
                })
                .unwrap_or(false)
        })
        .count();
    room.invited_count = UInt::try_from(invited_count as u64).ok();

    if bump_stamp > 0 {
        room.bump_stamp = UInt::try_from(bump_stamp).ok();
    }

    Ok(())
}

/// MSC3575 §"Required State" matching: each `(event_type, state_key)` rule is
/// OR'd against the current state; `"*"` is a wildcard for either field.
///
/// TODO(phase-4+): implement the special tokens `$LAZY` / `$ME` — needed only
/// if a client we care about starts sending them. `$LAZY` is paired with
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

/// Update what was just emitted to this connection for this room.
///
/// - For new/changed state events: write the latest event_id into
///   `required_state_keys`.
/// - For deleted state keys: drop them from `required_state_keys` so future
///   syncs don't keep emitting the stub.
fn update_sent(sent: &mut RoomSent, state_events: &[StoredEvent], deleted: &[(String, String)]) {
    for ev in state_events {
        if let Some(state_key) = &ev.state_key {
            sent.required_state_keys.insert(
                (ev.event_type.clone(), state_key.clone()),
                ev.event_id.clone(),
            );
        }
    }
    for key in deleted {
        sent.required_state_keys.remove(key);
    }
}

/// `StoredEvent` doesn't derive `Clone` in `neutrino-common`. Hand-clone it
/// here in the build path.
fn clone_event(e: &StoredEvent) -> StoredEvent {
    StoredEvent {
        event_id: e.event_id.clone(),
        room_id: e.room_id.clone(),
        event_type: e.event_type.clone(),
        state_key: e.state_key.clone(),
        sender: e.sender.clone(),
        origin_server_ts: e.origin_server_ts,
        json: e.json.clone(),
    }
}

#[cfg(test)]
mod unit_tests {
    use std::collections::HashMap;

    use neutrino_store::StoredEvent;
    use ruma::events::StateEventType;
    use ruma::{event_id, room_id, user_id};

    use super::super::conn::RoomSent;
    use super::{diff_required_state, effective_range};

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

    /// `diff_required_state` correctly identifies a state key that was sent
    /// before but no longer matches current state — regardless of whether
    /// `EMIT_STATE_STUBS` is on or off (the caller decides whether to
    /// surface it). Keeps coverage of the detection path even when the wire
    /// emission is gated off.
    #[test]
    fn diff_required_state_detects_deletion() {
        let room = room_id!("!r:example.org");
        let user = user_id!("@u:example.org");
        let name_ev = StoredEvent {
            event_id: event_id!("$name:example.org").to_owned(),
            room_id: room.to_owned(),
            event_type: "m.room.name".to_string(),
            state_key: Some(String::new()),
            sender: user.to_owned(),
            origin_server_ts: 100,
            json: serde_json::value::to_raw_value(&serde_json::json!({
                "type": "m.room.name",
                "state_key": "",
                "content": {"name": "X"},
            }))
            .unwrap(),
        };

        // Pretend the previous sync sent this name event.
        let mut sent = RoomSent::default();
        sent.required_state_keys.insert(
            ("m.room.name".to_string(), String::new()),
            event_id!("$name:example.org").to_owned(),
        );

        // First call: name is still current → no change, no deletion.
        let current_state: HashMap<(String, String), StoredEvent> = {
            let mut m = HashMap::new();
            m.insert(
                ("m.room.name".to_string(), String::new()),
                clone_for_test(&name_ev),
            );
            m
        };
        let rules = vec![(StateEventType::RoomName, String::new())];
        let (changed, deleted) = diff_required_state(&current_state, &rules, Some(&sent));
        assert!(changed.is_empty());
        assert!(deleted.is_empty());

        // Second call: current state no longer has the name → deletion
        // surfaced regardless of EMIT_STATE_STUBS.
        let empty_state: HashMap<(String, String), StoredEvent> = HashMap::new();
        let (changed, deleted) = diff_required_state(&empty_state, &rules, Some(&sent));
        assert!(changed.is_empty());
        assert_eq!(deleted, vec![("m.room.name".to_string(), String::new())]);
    }

    fn clone_for_test(e: &StoredEvent) -> StoredEvent {
        StoredEvent {
            event_id: e.event_id.clone(),
            room_id: e.room_id.clone(),
            event_type: e.event_type.clone(),
            state_key: e.state_key.clone(),
            sender: e.sender.clone(),
            origin_server_ts: e.origin_server_ts,
            json: e.json.clone(),
        }
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
