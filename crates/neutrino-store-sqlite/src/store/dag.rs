//! `DagStore` impl on `SqliteStore`.
//!
//! Both methods do a BFS over `event_edges` (`edge_type = 'prev'`). The
//! BFS frontier is queue-based; a `visited` set defends against malformed
//! cycles (MSC4242 says the DAG is acyclic, but we don't trust it). The
//! walk as a whole terminates only on `limit` or an empty frontier;
//! individual branches are *pruned* (not terminated) when:
//!
//! - a parent isn't in the local store, or sits in a different room
//!   (federation-backfill boundary — [`hydrate_pdu`] returns `None` and
//!   the walker keeps draining the rest of the frontier); or
//! - the parent is in the `earliest` exclusion set passed to
//!   `missing_events` (marked visited, skipped, its own parents are
//!   never enqueued).
//!
//! So a missing parent in one subtree doesn't stop the BFS — siblings
//! and the rest of the frontier still get walked.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use neutrino_store::{DagStore, Event, StorageError};
use ruma::{EventId, OwnedEventId, RoomId};

use crate::{
    SqliteStore,
    error::Error,
    row::{EVENT_COLUMNS, EventRow},
};

/// Fetch an event + its `prev_events` / `prev_state_events` from the DB,
/// constrained to a specific room. Returns `Ok(None)` if the event is
/// not in `events` (federation backfill may reference parents we haven't
/// yet seen) *or* if the event exists but belongs to a different room
/// (defence against cross-room edges from a corrupt `event_edges` row,
/// or callers passing event IDs from the wrong room).
fn hydrate_pdu(
    conn: &Connection,
    event_id: &EventId,
    room_id: &RoomId,
) -> Result<Option<Event>, Error> {
    let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE event_id = ? AND room_id = ?");
    let event_result: Option<Result<Event, Error>> = conn
        .query_row(
            &query,
            params![event_id.as_str(), room_id.as_str()],
            |row| Ok(EventRow::try_from(row).map(EventRow::into_event)),
        )
        .optional()?;

    let Some(inner) = event_result else {
        return Ok(None);
    };
    // `prev_events` and `prev_state_events` are populated by row hydration
    // from the canonical JSON in `events.json` — no edge-table lookup
    // needed for the per-event view. `event_edges` still exists for graph
    // queries (BFS in `events_before` / `missing_events`); see the
    // "denormalisation" note in `event-id-design.md`.
    Ok(Some(inner?))
}

/// Fetch a child's parents of a given edge type from `event_edges`,
/// sorted by `parent_event_id`. This pins the BFS sibling-visit order so
/// `events_before` / `missing_events` results are deterministic across
/// runs regardless of the order the parent IDs appeared in the
/// originating JSON. `event_edges` has a `WITHOUT ROWID PRIMARY KEY
/// (child_event_id, edge_type, parent_event_id)`, so the explicit
/// `ORDER BY` matches the natural PK scan — free at runtime, contractual
/// at the spec boundary.
fn fetch_edges(
    conn: &Connection,
    child_event_id: &EventId,
    edge_type: &str,
) -> Result<Vec<OwnedEventId>, Error> {
    let mut stmt = conn.prepare(
        "SELECT parent_event_id FROM event_edges \
         WHERE child_event_id = ? AND edge_type = ? \
         ORDER BY parent_event_id",
    )?;
    let rows = stmt.query_map(params![child_event_id.as_str(), edge_type], |row| {
        row.get::<_, String>(0)
    })?;
    let mut out = Vec::new();
    for r in rows {
        let s = r?;
        let id = OwnedEventId::try_from(s)
            .map_err(|e| Error::Internal(format!("malformed parent_event_id in DB: {e}")))?;
        out.push(id);
    }
    Ok(out)
}

/// BFS over `prev_events` edges from `start`, stopping at `limit` or when
/// no more parents resolve in the local store. `excluded` IDs are skipped
/// (and don't seed traversal). The walk is scoped to `room_id` — events
/// from any other room are treated as if they don't exist, so a corrupt
/// cross-room `event_edges` row terminates the walk rather than leaking
/// PDUs from a different room. The result preserves traversal order
/// (reverse-chronological).
fn walk_prev_events(
    conn: &Connection,
    room_id: &RoomId,
    start: Vec<OwnedEventId>,
    excluded: &HashSet<OwnedEventId>,
    limit: usize,
) -> Result<Vec<Event>, Error> {
    let mut visited: HashSet<OwnedEventId> = HashSet::new();
    let mut frontier: VecDeque<OwnedEventId> = start.into_iter().collect();
    let mut results = Vec::new();

    while let Some(id) = frontier.pop_front() {
        if results.len() >= limit {
            break;
        }
        if !visited.insert(id.clone()) {
            continue;
        }
        if excluded.contains(&id) {
            continue;
        }
        let Some(pdu) = hydrate_pdu(conn, &id, room_id)? else {
            // Event not in local store, or in a different room — either
            // way, federation-backfill / wrong-room boundary.
            continue;
        };
        // Walk via event_edges (sorted by parent_event_id) rather than
        // the JSON-derived `pdu.prev_events` — the SQL-side sort is what
        // pins BFS determinism across runs; the JSON order is whatever
        // the sender chose and not guaranteed stable.
        for parent in fetch_edges(conn, &id, "prev")? {
            frontier.push_back(parent);
        }
        results.push(pdu);
    }
    Ok(results)
}

/// IN-clause chunk size used by [`validate_events_exist`]. Stays well below
/// `SQLITE_LIMIT_VARIABLE_NUMBER` on every SQLite build (the default is
/// 999 pre-3.32, 32766 since), and small enough that the prepared-
/// statement compile cost stays bounded even on pathological inputs.
///
/// Tests override to a tiny value so the chunk boundary is reachable
/// from the test suite without persisting hundreds of events per case.
#[cfg(not(test))]
const VALIDATE_INPUTS_CHUNK: usize = 256;
#[cfg(test)]
const VALIDATE_INPUTS_CHUNK: usize = 4;

/// Confirm `room_id` exists in `rooms`. Standalone so callers that
/// don't validate event IDs (federation-leaning paths like
/// `missing_events`) can still cheaply 404 on unknown rooms.
fn validate_room_exists(conn: &Connection, room_id: &RoomId) -> Result<(), Error> {
    let room_exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM rooms WHERE room_id = ?",
            params![room_id.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    if room_exists.is_none() {
        return Err(Error::InvalidInput(format!(
            "room {room_id} does not exist"
        )));
    }
    Ok(())
}

/// Confirm every ID in `event_ids` exists in the `events` table
/// globally. Whether each event is in any particular room is
/// intentionally *not* checked — the walk is scoped via
/// [`hydrate_pdu`], so a cross-room seed naturally produces empty
/// walk output rather than an error here. All queries share the
/// read connection holding the snapshot the subsequent walk runs
/// against, so a concurrent writer can't sneak rows out from under
/// us between validate and walk. Chunked to stay below
/// `SQLITE_LIMIT_VARIABLE_NUMBER` on every build.
fn validate_events_exist(conn: &Connection, event_ids: &[&OwnedEventId]) -> Result<(), Error> {
    if event_ids.is_empty() {
        return Ok(());
    }
    // Dedupe before chunking so a caller passing the same ID in both
    // `latest` and `earliest` (or any other duplication) doesn't
    // multiply the round-trip count.
    let unique: Vec<&str> = {
        let mut set: HashSet<&str> = HashSet::with_capacity(event_ids.len());
        for id in event_ids {
            set.insert(id.as_str());
        }
        set.into_iter().collect()
    };

    let mut found: HashSet<String> = HashSet::with_capacity(unique.len());
    for window in unique.chunks(VALIDATE_INPUTS_CHUNK) {
        let placeholders = vec!["?"; window.len()].join(",");
        let query = format!("SELECT event_id FROM events WHERE event_id IN ({placeholders})");
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params_from_iter(window.iter()), |row| {
            row.get::<_, String>(0)
        })?;
        for row in rows {
            found.insert(row?);
        }
    }

    // Iterate the original input (not `unique`) so the error message
    // reports the first missing ID in caller order, which is the most
    // useful thing for the caller to see.
    for id in event_ids {
        if !found.contains(id.as_str()) {
            return Err(Error::InvalidInput(format!(
                "event {id} does not exist in the store"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl DagStore for SqliteStore {
    async fn events_before(
        &self,
        room_id: &RoomId,
        from: &[&EventId],
        limit: usize,
    ) -> Result<Vec<Event>, StorageError> {
        let room_id = room_id.to_owned();
        let from: Vec<OwnedEventId> = from.iter().map(|&e| e.to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<Event>, Error> {
            validate_room_exists(conn, &room_id)?;
            let id_refs: Vec<&OwnedEventId> = from.iter().collect();
            validate_events_exist(conn, &id_refs)?;
            walk_prev_events(conn, &room_id, from, &HashSet::new(), limit)
        })
        .await
    }

    async fn missing_events(
        &self,
        room_id: &RoomId,
        latest: &[&EventId],
        earliest: &[&EventId],
        limit: usize,
    ) -> Result<Vec<Event>, StorageError> {
        let room_id = room_id.to_owned();
        let latest: Vec<OwnedEventId> = latest.iter().map(|&e| e.to_owned()).collect();
        let earliest: Vec<OwnedEventId> = earliest.iter().map(|&e| e.to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<Event>, Error> {
            validate_room_exists(conn, &room_id)?;
            // Synapse-style semantics: `latest_events` are the
            // boundary the requester already has; the response is
            // events the requester *doesn't* have, walked backward
            // from latest's parents. Concretely:
            //   1. Initial frontier is parents-of-`latest`, not
            //      `latest` itself — so seeds never land in results.
            //   2. `excluded` is `earliest ∪ latest`, so a path that
            //      loops back into a sibling latest stops cleanly
            //      (and a parent-of-latest that happens to be in
            //      `earliest` is also stopped).
            //
            // Unknown IDs in `latest` resolve to no parents (empty
            // edges row → empty expansion), so they contribute
            // nothing — matches Synapse's no-existence-check
            // behaviour. Unknown IDs in `earliest` are no-ops on the
            // walk because no walked event matches.
            //
            // See `_get_missing_events` in synapse's
            // storage/databases/main/event_federation.py and
            // docs/get-missing-events.md.
            let mut initial_frontier: Vec<OwnedEventId> = Vec::new();
            for id in &latest {
                for parent in fetch_edges(conn, id, "prev")? {
                    initial_frontier.push(parent);
                }
            }
            let mut excluded: HashSet<OwnedEventId> = earliest.into_iter().collect();
            for id in latest {
                excluded.insert(id);
            }
            walk_prev_events(conn, &room_id, initial_frontier, &excluded, limit)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use neutrino_store::{DagStore, Event, EventStore, RoomStore};
    use ruma::{EventId, OwnedEventId, event_id, room_id};

    use crate::SqliteStore;
    use crate::tests::{ALICE_ROOM_ID, ALICE_USER_ID, create_event, message_with_prev, store};

    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        s
    }

    // D1: empty `from` → empty result.
    #[tokio::test]
    async fn events_before_empty_from_returns_empty() {
        let s = store_with_room().await;
        let got = s.events_before(*ALICE_ROOM_ID, &[], 10).await.unwrap();
        assert!(got.is_empty());
    }

    // D2: chain a → b → c; walk from [c] returns [c, b, a].
    #[tokio::test]
    async fn events_before_walks_prev_events_chain() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", &[&id_a]);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "c", &[&id_b]);
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_c], 10).await.unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        assert_eq!(ids, [id_c.as_str(), id_b.as_str(), id_a.as_str()]);
    }

    // D3: chain of 5, limit=3 → first 3 returned.
    #[tokio::test]
    async fn events_before_respects_limit() {
        let s = store_with_room().await;
        // Build a chain of 5 events; each one's prev points at the prior id.
        let mut ids: Vec<OwnedEventId> = Vec::with_capacity(5);
        for i in 0..5 {
            let prevs: Vec<&EventId> = if i == 0 {
                Vec::new()
            } else {
                vec![ids[i - 1].as_ref()]
            };
            // Distinct ts disambiguates otherwise-identical bodies.
            let ev = crate::tests::make_event(
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "m.room.message",
                None,
                serde_json::json!({"body": "x", "msgtype": "m.text"}),
                i as u64,
                &prevs,
                &[],
            );
            ids.push(ev.event_id.clone());
            s.persist_event(&ev, &[]).await.unwrap();
        }

        let got = s
            .events_before(*ALICE_ROOM_ID, &[ids[4].as_ref()], 3)
            .await
            .unwrap();
        assert_eq!(got.len(), 3);
        let result_ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        assert_eq!(
            result_ids,
            [ids[4].as_str(), ids[3].as_str(), ids[2].as_str()]
        );
    }

    // D4: event with two `prev_events` → BFS visits both parents.
    #[tokio::test]
    async fn events_before_handles_branching() {
        let s = store_with_room().await;
        // a and b have no prev_events; their bodies get stripped by v12
        // redaction (m.room.message has no content keep-list), so they
        // must differ via origin_server_ts to get distinct event_ids.
        let ev_a = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 0);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", 1);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "c", &[&id_a, &id_b]);
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_c], 10).await.unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        // Deterministic order: c is the seed; its prev_events come back
        // from `fetch_edges` sorted by `parent_event_id` (lex order on the
        // computed event_id strings), so the BFS pops in that order.
        let mut parents_sorted = vec![id_a.as_str(), id_b.as_str()];
        parents_sorted.sort();
        let expected: Vec<&str> = std::iter::once(id_c.as_str())
            .chain(parents_sorted)
            .collect();
        assert_eq!(ids, expected);
    }

    // D5: event's prev_events references an ID not in the local store →
    // walker hits the federation-backfill boundary, doesn't error.
    #[tokio::test]
    async fn events_before_skips_missing_parents() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "a",
            &[event_id!("$ghost:e")],
        );
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_a], 10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event_id.as_str(), id_a.as_str());
    }

    // D6: storage-corruption defence — event with self-loop in `prev_events`
    // doesn't trap the walker in an infinite loop. See module docstring on
    // why federation-supplied cycles aren't constructable in practice.
    #[tokio::test]
    async fn events_before_cycle_handling() {
        let s = store_with_room().await;
        // Forward self-loop is unbuildable under hash-derived ids (the id is
        // a fixed point of `reference_hash`, which can't exist). Inject an
        // a→b→a cycle via raw SQL on `event_edges` to exercise the walker's
        // visited-set defence.
        let ev_probe = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let probe_id = ev_probe.event_id.clone();
        s.persist_event(&ev_probe, &[]).await.unwrap();
        // b declares a as prev.
        let ev_b = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", &[&probe_id]);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        // Inject a cycle directly via the edges table: a's prev now points
        // back to b. `write_into_tx`'s edge insertion already wrote
        // (b -> a); we manually add (a -> b) to close the cycle, exercising
        // the walker's `visited` defence.
        let probe_id_str = probe_id.as_str().to_owned();
        let id_b_str = id_b.as_str().to_owned();
        s.run_write(move |conn| -> Result<(), crate::error::Error> {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO event_edges (child_event_id, edge_type, parent_event_id) \
                 VALUES (?, ?, ?)",
                deadpool_sqlite::rusqlite::params![probe_id_str, "prev", id_b_str],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[&probe_id], 10)
            .await
            .unwrap();
        // Both a and b are reachable; walker terminates rather than looping.
        assert_eq!(got.len(), 2);
        let ids: std::collections::HashSet<&str> =
            got.iter().map(|p| p.event_id.as_str()).collect();
        assert!(ids.contains(probe_id.as_str()));
        assert!(ids.contains(id_b.as_str()));
    }

    // D7: missing_events walks the ancestors of `latest` and excludes
    // IDs listed in `earliest`. Synapse-style semantics:
    //   - `latest_events` themselves are NEVER in the response (the
    //     requester already has them — they're the boundary).
    //   - `earliest_events` are likewise never in the response.
    // For chain a ← b ← c with latest=[c], earliest=[a], the only
    // event "between" them is b.
    #[tokio::test]
    async fn missing_events_excludes_earliest() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", &[&id_a]);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "c", &[&id_b]);
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_c], &[&id_a], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        // c is the latest (boundary, never returned); a is earliest
        // (boundary, never returned). Only b is "between" them.
        assert_eq!(ids, [id_b.as_str()]);
    }

    // D8: empty `latest` → empty result.
    #[tokio::test]
    async fn missing_events_empty_latest_returns_empty() {
        let s = store_with_room().await;
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[], &[], 10)
            .await
            .unwrap();
        assert!(got.is_empty());
    }

    // D9: events_before / missing_events are scoped to the requested
    // room. Passing an event ID that exists in a *different* room
    // (or following a corrupt cross-room edge) must not leak PDUs from
    // outside `room_id` — caller's mistake or DB corruption should
    // terminate the walk, not surface unrelated history.
    #[tokio::test]
    async fn dag_queries_scoped_to_room_id() {
        let s = store_with_room().await; // room A = *ALICE_ROOM_ID
        let other_room = room_id!("!r2:example.com");
        s.create_room(&create_event(other_room, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let ev_other = message_with_prev(other_room, *ALICE_USER_ID, "x", &[]);
        let id_other = ev_other.event_id.clone();
        s.persist_event(&ev_other, &[]).await.unwrap();

        // Seed exists, but in the *other* room. Validation rejects it
        // because `validate_events_exist` only checks existence in the events
        // table globally — but the walker scopes by `room_id`, so the seed
        // hydrates as None and we get an empty result.
        //
        // Note: D19 covers the bogus-seed InvalidInput case. Here the seed
        // is real (just in the wrong room), and the validator does find
        // it in events globally, so validation passes and the walk runs
        // (and returns empty due to room-scoping in hydrate_pdu).
        let got = s
            .events_before(*ALICE_ROOM_ID, &[&id_other], 10)
            .await
            .unwrap();
        assert!(got.is_empty(), "events_before leaked cross-room PDU");

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_other], &[], 10)
            .await
            .unwrap();
        assert!(got.is_empty(), "missing_events leaked cross-room PDU");
    }

    // D10: BFS in R1 follows a `prev_events` edge whose parent_event_id
    // resolves to a real event in R2. D9 covered "seed is in the wrong
    // room"; this covers the harder shape — the seed is legitimately in
    // R1, but its declared parent points across the room boundary.
    // `hydrate_pdu` rejects the cross-room hit, so the walker terminates
    // at $a rather than chasing the edge into R2.
    #[tokio::test]
    async fn events_before_does_not_cross_room_boundary_via_edges() {
        let s = store_with_room().await; // room A = *ALICE_ROOM_ID
        let other_room = room_id!("!r2:example.com");
        s.create_room(&create_event(other_room, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        // $b is a real persisted event in R2 — so the edge actually
        // resolves; the cross-room filter is the only thing stopping it.
        let ev_b = message_with_prev(other_room, *ALICE_USER_ID, "b", &[]);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        // $a is in R1 with prev_events=[$b:e]. `write_into_tx` only
        // records the edge string — it doesn't validate that the parent
        // is in the same room.
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[&id_b]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_a], 10).await.unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        assert_eq!(
            ids,
            [id_a.as_str()],
            "walker leaked cross-room PDU via edge"
        );

        // Same shape via missing_events. Under Synapse-style semantics,
        // a is the latest (boundary, excluded). The walker expands to
        // a's parent, which is the cross-room `b`; `hydrate_pdu`
        // rejects b for being in another room. Result: empty. The
        // cross-room boundary is the load-bearing property here —
        // the test name still applies.
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_a], &[], 10)
            .await
            .unwrap();
        assert!(
            got.is_empty(),
            "missing_events leaked cross-room PDU via edge: {got:?}"
        );
    }

    // D11: limit=0 returns empty without doing any work. Pins the
    // boundary so a refactor that flips the `>=` comparison can't
    // silently start returning one row.
    #[tokio::test]
    async fn events_before_limit_zero_returns_empty() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let got = s.events_before(*ALICE_ROOM_ID, &[&id_a], 0).await.unwrap();
        assert!(got.is_empty());
    }

    // D12: BFS with multiple disjoint seeds. The frontier interleaves
    // seeds at each level — pop seed1, push its parents, pop seed2, etc.
    // Build two parallel single-parent chains so the expected order is
    // unambiguous: [c, d, a, b].
    #[tokio::test]
    async fn events_before_handles_multiple_seeds() {
        let s = store_with_room().await;
        let ev_a = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 0);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 1);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "c", "msgtype": "m.text"}),
            0,
            &[&id_a],
            &[],
        );
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();
        let ev_d = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "d", "msgtype": "m.text"}),
            0,
            &[&id_b],
            &[],
        );
        let id_d = ev_d.event_id.clone();
        s.persist_event(&ev_d, &[]).await.unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[&id_c, &id_d], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        // BFS interleaves seeds. Seeds pop in the order passed (c, d);
        // their parents (a, b respectively) pop in the order pushed.
        assert_eq!(
            ids,
            [id_c.as_str(), id_d.as_str(), id_a.as_str(), id_b.as_str()]
        );
    }

    // D13: diamond DAG — the `visited` set must dedup shared ancestors
    // so they appear exactly once in the result. a → b, a → c, b → d,
    // c → d; walking from [d] should yield d, b, c, a — never a twice
    // even though both b and c reach it.
    #[tokio::test]
    async fn events_before_dedups_shared_ancestors_in_diamond() {
        let s = store_with_room().await;
        let ev_a = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 0);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        // b and c share prev=[a] and body collapses on redaction → use ts
        // to disambiguate.
        let ev_b = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "b", "msgtype": "m.text"}),
            1,
            &[&id_a],
            &[],
        );
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "c", "msgtype": "m.text"}),
            2,
            &[&id_a],
            &[],
        );
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();
        let ev_d = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "d", &[&id_b, &id_c]);
        let id_d = ev_d.event_id.clone();
        s.persist_event(&ev_d, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_d], 10).await.unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        // BFS: d -> sorted(b, c) -> a (deduped). Sort b/c lex.
        let mut bc_sorted = vec![id_b.as_str(), id_c.as_str()];
        bc_sorted.sort();
        let expected: Vec<&str> = std::iter::once(id_d.as_str())
            .chain(bc_sorted)
            .chain(std::iter::once(id_a.as_str()))
            .collect();
        assert_eq!(ids, expected);
        assert_eq!(ids.len(), 4, "shared ancestor surfaced more than once");
    }

    // D14: determinism applies at every BFS level, not just one hop
    // from the seed. D4 only verified the sort works for the seed's
    // direct parents. Build two layers and intentionally insert
    // `prev_events` arrays in reverse lex order — `fetch_edges` must
    // still sort them on read, otherwise the result order changes
    // depending on JSON-side insertion order.
    #[tokio::test]
    async fn events_before_determinism_holds_at_every_level() {
        let s = store_with_room().await;
        let ev_g1 = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 0);
        let id_g1 = ev_g1.event_id.clone();
        s.persist_event(&ev_g1, &[]).await.unwrap();
        let ev_g2 = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 1);
        let id_g2 = ev_g2.event_id.clone();
        s.persist_event(&ev_g2, &[]).await.unwrap();
        // Parents both reference grandparents in REVERSE lex order
        // (whichever id sorts higher first). `fetch_edges` sorts on read
        // so the BFS still visits in lex order regardless of insertion.
        let mut gs_reversed = [id_g1.clone(), id_g2.clone()];
        gs_reversed.sort_by(|a, b| b.as_str().cmp(a.as_str())); // descending
        let gs_refs: Vec<&EventId> = gs_reversed.iter().map(|i| i.as_ref()).collect();
        let ev_p1 = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "p1", "msgtype": "m.text"}),
            10,
            &gs_refs,
            &[],
        );
        let id_p1 = ev_p1.event_id.clone();
        s.persist_event(&ev_p1, &[]).await.unwrap();
        let ev_p2 = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "p2", "msgtype": "m.text"}),
            11,
            &gs_refs,
            &[],
        );
        let id_p2 = ev_p2.event_id.clone();
        s.persist_event(&ev_p2, &[]).await.unwrap();
        // Child references parents in reverse lex order too.
        let mut ps_reversed = [id_p1.clone(), id_p2.clone()];
        ps_reversed.sort_by(|a, b| b.as_str().cmp(a.as_str()));
        let ps_refs: Vec<&EventId> = ps_reversed.iter().map(|i| i.as_ref()).collect();
        let ev_c = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "c", "msgtype": "m.text"}),
            0,
            &ps_refs,
            &[],
        );
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();

        let got = s.events_before(*ALICE_ROOM_ID, &[&id_c], 10).await.unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        // Expected: c, sorted(p1,p2), sorted(g1,g2).
        let mut ps_sorted = vec![id_p1.as_str(), id_p2.as_str()];
        ps_sorted.sort();
        let mut gs_sorted = vec![id_g1.as_str(), id_g2.as_str()];
        gs_sorted.sort();
        let expected: Vec<&str> = std::iter::once(id_c.as_str())
            .chain(ps_sorted)
            .chain(gs_sorted)
            .collect();
        assert_eq!(ids, expected);
    }

    // D15: `hydrate_pdu` populates `prev_state_events` on the returned
    // `Event`. The BFS itself only follows `prev` edges (MSC4242
    // state DAG is walked separately), but downstream callers — state
    // resolution, etc. — rely on `prev_state_events` being present on
    // the PDUs `events_before` hands back. Build a message with both
    // edge kinds and assert both fields land.
    #[tokio::test]
    async fn stored_pdu_exposes_prev_state_events() {
        let s = store_with_room().await;
        // Build a message whose JSON declares both prev_events and
        // prev_state_events pointing at the room's create event. The
        // create event is real, so the BFS will hydrate it.
        let create_event_id: OwnedEventId = {
            // Recreate the create event to learn its computed id.
            let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
            ce.event_id
        };
        let msg = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "msg", "msgtype": "m.text"}),
            0,
            &[create_event_id.as_ref()],
            &[create_event_id.as_ref()],
        );
        let msg_id = msg.event_id.clone();
        s.persist_event(&msg, &[]).await.unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[&msg_id], 1)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        let pdu = &got[0];
        let prev_ids: Vec<&str> = pdu.prev_events.iter().map(|e| e.as_str()).collect();
        let prev_state_ids: Vec<&str> = pdu.prev_state_events.iter().map(|e| e.as_str()).collect();
        assert_eq!(prev_ids, [create_event_id.as_str()]);
        assert_eq!(prev_state_ids, [create_event_id.as_str()]);
    }

    // D16: missing_events limit=0 parity with D11. `events_before` and
    // `missing_events` share `walk_prev_events`, but the wiring through
    // the trait surface is independent — pin the boundary on both
    // surfaces so neither can regress in isolation.
    #[tokio::test]
    async fn missing_events_limit_zero_returns_empty() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_a], &[], 0)
            .await
            .unwrap();
        assert!(got.is_empty());
    }

    // D18: unknown room_id → InvalidInput on events_before.
    #[tokio::test]
    async fn events_before_unknown_room_returns_invalid_input() {
        let s = store().await; // no rooms set up
        let err = s
            .events_before(*ALICE_ROOM_ID, &[], 10)
            .await
            .expect_err("unknown room must reject");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput, got {err:?}"
        );
    }

    // D19: seed ID that isn't in the events table → InvalidInput.
    // Distinct from D5 (the federation-backfill case): in D5 the seed
    // exists and only its `prev_events` are missing. Here the seed
    // itself is bogus.
    #[tokio::test]
    async fn events_before_missing_seed_returns_invalid_input() {
        let s = store_with_room().await;
        let err = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$nope:e")], 10)
            .await
            .expect_err("missing seed must reject");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput, got {err:?}"
        );
    }

    // D20: unknown room_id → InvalidInput on missing_events.
    #[tokio::test]
    async fn missing_events_unknown_room_returns_invalid_input() {
        let s = store().await;
        let err = s
            .missing_events(*ALICE_ROOM_ID, &[], &[], 10)
            .await
            .expect_err("unknown room must reject");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput, got {err:?}"
        );
    }

    // D21: an unknown `latest_events` reference is treated as a starting
    // point with no parents in the local store — the walk produces no
    // events. This mirrors Synapse's `_get_missing_events`
    // (storage/databases/main/event_federation.py), which performs no
    // existence pre-check. The previous strict-reject contract
    // contradicted the federation use case where peers legitimately
    // hold events we haven't seen.
    #[tokio::test]
    async fn missing_events_unknown_latest_returns_empty() {
        let s = store_with_room().await;
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[event_id!("$nope:e")], &[], 10)
            .await
            .expect("unknown latest must not error");
        assert!(got.is_empty(), "expected empty result, got {got:?}");
    }

    // D23: validator chunks the IN-clause. With VALIDATE_INPUTS_CHUNK=4
    // under cfg(test), six inputs cross at least two chunks; all six
    // resolve, so validation passes and the BFS runs normally. Catches
    // a regression where the chunk loop only writes results from the
    // last window into the `found` set (or only checks the first).
    #[tokio::test]
    async fn events_before_validates_inputs_in_chunks() {
        let s = store_with_room().await;
        let count = 6usize;
        let mut owned_ids: Vec<OwnedEventId> = Vec::with_capacity(count);
        for i in 0..count {
            // Distinct ts so each event has a distinct computed id.
            let ev = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64);
            owned_ids.push(ev.event_id.clone());
            s.persist_event(&ev, &[]).await.unwrap();
        }
        let id_refs: Vec<&EventId> = owned_ids.iter().map(|id| id.as_ref()).collect();
        let got = s
            .events_before(*ALICE_ROOM_ID, &id_refs, 100)
            .await
            .unwrap();
        // Each seed has no prev_events, so the result is just the seeds
        // themselves — order doesn't matter, just cardinality.
        assert_eq!(got.len(), count);
    }

    // D24: a missing ID anywhere across multiple chunks still produces
    // InvalidInput. With CHUNK=4 and 6 inputs, the missing ID lands in
    // some chunk depending on HashSet iteration order — the validator
    // must aggregate `found` across chunks and only report a miss when
    // *no* chunk surfaced the ID.
    #[tokio::test]
    async fn events_before_chunked_validation_rejects_missing() {
        let s = store_with_room().await;
        let count = 5usize;
        let mut owned_ids: Vec<OwnedEventId> = Vec::with_capacity(count);
        for i in 0..count {
            let ev = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", i as u64);
            owned_ids.push(ev.event_id.clone());
            s.persist_event(&ev, &[]).await.unwrap();
        }
        let fake = OwnedEventId::try_from("$nope:e".to_owned()).unwrap();
        let mut all_ids: Vec<&EventId> = owned_ids.iter().map(|id| id.as_ref()).collect();
        all_ids.push(fake.as_ref());

        let err = s
            .events_before(*ALICE_ROOM_ID, &all_ids, 100)
            .await
            .expect_err("missing seed must reject across chunks");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput, got {err:?}"
        );
    }

    // D22: an unknown `earliest_events` reference is a no-op — no
    // walked event will ever match it, so the walk proceeds as if
    // `earliest` were empty. Same rationale as D21.
    //
    // Under Synapse-style semantics, `latest_events` are also
    // excluded from results, so a single-event seed `[a]` with no
    // ancestors produces an empty walk: a is excluded, and a has no
    // parents to expand into. The point of this test is that the
    // call *doesn't error* on the bogus earliest — empty is the
    // right success outcome.
    #[tokio::test]
    async fn missing_events_unknown_earliest_ignored() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_a], &[event_id!("$nope:e")], 10)
            .await
            .expect("unknown earliest must not error");
        assert!(got.is_empty(), "no ancestors to walk into; expected empty");
    }

    // D23a: a single node with 50 prev_events. Catches a regression
    // where the BFS frontier is bounded or `fetch_edges` truncates the
    // parent set. Under Synapse-style semantics, head is excluded
    // (latest = boundary), so the result is just the 50 leaves.
    #[tokio::test]
    async fn missing_events_wide_fanout() {
        let s = store_with_room().await;

        let mut leaf_ids: Vec<OwnedEventId> = Vec::with_capacity(50);
        for i in 0..50u64 {
            let ev = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "leaf", i);
            leaf_ids.push(ev.event_id.clone());
            s.persist_event(&ev, &[]).await.unwrap();
        }

        let leaf_refs: Vec<&EventId> = leaf_ids.iter().map(|id| id.as_ref()).collect();
        let head = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "head", &leaf_refs);
        let head_id = head.event_id.clone();
        s.persist_event(&head, &[]).await.unwrap();

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&head_id], &[], 100)
            .await
            .unwrap();

        assert_eq!(got.len(), 50, "50 leaves reachable; head itself excluded");
        let returned: HashSet<&str> = got.iter().map(|e| e.event_id.as_str()).collect();
        assert!(
            !returned.contains(head_id.as_str()),
            "head is latest/boundary, must not appear"
        );
    }

    // D23c: DAG shape ported from Synapse's
    // `_setup_room_for_backfill_tests` (tests/storage/test_event_federation.py).
    // Synapse uses this shape to test `get_backfill_points_in_room` —
    // a different primitive — so the assertions here are fresh, not
    // direct ports. The DAG: a main spine `1 → 2 → 3 → 4 → 5` with
    // two fan-out branches: events b1/b2/b3 between 2 and 3 merging
    // via event A, and events b4/b5/b6 between 3 and 4 merging via
    // event B. We persist the full 13-event DAG locally (Synapse's
    // test deliberately omits some to model backward extremities,
    // which is irrelevant to `missing_events`).
    //
    // Asserts:
    //  - walk from 5 with no earliest, generous limit → all 12
    //    ancestors of 5 (the `latest` event itself is excluded)
    //  - walk from 5 with earliest=[3], generous limit → exactly 5
    //    events (4, B, b4, b5, b6) — `latest` (5) and `earliest`
    //    (3) are excluded, and 3's transitive ancestors are
    //    unreachable past the earliest boundary; b4/b5/b6 enqueue
    //    but their sole prev (3) is in `excluded` so the chain stops.
    #[tokio::test]
    async fn missing_events_synapse_backfill_dag_shape() {
        use crate::tests::make_event;
        use serde_json::json;

        let s = store_with_room().await;

        // Build leaf-first so each event's prev_events resolve to real
        // ids. ts is the topological index; not load-bearing for our
        // walker (event_edges sort is by parent_event_id) but assigns
        // each event a distinct content hash so event_ids differ.
        let mk = |body: &str, prev: &[&EventId], ts: u64| -> Event {
            make_event(
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "m.room.message",
                None,
                json!({"body": body, "msgtype": "m.text"}),
                ts,
                prev,
                &[],
            )
        };

        let e1 = mk("1", &[], 1);
        let id1 = e1.event_id.clone();
        s.persist_event(&e1, &[]).await.unwrap();

        let e2 = mk("2", &[&id1], 2);
        let id2 = e2.event_id.clone();
        s.persist_event(&e2, &[]).await.unwrap();

        let b1 = mk("b1", &[&id2], 3);
        let b2 = mk("b2", &[&id2], 4);
        let b3 = mk("b3", &[&id2], 5);
        let id_b1 = b1.event_id.clone();
        let id_b2 = b2.event_id.clone();
        let id_b3 = b3.event_id.clone();
        s.persist_event(&b1, &[]).await.unwrap();
        s.persist_event(&b2, &[]).await.unwrap();
        s.persist_event(&b3, &[]).await.unwrap();

        let a = mk("A", &[&id_b1, &id_b2, &id_b3], 6);
        let id_a = a.event_id.clone();
        s.persist_event(&a, &[]).await.unwrap();

        let e3 = mk("3", &[&id2, &id_a], 7);
        let id3 = e3.event_id.clone();
        s.persist_event(&e3, &[]).await.unwrap();

        let b4 = mk("b4", &[&id3], 8);
        let b5 = mk("b5", &[&id3], 9);
        let b6 = mk("b6", &[&id3], 10);
        let id_b4 = b4.event_id.clone();
        let id_b5 = b5.event_id.clone();
        let id_b6 = b6.event_id.clone();
        s.persist_event(&b4, &[]).await.unwrap();
        s.persist_event(&b5, &[]).await.unwrap();
        s.persist_event(&b6, &[]).await.unwrap();

        let b = mk("B", &[&id_b4, &id_b5, &id_b6], 11);
        let id_b = b.event_id.clone();
        s.persist_event(&b, &[]).await.unwrap();

        let e4 = mk("4", &[&id3, &id_b], 12);
        let id4 = e4.event_id.clone();
        s.persist_event(&e4, &[]).await.unwrap();

        let e5 = mk("5", &[&id4], 13);
        let id5 = e5.event_id.clone();
        s.persist_event(&e5, &[]).await.unwrap();

        // Walk from 5 with no earliest, generous limit. Synapse-style
        // semantics: 5 is the boundary (excluded); the walk expands
        // to its parents and beyond. All 12 events strictly behind
        // 5 are reachable: 1, 2, 3, A, b1, b2, b3, 4, B, b4, b5, b6.
        let got_all = s
            .missing_events(*ALICE_ROOM_ID, &[&id5], &[], 100)
            .await
            .unwrap();
        let all_ids: HashSet<OwnedEventId> = got_all.iter().map(|e| e.event_id.clone()).collect();
        assert_eq!(
            all_ids.len(),
            12,
            "expected 12 ancestors of spine head 5, got {all_ids:?}"
        );
        assert!(!all_ids.contains(&id5), "latest 5 must not be in result");
        // Spot-check the deepest events to confirm the walk reached
        // the bottom of both branches and the spine root.
        assert!(all_ids.contains(&id1));
        assert!(all_ids.contains(&id_b1));
        assert!(all_ids.contains(&id_b2));
        assert!(all_ids.contains(&id_b3));

        // Walk from 5 with earliest=[3]. Both 5 (latest boundary)
        // and 3 (earliest boundary) are excluded; the walk reaches 4
        // and B and B's children (b4/b5/b6), but stops when those
        // children's only prev (= 3) hits the boundary. Reachable
        // result: 4, B, b4, b5, b6. (Cardinality 5.)
        let got_bounded = s
            .missing_events(*ALICE_ROOM_ID, &[&id5], &[&id3], 100)
            .await
            .unwrap();
        let bounded_ids: HashSet<OwnedEventId> =
            got_bounded.iter().map(|e| e.event_id.clone()).collect();
        assert_eq!(
            bounded_ids.len(),
            5,
            "expected 5 events strictly between 3 and 5, got {bounded_ids:?}"
        );
        assert!(bounded_ids.contains(&id4));
        assert!(bounded_ids.contains(&id_b));
        assert!(bounded_ids.contains(&id_b4));
        assert!(bounded_ids.contains(&id_b5));
        assert!(bounded_ids.contains(&id_b6));
        assert!(!bounded_ids.contains(&id5), "latest must not be in result");
        assert!(
            !bounded_ids.contains(&id3),
            "earliest must not be in result"
        );
        assert!(!bounded_ids.contains(&id2));
        assert!(!bounded_ids.contains(&id1));
        assert!(!bounded_ids.contains(&id_a));
    }

    // D23b: limit=1 returns exactly one event. Under Synapse-style
    // semantics, b is excluded (latest = boundary); the only walked
    // event is its parent a. Boundary between D16 (limit=0 → empty)
    // and the unbounded case.
    #[tokio::test]
    async fn missing_events_limit_one() {
        let s = store_with_room().await;
        let ev_a = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", &[]);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = message_with_prev(*ALICE_ROOM_ID, *ALICE_USER_ID, "b", &[&id_a]);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_b], &[], 1)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event_id, id_a, "expect ancestor a, not latest b");
    }

    // D25: schema-level — the FK `event_edges.child_event_id REFERENCES
    // events(event_id)` rejects orphan-child edges. `write_into_tx`
    // always INSERTs the child event before any edges within the same
    // transaction (so the FK is naturally satisfied on the canonical
    // write path); this test exercises the schema directly to pin the
    // constraint, against a future write path that tries to insert
    // edges first.
    #[tokio::test]
    async fn event_edges_rejects_orphan_child() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store_with_room().await;
        let err = s
            .run_write(|conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO event_edges (child_event_id, edge_type, parent_event_id) \
                     VALUES (?, ?, ?)",
                    params!["$orphan_child:e", "prev", "$create:e"],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("FK on child_event_id must reject orphan-child edge");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from FK violation, got {err:?}"
        );
    }

    // D17: multi-seed `latest` — two parallel chains c→a and d→b,
    // with latest=[c, d]. Under Synapse-style semantics, c and d are
    // boundary (never returned); the walk expands to their parents
    // [a, b] and emits them. Order isn't pinned (different from
    // `events_before` which does pin order); cardinality + set
    // membership are the load-bearing assertions.
    #[tokio::test]
    async fn missing_events_handles_multiple_latest() {
        let s = store_with_room().await;
        let ev_a = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 0);
        let id_a = ev_a.event_id.clone();
        s.persist_event(&ev_a, &[]).await.unwrap();
        let ev_b = crate::tests::message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "x", 1);
        let id_b = ev_b.event_id.clone();
        s.persist_event(&ev_b, &[]).await.unwrap();
        let ev_c = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "c", "msgtype": "m.text"}),
            0,
            &[&id_a],
            &[],
        );
        let id_c = ev_c.event_id.clone();
        s.persist_event(&ev_c, &[]).await.unwrap();
        let ev_d = crate::tests::make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            serde_json::json!({"body": "d", "msgtype": "m.text"}),
            0,
            &[&id_b],
            &[],
        );
        let id_d = ev_d.event_id.clone();
        s.persist_event(&ev_d, &[]).await.unwrap();

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[&id_c, &id_d], &[], 10)
            .await
            .unwrap();
        let ids: HashSet<&str> = got.iter().map(|p| p.event_id.as_str()).collect();
        assert_eq!(ids.len(), 2, "exactly the two parents a, b");
        assert!(ids.contains(id_a.as_str()));
        assert!(ids.contains(id_b.as_str()));
        assert!(!ids.contains(id_c.as_str()), "latest c must not appear");
        assert!(!ids.contains(id_d.as_str()), "latest d must not appear");
    }
}
