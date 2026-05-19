//! `DagStore` impl on `SqliteStore`.
//!
//! Both methods do a BFS over `event_edges` (`edge_type = 'prev'`). The
//! BFS frontier is queue-based; a `visited` set defends against malformed
//! cycles (MSC4242 says the DAG is acyclic, but we don't trust it). Walks
//! stop on `limit`, on missing parents (federation-backfill boundary), or
//! on the `earliest` exclusion set in `missing_events`.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{Connection, OptionalExtension, params};
use neutrino_store::{DagStore, StorageError, StoredEvent, StoredPdu};
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
) -> Result<Option<StoredPdu>, Error> {
    let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE event_id = ? AND room_id = ?");
    let event_result: Option<Result<StoredEvent, Error>> = conn
        .query_row(
            &query,
            params![event_id.as_str(), room_id.as_str()],
            |row| Ok(EventRow::try_from(row).map(EventRow::into_event)),
        )
        .optional()?;

    let Some(inner) = event_result else {
        return Ok(None);
    };
    let event = inner?;

    let prev_events = fetch_edges(conn, event_id, "prev")?;
    let prev_state_events = fetch_edges(conn, event_id, "prev_state")?;

    Ok(Some(StoredPdu {
        event,
        prev_events,
        prev_state_events,
    }))
}

fn fetch_edges(
    conn: &Connection,
    child_event_id: &EventId,
    edge_type: &str,
) -> Result<Vec<OwnedEventId>, Error> {
    // `ORDER BY parent_event_id` pins the BFS sibling-visit order so
    // `events_before` / `missing_events` results are deterministic.
    // `event_edges` is `WITHOUT ROWID PRIMARY KEY (child_event_id,
    // edge_type, parent_event_id)`, so the explicit sort matches the
    // natural PK scan — free at runtime, contractual at the spec
    // boundary.
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
) -> Result<Vec<StoredPdu>, Error> {
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
        for parent in &pdu.prev_events {
            frontier.push_back(parent.clone());
        }
        results.push(pdu);
    }
    Ok(results)
}

#[async_trait]
impl DagStore for SqliteStore {
    async fn events_before(
        &self,
        room_id: &RoomId,
        from: &[&EventId],
        limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        let room_id = room_id.to_owned();
        let from: Vec<OwnedEventId> = from.iter().map(|&e| e.to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<StoredPdu>, Error> {
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
    ) -> Result<Vec<StoredPdu>, StorageError> {
        let room_id = room_id.to_owned();
        let latest: Vec<OwnedEventId> = latest.iter().map(|&e| e.to_owned()).collect();
        let earliest: HashSet<OwnedEventId> = earliest.iter().map(|&e| e.to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<StoredPdu>, Error> {
            walk_prev_events(conn, &room_id, latest, &earliest, limit)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use neutrino_store::{DagStore, EventStore, RoomStore};
    use ruma::{EventId, RoomId, UserId, event_id, room_id, user_id};

    use crate::SqliteStore;
    use crate::tests::{create_event, message_with_prev, store};

    lazy_static! {
        static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
        static ref ALICE_ID: &'static UserId = user_id!("@alice:example.com");
    }

    async fn store_with_room() -> SqliteStore {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$create:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[],
        )
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
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$b:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "b",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "c",
                &[event_id!("$b:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$c:e")], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$c:e", "$b:e", "$a:e"]);
    }

    // D3: chain of 5, limit=3 → first 3 returned.
    #[tokio::test]
    async fn events_before_respects_limit() {
        let s = store_with_room().await;
        let chain_ids: [&EventId; 5] = [
            event_id!("$e0:e"),
            event_id!("$e1:e"),
            event_id!("$e2:e"),
            event_id!("$e3:e"),
            event_id!("$e4:e"),
        ];
        for (i, eid) in chain_ids.iter().enumerate() {
            let prevs: Vec<&EventId> = if i == 0 {
                Vec::new()
            } else {
                vec![chain_ids[i - 1]]
            };
            s.persist_event(
                &message_with_prev(eid, *ALICE_ROOM_ID, *ALICE_ID, "x", &prevs),
                &[],
            )
            .await
            .unwrap();
        }

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$e4:e")], 3)
            .await
            .unwrap();
        assert_eq!(got.len(), 3);
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$e4:e", "$e3:e", "$e2:e"]);
    }

    // D4: event with two `prev_events` → BFS visits both parents.
    #[tokio::test]
    async fn events_before_handles_branching() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(event_id!("$b:e"), *ALICE_ROOM_ID, *ALICE_ID, "b", &[]),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "c",
                &[event_id!("$a:e"), event_id!("$b:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$c:e")], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        // Deterministic order: c is the seed; its prev_events come back
        // from `fetch_edges` sorted by `parent_event_id`, so the BFS
        // pushes `$a:e` before `$b:e` and pops in FIFO order.
        assert_eq!(ids, ["$c:e", "$a:e", "$b:e"]);
    }

    // D5: event's prev_events references an ID not in the local store →
    // walker hits the federation-backfill boundary, doesn't error.
    #[tokio::test]
    async fn events_before_skips_missing_parents() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(
                event_id!("$a:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "a",
                &[event_id!("$ghost:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$a:e")], 10)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event.event_id.as_str(), "$a:e");
    }

    // D6: storage-corruption defence — event with self-loop in `prev_events`
    // doesn't trap the walker in an infinite loop. See module docstring on
    // why federation-supplied cycles aren't constructable in practice.
    #[tokio::test]
    async fn events_before_cycle_handling() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(
                event_id!("$a:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "a",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$a:e")], 10)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].event.event_id.as_str(), "$a:e");
    }

    // D7: missing_events excludes IDs listed in `earliest`.
    #[tokio::test]
    async fn missing_events_excludes_earliest() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$b:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "b",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "c",
                &[event_id!("$b:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .missing_events(
                *ALICE_ROOM_ID,
                &[event_id!("$c:e")],
                &[event_id!("$a:e")],
                10,
            )
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        // a is in `earliest`; walker skips it. c and b returned.
        assert_eq!(ids, ["$c:e", "$b:e"]);
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
        s.create_room(
            &create_event(event_id!("$c2:e"), other_room, *ALICE_ID),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(event_id!("$other:e"), other_room, *ALICE_ID, "x", &[]),
            &[],
        )
        .await
        .unwrap();

        // Seed exists, but in the *other* room.
        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$other:e")], 10)
            .await
            .unwrap();
        assert!(got.is_empty(), "events_before leaked cross-room PDU");

        let got = s
            .missing_events(*ALICE_ROOM_ID, &[event_id!("$other:e")], &[], 10)
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
        s.create_room(
            &create_event(event_id!("$cR2:e"), other_room, *ALICE_ID),
            &[],
        )
        .await
        .unwrap();
        // $b is a real persisted event in R2 — so the edge actually
        // resolves; the cross-room filter is the only thing stopping it.
        s.persist_event(
            &message_with_prev(event_id!("$b:e"), other_room, *ALICE_ID, "b", &[]),
            &[],
        )
        .await
        .unwrap();
        // $a is in R1 with prev_events=[$b:e]. `write_into_tx` only
        // records the edge string — it doesn't validate that the parent
        // is in the same room.
        s.persist_event(
            &message_with_prev(
                event_id!("$a:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "a",
                &[event_id!("$b:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$a:e")], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$a:e"], "walker leaked cross-room PDU via edge");

        // Same shape via missing_events — same underlying walk_prev_events.
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[event_id!("$a:e")], &[], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(
            ids,
            ["$a:e"],
            "missing_events leaked cross-room PDU via edge"
        );
    }

    // D11: limit=0 returns empty without doing any work. Pins the
    // boundary so a refactor that flips the `>=` comparison can't
    // silently start returning one row.
    #[tokio::test]
    async fn events_before_limit_zero_returns_empty() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$a:e")], 0)
            .await
            .unwrap();
        assert!(got.is_empty());
    }

    // D12: BFS with multiple disjoint seeds. The frontier interleaves
    // seeds at each level — pop seed1, push its parents, pop seed2, etc.
    // Build two parallel single-parent chains so the expected order is
    // unambiguous: [c, d, a, b].
    #[tokio::test]
    async fn events_before_handles_multiple_seeds() {
        let s = store_with_room().await;
        for id in [event_id!("$a:e"), event_id!("$b:e")] {
            s.persist_event(
                &message_with_prev(id, *ALICE_ROOM_ID, *ALICE_ID, "x", &[]),
                &[],
            )
            .await
            .unwrap();
        }
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "c",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$d:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "d",
                &[event_id!("$b:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(
                *ALICE_ROOM_ID,
                &[event_id!("$c:e"), event_id!("$d:e")],
                10,
            )
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$c:e", "$d:e", "$a:e", "$b:e"]);
    }

    // D13: diamond DAG — the `visited` set must dedup shared ancestors
    // so they appear exactly once in the result. a → b, a → c, b → d,
    // c → d; walking from [d] should yield d, b, c, a — never a twice
    // even though both b and c reach it.
    #[tokio::test]
    async fn events_before_dedups_shared_ancestors_in_diamond() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$b:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "b",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "c",
                &[event_id!("$a:e")],
            ),
            &[],
        )
        .await
        .unwrap();
        s.persist_event(
            &message_with_prev(
                event_id!("$d:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "d",
                &[event_id!("$b:e"), event_id!("$c:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$d:e")], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$d:e", "$b:e", "$c:e", "$a:e"]);
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
        for id in [event_id!("$g1:e"), event_id!("$g2:e")] {
            s.persist_event(
                &message_with_prev(id, *ALICE_ROOM_ID, *ALICE_ID, "x", &[]),
                &[],
            )
            .await
            .unwrap();
        }
        // Parents both reference grandparents in REVERSE lex order
        // ($g2 before $g1). `fetch_edges` sorts on read so the BFS
        // still visits in $g1 < $g2 order.
        for id in [event_id!("$p1:e"), event_id!("$p2:e")] {
            s.persist_event(
                &message_with_prev(
                    id,
                    *ALICE_ROOM_ID,
                    *ALICE_ID,
                    "x",
                    &[event_id!("$g2:e"), event_id!("$g1:e")],
                ),
                &[],
            )
            .await
            .unwrap();
        }
        // Child references parents in reverse lex order too.
        s.persist_event(
            &message_with_prev(
                event_id!("$c:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "x",
                &[event_id!("$p2:e"), event_id!("$p1:e")],
            ),
            &[],
        )
        .await
        .unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$c:e")], 10)
            .await
            .unwrap();
        let ids: Vec<&str> = got.iter().map(|p| p.event.event_id.as_str()).collect();
        assert_eq!(ids, ["$c:e", "$p1:e", "$p2:e", "$g1:e", "$g2:e"]);
    }

    // D15: `hydrate_pdu` populates `prev_state_events` on the returned
    // `StoredPdu`. The BFS itself only follows `prev` edges (MSC4242
    // state DAG is walked separately), but downstream callers — state
    // resolution, etc. — rely on `prev_state_events` being present on
    // the PDUs `events_before` hands back. Build a message with both
    // edge kinds and assert both fields land.
    #[tokio::test]
    async fn stored_pdu_exposes_prev_state_events() {
        use neutrino_store::StoredEvent;
        use serde_json::{json, value::RawValue};

        let s = store_with_room().await;
        // Build a message whose JSON declares both prev_events and
        // prev_state_events. The values just need to be syntactically
        // valid event IDs — `write_into_tx` writes the edges without
        // validating the parents exist, and the BFS doesn't follow
        // prev_state_events.
        let json_val = json!({
            "event_id": "$msg:e",
            "room_id": ALICE_ROOM_ID.as_str(),
            "sender": ALICE_ID.as_str(),
            "type": "m.room.message",
            "state_key": Option::<String>::None,
            "content": {"body": "msg", "msgtype": "m.text"},
            "origin_server_ts": 0,
            "prev_events": ["$create:e"],
            "prev_state_events": ["$create:e"],
        });
        let json_str = serde_json::to_string(&json_val).unwrap();
        let json = RawValue::from_string(json_str).unwrap();
        let event = StoredEvent {
            event_id: event_id!("$msg:e").to_owned(),
            room_id: ALICE_ROOM_ID.to_owned(),
            event_type: "m.room.message".to_owned(),
            state_key: None,
            sender: ALICE_ID.to_owned(),
            origin_server_ts: 0,
            json,
        };
        s.persist_event(&event, &[]).await.unwrap();

        let got = s
            .events_before(*ALICE_ROOM_ID, &[event_id!("$msg:e")], 1)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        let pdu = &got[0];
        let prev_ids: Vec<&str> = pdu.prev_events.iter().map(|e| e.as_str()).collect();
        let prev_state_ids: Vec<&str> =
            pdu.prev_state_events.iter().map(|e| e.as_str()).collect();
        assert_eq!(prev_ids, ["$create:e"]);
        assert_eq!(prev_state_ids, ["$create:e"]);
    }

    // D16: missing_events limit=0 parity with D11. `events_before` and
    // `missing_events` share `walk_prev_events`, but the wiring through
    // the trait surface is independent — pin the boundary on both
    // surfaces so neither can regress in isolation.
    #[tokio::test]
    async fn missing_events_limit_zero_returns_empty() {
        let s = store_with_room().await;
        s.persist_event(
            &message_with_prev(event_id!("$a:e"), *ALICE_ROOM_ID, *ALICE_ID, "a", &[]),
            &[],
        )
        .await
        .unwrap();
        let got = s
            .missing_events(*ALICE_ROOM_ID, &[event_id!("$a:e")], &[], 0)
            .await
            .unwrap();
        assert!(got.is_empty());
    }
}
