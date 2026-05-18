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

/// Fetch an event + its `prev_events` / `prev_state_events` from the DB.
/// Returns `Ok(None)` if the event is not in `events` — federation
/// backfill may reference parents we haven't yet seen.
fn hydrate_pdu(conn: &Connection, event_id: &str) -> Result<Option<StoredPdu>, Error> {
    let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE event_id = ?");
    let event_result: Option<Result<StoredEvent, Error>> = conn
        .query_row(&query, params![event_id], |row| {
            Ok(EventRow::try_from(row).map(EventRow::into_event))
        })
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
    child_event_id: &str,
    edge_type: &str,
) -> Result<Vec<OwnedEventId>, Error> {
    let mut stmt = conn.prepare(
        "SELECT parent_event_id FROM event_edges \
         WHERE child_event_id = ? AND edge_type = ?",
    )?;
    let rows = stmt.query_map(params![child_event_id, edge_type], |row| {
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
/// (and don't seed traversal). The result preserves traversal order
/// (reverse-chronological).
fn walk_prev_events(
    conn: &Connection,
    start: Vec<String>,
    excluded: &HashSet<String>,
    limit: usize,
) -> Result<Vec<StoredPdu>, Error> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: VecDeque<String> = start.into_iter().collect();
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
        let Some(pdu) = hydrate_pdu(conn, &id)? else {
            // Event not in local store — federation-backfill boundary.
            continue;
        };
        for parent in &pdu.prev_events {
            frontier.push_back(parent.as_str().to_owned());
        }
        results.push(pdu);
    }
    Ok(results)
}

#[async_trait]
impl DagStore for SqliteStore {
    async fn events_before(
        &self,
        _room_id: &RoomId,
        from: &[&EventId],
        limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        let from: Vec<String> = from.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<StoredPdu>, Error> {
            walk_prev_events(conn, from, &HashSet::new(), limit)
        })
        .await
    }

    async fn missing_events(
        &self,
        _room_id: &RoomId,
        latest: &[&EventId],
        earliest: &[&EventId],
        limit: usize,
    ) -> Result<Vec<StoredPdu>, StorageError> {
        let latest: Vec<String> = latest.iter().map(|e| e.as_str().to_owned()).collect();
        let earliest: HashSet<String> = earliest.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<StoredPdu>, Error> {
            walk_prev_events(conn, latest, &earliest, limit)
        })
        .await
    }
}
