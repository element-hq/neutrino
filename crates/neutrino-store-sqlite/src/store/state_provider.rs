//! [`neutrino_state::StateProvider`] impl backed by a borrowed
//! [`rusqlite::Connection`].
//!
//! `StateProvider` is a synchronous trait — by design (the state-res
//! machinery doesn't `await`). To bridge that to the async [`SqliteStore`],
//! callers construct a `SqliteStateProvider<'a>` *inside* a `run_read` /
//! `run_write` closure on the blocking thread and use it there. The
//! provider holds a borrow of that connection, so its lifetime is tied to
//! the closure scope:
//!
//! ```text
//! storage.run_read(move |conn| {
//!     let provider = SqliteStateProvider::new(conn);
//!     // ... call state-res / room_core::apply against `&provider`
//! }).await
//! ```
//!
//! Two trait methods, both single-query:
//!
//! - [`get_event`]: one SELECT against `events`, hydrating the row through
//!   the existing [`EventRow`] machinery so `auth_events` / `rejected` /
//!   `prev_events` / `prev_state_events` round-trip exactly like every
//!   other read path.
//! - [`auth_chain`]: one recursive CTE seeded from a JSON-array of input
//!   ids, walking `event_edges WHERE edge_type = 'auth'` backwards. A
//!   `LEFT JOIN events` flags any id (seed or transitively discovered)
//!   that doesn't have a row — those bubble up as
//!   [`StateResError::MissingEvent`] per the strict-closure invariant
//!   documented on the trait. `event_edges.parent_event_id` has no FK
//!   (federation backfill can reference parents we haven't yet seen),
//!   so the join is the only way to detect a dangling edge.
//!
//! [`StateResError::MissingEvent`]: neutrino_state::StateResError::MissingEvent
//! [`SqliteStore`]: crate::SqliteStore
//! [`get_event`]: neutrino_state::provider::StateProvider::get_event
//! [`auth_chain`]: neutrino_state::provider::StateProvider::auth_chain

use std::collections::HashSet;
use std::sync::Arc;

use deadpool_sqlite::rusqlite::{Connection, OptionalExtension, params};
use neutrino_common::Event;
use neutrino_state::StateResError;
use neutrino_state::provider::StateProvider;
use ruma::{EventId, OwnedEventId};

use crate::error::Error;
use crate::row::{EVENT_COLUMNS, EventRow};

/// `StateProvider` impl backed by a borrowed [`rusqlite::Connection`].
///
/// Construct inside a `run_read` / `run_write` closure on the blocking
/// thread; lifetime tied to the closure scope. See module-level docs.
pub struct SqliteStateProvider<'a> {
    conn: &'a Connection,
}

impl<'a> SqliteStateProvider<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Both trait methods funnel SQL errors through this — the trait
    /// only knows about [`StateResError`], so driver / hydration faults
    /// surface as `StateResError::Internal`.
    fn into_internal(err: Error) -> StateResError {
        StateResError::Internal(err.to_string())
    }
}

impl StateProvider for SqliteStateProvider<'_> {
    fn get_event(&self, id: &EventId) -> Option<Arc<Event>> {
        // Surface SQL / hydration errors as None — the trait has no
        // fallible return shape here. State-res handles the missing case
        // (it then errors with `MissingEvent` at the right call site),
        // so we don't lose error context: a genuine DB fault and an
        // unknown-id both lead to the same downstream MissingEvent error,
        // which is the right level of detail for the caller. The
        // alternative (panicking on hydration failure) would tear down
        // the whole apply pipeline for a recoverable corruption signal.
        let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE event_id = ?");
        let row: Option<Result<Event, Error>> = self
            .conn
            .query_row(&query, params![id.as_str()], |row| {
                Ok(EventRow::try_from(row).map(EventRow::into_event))
            })
            .optional()
            .ok()?;
        let event = row?.ok()?;
        Some(Arc::new(event))
    }

    fn auth_chain(
        &self,
        seeds: &HashSet<OwnedEventId>,
    ) -> Result<HashSet<OwnedEventId>, StateResError> {
        if seeds.is_empty() {
            return Ok(HashSet::new());
        }

        // Seeds enter the recursive CTE via `json_each(?)`, the recursive
        // arm follows `event_edges WHERE edge_type='auth'` backwards. The
        // outer SELECT LEFT JOINs `events` so a row with `e.event_id IS
        // NULL` flags an id (seed or discovered) that has no row —
        // those become `MissingEvent` errors per the strict-closure
        // invariant.
        let seeds_json: String =
            serde_json::to_string(&seeds.iter().map(|id| id.as_str()).collect::<Vec<_>>())
                .map_err(|e| StateResError::Internal(format!("serialising seeds: {e}")))?;

        let mut stmt = self
            .conn
            .prepare(
                "WITH RECURSIVE chain(event_id) AS ( \
                     SELECT value FROM json_each(?) \
                     UNION \
                     SELECT ee.parent_event_id \
                       FROM event_edges ee \
                       JOIN chain c ON ee.child_event_id = c.event_id \
                      WHERE ee.edge_type = 'auth' \
                 ) \
                 SELECT c.event_id, e.event_id IS NULL AS missing \
                   FROM chain c \
                   LEFT JOIN events e ON c.event_id = e.event_id",
            )
            .map_err(|e| Self::into_internal(Error::Sqlite(e)))?;

        let rows = stmt
            .query_map(params![seeds_json], |row| {
                let id: String = row.get(0)?;
                let missing: bool = row.get(1)?;
                Ok((id, missing))
            })
            .map_err(|e| Self::into_internal(Error::Sqlite(e)))?;

        let mut out: HashSet<OwnedEventId> = HashSet::new();
        for r in rows {
            let (id, missing) = r.map_err(|e| Self::into_internal(Error::Sqlite(e)))?;
            let owned = OwnedEventId::try_from(id).map_err(|e| {
                Self::into_internal(Error::Internal(format!(
                    "malformed event_id in DB row: {e}"
                )))
            })?;
            if missing {
                return Err(StateResError::MissingEvent(owned));
            }
            out.insert(owned);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteStore;
    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, make_event, message_with_ts, store_with_room_and_create,
    };
    use neutrino_store::EventStore;
    use ruma::{OwnedEventId, event_id};
    use serde_json::json;

    /// Run a closure on a reader connection synchronously — wraps the
    /// pool's async `interact` so each test reads like a sync block.
    async fn with_provider<F, T>(s: &SqliteStore, f: F) -> T
    where
        F: FnOnce(&SqliteStateProvider<'_>) -> T + Send + 'static,
        T: Send + 'static,
    {
        s.run_read(move |conn| {
            let provider = SqliteStateProvider::new(conn);
            Ok::<_, Error>(f(&provider))
        })
        .await
        .expect("with_provider")
    }

    /// Build a message with caller-supplied `auth_events`. The base
    /// `make_event` helper doesn't expose auth_events (state-res tests
    /// are the only consumer); mutate after construction so the
    /// reference-hash check stays satisfied (`auth_events` lives on the
    /// struct only, not in `raw`).
    fn message_with_auth(
        body: &str,
        ts: u64,
        auth_events: Vec<OwnedEventId>,
    ) -> neutrino_common::Event {
        let mut ev = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.message",
            None,
            json!({"body": body, "msgtype": "m.text"}),
            ts,
            &[],
            &[],
        );
        ev.auth_events = auth_events;
        ev
    }

    // -------- get_event --------

    #[tokio::test]
    async fn get_event_returns_persisted_event_with_full_struct() {
        let (s, create) = store_with_room_and_create().await;
        let create_id = create.event_id.clone();
        let got = with_provider(&s, move |p| p.get_event(&create_id)).await;
        let got = got.expect("create event present");
        assert_eq!(got.event_id, create.event_id);
        assert_eq!(got.room_id, create.room_id);
        assert_eq!(got.event_type, "m.room.create");
        assert!(!got.rejected);
    }

    #[tokio::test]
    async fn get_event_returns_none_for_unknown_id() {
        let s = store_with_room_and_create().await.0;
        let got = with_provider(&s, |p| {
            p.get_event(event_id!("$nope:example.org")).is_none()
        })
        .await;
        assert!(got, "unknown id must return None");
    }

    #[tokio::test]
    async fn get_event_round_trips_rejected_flag() {
        let (s, create) = store_with_room_and_create().await;
        let create_id_str = create.event_id.as_str().to_owned();
        // No production write path persists `rejected = 1` yet — flip
        // the column directly so the read-back is exercised.
        s.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE events SET rejected = 1 WHERE event_id = ?",
                params![create_id_str],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
        let create_id = create.event_id.clone();
        let got = with_provider(&s, move |p| p.get_event(&create_id)).await;
        assert!(got.expect("event present").rejected);
    }

    // -------- auth_chain --------

    #[tokio::test]
    async fn auth_chain_empty_seeds_returns_empty() {
        let s = store_with_room_and_create().await.0;
        let got = with_provider(&s, |p| p.auth_chain(&HashSet::new()).expect("ok")).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn auth_chain_seed_with_no_auth_events_yields_just_the_seed() {
        // The create event has empty `auth_events` — auth_chain([create])
        // should return {create}.
        let (s, create) = store_with_room_and_create().await;
        let create_id = create.event_id.clone();
        let seeds: HashSet<OwnedEventId> = [create_id.clone()].into_iter().collect();
        let got = with_provider(&s, move |p| p.auth_chain(&seeds).expect("ok")).await;
        assert_eq!(got, [create_id].into_iter().collect::<HashSet<_>>());
    }

    #[tokio::test]
    async fn auth_chain_walks_linear_chain_via_auth_events() {
        // a (no auth) ← b (auth=[a]) ← c (auth=[b])
        // Seed [c] yields {a, b, c}.
        let (s, create) = store_with_room_and_create().await;
        let a = message_with_auth("a", 1, vec![create.event_id.clone()]);
        let a_id = a.event_id.clone();
        s.persist_event(&a, &[]).await.unwrap();

        let b = message_with_auth("b", 2, vec![a_id.clone()]);
        let b_id = b.event_id.clone();
        s.persist_event(&b, &[]).await.unwrap();

        let c = message_with_auth("c", 3, vec![b_id.clone()]);
        let c_id = c.event_id.clone();
        s.persist_event(&c, &[]).await.unwrap();

        let seeds: HashSet<OwnedEventId> = [c_id.clone()].into_iter().collect();
        let got = with_provider(&s, move |p| p.auth_chain(&seeds).expect("ok")).await;

        let expected: HashSet<OwnedEventId> =
            [create.event_id, a_id, b_id, c_id].into_iter().collect();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn auth_chain_diamond_dedups_shared_ancestor() {
        //       a
        //      / \
        //     b   c
        //      \ /
        //       d
        // Seed [d] yields {a, b, c, d} (a once, not twice).
        let (s, create) = store_with_room_and_create().await;
        let a = message_with_auth("a", 1, vec![create.event_id.clone()]);
        let a_id = a.event_id.clone();
        s.persist_event(&a, &[]).await.unwrap();

        let b = message_with_auth("b", 2, vec![a_id.clone()]);
        let b_id = b.event_id.clone();
        s.persist_event(&b, &[]).await.unwrap();

        let c = message_with_auth("c", 3, vec![a_id.clone()]);
        let c_id = c.event_id.clone();
        s.persist_event(&c, &[]).await.unwrap();

        let d = message_with_auth("d", 4, vec![b_id.clone(), c_id.clone()]);
        let d_id = d.event_id.clone();
        s.persist_event(&d, &[]).await.unwrap();

        let seeds: HashSet<OwnedEventId> = [d_id.clone()].into_iter().collect();
        let got = with_provider(&s, move |p| p.auth_chain(&seeds).expect("ok")).await;

        let expected: HashSet<OwnedEventId> = [create.event_id, a_id, b_id, c_id, d_id]
            .into_iter()
            .collect();
        assert_eq!(got, expected);
        assert_eq!(got.len(), 5, "shared ancestor surfaced more than once");
    }

    #[tokio::test]
    async fn auth_chain_unknown_seed_returns_missing_event() {
        let s = store_with_room_and_create().await.0;
        let fake: OwnedEventId = "$nope:example.org".parse().unwrap();
        let seeds: HashSet<OwnedEventId> = [fake.clone()].into_iter().collect();
        let err = with_provider(&s, move |p| p.auth_chain(&seeds).unwrap_err()).await;
        match err {
            StateResError::MissingEvent(id) => assert_eq!(id, fake),
            other => panic!("expected MissingEvent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_chain_dangling_parent_edge_returns_missing_event() {
        // Inject an auth edge whose parent has no row in `events`. The
        // FK on `child_event_id` keeps the child honest, but
        // `parent_event_id` has no FK (federation backfill can reference
        // unseen parents) — so a hand-crafted dangling auth edge is
        // exactly the corruption case the strict-closure invariant
        // exists to catch.
        let (s, create) = store_with_room_and_create().await;
        let a = message_with_ts(*ALICE_ROOM_ID, *ALICE_USER_ID, "a", 1);
        let a_id = a.event_id.clone();
        s.persist_event(&a, &[]).await.unwrap();

        let a_id_str = a_id.as_str().to_owned();
        s.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO event_edges (child_event_id, edge_type, parent_event_id) \
                 VALUES (?, 'auth', '$ghost:example.org')",
                params![a_id_str],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

        let seeds: HashSet<OwnedEventId> = [a_id].into_iter().collect();
        let err = with_provider(&s, move |p| p.auth_chain(&seeds).unwrap_err()).await;
        let _ = create; // keep alive
        match err {
            StateResError::MissingEvent(id) => {
                assert_eq!(id.as_str(), "$ghost:example.org");
            }
            other => panic!("expected MissingEvent, got {other:?}"),
        }
    }
}
