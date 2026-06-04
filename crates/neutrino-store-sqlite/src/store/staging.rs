//! `StagingStore` impl on `SqliteStore`.
//!
//! The pre-auth holding pen for federation ancestry fetched during gap-fill.
//! See the `staged_events` table comment in `schema.sql` and the
//! `neutrino_store::StagingStore` trait docs for the contract. Nothing here
//! advances the persist watch — staged events are invisible until promoted.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{params, params_from_iter};
use neutrino_store::{AncestryGap, StagedPdu, StagingStore, StorageError};
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, ServerName};
use serde_json::value::RawValue as RawJsonValue;

use crate::{SqliteStore, error::Error};

/// IN-clause / batch chunk size, well below `SQLITE_LIMIT_VARIABLE_NUMBER` on
/// every build (default 999 pre-3.32, 32766 since). Matches `events.rs`.
const MAX_PARAMS: usize = 900;

#[async_trait]
impl StagingStore for SqliteStore {
    async fn stage_pdu(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        raw: &RawJsonValue,
    ) -> Result<(), StorageError> {
        let origin = origin.as_str().to_owned();
        let room_id = room_id.as_str().to_owned();
        let event_id = event_id.as_str().to_owned();
        let json = raw.get().to_owned();

        self.run_write(move |conn| -> Result<(), Error> {
            // INSERT OR IGNORE: a peer may resend, and gap-fill may re-fetch,
            // the same event; the event_id is derived from the bytes, so a
            // collision is a genuine duplicate — keep the first.
            conn.execute(
                "INSERT OR IGNORE INTO staged_events (event_id, room_id, origin, json) \
                 VALUES (?, ?, ?, ?)",
                params![event_id, room_id, origin, json],
            )?;
            Ok(())
        })
        .await
    }

    async fn staged_rooms(&self) -> Result<Vec<OwnedRoomId>, StorageError> {
        self.run_read(move |conn| -> Result<Vec<OwnedRoomId>, Error> {
            let mut stmt = conn.prepare("SELECT DISTINCT room_id FROM staged_events")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows {
                let id = OwnedRoomId::try_from(r?)
                    .map_err(|e| Error::Internal(format!("malformed staged room_id: {e}")))?;
                out.push(id);
            }
            Ok(out)
        })
        .await
    }

    async fn staged_for_room(&self, room_id: &RoomId) -> Result<Vec<StagedPdu>, StorageError> {
        let room_id = room_id.as_str().to_owned();
        self.run_read(move |conn| -> Result<Vec<StagedPdu>, Error> {
            let mut stmt =
                conn.prepare("SELECT event_id, origin, json FROM staged_events WHERE room_id = ?")?;
            let rows = stmt.query_map(params![room_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            let mut out = Vec::new();
            for r in rows {
                let (event_id, origin, json) = r?;
                let event_id = OwnedEventId::try_from(event_id)
                    .map_err(|e| Error::Internal(format!("malformed staged event_id: {e}")))?;
                let origin = OwnedServerName::try_from(origin)
                    .map_err(|e| Error::Internal(format!("malformed staged origin: {e}")))?;
                let raw = RawJsonValue::from_string(json).map_err(|e| {
                    Error::Internal(format!("malformed staged json in DB row: {e}"))
                })?;
                out.push(StagedPdu {
                    event_id,
                    origin,
                    raw,
                });
            }
            Ok(out)
        })
        .await
    }

    async fn ancestry_gap(
        &self,
        room_id: &RoomId,
        heads: &[&EventId],
    ) -> Result<AncestryGap, StorageError> {
        let room_id = room_id.as_str().to_owned();
        // Seed the recursive walk with a JSON array of the head ids so a
        // single `json_each(?)` expands them — avoids an N-placeholder IN list
        // inside the CTE.
        let heads_json =
            serde_json::to_string(&heads.iter().map(|e| e.as_str()).collect::<Vec<_>>())
                .map_err(|e| Error::Internal(format!("serialising ancestry heads: {e}")))?;

        self.run_read(move |conn| -> Result<AncestryGap, Error> {
            // Walk `prev_state_events` back from the heads *through staged
            // events only* — a committed `events` row is a grounded boundary
            // and is never expanded (the state ancestry below it is already
            // ours). `UNION` (not `UNION ALL`) dedups, so diamonds and cycles
            // in the DAG terminate. For each reached id, classify it: staged
            // (cache hit, to promote), committed (grounded boundary, ignore),
            // or neither (the still-missing frontier to fetch).
            let mut stmt = conn.prepare(
                "WITH RECURSIVE walk(event_id) AS ( \
                     SELECT je.value FROM json_each(?1) AS je \
                   UNION \
                     SELECT cje.value \
                     FROM walk w \
                     JOIN staged_events s \
                       ON s.event_id = w.event_id AND s.room_id = ?2 \
                     JOIN json_each(s.json, '$.prev_state_events') AS cje \
                 ) \
                 SELECT \
                     w.event_id, \
                     EXISTS(SELECT 1 FROM staged_events s2 \
                            WHERE s2.event_id = w.event_id AND s2.room_id = ?2), \
                     EXISTS(SELECT 1 FROM events e \
                            WHERE e.event_id = w.event_id AND e.room_id = ?2) \
                 FROM walk w",
            )?;
            let rows = stmt.query_map(params![heads_json, room_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })?;

            let mut gap = AncestryGap::default();
            for r in rows {
                let (id, is_staged, is_committed) = r?;
                let id = OwnedEventId::try_from(id)
                    .map_err(|e| Error::Internal(format!("malformed staged event_id: {e}")))?;
                if is_staged {
                    gap.staged.push(id);
                } else if !is_committed {
                    gap.missing.push(id);
                }
            }
            Ok(gap)
        })
        .await
    }

    async fn staged_raw(
        &self,
        event_ids: &[&EventId],
    ) -> Result<Vec<Box<RawJsonValue>>, StorageError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = event_ids.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<Box<RawJsonValue>>, Error> {
            let mut out = Vec::with_capacity(ids.len());
            for chunk in ids.chunks(MAX_PARAMS) {
                let placeholders = vec!["?"; chunk.len()].join(",");
                let query =
                    format!("SELECT json FROM staged_events WHERE event_id IN ({placeholders})");
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })?;
                for row in rows {
                    let json = row?;
                    let raw = RawJsonValue::from_string(json).map_err(|e| {
                        Error::Internal(format!("malformed staged json in DB row: {e}"))
                    })?;
                    out.push(raw);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn unstage_events(&self, event_ids: &[&EventId]) -> Result<(), StorageError> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = event_ids.iter().map(|e| e.as_str().to_owned()).collect();

        self.run_write(move |conn| -> Result<(), Error> {
            for chunk in ids.chunks(MAX_PARAMS) {
                let placeholders = vec!["?"; chunk.len()].join(",");
                let query = format!("DELETE FROM staged_events WHERE event_id IN ({placeholders})");
                conn.execute(&query, params_from_iter(chunk.iter()))?;
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::{EventStore, StagingStore};
    use ruma::{ServerName, server_name};
    use serde_json::json;

    use crate::tests::{ALICE_ROOM_ID, ALICE_USER_ID, make_event, store_with_room_and_create};

    /// The originating server recorded for staged rows in these tests.
    fn origin() -> &'static ServerName {
        server_name!("remote.example.org")
    }

    /// Build a state event on the given state-DAG head.
    fn state_event(prev_state: &[&ruma::EventId], ts: u64) -> neutrino_common::Event {
        make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.topic",
            Some(""),
            json!({ "topic": format!("t{ts}") }),
            ts,
            prev_state, // prev_events mirror prev_state for these fixtures
            prev_state,
        )
    }

    #[tokio::test]
    async fn stage_staged_raw_and_unstage_roundtrip() {
        let (s, create) = store_with_room_and_create().await;
        let a = state_event(&[create.event_id.as_ref()], 1);

        s.stage_pdu(origin(), *ALICE_ROOM_ID, &a.event_id, &a.raw)
            .await
            .unwrap();
        // Idempotent: re-staging the same id is a no-op.
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &a.event_id, &a.raw)
            .await
            .unwrap();

        let raws = s.staged_raw(&[a.event_id.as_ref()]).await.unwrap();
        assert_eq!(raws.len(), 1);
        assert_eq!(raws[0].get(), a.raw.get());

        s.unstage_events(&[a.event_id.as_ref()]).await.unwrap();
        assert!(
            s.staged_raw(&[a.event_id.as_ref()])
                .await
                .unwrap()
                .is_empty()
        );
        // Unstaging a missing id is a no-op, not an error.
        s.unstage_events(&[a.event_id.as_ref()]).await.unwrap();
    }

    #[tokio::test]
    async fn ancestry_gap_classifies_missing_staged_committed() {
        // Chain: create ←(prev_state) A ← B ← C. create is committed; we stage
        // and commit the rest in stages to exercise each classification.
        let (s, create) = store_with_room_and_create().await;
        let a = state_event(&[create.event_id.as_ref()], 1);
        let b = state_event(&[a.event_id.as_ref()], 2);
        let c = state_event(&[b.event_id.as_ref()], 3);

        // Stage C only. Walking from C's parent B: B is neither staged nor
        // committed ⇒ the missing frontier.
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &c.event_id, &c.raw)
            .await
            .unwrap();
        let gap = s
            .ancestry_gap(*ALICE_ROOM_ID, &[c.event_id.as_ref()])
            .await
            .unwrap();
        assert_eq!(gap.missing, vec![b.event_id.clone()]);
        assert_eq!(gap.staged, vec![c.event_id.clone()]);

        // Stage B too. Now the frontier recedes to A (B's parent).
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &b.event_id, &b.raw)
            .await
            .unwrap();
        let gap = s
            .ancestry_gap(*ALICE_ROOM_ID, &[c.event_id.as_ref()])
            .await
            .unwrap();
        assert_eq!(gap.missing, vec![a.event_id.clone()]);
        let mut staged = gap.staged;
        staged.sort();
        let mut want = vec![b.event_id.clone(), c.event_id.clone()];
        want.sort();
        assert_eq!(staged, want);

        // Commit A. Its parent is create (committed) ⇒ nothing missing; the
        // staged subgraph {B, C} is now fully grounded and promotable.
        s.persist_historical_event(&a).await.unwrap();
        let gap = s
            .ancestry_gap(*ALICE_ROOM_ID, &[c.event_id.as_ref()])
            .await
            .unwrap();
        assert!(gap.missing.is_empty(), "grounded once A is committed");
        assert_eq!(gap.staged.len(), 2);
    }

    #[tokio::test]
    async fn ancestry_gap_dedups_diamond_parents() {
        // Two staged events C and D both name A in prev_state_events (a diamond
        // merging at A). The recursive CTE uses UNION, so A must appear in
        // `missing` exactly once — a regression to UNION ALL would duplicate it.
        let (s, create) = store_with_room_and_create().await;
        let a = state_event(&[create.event_id.as_ref()], 1); // never staged/committed
        let c = state_event(&[a.event_id.as_ref()], 2);
        let d = state_event(&[a.event_id.as_ref()], 3);
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &c.event_id, &c.raw)
            .await
            .unwrap();
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &d.event_id, &d.raw)
            .await
            .unwrap();

        let gap = s
            .ancestry_gap(*ALICE_ROOM_ID, &[c.event_id.as_ref(), d.event_id.as_ref()])
            .await
            .unwrap();
        assert_eq!(
            gap.missing,
            vec![a.event_id.clone()],
            "A reached via two staged paths must appear once (UNION, not UNION ALL)"
        );
        let mut staged = gap.staged;
        staged.sort();
        let mut want = vec![c.event_id.clone(), d.event_id.clone()];
        want.sort();
        assert_eq!(staged, want);
    }

    #[tokio::test]
    async fn ancestry_gap_is_scoped_to_room() {
        // An event staged under room A must not count as held when the walk is
        // scoped to a different room id.
        let (s, create) = store_with_room_and_create().await;
        let a = state_event(&[create.event_id.as_ref()], 1);
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &a.event_id, &a.raw)
            .await
            .unwrap();

        let other_room = ruma::room_id!("!other:example.org");
        let gap = s
            .ancestry_gap(other_room, &[a.event_id.as_ref()])
            .await
            .unwrap();
        // Under the other room it is neither staged nor committed → missing.
        assert_eq!(gap.missing, vec![a.event_id.clone()]);
        assert!(gap.staged.is_empty());
    }

    #[tokio::test]
    async fn staged_rooms_and_staged_for_room_roundtrip() {
        // `staged_rooms` returns the distinct rooms with pending work;
        // `staged_for_room` returns that room's rows carrying origin + raw.
        let (s, create) = store_with_room_and_create().await;
        let a = state_event(&[create.event_id.as_ref()], 1);
        let b = state_event(&[a.event_id.as_ref()], 2);

        // No staged rows yet ⇒ no rooms.
        assert!(s.staged_rooms().await.unwrap().is_empty());

        s.stage_pdu(origin(), *ALICE_ROOM_ID, &a.event_id, &a.raw)
            .await
            .unwrap();
        s.stage_pdu(origin(), *ALICE_ROOM_ID, &b.event_id, &b.raw)
            .await
            .unwrap();

        // One distinct room despite two staged events.
        assert_eq!(
            s.staged_rooms().await.unwrap(),
            vec![ALICE_ROOM_ID.to_owned()]
        );

        let mut rows = s.staged_for_room(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(rows.len(), 2);
        // Each row carries the origin we staged under and its canonical bytes.
        assert!(rows.iter().all(|p| p.origin == origin()));
        rows.sort_by(|x, y| x.event_id.cmp(&y.event_id));
        let mut want = [(&a.event_id, a.raw.get()), (&b.event_id, b.raw.get())];
        want.sort_by(|x, y| x.0.cmp(y.0));
        for (row, (id, raw)) in rows.iter().zip(want) {
            assert_eq!(&row.event_id, id);
            assert_eq!(row.raw.get(), raw);
        }

        // A different room has no staged rows.
        assert!(
            s.staged_for_room(ruma::room_id!("!other:example.org"))
                .await
                .unwrap()
                .is_empty()
        );
    }
}
