//! `RoomStore` impl on `SqliteStore`.

use std::collections::BTreeSet;
use std::str::FromStr;

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_common::ROOM_VERSION_ID;
use neutrino_store::{Event, RoomStore, StorageError};
use ruma::{OwnedEventId, RoomId, RoomVersionId};
use serde_json::Value;

use crate::{SqliteStore, error::Error, row::EventRow};

#[async_trait]
impl RoomStore for SqliteStore {
    async fn create_room(
        &self,
        create_event: &Event,
        initial_events: &[Event],
    ) -> Result<(), StorageError> {
        // The schema is structurally agnostic to which (event_type,
        // state_key) lands in `events` / `current_state`. A non-create
        // or wrong-state-key event would still INSERT cleanly but leave
        // the room without a proper `("m.room.create", "")` state row —
        // silent corruption that downstream auth / state resolution
        // assumes can't happen. Fail fast at the trait boundary.
        if create_event.event_type != "m.room.create" {
            return Err(Error::InvalidInput(format!(
                "create_event has event_type {:?}, expected \"m.room.create\"",
                create_event.event_type
            ))
            .into());
        }
        if create_event.state_key.as_deref() != Some("") {
            return Err(Error::InvalidInput(format!(
                "create_event state_key must be Some(\"\"), got {:?}",
                create_event.state_key
            ))
            .into());
        }

        // Pull `content.room_version` out of the create event JSON before
        // crossing the closure boundary. Caller-supplied JSON, so a
        // missing/malformed field is `InvalidInput`. We compare against
        // `ROOM_VERSION_ID` (the unstable MSC4242 prefix) directly — going
        // through `RoomVersionId::from_str` would not have caught the
        // off-by-one between MSC4242's `"org.matrix.msc4242.12"` and ruma's
        // bare-`"12"` `V12` variant, because ruma silently maps unknown
        // strings into `RoomVersionId::Custom(...)` rather than erroring.
        let parsed: Value = serde_json::from_str(create_event.raw.get())
            .map_err(|e| Error::InvalidInput(format!("create_event json: {e}")))?;
        let room_version_str = parsed
            .pointer("/content/room_version")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::InvalidInput("create_event missing content.room_version".into())
            })?;

        // Reject at the create-room boundary rather than letting an
        // unsupported version land in the DB and surprise downstream code
        // that assumes the MSC4242-on-v12 state-resolution / auth rules.
        // Relax this gate if we ever broaden the target.
        if room_version_str != ROOM_VERSION_ID {
            return Err(Error::InvalidInput(format!(
                "unsupported room_version {room_version_str:?}; only {ROOM_VERSION_ID} is supported"
            ))
            .into());
        }

        // v12 m.room.create is the genesis event — it has no parents.
        // A create event that declares `prev_events` or `prev_state_events`
        // would otherwise land silently and write orphan `event_edges` rows
        // pointing at non-existent ancestors, which would then show up as
        // federation-backfill boundaries on every DAG walk. Reject at the
        // create boundary; arrays must be either absent or empty.
        let prev_events_empty = parsed
            .get("prev_events")
            .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()));
        let prev_state_events_empty = parsed
            .get("prev_state_events")
            .is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()));
        if !prev_events_empty {
            return Err(
                Error::InvalidInput("create_event must not declare prev_events".into()).into(),
            );
        }
        if !prev_state_events_empty {
            return Err(Error::InvalidInput(
                "create_event must not declare prev_state_events".into(),
            )
            .into());
        }

        // Wrap borrowed events into `'static` `EventRow`s for the closure.
        let create_event = EventRow::from(create_event).to_owned();
        let initial_events: Vec<EventRow<'static>> = initial_events
            .iter()
            .map(|e| EventRow::from(e).to_owned())
            .collect();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            let tx = conn.transaction()?;

            // 1. Register the room row first — the events table's
            //    `room_id REFERENCES rooms(room_id)` FK would reject the
            //    create event otherwise. `create_event.room_id` resolves
            //    through `EventRow: Deref<Target = Event>`.
            //    Version is checked == ROOM_VERSION_ID above; stored
            //    verbatim so a future column query can grep for it.
            tx.execute(
                "INSERT INTO rooms (room_id, room_version) VALUES (?, ?)",
                params![create_event.room_id.as_str(), ROOM_VERSION_ID],
            )?;

            // 2. Write the create event itself.
            let mut last_pos = create_event.write_into_tx(&tx)?;

            // 3. Write every initial event in order.
            for ev in &initial_events {
                last_pos = ev.write_into_tx(&tx)?;
            }

            // 4. Seed the room's forward extremities. createRoom writes a
            //    linear chain of state events (create → power_levels →
            //    creator-join → …), so the last event written is the sole
            //    head of *both* the timeline DAG and the state DAG; an empty
            //    `initial_events` leaves the create event as that head. This
            //    is what lets the per-room actor bootstrap a freshly created
            //    room (a `[]` head-set would give a new local event no
            //    `prev_events`). A non-linear / non-state initial batch would
            //    need the resolved heads computed via apply — out of scope
            //    while createRoom only emits linear state chains.
            let head = initial_events
                .last()
                .map(|e| e.event_id.as_str())
                .unwrap_or_else(|| create_event.event_id.as_str());
            let head_json = serde_json::to_string(&[head])
                .map_err(|e| Error::Internal(format!("serialising forward_extremities: {e}")))?;
            tx.execute(
                "UPDATE rooms \
                 SET forward_extremities = ?, state_dag_forward_extremities = ? \
                 WHERE room_id = ?",
                params![head_json, head_json, create_event.room_id.as_str()],
            )?;

            tx.commit()?;

            // 5. One watch advance for the whole batch — clients waking
            //    up will fetch all the new events via a single
            //    `events_after`.
            SqliteStore::notify_watch(&watch_tx, last_pos);

            Ok(())
        })
        .await
    }

    async fn get_room_version(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<RoomVersionId>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(move |conn| -> Result<Option<RoomVersionId>, Error> {
            let row: Option<String> = conn
                .query_row(
                    "SELECT room_version FROM rooms WHERE room_id = ?",
                    params![room_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;

            row.map(|s| {
                RoomVersionId::from_str(&s)
                    .map_err(|e| Error::Internal(format!("malformed room_version in DB: {e}")))
            })
            .transpose()
        })
        .await
    }

    async fn room_count(&self) -> Result<u64, StorageError> {
        self.run_read(move |conn| -> Result<u64, Error> {
            let n: i64 = conn.query_row("SELECT COUNT(*) FROM rooms", [], |r| r.get(0))?;
            Ok(n as u64)
        })
        .await
    }

    async fn forward_extremities(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<(BTreeSet<OwnedEventId>, BTreeSet<OwnedEventId>)>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(
            move |conn| -> Result<Option<(BTreeSet<OwnedEventId>, BTreeSet<OwnedEventId>)>, Error> {
                let row: Option<(String, String)> = conn
                    .query_row(
                        "SELECT forward_extremities, state_dag_forward_extremities \
                         FROM rooms WHERE room_id = ?",
                        params![room_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;

                match row {
                    None => Ok(None),
                    Some((timeline_json, state_json)) => Ok(Some((
                        parse_event_id_set(&timeline_json)?,
                        parse_event_id_set(&state_json)?,
                    ))),
                }
            },
        )
        .await
    }
}

/// Parse a JSON array of event ids (as stored in the `rooms`
/// forward-extremity columns) into a `BTreeSet`. A malformed column or a
/// non-parseable id is DB corruption, surfaced as `Internal` rather than
/// swallowed.
fn parse_event_id_set(json: &str) -> Result<BTreeSet<OwnedEventId>, Error> {
    let ids: Vec<String> = serde_json::from_str(json)
        .map_err(|e| Error::Internal(format!("malformed forward_extremities json: {e}")))?;
    ids.into_iter()
        .map(|s| {
            OwnedEventId::try_from(s).map_err(|e| {
                Error::Internal(format!("malformed event_id in forward_extremities: {e}"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use deadpool_sqlite::rusqlite::params;
    use neutrino_common::ROOM_VERSION_ID;
    use neutrino_store::{EventStore, RoomStore, StorageError, StreamPos};
    use ruma::{RoomVersionId, event_id, room_id};
    use serde_json::json;

    use crate::{
        error::Error,
        tests::{
            ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, create_event, make_event, member_join, store,
        },
    };

    // R3: missing content.room_version
    #[tokio::test]
    async fn create_room_rejects_missing_room_version() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            json!({"creator": ALICE_USER_ID.as_str()}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R3a: content.room_version is a JSON number, not a string
    #[tokio::test]
    async fn create_room_rejects_non_string_room_version() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            json!({"creator": ALICE_USER_ID.as_str(), "room_version": 12}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R3b: content.room_version is the empty string — passes the
    // JSON-shape check but fails `RoomVersionId::from_str` (room version
    // ids must be 1..=32 chars per Matrix spec).
    #[tokio::test]
    async fn create_room_rejects_empty_room_version() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            json!({"creator": ALICE_USER_ID.as_str(), "room_version": ""}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R3c: content.room_version parses as a valid identifier but isn't
    // the MSC4242 unstable v12. Out of scope for now (CLAUDE.md: only
    // MSC4242-on-v12 targeted) — relax if we ever broaden the target.
    // Covers both the stable `"12"` (which ruma's RoomVersionId::V12
    // would match but we do not) and an older numbered version.
    #[tokio::test]
    async fn create_room_rejects_non_v12_room_version() {
        let store = store().await;
        for version in ["11", "12"] {
            let bad = make_event(
                *ALICE_ROOM_ID,
                *ALICE_USER_ID,
                "m.room.create",
                Some(""),
                json!({"creator": ALICE_USER_ID.as_str(), "room_version": version}),
                0,
                &[],
                &[],
            );
            let result = store.create_room(&bad, &[]).await;
            assert!(
                matches!(result, Err(StorageError::InvalidInput(_))),
                "expected InvalidInput for room_version={version:?}, got {result:?}"
            );
        }
    }

    // R3d: m.room.create with non-empty prev_events is rejected. v12 spec:
    // create is the genesis event, no parents. Without the gate, the
    // create event lands and writes orphan event_edges rows that show up
    // as federation-backfill boundaries on every subsequent DAG walk.
    #[tokio::test]
    async fn create_room_rejects_create_event_with_prev_events() {
        use crate::tests::make_event_with_raw_json;
        let store = store().await;
        let bad = make_event_with_raw_json(
            event_id!("$c1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            &format!(
                r#"{{
                "content": {{"creator": "@alice:example.com", "room_version": "{ROOM_VERSION_ID}"}},
                "prev_events": ["$ghost:example.com"],
                "prev_state_events": []
            }}"#
            ),
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
        assert_eq!(store.room_count().await.unwrap(), 0);
    }

    // R3e: same as R3d but for prev_state_events — the MSC4242 state-DAG
    // genesis case. The create event itself bootstraps state, so it can't
    // claim prior state ancestors.
    #[tokio::test]
    async fn create_room_rejects_create_event_with_prev_state_events() {
        use crate::tests::make_event_with_raw_json;
        let store = store().await;
        let bad = make_event_with_raw_json(
            event_id!("$c1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            &format!(
                r#"{{
                "content": {{"creator": "@alice:example.com", "room_version": "{ROOM_VERSION_ID}"}},
                "prev_events": [],
                "prev_state_events": ["$ghost:example.com"]
            }}"#
            ),
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
        assert_eq!(store.room_count().await.unwrap(), 0);
    }

    // R3f: arrays present but empty — accepted (this is the JSON shape
    // our own test helpers emit for the create event).
    #[tokio::test]
    async fn create_room_accepts_empty_prev_arrays() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce, &[]).await.unwrap();
        assert_eq!(store.room_count().await.unwrap(), 1);
    }

    // R4: top-level JSON is a string, not an object
    #[tokio::test]
    async fn create_room_rejects_invalid_json_shape() {
        use crate::tests::make_event_with_raw_json;
        let store = store().await;
        let bad = make_event_with_raw_json(
            event_id!("$c1:example.com"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some(""),
            "\"hello\"",
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R5: duplicate room_id rejected on second create
    #[tokio::test]
    async fn create_room_rejects_duplicate_room_id() {
        let store = store().await;
        let ce1 = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce1, &[]).await.unwrap();

        // same room_id
        let ce2 = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let result = store.create_room(&ce2, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R10: initial_event with mismatched room_id rejected (FK)
    #[tokio::test]
    async fn create_room_rejects_initial_event_with_wrong_room_id() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        // member event for a different room
        let bad_member = member_join(*BOB_ROOM_ID, *ALICE_USER_ID);
        let result = store.create_room(&ce, &[bad_member]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    // R8: get_room_version on unknown room → None
    #[tokio::test]
    async fn get_room_version_none_for_unknown() {
        let store = store().await;
        let unknown = room_id!("!nope:example.com");
        assert!(store.get_room_version(unknown).await.unwrap().is_none());
    }

    // R9: room_count starts at 0 and increments per create_room
    #[tokio::test]
    async fn room_count_zero_then_increments() {
        let store = store().await;
        assert_eq!(store.room_count().await.unwrap(), 0);

        store
            .create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        assert_eq!(store.room_count().await.unwrap(), 1);

        store
            .create_room(&create_event(*BOB_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        assert_eq!(store.room_count().await.unwrap(), 2);
    }

    // R11: empty initial_events slice is allowed
    #[tokio::test]
    async fn create_room_empty_initial_events_ok() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce, &[]).await.unwrap();
        assert_eq!(store.room_count().await.unwrap(), 1);
    }

    // R6: event_type must be exactly "m.room.create". Schema doesn't
    // constrain which event_type lands as the create event, so this is
    // enforced at the trait boundary.
    #[tokio::test]
    async fn create_room_rejects_wrong_event_type() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(""),
            json!({"creator": ALICE_USER_ID.as_str(), "room_version": ROOM_VERSION_ID}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
        assert_eq!(store.room_count().await.unwrap(), 0);
    }

    // R7a: state_key = None is rejected — v12 m.room.create must have
    // an empty-string state key, not a missing one.
    #[tokio::test]
    async fn create_room_rejects_none_state_key() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            None,
            json!({"creator": ALICE_USER_ID.as_str(), "room_version": ROOM_VERSION_ID}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
        assert_eq!(store.room_count().await.unwrap(), 0);
    }

    // R7b: non-empty state_key is rejected.
    #[tokio::test]
    async fn create_room_rejects_nonempty_state_key() {
        let store = store().await;
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.create",
            Some("not-empty"),
            json!({"creator": ALICE_USER_ID.as_str(), "room_version": ROOM_VERSION_ID}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&bad, &[]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
        assert_eq!(store.room_count().await.unwrap(), 0);
    }

    // R12: success path — `create_room` with a valid create event plus
    // initial events makes (a) `get_room_version` return
    // `Some(V12)` and (b) every persisted event observable via the
    // EventStore read surface. Covers the rooms INSERT, the room-version
    // encoding round-trip through `RoomVersionId`, and the
    // `EventRow::write_into_tx` path for both the create event and the
    // initial-event batch.
    #[tokio::test]
    async fn create_room_persists_create_and_initial_events() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let create_id = ce.event_id.clone();
        let initial_member = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let member_id = initial_member.event_id.clone();
        store.create_room(&ce, &[initial_member]).await.unwrap();

        // (a) room version round-trips as the MSC4242 unstable id. ruma
        //     doesn't model MSC4242, so this comes back as
        //     `RoomVersionId::Custom(...)` rather than `V12` (the bare
        //     `"12"` variant — different wire string).
        let v = store.get_room_version(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(v, Some(RoomVersionId::from_str(ROOM_VERSION_ID).unwrap()));

        // (b) both events observable via `get_events`.
        let got = store.get_events(&[&create_id, &member_id]).await.unwrap();
        let ids: std::collections::HashSet<&str> =
            got.iter().map(|e| e.event_id.as_str()).collect();
        assert!(ids.contains(create_id.as_str()));
        assert!(ids.contains(member_id.as_str()));

        // (c) both also visible via `events_after` — sanity-checks
        // stream_pos got assigned (AUTOINCREMENT path through
        // `write_into_tx`).
        let stream = store.events_after(StreamPos(0), 100).await.unwrap();
        let stream_ids: std::collections::HashSet<&str> =
            stream.iter().map(|(_, e)| e.event_id.as_str()).collect();
        assert!(stream_ids.contains(create_id.as_str()));
        assert!(stream_ids.contains(member_id.as_str()));
    }

    // R13: atomic rollback on mid-batch failure. A bad initial event
    // (membership value the `current_state` CHECK rejects) fires inside
    // the transaction *after* the rooms INSERT, the create event, and
    // an earlier good initial event have already executed. Every one of
    // those must be rolled back — the whole point of the single-txn
    // contract.
    #[tokio::test]
    async fn create_room_rolls_back_on_mid_batch_failure() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let good = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        // `current_state.membership` CHECK rejects anything outside the
        // enum, so this fails the upsert from inside `write_into_tx`.
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({"membership": "garbage"}),
            0,
            &[],
            &[],
        );

        let result = store.create_room(&ce, &[good, bad]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));

        // No room, no events, no current_state survived.
        assert_eq!(store.room_count().await.unwrap(), 0);
        let stream = store.events_after(StreamPos(0), 100).await.unwrap();
        assert!(stream.is_empty(), "events table not rolled back");
        let cs_count: i64 = store
            .run_read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM current_state", [], |r| r.get(0))
                    .map_err(Error::from)
            })
            .await
            .unwrap();
        assert_eq!(cs_count, 0, "current_state not rolled back");
    }

    // R14: subscribe-before-create receives the watch advance. Exercises
    // the "one watch advance for the whole batch" path. Receiver value
    // after the call must equal the last event's `stream_pos`.
    #[tokio::test]
    async fn create_room_advances_subscribe_watch() {
        let store = store().await;
        let mut rx = store.subscribe();
        let initial = *rx.borrow_and_update();
        assert_eq!(initial, StreamPos(0));

        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let m1 = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce, &[m1]).await.unwrap();

        // Two events written → AUTOINCREMENT assigned stream_pos 1 and 2;
        // the batch advance carries the final pos.
        assert_eq!(*rx.borrow(), StreamPos(2));
        assert!(rx.has_changed().unwrap());
    }

    // R15: a failed `create_room` must NOT advance the watch. The notify
    // call sits after `tx.commit()?` so this is structurally safe, but
    // the regression test guards against someone moving the notify
    // earlier.
    #[tokio::test]
    async fn create_room_does_not_advance_watch_on_failure() {
        let store = store().await;
        let mut rx = store.subscribe();
        let initial = *rx.borrow_and_update();

        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let bad = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            json!({"membership": "garbage"}),
            0,
            &[],
            &[],
        );
        let result = store.create_room(&ce, &[bad]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));

        assert_eq!(*rx.borrow(), initial);
        assert!(!rx.has_changed().unwrap());
    }

    // R16: every initial state event lands in `current_state`. Trait
    // postcondition: "current state reflects all initial state events."
    // StateStore is not yet implemented on `SqliteStore`, so this test
    // reads `current_state` directly rather than going through the
    // (stubbed) trait surface.
    #[tokio::test]
    async fn create_room_persists_initial_state_to_current_state() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let ce_id = ce.event_id.clone();
        let m = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let m_id = m.event_id.clone();
        store.create_room(&ce, &[m]).await.unwrap();

        let room = ALICE_ROOM_ID.to_owned();
        let user = ALICE_USER_ID.to_owned();
        let rows: Vec<(String, String, String, Option<String>)> = store
            .run_read(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT event_type, state_key, event_id, membership \
                     FROM current_state WHERE room_id = ?",
                )?;
                let rows = stmt
                    .query_map(params![room.as_str()], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, Error>(rows)
            })
            .await
            .unwrap();

        // Create event lands as ("m.room.create", "") → membership NULL.
        // Member event lands as ("m.room.member", "<alice>") → "join".
        let mut by_key: std::collections::HashMap<(String, String), (String, Option<String>)> =
            rows.into_iter()
                .map(|(t, sk, eid, m)| ((t, sk), (eid, m)))
                .collect();
        let create_row = by_key
            .remove(&("m.room.create".into(), "".into()))
            .expect("create state row missing");
        assert_eq!(create_row.0, ce_id.as_str());
        assert_eq!(create_row.1, None);
        let member_row = by_key
            .remove(&("m.room.member".into(), user.as_str().to_owned()))
            .expect("member state row missing");
        assert_eq!(member_row.0, m_id.as_str());
        assert_eq!(member_row.1.as_deref(), Some("join"));
    }

    // R17: `create_room` must not create outbox entries — there are no
    // remote members on a fresh room. FederationOutbox isn't implemented
    // yet, so query the outbox table directly.
    #[tokio::test]
    async fn create_room_creates_no_outbox_entries() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let m = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce, &[m]).await.unwrap();

        let outbox_count: i64 = store
            .run_read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM outbox", [], |r| r.get(0))
                    .map_err(Error::from)
            })
            .await
            .unwrap();
        assert_eq!(outbox_count, 0);
    }

    // R18: initial_events land in `events` in input order and that order
    // is the ascending `stream_pos` order. The contract for `events_after`
    // is "ascending stream order"; this asserts that ordering matches
    // the input slice for the create-room batch.
    #[tokio::test]
    async fn create_room_preserves_initial_events_stream_order() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let ce_id = ce.event_id.clone();
        let e1 = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let e1_id = e1.event_id.clone();
        let e2 = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.name",
            Some(""),
            json!({"name": "Test"}),
            0,
            &[],
            &[],
        );
        let e2_id = e2.event_id.clone();
        let e3 = make_event(
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some("@bob:example.com"),
            json!({"membership": "join"}),
            0,
            &[],
            &[],
        );
        let e3_id = e3.event_id.clone();
        store.create_room(&ce, &[e1, e2, e3]).await.unwrap();

        let stream = store.events_after(StreamPos(0), 100).await.unwrap();
        let ids: Vec<&str> = stream.iter().map(|(_, e)| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                ce_id.as_str(),
                e1_id.as_str(),
                e2_id.as_str(),
                e3_id.as_str(),
            ]
        );
        // And the stream positions are strictly ascending.
        let positions: Vec<StreamPos> = stream.iter().map(|(p, _)| *p).collect();
        assert!(positions.windows(2).all(|w| w[0] < w[1]));
    }

    // R19: duplicate event_id within the `initial_events` batch is
    // rejected (UNIQUE on events.event_id) and the whole batch rolls
    // back. Covers both the rejection (InvalidInput) and the atomicity.
    #[tokio::test]
    async fn create_room_rejects_duplicate_event_id_in_batch() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        // Two events with identical canonicalised redacted shape produce
        // identical event_ids — supply the same member event twice.
        let e1 = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let e2 = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        assert_eq!(
            e1.event_id, e2.event_id,
            "test precondition: identical helpers must yield identical computed event_ids"
        );
        let result = store.create_room(&ce, &[e1, e2]).await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));

        assert_eq!(store.room_count().await.unwrap(), 0);
        let stream = store.events_after(StreamPos(0), 100).await.unwrap();
        assert!(stream.is_empty());
    }

    // R20: the schema-level `CHECK (room_version = 'org.matrix.msc4242.12')`
    // on `rooms` rejects any attempt to write a different room_version,
    // including the bypass path this test exercises (raw UPDATE outside
    // the trait's `create_room` validation gate). Before the CHECK,
    // `get_room_version` had to defensively parse the column on read and
    // map a failure to `Internal`; now the bad row can't exist in the
    // first place, so the defence-in-depth has moved one layer down.
    #[tokio::test]
    async fn rooms_check_constraint_rejects_non_msc4242_room_version() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        store.create_room(&ce, &[]).await.unwrap();

        let room = ALICE_ROOM_ID.to_owned();
        let result = store
            .run_write(move |conn| -> Result<(), Error> {
                conn.execute(
                    "UPDATE rooms SET room_version = ? WHERE room_id = ?",
                    params!["", room.as_str()],
                )?;
                Ok(())
            })
            .await;
        let err = result.expect_err("CHECK constraint must reject empty room_version");
        let msg = err.to_string();
        assert!(
            msg.contains("CHECK constraint failed"),
            "expected CHECK violation, got: {msg}"
        );
    }

    // FE1: forward_extremities for an unknown room → None.
    #[tokio::test]
    async fn forward_extremities_unknown_room_is_none() {
        let store = store().await;
        let got = store
            .forward_extremities(room_id!("!nope:example.com"))
            .await
            .unwrap();
        assert!(got.is_none());
    }

    // FE2: a freshly created room (create event only) seeds both head-sets
    // to the create event — createRoom writes a linear state chain whose last
    // event is the sole head of both DAGs.
    #[tokio::test]
    async fn forward_extremities_fresh_room_is_create_event() {
        let store = store().await;
        let ce = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let create_id = ce.event_id.clone();
        store.create_room(&ce, &[]).await.unwrap();
        let (timeline, state) = store
            .forward_extremities(*ALICE_ROOM_ID)
            .await
            .unwrap()
            .expect("room exists");
        assert_eq!(timeline, [create_id.clone()].into_iter().collect());
        assert_eq!(state, [create_id].into_iter().collect());
    }
}
