//! `StateStore` impl on `SqliteStore`.
//!
//! All queries JOIN `current_state` with `events` to project the
//! [`crate::row::EVENT_COLUMNS_PREFIXED`] columns. The `joined_rooms` /
//! `joined_members` queries match the partial-index `WHERE` clauses from
//! `schema.sql` exactly so SQLite picks the indexes.

use std::collections::HashMap;

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params, params_from_iter};
use neutrino_store::{StateStore, StorageError, StoredEvent};
use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};

use crate::{
    SqliteStore,
    error::Error,
    row::{EVENT_COLUMNS_PREFIXED, EventRow},
};

#[async_trait]
impl StateStore for SqliteStore {
    async fn current_room_state(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<(String, String), StoredEvent>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(
            move |conn| -> Result<HashMap<(String, String), StoredEvent>, Error> {
                let query = format!(
                    "SELECT cs.event_type AS map_event_type, cs.state_key AS map_state_key, \
                            {EVENT_COLUMNS_PREFIXED} \
                     FROM current_state cs \
                     JOIN events e ON cs.event_id = e.event_id \
                     WHERE cs.room_id = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(params![room_id.as_str()], |row| {
                    let map_event_type: String = row.get("map_event_type")?;
                    let map_state_key: String = row.get("map_state_key")?;
                    Ok((map_event_type, map_state_key, EventRow::try_from(row)))
                })?;

                let mut out = HashMap::new();
                for r in rows {
                    let (et, sk, ev) = r?;
                    out.insert((et, sk), ev?.into_event());
                }
                Ok(out)
            },
        )
        .await
    }

    async fn current_state_event(
        &self,
        room_id: &RoomId,
        event_type: &str,
        state_key: &str,
    ) -> Result<Option<StoredEvent>, StorageError> {
        let room_id = room_id.to_owned();
        let event_type = event_type.to_owned();
        let state_key = state_key.to_owned();

        self.run_read(move |conn| -> Result<Option<StoredEvent>, Error> {
            let query = format!(
                "SELECT {EVENT_COLUMNS_PREFIXED} \
                 FROM current_state cs \
                 JOIN events e ON cs.event_id = e.event_id \
                 WHERE cs.room_id = ? AND cs.event_type = ? AND cs.state_key = ?"
            );
            let result = conn
                .query_row(
                    &query,
                    params![room_id.as_str(), event_type, state_key],
                    |row| Ok(EventRow::try_from(row)),
                )
                .optional()?;
            match result {
                None => Ok(None),
                Some(inner) => Ok(Some(inner?.into_event())),
            }
        })
        .await
    }

    async fn current_state_events_of_type(
        &self,
        room_id: &RoomId,
        event_type: &str,
    ) -> Result<HashMap<String, StoredEvent>, StorageError> {
        let room_id = room_id.to_owned();
        let event_type = event_type.to_owned();

        self.run_read(move |conn| -> Result<HashMap<String, StoredEvent>, Error> {
            let query = format!(
                "SELECT cs.state_key AS map_state_key, {EVENT_COLUMNS_PREFIXED} \
                 FROM current_state cs \
                 JOIN events e ON cs.event_id = e.event_id \
                 WHERE cs.room_id = ? AND cs.event_type = ?"
            );
            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(params![room_id.as_str(), event_type], |row| {
                let map_state_key: String = row.get("map_state_key")?;
                Ok((map_state_key, EventRow::try_from(row)))
            })?;

            let mut out = HashMap::new();
            for r in rows {
                let (sk, ev) = r?;
                out.insert(sk, ev?.into_event());
            }
            Ok(out)
        })
        .await
    }

    async fn joined_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        let user_id = user_id.to_owned();

        self.run_read(move |conn| -> Result<Vec<OwnedRoomId>, Error> {
            // `state_key`-prefix + `event_type` matches the partial index
            // `ix_current_state_member`; `membership` filter narrows within
            // the index.
            let mut stmt = conn.prepare(
                "SELECT room_id FROM current_state \
                 WHERE state_key = ? AND event_type = 'm.room.member' AND membership = 'join'",
            )?;
            let rows = stmt.query_map(params![user_id.as_str()], |row| row.get::<_, String>(0))?;

            let mut out = Vec::new();
            for r in rows {
                let s = r?;
                let id = OwnedRoomId::try_from(s)
                    .map_err(|e| Error::Internal(format!("malformed room_id in DB: {e}")))?;
                out.push(id);
            }
            Ok(out)
        })
        .await
    }

    async fn rooms_with_membership(
        &self,
        user_id: &UserId,
        memberships: &[&str],
    ) -> Result<Vec<(OwnedRoomId, String)>, StorageError> {
        if memberships.is_empty() {
            return Ok(Vec::new());
        }
        let user_id = user_id.to_owned();
        let memberships: Vec<String> = memberships.iter().map(|s| (*s).to_owned()).collect();

        self.run_read(move |conn| -> Result<Vec<(OwnedRoomId, String)>, Error> {
            // Same `state_key`-prefix index (`ix_current_state_member`) as
            // `joined_rooms` — the `IN (…)` filter is applied within the
            // partial index without a full table scan.
            let placeholders = vec!["?"; memberships.len()].join(",");
            let query = format!(
                "SELECT room_id, membership FROM current_state \
                 WHERE state_key = ? AND event_type = 'm.room.member' \
                   AND membership IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&query)?;
            let mut binds: Vec<&str> = Vec::with_capacity(memberships.len() + 1);
            binds.push(user_id.as_str());
            for m in &memberships {
                binds.push(m.as_str());
            }
            let rows = stmt.query_map(params_from_iter(binds.iter()), |row| {
                let room_id: String = row.get(0)?;
                let membership: String = row.get(1)?;
                Ok((room_id, membership))
            })?;

            let mut out = Vec::new();
            for r in rows {
                let (room_id, membership) = r?;
                let room_id = OwnedRoomId::try_from(room_id)
                    .map_err(|e| Error::Internal(format!("malformed room_id in DB: {e}")))?;
                out.push((room_id, membership));
            }
            Ok(out)
        })
        .await
    }

    async fn joined_members(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, StoredEvent>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(
            move |conn| -> Result<HashMap<OwnedUserId, StoredEvent>, Error> {
                // `room_id`-prefix matches the partial index
                // `ix_current_state_room_member`; `membership` filter
                // narrows within the index.
                let query = format!(
                    "SELECT cs.state_key AS user_id, {EVENT_COLUMNS_PREFIXED} \
                     FROM current_state cs \
                     JOIN events e ON cs.event_id = e.event_id \
                     WHERE cs.room_id = ? AND cs.event_type = 'm.room.member' \
                       AND cs.membership = 'join'"
                );
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(params![room_id.as_str()], |row| {
                    let user: String = row.get("user_id")?;
                    Ok((user, EventRow::try_from(row)))
                })?;

                let mut out = HashMap::new();
                for r in rows {
                    let (user, ev) = r?;
                    let user_id = OwnedUserId::try_from(user)
                        .map_err(|e| Error::Internal(format!("malformed user_id in DB: {e}")))?;
                    out.insert(user_id, ev?.into_event());
                }
                Ok(out)
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use neutrino_store::{EventStore, RoomStore, StateStore};
    use ruma::{RoomId, UserId, event_id, room_id, user_id};

    use crate::tests::{create_event, member_join, member_leave, name_event, store};

    // ruma's `room_id!` / `user_id!` aren't const-fn, so `const` is out
    // (E0015). `lazy_static!` runs the macro on first access and caches.
    lazy_static! {
        static ref ALICE_ROOM_ID: &'static RoomId = room_id!("!r1:example.com");
        static ref BOB_ROOM_ID: &'static RoomId = room_id!("!r2:example.com");
        static ref ALICE_ID: &'static UserId = user_id!("@alice:example.com");
        static ref BOB_ID: &'static UserId = user_id!("@bob:example.com");
    }

    // S1
    #[tokio::test]
    async fn current_room_state_empty_for_unknown_room() {
        let s = store().await;
        let got = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
        assert!(got.is_empty());
    }

    // S2
    #[tokio::test]
    async fn current_room_state_returns_all_state_events() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[
                member_join(event_id!("$mj:e"), *ALICE_ROOM_ID, *ALICE_ID),
                name_event(event_id!("$n:e"), *ALICE_ROOM_ID, *ALICE_ID, "room"),
            ],
        )
        .await
        .unwrap();

        let got = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
        // Three state events: create, member, name
        assert_eq!(got.len(), 3);
        assert!(got.contains_key(&("m.room.create".to_owned(), "".to_owned())));
        assert!(got.contains_key(&("m.room.member".to_owned(), ALICE_ID.as_str().to_owned())));
        assert!(got.contains_key(&("m.room.name".to_owned(), "".to_owned())));
    }

    // S3
    #[tokio::test]
    async fn current_state_event_none_for_missing_key() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[],
        )
        .await
        .unwrap();
        let got = s
            .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap();
        assert!(got.is_none());
    }

    // S4
    #[tokio::test]
    async fn current_state_event_returns_specific() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[name_event(
                event_id!("$n:e"),
                *ALICE_ROOM_ID,
                *ALICE_ID,
                "Test Room",
            )],
        )
        .await
        .unwrap();
        let got = s
            .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap()
            .expect("name event should be present");
        assert_eq!(got.event_id.as_str(), "$n:e");
    }

    // S5
    #[tokio::test]
    async fn current_state_events_of_type_returns_subset() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[
                member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID),
                member_join(event_id!("$mj2:e"), *ALICE_ROOM_ID, *BOB_ID),
                name_event(event_id!("$n:e"), *ALICE_ROOM_ID, *ALICE_ID, "room"),
            ],
        )
        .await
        .unwrap();

        let got = s
            .current_state_events_of_type(*ALICE_ROOM_ID, "m.room.member")
            .await
            .unwrap();
        // Both members, but no create / name.
        assert_eq!(got.len(), 2);
        assert!(got.contains_key(ALICE_ID.as_str()));
        assert!(got.contains_key(BOB_ID.as_str()));
    }

    // S6
    #[tokio::test]
    async fn joined_rooms_empty_for_unknown_user() {
        let s = store().await;
        assert!(s.joined_rooms(*ALICE_ID).await.unwrap().is_empty());
    }

    // S7
    #[tokio::test]
    async fn joined_rooms_returns_user_rooms() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c1:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(event_id!("$c2:e"), *BOB_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj2:e"), *BOB_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();

        let mut rooms = s.joined_rooms(*ALICE_ID).await.unwrap();
        rooms.sort_by_key(|r| r.as_str().to_owned());
        assert_eq!(rooms.len(), 2);
        assert_eq!(rooms[0].as_str(), ALICE_ROOM_ID.as_str());
        assert_eq!(rooms[1].as_str(), BOB_ROOM_ID.as_str());
    }

    // S8
    #[tokio::test]
    async fn joined_rooms_excludes_non_join_membership() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c1:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        // alice leaves *ALICE_ROOM_ID
        let leave = member_leave(event_id!("$ml:e"), *ALICE_ROOM_ID, *ALICE_ID);
        s.persist_event(&leave, &[]).await.unwrap();

        assert!(s.joined_rooms(*ALICE_ID).await.unwrap().is_empty());
    }

    // S9
    #[tokio::test]
    async fn joined_members_returns_joined_users() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[
                member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID),
                member_join(event_id!("$mj2:e"), *ALICE_ROOM_ID, *BOB_ID),
            ],
        )
        .await
        .unwrap();

        let got = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains_key(*ALICE_ID));
        assert!(got.contains_key(*BOB_ID));
    }

    // S10
    #[tokio::test]
    async fn joined_members_excludes_non_joined() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[
                member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID),
                member_join(event_id!("$mj2:e"), *ALICE_ROOM_ID, *BOB_ID),
            ],
        )
        .await
        .unwrap();
        // bob leaves
        s.persist_event(
            &member_leave(event_id!("$ml:e"), *ALICE_ROOM_ID, *BOB_ID),
            &[],
        )
        .await
        .unwrap();

        let got = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(got.contains_key(*ALICE_ID));
        assert!(!got.contains_key(*BOB_ID));
    }

    // S11
    #[tokio::test]
    async fn rooms_with_membership_empty_memberships_returns_empty() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        let got = s.rooms_with_membership(*ALICE_ID, &[]).await.unwrap();
        assert!(got.is_empty());
    }

    // S12
    #[tokio::test]
    async fn rooms_with_membership_returns_matching_pairs() {
        let s = store().await;
        // *ALICE_ROOM_ID: alice joined
        s.create_room(
            &create_event(event_id!("$c1:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        // *BOB_ROOM_ID: alice joined then left
        s.create_room(
            &create_event(event_id!("$c2:e"), *BOB_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj2:e"), *BOB_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        s.persist_event(
            &member_leave(event_id!("$ml:e"), *BOB_ROOM_ID, *ALICE_ID),
            &[],
        )
        .await
        .unwrap();

        let mut got = s
            .rooms_with_membership(*ALICE_ID, &["join", "leave"])
            .await
            .unwrap();
        got.sort_by_key(|(r, _)| r.as_str().to_owned());
        assert_eq!(got.len(), 2);
        assert_eq!(
            (got[0].0.as_str(), got[0].1.as_str()),
            (ALICE_ROOM_ID.as_str(), "join")
        );
        assert_eq!(
            (got[1].0.as_str(), got[1].1.as_str()),
            (BOB_ROOM_ID.as_str(), "leave")
        );
    }

    // S13
    #[tokio::test]
    async fn rooms_with_membership_filters_by_value() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c1:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj1:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(event_id!("$c2:e"), *BOB_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj2:e"), *BOB_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        s.persist_event(
            &member_leave(event_id!("$ml:e"), *BOB_ROOM_ID, *ALICE_ID),
            &[],
        )
        .await
        .unwrap();

        // Only ask for "leave"
        let got = s
            .rooms_with_membership(*ALICE_ID, &["leave"])
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0.as_str(), BOB_ROOM_ID.as_str());
        assert_eq!(got[0].1, "leave");
    }

    // S14: unknown membership strings in slice silently ignored
    #[tokio::test]
    async fn rooms_with_membership_unknown_memberships_silently_ignored() {
        let s = store().await;
        s.create_room(
            &create_event(event_id!("$c:e"), *ALICE_ROOM_ID, *ALICE_ID),
            &[member_join(event_id!("$mj:e"), *ALICE_ROOM_ID, *ALICE_ID)],
        )
        .await
        .unwrap();
        let got = s
            .rooms_with_membership(*ALICE_ID, &["join", "bogus_value"])
            .await
            .unwrap();
        // "bogus_value" doesn't match any row; "join" matches one.
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, "join");
    }

    // S15
    #[tokio::test]
    async fn rooms_with_membership_empty_for_unknown_user() {
        let s = store().await;
        let got = s
            .rooms_with_membership(*ALICE_ID, &["join", "leave"])
            .await
            .unwrap();
        assert!(got.is_empty());
    }
}
