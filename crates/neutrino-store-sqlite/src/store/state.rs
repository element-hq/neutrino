//! `StateStore` impl on `SqliteStore`.
//!
//! Two query shapes, split by what the caller actually needs:
//!
//! - **State-event returns** (`current_room_state`,
//!   `current_state_event`, `current_state_events_of_type`,
//!   `joined_members`) JOIN `current_state` with `events` to project the
//!   [`crate::row::EVENT_COLUMNS_PREFIXED`] columns. The schema-level
//!   composite FK
//!   `current_state(event_id, room_id, event_type, state_key) →
//!   events(event_id, room_id, event_type, state_key)` (see
//!   `schema.sql`) guarantees that the two rows agree on all four
//!   columns, so a single-column JOIN on `cs.event_id = e.event_id` is
//!   sufficient — any desync between `current_state` and `events` is
//!   rejected at write time, not papered over at read time.
//! - **Membership lookups** (`joined_rooms`, `rooms_with_membership`)
//!   read from `current_state` only, filtering on the indexed
//!   `membership` column. They deliberately don't JOIN `events` —
//!   sliding sync calls these on every connect and we don't want to
//!   load member-event JSON when the only fact the caller needs is "is
//!   this user in this room with this membership". `joined_members`
//!   *does* need the event row (it returns full `Event`s), so it
//!   pays the JOIN; the partial index `ix_current_state_room_member`
//!   still lets the planner narrow before the JOIN.
//!
//! The `joined_rooms` / `joined_members` / `rooms_with_membership`
//! queries match the partial-index `WHERE` clauses from `schema.sql`
//! exactly so SQLite picks the indexes.

use std::collections::{BTreeSet, HashMap};

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params, params_from_iter};
use neutrino_store::{Event, Membership, StateStore, StorageError};
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
    ) -> Result<HashMap<(String, String), Event>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(
            move |conn| -> Result<HashMap<(String, String), Event>, Error> {
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
    ) -> Result<Option<Event>, StorageError> {
        let room_id = room_id.to_owned();
        let event_type = event_type.to_owned();
        let state_key = state_key.to_owned();

        self.run_read(move |conn| -> Result<Option<Event>, Error> {
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
    ) -> Result<HashMap<String, Event>, StorageError> {
        let room_id = room_id.to_owned();
        let event_type = event_type.to_owned();

        self.run_read(move |conn| -> Result<HashMap<String, Event>, Error> {
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

    async fn invited_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        let user_id = user_id.to_owned();

        self.run_read(move |conn| -> Result<Vec<OwnedRoomId>, Error> {
            // `state_key`-prefix + `event_type` matches the partial index
            // `ix_current_state_member`; `membership` filter narrows within
            // the index.
            let mut stmt = conn.prepare(
                "SELECT room_id FROM current_state \
                 WHERE state_key = ? AND event_type = 'm.room.member' AND membership = 'invite'",
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
        memberships: &BTreeSet<Membership>,
    ) -> Result<Vec<(OwnedRoomId, Membership)>, StorageError> {
        if memberships.is_empty() {
            return Ok(Vec::new());
        }
        let user_id = user_id.to_owned();
        // Copy the `Membership` values out of the borrowed `BTreeSet` so
        // the closure passed to `run_read` doesn't need to capture a
        // reference to caller-owned data.
        let memberships: Vec<Membership> = memberships.iter().copied().collect();

        self.run_read(
            move |conn| -> Result<Vec<(OwnedRoomId, Membership)>, Error> {
                // Build the `?` placeholders and the `binds` Vec from the
                // same iteration over `memberships` so the two can't fall
                // out of step in a future refactor (e.g. someone dedup'ing
                // one side without the other and silently misaligning
                // placeholders to values).
                //
                // Same `state_key`-prefix index (`ix_current_state_member`)
                // as `joined_rooms` — the `IN (…)` filter is applied
                // within the partial index without a full table scan.
                let mut binds: Vec<&str> = Vec::with_capacity(memberships.len() + 1);
                binds.push(user_id.as_str());
                let mut placeholders = String::new();
                for (i, m) in memberships.iter().enumerate() {
                    if i > 0 {
                        placeholders.push(',');
                    }
                    placeholders.push('?');
                    binds.push(m.as_str());
                }
                let query = format!(
                    "SELECT room_id, membership FROM current_state \
                 WHERE state_key = ? AND event_type = 'm.room.member' \
                   AND membership IN ({placeholders})"
                );
                let mut stmt = conn.prepare(&query)?;
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
                    let membership = Membership::from_wire(&membership).ok_or_else(|| {
                    Error::Internal(format!(
                        "unknown membership '{membership}' in DB — schema CHECK should prevent this"
                    ))
                })?;
                    out.push((room_id, membership));
                }
                Ok(out)
            },
        )
        .await
    }

    async fn joined_members(
        &self,
        room_id: &RoomId,
    ) -> Result<HashMap<OwnedUserId, Event>, StorageError> {
        let room_id = room_id.to_owned();

        self.run_read(move |conn| -> Result<HashMap<OwnedUserId, Event>, Error> {
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
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use neutrino_store::{EventStore, Membership, RoomStore, StateStore};
    use ruma::{event_id, room_id};

    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_ROOM_ID, BOB_USER_ID, create_event,
        make_event_with_raw_json, member_join, member_leave, message, name_event, store,
    };

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
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[
                member_join(*ALICE_ROOM_ID, *ALICE_USER_ID),
                name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "room"),
            ],
        )
        .await
        .unwrap();

        let got = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
        // Three state events: create, member, name
        assert_eq!(got.len(), 3);
        assert!(got.contains_key(&("m.room.create".to_owned(), "".to_owned())));
        assert!(got.contains_key(&(
            "m.room.member".to_owned(),
            ALICE_USER_ID.as_str().to_owned()
        )));
        assert!(got.contains_key(&("m.room.name".to_owned(), "".to_owned())));
    }

    // S3
    #[tokio::test]
    async fn current_state_event_none_for_missing_key() {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
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
        let name_ev = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "Test Room");
        let name_id = name_ev.event_id.clone();
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[name_ev])
            .await
            .unwrap();
        let got = s
            .current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
            .await
            .unwrap()
            .expect("name event should be present");
        assert_eq!(got.event_id.as_str(), name_id.as_str());
    }

    // S5
    #[tokio::test]
    async fn current_state_events_of_type_returns_subset() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[
                member_join(*ALICE_ROOM_ID, *ALICE_USER_ID),
                member_join(*ALICE_ROOM_ID, *BOB_USER_ID),
                name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "room"),
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
        assert!(got.contains_key(ALICE_USER_ID.as_str()));
        assert!(got.contains_key(BOB_USER_ID.as_str()));
    }

    // S6
    #[tokio::test]
    async fn joined_rooms_empty_for_unknown_user() {
        let s = store().await;
        assert!(s.joined_rooms(*ALICE_USER_ID).await.unwrap().is_empty());
    }

    // S7
    #[tokio::test]
    async fn joined_rooms_returns_user_rooms() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(*BOB_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*BOB_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();

        let mut rooms = s.joined_rooms(*ALICE_USER_ID).await.unwrap();
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
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        // alice leaves *ALICE_ROOM_ID
        let leave = member_leave(*ALICE_ROOM_ID, *ALICE_USER_ID);
        s.persist_event(&leave, &[]).await.unwrap();

        assert!(s.joined_rooms(*ALICE_USER_ID).await.unwrap().is_empty());
    }

    // S9
    #[tokio::test]
    async fn joined_members_returns_joined_users() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[
                member_join(*ALICE_ROOM_ID, *ALICE_USER_ID),
                member_join(*ALICE_ROOM_ID, *BOB_USER_ID),
            ],
        )
        .await
        .unwrap();

        let got = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains_key(*ALICE_USER_ID));
        assert!(got.contains_key(*BOB_USER_ID));
    }

    // S10
    #[tokio::test]
    async fn joined_members_excludes_non_joined() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[
                member_join(*ALICE_ROOM_ID, *ALICE_USER_ID),
                member_join(*ALICE_ROOM_ID, *BOB_USER_ID),
            ],
        )
        .await
        .unwrap();
        // bob leaves
        s.persist_event(&member_leave(*ALICE_ROOM_ID, *BOB_USER_ID), &[])
            .await
            .unwrap();

        let got = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(got.contains_key(*ALICE_USER_ID));
        assert!(!got.contains_key(*BOB_USER_ID));
    }

    // S11
    #[tokio::test]
    async fn rooms_with_membership_empty_memberships_returns_empty() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        let got = s
            .rooms_with_membership(*ALICE_USER_ID, &BTreeSet::new())
            .await
            .unwrap();
        assert!(got.is_empty());
    }

    // S12
    #[tokio::test]
    async fn rooms_with_membership_returns_matching_pairs() {
        let s = store().await;
        // *ALICE_ROOM_ID: alice joined
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        // *BOB_ROOM_ID: alice joined then left
        s.create_room(
            &create_event(*BOB_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*BOB_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        s.persist_event(&member_leave(*BOB_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();

        let mut got = s
            .rooms_with_membership(
                *ALICE_USER_ID,
                &BTreeSet::from([Membership::Join, Membership::Leave]),
            )
            .await
            .unwrap();
        got.sort_by_key(|(r, _)| r.as_str().to_owned());
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0.as_str(), ALICE_ROOM_ID.as_str());
        assert_eq!(got[0].1, Membership::Join);
        assert_eq!(got[1].0.as_str(), BOB_ROOM_ID.as_str());
        assert_eq!(got[1].1, Membership::Leave);
    }

    // S13
    #[tokio::test]
    async fn rooms_with_membership_filters_by_value() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(*BOB_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*BOB_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        s.persist_event(&member_leave(*BOB_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();

        // Only ask for "leave"
        let got = s
            .rooms_with_membership(*ALICE_USER_ID, &BTreeSet::from([Membership::Leave]))
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0.as_str(), BOB_ROOM_ID.as_str());
        assert_eq!(got[0].1, Membership::Leave);
    }

    // S14 (renumbered → S15 below): the old "unknown membership string"
    // case is now unrepresentable at the type boundary — `Membership` is
    // a closed enum, so no caller can ask for a bogus value.

    // S15
    #[tokio::test]
    async fn rooms_with_membership_empty_for_unknown_user() {
        let s = store().await;
        let got = s
            .rooms_with_membership(
                *ALICE_USER_ID,
                &BTreeSet::from([Membership::Join, Membership::Leave]),
            )
            .await
            .unwrap();
        assert!(got.is_empty());
    }

    // SQ-28 — exercise the `Ban` / `Invite` / `Knock` paths through
    // `rooms_with_membership`. The closed-enum migration meant `Invite`,
    // `Ban`, and `Knock` round-tripped through the `IN (…)` query had no
    // direct coverage; the index predicate / value comparison could
    // silently drift on any of them. One room per membership, all queried
    // in a single call so the SQL's `IN (…)` clause is exercised across
    // the multi-value path too.
    #[tokio::test]
    async fn rooms_with_membership_returns_ban_invite_knock_rows() {
        use crate::tests::member_event;
        let s = store().await;
        let inviter = *BOB_USER_ID;
        let alice = *ALICE_USER_ID;
        let room_invite = room_id!("!invite:example.com");
        let room_knock = room_id!("!knock:example.com");
        let room_ban = room_id!("!ban:example.com");

        // Invited room: inviter creates, inviter is joined, alice has
        // membership=invite. Note `member_event` lets sender ≠ target so
        // we can express "inviter invited alice".
        s.create_room(
            &create_event(room_invite, inviter),
            &[member_join(room_invite, inviter)],
        )
        .await
        .unwrap();
        s.persist_event(&member_event(room_invite, alice, inviter, "invite"), &[])
            .await
            .unwrap();

        // Knocked room: knocker creates, knocker joined, alice knocks
        // (sender == target). The knocker's own membership doesn't
        // affect alice's view; we include it for production realism.
        s.create_room(
            &create_event(room_knock, inviter),
            &[member_join(room_knock, inviter)],
        )
        .await
        .unwrap();
        s.persist_event(&member_event(room_knock, alice, alice, "knock"), &[])
            .await
            .unwrap();

        // Banned room: alice was in it; the banner bans her.
        s.create_room(
            &create_event(room_ban, inviter),
            &[member_join(room_ban, inviter)],
        )
        .await
        .unwrap();
        s.persist_event(&member_join(room_ban, alice), &[])
            .await
            .unwrap();
        s.persist_event(&member_event(room_ban, alice, inviter, "ban"), &[])
            .await
            .unwrap();

        let mut got = s
            .rooms_with_membership(
                alice,
                &BTreeSet::from([Membership::Ban, Membership::Invite, Membership::Knock]),
            )
            .await
            .unwrap();
        got.sort_by_key(|(r, _)| r.as_str().to_owned());

        assert_eq!(got.len(), 3, "one row per membership-distinct room");
        assert_eq!(got[0].0.as_str(), room_ban.as_str());
        assert_eq!(got[0].1, Membership::Ban);
        assert_eq!(got[1].0.as_str(), room_invite.as_str());
        assert_eq!(got[1].1, Membership::Invite);
        assert_eq!(got[2].0.as_str(), room_knock.as_str());
        assert_eq!(got[2].1, Membership::Knock);
    }

    #[tokio::test]
    async fn rooms_with_membership_filters_invite_excludes_join() {
        // Filter argument scopes results: with only `Invite` in the set,
        // a Joined room must not appear (rules out a SQL bug where the
        // membership column comparison is dropped).
        use crate::tests::member_event;
        let s = store().await;
        let inviter = *BOB_USER_ID;
        let alice = *ALICE_USER_ID;
        let joined_room = room_id!("!joined:example.com");
        let invite_room = room_id!("!invite-only:example.com");

        s.create_room(
            &create_event(joined_room, alice),
            &[member_join(joined_room, alice)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(invite_room, inviter),
            &[member_join(invite_room, inviter)],
        )
        .await
        .unwrap();
        s.persist_event(&member_event(invite_room, alice, inviter, "invite"), &[])
            .await
            .unwrap();

        let got = s
            .rooms_with_membership(alice, &BTreeSet::from([Membership::Invite]))
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0.as_str(), invite_room.as_str());
        assert_eq!(got[0].1, Membership::Invite);
    }
    // S16
    #[tokio::test]
    async fn current_room_state_isolates_by_room_id() {
        let s = store().await;
        let create_a = create_event(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let id_create_a = create_a.event_id.clone();
        let name_a = name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "A");
        let id_name_a = name_a.event_id.clone();
        s.create_room(&create_a, &[name_a]).await.unwrap();
        s.create_room(
            &create_event(*BOB_ROOM_ID, *ALICE_USER_ID),
            &[name_event(*BOB_ROOM_ID, *ALICE_USER_ID, "B")],
        )
        .await
        .unwrap();

        let got = s.current_room_state(*ALICE_ROOM_ID).await.unwrap();
        // Exactly room A's create + name — none of room B's state.
        assert_eq!(got.len(), 2);
        let create = got
            .get(&("m.room.create".to_owned(), "".to_owned()))
            .expect("room A create event present");
        assert_eq!(create.event_id.as_str(), id_create_a.as_str());
        let name = got
            .get(&("m.room.name".to_owned(), "".to_owned()))
            .expect("room A name event present");
        assert_eq!(name.event_id.as_str(), id_name_a.as_str());
    }

    // S17
    #[tokio::test]
    async fn current_state_event_isolates_by_room_id() {
        let s = store().await;
        // Room A has no name; room B does. Same (event_type, state_key)
        // pair on both sides, so a missing room-scoping filter would
        // surface B's name event when querying A.
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let name_b = name_event(*BOB_ROOM_ID, *ALICE_USER_ID, "B");
        let id_name_b = name_b.event_id.clone();
        s.create_room(&create_event(*BOB_ROOM_ID, *ALICE_USER_ID), &[name_b])
            .await
            .unwrap();

        assert!(
            s.current_state_event(*ALICE_ROOM_ID, "m.room.name", "")
                .await
                .unwrap()
                .is_none()
        );
        let got = s
            .current_state_event(*BOB_ROOM_ID, "m.room.name", "")
            .await
            .unwrap()
            .expect("room B name event present");
        assert_eq!(got.event_id.as_str(), id_name_b.as_str());
    }

    // S18
    #[tokio::test]
    async fn current_state_events_of_type_isolates_by_room_id() {
        let s = store().await;
        let alice_member = member_join(*ALICE_ROOM_ID, *ALICE_USER_ID);
        let id_alice_member = alice_member.event_id.clone();
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[alice_member],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(*BOB_ROOM_ID, *BOB_USER_ID),
            &[member_join(*BOB_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();

        let got = s
            .current_state_events_of_type(*ALICE_ROOM_ID, "m.room.member")
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        let alice = got
            .get(ALICE_USER_ID.as_str())
            .expect("alice's member event present in room A");
        assert_eq!(alice.event_id.as_str(), id_alice_member.as_str());
        assert!(!got.contains_key(BOB_USER_ID.as_str()));
    }

    // S19
    #[tokio::test]
    async fn joined_members_isolates_by_room_id() {
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        s.create_room(
            &create_event(*BOB_ROOM_ID, *BOB_USER_ID),
            &[member_join(*BOB_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();

        let got = s.joined_members(*ALICE_ROOM_ID).await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(got.contains_key(*ALICE_USER_ID));
        assert!(!got.contains_key(*BOB_USER_ID));
    }

    // S20: `joined_rooms` doesn't JOIN events, but the existing S7 test
    // happened to have only one user in the DB, so the query would have
    // passed even with no user filter. Force the user filter to do work
    // by giving each user their own room.
    #[tokio::test]
    async fn joined_rooms_filters_by_user_id() {
        let s = store().await;
        // Alice is in room A.
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();
        // Bob is in room B; alice is NOT.
        s.create_room(
            &create_event(*BOB_ROOM_ID, *BOB_USER_ID),
            &[member_join(*BOB_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();

        let rooms = s.joined_rooms(*ALICE_USER_ID).await.unwrap();
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].as_str(), ALICE_ROOM_ID.as_str());
    }

    // S21: schema-level — composite FK rejects a current_state row
    // whose event_id resolves to an event in a *different* room. The
    // FK is defined as
    // `current_state(event_id, room_id, event_type, state_key) →
    //  events(event_id, room_id, event_type, state_key)`,
    // so a (room_A, event_in_room_B) write doesn't satisfy any tuple in
    // the parent table and the INSERT itself fails. Maps through
    // `Error::Sqlite(ConstraintViolation)` → `StorageError::InvalidInput`
    // per `error.rs`.
    #[tokio::test]
    async fn current_state_rejects_cross_room_event_id() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        // Room B holds the real $nB:e — without it, the FK would fail
        // on event_id alone and we wouldn't be testing the room-axis.
        s.create_room(
            &create_event(*BOB_ROOM_ID, *BOB_USER_ID),
            &[name_event(*BOB_ROOM_ID, *BOB_USER_ID, "B")],
        )
        .await
        .unwrap();

        let alice_room = ALICE_ROOM_ID.as_str().to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO current_state \
                     (room_id, event_type, state_key, event_id, membership) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![alice_room, "m.room.name", "", "$nB:e", None::<&str>],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("composite FK must reject cross-room event_id");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from FK violation, got {err:?}"
        );
    }

    // S22: schema-level — composite FK rejects a current_state row
    // whose claimed (event_type, state_key) differ from the referenced
    // events row, even when the room_id agrees.
    #[tokio::test]
    async fn current_state_rejects_mismatched_event_type_or_state_key() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "A")],
        )
        .await
        .unwrap();

        let alice_room = ALICE_ROOM_ID.as_str().to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO current_state \
                     (room_id, event_type, state_key, event_id, membership) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![
                        alice_room,
                        "m.room.member",
                        ALICE_USER_ID.as_str(),
                        "$name:e",
                        "join"
                    ],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("composite FK must reject (event_type, state_key) mismatch");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from FK violation, got {err:?}"
        );
    }

    // S23: schema-level — composite FK rejects a current_state row
    // pointing at a *non-state* event. `events.state_key IS NULL` for
    // non-state events, so the FK's `events.state_key = cs.state_key`
    // comparison resolves to SQL UNKNOWN against `cs.state_key`'s NOT
    // NULL value — no parent-key tuple matches, INSERT fails.
    #[tokio::test]
    async fn current_state_rejects_pointing_at_non_state_event() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        s.persist_event(&message(*ALICE_ROOM_ID, *ALICE_USER_ID, "hi"), &[])
            .await
            .unwrap();

        let alice_room = ALICE_ROOM_ID.as_str().to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO current_state \
                     (room_id, event_type, state_key, event_id, membership) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![alice_room, "m.room.name", "", "$msg:e", None::<&str>],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("composite FK must reject non-state event reference");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from FK violation, got {err:?}"
        );
    }

    // S24: schema CHECK rejects an m.room.member row with NULL
    // membership. Without the CHECK, a malformed member event (no
    // `content.membership`) would land in current_state with
    // membership=NULL and be silently excluded from joined_rooms /
    // joined_members. The CHECK turns that into a write-time error.
    #[tokio::test]
    async fn current_state_rejects_member_with_null_membership() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *ALICE_USER_ID)],
        )
        .await
        .unwrap();

        // Persist wrote (member, alice, $mj:e, membership='join').
        // Now try to UPDATE membership to NULL on that same row —
        // schema CHECK rejects.
        let alice_room = ALICE_ROOM_ID.as_str().to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "UPDATE current_state SET membership = NULL \
                     WHERE room_id = ? AND event_type = 'm.room.member' AND state_key = ?",
                    params![alice_room, ALICE_USER_ID.as_str()],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("CHECK must reject NULL membership on member row");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from CHECK violation, got {err:?}"
        );
    }

    // S25: schema CHECK rejects a non-member row with a non-NULL
    // membership. The membership column is only meaningful for
    // m.room.member rows; pinning the inverse direction means no future
    // write path can accidentally stamp 'join' onto, say, an m.room.name
    // row and have it leak into the membership-indexed lookups.
    #[tokio::test]
    async fn current_state_rejects_non_member_with_non_null_membership() {
        use deadpool_sqlite::rusqlite::params;

        use crate::error::Error;

        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[name_event(*ALICE_ROOM_ID, *ALICE_USER_ID, "A")],
        )
        .await
        .unwrap();

        let alice_room = ALICE_ROOM_ID.as_str().to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), Error> {
                let tx = conn.transaction()?;
                tx.execute(
                    "UPDATE current_state SET membership = 'join' \
                     WHERE room_id = ? AND event_type = 'm.room.name' AND state_key = ''",
                    params![alice_room],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("CHECK must reject non-NULL membership on non-member row");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from CHECK violation, got {err:?}"
        );
    }

    // S26: end-to-end — persist_event on an m.room.member whose JSON
    // lacks `content.membership` surfaces InvalidInput rather than
    // silently writing a NULL-membership current_state row. This pins
    // the new persist_event behaviour at the trait boundary; previously
    // the malformed event landed silently and only manifested as
    // missing rows on the read side.
    #[tokio::test]
    async fn persist_event_rejects_member_without_membership() {
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();

        // write_into_tx only inspects `prev_events`, `prev_state_events`
        // and `content.membership` from the raw JSON, so the minimal
        // body below is enough to drive the code path. We bypass
        // `persist_event`'s B4 debug round-trip (which would panic on
        // the intentionally malformed raw bytes here) by writing via
        // `write_into_tx` directly — same SQL boundary the production
        // path uses, just without the dev-only id-vs-raw assertion.
        let event = make_event_with_raw_json(
            event_id!("$bad:e"),
            *ALICE_ROOM_ID,
            *ALICE_USER_ID,
            "m.room.member",
            Some(ALICE_USER_ID.as_str()),
            r#"{"prev_events":[],"prev_state_events":[],"content":{}}"#,
        );

        let row = crate::row::EventRow::unchecked(&event).to_owned();
        let err = s
            .run_write(move |conn| -> Result<(), crate::error::Error> {
                let tx = conn.transaction()?;
                row.write_into_tx(&tx)?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect_err("malformed member event must surface InvalidInput");
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "expected InvalidInput from CHECK violation, got {err:?}"
        );
    }
}
