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
