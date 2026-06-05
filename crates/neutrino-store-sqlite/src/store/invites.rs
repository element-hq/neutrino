//! `InviteStore` impl on [`crate::SqliteStore`].
//!
//! Out-of-band membership invites — invites for rooms where we host the
//! *invitee* but hold no room state. See the `oob_invites` table comment in
//! `schema.sql` and the [`neutrino_store::InviteStore`] trait docs for the
//! contract. Keyed by `(room_id, state_key)` where the `state_key` is the
//! invited user; `INSERT OR REPLACE` on that PK gives latest-invite-wins.
//!
//! Rows hydrate through the shared [`EventRow`] path (raw kept verbatim — no
//! redaction), the same way every other `Event` is read; the three
//! auth-verdict columns `EventRow` expects (`auth_events_json` / `rejected` /
//! `soft_failed`) are projected as constants since an OOB invite is never
//! authed. Nothing here advances the persist watch — an OOB invite is not a
//! room event and surfaces only via the sync invite path.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_common::Event;
use neutrino_store::{InviteStore, StorageError};
use ruma::{OwnedRoomId, RoomId, UserId};

use crate::{SqliteStore, error::Error, row::EventRow};

/// Projection feeding [`EventRow::try_from`]: the stored columns plus the
/// three auth-verdict columns as constants (an OOB invite is never authed).
/// `EventRow` reads columns by name, so the constant aliases satisfy it.
const OOB_INVITE_SELECT: &str = "SELECT event_id, room_id, event_type, state_key, sender, \
     origin_server_ts, json, '[]' AS auth_events_json, 0 AS rejected, 0 AS soft_failed \
     FROM oob_invites WHERE room_id = ? AND state_key = ?";

#[async_trait]
impl InviteStore for SqliteStore {
    async fn put_invite(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        event: &Event,
    ) -> Result<(), StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        let event_id = event.event_id.as_str().to_owned();
        let event_type = event.event_type.clone();
        let sender = event.sender.as_str().to_owned();
        let json = event.raw.get().to_owned();
        let origin_server_ts = i64::try_from(event.origin_server_ts).map_err(|_| {
            Error::InvalidInput(format!(
                "origin_server_ts {} exceeds i64::MAX",
                event.origin_server_ts
            ))
        })?;

        self.run_write(move |conn| -> Result<(), Error> {
            // INSERT OR REPLACE on the (room_id, state_key) PK: a re-invite for
            // the same pair overwrites the prior stub — latest wins.
            conn.execute(
                "INSERT OR REPLACE INTO oob_invites \
                 (room_id, state_key, event_id, event_type, sender, origin_server_ts, json) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    room_id,
                    state_key,
                    event_id,
                    event_type,
                    sender,
                    origin_server_ts,
                    json
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn get_invite(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<Event>, StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        self.run_read(move |conn| -> Result<Option<Event>, Error> {
            conn.query_row(OOB_INVITE_SELECT, params![room_id, state_key], |row| {
                Ok(EventRow::try_from(row).map(EventRow::into_event))
            })
            .optional()?
            .transpose()
        })
        .await
    }

    async fn remove_invite(&self, room_id: &RoomId, user_id: &UserId) -> Result<(), StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "DELETE FROM oob_invites WHERE room_id = ? AND state_key = ?",
                params![room_id, state_key],
            )?;
            Ok(())
        })
        .await
    }

    async fn invited_oob_rooms(&self, user_id: &UserId) -> Result<Vec<OwnedRoomId>, StorageError> {
        let state_key = user_id.as_str().to_owned();
        self.run_read(move |conn| -> Result<Vec<OwnedRoomId>, Error> {
            let mut stmt = conn.prepare("SELECT room_id FROM oob_invites WHERE state_key = ?")?;
            let rows = stmt.query_map(params![state_key], |row| row.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows {
                let id = OwnedRoomId::try_from(r?).map_err(|e| {
                    Error::Internal(format!("malformed oob invite room_id in DB row: {e}"))
                })?;
                out.push(id);
            }
            Ok(out)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use neutrino_store::InviteStore;
    use ruma::{room_id, user_id};
    use serde_json::json;

    use crate::tests::{make_event_with_raw_json, store};

    /// An invite `m.room.member` event for a *remote* room we don't host,
    /// carrying the inviting server's `unsigned.invite_room_state`. Built via
    /// `make_event_with_raw_json` so the raw bytes (incl. `unsigned`) are
    /// stored verbatim — the whole point is that the round-trip preserves
    /// them.
    fn remote_invite(
        room: &ruma::RoomId,
        invited: &ruma::UserId,
        inviter: &ruma::UserId,
        room_name: &str,
    ) -> neutrino_common::Event {
        let raw = json!({
            "type": "m.room.member",
            "room_id": room.as_str(),
            "sender": inviter.as_str(),
            "state_key": invited.as_str(),
            "origin_server_ts": 1_700_000_000_000u64,
            "content": { "membership": "invite" },
            "prev_events": [],
            "prev_state_events": [],
            "unsigned": {
                "invite_room_state": [
                    { "type": "m.room.name", "state_key": "", "sender": inviter.as_str(),
                      "content": { "name": room_name } },
                    { "type": "m.room.member", "state_key": inviter.as_str(),
                      "sender": inviter.as_str(), "content": { "membership": "join" } }
                ]
            }
        });
        // The event_id is arbitrary here: the store keys on (room_id,
        // state_key) and hydrates via EventRow (which reads the id from the
        // column), so it never recomputes / validates the hash.
        make_event_with_raw_json(
            ruma::event_id!("$oob_invite_fixture:other.example.org"),
            room,
            inviter,
            "m.room.member",
            Some(invited.as_str()),
            &serde_json::to_string(&raw).unwrap(),
        )
    }

    #[tokio::test]
    async fn put_get_remove_roundtrip_preserves_raw() {
        let s = store().await;
        let room = room_id!("!remote:other.example.org");
        let invited = user_id!("@alice:example.com");
        let inviter = user_id!("@bob:other.example.org");
        let ev = remote_invite(room, invited, inviter, "Cool Room");

        assert!(s.get_invite(room, invited).await.unwrap().is_none());

        s.put_invite(room, invited, &ev).await.unwrap();
        let got = s.get_invite(room, invited).await.unwrap().expect("stored");
        // Raw bytes verbatim ⇒ unsigned.invite_room_state survived the round
        // trip (the property the sync invite path depends on).
        assert_eq!(got.raw.get(), ev.raw.get());
        assert_eq!(got.event_id, ev.event_id);
        assert_eq!(got.state_key.as_deref(), Some(invited.as_str()));
        assert_eq!(got.sender, inviter);

        s.remove_invite(room, invited).await.unwrap();
        assert!(s.get_invite(room, invited).await.unwrap().is_none());
        // Removing a missing pair is a no-op, not an error.
        s.remove_invite(room, invited).await.unwrap();
    }

    #[tokio::test]
    async fn put_invite_latest_wins() {
        let s = store().await;
        let room = room_id!("!remote:other.example.org");
        let invited = user_id!("@alice:example.com");
        let inviter = user_id!("@bob:other.example.org");

        let first = remote_invite(room, invited, inviter, "Old Name");
        let second = remote_invite(room, invited, inviter, "New Name");
        // Distinct stored bytes so we can tell which one survives.
        assert_ne!(first.raw.get(), second.raw.get());

        s.put_invite(room, invited, &first).await.unwrap();
        s.put_invite(room, invited, &second).await.unwrap();

        let got = s.get_invite(room, invited).await.unwrap().expect("stored");
        assert_eq!(
            got.raw.get(),
            second.raw.get(),
            "the most recent invite for (room, user) must win"
        );
        // Still exactly one invite for the user (REPLACE, not a second row).
        assert_eq!(
            s.invited_oob_rooms(invited).await.unwrap(),
            vec![room.to_owned()]
        );
    }

    #[tokio::test]
    async fn invited_oob_rooms_lists_only_the_users_invites() {
        let s = store().await;
        let room_a = room_id!("!a:other.example.org");
        let room_b = room_id!("!b:other.example.org");
        let alice = user_id!("@alice:example.com");
        let carol = user_id!("@carol:example.com");
        let inviter = user_id!("@bob:other.example.org");

        s.put_invite(room_a, alice, &remote_invite(room_a, alice, inviter, "A"))
            .await
            .unwrap();
        s.put_invite(room_b, alice, &remote_invite(room_b, alice, inviter, "B"))
            .await
            .unwrap();
        s.put_invite(room_a, carol, &remote_invite(room_a, carol, inviter, "A"))
            .await
            .unwrap();

        let mut alice_rooms = s.invited_oob_rooms(alice).await.unwrap();
        alice_rooms.sort();
        let mut want = vec![room_a.to_owned(), room_b.to_owned()];
        want.sort();
        assert_eq!(alice_rooms, want);

        assert_eq!(
            s.invited_oob_rooms(carol).await.unwrap(),
            vec![room_a.to_owned()]
        );
        assert!(
            s.invited_oob_rooms(user_id!("@nobody:example.com"))
                .await
                .unwrap()
                .is_empty()
        );
    }
}
