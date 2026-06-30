//! `InviteStore` impl on [`crate::SqliteStore`].
//!
//! Out-of-band membership invites — invites for rooms where we host the
//! *invitee* but hold no room state. See the `oob_invites` table comment in
//! `schema.sql` and the [`neutrino_store::InviteStore`] trait docs for the
//! contract. Keyed by `(room_id, state_key)` where the `state_key` is the
//! invited user; `INSERT OR REPLACE` on that PK gives latest-invite-wins.
//!
//! Only the canonical invite event `json` is stored; `get_invite` rehydrates
//! the `Event` verbatim via `compute_event_id` + `parse_event` — the same
//! wire→`Event` field parsing `from_wire` uses, **minus the redaction step**.
//! Skipping redaction is the point: `from_wire` redacts on a content-hash
//! miss, which would strip the inviting server's `unsigned.invite_room_state`
//! (the stripped state the sync builder renders from). Every other field
//! (event_id, type, sender, ts, content, prev_events) is derivable from
//! `json`, so no denormalised columns are stored — the same posture as
//! `staged_events`. Nothing here advances the persist watch — an OOB invite is
//! not a room event and surfaces only via the sync invite path.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_common::Event;
use neutrino_common::event_id::compute_event_id;
use neutrino_common::validate::parse_event;
use neutrino_store::{InviteStore, StorageError};
use ruma::{OwnedRoomId, RoomId, UserId};
use serde_json::value::RawValue as RawJsonValue;

use crate::{SqliteStore, error::Error};

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
        let json = event.raw.get().to_owned();

        self.run_write(move |conn| -> Result<(), Error> {
            // INSERT OR REPLACE on the (room_id, state_key) PK: a re-invite for
            // the same pair overwrites the prior stub — latest wins.
            conn.execute(
                "INSERT OR REPLACE INTO oob_invites (room_id, state_key, json) \
                 VALUES (?, ?, ?)",
                params![room_id, state_key, json],
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
            let json: Option<String> = conn
                .query_row(
                    "SELECT json FROM oob_invites WHERE room_id = ? AND state_key = ?",
                    params![room_id, state_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(json) = json else { return Ok(None) };
            let raw = RawJsonValue::from_string(json).map_err(|e| {
                Error::Internal(format!("malformed oob invite json in DB row: {e}"))
            })?;
            // Verbatim rehydrate: compute the id from the canonical bytes, then
            // parse the fields keeping `raw` (and `unsigned.invite_room_state`)
            // byte-for-byte. NOT `from_wire` — that redacts on a content-hash
            // miss and would strip `unsigned`. The event was validated on
            // receipt, so a parse failure here is DB corruption ⇒ Internal.
            let event_id = compute_event_id(&raw).map_err(|e| {
                Error::Internal(format!("oob invite event_id recompute failed: {e}"))
            })?;
            let event = parse_event(raw, event_id, Vec::new()).map_err(|e| {
                Error::Internal(format!("malformed oob invite event in DB row: {e}"))
            })?;
            Ok(Some(event))
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
    use neutrino_common::Event;
    use neutrino_common::event_id::compute_event_id;
    use neutrino_store::InviteStore;
    use ruma::{RoomId, UserId, room_id, user_id};
    use serde_json::value::RawValue;
    use serde_json::{Value, json};

    use crate::tests::store;

    /// A wire-shaped invite `m.room.member` event for a *remote* room we don't
    /// host, carrying `hashes` (required by `parse_event`) and the inviting
    /// server's `unsigned.invite_room_state`. The `event_id` is the real
    /// reference hash of the bytes (as it would be on receipt), so a
    /// `get_invite` that recomputes it matches; storing the whole raw is what
    /// preserves `unsigned` across the round trip.
    fn remote_invite(room: &RoomId, invited: &UserId, inviter: &UserId, room_name: &str) -> Event {
        let body: Value = json!({
            "type": "m.room.member",
            "room_id": room.as_str(),
            "sender": inviter.as_str(),
            "state_key": invited.as_str(),
            "origin_server_ts": 1_700_000_000_000u64,
            "content": { "membership": "invite" },
            // parse_event requires `hashes` present but does not verify the
            // value — any string suffices for the fixture.
            "hashes": { "sha256": "abcDEF0123456789" },
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
        let raw = RawValue::from_string(serde_json::to_string(&body).unwrap()).unwrap();
        let event_id = compute_event_id(&raw).expect("fixture computes event_id");
        let content = serde_json::value::to_raw_value(body.get("content").unwrap()).unwrap();
        Event {
            event_id,
            room_id: room.to_owned(),
            event_type: "m.room.member".to_owned(),
            state_key: Some(invited.as_str().to_owned()),
            sender: inviter.to_owned(),
            origin_server_ts: 1_700_000_000_000,
            content,
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            auth_events: Vec::new(),
            rejected: false,
            soft_failed: false,
            raw,
        }
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
        // ts is parsed back from the stored json (no denormalised column), so
        // it must round-trip — the value `bump_stamp_for_invited` ranks on.
        assert_eq!(got.origin_server_ts, 1_700_000_000_000);

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
