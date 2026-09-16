//! `OobMembershipStore` impl on [`crate::SqliteStore`].
//!
//! Out-of-band memberships — the invite / leave / ban events for a local user
//! in a room this server is not in. See the `oob_memberships` table comment in
//! `schema.sql` and the [`neutrino_store::OobMembershipStore`] trait docs for
//! the contract. Keyed by `(room_id, state_key)` where the `state_key` is the
//! local user; `INSERT OR REPLACE` on that PK gives latest-wins.
//!
//! `get_oob_membership` rehydrates the `Event` verbatim from its stored id +
//! `parse_event` — the same wire→`Event` field parsing `from_wire` uses,
//! **minus the redaction step**. Skipping redaction is the point: `from_wire`
//! redacts on a content-hash miss, which would strip the inviting server's
//! `unsigned.invite_room_state` (the stripped state the sync builder renders
//! from). An out-of-band event is not a room event, so it does not advance
//! the stream cursor; but storing or removing one *does* wake the stream watch
//! (via [`SqliteStore::notify_watch_changed`]) so an in-flight sliding-sync
//! long-poll surfaces the change immediately instead of after its full timeout.

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{OptionalExtension, params};
use neutrino_event::Event;
use neutrino_event::validate::parse_event;
use neutrino_store::{Membership, OobMembership, OobMembershipStore, StorageError};
use ruma::{OwnedEventId, OwnedRoomId, RoomId, UserId};
use serde_json::value::RawValue as RawJsonValue;

use crate::{SqliteStore, error::Error};

#[async_trait]
impl OobMembershipStore for SqliteStore {
    async fn put_oob_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        event: &Event,
        room_version: &str,
    ) -> Result<(), StorageError> {
        let membership = event
            .content_str("membership")
            .as_deref()
            .and_then(Membership::from_wire)
            .filter(|m| matches!(m, Membership::Invite | Membership::Leave | Membership::Ban))
            .ok_or_else(|| {
                StorageError::InvalidInput(
                    "out-of-band membership must be invite, leave or ban".to_owned(),
                )
            })?;
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        let json = event.raw.get().to_owned();
        let event_id = event.event_id.as_str().to_owned();
        let room_version = room_version.to_owned();
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "INSERT OR REPLACE INTO oob_memberships \
                 (room_id, state_key, event_id, membership, room_version, json) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    room_id,
                    state_key,
                    event_id,
                    membership.as_str(),
                    room_version,
                    json
                ],
            )?;
            // Wake any in-flight sliding-sync long-poll so the change surfaces
            // now, not at the poll's timeout. Inside the closure (like
            // `notify_watch`) so a committed row is never stranded.
            SqliteStore::notify_watch_changed(&watch_tx);
            Ok(())
        })
        .await
    }

    async fn get_oob_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<OobMembership>, StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        self.run_read(move |conn| -> Result<Option<OobMembership>, Error> {
            let row: Option<(String, String, String, String)> = conn
                .query_row(
                    "SELECT event_id, membership, room_version, json FROM oob_memberships \
                     WHERE room_id = ? AND state_key = ?",
                    params![room_id, state_key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((event_id, membership, room_version, json)) = row else {
                return Ok(None);
            };
            let raw = RawJsonValue::from_string(json).map_err(|e| {
                Error::Internal(format!("malformed oob membership json in DB row: {e}"))
            })?;
            // Verbatim rehydrate: id from its column, fields parsed keeping `raw`
            // (and `unsigned.invite_room_state`) byte-for-byte. The event was
            // validated on receipt, so a failure here is DB corruption ⇒ Internal.
            let event_id = OwnedEventId::try_from(event_id).map_err(|e| {
                Error::Internal(format!("malformed event_id in oob_memberships row: {e}"))
            })?;
            let event = parse_event(raw, event_id, Vec::new()).map_err(|e| {
                Error::Internal(format!("malformed oob membership event in DB row: {e}"))
            })?;
            let membership = Membership::from_wire(&membership).ok_or_else(|| {
                Error::Internal(format!(
                    "malformed membership {membership:?} in oob_memberships row"
                ))
            })?;
            Ok(Some(OobMembership {
                event,
                membership,
                room_version,
            }))
        })
        .await
    }

    async fn remove_oob_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<(), StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        let watch_tx = self.watch_tx.clone();
        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "DELETE FROM oob_memberships WHERE room_id = ? AND state_key = ?",
                params![room_id, state_key],
            )?;
            SqliteStore::notify_watch_changed(&watch_tx);
            Ok(())
        })
        .await
    }

    async fn oob_memberships(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<(OwnedRoomId, Membership)>, StorageError> {
        let state_key = user_id.as_str().to_owned();
        self.run_read(
            move |conn| -> Result<Vec<(OwnedRoomId, Membership)>, Error> {
                let mut stmt = conn.prepare(
                    "SELECT room_id, membership FROM oob_memberships WHERE state_key = ?",
                )?;
                let rows = stmt.query_map(params![state_key], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                let mut out = Vec::new();
                for r in rows {
                    let (room_id, membership) = r?;
                    let room_id = OwnedRoomId::try_from(room_id).map_err(|e| {
                        Error::Internal(format!("malformed room_id in oob_memberships row: {e}"))
                    })?;
                    let membership = Membership::from_wire(&membership).ok_or_else(|| {
                        Error::Internal(format!(
                            "malformed membership {membership:?} in oob_memberships row"
                        ))
                    })?;
                    out.push((room_id, membership));
                }
                Ok(out)
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use neutrino_event::Event;
    use neutrino_event::event_id::base_version_event_id;
    use neutrino_store::{Membership, OobMembershipStore};
    use ruma::{RoomId, UserId, room_id, user_id};
    use serde_json::value::RawValue;
    use serde_json::{Value, json};

    use crate::tests::store;

    const VERSION: &str = "org.matrix.msc4242.12";

    /// A wire-shaped `m.room.member` event for a *remote* room we don't host,
    /// carrying `hashes` (as a signed peer's event would) and, for an invite,
    /// the inviting server's `unsigned.invite_room_state`. The `event_id` is
    /// the real reference hash of the bytes; storing the whole raw is what
    /// preserves `unsigned` across the round trip.
    fn remote_member(
        room: &RoomId,
        target: &UserId,
        sender: &UserId,
        membership: &str,
        room_name: &str,
    ) -> Event {
        let body: Value = json!({
            "type": "m.room.member",
            "room_id": room.as_str(),
            "sender": sender.as_str(),
            "state_key": target.as_str(),
            "origin_server_ts": 1_700_000_000_000u64,
            "content": { "membership": membership },
            // `parse_event` shape-checks `hashes` but never verifies the value
            // (that is `from_wire`'s receipt check) — any string suffices here.
            "hashes": { "sha256": "abcDEF0123456789" },
            "prev_events": [],
            "prev_state_events": [],
            "unsigned": {
                "invite_room_state": [
                    { "type": "m.room.name", "state_key": "", "sender": sender.as_str(),
                      "content": { "name": room_name } },
                    { "type": "m.room.member", "state_key": sender.as_str(),
                      "sender": sender.as_str(), "content": { "membership": "join" } }
                ]
            }
        });
        let raw = RawValue::from_string(serde_json::to_string(&body).unwrap()).unwrap();
        let event_id = base_version_event_id(&raw).expect("fixture computes event_id");
        let content = serde_json::value::to_raw_value(body.get("content").unwrap()).unwrap();
        Event {
            event_id,
            room_id: room.to_owned(),
            event_type: "m.room.member".to_owned(),
            state_key: Some(target.as_str().to_owned()),
            sender: sender.to_owned(),
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
        let ev = remote_member(room, invited, inviter, "invite", "Cool Room");

        assert!(s.get_oob_membership(room, invited).await.unwrap().is_none());

        s.put_oob_membership(room, invited, &ev, VERSION)
            .await
            .unwrap();
        let got = s
            .get_oob_membership(room, invited)
            .await
            .unwrap()
            .expect("stored");
        // Raw bytes verbatim ⇒ unsigned.invite_room_state survived the round
        // trip (the property the sync invite path depends on).
        assert_eq!(got.event.raw.get(), ev.raw.get());
        assert_eq!(got.event.event_id, ev.event_id);
        assert_eq!(got.event.state_key.as_deref(), Some(invited.as_str()));
        assert_eq!(got.event.sender, inviter);
        // ts is parsed back from the stored json (no denormalised column), so
        // it must round-trip — the value `bump_stamp_for_invited` ranks on.
        assert_eq!(got.event.origin_server_ts, 1_700_000_000_000);
        assert_eq!(got.membership, Membership::Invite);
        assert_eq!(got.room_version, VERSION);

        s.remove_oob_membership(room, invited).await.unwrap();
        assert!(s.get_oob_membership(room, invited).await.unwrap().is_none());
        // Removing a missing pair is a no-op, not an error.
        s.remove_oob_membership(room, invited).await.unwrap();
    }

    // A leave (our rejection) or ban (the inviter's rescission) replaces the
    // invite for the same pair, and the listing reports the new membership.
    #[tokio::test]
    async fn leave_replaces_invite_and_lists_as_leave() {
        let s = store().await;
        let room = room_id!("!remote:other.example.org");
        let alice = user_id!("@alice:example.com");
        let bob = user_id!("@bob:other.example.org");

        s.put_oob_membership(
            room,
            alice,
            &remote_member(room, alice, bob, "invite", "R"),
            VERSION,
        )
        .await
        .unwrap();
        assert_eq!(
            s.oob_memberships(alice).await.unwrap(),
            vec![(room.to_owned(), Membership::Invite)]
        );

        let leave = remote_member(room, alice, alice, "leave", "R");
        s.put_oob_membership(room, alice, &leave, VERSION)
            .await
            .unwrap();
        let got = s.get_oob_membership(room, alice).await.unwrap().unwrap();
        assert_eq!(got.membership, Membership::Leave);
        assert_eq!(got.event.event_id, leave.event_id);
        // REPLACE, not a second row.
        assert_eq!(
            s.oob_memberships(alice).await.unwrap(),
            vec![(room.to_owned(), Membership::Leave)]
        );

        // `join` is in-room state, never out-of-band.
        let err = s
            .put_oob_membership(
                room,
                alice,
                &remote_member(room, alice, alice, "join", "R"),
                VERSION,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn oob_memberships_lists_only_the_users_rows() {
        let s = store().await;
        let room_a = room_id!("!a:other.example.org");
        let room_b = room_id!("!b:other.example.org");
        let alice = user_id!("@alice:example.com");
        let carol = user_id!("@carol:example.com");
        let inviter = user_id!("@bob:other.example.org");

        for (room, who) in [(room_a, alice), (room_b, alice), (room_a, carol)] {
            s.put_oob_membership(
                room,
                who,
                &remote_member(room, who, inviter, "invite", "N"),
                VERSION,
            )
            .await
            .unwrap();
        }

        let mut alice_rooms: Vec<_> = s
            .oob_memberships(alice)
            .await
            .unwrap()
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        alice_rooms.sort();
        let mut want = vec![room_a.to_owned(), room_b.to_owned()];
        want.sort();
        assert_eq!(alice_rooms, want);

        assert_eq!(
            s.oob_memberships(carol).await.unwrap(),
            vec![(room_a.to_owned(), Membership::Invite)]
        );
        assert!(
            s.oob_memberships(user_id!("@nobody:example.com"))
                .await
                .unwrap()
                .is_empty()
        );
    }
}
