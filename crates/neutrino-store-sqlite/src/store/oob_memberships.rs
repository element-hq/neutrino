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

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use deadpool_sqlite::rusqlite::{self, OptionalExtension, params};
use neutrino_event::Event;
use neutrino_event::validate::parse_event;
use neutrino_store::{
    Membership, OobMembership, OobMembershipStore, OobWrite, ResolvedMembership, StorageError,
};
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, UserId};
use serde_json::value::RawValue as RawJsonValue;

use crate::row::{EVENT_COLUMNS_PREFIXED, EventRow};
use crate::{SqliteStore, error::Error};

#[async_trait]
impl OobMembershipStore for SqliteStore {
    async fn put_oob_membership(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
        event: &Event,
        room_version: &str,
        replaces: Option<&EventId>,
    ) -> Result<OobWrite, StorageError> {
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
        let server_suffix = format!(":{}", user_id.server_name());
        let replaces = replaces.map(|id| id.as_str().to_owned());
        let watch_tx = self.watch_tx.clone();

        self.run_write(move |conn| -> Result<OobWrite, Error> {
            // One transaction, so the checks and the insert see the same state:
            // a join persisted before this write refuses it, one persisted after
            // outranks its watermark.
            let tx = conn.transaction()?;
            let server_joined: bool = tx.query_row(
                "SELECT EXISTS (SELECT 1 FROM current_state \
                 WHERE room_id = ?1 AND event_type = 'm.room.member' \
                   AND membership = 'join' AND substr(state_key, -length(?2)) = ?2)",
                params![room_id, server_suffix],
                |row| row.get(0),
            )?;
            if server_joined {
                return Ok(OobWrite::ServerJoined);
            }
            if let Some(replaces) = replaces {
                let held: Option<String> = tx
                    .query_row(
                        "SELECT event_id FROM oob_memberships WHERE room_id = ? AND state_key = ?",
                        params![room_id, state_key],
                        |row| row.get(0),
                    )
                    .optional()?;
                if held.as_deref() != Some(replaces.as_str()) {
                    return Ok(OobWrite::Superseded);
                }
            }
            // `learned_after` is the stream high-water mark, so this row can be
            // ordered against the in-room member event for the same pair.
            // `MAX` over the rowid PK is an index lookup, not a scan, and is
            // preferred over `sqlite_sequence.seq` — after a delete they
            // diverge, and "the newest event we hold" is what the comparison
            // means.
            tx.execute(
                "INSERT OR REPLACE INTO oob_memberships \
                 (room_id, state_key, event_id, membership, room_version, json, learned_after) \
                 VALUES (?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(stream_pos), 0) FROM events))",
                params![
                    room_id,
                    state_key,
                    event_id,
                    membership.as_str(),
                    room_version,
                    json
                ],
            )?;
            tx.commit()?;
            // Wake any in-flight sliding-sync long-poll so the change surfaces
            // now, not at the poll's timeout. Inside the closure (like
            // `notify_watch`) so a committed row is never stranded.
            SqliteStore::notify_watch_changed(&watch_tx);
            Ok(OobWrite::Written)
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
        replaces: Option<&EventId>,
    ) -> Result<(), StorageError> {
        let room_id = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        let replaces = replaces.map(|id| id.as_str().to_owned());
        let watch_tx = self.watch_tx.clone();
        self.run_write(move |conn| -> Result<(), Error> {
            conn.execute(
                "DELETE FROM oob_memberships \
                 WHERE room_id = ?1 AND state_key = ?2 AND (?3 IS NULL OR event_id = ?3)",
                params![room_id, state_key, replaces],
            )?;
            SqliteStore::notify_watch_changed(&watch_tx);
            Ok(())
        })
        .await
    }

    async fn resolved_member_event(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<ResolvedMembership>, StorageError> {
        let room = room_id.as_str().to_owned();
        let state_key = user_id.as_str().to_owned();
        self.run_read(move |conn| -> Result<Option<ResolvedMembership>, Error> {
            // One snapshot for both tables, so a concurrent persist that prunes
            // the stub cannot pair it with a stale in-room row.
            let tx = conn.transaction()?;
            let in_room = in_room_member(&tx, &room, &state_key)?;
            let stub = stub_member(&tx, &room, &state_key)?;
            Ok(pick(in_room, stub))
        })
        .await
    }

    async fn resolved_memberships(
        &self,
        user_id: &UserId,
        memberships: &BTreeSet<Membership>,
    ) -> Result<Vec<(OwnedRoomId, Membership, bool)>, StorageError> {
        if memberships.is_empty() {
            return Ok(Vec::new());
        }
        let state_key = user_id.as_str().to_owned();
        let wanted: Vec<Membership> = memberships.iter().copied().collect();

        self.run_read(
            move |conn| -> Result<Vec<(OwnedRoomId, Membership, bool)>, Error> {
                // One snapshot for both tables — see `resolved_member_event`.
                let tx = conn.transaction()?;
                // (membership, learned_after/stream_pos, out_of_band) per room.
                let mut best: BTreeMap<OwnedRoomId, (Membership, i64, bool)> = BTreeMap::new();

                let mut stmt = tx.prepare(
                    "SELECT cs.room_id, cs.membership, e.stream_pos \
                     FROM current_state cs JOIN events e ON e.event_id = cs.event_id \
                     WHERE cs.state_key = ? AND cs.event_type = 'm.room.member'",
                )?;
                let rows = stmt.query_map(params![state_key], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?;
                for r in rows {
                    let (room_id, membership, pos) = r?;
                    let Some(membership) = membership.as_deref().and_then(Membership::from_wire)
                    else {
                        continue;
                    };
                    best.insert(parse_room(room_id)?, (membership, pos, false));
                }

                let mut stmt = tx.prepare(
                    "SELECT room_id, membership, learned_after FROM oob_memberships \
                     WHERE state_key = ?",
                )?;
                let rows = stmt.query_map(params![state_key], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?;
                for r in rows {
                    let (room_id, membership, learned_after) = r?;
                    let Some(membership) = Membership::from_wire(&membership) else {
                        continue;
                    };
                    let room_id = parse_room(room_id)?;
                    // Ties favour the stub — see `ResolvedMembership`.
                    match best.get(&room_id) {
                        Some((_, pos, _)) if *pos > learned_after => {}
                        _ => {
                            best.insert(room_id, (membership, learned_after, true));
                        }
                    }
                }

                Ok(best
                    .into_iter()
                    .filter(|(_, (m, _, _))| wanted.contains(m))
                    .map(|(room_id, (m, _, oob))| (room_id, m, oob))
                    .collect())
            },
        )
        .await
    }
}

/// Delete every stub in `room_id` that the room's current state has since
/// superseded. Hygiene, not correctness: a superseded stub already loses the
/// comparison in [`pick`], so this only bounds table growth and the per-sync
/// scan. Correlated per `state_key` and guarded on the *current-state* member
/// event, so a rejected or losing member event — which never enters
/// `current_state` — cannot delete a stub that would still have won.
///
/// Called inside the persist transaction, so a stub can never be dropped by a
/// write that later rolls back.
pub(crate) fn prune_superseded_oob(
    tx: &rusqlite::Transaction<'_>,
    room_id: &str,
) -> Result<(), Error> {
    tx.execute(
        "DELETE FROM oob_memberships \
         WHERE room_id = ?1 AND learned_after < ( \
             SELECT e.stream_pos FROM current_state cs \
             JOIN events e ON e.event_id = cs.event_id \
             WHERE cs.room_id = ?1 AND cs.event_type = 'm.room.member' \
               AND cs.state_key = oob_memberships.state_key)",
        params![room_id],
    )?;
    Ok(())
}

fn parse_room(room_id: String) -> Result<OwnedRoomId, Error> {
    OwnedRoomId::try_from(room_id)
        .map_err(|e| Error::Internal(format!("malformed room_id in DB row: {e}")))
}

/// The in-room current-state member event plus its `events.stream_pos`.
fn in_room_member(
    conn: &rusqlite::Connection,
    room_id: &str,
    state_key: &str,
) -> Result<Option<(Event, i64)>, Error> {
    let query = format!(
        "SELECT {EVENT_COLUMNS_PREFIXED}, e.stream_pos \
         FROM current_state cs JOIN events e ON e.event_id = cs.event_id \
         WHERE cs.room_id = ? AND cs.event_type = 'm.room.member' AND cs.state_key = ?"
    );
    let row = conn
        .query_row(&query, params![room_id, state_key], |row| {
            Ok((EventRow::try_from(row), row.get::<_, i64>("stream_pos")?))
        })
        .optional()?;
    match row {
        None => Ok(None),
        Some((ev, pos)) => Ok(Some((ev?.into_event(), pos))),
    }
}

/// The out-of-band stub plus its `learned_after` watermark.
fn stub_member(
    conn: &rusqlite::Connection,
    room_id: &str,
    state_key: &str,
) -> Result<Option<(Event, Membership, i64)>, Error> {
    let row: Option<(String, String, String, i64)> = conn
        .query_row(
            "SELECT event_id, membership, json, learned_after FROM oob_memberships \
             WHERE room_id = ? AND state_key = ?",
            params![room_id, state_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((event_id, membership, json, learned_after)) = row else {
        return Ok(None);
    };
    let raw = RawJsonValue::from_string(json)
        .map_err(|e| Error::Internal(format!("malformed oob membership json in DB row: {e}")))?;
    let event_id = OwnedEventId::try_from(event_id)
        .map_err(|e| Error::Internal(format!("malformed event_id in oob_memberships row: {e}")))?;
    let event = parse_event(raw, event_id, Vec::new())
        .map_err(|e| Error::Internal(format!("malformed oob membership event in DB row: {e}")))?;
    let parsed = Membership::from_wire(&membership).ok_or_else(|| {
        Error::Internal(format!(
            "malformed membership {membership:?} in oob_memberships row"
        ))
    })?;
    Ok(Some((event, parsed, learned_after)))
}

/// The newer of the two wins; ties favour the stub. See [`ResolvedMembership`].
fn pick(
    in_room: Option<(Event, i64)>,
    stub: Option<(Event, Membership, i64)>,
) -> Option<ResolvedMembership> {
    let from_stub = |(event, membership, _): (Event, Membership, i64)| ResolvedMembership {
        event,
        membership,
        out_of_band: true,
    };
    let from_room = |event: Event| {
        let membership = event
            .content_str("membership")
            .as_deref()
            .and_then(Membership::from_wire)?;
        Some(ResolvedMembership {
            event,
            membership,
            out_of_band: false,
        })
    };
    match (in_room, stub) {
        (None, None) => None,
        (Some((ev, _)), None) => from_room(ev),
        (None, Some(stub)) => Some(from_stub(stub)),
        (Some((ev, pos)), Some(stub)) => {
            if pos > stub.2 {
                from_room(ev)
            } else {
                Some(from_stub(stub))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use neutrino_event::Event;
    use neutrino_event::event_id::base_version_event_id;
    use neutrino_store::{Membership, OobMembershipStore, OobWrite};
    use ruma::{RoomId, UserId, room_id, user_id};
    use serde_json::value::RawValue;
    use serde_json::{Value, json};

    use crate::tests::{
        ALICE_ROOM_ID, ALICE_USER_ID, BOB_USER_ID, create_event, member_join, member_leave, store,
    };

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

        s.put_oob_membership(room, invited, &ev, VERSION, None)
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

        s.remove_oob_membership(room, invited, None).await.unwrap();
        assert!(s.get_oob_membership(room, invited).await.unwrap().is_none());
        // Removing a missing pair is a no-op, not an error.
        s.remove_oob_membership(room, invited, None).await.unwrap();
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
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            s.get_oob_membership(room, alice)
                .await
                .unwrap()
                .unwrap()
                .membership,
            Membership::Invite
        );

        let leave = remote_member(room, alice, alice, "leave", "R");
        s.put_oob_membership(room, alice, &leave, VERSION, None)
            .await
            .unwrap();
        let got = s.get_oob_membership(room, alice).await.unwrap().unwrap();
        assert_eq!(got.membership, Membership::Leave);
        assert_eq!(got.event.event_id, leave.event_id);
        // REPLACE, not a second row: the PK is (room_id, state_key), so the
        // resolver can only ever see one stub per pair.
        assert_eq!(
            s.resolved_memberships(
                alice,
                &BTreeSet::from([Membership::Invite, Membership::Leave])
            )
            .await
            .unwrap(),
            vec![(room.to_owned(), Membership::Leave, true)]
        );

        // `join` is in-room state, never out-of-band.
        let err = s
            .put_oob_membership(
                room,
                alice,
                &remote_member(room, alice, alice, "join", "R"),
                VERSION,
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, neutrino_store::StorageError::InvalidInput(_)),
            "{err:?}"
        );
    }

    // ---- ordering between the two membership tables -----------------------

    /// Neither table wins by default: whichever row was learned later does.
    /// These four cases are the whole rule.
    #[tokio::test]
    async fn resolved_member_event_picks_the_newer_of_the_two_tables() {
        use neutrino_store::{EventStore, RoomStore};

        // Only a stub.
        let s = store().await;
        let remote = ruma::room_id!("!remote:other.example.org");
        let invite = remote_member(remote, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(remote, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();
        let got = s
            .resolved_member_event(remote, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("stub alone wins");
        assert!(got.out_of_band);
        assert_eq!(got.membership, Membership::Invite);

        // Only an in-room event.
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_join(*ALICE_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();
        let got = s
            .resolved_member_event(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("in-room alone wins");
        assert!(!got.out_of_band);
        assert_eq!(got.membership, Membership::Join);

        // In-room first, stub after — the F3 shape: a re-invite to a room we
        // left must beat the stale in-room leave.
        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_leave(*ALICE_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();
        let got = s
            .resolved_member_event(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("resolved");
        assert!(
            got.out_of_band,
            "the later stub wins over a stale in-room leave"
        );
        assert_eq!(got.membership, Membership::Invite);

        // Stub first, in-room event after — the reverse: a dead stub must not
        // resurrect once the room's own state moves on.
        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();
        s.persist_event(&member_join(*ALICE_ROOM_ID, *BOB_USER_ID), &[])
            .await
            .unwrap();
        let got = s
            .resolved_member_event(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("resolved");
        assert!(
            !got.out_of_band,
            "the later in-room event wins over the stub"
        );
        assert_eq!(got.membership, Membership::Join);
    }

    /// A stub must not be written once a user on the stub user's server is
    /// joined: the write is refused rather than stamped with a watermark that
    /// ties the join and wins.
    #[tokio::test]
    async fn put_is_refused_while_a_same_server_user_is_joined() {
        use neutrino_store::{EventStore, RoomStore};

        let s = store().await;
        let carol = user_id!("@carol:other.example.org");
        s.create_room(
            &create_event(*ALICE_ROOM_ID, carol),
            &[member_join(*ALICE_ROOM_ID, carol)],
        )
        .await
        .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, carol, "invite", "R");
        assert_eq!(
            s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
                .await
                .unwrap(),
            OobWrite::Written,
            "a remote user's join does not put this server in the room"
        );

        s.persist_event(&member_join(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let rescind = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, carol, "leave", "R");
        assert_eq!(
            s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &rescind, VERSION, None)
                .await
                .unwrap(),
            OobWrite::ServerJoined
        );
        let held = s
            .get_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("earlier stub kept");
        assert_eq!(held.event.event_id, invite.event_id);
    }

    /// A write naming the stub it replaces is refused once that stub has been
    /// replaced: a rescission or decline of the first invite must not clobber
    /// a re-invite that landed meanwhile.
    #[tokio::test]
    async fn put_naming_a_replaced_stub_is_refused() {
        let s = store().await;
        let room = room_id!("!remote:other.example.org");
        let inviter = user_id!("@bob:other.example.org");
        let reinviter = user_id!("@dave:other.example.org");
        let first = remote_member(room, *ALICE_USER_ID, inviter, "invite", "first");
        let second = remote_member(room, *ALICE_USER_ID, reinviter, "invite", "second");
        let leave = remote_member(room, *ALICE_USER_ID, inviter, "leave", "R");

        assert_eq!(
            s.put_oob_membership(room, *ALICE_USER_ID, &leave, VERSION, Some(&first.event_id))
                .await
                .unwrap(),
            OobWrite::Superseded,
            "no row is not the named row"
        );
        s.put_oob_membership(room, *ALICE_USER_ID, &first, VERSION, None)
            .await
            .unwrap();
        s.put_oob_membership(room, *ALICE_USER_ID, &second, VERSION, None)
            .await
            .unwrap();
        assert_eq!(
            s.put_oob_membership(room, *ALICE_USER_ID, &leave, VERSION, Some(&first.event_id))
                .await
                .unwrap(),
            OobWrite::Superseded
        );
        let held = s.get_oob_membership(room, *ALICE_USER_ID).await.unwrap();
        assert_eq!(held.expect("kept").event.event_id, second.event_id);

        assert_eq!(
            s.put_oob_membership(
                room,
                *ALICE_USER_ID,
                &leave,
                VERSION,
                Some(&second.event_id)
            )
            .await
            .unwrap(),
            OobWrite::Written
        );
        let held = s.get_oob_membership(room, *ALICE_USER_ID).await.unwrap();
        assert_eq!(held.expect("replaced").event.event_id, leave.event_id);
    }

    #[tokio::test]
    async fn remove_naming_a_replaced_stub_keeps_the_newer_one() {
        let s = store().await;
        let room = room_id!("!remote:other.example.org");
        let inviter = user_id!("@bob:other.example.org");
        let reinviter = user_id!("@dave:other.example.org");
        let first = remote_member(room, *ALICE_USER_ID, inviter, "invite", "first");
        let second = remote_member(room, *ALICE_USER_ID, reinviter, "invite", "second");
        s.put_oob_membership(room, *ALICE_USER_ID, &second, VERSION, None)
            .await
            .unwrap();

        s.remove_oob_membership(room, *ALICE_USER_ID, Some(&first.event_id))
            .await
            .unwrap();
        assert!(
            s.get_oob_membership(room, *ALICE_USER_ID)
                .await
                .unwrap()
                .is_some()
        );

        s.remove_oob_membership(room, *ALICE_USER_ID, Some(&second.event_id))
            .await
            .unwrap();
        assert!(
            s.get_oob_membership(room, *ALICE_USER_ID)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// The bulk form must agree with the point form, emit exactly one row per
    /// room when both tables hold one, and filter on the winner.
    #[tokio::test]
    async fn resolved_memberships_matches_point_resolver_one_row_per_room() {
        use neutrino_store::RoomStore;

        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_leave(*ALICE_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();

        let wanted = BTreeSet::from([
            Membership::Join,
            Membership::Invite,
            Membership::Knock,
            Membership::Leave,
            Membership::Ban,
        ]);
        let rows = s.resolved_memberships(*BOB_USER_ID, &wanted).await.unwrap();
        let point = s
            .resolved_member_event(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("resolved");
        assert_eq!(
            rows,
            vec![(
                ALICE_ROOM_ID.to_owned(),
                point.membership,
                point.out_of_band
            )]
        );
        assert_eq!(rows[0].1, Membership::Invite);
        assert!(rows[0].2, "out_of_band");

        // Asking only for `leave` must not surface the room via the losing
        // in-room row.
        assert!(
            s.resolved_memberships(*BOB_USER_ID, &BTreeSet::from([Membership::Leave]))
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// Persisting a member event that becomes current state drops the stub it
    /// superseded — hygiene, so the table and the per-sync scan stay bounded.
    #[tokio::test]
    async fn persisting_a_superseding_member_event_prunes_the_stub() {
        use neutrino_store::{EventStore, RoomStore};

        let s = store().await;
        s.create_room(&create_event(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();
        assert!(
            s.get_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID)
                .await
                .unwrap()
                .is_some()
        );

        s.persist_event(&member_join(*ALICE_ROOM_ID, *BOB_USER_ID), &[])
            .await
            .unwrap();
        assert!(
            s.get_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID)
                .await
                .unwrap()
                .is_none(),
            "the superseded stub is pruned"
        );
    }

    /// The rule is per-(room, user), not per-room or per-server: two users in
    /// one room can have different winners at the same time. This is what
    /// makes the multi-user case unremarkable — a predicate keyed on the
    /// server's presence in the room (`server_in_room`) cannot express it,
    /// because the server is equally present for both users.
    #[tokio::test]
    async fn resolution_is_per_user_not_per_room() {
        use neutrino_store::{EventStore, RoomStore};

        let s = store().await;
        let dave = user_id!("@dave:other.example.org");
        s.create_room(
            &create_event(*ALICE_ROOM_ID, dave),
            &[member_join(*ALICE_ROOM_ID, dave)],
        )
        .await
        .unwrap();

        // Carol: in-room leave first, then a stub — the stub wins.
        let carol = user_id!("@carol:example.com");
        s.persist_event(&member_leave(*ALICE_ROOM_ID, carol), &[])
            .await
            .unwrap();
        let carol_invite = remote_member(*ALICE_ROOM_ID, carol, dave, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, carol, &carol_invite, VERSION, None)
            .await
            .unwrap();

        // Bob: stub first, then an in-room join — in-room wins.
        let bob_invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, dave, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &bob_invite, VERSION, None)
            .await
            .unwrap();
        s.persist_event(&member_join(*ALICE_ROOM_ID, *BOB_USER_ID), &[])
            .await
            .unwrap();

        let bob = s
            .resolved_member_event(*ALICE_ROOM_ID, *BOB_USER_ID)
            .await
            .unwrap()
            .expect("bob resolved");
        assert!(!bob.out_of_band, "bob's in-room join is the newer fact");
        assert_eq!(bob.membership, Membership::Join);

        let got = s
            .resolved_member_event(*ALICE_ROOM_ID, carol)
            .await
            .unwrap()
            .expect("carol resolved");
        assert!(got.out_of_band, "carol's stub is the newer fact");
        assert_eq!(got.membership, Membership::Invite);
    }

    /// The reverse: a stub newer than the room's current state survives the
    /// prune, or the prune would be the F3 bug with extra steps.
    #[tokio::test]
    async fn prune_keeps_a_stub_newer_than_current_state() {
        use neutrino_store::{EventStore, RoomStore};

        let s = store().await;
        s.create_room(
            &create_event(*ALICE_ROOM_ID, *ALICE_USER_ID),
            &[member_leave(*ALICE_ROOM_ID, *BOB_USER_ID)],
        )
        .await
        .unwrap();
        let invite = remote_member(*ALICE_ROOM_ID, *BOB_USER_ID, *ALICE_USER_ID, "invite", "R");
        s.put_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID, &invite, VERSION, None)
            .await
            .unwrap();

        // An unrelated event in the same room runs the prune.
        s.persist_event(&member_join(*ALICE_ROOM_ID, *ALICE_USER_ID), &[])
            .await
            .unwrap();
        assert!(
            s.get_oob_membership(*ALICE_ROOM_ID, *BOB_USER_ID)
                .await
                .unwrap()
                .is_some(),
            "a stub newer than the user's current-state member event survives"
        );
    }
}
