//! One outbound `/backfill` round: pick a joined peer, request older PDUs from
//! our backward extremities, persist the fresh, correct-room ones as history.
//! Best-effort — every fault logs and yields 0; the next pagination retries.
//!
//! Ties together the storage primitives from the earlier tasks
//! ([`backward_extremities`](neutrino_store::DagStore::backward_extremities),
//! [`joined_servers`](neutrino_store::StateStore::joined_servers),
//! [`get_events`](neutrino_store::EventStore::get_events),
//! [`persist_historical_event`](neutrino_store::EventStore::persist_historical_event))
//! with the outbound [`FederationClient::backfill`] client call.

use neutrino_event::event_builder::from_wire;
use neutrino_store::{DagStore, EventStore, StateStore};
use neutrino_store_sqlite::SqliteStore;
use ruma::RoomId;
use tracing::{info, warn};

use crate::federation::client::FederationClient;

/// Synapse parity: cap the number of seeds we send so the `/backfill` URI's
/// repeated `v` params stay bounded regardless of how forked the room is.
const MAX_SEEDS: usize = 5;

/// Run ONE outbound backfill round for `room_id`. Best-effort: returns the
/// number of new historical events persisted (0 on any no-op / failure).
/// Never errors out to the caller — logs and returns 0.
// The single live head of the outbound-backfill chain (`backfill_once` →
// `persist_pdus` → `FederationClient::backfill`); its production caller is the
// backward-underflow trigger in the `/messages` handler (`messages.rs`).
pub(crate) async fn backfill_once(
    store: &SqliteStore,
    client: &FederationClient,
    own_server: &str,
    room_id: &RoomId,
    limit: u32,
) -> usize {
    let seeds = match store.backward_extremities(room_id).await {
        Ok(s) if !s.is_empty() => s.into_iter().take(MAX_SEEDS).collect::<Vec<_>>(),
        Ok(_) => return 0,
        Err(e) => {
            warn!(target: "neutrino_http", %room_id, error = %e, "backfill: backward-extremity read failed");
            return 0;
        }
    };
    let dests: Vec<_> = match store.joined_servers(room_id).await {
        Ok(d) => d.into_iter().filter(|s| s.as_str() != own_server).collect(),
        Err(e) => {
            warn!(target: "neutrino_http", %room_id, error = %e, "backfill: joined_servers read failed");
            return 0;
        }
    };
    if dests.is_empty() {
        return 0;
    }

    // Sequential failover: the first peer that answers wins; on a peer fault we
    // move on to the next.
    for dest in dests {
        match client.backfill(&dest, room_id, &seeds, limit).await {
            Ok(pdus) => return persist_pdus(store, room_id, pdus, limit).await,
            Err(e) => {
                info!(target: "neutrino_http", %dest, %room_id, error = %e, "backfill: peer failed, trying next");
            }
        }
    }
    0
}

/// Persist the fresh, correct-room PDUs from one `/backfill` response, returning
/// the count persisted. The response is newest-first; we persist in that same
/// order so each successive `persist_historical_event` (which allocates a
/// stream position one below the running minimum) lays the newest backfilled
/// event closest to 0, preserving timeline order under a `room_messages` DESC
/// read. Do NOT reverse the list.
///
/// Defensive cap: we sent `limit` on the wire, but a buggy/hostile peer could
/// return more — `.take(limit)` bounds how many we persist regardless.
async fn persist_pdus(
    store: &SqliteStore,
    room_id: &RoomId,
    pdus: Vec<Box<serde_json::value::RawValue>>,
    limit: u32,
) -> usize {
    let mut persisted = 0usize;
    for raw in pdus.into_iter().take(limit as usize) {
        // `from_wire` derives the id from the reference hash; an unparseable
        // or drop-class PDU is dropped, exactly as the inbound `/send` path
        // does. A `Wire::Rejected` event persists *as rejected* — history
        // must carry the verdict so a descendant's reference check
        // cascade-rejects, and so the malformed content can never surface as
        // an accepted row (clients filter rejected; state-res excludes it).
        let event = match from_wire(raw, Vec::new()) {
            Ok(wire) => wire.into_event(),
            Err(e) => {
                warn!(target: "neutrino_http", %room_id, error = %e, "backfill: skipping unparseable PDU");
                continue;
            }
        };
        // A peer can return events for any room; only persist ones in *this* room.
        if event.room_id != *room_id {
            continue;
        }
        // Dedup: skip a PDU we already hold.
        match store.get_events(&[event.event_id.as_ref()]).await {
            Ok(held) if !held.is_empty() => continue,
            Ok(_) => {}
            Err(e) => {
                warn!(target: "neutrino_http", %room_id, error = %e, "backfill: dedup read failed, skipping PDU");
                continue;
            }
        }
        match store.persist_historical_event(&event).await {
            Ok(()) => persisted += 1,
            Err(e) => {
                warn!(target: "neutrino_http", %room_id, error = %e, "backfill: persist failed")
            }
        }
    }
    persisted
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, extract::Path, routing::get};
    use neutrino_event::event_builder::EventBuilder;
    use neutrino_event::{Event, ROOM_VERSION_ID};
    use neutrino_store::{Direction, EventStore, RoomStore};
    use neutrino_store_sqlite::SqliteStore;
    use ruma::{OwnedRoomId, OwnedUserId, event_id};
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::federation::test_support::spawn_stub;

    const OWN: &str = "example.org";

    fn alice() -> OwnedUserId {
        "@alice:example.org".parse().unwrap()
    }

    /// A user whose server name is `peer_server` — so that joining them makes
    /// `peer_server` a joined destination `backfill_once` will resolve and ask.
    fn user_on(peer_server: &ruma::ServerName) -> OwnedUserId {
        format!("@peer:{peer_server}").parse().unwrap()
    }

    async fn fresh_store() -> (Arc<SqliteStore>, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let store = Arc::new(
            SqliteStore::open_in_dir(dir.path())
                .await
                .expect("open sqlite"),
        );
        (store, dir)
    }

    fn create_event(sender: &OwnedUserId) -> Event {
        EventBuilder::new(sender.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .build()
            .expect("build create")
    }

    fn member_join(
        room_id: &OwnedRoomId,
        create_id: &ruma::EventId,
        sender: &OwnedUserId,
    ) -> Event {
        EventBuilder::new(sender.clone(), "m.room.member".to_owned())
            .room_id(room_id.clone())
            .state_key(sender.as_str().to_owned())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![create_id.to_owned()])
            .prev_state_events(vec![create_id.to_owned()])
            .build()
            .expect("build join")
    }

    /// Create a room joined by `members`, then persist a message whose
    /// `prev_events` dangles onto an unheld id — opening a backward extremity so
    /// `backfill_once` has a seed to walk back from. Returns the room id and the
    /// create event id (so callers can ground further joins onto it).
    async fn seed_room_with_extremity(
        store: &SqliteStore,
        members: &[OwnedUserId],
    ) -> (OwnedRoomId, ruma::OwnedEventId) {
        let creator = &members[0];
        let create = create_event(creator);
        let room_id = create.room_id.clone();
        let create_id = create.event_id.clone();
        let join = member_join(&room_id, &create_id, creator);
        store.create_room(&create, &[join]).await.expect("create");
        // Additional joined members (e.g. a remote peer's user).
        for m in &members[1..] {
            let mj = member_join(&room_id, &create_id, m);
            store.persist_event(&mj, &[]).await.expect("persist member");
        }
        // A message dangling onto a parent we don't hold → backward extremity.
        let dangling = EventBuilder::new(creator.clone(), "m.room.message".to_owned())
            .room_id(room_id.clone())
            .content(json!({ "msgtype": "m.text", "body": "tip" }))
            .prev_events(vec![
                event_id!("$unheld_parent:remote.example.org").to_owned(),
            ])
            .build()
            .expect("build dangling");
        store
            .persist_historical_event(&dangling)
            .await
            .expect("persist dangling");
        (room_id, create_id)
    }

    /// Build a real, parseable historical PDU in `room_id` (dangling prev so it
    /// has no ancestry requirement), returning its canonical wire bytes.
    fn pdu_in(room_id: &OwnedRoomId, body: &str) -> String {
        EventBuilder::new(alice(), "m.room.message".to_owned())
            .room_id(room_id.clone())
            .content(json!({ "msgtype": "m.text", "body": body }))
            .prev_events(vec![event_id!("$ghost:remote.example.org").to_owned()])
            .build()
            .expect("build pdu")
            .raw
            .get()
            .to_owned()
    }

    /// A `/backfill` peer stub with interior-mutable state, so a test can mount
    /// it, learn the room id, then set the PDUs it returns (mirrors the
    /// `StubFetcher` idiom in `federation/tests.rs`). `pdus` are returned
    /// newest-first in a transaction envelope; `hits` counts the requests served.
    #[derive(Clone)]
    struct PeerStub {
        pdus: Arc<Mutex<Vec<String>>>,
        hits: Arc<Mutex<usize>>,
    }

    impl PeerStub {
        /// Mount the stub on an ephemeral port; returns the stub handle and its
        /// `ServerName` (`127.0.0.1:{port}`).
        async fn spawn() -> (Self, ruma::OwnedServerName) {
            let stub = Self {
                pdus: Arc::new(Mutex::new(Vec::new())),
                hits: Arc::new(Mutex::new(0)),
            };
            let s = stub.clone();
            let app = Router::new().route(
                "/_matrix/federation/v1/backfill/{room}",
                get(move |Path(_room): Path<String>| {
                    let s = s.clone();
                    async move {
                        *s.hits.lock().unwrap() += 1;
                        let arr: Vec<Value> = s
                            .pdus
                            .lock()
                            .unwrap()
                            .iter()
                            .map(|raw| serde_json::from_str(raw).unwrap())
                            .collect();
                        Json(json!({
                            "origin": "remote.example.org",
                            "origin_server_ts": 0,
                            "pdus": arr,
                        }))
                    }
                }),
            );
            let dest = spawn_stub(app).await;
            (stub, dest)
        }

        fn set_pdus(&self, raws: Vec<String>) {
            *self.pdus.lock().unwrap() = raws;
        }

        fn hits(&self) -> usize {
            *self.hits.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn backfill_once_noop_without_peers() {
        // Only our own server is joined → no destination to ask.
        let (store, _dir) = fresh_store().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice()]).await;
        let client = FederationClient::new(OWN.to_owned(), None);

        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 0, "no other joined server -> nothing to backfill");
    }

    #[tokio::test]
    async fn backfill_once_noop_without_extremities() {
        // A grounded room (no backward extremity) has no seeds → no-op, even
        // with a remote peer joined.
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let peer = user_on(&dest);
        let create = create_event(&peer);
        let room_id = create.room_id.clone();
        let join = member_join(&room_id, &create.event_id, &peer);
        store.create_room(&create, &[join]).await.expect("create");
        let client = FederationClient::new(OWN.to_owned(), None);

        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 0, "grounded room -> no seeds -> nothing to backfill");
        assert_eq!(stub.hits(), 0, "no seeds -> peer never queried");
    }

    #[tokio::test]
    async fn backfill_once_persists_fresh_events() {
        // The joined peer's server name IS the stub's host:port, so the
        // orchestrator resolves and asks it end to end.
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        stub.set_pdus(vec![pdu_in(&room_id, "history")]);

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 1, "one fresh, correct-room PDU is persisted");
        assert_eq!(stub.hits(), 1, "the peer was queried once");
    }

    #[tokio::test]
    async fn backfill_once_persists_malformed_pdu_as_rejected() {
        // The Wire contract at the backfill ingress: a semantically-malformed
        // PDU (rule 5.1 — member without membership) fetched via /backfill
        // must persist AS REJECTED, never as an accepted row — otherwise it
        // would leak into client timelines, pass descendants' rule-2.3
        // reference checks, and expose the auth machinery to malformed
        // content via state walks. Hand-rolled wire JSON: EventBuilder
        // refuses to produce it, and the wrong content hash is fine (member
        // redaction keeps `membership`, which is absent — the defect
        // survives).
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        let bad_raw = json!({
            "type": "m.room.member",
            "state_key": "@mallory:remote.example.org",
            "sender": alice().as_str(),
            "room_id": room_id.as_str(),
            "content": {},
            "prev_events": ["$ghost:remote.example.org"],
            "prev_state_events": [],
            "origin_server_ts": 1_700_000_000_000u64,
            "hashes": { "sha256": "wrong" },
        })
        .to_string();
        let bad_id = from_wire(
            serde_json::value::RawValue::from_string(bad_raw.clone()).expect("valid JSON"),
            Vec::new(),
        )
        .expect("parseable")
        .into_event()
        .event_id;
        stub.set_pdus(vec![bad_raw]);

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 1, "the rejected row still counts as persisted");
        let got = store.get_events(&[bad_id.as_ref()]).await.expect("read");
        assert_eq!(got.len(), 1, "the malformed PDU must be persisted");
        assert!(got[0].rejected, "…and must carry the rejected verdict");
    }

    #[tokio::test]
    async fn backfill_once_caps_persisted_at_limit() {
        // The peer returns MORE fresh, correct-room PDUs than the requested
        // `limit`. The defensive `.take(limit)` in `persist_pdus` must bound
        // the count: a peer can't make us persist more than we asked for.
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        // Five distinct fresh PDUs, but we ask for at most 3.
        stub.set_pdus(
            (0..5)
                .map(|i| pdu_in(&room_id, &format!("hist-{i}")))
                .collect(),
        );

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 3).await;
        assert_eq!(n, 3, "persisted count is capped at the requested limit");
    }

    #[tokio::test]
    async fn backfill_once_rejects_wrong_room() {
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        // The stub returns a PDU built for a *different* room that ALSO EXISTS in
        // the store — so the only thing stopping it being persisted is the
        // room-id check, not a missing-room error at persist time. Drop the check
        // and this PDU would persist (n == 1), so the assertion genuinely pins it.
        let bob = "@bob:example.org".parse::<OwnedUserId>().unwrap();
        let other_create = create_event(&bob);
        let other_room = other_create.room_id.clone();
        let other_join = member_join(&other_room, &other_create.event_id, &bob);
        store
            .create_room(&other_create, &[other_join])
            .await
            .expect("create other room");
        stub.set_pdus(vec![pdu_in(&other_room, "intruder")]);

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 0, "a wrong-room PDU must be rejected, not persisted");
    }

    #[tokio::test]
    async fn backfill_once_continues_past_held_to_persist_fresh() {
        // No-early-abort: the peer returns an already-held PDU (newest-first)
        // followed by a *fresh* sibling. The held one is skipped via `continue`,
        // so the loop must still reach and persist the sibling after it. n == 1
        // (not 0 — proving the batch doesn't abort on the held PDU; not 2 —
        // proving the held one isn't counted).
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        let held_raw = pdu_in(&room_id, "already here");
        let held: Event = from_wire(
            serde_json::value::RawValue::from_string(held_raw.clone()).unwrap(),
            Vec::new(),
        )
        .expect("parse held")
        .into_event();
        store
            .persist_historical_event(&held)
            .await
            .expect("seed held");
        let fresh_raw = pdu_in(&room_id, "brand new");
        stub.set_pdus(vec![held_raw, fresh_raw]);

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(
            n, 1,
            "the held PDU is deduped (not counted); the fresh sibling is still persisted"
        );
    }

    #[tokio::test]
    async fn backfill_once_held_pdu_yields_no_duplicate_row() {
        // The dedup *contract*: a PDU we already hold is not re-persisted — the
        // peer returns ONLY the held event and the round persists nothing new,
        // leaving exactly one row for that id and the timeline unchanged.
        //
        // HONESTY NOTE: the `UNIQUE(event_id)` constraint is the *hard* backstop —
        // even if the `get_events` pre-check were deleted, the doomed re-INSERT
        // rolls back atomically (no row, no `stream_pos` consumed), so this test
        // would still pass. The pre-check is therefore an *optimisation* (skip a
        // guaranteed-failing write), and isolating "pre-check ran" from "UNIQUE
        // caught it" is not observable through the public store API without a
        // persistence spy — deliberately out of scope here. What this test (with
        // `…continues_past_held…` above) pins is the observable contract: held
        // PDUs never duplicate a row and never abort the batch.
        let (store, _dir) = fresh_store().await;
        let (stub, dest) = PeerStub::spawn().await;
        let (room_id, _create) = seed_room_with_extremity(&store, &[alice(), user_on(&dest)]).await;
        let held_raw = pdu_in(&room_id, "already here");
        let held: Event = from_wire(
            serde_json::value::RawValue::from_string(held_raw.clone()).unwrap(),
            Vec::new(),
        )
        .expect("parse held")
        .into_event();
        store
            .persist_historical_event(&held)
            .await
            .expect("seed held");
        let backward_ids = |s: Arc<SqliteStore>, rid: OwnedRoomId| async move {
            let (events, _) = s
                .room_messages(&rid, None, None, Direction::Backward, 100)
                .await
                .expect("backward read");
            events
                .iter()
                .map(|e| e.event_id.clone())
                .collect::<Vec<_>>()
        };
        let before = backward_ids(store.clone(), room_id.clone()).await;

        stub.set_pdus(vec![held_raw]);
        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 0, "the sole, already-held PDU is deduped → nothing new");

        let after = backward_ids(store.clone(), room_id.clone()).await;
        assert_eq!(
            after.iter().filter(|id| **id == held.event_id).count(),
            1,
            "the held event appears exactly once — never duplicated"
        );
        assert_eq!(
            after, before,
            "the timeline is unchanged: no new or re-positioned rows"
        );
    }

    #[tokio::test]
    async fn backfill_once_fails_over_to_next_dest() {
        // First dest refuses the connection (dead port); the second answers. The
        // round must skip the first fault and persist the live peer's PDU.
        // `joined_servers` returns names sorted, so we ensure the dead peer sorts
        // strictly before the live one (re-roll the dead port until it does) —
        // making the failover order deterministic rather than port-luck.
        let (store, _dir) = fresh_store().await;
        let (stub, live) = PeerStub::spawn().await;
        let mut dead = crate::federation::test_support::dead_peer().await;
        while dead.as_str() >= live.as_str() {
            dead = crate::federation::test_support::dead_peer().await;
        }

        let (room_id, create_id) =
            seed_room_with_extremity(&store, &[alice(), user_on(&dead)]).await;
        store
            .persist_event(&member_join(&room_id, &create_id, &user_on(&live)), &[])
            .await
            .expect("join live peer");
        stub.set_pdus(vec![pdu_in(&room_id, "from second peer")]);

        let client = FederationClient::new(OWN.to_owned(), None);
        let n = backfill_once(&store, &client, OWN, &room_id, 10).await;
        assert_eq!(n, 1, "failover reaches the live peer and persists its PDU");
        assert_eq!(stub.hits(), 1, "the live peer was queried once");
    }
}
