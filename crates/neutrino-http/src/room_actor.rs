//! Per-room state-machine actor.
//!
//! Exactly one [`RoomCore`] per room runs at a time, owned by a dedicated
//! async task fed from an mpsc channel. Serialising every apply through a
//! single owner is what keeps the state DAG consistent: two concurrent
//! applies against the same forward extremities would each read the same
//! heads, both extend them, and the second commit would clobber the first —
//! corrupting the DAG. (See PLAN.md, Phase 6d.)
//!
//! ## Layering
//!
//! - The actor owns the mutable state ([`RoomCore`]: the two head-sets +
//!   current_state) in memory. Single owner ⇒ no concurrent writer.
//! - `RoomCore::apply` reads only immutable data (persisted events + auth
//!   chains) through a provider, so it needs no write transaction. The
//!   provider is connection-bound and `pub(crate)`, so the actor borrows one
//!   for the apply via [`SqliteStore::with_state_provider`] — the store opens
//!   a reader connection and hands back a `&dyn StateProvider`; the apply
//!   itself stays here in the actor.
//! - Persisting the accepted event + new head-sets is the one short write
//!   transaction — `EventStore::persist_resolved_event`.
//!
//! The actor applies to a **clone** of its `RoomCore` and adopts it only
//! after the persist commits, so an apply hard-reject or a storage fault
//! leaves the in-memory state untouched.
//!
//! The CSAPI write handlers (`/send`, `/state`) drive this via the
//! `RoomRegistry` held in `AppState`.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use neutrino_common::Event;
use neutrino_state::room_core::{Effect, RoomCore};
use neutrino_state::{CoreError, FormatError, StateDelta, StateMap};
use neutrino_store::{EventStore, Membership, RoomStore, StateStore, StorageError};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

/// Inbox depth for a room actor. Senders `.await` when full, applying
/// natural back-pressure rather than unbounded buffering.
const ACTOR_INBOX: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum RoomActorError {
    #[error("no such room")]
    UnknownRoom,
    #[error("building event: {0}")]
    Build(#[from] FormatError),
    #[error("event rejected: {0}")]
    Apply(#[from] CoreError),
    /// A locally-originated event passed the pipeline but was *rejected* by
    /// the auth rules (REJECT disposition). Distinct from [`Self::Apply`]
    /// (a retryable/missing-data `CoreError`): a reject is a verdict, surfaced
    /// to the client as 403, and the event is NOT persisted (Synapse parity —
    /// locally-refused events are never stored). Federation PDUs take the
    /// opposite policy and persist their rejects, so this never arises there.
    #[error("event rejected by auth rules")]
    Rejected,
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    /// Apply produced no event to persist. A freshly built event (unique
    /// heads + timestamp) is never an idempotent no-op, so this is
    /// unreachable in practice — surfaced rather than panicked on.
    #[error("apply produced no event")]
    NotApplied,
    /// The actor task is gone (channel closed) — it panicked or was dropped.
    #[error("room actor unavailable")]
    ActorGone,
}

/// A command processed by a room actor.
enum Command {
    /// Build, apply, and persist a locally-originated event, then reply with
    /// the outcome.
    Send {
        sender: OwnedUserId,
        event_type: String,
        /// `Some` for a state event, `None` for a message-type event.
        state_key: Option<String>,
        content: Value,
        reply: oneshot::Sender<Result<Arc<Event>, RoomActorError>>,
    },
    /// Apply a fully-formed PDU received over federation. Unlike [`Self::Send`]
    /// there is nothing to build — the event arrives complete — and the
    /// federation persist policy applies: an accepted, soft-failed, or rejected
    /// event is all persisted (rejects are recorded, not dropped); only a
    /// missing-ancestry / fault `CoreError` propagates so the caller can
    /// backfill and re-deliver. `event` is boxed to keep the `Command` enum
    /// small (an `Event` dwarfs `Send`'s fields).
    ///
    /// Driven by [`RoomRegistry::apply_pdu`], called from the federation
    /// `PUT /send/{txn}` handler (`federation::send`).
    ApplyPdu {
        event: Box<Event>,
        reply: oneshot::Sender<Result<(), RoomActorError>>,
    },
}

/// The owned state machine for one room. Lives inside a spawned task; the
/// only handle to it is the mpsc `Sender` held in the [`RoomRegistry`].
struct RoomActor {
    room: RoomCore,
    store: Arc<SqliteStore>,
    /// This homeserver's own name, excluded from every outbound destination
    /// set so we never federate an event back to ourselves. Held as a `String`
    /// (the config form) since it's only ever compared by value.
    own_server: String,
}

impl RoomActor {
    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Send {
                    sender,
                    event_type,
                    state_key,
                    content,
                    reply,
                } => {
                    let outcome = self
                        .handle_send(sender, event_type, state_key, content)
                        .await;
                    // The caller may have given up (dropped the receiver);
                    // that's fine — the apply already committed or didn't.
                    let _ = reply.send(outcome);
                }
                Command::ApplyPdu { event, reply } => {
                    let outcome = self.handle_apply_pdu(*event).await;
                    let _ = reply.send(outcome);
                }
            }
        }
    }

    /// Run `apply_pdu` against a *clone* of the live `RoomCore`, off to the
    /// side. The store hands us a read-only provider; the state-machine logic
    /// stays here in the actor. Returns the post-apply core (adopted by the
    /// caller only after any persist commits) and the emitted effects.
    async fn run_apply(&self, event: Event) -> Result<(RoomCore, Vec<Effect>), RoomActorError> {
        let room = self.room.clone();
        let (next, verdict) = self
            .store
            .with_state_provider(move |provider| {
                let mut room = room;
                let verdict = room.apply_pdu(event, provider);
                (room, verdict)
            })
            .await?;
        Ok((next, verdict?))
    }

    async fn handle_send(
        &mut self,
        sender: OwnedUserId,
        event_type: String,
        state_key: Option<String>,
        content: Value,
    ) -> Result<Arc<Event>, RoomActorError> {
        // Build the event on the room's current heads — the state machine owns
        // the builder (`RoomCore::build_local_event`) so the actor and the
        // createRoom batch share one construction path.
        let event = self
            .room
            .build_local_event(sender, event_type, state_key, content)?;

        let (next, effects) = self.run_apply(event).await?;
        let (persisted, delta) = collect_effects(effects);
        let event = persisted.ok_or(RoomActorError::NotApplied)?;

        // Local policy: a rejected event is surfaced to the client as 403 and
        // is NOT persisted (Synapse never stores a locally-refused event). The
        // post-apply clone is discarded — `apply_pdu` doesn't mutate on reject,
        // so `self.room` is already correct.
        if event.rejected {
            return Err(RoomActorError::Rejected);
        }

        // Federate to every remote server in the post-apply room state — but a
        // soft-failed event must not be relayed (it failed auth against current
        // state, so peers would reject it too). Soft-fail is only ever set on
        // non-state events, so an unrejected state event always federates; the
        // explicit `state_key.is_none()` guard keeps that invariant local.
        let destinations = if event.soft_failed && event.state_key.is_none() {
            Vec::new()
        } else {
            outbound_destinations(next.current_state(), &event, &self.own_server)
        };
        let dest_refs: Vec<&ServerName> = destinations.iter().map(AsRef::as_ref).collect();

        self.store
            .persist_resolved_event(
                &event,
                next.forward_extremities(),
                next.state_forward_extremities(),
                &delta,
                &dest_refs,
            )
            .await?;
        // Commit succeeded — adopt the post-apply state.
        self.room = next;

        Ok(event)
    }

    /// Integrate a fully-formed federation PDU. The federation persist policy:
    /// an accepted, soft-failed, or rejected event is all persisted (a reject
    /// is recorded so it can be referenced and is never re-requested). A
    /// re-delivered PDU that is already persisted is an idempotent no-op
    /// (`apply_pdu` returns no effects). A missing-ancestry / fault `CoreError`
    /// propagates so the caller can backfill and re-deliver.
    async fn handle_apply_pdu(&mut self, event: Event) -> Result<(), RoomActorError> {
        let (next, effects) = self.run_apply(event).await?;
        // Idempotent no-op: the PDU is already persisted. Nothing to commit.
        if effects.is_empty() {
            return Ok(());
        }
        let (persisted, delta) = collect_effects(effects);
        let event = persisted.ok_or(RoomActorError::NotApplied)?;

        // No outbox rows: a federation-received PDU is not re-originated by us.
        // Its origin server fans it out to the rest of the room.
        self.store
            .persist_resolved_event(
                &event,
                next.forward_extremities(),
                next.state_forward_extremities(),
                &delta,
                &[],
            )
            .await?;
        self.room = next;
        Ok(())
    }
}

/// Servers that must receive a locally-originated event over federation: every
/// server with a `join` member in the post-apply `current_state`, plus — for an
/// `m.room.member` event — the target user's server, so a leave/kick/ban
/// reaches the departing server even though it has already dropped out of the
/// joined set. Our own server is never a destination.
fn outbound_destinations(
    current_state: &StateMap<Arc<Event>>,
    event: &Event,
    own_server: &str,
) -> Vec<OwnedServerName> {
    let mut servers: BTreeSet<OwnedServerName> = BTreeSet::new();

    for ((event_type, state_key), member) in current_state {
        if event_type == "m.room.member"
            && event_membership(member) == Some(Membership::Join)
            && let Ok(user) = UserId::parse(state_key.as_str())
        {
            servers.insert(user.server_name().to_owned());
        }
    }

    // A departing member (leave/ban) has already dropped out of the joined set
    // above, so explicitly notify their server of its own departure. Joins are
    // already covered by the joined-set scan; invites/knocks are delivered via
    // the dedicated `/invite` handshake endpoint, not transaction broadcast, so
    // they are deliberately excluded here.
    if event.event_type == "m.room.member"
        && matches!(
            event_membership(event),
            Some(Membership::Leave | Membership::Ban)
        )
        && let Some(state_key) = &event.state_key
        && let Ok(user) = UserId::parse(state_key.as_str())
    {
        servers.insert(user.server_name().to_owned());
    }

    servers.retain(|s| s.as_str() != own_server);
    servers.into_iter().collect()
}

/// An `m.room.member` event's `content.membership`, parsed through the
/// canonical [`Membership`] alphabet. `None` for a non-member event or any
/// missing / unrecognised membership string.
fn event_membership(event: &Event) -> Option<Membership> {
    serde_json::from_str::<Value>(event.content.get())
        .ok()
        .and_then(|c| {
            c.get("membership")
                .and_then(Value::as_str)
                .and_then(Membership::from_wire)
        })
}

/// Split `apply_pdu`'s effects into the event to persist and the current-state
/// delta. An accepted event emits exactly one `Persist`; a state event that
/// changes current_state additionally emits `UpdateCurrentState` (others leave
/// the delta empty). The returned event carries its own `soft_failed` /
/// `rejected` flags.
fn collect_effects(effects: Vec<Effect>) -> (Option<Arc<Event>>, StateDelta) {
    let mut persisted: Option<Arc<Event>> = None;
    let mut delta = StateDelta::new();
    for effect in effects {
        match effect {
            Effect::Persist { event } => persisted = Some(event),
            Effect::UpdateCurrentState(d) => delta = d,
        }
    }
    (persisted, delta)
}

/// Registry of per-room actors. Looks up (or lazily bootstraps + spawns) the
/// single actor for a room and forwards commands to it.
///
/// The map grows monotonically: an entry (and its spawned task) is created on
/// first access to a room and is never evicted. This is intentional for the
/// embedded single-user homeserver — the live room set is small and bounded by
/// the device's own membership, so there's no idle-eviction / LRU policy. If
/// this ever backs a multi-tenant deployment, add one.
pub struct RoomRegistry {
    store: Arc<SqliteStore>,
    /// This homeserver's own name, handed to each spawned actor so it can
    /// exclude itself from outbound federation destinations.
    own_server: String,
    actors: Mutex<HashMap<OwnedRoomId, mpsc::Sender<Command>>>,
}

impl RoomRegistry {
    pub fn new(store: Arc<SqliteStore>, own_server: String) -> Self {
        Self {
            store,
            own_server,
            actors: Mutex::new(HashMap::new()),
        }
    }

    /// Send a locally-originated event into `room_id` and await its outcome.
    /// Bootstraps + spawns the room's actor on first use.
    pub async fn send_event(
        &self,
        room_id: &RoomId,
        sender: OwnedUserId,
        event_type: String,
        state_key: Option<String>,
        content: Value,
    ) -> Result<Arc<Event>, RoomActorError> {
        let actor = self.actor_for(room_id).await?;
        let (reply, rx) = oneshot::channel();
        actor
            .send(Command::Send {
                sender,
                event_type,
                state_key,
                content,
                reply,
            })
            .await
            .map_err(|_| RoomActorError::ActorGone)?;
        rx.await.map_err(|_| RoomActorError::ActorGone)?
    }

    /// Integrate a fully-formed PDU received over federation into `room_id`
    /// and await its outcome. Bootstraps + spawns the room's actor on first
    /// use. `Ok(())` covers acceptance, soft-fail, rejection (all persisted),
    /// and idempotent re-delivery; an `Err` carrying a retryable `CoreError`
    /// (see [`CoreError::is_retryable`]) signals the caller to backfill the
    /// missing ancestry and re-deliver.
    ///
    /// Called from the federation `PUT /send/{txn}` handler
    /// (`federation::send::apply_with_gapfill`), which owns the backfill loop.
    pub async fn apply_pdu(&self, room_id: &RoomId, event: Event) -> Result<(), RoomActorError> {
        let actor = self.actor_for(room_id).await?;
        let (reply, rx) = oneshot::channel();
        actor
            .send(Command::ApplyPdu {
                event: Box::new(event),
                reply,
            })
            .await
            .map_err(|_| RoomActorError::ActorGone)?;
        rx.await.map_err(|_| RoomActorError::ActorGone)?
    }

    /// Existing handle, or bootstrap from storage and spawn a new actor.
    async fn actor_for(&self, room_id: &RoomId) -> Result<mpsc::Sender<Command>, RoomActorError> {
        if let Some(tx) = self.lookup(room_id) {
            return Ok(tx);
        }

        // Bootstrap the RoomCore from storage (no lock held — this is I/O).
        let (timeline_fes, state_fes) = self
            .store
            .forward_extremities(room_id)
            .await?
            .ok_or(RoomActorError::UnknownRoom)?;
        let current_state: StateMap<Arc<Event>> = self
            .store
            .current_room_state(room_id)
            .await?
            .into_iter()
            .map(|(key, ev)| (key, Arc::new(ev)))
            .collect();
        let room = RoomCore::hydrate(room_id.to_owned(), timeline_fes, state_fes, current_state);

        // Insert under the lock, re-checking so a racing bootstrap doesn't
        // spawn a second actor for the same room (the loser discards `room`).
        let mut actors = self.actors.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(tx) = actors.get(room_id) {
            return Ok(tx.clone());
        }
        let (tx, rx) = mpsc::channel(ACTOR_INBOX);
        tokio::spawn(
            RoomActor {
                room,
                store: self.store.clone(),
                own_server: self.own_server.clone(),
            }
            .run(rx),
        );
        actors.insert(room_id.to_owned(), tx.clone());
        Ok(tx)
    }

    fn lookup(&self, room_id: &RoomId) -> Option<mpsc::Sender<Command>> {
        self.actors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(room_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neutrino_common::ROOM_VERSION_ID;
    use neutrino_state::event_id::EventBuilder;
    use neutrino_store::{FederationOutbox, RoomStore};
    use ruma::server_name;
    use serde_json::json;

    const ALICE: &str = "@alice:example.org";
    const BOB: &str = "@bob:example.org";
    const ZARA: &str = "@zara:remote.example";

    /// Open an in-memory store, create a room with `ALICE` as creator (just
    /// the create event — no members yet), and return the registry, store,
    /// room id, and alice's user id. createRoom seeds the forward extremities
    /// to the create event, so the actor can bootstrap.
    async fn setup() -> (RoomRegistry, Arc<SqliteStore>, OwnedRoomId, OwnedUserId) {
        let store = Arc::new(SqliteStore::open_in_memory().await.expect("open store"));
        let alice: OwnedUserId = ALICE.parse().expect("alice");
        let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .build()
            .expect("build create");
        let room_id = create.room_id.clone();
        store.create_room(&create, &[]).await.expect("create_room");
        let registry = RoomRegistry::new(store.clone(), "example.org".to_owned());
        (registry, store, room_id, alice)
    }

    #[tokio::test]
    async fn applies_state_event_and_advances_heads() {
        let (registry, store, room_id, alice) = setup().await;

        let event = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join accepted");
        assert!(!event.soft_failed);

        // current_state now carries alice's join.
        let member = store
            .current_state_event(&room_id, "m.room.member", alice.as_str())
            .await
            .unwrap()
            .expect("alice member row");
        assert_eq!(member.event_id, event.event_id);

        // Both head-sets advanced to the join.
        let (timeline, state) = store
            .forward_extremities(&room_id)
            .await
            .unwrap()
            .expect("room exists");
        assert_eq!(timeline, [event.event_id.clone()].into_iter().collect());
        assert_eq!(state, [event.event_id.clone()].into_iter().collect());
    }

    #[tokio::test]
    async fn persists_message_without_changing_current_state() {
        let (registry, store, room_id, alice) = setup().await;
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("join");

        let state_before = store.current_room_state(&room_id).await.unwrap();

        let event = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.message".to_owned(),
                None,
                json!({ "msgtype": "m.text", "body": "hi" }),
            )
            .await
            .expect("message accepted");

        // current_state unchanged by a message.
        let state_after = store.current_room_state(&room_id).await.unwrap();
        assert_eq!(state_before.len(), state_after.len());
        assert!(!state_after.values().any(|e| e.event_id == event.event_id));

        // Timeline head advanced to the message; state head still the join.
        let (timeline, _state) = store
            .forward_extremities(&room_id)
            .await
            .unwrap()
            .expect("room exists");
        assert_eq!(timeline, [event.event_id.clone()].into_iter().collect());
    }

    #[tokio::test]
    async fn rejects_unauthorized_event_and_leaves_state_untouched() {
        let (registry, store, room_id, _alice) = setup().await;
        let (timeline_before, _) = store.forward_extremities(&room_id).await.unwrap().unwrap();

        // bob has never joined — a topic from him fails the auth rules.
        let bob: OwnedUserId = BOB.parse().unwrap();
        let err = registry
            .send_event(
                &room_id,
                bob,
                "m.room.topic".to_owned(),
                Some(String::new()),
                json!({ "topic": "nope" }),
            )
            .await
            .expect_err("unauthorized topic rejected");
        assert!(matches!(err, RoomActorError::Rejected), "got {err:?}");

        // Nothing persisted: heads unchanged.
        let (timeline_after, _) = store.forward_extremities(&room_id).await.unwrap().unwrap();
        assert_eq!(timeline_before, timeline_after);
    }

    #[tokio::test]
    async fn public_room_allows_join_without_invite() {
        // Demonstrates the `public` join rule: a user who was never invited can
        // join. alice (creator) joins and opens the room to public; bob — never
        // invited — then joins successfully. This can't be driven over HTTP
        // (the CSAPI write path hardwires the sender to the single embedded
        // user), so it goes through the actor, which takes an explicit sender.
        let (registry, store, room_id, alice) = setup().await;

        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join");
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.join_rules".to_owned(),
                Some(String::new()),
                json!({ "join_rule": "public" }),
            )
            .await
            .expect("alice opens the room to public");

        let bob: OwnedUserId = BOB.parse().unwrap();
        let join = registry
            .send_event(
                &room_id,
                bob.clone(),
                "m.room.member".to_owned(),
                Some(bob.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("bob joins a public room without an invite");
        assert!(!join.soft_failed);

        // bob's join is now the current membership for him.
        let member = store
            .current_state_event(&room_id, "m.room.member", bob.as_str())
            .await
            .unwrap()
            .expect("bob membership in state");
        assert_eq!(member.event_id, join.event_id);
    }

    #[tokio::test]
    async fn invite_only_room_rejects_join_without_invite() {
        // Contrast to the public case: with no `m.room.join_rules` the default
        // is `invite`, so an uninvited bob is rejected — proving the public
        // join above isn't being accepted for some unrelated reason.
        let (registry, _store, room_id, alice) = setup().await;
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join");

        let bob: OwnedUserId = BOB.parse().unwrap();
        let err = registry
            .send_event(
                &room_id,
                bob.clone(),
                "m.room.member".to_owned(),
                Some(bob.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect_err("uninvited bob rejected in an invite-only room");
        assert!(matches!(err, RoomActorError::Rejected), "got {err:?}");
    }

    // ----- apply_pdu (federation receive) -----

    /// Build a room with just the create event and return the registry, store,
    /// room id, creator id, and the create event's id — enough to author a
    /// federation PDU that references the create as its parent.
    async fn setup_with_create_id() -> (
        RoomRegistry,
        Arc<SqliteStore>,
        OwnedRoomId,
        OwnedUserId,
        ruma::OwnedEventId,
    ) {
        let store = Arc::new(SqliteStore::open_in_memory().await.expect("open store"));
        let alice: OwnedUserId = ALICE.parse().expect("alice");
        let create = EventBuilder::new(alice.clone(), "m.room.create".to_owned())
            .state_key(String::new())
            .content(json!({ "room_version": ROOM_VERSION_ID }))
            .build()
            .expect("build create");
        let room_id = create.room_id.clone();
        let create_id = create.event_id.clone();
        store.create_room(&create, &[]).await.expect("create_room");
        let registry = RoomRegistry::new(store.clone(), "example.org".to_owned());
        (registry, store, room_id, alice, create_id)
    }

    #[tokio::test]
    async fn apply_pdu_accepts_state_event_and_persists() {
        // alice's self-join arrives as a fully-formed federation PDU (no
        // auth_events on the wire — apply_pdu computes them).
        let (registry, store, room_id, alice, create_id) = setup_with_create_id().await;

        let join = EventBuilder::new(alice.clone(), "m.room.member".to_owned())
            .room_id(room_id.clone())
            .state_key(alice.to_string())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![create_id.clone()])
            .prev_state_events(vec![create_id])
            .build()
            .expect("build join pdu");
        let join_id = join.event_id.clone();

        registry
            .apply_pdu(&room_id, join)
            .await
            .expect("pdu accepted");

        let member = store
            .current_state_event(&room_id, "m.room.member", alice.as_str())
            .await
            .unwrap()
            .expect("alice member row");
        assert_eq!(member.event_id, join_id);
        let (timeline, state) = store.forward_extremities(&room_id).await.unwrap().unwrap();
        assert_eq!(timeline, [join_id.clone()].into_iter().collect());
        assert_eq!(state, [join_id].into_iter().collect());
    }

    #[tokio::test]
    async fn apply_pdu_persists_rejected_event_without_advancing_heads() {
        // alice joins (locally); then bob — never invited — sends a join PDU
        // over federation. It fails auth in an invite-only room → REJECT.
        // Federation policy: the rejected event is persisted, but it does not
        // advance the heads or enter current_state.
        let (registry, store, room_id, alice, _create_id) = setup_with_create_id().await;
        let alice_join = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join");

        let bob: OwnedUserId = BOB.parse().unwrap();
        let bob_join = EventBuilder::new(bob.clone(), "m.room.member".to_owned())
            .room_id(room_id.clone())
            .state_key(bob.to_string())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![alice_join.event_id.clone()])
            .prev_state_events(vec![alice_join.event_id.clone()])
            .build()
            .expect("build bob join pdu");
        let bob_join_id = bob_join.event_id.clone();

        let (timeline_before, state_before) =
            store.forward_extremities(&room_id).await.unwrap().unwrap();

        // Federation accepts the PDU into the store even though it's rejected.
        registry
            .apply_pdu(&room_id, bob_join)
            .await
            .expect("federation persists a reject (Ok, not Err)");

        // Heads unchanged; bob absent from current_state.
        let (timeline_after, state_after) =
            store.forward_extremities(&room_id).await.unwrap().unwrap();
        assert_eq!(timeline_before, timeline_after);
        assert_eq!(state_before, state_after);
        assert!(
            store
                .current_state_event(&room_id, "m.room.member", bob.as_str())
                .await
                .unwrap()
                .is_none()
        );
        // But the event itself is persisted, marked rejected.
        let fetched = store.get_events(&[&bob_join_id]).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert!(fetched[0].rejected);
    }

    // ----- outbound federation destinations -----

    /// alice (local) creates + joins + opens the room to `public`, then a
    /// remote user `@zara:remote.example` joins via a federation PDU. Returns
    /// everything needed to drive further sends. After this, `remote.example`
    /// has a *joined* member, so subsequent local events must federate to it.
    async fn setup_with_remote_member() -> (RoomRegistry, Arc<SqliteStore>, OwnedRoomId, OwnedUserId)
    {
        let (registry, store, room_id, alice) = setup().await;
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join");
        let rules = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.join_rules".to_owned(),
                Some(String::new()),
                json!({ "join_rule": "public" }),
            )
            .await
            .expect("alice opens public");

        let zara: OwnedUserId = ZARA.parse().unwrap();
        let zara_join = EventBuilder::new(zara.clone(), "m.room.member".to_owned())
            .room_id(room_id.clone())
            .state_key(zara.to_string())
            .content(json!({ "membership": "join" }))
            .prev_events(vec![rules.event_id.clone()])
            .prev_state_events(vec![rules.event_id.clone()])
            .build()
            .expect("build zara join pdu");
        let zara_join_id = zara_join.event_id.clone();
        registry
            .apply_pdu(&room_id, zara_join)
            .await
            .expect("zara joins via federation");

        // Precondition the rest of the suite leans on: zara is *actually*
        // joined in current_state. `apply_pdu` returns `Ok` even for a rejected
        // PDU (federation persists rejects), so the `expect` above is not
        // enough — assert the join landed in state, not just that it applied.
        let zara_member = store
            .current_state_event(&room_id, "m.room.member", zara.as_str())
            .await
            .unwrap()
            .expect("zara is a current member");
        assert_eq!(zara_member.event_id, zara_join_id, "zara's join must win");

        // A federation-received PDU is never re-originated: it writes no outbox.
        assert!(
            store
                .pending_pdus(server_name!("remote.example"))
                .await
                .unwrap()
                .is_empty(),
            "apply_pdu must not enqueue outbox rows"
        );
        (registry, store, room_id, alice)
    }

    #[tokio::test]
    async fn local_send_federates_to_remote_joined_member_not_self() {
        let (registry, store, room_id, alice) = setup_with_remote_member().await;

        let msg = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.message".to_owned(),
                None,
                json!({ "msgtype": "m.text", "body": "hi" }),
            )
            .await
            .expect("alice message");

        // The remote server with a joined member gets the message.
        let pending = store
            .pending_pdus(server_name!("remote.example"))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event_id, msg.event_id);

        // Our own server is never an outbound destination.
        assert!(
            store
                .pending_pdus(server_name!("example.org"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn kick_of_remote_member_still_federates_to_their_server() {
        // Post-apply, zara is no longer joined — so the "+target server" clause
        // is the only reason remote.example is still notified of its departure.
        let (registry, store, room_id, alice) = setup_with_remote_member().await;
        let zara: OwnedUserId = ZARA.parse().unwrap();

        let kick = registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(zara.to_string()),
                json!({ "membership": "leave" }),
            )
            .await
            .expect("alice kicks zara");

        let pending = store
            .pending_pdus(server_name!("remote.example"))
            .await
            .unwrap();
        assert!(
            pending.iter().any(|e| e.event_id == kick.event_id),
            "departing server must receive the kick"
        );
    }

    #[tokio::test]
    async fn invite_of_remote_user_is_not_federated_via_send() {
        // The "+target server" clause fires only for departing memberships
        // (leave/ban). An invite is delivered via the dedicated `/invite`
        // handshake, not transaction broadcast, so inviting a remote user must
        // NOT enqueue an outbox row for their server.
        let (registry, store, room_id, alice) = setup().await;
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(alice.to_string()),
                json!({ "membership": "join" }),
            )
            .await
            .expect("alice join");

        let zara: OwnedUserId = ZARA.parse().unwrap();
        registry
            .send_event(
                &room_id,
                alice.clone(),
                "m.room.member".to_owned(),
                Some(zara.to_string()),
                json!({ "membership": "invite" }),
            )
            .await
            .expect("alice invites zara");

        assert!(
            store
                .pending_pdus(server_name!("remote.example"))
                .await
                .unwrap()
                .is_empty(),
            "an invite must not federate over /send"
        );
    }
}
