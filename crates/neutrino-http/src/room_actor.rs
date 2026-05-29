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
//!   provider is connection-bound and `pub(crate)`, so the actor reaches it
//!   via [`SqliteStore::apply_event`] (the thin bridge that runs apply on a
//!   reader connection).
//! - Persisting the accepted event + new head-sets is the one short write
//!   transaction — `EventStore::persist_resolved_event`.
//!
//! The actor applies to a **clone** of its `RoomCore` and adopts it only
//! after the persist commits, so an apply hard-reject or a storage fault
//! leaves the in-memory state untouched.
//!
//! Scope (PLAN 6d PR3): the actor + registry, driven directly. Wiring the
//! CSAPI handlers through it is a later step; until then nothing constructs a
//! [`RoomRegistry`] in the live router, so the module is exercised only by
//! its own tests.

// Until the CSAPI handlers are wired through the registry (PLAN 6d PR4) the
// public surface here has no non-test caller. Allow dead_code module-wide
// rather than peppering each item; remove when the router constructs a
// `RoomRegistry`.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use neutrino_common::Event;
use neutrino_state::auth_events::calculate_auth_events;
use neutrino_state::event_id::EventBuilder;
use neutrino_state::room_core::{Effect, RoomCore};
use neutrino_state::{CoreError, FormatError, StateDelta, StateMap};
use neutrino_store::{EventStore, RoomStore, StateStore, StorageError};
use neutrino_store_sqlite::SqliteStore;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, RoomId};
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
}

/// The owned state machine for one room. Lives inside a spawned task; the
/// only handle to it is the mpsc `Sender` held in the [`RoomRegistry`].
struct RoomActor {
    room: RoomCore,
    store: Arc<SqliteStore>,
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
            }
        }
    }

    /// Build a locally-originated event against the room's current heads —
    /// all in memory. `prev_events`/`prev_state_events` come from the two
    /// head-sets; `auth_events` are calculated server-side from
    /// state-before-event, which for an event sitting on the current heads is
    /// `current_state`.
    fn build_event(
        &self,
        sender: OwnedUserId,
        event_type: String,
        state_key: Option<String>,
        content: Value,
    ) -> Result<Event, FormatError> {
        let prev_events: Vec<OwnedEventId> =
            self.room.forward_extremities().iter().cloned().collect();
        let prev_state_events: Vec<OwnedEventId> = self
            .room
            .state_forward_extremities()
            .iter()
            .cloned()
            .collect();
        let mut builder = EventBuilder::new(sender, event_type)
            .room_id(self.room.room_id().to_owned())
            .content(content)
            .prev_events(prev_events)
            .prev_state_events(prev_state_events);
        if let Some(sk) = state_key {
            builder = builder.state_key(sk);
        }
        let mut event = builder.build()?;
        let current_ids: StateMap<OwnedEventId> = self
            .room
            .current_state()
            .iter()
            .map(|(key, ev)| (key.clone(), ev.event_id.clone()))
            .collect();
        event.auth_events = calculate_auth_events(&event, &current_ids);
        Ok(event)
    }

    async fn handle_send(
        &mut self,
        sender: OwnedUserId,
        event_type: String,
        state_key: Option<String>,
        content: Value,
    ) -> Result<Arc<Event>, RoomActorError> {
        let event = self.build_event(sender, event_type, state_key, content)?;

        // Apply against a clone — adopt it only once the persist commits.
        let (next, verdict) = self.store.apply_event(self.room.clone(), event).await?;
        let effects = verdict?;

        // Collect the event to persist and the current-state delta. An
        // accepted event emits exactly one `Persist`; a state event
        // additionally emits `UpdateCurrentState` (a non-state event leaves
        // the delta empty). The returned event carries `soft_failed` itself.
        let mut persisted: Option<Arc<Event>> = None;
        let mut delta = StateDelta::new();
        for effect in effects {
            match effect {
                Effect::Persist { event } => persisted = Some(event),
                Effect::UpdateCurrentState(d) => delta = d,
            }
        }
        let event = persisted.ok_or(RoomActorError::NotApplied)?;

        self.store
            .persist_resolved_event(
                &event,
                next.forward_extremities(),
                next.state_forward_extremities(),
                &delta,
            )
            .await?;
        // Commit succeeded — adopt the post-apply state.
        self.room = next;

        Ok(event)
    }
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
    actors: Mutex<HashMap<OwnedRoomId, mpsc::Sender<Command>>>,
}

impl RoomRegistry {
    pub fn new(store: Arc<SqliteStore>) -> Self {
        Self {
            store,
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
    use neutrino_store::RoomStore;
    use serde_json::json;

    const ALICE: &str = "@alice:example.org";
    const BOB: &str = "@bob:example.org";

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
        let registry = RoomRegistry::new(store.clone());
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
        assert!(matches!(err, RoomActorError::Apply(_)), "got {err:?}");

        // Nothing persisted: heads unchanged.
        let (timeline_after, _) = store.forward_extremities(&room_id).await.unwrap().unwrap();
        assert_eq!(timeline_before, timeline_after);
    }
}
