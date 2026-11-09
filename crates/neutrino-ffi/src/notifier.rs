//! Tells the host about events that may deserve a notification for the local
//! user, so a host without push can raise them anyway.
//!
//! Push rules are not evaluated here: the host's client applies them when it
//! resolves the event, and drops whatever should not notify.
//!
//! An invite is reported when it becomes the user's membership, whether it
//! arrived in a room or out of band.
//!
//! A remote join applies the room's recent timeline as live events, so it is
//! reported too. The host opens the room as the join completes, which
//! suppresses it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use neutrino_main::ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, UserId};
use neutrino_main::{Event, EventStore, Membership, OobMembershipStore, StorageError, StreamPos};
use tokio::sync::watch;

/// Receives events that may deserve a notification. Called on the server's
/// only runtime thread, so an implementation must return at once.
#[uniffi::export(with_foreign)]
pub trait NotificationListener: Send + Sync {
    fn on_notifiable_event(&self, room_id: String, event_id: String);
}

pub(crate) type ListenerSlot = Option<Arc<dyn NotificationListener>>;

/// Rows per `events_after` read.
const PAGE: usize = 100;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Notifiable {
    pub room_id: OwnedRoomId,
    pub event_id: OwnedEventId,
}

pub(crate) struct Notifier<S> {
    store: Arc<S>,
    user_id: OwnedUserId,
    cursor: StreamPos,
    /// Invites already reported, or held when the notifier started.
    seen_invites: HashSet<OwnedEventId>,
}

struct Memberships {
    joined: HashSet<OwnedRoomId>,
    /// The event behind each invite the user holds.
    invites: HashMap<OwnedEventId, OwnedRoomId>,
}

impl<S: EventStore + OobMembershipStore> Notifier<S> {
    /// Reports only what arrives after `cursor`, and no invite held now.
    pub(crate) async fn new(
        store: Arc<S>,
        user_id: OwnedUserId,
        cursor: StreamPos,
    ) -> Result<Self, StorageError> {
        let mut notifier = Self {
            store,
            user_id,
            cursor,
            seen_invites: HashSet::new(),
        };
        notifier.seen_invites = notifier.memberships().await?.invites.into_keys().collect();
        Ok(notifier)
    }

    /// Everything that arrived since the last call, in stream order, with new
    /// invites last.
    pub(crate) async fn poll(&mut self) -> Result<Vec<Notifiable>, StorageError> {
        let mut out = Vec::new();
        let mut cursor = self.cursor;
        let Memberships { joined, invites } = self.memberships().await?;
        loop {
            let page = self.store.events_after(cursor, PAGE).await?;
            let full = page.len() == PAGE;
            for (pos, event) in page {
                cursor = pos;
                if self.notifies(&event, &joined) {
                    out.push(Notifiable {
                        room_id: event.room_id,
                        event_id: event.event_id,
                    });
                }
            }
            if !full {
                break;
            }
        }
        for (event_id, room_id) in &invites {
            if !self.seen_invites.contains(event_id) {
                out.push(Notifiable {
                    room_id: room_id.clone(),
                    event_id: event_id.clone(),
                });
            }
        }
        self.cursor = cursor;
        // A re-invite is a new event, so an invite no longer held cannot
        // return.
        self.seen_invites = invites.into_keys().collect();
        Ok(out)
    }

    fn notifies(&self, event: &Event, joined: &HashSet<OwnedRoomId>) -> bool {
        // A room the user has left keeps receiving events from members who
        // have not yet seen the leave.
        event.sender != self.user_id && event.state_key.is_none() && joined.contains(&event.room_id)
    }

    async fn memberships(&self) -> Result<Memberships, StorageError> {
        let rooms = self
            .store
            .resolved_memberships(
                &self.user_id,
                &[Membership::Join, Membership::Invite].into(),
            )
            .await?;
        let mut memberships = Memberships {
            joined: HashSet::new(),
            invites: HashMap::new(),
        };
        for (room_id, membership, _) in rooms {
            if membership == Membership::Join {
                memberships.joined.insert(room_id);
                continue;
            }
            // A separate read, so the invite may have been replaced since.
            let Some(member) = self
                .store
                .resolved_member_event(&room_id, &self.user_id)
                .await?
            else {
                continue;
            };
            if member.membership == Membership::Invite {
                memberships.invites.insert(member.event.event_id, room_id);
            }
        }
        Ok(memberships)
    }
}

/// Feeds `listener` until the store goes away. Waits for the server to resolve
/// its name, since the local user id depends on it, but counts only events
/// persisted after this call.
pub(crate) async fn run<S: EventStore + OobMembershipStore>(
    store: Arc<S>,
    localpart: String,
    mut identity: watch::Receiver<Option<String>>,
    listener: watch::Receiver<ListenerSlot>,
) {
    let mut stream = store.subscribe();
    let cursor = *stream.borrow_and_update();
    let server_name = match identity.wait_for(Option::is_some).await {
        Ok(name) => name.clone().unwrap_or_default(),
        Err(_) => return,
    };
    let user_id = match UserId::parse(format!("@{localpart}:{server_name}")) {
        Ok(user_id) => user_id,
        Err(e) => {
            tracing::error!(error = %e, "notifier: invalid local user id");
            return;
        }
    };
    let mut notifier = match Notifier::new(store, user_id, cursor).await {
        Ok(notifier) => notifier,
        Err(e) => {
            tracing::error!(error = %e, "notifier: failed to start");
            return;
        }
    };
    while stream.changed().await.is_ok() {
        let notifiable = match notifier.poll().await {
            Ok(notifiable) => notifiable,
            Err(e) => {
                // Nothing was consumed, so the next wake retries.
                tracing::warn!(error = %e, "notifier: failed to read new events");
                continue;
            }
        };
        let Some(listener) = listener.borrow().clone() else {
            continue;
        };
        for n in notifiable {
            listener.on_notifiable_event(n.room_id.to_string(), n.event_id.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use neutrino_main::ruma::{RoomId, room_id, user_id};
    use neutrino_main::{RoomStore, SqliteStore};
    use serde_json::{Value, json};

    use super::*;

    const ROOM_VERSION: &str = "org.matrix.msc4242.12";

    fn me() -> OwnedUserId {
        user_id!("@n:me").to_owned()
    }

    fn bob() -> OwnedUserId {
        user_id!("@bob:other").to_owned()
    }

    /// The store keys events by id and does not recompute it, so a fixture
    /// names its own.
    fn event(
        id: &str,
        room_id: &RoomId,
        event_type: &str,
        state_key: Option<&str>,
        sender: &UserId,
        content: Value,
    ) -> Event {
        let raw = json!({
            "room_id": room_id,
            "type": event_type,
            "state_key": state_key,
            "sender": sender,
            "origin_server_ts": 0,
            "content": content,
            "prev_events": [],
            "prev_state_events": [],
            "unsigned": {"invite_room_state": []},
        });
        Event {
            event_id: OwnedEventId::try_from(format!("${id}")).expect("valid event id"),
            room_id: room_id.to_owned(),
            sender: sender.to_owned(),
            event_type: event_type.to_owned(),
            state_key: state_key.map(str::to_owned),
            origin_server_ts: 0,
            content: serde_json::value::to_raw_value(&content).expect("content"),
            prev_events: Vec::new(),
            prev_state_events: Vec::new(),
            auth_events: Vec::new(),
            rejected: false,
            soft_failed: false,
            raw: serde_json::value::to_raw_value(&raw).expect("raw"),
        }
    }

    fn message(id: &str, room_id: &RoomId, sender: &UserId) -> Event {
        event(
            id,
            room_id,
            "m.room.message",
            None,
            sender,
            json!({"msgtype": "m.text", "body": id}),
        )
    }

    fn invite(id: &str, room_id: &RoomId, sender: &UserId, target: &UserId) -> Event {
        let target = target.as_str();
        event(
            id,
            room_id,
            "m.room.member",
            Some(target),
            sender,
            json!({"membership": "invite"}),
        )
    }

    /// A store holding one room the local user created and joined.
    async fn store_with_room(room_id: &RoomId) -> Arc<SqliteStore> {
        let store = SqliteStore::open_in_memory().await.expect("open store");
        let me = me();
        let create = event(
            "create",
            room_id,
            "m.room.create",
            Some(""),
            &me,
            json!({"room_version": ROOM_VERSION}),
        );
        let join = event(
            "join",
            room_id,
            "m.room.member",
            Some(me.as_str()),
            &me,
            json!({"membership": "join"}),
        );
        store
            .create_room(&create, &[join])
            .await
            .expect("create room");
        Arc::new(store)
    }

    async fn persist(store: &SqliteStore, event: &Event) {
        store.persist_event(event, &[]).await.expect("persist");
    }

    async fn notifier(store: &Arc<SqliteStore>) -> Notifier<SqliteStore> {
        let head = *store.subscribe().borrow();
        Notifier::new(store.clone(), me(), head)
            .await
            .expect("notifier")
    }

    fn ids(notifiable: &[Notifiable]) -> Vec<&str> {
        notifiable.iter().map(|n| n.event_id.as_str()).collect()
    }

    #[tokio::test]
    async fn remote_message_is_reported_once() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let mut notifier = notifier(&store).await;

        persist(&store, &message("hi", room, &bob())).await;

        let got = notifier.poll().await.expect("poll");
        assert_eq!(
            got,
            vec![Notifiable {
                room_id: room.to_owned(),
                event_id: OwnedEventId::try_from("$hi").expect("id"),
            }]
        );
        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    #[tokio::test]
    async fn events_before_start_are_not_reported() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        persist(&store, &message("old", room, &bob())).await;

        let mut notifier = notifier(&store).await;

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    #[tokio::test]
    async fn own_messages_and_state_are_not_reported() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let mut notifier = notifier(&store).await;

        persist(&store, &message("mine", room, &me())).await;
        let name = event(
            "name",
            room,
            "m.room.name",
            Some(""),
            &bob(),
            json!({"name": "x"}),
        );
        persist(&store, &name).await;
        let carol = user_id!("@carol:other");
        persist(&store, &invite("carol", room, &bob(), carol)).await;

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    #[tokio::test]
    async fn messages_after_leaving_are_not_reported() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let mut notifier = notifier(&store).await;
        let me = me();

        let leave = event(
            "leave",
            room,
            "m.room.member",
            Some(me.as_str()),
            &me,
            json!({"membership": "leave"}),
        );
        persist(&store, &leave).await;
        persist(&store, &message("late", room, &bob())).await;

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    fn member(id: &str, room_id: &RoomId, sender: &UserId, membership: &str) -> Event {
        let me = me();
        event(
            id,
            room_id,
            "m.room.member",
            Some(me.as_str()),
            sender,
            json!({"membership": membership}),
        )
    }

    #[tokio::test]
    async fn in_room_invite_is_reported_once_and_again_after_a_rescind() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let mut notifier = notifier(&store).await;
        persist(&store, &member("leave", room, &me(), "leave")).await;

        persist(&store, &invite("inv1", room, &bob(), &me())).await;
        assert_eq!(ids(&notifier.poll().await.expect("poll")), ["$inv1"]);
        assert!(notifier.poll().await.expect("poll").is_empty());

        persist(&store, &member("rescind", room, &bob(), "leave")).await;
        assert!(notifier.poll().await.expect("poll").is_empty());

        persist(&store, &invite("inv2", room, &bob(), &me())).await;
        assert_eq!(ids(&notifier.poll().await.expect("poll")), ["$inv2"]);
    }

    /// Joining a remote room applies the invite it accepts as a room event.
    #[tokio::test]
    async fn accepted_invite_is_not_reported_again() {
        let store = store_with_room(room_id!("!room:me")).await;
        let remote = room_id!("!remote:other");
        let inv = invite("inv", remote, &bob(), &me());
        let mut notifier = notifier(&store).await;

        store
            .put_oob_membership(remote, &me(), &inv, ROOM_VERSION, None)
            .await
            .expect("oob write");
        assert_eq!(ids(&notifier.poll().await.expect("poll")), ["$inv"]);

        let create = event(
            "remote-create",
            remote,
            "m.room.create",
            Some(""),
            &bob(),
            json!({"room_version": ROOM_VERSION}),
        );
        let bob_join = event(
            "bob-join",
            remote,
            "m.room.member",
            Some(bob().as_str()),
            &bob(),
            json!({"membership": "join"}),
        );
        store
            .create_room(&create, &[bob_join])
            .await
            .expect("create room");
        persist(&store, &inv).await;
        persist(&store, &member("my-join", remote, &me(), "join")).await;

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    #[tokio::test]
    async fn backfilled_events_are_not_reported() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let mut notifier = notifier(&store).await;

        store
            .persist_historical_event(&message("past", room, &bob()))
            .await
            .expect("backfill");

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    #[tokio::test]
    async fn oob_invite_is_reported_once_and_again_after_a_reinvite() {
        let store = store_with_room(room_id!("!room:me")).await;
        let remote = room_id!("!remote:other");
        let me = me();
        let oob = |event: Event| {
            let store = store.clone();
            let me = me.clone();
            async move {
                store
                    .put_oob_membership(remote, &me, &event, ROOM_VERSION, None)
                    .await
                    .expect("oob write");
            }
        };
        let mut notifier = notifier(&store).await;

        oob(invite("inv1", remote, &bob(), &me)).await;
        assert_eq!(ids(&notifier.poll().await.expect("poll")), ["$inv1"]);
        assert!(notifier.poll().await.expect("poll").is_empty());

        let leave = event(
            "leave",
            remote,
            "m.room.member",
            Some(me.as_str()),
            &me,
            json!({"membership": "leave"}),
        );
        oob(leave).await;
        assert!(notifier.poll().await.expect("poll").is_empty());

        oob(invite("inv2", remote, &bob(), &me)).await;
        assert_eq!(ids(&notifier.poll().await.expect("poll")), ["$inv2"]);
    }

    #[tokio::test]
    async fn oob_invite_held_at_start_is_not_reported() {
        let store = store_with_room(room_id!("!room:me")).await;
        let remote = room_id!("!remote:other");
        store
            .put_oob_membership(
                remote,
                &me(),
                &invite("inv", remote, &bob(), &me()),
                ROOM_VERSION,
                None,
            )
            .await
            .expect("oob write");

        let mut notifier = notifier(&store).await;

        assert!(notifier.poll().await.expect("poll").is_empty());
    }

    struct Recorder(Mutex<Vec<String>>, tokio::sync::Notify);

    impl NotificationListener for Recorder {
        fn on_notifiable_event(&self, _room_id: String, event_id: String) {
            self.0.lock().expect("lock").push(event_id);
            self.1.notify_one();
        }
    }

    #[tokio::test]
    async fn run_reports_to_the_listener() {
        let room = room_id!("!room:me");
        let store = store_with_room(room).await;
        let recorder = Arc::new(Recorder(Mutex::new(Vec::new()), tokio::sync::Notify::new()));
        let slot: ListenerSlot = Some(recorder.clone());
        let (_listener_tx, listener_rx) = watch::channel(slot);
        let (identity_tx, identity_rx) = watch::channel(None);
        tokio::spawn(run(store.clone(), "n".to_owned(), identity_rx, listener_rx));
        // `run` subscribes, then parks until the server has a name. Persisting
        // meanwhile keeps the in-memory store free of concurrent access, and
        // shows the cursor dates from the subscription, not the name.
        tokio::task::yield_now().await;
        persist(&store, &message("mine", room, &me())).await;
        persist(&store, &message("hi", room, &bob())).await;
        identity_tx.send_replace(Some("me".to_owned()));

        tokio::time::timeout(Duration::from_secs(5), recorder.1.notified())
            .await
            .expect("listener called");
        assert_eq!(*recorder.0.lock().expect("lock"), ["$hi"]);
    }
}
