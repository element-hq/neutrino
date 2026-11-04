//! Group tags: which locally created events share their first `/send` between
//! destinations.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, MutexGuard};

use neutrino_event::now_ms;
use ruma::{EventId, OwnedEventId, OwnedServerName, ServerName};

use crate::util::TxnIdGen;

/// Locally created events whose first `/send` to each listed destination
/// shares one transmission.
pub struct GroupTags {
    enabled: bool,
    idgen: TxnIdGen,
    tags: Mutex<HashMap<OwnedEventId, Tag>>,
}

struct Tag {
    txn_id: String,
    size: usize,
    dests: BTreeSet<OwnedServerName>,
}

/// The group a destination's first `/send` of a tagged event belongs to.
pub struct GroupSend {
    pub txn_id: String,
    pub size: usize,
}

impl GroupTags {
    /// `enabled`: whether the transport can share a transmission between
    /// destinations. Disabled, nothing is ever tagged.
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            // The process-startup prefix keeps txn ids unique across restarts.
            idgen: TxnIdGen::new(now_ms()),
            tags: Mutex::new(HashMap::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn next_txn_id(&self) -> String {
        self.idgen.next_id()
    }

    /// Group `event`'s first send to each of `dests` under one fresh `txn_id`.
    /// A group needs at least two destinations.
    pub(crate) fn tag(&self, event: &EventId, dests: BTreeSet<OwnedServerName>) {
        if !self.enabled || dests.len() < 2 {
            return;
        }
        let tag = Tag {
            txn_id: self.next_txn_id(),
            size: dests.len(),
            dests,
        };
        self.lock().insert(event.to_owned(), tag);
    }

    pub(crate) fn untag(&self, event: &EventId) {
        self.lock().remove(event);
    }

    pub(crate) fn tagged_for(&self, event: &EventId, dest: &ServerName) -> bool {
        self.lock()
            .get(event)
            .is_some_and(|tag| tag.dests.contains(dest))
    }

    /// Remove `dest` from `event`'s group and return the group. The tag is
    /// dropped with its last destination.
    pub(crate) fn take(&self, event: &EventId, dest: &ServerName) -> Option<GroupSend> {
        let mut tags = self.lock();
        let tag = tags.get_mut(event)?;
        if !tag.dests.remove(dest) {
            return None;
        }
        let send = GroupSend {
            txn_id: tag.txn_id.clone(),
            size: tag.size,
        };
        if tag.dests.is_empty() {
            tags.remove(event);
        }
        Some(send)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<OwnedEventId, Tag>> {
        self.tags.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruma::{event_id, server_name};

    fn dests(names: &[&ServerName]) -> BTreeSet<OwnedServerName> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn a_tag_needs_grouping_enabled_and_two_destinations() {
        let ev = event_id!("$ev");
        let (a, b) = (server_name!("a.test"), server_name!("b.test"));

        let enabled = GroupTags::new(true);
        enabled.tag(ev, dests(&[a]));
        assert!(!enabled.tagged_for(ev, a), "one destination is no group");
        enabled.tag(ev, dests(&[a, b]));
        assert!(enabled.tagged_for(ev, a) && enabled.tagged_for(ev, b));

        let disabled = GroupTags::new(false);
        disabled.tag(ev, dests(&[a, b]));
        assert!(!disabled.tagged_for(ev, a) && !disabled.tagged_for(ev, b));
    }

    #[test]
    fn take_returns_the_group_and_drops_the_tag_after_its_last_destination() {
        let ev = event_id!("$ev");
        let (a, b, c) = (
            server_name!("a.test"),
            server_name!("b.test"),
            server_name!("c.test"),
        );
        let tags = GroupTags::new(true);
        tags.tag(ev, dests(&[a, b, c]));

        let first = tags.take(ev, a).expect("a is in the group");
        assert_eq!(first.size, 3);
        assert!(!tags.tagged_for(ev, a), "a took its send");
        assert!(tags.take(ev, a).is_none(), "a takes it once");

        let second = tags.take(ev, b).expect("b is in the group");
        assert_eq!(
            (second.txn_id.as_str(), second.size),
            (first.txn_id.as_str(), 3)
        );
        let last = tags.take(ev, c).expect("c is in the group");
        assert_eq!(
            (last.txn_id.as_str(), last.size),
            (first.txn_id.as_str(), 3)
        );
        assert!(
            tags.lock().is_empty(),
            "the tag went with its last destination"
        );
    }

    #[test]
    fn untag_removes_a_tag() {
        let ev = event_id!("$ev");
        let (a, b) = (server_name!("a.test"), server_name!("b.test"));
        let tags = GroupTags::new(true);
        tags.tag(ev, dests(&[a, b]));
        tags.untag(ev);
        assert!(!tags.tagged_for(ev, a) && !tags.tagged_for(ev, b));
        assert!(tags.take(ev, a).is_none());
    }
}
