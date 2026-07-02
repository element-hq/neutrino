//! Out-of-band peer discovery registry.
//!
//! The embedded homeserver has no Matrix-level user directory: it learns of
//! other servers over a side channel (the BLE mesh), where each device
//! advertises a display name alongside its `server_name` (for the embedded
//! server, its node id). Those advertisements land here, and the CSAPI
//! user-directory search reads them back to answer "who can I invite?".
//!
//! Deliberately dependency-free and format-agnostic: it stores whatever
//! `localpart` the caller supplies (the embedded host uses a fixed constant,
//! but that is the host's choice, not an assumption baked in here) and never
//! itself constructs a user id — id formatting is the reader's concern.

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A peer discovered out of band — the value half of [`DiscoveryRegistry`],
/// keyed there by the peer's `server_name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    /// Localpart of the peer's user. Stored verbatim — the registry makes no
    /// assumption about it (the embedded host uses a fixed constant, but the
    /// registry is agnostic, so a future multi-user host needs no change here).
    pub localpart: String,
    /// Human-readable display name the peer advertised.
    pub display_name: String,
    /// Wall-clock milliseconds when the peer was last seen, stamped by the
    /// caller (the registry has no clock — it depends on nothing). Carried
    /// through to readers for a "last seen" affordance; not interpreted here.
    pub last_seen_ms: u64,
}

/// Thread-safe set of currently-known discovered peers, keyed by `server_name`.
///
/// Share via an `Arc` between the writer (the host's discovery callback, over
/// the FFI) and the reader (the user-directory handler). The write model is
/// snapshot replacement — each scan supplies the full set of visible peers, so
/// peers that drop out of range simply stop appearing in the next snapshot, no
/// separate removal bookkeeping required.
#[derive(Debug, Default)]
pub struct DiscoveryRegistry {
    peers: RwLock<HashMap<String, DiscoveredPeer>>,
}

impl DiscoveryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the entire known-peer set with a fresh snapshot, keyed by
    /// `server_name`. This is the primary write path (one call per scan).
    pub fn replace(&self, peers: HashMap<String, DiscoveredPeer>) {
        *self.write() = peers;
    }

    /// Insert or update a single peer. Complements [`replace`](Self::replace)
    /// for callers that observe peers incrementally rather than as a snapshot.
    pub fn upsert(&self, server_name: String, peer: DiscoveredPeer) {
        self.write().insert(server_name, peer);
    }

    /// The peer currently known at `server_name`, if any. Used by `/profile`
    /// to resolve a discovered peer's display name.
    pub fn get(&self, server_name: &str) -> Option<DiscoveredPeer> {
        self.read().get(server_name).cloned()
    }

    /// Peers whose display name contains `term` (case-insensitive), returned as
    /// `(server_name, peer)` pairs sorted by `(display_name, server_name)` so
    /// results are deterministic. An empty `term` matches every peer. The
    /// caller applies any result cap (and decides whether the cap was hit).
    pub fn search(&self, term: &str) -> Vec<(String, DiscoveredPeer)> {
        let needle = term.to_lowercase();
        let mut hits: Vec<(String, DiscoveredPeer)> = self
            .read()
            .iter()
            .filter(|(_, p)| p.display_name.to_lowercase().contains(&needle))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        hits.sort_by(|(a_name, a), (b_name, b)| {
            a.display_name
                .cmp(&b.display_name)
                .then_with(|| a_name.cmp(b_name))
        });
        hits
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<String, DiscoveredPeer>> {
        self.peers.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<String, DiscoveredPeer>> {
        self.peers.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(localpart: &str, display_name: &str) -> DiscoveredPeer {
        DiscoveredPeer {
            localpart: localpart.to_string(),
            display_name: display_name.to_string(),
            last_seen_ms: 0,
        }
    }

    fn seeded() -> DiscoveryRegistry {
        let reg = DiscoveryRegistry::new();
        reg.upsert("node_alice".to_string(), peer("n", "Alice"));
        reg.upsert("node_bob".to_string(), peer("n", "Bob"));
        reg.upsert("node_alex".to_string(), peer("n", "Alexandra"));
        reg
    }

    #[test]
    fn search_is_case_insensitive_substring() {
        let reg = seeded();
        let hits = reg.search("al");
        // "Alice" and "Alexandra" match "al"; "Bob" does not.
        let names: Vec<&str> = hits.iter().map(|(_, p)| p.display_name.as_str()).collect();
        assert_eq!(names, vec!["Alexandra", "Alice"]); // sorted by display_name
    }

    #[test]
    fn search_returns_server_name_key() {
        let reg = seeded();
        let hits = reg.search("bob");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "node_bob");
        assert_eq!(hits[0].1.localpart, "n");
    }

    #[test]
    fn empty_term_matches_all() {
        let reg = seeded();
        assert_eq!(reg.search("").len(), 3);
    }

    #[test]
    fn no_match_returns_empty() {
        let reg = seeded();
        assert!(reg.search("zzz").is_empty());
    }

    #[test]
    fn replace_drops_stale_peers() {
        let reg = seeded();
        let mut snapshot = HashMap::new();
        snapshot.insert("node_carol".to_string(), peer("n", "Carol"));
        reg.replace(snapshot);
        // Alice/Bob/Alex are gone; only the new snapshot remains.
        assert!(reg.search("alice").is_empty());
        let all = reg.search("");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "node_carol");
    }

    #[test]
    fn upsert_overwrites_same_server_name() {
        let reg = DiscoveryRegistry::new();
        reg.upsert("node_x".to_string(), peer("n", "Old"));
        reg.upsert("node_x".to_string(), peer("n", "New"));
        let all = reg.search("");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.display_name, "New");
    }
}
