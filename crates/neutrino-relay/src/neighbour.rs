use std::net::Ipv6Addr;
use std::num::NonZeroUsize;
use std::sync::{Mutex, PoisonError};

use lru::LruCache;

use crate::NodeKey;
use crate::vip::vip;

/// Upper bound on learned routes. Generous for an embedded mesh, but bounds
/// memory against a peer churning fresh identities (each adds one vip→node
/// entry): the least-recently-routed peer is evicted when full. A black-holed
/// evicted route self-heals — the next packet to it re-registers via the
/// resolver/sender path or inbound learning.
const MAX_ROUTES: NonZeroUsize = match NonZeroUsize::new(4096) {
    Some(n) => n,
    None => panic!("MAX_ROUTES must be non-zero"),
};

/// Reverse route table: virtual IP → node key.
///
/// [`vip`] is one-way, so egress — which sees only a destination vip in an
/// outbound packet — needs this map to recover the node to send the datagram
/// to. It is populated two ways, both funnelled through [`register`](Self::register):
/// the resolver path (something already holds a peer's node key, e.g. an invite
/// that carries it) and inbound learning (the source node of a datagram the
/// transport has cryptographically authenticated). There is no behavioural
/// difference between the two — only the call site differs — so they share one
/// method rather than splitting into `register`/`learn` variants.
///
/// Bounded by an LRU cache ([`MAX_ROUTES`]); a lookup counts as a use, so the
/// peers we actually route to stay resident.
pub struct NeighbourTable {
    by_vip: Mutex<LruCache<Ipv6Addr, NodeKey>>,
}

impl Default for NeighbourTable {
    fn default() -> Self {
        Self::new()
    }
}

impl NeighbourTable {
    pub fn new() -> Self {
        Self {
            by_vip: Mutex::new(LruCache::new(MAX_ROUTES)),
        }
    }

    /// Record that `node` is reachable, keyed by its vip. Idempotent; evicts the
    /// least-recently-used route if the table is at capacity.
    pub fn register(&self, node: NodeKey) {
        self.by_vip
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .put(vip(&node), node);
    }

    /// Resolve the node owning `dst` for egress. `None` means no route is known
    /// yet — the caller drops the packet, as on any best-effort link. A hit also
    /// marks the route most-recently-used (so active peers aren't evicted).
    pub fn lookup(&self, dst: &Ipv6Addr) -> Option<NodeKey> {
        self.by_vip
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(dst)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_lookup_returns_node() {
        let table = NeighbourTable::new();
        let node: NodeKey = [9u8; 32];
        table.register(node);
        assert_eq!(table.lookup(&vip(&node)), Some(node));
    }

    #[test]
    fn lookup_unknown_vip_is_none() {
        let table = NeighbourTable::new();
        assert_eq!(table.lookup(&vip(&[1u8; 32])), None);
    }

    /// A distinct node key per index (the index in the leading bytes, which is
    /// what `vip` truncates over — so distinct keys give distinct vips).
    fn key(i: u32) -> NodeKey {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        k
    }

    #[test]
    fn evicts_least_recently_used_when_full() {
        let table = NeighbourTable::new();
        let cap = MAX_ROUTES.get() as u32;
        // Fill exactly to capacity: all present.
        for i in 0..cap {
            table.register(key(i));
        }
        assert_eq!(table.lookup(&vip(&key(0))), Some(key(0)));
        // One more eviction pressure: registering a fresh route must evict the
        // least-recently-used. We just touched key(0) via the lookup above, so
        // key(1) is now the LRU and should be the one dropped.
        table.register(key(cap));
        assert_eq!(table.lookup(&vip(&key(cap))), Some(key(cap)));
        assert_eq!(table.lookup(&vip(&key(1))), None);
        // key(0), refreshed by its lookup, survived.
        assert_eq!(table.lookup(&vip(&key(0))), Some(key(0)));
    }
}
