use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::sync::{PoisonError, RwLock};

use crate::NodeKey;
use crate::vip::vip;

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
#[derive(Default)]
pub struct NeighbourTable {
    by_vip: RwLock<HashMap<Ipv6Addr, NodeKey>>,
}

impl NeighbourTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `node` is reachable, keyed by its vip. Idempotent.
    pub fn register(&self, node: NodeKey) {
        self.by_vip
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(vip(&node), node);
    }

    /// Resolve the node owning `dst` for egress. `None` means no route is known
    /// yet — the caller drops the packet, as on any best-effort link.
    pub fn lookup(&self, dst: &Ipv6Addr) -> Option<NodeKey> {
        self.by_vip
            .read()
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
}
