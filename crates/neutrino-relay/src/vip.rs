use std::net::Ipv6Addr;

use crate::NodeKey;

/// First octet of every virtual IP. Marks the address as an IPv6 unique-local
/// address (`fd00::/8`, the locally-assigned half of RFC 4193's `fc00::/7`).
/// The entire virtual network lives in this `/8`; each node owns one `/128`.
pub const VIP_PREFIX_BYTE: u8 = 0xfd;

/// Prefix length of the virtual subnet (`fd00::/8`). The host routes this whole
/// range into the tunnel; `vip(self)` is the single in-subnet `/128` address it
/// claims.
pub const VIP_SUBNET_PREFIX_LEN: u8 = 8;

/// Base address of the virtual subnet (`fd00::`).
pub const VIP_SUBNET: Ipv6Addr = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0);

/// Map a node's identity to its virtual IP — deterministic and one-way.
///
/// The address is `0xfd` followed by the first 120 bits of the node key, so the
/// vip sits in `fd00::/8` and is stable for the life of the identity. The map
/// is deliberately *not* invertible: egress sees only a destination vip in an
/// outbound IP packet and must consult the [`NeighbourTable`](crate::NeighbourTable)
/// to recover the node — a vip alone does not reveal its node.
///
/// Two collision regimes, kept distinct because they answer different
/// questions:
/// - *Accidental* collision between honest nodes follows the birthday bound,
///   around `2^60` nodes — far beyond any embedded deployment, so truncation to
///   120 bits is safe at our scale.
/// - *Targeted* collision (an attacker grinding a key whose vip equals a
///   victim's, to hijack its route) is a 120-bit second-preimage, ~`2^120`
///   work — infeasible. This, not the birthday bound, is the property the
///   ingress anti-spoof check relies on.
///
/// Note the corollary: the vip authenticates only the leading 120 bits of the
/// identity; the remaining key bits are not bound by the address. The leading
/// bytes of an ed25519 public key are uniformly distributed, so the truncation
/// spreads addresses without any extra hashing.
pub fn vip(node: &NodeKey) -> Ipv6Addr {
    let mut octets = [0u8; 16];
    octets[0] = VIP_PREFIX_BYTE;
    octets[1..16].copy_from_slice(&node[..15]);
    Ipv6Addr::from(octets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vip_is_deterministic() {
        let node: NodeKey = [7u8; 32];
        assert_eq!(vip(&node), vip(&node));
    }

    #[test]
    fn distinct_nodes_get_distinct_vips() {
        assert_ne!(vip(&[1u8; 32]), vip(&[2u8; 32]));
    }

    #[test]
    fn vip_sits_in_the_ula_subnet() {
        let v = vip(&[0x5a; 32]);
        assert_eq!(v.octets()[0], VIP_PREFIX_BYTE);
        assert_eq!(v.octets()[0], VIP_SUBNET.octets()[0]);
    }

    #[test]
    fn vip_embeds_the_node_key_prefix() {
        let mut node: NodeKey = [0u8; 32];
        node[..15].copy_from_slice(&[0x11u8; 15]);
        assert_eq!(&vip(&node).octets()[1..16], &[0x11u8; 15]);
    }

    #[test]
    fn keys_differing_only_past_byte_15_collide() {
        // Pins the documented (and security-relevant) truncation: only the
        // leading 120 bits map into the vip, so two keys identical there but
        // differing beyond collide. A future "use all 32 bytes" change must
        // break this test deliberately.
        let mut a: NodeKey = [0u8; 32];
        let mut b: NodeKey = [0u8; 32];
        a[..15].copy_from_slice(&[0x33u8; 15]);
        b[..15].copy_from_slice(&[0x33u8; 15]);
        a[20] = 0xaa;
        b[20] = 0xbb;
        assert_ne!(a, b);
        assert_eq!(vip(&a), vip(&b));
    }
}
