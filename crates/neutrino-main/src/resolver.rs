//! Federation routing glue for the datagram link.
//!
//! A peer's federation `server_name` is its node id (lowercase hex, as derived
//! by [`server_identity_from_secret`](crate::server_identity_from_secret)).
//! [`NodeIdResolver`] is the one pure step that maps that name onto the wire:
//! it rewrites the federation egress destination to the peer's bare 64-char
//! hex node id, which the sidecar's datagram egress (`LinkCoapWireClient`)
//! parses and dials the peer's datagram link by. The transport over those node
//! ids is the injected [`DatagramLink`](neutrino_lb::DatagramLink), out of
//! tree.

use neutrino_lb::DestinationResolver;
use tracing::warn;

/// A node's stable cryptographic identity, as raw ed25519 public-key bytes — the
/// hex of this is the peer's `server_name`. This layer only needs the byte
/// array to validate/round-trip the hex.
type NodeKey = [u8; 32];

/// Parse a federation `server_name` (`host[:port]`, host = the peer's node id in
/// hex) into its [`NodeKey`] and any explicit port override. `None` if the host
/// isn't a 32-byte hex node id (e.g. a dev `localhost`) — not a node route.
///
/// A node id is hex with no colons, so `rsplit_once(':')` cleanly peels an
/// optional `:port`; a bracketed IPv6 authority mis-splits but its host then
/// fails hex decode and is reported `None` (passed through), so it's harmless.
fn parse_server_name(authority: &str) -> Option<(NodeKey, Option<&str>)> {
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    };
    let key: NodeKey = hex::decode(host).ok()?.try_into().ok()?;
    // Reject hex that isn't a valid ed25519 public key — a real node id always
    // is (it's how `server_name` is derived), so a non-node authority falls
    // through to direct dial rather than resolving to an unroutable id. Uses
    // ed25519-dalek (already a dep) — same curve check as any link's identity.
    ed25519_dalek::VerifyingKey::from_bytes(&key).ok()?;
    Some((key, port))
}

/// [`DestinationResolver`] for the datagram link: maps a peer `server_name` to
/// its bare 64-char hex node id (the datagram egress addresses peers by node id,
/// not by an IP/port). Non-node authorities pass through unchanged so direct-dial
/// federation is unaffected.
pub(crate) struct NodeIdResolver;

impl NodeIdResolver {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl DestinationResolver for NodeIdResolver {
    fn resolve(&self, authority: String) -> String {
        match parse_server_name(&authority) {
            // The datagram egress (`LinkCoapWireClient`) parses `dest` as a
            // 64-char hex node id and dials the peer over the link by it, so
            // return the bare node id — no vip, no port.
            Some((key, _port)) => hex::encode(key),
            None => {
                warn!(%authority, "node-id resolver: server_name is not a node id; dialing verbatim");
                authority
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_and_name() -> (NodeKey, String) {
        // A real ed25519 public key, so it passes the curve-point validation in
        // `parse_server_name` (the same kind of key a node id always is).
        let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32])
            .verifying_key()
            .to_bytes();
        (key, hex::encode(key))
    }

    #[test]
    fn parse_extracts_key_and_port() {
        let (key, name) = key_and_name();
        assert_eq!(parse_server_name(&name), Some((key, None)));
        assert_eq!(
            parse_server_name(&format!("{name}:8448")),
            Some((key, Some("8448")))
        );
        assert_eq!(parse_server_name("localhost:8008"), None);
    }

    #[test]
    fn resolver_maps_node_name_to_hex() {
        let (key, name) = key_and_name();
        let resolver = NodeIdResolver::new();
        // A bare node-id name resolves to itself (the 64-char hex node id the
        // datagram egress dials by) — no vip/port rewrite.
        assert_eq!(resolver.resolve(name.clone()), hex::encode(key));
        // An explicit `:port` is dropped: the node id alone addresses the peer
        // over the link, so the port is meaningless on this path.
        assert_eq!(resolver.resolve(format!("{name}:7777")), hex::encode(key));
    }

    #[test]
    fn resolver_passes_through_non_node_authority() {
        let resolver = NodeIdResolver::new();
        assert_eq!(
            resolver.resolve("localhost:8008".to_owned()),
            "localhost:8008"
        );
        assert_eq!(
            resolver.resolve("[2001:db8::1]:8448".to_owned()),
            "[2001:db8::1]:8448"
        );
    }
}
