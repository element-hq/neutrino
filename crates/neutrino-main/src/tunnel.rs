//! Federation routing glue for the relay tunnel.
//!
//! A peer's federation `server_name` is its node id (lowercase hex, as derived
//! by [`server_identity_from_secret`](crate::server_identity_from_secret)). Two
//! pure, iroh-free steps map that name onto the relay's virtual network — the
//! actual transport over the resulting virtual IPs lives in the ffi/TUN layer:
//! - [`TunnelResolver`] rewrites the federation egress destination to the peer's
//!   vip (so outbound CoAP/UDP is addressed into the tunnel);
//! - [`register_route`] seeds the peer's vip→node route when it is first learned
//!   (invite-time), so the relay can carry the first packet.

use neutrino_lb::DestinationResolver;
use neutrino_relay::{NeighbourTable, NodeKey, vip};
use tracing::warn;

/// The port a peer's in-tunnel relay endpoint listens on, used when a
/// `server_name` carries no explicit `:port` override (Matrix's default).
const DEFAULT_FEDERATION_PORT: u16 = 8448;

/// Parse a federation `server_name` (`host[:port]`, host = the peer's node id in
/// hex) into its relay [`NodeKey`] and any explicit port override. `None` if the
/// host isn't a 32-byte hex node id (e.g. a dev `localhost`) — no tunnel route.
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
    Some((key, port))
}

/// [`DestinationResolver`] for the tunnel: maps a peer `server_name` to its vip,
/// preserving any port (else the federation default). Non-node authorities pass
/// through unchanged so direct-dial federation is unaffected. Pure — route
/// registration is the separate concern of [`register_route`].
#[derive(Debug, Default)]
pub(crate) struct TunnelResolver;

impl DestinationResolver for TunnelResolver {
    fn resolve(&self, authority: String) -> String {
        match parse_server_name(&authority) {
            Some((key, port)) => {
                let v = vip(&key);
                match port {
                    Some(port) => format!("[{v}]:{port}"),
                    None => format!("[{v}]:{DEFAULT_FEDERATION_PORT}"),
                }
            }
            None => {
                warn!(%authority, "tunnel resolver: server_name is not a node id; dialing verbatim");
                authority
            }
        }
    }
}

/// Record the relay route to a peer named by `server_name` (invite-time). No-op
/// for a non-node name.
// Wired into neutrino-http's invite/membership path (the next P3b stage); used
// by tests until then.
#[allow(dead_code)]
pub(crate) fn register_route(table: &NeighbourTable, server_name: &str) {
    match parse_server_name(server_name) {
        Some((key, _port)) => table.register(key),
        None => warn!(%server_name, "tunnel: cannot register non-node server_name"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_and_name() -> (NodeKey, String) {
        let key = [0x11u8; 32];
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
    fn resolver_maps_node_name_to_vip_default_and_override() {
        let (key, name) = key_and_name();
        let v = vip(&key);
        assert_eq!(
            TunnelResolver.resolve(name.clone()),
            format!("[{v}]:{DEFAULT_FEDERATION_PORT}")
        );
        assert_eq!(
            TunnelResolver.resolve(format!("{name}:7777")),
            format!("[{v}]:7777")
        );
    }

    #[test]
    fn resolver_passes_through_non_node_authority() {
        assert_eq!(
            TunnelResolver.resolve("localhost:8008".to_owned()),
            "localhost:8008"
        );
        assert_eq!(
            TunnelResolver.resolve("[2001:db8::1]:8448".to_owned()),
            "[2001:db8::1]:8448"
        );
    }

    #[test]
    fn register_route_adds_node_route() {
        let table = NeighbourTable::new();
        let (key, name) = key_and_name();
        register_route(&table, &name);
        assert_eq!(table.lookup(&vip(&key)), Some(key));
    }

    #[test]
    fn register_route_ignores_non_node_name() {
        let table = NeighbourTable::new();
        register_route(&table, "localhost");
        assert_eq!(table.lookup(&vip(&[1u8; 32])), None);
    }
}
