//! BLE peer-discovery advert payload: `node_id ‖ display_name`.
//!
//! Neutrino advertises a discovery payload in a BLE-5 extended advertisement's
//! manufacturer-specific data under [`NEUTRINO_MANUFACTURER_ID`], so a scanning
//! peer can learn a device's full node id (its `server_name`) **and** a
//! human-readable display name without connecting. The layout is deliberately
//! trivial — a fixed 32-byte node id followed by the UTF-8 display name — so
//! both ends agree byte-for-byte:
//!
//! ```text
//! ┌────────────── 32 bytes ──────────────┬── remaining bytes ──┐
//! │            node id (raw)              │  display name (UTF-8)│
//! └───────────────────────────────────────┴─────────────────────┘
//! ```
//!
//! The 32-byte node id is mandatory (it is the peer's `server_name`, which the
//! directory returns as `@…:{node_id}` and federation dials); the display name
//! is whatever bytes remain. Carrying the full id is why this needs extended
//! advertising — 32 + name overflows the 31-byte legacy budget.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

/// BLE company identifier under which neutrino carries its discovery payload.
/// Not a Bluetooth-SIG-assigned id — this is a private mesh, so any value both
/// ends agree on works.
pub const NEUTRINO_MANUFACTURER_ID: u16 = 0x0E1E;

/// Fixed marker service UUID every neutrino peer advertises alongside its
/// per-peer `69726f00-…` key-prefix UUID. The key-prefix UUID differs per peer
/// so it cannot be exact-match filtered; this shared marker is what scanners
/// filter on. A filtered scan (vs. the previous unfiltered one) keeps
/// delivering results while the screen is off, uses the controller's hardware
/// filter (battery), and sidesteps stacks that stop reporting new advertisers
/// to a long-lived unfiltered scan client.
pub const DISCOVERY_SERVICE_UUID: uuid::Uuid =
    uuid::Uuid::from_u128(0xc0ba1760_be7a_deca_1956_fade00000000);

/// The scan filter every discovery scan uses: match only adverts carrying
/// [`DISCOVERY_SERVICE_UUID`].
pub(crate) fn scan_filter() -> blew::central::ScanFilter {
    blew::central::ScanFilter {
        services: vec![DISCOVERY_SERVICE_UUID],
        ..Default::default()
    }
}

/// A peer discovered from a scanned advertisement: its full node id and the
/// display name it advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPeer {
    pub node_id: [u8; NODE_ID_LEN],
    pub display_name: String,
}

/// Accumulates BLE-discovered peers keyed by node id and publishes the full
/// current set on every change, so a consumer (the ffi layer) can forward each
/// snapshot to the homeserver's discovery registry. Cheap to clone (shared).
#[derive(Clone)]
pub struct DiscoverySink {
    inner: Arc<DiscoveryInner>,
}

struct DiscoveryInner {
    peers: Mutex<HashMap<[u8; NODE_ID_LEN], String>>,
    tx: watch::Sender<Vec<DiscoveredPeer>>,
}

impl DiscoverySink {
    /// Create a sink and its snapshot receiver (seeded with an empty set).
    pub fn new() -> (Self, watch::Receiver<Vec<DiscoveredPeer>>) {
        let (tx, rx) = watch::channel(Vec::new());
        let inner = Arc::new(DiscoveryInner {
            peers: Mutex::new(HashMap::new()),
            tx,
        });
        (Self { inner }, rx)
    }

    /// Record a discovered peer. Republishes the snapshot only when the set
    /// actually changes (a re-scan of an unchanged peer is a no-op — Android
    /// re-emits `DeviceDiscovered` on every scan hit).
    pub fn observe(&self, node_id: [u8; NODE_ID_LEN], display_name: String) {
        let mut peers = self
            .inner
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if peers.get(&node_id) == Some(&display_name) {
            return;
        }
        tracing::debug!(
            node_id = %hex32(&node_id),
            display_name = %display_name,
            "discovery: observed peer (new or renamed)"
        );
        peers.insert(node_id, display_name);
        let snapshot: Vec<DiscoveredPeer> = peers
            .iter()
            .map(|(id, name)| DiscoveredPeer {
                node_id: *id,
                display_name: name.clone(),
            })
            .collect();
        drop(peers);
        let _ = self.inner.tx.send(snapshot);
    }
}

/// The fixed node-id prefix length of a discovery payload.
const NODE_ID_LEN: usize = 32;

/// Lowercase-hex a node id for logging (no `hex` dependency in this crate).
fn hex32(bytes: &[u8; NODE_ID_LEN]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(NODE_ID_LEN * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Encode a discovery payload: the 32-byte `node_id` followed by the UTF-8
/// `display_name`. The caller is responsible for bounding the name length so
/// the whole advertisement fits the negotiated extended-advertising budget.
pub fn encode_discovery_payload(node_id: &[u8; NODE_ID_LEN], display_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(NODE_ID_LEN + display_name.len());
    out.extend_from_slice(node_id);
    out.extend_from_slice(display_name.as_bytes());
    out
}

/// Decode a discovery payload produced by [`encode_discovery_payload`]: the
/// first 32 bytes are the node id, the remainder the UTF-8 display name.
/// Returns `None` if the payload is too short to hold a node id or the name is
/// not valid UTF-8 (a foreign advertiser under the same company id, say).
pub fn decode_discovery_payload(bytes: &[u8]) -> Option<([u8; NODE_ID_LEN], String)> {
    if bytes.len() < NODE_ID_LEN {
        return None;
    }
    let mut node_id = [0u8; NODE_ID_LEN];
    node_id.copy_from_slice(&bytes[..NODE_ID_LEN]);
    let display_name = core::str::from_utf8(&bytes[NODE_ID_LEN..]).ok()?.to_owned();
    Some((node_id, display_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let node_id = [7u8; 32];
        let payload = encode_discovery_payload(&node_id, "Alice");
        assert_eq!(payload.len(), 32 + "Alice".len());
        let (got_id, got_name) = decode_discovery_payload(&payload).expect("decodes");
        assert_eq!(got_id, node_id);
        assert_eq!(got_name, "Alice");
    }

    #[test]
    fn empty_name_is_valid() {
        let node_id = [1u8; 32];
        let (got_id, got_name) =
            decode_discovery_payload(&encode_discovery_payload(&node_id, "")).expect("decodes");
        assert_eq!(got_id, node_id);
        assert_eq!(got_name, "");
    }

    #[test]
    fn too_short_is_rejected() {
        assert!(decode_discovery_payload(&[]).is_none());
        assert!(decode_discovery_payload(&[0u8; 31]).is_none());
        // Exactly 32 bytes → node id with an empty name, not a rejection.
        assert!(decode_discovery_payload(&[0u8; 32]).is_some());
    }

    #[test]
    fn invalid_utf8_name_is_rejected() {
        let mut payload = vec![0u8; 32];
        payload.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        assert!(decode_discovery_payload(&payload).is_none());
    }

    #[test]
    fn utf8_multibyte_name_survives() {
        let node_id = [3u8; 32];
        let name = "Zoë 日本語";
        let (_, got) =
            decode_discovery_payload(&encode_discovery_payload(&node_id, name)).expect("decodes");
        assert_eq!(got, name);
    }

    // Wire constant: the shared marker UUID scanners filter on and every peer
    // advertises. A change here silently splits the mesh (old peers become
    // invisible to new scanners and vice versa), so pin the exact string.
    #[test]
    fn discovery_service_uuid_is_the_agreed_marker() {
        assert_eq!(
            DISCOVERY_SERVICE_UUID.to_string(),
            "c0ba1760-be7a-deca-1956-fade00000000"
        );
        assert_eq!(scan_filter().services, vec![DISCOVERY_SERVICE_UUID]);
    }
}
