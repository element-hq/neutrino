//! Dedup tuning constants and tiebreaker helper. All state-machine
//! decisions about which of two competing BLE connections to keep live
//! here; the registry calls `should_win` on each duplicate-verified pass.

use std::time::Duration;

use iroh_base::EndpointId;

use crate::transport::peer::ConnectRole;

/// How long the lower-EndpointId side defers dialing after discovery so
/// the higher side has a chance to land first. Tunable as we measure
/// real-world connect latency on hardware.
pub const FAIRNESS_WINDOW: Duration = Duration::from_millis(800);

/// Uniform jitter added/subtracted from `FAIRNESS_WINDOW` to prevent lock-step
/// dialing when many peers enter the mesh at once.
pub const FAIRNESS_JITTER: Duration = Duration::from_millis(150);

/// Max time either side may spend swapping the pipe worker during an L2CAP
/// upgrade. If the peer side never spins up (e.g. blew listener jammed),
/// the L2CAP worker reports timeout and the winner reverts to GATT.
pub const L2CAP_HANDOVER_TIMEOUT: Duration = Duration::from_secs(1);

/// Return `true` iff the entry with role=`this_role` on *this* node is the
/// one to keep, given the two verified endpoints. Rule: keep the BLE
/// connection where the peer with the higher EndpointId is the one that
/// dialed (i.e. is central). Both sides compute this and reach the same
/// conclusion.
///
/// * `this_role` — role of *this node* in the candidate entry.
/// * `my_endpoint` — local node's EndpointId.
/// * `peer_endpoint` — remote peer's verified EndpointId.
#[must_use]
pub fn should_win(
    this_role: ConnectRole,
    my_endpoint: &EndpointId,
    peer_endpoint: &EndpointId,
) -> bool {
    match my_endpoint.as_bytes().cmp(peer_endpoint.as_bytes()) {
        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
            matches!(this_role, ConnectRole::Central)
        }
        std::cmp::Ordering::Less => matches!(this_role, ConnectRole::Peripheral),
    }
}

/// Return `true` iff this side should actively open the upgraded L2CAP
/// channel. The dedup winner is still a pair of complementary entries
/// (higher endpoint keeps Central, lower endpoint keeps Peripheral), but
/// only the central winner should dial the CoC channel. The peripheral
/// winner must remain accept-only or both sides race to create parallel
/// channels for the same peer.
#[must_use]
pub fn should_dial_l2cap(
    this_role: ConnectRole,
    my_endpoint: &EndpointId,
    peer_endpoint: &EndpointId,
) -> bool {
    matches!(this_role, ConnectRole::Central) && should_win(this_role, my_endpoint, peer_endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint_with_first_byte(b: u8) -> EndpointId {
        let mut bytes = [0u8; 32];
        bytes[0] = b;
        iroh_base::SecretKey::from_bytes(&bytes).public()
    }

    #[test]
    fn higher_endpoint_keeps_central_role_and_drains_peripheral() {
        let high = endpoint_with_first_byte(0xFF);
        let low = endpoint_with_first_byte(0x01);
        assert!(should_win(ConnectRole::Central, &high, &low));
        assert!(!should_win(ConnectRole::Peripheral, &high, &low));
    }

    #[test]
    fn lower_endpoint_keeps_peripheral_role_and_drains_central() {
        let high = endpoint_with_first_byte(0xFF);
        let low = endpoint_with_first_byte(0x01);
        assert!(should_win(ConnectRole::Peripheral, &low, &high));
        assert!(!should_win(ConnectRole::Central, &low, &high));
    }

    #[test]
    fn both_sides_converge_on_same_decision() {
        let a = endpoint_with_first_byte(0x80);
        let b = endpoint_with_first_byte(0x01);
        // Determine which endpoint is higher based on their actual public key bytes.
        let (high, low) = if a.as_bytes() > b.as_bytes() {
            (a, b)
        } else {
            (b, a)
        };
        let high_keeps_central = should_win(ConnectRole::Central, &high, &low);
        let low_keeps_peripheral = should_win(ConnectRole::Peripheral, &low, &high);
        assert!(
            high_keeps_central && low_keeps_peripheral,
            "both sides must agree on surviving connection"
        );
    }

    #[test]
    fn only_central_winner_dials_l2cap() {
        let high = endpoint_with_first_byte(0xFF);
        let low = endpoint_with_first_byte(0x01);
        assert!(should_dial_l2cap(ConnectRole::Central, &high, &low));
        assert!(!should_dial_l2cap(ConnectRole::Peripheral, &low, &high));
    }
}
