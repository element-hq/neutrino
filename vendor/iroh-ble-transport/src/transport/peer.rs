//! Peer types: `PeerEntry`, `PeerPhase`, `PeerCommand`, `PeerAction`.
//!
//! All types in this module are pure data. No I/O, no tokio tasks. Any code
//! here can be exercised from synchronous unit tests.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::task::Waker;
use std::time::Instant;

use blew::{BleDevice, DeviceId, DisconnectCause, L2capChannel};
use bytes::Bytes;

pub const KEY_PREFIX_LEN: usize = 12;
pub type KeyPrefix = [u8; KEY_PREFIX_LEN];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentSource {
    /// Produced by `run_central_events` when a P2C notification arrives.
    CentralReceivedP2c,
    /// Produced by `run_peripheral_events` when a C2P write arrives.
    PeripheralReceivedC2p,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRole {
    /// This node discovered the peer via scanning and drives the GATT client side.
    Central,
    /// This node received an inbound GATT write from the peer and answers as peripheral.
    Peripheral,
}

/// Shared monotonic clock used by a data-pipe's inbound delivery path to
/// advertise liveness to the registry. The pipe worker calls `bump()` whenever
/// a reassembled datagram is handed off to iroh; the registry watchdog reads
/// `last()` to detect wedged pipes where the peer stack went silent without
/// emitting a disconnect callback (observed on Android LE in low-power mode
/// and during iOS background freezes).
#[derive(Debug, Clone)]
pub struct LivenessClock {
    inner: Arc<parking_lot::Mutex<Instant>>,
}

impl LivenessClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(Instant::now())),
        }
    }

    pub fn bump(&self) {
        *self.inner.lock() = Instant::now();
    }

    #[must_use]
    pub fn last(&self) -> Instant {
        *self.inner.lock()
    }
}

impl Default for LivenessClock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct PipeHandles {
    pub outbound_tx: tokio::sync::mpsc::Sender<PendingSend>,
    pub inbound_tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
    pub swap_tx: tokio::sync::mpsc::Sender<blew::L2capChannel>,
    pub last_rx_at: LivenessClock,
}

#[derive(Debug)]
pub struct PeerEntry {
    pub device_id: DeviceId,
    pub phase: PeerPhase,
    pub last_adv: Option<Instant>,
    pub last_rx: Option<Instant>,
    pub last_tx: Option<Instant>,
    pub consecutive_failures: u32,
    /// Monotonic "which data-pipe generation this peer is on." Bumped every
    /// time the peer enters `Connected` with a freshly-built pipe. Every
    /// `SendDatagram` command carries the tx_gen of the pipe it was minted
    /// for, so a datagram queued against an old pipe (e.g. one that was torn
    /// down and rebuilt while the send sat in iroh's outbox) can be detected
    /// and rejected rather than silently delivered on a channel the caller
    /// never intended. Never decrements; the starting value is 0.
    pub tx_gen: u64,
    pub pending_sends: VecDeque<PendingSend>,
    pub role: ConnectRole,
    pub pipe: Option<PipeHandles>,
    pub rx_backlog: VecDeque<Bytes>,
    pub l2cap_channel: Option<L2capChannel>,
    /// Peripheral-side subscribed notify characteristics for the current GATT
    /// session. This lets the registry distinguish an idempotent second-char
    /// subscribe from a full remote unsubscribe.
    pub subscribed_chars: HashSet<uuid::Uuid>,
    /// Peer's 12-byte advertising prefix, learned from a scan. `None` when
    /// the entry was created by an inbound path (GATT write / L2CAP accept)
    /// before scan ever saw the advertisement — those peers don't get
    /// persisted to the `PeerStore` because we have no stable key for them.
    pub prefix: Option<KeyPrefix>,
    pub verified_endpoint: Option<iroh_base::EndpointId>,
    pub verified_at: Option<Instant>,
    pub l2cap_upgrade_failed: bool,
    pub verified_live_suppressed_logged: bool,
}

impl PeerEntry {
    pub fn new(device_id: DeviceId) -> Self {
        Self {
            device_id,
            phase: PeerPhase::Unknown,
            last_adv: None,
            last_rx: None,
            last_tx: None,
            consecutive_failures: 0,
            tx_gen: 0,
            pending_sends: VecDeque::new(),
            role: ConnectRole::Central,
            pipe: None,
            rx_backlog: VecDeque::new(),
            l2cap_channel: None,
            subscribed_chars: HashSet::new(),
            prefix: None,
            verified_endpoint: None,
            verified_at: None,
            l2cap_upgrade_failed: false,
            verified_live_suppressed_logged: false,
        }
    }
}

#[derive(Debug)]
pub struct PendingSend {
    pub tx_gen: u64,
    pub datagram: Bytes,
    pub waker: Waker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectPath {
    L2cap,
    Gatt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectReason {
    LocalClose,
    RemoteClose,
    LinkLoss,
    AdapterOff,
    Gatt133,
    Timeout,
    LinkDead,
    ProtocolMismatch,
    DedupLoser,
    Unknown(i32),
}

impl From<DisconnectCause> for DisconnectReason {
    fn from(cause: DisconnectCause) -> Self {
        match cause {
            DisconnectCause::LocalClose => DisconnectReason::LocalClose,
            DisconnectCause::RemoteClose => DisconnectReason::RemoteClose,
            DisconnectCause::LinkLoss => DisconnectReason::LinkLoss,
            DisconnectCause::AdapterOff => DisconnectReason::AdapterOff,
            DisconnectCause::Gatt133 => DisconnectReason::Gatt133,
            DisconnectCause::Timeout => DisconnectReason::Timeout,
            DisconnectCause::Unknown(c) => DisconnectReason::Unknown(c),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadReason {
    MaxRetries,
    ProtocolMismatch { got: u8, want: u8 },
    Forgotten,
}

#[derive(Debug)]
pub enum PeerPhase {
    Unknown,
    Discovered {
        since: Instant,
    },
    PendingDial {
        since: Instant,
        deadline: Instant,
        prefix: KeyPrefix,
    },
    Connecting {
        attempt: u32,
        started: Instant,
        path: ConnectPath,
    },
    Handshaking {
        since: Instant,
        channel: ChannelHandle,
    },
    Connected {
        since: Instant,
        channel: ChannelHandle,
        tx_gen: u64,
        upgrading: bool,
    },
    Draining {
        since: Instant,
        reason: DisconnectReason,
    },
    Reconnecting {
        attempt: u32,
        next_at: Instant,
        reason: DisconnectReason,
    },
    Dead {
        reason: DeadReason,
        at: Instant,
    },
    Restoring {
        since: Instant,
    },
}

/// Opaque handle to a live channel held inside the driver. The registry
/// treats this as a value it passes back to the driver when asking for I/O.
#[derive(Debug, Clone)]
pub struct ChannelHandle {
    pub id: u64,
    pub path: ConnectPath,
}

#[derive(Debug)]
pub enum PeerCommand {
    Advertised {
        prefix: KeyPrefix,
        device: BleDevice,
        rssi: Option<i16>,
    },
    CentralConnected {
        device_id: DeviceId,
    },
    CentralDisconnected {
        device_id: DeviceId,
        cause: DisconnectCause,
    },
    InboundGattFragment {
        device_id: DeviceId,
        source: FragmentSource,
        bytes: Bytes,
    },
    InboundL2capChannel {
        device_id: DeviceId,
        channel: L2capChannel,
    },
    AdapterStateChanged {
        powered: bool,
    },
    SendDatagram {
        device_id: DeviceId,
        tx_gen: u64,
        datagram: Bytes,
        waker: Waker,
    },
    Tick(Instant),
    ConnectSucceeded {
        device_id: DeviceId,
        channel: ChannelHandle,
    },
    ConnectFailed {
        device_id: DeviceId,
        error: String,
    },
    OpenL2capSucceeded {
        device_id: DeviceId,
        channel: L2capChannel,
    },
    OpenL2capFailed {
        device_id: DeviceId,
        error: String,
    },
    /// Central read the peer's VERSION characteristic and got back a byte
    /// that does not match our `PROTOCOL_VERSION`. The registry tears the
    /// peer down into `Dead { ProtocolMismatch }` rather than letting an
    /// incompatible data pipe start.
    ProtocolVersionMismatch {
        device_id: DeviceId,
        got: u8,
        want: u8,
    },
    Stalled {
        device_id: DeviceId,
    },
    /// Tell the registry to evict this DeviceId entirely. Used when the
    /// routing layer detects that a known prefix has flipped to a new
    /// DeviceId (e.g. peer restart with a new MAC) so the stale entry can be
    /// torn down before the new one is rebuilt by an inbound write.
    Forget {
        device_id: DeviceId,
    },
    DataPipeReady {
        device_id: DeviceId,
        tx_gen: u64,
        outbound_tx: tokio::sync::mpsc::Sender<PendingSend>,
        inbound_tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
        swap_tx: tokio::sync::mpsc::Sender<blew::L2capChannel>,
        last_rx_at: LivenessClock,
    },
    /// Emitted when `EndpointHooks::after_handshake` fires. The registry
    /// stamps all PeerEntries whose prefix matches and runs the dedup pass.
    VerifiedEndpoint {
        endpoint_id: iroh_base::EndpointId,
        token: Option<u64>,
    },
    /// Emitted by run_peripheral_events when a remote central subscribes
    /// to C2P or P2C. Registry materializes a Connected{Gatt} PeerEntry
    /// with role=Peripheral so peripheral-side traffic is visible to dedup.
    /// `prefix` is filled in when scan has already observed this DeviceId —
    /// without it the inbound entry can't be collapsed with its outbound
    /// sibling during the dedup pass, so the UI lists it separately.
    PeripheralClientSubscribed {
        client_id: DeviceId,
        char_uuid: uuid::Uuid,
        prefix: Option<KeyPrefix>,
    },
    PeripheralClientUnsubscribed {
        client_id: DeviceId,
        char_uuid: uuid::Uuid,
    },
    /// Emitted by the L2CAP pipe worker when its outbound write has been
    /// blocked on backpressure for longer than L2CAP_HANDOVER_TIMEOUT.
    L2capHandoverTimeout {
        device_id: DeviceId,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum PeerAction {
    StartConnect {
        device_id: DeviceId,
        attempt: u32,
    },
    /// Read the peer's VERSION characteristic and, on mismatch, emit
    /// [`PeerCommand::ProtocolVersionMismatch`] so the registry can Dead
    /// the peer instead of running an incompatible data pipe.
    ReadVersion {
        device_id: DeviceId,
    },
    CloseChannel {
        device_id: DeviceId,
        channel: ChannelHandle,
        reason: DisconnectReason,
    },
    Refresh {
        device_id: DeviceId,
    },
    AckSend {
        tx_gen: u64,
        waker: Waker,
        result: Result<(), std::io::ErrorKind>,
    },
    RebuildGattServer,
    RestartAdvertising,
    RestartL2capListener,
    StartDataPipe {
        device_id: DeviceId,
        tx_gen: u64,
        role: ConnectRole,
        path: ConnectPath,
        l2cap_channel: Option<L2capChannel>,
    },
    /// Winner of a dedup pass asks the driver to attempt an L2CAP open
    /// for this already-connected GATT peer.
    UpgradeToL2cap {
        device_id: DeviceId,
    },
    /// L2CAP open succeeded; swap the pipe worker from GATT to L2CAP on
    /// this peer. The existing GATT worker enters drain-only mode.
    SwapPipeToL2cap {
        device_id: DeviceId,
        channel: L2capChannel,
        swap_tx: tokio::sync::mpsc::Sender<L2capChannel>,
    },
    /// L2CAP handover stalled; kill the L2CAP worker, respawn a GATT
    /// worker, and stay Connected{Gatt} for the rest of this session.
    RevertToGattPipe {
        device_id: DeviceId,
        tx_gen: u64,
        role: ConnectRole,
    },
    /// Persist a snapshot of this peer to the configured `PeerStore`. Emitted
    /// when a peer leaves `Connected` for `Draining`: we've seen enough of
    /// this peer to want to remember them across restarts.
    PutPeerStore {
        prefix: KeyPrefix,
        snapshot: crate::transport::store::PeerSnapshot,
    },
    /// Drop a peer from the configured `PeerStore`. Emitted when the registry
    /// declares the peer permanently unusable (e.g. `Dead { MaxRetries }`):
    /// there's no reason to keep trying after transport restart.
    ForgetPeerStore {
        prefix: KeyPrefix,
    },
    EmitMetric(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_entry_defaults_to_unknown() {
        let e = PeerEntry::new(DeviceId::from("test"));
        assert!(matches!(e.phase, PeerPhase::Unknown));
        assert_eq!(e.tx_gen, 0);
    }

    #[test]
    fn disconnect_cause_maps_to_reason() {
        assert_eq!(
            DisconnectReason::from(DisconnectCause::Gatt133),
            DisconnectReason::Gatt133
        );
    }

    #[test]
    fn peer_entry_defaults_to_central_role_and_no_pipe() {
        let e = PeerEntry::new(DeviceId::from("test"));
        assert_eq!(e.role, ConnectRole::Central);
        assert!(e.pipe.is_none());
        assert!(e.rx_backlog.is_empty());
    }

    #[test]
    fn fragment_source_distinguishes_direction() {
        let c = FragmentSource::CentralReceivedP2c;
        let p = FragmentSource::PeripheralReceivedC2p;
        assert_ne!(c, p);
    }

    #[test]
    fn data_pipe_variants_are_constructible() {
        let (outbound_tx, _) = tokio::sync::mpsc::channel::<PendingSend>(1);
        let (inbound_tx, _) = tokio::sync::mpsc::channel::<Bytes>(1);
        let (swap_tx, _) = tokio::sync::mpsc::channel::<blew::L2capChannel>(1);
        let _cmd = PeerCommand::DataPipeReady {
            device_id: DeviceId::from("x"),
            tx_gen: 1,
            outbound_tx,
            inbound_tx,
            swap_tx,
            last_rx_at: LivenessClock::new(),
        };
        let _act = PeerAction::StartDataPipe {
            device_id: DeviceId::from("x"),
            tx_gen: 1,
            role: ConnectRole::Central,
            path: ConnectPath::Gatt,
            l2cap_channel: None,
        };
    }

    #[test]
    fn new_variants_are_constructible() {
        let _cmd1 = PeerCommand::VerifiedEndpoint {
            endpoint_id: iroh_base::SecretKey::from_bytes(&[0u8; 32]).public(),
            token: None,
        };
        let _cmd2 = PeerCommand::PeripheralClientSubscribed {
            client_id: DeviceId::from("x"),
            char_uuid: uuid::Uuid::nil(),
            prefix: None,
        };
        let _cmd3 = PeerCommand::PeripheralClientUnsubscribed {
            client_id: DeviceId::from("x"),
            char_uuid: uuid::Uuid::nil(),
        };
        let _cmd4 = PeerCommand::L2capHandoverTimeout {
            device_id: DeviceId::from("x"),
        };
        let _act1 = PeerAction::UpgradeToL2cap {
            device_id: DeviceId::from("x"),
        };
        let _act2 = PeerAction::RevertToGattPipe {
            device_id: DeviceId::from("x"),
            tx_gen: 1,
            role: ConnectRole::Central,
        };
        assert_eq!(DisconnectReason::DedupLoser, DisconnectReason::DedupLoser);
    }

    #[test]
    fn peer_entry_initializes_dedup_fields_empty() {
        let e = PeerEntry::new(DeviceId::from("x"));
        assert!(e.verified_endpoint.is_none());
        assert!(e.verified_at.is_none());
        assert!(!e.l2cap_upgrade_failed);
        assert!(!e.verified_live_suppressed_logged);
        assert!(e.subscribed_chars.is_empty());
    }
}
