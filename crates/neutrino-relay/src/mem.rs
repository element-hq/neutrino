//! In-memory implementations of the relay's two seams, for exercising the full
//! egress/ingress flow without a device or wire. Used by this crate's own unit
//! tests and — via the `test-support` feature — by downstream crates' tests
//! (e.g. neutrino-ffi's iroh transport test reuses [`MemPacketIo`]).

use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use crate::{DatagramTransport, NodeKey, PacketIo, RelayError};

type Datagram = (NodeKey, Vec<u8>);

/// In-memory datagram fabric: routes datagrams between [`MemTransport`]
/// endpoints by node key, standing in for the real wire. A cloneable handle to
/// a shared routing table — every endpoint shares one fabric.
#[derive(Clone, Default)]
pub struct MemNetwork {
    peers: Arc<Mutex<HashMap<NodeKey, mpsc::Sender<Datagram>>>>,
}

impl MemNetwork {
    /// Create an endpoint for `node`, registering its inbound queue on the
    /// fabric so other endpoints can `send` to it.
    pub fn endpoint(&self, node: NodeKey) -> MemTransport {
        let (tx, rx) = mpsc::channel(64);
        self.peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node, tx);
        MemTransport {
            node,
            rx: AsyncMutex::new(rx),
            net: self.clone(),
        }
    }
}

pub struct MemTransport {
    node: NodeKey,
    rx: AsyncMutex<mpsc::Receiver<Datagram>>,
    net: MemNetwork,
}

#[async_trait]
impl DatagramTransport for MemTransport {
    async fn send(&self, dst: NodeKey, datagram: &[u8]) -> Result<(), RelayError> {
        let tx = self
            .net
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&dst)
            .cloned();
        // Unknown / unreachable peer: drop, like any best-effort transport.
        if let Some(tx) = tx {
            let _ = tx.send((self.node, datagram.to_vec())).await;
        }
        Ok(())
    }

    async fn recv(&self) -> Option<Datagram> {
        self.rx.lock().await.recv().await
    }
}

/// In-memory TUN seam. `recv` yields packets the host "wrote out"; `send`
/// captures packets the relay delivers inward.
pub struct MemPacketIo {
    outbound: AsyncMutex<mpsc::Receiver<Vec<u8>>>,
    inbound: mpsc::Sender<Vec<u8>>,
}

/// Test-side handle to a [`MemPacketIo`]: [`emit`](Self::emit) plays a local
/// process writing an outbound packet into the TUN; [`next`](Self::next)
/// observes a packet the relay delivered inbound.
pub struct MemPacketHost {
    inject: mpsc::Sender<Vec<u8>>,
    observe: AsyncMutex<mpsc::Receiver<Vec<u8>>>,
}

/// Build a paired [`MemPacketIo`] (handed to the relay) and [`MemPacketHost`]
/// (kept by the test to drive and observe it).
pub fn mem_packet_io() -> (MemPacketIo, MemPacketHost) {
    let (out_tx, out_rx) = mpsc::channel(64);
    let (in_tx, in_rx) = mpsc::channel(64);
    (
        MemPacketIo {
            outbound: AsyncMutex::new(out_rx),
            inbound: in_tx,
        },
        MemPacketHost {
            inject: out_tx,
            observe: AsyncMutex::new(in_rx),
        },
    )
}

#[async_trait]
impl PacketIo for MemPacketIo {
    async fn recv(&self) -> Option<Vec<u8>> {
        self.outbound.lock().await.recv().await
    }

    async fn send(&self, packet: &[u8]) -> Result<(), RelayError> {
        self.inbound
            .send(packet.to_vec())
            .await
            .map_err(|err| RelayError::Io(err.to_string()))
    }
}

impl MemPacketHost {
    pub async fn emit(&self, packet: Vec<u8>) -> Result<(), RelayError> {
        self.inject
            .send(packet)
            .await
            .map_err(|err| RelayError::Io(err.to_string()))
    }

    pub async fn next(&self) -> Option<Vec<u8>> {
        self.observe.lock().await.recv().await
    }
}

/// Build a minimal well-formed IPv6 packet (40-byte header + payload) with the
/// given addresses. Only the fields the relay reads — the version nibble and
/// the source/destination addresses — are meaningful; the rest stay zero.
pub fn ipv6_packet(src: Ipv6Addr, dst: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
    let mut pkt = vec![0u8; 40];
    pkt[0] = 0x60; // version 6, traffic class 0
    pkt[8..24].copy_from_slice(&src.octets());
    pkt[24..40].copy_from_slice(&dst.octets());
    pkt.extend_from_slice(payload);
    pkt
}
