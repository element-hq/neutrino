use std::net::Ipv6Addr;
use std::sync::Arc;

use tracing::{trace, warn};

use crate::NodeKey;
use crate::neighbour::NeighbourTable;
use crate::packet_io::PacketIo;
use crate::transport::DatagramTransport;
use crate::vip::vip;

/// Run the relay until a seam closes.
///
/// Concurrently pumps the two directions — [`egress`] (host → wire) and
/// [`ingress`] (wire → host) — sharing one neighbour table. `self_node` is this
/// node's identity, used by ingress to drop packets not addressed to us.
/// Returns as soon as *either* seam's `recv` yields `None` (its queue closed),
/// cancelling the sibling direction: that is how shutdown propagates — the
/// embedding layer drops a seam and `run` falls out.
pub async fn run<P, T>(
    self_node: NodeKey,
    table: Arc<NeighbourTable>,
    io: Arc<P>,
    transport: Arc<T>,
) where
    P: PacketIo + 'static,
    T: DatagramTransport + 'static,
{
    tokio::select! {
        _ = egress(&table, io.as_ref(), transport.as_ref()) => {}
        _ = ingress(self_node, &table, io.as_ref(), transport.as_ref()) => {}
    }
}

/// Host → wire. Each outbound IP packet's destination vip is resolved to a node
/// via the neighbour table; an unroutable packet (no node known for that vip)
/// is dropped, as on any best-effort link.
async fn egress<P: PacketIo, T: DatagramTransport>(table: &NeighbourTable, io: &P, transport: &T) {
    while let Some(pkt) = io.recv().await {
        let Some((_src, dst)) = ipv6_addrs(&pkt) else {
            trace!("egress: dropped non-IPv6 / short packet");
            continue;
        };
        match table.lookup(&dst) {
            Some(node) => {
                if let Err(err) = transport.send(node, &pkt).await {
                    warn!(%err, %dst, "egress: transport send failed");
                }
            }
            None => trace!(%dst, "egress: no route for destination, dropped"),
        }
    }
}

/// Wire → host. Two checks gate delivery, both before we learn anything so a
/// rejected packet cannot poison the table: (1) the transport authenticates the
/// datagram's source node, and we assert the packet's own source address is
/// that node's vip — a peer cannot spoof another node's address; (2) the
/// packet's destination must be *our* vip, so a peer cannot make us ingest
/// traffic addressed to a third party. Only then do we learn the reverse route
/// and deliver.
async fn ingress<P: PacketIo, T: DatagramTransport>(
    self_node: NodeKey,
    table: &NeighbourTable,
    io: &P,
    transport: &T,
) {
    let self_vip = vip(&self_node);
    while let Some((src_node, pkt)) = transport.recv().await {
        let Some((src_ip, dst)) = ipv6_addrs(&pkt) else {
            trace!("ingress: dropped non-IPv6 / short packet");
            continue;
        };
        let expected_src = vip(&src_node);
        if src_ip != expected_src {
            warn!(%src_ip, %expected_src, "ingress: source-address spoof, dropped");
            continue;
        }
        if dst != self_vip {
            warn!(%dst, %self_vip, "ingress: packet not addressed to this node, dropped");
            continue;
        }
        table.register(src_node);
        if let Err(err) = io.send(&pkt).await {
            warn!(%err, "ingress: deliver to host failed");
        }
    }
}

/// `(source, destination)` of an IPv6 packet, or `None` if the buffer is too
/// short or its version nibble is not 6 — we carry only IPv6 on the virtual
/// network, so anything else is dropped.
fn ipv6_addrs(pkt: &[u8]) -> Option<(Ipv6Addr, Ipv6Addr)> {
    if pkt.len() < 40 || pkt[0] >> 4 != 6 {
        return None;
    }
    let src: [u8; 16] = pkt[8..24].try_into().ok()?;
    let dst: [u8; 16] = pkt[24..40].try_into().ok()?;
    Some((Ipv6Addr::from(src), Ipv6Addr::from(dst)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::{MemNetwork, ipv6_packet, mem_packet_io};
    use std::time::Duration;
    use tokio::time::timeout;

    const A: NodeKey = [0xaa; 32];
    const B: NodeKey = [0xbb; 32];

    #[tokio::test]
    async fn packet_round_trips_and_reverse_route_is_learned() {
        let net = MemNetwork::default();
        let a_tp = Arc::new(net.endpoint(A));
        let b_tp = Arc::new(net.endpoint(B));

        let a_table = Arc::new(NeighbourTable::new());
        let b_table = Arc::new(NeighbourTable::new());
        // A already knows B (resolver / invite path); B knows nobody yet.
        a_table.register(B);

        let (a_io, a_host) = mem_packet_io();
        let (b_io, b_host) = mem_packet_io();
        let a_io = Arc::new(a_io);
        let b_io = Arc::new(b_io);

        tokio::spawn(run(A, a_table.clone(), a_io, a_tp));
        tokio::spawn(run(B, b_table.clone(), b_io, b_tp));

        // A → B.
        let payload = b"hello-b";
        a_host
            .emit(ipv6_packet(vip(&A), vip(&B), payload))
            .await
            .expect("emit a->b");
        let got = timeout(Duration::from_secs(1), b_host.next())
            .await
            .expect("b receives in time")
            .expect("b channel open");
        assert_eq!(ipv6_addrs(&got), Some((vip(&A), vip(&B))));
        assert_eq!(&got[40..], payload);

        // B learned the reverse route from that inbound packet.
        assert_eq!(b_table.lookup(&vip(&A)), Some(A));

        // B → A reply now routes via the learned entry.
        let reply = b"hello-a";
        b_host
            .emit(ipv6_packet(vip(&B), vip(&A), reply))
            .await
            .expect("emit b->a");
        let got = timeout(Duration::from_secs(1), a_host.next())
            .await
            .expect("a receives in time")
            .expect("a channel open");
        assert_eq!(&got[40..], reply);
    }

    #[tokio::test]
    async fn spoofed_source_address_is_dropped_but_genuine_traffic_flows() {
        let net = MemNetwork::default();
        let a_tp = Arc::new(net.endpoint(A));
        let b_tp = Arc::new(net.endpoint(B));

        let a_table = Arc::new(NeighbourTable::new());
        let b_table = Arc::new(NeighbourTable::new());
        a_table.register(B);

        let (a_io, a_host) = mem_packet_io();
        let (b_io, b_host) = mem_packet_io();
        let a_io = Arc::new(a_io);
        let b_io = Arc::new(b_io);

        tokio::spawn(run(A, a_table.clone(), a_io, a_tp));
        tokio::spawn(run(B, b_table.clone(), b_io, b_tp));

        // A sends a packet whose source claims to be C. The transport still
        // authenticates the sender as A, so B's integrity check must reject it.
        let c: NodeKey = [0xcc; 32];
        a_host
            .emit(ipv6_packet(vip(&c), vip(&B), b"spoof"))
            .await
            .expect("emit spoof");

        // Nothing is delivered to B's host, and B learns no (false) route for C.
        assert!(
            timeout(Duration::from_millis(200), b_host.next())
                .await
                .is_err()
        );
        assert_eq!(b_table.lookup(&vip(&c)), None);

        // Positive control: a genuine A → B packet on the same live wire IS
        // delivered, proving the spoof was dropped by the integrity check —
        // not by a dead egress path or a wire that was never connected.
        a_host
            .emit(ipv6_packet(vip(&A), vip(&B), b"genuine"))
            .await
            .expect("emit genuine");
        let got = timeout(Duration::from_secs(1), b_host.next())
            .await
            .expect("b receives genuine in time")
            .expect("b channel open");
        assert_eq!(&got[40..], b"genuine");
    }

    #[tokio::test]
    async fn egress_packet_with_no_route_is_dropped() {
        let net = MemNetwork::default();
        let a_tp = Arc::new(net.endpoint(A));
        let b_tp = Arc::new(net.endpoint(B));

        // A's table is empty: it has no route to B.
        let a_table = Arc::new(NeighbourTable::new());
        let b_table = Arc::new(NeighbourTable::new());

        let (a_io, a_host) = mem_packet_io();
        let (b_io, b_host) = mem_packet_io();
        let a_io = Arc::new(a_io);
        let b_io = Arc::new(b_io);

        tokio::spawn(run(A, a_table, a_io, a_tp));
        tokio::spawn(run(B, b_table, b_io, b_tp));

        a_host
            .emit(ipv6_packet(vip(&A), vip(&B), b"no-route"))
            .await
            .expect("emit");
        // No route for vip(B) ⇒ egress drops it; nothing reaches B.
        assert!(
            timeout(Duration::from_millis(200), b_host.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_inbound_packet_is_dropped_and_learns_no_route() {
        let net = MemNetwork::default();
        let b_tp = Arc::new(net.endpoint(B));
        let b_table = Arc::new(NeighbourTable::new());
        let (b_io, b_host) = mem_packet_io();
        let b_io = Arc::new(b_io);
        tokio::spawn(run(B, b_table.clone(), b_io, b_tp));

        // A foreign node injects an unparseable (too-short) datagram. Parsing
        // fails before the spoof check, so it must be dropped AND must not
        // teach B a route — otherwise a malformed packet bypasses the gate.
        let s: NodeKey = [0x55; 32];
        let s_tp = net.endpoint(s);
        s_tp.send(B, &[0u8; 10]).await.expect("inject malformed");

        assert!(
            timeout(Duration::from_millis(200), b_host.next())
                .await
                .is_err()
        );
        assert_eq!(b_table.lookup(&vip(&s)), None);
    }

    #[tokio::test]
    async fn run_returns_when_a_seam_closes() {
        let net = MemNetwork::default();
        let tp = Arc::new(net.endpoint(A));
        let table = Arc::new(NeighbourTable::new());
        let (io, host) = mem_packet_io();
        let io = Arc::new(io);
        let handle = tokio::spawn(run(A, table, io, tp));

        // Dropping the host closes the host→relay queue; egress hits
        // recv() == None and `select!` tears the whole relay down even though
        // the wire stays open. (Under `join!` this would hang.)
        drop(host);
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("run returns after seam close")
            .expect("relay task did not panic");
    }

    #[test]
    fn ipv6_addrs_rejects_short_and_non_ipv6_packets() {
        assert_eq!(ipv6_addrs(&[]), None);
        assert_eq!(ipv6_addrs(&[0x60u8; 39]), None); // one byte short of a header
        let mut v4 = vec![0u8; 40];
        v4[0] = 0x40; // IPv4 version nibble
        assert_eq!(ipv6_addrs(&v4), None);
    }

    #[test]
    fn ipv6_addrs_parses_a_well_formed_packet() {
        let pkt = ipv6_packet(vip(&A), vip(&B), b"payload");
        assert_eq!(ipv6_addrs(&pkt), Some((vip(&A), vip(&B))));
    }
}
