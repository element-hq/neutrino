use std::net::Ipv6Addr;
use std::sync::Arc;

use tracing::{trace, warn};

use crate::neighbour::NeighbourTable;
use crate::packet_io::PacketIo;
use crate::transport::DatagramTransport;
use crate::vip::vip;

/// Run the relay until both seams close.
///
/// Concurrently pumps the two directions — [`egress`] (host → wire) and
/// [`ingress`] (wire → host) — sharing one neighbour table. Returns once the
/// [`PacketIo`] and [`DatagramTransport`] are both exhausted (their queues
/// closed): that is how a clean shutdown propagates — the embedding layer drops
/// the seams and `run` falls out.
pub async fn run<P, T>(table: Arc<NeighbourTable>, io: Arc<P>, transport: Arc<T>)
where
    P: PacketIo + 'static,
    T: DatagramTransport + 'static,
{
    tokio::join!(
        egress(&table, io.as_ref(), transport.as_ref()),
        ingress(&table, io.as_ref(), transport.as_ref()),
    );
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

/// Wire → host. The transport authenticates each datagram's source node; we
/// additionally assert the packet's own source address is that node's vip, so a
/// peer cannot inject packets spoofing another node's address. Only after the
/// check passes do we learn the reverse route and deliver — a failed packet
/// must not poison the table.
async fn ingress<P: PacketIo, T: DatagramTransport>(table: &NeighbourTable, io: &P, transport: &T) {
    while let Some((src_node, pkt)) = transport.recv().await {
        let Some((src_ip, _dst)) = ipv6_addrs(&pkt) else {
            trace!("ingress: dropped non-IPv6 / short packet");
            continue;
        };
        let expected = vip(&src_node);
        if src_ip != expected {
            warn!(%src_ip, %expected, "ingress: source-address spoof, dropped");
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
    use crate::NodeKey;
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

        tokio::spawn(run(a_table.clone(), a_io, a_tp));
        tokio::spawn(run(b_table.clone(), b_io, b_tp));

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
    async fn spoofed_source_address_is_dropped_and_not_learned() {
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

        tokio::spawn(run(a_table.clone(), a_io, a_tp));
        tokio::spawn(run(b_table.clone(), b_io, b_tp));

        // A sends a packet whose source claims to be C. The transport still
        // authenticates the sender as A, so B's integrity check must reject it.
        let c: NodeKey = [0xcc; 32];
        a_host
            .emit(ipv6_packet(vip(&c), vip(&B), b"spoof"))
            .await
            .expect("emit spoof");

        // Nothing is delivered to B's host...
        assert!(
            timeout(Duration::from_millis(200), b_host.next())
                .await
                .is_err()
        );
        // ...and B did not learn a (false) route for C.
        assert_eq!(b_table.lookup(&vip(&c)), None);
    }
}
