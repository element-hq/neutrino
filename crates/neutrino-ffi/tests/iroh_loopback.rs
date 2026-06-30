// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! P0a de-risk for the iroh-over-BLE federation work.
//!
//! Proves the iroh primitives the packet relay will rely on work over a direct
//! loopback link, with no Bluetooth and no mDNS in the loop:
//!
//! 1. bring up two endpoints (default IP transport, relay disabled),
//! 2. connect by an **explicit** `EndpointAddr` (not service discovery, so the
//!    test is deterministic in CI — multicast mDNS is validated separately),
//! 3. round-trip an **unreliable datagram** in both directions and confirm the
//!    authenticated remote node id.
//!
//! We bet on datagrams (not streams) for the relay: CoAP/UDP provides its own
//! reliability, so one IP packet maps to one iroh datagram. This test is the
//! seed of the P2 relay flow test; it exercises iroh only, no neutrino code.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use iroh::endpoint::presets::N0DisableRelay;
use iroh::{Endpoint, EndpointAddr, SecretKey};

/// ALPN for the federation packet relay (placeholder until P2 names it).
const ALPN: &[u8] = b"neutrino/iroh-relay/0";

/// A timeout bounding each network step so a hang fails loudly instead of
/// stalling the suite.
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

async fn bind_loopback() -> Endpoint {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    Endpoint::builder(N0DisableRelay)
        .secret_key(SecretKey::generate())
        .alpns(vec![ALPN.to_vec()])
        .bind_addr(addr)
        .expect("loopback bind addr accepted")
        .bind()
        .await
        .expect("endpoint binds")
}

#[tokio::test]
async fn iroh_datagram_roundtrip_over_loopback() {
    let listener = bind_loopback().await;
    let dialer = bind_loopback().await;

    let listener_id = listener.id();
    let listener_sock = *listener
        .bound_sockets()
        .iter()
        .find(|s| s.ip().is_loopback())
        .expect("a loopback bound socket");

    // Listener: accept one connection and echo the first datagram straight back,
    // then stay alive until the dialer closes so the echo flushes.
    let server = tokio::spawn(async move {
        let incoming = listener.accept().await.expect("an incoming connection");
        let conn = incoming.await.expect("connection accepted");
        let dgram = conn.read_datagram().await.expect("inbound datagram");
        conn.send_datagram(dgram).expect("echo datagram queued");
        // Resolves when the dialer drops its end; keeps the driver flushing.
        conn.closed().await;
    });

    // Connect by explicit address — no mDNS, no relay.
    let addr = EndpointAddr::new(listener_id).with_ip_addr(listener_sock);
    let conn = tokio::time::timeout(STEP_TIMEOUT, dialer.connect(addr, ALPN))
        .await
        .expect("connect within timeout")
        .expect("connection established");

    assert_eq!(
        conn.remote_id(),
        listener_id,
        "remote node id is authenticated by the connection"
    );
    assert!(
        conn.max_datagram_size().unwrap_or(0) > 0,
        "path advertises datagram support"
    );

    let payload = Bytes::from_static(b"neutrino federation packet");
    conn.send_datagram(payload.clone()).expect("send datagram");
    let echoed = tokio::time::timeout(STEP_TIMEOUT, conn.read_datagram())
        .await
        .expect("echo within timeout")
        .expect("read echoed datagram");
    assert_eq!(
        echoed, payload,
        "datagram round-trips intact in both directions"
    );

    drop(conn);
    tokio::time::timeout(STEP_TIMEOUT, server)
        .await
        .expect("listener task finishes")
        .expect("listener task does not panic");
}
