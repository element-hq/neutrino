//! v2 CoAP/UDP wire transport. `CoapWireClient` (egress→peer) and
//! `CoapWireServer` (peer→ingress), a sibling of `transport::http`, selected in
//! `crate::serve`. The codec stays opaque: this transport carries the CBOR body
//! verbatim and never inspects it.

// `allow(dead_code)` is removed in the task that first reads these (message.rs).
/// Exact HTTP status, carried as 2 big-endian bytes (CoAP response codes are not
/// 1:1 with HTTP, and federation needs the precise code).
#[allow(dead_code)]
pub(crate) const OPT_HTTP_STATUS: u16 = 2048;
/// One forwarded header per occurrence: `name` + 0x00 + `value`.
#[allow(dead_code)]
pub(crate) const OPT_FWD_HEADER: u16 = 2050;
/// `application/cbor` (RFC 8949 §9.1).
#[allow(dead_code)]
pub(crate) const CBOR_CONTENT_FORMAT: u16 = 60;

#[cfg(test)]
mod smoke_tests {
    use coap::Server;
    use coap::UdpCoAPClient;
    use coap_lite::{CoapRequest, RequestType};
    use std::net::SocketAddr;

    // Confirms the coap-rs 0.27 client+server API and a basic CON round-trip.
    // If this compiles and passes, the signatures the later tasks rely on hold.
    // `Server` exposes no bound-address accessor, so grab a free port with a
    // probe socket, drop it, and bind the server there.
    #[tokio::test]
    async fn coap_rs_client_server_roundtrip() {
        let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let server = Server::new_udp(addr).expect("bind udp server");

        tokio::spawn(async move {
            server
                .run(|mut request: Box<CoapRequest<SocketAddr>>| async move {
                    if let Some(ref mut resp) = request.response {
                        resp.message.payload = b"pong".to_vec();
                    }
                    request
                })
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = UdpCoAPClient::new(addr).await.expect("client");
        let mut req: CoapRequest<SocketAddr> = CoapRequest::new();
        req.set_method(RequestType::Get);
        req.set_path("/ping");
        let resp = client.send(req).await.expect("send");
        assert_eq!(resp.message.payload, b"pong");
    }
}
