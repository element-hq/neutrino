use coap_lite::{CoapRequest, RequestType as Method};
use std::net::{SocketAddr, UdpSocket};

fn main() {
    let mut request: CoapRequest<SocketAddr> = CoapRequest::new();

    request.set_method(Method::Get);
    request.set_path("/test");

    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    let packet = request.message.to_bytes().unwrap();
    socket
        .send_to(&packet[..], "127.0.0.1:5683")
        .expect("Could not send the data");
}
