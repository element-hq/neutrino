# coap-lite

[![Latest version](https://img.shields.io/crates/v/coap-lite)](https://crates.io/crates/coap-lite)
[![Documentation](https://docs.rs/coap-lite/badge.svg)](https://docs.rs/coap-lite)
[![License](https://img.shields.io/crates/l/coap-lite)](https://github.com/martindisch/coap-lite#license)

<!-- cargo-sync-readme start -->

A lightweight low-level CoAP message manipulation crate.

Its goal is to be compliant with the CoAP standards and to provide a
building block for libraries (e.g.
[coap](https://github.com/Covertness/coap-rs)) and applications.

`coap-lite` supports `#![no_std]` and embedded environments.

It was originally based on the improved low-level message handling code
from the [coap] crate as well as [rust-async-coap], made to work in bare
metal environments.

## Supported RFCs

- CoAP [RFC 7252](https://tools.ietf.org/html/rfc7252)
- CoAP Observe Option [RFC 7641](https://tools.ietf.org/html/rfc7641)
- Too Many Requests Response Code [RFC 8516](https://tools.ietf.org/html/rfc8516)
- Block-Wise Transfers [RFC 7959](https://tools.ietf.org/html/rfc7959)
- Constrained RESTful Environments (CoRE) Link Format
  [RFC6690](https://tools.ietf.org/html/rfc6690#:~:text=well-known%2Fcore)

## Usage

This crate provides several types that can be used to build, modify and
encode/decode CoAP messages to/from their byte representation.

**Note for no_std users**: it does require allocation, so you might have to
set a global allocator depending on your target.

### Client

The following example uses `std::net::UdpSocket` to send the UDP packet but
you can use anything, e.g. [smoltcp](https://github.com/smoltcp-rs/smoltcp)
for embedded.

```rust
use coap_lite::{
    CoapRequest, RequestType as Method
};
use std::net::{SocketAddr, UdpSocket};

fn main() {
    let mut request: CoapRequest<SocketAddr> = CoapRequest::new();

    request.set_method(Method::Get);
    request.set_path("/test");

    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    let packet = request.message.to_bytes().unwrap();
    socket.send_to(&packet[..], "127.0.0.1:5683").expect("Could not send the data");
}
```

### Server

```rust
use coap_lite::{CoapRequest, Packet};
use std::net::{UdpSocket};

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:5683").unwrap();
    let mut buf = [0; 100];
    let (size, src) = socket.recv_from(&mut buf).expect("Didn't receive data");

    println!("Payload {:x?}", &buf[..size]);

    let packet = Packet::from_bytes(&buf[..size]).unwrap();
    let request = CoapRequest::from_packet(packet, src);

    let method = request.get_method().clone();
    let path = request.get_path();

    println!("Received CoAP request '{:?} {}' from {}", method, path, src);

    let mut response = request.response.unwrap();
    response.message.payload = b"OK".to_vec();

    let packet = response.message.to_bytes().unwrap();
    socket.send_to(&packet[..], &src).expect("Could not send the data");
}
```

### Low-level binary conversion

```rust
use coap_lite::{
    CoapOption, MessageClass, MessageType,
    Packet, RequestType, ResponseType,
};

let mut request = Packet::new();
request.header.message_id = 23839;
request.header.code = MessageClass::Request(RequestType::Get);
request.set_token(vec![0, 0, 57, 116]);
request.add_option(CoapOption::UriHost, b"localhost".to_vec());
request.add_option(CoapOption::UriPath, b"tv1".to_vec());
assert_eq!(
    [
        0x44, 0x01, 0x5D, 0x1F, 0x00, 0x00, 0x39, 0x74, 0x39, 0x6C, 0x6F,
        0x63, 0x61, 0x6C, 0x68, 0x6F, 0x73, 0x74, 0x83, 0x74, 0x76, 0x31,
    ],
    request.to_bytes().unwrap()[..]
);

let response = Packet::from_bytes(&[
    0x64, 0x45, 0x5D, 0x1F, 0x00, 0x00, 0x39, 0x74, 0xFF, 0x48, 0x65,
    0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
])
.unwrap();
assert_eq!(23839, response.header.message_id);
assert_eq!(
    MessageClass::Response(ResponseType::Content),
    response.header.code
);
assert_eq!(MessageType::Acknowledgement, response.header.get_type());
assert_eq!([0, 0, 57, 116], response.get_token()[..]);
assert_eq!(b"Hello World!", &response.payload[..]);
```

[coap]: https://github.com/covertness/coap-rs
[rust-async-coap]: https://github.com/google/rust-async-coap

<!-- cargo-sync-readme end -->

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT license](LICENSE-MIT)

at your option.

This is a modification of the [coap](https://github.com/covertness/coap-rs)
and [rust-async-coap](https://github.com/google/rust-async-coap) crates, their
licenses are in [LICENSE-3RD-PARTY](LICENSE-3RD-PARTY).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
