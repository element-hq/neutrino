use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType, ResponseType,
};

fn main() {
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
}
