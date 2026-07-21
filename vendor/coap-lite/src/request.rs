use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::convert::TryFrom;

use crate::{
    error::{HandlingError, IncompatibleOptionValueFormat, InvalidObserve},
    header::{MessageClass, RequestType as Method},
    option_value::OptionValueString,
    packet::{CoapOption, ObserveOption, Packet},
    response::CoapResponse,
    ContentFormat,
};

/// The CoAP request.
#[derive(Clone, Debug, PartialEq)]
pub struct CoapRequest<Endpoint> {
    pub message: Packet,
    pub response: Option<CoapResponse>,
    pub source: Option<Endpoint>,
}

impl<Endpoint> CoapRequest<Endpoint> {
    /// Creates a new request.
    pub fn new() -> CoapRequest<Endpoint> {
        Default::default()
    }

    /// Creates a request from a packet.
    pub fn from_packet(
        packet: Packet,
        source: Endpoint,
    ) -> CoapRequest<Endpoint> {
        CoapRequest {
            response: CoapResponse::new(&packet),
            message: packet,
            source: Some(source),
        }
    }

    /// Applies the given error to the request and returns true if that was
    /// successful.
    pub fn apply_from_error(&mut self, error: HandlingError) -> bool {
        if let Some(reply) = &mut self.response {
            if let Some(code) = error.code {
                let message = &mut reply.message;
                message.header.code = MessageClass::Response(code);
                message.set_content_format(ContentFormat::TextPlain);
                message.payload = error.message.into_bytes();
                return true;
            }
        }
        false
    }

    /// Sets the method.
    pub fn set_method(&mut self, method: Method) {
        self.message.header.code = MessageClass::Request(method);
    }

    /// Returns the method.
    pub fn get_method(&self) -> &Method {
        match self.message.header.code {
            MessageClass::Request(Method::Get) => &Method::Get,
            MessageClass::Request(Method::Post) => &Method::Post,
            MessageClass::Request(Method::Put) => &Method::Put,
            MessageClass::Request(Method::Delete) => &Method::Delete,
            MessageClass::Request(Method::Fetch) => &Method::Fetch,
            MessageClass::Request(Method::Patch) => &Method::Patch,
            MessageClass::Request(Method::IPatch) => &Method::IPatch,
            _ => &Method::UnKnown,
        }
    }

    /// Sets the path.
    pub fn set_path(&mut self, path: &str) {
        self.message.clear_option(CoapOption::UriPath);

        let segs = path.split('/');
        for (i, s) in segs.enumerate() {
            if i == 0 && s.is_empty() {
                continue;
            }

            self.message
                .add_option(CoapOption::UriPath, s.as_bytes().to_vec());
        }
    }

    /// Returns the path.
    pub fn get_path(&self) -> String {
        match self.message.get_option(CoapOption::UriPath) {
            Some(options) => {
                let mut vec = Vec::new();
                for option in options.iter() {
                    if let Ok(seg) = core::str::from_utf8(option) {
                        vec.push(seg);
                    }
                }
                vec.join("/")
            }
            _ => "".to_string(),
        }
    }

    /// Returns the path as a vector (as it is encoded in CoAP rather than in
    /// HTTP-style paths).
    pub fn get_path_as_vec(
        &self,
    ) -> Result<Vec<String>, IncompatibleOptionValueFormat> {
        self.message
            .get_options_as::<OptionValueString>(CoapOption::UriPath)
            .map_or_else(
                || Ok(vec![]),
                |paths| {
                    paths
                        .into_iter()
                        .map(|segment_result| {
                            segment_result.map(|segment| segment.0)
                        })
                        .collect::<Result<Vec<_>, _>>()
                },
            )
    }

    /// Returns the flag in the Observe option or InvalidObserve if the flag
    /// was provided but not understood.
    pub fn get_observe_flag(
        &self,
    ) -> Option<Result<ObserveOption, InvalidObserve>> {
        self.message.get_observe_value().map(|observe| {
            observe
                .map(|value| usize::try_from(value).unwrap())
                .map_or(Err(InvalidObserve), |value| {
                    ObserveOption::try_from(value)
                })
        })
    }

    /// Sets the flag in the Observe option.
    pub fn set_observe_flag(&mut self, flag: ObserveOption) {
        let value = u32::try_from(usize::from(flag)).unwrap();
        self.message.set_observe_value(value);
    }
}

impl<Endpoint> Default for CoapRequest<Endpoint> {
    fn default() -> Self {
        CoapRequest {
            response: None,
            message: Packet::new(),
            source: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::header::MessageType;

    struct Endpoint(String);

    #[test]
    fn test_request_create() {
        let mut packet = Packet::new();
        let mut request1: CoapRequest<Endpoint> = CoapRequest::new();

        packet.set_token(vec![0x17, 0x38]);
        request1.message.set_token(vec![0x17, 0x38]);

        packet.add_option(CoapOption::UriPath, b"test-interface".to_vec());
        request1
            .message
            .add_option(CoapOption::UriPath, b"test-interface".to_vec());

        packet.header.message_id = 42;
        request1.message.header.message_id = 42;

        packet.header.set_version(2);
        request1.message.header.set_version(2);

        packet.header.set_type(MessageType::Confirmable);
        request1.message.header.set_type(MessageType::Confirmable);

        packet.header.set_code("0.04");
        request1.message.header.set_code("0.04");

        let endpoint = Endpoint(String::from("127.0.0.1:1234"));
        let request2 = CoapRequest::from_packet(packet, endpoint);

        assert_eq!(
            request1.message.to_bytes().unwrap(),
            request2.message.to_bytes().unwrap()
        );
    }

    #[test]
    fn test_method() {
        let mut request: CoapRequest<Endpoint> = CoapRequest::new();

        request.message.header.set_code("0.01");
        assert_eq!(&Method::Get, request.get_method());

        request.message.header.set_code("0.02");
        assert_eq!(&Method::Post, request.get_method());

        request.message.header.set_code("0.03");
        assert_eq!(&Method::Put, request.get_method());

        request.message.header.set_code("0.04");
        assert_eq!(&Method::Delete, request.get_method());

        request.message.header.set_code("0.06");
        assert_eq!(&Method::Patch, request.get_method());

        request.set_method(Method::Get);
        assert_eq!("0.01", request.message.header.get_code());

        request.set_method(Method::Post);
        assert_eq!("0.02", request.message.header.get_code());

        request.set_method(Method::Put);
        assert_eq!("0.03", request.message.header.get_code());

        request.set_method(Method::Delete);
        assert_eq!("0.04", request.message.header.get_code());

        request.set_method(Method::IPatch);
        assert_eq!("0.07", request.message.header.get_code());
    }

    #[test]
    fn test_path() {
        let mut request: CoapRequest<Endpoint> = CoapRequest::new();

        let path = "test-interface";
        request
            .message
            .add_option(CoapOption::UriPath, path.as_bytes().to_vec());
        assert_eq!(path, request.get_path());

        let path2 = "test-interface2";
        request.set_path(path2);
        assert_eq!(
            path2.as_bytes().to_vec(),
            *request
                .message
                .get_option(CoapOption::UriPath)
                .unwrap()
                .front()
                .unwrap()
        );

        request.set_path("/test-interface2");
        assert_eq!(
            path2.as_bytes().to_vec(),
            *request
                .message
                .get_option(CoapOption::UriPath)
                .unwrap()
                .front()
                .unwrap()
        );

        let path3 = "test-interface2/";
        request.set_path(path3);
        assert_eq!(path3, request.get_path());
    }

    #[test]
    fn test_path_as_vec() {
        let mut request: CoapRequest<Endpoint> = CoapRequest::new();

        let path = "test-interface";
        request
            .message
            .add_option(CoapOption::UriPath, path.as_bytes().to_vec());
        assert_eq!(Ok(vec![path.to_string()]), request.get_path_as_vec());

        request.set_path("/test-interface/second/third");
        assert_eq!(
            Ok(["test-interface", "second", "third"]
                .map(|x| x.to_string())
                .to_vec()),
            request.get_path_as_vec()
        );

        let bogus_path: Vec<u8> = vec![0xfe, 0xfe, 0xff, 0xff];
        request.message.clear_option(CoapOption::UriPath);
        request.message.add_option(CoapOption::UriPath, bogus_path);
        request
            .get_path_as_vec()
            .expect_err("must be a utf-8 decoding error");
    }

    #[test]
    fn test_unknown_observe_flag() {
        let mut request: CoapRequest<Endpoint> = CoapRequest::new();

        request.message.set_observe_value(32);
        let expected = Some(Err(InvalidObserve));
        let actual = request.get_observe_flag();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_garbage_in_observe_field() {
        let mut request: CoapRequest<Endpoint> = CoapRequest::new();

        request
            .message
            .add_option(CoapOption::Observe, b"bunch of nonsense".to_vec());
        let expected = Some(Err(InvalidObserve));
        let actual = request.get_observe_flag();
        assert_eq!(actual, expected);
    }
}
