use crate::{
    header::{MessageClass, MessageType, ResponseType as Status},
    packet::Packet,
};

/// The CoAP response.
#[derive(Clone, Debug, PartialEq)]
pub struct CoapResponse {
    pub message: Packet,
}

impl CoapResponse {
    /// Creates a new response.
    pub fn new(request: &Packet) -> Option<CoapResponse> {
        let mut packet = Packet::new();

        packet.header.set_version(1);
        let response_type = match request.header.get_type() {
            MessageType::Confirmable => MessageType::Acknowledgement,
            MessageType::NonConfirmable => MessageType::NonConfirmable,
            _ => return None,
        };
        packet.header.set_type(response_type);
        packet.header.code = MessageClass::Response(Status::Content);
        packet.header.message_id = request.header.message_id;
        packet.set_token(request.get_token().to_vec());

        Some(CoapResponse { message: packet })
    }

    /// Sets the status.
    pub fn set_status(&mut self, status: Status) {
        self.message.header.code = MessageClass::Response(status);
    }

    /// Returns the status.
    pub fn get_status(&self) -> &Status {
        match self.message.header.code {
            MessageClass::Response(ref code) => code,
            _ => &Status::UnKnown,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new_response_valid() {
        for mtyp in [MessageType::Confirmable, MessageType::NonConfirmable] {
            let mut packet = Packet::new();
            packet.header.set_type(mtyp);
            let opt_resp = CoapResponse::new(&packet);
            assert!(opt_resp.is_some());

            let response = opt_resp.unwrap();
            assert_eq!(packet.payload, response.message.payload);
        }
    }

    #[test]
    fn test_new_response_invalid() {
        let mut packet = Packet::new();
        packet.header.set_type(MessageType::Acknowledgement);
        assert!(CoapResponse::new(&packet).is_none());
    }
}
