use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use core::{fmt::Display, marker::PhantomData};

use crate::{request::CoapRequest, MessageClass, MessageType, Packet};

const DEFAULT_UNACKNOWLEDGED_LIMIT: u8 = 10;

type ResourcePath = String;

/// An observer client.
pub struct Observer<Endpoint: Display> {
    pub endpoint: Endpoint,
    pub token: Vec<u8>,
    unacknowledged_messages: u8,
    // The message id of the last update to be acknowledged
    message_id: Option<u16>,
}

/// An observed resource.
pub struct Resource<Endpoint: Display> {
    pub observers: Vec<Observer<Endpoint>>,
    pub sequence: u32,
}

/// Keeps track of the state of the observed resources.
pub struct Subject<Endpoint: Display + PartialEq> {
    resources: BTreeMap<ResourcePath, Resource<Endpoint>>,
    unacknowledged_limit: u8,
    // The Endpoint generic is needed internally for CoapRequest, but not as an
    // actual field for this struct
    phantom: PhantomData<Endpoint>,
}

impl<Endpoint: Display + PartialEq + Clone> Subject<Endpoint> {
    /// Registers an observer interested in a resource.
    pub fn register(&mut self, request: &CoapRequest<Endpoint>) {
        let observer_endpoint = request.source.as_ref().unwrap();
        let resource_path = request.get_path();
        let token = request.message.get_token();

        let observer = Observer {
            endpoint: observer_endpoint.clone(),
            token: token.to_vec(),
            unacknowledged_messages: 0,
            message_id: None,
        };

        coap_info!(
            "Registering observer {} for resource {}",
            observer_endpoint,
            resource_path
        );

        let resource =
            self.resources.entry(resource_path).or_insert(Resource {
                observers: Vec::new(),
                sequence: 0,
            });

        if let Some(position) = resource
            .observers
            .iter()
            .position(|x| x.endpoint == observer.endpoint)
        {
            resource.observers[position] = observer;
        } else {
            resource.observers.push(observer);
        }
    }

    /// Removes an observer from the interested resource.
    pub fn deregister(&mut self, request: &CoapRequest<Endpoint>) {
        let observer_endpoint = request.source.as_ref().unwrap();
        let resource_path = request.get_path();
        let token = request.message.get_token();

        if let Some(resource) = self.resources.get_mut(&resource_path) {
            let position = resource.observers.iter().position(|x| {
                x.endpoint == *observer_endpoint && x.token == *token
            });

            if let Some(position) = position {
                coap_info!(
                    "Deregistering observer {} for resource {}",
                    observer_endpoint,
                    resource_path
                );

                resource.observers.remove(position);
            }
        }
    }

    /// Updates the resource information after having notified the observers.
    ///
    /// It increments the resource sequence and counter of unacknowledged
    /// updates (the latter only if `is_confirmable` is `true`).
    pub fn resource_changed(
        &mut self,
        resource: &str,
        message_id: u16,
        is_confirmable: bool,
    ) {
        let unacknowledged_limit = self.unacknowledged_limit;
        coap_debug!("Resource changed");

        self.resources
            .entry(resource.to_string())
            .and_modify(|resource| {
                resource.sequence += 1;

                resource.observers.iter_mut().for_each(|observer| {
                    observer.message_id = Some(message_id);
                    if is_confirmable {
                        observer.unacknowledged_messages += 1;
                    }
                });

                resource.observers.retain(|observer| {
                    observer.unacknowledged_messages <= unacknowledged_limit
                });
            });
    }

    /// Resets the counter of unacknowledged updates for a resource observer.
    pub fn acknowledge(&mut self, request: &CoapRequest<Endpoint>) {
        let observer_endpoint = request.source.as_ref().unwrap();
        let message_id = request.message.header.message_id;

        for (resource_path, resource) in self.resources.iter_mut() {
            let observer = resource.observers.iter_mut().find(|x| {
                if let Some(observe_msg_id) = x.message_id {
                    // Unacknowledgement doesn't officially require the token
                    // to be passed in the ACK so it's not checked
                    return x.endpoint == *observer_endpoint
                        && observe_msg_id == message_id;
                }

                false
            });

            if let Some(observer) = observer {
                coap_debug!("Received ack for resource {}", resource_path);

                observer.unacknowledged_messages = 0;
                observer.message_id = None;
            }
        }
    }

    /// Gets the tracked resources.
    pub fn get_resource(&self, resource: &str) -> Option<&Resource<Endpoint>> {
        self.resources.get(resource)
    }

    /// Gets the observers of a resource.
    pub fn get_resource_observers(
        &self,
        resource: &str,
    ) -> Option<Vec<&Observer<Endpoint>>> {
        self.resources
            .get(resource)
            .map(|resource| resource.observers.iter().collect())
    }

    /// Sets the limit of unacknowledged updates before removing an observer.
    pub fn set_unacknowledged_limit(&mut self, limit: u8) {
        self.unacknowledged_limit = limit;
    }
}

/// Creates a notification response for notifying observers about an update.
pub fn create_notification(
    message_id: u16,
    token: Vec<u8>,
    sequence: u32,
    payload: Vec<u8>,
    is_confirmable: bool,
) -> Packet {
    let mut packet = Packet::new();

    packet.header.set_version(1);
    packet.header.set_type(if is_confirmable {
        MessageType::Confirmable
    } else {
        MessageType::NonConfirmable
    });
    packet.header.code = MessageClass::Response(crate::ResponseType::Content);
    packet.header.message_id = message_id;
    packet.set_token(token);
    packet.payload = payload;
    packet.set_observe_value(sequence);

    packet
}

impl<Endpoint: Display + PartialEq + Clone> Default for Subject<Endpoint> {
    fn default() -> Self {
        Subject {
            resources: BTreeMap::new(),
            unacknowledged_limit: DEFAULT_UNACKNOWLEDGED_LIMIT,
            phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        header::{MessageType, RequestType as Method},
        packet::ObserveOption,
    };

    type Endpoint = String;

    #[test]
    fn register() {
        let resource_path = "temp";

        let mut request = CoapRequest::new();
        request.source = Some(String::from("0.0.0.0"));
        request.set_method(Method::Get);
        request.set_path(resource_path);
        request.message.set_token(vec![0x7d, 0x34]);
        request.set_observe_flag(ObserveOption::Register);

        let mut subject: Subject<Endpoint> = Subject::default();
        subject.register(&request);

        let observers = subject.get_resource_observers(resource_path).unwrap();

        assert_eq!(observers.len(), 1);
    }

    #[test]
    fn register_replace() {
        let resource_path = "temp";

        let mut request1 = CoapRequest::new();
        request1.source = Some(String::from("0.0.0.0"));
        request1.set_method(Method::Get);
        request1.set_path(resource_path);
        request1.message.set_token(vec![0x00, 0x00]);
        request1.set_observe_flag(ObserveOption::Register);

        let mut request2 = CoapRequest::new();
        request2.source = Some(String::from("0.0.0.0"));
        request2.set_method(Method::Get);
        request2.set_path(resource_path);
        request2.message.set_token(vec![0xff, 0xff]);
        request2.set_observe_flag(ObserveOption::Register);

        let mut subject: Subject<Endpoint> = Subject::default();
        subject.register(&request1);
        subject.register(&request2);

        let observers = subject.get_resource_observers(resource_path).unwrap();

        assert_eq!(observers.len(), 1);

        let observer = observers.first().unwrap();

        assert_eq!(observer.token, vec![0xff, 0xff]);
    }

    #[test]
    fn ack_flow_ok() {
        let resource_path = "temp";

        let mut request1 = CoapRequest::new();
        request1.source = Some(String::from("0.0.0.0"));
        request1.set_method(Method::Get);
        request1.set_path(resource_path);
        request1.message.set_token(vec![0x00, 0x00]);
        request1.set_observe_flag(ObserveOption::Register);

        let mut subject: Subject<Endpoint> = Subject::default();
        subject.register(&request1);

        let sequence1 = subject.get_resource(resource_path).unwrap().sequence;
        subject.resource_changed(resource_path, 1, true);
        let sequence2 = subject.get_resource(resource_path).unwrap().sequence;

        assert!(sequence2 > sequence1);

        {
            let observers =
                subject.get_resource_observers(resource_path).unwrap();
            let observer = observers.first().unwrap();

            assert_eq!(observer.unacknowledged_messages, 1);
        }

        let mut ack = CoapRequest::new();
        ack.source = Some(String::from("0.0.0.0"));
        ack.message.header.set_type(MessageType::Acknowledgement);
        ack.set_path(resource_path);
        ack.message.set_token(vec![0x00, 0x00]);
        ack.message.header.message_id = 1;

        subject.acknowledge(&ack);

        {
            let observers =
                subject.get_resource_observers(resource_path).unwrap();
            let observer = observers.first().unwrap();

            assert_eq!(observer.unacknowledged_messages, 0);
        }
    }

    #[test]
    fn ack_flow_forget_observer() {
        let resource_path = "temp";

        let mut request1 = CoapRequest::new();
        request1.source = Some(String::from("0.0.0.0"));
        request1.set_method(Method::Get);
        request1.set_path(resource_path);
        request1.message.set_token(vec![0x00, 0x00]);
        request1.set_observe_flag(ObserveOption::Register);

        let mut subject: Subject<Endpoint> = Subject::default();
        subject.set_unacknowledged_limit(5);
        subject.register(&request1);

        subject.resource_changed(resource_path, 1, true);
        subject.resource_changed(resource_path, 2, true);
        subject.resource_changed(resource_path, 3, true);
        subject.resource_changed(resource_path, 4, true);
        subject.resource_changed(resource_path, 5, true);
        subject.resource_changed(resource_path, 6, true);

        let observers = subject.get_resource_observers(resource_path).unwrap();

        assert_eq!(observers.len(), 0);
    }

    #[test]
    fn combine_ack_and_no_ack() {
        let resource_path = "temp";

        let mut request1 = CoapRequest::new();
        request1.source = Some(String::from("0.0.0.0"));
        request1.set_method(Method::Get);
        request1.set_path(resource_path);
        request1.message.set_token(vec![0x00, 0x00]);
        request1.set_observe_flag(ObserveOption::Register);

        let mut subject: Subject<Endpoint> = Subject::default();
        subject.set_unacknowledged_limit(2);
        subject.register(&request1);

        subject.resource_changed(resource_path, 1, false);
        subject.resource_changed(resource_path, 2, false);
        subject.resource_changed(resource_path, 3, false);

        let observers = subject.get_resource_observers(resource_path).unwrap();
        assert_eq!(observers.len(), 1);

        subject.resource_changed(resource_path, 4, true);
        subject.resource_changed(resource_path, 5, true);
        subject.resource_changed(resource_path, 6, true);

        let observers = subject.get_resource_observers(resource_path).unwrap();
        assert_eq!(observers.len(), 0);
    }
}
