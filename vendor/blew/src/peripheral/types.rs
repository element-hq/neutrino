use crate::types::DeviceId;
use tokio::sync::oneshot;
use uuid::Uuid;

/// Configuration for initialising the peripheral role.
#[derive(Debug, Clone, Default)]
pub struct PeripheralConfig {
    /// On Apple platforms, passed as `CBPeripheralManagerOptionRestoreIdentifierKey` to
    /// `initWithDelegate:queue:options:`, enabling state restoration for the app's
    /// background BLE peripheral session. Ignored on all other platforms.
    ///
    /// See the crate-level "State restoration" docs for the iOS usage contract
    /// (entitlements, event-drain rules, L2CAP re-open requirement).
    pub restore_identifier: Option<String>,
}

/// Non-request events from the peripheral role. Clone-able; multiple subscribers welcome.
#[derive(Debug, Clone)]
pub enum PeripheralStateEvent {
    /// The local Bluetooth adapter was powered on or off.
    AdapterStateChanged { powered: bool },

    /// A remote central subscribed to or unsubscribed from a characteristic's notifications.
    SubscriptionChanged {
        client_id: DeviceId,
        char_uuid: Uuid,
        subscribed: bool,
    },
}

/// Inbound GATT requests from remote centrals.
///
/// Each variant carries an owned responder that must be consumed exactly once,
/// or dropped (which sends an ATT Application Error automatically). Single-consumer:
/// the request stream is handed out via
/// [`Peripheral::take_requests`](crate::peripheral::Peripheral::take_requests).
#[derive(Debug)]
pub enum PeripheralRequest {
    /// A remote central sent an ATT Read Request.
    Read {
        client_id: DeviceId,
        service_uuid: Uuid,
        char_uuid: Uuid,
        offset: u16,
        responder: ReadResponder,
    },

    /// A remote central sent an ATT Write Request or Write Command.
    /// `responder` is `Some` for Write Request (needs an ATT response) and
    /// `None` for Write Without Response.
    Write {
        client_id: DeviceId,
        service_uuid: Uuid,
        char_uuid: Uuid,
        value: Vec<u8>,
        responder: Option<WriteResponder>,
    },
}

pub(crate) type ReadResponseTx = oneshot::Sender<Result<Vec<u8>, ()>>;
pub(crate) type WriteResponseTx = oneshot::Sender<bool>;

#[derive(Debug)]
struct ResponderInner<T: Send + Clone + 'static> {
    tx: Option<oneshot::Sender<T>>,
    error_value: T,
}

impl<T: Send + Clone + 'static> ResponderInner<T> {
    fn new(tx: oneshot::Sender<T>, error_value: T) -> Self {
        Self {
            tx: Some(tx),
            error_value,
        }
    }

    fn send(&mut self, value: T) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(value);
        }
    }
}

impl<T: Send + Clone + 'static> Drop for ResponderInner<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(self.error_value.clone());
        }
    }
}

/// Owned handle for responding to a GATT read request.
///
/// Call [`respond`](Self::respond) to send a value or [`error`](Self::error) to send an ATT
/// Application Error. If this is dropped without being consumed, an ATT Application Error is
/// sent automatically.
#[derive(Debug)]
pub struct ReadResponder(ResponderInner<Result<Vec<u8>, ()>>);

impl ReadResponder {
    pub(crate) fn new(tx: ReadResponseTx) -> Self {
        Self(ResponderInner::new(tx, Err(())))
    }

    /// Send `value` as the read response.
    pub fn respond(mut self, value: Vec<u8>) {
        self.0.send(Ok(value));
    }

    /// Send an ATT Application Error response.
    pub fn error(mut self) {
        self.0.send(Err(()));
    }
}

/// Owned handle for acknowledging a GATT write request.
///
/// Call [`success`](Self::success) or [`error`](Self::error). Dropping without consuming
/// sends an error response automatically.
#[derive(Debug)]
pub struct WriteResponder(ResponderInner<bool>);

impl WriteResponder {
    pub(crate) fn new(tx: WriteResponseTx) -> Self {
        Self(ResponderInner::new(tx, false))
    }

    /// Acknowledge the write successfully.
    pub fn success(mut self) {
        self.0.send(true);
    }

    /// Send an ATT Application Error response.
    pub fn error(mut self) {
        self.0.send(false);
    }
}

/// Configuration for a BLE advertisement.
#[derive(Debug, Clone, Default)]
pub struct AdvertisingConfig {
    pub local_name: String,
    pub service_uuids: Vec<Uuid>,
    /// Manufacturer-specific data to advertise, as `(company_id, bytes)`. `None`
    /// advertises none. Carrying this requires **extended advertising** (a full
    /// 32-byte payload overflows the 31-byte legacy budget) — backends fall back
    /// to legacy only when it is `None`. The bytes are opaque to blew.
    pub manufacturer_data: Option<(u16, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_read_responder_respond() {
        let (tx, rx) = oneshot::channel();
        let responder = ReadResponder::new(tx);
        responder.respond(b"hello".to_vec());
        assert_eq!(rx.await.unwrap().unwrap(), b"hello");
    }

    #[tokio::test]
    async fn test_write_responder_success() {
        let (tx, rx) = oneshot::channel();
        let responder = WriteResponder::new(tx);
        responder.success();
        assert!(rx.await.unwrap());
    }

    use proptest::collection::vec;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum ReadAction {
        Drop,
        Respond(Vec<u8>),
        Error,
    }

    fn read_action() -> impl Strategy<Value = ReadAction> {
        prop_oneof![
            Just(ReadAction::Drop),
            vec(any::<u8>(), 0..32).prop_map(ReadAction::Respond),
            Just(ReadAction::Error),
        ]
    }

    #[derive(Debug, Clone)]
    enum WriteAction {
        Drop,
        Success,
        Error,
    }

    fn write_action() -> impl Strategy<Value = WriteAction> {
        prop_oneof![
            Just(WriteAction::Drop),
            Just(WriteAction::Success),
            Just(WriteAction::Error),
        ]
    }

    proptest! {
        /// For any `ReadResponder` action (respond / error / drop), the receiver
        /// observes exactly one value with the correct polarity. Drop is
        /// equivalent to `error()`.
        #[test]
        fn read_responder_value_matches_action(actions in vec(read_action(), 0..64)) {
            for action in actions {
                let (tx, mut rx) = oneshot::channel::<Result<Vec<u8>, ()>>();
                let responder = ReadResponder::new(tx);
                let expected: Result<Vec<u8>, ()> = match action {
                    ReadAction::Drop => {
                        drop(responder);
                        Err(())
                    }
                    ReadAction::Respond(v) => {
                        let expected = Ok(v.clone());
                        responder.respond(v);
                        expected
                    }
                    ReadAction::Error => {
                        responder.error();
                        Err(())
                    }
                };
                let got = rx
                    .try_recv()
                    .expect("responder must deliver a value synchronously");
                prop_assert_eq!(got, expected);
            }
        }

        /// For any `WriteResponder` action (success / error / drop), the receiver
        /// observes exactly one `bool` with the correct polarity. Drop is
        /// equivalent to `error()` (both deliver `false`).
        #[test]
        fn write_responder_value_matches_action(actions in vec(write_action(), 0..64)) {
            for action in actions {
                let (tx, mut rx) = oneshot::channel::<bool>();
                let responder = WriteResponder::new(tx);
                let expected = match action {
                    WriteAction::Drop => {
                        drop(responder);
                        false
                    }
                    WriteAction::Success => {
                        responder.success();
                        true
                    }
                    WriteAction::Error => {
                        responder.error();
                        false
                    }
                };
                let got = rx
                    .try_recv()
                    .expect("responder must deliver a value synchronously");
                prop_assert_eq!(got, expected);
            }
        }
    }
}
