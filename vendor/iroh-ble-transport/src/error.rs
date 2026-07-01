use blew::DisconnectCause;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum BleError {
    #[error("operation not supported on this platform")]
    Unsupported,
    #[error("bluetooth adapter is off")]
    AdapterOff,
    #[error("bluetooth adapter not found")]
    AdapterNotFound,
    #[error("not connected to peer")]
    NotConnected,
    #[error("timed out at stage: {stage}")]
    Timeout { stage: &'static str },
    #[error("disconnected: {0:?}")]
    Disconnected(DisconnectCause),
    #[error("GATT error: code {code}")]
    GattError { code: i32 },
    #[error("GATT busy")]
    GattBusy,
    #[error("protocol version mismatch: got {got}, want {want}")]
    ProtocolVersionMismatch { got: u8, want: u8 },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("blew error: {0}")]
    Blew(#[from] blew::error::BlewError),
}

pub type BleResult<T> = Result<T, BleError>;
