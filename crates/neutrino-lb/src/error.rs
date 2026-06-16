//! Top-level errors for the sidecar entrypoint.

#[derive(Debug, thiserror::Error)]
pub enum LbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wire error: {0}")]
    Wire(#[from] crate::transport::WireError),
}
