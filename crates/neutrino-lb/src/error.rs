//! Top-level errors for the sidecar entrypoint.

#[derive(Debug, thiserror::Error)]
pub enum LbError {
    #[error("wire error: {0}")]
    Wire(#[from] crate::transport::WireError),
}
