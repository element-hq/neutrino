//! The errors of the `coap` module.

use alloc::{
    borrow::ToOwned,
    string::{String, ToString},
};
use core::{fmt, num::TryFromIntError};
#[cfg(feature = "std")]
use std::error;

use crate::ResponseType;

/// The errors that can occur when encoding/decoding packets.
#[derive(Debug, PartialEq)]
pub enum MessageError {
    InvalidHeader,
    InvalidPacketLength,
    InvalidTokenLength,
    InvalidOptionDelta,
    InvalidOptionLength,
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageError::InvalidHeader => {
                write!(f, "CoAP error: invalid header")
            }
            MessageError::InvalidPacketLength => {
                write!(f, "CoAP error: invalid packet length, consider using BlockHandler")
            }
            MessageError::InvalidTokenLength => {
                write!(f, "CoAP error: invalid token length")
            }
            MessageError::InvalidOptionDelta => {
                write!(f, "CoAP error: invalid option delta")
            }
            MessageError::InvalidOptionLength => {
                write!(f, "CoAP error: invalid option length")
            }
        }
    }
}

#[cfg(feature = "std")]
impl error::Error for MessageError {}

/// The error that can occur when parsing a content-format.
#[derive(Debug, PartialEq)]
pub struct InvalidContentFormat;

impl fmt::Display for InvalidContentFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CoAP error: invalid content-format number")
    }
}

#[cfg(feature = "std")]
impl error::Error for InvalidContentFormat {}

/// The error that can occur when parsing an observe option value.
#[derive(Debug, PartialEq)]
pub struct InvalidObserve;

impl fmt::Display for InvalidObserve {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CoAP error: invalid observe option number")
    }
}

#[cfg(feature = "std")]
impl error::Error for InvalidObserve {}

/// The error that can occur when parsing an option value.
#[derive(Debug, PartialEq)]
pub struct IncompatibleOptionValueFormat {
    pub message: String,
}

impl fmt::Display for IncompatibleOptionValueFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Incompatible option value: {}", self.message)
    }
}

#[cfg(feature = "std")]
impl error::Error for IncompatibleOptionValueFormat {}

/// The errors that can occur when constructing a new block value.
#[derive(Debug, PartialEq)]
pub enum InvalidBlockValue {
    SizeExponentEncodingError(usize),
    TypeBoundsError(TryFromIntError),
}

impl fmt::Display for InvalidBlockValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidBlockValue::SizeExponentEncodingError(size) => {
                write!(f, "size cannot be encoded {}", size)
            }
            InvalidBlockValue::TypeBoundsError(err) => {
                write!(f, "size provided is outside type bounds: {}", err)
            }
        }
    }
}

#[cfg(feature = "std")]
impl error::Error for InvalidBlockValue {}

/// Participatory mechanism for the low-level library to communicate to callers
/// that unexpected errors occurred while handling standard parts of the
/// protocol that should ideally deliver a failure message to the peer. But
/// rather than apply that response message ourselves we yield this error and
/// ask the caller to perform the conversion.  For convenience, this can be
/// done with [`crate::CoapRequest::apply_from_error`].
#[derive(Debug, Clone)]
pub struct HandlingError {
    pub code: Option<ResponseType>,
    pub message: String,
}

impl fmt::Display for HandlingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handling error {:?}: {}", self.code, self.message)
    }
}

#[cfg(feature = "std")]
impl error::Error for HandlingError {}

impl HandlingError {
    pub fn not_handled() -> Self {
        Self {
            code: None,
            message: "Not handled".to_owned(),
        }
    }

    pub fn not_found() -> Self {
        Self::with_code(ResponseType::NotFound, "Not found")
    }

    pub fn bad_request<T: ToString>(e: T) -> Self {
        Self::with_code(ResponseType::BadRequest, e)
    }

    pub fn internal<T: ToString>(e: T) -> Self {
        Self::with_code(ResponseType::InternalServerError, e)
    }

    pub fn method_not_supported() -> Self {
        Self::with_code(ResponseType::MethodNotAllowed, "Method not supported")
    }

    pub fn with_code<T: ToString>(code: ResponseType, e: T) -> Self {
        Self {
            code: Some(code),
            message: e.to_string(),
        }
    }
}
