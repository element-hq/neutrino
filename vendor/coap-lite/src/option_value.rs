//! Convenience types for option values.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::convert::TryFrom;

use crate::error::IncompatibleOptionValueFormat;

/// Supertrait for types that can be used as option values.
pub trait OptionValueType:
    Into<Vec<u8>> + TryFrom<Vec<u8>, Error = IncompatibleOptionValueFormat>
{
}

macro_rules! option_value_uint_impl {
    ($struct_name:ident, $type:ty, $bytes:expr) => {
        #[derive(Debug, Clone, PartialEq)]
        pub struct $struct_name(pub $type);

        impl From<$struct_name> for Vec<u8> {
            fn from(value: $struct_name) -> Self {
                option_from_uint(value.0.into(), $bytes)
            }
        }

        impl TryFrom<Vec<u8>> for $struct_name {
            type Error = IncompatibleOptionValueFormat;

            fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
                option_to_uint(&value, $bytes)
                    // Safe because option_to_uint will yield error if the size
                    // would overflow
                    .map(|value_as_u64| $struct_name(value_as_u64 as $type))
            }
        }

        impl OptionValueType for $struct_name {}
    };
}

fn option_from_uint(value_as_u64: u64, value_size: usize) -> Vec<u8> {
    // Optimize common paths
    if value_as_u64 == 0 {
        vec![]
    } else if value_as_u64 < 256 {
        vec![value_as_u64 as u8]
    } else {
        let mut draining_value = value_as_u64;

        let mut output = Vec::with_capacity(value_size);
        while draining_value > 0 {
            let next_lowest_byte = (draining_value & 0xff) as u8;
            assert!(output.len() < value_size);
            output.push(next_lowest_byte);
            draining_value >>= 8;
        }

        // Output is in little endian, flip it to big endian (network order) as
        // required
        output.reverse();

        output
    }
}

fn option_to_uint(
    encoded: &[u8],
    value_size: usize,
) -> Result<u64, IncompatibleOptionValueFormat> {
    if encoded.len() > value_size {
        Err(IncompatibleOptionValueFormat {
            message: format!(
                "overflow: got {} bytes, expected {}",
                encoded.len(),
                value_size
            ),
        })
    } else {
        Ok(encoded.iter().fold(0, |acc, &b| (acc << 8) + b as u64))
    }
}

option_value_uint_impl!(OptionValueU8, u8, 1);
option_value_uint_impl!(OptionValueU16, u16, 2);
option_value_uint_impl!(OptionValueU32, u32, 4);
option_value_uint_impl!(OptionValueU64, u64, 8);

#[derive(Debug, Clone, PartialEq)]
pub struct OptionValueString(pub String);

impl From<OptionValueString> for Vec<u8> {
    fn from(option_value: OptionValueString) -> Self {
        option_value.0.into_bytes()
    }
}

impl TryFrom<Vec<u8>> for OptionValueString {
    type Error = IncompatibleOptionValueFormat;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        String::from_utf8(value)
            .map(OptionValueString)
            .map_err(|e| IncompatibleOptionValueFormat {
                message: e.to_string(),
            })
    }
}

impl OptionValueType for OptionValueString {}
