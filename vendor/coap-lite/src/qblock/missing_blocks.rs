//! Codec for the `application/missing-blocks+cbor-seq` body
//! (content-format 272, [`crate::ContentFormat::ApplicationMissingBlocksCborSeq`]).
//!
//! RFC 9177 §4.1 reports absent blocks as a *CBOR sequence* (RFC 8742) of
//! unsigned integers — bare CBOR uint items concatenated, with no enclosing
//! array. This is the payload of the 4.08 (Request Entity Incomplete) response
//! used to recover missing Q-Block1 blocks.
//!
//! Port of libcoap's `add_408_block`. We emit standard CBOR major-type-0 items
//! (immediate / `0x18` / `0x19` / `0x1a`). Note libcoap's encoder uses a
//! non-standard 4-byte form for block numbers in `0x10000..0x100000` (a `0x1a`
//! tag followed by only 3 bytes); we emit conformant CBOR instead. With the
//! current `u16` block-number ceiling (see `BlockValue`) that range is
//! unreachable, so this is forward-compatibility only.

use alloc::vec::Vec;
use core::fmt;

/// Error decoding an `application/missing-blocks+cbor-seq` body.
#[derive(Debug, PartialEq, Eq)]
pub enum MissingBlocksError {
    /// A leading item was not a CBOR unsigned integer (major type 0).
    NotUnsignedInt,
    /// The sequence ended in the middle of a multi-byte integer.
    Truncated,
    /// A block number did not fit in a `u32` (8-byte `0x1b` form or reserved
    /// additional-information value).
    OutOfRange,
}

impl fmt::Display for MissingBlocksError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MissingBlocksError::NotUnsignedInt => {
                write!(
                    f,
                    "missing-blocks: item is not a CBOR unsigned integer"
                )
            }
            MissingBlocksError::Truncated => {
                write!(f, "missing-blocks: truncated CBOR integer")
            }
            MissingBlocksError::OutOfRange => {
                write!(f, "missing-blocks: block number exceeds u32")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for MissingBlocksError {}

/// Appends the CBOR unsigned-integer encoding of `n` to `out`.
fn encode_uint(out: &mut Vec<u8>, n: u32) {
    if n < 24 {
        out.push(n as u8);
    } else if n < 0x100 {
        out.push(0x18);
        out.push(n as u8);
    } else if n < 0x1_0000 {
        out.push(0x19);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0x1a);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Encodes block numbers as an `application/missing-blocks+cbor-seq` body.
pub fn encode(blocks: impl IntoIterator<Item = u32>) -> Vec<u8> {
    let mut out = Vec::new();
    for n in blocks {
        encode_uint(&mut out, n);
    }
    out
}

/// Decodes an `application/missing-blocks+cbor-seq` body into block numbers.
pub fn decode(bytes: &[u8]) -> Result<Vec<u32>, MissingBlocksError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let initial = bytes[i];
        i += 1;
        if initial >> 5 != 0 {
            return Err(MissingBlocksError::NotUnsignedInt);
        }
        let info = initial & 0x1f;
        let n = match info {
            0..=23 => u32::from(info),
            24 => {
                let b = read(bytes, &mut i, 1)?;
                u32::from(b[0])
            }
            25 => {
                let b = read(bytes, &mut i, 2)?;
                u32::from(u16::from_be_bytes([b[0], b[1]]))
            }
            26 => {
                let b = read(bytes, &mut i, 4)?;
                u32::from_be_bytes([b[0], b[1], b[2], b[3]])
            }
            // 27 (8-byte) cannot fit a u32; 28..=31 are reserved.
            _ => return Err(MissingBlocksError::OutOfRange),
        };
        out.push(n);
    }
    Ok(out)
}

/// Reads `len` bytes at `*i`, advancing `*i`, or errors if truncated.
fn read<'a>(
    bytes: &'a [u8],
    i: &mut usize,
    len: usize,
) -> Result<&'a [u8], MissingBlocksError> {
    let end = *i + len;
    let slice = bytes.get(*i..end).ok_or(MissingBlocksError::Truncated)?;
    *i = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_each_cbor_width() {
        assert_eq!(encode([0u32]), vec![0x00]);
        assert_eq!(encode([23u32]), vec![0x17]);
        assert_eq!(encode([24u32]), vec![0x18, 0x18]);
        assert_eq!(encode([255u32]), vec![0x18, 0xff]);
        assert_eq!(encode([256u32]), vec![0x19, 0x01, 0x00]);
        assert_eq!(encode([0xffffu32]), vec![0x19, 0xff, 0xff]);
        assert_eq!(encode([0x1_0000u32]), vec![0x1a, 0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn roundtrips_a_sequence() {
        let blocks = vec![0u32, 5, 23, 24, 200, 256, 65_535, 70_000];
        assert_eq!(decode(&encode(blocks.clone())).unwrap(), blocks);
    }

    #[test]
    fn decodes_concatenated_items_without_array_wrapper() {
        // 1, 2, 3 as a bare CBOR sequence.
        assert_eq!(decode(&[0x01, 0x02, 0x03]).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn empty_body_is_empty_sequence() {
        assert_eq!(decode(&[]).unwrap(), Vec::<u32>::new());
    }

    #[test]
    fn rejects_non_uint_major_type() {
        // 0x41 = byte string of length 1 (major type 2).
        assert_eq!(
            decode(&[0x41, 0x00]),
            Err(MissingBlocksError::NotUnsignedInt)
        );
    }

    #[test]
    fn rejects_truncated_integer() {
        assert_eq!(decode(&[0x19, 0x01]), Err(MissingBlocksError::Truncated));
    }

    #[test]
    fn rejects_oversized_integer() {
        // 0x1b = 8-byte uint, cannot fit u32.
        assert_eq!(
            decode(&[0x1b, 0, 0, 0, 0, 0, 0, 0, 1]),
            Err(MissingBlocksError::OutOfRange)
        );
    }
}
