//! Shared L2CAP transport construction for Linux platforms.
//!
//! `bluer::l2cap::Stream` already implements async byte-stream I/O, so Linux can
//! use the native stream directly without an intermediate bridge.

use crate::l2cap::L2capChannel;

pub(crate) fn bridge_l2cap(stream: bluer::l2cap::Stream) -> L2capChannel {
    L2capChannel::from_stream(stream)
}
