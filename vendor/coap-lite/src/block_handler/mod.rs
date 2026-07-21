//! General purpose implementation of block transfer support (RFC 7959).
//!
//! Supports both Block1 and Block2 and is intended to be compliant with the
//! standard but lenient to tolerate mixed use cases.  In-memory caching of
//! request and response bodies is used to achieve the generic interaction.

use alloc::string::String;
use alloc::vec::Vec;
use core::cmp::min;
use core::iter;
use core::mem;
use core::ops::Bound;
use core::ops::{Deref, RangeBounds};
use core::time::Duration;

use lru_time_cache::LruCache;

mod block_value;

use crate::error::HandlingError;
use crate::{CoapOption, CoapRequest, MessageClass, Packet, ResponseType};
pub use block_value::BlockValue;

/// The maximum amount adding a block1 & block2 option to the message could add
/// to the total size.
const BLOCK_OPTIONS_MAX_LENGTH: usize = 12;

/// Maximum amount we're willing to extend a client cached payload without the
/// client committing to having to send us the bytes.  This prevents a common
/// denial of service (DoS) attack where the client claims that they want to
/// send say block num 10000 of a 1KB block size request and we preallocate
/// 10MB of space to honor the request and explode.  Note this is not limiting
/// the total cached payload we can accept, merely the amount the client can
/// "jump" on us between each request.  We need some wiggle room here to
/// accommodate for retransmits and such but not so much that we open ourselves
/// up to the DoS.
const MAXIMUM_UNCOMMITTED_BUFFER_RESERVE_LENGTH: usize = 16 * 1024;

/// Default taken from RFC 7252.
const DEFAULT_MAX_TOTAL_MESSAGE_SIZE: usize = 1152;

/// Implements block transfer by intercepting and caching requests and
/// responses.
pub struct BlockHandler<Endpoint: Ord + Clone> {
    config: BlockHandlerConfig,

    /// Maintains a block1 and 2 cache for requests that we expect a client to
    /// soon follow-up and ask about.  If this recency requirement is not
    /// meant, the system will still work however consistency of results will
    /// suffer.
    states: LruCache<RequestCacheKey<Endpoint>, BlockState>,
}

/// The configuration for [`BlockHandler`].
pub struct BlockHandlerConfig {
    /// Total framed message size to offer to the peer (packet size minus
    /// transport overhead).  In an ideal world this would be computed based on
    /// the endpoint MTU or even part of a more structured Endpoint API but
    /// we're pretty far off from that today.  Just make it configurable then.
    ///
    /// Note this is _not_ the suggested block size as that is referring only
    /// to the payload body. Because we have dynamic overhead for the CoAP
    /// message itself (for example if we add more options), we need to
    /// dynamically tune this to meet the physical requirements of the link
    /// rather than just some arbitrary limiting of the payload block size.
    pub max_total_message_size: usize,

    /// Length of time without interaction for cached responses to live (bumped
    /// each time the client requests some portion of the response).
    pub cache_expiry_duration: Duration,
}

impl Default for BlockHandlerConfig {
    fn default() -> Self {
        Self {
            max_total_message_size: DEFAULT_MAX_TOTAL_MESSAGE_SIZE,
            cache_expiry_duration: Duration::from_secs(120),
        }
    }
}

impl<Endpoint: Ord + Clone> BlockHandler<Endpoint> {
    /// Creates a new block handler which is expected to be re-used across all
    /// subsequent request/response pairs that may benefit from block handling.
    pub fn new(config: BlockHandlerConfig) -> Self {
        Self {
            states: LruCache::with_expiry_duration(
                config.cache_expiry_duration,
            ),
            config,
        }
    }

    /// Intercepts request before application processing has occurred.
    ///
    /// Returns true if the request requires Block1/2 handling and no further
    /// processing should occur (the response will be mutated inside the
    /// request and should be sent to the peer); false otherwise and handling
    /// should proceed to the application normally.
    pub fn intercept_request(
        &mut self,
        request: &mut CoapRequest<Endpoint>,
    ) -> Result<bool, HandlingError> {
        let state = self
            .states
            .entry(request.deref().into())
            .or_insert(BlockState::default());
        let block1_handled = Self::maybe_handle_request_block1(
            request,
            self.config.max_total_message_size,
            state,
        )?;
        if block1_handled {
            return Ok(true);
        }

        let block2_handled =
            Self::maybe_handle_request_block2(request, state)?;
        if block2_handled {
            return Ok(true);
        }

        Ok(false)
    }

    fn maybe_handle_request_block1(
        request: &mut CoapRequest<Endpoint>,
        max_total_message_size: usize,
        state: &mut BlockState,
    ) -> Result<bool, HandlingError> {
        let request_block1 = request
            .message
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .and_then(|x| x.ok());
        let maybe_response_block1 = Self::negotiate_block_size_if_necessary(
            request_block1.as_ref(),
            Self::compute_message_size_hack(&mut request.message),
            request.message.payload.len(),
            max_total_message_size,
        )?;

        match (request_block1, maybe_response_block1) {
            (Some(request_block1), Some(response_block1)) => {
                if state.cached_request_payload.is_none() {
                    state.cached_request_payload = Some(Vec::new());
                }
                let cached_payload =
                    state.cached_request_payload.as_mut().unwrap();

                let payload_offset =
                    usize::from(request_block1.num) * request_block1.size();
                extending_splice(
                    cached_payload,
                    payload_offset..payload_offset + request_block1.size(),
                    request.message.payload.iter().copied(),
                    MAXIMUM_UNCOMMITTED_BUFFER_RESERVE_LENGTH,
                )
                .map_err(HandlingError::internal)?;

                if request_block1.more {
                    let response = request
                        .response
                        .as_mut()
                        .ok_or_else(HandlingError::not_handled)?;
                    response
                        .message
                        .add_option_as(CoapOption::Block1, response_block1);
                    response.message.header.code =
                        MessageClass::Response(ResponseType::Continue);
                    Ok(true)
                } else {
                    let cached_payload =
                        mem::take(&mut state.cached_request_payload).unwrap();
                    request.message.payload = cached_payload;

                    // This is a little bit hacky, we really should be doing
                    // this in intercept_response but whatever, I doubt this
                    // will create any issues in practice.
                    let response = request
                        .response
                        .as_mut()
                        .ok_or_else(HandlingError::not_handled)?;
                    response
                        .message
                        .add_option_as(CoapOption::Block1, response_block1);

                    Ok(false)
                }
            }
            (None, Some(response_block1)) => {
                let response = request
                    .response
                    .as_mut()
                    .ok_or_else(HandlingError::not_handled)?;
                response
                    .message
                    .add_option_as(CoapOption::Block1, response_block1);
                response.message.header.code = MessageClass::Response(
                    ResponseType::RequestEntityTooLarge,
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn maybe_handle_request_block2(
        request: &mut CoapRequest<Endpoint>,
        state: &mut BlockState,
    ) -> Result<bool, HandlingError> {
        let maybe_block2 = request
            .message
            .get_first_option_as::<BlockValue>(CoapOption::Block2)
            .and_then(|x| x.ok());
        state.last_request_block2.clone_from(&maybe_block2);

        if let Some(block2) = maybe_block2 {
            if let Some(ref response) = state.cached_response {
                let has_more_chunks = Self::maybe_serve_cached_response(
                    request, block2, response,
                )?;
                if !has_more_chunks {
                    state.cached_response = None
                }
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn maybe_serve_cached_response(
        request: &mut CoapRequest<Endpoint>,
        request_block2: BlockValue,
        cached_response: &Packet,
    ) -> Result<bool, HandlingError> {
        let response = request
            .response
            .as_mut()
            .ok_or_else(HandlingError::not_handled)?;

        Self::packet_clone_limited(&mut response.message, cached_response);

        let cached_payload = &cached_response.payload;

        let request_block_size = request_block2.size();
        let mut chunks = cached_payload
            .chunks(request_block_size)
            .skip(usize::from(request_block2.num));

        let cached_payload_chunk = chunks.next().ok_or_else(|| {
            HandlingError::bad_request(format!(
                "num={}, block_size={}",
                request_block2.num,
                request_block2.size()
            ))
        })?;

        let response_payload = &mut response.message.payload;
        response_payload.clear();
        response_payload.extend(cached_payload_chunk);

        let has_more_chunks = chunks.next().is_some();
        let response_block2 = BlockValue {
            more: has_more_chunks,
            ..request_block2
        };

        response.message.set_options_as::<BlockValue>(
            CoapOption::Block2,
            [response_block2].into(),
        );

        Ok(has_more_chunks)
    }

    /// Equivalent to `dst.clone_from(src)` with the exception of not copying
    /// message_id or payload.
    fn packet_clone_limited(dst: &mut Packet, src: &Packet) {
        dst.header.set_version(src.header.get_version());
        dst.header.set_type(src.header.get_type());
        dst.header.code = src.header.code;
        for (&option, value) in src.options() {
            dst.set_option(CoapOption::from(option), value.clone());
        }
    }

    /// Intercepts a prepared response before it is to be delivered over the
    /// network.  If the payload assigned to the response is too large to be
    /// transmitted without fragmenting into blocks, the block handler will
    /// cache the response and serve it out via subsequent client requests
    /// (that in turn must be directed to [`BlockHandler::intercept_request`]).
    ///
    /// Returns true if the response has been manipulated and is being handled
    /// using Block1 or Block2 fragmentation; false otherwise.
    pub fn intercept_response(
        &mut self,
        request: &mut CoapRequest<Endpoint>,
    ) -> Result<bool, HandlingError> {
        let state = self
            .states
            .entry(request.deref().into())
            .or_insert(BlockState::default());
        if let Some(ref mut response) = request.response {
            // Don't do anything if the caller appears to be trying to
            // implement this manually.
            if response.message.get_option(CoapOption::Block2).is_none() {
                if let Some(request_block2) =
                    Self::negotiate_block_size_if_necessary(
                        state.last_request_block2.as_ref(),
                        Self::compute_message_size_hack(&mut response.message),
                        response.message.payload.len(),
                        self.config.max_total_message_size,
                    )?
                {
                    let cached_response = response.message.clone();
                    let has_more_chunks = Self::maybe_serve_cached_response(
                        request,
                        request_block2,
                        &cached_response,
                    )?;
                    if has_more_chunks {
                        state.cached_response = Some(cached_response);
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Hack to work around the lack of an API to compute the size of a message
    /// before producing it.
    fn compute_message_size_hack(packet: &mut Packet) -> usize {
        let moved_payload = mem::take(&mut packet.payload);
        let size_sans_payload = packet
            .to_bytes()
            .expect("Internal error encoding packet")
            .len();
        packet.payload = moved_payload;

        size_sans_payload + packet.payload.len()
    }

    fn negotiate_block_size_if_necessary(
        request_block: Option<&BlockValue>,
        message_size: usize,
        total_payload_size: usize,
        max_total_message_size: usize,
    ) -> Result<Option<BlockValue>, HandlingError> {
        let max_non_payload_size =
            (message_size + BLOCK_OPTIONS_MAX_LENGTH) - total_payload_size;
        let max_block_size = max_total_message_size
            .checked_sub(max_non_payload_size)
            .ok_or_else(|| {
                HandlingError::internal(format!(
            "Message too large to encode at any block size: {} exceeds {}",
            max_total_message_size,
            max_non_payload_size))
            })?;

        let maybe_response_block = match request_block {
            Some(request_block) => {
                // Client requested block encoding so let's give them that, but
                // not larger than our max block size.
                let negotiated_block_size =
                    min(request_block.size(), max_block_size);

                let reply_start_offset =
                    usize::from(request_block.num) * request_block.size();
                let reply_end_offset =
                    reply_start_offset + negotiated_block_size;

                let num = reply_start_offset / negotiated_block_size;
                let more = reply_end_offset < total_payload_size;

                Some(BlockValue::new(num, more, negotiated_block_size))
            }
            None => {
                if total_payload_size < max_block_size {
                    // The payload fits, and the client didn't request we do
                    // any different, so proceed normally without block-wise
                    // transfer.
                    None
                } else {
                    // Client did not ask for it, but we need block encoding
                    // for this to work given our max block size.
                    Some(BlockValue::new(
                        0,
                        true, /* more */
                        max_block_size,
                    ))
                }
            }
        };

        match maybe_response_block {
            Some(block) => block.map(Some).map_err(HandlingError::internal),
            None => Ok(None),
        }
    }
}

/// Similar to [`Vec::splice`] except that the Vec's length may be extended to
/// support the splice, but only up to an increase of `maximum_reserve_len`
/// (for security reasons if the data you're receiving is untrusted ensure this
/// is reasonably limited to avoid memory pressure denial of service attacks).
pub fn extending_splice<R, I, T>(
    dst: &mut Vec<T>,
    range: R,
    replace_with: I,
    maximum_reserve_len: usize,
) -> Result<alloc::vec::Splice<I::IntoIter>, String>
where
    R: RangeBounds<usize>,
    I: IntoIterator<Item = T>,
    T: Default + Copy,
{
    let end_index_plus_1 = match range.end_bound() {
        Bound::Included(&included) => included + 1,
        Bound::Excluded(&excluded) => excluded,
        Bound::Unbounded => panic!(),
    };

    if let Some(extend_len) = end_index_plus_1.checked_sub(dst.len()) {
        if extend_len > maximum_reserve_len {
            return Err(format!(
                "extend_len={}, maximum_extend_len={}",
                extend_len, maximum_reserve_len
            ));
        }
        // Safe but inefficient way...
        dst.extend(iter::repeat(T::default()).take(extend_len));
    }

    Ok(dst.splice(range, replace_with))
}

/// Cache key for uniquely identifying a request.
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct RequestCacheKey<Endpoint: Ord + Clone> {
    /// Request type as an integer to make it easy to derive Ord.
    request_type_ord: u8,
    path: Vec<String>,
    requester: Option<Endpoint>,
}

impl<Endpoint: Ord + Clone> From<&CoapRequest<Endpoint>>
    for RequestCacheKey<Endpoint>
{
    fn from(request: &CoapRequest<Endpoint>) -> Self {
        Self {
            request_type_ord: u8::from(MessageClass::Request(
                *request.get_method(),
            )),
            path: request.get_path_as_vec().unwrap_or_default(),
            requester: request.source.clone(),
        }
    }
}

/// State that is maintained over several requests.
#[derive(Debug, Clone, Default)]
pub struct BlockState {
    /// Last client request's block2 value (if any), which can either mean the
    /// client's attempt to suggest a block size or a request that came in
    /// after we expired our cache.
    last_request_block2: Option<BlockValue>,

    /// Packet we need to serve from if any future block-wise transfer requests
    /// come in.
    cached_response: Option<Packet>,

    /// Payload we are building up from a series of client requests.  Note that
    /// there is a deliberate lack of symmetry between the cached response and
    /// request due to the fact that the client is responsible for issuing
    /// multiple requests as we build up the cached payload.  This means that
    /// the client is ultimately responsible for making sure the last submitted
    /// packet is the one containing the interesting options we will need to
    /// handle the request and that we simply need to copy the payload into it.
    cached_request_payload: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use alloc::{borrow::ToOwned, collections::LinkedList};

    use crate::option_value::OptionValueString;
    use crate::{CoapResponse, RequestType, ResponseType};

    use super::*;

    #[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
    enum TestEndpoint {
        TestClient,
    }

    #[test]
    fn test_cached_response_with_blocks() {
        let block = "0123456789\n";

        let mut harness = TestServerHarness::new(32);

        let expected_payload = block.repeat(8).into_bytes();
        let delivered_payload = expected_payload.clone();

        let mut sent_req = create_get_request("test", 1, None);
        let mut received_response = harness
            .exchange_messages(&mut sent_req, move |received_request| {
                let sent_response =
                    received_request.response.as_mut().unwrap();
                sent_response.message.header.code =
                    MessageClass::Response(ResponseType::Content);
                sent_response.message.payload = delivered_payload;
                InterceptPolicy::Expected
            })
            .unwrap();

        let mut received_payload = Vec::<u8>::new();

        let total_blocks = loop {
            received_payload.extend(received_response.message.payload.clone());

            let received_block = received_response
                .message
                .get_first_option_as::<BlockValue>(CoapOption::Block2)
                .unwrap()
                .unwrap();
            let block_size = received_block.size();
            let block_num = received_block.num;

            if !received_block.more {
                break block_num;
            }

            let sent_block = BlockValue::new(
                usize::from(block_num + 1),
                false, /* more */
                block_size,
            )
            .unwrap();
            let mut next_sent_req = create_get_request(
                "test",
                received_response.message.header.message_id + 1,
                Some(sent_block),
            );

            received_response = harness
                .exchange_messages_using_cache(&mut next_sent_req)
                .unwrap();

            // Make sure the caching didn't do something clowny like copy the
            // message_id.
            assert_eq!(
                received_response.message.header.message_id,
                next_sent_req.message.header.message_id
            );
        };

        // Make sure that we _actually_ did block encoding :)
        assert!(total_blocks > 1);

        assert_eq!(
            String::from_utf8(received_payload).unwrap(),
            String::from_utf8(expected_payload).unwrap()
        );

        // Now verify that the cached entry is cleared...
        let mut followup_req = create_get_request("test", u16::MAX, None);
        let followup_block2 = BlockValue::new(0, false, 16).unwrap();
        followup_req
            .message
            .add_option_as::<BlockValue>(CoapOption::Block2, followup_block2);
        let followup_response = harness
            .exchange_messages(&mut followup_req, move |received_request| {
                let sent_response =
                    received_request.response.as_mut().unwrap();
                sent_response.message.header.code =
                    MessageClass::Response(ResponseType::Content);
                sent_response.message.payload = "small".as_bytes().to_vec();
                InterceptPolicy::NotExpected
            })
            .unwrap();

        assert_eq!(
            String::from_utf8(followup_response.message.payload).unwrap(),
            "small".to_owned()
        );
    }

    #[test]
    fn test_server_asserts_block1_encoding_required() {
        let block = "0123456789\n";

        let mut harness = TestServerHarness::new(32);

        let full_payload = block.repeat(8).into_bytes();

        let mut sent_request =
            create_put_request("test", 1, &full_payload, None);
        let received_response = harness
            .exchange_messages_using_cache(&mut sent_request)
            .unwrap();

        assert_eq!(
            received_response.message.header.code,
            MessageClass::Response(ResponseType::RequestEntityTooLarge)
        );
        let received_block = received_response
            .message
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .expect("Must respond with Block1 option")
            .expect("Must provide valid Block1 option");
        assert!(received_block.more);
    }

    #[test]
    fn test_cached_request_happy_path() {
        let block = "0123456789\n";

        let mut harness = TestServerHarness::new(32);

        let sent_payload = block.repeat(8).into_bytes();
        let expected_payload = sent_payload.clone();

        let block_size = 16;

        let chunks = sent_payload.chunks(block_size);
        let total_chunks = chunks.len();

        for (num, chunk) in chunks.enumerate() {
            let has_more_chunks = num + 1 < total_chunks;

            let block =
                BlockValue::new(num, has_more_chunks, block_size).unwrap();
            let mut sent_request =
                create_put_request("test", 1, chunk, Some(block));

            let received_response = if has_more_chunks {
                let received_response = harness
                    .exchange_messages_using_cache(&mut sent_request)
                    .unwrap();
                assert_eq!(
                    received_response.message.header.code,
                    MessageClass::Response(ResponseType::Continue)
                );
                received_response
            } else {
                let received_response = harness
                    .exchange_messages(&mut sent_request, |received_request| {
                        assert_eq!(
                            String::from_utf8(
                                received_request.message.payload.clone()
                            )
                            .unwrap(),
                            String::from_utf8(expected_payload.clone())
                                .unwrap()
                        );
                        let sent_response =
                            received_request.response.as_mut().unwrap();
                        sent_response.message.header.code =
                            MessageClass::Response(ResponseType::Changed);
                        InterceptPolicy::NotExpected
                    })
                    .unwrap();
                assert_eq!(
                    received_response.message.header.code,
                    MessageClass::Response(ResponseType::Changed)
                );
                received_response
            };

            let received_block = received_response
                .message
                .get_first_option_as::<BlockValue>(CoapOption::Block1)
                .unwrap()
                .unwrap();

            // This test isn't expecting renegotiation...
            assert_eq!(received_block.size(), block_size);
        }
    }

    struct TestServerHarness {
        handler: BlockHandler<TestEndpoint>,
    }

    impl TestServerHarness {
        pub fn new(max_message_size: usize) -> Self {
            TestServerHarness {
                handler: BlockHandler::new(BlockHandlerConfig {
                    max_total_message_size: max_message_size,
                    cache_expiry_duration: Duration::from_millis(
                        u32::MAX.into(),
                    ),
                }),
            }
        }

        pub fn exchange_messages_using_cache(
            &mut self,
            sent_request: &mut CoapRequest<TestEndpoint>,
        ) -> Option<CoapResponse> {
            self.exchange_messages_internal(sent_request, true, |_| {
                InterceptPolicy::DoNotInvoke
            })
        }

        pub fn exchange_messages<F>(
            &mut self,
            sent_request: &mut CoapRequest<TestEndpoint>,
            response_generator: F,
        ) -> Option<CoapResponse>
        where
            F: FnOnce(&mut CoapRequest<TestEndpoint>) -> InterceptPolicy,
        {
            self.exchange_messages_internal(
                sent_request,
                false,
                response_generator,
            )
        }

        fn exchange_messages_internal<F>(
            &mut self,
            sent_request: &mut CoapRequest<TestEndpoint>,
            expect_intercept_request: bool,
            response_generator: F,
        ) -> Option<CoapResponse>
        where
            F: FnOnce(&mut CoapRequest<TestEndpoint>) -> InterceptPolicy,
        {
            assert_eq!(
                self.handler.intercept_request(sent_request).unwrap(),
                expect_intercept_request
            );

            let mut received_request = sent_request.clone();
            match response_generator(&mut received_request) {
                InterceptPolicy::DoNotInvoke => sent_request.response.clone(),
                policy => {
                    assert_eq!(
                        self.handler
                            .intercept_response(&mut received_request)
                            .unwrap(),
                        match policy {
                            InterceptPolicy::Expected => true,
                            InterceptPolicy::NotExpected => false,
                            _ => panic!(),
                        }
                    );

                    received_request.response
                }
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum InterceptPolicy {
        Expected,
        NotExpected,
        DoNotInvoke,
    }

    fn create_get_request(
        path: &str,
        mid: u16,
        block2: Option<BlockValue>,
    ) -> CoapRequest<TestEndpoint> {
        create_request(RequestType::Get, path, mid, None, block2)
    }

    fn create_put_request(
        path: &str,
        mid: u16,
        payload: &[u8],
        block1: Option<BlockValue>,
    ) -> CoapRequest<TestEndpoint> {
        let mut request =
            create_request(RequestType::Put, path, mid, block1, None);
        request.message.payload.extend(payload);
        request
    }

    fn create_request(
        method: RequestType,
        path: &str,
        mid: u16,
        block1: Option<BlockValue>,
        block2: Option<BlockValue>,
    ) -> CoapRequest<TestEndpoint> {
        let mut packet = Packet::new();
        packet.header.code = MessageClass::Request(method);

        let uri_path: LinkedList<_> = path
            .split('/')
            .map(|x| OptionValueString(x.to_owned()))
            .collect();
        packet.set_options_as(CoapOption::UriPath, uri_path);

        let options =
            vec![(CoapOption::Block1, block1), (CoapOption::Block2, block2)];
        for (key, value) in options {
            if let Some(value) = value {
                packet.add_option_as(key, value);
            }
        }

        packet.header.message_id = mid;
        packet.payload = Vec::new();
        CoapRequest::<TestEndpoint>::from_packet(
            packet,
            TestEndpoint::TestClient,
        )
    }
}
