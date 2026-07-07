//! Q-Block (RFC 9177) runtime — the burst-and-recover transfer machinery that
//! layers on top of coap-lite's pure [`coap_lite::qblock`] primitives.
//!
//! Phase 2 scope: [`QBlockSender`], the NON-only **burst pump**. It drains a
//! body as bursts of up to `MAX_PAYLOADS` non-confirmable block PDUs, pausing
//! `NON_TIMEOUT` (× a random factor) between bursts for congestion control —
//! the pipelined send that replaces RFC 7959's stop-and-wait. Loss recovery
//! (refilling the outstanding set from missing-block requests) and the receive
//! side land in later phases; the structure here leaves room for both.
//!
//! NON-only is the current *implementation* scope, not an architectural
//! commitment: the burst gate matches on [`TransferKind`] so a CON arm
//! (NSTART/ACK) can be added without reworking this code. See
//! `neutrino/docs/superpowers/specs/2026-06-23-qblock-rfc9177-rust-design.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coap_lite::block_handler::BlockValue;
use coap_lite::option_value::{OptionValueU16, OptionValueU32};
use coap_lite::qblock::{missing_blocks, RangeSet};
use coap_lite::{CoapOption, ContentFormat, MessageClass, MessageType, Packet, ResponseType};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::client::ClientTransport;
use crate::server::Responder;

/// Request-Tag option number (RFC 9175). coap-lite has no named variant, so we
/// address it as `Unknown(292)`. RFC 9177 uses it to correlate the blocks of a
/// single Q-Block1 request body.
const REQUEST_TAG: u16 = 292;

/// The Request-Tag option used to correlate a Q-Block1 transfer.
fn request_tag() -> CoapOption {
    CoapOption::Unknown(REQUEST_TAG)
}

/// RFC 9177 §6.2 timing and congestion constants (per transfer/session).
///
/// The CON-mode fields (`probing_rate`, `nstart`, `non_probing_wait`) are
/// carried but unread in the NON-only v1 — present so the config surface
/// already accommodates CON. See design doc §3.1.
#[derive(Debug, Clone)]
pub struct QBlockConfig {
    /// Blocks sent per burst before the inter-burst delay (`MAX_PAYLOADS`, 10).
    pub max_payloads: u32,
    /// Base inter-burst congestion delay (`NON_TIMEOUT`, 2 s).
    pub non_timeout: Duration,
    /// Receiver wait-for-gaps base before requesting missing blocks
    /// (`NON_RECEIVE_TIMEOUT`, 4 s).
    pub non_receive_timeout: Duration,
    /// Max missing-block recovery rounds (`NON_MAX_RETRANSMIT`, 4).
    pub non_max_retransmit: u32,
    /// Time to hold a partially-received body before discarding
    /// (`NON_PARTIAL_TIMEOUT`, ~247 s).
    pub non_partial_timeout: Duration,
    /// CON: bytes/second cap when the peer is silent (`PROBING_RATE`). Unused in v1.
    pub probing_rate: u32,
    /// CON: in-flight confirmable cap (`NSTART`). Unused in v1.
    pub nstart: u32,
    /// CON: max wait while probing a silent peer (`NON_PROBING_WAIT`). Unused in v1.
    pub non_probing_wait: Duration,
    /// `Some(block size in bytes)`: assume every client supports Q-Block
    /// without negotiation (closed deployments where both ends run this
    /// stack) — the server streams any response larger than one block as a
    /// Q-Block2 transfer at this block size even when the request carries no
    /// Q-Block2 option. A request that does carry one still wins (its
    /// negotiated size is used). `None` (default): RFC 9177 behaviour — only
    /// requests that opt in via Q-Block2 get Q-Block2 responses.
    pub assume_peer_block_size: Option<usize>,
}

impl Default for QBlockConfig {
    fn default() -> Self {
        Self {
            max_payloads: 10,
            non_timeout: Duration::from_secs(2),
            non_receive_timeout: Duration::from_secs(4),
            non_max_retransmit: 4,
            non_partial_timeout: Duration::from_secs(247),
            probing_rate: 1,
            nstart: 1,
            non_probing_wait: Duration::from_secs(247),
            assume_peer_block_size: None,
        }
    }
}

/// How a transfer's blocks are sent. NON-only is implemented in v1; `Con` is
/// the seam for the (future) confirmable path and is not yet handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    /// Non-confirmable bursts (RFC 9177 default).
    Non,
    /// Confirmable, NSTART-gated, per-block ACK. Not implemented in v1.
    Con,
}

/// A sink for one serialized block PDU.
///
/// Abstracts the transport seam so [`QBlockSender`] is transport-agnostic: a
/// client adapts [`crate::client::ClientTransport::send`] (Q-Block1), a server
/// adapts [`crate::server::Responder::respond`] (Q-Block2), and tests use an
/// in-memory recorder.
#[async_trait]
pub trait BlockSink: Send + Sync {
    /// Transmit one already-encoded block PDU.
    async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()>;
}

/// Drives the transmission of one large body as Q-Block bursts.
pub struct QBlockSender {
    /// PDU template carrying the type, code, token/RTag, and non-block options.
    /// Each block is built by cloning this, adding the block option, and
    /// attaching the payload slice.
    template: Packet,
    /// Q-Block option this transfer uses ([`CoapOption::QBlock1`] for requests,
    /// [`CoapOption::QBlock2`] for responses).
    option: CoapOption,
    /// The whole body being sent.
    body: Arc<[u8]>,
    /// Block size exponent (block size = `1 << (szx + 4)`), 0..=6.
    szx: u8,
    kind: TransferKind,
    config: QBlockConfig,
    /// Blocks still to send (drained as sent, refilled on missing-block
    /// requests). Filled lazily on the first burst.
    outstanding: RangeSet,
    started: bool,
    /// splitmix64 state for the per-burst `NON_TIMEOUT` random factor.
    rng: u64,
}

impl QBlockSender {
    /// Creates a sender for `body`, modelled on `template` (which must carry the
    /// desired message type/code/token and any non-block options, but no block
    /// option or payload). `seed` seeds the inter-burst jitter — derive it from
    /// the transfer token/RTag so distinct transfers desynchronise.
    pub fn new(
        mut template: Packet,
        option: CoapOption,
        body: Arc<[u8]>,
        szx: u8,
        kind: TransferKind,
        config: QBlockConfig,
        seed: u64,
    ) -> Self {
        // Advertise the total body size (Size1 for requests / Size2 for
        // responses) so a receiver can request even a lost final block.
        let size_opt = if option == CoapOption::QBlock1 {
            CoapOption::Size1
        } else {
            CoapOption::Size2
        };
        template.add_option_as::<OptionValueU32>(size_opt, OptionValueU32(body.len() as u32));
        Self {
            template,
            option,
            body,
            szx,
            kind,
            config,
            outstanding: RangeSet::new(),
            started: false,
            rng: seed,
        }
    }

    /// Block size in bytes.
    fn block_size(&self) -> usize {
        1usize << (self.szx + 4)
    }

    /// Number of blocks the body spans (at least 1).
    fn total_blocks(&self) -> u32 {
        let bs = self.block_size();
        (self.body.len().div_ceil(bs)).max(1) as u32
    }

    /// Builds the serialized PDU for block `n` of `total`.
    fn build_block(&self, n: u32, total: u32) -> std::io::Result<Vec<u8>> {
        let bs = self.block_size();
        let start = n as usize * bs;
        let end = (start + bs).min(self.body.len());
        let more = n + 1 < total;

        let block = BlockValue::new(n as usize, more, bs)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

        let mut pdu = self.template.clone();
        pdu.add_option_as::<BlockValue>(self.option, block);
        pdu.payload = self.body[start..end].to_vec();
        pdu.to_bytes()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// The inter-burst delay: `NON_TIMEOUT × f` with `f ∈ [1.0, 1.5)`
    /// (`ACK_RANDOM_FACTOR` = 1.5), i.e. RFC 9177's `NON_TIMEOUT_RANDOM`.
    fn inter_burst_delay(&mut self) -> Duration {
        let frac = (self.next_rand() >> 11) as f64 / (1u64 << 53) as f64;
        self.config.non_timeout.mul_f64(1.0 + 0.5 * frac)
    }

    /// splitmix64 — small, allocation-free, deterministic from the seed.
    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Per-burst block limit. NON-only burst gate; the CON arm (NSTART/
    /// delayqueue-bounded) slots in here without disturbing the NON path.
    fn burst_limit(&self) -> u32 {
        match self.kind {
            TransferKind::Non => self.config.max_payloads,
            TransferKind::Con => {
                panic!("Q-Block CON mode is not implemented in v1 (NON-only)")
            }
        }
    }

    /// Fills the outstanding set with the whole body on first use.
    fn ensure_started(&mut self) {
        if self.started {
            return;
        }
        let total = self.total_blocks();
        // Inserting 0..total in order keeps this a single contiguous range, so
        // it never approaches the RBLOCK_CNT cap.
        for n in 0..total {
            self.outstanding.insert(n, n + 1 < total);
        }
        self.started = true;
    }

    /// Whether every block has been sent and none are awaiting retransmit.
    pub fn is_done(&self) -> bool {
        self.started && self.outstanding.is_empty()
    }

    /// Re-queues `blocks` for (re)transmission — called when the peer reports
    /// them missing. Numbers beyond the body are ignored.
    pub fn refill(&mut self, blocks: &[u32]) {
        self.ensure_started();
        let total = self.total_blocks();
        for &n in blocks {
            if n < total {
                self.outstanding.insert(n, n + 1 < total);
            }
        }
    }

    /// Sends the next burst (up to `MAX_PAYLOADS` lowest outstanding blocks) and
    /// returns how many were sent. Does not sleep — the caller paces bursts.
    pub async fn drain_burst<S: BlockSink + ?Sized>(&mut self, sink: &S) -> std::io::Result<u32> {
        self.ensure_started();
        let total = self.total_blocks();
        let limit = self.burst_limit();
        let mut sent = 0;
        while sent < limit {
            let Some(n) = self.outstanding.first() else {
                break;
            };
            let pdu = self.build_block(n, total)?;
            sink.send_block(pdu).await?;
            self.outstanding.remove(n);
            sent += 1;
        }
        Ok(sent)
    }

    /// Runs the burst pump to completion with no loss recovery: sends every
    /// block in `MAX_PAYLOADS`-sized NON bursts, pausing `inter_burst_delay()`
    /// between bursts. Returns the number of blocks in the body.
    ///
    /// Recovery-driven sending is done by the caller via [`drain_burst`] +
    /// [`refill`]; see the transfer driver wiring.
    ///
    /// Panics if `kind` is [`TransferKind::Con`] (not implemented in v1).
    pub async fn run<S: BlockSink + ?Sized>(mut self, sink: &S) -> std::io::Result<u32> {
        let total = self.total_blocks();
        loop {
            self.drain_burst(sink).await?;
            if self.is_done() {
                break;
            }
            tokio::time::sleep(self.inter_burst_delay()).await;
        }
        Ok(total)
    }
}

/// Outcome of feeding one block PDU to a [`QBlockReceiver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockOutcome {
    /// Block recorded; the body is still incomplete.
    Accepted,
    /// The final block arrived and the body is fully reassembled.
    Complete(Vec<u8>),
    /// The block could not be tracked (the [`RangeSet`] is at capacity, or the
    /// number is beyond a known total). The caller drops it; recovery (a later
    /// phase) will re-request it. Its payload is *not* written.
    Dropped,
    /// The PDU did not carry this receiver's Q-Block option — not part of a
    /// Q-Block transfer, hand it back to normal processing.
    NotQBlock,
}

/// Reassembles one large body delivered as Q-Block blocks.
///
/// Phase 3 scope: the happy-path receive side (no loss) — record each block,
/// write its payload at the right offset, and deliver the body once the set is
/// contiguous from 0 to the final (More-bit-unset) block. The gap-detection and
/// missing-block-request machinery (driven by [`RangeSet::missing`]) lands in
/// the recovery phase; the [`rec_blocks`](Self::rec_blocks) accessor is already
/// exposed for it.
pub struct QBlockReceiver {
    /// The Q-Block option this transfer uses ([`CoapOption::QBlock2`] when a
    /// client receives a large response, [`CoapOption::QBlock1`] when a server
    /// receives a large request).
    option: CoapOption,
    rec_blocks: RangeSet,
    /// Reassembly buffer, grown as blocks land at their offsets.
    body: Vec<u8>,
    /// The first accepted block's PDU with its payload and block/size options
    /// stripped — the "carrier" of the transfer's metadata (code, token, and any
    /// application options such as content-format or forwarded HTTP status/
    /// headers). Returned with the assembled body so callers can reconstruct the
    /// full response, not just its bytes.
    carrier: Option<Packet>,
    /// Exact body length, known once the final block (More unset) is seen.
    final_len: Option<usize>,
    /// Total body length advertised via the Size1/Size2 option, if present.
    /// Lets recovery request a lost *final* block (whose More-unset signal we
    /// never saw) before the transfer would otherwise stall.
    total_len: Option<usize>,
    /// Block size exponent, learned from the first block; all blocks of a body
    /// must agree.
    szx: Option<u8>,
    /// Missing-block recovery rounds issued so far (drives the backoff and the
    /// `NON_MAX_RETRANSMIT` cap). Reset to 0 whenever a new block is tracked.
    retry: u32,
    /// Template for building missing-block requests: for Q-Block2 the original
    /// request (method/uri/token, no block option); for Q-Block1 a base whose
    /// token is echoed into the 4.08.
    recovery_template: Packet,
    /// Upper bound on the reassembled body, to cap buffer growth from a block
    /// claiming a wild offset (mirrors coap-lite's uncommitted-reserve guard).
    max_body_len: usize,
    config: QBlockConfig,
}

/// What the recovery timer wants done for a stalled transfer.
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryOutcome {
    /// Nothing due yet; poll again after this long.
    Wait(Duration),
    /// Send this missing-block request PDU (a retry has been counted). The
    /// caller resets its since-activity timer after sending.
    Resend(Packet),
    /// `NON_MAX_RETRANSMIT` rounds exhausted; abandon the partial body.
    Expired,
}

impl QBlockReceiver {
    /// Creates a receiver for the given Q-Block `option`. `recovery_template` is
    /// the PDU that missing-block requests are built from (see the field doc).
    /// `max_body_len` caps the reassembled body size.
    pub fn new(
        option: CoapOption,
        recovery_template: Packet,
        max_body_len: usize,
        config: QBlockConfig,
    ) -> Self {
        Self {
            option,
            rec_blocks: RangeSet::new(),
            body: Vec::new(),
            carrier: None,
            final_len: None,
            total_len: None,
            szx: None,
            retry: 0,
            recovery_template,
            max_body_len,
            config,
        }
    }

    /// The set of received blocks (for recovery: [`RangeSet::missing`]).
    pub fn rec_blocks(&self) -> &RangeSet {
        &self.rec_blocks
    }

    /// The Size option (Size1/Size2) that pairs with this Q-Block option.
    fn size_option(&self) -> CoapOption {
        if self.option == CoapOption::QBlock1 {
            CoapOption::Size1
        } else {
            CoapOption::Size2
        }
    }

    /// Whether the whole body has been received.
    pub fn is_complete(&self) -> bool {
        self.rec_blocks.is_complete()
    }

    /// Feeds one received PDU. Extracts the Q-Block option, writes the payload
    /// into the reassembly buffer at the block's offset, and tracks it; returns
    /// [`BlockOutcome::Complete`] with the body once the transfer finishes.
    pub fn accept(&mut self, pdu: &Packet) -> std::io::Result<BlockOutcome> {
        let Some(block) = pdu
            .get_first_option_as::<BlockValue>(self.option)
            .and_then(|r| r.ok())
        else {
            return Ok(BlockOutcome::NotQBlock);
        };

        // Block size must be consistent across the whole body.
        match self.szx {
            Some(prev) if prev != block.size_exponent => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Q-Block size changed mid-transfer",
                ));
            }
            None => self.szx = Some(block.size_exponent),
            _ => {}
        }

        // Learn the total body size from Size1/Size2 (once) for tail recovery.
        if self.total_len.is_none() {
            if let Some(sz) = pdu
                .get_first_option_as::<OptionValueU32>(self.size_option())
                .and_then(|r| r.ok())
            {
                // Reject an advertised total beyond the cap *before* it is
                // recorded: final_block() derives a block count from total_len
                // that feeds RangeSet::missing(), which would allocate ~total/
                // block_size entries on the first recovery tick — a one-datagram
                // OOM. A body that can't fit under max_body_len is rejected at
                // the same threshold as a realized over-cap block (below).
                if sz.0 as usize > self.max_body_len {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Q-Block advertised body size exceeds maximum length",
                    ));
                }
                self.total_len = Some(sz.0 as usize);
            }
        }

        let num = u32::from(block.num);
        let offset = num as usize * block.size();
        let end = offset + pdu.payload.len();
        if end > self.max_body_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Q-Block body exceeds maximum length",
            ));
        }

        // Track first; on overflow/out-of-range drop without touching the buffer.
        if !self.rec_blocks.insert(num, block.more) {
            return Ok(BlockOutcome::Dropped);
        }
        // Activity resets the recovery backoff (mirrors libcoap blocks_add_entry).
        self.retry = 0;

        // Capture the transfer's metadata carrier from the first block.
        if self.carrier.is_none() {
            let mut carrier = pdu.clone();
            carrier.payload.clear();
            carrier.clear_option(self.option);
            carrier.clear_option(self.size_option());
            self.carrier = Some(carrier);
        }

        if self.body.len() < end {
            self.body.resize(end, 0);
        }
        self.body[offset..end].copy_from_slice(&pdu.payload);
        if !block.more {
            self.final_len = Some(end);
        }

        if self.rec_blocks.is_complete() {
            let mut body = core::mem::take(&mut self.body);
            if let Some(len) = self.final_len {
                body.truncate(len);
            }
            return Ok(BlockOutcome::Complete(body));
        }
        Ok(BlockOutcome::Accepted)
    }

    /// The metadata carrier (first block's PDU, payload/block options stripped),
    /// available once at least one block has been accepted. Combine with the
    /// assembled body to reconstruct the full response message.
    pub fn carrier(&self) -> Option<Packet> {
        self.carrier.clone()
    }

    /// The absolute hold time for a partially-received body
    /// (`NON_PARTIAL_TIMEOUT`, RFC 9177 §6.2). A transfer that has not completed
    /// within this — measured from its first block, independent of per-block
    /// activity — is abandoned, so a peer that dribbles one block per
    /// `NON_RECEIVE_TIMEOUT` (resetting the recovery backoff each time) cannot
    /// pin the reassembly buffer indefinitely.
    pub fn partial_timeout(&self) -> Duration {
        self.config.non_partial_timeout
    }

    /// The highest block number the body spans (final block index), derived
    /// from the More-unset block if seen, else the advertised Size. `None` if
    /// neither is known (can't safely bound the request).
    fn final_block(&self, block_size: usize) -> Option<u32> {
        if let Some(total) = self.rec_blocks.total_blocks() {
            return Some(total - 1);
        }
        let total_len = self.total_len?;
        Some((total_len.div_ceil(block_size).max(1) - 1) as u32)
    }

    /// Builds a missing-block request for the current gaps (windowed to
    /// `MAX_PAYLOADS` so it stays small), or `None` if nothing is missing /
    /// the body length isn't yet known.
    fn build_recovery_request(&self) -> Option<Packet> {
        let szx = self.szx?;
        let block_size = 1usize << (szx + 4);
        let final_block = self.final_block(block_size)?;
        let mut missing = self.rec_blocks.missing(final_block);
        if missing.is_empty() {
            return None;
        }
        missing.truncate(self.config.max_payloads as usize);

        let mut pdu = self.recovery_template.clone();
        match self.option {
            // Q-Block2 (client recovering a response): one repeated Q-Block2
            // option per missing block (RFC 9177 §4.4).
            CoapOption::QBlock2 => {
                for &n in &missing {
                    let bv = BlockValue::new(n as usize, false, block_size).ok()?;
                    pdu.add_option_as::<BlockValue>(CoapOption::QBlock2, bv);
                }
            }
            // Q-Block1 (server recovering a request): a 4.08 carrying the
            // missing blocks as application/missing-blocks+cbor-seq.
            CoapOption::QBlock1 => {
                pdu.header.code = MessageClass::Response(ResponseType::RequestEntityIncomplete);
                pdu.add_option_as::<OptionValueU16>(
                    CoapOption::ContentFormat,
                    OptionValueU16(
                        usize::from(ContentFormat::ApplicationMissingBlocksCborSeq) as u16
                    ),
                );
                pdu.payload = missing_blocks::encode(missing);
            }
            _ => return None,
        }
        Some(pdu)
    }

    /// Drives missing-block recovery. `since_activity` is the time elapsed since
    /// the last block was accepted or the last [`RecoveryOutcome::Resend`] was
    /// issued (the caller tracks this and resets it on either event).
    ///
    /// Returns [`RecoveryOutcome::Wait`] until the per-retry deadline
    /// (`NON_RECEIVE_TIMEOUT × 2^retry`) elapses, then [`Resend`] with a
    /// freshly-built request (counting the retry), and finally [`Expired`] once
    /// `NON_MAX_RETRANSMIT` rounds are spent.
    ///
    /// [`Resend`]: RecoveryOutcome::Resend
    pub fn poll_recovery(&mut self, since_activity: Duration) -> RecoveryOutcome {
        let deadline = self.config.non_receive_timeout * (1 << self.retry);
        if since_activity < deadline {
            return RecoveryOutcome::Wait(deadline - since_activity);
        }
        if self.retry >= self.config.non_max_retransmit {
            return RecoveryOutcome::Expired;
        }
        match self.build_recovery_request() {
            Some(pdu) => {
                self.retry += 1;
                RecoveryOutcome::Resend(pdu)
            }
            // Nothing actionable yet (e.g. body length unknown); try again later.
            None => RecoveryOutcome::Wait(self.config.non_receive_timeout),
        }
    }
}

/// Server-side demultiplexer for incoming **Q-Block1** request bodies.
///
/// A server may be receiving several large requests at once, so inbound blocks
/// must be correlated to the right in-progress body. RFC 9177 keys a Q-Block1
/// transfer on its **Request-Tag** (RFC 9175, option 292); this registry routes
/// each block to a per-`(Request-Tag)` [`QBlockReceiver`], creating one on the
/// first block and dropping it on completion.
///
/// (Q-Block2 *responses* are correlated by token at the client and use
/// [`QBlockReceiver`] directly — no registry needed there.)
pub struct QBlockReceivers {
    by_rtag: HashMap<Vec<u8>, QBlockReceiver>,
    max_body_len: usize,
    config: QBlockConfig,
}

impl QBlockReceivers {
    /// Creates an empty registry. `max_body_len`/`config` are applied to each
    /// per-transfer [`QBlockReceiver`] it creates.
    pub fn new(max_body_len: usize, config: QBlockConfig) -> Self {
        Self {
            by_rtag: HashMap::new(),
            max_body_len,
            config,
        }
    }

    /// The Request-Tag carried by `pdu` (empty vec if the option is present but
    /// zero-length, which is itself a valid tag).
    fn rtag_of(pdu: &Packet) -> Option<Vec<u8>> {
        pdu.get_option(request_tag())
            .map(|l| l.front().cloned().unwrap_or_default())
    }

    /// Routes one inbound PDU. Returns `None` if it is not a Q-Block1 block (the
    /// caller handles it normally); otherwise the transfer's Request-Tag and the
    /// [`BlockOutcome`]. On [`BlockOutcome::Complete`] the transfer is removed.
    pub fn accept(&mut self, pdu: &Packet) -> std::io::Result<Option<(Vec<u8>, BlockOutcome)>> {
        if pdu.get_option(CoapOption::QBlock1).is_none() {
            return Ok(None);
        }
        let rtag = Self::rtag_of(pdu).unwrap_or_default();

        let max = self.max_body_len;
        let cfg = self.config.clone();
        let entry = self.by_rtag.entry(rtag.clone()).or_insert_with(|| {
            // The 4.08 recovery requests echo the request's token and Request-Tag.
            let mut tmpl = Packet::new();
            tmpl.header.set_type(MessageType::NonConfirmable);
            tmpl.set_token(pdu.get_token().to_vec());
            if !rtag.is_empty() {
                tmpl.add_option(request_tag(), rtag.clone());
            }
            QBlockReceiver::new(CoapOption::QBlock1, tmpl, max, cfg)
        });

        let outcome = entry.accept(pdu)?;
        if matches!(outcome, BlockOutcome::Complete(_)) {
            self.by_rtag.remove(&rtag);
        }
        Ok(Some((rtag, outcome)))
    }

    /// Drives recovery for the transfer identified by `rtag`. Returns `None` if
    /// no such transfer is in progress; on [`RecoveryOutcome::Expired`] the
    /// transfer is dropped.
    pub fn poll_recovery(
        &mut self,
        rtag: &[u8],
        since_activity: Duration,
    ) -> Option<RecoveryOutcome> {
        let outcome = self.by_rtag.get_mut(rtag)?.poll_recovery(since_activity);
        if outcome == RecoveryOutcome::Expired {
            self.by_rtag.remove(rtag);
        }
        Some(outcome)
    }

    /// Number of transfers currently in progress.
    pub fn len(&self) -> usize {
        self.by_rtag.len()
    }

    /// Whether no transfers are in progress.
    pub fn is_empty(&self) -> bool {
        self.by_rtag.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Transport wiring: BlockSink adapters + the recovery drivers.
//
// The drivers below are transport-agnostic — they speak `BlockSink` and channels
// of byte PDUs — and are exercised by the offline tests with mock sinks + paused
// time. The two adapters bind `BlockSink` to coap-rs's real transports; the only
// piece that still needs a live socket to validate is the call site that spawns
// these drivers from `client::CoAPClient::send` / the server dispatch loop, which
// is left as the (live-test-required) final hook — see the design doc Phase 5/7.
// ---------------------------------------------------------------------------

/// [`BlockSink`] over a server [`Responder`] — used to push Q-Block2 response
/// blocks back to the peer that made the request.
pub struct ResponderSink(pub Arc<dyn Responder>);

#[async_trait]
impl BlockSink for ResponderSink {
    async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
        self.0.respond(pdu).await;
        Ok(())
    }
}

/// [`BlockSink`] over a client [`ClientTransport`] — used to push Q-Block1
/// request blocks (and a client's Q-Block2 missing-block requests) to the peer.
pub struct ClientTransportSink<T: ClientTransport>(pub Arc<T>);

#[async_trait]
impl<T: ClientTransport> BlockSink for ClientTransportSink<T> {
    async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
        self.0.send(&pdu).await.map(|_| ())
    }
}

/// Parses a missing-block request PDU into the block numbers it asks for — the
/// inverse of [`QBlockReceiver`]'s request builder, used by the sending side.
/// `option` is the transfer's Q-Block option (Q-Block2 → repeated options,
/// Q-Block1 → `application/missing-blocks+cbor-seq` payload).
pub fn parse_missing_request(pdu: &Packet, option: CoapOption) -> Vec<u32> {
    match option {
        CoapOption::QBlock2 => pdu
            .get_options_as::<BlockValue>(CoapOption::QBlock2)
            .map(|l| {
                l.into_iter()
                    .filter_map(|r| r.ok())
                    .map(|b| u32::from(b.num))
                    .collect()
            })
            .unwrap_or_default(),
        CoapOption::QBlock1 => missing_blocks::decode(&pdu.payload).unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Drives a Q-Block **send** to completion with loss recovery.
///
/// Emits bursts via `sink`, pacing `inter_burst_delay()` between them, while
/// concurrently consuming peer-reported missing blocks from `missing_rx` (the
/// caller parses inbound requests with [`parse_missing_request`]) and refilling.
/// Because a NON sender gets no positive acks, it cannot know the peer is done;
/// once everything is sent it lingers up to `linger` for a retransmit request,
/// finishing when none arrives (or the channel closes).
pub async fn drive_send<S: BlockSink + ?Sized>(
    mut sender: QBlockSender,
    sink: &S,
    mut missing_rx: mpsc::Receiver<Vec<u32>>,
    linger: Duration,
) -> std::io::Result<()> {
    loop {
        // Fold in any already-queued retransmit requests before sending.
        while let Ok(blocks) = missing_rx.try_recv() {
            sender.refill(&blocks);
        }
        sender.drain_burst(sink).await?;

        if sender.is_done() {
            match tokio::time::timeout(linger, missing_rx.recv()).await {
                Ok(Some(blocks)) => sender.refill(&blocks),
                // Lingered out, or every request sender dropped: transfer over.
                Ok(None) | Err(_) => break,
            }
        } else {
            let delay = sender.inter_burst_delay();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                m = missing_rx.recv() => {
                    if let Some(blocks) = m {
                        sender.refill(&blocks);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Drives a Q-Block **receive** with loss recovery, returning the reassembled
/// body paired with the transfer's metadata carrier ([`QBlockReceiver::carrier`])
/// — or `None` if the transfer expired / the input closed first.
///
/// Feeds inbound byte PDUs from `pdu_rx` to `receiver`; on the recovery timer it
/// builds a missing-block request and sends it via `request_sink`. Timing is
/// driven by the elapsed time since the last accepted block or issued request.
pub async fn drive_receive<S: BlockSink + ?Sized>(
    mut receiver: QBlockReceiver,
    mut pdu_rx: mpsc::Receiver<Vec<u8>>,
    request_sink: &S,
) -> std::io::Result<Option<(Vec<u8>, Packet)>> {
    let mut last_activity = Instant::now();
    // Absolute deadline for the whole transfer: a dribbling peer keeps resetting
    // the per-block recovery backoff, so the retry cap alone never fires. This
    // bounds the buffer's lifetime regardless of activity (RFC 9177 §6.2).
    let partial_deadline = Instant::now() + receiver.partial_timeout();
    loop {
        let wait = match receiver.poll_recovery(last_activity.elapsed()) {
            RecoveryOutcome::Wait(d) => d,
            RecoveryOutcome::Resend(req) => {
                let bytes = req.to_bytes().map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                request_sink.send_block(bytes).await?;
                last_activity = Instant::now();
                continue;
            }
            RecoveryOutcome::Expired => return Ok(None),
        };

        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = tokio::time::sleep_until(partial_deadline) => return Ok(None),
            pdu = pdu_rx.recv() => {
                let Some(bytes) = pdu else { return Ok(None) };
                let Ok(pkt) = Packet::from_bytes(&bytes) else { continue };
                if let BlockOutcome::Complete(body) = receiver.accept(&pkt)? {
                    let carrier = receiver.carrier().unwrap_or(pkt);
                    return Ok(Some((body, carrier)));
                }
                last_activity = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use coap_lite::{MessageClass, MessageType, RequestType, ResponseType};
    use tokio::time::Instant;

    /// Records each sent block PDU with the (virtual) time it was sent.
    #[derive(Default)]
    struct RecordingSink {
        sent: Mutex<Vec<(Vec<u8>, Instant)>>,
    }

    #[async_trait]
    impl BlockSink for RecordingSink {
        async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
            self.sent.lock().unwrap().push((pdu, Instant::now()));
            Ok(())
        }
    }

    /// (block_num, more) decoded from a recorded Q-Block2 PDU.
    fn decode(pdu: &[u8]) -> (u16, bool, Vec<u8>) {
        let pkt = Packet::from_bytes(pdu).unwrap();
        let bv = pkt
            .get_first_option_as::<BlockValue>(CoapOption::QBlock2)
            .unwrap()
            .unwrap();
        (bv.num, bv.more, pkt.payload)
    }

    fn response_template() -> Packet {
        let mut p = Packet::new();
        p.header.set_type(MessageType::NonConfirmable);
        p.header.code = MessageClass::Response(ResponseType::Content);
        p.set_token(vec![0xAB, 0xCD]);
        p
    }

    fn sender(body: Vec<u8>, szx: u8, config: QBlockConfig) -> QBlockSender {
        QBlockSender::new(
            response_template(),
            CoapOption::QBlock2,
            body.into(),
            szx,
            TransferKind::Non,
            config,
            /* seed */ 0,
        )
    }

    #[tokio::test]
    async fn sends_every_block_in_order_with_correct_more_bit() {
        // 25 blocks of 16 bytes (szx=0).
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let sink = RecordingSink::default();

        let total = sender(body.clone(), 0, QBlockConfig::default())
            .run(&sink)
            .await
            .unwrap();

        assert_eq!(total, 25);
        let sent = sink.sent.lock().unwrap();
        assert_eq!(sent.len(), 25);
        for (n, (pdu, _)) in sent.iter().enumerate() {
            let (num, more, payload) = decode(pdu);
            assert_eq!(num as usize, n);
            assert_eq!(more, n < 24, "more bit wrong at block {n}");
            assert_eq!(payload, vec![n as u8; 16]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bursts_are_max_payloads_sized_and_spaced_by_non_timeout() {
        // 25 blocks, MAX_PAYLOADS=10 -> bursts of 10, 10, 5.
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let config = QBlockConfig {
            max_payloads: 10,
            non_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let sink = RecordingSink::default();
        let start = Instant::now();

        sender(body, 0, config.clone()).run(&sink).await.unwrap();

        let sent = sink.sent.lock().unwrap();
        // Bucket sends by the burst they landed in (relative whole seconds).
        let times: Vec<Duration> = sent.iter().map(|(_, t)| t.duration_since(start)).collect();

        // Burst 0: blocks 0..10 all at t=0.
        for t in &times[0..10] {
            assert_eq!(*t, Duration::ZERO);
        }
        // Two inter-burst gaps, each in [non_timeout, 1.5*non_timeout).
        let lo = config.non_timeout;
        let hi = config.non_timeout.mul_f64(1.5);
        let gap1 = times[10] - times[9];
        let gap2 = times[20] - times[19];
        assert!(gap1 >= lo && gap1 < hi, "gap1 {gap1:?} out of range");
        assert!(gap2 >= lo && gap2 < hi, "gap2 {gap2:?} out of range");
        // Within a burst there is no delay.
        assert_eq!(times[10], times[19]);
        assert_eq!(times[20], times[24]);
    }

    #[tokio::test(start_paused = true)]
    async fn single_burst_has_no_inter_burst_delay() {
        // 5 blocks < MAX_PAYLOADS -> one burst, no sleep, completes at t=0.
        let body: Vec<u8> = (0..5u16).flat_map(|i| [i as u8; 16]).collect();
        let sink = RecordingSink::default();
        let start = Instant::now();

        sender(body, 0, QBlockConfig::default())
            .run(&sink)
            .await
            .unwrap();

        let sent = sink.sent.lock().unwrap();
        assert_eq!(sent.len(), 5);
        for (_, t) in sent.iter() {
            assert_eq!(t.duration_since(start), Duration::ZERO);
        }
    }

    // ----- receiver (phase 3) -----

    /// Feeds each sent PDU straight into a [`QBlockReceiver`], capturing the
    /// reassembled body once the transfer completes.
    struct LoopbackSink {
        receiver: Mutex<QBlockReceiver>,
        completed: Mutex<Option<Vec<u8>>>,
    }

    #[async_trait]
    impl BlockSink for LoopbackSink {
        async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
            let pkt = Packet::from_bytes(&pdu).unwrap();
            let outcome = self.receiver.lock().unwrap().accept(&pkt).unwrap();
            if let BlockOutcome::Complete(body) = outcome {
                *self.completed.lock().unwrap() = Some(body);
            }
            Ok(())
        }
    }

    /// A client request template (the recovery base for a Q-Block2 receiver).
    fn request_template() -> Packet {
        let mut p = Packet::new();
        p.header.set_type(MessageType::NonConfirmable);
        p.header.code = MessageClass::Request(RequestType::Get);
        p.set_token(vec![0xAB, 0xCD]);
        p
    }

    fn receiver() -> QBlockReceiver {
        QBlockReceiver::new(
            CoapOption::QBlock2,
            request_template(),
            1 << 20,
            QBlockConfig::default(),
        )
    }

    fn block_pkt(num: u16, more: bool, szx: u8, payload: Vec<u8>) -> Packet {
        let mut p = response_template();
        let bv = BlockValue::new(num as usize, more, 1 << (szx + 4)).unwrap();
        p.add_option_as::<BlockValue>(CoapOption::QBlock2, bv);
        p.payload = payload;
        p
    }

    /// The block numbers carried by the repeated Q-Block2 options of a request.
    fn qblock2_nums(pkt: &Packet) -> Vec<u32> {
        pkt.get_options_as::<BlockValue>(CoapOption::QBlock2)
            .map(|l| {
                l.into_iter()
                    .filter_map(|r| r.ok())
                    .map(|b| u32::from(b.num))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drains the sender fully (no pacing) into a recording sink, returning the
    /// serialized block PDUs in send order.
    async fn drain_all(tx: &mut QBlockSender) -> Vec<Vec<u8>> {
        let sink = RecordingSink::default();
        while !tx.is_done() {
            tx.drain_burst(&sink).await.unwrap();
        }
        let sent = sink.sent.lock().unwrap();
        sent.iter().map(|(pdu, _)| pdu.clone()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn loopback_sender_to_receiver_reassembles_body() {
        // 25 blocks across multiple bursts, fed straight back into a receiver.
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let sink = LoopbackSink {
            receiver: Mutex::new(receiver()),
            completed: Mutex::new(None),
        };

        sender(body.clone(), 0, QBlockConfig::default())
            .run(&sink)
            .await
            .unwrap();

        assert_eq!(sink.completed.lock().unwrap().as_ref(), Some(&body));
    }

    #[test]
    fn receiver_reassembles_out_of_order_blocks() {
        // 5 full 16-byte blocks delivered shuffled; only the last fed block
        // (which makes the set contiguous) should complete.
        let body: Vec<u8> = (0..5u8).flat_map(|i| [i; 16]).collect();
        let mut rx = receiver();

        let order = [4u16, 2, 0, 3, 1];
        for (i, &num) in order.iter().enumerate() {
            let more = num != 4; // block 4 is the last
            let chunk = body[num as usize * 16..num as usize * 16 + 16].to_vec();
            let outcome = rx.accept(&block_pkt(num, more, 0, chunk)).unwrap();
            if i + 1 == order.len() {
                assert_eq!(outcome, BlockOutcome::Complete(body.clone()));
            } else {
                assert_eq!(outcome, BlockOutcome::Accepted);
                assert!(!rx.is_complete());
            }
        }
    }

    #[test]
    fn receiver_handles_partial_final_block() {
        // 40 bytes at szx=0 -> blocks [0..16],[16..32],[32..40] (last is 8 bytes).
        let body: Vec<u8> = (0..40u8).collect();
        let mut rx = receiver();

        assert_eq!(
            rx.accept(&block_pkt(0, true, 0, body[0..16].to_vec()))
                .unwrap(),
            BlockOutcome::Accepted
        );
        assert_eq!(
            rx.accept(&block_pkt(1, true, 0, body[16..32].to_vec()))
                .unwrap(),
            BlockOutcome::Accepted
        );
        assert_eq!(
            rx.accept(&block_pkt(2, false, 0, body[32..40].to_vec()))
                .unwrap(),
            BlockOutcome::Complete(body)
        );
    }

    #[test]
    fn receiver_returns_not_qblock_for_plain_pdu() {
        let mut rx = receiver();
        let mut p = response_template();
        p.payload = b"hello".to_vec();
        assert_eq!(rx.accept(&p).unwrap(), BlockOutcome::NotQBlock);
    }

    #[test]
    fn receiver_rejects_body_exceeding_max() {
        let mut rx = QBlockReceiver::new(
            CoapOption::QBlock2,
            request_template(),
            32,
            QBlockConfig::default(),
        );
        // Block 10 at szx=0 starts at offset 160, well past the 32-byte cap.
        let err = rx
            .accept(&block_pkt(10, false, 0, vec![0u8; 16]))
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn receiver_rejects_size_option_exceeding_max() {
        // A peer can advertise a Size2/Size1 total far larger than any block it
        // actually sends. The opening block fits under the cap, but the
        // advertised total feeds final_block() -> RangeSet::missing(), which
        // would allocate a Vec of ~total/block_size entries on the first
        // recovery tick (a one-datagram OOM). The advertised total must be
        // rejected at the same threshold as a realized over-cap block.
        let mut rx = QBlockReceiver::new(
            CoapOption::QBlock2,
            request_template(),
            32,
            QBlockConfig::default(),
        );
        // Block 0 fits (16 B <= 32 B cap), but carries a Size2 of u32::MAX.
        let mut pkt = block_pkt(0, true, 0, vec![0u8; 16]);
        pkt.add_option_as::<OptionValueU32>(CoapOption::Size2, OptionValueU32(u32::MAX));
        let err = rx.accept(&pkt).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn receiver_rejects_changed_block_size() {
        let mut rx = receiver();
        rx.accept(&block_pkt(0, true, 0, vec![0u8; 16])).unwrap();
        // Second block claims a different szx.
        let err = rx
            .accept(&block_pkt(1, false, 2, vec![0u8; 64]))
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    // ----- recovery (phase 4) -----

    /// Feeds `pdus` to `rx`, skipping the indices in `drop`. Returns the last
    /// outcome.
    fn feed_except(rx: &mut QBlockReceiver, pdus: &[Vec<u8>], drop: &[usize]) -> BlockOutcome {
        let mut last = BlockOutcome::NotQBlock;
        for (i, pdu) in pdus.iter().enumerate() {
            if drop.contains(&i) {
                continue;
            }
            let pkt = Packet::from_bytes(pdu).unwrap();
            last = rx.accept(&pkt).unwrap();
        }
        last
    }

    #[tokio::test]
    async fn interior_loss_is_requested_and_recovered() {
        // 25 blocks; lose blocks 3 and 17 on the first pass.
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let mut tx = sender(body.clone(), 0, QBlockConfig::default());
        let first_pass = drain_all(&mut tx).await;

        let mut rx = receiver();
        assert_eq!(
            feed_except(&mut rx, &first_pass, &[3, 17]),
            BlockOutcome::Accepted
        );
        assert!(!rx.is_complete());

        // Recovery: a missing-block request naming exactly the lost blocks.
        let RecoveryOutcome::Resend(req) = rx.poll_recovery(Duration::from_secs(10)) else {
            panic!("expected a resend request");
        };
        assert_eq!(qblock2_nums(&req), vec![3, 17]);

        // The sender re-queues and resends them; the body then completes.
        tx.refill(&qblock2_nums(&req));
        let resent = drain_all(&mut tx).await;
        assert_eq!(resent.len(), 2);
        let outcome = feed_except(&mut rx, &resent, &[]);
        assert_eq!(outcome, BlockOutcome::Complete(body));
    }

    #[tokio::test]
    async fn lost_final_block_is_recovered_via_size_option() {
        // Lose the *last* block: its More-unset signal never arrives, so the
        // receiver only knows the total from the Size2 option the sender sends.
        let body: Vec<u8> = (0..12u16).flat_map(|i| [i as u8; 16]).collect();
        let mut tx = sender(body.clone(), 0, QBlockConfig::default());
        let first_pass = drain_all(&mut tx).await; // 12 blocks (0..=11)

        let mut rx = receiver();
        feed_except(&mut rx, &first_pass, &[11]);
        assert!(!rx.is_complete());
        // No More-unset block seen, but Size2 fixes the final block.
        assert_eq!(rx.rec_blocks().total_blocks(), None);

        let RecoveryOutcome::Resend(req) = rx.poll_recovery(Duration::from_secs(10)) else {
            panic!("expected a resend request");
        };
        assert_eq!(qblock2_nums(&req), vec![11]);

        tx.refill(&qblock2_nums(&req));
        let resent = drain_all(&mut tx).await;
        assert_eq!(
            feed_except(&mut rx, &resent, &[]),
            BlockOutcome::Complete(body)
        );
    }

    #[test]
    fn recovery_backoff_doubles_and_caps_at_non_max_retransmit() {
        // 5 blocks, lose block 2; block 4 (More unset) fixes the total.
        let body: Vec<u8> = (0..5u8).flat_map(|i| [i; 16]).collect();
        let mut rx = receiver();
        for n in [0u16, 1, 3, 4] {
            let more = n != 4;
            let chunk = body[n as usize * 16..n as usize * 16 + 16].to_vec();
            rx.accept(&block_pkt(n, more, 0, chunk)).unwrap();
        }

        let base = Duration::from_secs(4); // NON_RECEIVE_TIMEOUT
                                           // Too early on the first round.
        assert_eq!(
            rx.poll_recovery(Duration::from_secs(3)),
            RecoveryOutcome::Wait(Duration::from_secs(1))
        );

        // Four resends with exponential deadlines 4,8,16,32 s, each naming [2].
        for retry in 0..4u32 {
            let deadline = base * (1 << retry);
            let RecoveryOutcome::Resend(req) = rx.poll_recovery(deadline) else {
                panic!("expected resend at retry {retry}");
            };
            assert_eq!(qblock2_nums(&req), vec![2]);
        }
        // Fifth round (deadline 64 s) is past the cap.
        assert_eq!(
            rx.poll_recovery(Duration::from_secs(64)),
            RecoveryOutcome::Expired
        );
    }

    #[test]
    fn accept_resets_recovery_backoff() {
        let body: Vec<u8> = (0..5u8).flat_map(|i| [i; 16]).collect();
        let mut rx = receiver();
        for n in [0u16, 3, 4] {
            let more = n != 4;
            let chunk = body[n as usize * 16..n as usize * 16 + 16].to_vec();
            rx.accept(&block_pkt(n, more, 0, chunk)).unwrap();
        }
        // Spend a retry (deadline 4 s -> retry becomes 1).
        assert!(matches!(
            rx.poll_recovery(Duration::from_secs(4)),
            RecoveryOutcome::Resend(_)
        ));
        // A freshly accepted block resets the backoff: deadline back to 4 s.
        rx.accept(&block_pkt(1, true, 0, body[16..32].to_vec()))
            .unwrap();
        assert_eq!(
            rx.poll_recovery(Duration::from_secs(3)),
            RecoveryOutcome::Wait(Duration::from_secs(1))
        );
    }

    #[test]
    fn qblock1_recovery_builds_408_missing_blocks_cbor_seq() {
        // Server side: a Q-Block1 receiver reports missing request blocks as a
        // 4.08 carrying application/missing-blocks+cbor-seq.
        let mut rx = QBlockReceiver::new(
            CoapOption::QBlock1,
            request_template(),
            1 << 20,
            QBlockConfig::default(),
        );
        // Receive blocks 0 and 2 (More unset) of a 3-block request; lose 1.
        for (num, more) in [(0u16, true), (2, false)] {
            let mut p = Packet::new();
            p.header.set_type(MessageType::NonConfirmable);
            p.header.code = MessageClass::Request(RequestType::Put);
            let bv = BlockValue::new(num as usize, more, 16).unwrap();
            p.add_option_as::<BlockValue>(CoapOption::QBlock1, bv);
            p.payload = vec![num as u8; 16];
            rx.accept(&p).unwrap();
        }

        let RecoveryOutcome::Resend(pdu) = rx.poll_recovery(Duration::from_secs(10)) else {
            panic!("expected a 4.08 resend");
        };
        assert_eq!(
            pdu.header.code,
            MessageClass::Response(ResponseType::RequestEntityIncomplete)
        );
        let cf = pdu
            .get_first_option_as::<OptionValueU16>(CoapOption::ContentFormat)
            .unwrap()
            .unwrap();
        assert_eq!(
            usize::from(cf.0),
            usize::from(ContentFormat::ApplicationMissingBlocksCborSeq)
        );
        assert_eq!(missing_blocks::decode(&pdu.payload).unwrap(), vec![1]);
    }

    // ----- Q-Block1 + Request-Tag correlation (phase 5) -----

    /// A Q-Block1 (large request) sender tagged with `rtag` for correlation.
    fn q1_sender(body: Vec<u8>, rtag: &[u8]) -> QBlockSender {
        let mut t = Packet::new();
        t.header.set_type(MessageType::NonConfirmable);
        t.header.code = MessageClass::Request(RequestType::Put);
        t.set_token(vec![0x11, 0x22]);
        t.add_option(CoapOption::Unknown(292), rtag.to_vec());
        QBlockSender::new(
            t,
            CoapOption::QBlock1,
            body.into(),
            0,
            TransferKind::Non,
            QBlockConfig::default(),
            0,
        )
    }

    #[tokio::test]
    async fn registry_demuxes_concurrent_qblock1_transfers_by_rtag() {
        // Two large requests in flight at once, distinguished only by Request-Tag.
        let body_a: Vec<u8> = (0..12u16).flat_map(|i| [0xA0 | i as u8; 16]).collect();
        let body_b: Vec<u8> = (0..7u16).flat_map(|i| [0xB0 | i as u8; 16]).collect();
        let a = drain_all(&mut q1_sender(body_a.clone(), b"A")).await;
        let b = drain_all(&mut q1_sender(body_b.clone(), b"B")).await;

        let mut reg = QBlockReceivers::new(1 << 20, QBlockConfig::default());
        let mut done: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // Interleave the two transfers' blocks.
        for i in 0..a.len().max(b.len()) {
            for pass in [a.get(i), b.get(i)].into_iter().flatten() {
                let pkt = Packet::from_bytes(pass).unwrap();
                if let Some((rtag, BlockOutcome::Complete(body))) = reg.accept(&pkt).unwrap() {
                    done.insert(rtag, body);
                }
            }
        }

        assert_eq!(done.get(b"A".as_slice()), Some(&body_a));
        assert_eq!(done.get(b"B".as_slice()), Some(&body_b));
        assert!(reg.is_empty(), "completed transfers should be reaped");
    }

    #[tokio::test]
    async fn registry_recovers_missing_qblock1_block() {
        let body: Vec<u8> = (0..5u16).flat_map(|i| [i as u8; 16]).collect();
        let mut tx = q1_sender(body.clone(), b"X");
        let pass1 = drain_all(&mut tx).await;

        let mut reg = QBlockReceivers::new(1 << 20, QBlockConfig::default());
        for (i, pdu) in pass1.iter().enumerate() {
            if i == 2 {
                continue; // lose block 2
            }
            reg.accept(&Packet::from_bytes(pdu).unwrap()).unwrap();
        }
        assert_eq!(reg.len(), 1);

        // The registry issues a 4.08 naming the missing block.
        let Some(RecoveryOutcome::Resend(req)) = reg.poll_recovery(b"X", Duration::from_secs(10))
        else {
            panic!("expected a 4.08 resend request");
        };
        assert_eq!(missing_blocks::decode(&req.payload).unwrap(), vec![2]);

        // Sender resends; the body completes and the transfer is reaped.
        tx.refill(&[2]);
        let resent = drain_all(&mut tx).await;
        let mut last = None;
        for pdu in &resent {
            last = reg.accept(&Packet::from_bytes(pdu).unwrap()).unwrap();
        }
        assert!(matches!(last, Some((_, BlockOutcome::Complete(ref b))) if *b == body));
        assert!(reg.is_empty());
    }

    #[test]
    fn registry_ignores_non_qblock1_pdu() {
        let mut reg = QBlockReceivers::new(1 << 20, QBlockConfig::default());
        let mut p = response_template();
        p.payload = b"plain".to_vec();
        assert_eq!(reg.accept(&p).unwrap(), None);
    }

    // ----- transport drivers (wiring) -----

    #[tokio::test(start_paused = true)]
    async fn drive_send_sends_whole_body_then_lingers_out() {
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let sink = RecordingSink::default();
        // No retransmit requests: drop the request sender so the linger ends at once.
        let (req_tx, req_rx) = mpsc::channel::<Vec<u32>>(1);
        drop(req_tx);

        drive_send(
            sender(body, 0, QBlockConfig::default()),
            &sink,
            req_rx,
            Duration::from_secs(120),
        )
        .await
        .unwrap();

        assert_eq!(sink.sent.lock().unwrap().len(), 25);
    }

    #[tokio::test(start_paused = true)]
    async fn drive_receive_assembles_body_without_recovery() {
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();
        let pdus = drain_all(&mut sender(body.clone(), 0, QBlockConfig::default())).await;

        let (tx, rx) = mpsc::channel::<Vec<u8>>(256);
        for pdu in pdus {
            tx.send(pdu).await.unwrap();
        }
        drop(tx);

        let req_sink = RecordingSink::default();
        let got = drive_receive(receiver(), rx, &req_sink).await.unwrap();

        assert_eq!(got.map(|(body, _carrier)| body), Some(body));
        assert!(
            req_sink.sent.lock().unwrap().is_empty(),
            "no recovery requests expected on a lossless transfer"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drive_receive_abandons_partial_body_after_partial_timeout() {
        // A single non-final block of a larger body, with no Size2 option: the
        // total is never known, so recovery can never build a request and never
        // Expires — without an absolute partial-timeout the transfer would pin its
        // buffer forever (a slow-drip DoS). Feed it, keep the channel open, and
        // assert the transfer is abandoned at `non_partial_timeout`.
        let cfg = QBlockConfig {
            non_receive_timeout: Duration::from_secs(4),
            non_partial_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
        let blk = block_pkt(0, /* more */ true, 0, vec![0u8; 16]);
        tx.send(blk.to_bytes().unwrap()).await.unwrap();

        let req_sink = RecordingSink::default();
        let receiver =
            QBlockReceiver::new(CoapOption::QBlock2, request_template(), 1 << 20, cfg);
        // `tx` stays alive across the await, so the channel never closes — the
        // *only* way out is the partial-timeout (proves it, not a closed channel).
        let got = drive_receive(receiver, rx, &req_sink).await.unwrap();
        assert_eq!(
            got, None,
            "a stalled partial transfer must be abandoned at non_partial_timeout"
        );
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn drivers_complete_a_lossy_transfer_end_to_end() {
        use std::collections::HashSet;

        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();

        // sender -> receiver block PDUs; receiver -> sender missing-block requests.
        let (pdu_tx, pdu_rx) = mpsc::channel::<Vec<u8>>(256);
        let (miss_tx, miss_rx) = mpsc::channel::<Vec<u32>>(16);

        // Drops blocks 3 and 17 exactly once; they get through on retransmit.
        struct LossySink {
            tx: mpsc::Sender<Vec<u8>>,
            drop_once: Mutex<HashSet<u16>>,
        }
        #[async_trait]
        impl BlockSink for LossySink {
            async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
                let pkt = Packet::from_bytes(&pdu).unwrap();
                let num = pkt
                    .get_first_option_as::<BlockValue>(CoapOption::QBlock2)
                    .unwrap()
                    .unwrap()
                    .num;
                if self.drop_once.lock().unwrap().remove(&num) {
                    return Ok(()); // lost on first transmission
                }
                let _ = self.tx.send(pdu).await;
                Ok(())
            }
        }

        // Turns the receiver's missing-block requests back into block numbers.
        struct ReqSink {
            tx: mpsc::Sender<Vec<u32>>,
        }
        #[async_trait]
        impl BlockSink for ReqSink {
            async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
                let pkt = Packet::from_bytes(&pdu).unwrap();
                let _ = self
                    .tx
                    .send(parse_missing_request(&pkt, CoapOption::QBlock2))
                    .await;
                Ok(())
            }
        }

        let lossy = LossySink {
            tx: pdu_tx,
            drop_once: Mutex::new([3u16, 17].into_iter().collect()),
        };
        let req_sink = ReqSink { tx: miss_tx };

        let (send_res, recv_res) = tokio::join!(
            drive_send(
                sender(body.clone(), 0, QBlockConfig::default()),
                &lossy,
                miss_rx,
                Duration::from_secs(120),
            ),
            drive_receive(receiver(), pdu_rx, &req_sink),
        );

        send_res.unwrap();
        assert_eq!(recv_res.unwrap().map(|(body, _)| body), Some(body));
    }

    #[tokio::test]
    async fn end_to_end_over_real_udp_loopback_with_loss() {
        use std::collections::HashSet;
        use std::net::SocketAddr;
        use tokio::net::UdpSocket;

        // Real (unpaused) tokio timers — just short so the test is quick.
        let cfg = QBlockConfig {
            non_timeout: Duration::from_millis(20),
            non_receive_timeout: Duration::from_millis(40),
            ..Default::default()
        };

        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_addr = client_sock.local_addr().unwrap();

        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();

        let (pdu_tx, pdu_rx) = mpsc::channel::<Vec<u8>>(256);
        let (miss_tx, miss_rx) = mpsc::channel::<Vec<u32>>(16);

        // A real-UDP BlockSink with one-time loss injection on the named blocks.
        struct UdpSink {
            sock: Arc<UdpSocket>,
            peer: SocketAddr,
            drop_once: Mutex<HashSet<u16>>,
        }
        #[async_trait]
        impl BlockSink for UdpSink {
            async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
                if let Ok(pkt) = Packet::from_bytes(&pdu) {
                    if let Some(Ok(bv)) = pkt.get_first_option_as::<BlockValue>(CoapOption::QBlock2)
                    {
                        if self.drop_once.lock().unwrap().remove(&bv.num) {
                            return Ok(()); // lost on the wire, once
                        }
                    }
                }
                self.sock.send_to(&pdu, self.peer).await.map(|_| ())
            }
        }

        let to_client = UdpSink {
            sock: server_sock.clone(),
            peer: client_addr,
            drop_once: Mutex::new([3u16, 17].into_iter().collect()),
        };
        let to_server = UdpSink {
            sock: client_sock.clone(),
            peer: server_addr,
            drop_once: Mutex::new(HashSet::new()),
        };

        // Socket readers bridge inbound datagrams into the driver channels.
        let cs = client_sock.clone();
        let reader_c = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((n, _)) = cs.recv_from(&mut buf).await {
                if pdu_tx.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });
        let ss = server_sock.clone();
        let reader_s = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((n, _)) = ss.recv_from(&mut buf).await {
                if let Ok(pkt) = Packet::from_bytes(&buf[..n]) {
                    let nums = parse_missing_request(&pkt, CoapOption::QBlock2);
                    if miss_tx.send(nums).await.is_err() {
                        break;
                    }
                }
            }
        });

        let sender = QBlockSender::new(
            response_template(),
            CoapOption::QBlock2,
            body.clone().into(),
            0,
            TransferKind::Non,
            cfg.clone(),
            0,
        );
        let rx = QBlockReceiver::new(CoapOption::QBlock2, request_template(), 1 << 20, cfg);

        let (send_res, recv_res) = tokio::join!(
            drive_send(sender, &to_client, miss_rx, Duration::from_millis(500)),
            drive_receive(rx, pdu_rx, &to_server),
        );

        send_res.unwrap();
        assert_eq!(recv_res.unwrap().map(|(body, _)| body), Some(body));
        reader_c.abort();
        reader_s.abort();
    }
}
