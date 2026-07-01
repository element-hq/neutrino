//! Sliding-window reliable channel over BLE GATT (Selective Repeat ARQ).
//!
//! Provides per-fragment acknowledgement and retransmission on top of
//! unreliable GATT write-without-response / notifications.
//!
//! # Wire format
//!
//! Data fragments carry a 2-byte header, a payload, and a 1-byte canary
//! trailer ([`FRAGMENT_CANARY`], `0x5A`).  The canary lets the receiver detect
//! silent host-stack truncation (e.g. Android silently dropping the last few
//! bytes of an oversized write):
//!
//! ```text
//!   [header: 2 bytes][payload: 1..N bytes][canary: 0x5A]
//! ```
//!
//! Pure ACKs (no payload) carry only the header — no canary:
//!
//! ```text
//!   [header: 2 bytes]
//! ```
//!
//! Header layout:
//!
//! ```text
//!   Byte 0:
//!     Bits 0-3: SEQ    -- 4-bit sequence number (0-15)
//!     Bit  4:   FIRST  -- first fragment of a new datagram
//!     Bit  5:   LAST   -- last fragment of a datagram
//!     Bits 6-7: reserved (0)
//!
//!   Byte 1:
//!     Bits 0-3: ACK_SEQ -- cumulative ACK: all seq up to ACK_SEQ received
//!     Bit  4:   ACK     -- ACK_SEQ field is valid
//!     Bits 5-7: reserved (0)
//! ```
//!
//! FIRST+LAST = single-fragment datagram.  ACK without data = pure
//! acknowledgement.  Any combination with ACK = piggybacked acknowledgement.
//!
//! # Protocol
//!
//! Selective Repeat ARQ with a sliding window of [`WINDOW_SIZE`] fragments:
//!
//! 1. Sender transmits up to WINDOW_SIZE fragments with inter-frame pacing.
//! 2. Receiver buffers out-of-order fragments within the window.
//! 3. Receiver sends cumulative ACKs as gaps are filled.
//! 4. Sender slides its window forward on receiving cumulative ACKs.
//! 5. On timeout, sender retransmits only the oldest un-ACKed fragment
//!    (the one most likely lost), not the entire window.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{debug, trace, warn};

use super::mtu::MAX_DATAGRAM_SIZE;

const HEADER_SIZE: usize = 2;

/// Trailer byte appended to every outbound fragment so the receiver can
/// detect silent host-stack truncation (e.g. Android dropping the last 3
/// bytes of every write when MTU and chunk size disagree). `0x5A` is a
/// recognizable non-zero sentinel — any fixed non-zero byte works equally.
pub const FRAGMENT_CANARY: u8 = 0x5A;

const SEQ_MASK: u8 = 0x0F;
pub const FLAG_FIRST: u8 = 0x10;
pub const FLAG_LAST: u8 = 0x20;

const ACK_SEQ_MASK: u8 = 0x0F;
const FLAG_ACK: u8 = 0x10;

const SEQ_MODULUS: u8 = 16;

fn make_header(seq: u8, first: bool, last: bool) -> [u8; 2] {
    let b0 = (seq & SEQ_MASK)
        | (if first { FLAG_FIRST } else { 0 })
        | (if last { FLAG_LAST } else { 0 });
    [b0, 0]
}

fn set_ack(header: &mut [u8], ack_seq: u8) {
    header[1] = (ack_seq & ACK_SEQ_MASK) | FLAG_ACK;
}

fn seq_dist(a: u8, b: u8) -> u8 {
    b.wrapping_sub(a) % SEQ_MODULUS
}

/// Must be < SEQ_MODULUS / 2 for correct modular window arithmetic.
const WINDOW_SIZE: u8 = 6;

/// Pacing delay between fragments to avoid overwhelming the BLE controller's
/// write-without-response buffer.
const INTER_FRAME_GAP: Duration = Duration::from_millis(3);

/// BLE round-trips are typically 120-200ms; 300ms gives margin for jitter.
const ACK_TIMEOUT: Duration = Duration::from_millis(300);

const ACK_TIMEOUT_MAX: Duration = Duration::from_secs(5);

/// Delayed-ACK: gives outgoing data a chance to piggyback the ACK.
const ACK_DELAY: Duration = Duration::from_millis(15);

/// Hard wall-clock budget for forward progress. If the head of `in_flight` does
/// not advance within this window, the send loop declares `LinkDead` regardless
/// of how many retransmits have happened. Chosen to be close to iroh's default
/// `default_path_max_idle_timeout` (6s) so BLE and QUIC give up in the same
/// ballpark, and so a disappearing peer is detected in seconds — not minutes.
const LINK_DEAD_DEADLINE: Duration = Duration::from_secs(6);

const SEND_QUEUE_CAPACITY: usize = 32;
struct InFlightFragment {
    seq: u8,
    wire_msg: Vec<u8>,
}

struct FragmentEntry {
    payload: Vec<u8>,
    first: bool,
    last: bool,
}

struct BufferedFragment {
    seq: u8,
    first: bool,
    last: bool,
    payload: Vec<u8>,
}
/// Returned by [`ReliableChannel::run_send_loop`] when the link is declared dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkDead;

struct ChannelState {
    send_queue: VecDeque<Vec<u8>>,
    frag_queue: VecDeque<FragmentEntry>,
    send_next: u8,
    send_base: u8,
    in_flight: VecDeque<InFlightFragment>,
    recv_next: u8,
    reassembly: Vec<u8>,
    /// Subsequent fragments are dropped until the next FIRST fragment resets this.
    reassembly_overflow: bool,
    recv_buf: Vec<BufferedFragment>,
    ack_pending: Option<u8>,
    /// Delayed-ACK deadline; allows outgoing data to piggyback the ACK first.
    ack_deadline: Option<tokio::time::Instant>,
    /// Monotonic timestamp of the last forward-progress event — i.e. the last
    /// cumulative ACK that advanced `send_base`. The send loop uses this as
    /// the anchor for `LINK_DEAD_DEADLINE`; a stuck peer is detected purely
    /// by wall-clock silence, never by retry count.
    last_progress_at: tokio::time::Instant,
    link_dead: bool,
}

impl ChannelState {
    fn in_flight_count(&self) -> u8 {
        seq_dist(self.send_base, self.send_next)
    }

    fn can_send(&self) -> bool {
        self.in_flight_count() < WINDOW_SIZE
    }

    fn in_recv_window(&self, seq: u8) -> bool {
        let dist = seq_dist(self.recv_next, seq);
        dist < WINDOW_SIZE
    }

    fn is_buffered(&self, seq: u8) -> bool {
        self.recv_buf.iter().any(|f| f.seq == seq)
    }

    fn schedule_ack(&mut self, seq: u8) {
        self.ack_pending = Some(seq);
        if self.ack_deadline.is_none() {
            self.ack_deadline = Some(tokio::time::Instant::now() + ACK_DELAY);
        }
    }

    fn take_ack(&mut self) -> Option<u8> {
        self.ack_deadline = None;
        self.ack_pending.take()
    }
}

/// Bidirectional reliable channel over BLE GATT.
///
/// Does not perform BLE I/O directly. Enqueue datagrams, spawn
/// [`run_send_loop`] with a write callback, and feed incoming GATT
/// values into [`receive_fragment`].
pub struct ReliableChannel {
    state: Arc<Mutex<ChannelState>>,
    wake: Arc<Notify>,
    datagram_tx: mpsc::Sender<Vec<u8>>,
    /// Mutable so the pipe can start with a conservative floor and be updated
    /// once `resolve_chunk_size` lands — see `pipe::run_gatt_pipe`.
    chunk_size: AtomicUsize,
    send_waker: Arc<atomic_waker::AtomicWaker>,
    retransmit_counter: Arc<AtomicU64>,
    truncation_counter: Arc<AtomicU64>,
}

impl ReliableChannel {
    /// Create a new channel. `retransmit_counter` is incremented on each retransmit.
    pub fn new(
        chunk_size: usize,
        retransmit_counter: Arc<AtomicU64>,
        truncation_counter: Arc<AtomicU64>,
    ) -> (Self, mpsc::Receiver<Vec<u8>>) {
        let (datagram_tx, datagram_rx) = mpsc::channel(64);
        let ch = ReliableChannel {
            state: Arc::new(Mutex::new(ChannelState {
                send_queue: VecDeque::new(),
                frag_queue: VecDeque::new(),
                send_next: 0,
                send_base: 0,
                in_flight: VecDeque::new(),
                recv_next: 0,
                reassembly: Vec::new(),
                reassembly_overflow: false,
                recv_buf: Vec::new(),
                ack_pending: None,
                ack_deadline: None,
                last_progress_at: tokio::time::Instant::now(),
                link_dead: false,
            })),
            wake: Arc::new(Notify::new()),
            datagram_tx,
            chunk_size: AtomicUsize::new(chunk_size),
            send_waker: Arc::new(atomic_waker::AtomicWaker::new()),
            retransmit_counter,
            truncation_counter,
        };
        (ch, datagram_rx)
    }

    /// Update the outbound fragment chunk size. Affects future calls to
    /// `fragment_into`; fragments already split into `frag_queue` or sitting
    /// in `in_flight` keep their existing sizing so retransmits stay
    /// consistent. Intended to be called once per channel lifetime when the
    /// async MTU resolver lands a sane reading — see `pipe::run_gatt_pipe`.
    pub fn set_chunk_size(&self, chunk_size: usize) {
        self.chunk_size.store(chunk_size, Ordering::Relaxed);
    }

    /// Signal that the underlying link is gone — typically because blew has
    /// surfaced a `CentralDisconnected` event or the pipe owner is tearing
    /// down. Flips `link_dead` and wakes the send loop so it exits with
    /// `LinkDead` on its next poll instead of waiting for the
    /// `LINK_DEAD_DEADLINE` budget to burn.
    pub async fn mark_dead(&self) {
        self.state.lock().await.link_dead = true;
        self.wake.notify_one();
    }

    /// Queue a datagram for reliable delivery. Returns `false` if the queue is full.
    pub async fn enqueue_datagram(&self, data: Vec<u8>) -> bool {
        let mut state = self.state.lock().await;
        if state.send_queue.len() + state.frag_queue.len() >= SEND_QUEUE_CAPACITY {
            return false;
        }
        state.send_queue.push_back(data);
        drop(state);
        self.wake.notify_one();
        true
    }

    /// Non-blocking enqueue for `poll_send`. Returns `None` if the lock is contended.
    pub fn try_enqueue_datagram(&self, data: Vec<u8>) -> Option<bool> {
        let mut state = self.state.try_lock().ok()?;
        if state.send_queue.len() + state.frag_queue.len() >= SEND_QUEUE_CAPACITY {
            return Some(false);
        }
        state.send_queue.push_back(data);
        drop(state);
        self.wake.notify_one();
        Some(true)
    }

    /// Register a waker to be notified when queue space opens up.
    pub fn register_send_waker(&self, waker: &std::task::Waker) {
        self.send_waker.register(waker);
    }

    /// Process an incoming GATT value.
    pub async fn receive_fragment(&self, value: &[u8]) {
        if value.len() < HEADER_SIZE {
            return;
        }
        // Pure ACKs are exactly HEADER_SIZE bytes and carry no canary; the
        // sender emits them without a trailer (see next_send_action()).
        let value = if value.len() > HEADER_SIZE {
            if value.last().copied() != Some(FRAGMENT_CANARY) {
                self.truncation_counter.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    len = value.len(),
                    last_byte = ?value.last(),
                    expected_last = FRAGMENT_CANARY,
                    "fragment canary mismatch — silent host-stack truncation suspected"
                );
                return;
            }
            &value[..value.len() - 1]
        } else {
            value
        };
        let b0 = value[0];
        let b1 = value[1];
        let payload = &value[HEADER_SIZE..];

        let seq = b0 & SEQ_MASK;
        let first = b0 & FLAG_FIRST != 0;
        let last = b0 & FLAG_LAST != 0;
        let has_ack = b1 & FLAG_ACK != 0;
        let ack_seq = b1 & ACK_SEQ_MASK;

        let mut state = self.state.lock().await;
        if has_ack {
            let acked_count = seq_dist(state.send_base, (ack_seq + 1) % SEQ_MODULUS);
            // Reject stale/bogus ACKs that would advance send_base past send_next.
            if acked_count > 0
                && acked_count <= WINDOW_SIZE
                && acked_count <= state.in_flight_count()
            {
                let to_remove = acked_count as usize;
                let actually_remove = to_remove.min(state.in_flight.len());
                for _ in 0..actually_remove {
                    state.in_flight.pop_front();
                }
                state.send_base = (ack_seq + 1) % SEQ_MODULUS;
                state.last_progress_at = tokio::time::Instant::now();
                trace!(
                    ack_seq,
                    new_base = state.send_base,
                    in_flight = state.in_flight.len(),
                    "cumulative ACK received"
                );
                self.send_waker.wake();
                self.wake.notify_one();
            }
        }
        let has_data = first || last || !payload.is_empty();
        if has_data {
            if seq == state.recv_next {
                self.accept_and_deliver(&mut state, first, last, payload)
                    .await;
                self.drain_recv_buf(&mut state).await;
            } else if state.in_recv_window(seq) && !state.is_buffered(seq) {
                trace!(
                    seq,
                    expected = state.recv_next,
                    "buffering out-of-order fragment"
                );
                state.recv_buf.push(BufferedFragment {
                    seq,
                    first,
                    last,
                    payload: payload.to_vec(),
                });
                // Re-ACK current position to signal the gap.
                let ack_seq = (state.recv_next + SEQ_MODULUS - 1) % SEQ_MODULUS;
                state.schedule_ack(ack_seq);
                self.wake.notify_one();
            } else if !state.in_recv_window(seq) {
                trace!(
                    seq,
                    expected = state.recv_next,
                    "duplicate fragment, re-ACKing"
                );
                let ack_seq = (state.recv_next + SEQ_MODULUS - 1) % SEQ_MODULUS;
                state.schedule_ack(ack_seq);
                self.wake.notify_one();
            }
        }
    }

    async fn accept_and_deliver(
        &self,
        state: &mut ChannelState,
        first: bool,
        last: bool,
        payload: &[u8],
    ) {
        trace!(
            seq = state.recv_next,
            first,
            last,
            len = payload.len(),
            "accepted fragment"
        );

        if first {
            state.reassembly.clear();
            state.reassembly_overflow = false;
        }

        if !state.reassembly_overflow {
            state.reassembly.extend_from_slice(payload);
            if state.reassembly.len() > MAX_DATAGRAM_SIZE {
                warn!(
                    len = state.reassembly.len(),
                    max = MAX_DATAGRAM_SIZE,
                    "reassembly exceeded max size, discarding datagram"
                );
                state.reassembly.clear();
                state.reassembly_overflow = true;
            }
        }

        if last {
            state.reassembly_overflow = false;
            let complete = std::mem::take(&mut state.reassembly);
            if !complete.is_empty() {
                let _ = self.datagram_tx.send(complete).await;
            }
        }

        state.recv_next = (state.recv_next + 1) % SEQ_MODULUS;
        let ack_seq = (state.recv_next + SEQ_MODULUS - 1) % SEQ_MODULUS;
        state.schedule_ack(ack_seq);
        self.wake.notify_one();
    }

    async fn drain_recv_buf(&self, state: &mut ChannelState) {
        loop {
            let pos = state.recv_buf.iter().position(|f| f.seq == state.recv_next);
            match pos {
                Some(idx) => {
                    let frag = state.recv_buf.remove(idx);
                    self.accept_and_deliver(state, frag.first, frag.last, &frag.payload)
                        .await;
                }
                None => break,
            }
        }
    }

    /// Run the send loop as a background task. Returns `Err(LinkDead)` if the
    /// link dies.
    ///
    /// Liveness is gated by `LINK_DEAD_DEADLINE`: the send loop declares
    /// `LinkDead` if `last_progress_at` — the instant of the most recent
    /// cumulative ACK that advanced `send_base` — is older than the deadline.
    /// Retransmit cadence (exponential backoff from `ACK_TIMEOUT`) is
    /// orthogonal; it only controls *how often* we poke a silent peer within
    /// the wall-clock budget. The retransmit backoff is reset whenever
    /// `last_progress_at` advances (real forward progress), never by
    /// dispatching a fresh fragment — Selective Repeat keeps the window moving
    /// while the head is stalled, so "we sent something new" implies nothing
    /// about the dead peer.
    pub async fn run_send_loop<F, Fut, S>(
        &self,
        mut send_fn: F,
        is_tearing_down: S,
    ) -> Result<(), LinkDead>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
        S: Fn() -> bool,
    {
        let mut timeout = ACK_TIMEOUT;
        let mut tracked_progress_at: Option<tokio::time::Instant> = None;

        loop {
            let action = self.next_send_action().await;

            match action {
                SendAction::Dead => return Err(LinkDead),
                SendAction::Wait => {
                    let (head_seq, ack_deadline, last_progress_at) = {
                        let state = self.state.lock().await;
                        (
                            state.in_flight.front().map(|f| f.seq),
                            state.ack_deadline,
                            state.last_progress_at,
                        )
                    };

                    if head_seq.is_some() {
                        // Real forward progress since we last observed resets
                        // the retransmit backoff back to the aggressive base.
                        if tracked_progress_at.is_some_and(|prev| prev != last_progress_at) {
                            timeout = ACK_TIMEOUT;
                        }
                        tracked_progress_at = Some(last_progress_at);

                        let now = tokio::time::Instant::now();
                        let dead_at = last_progress_at + LINK_DEAD_DEADLINE;
                        if now >= dead_at {
                            warn!(
                                elapsed_ms = (now - last_progress_at).as_millis(),
                                "no forward progress within LINK_DEAD_DEADLINE, declaring link dead"
                            );
                            self.state.lock().await.link_dead = true;
                            return Err(LinkDead);
                        }

                        let retransmit_at = now + timeout;
                        let sleep_until = match ack_deadline {
                            Some(dl) => dl.min(retransmit_at).min(dead_at),
                            None => retransmit_at.min(dead_at),
                        };
                        let sleep_dur = sleep_until.saturating_duration_since(now);

                        match tokio::time::timeout(sleep_dur, self.wake.notified()).await {
                            Ok(()) => continue,
                            Err(_) => {
                                // ACK delay expired, not retransmit timeout yet.
                                if tokio::time::Instant::now() < retransmit_at {
                                    continue;
                                }

                                let resend = {
                                    let mut state = self.state.lock().await;
                                    let mut msg =
                                        state.in_flight.front().map(|f| f.wire_msg.clone());
                                    if let Some(ref mut m) = msg
                                        && let Some(ack_seq) = state.take_ack()
                                        && m.len() >= HEADER_SIZE
                                    {
                                        set_ack(m, ack_seq);
                                    }
                                    msg
                                };

                                if let Some(msg) = resend {
                                    self.retransmit_counter.fetch_add(1, Ordering::Relaxed);
                                    debug!(
                                        timeout_ms = timeout.as_millis(),
                                        "ACK timeout, retransmitting oldest fragment"
                                    );
                                    if let Err(e) = send_fn(msg).await {
                                        if is_tearing_down() {
                                            debug!(
                                                err = %e,
                                                "BLE retransmit failed during teardown"
                                            );
                                        } else {
                                            warn!(err = %e, "BLE retransmit failed");
                                        }
                                    }
                                }

                                timeout = (timeout * 2).min(ACK_TIMEOUT_MAX);
                                continue;
                            }
                        }
                    } else {
                        if let Some(dl) = ack_deadline {
                            let sleep_dur =
                                dl.saturating_duration_since(tokio::time::Instant::now());
                            let _ = tokio::time::timeout(sleep_dur, self.wake.notified()).await;
                        } else {
                            self.wake.notified().await;
                        }
                        timeout = ACK_TIMEOUT;
                        tracked_progress_at = None;
                        continue;
                    }
                }
                SendAction::Send(msg) => {
                    if let Err(e) = send_fn(msg).await {
                        if is_tearing_down() {
                            debug!(err = %e, "BLE send failed during teardown");
                        } else {
                            warn!(err = %e, "BLE send failed");
                        }
                    }

                    tokio::time::sleep(INTER_FRAME_GAP).await;
                }
            }
        }
    }

    async fn next_send_action(&self) -> SendAction {
        let mut state = self.state.lock().await;

        if state.link_dead {
            return SendAction::Dead;
        }

        if state.frag_queue.is_empty() && !state.send_queue.is_empty() {
            let datagram = state.send_queue.pop_front().unwrap();
            self.fragment_into(&mut state, datagram);
            self.send_waker.wake();
        }

        if state.can_send() {
            if state.frag_queue.is_empty() && !state.send_queue.is_empty() {
                let datagram = state.send_queue.pop_front().unwrap();
                self.fragment_into(&mut state, datagram);
                self.send_waker.wake();
            }

            if let Some(frag) = state.frag_queue.pop_front() {
                let seq = state.send_next;
                let mut hdr = make_header(seq, frag.first, frag.last);

                if let Some(ack_seq) = state.take_ack() {
                    set_ack(&mut hdr, ack_seq);
                }

                let mut msg = Vec::with_capacity(HEADER_SIZE + frag.payload.len() + 1);
                msg.extend_from_slice(&hdr);
                msg.extend_from_slice(&frag.payload);
                msg.push(FRAGMENT_CANARY);

                // Idle → busy transition: start a fresh liveness window.
                // The 6s deadline measures elapsed wall-clock since we began
                // expecting a response, so an idle channel that sat silent
                // for 10s must not trip the deadline on its very first send.
                if state.in_flight.is_empty() {
                    state.last_progress_at = tokio::time::Instant::now();
                }
                state.in_flight.push_back(InFlightFragment {
                    seq,
                    wire_msg: msg.clone(),
                });
                state.send_next = (state.send_next + 1) % SEQ_MODULUS;

                return SendAction::Send(msg);
            }
        }

        // Pure ACK (only after delay expires).
        if state.ack_pending.is_some() {
            let deadline_elapsed = state
                .ack_deadline
                .is_none_or(|dl| tokio::time::Instant::now() >= dl);
            if deadline_elapsed {
                let ack_seq = state.take_ack().unwrap();
                let mut hdr = [0u8; HEADER_SIZE];
                set_ack(&mut hdr, ack_seq);
                return SendAction::Send(hdr.to_vec());
            }
        }

        SendAction::Wait
    }

    fn fragment_into(&self, state: &mut ChannelState, datagram: Vec<u8>) {
        let max_payload = self.chunk_size.load(Ordering::Relaxed) - HEADER_SIZE - 1;

        if datagram.len() <= max_payload {
            state.frag_queue.push_back(FragmentEntry {
                payload: datagram,
                first: true,
                last: true,
            });
            return;
        }

        let chunks: Vec<&[u8]> = datagram.chunks(max_payload).collect();
        let last_idx = chunks.len() - 1;

        for (i, chunk) in chunks.into_iter().enumerate() {
            state.frag_queue.push_back(FragmentEntry {
                payload: chunk.to_vec(),
                first: i == 0,
                last: i == last_idx,
            });
        }
    }
}

enum SendAction {
    Send(Vec<u8>),
    Wait,
    Dead,
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    fn make_channel() -> (ReliableChannel, mpsc::Receiver<Vec<u8>>) {
        ReliableChannel::new(
            512,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn hdr(seq: u8, first: bool, last: bool) -> [u8; 2] {
        let b0 = (seq & SEQ_MASK)
            | (if first { FLAG_FIRST } else { 0 })
            | (if last { FLAG_LAST } else { 0 });
        [b0, 0]
    }

    fn fragment(seq: u8, first: bool, last: bool, payload: &[u8]) -> Vec<u8> {
        let mut v = hdr(seq, first, last).to_vec();
        v.extend_from_slice(payload);
        v.push(FRAGMENT_CANARY);
        v
    }
    #[test]
    fn test_seq_dist_basic() {
        assert_eq!(seq_dist(0, 0), 0);
        assert_eq!(seq_dist(0, 1), 1);
        assert_eq!(seq_dist(0, 15), 15);
    }

    #[test]
    fn test_seq_dist_wrap() {
        // 14 -> 15 -> 0 -> 1 -> 2  =  4 steps
        assert_eq!(seq_dist(14, 2), 4);
        // 15 -> 0  =  1 step
        assert_eq!(seq_dist(15, 0), 1);
    }
    #[tokio::test]
    async fn test_single_fragment_delivered() {
        let (ch, mut rx) = make_channel();
        ch.receive_fragment(&fragment(0, true, true, b"hello"))
            .await;
        let got = rx.try_recv().expect("should have delivered");
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn test_multi_fragment_delivered() {
        let (ch, mut rx) = make_channel();
        ch.receive_fragment(&fragment(0, true, false, b"hel")).await;
        assert!(rx.try_recv().is_err(), "not complete yet");
        ch.receive_fragment(&fragment(1, false, false, b"lo")).await;
        assert!(rx.try_recv().is_err(), "still not complete");
        ch.receive_fragment(&fragment(2, false, true, b"!")).await;
        let got = rx.try_recv().expect("should be complete");
        assert_eq!(got, b"hello!");
    }

    #[tokio::test]
    async fn test_out_of_order_delivery() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&fragment(1, false, false, b" world"))
            .await;
        assert!(rx.try_recv().is_err());

        ch.receive_fragment(&fragment(0, true, false, b"hello"))
            .await;
        assert!(rx.try_recv().is_err(), "LAST not received yet");

        ch.receive_fragment(&fragment(2, false, true, b"!")).await;
        let got = rx.try_recv().expect("should be complete");
        assert_eq!(got, b"hello world!");
    }

    #[tokio::test]
    async fn test_duplicate_fragment_ignored() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&fragment(0, true, true, b"ping")).await;
        let got = rx.try_recv().expect("should be delivered");
        assert_eq!(got, b"ping");

        ch.receive_fragment(&fragment(0, true, true, b"ping")).await;
        assert!(rx.try_recv().is_err(), "duplicate should not re-deliver");
    }

    #[tokio::test]
    async fn test_reassembly_overflow_dropped() {
        let (ch, mut rx) = make_channel();

        let big = vec![0u8; MAX_DATAGRAM_SIZE + 1];
        let msg = fragment(0, true, true, &big);
        ch.receive_fragment(&msg).await;

        assert!(
            rx.try_recv().is_err(),
            "oversized datagram must not be delivered"
        );
    }

    #[tokio::test]
    async fn test_reassembly_overflow_mid_stream() {
        let (ch, mut rx) = make_channel();

        let big = vec![0u8; MAX_DATAGRAM_SIZE + 1];
        ch.receive_fragment(&fragment(0, true, false, &big)).await;

        ch.receive_fragment(&fragment(1, false, true, b"end")).await;
        assert!(
            rx.try_recv().is_err(),
            "datagram that overflowed must be dropped"
        );
    }

    #[test]
    fn test_make_header_seq_only() {
        let h = make_header(5, false, false);
        assert_eq!(h[0] & SEQ_MASK, 5);
        assert_eq!(h[0] & FLAG_FIRST, 0);
        assert_eq!(h[0] & FLAG_LAST, 0);
        assert_eq!(h[1], 0);
    }

    #[test]
    fn test_make_header_first_last() {
        let h = make_header(3, true, true);
        assert_eq!(h[0] & SEQ_MASK, 3);
        assert_ne!(h[0] & FLAG_FIRST, 0);
        assert_ne!(h[0] & FLAG_LAST, 0);
    }

    #[test]
    fn test_make_header_seq_wraps_at_modulus() {
        // Only low 4 bits should be used.
        let h = make_header(17, false, false);
        assert_eq!(h[0] & SEQ_MASK, 1); // 17 & 0x0F = 1
    }

    #[test]
    fn test_set_ack() {
        let mut h = [0u8; 2];
        set_ack(&mut h, 7);
        assert_ne!(h[1] & FLAG_ACK, 0, "ACK flag must be set");
        assert_eq!(h[1] & ACK_SEQ_MASK, 7);
    }

    #[test]
    fn test_seq_dist_full_circle() {
        // Distance from N to N is always 0.
        for i in 0..SEQ_MODULUS {
            assert_eq!(seq_dist(i, i), 0);
        }
    }

    #[test]
    fn test_seq_dist_one_step() {
        for i in 0..SEQ_MODULUS {
            assert_eq!(seq_dist(i, (i + 1) % SEQ_MODULUS), 1);
        }
    }
    fn fragment_with_ack(seq: u8, first: bool, last: bool, payload: &[u8], ack_seq: u8) -> Vec<u8> {
        let mut v = hdr(seq, first, last).to_vec();
        set_ack(&mut v, ack_seq);
        v.extend_from_slice(payload);
        v.push(FRAGMENT_CANARY);
        v
    }

    fn pure_ack(ack_seq: u8) -> Vec<u8> {
        let mut h = [0u8; HEADER_SIZE];
        set_ack(&mut h, ack_seq);
        h.to_vec()
    }

    #[tokio::test]
    async fn test_ack_slides_send_window() {
        let (ch, _rx) = make_channel();

        ch.enqueue_datagram(b"test".to_vec()).await;

        {
            let state = ch.state.lock().await;
            assert!(!state.send_queue.is_empty());
        }

        let action = ch.next_send_action().await;
        assert!(matches!(action, SendAction::Send(_)));

        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 1);
            assert_eq!(state.send_base, 0);
            assert_eq!(state.send_next, 1);
        }

        ch.receive_fragment(&pure_ack(0)).await;

        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 0, "ACK should clear in-flight");
            assert_eq!(
                state.send_base, 1,
                "send_base should advance past ACK'd seq"
            );
        }
    }

    #[tokio::test]
    async fn test_ack_beyond_window_ignored() {
        let (ch, _rx) = make_channel();

        ch.receive_fragment(&pure_ack(5)).await;

        let state = ch.state.lock().await;
        assert_eq!(state.send_base, 0, "bogus ACK should not move send_base");
    }

    #[tokio::test]
    async fn test_multiple_fragments_acked_cumulatively() {
        let (ch, _rx) = make_channel();

        ch.enqueue_datagram(b"aaa".to_vec()).await;
        ch.enqueue_datagram(b"bbb".to_vec()).await;
        ch.enqueue_datagram(b"ccc".to_vec()).await;

        for _ in 0..3 {
            let action = ch.next_send_action().await;
            assert!(matches!(action, SendAction::Send(_)));
        }

        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 3);
        }

        ch.receive_fragment(&pure_ack(2)).await;

        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 0);
            assert_eq!(state.send_base, 3);
        }
    }
    #[tokio::test]
    async fn test_small_datagram_single_fragment() {
        let (ch, _rx) = make_channel();
        ch.enqueue_datagram(b"small".to_vec()).await;

        let action = ch.next_send_action().await;
        match action {
            SendAction::Send(msg) => {
                assert!(msg.len() > HEADER_SIZE);
                let b0 = msg[0];
                assert_ne!(b0 & FLAG_FIRST, 0, "single fragment must have FIRST");
                assert_ne!(b0 & FLAG_LAST, 0, "single fragment must have LAST");
                assert_eq!(msg.last().copied(), Some(FRAGMENT_CANARY));
                assert_eq!(&msg[HEADER_SIZE..msg.len() - 1], b"small");
            }
            _ => panic!("expected Send action"),
        }
    }

    #[tokio::test]
    async fn test_large_datagram_fragmented() {
        let retransmits = Arc::new(AtomicU64::new(0));
        // chunk_size=10 -> max_payload=7 (10 - 2 header - 1 canary) -> 20 bytes = 3 fragments.
        let (ch, _rx) = ReliableChannel::new(10, retransmits, Arc::new(AtomicU64::new(0)));

        let data = vec![0xAA; 20];
        ch.enqueue_datagram(data).await;

        let a1 = ch.next_send_action().await;
        match a1 {
            SendAction::Send(msg) => {
                assert_ne!(msg[0] & FLAG_FIRST, 0);
                assert_eq!(msg[0] & FLAG_LAST, 0);
                assert_eq!(msg.len() - HEADER_SIZE, 8); // 7 payload + 1 canary
            }
            _ => panic!("expected Send"),
        }

        let a2 = ch.next_send_action().await;
        match a2 {
            SendAction::Send(msg) => {
                assert_eq!(msg[0] & FLAG_FIRST, 0);
                assert_eq!(msg[0] & FLAG_LAST, 0);
                assert_eq!(msg.len() - HEADER_SIZE, 8); // 7 payload + 1 canary
            }
            _ => panic!("expected Send"),
        }

        let a3 = ch.next_send_action().await;
        match a3 {
            SendAction::Send(msg) => {
                assert_eq!(msg[0] & FLAG_FIRST, 0);
                assert_ne!(msg[0] & FLAG_LAST, 0);
                assert_eq!(msg.len() - HEADER_SIZE, 7); // 6 payload + 1 canary
            }
            _ => panic!("expected Send"),
        }
    }
    #[tokio::test]
    async fn test_window_blocks_when_full() {
        let (ch, _rx) = make_channel();

        for i in 0..WINDOW_SIZE + 2 {
            ch.enqueue_datagram(vec![i; 1]).await;
        }

        for _ in 0..WINDOW_SIZE {
            let action = ch.next_send_action().await;
            assert!(matches!(action, SendAction::Send(_)));
        }

        let action = ch.next_send_action().await;
        assert!(
            matches!(action, SendAction::Wait),
            "should block at full window"
        );
    }

    #[tokio::test]
    async fn test_window_unblocks_after_ack() {
        let (ch, _rx) = make_channel();

        for i in 0..WINDOW_SIZE + 1 {
            ch.enqueue_datagram(vec![i; 1]).await;
        }

        for _ in 0..WINDOW_SIZE {
            ch.next_send_action().await;
        }

        ch.receive_fragment(&pure_ack(0)).await;

        let action = ch.next_send_action().await;
        assert!(
            matches!(action, SendAction::Send(_)),
            "should unblock after ACK"
        );
    }
    #[tokio::test]
    async fn test_enqueue_backpressure() {
        let (ch, _rx) = make_channel();

        for _ in 0..SEND_QUEUE_CAPACITY {
            assert!(ch.enqueue_datagram(vec![1]).await);
        }

        assert!(
            !ch.enqueue_datagram(vec![2]).await,
            "should reject when full"
        );
    }

    #[tokio::test]
    async fn test_try_enqueue_datagram() {
        let (ch, _rx) = make_channel();

        let result = ch.try_enqueue_datagram(vec![1]);
        assert_eq!(result, Some(true));

        for _ in 1..SEND_QUEUE_CAPACITY {
            ch.try_enqueue_datagram(vec![1]);
        }

        let result = ch.try_enqueue_datagram(vec![2]);
        assert_eq!(result, Some(false), "should report full");
    }
    #[tokio::test]
    async fn test_channel_state_in_recv_window() {
        let (ch, _rx) = make_channel();
        let state = ch.state.lock().await;

        assert!(state.in_recv_window(0));
        assert!(state.in_recv_window(WINDOW_SIZE - 1));
        assert!(!state.in_recv_window(WINDOW_SIZE));
        assert!(!state.in_recv_window(WINDOW_SIZE + 1));
    }

    #[tokio::test]
    async fn test_channel_state_in_recv_window_wrapped() {
        let (ch, _rx) = make_channel();
        let mut state = ch.state.lock().await;
        state.recv_next = 14; // window covers [14, 14+6) = [14, 15, 0, 1, 2, 3]

        assert!(state.in_recv_window(14));
        assert!(state.in_recv_window(15));
        assert!(state.in_recv_window(0)); // wrapped
        assert!(state.in_recv_window(3)); // WINDOW_SIZE - 1 from 14
        assert!(!state.in_recv_window(4)); // outside window
        assert!(!state.in_recv_window(13)); // behind window
    }

    #[tokio::test]
    async fn test_in_flight_count() {
        let (ch, _rx) = make_channel();
        let mut state = ch.state.lock().await;

        assert_eq!(state.in_flight_count(), 0);
        state.send_next = 3;
        assert_eq!(state.in_flight_count(), 3);
        state.send_base = 3;
        assert_eq!(state.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_in_flight_count_wrapped() {
        let (ch, _rx) = make_channel();
        let mut state = ch.state.lock().await;
        state.send_base = 14;
        state.send_next = 2; // 14->15->0->1->2 = 4 in flight
        assert_eq!(state.in_flight_count(), 4);
    }
    #[tokio::test]
    async fn test_ack_piggybacked_on_data() {
        let (ch, _rx) = make_channel();

        ch.receive_fragment(&fragment(0, true, true, b"hello"))
            .await;

        ch.enqueue_datagram(b"reply".to_vec()).await;

        let action = ch.next_send_action().await;
        match action {
            SendAction::Send(msg) => {
                assert!(msg.len() >= HEADER_SIZE);
                assert_ne!(msg[1] & FLAG_ACK, 0, "ACK should be piggybacked");
                assert_eq!(msg[1] & ACK_SEQ_MASK, 0, "should ACK seq 0");
            }
            _ => panic!("expected Send"),
        }

        let state = ch.state.lock().await;
        assert!(state.ack_pending.is_none(), "ack_pending should be cleared");
    }
    #[tokio::test]
    async fn test_short_message_ignored() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&[]).await;

        ch.receive_fragment(&[0x00]).await;

        assert!(rx.try_recv().is_err(), "short messages should be ignored");
    }
    #[tokio::test]
    async fn test_sequential_datagrams() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&fragment(0, true, true, b"first"))
            .await;
        assert_eq!(rx.try_recv().unwrap(), b"first");

        ch.receive_fragment(&fragment(1, true, true, b"second"))
            .await;
        assert_eq!(rx.try_recv().unwrap(), b"second");

        ch.receive_fragment(&fragment(2, true, true, b"third"))
            .await;
        assert_eq!(rx.try_recv().unwrap(), b"third");
    }
    #[tokio::test]
    async fn test_receive_sequence_wraparound() {
        let (ch, mut rx) = make_channel();

        for i in 0..14u8 {
            ch.receive_fragment(&fragment(i, true, true, &[i])).await;
            rx.try_recv().unwrap();
        }

        ch.receive_fragment(&fragment(14, true, true, &[14])).await;
        assert_eq!(rx.try_recv().unwrap(), &[14]);

        ch.receive_fragment(&fragment(15, true, true, &[15])).await;
        assert_eq!(rx.try_recv().unwrap(), &[15]);

        ch.receive_fragment(&fragment(0, true, true, &[0])).await;
        assert_eq!(rx.try_recv().unwrap(), &[0]);
    }
    #[tokio::test]
    async fn test_overflow_recovery_next_datagram_ok() {
        let (ch, mut rx) = make_channel();

        let big = vec![0u8; MAX_DATAGRAM_SIZE + 1];
        ch.receive_fragment(&fragment(0, true, true, &big)).await;
        assert!(rx.try_recv().is_err());

        ch.receive_fragment(&fragment(1, true, true, b"ok")).await;
        assert_eq!(rx.try_recv().unwrap(), b"ok");
    }
    #[tokio::test]
    async fn test_send_loop_delivers_fragments() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);

        let sent = Arc::new(tokio::sync::Mutex::new(Vec::<Vec<u8>>::new()));

        ch.enqueue_datagram(b"hello".to_vec()).await;

        let ch2 = ch.clone();
        let sent2 = sent.clone();
        let handle = tokio::spawn(async move {
            ch2.run_send_loop(
                |data| {
                    let sent = sent2.clone();
                    async move {
                        sent.lock().await.push(data);
                        Ok(())
                    }
                },
                || false,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        ch.receive_fragment(&pure_ack(0)).await;

        ch.state.lock().await.link_dead = true;
        ch.wake.notify_one();
        let _ = handle.await;

        let messages = sent.lock().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].last().copied(), Some(FRAGMENT_CANARY));
        assert_eq!(&messages[0][HEADER_SIZE..messages[0].len() - 1], b"hello");
    }
    #[tokio::test(start_paused = true)]
    async fn test_send_loop_deadline_declares_link_dead() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);

        ch.enqueue_datagram(b"data".to_vec()).await;

        let ch2 = ch.clone();
        let handle =
            tokio::spawn(
                async move { ch2.run_send_loop(|_data| async { Ok(()) }, || false).await },
            );

        // Advance well past LINK_DEAD_DEADLINE (6s) with no ACKs. 30 × 500ms
        // is 15s — more than enough for the progress deadline to fire.
        for _ in 0..30 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            if handle.is_finished() {
                break;
            }
        }

        let result = handle.await.unwrap();
        assert_eq!(result, Err(LinkDead));
    }

    /// Regression: dispatching a fresh fragment must not reset the retransmit
    /// backoff on the stuck head of `in_flight`. In the original buggy code,
    /// every `SendAction::Send` cleared `retries` *and* `timeout`, so as long
    /// as the app kept enqueueing small datagrams the retransmit schedule
    /// restarted at `ACK_TIMEOUT` each time — preventing the link-dead path
    /// from ever firing against a silently-dead peer. The fix ties both
    /// retransmit cadence and the liveness deadline to real ACK progress.
    ///
    /// Strategy: force one retransmit to happen so the backoff doubles to
    /// 600ms, then enqueue a new datagram mid-backoff. In the fixed code the
    /// next retransmit stays on the 600ms schedule; in the buggy code it
    /// reverts to 300ms. We catch the divergence by checking the retransmit
    /// counter at a time window between the two.
    #[tokio::test(start_paused = true)]
    async fn test_new_send_does_not_reset_retry_backoff() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);
        let retransmit_counter = ch.retransmit_counter.clone();

        ch.enqueue_datagram(b"stuck".to_vec()).await;

        let ch2 = ch.clone();
        let handle =
            tokio::spawn(
                async move { ch2.run_send_loop(|_data| async { Ok(()) }, || false).await },
            );

        // Helper: advance virtual time in small chunks with yields between,
        // so each timer firing gets polled by the runtime. Coarse advances
        // skip polls and leave the send loop parked.
        async fn tick(ms: u64) {
            for _ in 0..(ms / 10).max(1) {
                tokio::time::advance(Duration::from_millis(10)).await;
                tokio::task::yield_now().await;
            }
        }

        // Drive the first retransmit (fires at ~t=300ms after ACK_TIMEOUT).
        tick(400).await;
        assert_eq!(
            retransmit_counter.load(Ordering::Relaxed),
            1,
            "first retransmit should have fired after ACK_TIMEOUT"
        );

        // Backoff is now 600ms; the next retransmit is armed for ~t=1000ms.
        // Inject a fresh datagram mid-backoff. In the buggy code this reset
        // both `retries` and `timeout`, so the next retransmit would shift to
        // ~t=700ms (300ms from now). In the fixed code the schedule is
        // unchanged.
        ch.enqueue_datagram(b"fresh".to_vec()).await;

        // Advance to ~t=800ms — past the buggy reset schedule (~700ms),
        // still before the correct schedule (~1000ms).
        tick(400).await;

        assert_eq!(
            retransmit_counter.load(Ordering::Relaxed),
            1,
            "new Send must not reset the retransmit backoff on the stuck head"
        );

        // Clean shutdown.
        ch.state.lock().await.link_dead = true;
        ch.wake.notify_one();
        let _ = handle.await;
    }

    /// `mark_dead()` is the fast-path signal from the pipe owner when the
    /// BLE link is gone (`CentralDisconnected`, registry-initiated teardown).
    /// It must wake a parked send loop within a tokio poll cycle and cause
    /// it to return `LinkDead` without waiting for the 6s deadline.
    #[tokio::test(start_paused = true)]
    async fn test_mark_dead_wakes_parked_send_loop() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);

        // Park the send loop on the ACK timer.
        ch.enqueue_datagram(b"stuck".to_vec()).await;

        let ch2 = ch.clone();
        let handle =
            tokio::spawn(
                async move { ch2.run_send_loop(|_data| async { Ok(()) }, || false).await },
            );

        // Let the loop dispatch frag0, clear INTER_FRAME_GAP, re-enter the
        // Wait arm and park on `wake.notified()`.
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_finished(),
            "loop should be parked waiting for ACK"
        );

        // Fire the disconnect signal.
        ch.mark_dead().await;

        // A small advance gives the runtime a turn after the notify so the
        // send loop can be polled and transition Dead → return.
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if handle.is_finished() {
                break;
            }
        }
        assert!(handle.is_finished(), "mark_dead must wake the parked loop");

        let result = handle.await.unwrap();
        assert_eq!(result, Err(LinkDead));
    }

    /// The liveness deadline is a hard wall-clock budget. With no ACKs at all,
    /// the send loop must declare `LinkDead` close to `LINK_DEAD_DEADLINE`
    /// (6s) — not wait for dozens of retransmits to exhaust.
    #[tokio::test(start_paused = true)]
    async fn test_link_dead_fires_within_deadline() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);

        ch.enqueue_datagram(b"stuck".to_vec()).await;

        let start = tokio::time::Instant::now();
        let ch2 = ch.clone();
        let handle =
            tokio::spawn(
                async move { ch2.run_send_loop(|_data| async { Ok(()) }, || false).await },
            );

        // Advance in 250ms steps — fine enough that every retransmit timer
        // and the deadline fire promptly.
        for _ in 0..60 {
            tokio::time::advance(Duration::from_millis(250)).await;
            tokio::task::yield_now().await;
            if handle.is_finished() {
                break;
            }
        }

        assert!(handle.is_finished(), "send loop should have exited");
        let result = handle.await.unwrap();
        assert_eq!(result, Err(LinkDead));

        let elapsed = tokio::time::Instant::now() - start;
        // Deadline is 6s; allow a small margin for the final sleep quantum
        // and the post-exit advance steps.
        assert!(
            elapsed >= LINK_DEAD_DEADLINE,
            "declared dead too early: elapsed={elapsed:?}"
        );
        assert!(
            elapsed < LINK_DEAD_DEADLINE + Duration::from_millis(750),
            "declared dead too late: elapsed={elapsed:?}"
        );
    }

    /// An ACK that advances the head of the send window is the only signal
    /// of genuine progress, and it must (a) reset the retransmit backoff to
    /// `ACK_TIMEOUT` and (b) slide the liveness deadline forward. We
    /// retransmit the same head fragment a few times inside the 6s window,
    /// then deliver an ACK that drains in-flight, and confirm the loop gets a
    /// fresh 6s budget and does not declare LinkDead.
    #[tokio::test(start_paused = true)]
    async fn test_deadline_resets_when_head_advances() {
        let (ch, _rx) = make_channel();
        let ch = Arc::new(ch);

        ch.enqueue_datagram(b"one".to_vec()).await;

        let ch2 = ch.clone();
        let handle =
            tokio::spawn(
                async move { ch2.run_send_loop(|_data| async { Ok(()) }, || false).await },
            );

        // Burn ~4s of the deadline with retransmits — under 6s, still alive.
        for _ in 0..8 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert!(!handle.is_finished(), "loop should still be alive pre-ACK");

        // ACK seq 0: cumulative ACK advances send_base, drains in_flight,
        // and (critically) bumps `last_progress_at` to now.
        ch.receive_fragment(&pure_ack(0)).await;
        ch.wake.notify_waiters();
        tokio::task::yield_now().await;

        // Enqueue a follow-up and advance another ~4s. If the deadline did
        // not reset, total elapsed (~8s) would trip LinkDead; it must not.
        ch.enqueue_datagram(b"two".to_vec()).await;
        for _ in 0..8 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_finished(),
            "after head advanced, deadline should have reset and loop should be alive"
        );

        ch.state.lock().await.link_dead = true;
        ch.wake.notify_one();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_link_dead_returns_dead() {
        let (ch, _rx) = make_channel();
        ch.state.lock().await.link_dead = true;

        let action = ch.next_send_action().await;
        assert!(matches!(action, SendAction::Dead));
    }
    #[tokio::test]
    async fn test_piggybacked_ack_in_data_fragment() {
        let (ch, mut rx) = make_channel();

        ch.enqueue_datagram(b"out".to_vec()).await;
        ch.next_send_action().await;
        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 1);
        }

        let msg = fragment_with_ack(0, true, true, b"incoming", 0);
        ch.receive_fragment(&msg).await;

        assert_eq!(rx.try_recv().unwrap(), b"incoming");
        {
            let state = ch.state.lock().await;
            assert_eq!(state.in_flight_count(), 0);
            assert_eq!(state.send_base, 1);
        }
    }
    #[tokio::test]
    async fn test_behind_window_duplicate_triggers_ack() {
        let (ch, _rx) = make_channel();

        for i in 0..3u8 {
            ch.receive_fragment(&fragment(i, true, true, &[i])).await;
        }

        // seq 1 is behind window (recv_next=3), should re-ACK.
        ch.receive_fragment(&fragment(1, true, true, b"dup")).await;

        let state = ch.state.lock().await;
        assert_eq!(state.ack_pending, Some(2));
    }
    #[test]
    fn test_window_size_invariant() {
        // WINDOW_SIZE must be < SEQ_MODULUS / 2 for correct modular arithmetic.
        const { assert!(WINDOW_SIZE < SEQ_MODULUS / 2) };
    }

    async fn pump_one(sender: &ReliableChannel, receiver: &ReliableChannel) -> Option<Vec<u8>> {
        match sender.next_send_action().await {
            SendAction::Send(msg) => {
                receiver.receive_fragment(&msg).await;
                Some(msg)
            }
            _ => None,
        }
    }

    async fn pump_all(sender: &ReliableChannel, receiver: &ReliableChannel) -> usize {
        let mut count = 0;
        while let SendAction::Send(msg) = sender.next_send_action().await {
            receiver.receive_fragment(&msg).await;
            count += 1;
        }
        count
    }
    #[tokio::test]
    async fn test_bidirectional_single_datagram() {
        let (a, mut a_rx) = make_channel();
        let (b, mut b_rx) = make_channel();

        a.enqueue_datagram(b"from-a".to_vec()).await;
        pump_one(&a, &b).await;
        assert_eq!(b_rx.try_recv().unwrap(), b"from-a");

        b.enqueue_datagram(b"from-b".to_vec()).await;
        pump_one(&b, &a).await;
        assert_eq!(a_rx.try_recv().unwrap(), b"from-b");
    }

    #[tokio::test]
    async fn test_bidirectional_interleaved() {
        let (a, mut a_rx) = make_channel();
        let (b, mut b_rx) = make_channel();

        a.enqueue_datagram(b"a1".to_vec()).await;
        b.enqueue_datagram(b"b1".to_vec()).await;

        pump_one(&a, &b).await;
        assert_eq!(b_rx.try_recv().unwrap(), b"a1");

        // B's reply should piggyback the ACK for A's seq 0.
        let msg = pump_one(&b, &a).await.unwrap();
        assert_eq!(a_rx.try_recv().unwrap(), b"b1");
        assert_ne!(msg[1] & FLAG_ACK, 0, "B should piggyback ACK on its data");
        let state_a = a.state.lock().await;
        assert_eq!(state_a.in_flight_count(), 0, "A's fragment should be ACK'd");
    }

    #[tokio::test]
    async fn test_bidirectional_multiple_datagrams() {
        let (a, mut a_rx) = make_channel();
        let (b, mut b_rx) = make_channel();

        for i in 0..3u8 {
            a.enqueue_datagram(vec![i; 10]).await;
        }
        for i in 10..13u8 {
            b.enqueue_datagram(vec![i; 10]).await;
        }

        let sent_a = pump_all(&a, &b).await;
        assert_eq!(sent_a, 3);
        for i in 0..3u8 {
            assert_eq!(b_rx.try_recv().unwrap(), vec![i; 10]);
        }

        let sent_b = pump_all(&b, &a).await;
        assert_eq!(sent_b, 3);
        for i in 10..13u8 {
            assert_eq!(a_rx.try_recv().unwrap(), vec![i; 10]);
        }

        let state_a = a.state.lock().await;
        assert_eq!(state_a.in_flight_count(), 0);
    }
    #[tokio::test]
    async fn test_dropped_fragment_requires_retransmit() {
        let retransmits = Arc::new(AtomicU64::new(0));
        let (a, _a_rx) =
            ReliableChannel::new(512, retransmits.clone(), Arc::new(AtomicU64::new(0)));
        let (_b, mut b_rx) = make_channel();

        a.enqueue_datagram(b"will-drop".to_vec()).await;

        // Send but don't deliver (simulating a drop).
        let action = a.next_send_action().await;
        assert!(matches!(action, SendAction::Send(_)));
        {
            let state = a.state.lock().await;
            assert_eq!(state.in_flight_count(), 1);
        }

        assert!(b_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_dropped_ack_sender_retransmits() {
        let retransmits_a = Arc::new(AtomicU64::new(0));
        let (a, _a_rx) =
            ReliableChannel::new(512, retransmits_a.clone(), Arc::new(AtomicU64::new(0)));
        let (b, mut b_rx) = make_channel();

        a.enqueue_datagram(b"data".to_vec()).await;

        pump_one(&a, &b).await;
        assert_eq!(b_rx.try_recv().unwrap(), b"data");

        // Don't pump B -> A (simulating ACK drop).
        {
            let state = a.state.lock().await;
            assert_eq!(state.in_flight_count(), 1);
        }

        {
            let state = a.state.lock().await;
            assert!(
                !state.in_flight.is_empty(),
                "fragment should remain in-flight for retransmission"
            );
            assert_eq!(state.send_base, 0);
            assert_eq!(state.send_next, 1);
        }
    }

    #[tokio::test]
    async fn test_drop_first_of_two_fragments() {
        let retransmits = Arc::new(AtomicU64::new(0));
        // chunk_size=10 -> 7 payload -> 20 bytes = 3 fragments.
        let (a, _a_rx) = ReliableChannel::new(10, retransmits, Arc::new(AtomicU64::new(0)));
        let (b, mut b_rx) = make_channel();

        a.enqueue_datagram(vec![0xAA; 20]).await;

        // Drop fragment 0, deliver 1 and 2.
        let frag0 = match a.next_send_action().await {
            SendAction::Send(msg) => msg,
            _ => panic!("expected send"),
        };
        // Don't deliver frag0.
        let _frag1 = match a.next_send_action().await {
            SendAction::Send(msg) => {
                b.receive_fragment(&msg).await;
                msg
            }
            _ => panic!("expected send"),
        };

        match a.next_send_action().await {
            SendAction::Send(msg) => {
                b.receive_fragment(&msg).await;
            }
            _ => panic!("expected send"),
        };

        assert!(b_rx.try_recv().is_err(), "incomplete without frag 0");

        // Retransmit frag 0.
        b.receive_fragment(&frag0).await;

        let got = b_rx.try_recv().expect("should deliver after retransmit");
        assert_eq!(got, vec![0xAA; 20]);
    }
    #[tokio::test]
    async fn test_reordered_fragments_reassembled_correctly() {
        let retransmits = Arc::new(AtomicU64::new(0));
        // chunk_size=10 -> max_payload=7 -> 16 bytes = 3 fragments (7+7+2).
        let (a, _a_rx) = ReliableChannel::new(10, retransmits, Arc::new(AtomicU64::new(0)));
        let (b2, mut b2_rx) = make_channel();

        a.enqueue_datagram(vec![0xBB; 16]).await;
        let frag0 = match a.next_send_action().await {
            SendAction::Send(msg) => msg,
            _ => panic!("expected send"),
        };
        let frag1 = match a.next_send_action().await {
            SendAction::Send(msg) => msg,
            _ => panic!("expected send"),
        };
        let frag2 = match a.next_send_action().await {
            SendAction::Send(msg) => msg,
            _ => panic!("expected send"),
        };

        // Deliver out of order: 2, 0, 1.
        b2.receive_fragment(&frag2).await;
        assert!(b2_rx.try_recv().is_err(), "not complete yet");

        b2.receive_fragment(&frag0).await;
        assert!(
            b2_rx.try_recv().is_err(),
            "not complete yet (frag1 missing)"
        );

        b2.receive_fragment(&frag1).await;
        let got = b2_rx.try_recv().expect("should deliver after reorder");
        assert_eq!(got, vec![0xBB; 16]);
    }

    #[tokio::test]
    async fn test_heavily_reordered_window() {
        let (ch, mut rx) = make_channel();

        // Deliver 5 datagrams in reverse order; all should drain in order
        // once seq 0 arrives.
        for seq in (0..5u8).rev() {
            ch.receive_fragment(&fragment(seq, true, true, &[seq]))
                .await;
        }

        for expected in 0..5u8 {
            let got = rx
                .try_recv()
                .unwrap_or_else(|_| panic!("should deliver seq {expected}"));
            assert_eq!(got, vec![expected]);
        }
    }
    async fn force_ack_deadline(ch: &ReliableChannel) {
        let mut state = ch.state.lock().await;
        if state.ack_pending.is_some() {
            state.ack_deadline = Some(tokio::time::Instant::now() - Duration::from_millis(1));
        }
    }

    async fn pump_one_force(
        sender: &ReliableChannel,
        receiver: &ReliableChannel,
    ) -> Option<Vec<u8>> {
        force_ack_deadline(sender).await;
        pump_one(sender, receiver).await
    }

    #[tokio::test]
    async fn test_full_sequence_cycle_bidirectional() {
        let (a, mut a_rx) = make_channel();
        let (b, mut b_rx) = make_channel();

        // 32 rounds = 2x the sequence space to exercise wraparound.
        for round in 0..32u8 {
            a.enqueue_datagram(vec![round]).await;
            pump_one(&a, &b).await;
            assert_eq!(b_rx.try_recv().unwrap(), vec![round]);

            b.enqueue_datagram(vec![round.wrapping_add(100)]).await;
            pump_one(&b, &a).await;
            assert_eq!(a_rx.try_recv().unwrap(), vec![round.wrapping_add(100)]);

            pump_one_force(&a, &b).await;
            pump_one_force(&b, &a).await;
        }

        let state_a = a.state.lock().await;
        let state_b = b.state.lock().await;
        assert_eq!(state_a.in_flight_count(), 0);
        assert_eq!(state_b.in_flight_count(), 0);
    }
    #[tokio::test]
    async fn test_multi_fragment_datagram_across_sequence_wrap() {
        let retransmits = Arc::new(AtomicU64::new(0));
        // chunk_size=10 -> 7 payload per fragment.
        let (a, _a_rx) = ReliableChannel::new(10, retransmits, Arc::new(AtomicU64::new(0)));
        let (b, mut b_rx) = make_channel();

        for i in 0..14u8 {
            a.enqueue_datagram(vec![i]).await;
            pump_one(&a, &b).await;
            b_rx.try_recv().unwrap();
            force_ack_deadline(&b).await;
            pump_one_force(&b, &a).await;
        }

        {
            let state = a.state.lock().await;
            assert_eq!(state.send_next, 14);
            assert_eq!(state.in_flight_count(), 0);
        }

        // 3 fragments spanning seq 14, 15, 0 (wraparound).
        a.enqueue_datagram(vec![0xCC; 20]).await;

        let sent = pump_all(&a, &b).await;
        assert_eq!(sent, 3);

        let got = b_rx
            .try_recv()
            .expect("should deliver multi-frag across wrap");
        assert_eq!(got, vec![0xCC; 20]);
    }
    #[tokio::test]
    async fn test_pure_ack_sent_when_no_outgoing_data() {
        let (a, _a_rx) = make_channel();
        let (b, _b_rx) = make_channel();

        a.enqueue_datagram(b"data".to_vec()).await;
        pump_one(&a, &b).await;

        {
            let mut state = b.state.lock().await;
            assert!(state.ack_pending.is_some());
            state.ack_deadline = Some(tokio::time::Instant::now() - Duration::from_millis(1));
        }

        let action = b.next_send_action().await;
        match action {
            SendAction::Send(msg) => {
                assert_eq!(msg.len(), HEADER_SIZE, "pure ACK should be header-only");
                assert_ne!(msg[1] & FLAG_ACK, 0, "must have ACK flag");
                assert_eq!(msg[1] & ACK_SEQ_MASK, 0, "should ACK seq 0");
            }
            _ => panic!("expected pure ACK Send"),
        }
    }
    #[tokio::test]
    async fn test_cumulative_ack_covers_gap_fill() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&fragment(1, true, true, b"b")).await;
        ch.receive_fragment(&fragment(0, true, true, b"a")).await;

        assert_eq!(rx.try_recv().unwrap(), b"a");
        assert_eq!(rx.try_recv().unwrap(), b"b");

        let state = ch.state.lock().await;
        assert_eq!(state.recv_next, 2);
        assert_eq!(state.ack_pending, Some(1));
    }
    #[tokio::test]
    async fn test_stale_ack_after_window_advance_ignored() {
        let (a, _rx) = make_channel();

        for i in 0..3u8 {
            a.enqueue_datagram(vec![i]).await;
        }
        for _ in 0..3 {
            a.next_send_action().await;
        }
        a.receive_fragment(&pure_ack(2)).await;
        {
            let state = a.state.lock().await;
            assert_eq!(state.send_base, 3);
            assert_eq!(state.in_flight_count(), 0);
        }

        a.receive_fragment(&pure_ack(0)).await;
        {
            let state = a.state.lock().await;
            assert_eq!(
                state.send_base, 3,
                "stale ACK should not move send_base backwards"
            );
        }
    }
    #[tokio::test]
    async fn test_empty_datagram_not_delivered() {
        let (ch, mut rx) = make_channel();

        ch.receive_fragment(&fragment(0, true, true, b"")).await;

        assert!(
            rx.try_recv().is_err(),
            "empty datagram should not be delivered"
        );

        let state = ch.state.lock().await;
        assert_eq!(state.recv_next, 1);
    }

    #[tokio::test]
    async fn send_loop_appends_canary_to_every_fragment() {
        let retransmit = Arc::new(AtomicU64::new(0));
        let truncation = Arc::new(AtomicU64::new(0));
        let (ch, _rx) = ReliableChannel::new(32, Arc::clone(&retransmit), Arc::clone(&truncation));
        let ch = Arc::new(ch);

        ch.enqueue_datagram(vec![0xAB; 100]).await;

        let captured = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let captured_for_cb = Arc::clone(&captured);
        let ch_for_task = Arc::clone(&ch);
        let handle = tokio::spawn(async move {
            ch_for_task
                .run_send_loop(
                    move |bytes| {
                        let captured = Arc::clone(&captured_for_cb);
                        async move {
                            captured.lock().unwrap().push(bytes);
                            Ok::<(), String>(())
                        }
                    },
                    || false,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();

        let frames = captured.lock().unwrap();
        assert!(
            !frames.is_empty(),
            "expected at least one fragment on the wire"
        );
        for (i, frame) in frames.iter().enumerate() {
            assert_eq!(
                frame.last().copied(),
                Some(FRAGMENT_CANARY),
                "fragment {i} did not end with FRAGMENT_CANARY: {frame:?}"
            );
        }
    }

    #[tokio::test]
    async fn receive_fragment_drops_and_counts_canary_mismatch() {
        let retransmit = Arc::new(AtomicU64::new(0));
        let truncation = Arc::new(AtomicU64::new(0));
        let (ch, mut rx) =
            ReliableChannel::new(32, Arc::clone(&retransmit), Arc::clone(&truncation));

        let mut bad = vec![FLAG_FIRST | FLAG_LAST, 0x00];
        bad.extend_from_slice(b"hi");

        ch.receive_fragment(&bad).await;

        assert_eq!(
            truncation.load(Ordering::Relaxed),
            1,
            "truncation counter should bump exactly once on missing canary"
        );
        assert!(
            rx.try_recv().is_err(),
            "corrupt fragment must not deliver a datagram"
        );
    }
}
