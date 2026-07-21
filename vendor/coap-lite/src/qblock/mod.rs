//! Q-Block (RFC 9177) primitives: the received/outstanding-block range tracker
//! and the `application/missing-blocks+cbor-seq` codec.
//!
//! This module is the pure, `no_std`, I/O-free foundation for Q-Block support.
//! It is a port of the bookkeeping in libcoap's `coap_rblock_t`
//! (`blocks_add_entry` / `blocks_delete_entry` / `check_all_blocks_in`) and its
//! `add_408_block` CBOR encoder, with the runtime (timers, transport, the burst
//! pump) layered on top in `coap-rs`.
//!
//! The block-option *value* (`(num << 4) | (more << 3) | szx`) is identical to
//! RFC 7959, so [`crate::block_handler::BlockValue`] is reused unchanged — only
//! the option numbers ([`crate::CoapOption::QBlock1`] = 19,
//! [`crate::CoapOption::QBlock2`] = 31) differ.

pub mod missing_blocks;

pub use missing_blocks::MissingBlocksError;

use alloc::vec::Vec;

/// Maximum number of disjoint received/outstanding ranges tracked at once.
///
/// Mirrors libcoap's `COAP_RBLOCK_CNT`. Loss scattered across more than this
/// many gaps cannot be tracked precisely; the offending block is dropped (see
/// [`RangeSet::insert`]) and recovered by retransmission instead. Keeps the
/// tracker O(1) in memory regardless of body size.
pub const RBLOCK_CNT: usize = 4;

/// An inclusive `[begin, end]` run of contiguous block numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Range {
    begin: u32,
    end: u32,
}

/// A bounded, sorted set of disjoint, non-adjacent block-number ranges.
///
/// Used in both directions of a Q-Block transfer:
/// - **receiver** — the set of blocks *received* so far ([`RangeSet::insert`]
///   on arrival; [`RangeSet::is_complete`] / [`RangeSet::missing`] to drive
///   reassembly and recovery requests);
/// - **sender** — the set of blocks *still to send* ([`RangeSet::remove`] as
///   each is transmitted; re-[`RangeSet::insert`]ed when the peer asks for a
///   retransmission).
///
/// Invariants: `ranges` is sorted ascending, the ranges are pairwise disjoint
/// and never adjacent (adjacent runs are always merged), and there are at most
/// [`RBLOCK_CNT`] of them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    ranges: Vec<Range>,
    /// `Some(n)` once the final block (the one with the More bit unset) has been
    /// seen, where `n` is the total block count (last block number + 1).
    total_blocks: Option<u32>,
}

impl RangeSet {
    /// Creates an empty range set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records block `block_num` as present, merging it into the range set.
    /// `more` is the block's More bit; when unset this block is the last one and
    /// fixes [`total_blocks`](RangeSet::total_blocks) at `block_num + 1`.
    ///
    /// Port of libcoap's `blocks_add_entry`. Returns `false` (and tracks
    /// nothing) when the block cannot be recorded, for either reason libcoap
    /// rejects it:
    /// - it lies beyond a known total (a stale/duplicate or malformed number), or
    /// - recording it would require a [`RBLOCK_CNT`]+1-th disjoint range
    ///   ("too many losses") — the caller should drop the block and let
    ///   retransmission recover it.
    pub fn insert(&mut self, block_num: u32, more: bool) -> bool {
        if matches!(self.total_blocks, Some(total) if block_num + 1 > total) {
            return false;
        }

        let mut handled = false;
        let mut i = 0;
        while i < self.ranges.len() {
            let r = self.ranges[i];
            if block_num >= r.begin && block_num <= r.end {
                // Already present.
                handled = true;
                break;
            }
            if block_num < r.begin {
                if block_num + 1 == r.begin {
                    self.ranges[i].begin = block_num;
                } else {
                    if self.ranges.len() == RBLOCK_CNT {
                        return false;
                    }
                    self.ranges.insert(
                        i,
                        Range {
                            begin: block_num,
                            end: block_num,
                        },
                    );
                }
                handled = true;
                break;
            }
            if block_num == r.end + 1 {
                self.ranges[i].end = block_num;
                // Bridge to the following range if now adjacent.
                if i + 1 < self.ranges.len()
                    && self.ranges[i + 1].begin == block_num + 1
                {
                    self.ranges[i].end = self.ranges[i + 1].end;
                    self.ranges.remove(i + 1);
                }
                handled = true;
                break;
            }
            i += 1;
        }

        if !handled {
            // Sits above every existing range.
            if self.ranges.len() == RBLOCK_CNT {
                return false;
            }
            self.ranges.push(Range {
                begin: block_num,
                end: block_num,
            });
        }

        if !more {
            self.total_blocks = Some(block_num + 1);
        }
        true
    }

    /// Removes block `block_num` from the set, splitting a range if it falls in
    /// the interior. Port of libcoap's `blocks_delete_entry`. Returns `false`
    /// when the block is beyond a known total, or when a split would exceed
    /// [`RBLOCK_CNT`] (in which case nothing is changed).
    ///
    /// Note: unlike libcoap's `blocks_delete_entry`, removing the `end` of a
    /// multi-element range here simply shrinks it (leaving a valid one-element
    /// range when applicable) rather than deleting the whole range — libcoap's
    /// version drops the surviving element, which loses data. We keep the
    /// correct semantics; see the `remove_*` unit tests.
    pub fn remove(&mut self, block_num: u32) -> bool {
        if matches!(self.total_blocks, Some(total) if block_num + 1 > total) {
            return false;
        }

        let mut i = 0;
        while i < self.ranges.len() {
            let r = self.ranges[i];
            if block_num >= r.begin && block_num <= r.end {
                if block_num == r.begin && block_num == r.end {
                    self.ranges.remove(i);
                } else if block_num == r.begin {
                    self.ranges[i].begin += 1;
                } else if block_num == r.end {
                    self.ranges[i].end -= 1;
                } else {
                    if self.ranges.len() == RBLOCK_CNT {
                        return false;
                    }
                    let upper = Range {
                        begin: block_num + 1,
                        end: r.end,
                    };
                    self.ranges[i].end = block_num - 1;
                    self.ranges.insert(i + 1, upper);
                }
                break;
            }
            i += 1;
        }
        true
    }

    /// The total block count, known once the final (More-bit-unset) block has
    /// been seen via [`insert`](RangeSet::insert).
    pub fn total_blocks(&self) -> Option<u32> {
        self.total_blocks
    }

    /// Whether any blocks are tracked.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Whether `block_num` is currently in the set.
    pub fn contains(&self, block_num: u32) -> bool {
        self.ranges
            .iter()
            .any(|r| block_num >= r.begin && block_num <= r.end)
    }

    /// The lowest tracked block number, if any. For a sender's outstanding set
    /// this is the next block to (re)transmit.
    pub fn first(&self) -> Option<u32> {
        self.ranges.first().map(|r| r.begin)
    }

    /// Whether the whole body has been received: the total is known and the set
    /// is the single contiguous run `0..=total-1`. Port of libcoap's
    /// `check_all_blocks_in`.
    pub fn is_complete(&self) -> bool {
        if self.total_blocks.is_none() {
            return false;
        }
        let mut block = 0u32;
        for r in &self.ranges {
            if block < r.begin {
                return false;
            }
            if block < r.end {
                block = r.end;
            }
        }
        true
    }

    /// The block numbers in `0..=final_block` that are *not* present, in
    /// ascending order. Callers (the recovery path in `coap-rs`) typically cap
    /// `final_block` to the current `MAX_PAYLOADS` window so a single recovery
    /// request stays small; that windowing is a policy concern left to the
    /// caller.
    pub fn missing(&self, final_block: u32) -> Vec<u32> {
        let mut out = Vec::new();
        let mut next = 0u32;
        for r in &self.ranges {
            while next < r.begin && next <= final_block {
                out.push(next);
                next += 1;
            }
            if next <= r.end {
                next = r.end.saturating_add(1);
            }
            if next > final_block {
                return out;
            }
        }
        while next <= final_block {
            out.push(next);
            next += 1;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(set: &RangeSet) -> Vec<(u32, u32)> {
        set.ranges.iter().map(|r| (r.begin, r.end)).collect()
    }

    #[test]
    fn insert_extends_and_bridges_ranges() {
        let mut s = RangeSet::new();
        // Out-of-order arrivals that should coalesce: 0,2,1 -> single [0,2].
        assert!(s.insert(0, true));
        assert!(s.insert(2, true));
        assert_eq!(ranges(&s), vec![(0, 0), (2, 2)]);
        assert!(s.insert(1, true)); // bridges the two ranges
        assert_eq!(ranges(&s), vec![(0, 2)]);
    }

    #[test]
    fn insert_extends_down_and_up() {
        let mut s = RangeSet::new();
        assert!(s.insert(5, true));
        assert!(s.insert(6, true)); // extend up
        assert!(s.insert(4, true)); // extend down
        assert_eq!(ranges(&s), vec![(4, 6)]);
    }

    #[test]
    fn insert_duplicate_is_noop() {
        let mut s = RangeSet::new();
        assert!(s.insert(3, true));
        assert!(s.insert(3, true));
        assert_eq!(ranges(&s), vec![(3, 3)]);
    }

    #[test]
    fn insert_overflow_drops_fifth_disjoint_range() {
        let mut s = RangeSet::new();
        // Four disjoint ranges (gaps between each): 0,2,4,6.
        for n in [0u32, 2, 4, 6] {
            assert!(s.insert(n, true));
        }
        assert_eq!(s.ranges.len(), RBLOCK_CNT);
        // A fifth disjoint range cannot be tracked.
        assert!(!s.insert(8, true));
        assert_eq!(s.ranges.len(), RBLOCK_CNT);
        // But a block that extends an existing range still works.
        assert!(s.insert(1, true)); // bridges 0 and 2
        assert_eq!(ranges(&s), vec![(0, 2), (4, 4), (6, 6)]);
    }

    #[test]
    fn insert_beyond_known_total_is_rejected() {
        let mut s = RangeSet::new();
        assert!(s.insert(2, false)); // last block -> total = 3
        assert_eq!(s.total_blocks(), Some(3));
        assert!(!s.insert(3, true)); // block 3 is beyond total
    }

    #[test]
    fn is_complete_only_when_contiguous_from_zero() {
        let mut s = RangeSet::new();
        assert!(!s.is_complete()); // no total yet
        assert!(s.insert(0, true));
        assert!(s.insert(2, false)); // total known (3) but gap at 1
        assert!(!s.is_complete());
        assert!(s.insert(1, true)); // fills the gap
        assert!(s.is_complete());
    }

    #[test]
    fn missing_enumerates_gaps_within_final_block() {
        let mut s = RangeSet::new();
        for n in [0u32, 1, 4, 5, 9] {
            s.insert(n, true);
        }
        assert_eq!(s.missing(9), vec![2, 3, 6, 7, 8]);
        // Capped to a window.
        assert_eq!(s.missing(5), vec![2, 3]);
        // Trailing gap beyond the last received block.
        assert_eq!(s.missing(11), vec![2, 3, 6, 7, 8, 10, 11]);
    }

    #[test]
    fn missing_is_empty_for_complete_run() {
        let mut s = RangeSet::new();
        for n in 0..=4 {
            s.insert(n, n != 4 /* more set on all but the last */);
        }
        assert!(s.is_complete());
        assert!(s.missing(4).is_empty());
    }

    #[test]
    fn remove_begin_end_and_single() {
        let mut s = RangeSet::new();
        for n in 0..=4 {
            s.insert(n, true);
        }
        assert!(s.remove(0)); // shrink begin
        assert_eq!(ranges(&s), vec![(1, 4)]);
        assert!(s.remove(4)); // shrink end (must NOT drop block 1..3)
        assert_eq!(ranges(&s), vec![(1, 3)]);
        assert!(s.first() == Some(1));
    }

    #[test]
    fn remove_end_of_two_element_range_keeps_survivor() {
        // Regression guard against libcoap's blocks_delete_entry quirk.
        let mut s = RangeSet::new();
        s.insert(5, true);
        s.insert(6, true);
        assert_eq!(ranges(&s), vec![(5, 6)]);
        assert!(s.remove(6));
        assert_eq!(ranges(&s), vec![(5, 5)]); // block 5 survives
        assert!(s.contains(5));
    }

    #[test]
    fn remove_interior_splits_range() {
        let mut s = RangeSet::new();
        for n in 0..=4 {
            s.insert(n, true);
        }
        assert!(s.remove(2)); // split [0,4] -> [0,1],[3,4]
        assert_eq!(ranges(&s), vec![(0, 1), (3, 4)]);
    }

    #[test]
    fn remove_interior_split_overflow_is_rejected() {
        let mut s = RangeSet::new();
        // Build four ranges with one of them wide enough to split.
        for n in [0u32, 1, 2, 4, 6, 8] {
            s.insert(n, true);
        }
        // [0,2],[4,4],[6,6],[8,8] — at capacity.
        assert_eq!(s.ranges.len(), RBLOCK_CNT);
        // Splitting [0,2] by removing 1 would need a 5th range -> rejected,
        // nothing changes.
        let before = ranges(&s);
        assert!(!s.remove(1));
        assert_eq!(ranges(&s), before);
    }
}
