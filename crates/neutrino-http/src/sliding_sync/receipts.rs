//! Synthesised delivery receipts for the sliding-sync receipts extension.
//!
//! This server produces no receipt EDUs and receives none — federation carries
//! PDUs only (CLAUDE.md). What it *does* know is when a peer acknowledged a
//! `/send` transaction, which the outbound sender records as a per-(room,
//! destination) high-water mark (`DeliveryStore`). This module turns those
//! marks into `m.receipt` ephemerals so a client can render "the other side
//! has it".
//!
//! Two deliberate lies are baked in, both of which are why the whole thing sits
//! behind `Config::delivery_receipts`:
//!
//! - **`m.read` means delivered here, not read.** Nobody read anything; the
//!   receiving *server* accepted the transaction. This is the receipt type
//!   stock clients already render, which is the point — but a deployment whose
//!   client doesn't want that reading should leave the knob off.
//! - **Delivery is per-server, receipts are per-user.** One acknowledgement
//!   from `peer.test` is expanded to a receipt for every user joined from
//!   `peer.test`. That is the strongest true statement available: their server
//!   holds the event, so each of their users is one local read away from it.

use std::collections::BTreeMap;

use neutrino_store::{Delivery, DeliveryPos, StorageBackend, StorageError};
use ruma::api::client::sync::sync_events::v5::response;
use ruma::events::receipt::{Receipt, ReceiptEventContent, ReceiptType, SyncReceiptEvent};
use ruma::serde::Raw;
use ruma::{MilliSecondsSinceUnixEpoch, RoomId, UInt, UserId};

use super::conn::Conn;
use super::{SyncError, SyncState};

/// Build the receipts extension for this response, advancing the connection's
/// delivery high-water mark past everything it carries.
///
/// Mirrors the timeline's initial/delta split: an initial sync takes every mark
/// the server holds (the marks *are* the full state — one row per room and
/// peer), a delta takes only those that moved since this connection's last
/// response.
///
/// The advance happens here rather than in the caller because a mark rendered
/// into a response must not be rendered again; the caller's long-poll can
/// re-enter `build_response` several times per request, and `has_data` counts a
/// non-empty receipts extension as data, so the build whose marks are consumed
/// is the build that gets returned.
pub(super) async fn build_receipts<S: StorageBackend>(
    state: &SyncState<S>,
    conn: &mut Conn,
    initial_sync: bool,
) -> Result<response::Receipts, SyncError> {
    let since = if initial_sync {
        DeliveryPos(0)
    } else {
        DeliveryPos(conn.last_delivery_pos)
    };
    let deliveries = state.store.deliveries_since(since).await?;

    let mut receipts = response::Receipts::default();
    let Some(highest) = deliveries.iter().map(|d| d.pos.0).max() else {
        return Ok(receipts);
    };

    // Marks for one room merge into a single `m.receipt`: several peers can
    // have acknowledged the same event, and each contributes its own users
    // under that event id.
    let mut by_room: BTreeMap<&RoomId, Vec<&Delivery>> = BTreeMap::new();
    for delivery in &deliveries {
        by_room.entry(&delivery.room_id).or_default().push(delivery);
    }

    for (room_id, marks) in by_room {
        // One membership read per room, not per mark — every peer's users come
        // out of the same joined-member set.
        let members = state.store.joined_members(room_id).await?;
        let mut content = ReceiptEventContent(BTreeMap::new());
        for mark in marks {
            let readers = members
                .keys()
                .map(AsRef::as_ref)
                .filter(|user: &&UserId| user.server_name() == mark.destination);
            let ts = MilliSecondsSinceUnixEpoch(UInt::new_saturating(mark.ts));
            for user in readers {
                content
                    .entry(mark.event_id.clone())
                    .or_default()
                    .entry(ReceiptType::Read)
                    .or_default()
                    .insert(user.to_owned(), Receipt::new(ts));
            }
        }
        // A peer whose users have all since left the room contributed nothing:
        // there is no user to attribute its acknowledgement to.
        if content.is_empty() {
            continue;
        }
        // Only reachable if `ReceiptEventContent` isn't representable as JSON,
        // which it is by construction — but the alternative to mapping it is an
        // `.unwrap()` in handler code (CLAUDE.md).
        let raw = Raw::new(&SyncReceiptEvent::new(content))
            .map_err(|e| SyncError::Storage(StorageError::Internal(e.to_string())))?;
        receipts.rooms.insert(room_id.to_owned(), raw);
    }

    conn.last_delivery_pos = conn.last_delivery_pos.max(highest);
    Ok(receipts)
}
