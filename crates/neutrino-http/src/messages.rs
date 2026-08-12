//! CSAPI `GET /_matrix/client/v3/rooms/{roomId}/messages` — paginated room history.
//!
//! Mirrors Synapse's `RoomMessageListRestServlet` / `PaginationHandler.get_messages`
//! for the parts neutrino has a mechanism for.
//!
//! KNOWN LIMITATIONS (deliberate — no mechanism in neutrino):
//! - The `filter` query param is **accepted but ignored**: event filtering and
//!   `lazy_load_members` are unimplemented, so the optional `state` field is never
//!   emitted.
//! - No history-visibility filtering: a joined user receives the full timeline chunk.
//!
//! A backward (`dir=b`) page that underflows `limit` triggers ONE federation
//! backfill round (see `backfill_and_reread`) when the room has backward
//! extremities and a remote peer; the client paginating again drives the next
//! round. With no peer / no extremities it stays local-only, and an empty `chunk`
//! with no `end` means the local timeline start was reached.

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use ruma::OwnedRoomId;
use ruma::events::AnyTimelineEvent;
use ruma::serde::Raw;
use serde_json::{Map, Value, json};

use neutrino_store::{Direction, EventStore, PaginationToken};

use crate::federation::client::FederationClient;
use crate::membership::current_membership;
use crate::{AppState, AuthUser, error_response, lock_app};

/// Parse `dir`. Absent → Forward (Synapse default; the spec marks it required,
/// we mirror Synapse's leniency). Only `b`/`f` accepted.
// A built HTTP `Response` is the deliberate error payload (mirroring the
// membership helpers); boxing it just to satisfy the large-Err heuristic adds
// noise on a per-request path.
#[allow(clippy::result_large_err)]
fn parse_dir(params: &HashMap<String, String>) -> Result<Direction, axum::response::Response> {
    match params.get("dir").map(String::as_str) {
        Some("b") => Ok(Direction::Backward),
        Some("f") | None => Ok(Direction::Forward),
        Some(other) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "M_INVALID_PARAM",
            &format!("dir must be 'b' or 'f', got '{other}'"),
        )),
    }
}

/// Parse an opaque pagination token (`from`/`to`). Absent, empty, or the legacy
/// `"END"` sentinel → `None`. A non-numeric value, or one exceeding `i64::MAX`
/// (stream positions are stored as `i64`), is client garbage → 400. Bounding
/// here keeps the store's only `room_messages` error a genuine fault, never a
/// malformed-token 500.
#[allow(clippy::result_large_err)] // see `parse_dir`
fn parse_token(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<PaginationToken>, axum::response::Response> {
    match params.get(key).map(String::as_str) {
        None | Some("") | Some("END") => Ok(None),
        Some(s) => match s.parse::<i64>() {
            Ok(n) => Ok(Some(PaginationToken(n))),
            Err(_) => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                &format!("'{key}' parameter is invalid"),
            )),
        },
    }
}

/// Parse `limit`. Absent → 10; capped at 1000 (mirrors Synapse); non-integer → 400.
#[allow(clippy::result_large_err)] // see `parse_dir`
fn parse_limit(params: &HashMap<String, String>) -> Result<usize, axum::response::Response> {
    match params.get("limit") {
        None => Ok(10),
        Some(s) => match usize::from_str(s) {
            Ok(n) => Ok(n.min(1000)),
            Err(_) => Err(error_response(
                StatusCode::BAD_REQUEST,
                "M_INVALID_PARAM",
                "'limit' parameter is invalid",
            )),
        },
    }
}

/// One bounded federation backfill round for a backward page that underflowed
/// `limit`. Runs a single
/// [`backfill_once`](crate::federation::backfill_out::backfill_once) round (which
/// itself reads the room's backward extremities and early-returns 0 when there
/// are none); if it persisted any events, re-reads and returns the fresh page,
/// otherwise returns the original `(events, next)` unchanged. Uses the shared
/// `App`-owned [`FederationClient`] so each back-page reuses its connection pool.
///
/// `Err` carries a built error `Response` for a re-read storage fault, mirroring
/// the first `room_messages` read's mapping.
#[allow(clippy::result_large_err)] // see `parse_dir`
async fn backfill_and_reread(
    store: &neutrino_store_sqlite::SqliteStore,
    own_server: &str,
    client: &FederationClient,
    policy: &neutrino_event::EventPolicy,
    rid: &ruma::RoomId,
    read: (
        Option<PaginationToken>,
        Option<PaginationToken>,
        Direction,
        usize,
    ),
    original: (Vec<neutrino_event::Event>, Option<PaginationToken>),
) -> Result<(Vec<neutrino_event::Event>, Option<PaginationToken>), axum::response::Response> {
    let (from, to, dir, limit) = read;
    // No outer backward-extremities pre-check: `backfill_once` reads them itself
    // and returns 0 when there are no seeds, so a redundant gate query is wasted.
    let n = crate::federation::backfill_out::backfill_once(
        store,
        client,
        policy,
        own_server,
        rid,
        limit as u32,
    )
    .await;
    if n > 0 {
        store
            .room_messages(rid, from, to, dir, limit)
            .await
            .map_err(|e| {
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "M_UNKNOWN",
                    &e.to_string(),
                )
            })
    } else {
        Ok(original)
    }
}

pub(crate) async fn get_messages(
    state: State<AppState>,
    AuthUser(user): AuthUser,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let rid = match OwnedRoomId::try_from(room_id) {
        Ok(r) => r,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "M_INVALID_PARAM", &e.to_string());
        }
    };

    // Parse query params (→ 400) *before* the membership check (→ 403), so a
    // malformed request is rejected as malformed regardless of membership
    // (mirrors Synapse, which builds the PaginationConfig before the room
    // check).
    let dir = match parse_dir(&params) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let from = match parse_token(&params, "from") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let to = match parse_token(&params, "to") {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let limit = match parse_limit(&params) {
        Ok(l) => l,
        Err(resp) => return resp,
    };

    // Join check: must be a current member. Not joined — including an unknown
    // room (no member event) — is 403, the spec's only documented error here.
    match current_membership(&state.0, &rid, &user).await {
        Ok(Some(m)) if m == "join" => {}
        Ok(_) => {
            return error_response(
                StatusCode::FORBIDDEN,
                "M_FORBIDDEN",
                "You aren't a member of the room.",
            );
        }
        Err(resp) => return resp,
    }

    // Clone the store, the own server name (for `backfill_once`'s self-skip), and
    // the shared outbound `FederationClient` that `App` builds once at startup —
    // so a backward-underflow backfill round reuses its connection pool rather
    // than constructing a client per back-page.
    let (store, own_server, fed_client, policy) = {
        let app = lock_app(&state.0);
        (
            app.store.clone(),
            app.config.server_name.clone(),
            app.fed_client.clone(),
            app.policy.clone(),
        )
    };

    // `start`: echo `from` if given; else the boundary we paginate from —
    // Forward → "0" (earliest), Backward → this room's stream head (latest).
    // The head is room-scoped (`room_stream_head`), not the global watch
    // position, which could belong to another room.
    let start = match &from {
        Some(t) => t.0.to_string(),
        None => match dir {
            Direction::Forward => "0".to_string(),
            Direction::Backward => match store.room_stream_head(&rid).await {
                Ok(head) => head.0.to_string(),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "M_UNKNOWN",
                        &e.to_string(),
                    );
                }
            },
        },
    };

    // `room_messages` consumes `from`/`to`; on the backward path keep copies for a
    // possible re-read after a federation backfill round below (forward never
    // backfills, so it pays nothing).
    let reread_tokens = match dir {
        Direction::Backward => Some((from.clone(), to.clone())),
        Direction::Forward => None,
    };
    let (events, next) = match store.room_messages(&rid, from, to, dir, limit).await {
        Ok(pair) => pair,
        Err(e) => {
            // Room existence is guaranteed by the join check above, so this is
            // a genuine storage fault, not an unknown-room case.
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "M_UNKNOWN",
                &e.to_string(),
            );
        }
    };

    // Federation backfill: a backward page that didn't fill `limit` may have hit
    // the local history boundary. If the room has backward extremities, pull one
    // round from a peer and re-read. Bounded to a single round per request — the
    // client paginating again drives the next round. `reread_tokens` is `Some`
    // exactly on the backward path, so this only fires for a backward underflow.
    let (events, next) = match reread_tokens {
        Some((from_again, to_again)) if events.len() < limit => {
            match backfill_and_reread(
                &store,
                &own_server,
                &fed_client,
                &policy,
                &rid,
                (from_again, to_again, dir, limit),
                (events, next),
            )
            .await
            {
                Ok(pair) => pair,
                Err(resp) => return resp,
            }
        }
        _ => (events, next),
    };

    // Order is exactly as room_messages returns it: `b` newest-first,
    // `f` oldest-first. No reversal (unlike sliding-sync).
    let chunk: Vec<Raw<AnyTimelineEvent>> =
        events.iter().map(Raw::<AnyTimelineEvent>::from).collect();

    let mut body: Map<String, Value> = Map::new();
    body.insert("chunk".to_string(), json!(chunk));
    body.insert("start".to_string(), Value::String(start));
    if let Some(t) = next {
        body.insert("end".to_string(), Value::String(t.0.to_string()));
    }

    (StatusCode::OK, axum::Json(Value::Object(body))).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use neutrino_event::event_builder::EventBuilder;
    use neutrino_event::{Event, ROOM_VERSION_ID};
    use neutrino_store::{DagStore, EventStore, RoomStore};
    use neutrino_store_sqlite::SqliteStore;
    use ruma::{OwnedEventId, OwnedUserId, event_id};
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    const OWN: &str = "example.org";

    /// Seed a room joined only by the local server (so backfill has NO remote peer
    /// to ask), with a message whose `prev_events` dangles onto an unheld id —
    /// opening a backward extremity. Returns the room id and the event id of that
    /// message, so the test can pin that the no-op re-read returns the SAME page.
    async fn room_with_extremity_no_peer(store: &SqliteStore) -> (ruma::OwnedRoomId, OwnedEventId) {
        let creator: OwnedUserId = format!("@alice:{OWN}").parse().unwrap();
        let create = EventBuilder::new(
            creator.clone(),
            "m.room.create".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .state_key(String::new())
        .content(json!({ "room_version": ROOM_VERSION_ID }))
        .build()
        .expect("build create");
        let room_id = create.room_id.clone();
        let join = EventBuilder::new(
            creator.clone(),
            "m.room.member".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .room_id(room_id.clone())
        .state_key(creator.as_str().to_owned())
        .content(json!({ "membership": "join" }))
        .prev_events(vec![create.event_id.clone()])
        .prev_state_events(vec![create.event_id.clone()])
        .build()
        .expect("build join");
        store.create_room(&create, &[join]).await.expect("create");
        let dangling = EventBuilder::new(
            creator,
            "m.room.message".to_owned(),
            neutrino_event::base_version().clone(),
        )
        .room_id(room_id.clone())
        .content(json!({ "msgtype": "m.text", "body": "tip" }))
        .prev_events(vec![event_id!("$unheld:remote.example.org").to_owned()])
        .build()
        .expect("build dangling");
        let tip_id = dangling.event_id.clone();
        store
            .persist_historical_event(&dangling)
            .await
            .expect("persist dangling");
        (room_id, tip_id)
    }

    /// The trigger path is wired and safe when there is nothing to do: a backward
    /// underflow on a room that HAS a backward extremity but NO remote peer runs
    /// the (no-op) backfill round and returns the ORIGINAL page byte-for-byte, not
    /// an empty or garbage page. The full peer round-trip — backfill actually
    /// persisting and the re-read returning fresh events — is left to a
    /// separate end-to-end test.
    #[tokio::test]
    async fn backfill_and_reread_is_noop_without_peer() {
        let dir = TempDir::new().expect("tempdir");
        let store = Arc::new(
            SqliteStore::open_in_dir(dir.path())
                .await
                .expect("open sqlite"),
        );
        let (rid, tip_id) = room_with_extremity_no_peer(&store).await;

        // A backward read from the room head that underflows (room holds far fewer
        // than `limit` events). The local page must be NON-empty, so asserting the
        // re-read returns the same ids is a real check, not `0 == 0`.
        let head = PaginationToken(store.room_stream_head(&rid).await.expect("head").0 as i64);
        let (orig_events, orig_next): (Vec<Event>, Option<PaginationToken>) = store
            .room_messages(&rid, Some(head.clone()), None, Direction::Backward, 100)
            .await
            .expect("first read");
        let orig_ids: Vec<OwnedEventId> = orig_events.iter().map(|e| e.event_id.clone()).collect();
        assert!(
            orig_ids.contains(&tip_id),
            "precondition: the local backward page is non-empty (contains the tip)"
        );
        assert!(
            orig_events.len() < 100,
            "precondition: the backward page underflows `limit`"
        );
        assert!(
            !store
                .backward_extremities(&rid)
                .await
                .expect("extremities")
                .is_empty(),
            "precondition: a backward extremity exists, so the trigger arm fires \
             and `backfill_once` actually runs rather than being skipped"
        );

        let client = FederationClient::new(OWN.to_owned(), None);
        let out = backfill_and_reread(
            &store,
            OWN,
            &client,
            &neutrino_event::EventPolicy::trusted_network(),
            &rid,
            (Some(head), None, Direction::Backward, 100),
            (orig_events, orig_next.clone()),
        )
        .await
        .expect("no-op backfill must not error");

        let out_ids: Vec<OwnedEventId> = out.0.iter().map(|e| e.event_id.clone()).collect();
        assert_eq!(
            out_ids, orig_ids,
            "no peer -> backfill is a no-op -> the exact original page is returned"
        );
        assert_eq!(out.1, orig_next, "the `next` token is unchanged too");
    }
}
