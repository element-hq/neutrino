//! Outbound federation HTTP client.
//!
//! The sending half of the Server-Server API: PUTs transactions to peers and
//! fetches missing ancestry for gap-filling. Trusted-mesh stance, matching the
//! inbound handlers:
//!
//! - **Resolution** is `http://{server_name}` — raw IP:port, no TLS, no
//!   `.well-known` / SRV lookup.
//! - **No X-Matrix auth** header and no request signing.
//! - PDUs are opaque `RawValue`s on the wire, never re-parsed here.
//!
//! Consumed by the per-destination sender pool (`federation::sender`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use reqwest::Client;
use ruma::{OwnedEventId, RoomId, ServerName};
use serde::Serialize;
use serde_json::value::RawValue as RawJsonValue;

use crate::federation::{get_missing_events, now_ms};

/// Connection-establishment timeout for a federation request.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Total per-request timeout (headers + body). Bounds a slow/black-holing peer
/// so it can't stall a sender task indefinitely.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Errors the outbound client can surface to a caller (the PR3 sender loop,
/// which decides retry-vs-give-up from the variant).
#[derive(Debug, thiserror::Error)]
pub(crate) enum FederationClientError {
    /// Transport-level failure: connection refused, DNS, timeout, or a
    /// malformed response body. Generally retryable.
    #[error("federation transport error: {0}")]
    Transport(#[from] reqwest::Error),
    /// The peer answered with a non-2xx status. Carries the raw code so the
    /// caller can distinguish e.g. a 4xx (give up) from a 5xx (retry).
    #[error("peer returned HTTP {0}")]
    Status(u16),
    /// The target URL could not be built from the destination + room id.
    /// Unreachable for a validated `ServerName` + a base `http://` URL, but
    /// surfaced rather than panicked on.
    // TODO(PR4): only constructed by `get_missing_events`, which the reqwest
    // `MissingEventsFetcher` will consume.
    #[allow(dead_code)]
    #[error("could not build federation URL")]
    InvalidUrl,
}

/// reqwest-backed client for outbound federation requests.
pub(crate) struct FederationClient {
    http: Client,
    /// This homeserver's own name, sent as the transaction `origin`.
    origin: String,
}

impl FederationClient {
    pub(crate) fn new(origin: String) -> Self {
        // Direct connections only: a trusted mesh resolves peers to raw
        // IP:port, so bypass any ambient HTTP proxy (which would otherwise
        // intercept `http://{ip}` traffic). `build()` only fails on TLS-backend
        // init, which we don't use — fall back to the default client if so.
        let http = Client::builder()
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { http, origin }
    }

    /// `PUT http://{dest}/_matrix/federation/v1/send/{txn_id}` carrying `pdus`
    /// (and an empty `edus` list — EDUs are out of scope). `Ok(())` on a 2xx;
    /// the per-PDU result map in the response body is ignored (the spec marks
    /// `error` advisory, and our durable retry lives in the outbox, not in
    /// parsing the peer's verdicts).
    pub(crate) async fn send_transaction(
        &self,
        dest: &ServerName,
        txn_id: &str,
        pdus: &[Box<RawJsonValue>],
    ) -> Result<(), FederationClientError> {
        // `txn_id` is locally generated (`{u64}-{u64}`) and `dest` is a
        // validated `ServerName`, so neither needs escaping in the path.
        let url = format!("http://{dest}/_matrix/federation/v1/send/{txn_id}");
        let body = TransactionRequest {
            origin: &self.origin,
            // The peer ignores this (see inbound `_origin_server_ts`), but the
            // field is required by the wire shape, so send a real timestamp.
            origin_server_ts: now_ms(),
            pdus,
            edus: &[],
        };
        let resp = self.http.put(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(FederationClientError::Status(resp.status().as_u16()));
        }
        Ok(())
    }

    /// `POST http://{dest}/_matrix/federation/v1/get_missing_events/{room_id}`
    /// to fetch ancestry between `earliest` (boundary already held) and
    /// `latest` (heads to walk back from), up to `limit` events. Returns the
    /// peer's `events` array (oldest-first), opaque PDU bytes.
    // TODO(PR4): consumed by the reqwest `MissingEventsFetcher` replacing
    // `NoFetcher`; unused until then.
    #[allow(dead_code)]
    pub(crate) async fn get_missing_events(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        earliest: &[OwnedEventId],
        limit: u32,
    ) -> Result<Vec<Box<RawJsonValue>>, FederationClientError> {
        // `room_id` goes in a path segment. ruma's `RoomId` localpart is not
        // URL-validated (it may contain `/`, `?`, `#`), so push it through
        // `Url` rather than `format!` to percent-encode it. v12 room ids are
        // url-safe-base64 in practice, but don't rely on that here.
        // No trailing slash on the base: `path_segments_mut().push()` appends a
        // segment, so a trailing slash would yield an empty segment + double
        // slash (`…/get_missing_events//{room}`).
        let mut url = reqwest::Url::parse(&format!(
            "http://{dest}/_matrix/federation/v1/get_missing_events"
        ))
        .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str());

        let body = MissingEventsRequest {
            earliest_events: earliest,
            latest_events: latest,
            limit,
        };
        let resp = self.http.post(url).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(FederationClientError::Status(resp.status().as_u16()));
        }
        Ok(resp
            .json::<get_missing_events::ResponseBody>()
            .await?
            .events)
    }
}

/// Monotonic transaction-id source: `{startup_prefix}-{counter}`. The prefix
/// (a process-startup timestamp, supplied by the caller) keeps ids unique
/// across restarts; the counter keeps them unique within a run. Receivers
/// dedup on `(origin, txn_id)` via `FederationInbox::record_federation_txn`.
pub(crate) struct TxnIdGen {
    prefix: u64,
    counter: AtomicU64,
}

impl TxnIdGen {
    pub(crate) fn new(prefix: u64) -> Self {
        Self {
            prefix,
            counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn next_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}", self.prefix, n)
    }
}

/// Outbound transaction body. Borrows everything — no clones on the send path.
#[derive(Serialize)]
struct TransactionRequest<'a> {
    origin: &'a str,
    origin_server_ts: u64,
    pdus: &'a [Box<RawJsonValue>],
    edus: &'a [Box<RawJsonValue>],
}

/// Outbound `/get_missing_events` request body. Mirrors the inbound
/// `RequestBody` (`get_missing_events.rs`): `min_depth` is omitted (optional,
/// and the peer ignores it).
// TODO(PR4): constructed only by `get_missing_events`; unused until the fetcher.
#[allow(dead_code)]
#[derive(Serialize)]
struct MissingEventsRequest<'a> {
    earliest_events: &'a [OwnedEventId],
    latest_events: &'a [OwnedEventId],
    limit: u32,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::Path,
        http::StatusCode,
        routing::{post, put},
    };
    use ruma::{OwnedRoomId, OwnedServerName, event_id, room_id};
    use serde_json::{Value, json};

    use super::*;

    /// Bind an axum stub on an ephemeral localhost port and return its
    /// `ServerName` (`127.0.0.1:{port}`). The listener is bound before the
    /// task spawns, so the OS accept queue absorbs an immediate client
    /// connect — no readiness race.
    async fn spawn_stub(app: Router) -> OwnedServerName {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn raw(json_str: &str) -> Box<RawJsonValue> {
        RawJsonValue::from_string(json_str.to_owned()).unwrap()
    }

    #[tokio::test]
    async fn send_transaction_puts_to_correct_path_and_body() {
        let captured: Arc<Mutex<Option<(String, Value)>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let app = Router::new().route(
            "/_matrix/federation/v1/send/{txn}",
            put(move |Path(txn): Path<String>, body: Json<Value>| {
                let cap = cap.clone();
                async move {
                    *cap.lock().unwrap() = Some((txn, body.0));
                    Json(json!({ "pdus": {} }))
                }
            }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let pdus = [raw(r#"{"n":1}"#), raw(r#"{"n":2}"#)];
        client
            .send_transaction(&dest, "txn-1", &pdus)
            .await
            .unwrap();

        let (txn, body) = captured
            .lock()
            .unwrap()
            .clone()
            .expect("stub got a request");
        assert_eq!(txn, "txn-1");
        assert_eq!(body["origin"], "local.test");
        // PDU order is preserved on the wire.
        assert_eq!(body["pdus"][0]["n"], 1);
        assert_eq!(body["pdus"][1]["n"], 2);
        assert!(body["edus"].as_array().unwrap().is_empty());
        // A real (non-saturated) timestamp, not the pre-epoch floor.
        assert!(body["origin_server_ts"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn send_transaction_surfaces_non_2xx_as_status_error() {
        let app = Router::new().route(
            "/_matrix/federation/v1/send/{txn}",
            put(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let pdu = raw("{}");
        let err = client
            .send_transaction(&dest, "t", std::slice::from_ref(&pdu))
            .await
            .unwrap_err();
        assert!(
            matches!(err, FederationClientError::Status(500)),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_missing_events_posts_request_and_parses_events() {
        let captured: Arc<Mutex<Option<(String, Value)>>> = Arc::new(Mutex::new(None));
        let cap = captured.clone();
        let app = Router::new().route(
            "/_matrix/federation/v1/get_missing_events/{room}",
            post(move |Path(room): Path<String>, body: Json<Value>| {
                let cap = cap.clone();
                async move {
                    *cap.lock().unwrap() = Some((room, body.0));
                    Json(json!({ "events": [ {"a": 1}, {"b": 2} ] }))
                }
            }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let latest = vec![event_id!("$late:example.org").to_owned()];
        let earliest = vec![event_id!("$early:example.org").to_owned()];

        let events = client
            .get_missing_events(&dest, &room, &latest, &earliest, 5)
            .await
            .unwrap();
        // Count, content, and order (oldest-first) all preserved.
        assert_eq!(events.len(), 2);
        let parsed: Vec<Value> = events
            .iter()
            .map(|e| serde_json::from_str(e.get()).unwrap())
            .collect();
        assert_eq!(parsed[0]["a"], 1);
        assert_eq!(parsed[1]["b"], 2);

        let (room_in, body) = captured
            .lock()
            .unwrap()
            .clone()
            .expect("stub got a request");
        assert_eq!(room_in, room.as_str());
        assert_eq!(body["limit"], 5);
        assert_eq!(body["latest_events"][0], "$late:example.org");
        assert_eq!(body["earliest_events"][0], "$early:example.org");
    }

    #[tokio::test]
    async fn get_missing_events_surfaces_non_2xx_as_status_error() {
        let app = Router::new().route(
            "/_matrix/federation/v1/get_missing_events/{room}",
            post(|| async { StatusCode::NOT_FOUND }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let err = client
            .get_missing_events(&dest, &room, &[], &[], 10)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FederationClientError::Status(404)),
            "got {err:?}"
        );
    }

    #[test]
    fn txn_id_gen_is_monotonic_and_prefixed() {
        let g = TxnIdGen::new(42);
        assert_eq!(g.next_id(), "42-0");
        assert_eq!(g.next_id(), "42-1");
        assert_eq!(g.next_id(), "42-2");
    }

    #[test]
    fn txn_id_gen_concurrent_ids_are_unique() {
        use std::collections::HashSet;
        // The whole point of the `AtomicU64` is concurrent senders; pin it.
        let idgen = Arc::new(TxnIdGen::new(7));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let g = idgen.clone();
                std::thread::spawn(move || (0..1000).map(|_| g.next_id()).collect::<Vec<_>>())
            })
            .collect();
        let mut all = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(all.insert(id), "duplicate txn id under concurrency");
            }
        }
        assert_eq!(all.len(), 8 * 1000);
    }

    #[tokio::test]
    async fn send_transaction_connection_refused_is_transport_error() {
        // Bind then drop to get a port nothing is listening on → connect fails.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let dest: OwnedServerName = format!("127.0.0.1:{port}").parse().unwrap();

        let client = FederationClient::new("local.test".to_owned());
        let pdu = raw("{}");
        let err = client
            .send_transaction(&dest, "t", std::slice::from_ref(&pdu))
            .await
            .unwrap_err();
        assert!(
            matches!(err, FederationClientError::Transport(_)),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_missing_events_empty_body_yields_empty_vec() {
        // A 2xx `{}` (no `events` key) decodes to an empty vec via
        // `#[serde(default)]` — "the peer gave us nothing new".
        let app = Router::new().route(
            "/_matrix/federation/v1/get_missing_events/{room}",
            post(|| async { Json(json!({})) }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let events = client
            .get_missing_events(&dest, &room, &[], &[], 10)
            .await
            .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn get_missing_events_malformed_body_is_transport_error() {
        // A 2xx with a non-JSON body fails to deserialize → `Transport`.
        let app = Router::new().route(
            "/_matrix/federation/v1/get_missing_events/{room}",
            post(|| async { "not json at all" }),
        );
        let dest = spawn_stub(app).await;

        let client = FederationClient::new("local.test".to_owned());
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let err = client
            .get_missing_events(&dest, &room, &[], &[], 10)
            .await
            .unwrap_err();
        assert!(
            matches!(err, FederationClientError::Transport(_)),
            "got {err:?}"
        );
    }

    /// The client's serialized request bodies must satisfy the *real* inbound
    /// parsers — the lax `Json<Value>` stubs above can't catch field-name drift
    /// between the two federation halves, but this does.
    #[test]
    fn outbound_bodies_round_trip_through_inbound_parsers() {
        use crate::federation::get_missing_events::RequestBody;
        use crate::federation::send::TransactionBody;

        let pdu = raw(r#"{"type":"m.room.message"}"#);
        let txn = TransactionRequest {
            origin: "local.test",
            origin_server_ts: 12345,
            pdus: std::slice::from_ref(&pdu),
            edus: &[],
        };
        let txn_json = serde_json::to_value(&txn).unwrap();
        let _: TransactionBody =
            serde_json::from_value(txn_json).expect("inbound /send parses the client's body");

        let latest = vec![event_id!("$l:example.org").to_owned()];
        let earliest = vec![event_id!("$e:example.org").to_owned()];
        let req = MissingEventsRequest {
            earliest_events: &earliest,
            latest_events: &latest,
            limit: 7,
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let _: RequestBody = serde_json::from_value(req_json)
            .expect("inbound /get_missing_events parses the client's body");
    }
}
