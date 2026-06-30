//! Outbound federation HTTP client.
//!
//! The sending half of the Server-Server API: PUTs transactions to peers and
//! fetches missing ancestry for gap-filling. Trusted-mesh stance, matching the
//! inbound handlers:
//!
//! - **Resolution** is `http://{server_name}` — raw IP:port, no TLS, no
//!   `.well-known` / SRV lookup.
//! - **X-Matrix header sent** (network-attested origin + destination, no
//!   key/sig — see [`crate::federation::auth`]); no request signing.
//! - PDUs are opaque `RawValue`s on the wire, never re-parsed here.
//!
//! Consumed by the per-destination sender pool (`federation::sender`).

use std::sync::Arc;
use std::time::Duration;

use std::collections::BTreeMap;

use reqwest::Client;
use ruma::{EventId, OwnedEventId, OwnedRoomId, RoomId, ServerName, UserId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue as RawJsonValue;
use tracing::{info, warn};

use neutrino_engine::{
    FederationTransport, ForwardExtremities, MissingEventsFetcher, MissingEventsQuery,
    TransportError,
};

use crate::federation::get_missing_events;
use neutrino_common::now_ms;

/// Connection-establishment timeout for a federation request.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Total per-request timeout (headers + body). Bounds a slow/black-holing peer
/// so it can't stall a sender task indefinitely.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Cap on how many characters of a peer's response body we log. Bounds the line
/// length on a verbose/hostile peer while still capturing a Matrix
/// `{errcode,error}` reason, which is small.
const BODY_LOG_LIMIT: usize = 1024;

/// Errors the outbound client can surface to a caller (the sender loop,
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
    #[error("could not build federation URL")]
    InvalidUrl,
}

/// reqwest-backed client for outbound federation requests.
pub(crate) struct FederationClient {
    http: Client,
    /// This homeserver's own name, sent as the transaction `origin`.
    origin: String,
}

/// Install rustls' ring crypto provider as the process default, once.
///
/// reqwest's TLS backend is unified to rustls with NO default crypto provider
/// (iroh, via `cargo test --workspace` feature unification), so building a
/// `reqwest::Client` panics ("No rustls crypto provider is configured") unless a
/// provider is installed first. `neutrino-lb` and `neutrino-ffi` install the same
/// one; this lib carries its own since neutrino-lb is only a dev-dependency here.
/// Idempotent (`install_default` is a no-op if one is already set); the `Once`
/// keeps repeat calls from every `FederationClient::new` cheap.
fn install_crypto_provider() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl FederationClient {
    pub(crate) fn new(origin: String, proxy: Option<&str>) -> Self {
        // Trusted mesh resolves peers to raw IP:port. Without a proxy we bypass
        // any ambient HTTP proxy (which would otherwise intercept `http://{ip}`
        // traffic). With one (the `neutrino-lb` egress) we route all outbound
        // federation through it so it can transcode bodies to CBOR.
        // reqwest is on a no-provider rustls backend (iroh, via workspace feature
        // unification); install the crypto provider before building any client.
        install_crypto_provider();
        let mut builder = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);
        builder = match proxy {
            Some(url) => match reqwest::Proxy::all(url) {
                Ok(p) => builder.proxy(p),
                // `federation_proxy` is validated at startup (`AppState::new`
                // returns `StartupError::InvalidFederationProxy`). Reaching this
                // arm means the config was constructed past that check, which is
                // a programming bug — fail loud rather than silently go direct.
                Err(e) => unreachable!(
                    "federation_proxy {url:?} unparseable after startup validation: {e}"
                ),
            },
            None => builder.no_proxy(),
        };
        // `build()` only fails on TLS-backend init; this is a plaintext client
        // (no TLS), so it can't fail. Panic loud rather than fall back to a
        // default `Client::new()` that silently drops the timeouts and the
        // proxy/`no_proxy` config above — consistent with the `unreachable!()`
        // for a bad proxy URL just above, not a silent degrade beside it.
        let http = builder
            .build()
            .expect("plaintext reqwest client always builds; no TLS backend to init");
        Self { http, origin }
    }

    /// The `Authorization: X-Matrix origin="…",destination="…"` header value for
    /// an outbound request to `dest`. No `key`/`sig`: we have no signing key, so
    /// this is a network-attested identity, not a signature (see
    /// [`crate::federation::auth`]). Server names contain no `"`/`,`, so the
    /// values need no escaping.
    fn x_matrix(&self, dest: &ServerName) -> String {
        format!(
            "X-Matrix origin=\"{}\",destination=\"{}\"",
            self.origin, dest
        )
    }

    /// `PUT http://{dest}/_matrix/federation/v1/send/{txn_id}` carrying `pdus`
    /// (and an empty `edus` list — EDUs are out of scope) plus our
    /// `forward_extremities` advertisement. The per-PDU result map in the response
    /// is ignored (the spec marks `error` advisory, and our durable retry lives in
    /// the outbox), but the response's `forward_extremities` (the peer's heads) is
    /// returned so the sender can reconcile against them. A response that omits or
    /// malforms that field yields an empty map — a 2xx is still a successful
    /// delivery regardless of whether the peer implements reconciliation.
    pub(crate) async fn send_transaction(
        &self,
        dest: &ServerName,
        txn_id: &str,
        pdus: &[Box<RawJsonValue>],
        forward_extremities: &BTreeMap<OwnedRoomId, ForwardExtremities>,
    ) -> Result<BTreeMap<OwnedRoomId, ForwardExtremities>, FederationClientError> {
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
            forward_extremities,
        };
        let resp = self
            .http
            .put(&url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "PUT /send").await);
        }
        // Anti-entropy: read the peer's advertised heads. A parse failure (legacy
        // peer, or a body without the field) is NOT a delivery failure — the 2xx
        // already committed acceptance — so degrade to an empty advertisement.
        Ok(resp
            .json::<TransactionResponse>()
            .await
            .map(|r| r.forward_extremities)
            .unwrap_or_default())
    }

    /// `POST http://{dest}/_matrix/federation/v1/get_missing_events/{room_id}`
    /// to fetch ancestry between `earliest` (boundary already held) and
    /// `latest` (heads to walk back from), up to `limit` events. Returns the
    /// peer's `events` array (oldest-first), opaque PDU bytes.
    ///
    /// `state_dag` (MSC4242) asks the peer to walk back via `prev_state_events`
    /// rather than `prev_events`; the gap-fill fetcher sets it `true` to close
    /// a received PDU's missing *state* ancestry. `include_latest_events`
    /// (anti-entropy) asks the peer to also return the `latest` heads themselves.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn get_missing_events(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        latest: &[OwnedEventId],
        earliest: &[OwnedEventId],
        limit: u32,
        state_dag: bool,
        include_latest_events: bool,
    ) -> Result<Vec<Box<RawJsonValue>>, FederationClientError> {
        // `room_id` goes in a path segment. ruma's `RoomId` localpart is not
        // URL-validated (it may contain `/`, `?`, `#`), so push it through
        // `Url` rather than `format!` to percent-encode it. v12 room ids are
        // url-safe-base64 in practice, but don't rely on that here.
        // No trailing slash on the base: `path_segments_mut().push()` appends a
        // segment, so a trailing slash would yield an empty segment + double
        // slash (`…/get_missing_events//{room}`).
        info!(target: "neutrino_http", %dest, %room_id, limit, state_dag, include_latest_events, "outbound POST /_matrix/federation/v1/get_missing_events");
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
            state_dag,
            include_latest_events,
        };
        let resp = self
            .http
            .post(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "POST /get_missing_events").await);
        }
        Ok(
            parse_2xx::<get_missing_events::ResponseBody>(resp, dest, "POST /get_missing_events")
                .await?
                .events,
        )
    }

    /// `GET http://{dest}/_matrix/federation/v1/make_join/{room}/{user}?ver={ver}`
    /// — request a membership-event template from the resident server (the
    /// first half of the join handshake). Returns the template + the room's
    /// version. We send a single `ver` (the only version we support).
    pub(crate) async fn make_join(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        user_id: &UserId,
        ver: &str,
    ) -> Result<MakeJoinResponse, FederationClientError> {
        info!(target: "neutrino_http", %dest, %room_id, %user_id, "outbound GET /_matrix/federation/v1/make_join");
        let mut url =
            reqwest::Url::parse(&format!("http://{dest}/_matrix/federation/v1/make_join"))
                .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str())
            .push(user_id.as_str());
        url.query_pairs_mut().append_pair("ver", ver);

        let resp = self
            .http
            .get(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "GET /make_join").await);
        }
        parse_2xx::<MakeJoinResponse>(resp, dest, "GET /make_join").await
    }

    /// `PUT http://{dest}/_matrix/federation/v2/send_join/{room}/{event_id}`
    /// carrying the completed membership `event` — the second half of the join
    /// handshake. Returns the MSC4242 `{ state_dag, timeline, event }` response.
    pub(crate) async fn send_join(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        event: &RawJsonValue,
    ) -> Result<SendJoinResponse, FederationClientError> {
        info!(target: "neutrino_http", %dest, %room_id, %event_id, "outbound PUT /_matrix/federation/v2/send_join");
        let mut url =
            reqwest::Url::parse(&format!("http://{dest}/_matrix/federation/v2/send_join"))
                .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str())
            .push(event_id.as_str());

        let resp = self
            .http
            .put(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .json(&event)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "PUT /send_join").await);
        }
        parse_2xx::<SendJoinResponse>(resp, dest, "PUT /send_join").await
    }

    /// `PUT http://{dest}/_matrix/federation/v2/invite/{room}/{event_id}`
    /// carrying the **v2 request envelope** `{ event, room_version,
    /// invite_room_state }` (the v2 endpoint wraps the PDU; v1's bare event is
    /// not used). `invite_room_state` is the stripped state for the invitee to
    /// render the room. Returns the peer's copy of the event (`{ event }`) — in
    /// a signatures world this is where the invitee server's signature is added.
    pub(crate) async fn invite(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        event: &RawJsonValue,
        room_version: &str,
        invite_room_state: &[Value],
    ) -> Result<InviteResponse, FederationClientError> {
        info!(target: "neutrino_http", %dest, %room_id, %event_id, "outbound PUT /_matrix/federation/v2/invite");
        let mut url = reqwest::Url::parse(&format!("http://{dest}/_matrix/federation/v2/invite"))
            .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str())
            .push(event_id.as_str());

        let body = InviteRequest {
            event,
            room_version,
            invite_room_state,
        };
        let resp = self
            .http
            .put(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "PUT /invite").await);
        }
        parse_2xx::<InviteResponse>(resp, dest, "PUT /invite").await
    }

    /// `GET http://{dest}/_matrix/federation/v1/make_leave/{room}/{user}?ver={ver}`
    /// — request a leave/rejection template from the resident (the first half of
    /// the leave handshake; used by us to reject an invite). Returns the template
    /// and the room's version. We send our `ver` for completeness; a spec-
    /// compliant resident is lenient on leave (a user must always be able to
    /// depart a room it is in) and won't gate on it.
    pub(crate) async fn make_leave(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        user_id: &UserId,
        ver: &str,
    ) -> Result<MakeLeaveResponse, FederationClientError> {
        info!(target: "neutrino_http", %dest, %room_id, %user_id, "outbound GET /_matrix/federation/v1/make_leave");
        let mut url =
            reqwest::Url::parse(&format!("http://{dest}/_matrix/federation/v1/make_leave"))
                .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str())
            .push(user_id.as_str());
        url.query_pairs_mut().append_pair("ver", ver);

        let resp = self
            .http
            .get(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "GET /make_leave").await);
        }
        parse_2xx::<MakeLeaveResponse>(resp, dest, "GET /make_leave").await
    }

    /// `PUT http://{dest}/_matrix/federation/v2/send_leave/{room}/{event_id}`
    /// carrying the completed leave `event` — the second half of the leave
    /// handshake. The v2 response is an empty object and carries no state, so it
    /// is ignored: `Ok(())` on any 2xx.
    pub(crate) async fn send_leave(
        &self,
        dest: &ServerName,
        room_id: &RoomId,
        event_id: &EventId,
        event: &RawJsonValue,
    ) -> Result<(), FederationClientError> {
        info!(target: "neutrino_http", %dest, %room_id, %event_id, "outbound PUT /_matrix/federation/v2/send_leave");
        let mut url =
            reqwest::Url::parse(&format!("http://{dest}/_matrix/federation/v2/send_leave"))
                .map_err(|_| FederationClientError::InvalidUrl)?;
        url.path_segments_mut()
            .map_err(|()| FederationClientError::InvalidUrl)?
            .push(room_id.as_str())
            .push(event_id.as_str());

        let resp = self
            .http
            .put(url)
            .header(reqwest::header::AUTHORIZATION, self.x_matrix(dest))
            .json(&event)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(non_2xx_error(resp, dest, "PUT /send_leave").await);
        }
        Ok(())
    }
}

/// Drain a non-2xx federation response into a [`FederationClientError::Status`],
/// logging the peer's response **body** first. Without this the peer's Matrix
/// `{errcode,error}` (the actual reason it rejected) is discarded and an
/// operator debugging the failure sees only a bare status code. `endpoint` is a
/// short label (e.g. `"PUT /send_join"`) for the log line.
async fn non_2xx_error(
    resp: reqwest::Response,
    dest: &ServerName,
    endpoint: &str,
) -> FederationClientError {
    let status = resp.status().as_u16();
    // `text()` consumes `resp`, which we are discarding anyway. A body-read
    // failure degrades to an empty body (logged as `body=""`) — the status is
    // what matters.
    let body = resp.text().await.unwrap_or_default();
    let body: String = body.chars().take(BODY_LOG_LIMIT).collect();
    warn!(target: "neutrino_http", %dest, endpoint, status, %body, "federation peer returned non-2xx");
    FederationClientError::Status(status)
}

/// Deserialize a 2xx federation response body, logging the parse error on
/// failure. A peer that answers `200` with a malformed/unexpected body otherwise
/// surfaces as an indistinguishable [`FederationClientError::Transport`] with no
/// record of *what* failed to parse; the `{:?}` rendering of the reqwest error
/// carries the underlying serde detail (e.g. a missing field).
async fn parse_2xx<T: DeserializeOwned>(
    resp: reqwest::Response,
    dest: &ServerName,
    endpoint: &str,
) -> Result<T, FederationClientError> {
    resp.json::<T>().await.map_err(|e| {
        warn!(target: "neutrino_http", %dest, endpoint, error = ?e, "federation peer returned an unparseable 2xx body");
        e.into()
    })
}

/// The v2 `/invite` request envelope (mirror of the inbound
/// `invite::InviteRequestBody`): the PDU plus the room version and the stripped
/// `invite_room_state` for the invitee to render the room.
#[derive(Serialize)]
struct InviteRequest<'a> {
    event: &'a RawJsonValue,
    room_version: &'a str,
    invite_room_state: &'a [Value],
}

/// Deserialized `make_join` response (mirror of the inbound
/// `make_join::ResponseBody`). The `event` is the unsigned template.
#[derive(Deserialize)]
pub(crate) struct MakeJoinResponse {
    pub(crate) event: Box<RawJsonValue>,
    pub(crate) room_version: String,
}

/// Deserialized `send_join` (v2) response — MSC4242 shape (mirror of the
/// inbound `send_join::ResponseBody`). `auth_chain` / `state` are never present
/// and never read.
#[derive(Deserialize)]
pub(crate) struct SendJoinResponse {
    #[serde(default)]
    pub(crate) state_dag: Vec<Box<RawJsonValue>>,
    #[serde(default)]
    pub(crate) timeline: Vec<Box<RawJsonValue>>,
    pub(crate) event: Box<RawJsonValue>,
}

/// Deserialized `/invite` (v2) response (mirror of the inbound
/// `invite::ResponseBody`): the invitee server's copy of the event.
#[derive(Deserialize)]
pub(crate) struct InviteResponse {
    pub(crate) event: Box<RawJsonValue>,
}

/// Deserialized `make_leave` response (mirror of the inbound
/// `make_leave::ResponseBody`). The `event` is the unsigned leave template.
/// Structurally identical to [`MakeJoinResponse`], kept distinct per the
/// one-mirror-per-endpoint convention.
#[derive(Deserialize)]
pub(crate) struct MakeLeaveResponse {
    pub(crate) event: Box<RawJsonValue>,
    pub(crate) room_version: String,
}

/// The production [`MissingEventsFetcher`]: a thin adapter that closes a
/// received PDU's missing state ancestry by asking the originating peer via
/// Map the reqwest-backed client error onto the engine's neutral
/// [`TransportError`] at the port boundary: status codes pass through (the
/// sender still distinguishes 4xx from 5xx), everything else collapses to a
/// rendered `Transient` so `reqwest::Error` never escapes into `neutrino-engine`.
impl From<FederationClientError> for TransportError {
    fn from(e: FederationClientError) -> Self {
        match e {
            FederationClientError::Status(code) => TransportError::Status(code),
            other => TransportError::Transient(other.to_string()),
        }
    }
}

/// Outbound-delivery port. Delegates to the inherent
/// [`FederationClient::send_transaction`] (disambiguated by the explicit path,
/// since the trait method shares its name) and maps the error.
#[async_trait::async_trait]
impl FederationTransport for FederationClient {
    async fn send_transaction(
        &self,
        dest: &ServerName,
        txn_id: &str,
        pdus: &[Box<RawJsonValue>],
        forward_extremities: &BTreeMap<OwnedRoomId, ForwardExtremities>,
    ) -> Result<BTreeMap<OwnedRoomId, ForwardExtremities>, TransportError> {
        FederationClient::send_transaction(self, dest, txn_id, pdus, forward_extremities)
            .await
            .map_err(TransportError::from)
    }
}

/// [`FederationClient::get_missing_events`] with MSC4242 `state_dag: true`.
/// Holds its own `FederationClient` (a separate reqwest pool from the sender
/// pool's — see `AppState::from_store`: a second pool is cheap and avoids a
/// derivable `App` field, and inbound-gap-fill origins differ from outbound
/// destinations anyway).
pub(crate) struct ReqwestFetcher {
    client: Arc<FederationClient>,
}

impl ReqwestFetcher {
    pub(crate) fn new(client: Arc<FederationClient>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl MissingEventsFetcher for ReqwestFetcher {
    async fn fetch(
        &self,
        q: MissingEventsQuery<'_>,
    ) -> Result<Vec<Box<RawJsonValue>>, TransportError> {
        self.client
            .get_missing_events(
                q.origin,
                q.room_id,
                q.latest,
                q.earliest,
                q.limit,
                q.state_dag,
                q.include_latest_events,
            )
            .await
            .map_err(TransportError::from)
    }
}

/// Outbound transaction body. Borrows everything — no clones on the send path.
#[derive(Serialize)]
struct TransactionRequest<'a> {
    origin: &'a str,
    origin_server_ts: u64,
    pdus: &'a [Box<RawJsonValue>],
    edus: &'a [Box<RawJsonValue>],
    /// Anti-entropy: our per-room forward extremities. Omitted when empty so a
    /// transaction with nothing to advertise keeps the legacy wire shape.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    forward_extremities: &'a BTreeMap<OwnedRoomId, ForwardExtremities>,
}

/// Deserialized `/send` transaction response. The per-PDU `pdus` verdicts are
/// ignored (advisory); only the anti-entropy `forward_extremities` advertisement
/// is read. `#[serde(default)]` lets a legacy `{ "pdus": {} }` body decode with
/// no heads.
#[derive(Deserialize)]
struct TransactionResponse {
    #[serde(default)]
    forward_extremities: BTreeMap<OwnedRoomId, ForwardExtremities>,
}

/// Outbound `/get_missing_events` request body. Mirrors the inbound
/// `RequestBody` (`get_missing_events.rs`): `min_depth` is omitted (optional,
/// and the peer ignores it). `state_dag` (MSC4242) is always sent by our one
/// caller (the gap-fill fetcher) but is a field rather than hard-coded so the
/// wire shape stays explicit.
#[derive(Serialize)]
struct MissingEventsRequest<'a> {
    earliest_events: &'a [OwnedEventId],
    latest_events: &'a [OwnedEventId],
    limit: u32,
    state_dag: bool,
    /// Anti-entropy: ask the peer to also return the `latest_events` it holds.
    include_latest_events: bool,
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
    use ruma::{OwnedRoomId, event_id, room_id};
    use serde_json::{Value, json};

    use super::*;
    use crate::federation::test_support::{dead_peer, spawn_stub};
    use neutrino_engine::TxnIdGen;

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

        let client = FederationClient::new("local.test".to_owned(), None);
        let pdus = [raw(r#"{"n":1}"#), raw(r#"{"n":2}"#)];
        client
            .send_transaction(&dest, "txn-1", &pdus, &BTreeMap::new())
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

        let client = FederationClient::new("local.test".to_owned(), None);
        let pdu = raw("{}");
        let err = client
            .send_transaction(&dest, "t", std::slice::from_ref(&pdu), &BTreeMap::new())
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

        let client = FederationClient::new("local.test".to_owned(), None);
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let latest = vec![event_id!("$late:example.org").to_owned()];
        let earliest = vec![event_id!("$early:example.org").to_owned()];

        let events = client
            .get_missing_events(&dest, &room, &latest, &earliest, 5, true, false)
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

        let client = FederationClient::new("local.test".to_owned(), None);
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let err = client
            .get_missing_events(&dest, &room, &[], &[], 10, true, false)
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
        // A port nothing is listening on → connect fails.
        let dest = dead_peer().await;

        let client = FederationClient::new("local.test".to_owned(), None);
        let pdu = raw("{}");
        let err = client
            .send_transaction(&dest, "t", std::slice::from_ref(&pdu), &BTreeMap::new())
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

        let client = FederationClient::new("local.test".to_owned(), None);
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let events = client
            .get_missing_events(&dest, &room, &[], &[], 10, true, false)
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

        let client = FederationClient::new("local.test".to_owned(), None);
        let room: OwnedRoomId = room_id!("!room:example.org").to_owned();
        let err = client
            .get_missing_events(&dest, &room, &[], &[], 10, true, false)
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
        let fes = BTreeMap::new();
        let txn = TransactionRequest {
            origin: "local.test",
            origin_server_ts: 12345,
            pdus: std::slice::from_ref(&pdu),
            edus: &[],
            forward_extremities: &fes,
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
            state_dag: true,
            include_latest_events: true,
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let _: RequestBody = serde_json::from_value(req_json)
            .expect("inbound /get_missing_events parses the client's body");
    }

    /// The anti-entropy `forward_extremities` advertisement must round-trip
    /// across the two hand-rolled halves: the outbound `TransactionRequest`'s
    /// field has to parse on the inbound `/send` (`send::TransactionBody`), and
    /// the inbound response's `forward_extremities` has to parse back into the
    /// outbound `TransactionResponse`. Catches field-name / shape drift.
    #[test]
    fn forward_extremities_round_trip_through_both_send_halves() {
        use crate::federation::send::TransactionBody;

        let room: OwnedRoomId = room_id!("!r:example.org").to_owned();
        let mut fes = BTreeMap::new();
        fes.insert(
            room.clone(),
            ForwardExtremities {
                timeline: vec![event_id!("$t:example.org").to_owned()],
                state: vec![event_id!("$s:example.org").to_owned()],
            },
        );

        // Request half: outbound body parses on the inbound handler.
        let pdu = raw(r#"{"type":"m.room.message"}"#);
        let txn = TransactionRequest {
            origin: "local.test",
            origin_server_ts: 1,
            pdus: std::slice::from_ref(&pdu),
            edus: &[],
            forward_extremities: &fes,
        };
        let txn_json = serde_json::to_value(&txn).unwrap();
        assert_eq!(
            txn_json["forward_extremities"][room.as_str()]["state"][0],
            "$s:example.org"
        );
        let _: TransactionBody = serde_json::from_value(txn_json)
            .expect("inbound /send parses the client's forward_extremities");

        // Response half: an inbound-shaped response parses back into the client.
        let resp_json = json!({
            "pdus": {},
            "forward_extremities": {
                room.as_str(): { "timeline": ["$t:example.org"], "state": ["$s:example.org"] }
            }
        });
        let resp: TransactionResponse = serde_json::from_value(resp_json)
            .expect("client parses the inbound /send response forward_extremities");
        assert_eq!(
            resp.forward_extremities[&room].state[0],
            event_id!("$s:example.org")
        );

        // A legacy response (no field) decodes to an empty advertisement.
        let legacy: TransactionResponse =
            serde_json::from_value(json!({ "pdus": {} })).expect("legacy body parses");
        assert!(legacy.forward_extremities.is_empty());
    }
}
