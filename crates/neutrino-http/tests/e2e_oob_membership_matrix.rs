//! Exhaustive enumeration of out-of-band membership transitions between two
//! real homeservers over real HTTP.
//!
//! Two full `neutrino_http::serve` stacks (A = resident, hosts the room;
//! B = remote) reachable at their own bound addresses, as in `e2e_backfill`.
//! Every S2S call is a genuine HTTP request against A's router carrying an
//! `X-Matrix` header that attests B as the origin; every C-S assertion is a
//! genuine HTTP request against A's or B's client API.
//!
//! For each federation endpoint the matrix varies **one** axis: the prior
//! membership of B's user in A's room — `never` / `invited` / `joined` /
//! `left`. The room is always `public_chat`, so join rules never confound the
//! result; an invite-only room is covered separately where it changes the
//! answer.
//!
//! These tests **document** what neutrino does today. They are deliberately
//! descriptive, not prescriptive: where the observed behaviour diverges from
//! the spec or from Synapse the assertion records the divergence and the
//! comment says so, rather than failing. The two things asserted as genuine
//! invariants throughout are:
//!
//! 1. **no wedge** — after every permutation the room is still usable: A can
//!    still send, and B's user can still reach a correct membership.
//! 2. **no 5xx** — a membership transition is either performed or refused with
//!    a 4xx; it never faults.
//!
//! Reference points used for the "should this 4xx?" column:
//! `https://spec.matrix.org/unstable/server-server-api/`, and where the spec is
//! silent, Synapse at /workspace/synapse (`handlers/federation.py`,
//! `federation/federation_server.py`).
//!
//! # Observed behaviour
//!
//! Every cell below is produced by the tests in this file; run with
//! `--nocapture` to print the tables. `prior` is the membership of B's user in
//! A's room before the call.
//!
//! ## Resident-side endpoints (called on A, the server hosting the room)
//!
//! ```text
//! endpoint      never      invited    joined     left       room
//! make_join     200        200        200        200        public
//! make_join     403        200        200        403        invite-only
//! make_leave    200        200        200        200        public
//! send_join     200→join   200→join   200→join   200→join   public
//! send_leave    403        200→leave  200→leave  403        public
//! ```
//!
//! `/send` carrying a self-authored member event for B's user, delivered to A.
//! "changed" = A's current state moved; a 200 here is only a durable *accept*,
//! so the C-S API is the oracle:
//!
//! ```text
//! kind     never      invited    joined     left
//! invite   no change  no change  no change  no change   (self-invite is illegal)
//! join     →join      →join      no change  →join
//! leave    no change  →leave     →leave     no change
//! ```
//!
//! ## Invitee-side endpoints (called on B, the invited user's server)
//!
//! ```text
//! endpoint          never            invited          joined     left
//! /invite           200, sync=invite 200, sync=invite 200, join   200, sync=invite
//! /send rescission  200, no room     200, sync=leave  200, leave  200, leave
//! ```
//!
//! # Findings
//!
//! **F1 — `make_leave` issues templates `send_leave` will refuse.**
//! 200 in all four states, then 403 from `send_leave` for `never` and `left`
//! (auth rule 5.5.1 permits a self-leave only from invite/join/knock). Not a
//! wedge; a documented deliberate choice; a divergence from Synapse, which
//! 403s at `make_leave`. Its practical cost is a membership-ungated disclosure
//! of room existence, version and current DAG heads.
//! Pinned by `make_leave_hands_out_a_template_send_leave_will_refuse`.
//!
//! **F2 (open) — the two client surfaces disagree about membership.**
//! `/sync` answers "what is my membership" and sees out-of-band memberships;
//! `GET /rooms/{id}/state/m.room.member/{u}` answers "what is the room's
//! state" and does not. For a pure out-of-band invite the state endpoint 404s
//! (no room DAG); for an invite to a room the server has *left* it returns the
//! stale in-room `leave` while sync correctly says `invite`. Both are
//! defensible under their own definitions — Synapse 403s `/state` for a room
//! you are not in — but a client reading membership through the state endpoint
//! alone gets the wrong answer. Not fixed here.
//! Pinned by `oob_invite_is_visible_to_sync_but_not_to_the_state_endpoint`.
//!
//! **F3 (fixed) — a re-invite after invite → join → leave reaches the client.**
//! Two tables can hold a user's membership for one room and *either* can be the
//! newer fact: the stub is newer when a peer invites a user to a room this
//! server has left, the in-room event is newer when the user joined for real
//! and a stale stub was left behind. The previous rule ("in-room always wins",
//! encoded independently in `candidate_rooms`, `member_event` and `oob_left`)
//! got the first case wrong and silently dropped the invite.
//!
//! The server's presence in the room does not decide it either — a stub
//! written *before* the server joined outlives the join, so once every local
//! user leaves, that dead stub would beat the correct in-room `leave`. Only an
//! explicit order works: `oob_memberships.learned_after` records the `events`
//! stream high-water mark at write time, and the newer row wins (ties favour
//! the stub). The rule lives once, in the store — `resolved_member_event` /
//! `resolved_memberships` — because the two tables are a storage detail and
//! only the store can compare them in one read.
//!
//! Pinned by `invite_to_a_room_the_invitee_left_is_surfaced` (raw S2S) and
//! `csapi_invite_join_leave_reinvite_round_trip` (product path, both presets),
//! with the ordering rule itself covered in `neutrino-store-sqlite`.
//!
//! **No wedges found.** Across all 40 permutations: no 5xx, the resident could
//! always still send into its own room, every refusal left the prior state
//! intact, and a rescinded invite was never left presented as actionable.
#![cfg(not(feature = "multi-user-shim"))]

use std::time::Duration;

use neutrino_ctl::{Command, Config, DiscoveryRegistry};
use neutrino_event::event_builder::EventBuilder;
use neutrino_event::ruma;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

// ===================================================================
// Harness
// ===================================================================

/// A running homeserver reachable by peers at `server_name` (its own bound
/// HTTP address, so the direct `http://{server_name}` resolver reaches it).
struct Node {
    server_name: String,
    http_base: String,
    user: OwnedUserId,
    _cmd_tx: mpsc::UnboundedSender<Command>,
    _tmp: tempfile::TempDir,
}

async fn start_node(localpart: &str) -> Node {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let server_name = http_addr.to_string();

    let tmp = tempfile::TempDir::new().unwrap();
    let config = Config {
        server_name: server_name.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        localpart: localpart.to_string(),
        storage_dir: tmp.path().to_path_buf(),
        federation_proxy: None,
        ..Default::default()
    };

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let spawn_config = config.clone();
    tokio::spawn(async move {
        let store = std::sync::Arc::new(
            neutrino_store_sqlite::SqliteStore::open_in_dir(&spawn_config.storage_dir)
                .await
                .unwrap(),
        );
        let _ = neutrino_http::serve(
            http_listener,
            spawn_config,
            store,
            cmd_rx,
            std::sync::Arc::new(DiscoveryRegistry::new()),
            None,
            neutrino_event::EventPolicy::trusted_network(),
        )
        .await;
    });

    Node {
        user: format!("@{localpart}:{server_name}").parse().unwrap(),
        server_name,
        http_base: format!("http://{http_addr}"),
        _cmd_tx: cmd_tx,
        _tmp: tmp,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

/// `(status, body)` of a C-S API call. Body is `Value::Null` when not JSON.
async fn csapi(
    http: &reqwest::Client,
    method: &str,
    url: String,
    body: Option<&Value>,
) -> (u16, Value) {
    let mut req = match method {
        "GET" => http.get(url),
        "POST" => http.post(url),
        "PUT" => http.put(url),
        m => panic!("unsupported method {m}"),
    };
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req.send().await.expect("csapi request");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// An S2S call against `dest`, attested as coming from `origin`.
async fn s2s(
    http: &reqwest::Client,
    method: &str,
    dest: &Node,
    origin: &Node,
    path: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let url = format!("{}{}", dest.http_base, path);
    let mut req = match method {
        "GET" => http.get(url),
        "PUT" => http.put(url),
        m => panic!("unsupported method {m}"),
    };
    req = req.header(
        reqwest::header::AUTHORIZATION,
        format!(
            r#"X-Matrix origin="{}",destination="{}""#,
            origin.server_name, dest.server_name
        ),
    );
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req.send().await.expect("s2s request");
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

fn errcode(body: &Value) -> Option<String> {
    body.get("errcode")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

// ===================================================================
// Observation helpers — all reads go through the client API
// ===================================================================

/// `content.membership` of `user` in `room` as `node`'s C-S API reports it.
/// `None` = no member event in the room's current state (or 404).
async fn membership(
    http: &reqwest::Client,
    node: &Node,
    room: &str,
    user: &OwnedUserId,
) -> Option<String> {
    let (status, body) = csapi(
        http,
        "GET",
        format!(
            "{}/_matrix/client/v3/rooms/{}/state/m.room.member/{}",
            node.http_base, room, user
        ),
        None,
    )
    .await;
    if status != 200 {
        return None;
    }
    body.get("membership")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// The rooms `node`'s own user sees in each `/sync` bucket, as a
/// `(joined, invited, left)` triple of booleans for `room`. This is the
/// "reflected down the C-S API" check.
async fn sync_buckets(http: &reqwest::Client, node: &Node, room: &str) -> (bool, bool, bool) {
    let (status, body) = csapi(
        http,
        "GET",
        format!("{}/_matrix/client/v3/sync?timeout=0", node.http_base),
        None,
    )
    .await;
    if status != 200 {
        return (false, false, false);
    }
    let has = |bucket: &str| {
        body.pointer(&format!("/rooms/{bucket}"))
            .and_then(Value::as_object)
            .is_some_and(|m| m.contains_key(room))
    };
    (has("join"), has("invite"), has("leave"))
}

/// What `node`'s client API says about `user`'s membership in `room`, taking
/// **both** surfaces into account: the state endpoint (which reads the room's
/// resolved DAG state) and `/sync`'s bucketing (which additionally surfaces
/// out-of-band memberships held as stubs for rooms the server is not in).
///
/// They genuinely disagree: an out-of-band invite has no room DAG to be state
/// in, so `GET /rooms/{id}/state/m.room.member/{user}` 404s while `/sync` puts
/// the room in `invite`. Any check that consults only the state endpoint is
/// blind to exactly the out-of-band cases this file is about.
async fn effective_membership(
    http: &reqwest::Client,
    node: &Node,
    room: &str,
    user: &OwnedUserId,
) -> Option<String> {
    // Sync first: it answers "what is my membership", which is the question
    // here, and it is the surface that sees an out-of-band membership. The
    // state endpoint answers "what is the room's state", which for a room the
    // server has left is a real but stale `leave` — see F2.
    let (joined, invited, left) = sync_buckets(http, node, room).await;
    match (joined, invited, left) {
        (true, _, _) => return Some("join".to_owned()),
        (_, true, _) => return Some("invite".to_owned()),
        (_, _, true) => return Some("leave".to_owned()),
        _ => {}
    }
    membership(http, node, room, user).await
}
/// Wait until `pred` holds over the *effective* membership `node` reports.
async fn settle_effective(
    http: &reqwest::Client,
    node: &Node,
    room: &str,
    user: &OwnedUserId,
    pred: impl Fn(Option<&str>) -> bool,
) -> Option<String> {
    for _ in 0..60 {
        let m = effective_membership(http, node, room, user).await;
        if pred(m.as_deref()) {
            return m;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    effective_membership(http, node, room, user).await
}

/// Wait until `pred` holds over the membership `node` reports, or give up.
/// Federation is asynchronous (stage → worker → persist), so a transition that
/// a 200 promises is not visible synchronously.
async fn settle(
    http: &reqwest::Client,
    node: &Node,
    room: &str,
    user: &OwnedUserId,
    pred: impl Fn(Option<&str>) -> bool,
) -> Option<String> {
    for _ in 0..60 {
        let m = membership(http, node, room, user).await;
        if pred(m.as_deref()) {
            return m;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    membership(http, node, room, user).await
}

// ===================================================================
// The prior-membership axis
// ===================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prior {
    /// No membership event for B's user has ever existed in the room.
    Never,
    /// A invited B's user (a real outbound `/invite` to B).
    Invited,
    /// B's user joined via a real `make_join` + `send_join` handshake.
    Joined,
    /// B's user joined, then left via a real `make_leave` + `send_leave`.
    Left,
}

impl Prior {
    fn all() -> [Prior; 4] {
        [Prior::Never, Prior::Invited, Prior::Joined, Prior::Left]
    }
    fn label(self) -> &'static str {
        match self {
            Prior::Never => "never",
            Prior::Invited => "invited",
            Prior::Joined => "joined",
            Prior::Left => "left",
        }
    }
    /// What A's current state should hold for B's user once established.
    fn expected_membership(self) -> Option<&'static str> {
        match self {
            Prior::Never => None,
            Prior::Invited => Some("invite"),
            Prior::Joined => Some("join"),
            Prior::Left => Some("leave"),
        }
    }
}

/// A fresh room on A, plus the prior membership for B's user established
/// through the real federation paths. Returns the room id.
async fn room_in_state(
    http: &reqwest::Client,
    a: &Node,
    b: &Node,
    prior: Prior,
    preset: &str,
) -> String {
    let (status, body) = csapi(
        http,
        "POST",
        format!("{}/_matrix/client/v3/createRoom", a.http_base),
        Some(&json!({ "preset": preset })),
    )
    .await;
    assert_eq!(status, 200, "createRoom: {body}");
    let room = body["room_id"].as_str().expect("room_id").to_owned();

    match prior {
        Prior::Never => {}
        Prior::Invited => {
            let (s, bd) = csapi(
                http,
                "POST",
                format!("{}/_matrix/client/v3/rooms/{}/invite", a.http_base, room),
                Some(&json!({ "user_id": b.user })),
            )
            .await;
            assert_eq!(s, 200, "setup invite: {bd}");
        }
        Prior::Joined | Prior::Left => {
            let (s, bd) = csapi(
                http,
                "POST",
                format!(
                    "{}/_matrix/client/v3/join/{}?server_name={}",
                    b.http_base, room, a.server_name
                ),
                Some(&json!({})),
            )
            .await;
            assert_eq!(s, 200, "setup join: {bd}");
            if prior == Prior::Left {
                let (s, bd) = csapi(
                    http,
                    "POST",
                    format!("{}/_matrix/client/v3/rooms/{}/leave", b.http_base, room),
                    Some(&json!({})),
                )
                .await;
                assert_eq!(s, 200, "setup leave: {bd}");
            }
        }
    }

    // Setup is only meaningful once A's state actually reflects it.
    let want = prior.expected_membership();
    let got = settle(http, a, &room, &b.user, |m| m == want).await;
    assert_eq!(
        got.as_deref(),
        want,
        "prior state {} not established on A for room {room}",
        prior.label()
    );
    room
}

// ===================================================================
// Building the events the raw S2S endpoints need
// ===================================================================

/// A member event for `user` in `room`, standing on the DAG heads `template`
/// advertises. `template` is a `make_join` / `make_leave` response body: the
/// resident's own statement of the room's current `prev_events` /
/// `prev_state_events` and version. Rebuilt locally (never echoed), which is
/// what a conformant remote does.
fn member_from_template(
    template: &Value,
    sender: &OwnedUserId,
    state_key: &OwnedUserId,
    membership_kind: &str,
) -> neutrino_event::Event {
    let ev = &template["event"];
    let room_id: OwnedRoomId = ev["room_id"]
        .as_str()
        .expect("template room_id")
        .parse()
        .expect("room id");
    let ids = |key: &str| -> Vec<OwnedEventId> {
        ev[key]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .filter_map(|s| s.parse().ok())
                    .collect()
            })
            .unwrap_or_default()
    };
    EventBuilder::new(
        sender.clone(),
        "m.room.member".to_owned(),
        neutrino_event::base_version().clone(),
    )
    .room_id(room_id)
    .state_key(state_key.to_string())
    .content(json!({ "membership": membership_kind }))
    .prev_events(ids("prev_events"))
    .prev_state_events(ids("prev_state_events"))
    .build()
    .expect("build member event")
}

/// `make_leave` is neutrino's universal head oracle: unlike `make_join` it
/// applies no membership pre-check (documented at `federation/make_leave.rs`),
/// so it yields a usable template in every prior state. A Synapse resident
/// would 403 two of the four (see the findings table), which is itself one of
/// the divergences this file records.
async fn heads_template(http: &reqwest::Client, a: &Node, b: &Node, room: &str) -> Value {
    let (status, body) = s2s(
        http,
        "GET",
        a,
        b,
        &format!("/_matrix/federation/v1/make_leave/{}/{}", room, b.user),
        None,
    )
    .await;
    assert_eq!(status, 200, "head oracle (make_leave) failed: {body}");
    body
}

/// The event as wire JSON, for use as a request body.
fn raw(ev: &neutrino_event::Event) -> Value {
    serde_json::from_str(ev.raw.get()).expect("event raw json")
}

/// A `/send` transaction envelope carrying `pdus`.
fn txn(pdus: &[&neutrino_event::Event]) -> Value {
    json!({ "pdus": pdus.iter().map(|e| raw(e)).collect::<Vec<_>>() })
}

/// Establish `prior` for B's user in a fresh room on A, and additionally wait
/// for **B**'s own view to settle — for the matrices that drive B's endpoints,
/// where B's state is the one under test.
async fn room_in_state_on_both(http: &reqwest::Client, a: &Node, b: &Node, prior: Prior) -> String {
    let room = room_in_state(http, a, b, prior, "public_chat").await;
    // B's view: `invite` arrives out of band (B is not in the room), `join` and
    // `leave` are in-room. `never` means B holds nothing at all.
    let want = prior.expected_membership();
    if want.is_some() {
        let got = settle_effective(http, b, &room, &b.user, |m| m == want).await;
        assert_eq!(
            got.as_deref(),
            want,
            "prior state {} not established on B for room {room}",
            prior.label()
        );
    }
    room
}

// ===================================================================
// Matrix 1 — GET /make_join and GET /make_leave
// ===================================================================

#[tokio::test]
async fn matrix_make_join_and_make_leave_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();

    for prior in Prior::all() {
        for op in ["make_join", "make_leave"] {
            let room = room_in_state(&http, &a, &b, prior, "public_chat").await;
            // `ver` is defined for make_join only; omitting it means `["1"]`
            // and a spec-mandated 400 (pinned separately below).
            let query = if op == "make_join" {
                format!("?ver={}", neutrino_event::ROOM_VERSION_ID)
            } else {
                String::new()
            };
            let (status, body) = s2s(
                &http,
                "GET",
                &a,
                &b,
                &format!("/_matrix/federation/v1/{}/{}/{}{}", op, room, b.user, query),
                None,
            )
            .await;

            // A template response must be usable: it carries the room version
            // and an event skeleton for the right user.
            if status == 200 {
                assert_eq!(
                    body["room_version"].as_str(),
                    Some(neutrino_event::ROOM_VERSION_ID),
                    "{op}/{}: template must state the room version",
                    prior.label()
                );
                assert_eq!(
                    body["event"]["state_key"].as_str(),
                    Some(b.user.as_str()),
                    "{op}/{}: template must be for the requested user",
                    prior.label()
                );
                assert_eq!(
                    body["event"]["content"]["membership"].as_str(),
                    Some(if op == "make_join" { "join" } else { "leave" }),
                    "{op}/{}: template membership",
                    prior.label()
                );
            }

            // No wedge: a template request never mutates state.
            let after = membership(&http, &a, &room, &b.user).await;
            assert_eq!(
                after.as_deref(),
                prior.expected_membership(),
                "{op}/{}: a make_* request must not change membership",
                prior.label()
            );
            assert!(status < 500, "{op}/{}: {status} {body}", prior.label());

            rows.push(format!(
                "| {:<9} | {:<10} | {:<3} | {:<18} |",
                op,
                prior.label(),
                status,
                errcode(&body).unwrap_or_else(|| "-".into())
            ));
        }
    }

    eprintln!("\n=== make_join / make_leave × prior membership (public room) ===");
    eprintln!("| op        | prior      | st  | errcode            |");
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Matrix 2 — GET /make_join on an invite-only room
// ===================================================================

#[tokio::test]
async fn matrix_make_join_on_invite_only_room_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();
    for prior in Prior::all() {
        // `private_chat` = join_rules invite. A `joined`/`left` prior needs a
        // join first, which an invite-only room refuses without an invite, so
        // those two are established by inviting and then joining.
        let room = if matches!(prior, Prior::Joined | Prior::Left) {
            let room = room_in_state(&http, &a, &b, Prior::Invited, "private_chat").await;
            let (s, bd) = csapi(
                &http,
                "POST",
                format!("{}/_matrix/client/v3/rooms/{}/join", b.http_base, room),
                Some(&json!({})),
            )
            .await;
            assert_eq!(s, 200, "accept invite: {bd}");
            settle(&http, &a, &room, &b.user, |m| m == Some("join")).await;
            if prior == Prior::Left {
                let (s, bd) = csapi(
                    &http,
                    "POST",
                    format!("{}/_matrix/client/v3/rooms/{}/leave", b.http_base, room),
                    Some(&json!({})),
                )
                .await;
                assert_eq!(s, 200, "leave: {bd}");
                settle(&http, &a, &room, &b.user, |m| m == Some("leave")).await;
            }
            room
        } else {
            room_in_state(&http, &a, &b, prior, "private_chat").await
        };

        let (status, body) = s2s(
            &http,
            "GET",
            &a,
            &b,
            &format!(
                "/_matrix/federation/v1/make_join/{}/{}?ver={}",
                room,
                b.user,
                neutrino_event::ROOM_VERSION_ID
            ),
            None,
        )
        .await;
        assert!(status < 500, "make_join/{}: {status} {body}", prior.label());
        rows.push(format!(
            "| {:<10} | {:<3} | {:<18} |",
            prior.label(),
            status,
            errcode(&body).unwrap_or_else(|| "-".into())
        ));
    }

    eprintln!("\n=== make_join × prior membership (INVITE-ONLY room) ===");
    eprintln!("| prior      | st  | errcode            |");
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Matrix 3 — PUT /send_join and PUT /send_leave
// ===================================================================

#[tokio::test]
async fn matrix_send_join_and_send_leave_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();

    for prior in Prior::all() {
        for kind in ["join", "leave"] {
            let room = room_in_state(&http, &a, &b, prior, "public_chat").await;
            // A conformant remote completes the resident's template rather than
            // authoring an event on heads it guessed; `make_leave` is the head
            // oracle that answers in every prior state.
            let template = heads_template(&http, &a, &b, &room).await;
            let ev = member_from_template(&template, &b.user, &b.user, kind);
            let endpoint = if kind == "join" {
                "send_join"
            } else {
                "send_leave"
            };
            let (status, body) = s2s(
                &http,
                "PUT",
                &a,
                &b,
                &format!(
                    "/_matrix/federation/v2/{}/{}/{}",
                    endpoint, room, ev.event_id
                ),
                Some(&raw(&ev)),
            )
            .await;
            assert!(
                status < 500,
                "{endpoint}/{}: {status} {body}",
                prior.label()
            );

            // Where the endpoint accepted, the transition must be reflected
            // down A's client API; where it refused, the prior state stands.
            let want = if status == 200 {
                Some(kind)
            } else {
                prior.expected_membership()
            };
            let seen = settle(&http, &a, &room, &b.user, |m| m == want).await;

            // No wedge: A can still send into its own room afterwards.
            let (send_status, send_body) = csapi(
                &http,
                "PUT",
                format!(
                    "{}/_matrix/client/v3/rooms/{}/send/m.room.message/probe-{}-{}",
                    a.http_base,
                    room,
                    endpoint,
                    prior.label()
                ),
                Some(&json!({ "msgtype": "m.text", "body": "probe" })),
            )
            .await;
            assert_eq!(
                send_status,
                200,
                "{endpoint}/{}: room wedged for its own resident: {send_body}",
                prior.label()
            );

            rows.push(format!(
                "| {:<10} | {:<9} | {:<3} | {:<22} | {:<8} -> {:<8} |",
                endpoint,
                prior.label(),
                status,
                errcode(&body).unwrap_or_else(|| "-".into()),
                prior.expected_membership().unwrap_or("none"),
                seen.as_deref().unwrap_or("none"),
            ));
        }
    }

    eprintln!("\n=== send_join / send_leave × prior membership (public room) ===");
    eprintln!("| endpoint   | prior     | st  | errcode                | A's CS-API view    |");
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Matrix 4 — the same join / leave delivered via PUT /send instead
// ===================================================================

#[tokio::test]
async fn matrix_send_transaction_membership_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();

    for prior in Prior::all() {
        // `invite` is included even though a self-invite is not a legal
        // transition (rule 5.5 requires sender != state_key for invite) — the
        // point is that an illegal transition must be refused, not wedge.
        for kind in ["invite", "join", "leave"] {
            let room = room_in_state(&http, &a, &b, prior, "public_chat").await;
            let template = heads_template(&http, &a, &b, &room).await;
            let ev = member_from_template(&template, &b.user, &b.user, kind);
            let (status, body) = s2s(
                &http,
                "PUT",
                &a,
                &b,
                &format!("/_matrix/federation/v1/send/txn-{}-{}", kind, prior.label()),
                Some(&txn(&[&ev])),
            )
            .await;
            assert!(
                status < 500,
                "send/{kind}/{}: {status} {body}",
                prior.label()
            );

            // `/send` is stage-then-async: a 200 is a durable *accept*, not an
            // applied transition, so the C-S API is the only honest oracle.
            let settled = settle(&http, &a, &room, &b.user, |m| m == Some(kind)).await;

            let (send_status, _) = csapi(
                &http,
                "PUT",
                format!(
                    "{}/_matrix/client/v3/rooms/{}/send/m.room.message/probe-{}-{}",
                    a.http_base,
                    room,
                    kind,
                    prior.label()
                ),
                Some(&json!({ "msgtype": "m.text", "body": "probe" })),
            )
            .await;
            assert_eq!(
                send_status,
                200,
                "send/{kind}/{}: room wedged for its own resident",
                prior.label()
            );

            // "changed" is the honest question: `settled == kind` is a false
            // positive whenever the prior state already equalled `kind`.
            rows.push(format!(
                "| {:<6} | {:<9} | {:<3} | {:<8} -> {:<8} | changed={:<5} |",
                kind,
                prior.label(),
                status,
                prior.expected_membership().unwrap_or("none"),
                settled.as_deref().unwrap_or("none"),
                settled.as_deref() != prior.expected_membership(),
            ));
        }
    }

    eprintln!("\n=== /send self-authored member event × prior membership (to the resident) ===");
    eprintln!("| kind   | prior     | st  | A's CS-API view    | changed       |");
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Matrix 5 — PUT /invite against the *invitee's* server
// ===================================================================
//
// The other direction: A invites B's user, so the endpoint under test is on B.
// This is the out-of-band path — for `never` and `left` B may hold no room at
// all, so the invite lands in the out-of-band stub table rather than a room
// DAG, and the two dispositions must agree with what B later does with a
// rescission (matrix 6).

#[tokio::test]
async fn matrix_invite_endpoint_against_invitee_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();

    for prior in Prior::all() {
        let room = room_in_state_on_both(&http, &a, &b, prior).await;
        let template = heads_template(&http, &a, &b, &room).await;
        // Authored by A (the inviter), targeting B's user.
        let invite = member_from_template(&template, &a.user, &b.user, "invite");
        let (status, body) = s2s(
            &http,
            "PUT",
            &b,
            &a,
            &format!("/_matrix/federation/v2/invite/{}/{}", room, invite.event_id),
            Some(&json!({
                "event": raw(&invite),
                "room_version": neutrino_event::ROOM_VERSION_ID,
                "invite_room_state": [],
            })),
        )
        .await;
        assert!(status < 500, "invite/{}: {status} {body}", prior.label());

        // A 200 means B accepted and co-signed; the invite must then be
        // visible to B's own client somewhere.
        let settled = settle_effective(&http, &b, &room, &b.user, |m| m == Some("invite")).await;
        let state_view = membership(&http, &b, &room, &b.user).await;
        let (joined, invited, left) = sync_buckets(&http, &b, &room).await;

        // No wedge: B's user can still reach a correct membership afterwards.
        // Accepting is the transition a real client would attempt next.
        let (join_status, _) = csapi(
            &http,
            "POST",
            format!(
                "{}/_matrix/client/v3/join/{}?server_name={}",
                b.http_base, room, a.server_name
            ),
            Some(&json!({})),
        )
        .await;
        let after_join = settle_effective(&http, &b, &room, &b.user, |m| m == Some("join")).await;
        // No wedge: whatever the endpoint decided, B's user can always reach a
        // real membership afterwards.
        assert_eq!(
            after_join.as_deref(),
            Some("join"),
            "invite/{}: B's user could not recover to a joined state",
            prior.label()
        );

        rows.push(format!(
            "| {:<9} | {:<3} | {:<7} | state={:<7} sync={:<7} | j={} i={} l={} | join={} -> {:<6} |",
            prior.label(),
            status,
            errcode(&body).unwrap_or_else(|| "-".into()),
            state_view.as_deref().unwrap_or("none"),
            settled.as_deref().unwrap_or("none"),
            joined as u8,
            invited as u8,
            left as u8,
            join_status,
            after_join.as_deref().unwrap_or("none"),
        ));
    }

    eprintln!("\n=== /invite × prior membership (against the INVITEE server B) ===");
    eprintln!(
        "| prior     | st  | errcode | B's two C-S surfaces        | B sync buckets | recovery          |"
    );
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Matrix 6 — the rescission: an A-authored leave for B's user, via /send
// ===================================================================
//
// The out-of-band rescind path. For `never` / `left` the invite lives in B's
// stub table; for `invited` (in-room) and `joined` it lives in the room DAG.
// Whichever it is, a subsequent leave/kick from A must land somewhere B's
// client can see, and must never strand the invite.

#[tokio::test]
async fn matrix_send_rescission_against_invitee_across_prior_memberships() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let mut rows: Vec<String> = Vec::new();

    for prior in Prior::all() {
        let room = room_in_state_on_both(&http, &a, &b, prior).await;

        // A kicks B's user: authored by A, targeting B's user.
        let template = heads_template(&http, &a, &b, &room).await;
        let kick = member_from_template(&template, &a.user, &b.user, "leave");
        let (status, body) = s2s(
            &http,
            "PUT",
            &b,
            &a,
            &format!("/_matrix/federation/v1/send/rescind-{}", prior.label()),
            Some(&txn(&[&kick])),
        )
        .await;
        assert!(status < 500, "rescind/{}: {status} {body}", prior.label());

        let settled = settle_effective(&http, &b, &room, &b.user, |m| m == Some("leave")).await;
        let state_view = membership(&http, &b, &room, &b.user).await;
        let (joined, invited, left) = sync_buckets(&http, &b, &room).await;

        // No wedge: whatever happened, B's client can still act. `/leave` must
        // not hang or lie — it either succeeds or reports a real error, and
        // afterwards B must not still be presented as invited.
        let (leave_status, leave_body) = csapi(
            &http,
            "POST",
            format!("{}/_matrix/client/v3/rooms/{}/leave", b.http_base, room),
            Some(&json!({})),
        )
        .await;
        assert!(
            leave_status < 500,
            "rescind/{}: /leave faulted: {leave_status} {leave_body}",
            prior.label()
        );
        let after_leave = settle_effective(&http, &b, &room, &b.user, |m| {
            m == Some("leave") || m.is_none()
        })
        .await;
        let (_, still_invited, _) = sync_buckets(&http, &b, &room).await;
        // The wedge invariant: an invite that has been rescinded must never
        // still be presented to the user as actionable.
        assert!(
            !still_invited,
            "rescind/{}: B still shows the room as invited after the rescission \
             and a /leave — the invite is stranded",
            prior.label()
        );

        rows.push(format!(
            "| {:<9} | {:<3} | state={:<7} sync={:<7} | j={} i={} l={} | /leave={} -> {:<6} inv={} |",
            prior.label(),
            status,
            state_view.as_deref().unwrap_or("none"),
            settled.as_deref().unwrap_or("none"),
            joined as u8,
            invited as u8,
            left as u8,
            leave_status,
            after_leave.as_deref().unwrap_or("none"),
            still_invited as u8,
        ));
    }

    eprintln!("\n=== /send A-authored leave (rescission) × prior membership (against B) ===");
    eprintln!(
        "| prior     | st  | B's two C-S surfaces        | B sync buckets | recovery via /leave        |"
    );
    for r in &rows {
        eprintln!("{r}");
    }
    eprintln!();
}

// ===================================================================
// Focused pins for behaviours the matrices above depend on
// ===================================================================

/// `make_join` without `?ver=` means the requester supports only room version
/// `"1"` (spec: "Defaults to `["1"]`"), so a v12 room must refuse it. This is
/// the branch every other `make_join` case in this file deliberately avoids by
/// sending `ver`; pinned here so the matrices cannot silently start measuring
/// version negotiation instead of membership.
#[tokio::test]
async fn make_join_without_ver_is_refused_regardless_of_membership() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    for prior in Prior::all() {
        let room = room_in_state(&http, &a, &b, prior, "public_chat").await;
        let (status, body) = s2s(
            &http,
            "GET",
            &a,
            &b,
            &format!("/_matrix/federation/v1/make_join/{}/{}", room, b.user),
            None,
        )
        .await;
        assert_eq!(
            status,
            400,
            "make_join without ver ({}): {body}",
            prior.label()
        );
        assert_eq!(
            errcode(&body).as_deref(),
            Some("M_INCOMPATIBLE_ROOM_VERSION"),
            "make_join without ver ({})",
            prior.label()
        );
    }

    // `make_leave` has no `ver` negotiation at all, so the same bare request
    // succeeds — the asymmetry is deliberate (see `federation/make_leave.rs`).
    let room = room_in_state(&http, &a, &b, Prior::Joined, "public_chat").await;
    let (status, _) = s2s(
        &http,
        "GET",
        &a,
        &b,
        &format!("/_matrix/federation/v1/make_leave/{}/{}", room, b.user),
        None,
    )
    .await;
    assert_eq!(status, 200, "make_leave must not gate on ver");
}

/// FINDING (F1). `make_leave` answers 200 in every prior membership state,
/// including `never` and `left` — but `send_leave` then refuses those two with
/// 403, because auth rule 5.5.1 only permits a self-leave from `invite`,
/// `join` or `knock` (`neutrino-room/src/auth_rules.rs:612-617`).
///
/// This is deliberate and documented (`federation/make_leave.rs:13-16`: "no
/// membership / join-rules eligibility pre-check … `send_leave`'s apply is
/// authoritative"), and it does not wedge: the remote simply fails at step two
/// of the handshake. It IS a divergence from Synapse, whose
/// `on_make_leave_request` runs `check_auth_rules_from_context` on the template
/// it just built and raises `AuthError` → 403 (`handlers/federation.py:1322-
/// 1329`). The spec documents only 400 and 404 for `make_leave` and is silent
/// on membership, so neither is non-conformant.
///
/// The practical consequence is an unauthenticated-by-membership disclosure:
/// any server may learn whether an arbitrary room exists on this one, plus its
/// version and current DAG heads, by asking for a leave template for a user it
/// owns. `make_join` gates the equivalent disclosure behind `check_can_join`.
#[tokio::test]
async fn make_leave_hands_out_a_template_send_leave_will_refuse() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    for prior in [Prior::Never, Prior::Left] {
        let room = room_in_state(&http, &a, &b, prior, "public_chat").await;

        let template = heads_template(&http, &a, &b, &room).await;
        assert_eq!(
            template["event"]["content"]["membership"].as_str(),
            Some("leave"),
            "make_leave answers with a usable template even from {}",
            prior.label()
        );

        let ev = member_from_template(&template, &b.user, &b.user, "leave");
        let (status, body) = s2s(
            &http,
            "PUT",
            &a,
            &b,
            &format!("/_matrix/federation/v2/send_leave/{}/{}", room, ev.event_id),
            Some(&raw(&ev)),
        )
        .await;
        assert_eq!(
            status,
            403,
            "send_leave from {} must refuse the template make_leave issued: {body}",
            prior.label()
        );
        assert_eq!(errcode(&body).as_deref(), Some("M_FORBIDDEN"));

        // Not a wedge: the refusal leaves the prior state untouched.
        assert_eq!(
            membership(&http, &a, &room, &b.user).await.as_deref(),
            prior.expected_membership(),
        );
    }
}

/// FINDING (F2). An out-of-band invite is visible to `/sync` but **not** to
/// `GET /rooms/{roomId}/state/m.room.member/{userId}`: with no room DAG there
/// is no current state to read, so the state endpoint 404s while sync buckets
/// the room under `invite`. A client that reads membership through the state
/// endpoint alone cannot see an invite it has been sent.
#[tokio::test]
async fn oob_invite_is_visible_to_sync_but_not_to_the_state_endpoint() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let room = room_in_state(&http, &a, &b, Prior::Invited, "public_chat").await;
    settle_effective(&http, &b, &room, &b.user, |m| m == Some("invite")).await;

    let (_, invited, _) = sync_buckets(&http, &b, &room).await;
    assert!(
        invited,
        "sync must bucket the out-of-band invite under invite"
    );
    assert_eq!(
        membership(&http, &b, &room, &b.user).await,
        None,
        "the state endpoint is blind to an out-of-band invite (no room DAG)"
    );

    // It is a real invite: B's user can accept it.
    let (status, body) = csapi(
        &http,
        "POST",
        format!(
            "{}/_matrix/client/v3/join/{}?server_name={}",
            b.http_base, room, a.server_name
        ),
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 200, "accept the out-of-band invite: {body}");
    assert_eq!(
        settle_effective(&http, &b, &room, &b.user, |m| m == Some("join")).await,
        Some("join".to_owned())
    );
}

/// F3, raw-S2S form: an invite for a room the invitee's server has **left**
/// must reach the invitee. The stale in-room `leave` and the fresh stub are
/// ordered by `oob_memberships.learned_after`, so the newer one — the invite —
/// wins. See `csapi_invite_join_leave_reinvite_round_trip` for the product
/// path and the module docs for the rule.
#[tokio::test]
async fn invite_to_a_room_the_invitee_left_is_surfaced() {
    let a = start_node("alice").await;
    let b = start_node("bob").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let http = client();

    let room = room_in_state_on_both(&http, &a, &b, Prior::Left).await;
    let template = heads_template(&http, &a, &b, &room).await;
    let invite = member_from_template(&template, &a.user, &b.user, "invite");

    let (status, body) = s2s(
        &http,
        "PUT",
        &b,
        &a,
        &format!("/_matrix/federation/v2/invite/{}/{}", room, invite.event_id),
        Some(&json!({
            "event": raw(&invite),
            "room_version": neutrino_event::ROOM_VERSION_ID,
            "invite_room_state": [],
        })),
    )
    .await;
    assert_eq!(status, 200, "the invite is accepted: {body}");

    assert_eq!(
        settle_effective(&http, &b, &room, &b.user, |m| m == Some("invite")).await,
        Some("invite".to_owned()),
        "the invite is newer than the in-room leave, so it must win"
    );
    let (_, invited, _) = sync_buckets(&http, &b, &room).await;
    assert!(
        invited,
        "and it must reach the invitee's sync invite bucket"
    );
}

/// F3, driven entirely through the client API on both sides — the sequence a
/// real user performs: invited → joined → left → re-invited. Nothing here is
/// hand-built; A invites through `POST /rooms/{id}/invite` and B accepts and
/// leaves through its own client API, so this is the product path, not a
/// synthetic S2S call.
#[tokio::test]
async fn csapi_invite_join_leave_reinvite_round_trip() {
    for preset in ["public_chat", "private_chat"] {
        let a = start_node("alice").await;
        let b = start_node("bob").await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let http = client();

        let (status, body) = csapi(
            &http,
            "POST",
            format!("{}/_matrix/client/v3/createRoom", a.http_base),
            Some(&json!({ "preset": preset })),
        )
        .await;
        assert_eq!(status, 200, "createRoom: {body}");
        let room = body["room_id"].as_str().unwrap().to_owned();

        let invite = |http: &reqwest::Client, room: String| {
            let url = format!("{}/_matrix/client/v3/rooms/{}/invite", a.http_base, room);
            let user = b.user.clone();
            let http = http.clone();
            async move { csapi(&http, "POST", url, Some(&json!({ "user_id": user }))).await }
        };

        // 1. invited
        let (s, bd) = invite(&http, room.clone()).await;
        assert_eq!(s, 200, "[{preset}] first invite: {bd}");
        assert_eq!(
            settle_effective(&http, &b, &room, &b.user, |m| m == Some("invite")).await,
            Some("invite".to_owned()),
            "[{preset}] B must see the first invite"
        );

        // 2. joined
        let (s, bd) = csapi(
            &http,
            "POST",
            format!(
                "{}/_matrix/client/v3/join/{}?server_name={}",
                b.http_base, room, a.server_name
            ),
            Some(&json!({})),
        )
        .await;
        assert_eq!(s, 200, "[{preset}] accept invite: {bd}");
        assert_eq!(
            settle_effective(&http, &b, &room, &b.user, |m| m == Some("join")).await,
            Some("join".to_owned()),
            "[{preset}] B must see the join"
        );

        // 3. left
        let (s, bd) = csapi(
            &http,
            "POST",
            format!("{}/_matrix/client/v3/rooms/{}/leave", b.http_base, room),
            Some(&json!({})),
        )
        .await;
        assert_eq!(s, 200, "[{preset}] leave: {bd}");
        assert_eq!(
            settle_effective(&http, &b, &room, &b.user, |m| m == Some("leave")).await,
            Some("leave".to_owned()),
            "[{preset}] B must see the leave"
        );
        // A agrees B has gone.
        assert_eq!(
            settle(&http, &a, &room, &b.user, |m| m == Some("leave")).await,
            Some("leave".to_owned()),
            "[{preset}] A must see B's leave"
        );

        // 4. re-invited — the question.
        let (s, bd) = invite(&http, room.clone()).await;
        assert_eq!(s, 200, "[{preset}] re-invite: {bd}");
        // A definitely re-invited: its own state says so.
        assert_eq!(
            settle(&http, &a, &room, &b.user, |m| m == Some("invite")).await,
            Some("invite".to_owned()),
            "[{preset}] A's state must record the re-invite"
        );

        let seen = settle_effective(&http, &b, &room, &b.user, |m| m == Some("invite")).await;
        let (_, invited_bucket, _) = sync_buckets(&http, &b, &room).await;
        eprintln!(
            "[{preset}] after re-invite: B effective={:?} invite_bucket={} state={:?}",
            seen,
            invited_bucket,
            membership(&http, &b, &room, &b.user).await,
        );
        assert_eq!(
            seen.as_deref(),
            Some("invite"),
            "[{preset}] the re-invite must reach the client"
        );
        assert!(
            invited_bucket,
            "[{preset}] and must be bucketed under invite in /sync"
        );

        // The invite is not lost, only unshown: B's user can *decline* it, and
        // declining runs the federated reject handshake, which moves A's state
        // invite -> leave. A no-op /leave (which is what B would do if it held
        // no stub) could not move A. So this discriminates "the stub exists
        // and sync hides it" from "the invite never arrived".
        let (s, bd) = csapi(
            &http,
            "POST",
            format!("{}/_matrix/client/v3/rooms/{}/leave", b.http_base, room),
            Some(&json!({})),
        )
        .await;
        assert_eq!(s, 200, "[{preset}] decline the unseen re-invite: {bd}");
        assert_eq!(
            settle(&http, &a, &room, &b.user, |m| m == Some("leave")).await,
            Some("leave".to_owned()),
            "[{preset}] declining moved A's state, so B did hold the invite \
             stub — sync hid it rather than the invite going missing"
        );

        // Re-invite once more and confirm the same invisibility, then check the
        // invite is still actionable by joining. On an invite-only room the
        // join only works *because* A's room state holds a valid invite — B
        // is acting on something its own client never showed it.
        let (s, bd) = invite(&http, room.clone()).await;
        assert_eq!(s, 200, "[{preset}] second re-invite: {bd}");
        settle(&http, &a, &room, &b.user, |m| m == Some("invite")).await;
        let (s, bd) = csapi(
            &http,
            "POST",
            format!(
                "{}/_matrix/client/v3/join/{}?server_name={}",
                b.http_base, room, a.server_name
            ),
            Some(&json!({})),
        )
        .await;
        eprintln!("[{preset}] re-join after re-invite: {s} {bd}");
        assert_eq!(s, 200, "[{preset}] B could not act on the re-invite: {bd}");
        assert_eq!(
            settle_effective(&http, &b, &room, &b.user, |m| m == Some("join")).await,
            Some("join".to_owned()),
            "[{preset}] B must end up joined"
        );
    }
}
