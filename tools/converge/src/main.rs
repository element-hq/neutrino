//! converge — seeded, randomized convergence fuzzer over the 3-server rig.
//!
//! The Rust port of the former `testrig/converge.sh`. It drives random episodes
//! of room operations interleaved with random partition cut/heal, heals
//! everything at a barrier, and asserts every joined server reaches a
//! byte-identical resolved `/state` with no locally-accepted message lost.
//!
//! Usage:
//!   converge [SEED]
//!     SEED   u64; reproduces an exact op sequence. Omitted -> a fresh seed is
//!            chosen and printed (and re-printed on failure) so any run replays.
//!   env knobs: EPISODES (6), ROUNDS_PER_EPISODE (12), DEADLINE (90s),
//!              INTERVAL (2s), STABLE_POLLS (2), STRICT_EVENT_PRESENCE (1),
//!              CRASH_PROB (10).
//!
//! ## What it tests
//!
//! The convergence invariant: after every partition is healed and outboxes
//! drain, every server joined to the room agrees on the SAME resolved room
//! state, and no locally-accepted message is lost. It does not predict which
//! concurrent write wins (state-res tie-breaks on wall-clock `origin_server_ts`
//! then `event_id`); it asserts AGREEMENT, which is timing-independent.
//!
//! ## Determinism
//!
//! One [`StdRng`] seeded from `SEED` is the single source of every choice. Unlike
//! the bash original there is no shared-`$RANDOM`-stream hazard: room mutations
//! run over HTTP in-process (no child process burns the stream), so the op
//! sequence is a pure function of the seed.
//!
//! ## Two-tier status policy
//!
//! * EXACT tier — ops issued while fully healed + converged (the shadow model ==
//!   reality). Status is asserted exactly (2xx, or a specific `M_` code). This is
//!   where admin / power-level / membership-validity is checked hard.
//! * RECORD tier — mutating ops during the fuzz phase. The model picks ops it
//!   believes valid, issues them, and records the actual outcome: a 2xx event
//!   joins the must-converge ledger; a non-2xx is a legitimate stale-view
//!   rejection mid-partition, logged not asserted.
//!
//! ## Topology control
//!
//! Partition cut/heal is delegated to `testrig/nctl partition`, which owns the
//! docker-network manipulation (and the `--alias` heal fix). The harness itself
//! sends no SIGUSR2: `nctl`'s heal already resets outbound backoff on both ends
//! of the healed link, so a healed link drains without any explicit per-poll
//! kick from here.
//!
//! ## Crash testing
//!
//! `CRASH_PROB`% of rounds (default 10) instead crash a live server (`nctl
//! crash` => SIGKILL — no graceful shutdown) or revive a crashed one (`nctl
//! revive` => `docker start`). This is distinct from a partition: a cut leaves
//! the process alive (its outbox accrues and retries), a crash kills it
//! outright. The container filesystem — and `/data/neutrino.db` — survives the
//! kill (only `compose down -v` wipes it), so the revived process recovers its
//! committed state and re-arms its outbox sender on startup, redelivering any
//! federation transaction parked before the crash. With WAL + `synchronous =
//! NORMAL`, a SIGKILL preserves committed data via the OS page cache, so this
//! models a process crash, not host power-loss. At least one server is always
//! kept alive; the barrier revives any still-down server before checking
//! convergence. A run that crashes only while idle/converged is flagged at the
//! end (recovery was vacuous) — mirroring the partition divergence guard.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::Method;
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::sleep;

const CS: &str = "_matrix/client/v3";
const SERVERS: [&str; 3] = ["hs1", "hs2", "hs3"];
const LINKS: [&str; 3] = ["12", "13", "23"];

fn port_of(hs: &str) -> u16 {
    match hs {
        "hs1" => 8001,
        "hs2" => 8002,
        "hs3" => 8003,
        _ => 0,
    }
}

fn mxid(hs: &str) -> String {
    format!("@alice:{hs}")
}

/// pair key -> the two server names it separates (`"12"` -> `("hs1","hs2")`).
fn link_servers(link: &str) -> (&'static str, &'static str) {
    match link {
        "12" => ("hs1", "hs2"),
        "13" => ("hs1", "hs3"),
        "23" => ("hs2", "hs3"),
        _ => ("", ""),
    }
}

/// Last `m.room.member` membership for `u` in a resolved-state array (`/state`
/// returns one event per (type,state_key), so "last" == current).
fn membership_of(state: &Value, u: &str) -> String {
    state
        .as_array()
        .into_iter()
        .flatten()
        .rfind(|e| {
            e.get("type").and_then(Value::as_str) == Some("m.room.member")
                && e.get("state_key").and_then(Value::as_str) == Some(u)
        })
        .and_then(|e| e.get("content")?.get("membership")?.as_str())
        .unwrap_or("leave")
        .to_string()
}

/// `u`'s effective power level in a resolved-state array (their explicit level,
/// else `users_default`).
fn power_in_state(state: &Value, u: &str) -> i64 {
    let pl = state
        .as_array()
        .into_iter()
        .flatten()
        .rfind(|e| e.get("type").and_then(Value::as_str) == Some("m.room.power_levels"))
        .and_then(|e| e.get("content"));
    let def = pl
        .and_then(|c| c.get("users_default"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    pl.and_then(|c| c.get("users"))
        .and_then(|us| us.get(u))
        .and_then(Value::as_i64)
        .unwrap_or(def)
}

/// Outcome of a single mutating request.
struct Outcome {
    ok: bool,
    status: u16,
    errcode: Option<String>,
    event_id: Option<String>,
}

/// A concrete room mutation. Built fresh each time it is issued so `m.room.name`
/// / message bodies carry the right monotonic tag.
#[derive(Clone)]
enum Op {
    Join {
        hs: String,
        resident: String,
    },
    Msg {
        hs: String,
        text: String,
    },
    Name {
        hs: String,
        text: String,
    },
    Power {
        hs: String,
        target: String,
        level: i64,
    },
    Invite {
        hs: String,
        target: String,
    },
    Kick {
        hs: String,
        target: String,
    },
    Leave {
        hs: String,
    },
}

impl Op {
    fn label(&self) -> String {
        match self {
            Op::Join { hs, resident } => format!("{hs} join via {resident}"),
            Op::Msg { hs, text } => format!("{hs} msg {text}"),
            Op::Name { hs, text } => format!("{hs} name {text}"),
            Op::Power { hs, target, level } => format!("{hs} power {target} {level}"),
            Op::Invite { hs, target } => format!("{hs} invite {target}"),
            Op::Kick { hs, target } => format!("{hs} kick {target}"),
            Op::Leave { hs } => format!("{hs} leave"),
        }
    }
}

/// A model-valid candidate op (no tag/level assigned yet — those are drawn only
/// for the chosen candidate so the PRNG stream matches one decision per round).
#[derive(Clone)]
enum Cand {
    Msg(String),
    Name(String),
    Power(String, String),
    Invite(String, String),
    Kick(String, String),
    Leave(String),
    Join(String),
}

struct Config {
    episodes: u32,
    rounds_per_episode: u32,
    deadline: Duration,
    interval: Duration,
    stable_polls: u32,
    strict_event_presence: bool,
    /// Per-round probability (percent) of a crash/revive action. Clamped so the
    /// fixed topology (30%) + conflict (15%) bands plus this still leave room for
    /// a mutation, i.e. `crash_prob <= 55`.
    crash_prob: u32,
}

/// Best-effort `docker compose down -v` on drop, so a panic still tears the rig
/// down. The normal paths also call [`Harness::cleanup`] explicitly (it runs
/// before any `process::exit`, which would skip this guard).
struct ComposeGuard {
    compose_file: PathBuf,
}

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&self.compose_file)
            .args(["down", "-v"])
            .output();
    }
}

struct Harness {
    cfg: Config,
    seed: u64,
    rng: StdRng,
    client: reqwest::Client,
    rig_dir: PathBuf,
    room: String,
    txn_ctr: u64,
    seq: u64,

    // shadow room model (re-synced from reality at every barrier)
    member: HashMap<String, String>,
    pl: HashMap<String, i64>,
    pl_users_default: i64,
    pl_state_default: i64,
    pl_events_default: i64,
    pl_invite: i64,
    pl_kick: i64,
    ev_name: Option<i64>,
    ev_pl: Option<i64>,

    // topology: true = link up
    up: HashMap<String, bool>,
    // process liveness: true = server process running (false between crash/revive)
    up_proc: HashMap<String, bool>,
    // message event_id -> servers required to hold it (joined at send time)
    msg_accepted: HashMap<String, Vec<String>>,

    op_log: Vec<String>,
    divergence_seen: bool,
    conflict_fired: bool,
    any_cut: bool,
    crash_fired: bool,
    // a crash landed while there was recoverable work in flight (writes recorded
    // since the last barrier) — i.e. the restart had something non-trivial to
    // recover/redeliver, so convergence-after-crash wasn't vacuous.
    crash_bit: bool,
    // 2xx mutations recorded since the last barrier; gates the crash-bit signal.
    writes_since_barrier: u32,
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Harness {
    fn new() -> Self {
        let seed = std::env::args()
            .nth(1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(rand::random::<u64>);
        let cfg = Config {
            episodes: env_u32("EPISODES", 6),
            rounds_per_episode: env_u32("ROUNDS_PER_EPISODE", 12),
            deadline: Duration::from_secs(u64::from(env_u32("DEADLINE", 90))),
            interval: Duration::from_secs(u64::from(env_u32("INTERVAL", 2))),
            stable_polls: env_u32("STABLE_POLLS", 2),
            strict_event_presence: env_u32("STRICT_EVENT_PRESENCE", 1) == 1,
            crash_prob: env_u32("CRASH_PROB", 10).min(55),
        };
        let rig_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testrig");
        Harness {
            cfg,
            seed,
            rng: StdRng::seed_from_u64(seed),
            client: reqwest::Client::new(),
            rig_dir,
            room: String::new(),
            txn_ctr: 0,
            seq: 0,
            member: SERVERS
                .iter()
                .map(|s| (s.to_string(), "leave".into()))
                .collect(),
            pl: SERVERS.iter().map(|s| (s.to_string(), 0)).collect(),
            pl_users_default: 0,
            pl_state_default: 50,
            pl_events_default: 0,
            pl_invite: 0,
            pl_kick: 50,
            ev_name: None,
            ev_pl: None,
            up: LINKS.iter().map(|k| (k.to_string(), true)).collect(),
            up_proc: SERVERS.iter().map(|s| (s.to_string(), true)).collect(),
            msg_accepted: HashMap::new(),
            op_log: Vec::new(),
            divergence_seen: false,
            conflict_fired: false,
            any_cut: false,
            crash_fired: false,
            crash_bit: false,
            writes_since_barrier: 0,
        }
    }

    // ---- output helpers -----------------------------------------------------

    fn log(&self, m: &str) {
        eprintln!("\n=== {m} ===");
    }
    fn note(&self, m: &str) {
        eprintln!("  · {m}");
    }
    fn oplog(&mut self, m: &str) {
        self.op_log.push(m.to_string());
        self.note(m);
    }

    // ---- model accessors ----------------------------------------------------

    fn power_of(&self, hs: &str) -> i64 {
        self.pl.get(hs).copied().unwrap_or(self.pl_users_default)
    }
    fn memb(&self, hs: &str) -> &str {
        self.member.get(hs).map(String::as_str).unwrap_or("leave")
    }
    fn is_up(&self, link: &str) -> bool {
        self.up.get(link).copied().unwrap_or(false)
    }
    fn is_proc_up(&self, hs: &str) -> bool {
        self.up_proc.get(hs).copied().unwrap_or(false)
    }
    /// Power required to send `event_type` as state.
    fn state_req(&self, event_type: &str) -> i64 {
        match event_type {
            "m.room.name" => self.ev_name.unwrap_or(self.pl_state_default),
            "m.room.power_levels" => self.ev_pl.unwrap_or(self.pl_state_default),
            _ => self.pl_state_default,
        }
    }

    // ---- low-level HTTP -----------------------------------------------------

    /// Issue a request; returns `(status, body)` with `(0, Null)` on transport
    /// error (so read polls tolerate a partitioned/booting peer).
    async fn http(
        &self,
        method: Method,
        hs: &str,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let url = format!("http://localhost:{}/{}", port_of(hs), path);
        let mut rb = self.client.request(method, &url);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        match rb.send().await {
            Ok(resp) => {
                let st = resp.status().as_u16();
                let txt = resp.text().await.unwrap_or_default();
                (st, serde_json::from_str(&txt).unwrap_or(Value::Null))
            }
            Err(_) => (0, Value::Null),
        }
    }

    async fn get(&self, hs: &str, path: &str) -> (u16, Value) {
        self.http(Method::GET, hs, path, None).await
    }

    async fn mutate(&self, hs: &str, method: Method, path: &str, body: Option<Value>) -> Outcome {
        let (st, val) = self.http(method, hs, path, body).await;
        Outcome {
            ok: (200..300).contains(&st),
            status: st,
            errcode: val.get("errcode").and_then(Value::as_str).map(String::from),
            event_id: val
                .get("event_id")
                .and_then(Value::as_str)
                .map(String::from),
        }
    }

    async fn issue(&mut self, op: &Op) -> Outcome {
        match op {
            Op::Join { hs, resident } => {
                let p = format!("{CS}/join/{}?server_name={resident}", self.room);
                self.mutate(hs, Method::POST, &p, Some(json!({}))).await
            }
            Op::Leave { hs } => {
                let p = format!("{CS}/rooms/{}/leave", self.room);
                self.mutate(hs, Method::POST, &p, Some(json!({}))).await
            }
            Op::Msg { hs, text } => {
                self.txn_ctr += 1;
                let p = format!(
                    "{CS}/rooms/{}/send/m.room.message/txn{}",
                    self.room, self.txn_ctr
                );
                self.mutate(
                    hs,
                    Method::PUT,
                    &p,
                    Some(json!({"msgtype":"m.text","body":text})),
                )
                .await
            }
            Op::Name { hs, text } => {
                let p = format!("{CS}/rooms/{}/state/m.room.name", self.room);
                self.mutate(hs, Method::PUT, &p, Some(json!({ "name": text })))
                    .await
            }
            Op::Invite { hs, target } => {
                let p = format!("{CS}/rooms/{}/invite", self.room);
                self.mutate(
                    hs,
                    Method::POST,
                    &p,
                    Some(json!({ "user_id": mxid(target) })),
                )
                .await
            }
            Op::Kick { hs, target } => {
                let p = format!("{CS}/rooms/{}/kick", self.room);
                self.mutate(
                    hs,
                    Method::POST,
                    &p,
                    Some(json!({"user_id": mxid(target), "reason": "kick"})),
                )
                .await
            }
            Op::Power { hs, target, level } => self.issue_power(hs, target, *level).await,
        }
    }

    /// `power` is a read-modify-write: PUT replaces the whole `m.room.power_levels`
    /// content, so merge the new user level into the current content.
    async fn issue_power(&self, hs: &str, target: &str, level: i64) -> Outcome {
        let p = format!("{CS}/rooms/{}/state/m.room.power_levels", self.room);
        let (st, mut content) = self.get(hs, &p).await;
        if !(200..300).contains(&st) {
            return Outcome {
                ok: false,
                status: st,
                errcode: None,
                event_id: None,
            };
        }
        if !content.get("users").map(Value::is_object).unwrap_or(false) {
            content["users"] = json!({});
        }
        content["users"][mxid(target)] = json!(level);
        self.mutate(hs, Method::PUT, &p, Some(content)).await
    }

    async fn create_room(&mut self) -> Result<(), String> {
        let body = json!({"preset":"public_chat","name":"converge lab"});
        let (st, val) = self
            .http(Method::POST, "hs1", &format!("{CS}/createRoom"), Some(body))
            .await;
        if !(200..300).contains(&st) {
            return Err(format!("createRoom failed (status={st} body={val})"));
        }
        let room = val.get("room_id").and_then(Value::as_str).unwrap_or("");
        if !room.starts_with('!') {
            return Err(format!("createRoom returned no room id (got '{room}')"));
        }
        self.room = room.to_string();
        self.note(&format!("room={}", self.room));
        Ok(())
    }

    // ---- model maintenance --------------------------------------------------

    /// Optimistic model update after a 2xx mutation. The barrier re-sync corrects
    /// any partition-time drift.
    fn apply_model(&mut self, op: &Op) {
        match op {
            Op::Power { target, level, .. } => {
                self.pl.insert(target.clone(), *level);
            }
            Op::Invite { target, .. } => {
                self.member.insert(target.clone(), "invite".into());
            }
            Op::Kick { target, .. } => {
                self.member.insert(target.clone(), "leave".into());
            }
            Op::Leave { hs } => {
                self.member.insert(hs.clone(), "leave".into());
            }
            Op::Join { hs, .. } => {
                self.member.insert(hs.clone(), "join".into());
            }
            Op::Msg { .. } | Op::Name { .. } => {}
        }
    }

    /// Ledger a message: only messages are checked via `/messages` (state is
    /// covered by the resolved-state equality check). Audience = servers joined
    /// (per the model) at send time; a later joiner is not expected to backfill.
    fn ledger(&mut self, op: &Op, out: &Outcome) {
        if let Op::Msg { .. } = op
            && let Some(eid) = &out.event_id
        {
            let aud = SERVERS
                .iter()
                .filter(|h| self.memb(h) == "join")
                .map(|h| h.to_string())
                .collect();
            self.msg_accepted.insert(eid.clone(), aud);
        }
    }

    /// Re-derive the whole model from hs1's resolved state. Called at every
    /// barrier, where all servers agree, so any one is ground truth.
    async fn sync_model(&mut self) -> Result<(), String> {
        let (st, val) = self
            .get("hs1", &format!("{CS}/rooms/{}/state", self.room))
            .await;
        if !(200..300).contains(&st) {
            return Err(format!("sync_model: hs1 /state returned {st}"));
        }
        let plc = val
            .as_array()
            .into_iter()
            .flatten()
            .rfind(|e| e.get("type").and_then(Value::as_str) == Some("m.room.power_levels"))
            .and_then(|e| e.get("content"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let geti = |k: &str, d: i64| plc.get(k).and_then(Value::as_i64).unwrap_or(d);
        self.pl_users_default = geti("users_default", 0);
        self.pl_state_default = geti("state_default", 50);
        self.pl_events_default = geti("events_default", 0);
        self.pl_invite = geti("invite", 0);
        self.pl_kick = geti("kick", 50);
        self.ev_name = plc
            .get("events")
            .and_then(|e| e.get("m.room.name"))
            .and_then(Value::as_i64);
        self.ev_pl = plc
            .get("events")
            .and_then(|e| e.get("m.room.power_levels"))
            .and_then(Value::as_i64);
        for hs in SERVERS {
            let u = mxid(hs);
            let p = plc
                .get("users")
                .and_then(|us| us.get(&u))
                .and_then(Value::as_i64)
                .unwrap_or(self.pl_users_default);
            self.pl.insert(hs.to_string(), p);
            self.member.insert(hs.to_string(), membership_of(&val, &u));
        }
        Ok(())
    }

    // ---- tiered mutation drivers --------------------------------------------

    async fn expect_ok(&mut self, op: Op) -> Result<(), String> {
        let out = self.issue(&op).await;
        if !out.ok {
            return Err(format!(
                "exact: expected 2xx for [{}] (status={} err={:?})",
                op.label(),
                out.status,
                out.errcode
            ));
        }
        self.ledger(&op, &out);
        self.apply_model(&op);
        let evid = out.event_id.as_deref().unwrap_or("–");
        self.oplog(&format!("EXACT ok    [{}] evid={evid}", op.label()));
        Ok(())
    }

    async fn expect_err(&mut self, want: &str, op: Op) -> Result<(), String> {
        let out = self.issue(&op).await;
        if out.ok {
            return Err(format!(
                "exact: expected error {want} for [{}] but got 2xx",
                op.label()
            ));
        }
        match out.errcode.as_deref() {
            Some(e) if e == want => {
                self.oplog(&format!("EXACT deny  [{}] -> {e}", op.label()));
                Ok(())
            }
            other => Err(format!(
                "exact: expected {want} for [{}], got {other:?}",
                op.label()
            )),
        }
    }

    async fn record(&mut self, op: Op) {
        let out = self.issue(&op).await;
        if out.ok {
            self.ledger(&op, &out);
            self.apply_model(&op);
            self.writes_since_barrier += 1;
            let evid = out.event_id.as_deref().unwrap_or("–");
            self.oplog(&format!("record ok   [{}] evid={evid}", op.label()));
        } else {
            let err = out.errcode.as_deref().unwrap_or("rc");
            self.oplog(&format!(
                "record drop [{}] -> {err} (stale-view reject, ok mid-partition)",
                op.label()
            ));
        }
    }

    // ---- single-fact polls --------------------------------------------------

    async fn poll_member(&self, viewer: &str, subj: &str, want: &str) -> Result<(), String> {
        let u = mxid(subj);
        let deadline = Instant::now() + self.cfg.deadline;
        loop {
            let (st, val) = self
                .get(viewer, &format!("{CS}/rooms/{}/state", self.room))
                .await;
            if (200..300).contains(&st) && membership_of(&val, &u) == want {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("poll_member: {viewer} never saw {subj}={want}"));
            }
            sleep(self.cfg.interval).await;
        }
    }

    async fn poll_power(
        &self,
        viewer: &str,
        subj: &str,
        cmp: &str,
        lvl: i64,
    ) -> Result<(), String> {
        let u = mxid(subj);
        let deadline = Instant::now() + self.cfg.deadline;
        loop {
            let (st, val) = self
                .get(viewer, &format!("{CS}/rooms/{}/state", self.room))
                .await;
            if (200..300).contains(&st) {
                let seen = power_in_state(&val, &u);
                if (cmp == "ge" && seen >= lvl) || (cmp == "lt" && seen < lvl) {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(format!("poll_power: {viewer} never saw {subj} {cmp} {lvl}"));
            }
            sleep(self.cfg.interval).await;
        }
    }

    // ---- convergence oracle -------------------------------------------------

    /// Canonical `(type,state_key) -> event_id` map for one server, or `Err` if
    /// its `/state` is unreadable (partitioned / left).
    async fn state_map(&self, hs: &str) -> Result<BTreeMap<String, String>, ()> {
        let (st, val) = self
            .get(hs, &format!("{CS}/rooms/{}/state", self.room))
            .await;
        if !(200..300).contains(&st) {
            return Err(());
        }
        let mut m = BTreeMap::new();
        if let Some(arr) = val.as_array() {
            for e in arr {
                let t = e.get("type").and_then(Value::as_str).unwrap_or("");
                let sk = e.get("state_key").and_then(Value::as_str).unwrap_or("");
                let eid = e.get("event_id").and_then(Value::as_str).unwrap_or("");
                m.insert(format!("{t} {sk}"), eid.to_string());
            }
        }
        Ok(m)
    }

    /// Every message-timeline event_id a server holds, paged oldest-first via
    /// `/messages`. `None` on read failure.
    async fn collect_msg_ids(&self, hs: &str) -> Option<HashSet<String>> {
        let mut out = HashSet::new();
        let mut from = String::new();
        loop {
            let mut url = format!("{CS}/rooms/{}/messages?dir=f&limit=100", self.room);
            if !from.is_empty() {
                url.push_str(&format!("&from={from}"));
            }
            let (st, val) = self.get(hs, &url).await;
            if !(200..300).contains(&st) {
                return None;
            }
            let chunk = val
                .get("chunk")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let n = chunk.len();
            for e in &chunk {
                if let Some(id) = e.get("event_id").and_then(Value::as_str) {
                    out.insert(id.to_string());
                }
            }
            let end = val
                .get("end")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if end.is_empty() || end == from || n == 0 {
                break;
            }
            from = end;
        }
        Some(out)
    }

    /// Every ledgered message must be present on every server that was in its
    /// send-time audience.
    async fn messages_present_all(&self) -> bool {
        if !self.cfg.strict_event_presence || self.msg_accepted.is_empty() {
            return true;
        }
        for hs in SERVERS {
            if self.memb(hs) != "join" {
                continue;
            }
            let Some(ids) = self.collect_msg_ids(hs).await else {
                return false;
            };
            for (eid, aud) in &self.msg_accepted {
                if aud.iter().any(|a| a == hs) && !ids.contains(eid) {
                    return false;
                }
            }
        }
        true
    }

    /// Wait until all three servers' resolved state is identical, stable for
    /// `STABLE_POLLS`, and every ledgered message is present everywhere.
    async fn converge_check(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + self.cfg.deadline;
        let mut stable = 0u32;
        let mut prev: Option<BTreeMap<String, String>> = None;
        loop {
            let m1 = self.state_map("hs1").await.ok();
            let m2 = self.state_map("hs2").await.ok();
            let m3 = self.state_map("hs3").await.ok();
            let ok = matches!((&m1, &m2, &m3), (Some(a), Some(b), Some(c)) if a == b && b == c);
            if ok && prev.as_ref() == m1.as_ref() {
                stable += 1;
            } else {
                stable = 0;
            }
            prev = m1;
            if ok && stable >= self.cfg.stable_polls && self.messages_present_all().await {
                self.note(&format!(
                    "converged (state identical; {} message events present on all servers)",
                    self.msg_accepted.len()
                ));
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("no convergence within {:?}", self.cfg.deadline));
            }
            sleep(self.cfg.interval).await;
        }
    }

    /// Two servers on opposite sides of an ACTIVE cut, each having just locally
    /// accepted a write to the SAME state key, MUST now disagree on `/state`. If
    /// they agree, the partition is not biting and convergence is vacuous.
    async fn assert_divergent(&mut self, a: &str, b: &str, ctx: &str) -> Result<(), String> {
        let ma = self
            .state_map(a)
            .await
            .map_err(|()| format!("divergence guard: {a} /state unreadable ({ctx})"))?;
        let mb = self
            .state_map(b)
            .await
            .map_err(|()| format!("divergence guard: {b} /state unreadable ({ctx})"))?;
        if ma == mb {
            return Err(format!(
                "divergence guard: {a} and {b} agree on /state under an active cut after conflicting \
                 writes ({ctx}) — the partition is NOT biting, so convergence checks would pass vacuously"
            ));
        }
        self.divergence_seen = true;
        self.oplog(&format!(
            "  divergence confirmed: {a} != {b} under active cut ({ctx})"
        ));
        Ok(())
    }

    /// Pre-heal probe at the top of every barrier: if two readable servers
    /// already disagree, a partition produced real divergence this episode.
    async fn sample_divergence(&mut self) {
        let mut maps = Vec::new();
        for hs in SERVERS {
            if let Ok(m) = self.state_map(hs).await {
                maps.push(m);
            }
        }
        if maps.len() < 2 {
            return;
        }
        if maps[1..].iter().any(|m| *m != maps[0]) {
            self.divergence_seen = true;
            self.note("pre-heal divergence observed (partitions are biting)");
        }
    }

    // ---- barrier ------------------------------------------------------------

    async fn ensure_joined(&mut self, hs: &str) -> Result<(), String> {
        let (st, val) = self
            .get(hs, &format!("{CS}/rooms/{}/state", self.room))
            .await;
        let cur = if (200..300).contains(&st) {
            membership_of(&val, &mxid(hs))
        } else {
            "leave".to_string()
        };
        if cur == "join" {
            return Ok(());
        }
        let _ = self
            .issue(&Op::Join {
                hs: hs.to_string(),
                resident: "hs1".into(),
            })
            .await;
        self.member.insert(hs.to_string(), "join".into());
        self.poll_member(hs, hs, "join").await
    }

    async fn barrier(&mut self) -> Result<(), String> {
        self.log("barrier: revive crashed servers, heal all links, restore membership, converge");
        self.sample_divergence().await;
        // Revive any crashed server first: `docker start` reattaches networks
        // nondeterministically, so the heal-all below then normalises link state
        // regardless of what the restart left behind.
        for hs in SERVERS {
            if !self.is_proc_up(hs) {
                self.revive(hs).await?;
                self.up_proc.insert(hs.to_string(), true);
                self.note(&format!("revived {hs} (recovers committed state + outbox)"));
            }
        }
        for k in LINKS {
            if !self.is_up(k) {
                self.partition("heal", k).await;
            }
            self.up.insert(k.to_string(), true);
        }
        self.ensure_joined("hs2").await?;
        self.ensure_joined("hs3").await?;
        self.converge_check().await?;
        self.sync_model().await?;
        self.writes_since_barrier = 0;
        Ok(())
    }

    // ---- exact-tier probe battery -------------------------------------------

    /// Walk a joined non-admin through deny -> promote -> allow -> demote -> deny,
    /// then a leave/rejoin. Rigorously checks the admin/PL/membership contract.
    async fn exact_probes(&mut self) -> Result<(), String> {
        let req = self.state_req("m.room.name");
        let na = ["hs2", "hs3"]
            .into_iter()
            .find(|s| self.memb(s) == "join" && self.power_of(s) < req)
            .map(str::to_string);
        let Some(na) = na else {
            self.note("exact-probes: no joined non-admin (everyone is admin); skipping PL battery");
            return Ok(());
        };
        self.log(&format!(
            "exact-probes: admin/PL/membership contract via {na}"
        ));

        self.seq += 1;
        let t = self.seq;
        self.expect_err(
            "M_FORBIDDEN",
            Op::Name {
                hs: na.clone(),
                text: format!("probe-deny-{t}"),
            },
        )
        .await?;
        self.expect_ok(Op::Power {
            hs: "hs1".into(),
            target: na.clone(),
            level: 100,
        })
        .await?;
        self.poll_power(&na, &na, "ge", req).await?;
        self.seq += 1;
        let t = self.seq;
        self.expect_ok(Op::Name {
            hs: na.clone(),
            text: format!("probe-allow-{t}"),
        })
        .await?;
        self.expect_ok(Op::Power {
            hs: "hs1".into(),
            target: na.clone(),
            level: 0,
        })
        .await?;
        self.poll_power(&na, &na, "lt", req).await?;
        self.seq += 1;
        let t = self.seq;
        self.expect_err(
            "M_FORBIDDEN",
            Op::Name {
                hs: na.clone(),
                text: format!("probe-deny2-{t}"),
            },
        )
        .await?;
        self.expect_ok(Op::Leave { hs: na.clone() }).await?;
        self.poll_member(&na, &na, "leave").await?;
        self.expect_ok(Op::Join {
            hs: na.clone(),
            resident: "hs1".into(),
        })
        .await?;
        self.poll_member(&na, &na, "join").await
    }

    // ---- fuzz phase ---------------------------------------------------------

    /// Enumerate model-valid candidate ops from the shadow model.
    fn candidates(&self) -> Vec<Cand> {
        let mut cand = Vec::new();
        let name_req = self.state_req("m.room.name");
        let pl_req = self.state_req("m.room.power_levels");
        for s in SERVERS {
            // A crashed server can't issue anything; skip it as a candidate actor
            // (it's revived, and its membership reconverged, at the barrier).
            if !self.is_proc_up(s) {
                continue;
            }
            let sp = self.power_of(s);
            if self.memb(s) == "join" {
                if sp >= self.pl_events_default {
                    cand.push(Cand::Msg(s.into()));
                }
                if sp >= name_req {
                    cand.push(Cand::Name(s.into()));
                }
                for t in SERVERS {
                    if t == s {
                        continue;
                    }
                    let tp = self.power_of(t);
                    let tm = self.memb(t);
                    if sp >= pl_req && t != "hs1" {
                        cand.push(Cand::Power(s.into(), t.into()));
                    }
                    if sp >= self.pl_invite && tm == "leave" {
                        cand.push(Cand::Invite(s.into(), t.into()));
                    }
                    if sp >= self.pl_kick
                        && sp > tp
                        && t != "hs1"
                        && (tm == "join" || tm == "invite")
                    {
                        cand.push(Cand::Kick(s.into(), t.into()));
                    }
                }
                if s != "hs1" {
                    cand.push(Cand::Leave(s.into()));
                }
            } else {
                cand.push(Cand::Join(s.into()));
            }
        }
        cand
    }

    /// Pick one valid (actor, op) from the shadow model and run it RECORD-tier.
    async fn fuzz_mutate(&mut self) {
        let cand = self.candidates();
        if cand.is_empty() {
            self.seq += 1;
            let n = self.seq;
            self.record(Op::Msg {
                hs: "hs1".into(),
                text: format!("fuzz-{n}"),
            })
            .await;
            return;
        }
        let pick = cand[self.rng.random_range(0..cand.len())].clone();
        match pick {
            Cand::Msg(s) => {
                self.seq += 1;
                let n = self.seq;
                self.record(Op::Msg {
                    hs: s,
                    text: format!("fuzz-{n}"),
                })
                .await;
            }
            Cand::Name(s) => {
                self.seq += 1;
                let n = self.seq;
                self.record(Op::Name {
                    hs: s,
                    text: format!("fuzz-name-{n}"),
                })
                .await;
            }
            Cand::Power(s, t) => {
                let lvl = self.rng.random_range(0..=self.power_of(&s));
                self.record(Op::Power {
                    hs: s,
                    target: t,
                    level: lvl,
                })
                .await;
            }
            Cand::Invite(s, t) => self.record(Op::Invite { hs: s, target: t }).await,
            Cand::Kick(s, t) => self.record(Op::Kick { hs: s, target: t }).await,
            Cand::Leave(s) => self.record(Op::Leave { hs: s }).await,
            Cand::Join(s) => {
                self.record(Op::Join {
                    hs: s,
                    resident: "hs1".into(),
                })
                .await
            }
        }
    }

    /// Manufacture a state-res conflict: two joined co-admins set `m.room.name`
    /// to different values across an active cut. Returns `false` when no two
    /// co-admins are joined (caller falls through to a normal mutation).
    async fn fuzz_conflict(&mut self) -> Result<bool, String> {
        let req = self.state_req("m.room.name");
        let pairs: Vec<&str> = LINKS
            .into_iter()
            .filter(|k| {
                let (a, b) = link_servers(k);
                self.memb(a) == "join"
                    && self.memb(b) == "join"
                    && self.power_of(a) >= req
                    && self.power_of(b) >= req
                    && self.is_proc_up(a)
                    && self.is_proc_up(b)
            })
            .collect();
        if pairs.is_empty() {
            return Ok(false);
        }
        let k = pairs[self.rng.random_range(0..pairs.len())];
        let (a, b) = link_servers(k);
        if self.is_up(k) {
            self.partition("cut", k).await;
            self.up.insert(k.to_string(), false);
            self.any_cut = true;
            self.oplog(&format!("topology cut  {k} (to manufacture a conflict)"));
        }
        self.seq += 1;
        let tag = self.seq;
        self.oplog(&format!(
            "conflict {k}: {a} vs {b} both set m.room.name under an active cut"
        ));
        let ra = self
            .issue(&Op::Name {
                hs: a.into(),
                text: format!("conflict-{tag}-{a}"),
            })
            .await;
        self.oplog(&format!(
            "  {a} name -> {}",
            if ra.ok {
                "ok"
            } else {
                ra.errcode.as_deref().unwrap_or("rc")
            }
        ));
        let rb = self
            .issue(&Op::Name {
                hs: b.into(),
                text: format!("conflict-{tag}-{b}"),
            })
            .await;
        self.oplog(&format!(
            "  {b} name -> {}",
            if rb.ok {
                "ok"
            } else {
                rb.errcode.as_deref().unwrap_or("rc")
            }
        ));
        // Accepted conflict writes are recoverable state a later crash must
        // survive, so they count toward the crash-bit vacuity signal too.
        self.writes_since_barrier += u32::from(ra.ok) + u32::from(rb.ok);
        if ra.ok && rb.ok {
            self.conflict_fired = true;
            self.assert_divergent(a, b, &format!("conflict-{tag} link={k}"))
                .await?;
        } else {
            self.oplog(&format!(
                "  conflict not both-accepted (ra={} rb={}); divergence not asserted (stale view, ok)",
                ra.ok, rb.ok
            ));
        }
        Ok(true)
    }

    /// Cut a random up-link or heal a random down-link, biased toward healing
    /// when many links are down.
    async fn fuzz_topology(&mut self) {
        let ups: Vec<&str> = LINKS.into_iter().filter(|k| self.is_up(k)).collect();
        let downs: Vec<&str> = LINKS.into_iter().filter(|k| !self.is_up(k)).collect();
        let do_heal = !downs.is_empty() && (ups.is_empty() || self.rng.random_range(0..3) == 0);
        if do_heal {
            let k = downs[self.rng.random_range(0..downs.len())];
            self.partition("heal", k).await;
            self.up.insert(k.to_string(), true);
            self.oplog(&format!("topology heal {k}"));
        } else if !ups.is_empty() {
            let k = ups[self.rng.random_range(0..ups.len())];
            self.partition("cut", k).await;
            self.up.insert(k.to_string(), false);
            self.any_cut = true;
            self.oplog(&format!("topology cut  {k}"));
        }
    }

    /// Crash a live server or revive a dead one (biased toward reviving so a
    /// crash doesn't pin a server down for the whole episode). At least one
    /// server is always kept alive, so the rig keeps making progress and a
    /// readable `/state` always exists for the divergence probes. Reviving brings
    /// a server back fully connected: `docker start` reattaches it to its
    /// networks, so any cut touching it is healed — the model is squared with
    /// that here.
    async fn fuzz_crash(&mut self) -> Result<(), String> {
        let alive: Vec<&str> = SERVERS.into_iter().filter(|s| self.is_proc_up(s)).collect();
        let dead: Vec<&str> = SERVERS
            .into_iter()
            .filter(|s| !self.is_proc_up(s))
            .collect();
        let do_revive = !dead.is_empty() && (alive.len() < 2 || self.rng.random_range(0..2) == 0);
        if do_revive {
            let hs = dead[self.rng.random_range(0..dead.len())];
            self.revive(hs).await?;
            self.up_proc.insert(hs.to_string(), true);
            for k in LINKS {
                let (a, b) = link_servers(k);
                if (a == hs || b == hs) && !self.is_up(k) {
                    self.partition("heal", k).await;
                    self.up.insert(k.to_string(), true);
                }
            }
            self.oplog(&format!(
                "revive {hs} (restart; recovers /data, fully reconnected)"
            ));
        } else if alive.len() >= 2 {
            let hs = alive[self.rng.random_range(0..alive.len())];
            self.crash(hs).await;
            self.up_proc.insert(hs.to_string(), false);
            self.crash_fired = true;
            if self.writes_since_barrier > 0 {
                self.crash_bit = true;
            }
            self.oplog(&format!(
                "crash {hs} (SIGKILL; {} writes recoverable since last barrier)",
                self.writes_since_barrier
            ));
        }
        Ok(())
    }

    /// ~30% topology change; ~15% manufacture a conflict (falling through to a
    /// normal mutation when impossible); `crash_prob`% a crash/revive; else a
    /// model-valid mutation.
    async fn fuzz_round(&mut self) -> Result<(), String> {
        let roll = self.rng.random_range(0..100);
        if roll < 30 {
            self.fuzz_topology().await;
        } else if roll < 45 {
            if !self.fuzz_conflict().await? {
                self.fuzz_mutate().await;
            }
        } else if roll < 45 + self.cfg.crash_prob {
            self.fuzz_crash().await?;
        } else {
            self.fuzz_mutate().await;
        }
        Ok(())
    }

    // ---- rig lifecycle ------------------------------------------------------

    async fn compose(&self, args: &[&str]) -> bool {
        let compose_file = self.rig_dir.join("docker-compose.yml");
        Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&compose_file)
            .args(args)
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Run `nctl <args>` (best-effort; topology/crash control is fire-and-forget,
    /// the harness re-reads reality via polls).
    async fn nctl(&self, args: &[&str]) {
        let nctl = self.rig_dir.join("nctl");
        let _ = Command::new(&nctl).args(args).output().await;
    }

    /// Topology control via `nctl partition`. `nctl`'s heal resets outbound
    /// backoff on both ends, so the harness never signals the servers itself.
    async fn partition(&self, action: &str, link: &str) {
        self.nctl(&["partition", action, link]).await;
    }

    /// Hard-crash a server (`nctl crash` => SIGKILL). The process dies with no
    /// graceful shutdown; `/data` on disk survives, so this models a process
    /// crash, not host power-loss.
    async fn crash(&self, hs: &str) {
        self.nctl(&["crash", hs]).await;
    }

    /// Restart a crashed server and wait for it to serve again. `docker start`
    /// recovers `/data` (committed state) and the server re-arms its outbox
    /// sender on startup; the trailing `nctl kick` resets every peer's backoff so
    /// delivery to/from the revived server resumes immediately instead of waiting
    /// out a backoff that built up while it was down.
    async fn revive(&self, hs: &str) -> Result<(), String> {
        self.nctl(&["revive", hs]).await;
        self.wait_ready(hs).await?;
        self.nctl(&["kick"]).await;
        Ok(())
    }

    async fn wait_ready(&self, hs: &str) -> Result<(), String> {
        let url = format!("http://localhost:{}/_matrix/client/versions", port_of(hs));
        let deadline = Instant::now() + self.cfg.deadline;
        loop {
            if let Ok(r) = self.client.get(&url).send().await
                && r.status().is_success()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("{hs} never became ready"));
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn bring_up(&mut self) -> Result<(), String> {
        self.log(&format!(
            "seed={} episodes={} rounds/episode={} deadline={:?}",
            self.seed, self.cfg.episodes, self.cfg.rounds_per_episode, self.cfg.deadline
        ));
        self.log("starting rig");
        let have_image = Command::new("docker")
            .args(["image", "inspect", "neutrino-testrig:latest"])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        let up_args: &[&str] = if have_image {
            &["up", "-d"]
        } else {
            &["up", "-d", "--build"]
        };
        if !self.compose(up_args).await {
            return Err("compose up failed".into());
        }
        self.wait_ready("hs1").await?;
        self.wait_ready("hs2").await?;
        self.wait_ready("hs3").await
    }

    async fn cleanup(&self) {
        let _ = self.compose(&["down", "-v"]).await;
    }

    /// On failure, dump seed + op log + each server's resolved state + rig logs.
    async fn dump(&self) {
        eprintln!(
            "---- SEED={}  (re-run: converge {}) ----",
            self.seed, self.seed
        );
        eprintln!("---- op log ({} actions) ----", self.op_log.len());
        for l in &self.op_log {
            eprintln!("{l}");
        }
        eprintln!("---- final per-server resolved state ----");
        for hs in SERVERS {
            let proc = if self.is_proc_up(hs) {
                "alive"
            } else {
                "CRASHED (not revived)"
            };
            eprintln!("## {hs} [{proc}]");
            let (_, val) = self
                .get(hs, &format!("{CS}/rooms/{}/state", self.room))
                .await;
            eprintln!("{val}");
        }
        eprintln!("---- compose logs ----");
        let compose_file = self.rig_dir.join("docker-compose.yml");
        if let Ok(o) = Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&compose_file)
            .args(["logs", "--no-color"])
            .output()
            .await
        {
            eprintln!("{}", String::from_utf8_lossy(&o.stdout));
        }
    }

    /// Run-level vacuity signal: links cut but divergence never observed almost
    /// certainly means the partitions aren't biting.
    fn vacuity_check(&self) {
        if self.conflict_fired {
            self.note("state-res conflicts exercised; divergence guard held on every manufactured conflict");
        } else if self.any_cut && self.divergence_seen {
            self.note(
                "no two co-admins coincided on a cut (no conflict manufactured), but partition \
                 divergence WAS observed pre-heal",
            );
        } else if self.any_cut {
            self.log(
                "WARNING: links were cut but divergence was never observed and no conflict fired — \
                 partitions may not be biting, or this seed under-exercised them (try more \
                 EPISODES/ROUNDS_PER_EPISODE, or another seed)",
            );
        }
        // Crash coverage is orthogonal to partition divergence: a crash always
        // forces a real restart + recovery + reconverge at the barrier, but a
        // crash that lands while the server is idle/converged recovers nothing
        // interesting. crash_bit means at least one crash had recoverable work.
        if self.crash_bit {
            self.note(
                "process crashes exercised with recoverable work in flight; every revived server \
                 recovered its committed state, redelivered its outbox, and reconverged",
            );
        } else if self.crash_fired {
            self.note(
                "crashes fired but only while idle/converged (no writes pending) — recovery was \
                 trivial; raise CRASH_PROB or run more rounds to crash mid-activity",
            );
        } else if self.cfg.crash_prob > 0 {
            self.note("no crash fired this run (try a higher CRASH_PROB or another seed)");
        }
    }

    async fn run(&mut self) -> Result<(), String> {
        self.bring_up().await?;
        self.log("create room on hs1, join hs2 + hs3");
        self.create_room().await?;
        self.expect_ok(Op::Invite {
            hs: "hs1".into(),
            target: "hs2".into(),
        })
        .await?;
        self.expect_ok(Op::Invite {
            hs: "hs1".into(),
            target: "hs3".into(),
        })
        .await?;
        self.expect_ok(Op::Join {
            hs: "hs2".into(),
            resident: "hs1".into(),
        })
        .await?;
        self.expect_ok(Op::Join {
            hs: "hs3".into(),
            resident: "hs1".into(),
        })
        .await?;
        self.barrier().await?;
        self.exact_probes().await?;
        for ep in 1..=self.cfg.episodes {
            self.log(&format!(
                "episode {ep}/{}: {} fuzz rounds (record tier)",
                self.cfg.episodes, self.cfg.rounds_per_episode
            ));
            for _ in 0..self.cfg.rounds_per_episode {
                self.fuzz_round().await?;
            }
            self.barrier().await?;
            self.exact_probes().await?;
        }
        self.vacuity_check();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut h = Harness::new();
    let guard = ComposeGuard {
        compose_file: h.rig_dir.join("docker-compose.yml"),
    };
    let result = h.run().await;
    match &result {
        Ok(()) => h.log(&format!(
            "PASS — all {} episodes converged (seed={})",
            h.cfg.episodes, h.seed
        )),
        Err(e) => {
            eprintln!("CONVERGE FAIL: {e}");
            h.dump().await;
        }
    }
    h.cleanup().await;
    drop(guard);
    if result.is_err() {
        std::process::exit(1);
    }
}
