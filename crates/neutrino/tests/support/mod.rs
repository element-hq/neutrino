//! Child-process multi-server federation test harness.
//!
//! Spawns N real `neutrino` binaries (located via `CARGO_BIN_EXE_neutrino`,
//! which cargo builds before these integration tests run — so the binary under
//! test is never stale) and drives real federation HTTP between them. No docker.
//!
//! ## Two ports per server, and how partitions work
//!
//! Each server gets two loopback ports:
//! * a **backend** port the child `neutrino` binds (`NEUTRINO_BIND_ADDR`), and
//! * an **advertised** port served by a parent-hosted toggleable reverse proxy.
//!
//! `NEUTRINO_SERVER_NAME` is the *advertised* `127.0.0.1:<advertised>`, so a peer
//! dials the proxy and the child stamps that address as its `origin`. The proxy
//! reads the `X-Matrix origin` header (every outbound federation request carries
//! it) and, if the unordered pair `{self, origin}` is in the shared [`CutSet`],
//! replies `503` (retryable — the sender parks the transaction in its durable
//! outbox and redelivers on heal); otherwise it forwards to the backend. The
//! proxy lives in the parent process and outlives child crash/revive, so the
//! advertised endpoint is stable even while the child is dead (it then 502s,
//! which peers treat as down).
//!
//! ## Crash / revive / heal
//!
//! * **crash** = SIGKILL the child's process group — a real abrupt kill, so this
//!   genuinely exercises on-disk durability (WAL + `synchronous=NORMAL`).
//! * **revive** = re-spawn the binary with the same env (same backend port and
//!   storage dir, which the parent owns so it survives the kill).
//! * **heal** = clear the cut pair and SIGUSR2 the live children, which the
//!   `neutrino` binary maps to `KickBackoff` so a healed link redrains promptly.
//!
//! CSAPI is driven over the backend port directly, so a partition never blinds
//! the harness, and a server can be fully isolated (both links cut) without
//! losing its CSAPI.

use std::collections::HashSet;
use std::net::TcpListener as StdTcpListener;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// Unordered pairs of server names that are currently partitioned.
type CutSet = Arc<Mutex<HashSet<(String, String)>>>;

/// Sliding-sync endpoint (MSC4186 / simplified MSC3575).
const SYNC: &str = "_matrix/client/unstable/org.matrix.simplified_msc3575/sync";

/// Canonical unordered key for a link between two server names.
fn pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

/// Grab a free loopback port by binding ephemeral and immediately releasing it.
/// The child re-binds it a moment later; on loopback in a test the race is benign.
fn free_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

/// Spawn the real `neutrino` binary as a child in its own process group (so a
/// parent panic can't orphan it), configured entirely by env.
fn spawn_neutrino(server_name: &str, backend: &str, storage: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_neutrino"))
        .env("NEUTRINO_SERVER_NAME", server_name)
        .env("NEUTRINO_BIND_ADDR", backend)
        .env("NEUTRINO_STORAGE_DIR", storage)
        // No startup jitter in tests: a revived server should redrain its outbox
        // immediately, so the crash test doesn't wait out the 30s production guard.
        .env("NEUTRINO_STARTUP_JITTER_MS", "0")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .expect("spawn neutrino")
}

/// Send `sig` to the process (positive pid) or its group (negate the pid).
fn signal(pid: u32, sig: i32) {
    // SAFETY: `kill(2)` with a constant signal; an invalid/dead pid just returns ESRCH.
    unsafe {
        libc::kill(pid as i32, sig);
    }
}

struct Server {
    /// Advertised `server_name` (`127.0.0.1:<advertised-port>`) — dialed by peers,
    /// stamped as this server's federation `origin`.
    name: String,
    /// `127.0.0.1:<backend-port>` — the child's real router, where CSAPI is driven.
    backend: String,
    /// The running child, or `None` between crash and revive.
    child: Option<Child>,
    /// Parent-owned storage dir; survives child crash/revive (the durability point).
    storage: tempfile::TempDir,
}

pub(crate) struct Harness {
    servers: Vec<Server>,
    cut: CutSet,
    http: reqwest::Client,
    deadline: Duration,
    txn: AtomicU64,
}

impl Harness {
    /// Start `n` child-process servers, all links up, and wait until each serves
    /// through its proxy.
    pub(crate) async fn start(n: usize) -> Harness {
        let cut: CutSet = Arc::new(Mutex::new(HashSet::new()));
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("reqwest client");
        let mut servers = Vec::with_capacity(n);
        for _ in 0..n {
            // Bind the proxy (advertised) listener for real and keep it — no race
            // on its port. The backend port is discovered-then-passed to the child.
            let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
            let name = proxy_listener.local_addr().expect("proxy addr").to_string();
            let backend = format!("127.0.0.1:{}", free_port());
            let storage = tempfile::TempDir::new().expect("storage tempdir");

            let child = spawn_neutrino(&name, &backend, storage.path());

            let proxy = Router::new()
                .fallback(proxy_handler)
                .with_state(ProxyState {
                    backend: backend.clone(),
                    me: name.clone(),
                    cut: cut.clone(),
                    client: reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .expect("proxy client"),
                });
            tokio::spawn(async move {
                let _ = axum::serve(proxy_listener, proxy).await;
            });

            servers.push(Server {
                name,
                backend,
                child: Some(child),
                storage,
            });
        }
        let h = Harness {
            servers,
            cut,
            http,
            deadline: Duration::from_secs(20),
            txn: AtomicU64::new(0),
        };
        for i in 0..n {
            h.await_ready(i).await;
        }
        h
    }

    pub(crate) fn name(&self, i: usize) -> &str {
        &self.servers[i].name
    }

    /// `@alice:<server_name>` — each server acts as its single default user.
    pub(crate) fn mxid(&self, i: usize) -> String {
        format!("@alice:{}", self.servers[i].name)
    }

    // ---- topology + lifecycle ----------------------------------------------

    pub(crate) fn cut(&self, i: usize, j: usize) {
        let p = pair(self.name(i), self.name(j));
        self.cut.lock().expect("cut-set").insert(p);
    }

    pub(crate) fn heal(&self, i: usize, j: usize) {
        let p = pair(self.name(i), self.name(j));
        self.cut.lock().expect("cut-set").remove(&p);
        // Reset both live senders' backoff (SIGUSR2 -> KickBackoff) so the healed
        // link redrains promptly instead of waiting out accrued backoff.
        for k in [i, j] {
            if let Some(child) = &self.servers[k].child {
                signal(child.id(), libc::SIGUSR2);
            }
        }
    }

    /// SIGKILL the server's process group — a real abrupt crash. Committed state
    /// must survive on disk; anything parked in its outbox must redeliver on revive.
    pub(crate) fn crash(&mut self, i: usize) {
        if let Some(mut child) = self.servers[i].child.take() {
            signal_group(&child, libc::SIGKILL);
            let _ = child.wait(); // reap the zombie
        }
    }

    /// Re-spawn a crashed server with the same env (same backend port + storage
    /// dir) and wait until it serves again.
    pub(crate) async fn revive(&mut self, i: usize) {
        let s = &mut self.servers[i];
        if s.child.is_none() {
            s.child = Some(spawn_neutrino(&s.name, &s.backend, s.storage.path()));
        }
        self.await_ready(i).await;
    }

    // ---- CSAPI (always over the backend port — never partitioned) -----------

    async fn cs(&self, i: usize, method: reqwest::Method, path: &str, body: Value) -> (u16, Value) {
        let url = format!("http://{}/{}", self.servers[i].backend, path);
        let resp = self
            .http
            .request(method, &url)
            .json(&body)
            .send()
            .await
            .expect("csapi request");
        let st = resp.status().as_u16();
        let val = resp.json::<Value>().await.unwrap_or(Value::Null);
        (st, val)
    }

    pub(crate) async fn create_room(&self, i: usize, name: &str) -> String {
        let (st, val) = self
            .cs(
                i,
                reqwest::Method::POST,
                "_matrix/client/v3/createRoom",
                json!({ "preset": "public_chat", "name": name }),
            )
            .await;
        assert!((200..300).contains(&st), "createRoom status {st}: {val}");
        val.get("room_id")
            .and_then(Value::as_str)
            .expect("room_id")
            .to_owned()
    }

    pub(crate) async fn invite(&self, i: usize, room: &str, target: &str) -> u16 {
        self.cs(
            i,
            reqwest::Method::POST,
            &format!("_matrix/client/v3/rooms/{room}/invite"),
            json!({ "user_id": target }),
        )
        .await
        .0
    }

    /// `POST /join/{room}?server_name=<resident>` — join via a resident server.
    pub(crate) async fn join(&self, i: usize, room: &str, resident: &str) -> u16 {
        // `resident` is `127.0.0.1:<port>` — only query-safe chars, so inline it
        // (this reqwest feature set has no `RequestBuilder::query`).
        let url = format!(
            "http://{}/_matrix/client/v3/join/{room}?server_name={resident}",
            self.servers[i].backend
        );
        let resp = self
            .http
            .post(&url)
            .json(&json!({}))
            .send()
            .await
            .expect("join request");
        resp.status().as_u16()
    }

    pub(crate) async fn leave(&self, i: usize, room: &str) -> u16 {
        self.cs(
            i,
            reqwest::Method::POST,
            &format!("_matrix/client/v3/rooms/{room}/leave"),
            json!({}),
        )
        .await
        .0
    }

    pub(crate) async fn set_name(&self, i: usize, room: &str, name: &str) -> u16 {
        self.cs(
            i,
            reqwest::Method::PUT,
            &format!("_matrix/client/v3/rooms/{room}/state/m.room.name"),
            json!({ "name": name }),
        )
        .await
        .0
    }

    pub(crate) async fn send_message(&self, i: usize, room: &str, body: &str) -> u16 {
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        self.cs(
            i,
            reqwest::Method::PUT,
            &format!("_matrix/client/v3/rooms/{room}/send/m.room.message/t{txn}"),
            json!({ "msgtype": "m.text", "body": body }),
        )
        .await
        .0
    }

    /// Set `target`'s power level (read-modify-write of `m.room.power_levels`,
    /// since a PUT replaces the whole content).
    pub(crate) async fn set_power(&self, i: usize, room: &str, target: &str, level: i64) -> u16 {
        let path = format!("_matrix/client/v3/rooms/{room}/state/m.room.power_levels");
        let (st, mut content) = self.cs(i, reqwest::Method::GET, &path, Value::Null).await;
        if !(200..300).contains(&st) {
            return st;
        }
        if !content.get("users").map(Value::is_object).unwrap_or(false) {
            content["users"] = json!({});
        }
        content["users"][target] = json!(level);
        self.cs(i, reqwest::Method::PUT, &path, content).await.0
    }

    // ---- reads --------------------------------------------------------------

    /// Resolved `/state` array, or `None` if unreadable.
    async fn state(&self, i: usize, room: &str) -> Option<Value> {
        let (st, val) = self
            .cs(
                i,
                reqwest::Method::GET,
                &format!("_matrix/client/v3/rooms/{room}/state"),
                Value::Null,
            )
            .await;
        ((200..300).contains(&st)).then_some(val)
    }

    /// Sorted `(type, state_key, event_id)` triples for one server.
    async fn state_map(&self, i: usize, room: &str) -> Option<Vec<(String, String, String)>> {
        let arr = self.state(i, room).await?;
        let arr = arr.as_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for e in arr {
            let t = e.get("type").and_then(Value::as_str).unwrap_or_default();
            let sk = e
                .get("state_key")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let id = e
                .get("event_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            out.push((t.to_owned(), sk.to_owned(), id.to_owned()));
        }
        out.sort();
        Some(out)
    }

    pub(crate) async fn current_name(&self, i: usize, room: &str) -> Option<String> {
        let arr = self.state(i, room).await?;
        arr.as_array()?
            .iter()
            .rev()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("m.room.name"))
            .and_then(|e| e.get("content")?.get("name")?.as_str())
            .map(str::to_owned)
    }

    async fn membership(&self, i: usize, room: &str, mxid: &str) -> String {
        let Some(arr) = self.state(i, room).await else {
            return "unreadable".to_owned();
        };
        arr.as_array()
            .into_iter()
            .flatten()
            .rev()
            .find(|e| {
                e.get("type").and_then(Value::as_str) == Some("m.room.member")
                    && e.get("state_key").and_then(Value::as_str) == Some(mxid)
            })
            .and_then(|e| e.get("content")?.get("membership")?.as_str())
            .unwrap_or("leave")
            .to_owned()
    }

    /// `target`'s explicit power level in `i`'s resolved state, if set.
    async fn current_power(&self, i: usize, room: &str, target: &str) -> Option<i64> {
        let arr = self.state(i, room).await?;
        arr.as_array()?
            .iter()
            .rev()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("m.room.power_levels"))
            .and_then(|e| e.get("content")?.get("users")?.get(target)?.as_i64())
    }

    /// `i`'s sliding-sync timeline for `room` (initial sync, no `pos`, large
    /// `timeline_limit`) — same coverage as `/messages`, including state PDUs.
    async fn sync_timeline(&self, i: usize, room: &str) -> Vec<Value> {
        let body =
            json!({ "lists": { "default": { "ranges": [[0, 99]], "timeline_limit": 1000 } } });
        let (st, val) = self.cs(i, reqwest::Method::POST, SYNC, body).await;
        if !(200..300).contains(&st) {
            return Vec::new();
        }
        val.get("rooms")
            .and_then(|rooms| rooms.get(room))
            .and_then(|r| r.get("timeline"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    // ---- polls --------------------------------------------------------------

    async fn await_ready(&self, i: usize) {
        let url = format!("http://{}/_matrix/client/versions", self.servers[i].name);
        let deadline = Instant::now() + self.deadline;
        loop {
            if let Ok(r) = self.http.get(&url).send().await
                && r.status().is_success()
            {
                return;
            }
            assert!(Instant::now() < deadline, "server {i} never became ready");
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub(crate) async fn await_membership(&self, i: usize, room: &str, mxid: &str, want: &str) {
        let deadline = Instant::now() + self.deadline;
        loop {
            if self.membership(i, room, mxid).await == want {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server {i} never saw {mxid} = {want}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub(crate) async fn await_name(&self, i: usize, room: &str, want: &str) {
        let deadline = Instant::now() + self.deadline;
        loop {
            if self.current_name(i, room).await.as_deref() == Some(want) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server {i} never resolved m.room.name = {want}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub(crate) async fn await_power(&self, i: usize, room: &str, target: &str, level: i64) {
        let deadline = Instant::now() + self.deadline;
        loop {
            if self.current_power(i, room, target).await == Some(level) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server {i} never saw {target} at power {level}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until every server in `who` has a byte-identical resolved `/state`.
    pub(crate) async fn await_converged(&self, room: &str, who: &[usize]) {
        let deadline = Instant::now() + self.deadline;
        loop {
            let mut maps = Vec::with_capacity(who.len());
            for &i in who {
                maps.push(self.state_map(i, room).await);
            }
            let all_some = maps.iter().all(Option::is_some);
            if all_some && maps.windows(2).all(|w| w[0] == w[1]) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "servers {who:?} never converged on identical /state"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// One-shot: does `i`'s current sliding-sync timeline for `room` contain an
    /// event matching `pred`? (No polling — for asserting *absence* at a point.)
    pub(crate) async fn timeline_has(
        &self,
        i: usize,
        room: &str,
        pred: impl Fn(&Value) -> bool,
    ) -> bool {
        self.sync_timeline(i, room).await.iter().any(&pred)
    }

    /// Wait until `i`'s sliding-sync timeline for `room` contains an event
    /// matching `pred`. `desc` names the wait for the panic message.
    pub(crate) async fn await_timeline(
        &self,
        i: usize,
        room: &str,
        desc: &str,
        pred: impl Fn(&Value) -> bool,
    ) {
        let deadline = Instant::now() + self.deadline;
        loop {
            if self.sync_timeline(i, room).await.iter().any(&pred) {
                return;
            }
            assert!(Instant::now() < deadline, "server {i} timeline: {desc}");
            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Kill each child's whole process group so a panic can't orphan a real
        // `neutrino` process; reap to avoid zombies. Tempdirs drop afterwards.
        for s in &mut self.servers {
            if let Some(mut child) = s.child.take() {
                signal_group(&child, libc::SIGKILL);
                let _ = child.wait();
            }
        }
    }
}

/// SIGKILL/SIGUSR2 a child's whole process group (negative pid). The child was
/// spawned with `process_group(0)`, so its pid is its pgid.
fn signal_group(child: &Child, sig: i32) {
    // SAFETY: `kill(2)` with a negative pid targets the process group; a dead
    // group just yields ESRCH.
    unsafe {
        libc::kill(-(child.id() as i32), sig);
    }
}

#[derive(Clone)]
struct ProxyState {
    backend: String,
    me: String,
    cut: CutSet,
    client: reqwest::Client,
}

/// Extract the `origin` from an `X-Matrix origin="…",destination="…"` header.
fn x_matrix_origin(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let start = auth.find("origin=\"")? + "origin=\"".len();
    let rest = &auth[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// Forward the request to the backend, unless the link from its `origin` to this
/// server is cut — in which case return `503` (retryable; the sender re-queues).
async fn proxy_handler(State(st): State<ProxyState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    if let Some(origin) = x_matrix_origin(&parts.headers)
        && st
            .cut
            .lock()
            .expect("cut-set")
            .contains(&pair(&st.me, &origin))
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "partitioned").into_response();
    }
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let url = format!("http://{}{}", st.backend, path_and_query);
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);
    let mut out = st.client.request(method, &url).body(bytes.to_vec());
    for (k, v) in &parts.headers {
        let key = k.as_str();
        if key == "host" || key == "content-length" {
            continue;
        }
        out = out.header(key, v.as_bytes());
    }
    match out.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let ctype = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = resp.bytes().await.unwrap_or_default();
            let mut builder = Response::builder().status(status);
            if let Some(ct) = ctype {
                builder = builder.header("content-type", ct);
            }
            builder
                .body(Body::from(body))
                .expect("build proxy response")
        }
        Err(_) => (StatusCode::BAD_GATEWAY, "proxy upstream error").into_response(),
    }
}
