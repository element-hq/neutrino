//! In-process multi-server federation test harness.
//!
//! Spawns N real `neutrino` binaries (at a caller-provided path) on loopback and
//! drives real federation HTTP between them, with toggleable proxy partitions and
//! real SIGKILL crash/revive. No docker. Shared by the `neutrino` crate's
//! integration tests (which pass `env!("CARGO_BIN_EXE_neutrino")`) and the
//! `converge` fuzzer (which builds + locates the binary itself). Depends on no
//! other neutrino crate.
//!
//! ## Two ports per server, and how partitions work
//!
//! Each server gets two loopback ports: a **backend** port the child `neutrino`
//! binds (`NEUTRINO_BIND_ADDR`), and an **advertised** port served by a
//! parent-hosted toggleable reverse proxy. `NEUTRINO_SERVER_NAME` is the
//! advertised `127.0.0.1:<advertised>`, so a peer dials the proxy and the child
//! stamps that address as its `origin`. The proxy reads the `X-Matrix origin`
//! header and, if the unordered pair `{self, origin}` is in the shared cut-set,
//! replies `503` (retryable — the sender parks in its outbox and redelivers on
//! heal); otherwise it forwards to the backend. The proxy outlives child
//! crash/revive, so the advertised endpoint is stable while the child is dead.
//!
//! Drive CSAPI over the [`Harness::backend`] port directly (never partitioned);
//! the typed CSAPI/await helpers do exactly that.
//!
//! ## Crash / revive / heal
//!
//! * **crash** = SIGKILL the child's process group (real abrupt kill → genuine
//!   WAL + `synchronous=NORMAL` durability).
//! * **revive** = re-spawn the binary with the same env (same backend port and
//!   parent-owned storage dir, which survives the kill).
//! * **heal** = clear the cut pair and SIGUSR2 the live children (→ `KickBackoff`,
//!   so a healed link redrains promptly). Set `NEUTRINO_STARTUP_JITTER_MS=0` (done
//!   here) so a revived server redrains immediately.

use std::collections::HashSet;
use std::net::TcpListener as StdTcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
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

/// Spawn the `neutrino` binary at `bin` as a child in its own process group (so a
/// parent panic can't orphan it), configured entirely by env. The child's
/// stdout+stderr are appended to `log` so a startup failure (e.g. a bind error,
/// which the binary prints as `Error: …` to stderr and exits) is visible rather
/// than guessed at — [`Harness::await_ready`] dumps this on timeout.
fn spawn_neutrino(
    bin: &Path,
    server_name: &str,
    backend: &str,
    storage: &Path,
    log: &Path,
) -> Child {
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .expect("open child log");
    let err = out.try_clone().expect("clone child log fd");
    Command::new(bin)
        .env("NEUTRINO_SERVER_NAME", server_name)
        .env("NEUTRINO_BIND_ADDR", backend)
        .env("NEUTRINO_STORAGE_DIR", storage)
        // No startup jitter in tests: a revived server redrains its outbox at once.
        .env("NEUTRINO_STARTUP_JITTER_MS", "0")
        // Synthesised delivery receipts on, so a test can assert them. Inert for
        // every other test: the receipts extension is opt-in per request, and
        // only `sync_receipts` opts in.
        .env("NEUTRINO_DELIVERY_RECEIPTS", "1")
        .env("RUST_LOG", "info")
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .process_group(0)
        .spawn()
        .expect("spawn neutrino")
}

/// Send `sig` to the process (positive pid).
fn signal(pid: u32, sig: i32) {
    // SAFETY: `kill(2)` with a constant signal; an invalid/dead pid just returns ESRCH.
    unsafe {
        libc::kill(pid as i32, sig);
    }
}

/// SIGKILL/SIGUSR2 a child's whole process group (negative pid). The child was
/// spawned with `process_group(0)`, so its pid is its pgid.
fn signal_group(child: &Child, sig: i32) {
    // SAFETY: negative pid targets the process group; a dead group yields ESRCH.
    unsafe {
        libc::kill(-(child.id() as i32), sig);
    }
}

struct Server {
    /// Advertised `server_name` (`127.0.0.1:<advertised-port>`) — dialed by peers,
    /// stamped as this server's federation `origin`.
    name: String,
    /// `127.0.0.1:<backend-port>` — the child's real router, where CSAPI is driven.
    /// Shared with this server's proxy and re-pointed on `revive` (a revived child
    /// takes a *fresh* port — reusing the old one races TIME_WAIT on macOS/BSD).
    backend: Arc<Mutex<String>>,
    /// The running child, or `None` between crash and revive.
    child: Option<Child>,
    /// Parent-owned storage dir; survives child crash/revive (the durability point).
    storage: tempfile::TempDir,
    /// The child's combined stdout+stderr log (appended across crash/revive).
    log: PathBuf,
}

pub struct Harness {
    bin: PathBuf,
    servers: Vec<Server>,
    cut: CutSet,
    http: reqwest::Client,
    deadline: Duration,
    txn: AtomicU64,
}

/// Install rustls' ring crypto provider as the process default, once.
///
/// reqwest has no TLS backend in this workspace, but a composed build can
/// feature-unify it onto rustls with NO default crypto provider — building a
/// `reqwest::Client` then panics unless a provider is installed first. This harness
/// depends on no other neutrino crate (it spawns a binary), so it installs the
/// provider itself rather than reusing `neutrino_lb::install_crypto_provider`.
/// Idempotent (`install_default` is a no-op if one is already set); the `Once`
/// keeps repeat calls cheap.
fn install_crypto_provider() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl Harness {
    /// Start `n` servers from the `neutrino` binary at `bin`, all links up, and
    /// wait until each serves through its proxy.
    pub async fn start(n: usize, bin: &Path) -> Harness {
        let cut: CutSet = Arc::new(Mutex::new(HashSet::new()));
        install_crypto_provider();
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
            let backend_str = format!("127.0.0.1:{}", free_port());
            let backend = Arc::new(Mutex::new(backend_str.clone()));
            let storage = tempfile::TempDir::new().expect("storage tempdir");
            let log = storage.path().join("server.log");

            let child = spawn_neutrino(bin, &name, &backend_str, storage.path(), &log);

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
                log,
            });
        }
        let h = Harness {
            bin: bin.to_path_buf(),
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

    /// Override the poll deadline used by the `await_*` helpers (default 20s).
    pub fn set_deadline(&mut self, d: Duration) {
        self.deadline = d;
    }

    /// Advertised `server_name` of server `i` (use as a join's resident, etc.).
    pub fn name(&self, i: usize) -> &str {
        &self.servers[i].name
    }

    /// Path to server `i`'s captured stdout+stderr log (one file per server; they
    /// never interleave across servers). Lives for the harness's lifetime, so you
    /// can `tail -f` it during a run; removed when the harness drops.
    pub fn log_path(&self, i: usize) -> &Path {
        &self.servers[i].log
    }

    fn backend_of(&self, i: usize) -> String {
        self.servers[i].backend.lock().expect("backend").clone()
    }

    /// `@alice:<server_name>` — each server acts as its single default user.
    pub fn mxid(&self, i: usize) -> String {
        format!("@alice:{}", self.servers[i].name)
    }

    // ---- topology + lifecycle ----------------------------------------------

    pub fn cut(&self, i: usize, j: usize) {
        let p = pair(self.name(i), self.name(j));
        self.cut.lock().expect("cut-set").insert(p);
    }

    pub fn heal(&self, i: usize, j: usize) {
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
    pub fn crash(&mut self, i: usize) {
        if let Some(mut child) = self.servers[i].child.take() {
            signal_group(&child, libc::SIGKILL);
            let _ = child.wait(); // reap the zombie
        }
    }

    /// Re-spawn a crashed server (same advertised name + storage dir, so its
    /// identity and committed state are unchanged) on a **fresh** backend port,
    /// re-pointing its proxy. Reusing the old port races TIME_WAIT from the
    /// crashed server's federation connections — fine on Linux, but macOS/BSD
    /// rejects the rebind. Then wait until it serves again.
    pub async fn revive(&mut self, i: usize) {
        if self.servers[i].child.is_none() {
            let backend = format!("127.0.0.1:{}", free_port());
            *self.servers[i].backend.lock().expect("backend") = backend.clone();
            let child = spawn_neutrino(
                &self.bin,
                &self.servers[i].name,
                &backend,
                self.servers[i].storage.path(),
                &self.servers[i].log,
            );
            self.servers[i].child = Some(child);
        }
        self.await_ready(i).await;
    }

    // ---- CSAPI (always over the backend port — never partitioned) -----------

    /// Issue a CSAPI request to server `i`'s backend; returns `(status, body)`,
    /// or `(0, Null)` on a transport error so callers can poll a crashed or
    /// still-booting peer without panicking.
    pub async fn request(
        &self,
        i: usize,
        method: reqwest::Method,
        path: &str,
        body: Value,
    ) -> (u16, Value) {
        let url = format!("http://{}/{}", self.backend_of(i), path);
        match self.http.request(method, &url).json(&body).send().await {
            Ok(resp) => {
                let st = resp.status().as_u16();
                let val = resp.json::<Value>().await.unwrap_or(Value::Null);
                (st, val)
            }
            Err(_) => (0, Value::Null),
        }
    }

    pub async fn create_room(&self, i: usize, name: &str) -> String {
        let (st, val) = self
            .request(
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

    pub async fn invite(&self, i: usize, room: &str, target: &str) -> u16 {
        self.request(
            i,
            reqwest::Method::POST,
            &format!("_matrix/client/v3/rooms/{room}/invite"),
            json!({ "user_id": target }),
        )
        .await
        .0
    }

    /// `POST /join/{room}?server_name=<resident>` — join via a resident server.
    pub async fn join(&self, i: usize, room: &str, resident: &str) -> u16 {
        // `resident` is `127.0.0.1:<port>` — only query-safe chars, so inline it
        // (this reqwest feature set has no `RequestBuilder::query`).
        let url = format!(
            "http://{}/_matrix/client/v3/join/{room}?server_name={resident}",
            self.backend_of(i)
        );
        // Transport-tolerant like `request`: 0 on a connection error so a caller
        // polling a crashed/booting peer doesn't panic.
        match self.http.post(&url).json(&json!({})).send().await {
            Ok(resp) => resp.status().as_u16(),
            Err(_) => 0,
        }
    }

    pub async fn leave(&self, i: usize, room: &str) -> u16 {
        self.request(
            i,
            reqwest::Method::POST,
            &format!("_matrix/client/v3/rooms/{room}/leave"),
            json!({}),
        )
        .await
        .0
    }

    pub async fn set_name(&self, i: usize, room: &str, name: &str) -> u16 {
        self.request(
            i,
            reqwest::Method::PUT,
            &format!("_matrix/client/v3/rooms/{room}/state/m.room.name"),
            json!({ "name": name }),
        )
        .await
        .0
    }

    pub async fn send_message(&self, i: usize, room: &str, body: &str) -> u16 {
        let txn = self.txn.fetch_add(1, Ordering::Relaxed);
        self.request(
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
    pub async fn set_power(&self, i: usize, room: &str, target: &str, level: i64) -> u16 {
        let path = format!("_matrix/client/v3/rooms/{room}/state/m.room.power_levels");
        let (st, mut content) = self
            .request(i, reqwest::Method::GET, &path, Value::Null)
            .await;
        if !(200..300).contains(&st) {
            return st;
        }
        if !content.get("users").map(Value::is_object).unwrap_or(false) {
            content["users"] = json!({});
        }
        content["users"][target] = json!(level);
        self.request(i, reqwest::Method::PUT, &path, content)
            .await
            .0
    }

    // ---- reads --------------------------------------------------------------

    /// Resolved `/state` array, or `None` if unreadable.
    pub async fn state(&self, i: usize, room: &str) -> Option<Value> {
        let (st, val) = self
            .request(
                i,
                reqwest::Method::GET,
                &format!("_matrix/client/v3/rooms/{room}/state"),
                Value::Null,
            )
            .await;
        ((200..300).contains(&st)).then_some(val)
    }

    /// Sorted `(type, state_key, event_id)` triples for one server.
    pub async fn state_map(&self, i: usize, room: &str) -> Option<Vec<(String, String, String)>> {
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

    pub async fn current_name(&self, i: usize, room: &str) -> Option<String> {
        let arr = self.state(i, room).await?;
        arr.as_array()?
            .iter()
            .rev()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("m.room.name"))
            .and_then(|e| e.get("content")?.get("name")?.as_str())
            .map(str::to_owned)
    }

    pub async fn membership(&self, i: usize, room: &str, mxid: &str) -> String {
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
    pub async fn current_power(&self, i: usize, room: &str, target: &str) -> Option<i64> {
        let arr = self.state(i, room).await?;
        arr.as_array()?
            .iter()
            .rev()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("m.room.power_levels"))
            .and_then(|e| e.get("content")?.get("users")?.get(target)?.as_i64())
    }

    /// `i`'s sliding-sync timeline for `room` (initial sync, no `pos`, large
    /// `timeline_limit`) — same coverage as `/messages`, including state PDUs.
    pub async fn sync_timeline(&self, i: usize, room: &str) -> Vec<Value> {
        let body =
            json!({ "lists": { "default": { "ranges": [[0, 99]], "timeline_limit": 1000 } } });
        let (st, val) = self.request(i, reqwest::Method::POST, SYNC, body).await;
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

    /// `i`'s synthesised delivery receipts for `room` — the `m.receipt` content
    /// (`{event_id: {"m.read": {user: {ts}}}}`) from the receipts extension, or
    /// `None` if there are none yet.
    ///
    /// Each call is a fresh connection (no `pos`), so it always reports the
    /// server's *current* marks rather than a delta — which is what a polling
    /// assertion wants.
    pub async fn sync_receipts(&self, i: usize, room: &str) -> Option<Value> {
        let body = json!({
            "lists": { "default": { "ranges": [[0, 99]], "timeline_limit": 1 } },
            "extensions": { "receipts": { "enabled": true } },
        });
        let (st, val) = self.request(i, reqwest::Method::POST, SYNC, body).await;
        if !(200..300).contains(&st) {
            return None;
        }
        val.get("extensions")?
            .get("receipts")?
            .get("rooms")?
            .get(room)?
            .get("content")
            .cloned()
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
            if Instant::now() >= deadline {
                panic!(
                    "server {i} never became ready; last child log:\n{}",
                    self.child_log_tail(i)
                );
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// The tail (last ~40 lines) of server `i`'s captured stdout+stderr — surfaced
    /// when it fails to come up, so a startup/bind error is visible not guessed.
    pub fn child_log_tail(&self, i: usize) -> String {
        let text = std::fs::read_to_string(&self.servers[i].log).unwrap_or_default();
        if text.is_empty() {
            return "(child log empty)".to_owned();
        }
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(40);
        lines[start..].join("\n")
    }

    pub async fn await_membership(&self, i: usize, room: &str, mxid: &str, want: &str) {
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

    pub async fn await_name(&self, i: usize, room: &str, want: &str) {
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

    pub async fn await_power(&self, i: usize, room: &str, target: &str, level: i64) {
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
    pub async fn await_converged(&self, room: &str, who: &[usize]) {
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
    pub async fn timeline_has(&self, i: usize, room: &str, pred: impl Fn(&Value) -> bool) -> bool {
        self.sync_timeline(i, room).await.iter().any(&pred)
    }

    /// Wait until `i`'s sliding-sync timeline for `room` contains an event
    /// matching `pred`. `desc` names the wait for the panic message.
    pub async fn await_timeline(
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

#[derive(Clone)]
struct ProxyState {
    /// Re-pointed on `revive` (shared with the `Server`), so the proxy always
    /// forwards to the live child's current backend port.
    backend: Arc<Mutex<String>>,
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
    let backend = st.backend.lock().expect("backend").clone();
    let url = format!("http://{backend}{path_and_query}");
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
