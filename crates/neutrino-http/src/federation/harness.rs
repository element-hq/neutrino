//! In-process multi-server federation test harness.
//!
//! Stands up N real homeservers in one process — each a genuine [`crate::serve`]
//! (real `SqliteStore`, real spawned outbound sender, real reqwest
//! `FederationClient`) bound on a loopback port — and runs real federation HTTP
//! between them. No docker, no `nctl`: it runs as a plain `cargo test`.
//!
//! ## Two ports per server, and how partitions work
//!
//! Each server gets two loopback ports:
//! * a **backend** port serving the real router, and
//! * an **advertised** port serving a tiny toggleable reverse proxy.
//!
//! `config.server_name` is set to the *advertised* `127.0.0.1:<advertised>`, so a
//! peer's `FederationClient` dials the proxy, and the server stamps that address
//! as its `origin`. The proxy reads the `X-Matrix origin` header (every outbound
//! federation request carries it), and if the unordered pair `{self, origin}` is
//! in the shared [`CutSet`] it replies `503` (a retryable status — the sender
//! parks the transaction in its durable outbox and redelivers on heal); otherwise
//! it forwards to the backend. A "cut" is thus a real, directional-aware,
//! toggleable link, the in-process analogue of `nctl partition`.
//!
//! The harness drives each server's **CSAPI over the backend port directly**, so
//! a partition never blinds the test (the god view always sees through cuts).
//! That also means a server can be fully isolated (both its links cut) without
//! losing its CSAPI — unlike the docker rig, where stripping a container of every
//! network kills its published port.
//!
//! ## Heal
//!
//! Healing clears the pair from the cut-set and sends [`Command::KickBackoff`] to
//! both endpoints' command channels, so their senders retry immediately rather
//! than waiting out accrued backoff — the in-process equivalent of `nctl`'s
//! heal-resets-backoff.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use neutrino_common::{Command, Config};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Unordered pairs of server names that are currently partitioned.
type CutSet = Arc<Mutex<HashSet<(String, String)>>>;

/// Canonical unordered key for a link between two server names.
fn pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

struct Server {
    /// Advertised `server_name` (`127.0.0.1:<advertised-port>`) — what peers dial
    /// and what this server stamps as its federation `origin`.
    name: String,
    /// `127.0.0.1:<backend-port>` — the real router, where the harness drives CSAPI.
    backend: String,
    /// Command channel into this server's `serve` loop (KickBackoff / Shutdown).
    commands: mpsc::UnboundedSender<Command>,
    /// Held for the server's lifetime: dropping it deletes the SQLite directory.
    _tmp: tempfile::TempDir,
}

pub(crate) struct Harness {
    servers: Vec<Server>,
    cut: CutSet,
    http: reqwest::Client,
    deadline: Duration,
}

impl Harness {
    /// Start `n` in-process servers, all links initially up, and wait until each
    /// is serving (through its proxy).
    pub(crate) async fn start(n: usize) -> Harness {
        let cut: CutSet = Arc::new(Mutex::new(HashSet::new()));
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("reqwest client");
        let mut servers = Vec::with_capacity(n);
        for _ in 0..n {
            servers.push(spawn_server(cut.clone()).await);
        }
        let h = Harness {
            servers,
            cut,
            http,
            deadline: Duration::from_secs(20),
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

    // ---- topology -----------------------------------------------------------

    pub(crate) fn cut(&self, i: usize, j: usize) {
        let p = pair(self.name(i), self.name(j));
        self.cut.lock().expect("cut-set").insert(p);
    }

    pub(crate) fn heal(&self, i: usize, j: usize) {
        let p = pair(self.name(i), self.name(j));
        self.cut.lock().expect("cut-set").remove(&p);
        // Reset both senders' backoff so the healed link redrains promptly.
        let _ = self.servers[i].commands.send(Command::KickBackoff);
        let _ = self.servers[j].commands.send(Command::KickBackoff);
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

    /// `(type, state_key) -> event_id` map for one server.
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
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Best-effort: ask each server to wind down so its tasks stop and the
        // tempdir can be removed. The test process exits regardless.
        for s in &self.servers {
            let _ = s.commands.send(Command::Shutdown);
        }
    }
}

/// Bind a backend + advertised port, start a real `serve` on the backend and the
/// toggleable proxy on the advertised port, and return the handle.
async fn spawn_server(cut: CutSet) -> Server {
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend");
    let backend = backend_listener
        .local_addr()
        .expect("backend addr")
        .to_string();
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let name = proxy_listener.local_addr().expect("proxy addr").to_string();

    let tmp = tempfile::TempDir::new().expect("storage tempdir");
    let config = Config {
        server_name: name.clone(),
        bind_addr: backend.clone(),
        localpart: "alice".to_owned(),
        storage_dir: tmp.path().to_path_buf(),
        ..Default::default()
    };
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _ = crate::serve(backend_listener, config, rx).await;
    });

    let proxy = Router::new()
        .fallback(proxy_handler)
        .with_state(ProxyState {
            backend: backend.clone(),
            me: name.clone(),
            cut,
            client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("proxy client"),
        });
    tokio::spawn(async move {
        let _ = axum::serve(proxy_listener, proxy).await;
    });

    Server {
        name,
        backend,
        commands: tx,
        _tmp: tmp,
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
        // Let reqwest recompute these for the rewritten request.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Directed reproduction of the joined-set-growth advertisement
    /// (`anti-entropy-extension.md`), as a real-federation integration test.
    ///
    /// Background: a server only sends an event to the servers it currently sees
    /// as joined. The base anti-entropy mechanism repairs any miss by piggybacking
    /// its latest events on the next `/send` to that server — but if there is no
    /// next `/send` (the room goes quiet), the miss is permanent. The extension
    /// fixes this by sending an advertisement when a server newly becomes joined.
    /// This test sets up exactly that permanent miss and checks the advertisement
    /// repairs it. Three servers: `H` = holder, `L` = laggard, `R` = resident.
    ///
    /// - `H`, `L`, `R` are all joined to the room.
    /// - `L` leaves the room (`$Lv`). `H` and `R` both see the leave.
    /// - Cut the `H–R` link.
    /// - `H` sets the room name (`$E`). It is sent to no one: `R` is cut off, and
    ///   `L` already left, so `H`'s fan-out skips it — even though `H–L` is up.
    /// - `L` rejoins, via `R`. `R` never saw `$E`, so the rejoin `$J` is built on
    ///   the old state — `$J` and `$E` are concurrent (neither is built on the
    ///   other).
    /// - Cut the `L–R` link too. Now `L`'s only live link is `H–L`.
    /// - Check: `L` still does not have `$E`, even though `H–L` was never cut. The
    ///   base mechanism did not deliver it (`H` never sent `$E` to `L`, because
    ///   `L` was not joined when `$E` was created, and the room is now quiet).
    /// - Heal the `H–R` link. `H` now receives `L`'s rejoin `$J` and applies it,
    ///   so `L` becomes joined in `H`'s view.
    /// - `L` newly becoming joined, while `H` holds the concurrent `$E`, is what
    ///   triggers the advertisement: `H` tells `L` its latest events over `H–L`
    ///   (`L`'s only link). `L` sees it is missing `$E`, fetches it, and applies it.
    /// - Check: `L`'s room name is now `$E` — delivered solely by the
    ///   advertisement. Without the extension this step never happens.
    /// - Heal the `L–R` link. All three servers agree the room name is `$E`.
    #[tokio::test]
    async fn tail_convergence_advertises_on_joined_set_growth() {
        const H: usize = 0;
        const L: usize = 1;
        const R: usize = 2;
        let e_name = "tail-converge-E";

        let h = Harness::start(3).await;
        let room = h.create_room(H, "init").await;
        assert!(matches!(h.invite(H, &room, &h.mxid(L)).await, 200..=299));
        assert!(matches!(h.invite(H, &room, &h.mxid(R)).await, 200..=299));
        assert!(matches!(h.join(L, &room, h.name(H)).await, 200..=299));
        assert!(matches!(h.join(R, &room, h.name(H)).await, 200..=299));
        h.await_converged(&room, &[H, L, R]).await;

        // (1) L leaves; both holders apply it so $E and $J build on the leave.
        assert!(matches!(h.leave(L, &room).await, 200..=299));
        let l_mxid = h.mxid(L);
        h.await_membership(H, &room, &l_mxid, "leave").await;
        h.await_membership(R, &room, &l_mxid, "leave").await;

        // (2) cut H–R; H authors the lone extremity; L rejoins via R (sibling $J).
        h.cut(H, R);
        assert!(matches!(h.set_name(H, &room, e_name).await, 200..=299));
        assert!(matches!(h.join(L, &room, h.name(R)).await, 200..=299));
        h.await_membership(L, &room, &l_mxid, "join").await;
        h.await_membership(R, &room, &l_mxid, "join").await;

        // (3) isolate L to H–L only, so the advertisement is its sole delivery path.
        h.cut(L, R);

        // (4) H–L has been up the whole time, yet L must still lack $E. Pin both
        // sides: H holds $E, L still has the pre-$E name "init" (a readable,
        // specific value — not a vacuous `None` from an unreadable state). This is
        // the partition genuinely biting: H and L diverge on the name.
        assert_eq!(h.current_name(H, &room).await.as_deref(), Some(e_name));
        assert_eq!(
            h.current_name(L, &room).await.as_deref(),
            Some("init"),
            "L should still hold the pre-$E name; if it has $E the base mechanism delivered it"
        );

        // (5) heal H–R: H learns $J, advertises the concurrent $E to L over H–L.
        h.heal(H, R);
        h.await_membership(H, &room, &l_mxid, "join").await; // trigger fired
        h.await_name(L, &room, e_name).await; // <-- the advertisement delivered $E

        // (6) full topology; everyone agrees on the advertised name.
        h.heal(L, R);
        h.await_converged(&room, &[H, L, R]).await;
        for i in [H, L, R] {
            assert_eq!(h.current_name(i, &room).await.as_deref(), Some(e_name));
        }
    }
}
