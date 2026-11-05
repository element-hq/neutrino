//! End-to-end: a grouped `/send` from our federation client's wire shape, through
//! one `neutrino-lb` egress, over a link with a group address, into a homeserver
//! behind another `neutrino-lb` ingress.
//!
//! The group header crosses two crates under two spellings: lb strips it into
//! the wire request on egress, carries it as the CoAP Group option, and rebuilds
//! it on ingress for the `/send` handler. A homeserver outside the transaction's
//! room answers that handler's 404, which lb keeps off the link, so the sender's
//! exchange times out. If the header is lost anywhere on the way, the homeserver
//! answers 200 instead.
#![cfg(not(feature = "multi-user-shim"))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use neutrino_ctl::{Config, DiscoveryRegistry};
use neutrino_lb::{DatagramLink, LinkAddr};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const GROUP: &[u8] = b"group";

/// A datagram and the link address it came from.
type Datagram = (LinkAddr, Vec<u8>);

/// A broadcast medium: a send to [`GROUP`] reaches every other node, from the
/// sender's address.
#[derive(Default)]
struct Mesh {
    nodes: Mutex<HashMap<LinkAddr, mpsc::UnboundedSender<Datagram>>>,
}

struct MeshLink {
    node: LinkAddr,
    mesh: Arc<Mesh>,
    inbound: tokio::sync::Mutex<mpsc::UnboundedReceiver<Datagram>>,
}

impl MeshLink {
    fn join(mesh: &Arc<Mesh>, node: &str) -> Arc<MeshLink> {
        let (tx, rx) = mpsc::unbounded_channel();
        mesh.nodes
            .lock()
            .unwrap()
            .insert(node.as_bytes().to_vec(), tx);
        Arc::new(MeshLink {
            node: node.as_bytes().to_vec(),
            mesh: mesh.clone(),
            inbound: tokio::sync::Mutex::new(rx),
        })
    }
}

#[async_trait]
impl DatagramLink for MeshLink {
    async fn send(&self, dst: &[u8], datagram: &[u8]) -> std::io::Result<()> {
        for (node, tx) in self.mesh.nodes.lock().unwrap().iter() {
            if node == dst || (dst == GROUP && *node != self.node) {
                let _ = tx.send((self.node.clone(), datagram.to_vec()));
            }
        }
        Ok(())
    }

    async fn recv(&self) -> Option<Datagram> {
        self.inbound.lock().await.recv().await
    }

    fn group_addr(&self, dst: &[u8]) -> Option<LinkAddr> {
        let known = self.mesh.nodes.lock().unwrap().contains_key(dst);
        known.then(|| GROUP.to_vec())
    }
}

async fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

/// An lb over `link` that forwards inbound requests to `upstream`, and whose
/// egress is returned.
fn start_lb(link: Arc<MeshLink>, upstream: String, shutdown: &CancellationToken) -> SocketAddr {
    let egress = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let lb = neutrino_lb::LbConfig {
        ingress_bind: "127.0.0.1:0".parse().unwrap(),
        egress_bind: egress,
        upstream,
        wire: neutrino_lb::WireKind::CoapQBlock {
            block1_size: None,
            qblock: neutrino_lb::QBlockTuning {
                non_receive_timeout: Duration::from_millis(500),
                ..Default::default()
            },
        },
        resolver: None,
        link: Some(link),
        capture: None,
    };
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = neutrino_lb::serve(lb, shutdown).await;
    });
    egress
}

#[tokio::test]
async fn the_group_header_reaches_send_through_lb() {
    let mesh = Arc::new(Mesh::default());
    let shutdown = CancellationToken::new();
    let _shutdown = shutdown.clone().drop_guard();

    // `b` runs a homeserver; `c` is a second destination that never answers.
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let tmp = tempfile::TempDir::new().unwrap();
    let config = Config {
        server_name: "b".to_owned(),
        bind_addr: "127.0.0.1:0".to_owned(),
        localpart: "bob".to_owned(),
        storage_dir: tmp.path().to_path_buf(),
        ..Default::default()
    };
    let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let store = Arc::new(
            neutrino_store_sqlite::SqliteStore::open_in_dir(&config.storage_dir)
                .await
                .unwrap(),
        );
        let _ = neutrino_http::serve(
            http_listener,
            config,
            store,
            cmd_rx,
            Arc::new(DiscoveryRegistry::new()),
            None,
            neutrino_event::EventPolicy::trusted_network(),
        )
        .await;
    });
    start_lb(
        MeshLink::join(&mesh, "b"),
        format!("http://{http_addr}"),
        &shutdown,
    );
    let _c = MeshLink::join(&mesh, "c");
    let egress = start_lb(
        MeshLink::join(&mesh, "a"),
        format!("http://{}", free_port().await),
        &shutdown,
    );
    tokio::time::sleep(Duration::from_millis(250)).await;

    neutrino_lb::install_crypto_provider();
    let http = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all(format!("http://{egress}")).unwrap())
        .build()
        .unwrap();
    let send = |dest: &'static str, txn: &'static str, group: Option<&'static str>| {
        let mut req = http
            .put(format!("http://{dest}/_matrix/federation/v1/send/{txn}"))
            .header(
                "Authorization",
                format!(r#"X-Matrix origin="a",destination="{dest}""#),
            )
            .json(&json!({
                "origin": "a",
                "origin_server_ts": 0,
                "pdus": [{ "room_id": "!elsewhere:a", "type": "m.room.message" }],
            }));
        if let Some(group) = group {
            req = req.header("X-Neutrino-Group", group);
        }
        async move { req.send().await.expect("proxied request").status() }
    };

    // Ungrouped, `b` accepts the transaction.
    assert_eq!(send("b", "plain", None).await, 200);

    // Grouped, `b` is outside the room: its 404 stays off the link, and the
    // egress reports the silent exchange as a wire failure.
    let (b, c) = tokio::join!(
        send("b", "grouped", Some("2")),
        send("c", "grouped", Some("2")),
    );
    assert_eq!(b, 502, "the group header did not reach /send");
    assert_eq!(c, 502);
}
