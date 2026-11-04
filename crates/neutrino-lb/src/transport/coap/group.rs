//! Group requests: the requests the engine marks as one group
//! ([`WireRequest::group_size`]) run as ordinary exchanges, each on its own
//! peer's client, whose first transmission is sent once, to the link's group
//! address.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use axum::http::Method;
use coap::qblock::{BlockSink, GROUP_OPTION, SharedFirstBurst};
use coap_lite::CoapRequest;
use coap_lite::option_value::OptionValueU32;
use std::net::SocketAddr;
use tokio::sync::{Notify, oneshot};

use super::datagram::{Hub, LinkAddr, LinkCoapWireClient};
use super::{SharedExchange, message};
use crate::transport::{WireError, WireRequest, WireResponse};

/// How long the first request of a group waits for the rest. It bounds the
/// scheduling skew between the engine's per-destination tasks.
// TODO: measure the engine's task skew on device and set this from it.
pub(super) const GATHER_TIMEOUT: Duration = Duration::from_millis(250);

/// A group's requests share their method and path, which carries the txn id.
type GatherKey = (Method, String);

/// The groups being gathered or exchanged.
#[derive(Clone, Default)]
pub(super) struct Gathers(Arc<Mutex<HashMap<GatherKey, Gather>>>);

impl Gathers {
    fn lock(&self) -> MutexGuard<'_, HashMap<GatherKey, Gather>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

enum Gather {
    Waiting {
        expected: usize,
        members: Vec<Member>,
        full: Arc<Notify>,
    },
    /// The group exchange is running: later arrivals run ordinary.
    Started,
}

/// A gathered request, and where to send its turn.
struct Member {
    req: WireRequest,
    group_addr: LinkAddr,
    turn: oneshot::Sender<Turn>,
}

/// How a gathered request runs.
enum Turn {
    Ordinary(WireRequest),
    Member {
        dest: String,
        coap_req: CoapRequest<SocketAddr>,
        shared: SharedExchange,
        end: Arc<GroupEnd>,
    },
}

/// Removes its group's entry once the last member's exchange ends.
struct GroupEnd {
    gathers: Gathers,
    key: GatherKey,
}

impl Drop for GroupEnd {
    fn drop(&mut self) {
        self.gathers.lock().remove(&self.key);
    }
}

/// What a request found when it arrived to be gathered.
enum Arrival {
    /// It opened the gather, and is told when it is full.
    First(Arc<Notify>),
    Gathered,
    /// Its group already started.
    Late(WireRequest),
}

/// The far ends of one member's [`SharedFirstBurst`].
struct Sharer {
    registered: oneshot::Receiver<()>,
    sent: oneshot::Sender<std::io::Result<()>>,
}

/// Sends every block to the group address.
struct GroupSink {
    hub: Arc<Hub>,
    addr: LinkAddr,
}

#[async_trait]
impl BlockSink for GroupSink {
    async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
        self.hub.link().send(&self.addr, &pdu).await
    }
}

impl LinkCoapWireClient {
    /// Sends `req`, one of `expected` requests that share a transmission to
    /// `group_addr`.
    ///
    /// The first arrival gathers the rest for up to [`GATHER_TIMEOUT`], then
    /// hands every member its turn: a member of the group exchange, or an
    /// ordinary request when its body or group address differs from the
    /// first's, or the group would be smaller than two. A request arriving
    /// after the group started, until its exchange ends, runs ordinary.
    pub(super) async fn send_grouped(
        &self,
        req: WireRequest,
        expected: usize,
        group_addr: LinkAddr,
    ) -> Result<WireResponse, WireError> {
        let key = (req.method.clone(), req.path.clone());
        let (turn, my_turn) = oneshot::channel();
        let member = Member {
            req,
            group_addr,
            turn,
        };
        let arrival = {
            let mut gathers = self.gathers.lock();
            match gathers.get_mut(&key) {
                None => {
                    let full = Arc::new(Notify::new());
                    if expected <= 1 {
                        full.notify_one();
                    }
                    gathers.insert(
                        key.clone(),
                        Gather::Waiting {
                            expected,
                            members: vec![member],
                            full: full.clone(),
                        },
                    );
                    Arrival::First(full)
                }
                Some(Gather::Waiting {
                    expected,
                    members,
                    full,
                }) => {
                    members.push(member);
                    if members.len() >= *expected {
                        full.notify_one();
                    }
                    Arrival::Gathered
                }
                Some(Gather::Started) => Arrival::Late(member.req),
            }
        };
        let full = match arrival {
            Arrival::First(full) => full,
            Arrival::Gathered => return self.run_turn(my_turn).await,
            Arrival::Late(req) => return self.send_ordinary(req).await,
        };

        let end = Arc::new(GroupEnd {
            gathers: self.gathers.clone(),
            key: key.clone(),
        });
        let _ = tokio::time::timeout(GATHER_TIMEOUT, full.notified()).await;
        let Some(Gather::Waiting { members, .. }) =
            self.gathers.lock().insert(key, Gather::Started)
        else {
            return Err(WireError::Transport("group gather vanished".to_owned()));
        };

        let mut members = members.into_iter();
        let Some(first) = members.next() else {
            return Err(WireError::Transport("group gather vanished".to_owned()));
        };
        let (mut group, mut ordinary): (Vec<_>, Vec<_>) =
            members.partition(|m| m.req.body == first.req.body && m.group_addr == first.group_addr);
        group.insert(0, first);
        let coap_req = if group.len() >= 2 {
            self.group_request(&group[0].req)
        } else {
            None
        };
        let Some(coap_req) = coap_req else {
            ordinary.append(&mut group);
            for m in ordinary {
                let _ = m.turn.send(Turn::Ordinary(m.req));
            }
            return self.run_turn(my_turn).await;
        };
        for m in ordinary {
            let _ = m.turn.send(Turn::Ordinary(m.req));
        }

        let size = group.len();
        tracing::debug!(path = %end.key.1, size, "datagram wire: sending group request");
        let deadline_extra = self.qblock.as_ref().map_or(Duration::ZERO, |q| {
            q.leisure(u32::try_from(size).unwrap_or(u32::MAX))
        });
        let client = self.client_for(group[0].req.dest.as_bytes()).await;
        let group_addr = group[0].group_addr.clone();
        let mut sharers = Vec::with_capacity(size);
        for m in group {
            let (registered, registered_rx) = oneshot::channel();
            let (sent_tx, sent) = oneshot::channel();
            let turn = Turn::Member {
                dest: m.req.dest,
                coap_req: coap_req.clone(),
                shared: SharedExchange {
                    burst: SharedFirstBurst { registered, sent },
                    deadline_extra,
                },
                end: end.clone(),
            };
            if m.turn.send(turn).is_ok() {
                sharers.push(Sharer {
                    registered: registered_rx,
                    sent: sent_tx,
                });
            }
        }
        drop(end);

        let send_copy = async {
            // No response can arrive before its exchange listens for it.
            for sharer in &mut sharers {
                let _ = (&mut sharer.registered).await;
            }
            let mut copy = coap_req;
            copy.message.add_option_as(
                GROUP_OPTION,
                OptionValueU32(u32::try_from(size).unwrap_or(u32::MAX)),
            );
            let sink = GroupSink {
                hub: self.hub.clone(),
                addr: group_addr,
            };
            let result = client.send_first_burst(copy, &sink).await;
            for sharer in sharers {
                let _ = sharer.sent.send(match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
                });
            }
        };
        let (result, ()) = tokio::join!(self.run_turn(my_turn), send_copy);
        result
    }

    /// The CoAP request the group shares, built from its first member's `req`,
    /// or `None` when it can't go out as one burst.
    fn group_request(&self, req: &WireRequest) -> Option<CoapRequest<SocketAddr>> {
        let mut req = req.clone();
        if let Some(codec) = &self.codec
            && let Err(e) = codec.encode_request(&mut req)
        {
            tracing::debug!(error = %e, "datagram wire: group request did not encode, running ordinary");
            return None;
        }
        let block = self
            .block1_size
            .unwrap_or(coap::client::UdpCoAPClient::MAX_PAYLOAD_BLOCK);
        let burst = self.qblock.as_ref()?.max_payloads as usize;
        if req.body.len() > block * burst {
            tracing::debug!(
                body_len = req.body.len(),
                "datagram wire: group request is larger than one burst, running ordinary"
            );
            return None;
        }
        let mut coap_req = message::build_request(&req);
        coap_req.message.set_token(self.next_token());
        Some(coap_req)
    }

    async fn run_turn(&self, turn: oneshot::Receiver<Turn>) -> Result<WireResponse, WireError> {
        match turn.await {
            Ok(Turn::Ordinary(req)) => self.send_ordinary(req).await,
            Ok(Turn::Member {
                dest,
                coap_req,
                shared,
                end: _end,
            }) => self.send_member(&dest, coap_req, shared).await,
            Err(_) => Err(WireError::Transport("group gather abandoned".to_owned())),
        }
    }
}
