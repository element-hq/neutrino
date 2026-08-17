#[cfg(feature = "router")]
use crate::router::{request::Request, Router};
use async_trait::async_trait;
use coap_lite::{BlockHandler, BlockHandlerConfig, CoapOption, CoapRequest, CoapResponse, Packet};
use log::debug;
use std::{
    future::Future,
    net::{self, IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{
    io,
    net::UdpSocket,
    select,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
};

use crate::observer::{encode_coap_uint, Observer};

#[cfg(feature = "q-block")]
use crate::qblock::{
    drive_receive, drive_send, parse_missing_request, QBlockConfig, QBlockReceiver, QBlockSender,
    ResponderSink, TransferKind,
};
#[cfg(feature = "q-block")]
use coap_lite::{block_handler::BlockValue, MessageType};
#[cfg(feature = "q-block")]
use std::collections::HashMap;

#[derive(Debug)]
pub enum CoAPServerError {
    NetworkError,
    EventLoopError,
    AnotherHandlerIsRunning,
    EventSendError,
}

use tokio::io::Error;

#[async_trait]
pub trait Dispatcher: Send + Sync {
    async fn dispatch(&self, request: CoapRequest<SocketAddr>) -> Option<CoapResponse>;
}

#[async_trait]
/// This trait represents a generic way to respond to a listener. If you want to implement your own
/// listener, you have to implement this trait to be able to send responses back through the
/// correct transport
pub trait Responder: Sync + Send {
    async fn respond(&self, response: Vec<u8>);
    fn address(&self) -> SocketAddr;
}

/// channel to send new requests from a transport to the CoAP server
pub type TransportRequestSender = UnboundedSender<(Vec<u8>, Arc<dyn Responder>)>;

/// channel used by CoAP server to receive new requests
pub type TransportRequestReceiver = UnboundedReceiver<(Vec<u8>, Arc<dyn Responder>)>;

type UdpResponseReceiver = UnboundedReceiver<(Vec<u8>, SocketAddr)>;
type UdpResponseSender = UnboundedSender<(Vec<u8>, SocketAddr)>;

// listeners receive new connections
#[async_trait]
pub trait Listener: Send {
    async fn listen(
        self: Box<Self>,
        sender: TransportRequestSender,
    ) -> std::io::Result<JoinHandle<std::io::Result<()>>>;
}
/// listener for a UDP socket
pub struct UdpCoapListener {
    socket: UdpSocket,
    multicast_addresses: Vec<IpAddr>,
    response_receiver: UdpResponseReceiver,
    response_sender: UdpResponseSender,
}

#[async_trait]
/// A trait for handling incoming requests. Use this instead of a closure
/// if you want to modify some external state
pub trait RequestHandler: Send + Sync + 'static {
    async fn handle_request(
        &self,
        mut request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>>;
}

#[async_trait]
impl<F, HandlerRet> RequestHandler for F
where
    F: Fn(Box<CoapRequest<SocketAddr>>) -> HandlerRet + Send + Sync + 'static,
    HandlerRet: Future<Output = Box<CoapRequest<SocketAddr>>> + Send,
{
    async fn handle_request(
        &self,
        request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        self(request).await
    }
}

/// A listener for UDP packets. This listener can also subscribe to multicast addresses
impl UdpCoapListener {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let std_socket = net::UdpSocket::bind(addr)?;
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;
        Ok(Self::from_socket(socket))
    }

    pub fn from_socket(socket: tokio::net::UdpSocket) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            socket,
            multicast_addresses: Vec::new(),
            response_receiver: rx,
            response_sender: tx,
        }
    }

    /// join multicast - adds the multicast addresses to the unicast listener
    /// - IPv4 multicast address range is '224.0.0.0/4'
    /// - IPv6 AllCoAp multicast addresses are 'ff00::/8'
    ///
    /// Parameter segment is used with IPv6 to determine the first octet.
    /// - It's value can be between 0x0 and 0xf.
    /// - To join multiple segments, you have to call enable_discovery for each of the segments.
    ///
    /// Some Multicast address scope
    /// IPv6        IPv4 equivalent[16]            Scope                Purpose
    /// ffx1::/16    127.0.0.0/8                    Interface-local        Packets with this destination address may not be sent over any network link, but must remain within the current node; this is the multicast equivalent of the unicast loopback address.
    /// ffx2::/16    224.0.0.0/24                Link-local            Packets with this destination address may not be routed anywhere.
    /// ffx3::/16    239.255.0.0/16                IPv4 local scope
    /// ffx4::/16                                Admin-local            The smallest scope that must be administratively configured.
    /// ffx5::/16                                Site-local            Restricted to the local physical network.
    /// ffx8::/16    239.192.0.0/14                Organization-local    Restricted to networks used by the organization administering the local network. (For example, these addresses might be used over VPNs; when packets for this group are routed over the public internet (where these addresses are not valid), they would have to be encapsulated in some other protocol.)
    /// ffxe::/16    224.0.1.0-238.255.255.255    Global scope        Eligible to be routed over the public internet.
    ///
    /// Notable addresses:
    /// ff02::1        All nodes on the local network segment
    /// ff0x::c        Simple Service Discovery Protocol
    /// ff0x::fb    Multicast DNS
    /// ff0x::fb    Multicast CoAP
    /// ff0x::114    Used for experiments
    //    pub fn join_multicast(&mut self, addr: IpAddr) {
    //        self.udp_server.join_multicast(addr);
    //    }
    pub fn join_multicast(&mut self, addr: IpAddr) {
        assert!(addr.is_multicast());
        // determine wether IPv4 or IPv6 and
        // join the appropriate multicast address
        match self.socket.local_addr().unwrap() {
            SocketAddr::V4(val) => {
                match addr {
                    IpAddr::V4(ipv4) => {
                        let i = *val.ip();
                        self.socket.join_multicast_v4(ipv4, i).unwrap();
                        self.multicast_addresses.push(addr);
                    }
                    IpAddr::V6(_ipv6) => { /* handle IPv6 */ }
                }
            }
            SocketAddr::V6(_val) => {
                match addr {
                    IpAddr::V4(_ipv4) => { /* handle IPv4 */ }
                    IpAddr::V6(ipv6) => {
                        self.socket.join_multicast_v6(&ipv6, 0).unwrap();
                        self.multicast_addresses.push(addr);
                        //self.socket.set_only_v6(true)?;
                    }
                }
            }
        }
    }

    /// leave multicast - remove the multicast address from the listener
    pub fn leave_multicast(&mut self, addr: IpAddr) {
        assert!(addr.is_multicast());
        // determine wether IPv4 or IPv6 and
        // leave the appropriate multicast address
        match self.socket.local_addr().unwrap() {
            SocketAddr::V4(val) => {
                match addr {
                    IpAddr::V4(ipv4) => {
                        let i = *val.ip();
                        self.socket.leave_multicast_v4(ipv4, i).unwrap();
                        let index = self
                            .multicast_addresses
                            .iter()
                            .position(|&item| item == addr)
                            .unwrap();
                        self.multicast_addresses.remove(index);
                    }
                    IpAddr::V6(_ipv6) => { /* handle IPv6 */ }
                }
            }
            SocketAddr::V6(_val) => {
                match addr {
                    IpAddr::V4(_ipv4) => { /* handle IPv4 */ }
                    IpAddr::V6(ipv6) => {
                        self.socket.leave_multicast_v6(&ipv6, 0).unwrap();
                        let index = self
                            .multicast_addresses
                            .iter()
                            .position(|&item| item == addr)
                            .unwrap();
                        self.multicast_addresses.remove(index);
                    }
                }
            }
        }
    }
    /// enable AllCoAP multicasts - adds the AllCoap addresses to the listener
    /// - IPv4 AllCoAP multicast address is '224.0.1.187'
    /// - IPv6 AllCoAp multicast addresses are 'ff0?::fd'
    ///
    /// Parameter segment is used with IPv6 to determine the first octet.
    /// - It's value can be between 0x0 and 0xf.
    /// - To join multiple segments, you have to call enable_discovery for each of the segments.
    ///
    /// For further details see method join_multicast
    pub fn enable_all_coap(&mut self, segment: u8) {
        assert!(segment <= 0xf);
        let m = match self.socket.local_addr().unwrap() {
            SocketAddr::V4(_val) => IpAddr::V4(Ipv4Addr::new(224, 0, 1, 187)),
            SocketAddr::V6(_val) => IpAddr::V6(Ipv6Addr::new(
                0xff00 + segment as u16,
                0,
                0,
                0,
                0,
                0,
                0,
                0xfd,
            )),
        };
        self.join_multicast(m);
    }
}
#[async_trait]
impl Listener for UdpCoapListener {
    async fn listen(
        mut self: Box<Self>,
        sender: TransportRequestSender,
    ) -> std::io::Result<JoinHandle<std::io::Result<()>>> {
        return Ok(tokio::spawn(self.receive_loop(sender)));
    }
}

#[derive(Clone)]
struct UdpResponder {
    address: SocketAddr, // this is the address we are sending to
    tx: UdpResponseSender,
}

#[async_trait]
impl Responder for UdpResponder {
    async fn respond(&self, response: Vec<u8>) {
        let _ = self.tx.send((response, self.address));
    }
    fn address(&self) -> SocketAddr {
        self.address
    }
}

impl UdpCoapListener {
    pub async fn receive_loop(mut self, sender: TransportRequestSender) -> std::io::Result<()> {
        loop {
            let mut recv_vec = Vec::with_capacity(u16::MAX as usize);
            select! {
                message =self.socket.recv_buf_from(&mut recv_vec)=> {
                    match message {
                        Ok((_size, from)) => {
                            sender.send((recv_vec, Arc::new(UdpResponder{address: from, tx: self.response_sender.clone()}))).map_err( |_| std::io::Error::other("server channel error"))?;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                },
                response = self.response_receiver.recv() => {
                    if let Some((bytes, to)) = response{
                        debug!("sending {:?} to {:?}", bytes, to);
                        self.socket.send_to(&bytes, to).await?;
                    }
                    else {
                        // in case nobody is listening to us, we can just terminate, though this
                        // should never happen for UDP
                        return Ok(());
                    }

                }
            }
        }
    }
}

#[derive(Debug)]
pub struct QueuedMessage {
    pub address: SocketAddr,
    pub message: Packet,
}

struct ServerCoapState {
    observer: Observer,
    block_handler: BlockHandler<SocketAddr>,
    disable_observe: bool,
}

pub enum ShouldForwardToHandler {
    True,
    False,
}

impl ServerCoapState {
    pub async fn intercept_request(
        &mut self,
        request: &mut CoapRequest<SocketAddr>,
        responder: Arc<dyn Responder>,
    ) -> ShouldForwardToHandler {
        match self.block_handler.intercept_request(request) {
            Ok(true) => return ShouldForwardToHandler::False,
            Err(_err) => return ShouldForwardToHandler::False,
            Ok(false) => {}
        };

        if self.disable_observe {
            return ShouldForwardToHandler::True;
        }

        let should_be_forwarded = self.observer.request_handler(request, responder).await;
        if should_be_forwarded {
            ShouldForwardToHandler::True
        } else {
            ShouldForwardToHandler::False
        }
    }

    pub async fn intercept_response(&mut self, request: &mut CoapRequest<SocketAddr>) {
        // Q-Block2 responses are handled by the dedicated Q-Block path; keep the
        // RFC 7959 BlockHandler out of them so the two don't both fragment.
        #[cfg(feature = "q-block")]
        if request.message.get_option(CoapOption::QBlock2).is_some() {
            return;
        }

        let resource_path = request.get_path();

        let is_block_fetch_for_observer = request.message.get_option(CoapOption::Block2).is_some()
            && request.message.get_option(CoapOption::Observe).is_none()
            && request.source.is_some()
            && self
                .observer
                .is_observing(&request.source.unwrap(), &resource_path);

        if is_block_fetch_for_observer {
            if let Some((payload, etag)) =
                self.observer.get_resource_payload_and_etag(&resource_path)
            {
                if let Some(ref mut response) = request.response {
                    response.message.payload = payload.to_vec();
                    response.message.clear_option(CoapOption::ETag);
                    response.message.add_option(CoapOption::ETag, etag);
                    // Prevent duplicate Size2 options, clear first.
                    response.message.clear_option(CoapOption::Size2);
                    response
                        .message
                        .add_option(CoapOption::Size2, encode_coap_uint(payload.len()));
                }
            }
        }

        if let Err(err) = self.block_handler.intercept_response(request) {
            let _ = request.apply_from_error(err);
        }
    }

    pub fn new(block_config: BlockHandlerConfig) -> Self {
        Self {
            observer: Observer::new(),
            block_handler: BlockHandler::new(block_config),
            disable_observe: false,
        }
    }
    pub fn disable_observe_handling(&mut self, value: bool) {
        self.disable_observe = value
    }
}

/// Default cap on a reassembled inbound Q-Block1 request body (DoS guard;
/// override with [`Server::set_qblock_max_body_len`], as neutrino-lb does). 16
/// MiB is generous for any realistic CoAP request.
#[cfg(feature = "q-block")]
const DEFAULT_QBLOCK_MAX_BODY: usize = 16 * 1024 * 1024;

/// Default cap on the number of concurrent in-flight Q-Block1 request
/// reassemblies (DoS guard; override with [`Server::set_qblock_max_transfers`]).
/// Each in-flight transfer holds a reassembly buffer of up to `max_body_len`, so
/// `max_transfers * max_body_len` bounds the server's worst-case Q-Block1 memory.
#[cfg(feature = "q-block")]
const DEFAULT_QBLOCK_MAX_TRANSFERS: usize = 64;

/// A per-Request-Tag in-flight Q-Block1 reassembly: the source address the
/// transfer is bound to (so cross-source blocks are dropped) paired with the
/// channel feeding its [`drive_receive`] task.
#[cfg(feature = "q-block")]
type QBlockRecv = (SocketAddr, mpsc::Sender<Vec<u8>>);

/// Server-side Q-Block (RFC 9177) state. Only used under the `q-block` feature;
/// the existing RFC 7959 `BlockHandler` path is untouched and handles every
/// non-Q-Block request exactly as before. Holds:
/// - `sends`: in-flight Q-Block2 *response* sends, keyed by request token, so a
///   client's follow-up missing-block request routes to the right transfer;
/// - `recvs`: in-flight Q-Block1 *request* reassemblies, keyed by Request-Tag,
///   each draining into a per-transfer [`drive_receive`] task. The value pairs
///   the transfer's bound **source address** (fixed by its first block) with the
///   block channel, so blocks arriving for that Request-Tag from a *different*
///   source — spoofable in NON mode — are dropped instead of injected into the
///   reassembly (RFC 9177 §5 keys transfers on Request-Tag, which is not itself
///   authenticated).
#[cfg(feature = "q-block")]
struct QBlockServerState {
    config: QBlockConfig,
    max_body_len: usize,
    max_transfers: usize,
    sends: Mutex<HashMap<Vec<u8>, mpsc::Sender<Vec<u32>>>>,
    recvs: Mutex<HashMap<Vec<u8>, QBlockRecv>>,
}

/// The Request-Tag (RFC 9175, option 292) carried by `packet`, used to correlate
/// the blocks of one Q-Block1 request. Empty vec if the option is absent.
#[cfg(feature = "q-block")]
fn request_tag_of(packet: &Packet) -> Vec<u8> {
    packet
        .get_option(CoapOption::Unknown(292))
        .and_then(|l| l.front().cloned())
        .unwrap_or_default()
}

#[cfg(feature = "q-block")]
impl QBlockServerState {
    fn new(config: QBlockConfig) -> Self {
        Self::new_with(
            config,
            DEFAULT_QBLOCK_MAX_BODY,
            DEFAULT_QBLOCK_MAX_TRANSFERS,
        )
    }

    fn new_with(config: QBlockConfig, max_body_len: usize, max_transfers: usize) -> Self {
        Self {
            config,
            max_body_len,
            max_transfers,
            sends: Mutex::new(HashMap::new()),
            recvs: Mutex::new(HashMap::new()),
        }
    }

    /// If `packet` is a missing-block request for an in-flight Q-Block2 send
    /// (carries a Q-Block2 option and a token we are currently serving), forward
    /// the requested block numbers to that transfer and return `true`. Otherwise
    /// `false` — the packet is a fresh request and should be dispatched normally.
    async fn try_route_missing(&self, packet: &Packet) -> bool {
        if packet.get_option(CoapOption::QBlock2).is_none() {
            return false;
        }
        let token = packet.get_token().to_vec();
        let tx = self.sends.lock().await.get(&token).cloned();
        match tx {
            Some(tx) => {
                let _ = tx
                    .send(parse_missing_request(packet, CoapOption::QBlock2))
                    .await;
                true
            }
            None => false,
        }
    }

    /// If the peer can take a Q-Block2 transfer and the handler's response is
    /// larger than one block, stream it as a Q-Block2 burst transfer (spawning
    /// a [`drive_send`] task over the [`Responder`]) and return `true`.
    /// Otherwise `false` — the caller should send the response in the normal
    /// single PDU. "Can take": the request carries a Q-Block2 opt-in (RFC 9177
    /// §4.4 early negotiation, which also fixes the block size), or
    /// [`QBlockConfig::assume_peer_block_size`] declares peer support
    /// out of band.
    async fn maybe_serve(
        self: &Arc<Self>,
        request: &CoapRequest<SocketAddr>,
        respond: Arc<dyn Responder>,
    ) -> bool {
        let szx = match request
            .message
            .get_first_option_as::<BlockValue>(CoapOption::QBlock2)
            .and_then(|r| r.ok())
        {
            Some(req_block) => req_block.size_exponent,
            None => {
                let Some(size) = self.config.assume_peer_block_size else {
                    return false;
                };
                match BlockValue::new(0, false, size) {
                    Ok(bv) => bv.size_exponent,
                    Err(err) => {
                        log::warn!("invalid assume_peer_block_size {size}: {err}");
                        return false;
                    }
                }
            }
        };
        let Some(response) = request.response.as_ref() else {
            return false;
        };
        let body = response.message.payload.clone();
        if body.len() <= (1usize << (szx + 4)) {
            return false; // fits one block — no Q-Block needed
        }

        let token = request.message.get_token().to_vec();
        let mut template = response.message.clone();
        template.payload.clear();
        template.clear_option(CoapOption::QBlock2);
        template.clear_option(CoapOption::Block2);
        template.clear_option(CoapOption::Size2);
        template.header.set_type(MessageType::NonConfirmable);
        template.set_token(token.clone());

        let seed = token
            .iter()
            .fold(0u64, |a, &b| a.wrapping_mul(31).wrapping_add(u64::from(b)));
        let (tx, rx) = mpsc::channel::<Vec<u32>>(16);
        self.sends.lock().await.insert(token.clone(), tx);

        let sender = QBlockSender::new(
            template,
            CoapOption::QBlock2,
            body.into(),
            szx,
            TransferKind::Non,
            self.config.clone(),
            seed,
        );
        let linger = self.config.non_receive_timeout * (self.config.non_max_retransmit + 2);
        let sink = ResponderSink(respond);
        let state = self.clone();
        tokio::spawn(async move {
            let _ = drive_send(sender, &sink, rx, linger).await;
            state.sends.lock().await.remove(&token);
        });
        true
    }
}

pub struct Server {
    listeners: Vec<Box<dyn Listener>>,
    coap_state: Arc<Mutex<ServerCoapState>>,
    new_packet_receiver: TransportRequestReceiver,
    new_packet_sender: TransportRequestSender,
    #[cfg(feature = "q-block")]
    qblock: Arc<QBlockServerState>,
}

impl Server {
    /// Creates a CoAP server listening on the given address.
    pub fn new_udp<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        Self::new_udp_with_config(addr, BlockHandlerConfig::default())
    }

    /// Like [`Server::new_udp`], but with a custom [`BlockHandlerConfig`]. Use a
    /// smaller `max_total_message_size` to fit a constrained-MTU link; note it
    /// bounds *both* the accepted inbound request size and the outbound Block2
    /// (response) fragment size.
    pub fn new_udp_with_config<A: ToSocketAddrs>(
        addr: A,
        block_config: BlockHandlerConfig,
    ) -> Result<Self, io::Error> {
        let listener: Vec<Box<dyn Listener>> = vec![Box::new(UdpCoapListener::new(addr)?)];
        Ok(Self::from_listeners_with_config(listener, block_config))
    }

    pub fn from_listeners(listeners: Vec<Box<dyn Listener>>) -> Self {
        Self::from_listeners_with_config(listeners, BlockHandlerConfig::default())
    }

    /// Like [`Server::from_listeners`], but with a custom [`BlockHandlerConfig`].
    pub fn from_listeners_with_config(
        listeners: Vec<Box<dyn Listener>>,
        block_config: BlockHandlerConfig,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Server {
            listeners,
            coap_state: Arc::new(Mutex::new(ServerCoapState::new(block_config))),
            new_packet_receiver: rx,
            new_packet_sender: tx,
            #[cfg(feature = "q-block")]
            qblock: Arc::new(QBlockServerState::new(QBlockConfig::default())),
        }
    }

    /// Sets the Q-Block (RFC 9177) configuration for large response transfers.
    /// Must be called before [`run`](Server::run). Preserves any previously-set
    /// `max_body_len` / `max_transfers` caps.
    #[cfg(feature = "q-block")]
    pub fn set_qblock_config(&mut self, config: QBlockConfig) {
        self.qblock = Arc::new(QBlockServerState::new_with(
            config,
            self.qblock.max_body_len,
            self.qblock.max_transfers,
        ));
    }

    /// Caps the reassembled size of an inbound Q-Block1 request body (DoS guard).
    /// Defaults to 16 MiB. Must be called before [`run`](Server::run).
    #[cfg(feature = "q-block")]
    pub fn set_qblock_max_body_len(&mut self, max_body_len: usize) {
        self.qblock = Arc::new(QBlockServerState::new_with(
            self.qblock.config.clone(),
            max_body_len,
            self.qblock.max_transfers,
        ));
    }

    /// Caps the number of concurrent in-flight Q-Block1 request reassemblies
    /// (DoS guard against a peer opening many partial transfers). Defaults to 64.
    /// Must be called before [`run`](Server::run).
    #[cfg(feature = "q-block")]
    pub fn set_qblock_max_transfers(&mut self, max_transfers: usize) {
        self.qblock = Arc::new(QBlockServerState::new_with(
            self.qblock.config.clone(),
            self.qblock.max_body_len,
            max_transfers,
        ));
    }

    async fn spawn_handles(
        listeners: Vec<Box<dyn Listener>>,
        sender: TransportRequestSender,
    ) -> std::io::Result<Vec<JoinHandle<std::io::Result<()>>>> {
        let mut handles = vec![];
        for listener in listeners.into_iter() {
            let handle = listener.listen(sender.clone()).await?;
            handles.push(handle);
        }
        Ok(handles)
    }

    /// run the server.
    pub async fn run<Handler: RequestHandler>(mut self, handler: Handler) -> Result<(), io::Error> {
        // Abort the spawned listener task(s) when `run` is dropped — e.g. when a
        // caller races it against a shutdown signal. Dropping a `JoinHandle`
        // merely detaches its task; without this the listener (which owns the
        // UDP socket) keeps running and the socket stays bound after `run` is
        // dropped, leaking the port until the process exits.
        struct AbortOnDrop(Vec<JoinHandle<std::io::Result<()>>>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                for handle in &self.0 {
                    handle.abort();
                }
            }
        }
        let _handles =
            AbortOnDrop(Self::spawn_handles(self.listeners, self.new_packet_sender.clone()).await?);

        let handler_arc = Arc::new(handler);
        // receive an input, sync our cache / states, then call custom handler
        loop {
            let (bytes, respond) = self
                .new_packet_receiver
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("listen channel closed"))?;
            if let Ok(packet) = Packet::from_bytes(&bytes) {
                // Reassemble an inbound Q-Block1 request body before dispatch;
                // recovery + completion run in a per-transfer task keyed by
                // Request-Tag. Then route a client's Q-Block2 missing-block
                // request to its in-flight response send. Everything else falls
                // through to the unchanged RFC 7959 / single-PDU path.
                #[cfg(feature = "q-block")]
                if packet.get_option(CoapOption::QBlock1).is_some() {
                    Self::route_qblock1_recv(
                        self.qblock.clone(),
                        self.coap_state.clone(),
                        &packet,
                        bytes,
                        respond,
                        handler_arc.clone(),
                    )
                    .await;
                    continue;
                }
                #[cfg(feature = "q-block")]
                if self.qblock.try_route_missing(&packet).await {
                    continue;
                }
                let mut request = Box::new(CoapRequest::<SocketAddr>::from_packet(
                    packet,
                    respond.address(),
                ));
                let mut coap_state = self.coap_state.lock().await;
                let should_forward = coap_state
                    .intercept_request(&mut request, respond.clone())
                    .await;
                drop(coap_state);

                match should_forward {
                    ShouldForwardToHandler::True => {
                        let handler_clone = handler_arc.clone();
                        let coap_state_clone = self.coap_state.clone();
                        #[cfg(feature = "q-block")]
                        let qblock_clone = self.qblock.clone();
                        tokio::spawn(async move {
                            Self::dispatch_and_respond(
                                handler_clone,
                                coap_state_clone,
                                #[cfg(feature = "q-block")]
                                qblock_clone,
                                request,
                                respond,
                            )
                            .await;
                        });
                    }
                    ShouldForwardToHandler::False => {
                        Self::respond_to_request(request, respond).await;
                    }
                }
            }
        }
    }

    /// Run the handler over `request`, post-process the response (RFC 7959 or
    /// Q-Block2), and send it. Shared by the normal dispatch path and the
    /// Q-Block1 reassembly-completion task.
    #[cfg(not(feature = "q-block"))]
    async fn dispatch_and_respond<Handler: RequestHandler>(
        handler: Arc<Handler>,
        coap_state: Arc<Mutex<ServerCoapState>>,
        mut request: Box<CoapRequest<SocketAddr>>,
        respond: Arc<dyn Responder>,
    ) {
        request = handler.handle_request(request).await;
        coap_state
            .lock()
            .await
            .intercept_response(request.as_mut())
            .await;
        Self::respond_to_request(request, respond).await;
    }

    #[cfg(feature = "q-block")]
    async fn dispatch_and_respond<Handler: RequestHandler>(
        handler: Arc<Handler>,
        coap_state: Arc<Mutex<ServerCoapState>>,
        qblock: Arc<QBlockServerState>,
        mut request: Box<CoapRequest<SocketAddr>>,
        respond: Arc<dyn Responder>,
    ) {
        request = handler.handle_request(request).await;
        // Stream a large response via Q-Block2 if the client asked for it (or
        // `assume_peer_block_size` says it can take one). Runs before
        // `intercept_response` so the RFC 7959 BlockHandler never sees a
        // response the Q-Block path is fragmenting.
        if qblock.maybe_serve(&request, respond.clone()).await {
            return;
        }
        coap_state
            .lock()
            .await
            .intercept_response(request.as_mut())
            .await;
        Self::respond_to_request(request, respond).await;
    }

    /// Routes an inbound Q-Block1 block: appends to the in-flight reassembly for
    /// its Request-Tag, or starts one (a per-transfer [`drive_receive`] task that
    /// recovers losses via 4.08 and, on completion, dispatches the reassembled
    /// request through [`dispatch_and_respond`](Self::dispatch_and_respond)).
    #[cfg(feature = "q-block")]
    async fn route_qblock1_recv<Handler: RequestHandler>(
        qblock: Arc<QBlockServerState>,
        coap_state: Arc<Mutex<ServerCoapState>>,
        packet: &Packet,
        bytes: Vec<u8>,
        respond: Arc<dyn Responder>,
        handler: Arc<Handler>,
    ) {
        let rtag = request_tag_of(packet);
        let src = respond.address();
        // Decide routing under a single lock so the source-binding check and the
        // concurrency-cap check can't race a concurrent first block for the same
        // Request-Tag. No `.await` is held across the guard.
        let (tx, rx) = {
            let mut recvs = qblock.recvs.lock().await;
            if let Some((bound_src, tx)) = recvs.get(&rtag) {
                if *bound_src != src {
                    // A block for an in-flight transfer from a different source:
                    // drop it rather than let a spoofed datagram inject into (or
                    // stall) another peer's reassembly.
                    return;
                }
                let tx = tx.clone();
                drop(recvs);
                let _ = tx.send(bytes).await;
                return;
            }
            if recvs.len() >= qblock.max_transfers {
                // At the concurrent-transfer cap: drop the opening block. The peer
                // can retry once an in-flight transfer completes or expires.
                return;
            }
            let (tx, rx) = mpsc::channel::<Vec<u8>>(256);
            recvs.insert(rtag.clone(), (src, tx.clone()));
            (tx, rx)
        };
        let _ = tx.send(bytes).await;

        let token = packet.get_token().to_vec();
        tokio::spawn(async move {
            // 4.08 recovery requests echo the request token + Request-Tag.
            let mut tmpl = Packet::new();
            tmpl.header.set_type(MessageType::NonConfirmable);
            tmpl.set_token(token);
            if !rtag.is_empty() {
                tmpl.add_option(CoapOption::Unknown(292), rtag.clone());
            }
            // This transfer exists because a block arrived, so the peer is already
            // sending: if nothing is ever recorded, every block it sent was
            // rejected and there is nothing to wait for. Release the slot and the
            // source binding on the head timeout rather than the partial timeout.
            let receiver = QBlockReceiver::new(
                CoapOption::QBlock1,
                tmpl,
                qblock.max_body_len,
                qblock.config.clone(),
            )
            .expire_when_nothing_arrives();
            let sink = ResponderSink(respond.clone());
            if let Ok(Some((body, mut carrier))) = drive_receive(receiver, rx, &sink).await {
                carrier.payload = body;
                let request = Box::new(CoapRequest::from_packet(carrier, src));
                Self::dispatch_and_respond(handler, coap_state, qblock.clone(), request, respond)
                    .await;
            }
            qblock.recvs.lock().await.remove(&rtag);
        });
    }

    #[cfg(feature = "router")]
    pub async fn serve<S>(self, router: Router<S>) -> Result<(), io::Error>
    where
        S: Clone + Send + Sync + 'static,
    {
        let router = Arc::new(router);
        let handler = {
            move |req| {
                let r = router.clone();
                let req = Request::new(req);
                async move { r.handle(req).await.req }
            }
        };
        self.run(handler).await
    }

    async fn respond_to_request(req: Box<CoapRequest<SocketAddr>>, responder: Arc<dyn Responder>) {
        // if we have some reponse to send, send it
        if let Some(Ok(b)) = req.response.map(|resp| resp.message.to_bytes()) {
            responder.respond(b).await;
        }
    }
    #[deprecated(
        since = "0.21.0",
        note = "Use 'coap::Server::automatic_observe_handling' instead."
    )]
    /// disable auto-observe handling in server
    pub async fn disable_observe_handling(&mut self, value: bool) {
        self.automatic_observe_handling(value).await
    }
    /// Controls whether the server automatically handles observe options.
    /// Automatic handling is on by default.
    ///
    /// Set `bypass` to `true` when your handler needs full control over
    /// observe — the server will skip its built-in processing.
    pub async fn automatic_observe_handling(&mut self, bypass: bool) {
        let mut coap_state = self.coap_state.lock().await;
        coap_state.disable_observe_handling(bypass)
    }
}

#[cfg(test)]
pub mod test {
    use crate::request::RequestBuilder;

    use super::super::*;
    use super::*;
    use coap_lite::{block_handler::BlockValue, CoapOption, RequestType};
    use std::str;
    use std::time::Duration;

    pub fn spawn_server<
        F: Fn(Box<CoapRequest<SocketAddr>>) -> HandlerRet + Send + Sync + 'static,
        HandlerRet,
    >(
        ip: &'static str,
        request_handler: F,
    ) -> mpsc::UnboundedReceiver<u16>
    where
        HandlerRet: Future<Output = Box<CoapRequest<SocketAddr>>> + Send,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let _task = tokio::spawn(async move {
            let sock = UdpSocket::bind(ip).await.unwrap();
            let addr = sock.local_addr().unwrap();
            let listener = Box::new(UdpCoapListener::from_socket(sock));
            let server = Server::from_listeners(vec![listener]);
            tx.send(addr.port()).unwrap();
            server.run(request_handler).await.unwrap();
        });

        rx
    }

    async fn request_handler(
        mut req: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        let uri_path_list = req.message.get_option(CoapOption::UriPath).unwrap().clone();
        assert_eq!(uri_path_list.len(), 1);

        if let Some(ref mut response) = req.response {
            response.message.payload = uri_path_list.front().unwrap().clone();
        }
        req
    }

    pub fn spawn_server_with_all_coap<
        F: Fn(Box<CoapRequest<SocketAddr>>) -> HandlerRet + Send + Sync + 'static,
        HandlerRet,
    >(
        ip: &'static str,
        request_handler: F,
        segment: u8,
    ) -> mpsc::UnboundedReceiver<u16>
    where
        HandlerRet: Future<Output = Box<CoapRequest<SocketAddr>>> + Send,
    {
        let (tx, rx) = mpsc::unbounded_channel();

        std::thread::Builder::new()
            .name(String::from("v4-server"))
            .spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        // multicast needs a server on a real interface
                        let sock = UdpSocket::bind((ip, 0)).await.unwrap();
                        let addr = sock.local_addr().unwrap();
                        let mut listener = Box::new(UdpCoapListener::from_socket(sock));
                        listener.enable_all_coap(segment);
                        let server = Server::from_listeners(vec![listener]);
                        tx.send(addr.port()).unwrap();
                        server.run(request_handler).await.unwrap();
                    })
            })
            .unwrap();

        rx
    }

    pub fn spawn_server_disable_observe<
        F: Fn(Box<CoapRequest<SocketAddr>>) -> HandlerRet + Send + Sync + 'static,
        HandlerRet,
    >(
        ip: &'static str,
        request_handler: F,
    ) -> mpsc::UnboundedReceiver<u16>
    where
        HandlerRet: Future<Output = Box<CoapRequest<SocketAddr>>> + Send,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let _task = tokio::spawn(async move {
            let sock = UdpSocket::bind(ip).await.unwrap();
            let addr = sock.local_addr().unwrap();
            let listener = Box::new(UdpCoapListener::from_socket(sock));
            let mut server = Server::from_listeners(vec![listener]);
            // `bypass = true` sets the internal `disable_observe` flag,
            // so the server skips its built-in observe handling.
            server.automatic_observe_handling(true).await;
            tx.send(addr.port()).unwrap();
            server.run(request_handler).await.unwrap();
        });

        rx
    }

    #[tokio::test]
    async fn test_listener_instantiation() {
        let listener = UdpCoapListener::new("127.0.0.1:0").unwrap();
        assert!(
            listener.socket.local_addr().unwrap().ip() == IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
        );
        // assert!(listener.socket.blocking() == false);

        let explicit_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let another_listener = UdpCoapListener::from_socket(explicit_socket);
        assert!(
            another_listener.socket.local_addr().unwrap().ip()
                == IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[tokio::test]
    async fn test_echo_server() {
        let server_port = spawn_server("127.0.0.1:0", request_handler)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();
        let mut request = CoapRequest::new();
        request.message.header.set_version(1);
        request
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        request.message.header.set_code("0.01");
        request.message.header.message_id = 1;
        request.message.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        request
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());
        client.send_single_request(&request).await.unwrap();

        let recv_packet = client.send(request).await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    #[tokio::test]
    async fn test_put_block() {
        let server_port = spawn_server("127.0.0.1:0", request_handler)
            .recv()
            .await
            .unwrap();
        let data = "hello this is a payload";
        let mut v = Vec::new();
        for _ in 0..1024 {
            v.extend_from_slice(data.as_bytes());
        }
        let payload_size = v.len();
        let server_string = format!("127.0.0.1:{}", server_port);
        let client = UdpCoAPClient::new(server_string.clone()).await.unwrap();

        let request = RequestBuilder::new("/large", RequestType::Put)
            .data(Some(v))
            .domain(server_string.clone())
            .build();

        let resp = client.send(request).await.unwrap();
        let block_opt = resp
            .message
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .expect("expected block opt in response")
            .expect("could not decode block1 option");
        let expected_number = (payload_size as f32 / 1024.0).ceil() as u16 - 1;
        assert_eq!(
            block_opt.num, expected_number,
            "block not completely received!"
        );

        assert_eq!(resp.message.payload, b"large".to_vec());
    }

    #[tokio::test]
    #[ignore]
    async fn test_echo_server_v6() {
        let server_port = spawn_server("::1:0", request_handler).recv().await.unwrap();

        let client = UdpCoAPClient::new(format!("::1:{}", server_port))
            .await
            .unwrap();
        let mut request = CoapRequest::new();
        request.message.header.set_version(1);
        request
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        request.message.header.set_code("0.01");
        request.message.header.message_id = 1;
        request.message.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        request
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());

        let recv_packet = client.send(request).await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    #[tokio::test]
    async fn test_echo_server_no_token() {
        let server_port = spawn_server("127.0.0.1:0", request_handler)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();
        let mut packet = CoapRequest::new();
        packet.message.header.set_version(1);
        packet
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        packet.message.header.set_code("0.01");
        packet.message.header.message_id = 1;
        packet
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());
        let recv_packet = client.send(packet).await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    #[tokio::test]
    #[ignore]
    async fn test_echo_server_no_token_v6() {
        let server_port = spawn_server("::1:0", request_handler).recv().await.unwrap();

        let client = UdpCoAPClient::new(format!("::1:{}", server_port))
            .await
            .unwrap();
        let mut packet = CoapRequest::new();
        packet.message.header.set_version(1);
        packet
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        packet.message.header.set_code("0.01");
        packet.message.header.message_id = 1;
        packet
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());

        let recv_packet = client.send(packet).await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    #[tokio::test]
    async fn test_update_resource() {
        let path = "/test";
        let payload1 = b"data1".to_vec();
        let payload2 = b"data2".to_vec();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let mut step = 1;

        let server_port = spawn_server("127.0.0.1:0", request_handler)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();

        tx.send(step).unwrap();
        let mut request = CoapRequest::new();
        request.set_method(RequestType::Put);
        request.set_path(path);
        request.message.payload = payload1.clone();
        client.send(request.clone()).await.unwrap();

        let mut receive_step = 1;
        let payload1_clone = payload1.clone();
        let payload2_clone = payload2.clone();
        client
            .observe(path, move |result| {
                let msg = result.unwrap();
                if let Ok(n) = rx.try_recv() {
                    receive_step = n;
                }

                match receive_step {
                    1 => assert_eq!(msg.payload, payload1_clone),
                    2 => {
                        assert_eq!(msg.payload, payload2_clone);
                        tx2.send(()).unwrap();
                    }
                    _ => panic!("unexpected step"),
                }
            })
            .await
            .unwrap();

        step = 2;
        tx.send(step).unwrap();
        request.message.payload = payload2.clone();
        let client2 = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();
        let _ = client2.send(request).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::new(5, 0), rx2.recv())
                .await
                .unwrap(),
            Some(())
        );
    }

    #[tokio::test]
    async fn test_observe_transparent_transmission() {
        let path = "/test";
        let (tx, mut rx) = mpsc::unbounded_channel();

        let server_port = spawn_server_disable_observe("127.0.0.1:0", request_handler)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();

        client
            .observe(path, move |result| {
                let msg = result.unwrap();
                assert_eq!(msg.payload, b"test".to_vec());
                tx.send(()).unwrap();
            })
            .await
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::new(5, 0), rx.recv())
                .await
                .unwrap(),
            Some(())
        );
    }

    #[tokio::test]
    async fn multicast_server_all_coap() {
        // segment not relevant with IPv4
        let segment = 0x0;
        let server_port = spawn_server_with_all_coap("0.0.0.0", request_handler, segment)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();
        let mut request = CoapRequest::new();
        request.message.header.set_version(1);
        request
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        request.message.header.set_code("0.01");
        request.message.header.message_id = 1;
        request.message.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        request
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());
        let recv_packet = client.send(request).await.unwrap();

        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());

        let client = UdpCoAPClient::new(format!("224.0.1.187:{}", server_port))
            .await
            .unwrap();
        let mut request = RequestBuilder::new("test-echo", RequestType::Get)
            .data(Some(vec![0x51, 0x55, 0x77, 0xE8]))
            .confirmable(true)
            .build();

        let mut receiver = client.create_receiver_for(&request).await;
        client.send_all_coap(&mut request, segment).await.unwrap();
        let recv_packet = receiver.receive().await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    //This test right now does not work on windows
    #[cfg(unix)]
    #[tokio::test]
    #[ignore]
    async fn multicast_server_all_coap_v6() {
        // use segment 0x04 which should be the smallest administered scope

        let segment = 0x04;
        let server_port = spawn_server_with_all_coap("::0", request_handler, segment)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("::1:{}", server_port))
            .await
            .unwrap();
        let mut request = CoapRequest::new();
        request.message.header.set_version(1);
        request
            .message
            .header
            .set_type(coap_lite::MessageType::Confirmable);
        request.message.header.set_code("0.01");
        request.message.header.message_id = 1;
        request.message.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        request
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());
        client.send_single_request(&request).await.unwrap();

        let recv_packet = client.send(request).await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());

        // use 0xff02 to keep it within this network
        let client = UdpCoAPClient::new(format!("ff0{}::fd:{}", segment, server_port))
            .await
            .unwrap();
        let mut request = CoapRequest::new();
        request.message.header.set_version(1);
        request
            .message
            .header
            .set_type(coap_lite::MessageType::NonConfirmable);
        request.message.header.set_code("0.01");
        request.message.header.message_id = 2;
        request.message.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        request
            .message
            .add_option(CoapOption::UriPath, b"test-echo".to_vec());
        let mut receiver = client.create_receiver_for(&request).await;
        client.send_all_coap(&mut request, segment).await.unwrap();
        let recv_packet = receiver.receive().await.unwrap();
        assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    }

    #[test]
    fn multicast_join_leave() {
        std::thread::Builder::new()
            .name(String::from("v4-server"))
            .spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        // multicast needs a server on a real interface
                        let sock = UdpSocket::bind(("0.0.0.0", 0)).await.unwrap();
                        let mut listener = Box::new(UdpCoapListener::from_socket(sock));
                        listener.join_multicast(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)));
                        listener.join_multicast(IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)));
                        listener.leave_multicast(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)));
                        listener.leave_multicast(IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)));
                        let server = Server::from_listeners(vec![listener]);
                        server.run(request_handler).await.unwrap();
                    })
            })
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    #[test]
    #[ignore]
    fn multicast_join_leave_v6() {
        std::thread::Builder::new()
            .name(String::from("v6-server"))
            .spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        // multicast needs a server on a real interface
                        let sock = UdpSocket::bind(("0.0.0.0", 0)).await.unwrap();
                        let mut listener = Box::new(UdpCoapListener::from_socket(sock));
                        listener.join_multicast(IpAddr::V6(Ipv6Addr::new(
                            0xff02, 0, 0, 0, 0, 0, 1, 0x1,
                        )));
                        listener.join_multicast(IpAddr::V6(Ipv6Addr::new(
                            0xff02, 0, 0, 0, 0, 1, 0, 0x2,
                        )));
                        listener.leave_multicast(IpAddr::V6(Ipv6Addr::new(
                            0xff02, 0, 0, 0, 0, 0, 1, 0x1,
                        )));
                        listener.join_multicast(IpAddr::V6(Ipv6Addr::new(
                            0xff02, 0, 0, 0, 0, 1, 0, 0x2,
                        )));
                        let server = Server::from_listeners(vec![listener]);
                        server.run(request_handler).await.unwrap();
                    })
            })
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    fn get_expected_response() -> Vec<u8> {
        let mut resp = vec![];
        for c in b'a'..=b'z' {
            resp.resize(resp.len() + 1024, c);
        }
        resp
    }
    async fn block2_responder(
        mut req: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        // vec should contain 'a' 1024 times, then 'b' 1024, up to ascii 'z'

        if let Some(ref mut response) = req.response {
            response.message.payload = get_expected_response();
        }
        req
    }
    #[tokio::test]
    async fn test_block2_server_response() {
        let server_port = spawn_server("127.0.0.1:0", block2_responder)
            .recv()
            .await
            .unwrap();

        let client = UdpCoAPClient::new(format!("127.0.0.1:{}", server_port))
            .await
            .unwrap();
        let resp = client
            .send(RequestBuilder::new("/", RequestType::Get).build())
            .await
            .unwrap();
        assert_eq!(
            resp.message.payload,
            get_expected_response(),
            "responses do not match"
        );
    }

    /// End-to-end over real loopback UDP: a request opting into Q-Block2 gets a
    /// large response streamed by the *built-in server dispatch* as a Q-Block2
    /// burst; a couple of blocks are dropped on the wire and recovered via the
    /// server's missing-block routing. Exercises the actual `Server::run` call
    /// site, not the drivers in isolation.
    #[cfg(feature = "q-block")]
    #[tokio::test]
    async fn qblock2_large_response_over_real_server_with_loss() {
        use crate::qblock::{drive_receive, BlockSink, QBlockConfig, QBlockReceiver};
        use coap_lite::block_handler::BlockValue;
        use coap_lite::{MessageClass, MessageType, ResponseType};
        use std::collections::HashSet;
        use tokio::net::UdpSocket;

        let short_cfg = || QBlockConfig {
            non_timeout: Duration::from_millis(20),
            non_receive_timeout: Duration::from_millis(40),
            ..Default::default()
        };
        let body: Vec<u8> = (0..25u16).flat_map(|i| [i as u8; 16]).collect();

        // Server on loopback, large body for any request, short Q-Block timers.
        let server_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server_sock.local_addr().unwrap();
        let mut server =
            Server::from_listeners(vec![Box::new(UdpCoapListener::from_socket(server_sock))]);
        server.set_qblock_config(short_cfg());
        let body_h = body.clone();
        let _server = tokio::spawn(async move {
            let _ = server
                .run(move |mut req: Box<CoapRequest<SocketAddr>>| {
                    let body = body_h.clone();
                    async move {
                        if let Some(resp) = req.response.as_mut() {
                            resp.message.payload = body;
                            resp.message.header.code =
                                MessageClass::Response(ResponseType::Content);
                        }
                        req
                    }
                })
                .await;
        });

        // Client: send a Q-Block2-tagged request, then reassemble via the driver.
        let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let mut req = Packet::new();
        req.header.set_type(MessageType::NonConfirmable);
        req.header.code = MessageClass::Request(RequestType::Get);
        req.set_token(vec![0x42]);
        req.add_option(CoapOption::UriPath, b"big".to_vec());
        req.add_option_as::<BlockValue>(
            CoapOption::QBlock2,
            BlockValue::new(0, false, 16).unwrap(),
        );
        client
            .send_to(&req.to_bytes().unwrap(), server_addr)
            .await
            .unwrap();

        // Reader bridges inbound datagrams into the driver, dropping 3 & 17 once.
        let (pdu_tx, pdu_rx) = mpsc::channel::<Vec<u8>>(256);
        let cr = client.clone();
        let reader = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            let mut drop_once: HashSet<u16> = [3u16, 17].into_iter().collect();
            while let Ok((n, _)) = cr.recv_from(&mut buf).await {
                if let Ok(pkt) = Packet::from_bytes(&buf[..n]) {
                    if let Some(Ok(bv)) = pkt.get_first_option_as::<BlockValue>(CoapOption::QBlock2)
                    {
                        if drop_once.remove(&bv.num) {
                            continue;
                        }
                    }
                }
                if pdu_tx.send(buf[..n].to_vec()).await.is_err() {
                    break;
                }
            }
        });

        struct ReqSink {
            sock: Arc<UdpSocket>,
            peer: SocketAddr,
        }
        #[async_trait]
        impl BlockSink for ReqSink {
            async fn send_block(&self, pdu: Vec<u8>) -> std::io::Result<()> {
                self.sock.send_to(&pdu, self.peer).await.map(|_| ())
            }
        }
        let req_sink = ReqSink {
            sock: client.clone(),
            peer: server_addr,
        };

        // Recovery requests must echo the original token so the server routes them.
        let mut recovery_template = Packet::new();
        recovery_template
            .header
            .set_type(MessageType::NonConfirmable);
        recovery_template.header.code = MessageClass::Request(RequestType::Get);
        recovery_template.set_token(vec![0x42]);
        let rx = QBlockReceiver::new(CoapOption::QBlock2, recovery_template, 1 << 20, short_cfg());

        let got =
            tokio::time::timeout(Duration::from_secs(5), drive_receive(rx, pdu_rx, &req_sink))
                .await
                .expect("transfer timed out")
                .unwrap();

        assert_eq!(got.map(|(body, _)| body), Some(body));
        reader.abort();
    }

    /// `maybe_serve`-level tests for the `assume_peer_block_size` gate:
    /// requests deliberately carry no blanket Q-Block2 opt-in (it broke
    /// Wireshark's Q-Block reassembly and diverged from libcoap), so peer
    /// support is declared by config in closed deployments.
    #[cfg(feature = "q-block")]
    mod qblock_assume_peer_tests {
        use super::*;
        use crate::qblock::QBlockConfig;
        use coap_lite::{MessageClass, MessageType, ResponseType};

        struct RecordingResponder {
            tx: mpsc::UnboundedSender<Vec<u8>>,
            addr: SocketAddr,
        }
        #[async_trait]
        impl Responder for RecordingResponder {
            async fn respond(&self, response: Vec<u8>) {
                let _ = self.tx.send(response);
            }
            fn address(&self) -> SocketAddr {
                self.addr
            }
        }

        /// A GET (optionally opting into Q-Block2 at `opt_in` bytes/block)
        /// whose handler response carries `body`.
        fn request_with_response(body: &[u8], opt_in: Option<usize>) -> CoapRequest<SocketAddr> {
            let mut pkt = Packet::new();
            pkt.header.set_type(MessageType::NonConfirmable);
            pkt.header.code = MessageClass::Request(RequestType::Get);
            pkt.set_token(vec![0x42]);
            pkt.add_option(CoapOption::UriPath, b"big".to_vec());
            if let Some(size) = opt_in {
                pkt.add_option_as::<BlockValue>(
                    CoapOption::QBlock2,
                    BlockValue::new(0, false, size).unwrap(),
                );
            }
            let mut request = CoapRequest::from_packet(pkt, "127.0.0.1:5683".parse().unwrap());
            let response = request.response.as_mut().unwrap();
            response.message.payload = body.to_vec();
            response.message.header.code = MessageClass::Response(ResponseType::Content);
            request
        }

        /// Drain `blocks` Q-Block2 PDUs and reassemble their payloads,
        /// asserting each block's size exponent.
        async fn reassemble(
            rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
            blocks: usize,
            want_szx: u8,
        ) -> Vec<u8> {
            let mut got = vec![Vec::new(); blocks];
            for _ in 0..blocks {
                let pdu = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                    .await
                    .expect("block timed out")
                    .expect("sender closed");
                let pkt = Packet::from_bytes(&pdu).unwrap();
                let bv = pkt
                    .get_first_option_as::<BlockValue>(CoapOption::QBlock2)
                    .expect("block PDU missing Q-Block2")
                    .unwrap();
                assert_eq!(bv.size_exponent, want_szx);
                assert_eq!(pkt.header.get_type(), MessageType::NonConfirmable);
                got[bv.num as usize] = pkt.payload.clone();
            }
            got.concat()
        }

        fn serve_state(assume: Option<usize>) -> Arc<QBlockServerState> {
            let cfg = QBlockConfig {
                non_timeout: Duration::from_millis(20),
                non_receive_timeout: Duration::from_millis(40),
                assume_peer_block_size: assume,
                ..Default::default()
            };
            Arc::new(QBlockServerState::new_with(cfg, 1 << 20, 8))
        }

        #[tokio::test]
        async fn streams_without_opt_in_when_peer_support_assumed() {
            let body: Vec<u8> = (0..64u8).collect();
            let request = request_with_response(&body, None);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let respond: Arc<dyn Responder> = Arc::new(RecordingResponder {
                tx,
                addr: request.source.unwrap(),
            });
            let state = serve_state(Some(16));
            assert!(state.maybe_serve(&request, respond).await);
            assert_eq!(reassemble(&mut rx, 4, 0).await, body);
        }

        #[tokio::test]
        async fn declines_without_opt_in_by_default() {
            let body: Vec<u8> = (0..64u8).collect();
            let request = request_with_response(&body, None);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let respond: Arc<dyn Responder> = Arc::new(RecordingResponder {
                tx,
                addr: request.source.unwrap(),
            });
            let state = serve_state(None);
            assert!(!state.maybe_serve(&request, respond).await);
            assert!(rx.try_recv().is_err(), "nothing may be sent");
        }

        #[tokio::test]
        async fn request_opt_in_overrides_assumed_block_size() {
            let body: Vec<u8> = (0..64u8).collect();
            // Request negotiates 32 B blocks; config assumes 16 B. The
            // request wins: 2 blocks of szx=1.
            let request = request_with_response(&body, Some(32));
            let (tx, mut rx) = mpsc::unbounded_channel();
            let respond: Arc<dyn Responder> = Arc::new(RecordingResponder {
                tx,
                addr: request.source.unwrap(),
            });
            let state = serve_state(Some(16));
            assert!(state.maybe_serve(&request, respond).await);
            assert_eq!(reassemble(&mut rx, 2, 1).await, body);
        }
    }

    /// DoS-hardening of the Q-Block1 (inbound request) receive path: source
    /// binding, the concurrent-transfer cap, and the body-size cap.
    #[cfg(feature = "q-block")]
    mod qblock_dos_tests {
        use super::*;
        use coap_lite::MessageClass;
        use std::sync::Mutex;

        /// One serialized Q-Block1 request block PDU (16 B blocks, szx=0).
        fn q1_block(rtag: &[u8], token: &[u8], num: u16, more: bool, payload: Vec<u8>) -> Vec<u8> {
            let mut p = Packet::new();
            p.header.set_type(MessageType::NonConfirmable);
            p.header.code = MessageClass::Request(RequestType::Put);
            p.set_token(token.to_vec());
            p.add_option(CoapOption::Unknown(292), rtag.to_vec()); // Request-Tag
            p.add_option(CoapOption::UriPath, b"r".to_vec());
            p.add_option_as::<BlockValue>(
                CoapOption::QBlock1,
                BlockValue::new(num as usize, more, 16).unwrap(),
            );
            p.payload = payload;
            p.to_bytes().unwrap()
        }

        /// A loopback Q-Block server whose handler records every reassembled
        /// request body it is dispatched. `configure` sets the Q-Block knobs.
        async fn spawn_recording_server(
            configure: impl FnOnce(&mut Server),
        ) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
            let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = sock.local_addr().unwrap();
            let mut server =
                Server::from_listeners(vec![Box::new(UdpCoapListener::from_socket(sock))]);
            configure(&mut server);
            let bodies = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
            let bodies_h = bodies.clone();
            tokio::spawn(async move {
                let _ = server
                    .run(move |req: Box<CoapRequest<SocketAddr>>| {
                        let bodies = bodies_h.clone();
                        async move {
                            bodies.lock().unwrap().push(req.message.payload.clone());
                            req
                        }
                    })
                    .await;
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            (addr, bodies)
        }

        // I3: a Q-Block1 block for an in-flight transfer's Request-Tag, but from a
        // *different* source, must be dropped — not injected into the reassembly.
        #[tokio::test]
        async fn block_from_a_different_source_is_dropped() {
            let cfg = QBlockConfig {
                non_receive_timeout: Duration::from_millis(40),
                ..Default::default()
            };
            let (addr, bodies) =
                spawn_recording_server(move |s| s.set_qblock_config(cfg)).await;

            let good: Vec<u8> = (0..32u8).collect();
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

            // A opens transfer "T" with block 0 → binds it to A's source.
            a.send_to(&q1_block(b"T", b"\x01", 0, true, good[0..16].to_vec()), addr)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            // B (different source) forges the final block for the same tag.
            b.send_to(&q1_block(b"T", b"\x01", 1, false, vec![0xFFu8; 16]), addr)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            // A completes the transfer correctly.
            a.send_to(&q1_block(b"T", b"\x01", 1, false, good[16..32].to_vec()), addr)
                .await
                .unwrap();

            let mut got = None;
            for _ in 0..50 {
                if let Some(body) = bodies.lock().unwrap().first().cloned() {
                    got = Some(body);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                got.as_deref(),
                Some(good.as_slice()),
                "a Q-Block1 block from a different source must not corrupt the transfer"
            );
        }

        // I2: a transfer opened past the concurrent-transfer cap is dropped, not
        // dispatched.
        #[tokio::test]
        async fn concurrent_transfer_cap_drops_excess() {
            let (addr, bodies) = spawn_recording_server(|s| s.set_qblock_max_transfers(1)).await;

            let body: Vec<u8> = (0..32u8).collect();
            // Transfer A occupies the one slot with an incomplete (more=true) block.
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            a.send_to(&q1_block(b"A", b"\x01", 0, true, body[0..16].to_vec()), addr)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            // Transfer B is a complete single block — without the cap it would
            // dispatch immediately; at the cap its opening block is dropped.
            let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            b.send_to(&q1_block(b"B", b"\x02", 0, false, body[16..32].to_vec()), addr)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                bodies.lock().unwrap().is_empty(),
                "a transfer opened past the concurrency cap must not be dispatched"
            );
        }

        // A malformed opening block is rejected, so its transfer records nothing —
        // but the transfer still exists, holding a slot and its source binding. It
        // must be released one `non_receive_timeout` later, not held for the whole
        // partial timeout: otherwise one bad datagram costs a slot for minutes.
        #[tokio::test]
        async fn a_transfer_whose_opening_block_was_rejected_releases_its_slot() {
            let cfg = QBlockConfig {
                non_receive_timeout: Duration::from_millis(100),
                non_partial_timeout: Duration::from_secs(10),
                ..Default::default()
            };
            let (addr, bodies) = spawn_recording_server(move |s| {
                s.set_qblock_config(cfg);
                s.set_qblock_max_transfers(1);
            })
            .await;

            // 8 bytes in a 16-byte non-final block: dropped, nothing recorded.
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            a.send_to(&q1_block(b"A", b"\x01", 0, true, vec![0u8; 8]), addr)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(800)).await;

            // The one slot must be free again by now.
            let body: Vec<u8> = (0..16u8).collect();
            let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            b.send_to(&q1_block(b"B", b"\x02", 0, false, body.clone()), addr)
                .await
                .unwrap();

            for _ in 0..50 {
                if !bodies.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                *bodies.lock().unwrap(),
                vec![body],
                "the rejected transfer was still holding the only slot"
            );
        }

        // I2: a Q-Block1 body exceeding `max_body_len` is rejected, not dispatched.
        #[tokio::test]
        async fn oversized_body_is_rejected_by_max_body_len() {
            let (addr, bodies) = spawn_recording_server(|s| s.set_qblock_max_body_len(16)).await;

            let body: Vec<u8> = (0..32u8).collect();
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            a.send_to(&q1_block(b"X", b"\x01", 0, true, body[0..16].to_vec()), addr)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            // Block 1 starts at offset 16, end 32 > the 16-byte cap → rejected.
            a.send_to(&q1_block(b"X", b"\x01", 1, false, body[16..32].to_vec()), addr)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                bodies.lock().unwrap().is_empty(),
                "a Q-Block1 body exceeding max_body_len must not be dispatched"
            );
        }
    }
}
