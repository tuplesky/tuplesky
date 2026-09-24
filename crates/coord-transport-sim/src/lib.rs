//! Packet-level transport simulation (task-32; design Sections 12.1,
//! 12.2, 21.2 fidelity level C): the resolved `quinn-proto` state machines
//! driven by virtual datagrams and virtual time, with the framing,
//! negotiation, role and lane rules shared with the production adapter.
//!
//! Every node owns a `quinn_proto::Endpoint` seeded from the scenario, a
//! real rustls TLS 1.3 configuration over the AWS-LC provider with a
//! virtual time provider, and the session logic of the production
//! transport: the first frame on the control stream is `Hello`, the
//! acceptor validates cluster, domain, role class, lane and the identity
//! binder before `HelloAck`, peer evidence travels one frame per
//! unidirectional stream through the `wire_v1` reader, and a lane whose
//! consumer stalls simply stops reading, so QUIC flow control backs the
//! sender up. Datagrams cross a seeded link model (delay, loss,
//! duplication, reordering, MTU, directed cuts) into per-node inbound
//! queues ordered by delivery time; time advances to the next timer or
//! delivery.
//!
//! What this makes deterministic: the schedule, the endpoint's protocol
//! randomness (connection identifiers, packet spacing), timers, and the
//! sizes and timing of every datagram. What it does not: TLS key shares,
//! nonces and signatures, which stay real cryptography (test keys are
//! Ed25519 so signature lengths, and therefore packet sizes, are fixed).
//! Production endpoints never seed their RNG or inject time; this crate
//! is `test-only` and the dependency policy keeps it out of every
//! production edge.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use coord_core::event::PeerProvenance;
use coord_sim::rng::NamedStreams;
use coord_transport::{
    ALPN_API, ALPN_PEER, BoundIdentity, Budget, BudgetError, Class, CloseCode, CloseReason,
    FairQueue, IdentityBinder, Lane, LaneLimits, Limits, QueueError, Queued, lane_of_hello,
    role_class, role_lanes,
};
use coord_transport_testkit::TestIdentity;
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedVec, FrameReader, HelloAckV1, HelloV1, MessageV1, PeerRole, WireError, decode,
};
use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn_proto::{
    ConnectionEvent, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig, EndpointEvent,
    Event, ReadError, StreamEvent, StreamId, Transmit, VarInt, WriteError,
};
use rustls::server::WebPkiClientVerifier;
use rustls::time_provider::TimeProvider;
use rustls_pki_types::{CertificateDer, UnixTime};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

/// Node index in a world.
pub type NodeId = usize;

/// One tick of virtual time is one millisecond.
pub const TICK: Duration = Duration::from_millis(1);

/// Faults of one directed link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkFaults {
    /// Minimum one-way delay in ticks.
    pub delay_min: u64,
    /// Maximum one-way delay in ticks.
    pub delay_max: u64,
    /// Loss per datagram, parts per million.
    pub loss_ppm: u32,
    /// Duplication per datagram, parts per million.
    pub duplicate_ppm: u32,
    /// Reordering: extra delay of up to `reorder_ticks` with this
    /// probability, parts per million.
    pub reorder_ppm: u32,
    /// Extra delay applied to reordered datagrams.
    pub reorder_ticks: u64,
    /// Datagrams above this size are dropped.
    pub mtu: usize,
    /// Directed cut: nothing is delivered.
    pub cut: bool,
}

impl Default for LinkFaults {
    fn default() -> Self {
        LinkFaults {
            delay_min: 5,
            delay_max: 20,
            loss_ppm: 0,
            duplicate_ppm: 0,
            reorder_ppm: 0,
            reorder_ticks: 30,
            mtu: 1452,
            cut: false,
        }
    }
}

/// Virtual time: a base instant plus ticks.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    base: Instant,
    tick: u64,
}

impl Clock {
    fn new() -> Self {
        Clock {
            base: Instant::now(),
            tick: 0,
        }
    }

    /// Current tick.
    pub const fn tick(&self) -> u64 {
        self.tick
    }

    fn now(&self) -> Instant {
        self.base + TICK * u32::try_from(self.tick).unwrap_or(u32::MAX)
    }

    fn at(&self, instant: Instant) -> u64 {
        instant
            .saturating_duration_since(self.base)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

/// The virtual time the TLS stack validates certificates against.
#[derive(Debug)]
struct VirtualTime {
    unix_base: u64,
}

impl TimeProvider for VirtualTime {
    fn current_time(&self) -> Option<UnixTime> {
        Some(UnixTime::since_unix_epoch(Duration::from_secs(
            self.unix_base,
        )))
    }
}

/// What a node observed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimEvent {
    /// A connection completed negotiation.
    Connected {
        /// Node.
        node: NodeId,
        /// Lane.
        lane: Lane,
        /// Class.
        class: Class,
        /// Bound identity of the peer.
        identity: BoundIdentity,
    },
    /// A complete peer frame arrived.
    PeerFrame {
        /// Node.
        node: NodeId,
        /// Lane.
        lane: Lane,
        /// Provenance bound at negotiation.
        provenance: PeerProvenance,
        /// Kind.
        kind: u16,
        /// Payload.
        payload: Vec<u8>,
    },
    /// A connection ended.
    Closed {
        /// Node.
        node: NodeId,
        /// Lane, when negotiated.
        lane: Option<Lane>,
        /// Why.
        reason: CloseReason,
    },
}

/// Configuration of one node.
pub struct NodeConfig {
    /// Certificate, key, replica identity and role.
    pub identity: TestIdentity,
    /// Trust anchors.
    pub roots: Arc<rustls::RootCertStore>,
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Identity binder.
    pub binder: Arc<dyn IdentityBinder>,
    /// Bounds (lane limits and windows are applied as in production).
    pub limits: Limits,
    /// Capabilities granted.
    pub capabilities: Vec<u16>,
}

enum Side {
    Dialer {
        local_role: PeerRole,
        local_incarnation: Option<ReplicaIncarnation>,
        lane: Lane,
        expected: BoundIdentity,
    },
    Acceptor,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Fresh,
    HelloSent,
    Ready,
    Closing,
}

struct Conn {
    conn: quinn_proto::Connection,
    side: Side,
    phase: Phase,
    class: Option<Class>,
    lane: Option<Lane>,
    identity: Option<BoundIdentity>,
    control_send: Option<StreamId>,
    control_recv: Option<StreamId>,
    /// Per stream: the frame reader and whether its one frame was
    /// delivered (anything after it is a protocol violation).
    readers: BTreeMap<StreamId, (FrameReader, bool)>,
    /// Unwritten remainders of stream writes blocked by flow control.
    pending_writes: VecDeque<(StreamId, Vec<u8>, bool)>,
    /// Frames waiting for stream credit.
    /// Frames waiting for stream credit, in the production round-robin
    /// queue with the lane's depth and group bounds.
    pending_opens: FairQueue,
    /// Bytes this destination may hold in flight (checked, as production
    /// checks, before a frame is admitted).
    budget: Budget,
    timeout: Option<Instant>,
    connection_id: u64,
    reported_closed: bool,
    close_reason: Option<CloseReason>,
}

struct Node {
    id: NodeId,
    addr: SocketAddr,
    endpoint: Endpoint,
    client_tls: [Arc<QuicClientConfig>; 2],
    lane_transport: [Arc<quinn_proto::TransportConfig>; 4],
    connections: BTreeMap<ConnectionHandle, Conn>,
    conn_events: BTreeMap<ConnectionHandle, VecDeque<ConnectionEvent>>,
    inbound: VecDeque<(u64, u64, SocketAddr, BytesMut)>,
    outbound: Vec<(Transmit, Bytes)>,
    binder: Arc<dyn IdentityBinder>,
    cluster: ClusterId,
    domain: DomainId,
    capabilities: Vec<u16>,
    limits: Limits,
    next_connection: u64,
    events: Vec<SimEvent>,
    by_link: BTreeMap<(ReplicaId, ReplicaIncarnation, Lane), ConnectionHandle>,
    /// Bytes this node may hold in flight across every destination.
    node_budget: Budget,
    stalled: BTreeSet<Lane>,
    /// Frames this node handed to QUIC, per lane (message-level view).
    sent: Vec<(Lane, Vec<u8>)>,
}

fn node_addr(id: NodeId) -> SocketAddr {
    SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(
            10,
            0,
            0,
            u8::try_from(id + 1).expect("few nodes"),
        )),
        4433,
    )
}

fn frame_or_close(
    reader: &mut FrameReader,
) -> Result<Option<coord_types::wire_v1::Frame>, WireError> {
    reader.next_frame()
}

impl Node {
    fn new(id: NodeId, config: NodeConfig, seed: [u8; 32], time: Arc<VirtualTime>) -> Self {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier =
            WebPkiClientVerifier::builder_with_provider(config.roots.clone(), provider.clone())
                .build()
                .expect("verifier");
        let mut server = rustls::ServerConfig::builder_with_details(provider.clone(), time.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("tls13")
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                config.identity.chain.clone(),
                config.identity.key.clone_key(),
            )
            .expect("server cert");
        server.alpn_protocols = vec![ALPN_API.to_vec(), ALPN_PEER.to_vec()];
        server.max_early_data_size = 0;
        let quic_server = QuicServerConfig::try_from(server).expect("quic server");
        let limits = config.limits;
        let lane_config = |l: &LaneLimits| {
            coord_transport::lane::transport_config(l, limits.idle_timeout, limits.keep_alive)
                .expect("lane config")
        };
        let lane_transport = [
            lane_config(&limits.lanes[0]),
            lane_config(&limits.lanes[1]),
            lane_config(&limits.lanes[2]),
            lane_config(&limits.lanes[3]),
        ];
        let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(quic_server));
        server_config
            .transport_config(lane_config(&LaneLimits::floor(&limits.lanes)))
            .migration(false)
            .max_incoming(limits.max_connections);
        let client_for = |alpn: &[u8]| {
            let mut client =
                rustls::ClientConfig::builder_with_details(provider.clone(), time.clone())
                    .with_protocol_versions(&[&rustls::version::TLS13])
                    .expect("tls13")
                    .with_root_certificates(config.roots.clone())
                    .with_client_auth_cert(
                        config.identity.chain.clone(),
                        config.identity.key.clone_key(),
                    )
                    .expect("client cert");
            client.alpn_protocols = vec![alpn.to_vec()];
            client.enable_early_data = false;
            Arc::new(QuicClientConfig::try_from(client).expect("quic client"))
        };
        let client_tls = [client_for(ALPN_API), client_for(ALPN_PEER)];
        let mut endpoint_config = EndpointConfig::default();
        endpoint_config.rng_seed(Some(seed));
        let endpoint = Endpoint::new(
            Arc::new(endpoint_config),
            Some(Arc::new(server_config)),
            false,
            None,
        );
        Node {
            id,
            addr: node_addr(id),
            endpoint,
            client_tls,
            lane_transport,
            connections: BTreeMap::new(),
            conn_events: BTreeMap::new(),
            inbound: VecDeque::new(),
            outbound: Vec::new(),
            binder: config.binder,
            cluster: config.cluster,
            domain: config.domain,
            capabilities: config.capabilities,
            limits,
            next_connection: 1,
            events: Vec::new(),
            by_link: BTreeMap::new(),
            node_budget: Budget::new(
                config.limits.budget.node_bytes,
                config.limits.budget.control_reserve,
            ),
            stalled: BTreeSet::new(),
            sent: Vec::new(),
        }
    }

    fn next_wakeup(&self, clock: &Clock) -> Option<u64> {
        let inbound = self.inbound.front().map(|(t, _, _, _)| *t);
        let timers = self
            .connections
            .values()
            .filter_map(|c| c.timeout.map(|t| clock.at(t)))
            .min();
        match (inbound, timers) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn drive_incoming(&mut self, clock: &Clock) {
        let now = clock.now();
        let mut buf = Vec::with_capacity(1500);
        while self.inbound.front().is_some_and(|x| x.0 <= clock.tick) {
            let (_, _, remote, data) = self.inbound.pop_front().unwrap();
            let Some(event) = self
                .endpoint
                .handle(now, remote, None, None, data, &mut buf)
            else {
                continue;
            };
            match event {
                DatagramEvent::NewConnection(incoming) => {
                    match self.endpoint.accept(incoming, now, &mut buf, None) {
                        Ok((ch, conn)) => {
                            let id = self.next_connection;
                            self.next_connection += 1;
                            self.connections.insert(
                                ch,
                                Conn {
                                    conn,
                                    side: Side::Acceptor,
                                    phase: Phase::Fresh,
                                    class: None,
                                    lane: None,
                                    identity: None,
                                    control_send: None,
                                    control_recv: None,
                                    readers: BTreeMap::new(),
                                    pending_writes: VecDeque::new(),
                                    pending_opens: FairQueue::new(
                                        self.limits.lanes[0].queue_depth,
                                        self.limits.lanes[0].max_groups,
                                    ),
                                    budget: Budget::new(
                                        self.limits.budget.destination_bytes,
                                        self.limits.budget.control_reserve,
                                    ),
                                    timeout: None,
                                    connection_id: id,
                                    reported_closed: false,
                                    close_reason: None,
                                },
                            );
                        }
                        Err(e) => {
                            if let Some(t) = e.response {
                                let size = t.size;
                                self.outbound
                                    .push((t, Bytes::copy_from_slice(&buf[..size])));
                            }
                        }
                    }
                    buf.clear();
                }
                DatagramEvent::ConnectionEvent(ch, event) => {
                    self.conn_events.entry(ch).or_default().push_back(event);
                }
                DatagramEvent::Response(t) => {
                    let size = t.size;
                    self.outbound
                        .push((t, Bytes::copy_from_slice(&buf[..size])));
                    buf.clear();
                }
            }
        }
    }

    fn drive_outgoing(&mut self, clock: &Clock) {
        let now = clock.now();
        let mut buf = Vec::with_capacity(1500);
        loop {
            let mut endpoint_events: Vec<(ConnectionHandle, EndpointEvent)> = Vec::new();
            // Connection order decides routing order, which consumes
            // link randomness and enters the trace digest. A hashed map
            // would order it differently per process, so two runs of one
            // seed would schedule differently: the order is the handle
            // order, which is stable.
            let handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();
            for ch in handles {
                if let Some(c) = self.connections.get_mut(&ch)
                    && c.timeout.is_some_and(|t| t <= now)
                {
                    c.timeout = None;
                    c.conn.handle_timeout(now);
                }
                if let Some(mut events) = self.conn_events.remove(&ch)
                    && let Some(c) = self.connections.get_mut(&ch)
                {
                    for e in events.drain(..) {
                        c.conn.handle_event(e);
                    }
                }
                self.service(ch, clock);
                let Some(c) = self.connections.get_mut(&ch) else {
                    continue;
                };
                while let Some(e) = c.conn.poll_endpoint_events() {
                    endpoint_events.push((ch, e));
                }
                while let Some(t) = c.conn.poll_transmit(now, 1, &mut buf) {
                    let size = t.size;
                    self.outbound
                        .push((t, Bytes::copy_from_slice(&buf[..size])));
                    buf.clear();
                }
                c.timeout = c.conn.poll_timeout();
            }
            if endpoint_events.is_empty() {
                break;
            }
            for (ch, e) in endpoint_events {
                if let Some(e) = self.endpoint.handle_event(ch, e)
                    && let Some(c) = self.connections.get_mut(&ch)
                {
                    c.conn.handle_event(e);
                }
            }
        }
        // Drained connections are gone.
        self.connections.retain(|_, c| !c.conn.is_drained());
    }

    /// Application logic of one connection: negotiation, frames, sends.
    fn service(&mut self, ch: ConnectionHandle, clock: &Clock) {
        let now = clock.now();
        let mut readable: Vec<StreamId> = Vec::new();
        let mut connected = false;
        let mut lost: Option<CloseReason> = None;
        let mut opened: Vec<Dir> = Vec::new();
        let mut available = false;
        let mut writable = false;
        {
            let c = self.connections.get_mut(&ch).expect("connection");
            while let Some(event) = c.conn.poll() {
                match event {
                    Event::Connected => connected = true,
                    Event::ConnectionLost { reason } => {
                        lost = Some(match reason {
                            quinn_proto::ConnectionError::ApplicationClosed(a) => {
                                CloseReason::PeerClosed {
                                    code: u16::try_from(a.error_code.into_inner())
                                        .unwrap_or(u16::MAX),
                                }
                            }
                            quinn_proto::ConnectionError::LocallyClosed => CloseReason::Shutdown,
                            quinn_proto::ConnectionError::TimedOut => CloseReason::Timeout,
                            other => CloseReason::Transport(other.to_string()),
                        });
                    }
                    Event::Stream(StreamEvent::Opened { dir }) => opened.push(dir),
                    Event::Stream(StreamEvent::Readable { id }) => readable.push(id),
                    Event::Stream(StreamEvent::Writable { .. }) => writable = true,
                    Event::Stream(StreamEvent::Available { .. }) => available = true,
                    _ => {}
                }
            }
        }
        if let Some(reason) = lost {
            let c = self.connections.get_mut(&ch).expect("connection");
            if !c.reported_closed {
                c.reported_closed = true;
                let reason = c.close_reason.take().unwrap_or(reason);
                self.events.push(SimEvent::Closed {
                    node: self.id,
                    lane: c.lane,
                    reason,
                });
                if let (
                    Some(BoundIdentity {
                        replica: Some(r),
                        incarnation: Some(i),
                        ..
                    }),
                    Some(l),
                ) = (c.identity.as_ref(), c.lane)
                {
                    let key = (*r, *i, l);
                    if self.by_link.get(&key) == Some(&ch) {
                        self.by_link.remove(&key);
                    }
                }
            }
            return;
        }
        if connected {
            self.on_connected(ch);
        }
        for dir in opened {
            self.accept_streams(ch, dir, &mut readable);
        }
        for id in readable {
            self.on_readable(ch, id, now);
        }
        if available || writable {
            self.flush(ch);
        }
    }

    fn on_connected(&mut self, ch: ConnectionHandle) {
        let c = self.connections.get_mut(&ch).expect("connection");
        if let Side::Dialer {
            local_role,
            local_incarnation,
            lane,
            ..
        } = &c.side
        {
            let mut capabilities = self.capabilities.clone();
            capabilities.push(lane.capability());
            capabilities.sort_unstable();
            capabilities.dedup();
            let hello = MessageV1::Hello(HelloV1 {
                role: *local_role,
                cluster_id: self.cluster,
                domain_id: self.domain,
                incarnation: *local_incarnation,
                capabilities: BoundedVec::new(capabilities).expect("bounded"),
            })
            .encode()
            .expect("hello");
            let id = c.conn.streams().open(Dir::Bi).expect("control stream");
            c.control_send = Some(id);
            c.control_recv = Some(id);
            c.readers.insert(id, (FrameReader::new(), false));
            c.pending_writes.push_back((id, hello, false));
            c.phase = Phase::HelloSent;
        }
        self.flush(ch);
    }

    fn accept_streams(&mut self, ch: ConnectionHandle, dir: Dir, readable: &mut Vec<StreamId>) {
        let c = self.connections.get_mut(&ch).expect("connection");
        while let Some(id) = c.conn.streams().accept(dir) {
            if dir == Dir::Bi && c.control_recv.is_none() {
                c.control_recv = Some(id);
                c.control_send = Some(id);
            }
            c.readers.insert(id, (FrameReader::new(), false));
            readable.push(id);
        }
    }

    fn on_readable(&mut self, ch: ConnectionHandle, id: StreamId, now: Instant) {
        let (is_control, lane, phase) = {
            let c = self.connections.get(&ch).expect("connection");
            (c.control_recv == Some(id), c.lane, c.phase)
        };
        if !is_control && lane.is_some_and(|l| self.stalled.contains(&l)) {
            // The consumer of this lane is stalled: leave the data unread
            // so flow control, not buffering, holds the sender.
            return;
        }
        // Read what is available into the frame reader.
        let (chunks, finished) = {
            let c = self.connections.get_mut(&ch).expect("connection");
            let mut out = Vec::new();
            let mut finished = false;
            match c.conn.recv_stream(id).read(true) {
                Ok(mut chunks) => {
                    loop {
                        match chunks.next(16 * 1024) {
                            Ok(Some(chunk)) => out.push(chunk.bytes),
                            Ok(None) => {
                                finished = true;
                                break;
                            }
                            Err(ReadError::Blocked) => break,
                            Err(ReadError::Reset(_)) => {
                                finished = true;
                                break;
                            }
                        }
                    }
                    let _ = chunks.finalize();
                }
                Err(_) => finished = true,
            }
            (out, finished)
        };
        let c = self.connections.get_mut(&ch).expect("connection");
        let Some((reader, delivered)) = c.readers.get_mut(&id) else {
            return;
        };
        let arrived: usize = chunks.iter().map(Bytes::len).sum();
        if *delivered {
            // The stream already carried its one frame.
            if arrived > 0 {
                self.close(ch, CloseReason::Malformed("Trailing".into()), now);
            } else if finished {
                c.readers.remove(&id);
            }
            return;
        }
        // More bytes than one frame of the largest class can need is a
        // protocol violation, not something to buffer.
        let mut overflow = None;
        for bytes in &chunks {
            if let Err(e) = reader.push(bytes) {
                overflow = Some(e);
                break;
            }
        }
        if let Some(e) = overflow {
            self.close(ch, CloseReason::Malformed(format!("{e:?}")), now);
            return;
        }
        let frame = match frame_or_close(reader) {
            Ok(f) => f,
            Err(e) => {
                self.close(ch, CloseReason::Malformed(format!("{e:?}")), now);
                return;
            }
        };
        if is_control {
            let Some(frame) = frame else {
                return;
            };
            match (phase, decode(&frame)) {
                (Phase::HelloSent, Ok(MessageV1::HelloAck(ack))) => self.on_hello_ack(ch, ack, now),
                (Phase::Fresh, Ok(MessageV1::Hello(hello))) => self.on_hello(ch, hello, now),
                (_, Ok(MessageV1::Close(close))) => {
                    self.close(ch, CloseReason::PeerClosed { code: close.code }, now)
                }
                (_, Ok(_)) => self.close(ch, CloseReason::Malformed("control frame".into()), now),
                (_, Err(e)) => self.close(ch, CloseReason::Malformed(format!("{e:?}")), now),
            }
            return;
        }
        // One frame per stream: nothing may follow it.
        match frame {
            Some(frame) => {
                if reader.finish().is_err() {
                    self.close(ch, CloseReason::Malformed("Trailing".into()), now);
                    return;
                }
                *delivered = true;
                if finished {
                    c.readers.remove(&id);
                }
                self.deliver(ch, id, frame);
            }
            None if finished => {
                self.close(ch, CloseReason::Malformed("Truncated".into()), now);
            }
            None => {}
        }
    }

    fn deliver(&mut self, ch: ConnectionHandle, _id: StreamId, frame: coord_types::wire_v1::Frame) {
        let c = self.connections.get(&ch).expect("connection");
        let (Some(identity), Some(lane)) = (c.identity.as_ref(), c.lane) else {
            return;
        };
        let (Some(replica), Some(incarnation)) = (identity.replica, identity.incarnation) else {
            return;
        };
        let connection_id = c.connection_id;
        self.events.push(SimEvent::PeerFrame {
            node: self.id,
            lane,
            provenance: PeerProvenance::from_transport(replica, incarnation, connection_id),
            kind: frame.kind,
            payload: frame.payload,
        });
    }

    fn peer_certs(conn: &quinn_proto::Connection) -> Option<Vec<CertificateDer<'static>>> {
        conn.crypto_session()
            .peer_identity()
            .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())
            .map(|v| *v)
            .filter(|v| !v.is_empty())
    }

    fn alpn_class(conn: &quinn_proto::Connection) -> Option<Class> {
        conn.crypto_session()
            .handshake_data()
            .and_then(|d| {
                d.downcast::<quinn_proto::crypto::rustls::HandshakeData>()
                    .ok()
            })
            .and_then(|d| d.protocol)
            .and_then(|p| Class::of_alpn(&p))
    }

    fn on_hello(&mut self, ch: ConnectionHandle, hello: HelloV1, now: Instant) {
        let (class, certs) = {
            let c = self.connections.get(&ch).expect("connection");
            (Self::alpn_class(&c.conn), Self::peer_certs(&c.conn))
        };
        let Some(class) = class else {
            self.close(ch, CloseReason::Rejected("alpn".into()), now);
            return;
        };
        let Some(certs) = certs else {
            self.close(ch, CloseReason::Rejected("no certificate".into()), now);
            return;
        };
        let reject = |what: String| CloseReason::Rejected(what);
        if hello.cluster_id != self.cluster {
            self.close(ch, reject("cluster".into()), now);
            return;
        }
        if hello.domain_id != self.domain {
            self.close(ch, reject("domain".into()), now);
            return;
        }
        if role_class(hello.role) != class {
            self.close(ch, reject("role class".into()), now);
            return;
        }
        if class == Class::Peer && hello.incarnation.is_none() {
            self.close(ch, reject("incarnation".into()), now);
            return;
        }
        let lane = match lane_of_hello(&hello) {
            Ok(l) => l,
            Err(e) => {
                self.close(ch, reject(format!("lane {e:?}")), now);
                return;
            }
        };
        let bound = match self.binder.bind(&certs, &hello) {
            Ok(b) => b,
            Err(e) => {
                self.close(ch, reject(format!("{e:?}")), now);
                return;
            }
        };
        let mut granted: Vec<u16> = hello
            .capabilities
            .as_slice()
            .iter()
            .copied()
            .filter(|c| self.capabilities.contains(c))
            .collect();
        granted.push(lane.capability());
        granted.sort_unstable();
        granted.dedup();
        let ack = MessageV1::HelloAck(HelloAckV1 {
            capabilities: BoundedVec::new(granted.clone()).expect("bounded"),
            max_inflight: self.limits.max_inflight,
        })
        .encode()
        .expect("ack");
        let identity = BoundIdentity {
            role: bound.role,
            replica: bound.replica,
            incarnation: bound.incarnation,
            capabilities: granted,
        };
        let limits = self.limits.lanes[lane.index()];
        let c = self.connections.get_mut(&ch).expect("connection");
        c.conn
            .set_max_concurrent_streams(Dir::Uni, VarInt::from_u32(limits.max_uni_streams));
        c.conn
            .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(limits.max_bidi_streams));
        // The acceptor starts at the cross-lane floor and is raised to
        // the lane once `Hello` names it, windows included. Raising only
        // the stream counts would leave bulk and watch credit exhausting
        // at a different point here than in the real adapter.
        if let Ok(window) = VarInt::from_u64(limits.receive_window) {
            c.conn.set_receive_window(window);
        }
        let control = c.control_send.expect("control stream");
        c.pending_writes.push_back((control, ack, false));
        c.phase = Phase::Ready;
        c.class = Some(class);
        c.lane = Some(lane);
        c.identity = Some(identity.clone());
        if let (Some(r), Some(i)) = (identity.replica, identity.incarnation) {
            self.by_link.insert((r, i, lane), ch);
        }
        self.events.push(SimEvent::Connected {
            node: self.id,
            lane,
            class,
            identity,
        });
        self.flush(ch);
    }

    fn on_hello_ack(&mut self, ch: ConnectionHandle, ack: HelloAckV1, now: Instant) {
        let c = self.connections.get_mut(&ch).expect("connection");
        let Side::Dialer {
            lane,
            expected,
            local_role,
            ..
        } = &c.side
        else {
            return;
        };
        let (lane, local_role) = (*lane, *local_role);
        let expected = expected.clone();
        // Every certificate here is issued by one trusted authority, so
        // TLS succeeding says nothing about *which* node answered. The
        // production dialer binds the server's certificate to the
        // identity it expected before it accepts anything; without the
        // same binding the simulator would negotiate with one node,
        // register it under another replica and misattribute its frames.
        let certs = Self::peer_certs(&c.conn);
        let claim = HelloV1 {
            role: expected.role,
            cluster_id: self.cluster,
            domain_id: self.domain,
            incarnation: expected.incarnation,
            capabilities: BoundedVec::new(Vec::new()).expect("bounded"),
        };
        let bound = certs
            .as_ref()
            .and_then(|certs| self.binder.bind(certs, &claim).ok());
        let accepted = bound.as_ref().is_some_and(|b| {
            b.replica == expected.replica && b.incarnation == expected.incarnation
        });
        if !accepted {
            self.close(ch, CloseReason::Rejected("identity".into()), now);
            return;
        }
        let identity = BoundIdentity {
            role: expected.role,
            replica: expected.replica,
            incarnation: expected.incarnation,
            capabilities: ack.capabilities.as_slice().to_vec(),
        };
        let class = role_class(local_role);
        c.phase = Phase::Ready;
        c.class = Some(class);
        c.lane = Some(lane);
        c.identity = Some(identity.clone());
        if let (Some(r), Some(i)) = (identity.replica, identity.incarnation) {
            self.by_link.insert((r, i, lane), ch);
        }
        self.events.push(SimEvent::Connected {
            node: self.id,
            lane,
            class,
            identity,
        });
    }

    fn close(&mut self, ch: ConnectionHandle, reason: CloseReason, now: Instant) {
        let c = self.connections.get_mut(&ch).expect("connection");
        if c.phase == Phase::Closing {
            return;
        }
        c.phase = Phase::Closing;
        let code = match &reason {
            CloseReason::Malformed(_) => CloseCode::Protocol,
            CloseReason::Rejected(_) => CloseCode::Rejected,
            CloseReason::Timeout => CloseCode::Timeout,
            CloseReason::Shutdown => CloseCode::Shutdown,
            _ => CloseCode::Orderly,
        };
        c.close_reason = Some(reason.clone());
        c.conn
            .close(now, VarInt::from_u32(code as u32), Bytes::new());
        c.reported_closed = true;
        self.events.push(SimEvent::Closed {
            node: self.id,
            lane: c.lane,
            reason,
        });
    }

    /// Write pending frames as credit and flow control allow.
    fn flush(&mut self, ch: ConnectionHandle) {
        let c = self.connections.get_mut(&ch).expect("connection");
        // A stream is opened only when there is a frame for it, so a
        // frame is never taken out of the queue and then dropped for want
        // of credit: without credit it simply stays queued, and the lane's
        // depth keeps bounding what is held.
        while !c.pending_opens.is_empty() {
            let Some(id) = c.conn.streams().open(Dir::Uni) else {
                break;
            };
            let queued = c.pending_opens.pop().expect("not empty");
            c.pending_writes.push_back((id, queued.frame, true));
        }
        while let Some((id, data, finish)) = c.pending_writes.pop_front() {
            match c.conn.send_stream(id).write(&data) {
                Ok(n) if n == data.len() => {
                    if finish {
                        let _ = c.conn.send_stream(id).finish();
                    }
                }
                Ok(n) => {
                    c.pending_writes
                        .push_front((id, data[n..].to_vec(), finish));
                    break;
                }
                Err(WriteError::Blocked) => {
                    c.pending_writes.push_front((id, data, finish));
                    break;
                }
                Err(_) => {}
            }
        }
    }
}

/// The packet world: nodes, links, clock and the datagram trace.
pub struct PacketWorld {
    nodes: Vec<Node>,
    faults: BTreeMap<(NodeId, NodeId), LinkFaults>,
    default_faults: LinkFaults,
    rng: NamedStreams,
    clock: Clock,
    trace: blake3::Hasher,
    datagrams: u64,
    dropped: u64,
    time: Arc<VirtualTime>,
    sequence: u64,
}

impl PacketWorld {
    /// A world over `nodes` with a master seed; every endpoint's protocol
    /// RNG is derived from it.
    pub fn new(seed: [u8; 32], nodes: Vec<NodeConfig>) -> Self {
        let mut rng = NamedStreams::new(seed);
        let time = Arc::new(VirtualTime {
            unix_base: 1_800_000_000,
        });
        let nodes = nodes
            .into_iter()
            .enumerate()
            .map(|(i, config)| {
                let mut s = [0u8; 32];
                s[..16].copy_from_slice(&rng.bytes16(&format!("endpoint-{i}-a")));
                s[16..].copy_from_slice(&rng.bytes16(&format!("endpoint-{i}-b")));
                Node::new(i, config, s, time.clone())
            })
            .collect();
        PacketWorld {
            nodes,
            faults: BTreeMap::new(),
            default_faults: LinkFaults::default(),
            rng,
            clock: Clock::new(),
            trace: blake3::Hasher::new(),
            datagrams: 0,
            dropped: 0,
            time,
            sequence: 0,
        }
    }

    /// Faults of every link without an explicit entry.
    pub fn set_default_faults(&mut self, faults: LinkFaults) {
        self.default_faults = faults;
    }

    /// Faults of one directed link.
    pub fn set_link(&mut self, from: NodeId, to: NodeId, faults: LinkFaults) {
        self.faults.insert((from, to), faults);
    }

    /// Current tick.
    pub const fn tick(&self) -> u64 {
        self.clock.tick()
    }

    /// The unix time the TLS stack sees.
    pub fn tls_time(&self) -> u64 {
        self.time.unix_base
    }

    /// Datagrams delivered and dropped so far.
    pub const fn datagrams(&self) -> (u64, u64) {
        (self.datagrams, self.dropped)
    }

    /// Digest of the datagram trace: `(tick, from, to, size)` of every
    /// delivered datagram, in order.
    pub fn trace_digest(&self) -> [u8; 32] {
        *self.trace.clone().finalize().as_bytes()
    }

    /// Stall a lane's consumer at a node: its streams are no longer read.
    pub fn stall(&mut self, node: NodeId, lane: Lane) {
        self.nodes[node].stalled.insert(lane);
    }

    /// Resume a stalled lane.
    pub fn resume(&mut self, node: NodeId, lane: Lane) {
        self.nodes[node].stalled.remove(&lane);
        let handles: Vec<ConnectionHandle> = self.nodes[node].connections.keys().copied().collect();
        for ch in handles {
            let ids: Vec<StreamId> = self.nodes[node].connections[&ch]
                .readers
                .keys()
                .copied()
                .collect();
            let now = self.clock.now();
            for id in ids {
                self.nodes[node].on_readable(ch, id, now);
            }
        }
    }

    /// Open a `lane` connection from `from` to `to` as `local_role` (the
    /// production `connect` signature plus the two node identities).
    #[allow(clippy::too_many_arguments)]
    pub fn connect(
        &mut self,
        from: NodeId,
        to: NodeId,
        local_role: PeerRole,
        local_incarnation: Option<ReplicaIncarnation>,
        lane: Lane,
        server_name: &str,
        expected: BoundIdentity,
    ) -> Result<(), coord_transport::TransportError> {
        if !role_lanes(local_role).contains(&lane) {
            return Err(coord_transport::TransportError::LaneNotAdmitted(lane));
        }
        let remote = node_addr(to);
        let now = self.clock.now();
        let node = &mut self.nodes[from];
        let class = role_class(local_role);
        let tls = match class {
            Class::Api => node.client_tls[0].clone(),
            Class::Peer => node.client_tls[1].clone(),
        };
        let mut config = quinn_proto::ClientConfig::new(tls);
        config.transport_config(node.lane_transport[lane.index()].clone());
        let (ch, conn) = node
            .endpoint
            .connect(now, config, remote, server_name)
            .map_err(|e| coord_transport::TransportError::Connect(e.to_string()))?;
        let id = node.next_connection;
        node.next_connection += 1;
        node.connections.insert(
            ch,
            Conn {
                conn,
                side: Side::Dialer {
                    local_role,
                    local_incarnation,
                    lane,
                    expected,
                },
                phase: Phase::Fresh,
                class: Some(class),
                lane: Some(lane),
                identity: None,
                control_send: None,
                control_recv: None,
                readers: BTreeMap::new(),
                pending_writes: VecDeque::new(),
                pending_opens: FairQueue::new(
                    node.limits.lanes[lane.index()].queue_depth,
                    node.limits.lanes[lane.index()].max_groups,
                ),
                budget: Budget::new(
                    node.limits.budget.destination_bytes,
                    node.limits.budget.control_reserve,
                ),
                timeout: None,
                connection_id: id,
                reported_closed: false,
                close_reason: None,
            },
        );
        Ok(())
    }

    /// Queue one frame from `from` to a replica's lane; it is handed to
    /// QUIC as stream credit and flow control allow.
    pub fn send(
        &mut self,
        from: NodeId,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        lane: Lane,
        group: DomainId,
        frame: Vec<u8>,
    ) -> Result<(), coord_transport::SendError> {
        let now = self.clock.now();
        let node = &mut self.nodes[from];
        let ch = *node
            .by_link
            .get(&(replica, incarnation, lane))
            .ok_or(coord_transport::SendError::NotConnected)?;
        let node_budget = &node.node_budget;
        let c = node
            .connections
            .get_mut(&ch)
            .ok_or(coord_transport::SendError::NotConnected)?;
        // Admission is what production does, or an overload scenario here
        // would accept frames the real adapter refuses, report a
        // different visible outcome and grow without the configured
        // bounds.
        let bytes = frame.len();
        for budget in [&c.budget, node_budget] {
            if let Err(BudgetError::TooLarge { bytes, limit }) = budget.check(lane, bytes) {
                return Err(coord_transport::SendError::TooLarge { bytes, limit });
            }
        }
        match c.pending_opens.push(Queued {
            group,
            frame: frame.clone(),
            enqueued: now,
        }) {
            Ok(()) => {}
            Err(QueueError::GroupFull) => {
                return Err(coord_transport::SendError::QueueFull { lane });
            }
            Err(QueueError::TooManyGroups) => {
                return Err(coord_transport::SendError::TooManyGroups { lane });
            }
        }
        node.sent.push((lane, frame));
        node.flush(ch);
        Ok(())
    }

    /// Drain the events of a node.
    pub fn take_events(&mut self, node: NodeId) -> Vec<SimEvent> {
        std::mem::take(&mut self.nodes[node].events)
    }

    /// Closed-connection events a node has recorded (not yet taken).
    pub fn nodes_closed(&self, node: NodeId) -> usize {
        self.nodes[node]
            .events
            .iter()
            .filter(|e| matches!(e, SimEvent::Closed { .. }))
            .count()
    }

    /// Negotiated connections of a node.
    pub fn connections(&self, node: NodeId) -> usize {
        self.nodes[node]
            .connections
            .values()
            .filter(|c| c.phase == Phase::Ready)
            .count()
    }

    /// Frames a node queued, per lane (the message-level view).
    pub fn sent(&self, node: NodeId) -> &[(Lane, Vec<u8>)] {
        &self.nodes[node].sent
    }

    /// One step: drive every node at the current tick, move datagrams
    /// across the links, then advance to the next wakeup. Returns false
    /// when nothing is pending.
    pub fn step(&mut self) -> bool {
        for i in 0..self.nodes.len() {
            self.nodes[i].drive_incoming(&self.clock);
            self.nodes[i].drive_outgoing(&self.clock);
            let outbound = std::mem::take(&mut self.nodes[i].outbound);
            for (transmit, bytes) in outbound {
                self.route(i, transmit, bytes);
            }
        }
        let next = self
            .nodes
            .iter()
            .filter_map(|n| n.next_wakeup(&self.clock))
            .min();
        match next {
            Some(t) => {
                // Never spin at one tick: a timer that re-arms at "now"
                // still moves virtual time forward by one tick.
                self.clock.tick = t.max(self.clock.tick + 1);
                true
            }
            None => false,
        }
    }

    /// Step until `done` holds or `max_ticks` of virtual time passed.
    /// Returns whether `done` held.
    pub fn run_until(
        &mut self,
        max_ticks: u64,
        mut done: impl FnMut(&PacketWorld) -> bool,
    ) -> bool {
        let deadline = self.clock.tick.saturating_add(max_ticks);
        loop {
            if done(self) {
                return true;
            }
            if self.clock.tick >= deadline || !self.step() {
                return done(self);
            }
        }
    }

    fn route(&mut self, from: NodeId, transmit: Transmit, bytes: Bytes) {
        let Some(to) = self
            .nodes
            .iter()
            .position(|n| n.addr == transmit.destination)
        else {
            self.dropped += 1;
            return;
        };
        let faults = self
            .faults
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_faults);
        let name = format!("link-{from}-{to}");
        if faults.cut || bytes.len() > faults.mtu || self.rng.chance(&name, faults.loss_ppm) {
            self.dropped += 1;
            return;
        }
        let copies = if self.rng.chance(&name, faults.duplicate_ppm) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let mut delay = self.rng.range(
                &name,
                faults.delay_min,
                faults.delay_max.max(faults.delay_min) + 1,
            );
            if self.rng.chance(&name, faults.reorder_ppm) {
                delay += faults.reorder_ticks;
            }
            let at = self.clock.tick + delay;
            self.sequence += 1;
            self.datagrams += 1;
            self.trace.update(&at.to_be_bytes());
            self.trace.update(&(from as u64).to_be_bytes());
            self.trace.update(&(to as u64).to_be_bytes());
            self.trace.update(&(bytes.len() as u64).to_be_bytes());
            let inbound = &mut self.nodes[to].inbound;
            let pos = inbound.partition_point(|(t, s, _, _)| (*t, *s) <= (at, self.sequence));
            inbound.insert(
                pos,
                (
                    at,
                    self.sequence,
                    node_addr(from),
                    BytesMut::from(&bytes[..]),
                ),
            );
        }
    }

    /// The visible outcome: per node, the frames it received, keyed by
    /// sender replica and lane, as payload digests in arrival order.
    pub fn visible_outcome(&self) -> BTreeMap<(NodeId, ReplicaId, Lane), Vec<[u8; 32]>> {
        let mut out: BTreeMap<(NodeId, ReplicaId, Lane), Vec<[u8; 32]>> = BTreeMap::new();
        for n in &self.nodes {
            for e in &n.events {
                if let SimEvent::PeerFrame {
                    node,
                    lane,
                    provenance,
                    payload,
                    ..
                } = e
                {
                    out.entry((*node, provenance.from(), *lane))
                        .or_default()
                        .push(*blake3::hash(payload).as_bytes());
                }
            }
        }
        out
    }
}
