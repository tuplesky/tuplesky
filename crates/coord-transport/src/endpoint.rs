//! The endpoint: TLS lifecycle, accept and connect, negotiation, lanes,
//! links with fair queues and budgets, stream readers and owned dispatch.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use coord_core::event::PeerProvenance;
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedBytes, BoundedVec, CloseV1, Frame, HelloAckV1, HelloV1, MessageV1, PeerRole, decode,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{RecvStream, SendStream, VarInt};
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::CertificateDer;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio::time::timeout;

use crate::budget::{Budget, BudgetError, Opens};
use crate::config::{ALPN_API, ALPN_PEER, Class, Limits, LocalIdentity, TlsProfile};
use crate::frames::{
    ControlStream, FrameError, KIND_PEER_EVIDENCE, PEER_EVIDENCE_VERSION, read_frame,
};
use crate::identity::{BoundIdentity, IdentityBinder, role_class};
use crate::lane::{self, Lane, LaneLimits, lane_of_hello, role_lanes};
use crate::sched::{FairQueue, LaneStats, QueueError, Queued};

/// Identity of one connection at this endpoint (never reused).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(pub u64);

/// Close codes carried in `Close` frames and QUIC close reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum CloseCode {
    /// Orderly close.
    Orderly = 0,
    /// The peer violated the framing or negotiation protocol.
    Protocol = 1,
    /// Negotiation rejected (origin, role, lane, version, identity).
    Rejected = 2,
    /// A deadline passed.
    Timeout = 3,
    /// This endpoint is shutting down.
    Shutdown = 4,
}

/// Why a connection ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseReason {
    /// A frame or stream violated the protocol (redacted description).
    Malformed(String),
    /// Negotiation was rejected (redacted description).
    Rejected(String),
    /// A handshake or frame deadline passed.
    Timeout,
    /// The peer closed with a code.
    PeerClosed {
        /// Code the peer sent.
        code: u16,
    },
    /// This endpoint shut down.
    Shutdown,
    /// A newer connection took this lane's single slot.
    Replaced,
    /// The QUIC connection failed (redacted description).
    Transport(String),
}

impl CloseReason {
    const fn code(&self) -> CloseCode {
        match self {
            CloseReason::Malformed(_) => CloseCode::Protocol,
            CloseReason::Rejected(_) => CloseCode::Rejected,
            CloseReason::Timeout => CloseCode::Timeout,
            CloseReason::PeerClosed { .. } => CloseCode::Orderly,
            CloseReason::Shutdown => CloseCode::Shutdown,
            CloseReason::Replaced => CloseCode::Orderly,
            CloseReason::Transport(_) => CloseCode::Orderly,
        }
    }
}

/// What the runtime receives. There is deliberately no variant for
/// durability, learning or establishment: transport completion is never
/// any of them.
#[derive(Debug)]
pub enum TransportEvent {
    /// A connection completed negotiation.
    Connected {
        /// Connection.
        connection: ConnectionId,
        /// Class.
        class: Class,
        /// Lane.
        lane: Lane,
        /// Bound identity of the peer.
        identity: BoundIdentity,
        /// Remote address (diagnostic).
        remote: SocketAddr,
    },
    /// A complete peer frame arrived on a peer-class connection.
    PeerFrame {
        /// Connection.
        connection: ConnectionId,
        /// Lane it arrived on.
        lane: Lane,
        /// Provenance bound at negotiation.
        provenance: PeerProvenance,
        /// Frame kind.
        kind: u16,
        /// Schema version.
        version: u16,
        /// Payload.
        payload: Vec<u8>,
    },
    /// A unary request arrived on an API-class connection.
    ApiRequest {
        /// Connection.
        connection: ConnectionId,
        /// Lane it arrived on.
        lane: Lane,
        /// Bound identity of the requester.
        identity: BoundIdentity,
        /// The request frame.
        frame: Frame,
        /// Where the response goes.
        responder: Responder,
    },
    /// A frame the peer delivered on an API connection this side dialed
    /// (output addressed to this node, never a request to serve).
    ApiDelivery {
        /// Connection.
        connection: ConnectionId,
        /// Lane it arrived on.
        lane: Lane,
        /// Bound identity of the peer.
        identity: BoundIdentity,
        /// The delivered frame.
        frame: Frame,
    },
    /// A connection ended (after `Connected`, or instead of it).
    Closed {
        /// Connection.
        connection: ConnectionId,
        /// Lane, when negotiation had reached one.
        lane: Option<Lane>,
        /// Why.
        reason: CloseReason,
    },
}

impl TransportEvent {
    /// The lane whose queue delivers the event.
    const fn lane(&self) -> Lane {
        match self {
            TransportEvent::Connected { lane, .. }
            | TransportEvent::PeerFrame { lane, .. }
            | TransportEvent::ApiRequest { lane, .. }
            | TransportEvent::ApiDelivery { lane, .. } => *lane,
            TransportEvent::Closed { lane, .. } => match lane {
                Some(l) => *l,
                None => Lane::Control,
            },
        }
    }
}

/// The response half of a unary request stream.
pub struct Responder {
    send: SendStream,
    lane: Lane,
    link: Arc<Link>,
    node: Arc<Budget>,
    deadline: Duration,
}

impl core::fmt::Debug for Responder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Responder")
            .field("lane", &self.lane)
            .finish()
    }
}

impl Responder {
    /// Write the complete response frame and finish the stream.
    ///
    /// A reply is traffic like any other: it is admitted under the
    /// destination and node budgets first, and the bytes stay counted
    /// against both until the peer acknowledges them. Writing straight to
    /// the stream would let concurrent unary requests, or many API
    /// connections, hand window after window to QUIC outside the caps
    /// this lane exists to enforce.
    pub async fn respond(mut self, frame: Vec<u8>) -> Result<(), SendError> {
        let bytes = frame.len();
        for budget in [&self.link.budget, self.node.as_ref()] {
            if let Err(BudgetError::TooLarge { bytes, limit }) = budget.check(self.lane, bytes) {
                return Err(SendError::TooLarge { bytes, limit });
            }
        }
        let dest = self
            .link
            .budget
            .acquire(self.lane, bytes)
            .await
            .map_err(|_| SendError::NotConnected)?;
        let node = self
            .node
            .acquire(self.lane, bytes)
            .await
            .map_err(|_| SendError::NotConnected)?;
        timeout(self.deadline, async {
            self.send
                .write_all(&frame)
                .await
                .map_err(|e| SendError::Stream(e.to_string()))?;
            self.send
                .finish()
                .map_err(|e| SendError::Stream(e.to_string()))
        })
        .await
        .map_err(|_| SendError::Timeout)??;
        let stopped = self.send.stopped();
        tokio::spawn(async move {
            let _dest = dest;
            let _node = node;
            let _ = stopped.await;
        });
        Ok(())
    }
}

/// Where a frame goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Destination {
    /// A replica's lane (peer roles bound with replica and incarnation).
    Replica {
        /// Replica.
        replica: ReplicaId,
        /// Incarnation.
        incarnation: ReplicaIncarnation,
        /// Lane.
        lane: Lane,
    },
    /// A specific negotiated connection (API-class peers).
    Connection(ConnectionId),
}

/// Why a frame was not admitted to the transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    /// No negotiated connection for that destination.
    NotConnected,
    /// The frame can never fit the lane's share of the destination budget.
    TooLarge {
        /// Frame bytes.
        bytes: usize,
        /// Bytes the lane may hold at most.
        limit: usize,
    },
    /// The destination's queue for this group and lane is at its depth.
    QueueFull {
        /// Lane.
        lane: Lane,
    },
    /// Too many groups have frames queued on this lane.
    TooManyGroups {
        /// Lane.
        lane: Lane,
    },
    /// The stream could not be opened within the deadline.
    Timeout,
    /// The stream failed (redacted description).
    Stream(String),
}

/// Why the endpoint could not be built or a connection could not be made.
#[derive(Debug)]
pub enum TransportError {
    /// TLS configuration failed (redacted description).
    Tls(String),
    /// Socket failure.
    Io(std::io::Error),
    /// Connection failed (redacted description).
    Connect(String),
    /// Negotiation with the remote failed.
    Rejected(CloseReason),
    /// The dialing role may not open that lane.
    LaneNotAdmitted(Lane),
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        TransportError::Io(e)
    }
}

struct Peer {
    id: ConnectionId,
    conn: quinn::Connection,
    class: Class,
    lane: Lane,
    identity: BoundIdentity,
    close_reason: Mutex<Option<CloseReason>>,
    /// Whether this side accepted the connection. An accepted API
    /// connection carries client requests inward; a dialed one carries
    /// this node's output outward and receives the peer's replies.
    accepted: bool,
    /// Frames held between the stream and the lane's event queue. When
    /// the consumer stalls, the readers hold their permits and no further
    /// stream is accepted: QUIC flow control then backs the sender up
    /// instead of this side buffering without limit.
    readers: Arc<Semaphore>,
}

impl Peer {
    fn new(
        id: ConnectionId,
        conn: quinn::Connection,
        class: Class,
        lane: Lane,
        identity: BoundIdentity,
        accepted: bool,
        limits: &LaneLimits,
    ) -> Arc<Peer> {
        let readers = match class {
            Class::Peer => limits.max_uni_streams,
            Class::Api => limits.max_bidi_streams,
        };
        Arc::new(Peer {
            id,
            conn,
            class,
            lane,
            identity,
            close_reason: Mutex::new(None),
            accepted,
            readers: Arc::new(Semaphore::new(readers.max(1) as usize)),
        })
    }
}

/// Link key: a replica for peer roles, the connection otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum LinkKey {
    Replica(ReplicaId, ReplicaIncarnation),
    Connection(ConnectionId),
}

struct LaneState {
    peer: Option<Arc<Peer>>,
    queue: FairQueue,
    stats: LaneStats,
}

/// Everything shared by the lanes to one destination.
struct Link {
    budget: Budget,
    opens: Opens,
    lanes: [Mutex<LaneState>; 4],
    notify: [Notify; 4],
}

impl Link {
    fn new(limits: &Limits) -> Self {
        let lane_state = |l: &LaneLimits| {
            Mutex::new(LaneState {
                peer: None,
                queue: FairQueue::new(l.queue_depth, l.max_groups),
                stats: LaneStats::default(),
            })
        };
        Link {
            budget: Budget::new(
                limits.budget.destination_bytes,
                limits.budget.control_reserve,
            ),
            opens: Opens::new(limits.budget.max_opens),
            lanes: [
                lane_state(&limits.lanes[0]),
                lane_state(&limits.lanes[1]),
                lane_state(&limits.lanes[2]),
                lane_state(&limits.lanes[3]),
            ],
            notify: [Notify::new(), Notify::new(), Notify::new(), Notify::new()],
        }
    }
}

struct Shared {
    cluster: ClusterId,
    domain: DomainId,
    capabilities: Vec<u16>,
    limits: Limits,
    binder: Arc<dyn IdentityBinder>,
    events: [mpsc::Sender<TransportEvent>; 4],
    peers: Mutex<HashMap<ConnectionId, Arc<Peer>>>,
    links: Mutex<HashMap<LinkKey, Arc<Link>>>,
    node_budget: Arc<Budget>,
    next: AtomicU64,
    accept_permits: Arc<Semaphore>,
}

impl Shared {
    fn next_id(&self) -> ConnectionId {
        ConnectionId(self.next.fetch_add(1, Ordering::SeqCst))
    }

    async fn emit(&self, event: TransportEvent) {
        // A closed receiver means the runtime is gone; dropping is fine.
        let _ = self.events[event.lane().index()].send(event).await;
    }

    fn link_key(peer: &Peer) -> LinkKey {
        match (peer.identity.replica, peer.identity.incarnation) {
            (Some(r), Some(i)) => LinkKey::Replica(r, i),
            _ => LinkKey::Connection(peer.id),
        }
    }

    fn link(&self, key: LinkKey) -> Arc<Link> {
        self.links
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| Arc::new(Link::new(&self.limits)))
            .clone()
    }

    /// Register a negotiated connection on its link's lane and start the
    /// lane's sender.
    fn register(self: &Arc<Self>, peer: Arc<Peer>) {
        self.peers.lock().unwrap().insert(peer.id, peer.clone());
        let link = self.link(Self::link_key(&peer));
        let displaced = {
            let mut state = link.lanes[peer.lane.index()].lock().unwrap();
            state.peer.replace(peer.clone())
        };
        // One connection per lane: a second dial of the same lane takes
        // the slot, and the connection it displaces is closed here.
        // Leaving it open would keep its receive loop, streams and
        // windows alive, so repeated dials would multiply exactly the
        // capacity the lane bounds.
        if let Some(old) = displaced
            && old.id != peer.id
        {
            *old.close_reason.lock().unwrap() = Some(CloseReason::Replaced);
            old.conn
                .close(VarInt::from_u32(CloseCode::Orderly as u32), b"replaced");
        }
        tokio::spawn(sender_loop(self.clone(), link, peer));
    }

    fn deregister(&self, peer: &Peer) {
        self.peers.lock().unwrap().remove(&peer.id);
        let key = Self::link_key(peer);
        let link = self.links.lock().unwrap().get(&key).cloned();
        if let Some(link) = link {
            let mut state = link.lanes[peer.lane.index()].lock().unwrap();
            if state.peer.as_ref().is_some_and(|p| p.id == peer.id) {
                state.peer = None;
                // Frames queued for a lane whose connection is gone are
                // transport losses: dropped and counted, never buffered.
                while state.queue.pop().is_some() {
                    state.stats.refused += 1;
                }
                state.stats.queued = 0;
            }
            drop(state);
            link.notify[peer.lane.index()].notify_one();
            // A link whose every lane is idle holds only empty queues,
            // semaphores and counters. Ordinary connect/disconnect churn
            // creates a new key per API connection and per replica
            // incarnation, so keeping them would grow without bound over
            // the process lifetime. The map is locked first, so a
            // registration racing this cannot insert into an entry that
            // is about to be removed.
            let mut links = self.links.lock().unwrap();
            let idle = link.lanes.iter().all(|l| {
                let state = l.lock().unwrap();
                state.peer.is_none() && state.queue.is_empty()
            });
            if idle && links.get(&key).is_some_and(|held| Arc::ptr_eq(held, &link)) {
                links.remove(&key);
            }
        }
    }
}

/// The transport endpoint of one process.
pub struct Transport {
    endpoint: quinn::Endpoint,
    shared: Arc<Shared>,
    events: [mpsc::Receiver<TransportEvent>; 4],
    client_tls: [Arc<QuicClientConfig>; 2],
    lane_transport: [Arc<quinn::TransportConfig>; 4],
}

fn tls_err(e: impl std::fmt::Display) -> TransportError {
    TransportError::Tls(e.to_string())
}

impl Transport {
    /// Bind a server and client endpoint on `addr` with the fixed TLS
    /// profile ([`TlsProfile::FIXED`]) and start accepting.
    pub fn bind(
        addr: SocketAddr,
        local: LocalIdentity,
        binder: Arc<dyn IdentityBinder>,
        limits: Limits,
    ) -> Result<Transport, TransportError> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier =
            WebPkiClientVerifier::builder_with_provider(local.roots.clone(), provider.clone())
                .build()
                .map_err(tls_err)?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(tls_err)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(local.chain.clone(), local.key.clone_key())
            .map_err(tls_err)?;
        server.alpn_protocols = vec![ALPN_API.to_vec(), ALPN_PEER.to_vec()];
        server.max_early_data_size = TlsProfile::FIXED.max_early_data_size;
        let quic_server = QuicServerConfig::try_from(server).map_err(tls_err)?;

        let lane_config = |i: usize| {
            lane::transport_config(&limits.lanes[i], limits.idle_timeout, limits.keep_alive)
                .map_err(TransportError::Tls)
        };
        let lane_transport: [Arc<quinn::TransportConfig>; 4] = [
            lane_config(0)?,
            lane_config(1)?,
            lane_config(2)?,
            lane_config(3)?,
        ];
        // Accepted connections start with the floor of every lane's
        // limits (credit can only be raised once advertised); the lane
        // declared in `Hello` raises them to its own.
        let floor = lane::transport_config(
            &LaneLimits::floor(&limits.lanes),
            limits.idle_timeout,
            limits.keep_alive,
        )
        .map_err(TransportError::Tls)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));
        server_config
            .transport_config(floor)
            .migration(TlsProfile::FIXED.server_migration)
            .max_incoming(limits.max_connections);

        let client_for = |alpn: &[u8]| -> Result<Arc<QuicClientConfig>, TransportError> {
            let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(tls_err)?
                .with_root_certificates(local.roots.clone())
                .with_client_auth_cert(local.chain.clone(), local.key.clone_key())
                .map_err(tls_err)?;
            client.alpn_protocols = vec![alpn.to_vec()];
            client.enable_early_data = TlsProfile::FIXED.client_early_data;
            Ok(Arc::new(
                QuicClientConfig::try_from(client).map_err(tls_err)?,
            ))
        };
        let client_tls = [client_for(ALPN_API)?, client_for(ALPN_PEER)?];

        let mut endpoint = quinn::Endpoint::server(server_config, addr)?;
        let mut default_client = quinn::ClientConfig::new(client_tls[1].clone());
        default_client.transport_config(lane_transport[Lane::Control.index()].clone());
        endpoint.set_default_client_config(default_client);

        let depth = limits.event_queue.max(1);
        let (tx0, rx0) = mpsc::channel(depth);
        let (tx1, rx1) = mpsc::channel(depth);
        let (tx2, rx2) = mpsc::channel(depth);
        let (tx3, rx3) = mpsc::channel(depth);
        let shared = Arc::new(Shared {
            cluster: local.cluster,
            domain: local.domain,
            capabilities: local.capabilities,
            limits,
            binder,
            events: [tx0, tx1, tx2, tx3],
            peers: Mutex::new(HashMap::new()),
            links: Mutex::new(HashMap::new()),
            node_budget: Arc::new(Budget::new(
                limits.budget.node_bytes,
                limits.budget.control_reserve,
            )),
            next: AtomicU64::new(1),
            accept_permits: Arc::new(Semaphore::new(limits.max_connections.max(1))),
        });
        tokio::spawn(accept_loop(endpoint.clone(), shared.clone()));
        Ok(Transport {
            endpoint,
            shared,
            events: [rx0, rx1, rx2, rx3],
            client_tls,
            lane_transport,
        })
    }

    /// The fixed TLS profile.
    pub const fn tls_profile() -> TlsProfile {
        TlsProfile::FIXED
    }

    /// Bound address.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.endpoint.local_addr()?)
    }

    /// Negotiated connections currently open.
    pub fn connections(&self) -> usize {
        self.shared.peers.lock().unwrap().len()
    }

    /// The next owned event, lanes in priority order (control first);
    /// `None` once the endpoint is gone.
    pub async fn next_event(&mut self) -> Option<TransportEvent> {
        let [c, u, w, b] = &mut self.events;
        tokio::select! {
            biased;
            e = c.recv() => e,
            e = u.recv() => e,
            e = w.recv() => e,
            e = b.recv() => e,
        }
    }

    /// The next event of one lane only; other lanes are left untouched.
    pub async fn next_event_in(&mut self, lane: Lane) -> Option<TransportEvent> {
        self.events[lane.index()].recv().await
    }

    /// Open a `lane` connection to `addr` as `local_role`, expecting the
    /// remote to be `expected` (its certificate is bound to that identity
    /// before anything is sent). Registered on `HelloAck`.
    pub async fn connect(
        &self,
        addr: SocketAddr,
        server_name: &str,
        local_role: PeerRole,
        local_incarnation: Option<ReplicaIncarnation>,
        lane: Lane,
        expected: BoundIdentity,
    ) -> Result<ConnectionId, TransportError> {
        if !role_lanes(local_role).contains(&lane) {
            return Err(TransportError::LaneNotAdmitted(lane));
        }
        let class = role_class(local_role);
        let tls = match class {
            Class::Api => self.client_tls[0].clone(),
            Class::Peer => self.client_tls[1].clone(),
        };
        let mut config = quinn::ClientConfig::new(tls);
        config.transport_config(self.lane_transport[lane.index()].clone());
        let connecting = self
            .endpoint
            .connect_with(config, addr, server_name)
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        let limits = self.shared.limits;
        let conn = match timeout(limits.handshake_timeout, connecting).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => return Err(TransportError::Connect(e.to_string())),
            Err(_) => return Err(TransportError::Rejected(CloseReason::Timeout)),
        };
        let id = self.shared.next_id();
        match self
            .negotiate_outgoing(&conn, local_role, local_incarnation, lane, &expected)
            .await
        {
            Ok((identity, control_recv)) => {
                let peer = Peer::new(
                    id,
                    conn.clone(),
                    class,
                    lane,
                    identity.clone(),
                    false,
                    &self.shared.limits.lanes[lane.index()],
                );
                self.shared.register(peer.clone());
                self.shared
                    .emit(TransportEvent::Connected {
                        connection: id,
                        class,
                        lane,
                        identity,
                        remote: conn.remote_address(),
                    })
                    .await;
                tokio::spawn(serve(self.shared.clone(), peer, control_recv));
                Ok(id)
            }
            Err(reason) => {
                close_connection(&conn, None, &reason).await;
                Err(TransportError::Rejected(reason))
            }
        }
    }

    async fn negotiate_outgoing(
        &self,
        conn: &quinn::Connection,
        local_role: PeerRole,
        local_incarnation: Option<ReplicaIncarnation>,
        lane: Lane,
        expected: &BoundIdentity,
    ) -> Result<(BoundIdentity, ControlStream), CloseReason> {
        let shared = &self.shared;
        let certs = peer_certs(conn)?;
        // Bind the server's certificate to the identity we expect before
        // sending anything: a valid certificate for the wrong node is a
        // rejection, not a peer.
        let claim = HelloV1 {
            role: expected.role,
            cluster_id: shared.cluster,
            domain_id: shared.domain,
            incarnation: expected.incarnation,
            capabilities: BoundedVec::new(Vec::new())
                .map_err(|_| CloseReason::Rejected("caps".into()))?,
        };
        let bound = shared
            .binder
            .bind(&certs, &claim)
            .map_err(|e| CloseReason::Rejected(format!("{e:?}")))?;
        if bound.replica != expected.replica || bound.incarnation != expected.incarnation {
            return Err(CloseReason::Rejected("identity".into()));
        }
        let (mut send, recv) = timeout(shared.limits.handshake_timeout, conn.open_bi())
            .await
            .map_err(|_| CloseReason::Timeout)?
            .map_err(|e| CloseReason::Transport(e.to_string()))?;
        let mut capabilities = shared.capabilities.clone();
        capabilities.push(lane.capability());
        capabilities.sort_unstable();
        capabilities.dedup();
        let hello = MessageV1::Hello(HelloV1 {
            role: local_role,
            cluster_id: shared.cluster,
            domain_id: shared.domain,
            incarnation: local_incarnation,
            capabilities: BoundedVec::new(capabilities)
                .map_err(|_| CloseReason::Rejected("caps".into()))?,
        })
        .encode()
        .map_err(|e| CloseReason::Malformed(format!("{e:?}")))?;
        send.write_all(&hello)
            .await
            .map_err(|e| CloseReason::Transport(e.to_string()))?;
        let mut control = ControlStream::new(recv);
        let frame = control
            .next_frame(shared.limits.handshake_timeout)
            .await
            .map_err(frame_reason)?;
        match decode(&frame) {
            Ok(MessageV1::HelloAck(ack)) => {
                tokio::spawn(park_control(send, conn.clone()));
                Ok((
                    BoundIdentity {
                        role: bound.role,
                        replica: bound.replica,
                        incarnation: bound.incarnation,
                        capabilities: ack.capabilities.as_slice().to_vec(),
                    },
                    control,
                ))
            }
            Ok(MessageV1::Close(c)) => Err(CloseReason::PeerClosed { code: c.code }),
            Ok(_) => Err(CloseReason::Malformed("expected hello ack".into())),
            Err(e) => Err(CloseReason::Malformed(format!("{e:?}"))),
        }
    }

    /// Queue one complete frame for `to` in `group`. Success means the
    /// frame was admitted to the lane's fair queue under the destination
    /// and node budgets; it is handed to QUIC by the lane's sender and
    /// that is all a success ever means.
    pub fn send(&self, to: Destination, group: DomainId, frame: Vec<u8>) -> Result<(), SendError> {
        let (key, lane) = match to {
            Destination::Replica {
                replica,
                incarnation,
                lane,
            } => (LinkKey::Replica(replica, incarnation), lane),
            Destination::Connection(id) => {
                let peer = self
                    .shared
                    .peers
                    .lock()
                    .unwrap()
                    .get(&id)
                    .cloned()
                    .ok_or(SendError::NotConnected)?;
                (Shared::link_key(&peer), peer.lane)
            }
        };
        let link = self
            .shared
            .links
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or(SendError::NotConnected)?;
        let bytes = frame.len();
        for budget in [&link.budget, self.shared.node_budget.as_ref()] {
            if let Err(BudgetError::TooLarge { bytes, limit }) = budget.check(lane, bytes) {
                return Err(SendError::TooLarge { bytes, limit });
            }
        }
        let mut state = link.lanes[lane.index()].lock().unwrap();
        if state.peer.is_none() {
            return Err(SendError::NotConnected);
        }
        let result = state.queue.push(Queued {
            group,
            frame,
            enqueued: Instant::now(),
        });
        match result {
            Ok(()) => {
                state.stats.queued = state.queue.len();
                drop(state);
                link.notify[lane.index()].notify_one();
                Ok(())
            }
            Err(QueueError::GroupFull) => {
                state.stats.refused += 1;
                Err(SendError::QueueFull { lane })
            }
            Err(QueueError::TooManyGroups) => {
                state.stats.refused += 1;
                Err(SendError::TooManyGroups { lane })
            }
        }
    }

    /// Queue the same frame for several replicas on one lane. Each
    /// destination is admitted independently under its own budget and
    /// queue; one saturated destination never blocks the others.
    pub fn fan_out(
        &self,
        replicas: &[(ReplicaId, ReplicaIncarnation)],
        lane: Lane,
        group: DomainId,
        frame: &[u8],
    ) -> Vec<Result<(), SendError>> {
        replicas
            .iter()
            .map(|(replica, incarnation)| {
                self.send(
                    Destination::Replica {
                        replica: *replica,
                        incarnation: *incarnation,
                        lane,
                    },
                    group,
                    frame.to_vec(),
                )
            })
            .collect()
    }

    /// Accounting of one lane of a replica link, with the path RTT.
    pub fn stats(
        &self,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        lane: Lane,
    ) -> Option<LaneStats> {
        let link = self
            .shared
            .links
            .lock()
            .unwrap()
            .get(&LinkKey::Replica(replica, incarnation))
            .cloned()?;
        let state = link.lanes[lane.index()].lock().unwrap();
        let mut stats = state.stats;
        stats.queued = state.queue.len();
        stats.rtt = state.peer.as_ref().map_or(Duration::ZERO, |p| p.conn.rtt());
        Some(stats)
    }

    /// Bytes in flight to a replica now, and the most ever.
    pub fn budget_of(
        &self,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
    ) -> Option<(usize, usize)> {
        let link = self
            .shared
            .links
            .lock()
            .unwrap()
            .get(&LinkKey::Replica(replica, incarnation))
            .cloned()?;
        Some((link.budget.in_flight(), link.budget.peak()))
    }

    /// Destination links currently held (one per replica link or API
    /// connection with an active lane). Diagnostic: an idle link is
    /// removed, so this does not grow with connection churn.
    pub fn links(&self) -> usize {
        self.shared.links.lock().unwrap().len()
    }

    /// Bytes in flight across every destination now, and the most ever.
    pub fn node_budget(&self) -> (usize, usize) {
        (
            self.shared.node_budget.in_flight(),
            self.shared.node_budget.peak(),
        )
    }

    /// Close every connection and wait at most `deadline` for the
    /// endpoint to drain. Returns whether it drained in time.
    pub async fn shutdown(self, deadline: Duration) -> bool {
        self.endpoint
            .close(VarInt::from_u32(CloseCode::Shutdown as u32), b"shutdown");
        timeout(deadline, self.endpoint.wait_idle()).await.is_ok()
    }
}

/// One lane's sender: fair across groups, bounded by the destination and
/// node budgets and the open limit, measuring queue and credit waits.
async fn sender_loop(shared: Arc<Shared>, link: Arc<Link>, peer: Arc<Peer>) {
    let lane = peer.lane;
    let idx = lane.index();
    loop {
        let next = {
            let mut state = link.lanes[idx].lock().unwrap();
            if state.peer.as_ref().is_none_or(|p| p.id != peer.id) {
                return;
            }
            let next = state.queue.pop();
            state.stats.queued = state.queue.len();
            if let Some(q) = &next {
                state.stats.queue_wait.record(q.enqueued.elapsed());
            }
            next
        };
        let Some(queued) = next else {
            link.notify[idx].notified().await;
            continue;
        };
        let picked = Instant::now();
        let bytes = queued.frame.len();
        let Ok(dest_permit) = link.budget.acquire(lane, bytes).await else {
            continue;
        };
        let Ok(node_permit) = shared.node_budget.acquire(lane, bytes).await else {
            continue;
        };
        let open_permit = link.opens.acquire().await;
        // An API lane advertises no unidirectional streams and its peer
        // consumes bidirectional ones, so opening a uni stream there
        // would wait for credit that never comes and lose the frame.
        // Each class opens what its lane carries.
        let opened = timeout(shared.limits.frame_timeout, async {
            match peer.class {
                Class::Peer => peer.conn.open_uni().await,
                Class::Api => peer.conn.open_bi().await.map(|(send, _recv)| send),
            }
        })
        .await;
        let mut send = match opened {
            Ok(Ok(s)) => s,
            _ => {
                // The connection is gone or credit never came: a transport
                // loss, counted, never buffered.
                let mut state = link.lanes[idx].lock().unwrap();
                state.stats.refused += 1;
                continue;
            }
        };
        {
            let mut state = link.lanes[idx].lock().unwrap();
            state.stats.credit_wait.record(picked.elapsed());
            state.stats.frames += 1;
            state.stats.bytes += bytes as u64;
        }
        // The deadline covers the write and the finish too. A peer that
        // completes the open and then grants no more flow-control credit
        // would otherwise leave the write pending forever and stall this
        // lane's sender behind it.
        let written = timeout(shared.limits.frame_timeout, async {
            send.write_all(&queued.frame).await.is_ok() && send.finish().is_ok()
        })
        .await;
        if written != Ok(true) {
            let mut state = link.lanes[idx].lock().unwrap();
            state.stats.refused += 1;
            continue;
        }
        // The bytes stay counted against both budgets until the peer
        // acknowledges them or the stream ends otherwise.
        let stopped = send.stopped();
        tokio::spawn(async move {
            let _dest = dest_permit;
            let _node = node_permit;
            let _open = open_permit;
            let _ = stopped.await;
        });
    }
}

fn frame_reason(e: FrameError) -> CloseReason {
    match e {
        FrameError::Timeout => CloseReason::Timeout,
        FrameError::Stream(s) => CloseReason::Transport(s),
        other => CloseReason::Malformed(format!("{other:?}")),
    }
}

fn peer_certs(conn: &quinn::Connection) -> Result<Vec<CertificateDer<'static>>, CloseReason> {
    conn.peer_identity()
        .and_then(|id| id.downcast::<Vec<CertificateDer<'static>>>().ok())
        .map(|v| *v)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CloseReason::Rejected("no certificate".into()))
}

fn negotiated_class(conn: &quinn::Connection) -> Result<Class, CloseReason> {
    conn.handshake_data()
        .and_then(|d| d.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|d| d.protocol)
        .and_then(|p| Class::of_alpn(&p))
        .ok_or_else(|| CloseReason::Rejected("alpn".into()))
}

async fn close_connection(
    conn: &quinn::Connection,
    control: Option<&mut SendStream>,
    reason: &CloseReason,
) {
    let code = reason.code();
    if let Some(send) = control
        && let Ok(reason_bytes) = BoundedBytes::new(b"closed".to_vec())
        && let Ok(frame) = MessageV1::Close(CloseV1 {
            code: code as u16,
            reason: reason_bytes,
        })
        .encode()
    {
        let _ = timeout(Duration::from_millis(200), send.write_all(&frame)).await;
    }
    conn.close(VarInt::from_u32(code as u32), b"");
}

async fn accept_loop(endpoint: quinn::Endpoint, shared: Arc<Shared>) {
    while let Some(incoming) = endpoint.accept().await {
        let Ok(permit) = shared.accept_permits.clone().try_acquire_owned() else {
            incoming.refuse();
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let _permit = permit;
            serve_incoming(shared, incoming).await;
        });
    }
}

async fn serve_incoming(shared: Arc<Shared>, incoming: quinn::Incoming) {
    let id = shared.next_id();
    let conn = match timeout(shared.limits.handshake_timeout, incoming).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            shared
                .emit(TransportEvent::Closed {
                    connection: id,
                    lane: None,
                    reason: CloseReason::Transport(e.to_string()),
                })
                .await;
            return;
        }
        Err(_) => {
            shared
                .emit(TransportEvent::Closed {
                    connection: id,
                    lane: None,
                    reason: CloseReason::Timeout,
                })
                .await;
            return;
        }
    };
    match negotiate_incoming(&shared, &conn).await {
        Ok((class, lane, identity, control_recv)) => {
            let peer = Peer::new(
                id,
                conn.clone(),
                class,
                lane,
                identity.clone(),
                true,
                &shared.limits.lanes[lane.index()],
            );
            shared.register(peer.clone());
            shared
                .emit(TransportEvent::Connected {
                    connection: id,
                    class,
                    lane,
                    identity,
                    remote: conn.remote_address(),
                })
                .await;
            serve(shared, peer, control_recv).await;
        }
        Err((reason, control)) => {
            let mut control = control;
            close_connection(&conn, control.as_mut(), &reason).await;
            shared
                .emit(TransportEvent::Closed {
                    connection: id,
                    lane: None,
                    reason,
                })
                .await;
        }
    }
}

type Negotiated = (Class, Lane, BoundIdentity, ControlStream);

async fn negotiate_incoming(
    shared: &Shared,
    conn: &quinn::Connection,
) -> Result<Negotiated, (CloseReason, Option<SendStream>)> {
    let class = negotiated_class(conn).map_err(|r| (r, None))?;
    let certs = peer_certs(conn).map_err(|r| (r, None))?;
    let (mut send, recv) = match timeout(shared.limits.handshake_timeout, conn.accept_bi()).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err((CloseReason::Transport(e.to_string()), None)),
        Err(_) => return Err((CloseReason::Timeout, None)),
    };
    let mut control = ControlStream::new(recv);
    let frame = match control.next_frame(shared.limits.handshake_timeout).await {
        Ok(f) => f,
        Err(e) => return Err((frame_reason(e), Some(send))),
    };
    let hello = match decode(&frame) {
        Ok(MessageV1::Hello(h)) => h,
        Ok(_) => {
            return Err((
                CloseReason::Malformed("first frame not hello".into()),
                Some(send),
            ));
        }
        Err(e) => return Err((CloseReason::Malformed(format!("{e:?}")), Some(send))),
    };
    if hello.cluster_id != shared.cluster {
        return Err((CloseReason::Rejected("cluster".into()), Some(send)));
    }
    if hello.domain_id != shared.domain {
        return Err((CloseReason::Rejected("domain".into()), Some(send)));
    }
    if role_class(hello.role) != class {
        return Err((CloseReason::Rejected("role class".into()), Some(send)));
    }
    if class == Class::Peer && hello.incarnation.is_none() {
        return Err((CloseReason::Rejected("incarnation".into()), Some(send)));
    }
    let lane = match lane_of_hello(&hello) {
        Ok(l) => l,
        Err(e) => return Err((CloseReason::Rejected(format!("lane {e:?}")), Some(send))),
    };
    let bound = match shared.binder.bind(&certs, &hello) {
        Ok(b) => b,
        Err(e) => return Err((CloseReason::Rejected(format!("{e:?}")), Some(send))),
    };
    let mut granted: Vec<u16> = hello
        .capabilities
        .as_slice()
        .iter()
        .copied()
        .filter(|c| shared.capabilities.contains(c))
        .collect();
    granted.push(lane.capability());
    granted.sort_unstable();
    granted.dedup();
    let ack = match BoundedVec::new(granted.clone()).map(|capabilities| {
        MessageV1::HelloAck(HelloAckV1 {
            capabilities,
            max_inflight: shared.limits.max_inflight,
        })
        .encode()
    }) {
        Ok(Ok(bytes)) => bytes,
        _ => return Err((CloseReason::Malformed("hello ack".into()), Some(send))),
    };
    // The lane's stream limits and window apply from here on.
    lane::apply_to_connection(conn, &shared.limits.lanes[lane.index()]);
    if let Err(e) = send.write_all(&ack).await {
        return Err((CloseReason::Transport(e.to_string()), None));
    }
    // The control stream stays open: `Close` travels on it. The send half
    // is parked with the connection (dropping it would finish the stream).
    tokio::spawn(park_control(send, conn.clone()));
    Ok((
        class,
        lane,
        BoundIdentity {
            role: bound.role,
            replica: bound.replica,
            incarnation: bound.incarnation,
            capabilities: granted,
        },
        control,
    ))
}

/// Keep the control send half alive until the connection closes.
async fn park_control(send: SendStream, conn: quinn::Connection) {
    let _send = send;
    conn.closed().await;
}

/// Serve a negotiated connection until it closes.
async fn serve(shared: Arc<Shared>, peer: Arc<Peer>, control: ControlStream) {
    tokio::spawn(watch_control(peer.clone(), control));
    let reason = loop {
        // A reader permit is taken before the stream is accepted, so a
        // stalled consumer stops acceptance rather than growing a backlog.
        let permit = peer
            .readers
            .clone()
            .acquire_owned()
            .await
            .expect("reader semaphore never closes");
        match peer.class {
            Class::Peer => match peer.conn.accept_uni().await {
                Ok(recv) => {
                    tokio::spawn(read_uni(shared.clone(), peer.clone(), recv, permit));
                }
                Err(e) => break e,
            },
            Class::Api if peer.accepted => match peer.conn.accept_bi().await {
                Ok((send, recv)) => {
                    tokio::spawn(read_request(
                        shared.clone(),
                        peer.clone(),
                        send,
                        recv,
                        permit,
                    ));
                }
                Err(e) => break e,
            },
            // This side dialed: streams the peer opens carry its output
            // to us, never requests of ours to serve.
            Class::Api => match peer.conn.accept_bi().await {
                Ok((_send, recv)) => {
                    tokio::spawn(read_delivery(shared.clone(), peer.clone(), recv, permit));
                }
                Err(e) => break e,
            },
        }
    };
    let recorded = peer.close_reason.lock().unwrap().take();
    let reason = recorded.unwrap_or_else(|| match reason {
        quinn::ConnectionError::LocallyClosed => CloseReason::Shutdown,
        quinn::ConnectionError::ApplicationClosed(a) => CloseReason::PeerClosed {
            code: u16::try_from(a.error_code.into_inner()).unwrap_or(u16::MAX),
        },
        other => CloseReason::Transport(other.to_string()),
    });
    shared.deregister(&peer);
    shared
        .emit(TransportEvent::Closed {
            connection: peer.id,
            lane: Some(peer.lane),
            reason,
        })
        .await;
}

/// Watch the control stream for an orderly `Close`.
async fn watch_control(peer: Arc<Peer>, mut control: ControlStream) {
    loop {
        match control.next_frame(Duration::from_secs(3600)).await {
            Ok(frame) => match decode(&frame) {
                Ok(MessageV1::Close(c)) => {
                    *peer.close_reason.lock().unwrap() =
                        Some(CloseReason::PeerClosed { code: c.code });
                    peer.conn
                        .close(VarInt::from_u32(CloseCode::Orderly as u32), b"");
                    return;
                }
                _ => {
                    *peer.close_reason.lock().unwrap() =
                        Some(CloseReason::Malformed("control frame".into()));
                    peer.conn
                        .close(VarInt::from_u32(CloseCode::Protocol as u32), b"");
                    return;
                }
            },
            Err(FrameError::Timeout) => continue,
            Err(FrameError::Stream(_)) | Err(FrameError::Truncated) => return,
            Err(e) => {
                *peer.close_reason.lock().unwrap() = Some(CloseReason::Malformed(format!("{e:?}")));
                peer.conn
                    .close(VarInt::from_u32(CloseCode::Protocol as u32), b"");
                return;
            }
        }
    }
}

async fn read_uni(
    shared: Arc<Shared>,
    peer: Arc<Peer>,
    mut recv: RecvStream,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _held = permit;
    match read_frame(&mut recv, shared.limits.frame_timeout, true).await {
        Ok(frame) => {
            // The frame reader checks lengths and class limits, not what
            // the frame is. A peer stream carries peer evidence of a
            // version this build understands and nothing else; anything
            // else is a protocol violation and closes the connection
            // rather than reaching consensus as authenticated input.
            if frame.kind != KIND_PEER_EVIDENCE || frame.version != PEER_EVIDENCE_VERSION {
                let reason = CloseReason::Malformed(format!(
                    "peer frame kind {:#06x} version {}",
                    frame.kind, frame.version
                ));
                *peer.close_reason.lock().unwrap() = Some(reason.clone());
                peer.conn.close(VarInt::from_u32(reason.code() as u32), b"");
                return;
            }
            let (Some(replica), Some(incarnation)) =
                (peer.identity.replica, peer.identity.incarnation)
            else {
                return;
            };
            shared
                .emit(TransportEvent::PeerFrame {
                    connection: peer.id,
                    lane: peer.lane,
                    provenance: PeerProvenance::from_transport(replica, incarnation, peer.id.0),
                    kind: frame.kind,
                    version: frame.version,
                    payload: frame.payload,
                })
                .await;
        }
        Err(FrameError::Stream(_)) => {}
        Err(e) => {
            let reason = frame_reason(e);
            *peer.close_reason.lock().unwrap() = Some(reason.clone());
            peer.conn.close(VarInt::from_u32(reason.code() as u32), b"");
        }
    }
}

/// Whether a client may open a request stream with this message. Replies
/// and handshake messages never originate at a client.
const fn client_request(message: &MessageV1) -> bool {
    matches!(
        message,
        MessageV1::Request(_)
            | MessageV1::ResolveRequest(_)
            | MessageV1::WatchOpen(_)
            | MessageV1::WatchClose(_)
    )
}

/// Read one frame the peer delivered on an API connection this side
/// dialed, and hand it to the runtime as it arrived.
async fn read_delivery(
    shared: Arc<Shared>,
    peer: Arc<Peer>,
    mut recv: RecvStream,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _held = permit;
    match read_frame(&mut recv, shared.limits.frame_timeout, true).await {
        Ok(frame) => {
            shared
                .emit(TransportEvent::ApiDelivery {
                    connection: peer.id,
                    lane: peer.lane,
                    identity: peer.identity.clone(),
                    frame,
                })
                .await;
        }
        Err(FrameError::Stream(_)) => {}
        Err(e) => {
            let reason = frame_reason(e);
            *peer.close_reason.lock().unwrap() = Some(reason.clone());
            peer.conn.close(VarInt::from_u32(reason.code() as u32), b"");
        }
    }
}

async fn read_request(
    shared: Arc<Shared>,
    peer: Arc<Peer>,
    send: SendStream,
    mut recv: RecvStream,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    let _held = permit;
    match read_frame(&mut recv, shared.limits.frame_timeout, true).await {
        Ok(frame) => {
            // Decode here, at the boundary, and accept only what a client
            // may open a request stream with. Emitting an undecodable
            // frame, a server-origin message or a handshake message as an
            // API request would make the malformed-frame contract the duty
            // of every consumer downstream.
            let reason = match decode(&frame) {
                Ok(m) if client_request(&m) => None,
                Ok(m) => Some(CloseReason::Malformed(format!(
                    "api frame {:?} is not a request",
                    m.kind()
                ))),
                Err(e) => Some(CloseReason::Malformed(format!("{e:?}"))),
            };
            if let Some(reason) = reason {
                *peer.close_reason.lock().unwrap() = Some(reason.clone());
                peer.conn.close(VarInt::from_u32(reason.code() as u32), b"");
                return;
            }
            shared
                .emit(TransportEvent::ApiRequest {
                    connection: peer.id,
                    lane: peer.lane,
                    identity: peer.identity.clone(),
                    frame,
                    responder: Responder {
                        send,
                        lane: peer.lane,
                        link: shared.link(Shared::link_key(&peer)),
                        node: shared.node_budget.clone(),
                        deadline: shared.limits.frame_timeout,
                    },
                })
                .await;
        }
        Err(FrameError::Stream(_)) => {}
        Err(e) => {
            let reason = frame_reason(e);
            *peer.close_reason.lock().unwrap() = Some(reason.clone());
            peer.conn.close(VarInt::from_u32(reason.code() as u32), b"");
        }
    }
}
