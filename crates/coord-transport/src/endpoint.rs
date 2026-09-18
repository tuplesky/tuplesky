//! The endpoint: TLS lifecycle, accept and connect, negotiation, stream
//! readers and owned dispatch.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coord_core::event::PeerProvenance;
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{
    BoundedBytes, BoundedVec, CloseV1, Frame, HelloAckV1, HelloV1, MessageV1, PeerRole, decode,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{RecvStream, SendStream, VarInt};
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::CertificateDer;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::timeout;

use crate::config::{ALPN_API, ALPN_PEER, Class, Limits, LocalIdentity, TlsProfile};
use crate::frames::{FrameError, read_frame};
use crate::identity::{BoundIdentity, IdentityBinder, role_class};

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
    /// Negotiation rejected (origin, role, version, identity).
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
        /// Bound identity of the peer.
        identity: BoundIdentity,
        /// Remote address (diagnostic).
        remote: SocketAddr,
    },
    /// A complete peer frame arrived on a peer-class connection.
    PeerFrame {
        /// Connection.
        connection: ConnectionId,
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
        /// Bound identity of the requester.
        identity: BoundIdentity,
        /// The request frame.
        frame: Frame,
        /// Where the response goes.
        responder: Responder,
    },
    /// A connection ended (after `Connected`, or instead of it).
    Closed {
        /// Connection.
        connection: ConnectionId,
        /// Why.
        reason: CloseReason,
    },
}

/// The response half of a unary request stream.
#[derive(Debug)]
pub struct Responder {
    send: SendStream,
}

impl Responder {
    /// Write the complete response frame and finish the stream.
    pub async fn respond(mut self, frame: Vec<u8>) -> Result<(), SendError> {
        self.send
            .write_all(&frame)
            .await
            .map_err(|e| SendError::Stream(e.to_string()))?;
        self.send
            .finish()
            .map_err(|e| SendError::Stream(e.to_string()))
    }
}

/// Why a send did not reach the transport.
#[derive(Debug)]
pub enum SendError {
    /// No negotiated connection to that replica and incarnation.
    NotConnected,
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
    identity: BoundIdentity,
    close_reason: Mutex<Option<CloseReason>>,
}

struct Shared {
    cluster: ClusterId,
    domain: DomainId,
    capabilities: Vec<u16>,
    limits: Limits,
    binder: Arc<dyn IdentityBinder>,
    events: mpsc::Sender<TransportEvent>,
    peers: Mutex<HashMap<ConnectionId, Arc<Peer>>>,
    by_replica: Mutex<HashMap<(ReplicaId, ReplicaIncarnation), ConnectionId>>,
    next: AtomicU64,
    accept_permits: Arc<Semaphore>,
}

impl Shared {
    fn next_id(&self) -> ConnectionId {
        ConnectionId(self.next.fetch_add(1, Ordering::SeqCst))
    }

    async fn emit(&self, event: TransportEvent) {
        // A closed receiver means the runtime is gone; dropping is fine.
        let _ = self.events.send(event).await;
    }

    fn register(&self, peer: Arc<Peer>) {
        if let (Some(r), Some(i)) = (peer.identity.replica, peer.identity.incarnation) {
            self.by_replica.lock().unwrap().insert((r, i), peer.id);
        }
        self.peers.lock().unwrap().insert(peer.id, peer);
    }

    fn deregister(&self, id: ConnectionId) {
        if let Some(peer) = self.peers.lock().unwrap().remove(&id)
            && let (Some(r), Some(i)) = (peer.identity.replica, peer.identity.incarnation)
        {
            let mut by = self.by_replica.lock().unwrap();
            if by.get(&(r, i)) == Some(&id) {
                by.remove(&(r, i));
            }
        }
    }
}

/// The transport endpoint of one process.
pub struct Transport {
    endpoint: quinn::Endpoint,
    shared: Arc<Shared>,
    events: mpsc::Receiver<TransportEvent>,
    client_api: quinn::ClientConfig,
    client_peer: quinn::ClientConfig,
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

        let mut transport = quinn::TransportConfig::default();
        transport
            .max_concurrent_uni_streams(VarInt::from_u32(limits.max_uni_streams))
            .max_concurrent_bidi_streams(VarInt::from_u32(limits.max_bidi_streams))
            .max_idle_timeout(Some(
                quinn::IdleTimeout::try_from(limits.idle_timeout).map_err(tls_err)?,
            ))
            .keep_alive_interval(Some(limits.keep_alive))
            .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
        let transport = Arc::new(transport);

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));
        server_config
            .transport_config(transport.clone())
            .migration(TlsProfile::FIXED.server_migration)
            .max_incoming(limits.max_connections);

        let client_for = |alpn: &[u8]| -> Result<quinn::ClientConfig, TransportError> {
            let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(tls_err)?
                .with_root_certificates(local.roots.clone())
                .with_client_auth_cert(local.chain.clone(), local.key.clone_key())
                .map_err(tls_err)?;
            client.alpn_protocols = vec![alpn.to_vec()];
            client.enable_early_data = TlsProfile::FIXED.client_early_data;
            let quic_client = QuicClientConfig::try_from(client).map_err(tls_err)?;
            let mut config = quinn::ClientConfig::new(Arc::new(quic_client));
            config.transport_config(transport.clone());
            Ok(config)
        };
        let client_api = client_for(ALPN_API)?;
        let client_peer = client_for(ALPN_PEER)?;

        let mut endpoint = quinn::Endpoint::server(server_config, addr)?;
        endpoint.set_default_client_config(client_peer.clone());
        let (tx, rx) = mpsc::channel(limits.event_queue.max(1));
        let shared = Arc::new(Shared {
            cluster: local.cluster,
            domain: local.domain,
            capabilities: local.capabilities,
            limits,
            binder,
            events: tx,
            peers: Mutex::new(HashMap::new()),
            by_replica: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            accept_permits: Arc::new(Semaphore::new(limits.max_connections.max(1))),
        });
        tokio::spawn(accept_loop(endpoint.clone(), shared.clone()));
        Ok(Transport {
            endpoint,
            shared,
            events: rx,
            client_api,
            client_peer,
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

    /// The next owned event; `None` once the endpoint is gone.
    pub async fn next_event(&mut self) -> Option<TransportEvent> {
        self.events.recv().await
    }

    /// Open a connection to `addr` as `local_role`, expecting the remote
    /// to be `expected` (its certificate is bound to that identity before
    /// anything is sent). The connection is registered on `HelloAck`.
    pub async fn connect(
        &self,
        addr: SocketAddr,
        server_name: &str,
        local_role: PeerRole,
        local_incarnation: Option<ReplicaIncarnation>,
        expected: BoundIdentity,
    ) -> Result<ConnectionId, TransportError> {
        let class = role_class(local_role);
        let config = match class {
            Class::Api => self.client_api.clone(),
            Class::Peer => self.client_peer.clone(),
        };
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
            .negotiate_outgoing(&conn, local_role, local_incarnation, &expected)
            .await
        {
            Ok((identity, control_recv)) => {
                let peer = Arc::new(Peer {
                    id,
                    conn: conn.clone(),
                    class,
                    identity: identity.clone(),
                    close_reason: Mutex::new(None),
                });
                self.shared.register(peer.clone());
                self.shared
                    .emit(TransportEvent::Connected {
                        connection: id,
                        class,
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
        expected: &BoundIdentity,
    ) -> Result<(BoundIdentity, RecvStream), CloseReason> {
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
        let (mut send, mut recv) = timeout(shared.limits.handshake_timeout, conn.open_bi())
            .await
            .map_err(|_| CloseReason::Timeout)?
            .map_err(|e| CloseReason::Transport(e.to_string()))?;
        let hello = MessageV1::Hello(HelloV1 {
            role: local_role,
            cluster_id: shared.cluster,
            domain_id: shared.domain,
            incarnation: local_incarnation,
            capabilities: BoundedVec::new(shared.capabilities.clone())
                .map_err(|_| CloseReason::Rejected("caps".into()))?,
        })
        .encode()
        .map_err(|e| CloseReason::Malformed(format!("{e:?}")))?;
        send.write_all(&hello)
            .await
            .map_err(|e| CloseReason::Transport(e.to_string()))?;
        let frame = read_frame(&mut recv, shared.limits.handshake_timeout, false)
            .await
            .map_err(frame_reason)?;
        match decode(&frame) {
            Ok(MessageV1::HelloAck(ack)) => Ok((
                BoundIdentity {
                    role: bound.role,
                    replica: bound.replica,
                    incarnation: bound.incarnation,
                    capabilities: ack.capabilities.as_slice().to_vec(),
                },
                recv,
            )),
            Ok(MessageV1::Close(c)) => Err(CloseReason::PeerClosed { code: c.code }),
            Ok(_) => Err(CloseReason::Malformed("expected hello ack".into())),
            Err(e) => Err(CloseReason::Malformed(format!("{e:?}"))),
        }
    }

    /// Send one complete frame to a negotiated peer on a fresh
    /// unidirectional stream. Success means the frame was handed to the
    /// transport, nothing more.
    pub async fn send_peer(
        &self,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        frame: Vec<u8>,
    ) -> Result<(), SendError> {
        let id = self
            .shared
            .by_replica
            .lock()
            .unwrap()
            .get(&(replica, incarnation))
            .copied()
            .ok_or(SendError::NotConnected)?;
        let peer = self
            .shared
            .peers
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(SendError::NotConnected)?;
        let mut send = timeout(self.shared.limits.frame_timeout, peer.conn.open_uni())
            .await
            .map_err(|_| SendError::Timeout)?
            .map_err(|e| SendError::Stream(e.to_string()))?;
        send.write_all(&frame)
            .await
            .map_err(|e| SendError::Stream(e.to_string()))?;
        send.finish().map_err(|e| SendError::Stream(e.to_string()))
    }

    /// Close every connection and wait at most `deadline` for the
    /// endpoint to drain. Returns whether it drained in time.
    pub async fn shutdown(self, deadline: Duration) -> bool {
        self.endpoint
            .close(VarInt::from_u32(CloseCode::Shutdown as u32), b"shutdown");
        timeout(deadline, self.endpoint.wait_idle()).await.is_ok()
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
                    reason: CloseReason::Transport(e.to_string()),
                })
                .await;
            return;
        }
        Err(_) => {
            shared
                .emit(TransportEvent::Closed {
                    connection: id,
                    reason: CloseReason::Timeout,
                })
                .await;
            return;
        }
    };
    match negotiate_incoming(&shared, &conn).await {
        Ok((class, identity, control_recv)) => {
            let peer = Arc::new(Peer {
                id,
                conn: conn.clone(),
                class,
                identity: identity.clone(),
                close_reason: Mutex::new(None),
            });
            shared.register(peer.clone());
            shared
                .emit(TransportEvent::Connected {
                    connection: id,
                    class,
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
                    reason,
                })
                .await;
        }
    }
}

type Negotiated = (Class, BoundIdentity, RecvStream);

async fn negotiate_incoming(
    shared: &Shared,
    conn: &quinn::Connection,
) -> Result<Negotiated, (CloseReason, Option<SendStream>)> {
    let class = negotiated_class(conn).map_err(|r| (r, None))?;
    let certs = peer_certs(conn).map_err(|r| (r, None))?;
    let (mut send, mut recv) =
        match timeout(shared.limits.handshake_timeout, conn.accept_bi()).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err((CloseReason::Transport(e.to_string()), None)),
            Err(_) => return Err((CloseReason::Timeout, None)),
        };
    let frame = match read_frame(&mut recv, shared.limits.handshake_timeout, false).await {
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
    let bound = match shared.binder.bind(&certs, &hello) {
        Ok(b) => b,
        Err(e) => return Err((CloseReason::Rejected(format!("{e:?}")), Some(send))),
    };
    let granted: Vec<u16> = hello
        .capabilities
        .as_slice()
        .iter()
        .copied()
        .filter(|c| shared.capabilities.contains(c))
        .collect();
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
    if let Err(e) = send.write_all(&ack).await {
        return Err((CloseReason::Transport(e.to_string()), None));
    }
    // The control stream stays open: `Close` travels on it. The send half
    // is parked with the connection (dropping it would finish the stream).
    tokio::spawn(park_control(send, conn.clone()));
    Ok((
        class,
        BoundIdentity {
            role: bound.role,
            replica: bound.replica,
            incarnation: bound.incarnation,
            capabilities: granted,
        },
        recv,
    ))
}

/// Keep the control send half alive until the connection closes.
async fn park_control(send: SendStream, conn: quinn::Connection) {
    let _send = send;
    conn.closed().await;
}

/// Serve a negotiated connection until it closes.
async fn serve(shared: Arc<Shared>, peer: Arc<Peer>, control_recv: RecvStream) {
    tokio::spawn(watch_control(peer.clone(), control_recv));
    let reason = loop {
        match peer.class {
            Class::Peer => match peer.conn.accept_uni().await {
                Ok(recv) => {
                    tokio::spawn(read_uni(shared.clone(), peer.clone(), recv));
                }
                Err(e) => break e,
            },
            Class::Api => match peer.conn.accept_bi().await {
                Ok((send, recv)) => {
                    tokio::spawn(read_request(shared.clone(), peer.clone(), send, recv));
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
    shared.deregister(peer.id);
    shared
        .emit(TransportEvent::Closed {
            connection: peer.id,
            reason,
        })
        .await;
}

/// Watch the control stream for an orderly `Close`.
async fn watch_control(peer: Arc<Peer>, mut recv: RecvStream) {
    loop {
        match read_frame(&mut recv, Duration::from_secs(3600), false).await {
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

async fn read_uni(shared: Arc<Shared>, peer: Arc<Peer>, mut recv: RecvStream) {
    match read_frame(&mut recv, shared.limits.frame_timeout, true).await {
        Ok(frame) => {
            let (Some(replica), Some(incarnation)) =
                (peer.identity.replica, peer.identity.incarnation)
            else {
                return;
            };
            shared
                .emit(TransportEvent::PeerFrame {
                    connection: peer.id,
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

async fn read_request(
    shared: Arc<Shared>,
    peer: Arc<Peer>,
    send: SendStream,
    mut recv: RecvStream,
) {
    match read_frame(&mut recv, shared.limits.frame_timeout, true).await {
        Ok(frame) => {
            shared
                .emit(TransportEvent::ApiRequest {
                    connection: peer.id,
                    identity: peer.identity.clone(),
                    frame,
                    responder: Responder { send },
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
