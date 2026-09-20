//! Static configuration: ALPN classes, bounds and the local TLS identity.

use std::sync::Arc;
use std::time::Duration;

use coord_types::ids::{ClusterId, DomainId, ReplicaId};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::budget::BudgetLimits;
use crate::lane::LaneLimits;

/// ALPN of the native API plane (clients, trusted frontends, Kine
/// collectors). Not a registered standard.
pub const ALPN_API: &[u8] = b"coord-api/1";
/// ALPN of the internal peer plane (voters, observers, learners).
pub const ALPN_PEER: &[u8] = b"coord-peer/1";

/// Connection class, decided by the negotiated ALPN.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    /// Native API.
    Api,
    /// Internal peer plane.
    Peer,
}

impl Class {
    /// The ALPN of the class.
    pub const fn alpn(self) -> &'static [u8] {
        match self {
            Class::Api => ALPN_API,
            Class::Peer => ALPN_PEER,
        }
    }

    /// Classify a negotiated ALPN.
    pub fn of_alpn(alpn: &[u8]) -> Option<Class> {
        if alpn == ALPN_API {
            Some(Class::Api)
        } else if alpn == ALPN_PEER {
            Some(Class::Peer)
        } else {
            None
        }
    }
}

/// Explicit bounds. Every queue, stream count and wait has one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Connections accepted concurrently (further arrivals are refused).
    pub max_connections: usize,
    /// Concurrent unidirectional streams a peer may open on one connection.
    pub max_uni_streams: u32,
    /// Concurrent bidirectional streams a peer may open on one connection.
    pub max_bidi_streams: u32,
    /// TLS handshake plus `Hello` negotiation deadline.
    pub handshake_timeout: Duration,
    /// Deadline for one complete frame on a stream.
    pub frame_timeout: Duration,
    /// QUIC idle timeout.
    pub idle_timeout: Duration,
    /// Keep-alive interval (liveness only; never a lease or membership).
    pub keep_alive: Duration,
    /// Depth of each lane's owned event queue; that lane's readers wait
    /// when it is full while the other lanes keep flowing.
    pub event_queue: usize,
    /// Unary requests a peer may keep in flight (`HelloAck.max_inflight`).
    pub max_inflight: u32,
    /// Per-lane stream limits, windows and queues (task-31).
    pub lanes: [LaneLimits; 4],
    /// Shared destination and node byte budgets (task-31).
    pub budget: BudgetLimits,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_connections: 256,
            max_uni_streams: 256,
            max_bidi_streams: 64,
            handshake_timeout: Duration::from_secs(5),
            frame_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            keep_alive: Duration::from_secs(5),
            event_queue: 1024,
            max_inflight: 64,
            lanes: LaneLimits::DEFAULTS,
            budget: BudgetLimits::default(),
        }
    }
}

/// A certificate chain and key presented in one direction.
///
/// Separate from [`LocalIdentity`] because a process may be more than
/// one principal: the node it serves as is not necessarily the
/// principal it acts as when it dials somebody else.
pub struct ClientIdentity {
    /// Certificate chain.
    pub chain: Vec<CertificateDer<'static>>,
    /// Private key of the leaf.
    pub key: PrivateKeyDer<'static>,
}

/// The local identity: cluster and domain scope, the certificate chain
/// and key this endpoint presents, the trust anchors it validates peers
/// against, and its capabilities.
pub struct LocalIdentity {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Certificate chain presented to peers.
    pub chain: Vec<CertificateDer<'static>>,
    /// Private key of the leaf.
    pub key: PrivateKeyDer<'static>,
    /// Trust anchors for peer certificates (server and client roles).
    pub roots: Arc<RootCertStore>,
    /// Capabilities this endpoint grants (frozen numeric identifiers).
    pub capabilities: Vec<u16>,
    /// The credential to present when *dialling* an API-class peer,
    /// where that is a different principal from the node itself.
    ///
    /// A node certificate binds exactly one role, because the binder
    /// refuses a `Hello` that declares any other. A process that runs
    /// both a voter and that domain's collector is therefore two
    /// principals, and it must present the collector's credential when
    /// it submits to another voter -- otherwise it would either be
    /// refused, or, worse, be admitted as a voter submitting on a
    /// client's behalf, which is a second and weaker way into the
    /// protocol.
    ///
    /// `None` means the node presents its own certificate in both
    /// directions, which is what a single-role process wants.
    pub api_client: Option<ClientIdentity>,
    /// The one connection class this endpoint accepts, where it accepts
    /// only one.
    ///
    /// The ALPN is what separates the two planes, and it separates them
    /// only if each listener offers its own. A listener that offered
    /// both would accept a caller's unary traffic on the peer plane and
    /// a voter's protocol traffic on the api plane -- and since a
    /// runtime reads the two planes' events in different places, what
    /// arrived on the wrong one would simply never be served.
    ///
    /// `None` accepts both, which is what an endpoint that *is* both
    /// planes wants: a test fixture, or a single-socket deployment.
    pub serves: Option<Class>,
    /// This node's own replica identity, where it has one.
    ///
    /// Not an authority over anything -- what a peer may act as is its
    /// certificate's and the binder's answer, and this endpoint's own
    /// identity is its certificate's. It is here for one decision: when
    /// both ends of a peer pair dial each other, a lane is offered two
    /// connections and exactly one must survive at *both* ends. Ordering
    /// the two replica identities is how each end reaches the same
    /// answer without asking the other.
    ///
    /// `None` for an endpoint that holds no replica identity, and for
    /// one whose links are per-connection anyway.
    pub replica: Option<ReplicaId>,
}

/// The TLS profile the adapter always builds (reported for tests and
/// diagnostics; it is not configurable).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TlsProfile {
    /// Only TLS 1.3.
    pub tls13_only: bool,
    /// The explicit AWS-LC provider.
    pub aws_lc_provider: bool,
    /// Server early data size (always zero: no application 0-RTT).
    pub max_early_data_size: u32,
    /// Client early data (always off).
    pub client_early_data: bool,
    /// Mutual TLS required of peer-plane connections (`ALPN_PEER`).
    pub peer_mutual_tls: bool,
    /// Mutual TLS required of API-plane connections that act for others
    /// (`Frontend`, `KineCollector`). A `Client` is not one of them: it
    /// speaks only for itself and its authority is the session binding it
    /// presents above this layer, so the API plane authenticates the
    /// server and leaves the client certificate optional. A certificate a
    /// client does present is always validated against the same roots.
    pub api_mutual_tls_for_trusted_roles: bool,
    /// Active migration accepted by the server (always off).
    pub server_migration: bool,
}

impl TlsProfile {
    /// The fixed profile.
    pub const FIXED: TlsProfile = TlsProfile {
        tls13_only: true,
        aws_lc_provider: true,
        max_early_data_size: 0,
        client_early_data: false,
        peer_mutual_tls: true,
        api_mutual_tls_for_trusted_roles: true,
        server_migration: false,
    };
}
