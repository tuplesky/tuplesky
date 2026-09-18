//! Static configuration: ALPN classes, bounds and the local TLS identity.

use std::sync::Arc;
use std::time::Duration;

use coord_types::ids::{ClusterId, DomainId};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

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
    /// Depth of the owned event queue; readers wait when it is full.
    pub event_queue: usize,
    /// Unary requests a peer may keep in flight (`HelloAck.max_inflight`).
    pub max_inflight: u32,
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
        }
    }
}

/// The local identity: cluster and domain scope, the certificate chain
/// and key this endpoint presents (both as server and as client), the
/// trust anchors it validates peers against, and its capabilities.
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
    /// Mutual TLS required.
    pub client_certificate_required: bool,
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
        client_certificate_required: true,
        server_migration: false,
    };
}
