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
    /// The longest an authenticated connection may live, whatever its
    /// credential's expiry (task-58; design Section 10.4).
    ///
    /// A cap and not the only bound: a connection also ends when the
    /// credential it was bound under does, where the binder says so.
    /// Whichever comes first ends it, because a warm connection is
    /// authentication that has already happened and a long-lived one is
    /// a decision nobody re-made.
    pub max_connection_age: Duration,
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
    /// The most frames a peer lane's sender puts on one stream, where the
    /// link grants [`crate::CAPABILITY_FRAMES_PER_STREAM`] (task-d61).
    /// One, and the endpoint does not offer the capability: each frame
    /// has a stream of its own. Above [`crate::MAX_FRAMES_PER_STREAM`]
    /// it is that.
    pub stream_frames: usize,
    /// The most bytes of frames a peer lane's sender puts on one stream.
    /// A frame larger than this still goes, alone.
    pub stream_bytes: usize,
    /// How often this endpoint asks the other end of each connection to
    /// acknowledge (task-d70). [`AckFrequency::DEFAULT`] by default.
    /// `None` asks nothing, and QUIC's own cadence holds: an ACK every
    /// second ack-eliciting packet, within 25 ms.
    pub ack_frequency: Option<AckFrequency>,
    /// How far out of order this endpoint's packets may arrive before its
    /// loss detection calls the ones overtaken lost.
    /// [`Reordering::DEFAULT`] by default.
    pub reordering: Reordering,
}

/// The acknowledgement cadence an endpoint asks of its peers, through
/// QUIC's acknowledgement-frequency extension (task-d70). A peer that
/// does not support the extension ignores it.
///
/// With about one data datagram per peer per command, an acknowledgement
/// sent after every second packet often has nothing to ride on and
/// travels as a datagram of its own; a higher threshold lets it wait for
/// the reply that follows. The delay is best left to the peer: QUIC's own
/// is 25 ms, so a shorter one sends an acknowledgement sooner, alone, on a
/// connection with little traffic. At threshold 8 and QUIC's delay the
/// runner's five voters sent about a sixth fewer datagrams per command and
/// half the api plane's ACKs, for 4% less CPU per operation (#98). Loss is
/// learned after as many packets more, within the same 25 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckFrequency {
    /// Ack-eliciting packets the peer may receive before it must
    /// acknowledge: zero acknowledges every packet, one every second.
    pub threshold: u32,
    /// The longest the peer may hold an acknowledgement below the
    /// threshold. `None` keeps the peer's own `max_ack_delay`, QUIC's
    /// 25 ms unless it says otherwise. QUIC clamps a delay to at least the
    /// peer's `min_ack_delay` and at most the greater of the path's RTT
    /// and 25 ms.
    pub max_delay: Option<Duration>,
}

impl AckFrequency {
    /// The cadence asked by default: an ACK after eight ack-eliciting
    /// packets, within the peer's own delay (task-d70).
    pub const DEFAULT: AckFrequency = AckFrequency {
        threshold: 8,
        max_delay: None,
    };
}

/// How far out of order a packet may arrive before loss detection calls
/// it lost (#98's +jitter10 cell).
///
/// A sender calls a packet lost once a packet sent after it is
/// acknowledged and either `packets` were sent after it, or it was sent
/// more than `time_eighths / 8` of the round trip earlier (RFC 9002,
/// Section 6.1). Each call is a retransmission and, at most once a round
/// trip, a cut of the congestion window (CUBIC's to 0.7); quinn does not
/// undo a cut it later finds spurious.
///
/// QUIC's thresholds, [`Reordering::QUIC`], are three packets and 9/8.
/// They suit neither a path whose delay jitters nor this transport's
/// acknowledgement cadence:
/// - Jitter reorders: packets sent milliseconds apart swap places, and
///   at a packet or more a millisecond far more than three are overtaken.
/// - [`AckFrequency::DEFAULT`] lets the peer hold an acknowledgement up
///   to its `max_ack_delay` (25 ms), longer than the eighth of a round
///   trip QUIC's time threshold leaves.
///
/// In the packet simulator, a link of 50 ms each way that jitters by
/// 10 ms either side and loses nothing had 269 to 290 of about 2,000
/// packets called lost at QUIC's thresholds, the window cut about 200
/// times, and the transfer take 49 to 54 s. At the default 0 or 1 were
/// called lost, and it took 2.8 to 6.9 s.
///
/// The default calls loss by time alone, as Linux's RACK does once it
/// has seen reordering (RFC 8985): no count of packets is reached, so
/// a send rate that reorders more packets in the same milliseconds does
/// not change it. A real loss is found 7/4 of a round trip after the
/// packet was sent, where QUIC's thresholds find it about one round trip
/// after, once three later packets are acknowledged. In the same
/// simulator at 1% loss, the default still moved the transfer 2.4 to 6
/// times sooner than QUIC's thresholds, with jitter or without.
///
/// The thresholds are the sender's, but the receiver decides when an
/// acknowledgement goes: with [`AckFrequency`] set, the peer is asked to
/// acknowledge out of order arrivals at once only when the gap could call
/// a packet lost, `packets - 1` (quinn's advice), so never at the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reordering {
    /// Packets sent after a packet, and acknowledged before it, that call
    /// it lost. At least three (RFC 9002).
    pub packets: u32,
    /// The time threshold in eighths of the round trip: a packet is
    /// called lost once this long has passed since it was sent and a
    /// packet sent after it has been acknowledged. Nine is QUIC's 9/8; at
    /// least nine.
    pub time_eighths: u32,
}

impl Reordering {
    /// QUIC's own thresholds (RFC 9002, and quinn's defaults): three
    /// packets and 9/8 of the round trip. The control.
    pub const QUIC: Reordering = Reordering {
        packets: 3,
        time_eighths: 9,
    };

    /// The thresholds by default: loss by time alone, at 7/4 of the round
    /// trip.
    pub const DEFAULT: Reordering = Reordering {
        packets: u32::MAX,
        time_eighths: 14,
    };

    /// The time threshold as quinn takes it, a multiple of the round
    /// trip.
    pub fn time_threshold(self) -> f32 {
        self.time_eighths as f32 / 8.0
    }
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
            max_connection_age: Duration::from_secs(12 * 3600),
            keep_alive: Duration::from_secs(5),
            event_queue: 1024,
            max_inflight: 64,
            lanes: LaneLimits::DEFAULTS,
            budget: BudgetLimits::default(),
            stream_frames: 64,
            stream_bytes: 256 * 1024,
            ack_frequency: Some(AckFrequency::DEFAULT),
            reordering: Reordering::DEFAULT,
        }
    }
}

impl Limits {
    /// The limits a domain node's endpoints run under, on both planes
    /// (task-d82).
    ///
    /// A killed process closes nothing, so a link to it ends only at the
    /// idle timeout, and what the link decides waits that long:
    /// - **The peer plane:** a follower counts itself led while it holds a
    ///   link to the leader it promised. And a voter restarted under its
    ///   incarnation is locked out until then, since its peers keep their
    ///   own connections to the dead process and those win the lane.
    /// - **The api plane:** a frontend's submission link to a voter is an
    ///   api connection, and a submission it sends on one to a dead voter
    ///   waits out the timeout before the link is dialled again.
    ///
    /// So a node runs seconds, with a keep-alive that four losses in a row
    /// do not close. QUIC takes the smaller of the two ends' idle
    /// timeouts, so a link to a peer of an older build, or a caller on
    /// the defaults, runs at this one's.
    pub fn node() -> Self {
        Limits {
            idle_timeout: Duration::from_secs(5),
            keep_alive: Duration::from_secs(1),
            ..Limits::default()
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
