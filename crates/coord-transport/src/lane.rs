//! Traffic lanes (task-31; design Sections 3.3, 11.3, 19.2): control and
//! consensus evidence, unary requests, watches and bulk transfers use
//! separate connections with their own stream limits, windows and
//! queues, so a stalled watch or snapshot never shares a queue or stream
//! credit with a vote. A lane is declared in `Hello` through a frozen
//! capability identifier and admitted only for roles that may use it.

use std::sync::Arc;
use std::time::Duration;

use coord_types::wire_v1::{HelloV1, PeerRole};
use quinn::VarInt;

/// One traffic lane of a peer link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Lane {
    /// Control, negotiation, consensus evidence, recovery pages.
    Control = 0,
    /// Native unary requests and responses.
    Unary = 1,
    /// Long-lived watch streams.
    Watch = 2,
    /// Snapshots, replication and other bulk transfers.
    Bulk = 3,
}

impl Lane {
    /// Every lane, in priority order.
    pub const ALL: [Lane; 4] = [Lane::Control, Lane::Unary, Lane::Watch, Lane::Bulk];

    /// Index into per-lane tables.
    pub const fn index(self) -> usize {
        self as usize
    }

    /// The frozen capability identifier that declares this lane in
    /// `Hello` (registered in `spec/wire-v1.md`).
    pub const fn capability(self) -> u16 {
        0x0010 + self as u16
    }

    /// The lane a capability identifier declares, if any.
    pub const fn of_capability(capability: u16) -> Option<Lane> {
        match capability {
            0x0010 => Some(Lane::Control),
            0x0011 => Some(Lane::Unary),
            0x0012 => Some(Lane::Watch),
            0x0013 => Some(Lane::Bulk),
            _ => None,
        }
    }
}

/// The lanes a dialing role may open.
pub const fn role_lanes(role: PeerRole) -> &'static [Lane] {
    match role {
        PeerRole::Voter => &[Lane::Control, Lane::Bulk],
        PeerRole::Observer | PeerRole::Learner => &[Lane::Control, Lane::Bulk],
        PeerRole::Frontend | PeerRole::KineCollector => &[Lane::Control, Lane::Unary, Lane::Watch],
        PeerRole::Client => &[Lane::Unary, Lane::Watch],
    }
}

/// Why a `Hello` did not declare an admissible lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneError {
    /// No lane capability, or more than one.
    NotExactlyOne,
    /// The role may not open the lane.
    NotAdmitted(Lane),
}

/// The lane a `Hello` declares (exactly one lane capability), checked
/// against its role.
pub fn lane_of_hello(hello: &HelloV1) -> Result<Lane, LaneError> {
    let mut found = None;
    for c in hello.capabilities.as_slice() {
        if let Some(lane) = Lane::of_capability(*c) {
            if found.is_some() {
                return Err(LaneError::NotExactlyOne);
            }
            found = Some(lane);
        }
    }
    let lane = found.ok_or(LaneError::NotExactlyOne)?;
    if role_lanes(hello.role).contains(&lane) {
        Ok(lane)
    } else {
        Err(LaneError::NotAdmitted(lane))
    }
}

/// Bounds of one lane: stream counts, windows and the fair queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LaneLimits {
    /// Concurrent unidirectional streams the peer may open.
    pub max_uni_streams: u32,
    /// Concurrent bidirectional streams the peer may open.
    pub max_bidi_streams: u32,
    /// Receive window per stream.
    pub stream_receive_window: u64,
    /// Receive window per connection.
    pub receive_window: u64,
    /// Send window per connection (bytes handed to QUIC unacknowledged).
    pub send_window: u64,
    /// Frames queued per group before the sender is refused.
    pub queue_depth: usize,
    /// Groups (domains) with queued frames before the sender is refused.
    pub max_groups: usize,
}

impl LaneLimits {
    /// Control: many short streams, small windows, deep enough queue.
    pub const CONTROL: LaneLimits = LaneLimits {
        max_uni_streams: 256,
        max_bidi_streams: 32,
        stream_receive_window: 256 * 1024,
        receive_window: 4 * 1024 * 1024,
        send_window: 4 * 1024 * 1024,
        queue_depth: 256,
        max_groups: 64,
    };
    /// Unary: bounded bidirectional streams.
    pub const UNARY: LaneLimits = LaneLimits {
        max_uni_streams: 0,
        max_bidi_streams: 64,
        stream_receive_window: 3 * 1024 * 1024 + 64 * 1024,
        receive_window: 8 * 1024 * 1024,
        send_window: 8 * 1024 * 1024,
        queue_depth: 64,
        max_groups: 64,
    };
    /// Watch: few long streams, one revision batch at a time.
    pub const WATCH: LaneLimits = LaneLimits {
        max_uni_streams: 0,
        max_bidi_streams: 16,
        stream_receive_window: 8 * 1024 * 1024 + 64 * 1024,
        receive_window: 16 * 1024 * 1024,
        send_window: 16 * 1024 * 1024,
        queue_depth: 16,
        max_groups: 64,
    };
    /// Bulk: few streams, large windows, a queue deep enough for a
    /// replica catching up.
    ///
    /// The depth was eight while the only bulk traffic was a checkpoint
    /// image, which is one transfer at a time. It also carries payload
    /// transfer now -- a replica fetching the content of commands it
    /// missed, a bounded batch at a time -- and a queue as deep as one
    /// batch drops part of every batch, so the replica re-asks and
    /// catches up at a fraction of the rate it could. What it must stay
    /// far from is the control lane's depth, because the point of the
    /// separation is that catch-up traffic cannot crowd out the frames
    /// the protocol needs to make progress.
    pub const BULK: LaneLimits = LaneLimits {
        max_uni_streams: 4,
        max_bidi_streams: 4,
        stream_receive_window: 8 * 1024 * 1024 + 64 * 1024,
        receive_window: 16 * 1024 * 1024,
        send_window: 16 * 1024 * 1024,
        queue_depth: 64,
        max_groups: 16,
    };

    /// The most restrictive limits across lanes: what an accepted
    /// connection starts with before `Hello` names its lane. QUIC stream
    /// and window credit can only be raised once advertised, so the
    /// acceptor advertises the floor and raises to the lane afterwards.
    pub fn floor(lanes: &[LaneLimits; 4]) -> LaneLimits {
        let mut out = lanes[0];
        for l in &lanes[1..] {
            out.max_uni_streams = out.max_uni_streams.min(l.max_uni_streams);
            out.max_bidi_streams = out.max_bidi_streams.min(l.max_bidi_streams);
            out.stream_receive_window = out.stream_receive_window.min(l.stream_receive_window);
            out.receive_window = out.receive_window.min(l.receive_window);
            out.send_window = out.send_window.min(l.send_window);
        }
        // The control stream itself needs one bidirectional stream.
        out.max_bidi_streams = out.max_bidi_streams.max(1);
        out
    }

    /// The defaults per lane.
    pub const DEFAULTS: [LaneLimits; 4] = [
        LaneLimits::CONTROL,
        LaneLimits::UNARY,
        LaneLimits::WATCH,
        LaneLimits::BULK,
    ];
}

/// The QUIC transport configuration of a lane: explicit CUBIC, the
/// lane's stream limits and windows, keep-alive and idle timeout.
pub fn transport_config(
    limits: &LaneLimits,
    idle_timeout: Duration,
    keep_alive: Duration,
) -> Result<Arc<quinn::TransportConfig>, String> {
    let mut transport = quinn::TransportConfig::default();
    transport
        .max_concurrent_uni_streams(VarInt::from_u32(limits.max_uni_streams))
        .max_concurrent_bidi_streams(VarInt::from_u32(limits.max_bidi_streams))
        .stream_receive_window(varint(limits.stream_receive_window)?)
        .receive_window(varint(limits.receive_window)?)
        .send_window(limits.send_window)
        .max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(idle_timeout).map_err(|e| e.to_string())?,
        ))
        .keep_alive_interval(Some(keep_alive))
        .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
    Ok(Arc::new(transport))
}

fn varint(v: u64) -> Result<VarInt, String> {
    VarInt::from_u64(v).map_err(|e| e.to_string())
}

/// Apply a lane's limits to an accepted connection (the acceptor learns
/// the lane from `Hello`, after the handshake).
pub fn apply_to_connection(conn: &quinn::Connection, limits: &LaneLimits) {
    conn.set_max_concurrent_uni_streams(VarInt::from_u32(limits.max_uni_streams));
    conn.set_max_concurrent_bi_streams(VarInt::from_u32(limits.max_bidi_streams));
    if let Ok(w) = VarInt::from_u64(limits.receive_window) {
        conn.set_receive_window(w);
    }
}
