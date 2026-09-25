//! The serving loop: transport in, frontend decision, storage out
//! (design Sections 3.2, 4.3, 22.1).
//!
//! Every piece this joins was built to hold none of the others.
//! `coord-transport` negotiates and frames and does not know what a
//! session is. `coord-session` decides what a frame of a bound
//! connection means and holds no sockets. `coord-collector` plans a
//! submission and does not send it. `coord-daemon` says what each
//! decision does to the stream it arrived on, and drives one voter's
//! effects, and owns neither the endpoint nor the store.
//!
//! This is where they meet, and it is deliberately the only place they
//! do: a join that happened in several places would be several
//! opportunities for one of them to answer the wrong caller.

use std::collections::BTreeMap;

use coord_authn::ClockHealth;
use coord_checkpoint::local::LocalLimits;
use coord_checkpoint::{LocalBaseline, LocalCheckpointStore};
use coord_collector::ingress::is_collector;
use coord_collector::wire::{
    KIND_EVIDENCE, KIND_RELEASE, KIND_SUBMIT, decode_evidence, decode_release,
};
use coord_collector::{
    Admission, AdmissionLimits, Collector, CollectorConfig, Dispatcher, MonotonicMillis,
};
use coord_core::event::PeerProvenance;
use coord_daemon::mailbox::LocalRoute;
use coord_daemon::metrics::Stage;
use coord_daemon::node::DriveError;
use coord_daemon::pending::Pending;
use coord_daemon::serve::{Step, step};
use coord_daemon::voter::Voter;
use coord_daemon::{Config, fanout};
use coord_membership::membership::Membership;
use coord_session::{BindingConfig, BoundFrontend, Delivered, StorePolicySource};
use coord_storage::views::ViewBudget;
use coord_storage::{Applier, Persistence};
use coord_transport::{Responder, Transport, TransportEvent};
use coord_types::ids::ReplicaId;
use coord_types::wire_v1::{Frame, MessageV1, decode};
use coord_types::{CommandId, RetryKey};

/// Why the loop could not be built.
#[derive(Debug)]
pub enum ServeError {
    /// The configuration does not name the token service this process
    /// needs to verify its callers.
    NoTokenService,
    /// The issuer's published keys could not be read.
    Jwks {
        /// Where this node looked.
        path: String,
        /// Why.
        reason: String,
    },
    /// The committed configuration does not describe a quorum.
    Quorum(String),
}

impl core::fmt::Display for ServeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ServeError::NoTokenService => f.write_str(
                "this process serves clients but names no token service to verify them against",
            ),
            ServeError::Jwks { path, reason } => {
                write!(f, "cannot read the issuer's keys at {path}: {reason}")
            }
            ServeError::Quorum(why) => {
                write!(f, "the committed configuration is not a quorum: {why}")
            }
        }
    }
}

impl core::error::Error for ServeError {}

/// The frontend of one domain: what a caller's frame means, and where a
/// submission goes.
///
/// It holds no store. The store belongs to whatever is writing it --
/// a voter in this process, or, for a process that only serves, the
/// applier itself -- and is passed in for the one decision that needs
/// it. That is not a style choice: a frontend with its own handle on
/// the store would be a second writer's worth of opportunity, and the
/// profile has exactly one.
pub struct Frontend {
    frontend: BoundFrontend,
    membership: Membership,
    pending: Pending<Responder>,
    /// The ingress of a voter running in this process, where there is
    /// one. A frontend-only process has `None` here and reaches every
    /// voter over the wire.
    local: Option<LocalRoute>,
    /// The output stream of every live watch, by the connection and the
    /// client's own identifier for the subscription.
    ///
    /// A watch is the one shape whose stream outlives the frame that
    /// opened it: the caller opens it once and the events, the progress
    /// notifications and finally the close are written onto that same
    /// stream for as long as the subscription lasts. So the responder is
    /// kept here rather than in `pending`, which holds a stream until
    /// one result arrives and then lets it go.
    watches: BTreeMap<(u64, u64), Responder>,
    /// What the loop has done, for the readiness report. Counts, not
    /// contents: a diagnostic that carried a caller's data would be a
    /// disclosure by another name.
    counts: Counts,
}

/// What a serving loop has seen.
///
/// Every submission count below says an ingress accepted responsibility
/// for a frame. None of them says a voter processed it, that anything
/// became durable, or that a command was applied: evidence is counted by
/// the collector, by voter identity, and nowhere else.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Frames taken by a voter running in this process.
    pub queued_local: u64,
    /// Frames the transport admitted for a voter elsewhere.
    pub queued_remote: u64,
    /// Targets the committed configuration does not name as voters of
    /// this domain: the plan and the configuration disagree.
    pub not_a_voter: u64,
    /// Destinations whose ingress was full. A live voter under load, not
    /// an absent one.
    pub saturated: u64,
    /// Destinations there was no route to. Not a failure of the
    /// submission: only the quorum rule decides that.
    pub unavailable: u64,
    /// Destinations an envelope could not reach as it stood. Permanent
    /// for that frame, so it is not repeated on the congestion
    /// schedule.
    pub undeliverable: u64,
    /// Offers of a submission this node made again, to destinations
    /// that had not taken it. Not second submissions: the same
    /// envelope, the same command identity.
    pub reoffered: u64,
    /// Submissions this node's own voter refused at its door.
    pub refused: u64,
    /// Results the collector released to a waiting caller.
    pub released: u64,
    /// Evidence and releases this voter sent back to the collector in
    /// another process that submitted the command.
    pub returned: u64,
    /// Evidence this voter could not return: the collector that
    /// submitted is no longer reachable. The command is unaffected --
    /// it is established and durable by the quorum rule, not by whether
    /// one submitter heard about it.
    pub unreturnable: u64,
    /// Watches opened.
    pub watches: u64,
    /// Frames written onto a watch's own stream: events, progress and
    /// closes. A watch that was opened and never pumped is the
    /// difference between this and `watches`.
    pub watch_frames: u64,
    /// Watches whose stream ended before the subscription did: the
    /// caller stopped reading, or the connection went. The subscription
    /// is forgotten with the stream; nothing is retained for a consumer
    /// that is no longer there.
    pub watches_lost: u64,
    /// Frames and effects this build has no loop for yet, counted rather
    /// than discarded so what arrives is visible.
    pub unserved: u64,
    /// Evidence this voter produced for a command whose submitter it did
    /// not know yet, held until it does.
    pub parked: u64,
    /// Held evidence let go because the submission naming it did not
    /// arrive inside the window the race can take.
    ///
    /// Ordinary under load, and the ordinary cause is named: a
    /// collector's fan-out that the peer's lane could not queue is
    /// dropped by the transport, so this voter is never told where that
    /// command's evidence belongs. The command is decided and durable
    /// on a quorum that did get the submission, and the caller resolves
    /// it by identity.
    pub unclaimed: u64,
    /// Held evidence dropped because the hold itself was full.
    ///
    /// Not ordinary. The hold is bounded by the commands that can be in
    /// this state at once, and reaching it means more were than the
    /// bound allows for -- an operator wants to know that the bound and
    /// the traffic have diverged, which is a different thing from a
    /// submission that never came.
    pub crowded_out: u64,
    /// Commands the collector completed from this node's own durable
    /// record of their execution, because half of what a release needs
    /// had arrived and the other half had not (task-c02).
    pub settled_from_record: u64,
}

impl Frontend {
    /// Build the frontend this configuration describes.
    ///
    /// `local` is the ingress of a voter running in this process, where
    /// there is one. It is a capability the voter's runtime hands over,
    /// not something the frontend may decide it has; which target it
    /// applies to is still the committed configuration's answer, checked
    /// on every submission.
    pub fn new(
        config: &Config,
        membership: Membership,
        local: Option<LocalRoute>,
    ) -> Result<Self, ServeError> {
        let sts = config.sts.as_ref().ok_or(ServeError::NoTokenService)?;
        let jwks = verification_keys(&sts.jwks)?;

        // The quorum is the committed configuration's, never a setting:
        // a frontend that could be told its own quorum could be told a
        // smaller one, and would then release results on less evidence
        // than the cluster agreed on.
        let quorum = quorum_of(&membership)?;

        let collector = Collector::new(CollectorConfig {
            quorum,
            max_pending: config.limits.max_outstanding_per_session,
            max_resolved: config.limits.max_outstanding_per_session,
            // Derived from the two bounds that already govern this
            // domain rather than given a number of its own: what the
            // collector may owe at once is at most one whole submission
            // envelope for each command it may hold pending -- the
            // request, and the admission facts, request fields and
            // frame header the envelope carries around it
            // (`SUBMIT_ENVELOPE_ALLOWANCE`). The request alone is not
            // what is held, and budgeting only it refused a legal
            // request near the limit when one command may be pending. A
            // separate setting here could be raised past either bound
            // and would stop meaning anything.
            max_undelivered_bytes: coord_collector::undelivered_budget(
                config.limits.max_outstanding_per_session,
                config.limits.max_request_bytes,
            ),
        });
        let admission = Admission::new(
            membership.cluster(),
            membership.domain(),
            AdmissionLimits {
                max_pending_per_session: config.limits.max_outstanding_per_session,
            },
        );
        let dispatcher =
            Dispatcher::new(admission, collector, config.limits.max_live_subscriptions);
        let frontend = BoundFrontend::new(
            dispatcher,
            BindingConfig {
                issuer: sts.issuer.clone(),
                resource: sts.resource.clone(),
                jwks,
                cluster: membership.cluster(),
                domain: membership.domain(),
            },
            PUMP_BOUND,
            config.limits.max_outstanding_per_session,
        );
        Ok(Frontend {
            frontend,
            membership,
            pending: Pending::new(),
            local,
            watches: BTreeMap::new(),
            counts: Counts::default(),
        })
    }
}

/// The ballot every replica of this epoch starts at.
///
/// Derived from the committed configuration and from nothing else, so
/// the voter that leads it and the frontend that counts evidence for it
/// cannot come to different answers -- which they would, silently, if
/// each worked it out its own way.
pub fn genesis_ballot(membership: &Membership) -> Result<coord_types::ids::Ballot, ServeError> {
    Ok(coord_types::ids::Ballot {
        epoch: membership.epoch(),
        number: 0,
        leader: membership
            .voters()
            .map(|v| v.node)
            .next()
            .ok_or_else(|| ServeError::Quorum("no voters".into()))?,
    })
}

/// The quorum rule of the committed configuration.
pub fn quorum_of(
    membership: &Membership,
) -> Result<coord_consensus::BallotConfiguration, ServeError> {
    let voters = membership.voters().map(|v| v.node).collect();
    let ballot = genesis_ballot(membership)?;
    coord_consensus::BallotConfiguration::c2_default(membership.epoch(), ballot, voters)
        .map_err(|e| ServeError::Quorum(format!("{e:?}")))
}

/// The usable verification keys at `path`.
///
/// "The frontend is up" has to mean the frontend can verify a caller,
/// not that a file parsed. A JWKS that is syntactically fine and
/// contains no key this build can verify with refuses every caller, and
/// a process that announced itself ready with one would look, from
/// outside, exactly like a client problem.
fn verification_keys(path: &str) -> Result<serde_json::Value, ServeError> {
    let bytes = std::fs::read(path).map_err(|e| ServeError::Jwks {
        path: path.to_owned(),
        reason: e.to_string(),
    })?;
    let jwks: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| ServeError::Jwks {
        path: path.to_owned(),
        reason: e.to_string(),
    })?;
    let usable = coord_session::usable_verification_keys(&jwks);
    if usable == 0 {
        return Err(ServeError::Jwks {
            path: path.to_owned(),
            reason: "no key in this set can verify a caller's token".into(),
        });
    }
    Ok(jwks)
}

/// Who is writing the one store this domain has.
///
/// A voter writes its own protocol and application transitions; a
/// process that runs no voter still reads, and still holds the applier
/// that would do the writing if it did. Either way there is one.
pub enum Backing<P: Persistence> {
    /// A voter of this domain runs here.
    Voting(Box<Voter<P>>),
    /// No voter runs here: this process serves and reads only.
    Serving(Box<Applier<P>>),
}

impl<P: Persistence> Backing<P> {
    /// The applier, whoever holds it.
    pub fn applier(&self) -> &Applier<P> {
        match self {
            Backing::Voting(v) => v.node().applier(),
            Backing::Serving(a) => a,
        }
    }

    /// The applier, mutably. One store, one writer: this is the same
    /// handle, and local maintenance goes through it rather than
    /// through a second one.
    pub fn applier_mut(&mut self) -> &mut Applier<P> {
        match self {
            Backing::Voting(v) => v.node_mut().applier_mut(),
            Backing::Serving(a) => a,
        }
    }
}

/// How much work each side of this process gets per turn.
///
/// Local is not free, and neither is the frontend. These are the two
/// sides' own budgets: what a voter takes from its ingress in one turn,
/// and what the frontend may hold. Charging one side's work to the
/// other's limit is how a busy caller starves a voter's recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budgets {
    /// Local submissions a voter takes in one turn.
    pub local_per_turn: usize,
}

impl Default for Budgets {
    fn default() -> Self {
        Budgets { local_per_turn: 32 }
    }
}

/// The other voters of this domain, and the connections held to them.
///
/// Dialling is best effort and never a precondition. A voter this
/// process cannot reach right now contributes no evidence and nothing
/// else: it does not fail a submission, it does not stop the others from
/// receiving one, and it does not change the quorum rule. So a failed
/// dial is counted and retried, not reported as an error.
pub struct PeerPlane {
    transport: Transport,
    domain: coord_types::ids::DomainId,
    me: coord_types::ids::ReplicaIncarnation,
    peers: Vec<crate::peers::Peer>,
    /// Dials attempted and dials that reached a voter (diagnostic).
    pub dialled: (u64, u64),
    /// The control and bulk counts last reported on stderr, so the report is
    /// repeated when it changes and not on every connection event.
    reported: Option<(usize, usize)>,
    /// When each voter is dialled again after the first time (task-d03).
    redial: crate::redial::Redial,
}

impl PeerPlane {
    /// The plane over `transport`, for the voters in `peers`.
    pub fn new(
        transport: Transport,
        domain: coord_types::ids::DomainId,
        me: coord_types::ids::ReplicaIncarnation,
        peers: Vec<crate::peers::Peer>,
    ) -> Self {
        let redial =
            crate::redial::Redial::new(peers.iter().map(|p| crate::redial::salt(&p.replica.0)));
        PeerPlane {
            transport,
            domain,
            me,
            peers,
            dialled: (0, 0),
            reported: None,
            redial,
        }
    }

    /// Say on stderr how many voters this node holds a link to, when
    /// that differs from what it last said.
    ///
    /// Reachability is not settled by the startup dial. Both ends of a
    /// pair dial, the connection that loses the collision is closed,
    /// and the one that won may still be finishing its handshake at
    /// this end when the loser goes: a count taken at that instant is
    /// one short, and a count reported only once would stay so for
    /// whoever reads it. So the line follows the mesh instead.
    ///
    /// Stderr, not stdout: this happens after the startup report, and
    /// a process must not die because whoever read its startup report
    /// has stopped reading.
    fn report(&mut self) {
        let reachable = (self.reachable(), self.reachable_bulk());
        if self.reported == Some(reachable) {
            return;
        }
        self.reported = Some(reachable);
        eprintln!(
            "peers connected={} of {} attempts={} bulk={}",
            reachable.0,
            self.peers.len(),
            self.dialled.0,
            reachable.1
        );
    }

    /// Voters currently reachable: those whose control lane a
    /// connection is holding, whichever end dialled it.
    ///
    /// Not the dials that succeeded. Both ends of a pair dial, one of
    /// the two connections is closed as soon as they meet, and a node
    /// that counted its own successful dials would report a link it no
    /// longer holds and miss one it holds only because the peer dialled.
    pub fn reachable(&self) -> usize {
        self.peers.iter().filter(|p| self.holds(p)).count()
    }

    /// Voters whose bulk lane a connection is holding.
    pub fn reachable_bulk(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| self.holds_lane(p, coord_transport::Lane::Bulk))
            .count()
    }

    /// Whether a connection is holding this peer's control lane.
    fn holds(&self, peer: &crate::peers::Peer) -> bool {
        self.holds_lane(peer, coord_transport::Lane::Control)
    }

    /// Whether a connection is holding one of this peer's lanes.
    fn holds_lane(&self, peer: &crate::peers::Peer, lane: coord_transport::Lane) -> bool {
        self.transport.linked(peer.replica, peer.incarnation, lane)
    }

    /// Queue one protocol frame for a voter.
    fn send(
        &self,
        to: coord_core::effect::PeerId,
        message: &[u8],
    ) -> Result<(), coord_transport::SendError> {
        // A machine publishes the consensus message; the frame around it
        // is the transport's vocabulary, put on here. A peer stream
        // carries exactly this kind, and the reader on the other end
        // refuses anything else rather than handing it to consensus.
        let frame = coord_transport::evidence_frame(message)
            .map_err(|_| coord_transport::SendError::NotConnected)?;
        // Catch-up traffic goes down the bulk lane, and everything the
        // protocol needs to make progress goes down the control one.
        // They are separated because they compete: a replica fetching
        // the content of commands it missed moves whole payloads, and
        // sharing a queue with proposals and acknowledgements means the
        // frames it drops when that queue fills are the ones it is
        // catching up *with*. That is not a hypothesis -- it is what one
        // voter of three did under a benchmark, permanently.
        let lane = if coord_consensus::is_payload_transfer(message) {
            coord_transport::Lane::Bulk
        } else {
            coord_transport::Lane::Control
        };
        self.transport.send(
            coord_transport::Destination::Replica {
                replica: to.replica,
                incarnation: to.incarnation,
                lane,
            },
            self.domain,
            frame,
        )
    }
}

/// The other voters, as this domain's collector sees them.
///
/// A submission is the *collector's*, not the voter's: it carries
/// admission claims minted for a client's session, and only a principal
/// this domain trusts to speak for clients may make one. So these links
/// are API-class -- the same kind an out-of-process Kine collector
/// holds -- and this process presents its collector credential on them,
/// not the node certificate it votes with.
///
/// They share the API endpoint rather than having their own. One
/// endpoint serves callers and dials voters, because a connection's
/// direction already says which of the two a peer is: a stream a caller
/// opens is a request to serve, and a stream a voter opens on a link
/// this process dialled is that voter's evidence.
pub struct CollectorLinks {
    peers: Vec<crate::peers::Peer>,
    /// Dials attempted and dials that reached a voter (diagnostic).
    pub dialled: (u64, u64),
    /// The count last reported on stderr (see [`PeerPlane`]'s report).
    reported: Option<usize>,
    /// When each voter is dialled again after the first time (task-d03).
    redial: crate::redial::Redial,
}

impl CollectorLinks {
    /// Links to `peers`, none of them held yet.
    pub fn new(peers: Vec<crate::peers::Peer>) -> Self {
        let redial =
            crate::redial::Redial::new(peers.iter().map(|p| crate::redial::salt(&p.replica.0)));
        CollectorLinks {
            peers,
            dialled: (0, 0),
            reported: None,
            redial,
        }
    }

    /// Say on stderr how many voters this process can submit to, when
    /// that differs from what it last said.
    fn report(&mut self, api: &Transport) {
        if self.peers.is_empty() {
            return;
        }
        let reachable = self.reachable(api);
        if self.reported == Some(reachable) {
            return;
        }
        self.reported = Some(reachable);
        eprintln!(
            "voters submittable={} of {} attempts={}",
            reachable,
            self.peers.len(),
            self.dialled.0
        );
    }

    /// Voters currently reachable for a submission: those whose unary
    /// lane a link this process dialled is holding.
    pub fn reachable(&self, api: &Transport) -> usize {
        self.peers.iter().filter(|p| Self::holds(api, p)).count()
    }

    /// Whether this process holds a submission link to `peer`.
    fn holds(api: &Transport, peer: &crate::peers::Peer) -> bool {
        api.linked(peer.replica, peer.incarnation, coord_transport::Lane::Unary)
    }
}

/// One domain running in this process: the voter that writes its store,
/// and the frontend in front of it.
///
/// One task, one domain: the frontend, the voter, the store and the held
/// streams are all the same `&mut`, so there is no lock between a
/// decision and the state it was made against, and no window in which a
/// second task could answer the same caller.
pub struct Domain<P: Persistence> {
    backing: Backing<P>,
    frontend: Frontend,
    /// This node's own recovery baseline, where it keeps one.
    housekeeping: Option<Housekeeping>,
    /// The other voters, where this process votes. `None` for a process
    /// that does not, and for a domain with nobody else in it.
    plane: Option<PeerPlane>,
    /// The other voters, as somewhere to submit to. Empty for a domain
    /// with nobody else in it, and for a process whose frontend holds no
    /// collector credential to submit with.
    links: CollectorLinks,
    /// Lease and private-TTL expiry, where this process votes. A node
    /// that runs no voter schedules nothing: an expiry is a command,
    /// and proposing one is a leader's.
    expiry: Option<crate::leases::Expiry>,
    /// When this node last asked for a payload it lacks, and how many
    /// payload transfers a peer had answered it with by then. A time
    /// and not a turn count: a domain with nothing else happening takes
    /// no turns, and that is exactly when the asking has to go on. The
    /// count beside it makes the ask a window rather than a rate -- a
    /// batch answered in full moves it, and the next ask goes at once
    /// -- and it counts answers rather than what is missing, because
    /// what is missing also moves when a command arrives by identity,
    /// which under load is every turn. The third field is how many
    /// that ask was for, so a partial answer is not mistaken for a
    /// complete one.
    asked: Option<(std::time::Instant, u64, u64)>,
    /// Evidence this voter has produced for commands whose submitter it
    /// does not know yet, oldest first.
    parked: coord_daemon::parked::Parked,
    /// Where the last turn's look at the collector's half-held commands
    /// left off, so a command that cannot settle here does not keep the
    /// ones behind it from being looked at.
    settle_cursor: usize,
    /// Peers this node currently cannot queue a frame for, and how many
    /// frames it has dropped for each since it last could. Kept so the
    /// condition is said once when it starts and once when it ends,
    /// rather than once per frame.
    undeliverable: BTreeMap<ReplicaId, u64>,
    /// Whether this node has already said it has no peer plane. The
    /// same reason: it is a standing condition, not an event.
    no_plane_said: bool,
    /// Refusals this node keeps saying, so that it says them less.
    recurring: Recurring,
    /// When this domain's loop started, as the base of the monotonic
    /// milliseconds the collector schedules re-offers against.
    ///
    /// Not the wall clock the deadline path uses. A retry schedule on a
    /// clock that can step is a schedule that can stall or stampede,
    /// and a node whose time source is corrected must not answer that
    /// by re-offering everything at once.
    started: std::time::Instant,
    /// Peer events taken since the last caller's event, so the peer
    /// plane's priority cannot become the caller plane's starvation.
    peer_streak: u32,
    budgets: Budgets,
    /// This run's stage accounting (task-61), shared with the voter's
    /// node so the journal and materialization points record into the
    /// same cells the snapshot reads.
    recorder: std::sync::Arc<coord_daemon::metrics::Recorder>,
    /// Dials out after the first ones, one task each (task-d03). A dial is
    /// mostly waiting -- an absent voter costs a whole handshake timeout
    /// -- so it runs beside the loop rather than inside it, and ends with
    /// the domain: a set aborts what it still holds when it is dropped.
    dials: tokio::task::JoinSet<Dialled>,
    /// Which plane and voter each dial in `dials` is for, so one that
    /// ends without an answer still frees its voter's schedule.
    dialling: BTreeMap<tokio::task::Id, (Plane, usize)>,
}

/// The two planes a voter is dialled on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plane {
    /// The peer plane, as a voter.
    Peer,
    /// The api plane, as this domain's collector.
    Collector,
}

/// How one dial out ended.
struct Dialled {
    plane: Plane,
    /// The voter's place in its plane's list.
    index: usize,
    /// Whether the connection it opened was registered; on the peer plane,
    /// the control lane's.
    reached: bool,
    /// Why not, when it was not.
    error: Option<String>,
}

/// The stages this daemon has an instrumentation point for (task-61).
///
/// Admission is recorded where the frontend decides a caller's frame;
/// the journal and materialization are recorded by the voter's node,
/// around the flush of a round's protocol transitions and around the
/// application of each executable command. Every other stage has no
/// point in this build and is reported as not instrumented rather than
/// as a zero: a count nobody took is not a count of nothing.
pub const INSTRUMENTED: [coord_daemon::metrics::Stage; 3] = [
    coord_daemon::metrics::Stage::Admission,
    coord_daemon::metrics::Stage::Journal,
    coord_daemon::metrics::Stage::Materialization,
];

/// A recorder for this daemon: nothing observed yet, and the stages it
/// does not instrument stated as such.
///
/// What is instrumented follows what runs here, not the roles alone. The
/// journal and materialization points are the voter's node's, so a
/// process with no voter (`voting` false) records neither, and reports
/// both as not instrumented rather than as observed zeroes -- which they
/// would be the day its serving applier materializes something.
pub fn recorder(voting: bool) -> coord_daemon::metrics::Recorder {
    if voting {
        coord_daemon::metrics::Recorder::instrumenting(&INSTRUMENTED)
    } else {
        coord_daemon::metrics::Recorder::instrumenting(&INSTRUMENTED[..1])
    }
}

/// How long evidence waits for the submission that says where it goes.
///
/// The race this covers is short: the submission and the proposal leave
/// the collector's node at nearly the same moment, and the proposal
/// arriving first is a matter of scheduling, not of distance. A second
/// is orders of magnitude past it.
///
/// A performance control, not a correctness boundary (task-c02). What
/// is let go here is not lost: the voter that produced it keeps what it
/// published, and the submission that eventually names the command --
/// a collector's repeat, or a retry -- has the voter publish it again.
/// The hold exists so that the ordinary race, won in microseconds, is
/// served from here rather than from that repair, and so that a loaded
/// voter holds a window's evidence rather than a whole depth of it.
///
/// The window is sized against the collector's repeat schedule. A
/// submission a destination's lane could not queue is offered again by
/// the collector that owns it, on a schedule that backs off to
/// [`coord_collector::OFFER_CEILING_MILLIS`] between repeats; a hold
/// that spans that schedule serves the repeat from here, and a hold
/// shorter than it serves the repeat from the voter's repair -- correct
/// either way, but the repair is bounded per command and per boot, and
/// spending it on the ordinary case is spending it on the wrong one.
/// Twice the ceiling is what the schedule actually needs: the first
/// repeat goes at the floor and the wait doubles from there, so two
/// ceilings covers six consecutive refusals of the same destination
/// before the wait is at the ceiling at all. Sizing it for the ceiling
/// *itself* would be sizing the common case for the rare one -- the
/// hold's volume is the ordinary race, which resolves in microseconds,
/// and every millisecond of window costs a domain's whole offered rate
/// in entries.
const PARKED_HOLD: core::time::Duration =
    core::time::Duration::from_millis(coord_collector::OFFER_CEILING_MILLIS * 2);

/// How much evidence a node holds for want of a submitter.
///
/// The ceiling on what the window above can cost. The two are one
/// bound, not two: a node holds roughly its offered rate times the
/// window, so a window that grows and a depth that does not is a depth
/// that starts being reached. This covers a couple of seconds of a
/// domain running at hundreds of commands a second, in frames that are
/// one vote each.
///
/// With the hold above this is a ceiling rather than the working bound:
/// what is normally in here is one second's worth of a race that is won
/// in microseconds. Reaching it is a signal in its own right, counted
/// and said apart from the hold expiring. Like the hold, what it drops
/// is recoverable the same way (task-c02).
const PARKED_EVIDENCE: usize = 1024;

/// Held evidence is due to be let go when the oldest of it has waited
/// the whole hold.
///
/// The hold is applied only when a turn runs, and on a domain that has
/// gone quiet nothing else would run one: evidence parked just before
/// the last submission would sit past the hold, and `unclaimed` would
/// not be said, until unrelated traffic arrived. Registered here, the
/// loop takes a turn when the oldest hold runs out, and that turn lets
/// it go -- which moves this deadline on to the next oldest, or away.
impl Deadline for coord_daemon::parked::Parked {
    fn next_deadline(&self) -> Option<std::time::Instant> {
        self.next_expiry()
    }
}

/// How often a node repeats a request for a payload it is waiting on.
///
/// Bounded by time rather than by events for the same reason the ask
/// exists at all: the replica that can answer may not have been able to
/// the first time -- its own proposal was not durable yet -- and nothing
/// else is going to happen on an idle domain to prompt a second try.
const PAYLOAD_RETRY: std::time::Duration = std::time::Duration::from_millis(50);

/// Something in a domain that has work due at a time rather than on an
/// event.
///
/// The domain loop otherwise waits only on its sockets, so work that
/// becomes due while nothing arrives -- a lease whose deadline passes on
/// a quiet domain -- would wait for unrelated traffic to be noticed. The
/// loop instead sleeps until the earliest deadline any component
/// registers ([`Domain::next_deadline`]) and gives the voter a turn when
/// it passes. Lease expiry, the expiry of held evidence and the
/// collector's re-offers all register here rather than adding arms of
/// their own to the loop.
pub trait Deadline {
    /// The earliest instant this component has work due, if it has any.
    ///
    /// It must move past a deadline once the turn that deadline wakes has
    /// done the work due at it; a deadline that stays in the past would
    /// wake the loop again at once, for ever.
    fn next_deadline(&self) -> Option<std::time::Instant>;
}

/// How many half-held commands one turn consults the durable record for
/// (task-c02). A bound on one turn's reads, not on the obligation: what
/// is not reached this turn is reached on a later one, because the turns
/// rotate through the list rather than each starting at its head.
const SETTLE_PER_TURN: usize = 16;

/// The collector's re-offer schedule, on this domain's clock.
///
/// The collector decides when a destination that could not take a
/// submission is due again, and `Domain::reoffer` makes the offers that
/// are due on every pass of the loop. What was missing is the pass: a
/// rejected fan-out is usually recorded before the retry floor has
/// elapsed, so that pass finds nothing due, and a domain with no other
/// traffic -- a frontend-only one above all -- then waited on its
/// sockets and never re-offered, which is the caller retry the collector
/// exists to make unnecessary. Registered here, the loop wakes when the
/// next re-offer falls due, and the offer it makes moves the schedule on.
struct Reoffers<'a> {
    dispatcher: &'a Dispatcher,
    /// The instant the collector's millisecond clock counts from.
    started: std::time::Instant,
}

impl Deadline for Reoffers<'_> {
    fn next_deadline(&self) -> Option<std::time::Instant> {
        self.dispatcher
            .next_due()
            .map(|at| self.started + std::time::Duration::from_millis(at.get()))
    }
}

/// How many times a recurring condition is said in full before it is
/// said only at each doubling.
const SAID_IN_FULL: u64 = 8;

/// How many distinct recurring reasons this node tracks separately.
///
/// The reasons come from bounded enums, so this is headroom rather than
/// a policy. Past it everything new is folded together, because a map
/// keyed by something a peer influences is a map a peer can grow.
const RECURRING_REASONS: usize = 64;

/// Conditions this node says over and over, kept so that it says them
/// less.
///
/// A refusal under sustained load is not an event, it is a condition,
/// and the loop that produces it runs as fast as the runtime turns. A
/// line each time is a line every few microseconds: it fills a disk, it
/// buries the line an operator was meant to read, and it costs the node
/// the turns it should be spending on the work that would end the
/// condition. One voter of three wrote a 106 MB log saying
/// `Backpressure` while the payloads it was waiting for went
/// undelivered, which is the failure this exists to stop being part of.
///
/// Each reason is said in full the first `SAID_IN_FULL` times -- an
/// operator debugging a handful of refusals wants all of them -- and
/// after that at each doubling, so the lines are logarithmic in the
/// occurrences and the last one is within a factor of two of the truth.
#[derive(Default)]
struct Recurring {
    counts: BTreeMap<String, u64>,
}

impl Recurring {
    /// Record one occurrence of `reason`, and say how many there have
    /// been if this one should be printed.
    fn seen(&mut self, reason: &str) -> Option<u64> {
        // The head, because a reason carrying a command identity is a
        // different string every time and would defeat the point.
        let head = reason.split('(').next().unwrap_or(reason);
        let key = if self.counts.contains_key(head) || self.counts.len() < RECURRING_REASONS {
            head
        } else {
            "other"
        };
        let n = self.counts.entry(key.to_string()).or_insert(0);
        *n += 1;
        (*n <= SAID_IN_FULL || n.is_power_of_two()).then_some(*n)
    }
}

/// How many frames this node must fail to queue for one peer before it
/// says so.
///
/// A queue that fills and drains again is what backpressure looks like
/// when it is working, and saying so would be a line per turn about a
/// domain that is fine. What an operator needs to hear about is a lane
/// that stays full, which is a peer too far behind to take what this
/// node has for it.
const UNDELIVERABLE_SAID_AT: u64 = 64;

/// Destinations one turn will re-offer a submission to.
///
/// A repeat is delivery work, and delivery work must never be able to
/// crowd out the work it exists to enable. This is the whole of what a
/// turn spends on it; what is still owed after that is owed on the next
/// turn, on the collector's schedule.
const OFFERS_PER_TURN: usize = 16;

/// How many peer events in a row are taken before the caller's plane is
/// polled first for one turn.
///
/// High enough that the ordering still holds in the case it is for -- a
/// burst of votes for one command is a handful of frames, and a
/// recovery summary is not much more -- and low enough that a caller's
/// request waits for a bounded number of peer frames rather than for
/// the domain to go quiet.
const PEER_BEFORE_API: u32 = 64;

/// Whether the caller's plane is polled before the peer plane this
/// turn, given how many peer events have been taken since the last
/// caller's event.
///
/// The peer plane keeps its priority -- it is what lets a caller's
/// request finish -- but only for a bounded run. A priority with
/// nothing behind it is starvation, and on a busy domain the peer plane
/// is ready on every poll, so a loop that always prefers it never polls
/// the caller's plane at all.
const fn poll_api_first(peer_streak: u32) -> bool {
    peer_streak >= PEER_BEFORE_API
}

/// How many payloads one ask is for, given how many are missing.
///
/// The protocol's own bound, applied here so the runtime knows what a
/// complete answer to its ask looks like without the machine having to
/// tell it.
const fn payload_batch_size(missing: usize) -> u64 {
    let bound = coord_consensus::messages::MAX_PAYLOAD_TRANSFER;
    if missing < bound {
        missing as u64
    } else {
        bound as u64
    }
}

/// Whether a replica lacking payloads should ask for them now.
///
/// `asked` is what the last ask was: when it went, how many payload
/// transfers had been answered by then, and how many it was for.
/// `answered` is how many have been answered since boot.
///
/// Two rules, and the second is the one that was missing. The ask goes
/// again at once when the last one was answered **in full**, so a
/// replica behind by more than one batch catches up at a batch per
/// round trip rather than a batch per interval. And it goes again on
/// the interval when it was not, so an ask nobody could answer -- part
/// of a batch the peer does not hold durably yet -- does not stop the
/// asking for good.
///
/// What it must not do is treat *part* of an answer as the whole. An
/// ask is worth up to `MAX_PAYLOAD_TRANSFER` frames on the bulk lane,
/// so re-asking when the first of them lands puts a fresh batch of
/// eight on that lane for every payload that arrives. On a peer that is
/// already behind -- which is the only peer that asks -- the lane fills,
/// the answers are dropped, and the replica falls further behind for
/// having asked: the loop `MAX_PAYLOAD_TRANSFER` exists to prevent, one
/// lane over. The same goes for pacing by how many payloads are still
/// missing, which moves whenever a command arrives by identity and so,
/// under load, moves on nearly every turn.
fn ask_for_payloads_now(
    asked: Option<(std::time::Instant, u64, u64)>,
    answered: u64,
    now: std::time::Instant,
) -> bool {
    asked.is_none_or(|(last, then, batch)| {
        answered >= then.saturating_add(batch) || now.duration_since(last) >= PAYLOAD_RETRY
    })
}

/// Where this node's local recovery images live, and when it makes one.
///
/// A local checkpoint is not a replicated fact and not a protocol step:
/// it is how this node stops its own journal growing without bound. So
/// the trigger is a local setting, the work happens between turns, and
/// a failure costs disk rather than correctness.
struct Housekeeping {
    images: LocalCheckpointStore,
    /// Journal records past the baseline that this node tolerates
    /// before publishing a new one. Zero never publishes.
    after: u64,
    /// The gap this node next attempts at.
    ///
    /// `after` normally, and raised past the current gap after a
    /// failure. Without it a node that cannot publish -- a full disk is
    /// the obvious way -- would attempt a complete export on every turn
    /// for as long as the condition lasted, which is the one shape of
    /// housekeeping that can make an incident worse. Raised, it retries
    /// as the journal grows instead.
    floor: u64,
    limits: LocalLimits,
    /// Publications and failures so far (diagnostic).
    done: (u64, u64),
}

impl<P: Persistence + LocalBaseline> Domain<P> {
    /// Compose `frontend` over `backing`.
    pub fn new(frontend: Frontend, mut backing: Backing<P>, budgets: Budgets) -> Self {
        let recorder = std::sync::Arc::new(recorder(matches!(backing, Backing::Voting(_))));
        if let Backing::Voting(voter) = &mut backing {
            voter
                .node_mut()
                .record_into(std::sync::Arc::clone(&recorder));
        }
        Domain {
            backing,
            frontend,
            housekeeping: None,
            plane: None,
            links: CollectorLinks::new(Vec::new()),
            expiry: None,
            asked: None,
            parked: coord_daemon::parked::Parked::new(PARKED_HOLD, PARKED_EVIDENCE),
            settle_cursor: 0,
            undeliverable: BTreeMap::new(),
            no_plane_said: false,
            recurring: Recurring::default(),
            started: std::time::Instant::now(),
            dials: tokio::task::JoinSet::new(),
            dialling: BTreeMap::new(),
            peer_streak: 0,
            budgets,
            recorder,
        }
    }

    /// A bounded, secret-free metrics snapshot of this node (task-61).
    ///
    /// Assembled from what the store and the transport already measure,
    /// rather than from a parallel set of counters: a second accounting
    /// of the same work is a second thing that can be wrong, and the
    /// one an operator reads would be the one nobody validates.
    ///
    /// Everything this node does not have reports *why*, never zero. A
    /// gap in a dashboard is a question; a zero is an answer, and a
    /// wrong one. The stage counts are this domain's own recorder,
    /// which the frontend and the voter's node record into as they work
    /// (see [`INSTRUMENTED`]).
    pub fn metrics(
        &self,
        roles: &coord_daemon::role::RoleSet,
    ) -> coord_daemon::metrics::MetricsSnapshot {
        let recorder = &*self.recorder;
        use coord_daemon::metrics::{
            Frontiers, Lane, LaneReading, Measure, MetricsSnapshot, ShardIndex, ShardReading,
            Unavailable,
        };

        let frontiers = match self.backing.applier().store().frontiers() {
            Some((journal, materialized, checkpoint)) => Measure::Observed(Frontiers {
                journal,
                materialized,
                checkpoint,
            }),
            None => Measure::Unavailable(Unavailable::Quarantined),
        };
        // The transport does not account per lane in this build: no
        // wait is timed and no frame is counted. So every lane reading
        // is "not instrumented" -- "no samples" would claim the lane was
        // measured and idle, and a zero count that it carried nothing,
        // on a node that may have served all day.
        let lanes = Lane::ALL
            .iter()
            .map(|lane| LaneReading {
                lane: *lane,
                queue_wait: Measure::Unavailable(Unavailable::NotInstrumented),
                credit_wait: Measure::Unavailable(Unavailable::NotInstrumented),
                frames: Measure::Unavailable(Unavailable::NotInstrumented),
                refused: Measure::Unavailable(Unavailable::NotInstrumented),
                headroom: Measure::Unavailable(Unavailable::NoBound),
            })
            .collect();
        // Shard zero for the single-domain preview; task-j07 is where a
        // node spreads domains over a shard set and reports each.
        let shards = ShardIndex::new(0)
            .map(|shard| {
                vec![ShardReading {
                    shard,
                    headroom: Measure::Unavailable(Unavailable::NoBound),
                    pressure_permille: Measure::Unavailable(Unavailable::NoBound),
                }]
            })
            .unwrap_or_default();
        MetricsSnapshot {
            stages: recorder.snapshot_stages(roles),
            lanes,
            shards,
            // Neither the three durability quantities nor the age of a
            // pinned view is timed by this build, whatever the node did.
            durability: Measure::Unavailable(Unavailable::NotInstrumented),
            frontiers,
            view_age: Measure::Unavailable(Unavailable::NotInstrumented),
            engine_pressure: Measure::Unavailable(Unavailable::NoBound),
        }
    }

    /// Publish this node's recovery baseline into `images` once the
    /// journal has run `after` records past the last one.
    pub fn with_checkpoints(mut self, images: LocalCheckpointStore, after: u64) -> Self {
        self.housekeeping = Some(Housekeeping {
            images,
            after,
            floor: after,
            limits: LocalLimits::default(),
            done: (0, 0),
        });
        self
    }

    /// Baselines published, and cycles that failed.
    pub fn checkpoints(&self) -> (u64, u64) {
        self.housekeeping.as_ref().map_or((0, 0), |h| h.done)
    }

    /// Submit to the other voters over `links`.
    pub fn with_links(mut self, links: CollectorLinks) -> Self {
        self.links = links;
        self
    }

    /// Voters this process can currently submit to.
    pub fn submittable(&self, api: &Transport) -> usize {
        self.links.reachable(api)
    }

    /// Reach the other voters through `plane`.
    pub fn with_peers(mut self, plane: PeerPlane) -> Self {
        self.plane = Some(plane);
        self
    }

    /// Voters this process is currently connected to.
    pub fn reachable(&self) -> usize {
        self.plane.as_ref().map_or(0, PeerPlane::reachable)
    }

    /// What this loop has seen.
    pub const fn counts(&self) -> Counts {
        self.frontend.counts
    }

    /// Streams held open for results that do not exist yet.
    pub fn waiting(&self) -> usize {
        self.frontend.pending.len()
    }

    /// Serve `transport` until it ends.
    ///
    /// The voter goes first, every time round. A frontend under load
    /// must not be able to decide how long the voter goes without its
    /// own turn -- its timers, its peers, its recovery -- so the loop
    /// only waits on a socket once the voter has nothing left to do.
    pub async fn run(&mut self, transport: &mut Transport, clock: impl Fn() -> u64) {
        // Reach the other voters. The first dials are the re-dial
        // schedule's, due at once, and go out on the loop's first pass
        // as tasks of their own (task-d03): serving does not wait for
        // them. It used to. Every absent voter then cost a whole
        // handshake timeout per lane before this node served anything,
        // so a voter restarted while another was down sat ten seconds
        // deaf to the domain it had come back to -- and a submission
        // arriving first was always correct anyway, since an unreachable
        // voter contributes nothing and the quorum rule decides.
        let now = std::time::Instant::now();
        if let Some(plane) = &mut self.plane {
            plane.redial.start(now);
        }
        self.links.redial.start(now);
        loop {
            let progressed = match self.turn(transport).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("this voter cannot make its transitions durable: {e}");
                    return;
                }
            };
            // Whatever applying produced for a watch goes out before
            // this loop waits on anything: a subscriber is not an event
            // source, so nothing will wake the loop on its behalf.
            self.pump_watches(&ClockHealth::healthy(clock(), CLOCK_UNCERTAINTY_SECONDS))
                .await;
            self.settle_from_records();
            // A submission a destination could not take is offered
            // again here, on the collector's schedule and within a
            // per-turn budget. It goes after the voter's own turn and
            // before the loop waits. No socket wakes this loop on a
            // re-offer's behalf, and a domain that has gone quiet is
            // exactly when the destination that was busy is free again,
            // so the collector's next due re-offer is registered as a
            // deadline (`Reoffers`) and the loop wakes for it.
            self.reoffer(transport);
            // A link that went down is dialled again, and one that came
            // back lets what was waiting for it go (task-d03).
            self.redial(transport);
            // Two planes and one voter. The peer plane is polled first:
            // a vote, an adoption or a recovery summary from a peer is
            // the work that lets a caller's request finish, and a
            // frontend under load must not be able to hold it up.
            //
            // Which plane an event arrived on is not a detail: a
            // connection identity is unique within its own transport and
            // nowhere else, so an api connection and a peer connection
            // can share a number. Handling them through one arm would
            // let a caller's closing stream look like a voter's.
            // Work due at a time rather than on an event. Last in the
            // order, so it never holds up a frame that is already here;
            // when it fires the loop simply takes another turn, which is
            // where the component whose deadline it was does its work.
            let wake = self.next_deadline();
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
                wake.unwrap_or_else(std::time::Instant::now),
            ));
            // A replica waiting for the content of a command it knows
            // by identity is waiting on a message no socket here will
            // deliver on its own: the peer it asked may not have been
            // able to answer -- its own proposal was not durable yet --
            // and on an idle domain nothing else will prompt a second
            // ask. So it comes back on a timer rather than waiting for
            // an event that is not coming. Every command after the one
            // it lacks is queued behind it, and so is every caller
            // whose stream this node is holding.
            let waiting_for_content = match &self.backing {
                Backing::Voting(v) => v.awaiting().is_some() || v.missing_payloads() > 0,
                Backing::Serving(_) => false,
            };
            // The peer plane goes first, and not for ever. A vote, an
            // adoption or a recovery summary is the work that lets a
            // caller's request finish, so a frontend under load must
            // not be able to hold it up -- but a biased select is a
            // priority, and a priority with nothing behind it is
            // starvation. A busy domain keeps the peer plane ready on
            // every poll, and this loop then never polls the caller's
            // plane at all: measured on a three-voter domain, one
            // voter served 4560 api events and then not one more while
            // its peer arm took another 80000, so every caller bound
            // to that frontend waited out its deadline against a node
            // that was working perfectly.
            //
            // So the bias is a budget. After `PEER_BEFORE_API`
            // consecutive peer events the caller's plane is polled
            // first for one turn, which is enough to guarantee it a
            // share without giving up the ordering that makes the
            // domain progress.
            let api_first = poll_api_first(self.peer_streak);
            let arrived = if api_first {
                tokio::select! {
                    biased;
                    // Voter work that is still outstanding pre-empts
                    // waiting on anything. This branch is taken only
                    // when the last turn actually did something, so a
                    // voter that cannot progress waits rather than
                    // spins.
                    () = std::future::ready(()), if progressed => continue,
                    () = tokio::time::sleep(PAYLOAD_RETRY), if waiting_for_content => continue,
                    event = transport.next_event() => event.map(Arrived::Api),
                    peer = next_peer_event(self.plane.as_mut()) => peer.map(Arrived::Peer),
                    Some(done) = self.dials.join_next_with_id(), if !self.dials.is_empty() => {
                        self.dialled(done);
                        continue;
                    }
                    () = sleep, if wake.is_some() => continue,
                }
            } else {
                tokio::select! {
                    biased;
                    () = std::future::ready(()), if progressed => continue,
                    () = tokio::time::sleep(PAYLOAD_RETRY), if waiting_for_content => continue,
                    peer = next_peer_event(self.plane.as_mut()) => peer.map(Arrived::Peer),
                    event = transport.next_event() => event.map(Arrived::Api),
                    Some(done) = self.dials.join_next_with_id(), if !self.dials.is_empty() => {
                        self.dialled(done);
                        continue;
                    }
                    () = sleep, if wake.is_some() => continue,
                }
            };
            match arrived {
                Some(Arrived::Api(event)) => {
                    self.peer_streak = 0;
                    self.on_transport(transport, event, &clock).await;
                }
                Some(Arrived::Peer(event)) => {
                    self.peer_streak = self.peer_streak.saturating_add(1);
                    self.on_peer_plane(transport, event);
                }
                None => return,
            }
        }
    }

    /// Keep both planes' links to the other voters up (task-d03).
    ///
    /// Looks at which voters a connection is holding, starts the dials the
    /// schedule says are due, and tells the collector about a voter whose
    /// submission link has come back, so what it is owed goes on the next
    /// pass rather than at the backed-off time it had reached. Cheap when
    /// nothing is due: a look at a handful of links.
    fn redial(&mut self, api: &Transport) {
        let now = std::time::Instant::now();
        if let Some(plane) = &mut self.plane {
            for i in 0..plane.peers.len() {
                let held = plane.holds(&plane.peers[i]);
                plane.redial.observe(i, held, now);
            }
            for i in plane.redial.due(now) {
                let peer = plane.peers[i].clone();
                // Only the lanes that are down: a bulk lane still up is
                // not dialled again because the control lane went.
                let lanes: Vec<coord_transport::Lane> =
                    [coord_transport::Lane::Control, coord_transport::Lane::Bulk]
                        .into_iter()
                        .filter(|lane| !plane.holds_lane(&peer, *lane))
                        .collect();
                plane.dialled.0 += 1;
                let dialer = plane.transport.dialer();
                let me = plane.me;
                let task = self.dials.spawn(async move {
                    let mut outcome = Dialled {
                        plane: Plane::Peer,
                        index: i,
                        reached: true,
                        error: None,
                    };
                    for lane in lanes {
                        let result = dial(
                            &dialer,
                            &peer,
                            Some(me),
                            coord_types::wire_v1::PeerRole::Voter,
                            lane,
                        )
                        .await;
                        if let Err(e) = result
                            && lane == coord_transport::Lane::Control
                        {
                            outcome.reached = false;
                            outcome.error = Some(format!("{e:?}"));
                        }
                    }
                    outcome
                });
                self.dialling.insert(task.id(), (Plane::Peer, i));
            }
            plane.report();
        }
        let links = &mut self.links;
        for i in 0..links.peers.len() {
            let held = CollectorLinks::holds(api, &links.peers[i]);
            if links.redial.observe(i, held, now) == crate::redial::Change::Returned {
                let replica = links.peers[i].replica;
                let at = MonotonicMillis::new(
                    u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
                );
                self.frontend
                    .frontend
                    .dispatcher_mut()
                    .reachable_again(&replica, at);
            }
        }
        for i in links.redial.due(now) {
            let peer = links.peers[i].clone();
            links.dialled.0 += 1;
            let dialer = api.dialer();
            let task = self.dials.spawn(async move {
                let result = dial(
                    &dialer,
                    &peer,
                    None,
                    coord_types::wire_v1::PeerRole::Frontend,
                    coord_transport::Lane::Unary,
                )
                .await;
                Dialled {
                    plane: Plane::Collector,
                    index: i,
                    reached: result.is_ok(),
                    error: result.err().map(|e| format!("{e:?}")),
                }
            });
            self.dialling.insert(task.id(), (Plane::Collector, i));
        }
        links.report(api);
    }

    /// A dial started by [`Domain::redial`] has ended.
    fn dialled(&mut self, done: Result<(tokio::task::Id, Dialled), tokio::task::JoinError>) {
        let now = std::time::Instant::now();
        let (plane, index, reached, error) = match done {
            Ok((id, d)) => {
                self.dialling.remove(&id);
                (d.plane, d.index, d.reached, d.error)
            }
            // A dial task that did not finish -- aborted, or it panicked
            // -- still frees its voter's schedule, or the voter would be
            // left in flight and never dialled again.
            Err(e) => match self.dialling.remove(&e.id()) {
                Some((plane, index)) => (plane, index, false, Some(format!("{e}"))),
                None => return,
            },
        };
        let (name, held) = match plane {
            Plane::Peer => {
                let Some(p) = &mut self.plane else { return };
                p.redial.finished(index, reached, now);
                if reached {
                    p.dialled.1 += 1;
                }
                let held = p.peers.get(index).is_some_and(|peer| p.holds(peer));
                (p.peers.get(index).map(|peer| peer.replica), held)
            }
            Plane::Collector => {
                self.links.redial.finished(index, reached, now);
                if reached {
                    self.links.dialled.1 += 1;
                }
                // Whether it is held is known on the next pass; a failed
                // dial is said below only when the link is not up.
                (self.links.peers.get(index).map(|peer| peer.replica), false)
            }
        };
        // A voter that stays away is dialled for as long as it does, so
        // this is a condition and is said less as it goes on.
        if let (Some(error), Some(replica), false) = (error, name, held) {
            let what = match plane {
                Plane::Peer => "cannot reach a voter on the peer plane",
                Plane::Collector => "cannot reach a voter to submit to it",
            };
            if let Some(n) = self.recurring.seen(what) {
                eprintln!("{what}: voter {} {error} ({n} so far)", hex4(&replica));
            }
        }
    }

    /// Schedule lease and private-TTL expiry from this node's leader.
    pub fn with_expiry(mut self, expiry: crate::leases::Expiry) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// The earliest deadline any component of this domain has
    /// registered: when the loop has to take a turn even if nothing
    /// arrives (see [`Deadline`]).
    ///
    /// Expiry counts only where this replica leads, because only then
    /// does a turn run it; a follower's driver has nothing due. Held
    /// evidence counts on any voter, since every voter's turn applies
    /// the hold. The collector's re-offers count on every domain, a
    /// frontend-only one included, since every pass of the loop makes
    /// the re-offers that are due. Further components are folded into
    /// the same minimum.
    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        let (expiry, parked) = match &self.backing {
            Backing::Voting(voter) => (
                self.expiry
                    .as_ref()
                    .filter(|_| voter.leads())
                    .and_then(Deadline::next_deadline),
                Deadline::next_deadline(&self.parked),
            ),
            Backing::Serving(_) => (None, None),
        };
        let reoffer = Reoffers {
            dispatcher: self.frontend.frontend.dispatcher(),
            started: self.started,
        }
        .next_deadline();
        // A voter whose link is down is dialled again on a schedule, and
        // nothing arrives to say the schedule has come due.
        let redial = self
            .plane
            .as_ref()
            .and_then(|p| p.redial.next_deadline())
            .into_iter()
            .chain(self.links.redial.next_deadline())
            .min();
        [expiry, parked, reoffer, redial]
            .into_iter()
            .flatten()
            .min()
    }

    /// Authority epochs proposed, expiry candidates proposed, and leases
    /// currently armed.
    pub fn expiries(&self) -> (u64, u64, usize) {
        self.expiry
            .as_ref()
            .map_or((0, 0, 0), |e| (e.done.0, e.done.1, e.armed()))
    }

    /// Give the voter its turn: the local submissions it is owed, then
    /// whatever applying those produced.
    ///
    /// Returns whether anything happened, which is what tells the loop
    /// to come back rather than wait.
    async fn turn(&mut self, api: &Transport) -> Result<bool, DriveError> {
        self.maintain();
        let Backing::Voting(voter) = &mut self.backing else {
            return Ok(false);
        };
        let (mut out, refused) = voter.serve_local(self.budgets.local_per_turn)?;
        out.absorb(voter.execute()?);
        // A command whose identity this replica learned from evidence
        // and whose content nobody sent it. Execution stops at it
        // rather than going past it, so what unblocks the domain is
        // asking the leader for the payload; the request carries no
        // durable prerequisite and goes out at once.
        //
        // Asked on an interval rather than on every turn: the request
        // has no durable prerequisite and goes out at once, so one is
        // enough until the answer comes, and a busy domain must not
        // turn one missing payload into a request per event.
        let missing = voter.missing_payloads();
        if voter.awaiting().is_some() || missing > 0 {
            let now = std::time::Instant::now();
            // One ask outstanding at a time, and the next one goes the
            // moment the last was answered. The interval is the floor
            // for an ask nobody answered, not the rate a replica catches
            // up at: an ask is bounded at `MAX_PAYLOAD_TRANSFER`
            // commands, so an interval alone would cap catching up at
            // that many per interval -- and a replica behind by more
            // than the domain produces in an interval would never close
            // the gap, however long it ran.
            //
            // What says the last ask was answered is the count of
            // answers to it -- the machine counts an answer only when it
            // names a command the outstanding ask named, and once -- not
            // the count of what is still missing. The
            // missing count moves for two reasons -- a payload arrived,
            // or a command arrived by identity -- and under load the
            // second happens on nearly every turn, so a replica pacing
            // itself by it asks continuously. That is not a wasted
            // message: an ask is answered with up to
            // `MAX_PAYLOAD_TRANSFER` payload frames on the bulk lane,
            // so asking on every turn fills that lane with answers to
            // asks already superseded, the lane drops them, and the
            // replica falls further behind for having asked -- the same
            // loop `MAX_PAYLOAD_TRANSFER` exists to prevent, one lane
            // over.
            let answered = voter.payloads_answered();
            let want = payload_batch_size(missing);
            if ask_for_payloads_now(self.asked, answered, now) {
                self.asked = Some((now, answered, want));
                out.absorb(voter.request_payloads()?);
            }
        } else {
            self.asked = None;
        }
        // Expiry is the leader's to schedule, and every candidate it
        // produces is conditional: nothing here decides that a key
        // goes, only that the cluster should be asked.
        if voter.leads()
            && let Some(expiry) = self.expiry.as_mut()
        {
            let applied = voter.node().executed;
            let frames = expiry.due(voter.node().applier().store(), applied);
            for frame in frames {
                out.absorb(voter.propose_service(&frame)?);
            }
        }
        let provenance = voter.provenance();
        let did = !refused.is_empty() || !out.is_empty();
        // A refusal at the voter's door is silent to the caller, whose
        // stream is held for an answer that is not coming. The reason is
        // a bounded enum, so this node says it rather than leaving an
        // operator with a counter -- and says it less as it repeats, so
        // a domain under sustained backpressure is described rather
        // than transcribed.
        for why in &refused {
            let said = format!("{why:?}");
            if let Some(n) = self.recurring.seen(&said) {
                eprintln!("this voter refused a submission: {said} ({n} so far)");
            }
        }
        self.frontend.counts.refused += refused.len() as u64;
        // What the protocol machine itself refused. Drained every turn
        // rather than left to grow: a refusal nobody reads is a list
        // that lives as long as the process and a reason an operator
        // never sees.
        for why in voter.take_rejections() {
            if let Some(n) = self.recurring.seen(&why) {
                eprintln!("this voter's machine refused: {why} ({n} so far)");
            }
        }
        self.carry(api, out, provenance);
        // A submission is the only thing that can say where this voter's
        // evidence for a command belongs, so whatever was waiting for
        // one is tried again here.
        self.route_parked(api);
        Ok(did)
    }

    /// Keep this node's own recovery baseline current.
    ///
    /// Checked every turn and almost always a comparison: the trigger
    /// is `J - C`, which the store already knows, and only crossing it
    /// spends any I/O. Deliberately not counted as progress -- a
    /// publication is not work a caller is waiting for, and a loop that
    /// treated it as such would keep itself awake to do housekeeping.
    ///
    /// Nothing here can fail the node. An image that could not be
    /// written or a pointer that was refused leaves the previous
    /// baseline selected and the journal holding a longer prefix than
    /// it needs, so the failure is reported on stderr and the domain
    /// goes on serving. The one thing it must not do is go quiet: a
    /// node whose checkpoints have been failing for a week is a node
    /// whose disk is filling, and that is an operator's to see.
    fn maintain(&mut self) {
        let Some(housekeeping) = &self.housekeeping else {
            return;
        };
        if housekeeping.after == 0 {
            return;
        }
        let (after, floor, limits) = (housekeeping.after, housekeeping.floor, housekeeping.limits);
        let gap = self.backing.applier().store().unreclaimed();
        if gap < floor {
            return;
        }
        let images = housekeeping.images.clone();
        let outcome = self
            .backing
            .applier_mut()
            .store_mut()
            .publish_local(&images, &limits);
        let housekeeping = self.housekeeping.as_mut().expect("just borrowed");
        match outcome {
            Ok(Some(published)) => {
                housekeeping.done.0 += 1;
                housekeeping.floor = after;
                eprintln!(
                    "checkpoint represented={} retired={} reclaimed={}",
                    published.represented.get(),
                    published.retired,
                    published.reclaimed
                );
            }
            // Nothing new to represent: the projection has materialized
            // nothing since the last baseline, so an image would select
            // the same state and retire nothing. Not a failure, and not
            // worth a line.
            Ok(None) => housekeeping.floor = after,
            Err(e) => {
                housekeeping.done.1 += 1;
                housekeeping.floor = gap.saturating_add(after);
                eprintln!("this node could not publish a recovery checkpoint: {e}");
            }
        }
    }

    /// Carry out what a voter's round asked for.
    ///
    /// Its evidence and releases go to the collector under this voter's
    /// committed identity, through the same calls a peer's frame makes:
    /// there is no local acknowledgement, and one co-located voter is
    /// one voter's worth of evidence.
    fn carry(&mut self, api: &Transport, out: coord_daemon::Outbound, provenance: PeerProvenance) {
        for frame in out.frontend {
            self.hand_to_collector(api, provenance, &frame);
        }
        for (to, frame) in out.peer {
            // Which generation of that node may receive it is the
            // committed configuration's answer, settled here on the way
            // out.
            //
            // A machine addresses a peer by replica and leaves the
            // generation open, as `ReplicaIncarnation::ZERO`. That is
            // deliberate: a sender does not know which incarnation of
            // another node is current, and it is not the sender's to
            // decide -- what binds an incarnation is the receiver's own
            // certificate, checked when the link was bound. So the
            // runtime resolves it against the committed configuration,
            // exactly as a submission's fan-out does, and a machine that
            // did name a generation is held to the committed one rather
            // than believed.
            let Some(to) = addressed(&self.frontend.membership, to) else {
                self.frontend.counts.not_a_voter += 1;
                continue;
            };
            match self.plane.as_ref().map(|p| p.send(to, &frame)) {
                // Admitted to the lane's queue. Not a vote and not a
                // delivery: what the peer does with it is the peer's,
                // and the collector counts the evidence.
                Some(Ok(())) => {
                    self.frontend.counts.queued_remote += 1;
                    // Coming back is as much an operator's business as
                    // going away, and it is what closes the episode
                    // above. Said only when the going away was said,
                    // so a lane that fills and drains inside one turn
                    // -- ordinary backpressure -- stays quiet in both
                    // directions.
                    if let Some(lost) = self.undeliverable.remove(&to.replica)
                        && lost >= UNDELIVERABLE_SAID_AT
                    {
                        eprintln!(
                            "this voter can send to {} again, after {lost} frames it could not",
                            hex4(&to.replica)
                        );
                    }
                }
                // No route right now. The voter contributes nothing
                // through this process until there is one; the quorum
                // rule decides what that costs.
                //
                // Said, not only counted. A vote this node produced and
                // could not send is the difference between a quorum
                // that forms and one that does not, and it is invisible
                // from every other node.
                //
                // Not once per frame, though, and not once per
                // episode either. A lane that is full stays full for as
                // long as the peer is behind, and this loop runs as
                // fast as the runtime turns: a line each time is a line
                // every few microseconds, which fills a disk and buries
                // the line an operator was meant to read. A lane that
                // fills and drains inside a turn is ordinary
                // backpressure and says nothing at all, and once an
                // episode is long enough to matter it is said at each
                // doubling -- so the lines are logarithmic in the
                // frames lost and the last one an operator reads is
                // within a factor of two of the truth.
                Some(Err(e)) => {
                    let lost = self.undeliverable.entry(to.replica).or_insert(0);
                    *lost += 1;
                    if *lost >= UNDELIVERABLE_SAID_AT && lost.is_power_of_two() {
                        eprintln!(
                            "this voter could not send to {}: {e:?} ({lost} frames and counting)",
                            hex4(&to.replica)
                        );
                    }
                    self.frontend.counts.unavailable += 1;
                }
                None => {
                    if !self.no_plane_said {
                        self.no_plane_said = true;
                        eprintln!("this voter has no peer plane to send on");
                    }
                    self.frontend.counts.unavailable += 1;
                }
            }
        }
        // Timers, read views and entropy are the runtime's, and this
        // build has no loop for them yet. They are counted rather than
        // dropped silently, so what this process cannot do is visible in
        // its own report.
        self.frontend.counts.unserved +=
            (out.arm.len() + out.cancel.len() + out.views.len() + out.entropy.len()) as u64;
    }

    /// One frame a voter addressed to the trusted collector: to
    /// whichever collector submitted the command it is about.
    ///
    /// A voter's evidence belongs to the collector that asked for the
    /// work, not to whichever collector shares its process. In a
    /// deployment where every node runs a frontend, delivering it here
    /// would leave the caller's collector with nothing to count and
    /// hand a second collector evidence for a request it never made.
    ///
    /// Where this voter admitted the command locally, the frame goes to
    /// the collector in this process. The bytes are the same bytes
    /// either way: what changes is which process counts them, and the
    /// identity they are counted under is this voter's committed one in
    /// both cases.
    ///
    /// Where it cannot say at all, the frame is held rather than
    /// delivered here. A voter learns a command's content from a
    /// submission *or* from a peer, and when it learns it from a peer it
    /// acknowledges a command no collector has yet asked it for. That
    /// acknowledgement belongs to whichever collector did the asking,
    /// and the submission naming it is usually already in flight; giving
    /// it to the collector in this process instead loses it, because
    /// that collector has never heard of the command. So it waits for
    /// the submission to say where it goes.
    fn hand_to_collector(&mut self, api: &Transport, provenance: PeerProvenance, bytes: &[u8]) {
        let Ok(frame) = one_frame(bytes) else {
            self.frontend.counts.unserved += 1;
            return;
        };
        match self.owed_to(&frame) {
            Some(coord_daemon::voter::Origin::Connection(id)) => {
                self.return_to_collector(api, id, bytes);
            }
            Some(coord_daemon::voter::Origin::Local) => {
                self.on_frame_from_voter(provenance, &frame);
            }
            None => match command_of_frame(&frame) {
                Some(command) => self.park(command, provenance, bytes),
                // Not about a command at all: nothing can arrive later to
                // say where it belongs, so this process is where it ends.
                None => self.on_frame_from_voter(provenance, &frame),
            },
        }
    }

    /// Send a frame back to the collector on the other end of `id`.
    fn return_to_collector(&mut self, api: &Transport, id: u64, bytes: &[u8]) {
        let sent = api.send(
            coord_transport::Destination::Connection(coord_transport::ConnectionId(id)),
            self.frontend.membership.domain(),
            bytes.to_vec(),
        );
        match sent {
            Ok(()) => self.frontend.counts.returned += 1,
            Err(_) => self.frontend.counts.unreturnable += 1,
        }
    }

    /// Hold a frame until the submission that says where it belongs
    /// arrives.
    ///
    /// Bounded in depth as well as in time, and past the depth the
    /// oldest goes, which is the one whose submission is least likely
    /// still coming. Neither bound loses anything a caller needs: the
    /// command is decided and durable either way, and the submission
    /// that names it has this voter publish its evidence again
    /// (task-c02).
    fn park(&mut self, command: CommandId, provenance: PeerProvenance, bytes: &[u8]) {
        let crowded_out = self.parked.park(
            command,
            provenance,
            bytes.to_vec(),
            std::time::Instant::now(),
        );
        self.frontend.counts.parked += 1;
        // Said once, not once per frame. That this happens at all is
        // ordinary -- it is a command this voter heard about from a
        // peer before the collector asked it -- and an operator wants
        // to know the path is in use without a line per acknowledgement.
        if self.frontend.counts.parked == 1 {
            eprintln!("this voter is holding evidence for a submitter it does not know yet");
        }
        if crowded_out > 0 {
            // Said once, and not the same thing as the hold expiring:
            // this is the bound itself being reached, which it should
            // not be while the hold is doing the releasing.
            if self.frontend.counts.crowded_out == 0 {
                eprintln!("this voter dropped held evidence for want of room");
            }
            self.frontend.counts.crowded_out += crowded_out as u64;
        }
    }

    /// Monotonic milliseconds since this domain's loop started: the
    /// reading a request's deadline and a destination's repeat schedule
    /// are measured on. Not the wall clock the caller's token is judged
    /// by, and never stands in for it.
    fn now_millis(&self) -> MonotonicMillis {
        MonotonicMillis::new(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    /// Record one offer's outcome with the collector, and count it.
    ///
    /// Every offer goes through here, the first and every repeat, so
    /// the delivery state the collector re-offers from is the state
    /// this process actually produced rather than a plan it was
    /// assumed to have carried out.
    fn record_offer(&mut self, command: CommandId, out: &fanout::Dispatched) {
        let counts = &mut self.frontend.counts;
        counts.queued_local += out.queued_local() as u64;
        counts.queued_remote += out.queued_remote() as u64;
        counts.not_a_voter += out.not_a_committed_voter() as u64;
        counts.saturated += out.saturated() as u64;
        counts.unavailable += out.unavailable() as u64;
        counts.undeliverable += out.undeliverable() as u64;
        let now = self.now_millis();
        self.frontend
            .frontend
            .dispatcher_mut()
            .offered(now, &out.offered(command));
    }

    /// Offer again what a destination could not take, for commands this
    /// collector still owes one.
    ///
    /// Bounded in two ways that matter. `OFFERS_PER_TURN` caps what one
    /// turn spends on repeats, so a congested destination cannot
    /// displace new work, protocol traffic or recovery; and the
    /// collector's own schedule decides which destinations are due, so
    /// the budget is spread across commands rather than poured into the
    /// first one. Neither bound ever gives up on an accepted command:
    /// what they bound is the rate, and the obligation ends at
    /// settlement.
    fn reoffer(&mut self, transport: &Transport) {
        let now = self.now_millis();
        let due = self
            .frontend
            .frontend
            .dispatcher_mut()
            .due_offers(now, OFFERS_PER_TURN);
        for plan in due {
            let command = plan.command;
            let out = fanout::dispatch(
                &self.frontend.membership,
                transport,
                self.frontend
                    .local
                    .as_ref()
                    .map(|l| l as &dyn fanout::LocalIngress),
                &plan,
            );
            self.frontend.counts.reoffered += 1;
            self.record_offer(command, &out);
            // Said, and said less as it goes on: a re-offer is delivery
            // backpressure being worked off, which an operator wants to
            // know is happening without a line per attempt.
            if let Some(n) = self.recurring.seen("reoffered") {
                eprintln!(
                    "this collector offered a submission again to a voter that could not take it ({n} so far)"
                );
            }
        }
    }
    /// Deliver held evidence whose submitter this voter now knows, and
    /// let go of what has waited longer than the race can take.
    ///
    /// Called after submissions are taken in, which is the only thing
    /// that can supply the answer, and at the end of every turn. On a
    /// node nobody is submitting to no turn would come, so the oldest
    /// evidence's hold is registered as a deadline (see [`Deadline`]):
    /// the loop wakes when it runs out and the turn it takes lets that
    /// evidence go and says `unclaimed`.
    fn route_parked(&mut self, api: &Transport) {
        if self.parked.is_empty() {
            return;
        }
        let Backing::Voting(voter) = &self.backing else {
            return;
        };
        let routed = self.parked.route(std::time::Instant::now(), |command| {
            voter.origin_of(command)
        });
        if routed.unclaimed > 0 {
            if self.frontend.counts.unclaimed == 0 {
                eprintln!("this voter let go of evidence no submission named inside the window");
            }
            self.frontend.counts.unclaimed += routed.unclaimed as u64;
        }
        for (origin, provenance, bytes) in routed.ready {
            match origin {
                coord_daemon::voter::Origin::Connection(id) => {
                    self.return_to_collector(api, id, &bytes);
                }
                coord_daemon::voter::Origin::Local => {
                    if let Ok(frame) = one_frame(&bytes) {
                        self.on_frame_from_voter(provenance, &frame);
                    }
                }
            }
        }
    }

    /// Complete what the collector half holds from this node's own
    /// durable record of the command's execution (task-c02).
    ///
    /// A collector that holds the leader's release but not the votes, or
    /// the votes but not the release, is the shape a lost frontend
    /// delivery leaves behind: the acknowledgement or the release went to
    /// a frontend that did not yet know which collector had asked, and
    /// that frontend stopped holding it. A voter repairs the first case
    /// when the submission reaches it again; this closes both when the
    /// command has executed *here*, because then this node's committed
    /// state says what the missing half would have said. It is the same
    /// record that answers a caller's retry before anything is
    /// submitted, read the same way, and trusted for the same reason.
    ///
    /// Bounded per turn, and cheap when nothing is half held, which is
    /// nearly always.
    fn settle_from_records(&mut self) {
        let half = self.frontend.frontend.dispatcher_mut().half_established();
        if half.is_empty() {
            return;
        }
        let mut deliveries = Vec::new();
        // Bounded per turn and rotated across turns: the bound is on
        // this turn's reads, and the rotation is what keeps it from
        // becoming a bound on which commands are ever read.
        let (this_turn, cursor) =
            coord_daemon::settle::window(half, self.settle_cursor, SETTLE_PER_TURN);
        self.settle_cursor = cursor;
        let records = coord_daemon::settle::records_for(self.backing.applier(), this_turn);
        for (command, record) in records {
            match self.frontend.frontend.dispatcher_mut().settle_from_record(
                command,
                record.result_digest,
                record.revision,
                &record.response,
            ) {
                Ok(delivery) => {
                    self.frontend.counts.settled_from_record += 1;
                    deliveries.extend(delivery);
                }
                // The release this collector holds and what this node
                // executed disagree. Neither is believed; said, because
                // it is the one outcome here that is not ordinary.
                Err(coord_collector::SettleError::Mismatch) => {
                    let head: String = command.as_bytes()[..4]
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect();
                    let said = format!("release-record-mismatch({head})");
                    if let Some(n) = self.recurring.seen(&said) {
                        eprintln!(
                            "this node's durable record and the leader's release disagree: {said} ({n} so far)"
                        );
                    }
                }
                Err(_) => {}
            }
        }
        for delivery in deliveries {
            self.answer(delivery);
        }
    }

    /// Which collector this voter owes a frame to, where it knows.
    ///
    /// The command the frame is about is what says so, and it is read
    /// from the frame's own payload rather than tracked alongside it: a
    /// second bookkeeping of which frame belongs to which command is a
    /// second thing that can be wrong.
    fn owed_to(&self, frame: &Frame) -> Option<coord_daemon::voter::Origin> {
        let Backing::Voting(voter) = &self.backing else {
            return None;
        };
        voter.origin_of(&command_of_frame(frame)?)
    }

    /// One frame of a voter's evidence, for the collector in this
    /// process.
    ///
    /// The same call for a voter running here and for one on the other
    /// end of a link: the provenance is the committed identity the
    /// frame's sender was bound to, and the collector validates and
    /// deduplicates by that identity either way.
    fn on_frame_from_voter(&mut self, provenance: PeerProvenance, frame: &Frame) {
        let delivered = match frame.kind {
            KIND_EVIDENCE => decode_evidence(frame).ok().and_then(|message| {
                self.frontend
                    .frontend
                    .dispatcher_mut()
                    .on_evidence(provenance, message)
                    .ok()
                    .flatten()
            }),
            KIND_RELEASE => decode_release(frame).ok().and_then(|released| {
                self.frontend
                    .frontend
                    .dispatcher_mut()
                    .on_release(provenance, released)
                    .ok()
                    .flatten()
            }),
            _ => {
                self.frontend.counts.unserved += 1;
                return;
            }
        };
        if let Some(delivery) = delivered {
            self.answer(delivery);
        }
    }

    /// The answer this domain's durable record already holds for the
    /// invocation `frame` names, if it holds one for this exact command.
    ///
    /// It is answered under the same authorization a live result is.
    /// The record is resolved through `retry::resolve`, which refuses a
    /// session that can no longer execute -- retired, or its rule
    /// disabled or regenerated -- and a result the request would no
    /// longer be authorized for (`retained_is_authorized`, the rule
    /// execution applies to a retained result too). Those are answered
    /// with a refusal rather than the result, and rather than submitted
    /// again: the command has executed, so there is nothing new to order.
    /// What is handed out then goes through the frontend's output gate
    /// with the request's own metadata, which is recorded from this
    /// request because a retry answered here never reached the
    /// dispatcher that records it for a live one.
    fn retained(
        &mut self,
        health: &ClockHealth,
        connection: u64,
        frame: &Frame,
    ) -> Option<Vec<u8>> {
        use coord_storage::retry::{Resolution, RetryBinding};

        let MessageV1::Request(request) = decode(frame).ok()? else {
            return None;
        };
        let key = request.retry_key;
        // The connection must be bound, and bound to the session whose
        // invocation this is. A retained result belongs to a session,
        // and reading one is not something an unbound caller -- or a
        // caller of another session -- may do.
        let binding = self.frontend.frontend.binding(connection)?;
        if !binding.active(health) || binding.session != key.session_id {
            return None;
        }
        let logical = request.logical().ok()?;
        let command = coord_types::CommandId::derive(&key, &logical).ok()?;
        let resolved = {
            let gated = self.backing.applier().store().reader().snapshot().ok()?;
            coord_storage::retry::resolve(
                gated.view(),
                &RetryBinding {
                    retry_key: key,
                    command_id: command,
                    retires: None,
                },
                |record| {
                    coord_storage::apply::retained_is_authorized(
                        gated.view(),
                        &key.session_id,
                        &logical,
                        record,
                    )
                },
            )
            .ok()?
        };
        let record = match resolved {
            Resolution::Result(record) => record,
            // The command executed, and this caller may not have what it
            // produced -- or may not execute at all any more. The same
            // refusal the output gate gives a live result it will not
            // disclose; the command is not proposed a second time.
            Resolution::NoSession => {
                return refusal(command, "the session may no longer execute");
            }
            Resolution::Unauthorized => {
                return refusal(command, "output not authorized by current policy");
            }
            // Not executed, retired below the floor, or a retry key bound
            // to another payload: replicated execution decides each of
            // those at the command's own position, not this node from a
            // record that is not this command's.
            Resolution::Pending | Resolution::Retired { .. } | Resolution::Conflict { .. } => {
                return None;
            }
        };
        let response = coord_types::wire_v1::ResponseV1 {
            command_id: command,
            outcome: coord_types::wire_v1::OutcomeV1::Ok {
                revision: record.revision,
                result: coord_types::wire_v1::BoundedBytes::new(record.response.clone()).ok()?,
            },
        };
        let delivery = coord_collector::Delivery {
            connection,
            retry_key: key,
            frame: MessageV1::Response(response).encode().ok()?,
        };
        let policy = StorePolicySource {
            store: self.backing.applier().store(),
            budget: ViewBudget::default(),
        };
        match self
            .frontend
            .frontend
            .deliver_retained(delivery, command, &logical, &policy)
        {
            Delivered::Answer(delivery) => Some(delivery.frame),
            Delivered::Unbound { .. } => None,
        }
    }

    /// Answer the caller a delivery belongs to, gated against a fresh
    /// barrier.
    fn answer(&mut self, delivery: coord_collector::Delivery) {
        let policy = StorePolicySource {
            store: self.backing.applier().store(),
            budget: ViewBudget::default(),
        };
        let delivery = match self.frontend.frontend.deliver(delivery, &policy) {
            Delivered::Answer(delivery) => delivery,
            // The session this caller's credential named was not
            // established. Its bind stream ends with no answer, which
            // is the whole of what an unbound caller is told; the
            // connection itself stays open and has no session, so its
            // next frame closes it.
            Delivered::Unbound {
                connection,
                retry_key,
                reason,
            } => {
                // Said on this node's log for the same reason a refused
                // bind is: the caller is told only that it has no
                // session, so an operator who cannot see why has nothing
                // to go on.
                eprintln!("a session was not established: {reason:?}");
                if let Ok(responder) = self.frontend.pending.take(connection, &retry_key) {
                    drop(responder);
                }
                self.frontend.counts.unreturnable += 1;
                return;
            }
        };
        match self
            .frontend
            .pending
            .take(delivery.connection, &delivery.retry_key)
        {
            Ok(responder) => {
                self.frontend.counts.released += 1;
                // The caller is answered on the stream it asked on, and
                // the send is not awaited here: a slow reader must not
                // hold up the voter's next turn.
                tokio::spawn(async move {
                    let _ = responder.respond(delivery.frame).await;
                });
            }
            Err(_) => {
                // Nothing is waiting: the caller went away, or resolved
                // the result by identity instead. The release stands.
                self.frontend.counts.unserved += 1;
            }
        }
    }

    async fn on_transport(
        &mut self,
        transport: &mut Transport,
        event: TransportEvent,
        clock: &impl Fn() -> u64,
    ) {
        match event {
            TransportEvent::ApiRequest {
                connection,
                identity,
                frame,
                responder,
                ..
            } => {
                // A collector's submission is a voter's work, not a
                // caller's request, and it takes the same door a local
                // one does. Everything else is a client's frame.
                if frame.kind == KIND_SUBMIT && is_collector(identity.role) {
                    self.on_remote_submission(transport, identity.role, &frame, connection);
                    drop(responder);
                    return;
                }
                let health = ClockHealth::healthy(clock(), CLOCK_UNCERTAINTY_SECONDS);
                let retry_key = invocation_of(&frame);
                // A request this domain has already executed is answered
                // from its durable record rather than submitted again.
                //
                // The collector's retained results live in one process's
                // memory, so after a restart an ordinary client retry
                // would be new work to it: it would be proposed a second
                // time, and a replica that has executed it already has
                // nothing new to order -- the applier would hand back the
                // outcome at the position the command already has, which
                // is not the position the replica's learner is waiting
                // to fill. The record the command left behind is what
                // answers, before anything is submitted, and it is gated
                // on the way out exactly as a fresh result is: a
                // permission lost since it executed protects what it
                // produced.
                if let Some(answer) = self.retained(&health, connection.0, &frame) {
                    self.frontend.counts.released += 1;
                    let _ = responder.respond(answer).await;
                    return;
                }
                let policy = StorePolicySource {
                    store: self.backing.applier().store(),
                    budget: ViewBudget::default(),
                };
                // Admission is the frontend's decision about this frame:
                // verified and admitted, answered, or refused at the door.
                self.recorder.entered(Stage::Admission);
                let started = std::time::Instant::now();
                let now = self.now_millis();
                let ingress = self.frontend.frontend.on_frame(
                    &health,
                    now,
                    connection.0,
                    &frame,
                    self.backing.applier().hub(),
                    &policy,
                );
                // A refused binding is the one refusal an operator
                // cannot diagnose from the outside: the caller is told
                // only that it was refused, deliberately, because
                // whether a token was wrong or merely late is not
                // something an unbound caller may distinguish. The
                // reason is a bounded enum with nothing of the caller
                // in it, so this node says it on its own log.
                if let coord_session::Ingress::Rejected(reason) = &ingress {
                    eprintln!("a binding was refused: {reason:?}");
                }
                let decided = step(ingress, retry_key);
                if matches!(decided, Step::Close { .. }) {
                    self.recorder.refused(Stage::Admission);
                } else {
                    self.recorder.completed(Stage::Admission, started.elapsed());
                }
                self.carry_out(transport, connection, decided, responder)
                    .await;
            }
            TransportEvent::Closed { connection, .. } => {
                // Both sides of the same closing: the frontend forgets
                // the binding and the watches, and every stream this
                // connection held is released rather than dropped, so
                // nothing waits out a deadline for an answer that is no
                // longer coming.
                self.frontend
                    .frontend
                    .on_connection_closed(connection.0, self.backing.applier().hub());
                for responder in self.frontend.pending.close(connection.0) {
                    drop(responder);
                }
                self.frontend.watches.retain(|(c, _), _| *c != connection.0);
            }
            // A voter answering a submission this process's collector
            // made. It is evidence, and it is counted by the identity
            // the transport bound the link to -- never by anything the
            // frame says about itself.
            TransportEvent::ApiDelivery {
                provenance, frame, ..
            } => match provenance {
                Some(provenance) => self.on_frame_from_voter(provenance, &frame),
                // A peer holding no replica identity is not a voter, so
                // what it delivered is not evidence of anything.
                None => self.frontend.counts.unserved += 1,
            },
            // A caller's plane carries no protocol traffic.
            TransportEvent::PeerFrame { .. } => {
                self.frontend.counts.unserved += 1;
            }
            TransportEvent::Connected { .. } => {}
        }
    }

    /// One event from the peer plane.
    ///
    /// Everything here belongs to a voter: a proposal, a vote, an
    /// adoption, a recovery summary. There is no caller's stream to
    /// answer and no session to forget, which is why it is a different
    /// handler and not a branch of the api one.
    fn on_peer_plane(&mut self, api: &Transport, event: TransportEvent) {
        match event {
            TransportEvent::PeerFrame {
                provenance,
                payload,
                ..
            } => {
                let Backing::Voting(voter) = &mut self.backing else {
                    // A process that runs no voter has no use for
                    // protocol traffic, and should not have a peer plane
                    // at all.
                    self.frontend.counts.unserved += 1;
                    return;
                };
                // The payload as it arrived, not re-framed. A machine
                // consumes the consensus message itself; the frame
                // around it is the transport's, and the transport has
                // already refused any kind or version that is not this
                // build's peer evidence.
                match voter.on_peer(provenance, payload) {
                    Ok(out) => {
                        let provenance = voter.provenance();
                        self.carry(api, out, provenance);
                    }
                    Err(e) => eprintln!("this voter cannot carry out a peer's frame: {e}"),
                }
            }
            // A connection ended. Nothing is remembered about which one:
            // whether this process can still reach a voter is the
            // transport's own answer, from whether a connection is
            // holding that link's lane, and in a full mesh one
            // connection of every pair is closed as soon as the two
            // meet. Re-dialling here would answer that with a storm, so
            // it is the re-dial schedule's (`Domain::redial`, on the
            // loop's next pass): a voter whose link is still held after
            // this close costs nothing, and one that lost it is dialled
            // at the floor, jittered. What does happen here is the
            // report: the count of voters held
            // is what an operator reads to see a mesh form, and it is
            // said again whenever a connection's coming or going
            // changed it.
            TransportEvent::Closed { .. } | TransportEvent::Connected { .. } => {
                if let Some(plane) = &mut self.plane {
                    plane.report();
                }
            }
            // A voter's plane carries no requests and no deliveries.
            TransportEvent::ApiRequest { .. } | TransportEvent::ApiDelivery { .. } => {
                self.frontend.counts.unserved += 1;
            }
        }
    }

    /// A submission a collector in another process made.
    ///
    /// The same door the local route uses: the role is the one the
    /// transport bound to that peer's certificate, and the frame is
    /// admitted or refused by the same boundary.
    fn on_remote_submission(
        &mut self,
        api: &Transport,
        role: coord_types::wire_v1::PeerRole,
        frame: &Frame,
        from: coord_transport::ConnectionId,
    ) {
        let Backing::Voting(voter) = &mut self.backing else {
            self.frontend.counts.unserved += 1;
            return;
        };
        // The connection is remembered, not the responder: a submission
        // is not a request and its evidence is not a reply. The stream
        // it arrived on is closed at once, and what this voter
        // eventually has to say goes back on the link as its own
        // delivery, whenever the outbox releases it.
        match voter.on_submission(role, frame, coord_daemon::voter::Origin::Connection(from.0)) {
            Ok(Ok(out)) => {
                let provenance = voter.provenance();
                self.carry(api, out, provenance);
            }
            // A refusal at the machine's door still told this voter
            // where the command came from -- a duplicate submission is
            // exactly the case where the evidence went out before the
            // submitter was known -- so what was held is tried again
            // either way.
            Ok(Err(why)) => {
                let said = format!("{why:?}");
                if let Some(n) = self.recurring.seen(&said) {
                    eprintln!("this voter refused a collector's submission: {said} ({n} so far)");
                }
                self.frontend.counts.refused += 1;
            }
            Err(e) => eprintln!("this voter cannot carry out a submission: {e}"),
        }
        self.route_parked(api);
    }

    async fn carry_out(
        &mut self,
        transport: &Transport,
        connection: coord_transport::ConnectionId,
        decided: Step,
        responder: Responder,
    ) {
        match decided {
            Step::Answer(frame) => {
                let _ = responder.respond(frame).await;
            }
            Step::Hold(key) => {
                if let Some(displaced) = self.frontend.pending.hold(connection.0, key, responder) {
                    drop(displaced);
                }
            }
            Step::Submit(plan) => {
                // The stream is held before the frame goes out, not
                // after: evidence can come back faster than this task
                // returns, and a delivery that arrived first would find
                // nothing waiting and be dropped.
                if let Some(displaced) =
                    self.frontend
                        .pending
                        .hold(connection.0, plan.retry_key, responder)
                {
                    drop(displaced);
                }
                let command = plan.command;
                let out = fanout::dispatch(
                    &self.frontend.membership,
                    transport,
                    self.frontend
                        .local
                        .as_ref()
                        .map(|l| l as &dyn fanout::LocalIngress),
                    &plan,
                );
                self.record_offer(command, &out);
            }
            Step::Watch {
                watch_id,
                registration,
            } => {
                self.frontend.counts.watches += 1;
                self.open_watch(connection.0, watch_id, registration, responder)
                    .await;
            }
            Step::Close { code, reason } => {
                drop(responder);
                transport.disconnect(connection, code, reason);
            }
        }
    }

    /// Take over the stream a watch was opened on: replay what the
    /// registration named from a snapshot, then keep the stream for the
    /// life of the subscription.
    ///
    /// The hub attached the watch at the frontier it had when the open
    /// was decided and queued everything above it from that moment.
    /// What is below the frontier is not the hub's to produce -- it is
    /// history, and it is read here out of one pinned snapshot, in
    /// revision order, into the same watch. Only when that is done does
    /// the subscription become live, so the caller sees one gap-free
    /// sequence across the handover rather than the live tail first and
    /// the backlog after it.
    ///
    /// A replay that cannot be completed ends the subscription rather
    /// than starting it live: a watch that silently began above its
    /// requested revision would be a skipped change, which is exactly
    /// what a resumable stream may not do.
    async fn open_watch(
        &mut self,
        connection: u64,
        watch_id: u64,
        registration: coord_storage::watch::Registration,
        mut responder: Responder,
    ) {
        let hub = self.backing.applier().hub().clone();
        // The namespace the open named, from the frontend that admitted
        // it. A watch this frontend does not know is not one this
        // process may serve output for.
        let Some(namespace) = self.frontend.frontend.watch_namespace(connection, watch_id) else {
            self.frontend.counts.unserved += 1;
            drop(responder);
            return;
        };
        if let Some((from, through)) = registration.replay {
            use coord_storage::watch::ReplayFromViewError;
            use coord_types::wire_v1::WatchCloseReasonV1;
            // One snapshot for the whole replay. Reading history out of
            // several would be reading it at several execution points,
            // and a watch that spanned them would be neither a complete
            // history nor a resumable one.
            let failure = match self.backing.applier().store().reader().snapshot() {
                Err(e) => Some((WatchCloseReasonV1::SourceLost, format!("{e:?}"))),
                Ok(gated) => match coord_storage::watch::replay_from_view(
                    &hub,
                    gated.view(),
                    registration.id,
                    namespace,
                    from,
                    through,
                ) {
                    Ok(()) => None,
                    // The revision was retained when the hub attached
                    // the watch and compacted before it was read. The
                    // client is told which, because the two have
                    // different resumptions: a compacted start needs a
                    // fresh list, a lost source needs another replica.
                    Err(ReplayFromViewError::Compacted { floor }) => Some((
                        WatchCloseReasonV1::Compacted,
                        format!("below the retention floor {}", floor.get()),
                    )),
                    Err(e) => Some((WatchCloseReasonV1::SourceLost, format!("{e:?}"))),
                },
            };
            if let Some((reason, why)) = failure {
                eprintln!("a watch could not be replayed from {}: {why}", from.get());
                self.forget_watch(connection, watch_id, &hub);
                let close = MessageV1::WatchClose(coord_types::wire_v1::WatchCloseV1 {
                    watch_id,
                    reason,
                    last_complete_revision: None,
                })
                .encode()
                .expect("bounded");
                if responder.push(&close).await.is_ok() {
                    self.frontend.counts.watch_frames += 1;
                }
                let _ = responder.finish();
                return;
            }
        }
        if let Some(displaced) = self
            .frontend
            .watches
            .insert((connection, watch_id), responder)
        {
            // The frontend refuses a second open under a live
            // identifier, so this cannot be a live subscription's
            // stream. Ending it is still better than dropping it: a
            // dropped stream is a reset, which a peer reads as a
            // failure rather than an end.
            self.frontend.counts.watches_lost += 1;
            let _ = displaced.finish();
        }
    }

    /// Cancel a watch at the hub and drain it, so both the hub and the
    /// frontend forget the subscription.
    ///
    /// The frames the drain produces are discarded deliberately: this is
    /// the path where the daemon has its own close to write, and a
    /// client that received two closes for one watch would have to
    /// decide which one to resume from.
    fn forget_watch(&mut self, connection: u64, watch_id: u64, hub: &coord_storage::WatchHub) {
        self.frontend
            .frontend
            .cancel_watch(connection, watch_id, hub);
        self.frontend.watches.remove(&(connection, watch_id));
    }

    /// Move whatever the hub has for each live watch onto its stream.
    ///
    /// Called every time round the loop rather than when something looks
    /// like it produced events. A revision reaches the hub from applying
    /// a command, and a follower applies commands it learned from its
    /// peers, which is not work this process's own turn reports; a pump
    /// conditioned on local progress would deliver a follower's watches
    /// only when that node happened to have business of its own.
    ///
    /// One bounded pump reads one fresh authorization barrier and
    /// authorizes exactly the batches it selected under it, so a
    /// permission lost mid-subscription stops the very next batch. The
    /// loop repeats until the hub has nothing left, which is what makes
    /// a backlog drain without waiting for the next event.
    async fn pump_watches(&mut self, health: &ClockHealth) {
        if self.frontend.watches.is_empty() {
            return;
        }
        let hub = self.backing.applier().hub().clone();
        let live: Vec<(u64, u64)> = self.frontend.watches.keys().copied().collect();
        for (connection, watch_id) in live {
            // Whether the subscription is over is the frontend's answer,
            // not this map's: a client cancels on a stream of its own,
            // and the close it was acknowledged with is written there.
            let mut lost = false;
            loop {
                let policy = StorePolicySource {
                    store: self.backing.applier().store(),
                    budget: ViewBudget::default(),
                };
                let frames = self
                    .frontend
                    .frontend
                    .pump_watch(health, connection, watch_id, &hub, &policy);
                if frames.is_empty() {
                    break;
                }
                let Some(responder) = self.frontend.watches.get_mut(&(connection, watch_id)) else {
                    break;
                };
                let mut written = 0;
                for frame in &frames {
                    // A push completes when QUIC has room for it, so a
                    // consumer that stopped reading blocks here rather
                    // than accumulating. What it cannot do is block
                    // forever: the responder's deadline ends the write,
                    // and the subscription ends with it.
                    if responder.push(frame).await.is_err() {
                        lost = true;
                        break;
                    }
                    written += 1;
                }
                self.frontend.counts.watch_frames += written;
                if lost {
                    break;
                }
            }
            let ended = self
                .frontend
                .frontend
                .watch_namespace(connection, watch_id)
                .is_none();
            if (lost || ended)
                && let Some(responder) = self.frontend.watches.remove(&(connection, watch_id))
            {
                {
                    if lost {
                        // Nobody to deliver to. The subscription ends
                        // with its stream: a hub that kept queueing for
                        // it would be a queue filling on behalf of a
                        // consumer that is gone.
                        self.frontend.counts.watches_lost += 1;
                        drop(responder);
                        self.forget_watch(connection, watch_id, &hub);
                    } else {
                        // The close has already gone out on this stream,
                        // or the client cancelled on another: either way
                        // the subscription is over and the stream ends
                        // orderly rather than as a reset.
                        let _ = responder.finish();
                    }
                }
            }
        }
    }
}

/// Dial `peer` as `role` on `lane`, trying each address it lists.
///
/// A catalog entry names every address a node can be found at, and a
/// node has two listeners. Which address serves which plane is settled
/// here: the planes negotiate different ALPNs, so an address that is the
/// wrong one for this role simply fails to negotiate and the next is
/// tried. The identity expected on the other end is the committed one
/// throughout, so a certificate that is not this domain's voter at its
/// committed incarnation fails the handshake whichever address answered.
async fn dial(
    transport: &coord_transport::Dialer,
    peer: &crate::peers::Peer,
    me: Option<coord_types::ids::ReplicaIncarnation>,
    role: coord_types::wire_v1::PeerRole,
    lane: coord_transport::Lane,
) -> Result<coord_transport::ConnectionId, coord_transport::TransportError> {
    let expected = coord_transport::BoundIdentity {
        role: coord_types::wire_v1::PeerRole::Voter,
        replica: Some(peer.replica),
        incarnation: Some(peer.incarnation),
        capabilities: Vec::new(),
    };
    let mut last = coord_transport::TransportError::Connect("no address listed".into());
    for (address, server_name) in &peer.addresses {
        match transport
            .connect(*address, server_name, role, me, lane, expected.clone())
            .await
        {
            Ok(connection) => return Ok(connection),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// The peer a protocol frame may actually be sent to.
///
/// `None` when the committed configuration does not name that replica a
/// voter of this domain, or names it at another generation than the one
/// the machine asked for. Either way the frame is not sent: a protocol
/// frame reaching a generation the cluster has replaced is a vote from
/// a node that is no longer this node.
fn addressed(
    membership: &Membership,
    to: coord_core::effect::PeerId,
) -> Option<coord_core::effect::PeerId> {
    let incarnation = membership.voter_incarnation(&to.replica)?;
    let open = to.incarnation == coord_types::ids::ReplicaIncarnation::ZERO;
    (open || to.incarnation == incarnation).then_some(coord_core::effect::PeerId {
        replica: to.replica,
        incarnation,
    })
}

fn hex4(replica: &coord_types::ids::ReplicaId) -> String {
    replica.0[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// The peer plane's next event, or nothing for ever when there is no
/// peer plane.
///
/// The command a voter's frame is about, where it is about one.
///
/// Read from the frame's own payload rather than tracked beside it, for
/// the same reason [`Domain::owed_to`] does: a second bookkeeping of
/// which frame belongs to which command is a second thing that can be
/// wrong.
fn command_of_frame(frame: &Frame) -> Option<CommandId> {
    match frame.kind {
        KIND_EVIDENCE => coord_consensus::ProtocolMessage::decode(&frame.payload)
            .ok()?
            .command(),
        KIND_RELEASE => decode_release(frame)
            .ok()
            .map(|r| r.established().command()),
        _ => None,
    }
}

/// A `select!` arm needs a future either way. A process with no peers
/// must not have its arm resolve immediately -- that would spin the loop
/// -- so it gets one that never completes and the other arms decide.
async fn next_peer_event(plane: Option<&mut PeerPlane>) -> Option<TransportEvent> {
    match plane {
        Some(plane) => plane.transport.next_event().await,
        None => std::future::pending().await,
    }
}

/// Which plane an event arrived on.
enum Arrived {
    /// The api plane: callers and collectors.
    Api(TransportEvent),
    /// The peer plane: this domain's other voters.
    Peer(TransportEvent),
}

/// One frame from bytes, through the reader the transport uses.
fn one_frame(bytes: &[u8]) -> Result<Frame, coord_types::wire_v1::WireError> {
    let mut reader = coord_types::wire_v1::FrameReader::new();
    reader.push(bytes)?;
    let frame =
        reader
            .next_frame()?
            .ok_or(coord_types::wire_v1::WireError::LengthBelowMinimum {
                length: bytes.len() as u32,
            })?;
    reader.finish()?;
    Ok(frame)
}

/// A refusal of a retained result, in the shape the output gate refuses
/// a live one it will not disclose.
fn refusal(command: coord_types::CommandId, detail: &str) -> Option<Vec<u8>> {
    MessageV1::Response(coord_collector::codes::error_response(
        command,
        coord_collector::codes::NOT_ADMITTED,
        detail,
    ))
    .encode()
    .ok()
}

/// The invocation a frame names, where it names one.
fn invocation_of(frame: &coord_types::wire_v1::Frame) -> Option<RetryKey> {
    match decode(frame) {
        Ok(MessageV1::Request(request)) => Some(request.retry_key),
        Ok(MessageV1::ResolveRequest(resolve)) => Some(resolve.retry_key),
        _ => None,
    }
}

/// Items one watch pump moves under a single barrier read.
const PUMP_BOUND: usize = 64;

/// How far the local clock may be out before a binding is refused.
const CLOCK_UNCERTAINTY_SECONDS: u64 = 5;

/// Build the peer-plane endpoint on the socket already bound.
///
/// The same binder as the API plane, because the question is the same
/// one: whoever is on the other end is a voter of this domain because
/// the committed configuration says the certificate they presented is,
/// or they are nobody. What differs is the role this node presents and
/// therefore the lanes and the ALPN: a peer connection carries protocol
/// traffic and a client's never does.
pub fn peer_endpoint(
    config: &Config,
    membership: &Membership,
    socket: std::net::UdpSocket,
    me: coord_types::ids::ReplicaId,
) -> Result<Transport, TransportError> {
    endpoint(
        config,
        membership,
        socket,
        coord_types::wire_v1::PeerRole::Voter,
        Some(me),
    )
}

/// Build the API-plane endpoint on the socket already bound.
///
/// The socket is taken rather than re-bound: binding again from the
/// address that was reported would either collide with the socket this
/// process is still holding, or leave a window in which the port was
/// free for another process to take.
///
/// The binder is the committed membership's, so what a peer may act as
/// is decided by the configuration the cluster agreed on and not by what
/// the peer says about itself.
pub fn api_endpoint(
    config: &Config,
    membership: &Membership,
    socket: std::net::UdpSocket,
) -> Result<Transport, TransportError> {
    // No replica identity: an api link is keyed by its connection, so
    // two of them never contend for one lane slot and there is nothing
    // for an identity to settle.
    endpoint(
        config,
        membership,
        socket,
        coord_types::wire_v1::PeerRole::Frontend,
        None,
    )
}

fn endpoint(
    config: &Config,
    membership: &Membership,
    socket: std::net::UdpSocket,
    role: coord_types::wire_v1::PeerRole,
    me: Option<coord_types::ids::ReplicaId>,
) -> Result<Transport, TransportError> {
    let identity = coord_daemon::load_identity(
        &config.identity,
        membership.cluster(),
        membership.domain(),
        coord_transport::role_lanes(role)
            .iter()
            .map(|lane| lane.capability())
            .collect(),
        // One listener, one plane. A voter's listener offers the peer
        // ALPN and a caller's offers the api one, so an address that is
        // the wrong one for the plane a caller wants fails to negotiate
        // and the next is tried -- which is what lets a catalog list
        // both of a node's addresses without saying which is which.
        coord_transport::role_class(role),
        me,
    )
    .map_err(|e| TransportError::Identity(e.to_string()))?;
    let binder = coord_membership::binder::PeerBinder::new(membership.clone());
    // The transport's limits are its own (streams, windows, deadlines);
    // the semantic limits the configuration states are the frontend's,
    // and are enforced where the meaning is, not at the framing.
    #[allow(unused_mut)]
    let mut limits = coord_transport::Limits::default();
    // A test build can shorten the connection age cap, so the test that
    // shows a capped connection is dialled again (task-d03) does not wait
    // the twelve hours a real one lasts. Compiled out of a release build:
    // the cap there is the transport's, and is not configuration.
    #[cfg(debug_assertions)]
    if let Some(ms) = std::env::var("COORDD_TEST_CONNECTION_AGE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        limits.max_connection_age = std::time::Duration::from_millis(ms);
    }
    Transport::with_socket(socket, identity, std::sync::Arc::new(binder), limits)
        .map_err(|e| TransportError::Endpoint(format!("{e:?}")))
}

/// Why the endpoint could not be built.
#[derive(Debug)]
pub enum TransportError {
    /// This node's credentials could not be loaded.
    Identity(String),
    /// The endpoint refused the socket or the configuration.
    Endpoint(String),
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            TransportError::Identity(e) => write!(f, "cannot present an identity: {e}"),
            TransportError::Endpoint(e) => write!(f, "cannot serve on the bound socket: {e}"),
        }
    }
}

impl core::error::Error for TransportError {}

#[cfg(test)]
mod tests {
    use coord_membership::genesis::{GenesisManifest, VoterSeed};
    use coord_types::ids::{ReplicaId, ReplicaIncarnation};

    use super::{
        PAYLOAD_RETRY, PEER_BEFORE_API, RECURRING_REASONS, Recurring, SAID_IN_FULL, addressed,
        ask_for_payloads_now, payload_batch_size, poll_api_first,
    };

    /// Held evidence registers when it is due to be let go.
    ///
    /// The hold is applied only when a turn runs. Without a deadline an
    /// idle voter never runs one, so evidence parked just before the
    /// domain went quiet sat past the hold and `unclaimed` was not said
    /// until unrelated traffic arrived. What the domain registers is the
    /// held evidence's own next expiry; that it is the oldest hold, and
    /// moves on once the turn it wakes lets that evidence go, is tested
    /// with the type itself in coord-daemon
    /// (`the_next_expiry_is_the_oldest_hold_and_moves_on_when_it_goes`),
    /// which is also the only crate allowed to mint the voter provenance
    /// a held frame carries.
    #[test]
    fn held_evidence_registers_its_own_next_expiry() {
        use super::{Deadline, PARKED_EVIDENCE, PARKED_HOLD};

        let parked = coord_daemon::parked::Parked::new(PARKED_HOLD, PARKED_EVIDENCE);
        assert_eq!(parked.next_deadline(), None, "nothing held, nothing due");
        assert_eq!(parked.next_deadline(), parked.next_expiry());
    }

    /// The peer plane's priority is a budget, not a licence.
    ///
    /// Without the budget a busy domain keeps the peer plane ready on
    /// every poll and the caller's plane is never polled: measured on a
    /// three-voter domain under the benchmark's load, one voter served
    /// 4560 api events and then not one more while its peer arm took
    /// another 80000, and every caller bound to that frontend waited
    /// out its deadline against a node that was otherwise working.
    #[test]
    fn the_peer_planes_priority_is_bounded_so_a_caller_is_never_starved() {
        // Ordinary: the peer plane goes first, which is the ordering
        // that makes a caller's own request finish.
        assert!(!poll_api_first(0));
        assert!(!poll_api_first(1));
        assert!(!poll_api_first(PEER_BEFORE_API - 1));
        // And a run of peer events cannot go on for ever.
        assert!(poll_api_first(PEER_BEFORE_API));
        assert!(poll_api_first(u32::MAX));
        // The budget is big enough that the ordering still holds for
        // what it is for -- a burst of votes for one command, or a
        // recovery summary -- and small enough that a caller waits for
        // a bounded number of frames rather than for the domain to go
        // quiet.
        assert!((8..=1024).contains(&PEER_BEFORE_API));
    }

    /// A condition that keeps happening is said in full a few times and
    /// then logarithmically, so a node under sustained backpressure
    /// describes itself instead of transcribing itself.
    ///
    /// The number that matters is the last one: 100_000 occurrences
    /// must cost tens of lines, not 100_000. A voter of three spent a
    /// 106 MB log and the turns it needed to catch up saying
    /// `Backpressure` once per refusal, and the domain lost it.
    #[test]
    fn a_condition_that_keeps_happening_is_said_less_as_it_does() {
        let mut said = Recurring::default();
        let mut lines = 0;
        let mut last = 0;
        for _ in 0..100_000 {
            if let Some(n) = said.seen("Backpressure") {
                lines += 1;
                last = n;
            }
        }
        assert!(
            lines < 32,
            "100000 occurrences cost {lines} lines, which is not logarithmic"
        );
        assert!(
            last * 2 > 100_000,
            "the last line said {last}, which is not within a factor of two of 100000"
        );
        // The first few are said in full, because an operator chasing a
        // handful of refusals wants all of them.
        let mut fresh = Recurring::default();
        for n in 1..=SAID_IN_FULL {
            assert_eq!(fresh.seen("Duplicate"), Some(n));
        }
    }

    /// Reasons are counted apart, and a reason carrying a command
    /// identity is still one reason.
    #[test]
    fn a_reason_is_its_head_and_the_set_of_them_is_bounded() {
        let mut said = Recurring::default();
        // Two reasons, counted separately.
        assert_eq!(said.seen("Backpressure"), Some(1));
        assert_eq!(said.seen("Duplicate(CommandIdDigest32(aa))"), Some(1));
        assert_eq!(said.seen("Backpressure"), Some(2));
        // A different digest is the same reason, which is the whole
        // point: keyed by the full string this would never repeat and
        // would never be folded.
        assert_eq!(said.seen("Duplicate(CommandIdDigest32(bb))"), Some(2));
        // And a build that started producing unbounded reason heads
        // costs a fold rather than a growing map.
        let mut many = Recurring::default();
        for n in 0..(RECURRING_REASONS * 4) {
            many.seen(&format!("reason{n}"));
        }
        assert!(many.counts.len() <= RECURRING_REASONS + 1);
    }

    /// A replica catching up asks a batch at a time, and a partial
    /// answer is not a complete one.
    ///
    /// The negative control is the third case: with the old rule -- ask
    /// again as soon as anything came back -- it is true, and one
    /// payload landing puts another whole batch on the bulk lane. That
    /// is what made a replica behind by a few hundred commands ask
    /// thousands of times a second, fill the lane its answers travel
    /// on, and never catch up.
    #[test]
    fn a_replica_asks_again_when_its_batch_is_answered_in_full_and_not_before() {
        let t0 = std::time::Instant::now();
        // Nothing asked yet: ask.
        assert!(ask_for_payloads_now(None, 0, t0));
        // A batch of eight asked, none answered, no time passed.
        let asked = Some((t0, 0, 8));
        assert!(!ask_for_payloads_now(asked, 0, t0));
        // Seven of eight back. Still outstanding -- this is the case
        // the old rule got wrong.
        assert!(!ask_for_payloads_now(asked, 7, t0));
        // All eight back: ask again at once, without waiting out the
        // interval. This is what lets a replica behind by more than one
        // batch catch up at a batch per round trip.
        assert!(ask_for_payloads_now(asked, 8, t0));
        // A late or duplicated answer -- to an ask the retry floor
        // repeated, or one a later ask superseded -- never reaches the
        // count: the machine counts only answers to the outstanding ask,
        // each once (coord-consensus's
        // `a_late_or_duplicated_payload_answer_does_not_count_as_answering_the_ask`).
        // So the count cannot pass the ask's own size, and a duplicate of
        // the seventh answer is still seven.
        assert!(!ask_for_payloads_now(asked, 7, t0));
        // Not answered, but the interval has passed: ask again, so a
        // batch the peer cannot answer does not stop the asking.
        assert!(ask_for_payloads_now(
            asked,
            3,
            t0 + PAYLOAD_RETRY + std::time::Duration::from_millis(1)
        ));
    }

    /// One ask is for at most the protocol's bound, and for no more
    /// than is missing -- so a replica missing three does not wait for
    /// eight answers that are not coming.
    #[test]
    fn an_ask_is_for_what_is_missing_up_to_the_protocol_bound() {
        assert_eq!(payload_batch_size(0), 0);
        assert_eq!(payload_batch_size(3), 3);
        assert_eq!(
            payload_batch_size(coord_consensus::messages::MAX_PAYLOAD_TRANSFER),
            coord_consensus::messages::MAX_PAYLOAD_TRANSFER as u64
        );
        assert_eq!(
            payload_batch_size(10_000),
            coord_consensus::messages::MAX_PAYLOAD_TRANSFER as u64
        );
        // And a short ask is answered in full by its own size, not by
        // the bound: a replica missing three that gets three asks again
        // at once.
        let t0 = std::time::Instant::now();
        assert!(ask_for_payloads_now(
            Some((t0, 0, payload_batch_size(3))),
            3,
            t0
        ));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn b64url(bytes: &[u8]) -> String {
        const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(A[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
                }
            }
        }
        out
    }

    fn membership() -> coord_membership::membership::Membership {
        let manifest = GenesisManifest {
            cluster: hex(&[0x11; 16]),
            domain: hex(&[0x22; 16]),
            epoch: 1,
            voters: (1u8..=3)
                .map(|n| VoterSeed {
                    node: hex(&[n; 16]),
                    incarnation: u64::from(n) * 7,
                    public_key: b64url(&[n; 32]),
                })
                .collect(),
            issuer_roots: vec![b64url(&[0xca; 8])],
            wif_rules: vec![serde_json::json!({ "issuer": "test" })],
            admin: hex(&[0xa; 16]),
            protocol_version: 1,
        };
        coord_membership::membership::Membership::from_genesis(&manifest).expect("membership")
    }

    fn peer(replica: u8, incarnation: ReplicaIncarnation) -> coord_core::effect::PeerId {
        coord_core::effect::PeerId {
            replica: ReplicaId([replica; 16]),
            incarnation,
        }
    }

    /// A machine leaves the generation open, and the committed
    /// configuration fills it in.
    ///
    /// This is the same rule a submission's fan-out follows, and it has
    /// to be: a sender does not know which incarnation of another node
    /// is current, so a runtime that passed the open value through
    /// would address a generation that exists nowhere and reach nobody.
    #[test]
    fn an_open_generation_is_resolved_to_the_committed_one() {
        let membership = membership();

        assert_eq!(
            addressed(&membership, peer(2, ReplicaIncarnation::ZERO)),
            Some(peer(2, ReplicaIncarnation::new(14).unwrap())),
            "voter 2 is committed at incarnation 14"
        );
    }

    /// And a generation a machine did name is held to the committed one
    /// rather than believed.
    ///
    /// A protocol frame reaching a generation the cluster has replaced
    /// is a frame from a node that is no longer this node, so it is not
    /// sent at all -- the alternative is a vote counted for a replica
    /// that the configuration has moved on from.
    #[test]
    fn a_generation_the_configuration_replaced_is_not_addressed() {
        let membership = membership();

        assert_eq!(
            addressed(&membership, peer(2, ReplicaIncarnation::new(13).unwrap())),
            None,
            "voter 2 is committed at 14, not 13"
        );
        assert_eq!(
            addressed(&membership, peer(9, ReplicaIncarnation::ZERO)),
            None,
            "replica 9 is not a committed voter of this domain"
        );
    }

    /// The stages a recorder reports as instrumented follow what runs
    /// in the process. An observer journals and materializes its own
    /// storage, so both stages apply to it, but only a voter's node
    /// records them; without a voter they are not instrumented, not
    /// observed zeroes.
    #[test]
    fn journal_and_materialization_are_instrumented_only_where_a_voter_records_them() {
        use coord_daemon::metrics::{Measure, Stage, Unavailable};
        use coord_daemon::role::RoleSet;

        let reading = |voting: bool, roles: &str, stage: Stage| {
            super::recorder(voting)
                .snapshot_stages(&RoleSet::parse(roles).expect("roles"))
                .into_iter()
                .find(|r| r.stage == stage)
                .expect("every stage is reported")
                .metrics
        };
        for stage in [Stage::Journal, Stage::Materialization] {
            assert!(
                matches!(
                    reading(true, "voter-frontend-observer", stage),
                    Measure::Observed(_)
                ),
                "{stage:?} is recorded by a voter"
            );
            for roles in ["frontend-observer", "observer"] {
                assert_eq!(
                    reading(false, roles, stage),
                    Measure::Unavailable(Unavailable::NotInstrumented),
                    "{stage:?} reported as observed with no voter to record it ({roles})"
                );
            }
        }
        // Admission is the frontend's, and is recorded wherever one runs.
        assert!(matches!(
            reading(false, "frontend-observer", Stage::Admission),
            Measure::Observed(_)
        ));
    }
}
