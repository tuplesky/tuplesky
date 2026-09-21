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
use coord_collector::{Admission, AdmissionLimits, Collector, CollectorConfig, Dispatcher};
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
use coord_types::RetryKey;
use coord_types::wire_v1::{Frame, MessageV1, decode};

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
    /// The reachable count last reported on stderr, so the report is
    /// repeated when it changes and not on every connection event.
    reported: Option<usize>,
}

impl PeerPlane {
    /// The plane over `transport`, for the voters in `peers`.
    pub fn new(
        transport: Transport,
        domain: coord_types::ids::DomainId,
        me: coord_types::ids::ReplicaIncarnation,
        peers: Vec<crate::peers::Peer>,
    ) -> Self {
        PeerPlane {
            transport,
            domain,
            me,
            peers,
            dialled: (0, 0),
            reported: None,
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
        let reachable = self.reachable();
        if self.reported == Some(reachable) {
            return;
        }
        self.reported = Some(reachable);
        eprintln!(
            "peers connected={} of {} attempts={}",
            reachable,
            self.peers.len(),
            self.dialled.0
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

    /// Whether a connection is holding this peer's control lane.
    fn holds(&self, peer: &crate::peers::Peer) -> bool {
        self.transport.linked(
            peer.replica,
            peer.incarnation,
            coord_transport::Lane::Control,
        )
    }

    /// Try every voter not currently connected.
    ///
    /// The identity expected on the other end is the committed one, so a
    /// certificate that is not this domain's voter at its committed
    /// incarnation fails the handshake rather than becoming a peer. That
    /// is the whole of what an address is trusted for.
    pub async fn dial_missing(&mut self) {
        let missing: Vec<crate::peers::Peer> = self
            .peers
            .iter()
            .filter(|p| !self.holds(p))
            .cloned()
            .collect();
        self.dialled.0 += missing.len() as u64;
        // A voter's lanes are control and bulk; a unary lane is a
        // collector's or a client's. Protocol traffic between replicas
        // is control traffic, which is also why a bulk checkpoint
        // transfer cannot delay a vote.
        let transport = &self.transport;
        let me = self.me;
        let reached = concurrently(
            missing
                .iter()
                .map(|peer| {
                    boxed(dial(
                        transport,
                        peer,
                        Some(me),
                        coord_types::wire_v1::PeerRole::Voter,
                        coord_transport::Lane::Control,
                    ))
                })
                .collect(),
        )
        .await;
        for (peer, outcome) in missing.iter().zip(reached) {
            match outcome {
                Ok(_) => self.dialled.1 += 1,
                // Not a failure of anything: a voter this process cannot
                // reach right now contributes no evidence and nothing
                // else. It is reported because an operator wants to see
                // a cluster that cannot form, and retried on the next
                // occasion.
                //
                // Unless the link is held anyway. Both ends dial, one of
                // the two connections loses the collision and is closed,
                // and whichever end dialled it sees an error for a link
                // it now has: reporting that as unreachable would make
                // an ordinary mesh look broken.
                Err(e) => {
                    if !self.holds(peer) {
                        eprintln!(
                            "cannot reach voter {} on the peer plane: {e:?}",
                            hex4(&peer.replica)
                        );
                    }
                }
            }
        }
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
        self.transport.send(
            coord_transport::Destination::Replica {
                replica: to.replica,
                incarnation: to.incarnation,
                lane: coord_transport::Lane::Control,
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
}

impl CollectorLinks {
    /// Links to `peers`, none of them held yet.
    pub fn new(peers: Vec<crate::peers::Peer>) -> Self {
        CollectorLinks {
            peers,
            dialled: (0, 0),
        }
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

    /// How many voters this process must reach to submit to all of them.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Try every voter not currently linked.
    ///
    /// A voter this process cannot reach contributes nothing to a
    /// submission and fails nothing: the quorum rule decides what that
    /// costs. So a failed dial is reported and retried, never an error.
    pub async fn dial_missing(&mut self, api: &Transport) {
        let missing: Vec<crate::peers::Peer> = self
            .peers
            .iter()
            .filter(|p| !Self::holds(api, p))
            .cloned()
            .collect();
        self.dialled.0 += missing.len() as u64;
        // A submission is unary work, on the lane a collector is
        // granted: a bulk transfer or a watch backlog cannot delay it,
        // and a voter's own control traffic is a different lane on a
        // different plane.
        let reached = concurrently(
            missing
                .iter()
                .map(|peer| {
                    boxed(dial(
                        api,
                        peer,
                        None,
                        coord_types::wire_v1::PeerRole::Frontend,
                        coord_transport::Lane::Unary,
                    ))
                })
                .collect(),
        )
        .await;
        for (peer, outcome) in missing.iter().zip(reached) {
            match outcome {
                Ok(_) => self.dialled.1 += 1,
                Err(e) => {
                    if !Self::holds(api, peer) {
                        eprintln!(
                            "cannot reach voter {} to submit to it: {e:?}",
                            hex4(&peer.replica)
                        );
                    }
                }
            }
        }
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
    /// When this node last asked for a payload it lacks. A time and
    /// not a turn count: a domain with nothing else happening takes no
    /// turns, and that is exactly when the asking has to go on.
    asked: Option<std::time::Instant>,
    budgets: Budgets,
    /// This run's stage accounting (task-61), shared with the voter's
    /// node so the journal and materialization points record into the
    /// same cells the snapshot reads.
    recorder: std::sync::Arc<coord_daemon::metrics::Recorder>,
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
/// it passes. Lease expiry is the first component; the collector's
/// re-offers and the expiry of held evidence are meant to register here
/// in the same way rather than add arms of their own to the loop.
pub trait Deadline {
    /// The earliest instant this component has work due, if it has any.
    ///
    /// It must move past a deadline once the turn that deadline wakes has
    /// done the work due at it; a deadline that stays in the past would
    /// wake the loop again at once, for ever.
    fn next_deadline(&self) -> Option<std::time::Instant>;
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
        // Reach the other voters before serving. A submission that
        // arrived first would still be correct -- an unreachable voter
        // contributes nothing and the quorum rule decides -- but it
        // would need the peers to answer it, and they are not there yet.
        //
        // Both planes at once: an absent voter costs a handshake
        // timeout on each, and waiting out one plane's before starting
        // the other's would make a process that is merely waiting look
        // like one that cannot start.
        let Domain { plane, links, .. } = self;
        tokio::join!(
            async {
                if let Some(plane) = plane {
                    plane.dial_missing().await;
                    plane.report();
                }
            },
            // The links this domain's collector submits over, which go
            // to a different listener: the voters answer a submission
            // with evidence, and evidence is the collector's to count,
            // not a peer's to process.
            async {
                links.dial_missing(transport).await;
                if links.len() > 0 {
                    eprintln!(
                        "voters submittable={} of {} attempts={}",
                        links.reachable(transport),
                        links.len(),
                        links.dialled.0
                    );
                }
            },
        );
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
                Backing::Voting(v) => v.awaiting().is_some() || v.wants_payloads(),
                Backing::Serving(_) => false,
            };
            let arrived = tokio::select! {
                biased;
                // Voter work that is still outstanding pre-empts waiting
                // on anything. This branch is taken only when the last
                // turn actually did something, so a voter that cannot
                // progress waits rather than spins.
                () = std::future::ready(()), if progressed => continue,
                () = tokio::time::sleep(PAYLOAD_RETRY), if waiting_for_content => continue,
                peer = next_peer_event(self.plane.as_mut()) => peer.map(Arrived::Peer),
                event = transport.next_event() => event.map(Arrived::Api),
                () = sleep, if wake.is_some() => continue,
            };
            match arrived {
                Some(Arrived::Api(event)) => self.on_transport(transport, event, &clock).await,
                Some(Arrived::Peer(event)) => self.on_peer_plane(transport, event),
                None => return,
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
    /// does a turn run it; a follower's driver has nothing due. Further
    /// components are folded into the same minimum.
    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        let expiry = match &self.backing {
            Backing::Voting(voter) if voter.leads() => {
                self.expiry.as_ref().and_then(Deadline::next_deadline)
            }
            _ => None,
        };
        [expiry].into_iter().flatten().min()
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
        if voter.awaiting().is_some() || voter.wants_payloads() {
            let now = std::time::Instant::now();
            if self
                .asked
                .is_none_or(|last| now.duration_since(last) >= PAYLOAD_RETRY)
            {
                self.asked = Some(now);
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
        // operator with a counter.
        for why in &refused {
            eprintln!("this voter refused a submission: {why:?}");
        }
        self.frontend.counts.refused += refused.len() as u64;
        // What the protocol machine itself refused. Drained every turn
        // rather than left to grow: a refusal nobody reads is a list
        // that lives as long as the process and a reason an operator
        // never sees.
        for why in voter.take_rejections() {
            eprintln!("this voter's machine refused: {why}");
        }
        self.carry(api, out, provenance);
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
                Some(Ok(())) => self.frontend.counts.queued_remote += 1,
                // No route right now. The voter contributes nothing
                // through this process until there is one; the quorum
                // rule decides what that costs.
                //
                // Said, not only counted. A vote this node produced and
                // could not send is the difference between a quorum
                // that forms and one that does not, and it is invisible
                // from every other node.
                Some(Err(e)) => {
                    eprintln!("this voter could not send to {}: {e:?}", hex4(&to.replica));
                    self.frontend.counts.unavailable += 1;
                }
                None => {
                    eprintln!("this voter has no peer plane to send on");
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
    /// Where this voter admitted the command locally -- and where it
    /// cannot say, which is what an unrecognised or forgotten command
    /// looks like -- the frame goes to the collector in this process.
    /// The bytes are the same bytes either way: what changes is which
    /// process counts them, and the identity they are counted under is
    /// this voter's committed one in both cases.
    fn hand_to_collector(&mut self, api: &Transport, provenance: PeerProvenance, bytes: &[u8]) {
        let Ok(frame) = one_frame(bytes) else {
            self.frontend.counts.unserved += 1;
            return;
        };
        let owed = self.owed_to(&frame);
        if let Some(coord_daemon::voter::Origin::Connection(id)) = owed {
            let sent = api.send(
                coord_transport::Destination::Connection(coord_transport::ConnectionId(id)),
                self.frontend.membership.domain(),
                bytes.to_vec(),
            );
            match sent {
                Ok(()) => self.frontend.counts.returned += 1,
                Err(_) => self.frontend.counts.unreturnable += 1,
            }
            return;
        }
        self.on_frame_from_voter(provenance, &frame);
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
        let command = match frame.kind {
            KIND_EVIDENCE => coord_consensus::ProtocolMessage::decode(&frame.payload)
                .ok()?
                .command()?,
            KIND_RELEASE => decode_release(frame).ok()?.established().command(),
            _ => return None,
        };
        voter.origin_of(&command)
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
                let ingress = self.frontend.frontend.on_frame(
                    &health,
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
            // meet. Re-dialling here would answer that with a storm;
            // periodic reconnection belongs with the timer loop. What
            // does happen here is the report: the count of voters held
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
            Ok(Err(why)) => {
                eprintln!("this voter refused a collector's submission: {why:?}");
                self.frontend.counts.refused += 1;
            }
            Err(e) => eprintln!("this voter cannot carry out a submission: {e}"),
        }
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
                let out = fanout::dispatch(
                    &self.frontend.membership,
                    transport,
                    self.frontend
                        .local
                        .as_ref()
                        .map(|l| l as &dyn fanout::LocalIngress),
                    &plan,
                );
                let counts = &mut self.frontend.counts;
                counts.queued_local += out.queued_local() as u64;
                counts.queued_remote += out.queued_remote() as u64;
                counts.not_a_voter += out.not_a_committed_voter() as u64;
                counts.saturated += out.saturated() as u64;
                counts.unavailable += out.unavailable() as u64;
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
    transport: &Transport,
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

/// One future, type-erased so a set of them can be driven together.
fn boxed<'a, T: 'a>(
    future: impl core::future::Future<Output = T> + 'a,
) -> core::pin::Pin<Box<dyn core::future::Future<Output = T> + 'a>> {
    Box::pin(future)
}

/// Drive every future to completion on this task, concurrently, and
/// answer in the order they were given.
///
/// Dialling is almost entirely waiting, and a voter that is not there
/// costs a whole handshake timeout. One at a time, the wait would be
/// the sum of every absent voter's -- and a process that has not
/// started serving yet looks, from outside, exactly like one that
/// cannot.
///
/// This is one task, not a task each: the futures borrow the endpoint
/// they dial through, and nothing here outlives the call.
async fn concurrently<T>(
    work: Vec<core::pin::Pin<Box<dyn core::future::Future<Output = T> + '_>>>,
) -> Vec<T> {
    let mut work: Vec<Option<_>> = work.into_iter().map(Some).collect();
    let mut done: Vec<Option<T>> = work.iter().map(|_| None).collect();
    core::future::poll_fn(move |cx| {
        let mut waiting = false;
        for (slot, answer) in work.iter_mut().zip(done.iter_mut()) {
            let Some(future) = slot else { continue };
            match future.as_mut().poll(cx) {
                core::task::Poll::Ready(value) => {
                    *answer = Some(value);
                    *slot = None;
                }
                core::task::Poll::Pending => waiting = true,
            }
        }
        if waiting {
            return core::task::Poll::Pending;
        }
        core::task::Poll::Ready(
            done.iter_mut()
                .map(|answer| answer.take().expect("every future finished"))
                .collect(),
        )
    })
    .await
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
    let limits = coord_transport::Limits::default();
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

    use super::addressed;

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
