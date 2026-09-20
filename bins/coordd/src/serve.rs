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

use coord_authn::ClockHealth;
use coord_collector::ingress::is_collector;
use coord_collector::wire::{
    KIND_EVIDENCE, KIND_RELEASE, KIND_SUBMIT, decode_evidence, decode_release,
};
use coord_collector::{Admission, AdmissionLimits, Collector, CollectorConfig, Dispatcher};
use coord_core::event::PeerProvenance;
use coord_daemon::mailbox::LocalRoute;
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
        }
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
    /// The other voters, where this process votes. `None` for a process
    /// that does not, and for a domain with nobody else in it.
    plane: Option<PeerPlane>,
    /// The other voters, as somewhere to submit to. Empty for a domain
    /// with nobody else in it, and for a process whose frontend holds no
    /// collector credential to submit with.
    links: CollectorLinks,
    budgets: Budgets,
}

impl<P: Persistence> Domain<P> {
    /// Compose `frontend` over `backing`.
    pub fn new(frontend: Frontend, backing: Backing<P>, budgets: Budgets) -> Self {
        Domain {
            backing,
            frontend,
            plane: None,
            links: CollectorLinks::new(Vec::new()),
            budgets,
        }
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
                    // Stderr, not stdout: this happens after the startup
                    // report, and a process must not die because whoever
                    // read its startup report has stopped reading.
                    eprintln!(
                        "peers connected={} of {} attempts={}",
                        plane.reachable(),
                        plane.peers.len(),
                        plane.dialled.0
                    );
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
            let arrived = tokio::select! {
                biased;
                // Voter work that is still outstanding pre-empts waiting
                // on anything. This branch is taken only when the last
                // turn actually did something, so a voter that cannot
                // progress waits rather than spins.
                () = std::future::ready(()), if progressed => continue,
                peer = next_peer_event(self.plane.as_mut()) => peer.map(Arrived::Peer),
                event = transport.next_event() => event.map(Arrived::Api),
            };
            match arrived {
                Some(Arrived::Api(event)) => self.on_transport(transport, event, &clock).await,
                Some(Arrived::Peer(event)) => self.on_peer_plane(transport, event),
                None => return,
            }
        }
    }

    /// Give the voter its turn: the local submissions it is owed, then
    /// whatever applying those produced.
    ///
    /// Returns whether anything happened, which is what tells the loop
    /// to come back rather than wait.
    async fn turn(&mut self, api: &Transport) -> Result<bool, DriveError> {
        let Backing::Voting(voter) = &mut self.backing else {
            return Ok(false);
        };
        let (mut out, refused) = voter.serve_local(self.budgets.local_per_turn)?;
        out.absorb(voter.execute()?);
        let provenance = voter.provenance();
        let did = !refused.is_empty() || !out.is_empty();
        self.frontend.counts.refused += refused.len() as u64;
        self.carry(api, out, provenance);
        Ok(did)
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
                Some(Err(_)) | None => self.frontend.counts.unavailable += 1,
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
    /// invocation `frame` names, if it holds one for this exact command
    /// and this caller may have it.
    fn retained(
        &mut self,
        health: &ClockHealth,
        connection: u64,
        frame: &Frame,
    ) -> Option<Vec<u8>> {
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
        let command = coord_types::CommandId::derive(&key, &request.logical().ok()?).ok()?;
        let record = {
            let gated = self.backing.applier().store().reader().snapshot().ok()?;
            coord_storage::retry::lookup(gated.view(), &key).ok()?
        }?;
        // Only for the command this frame is of: a retry key bound to
        // another payload is a conflict, which is replicated execution's
        // to decide at that command's own position, not this node's to
        // answer from a record that belongs to something else.
        if record.command_id != command {
            return None;
        }
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
        match self.frontend.frontend.deliver(delivery, &policy) {
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
                ..
            } => {
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
                let ingress = self.frontend.frontend.on_frame(
                    &health,
                    connection.0,
                    &frame,
                    self.backing.applier().hub(),
                    &policy,
                );
                let decided = step(ingress, retry_key);
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
            // periodic reconnection belongs with the timer loop.
            TransportEvent::Closed { .. } => {}
            TransportEvent::Connected { .. } => {}
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
            Ok(Err(_)) => self.frontend.counts.refused += 1,
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
            Step::Watch { .. } => {
                // The watch's own stream, held for its lifetime. Pumping
                // it is the next piece of this loop.
                self.frontend.counts.watches += 1;
                drop(responder);
            }
            Step::Close { code, reason } => {
                drop(responder);
                transport.disconnect(connection, code, reason);
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
}
