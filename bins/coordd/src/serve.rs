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
use coord_session::{BindingConfig, BoundFrontend, StorePolicySource};
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
    budgets: Budgets,
}

impl<P: Persistence> Domain<P> {
    /// Compose `frontend` over `backing`.
    pub const fn new(frontend: Frontend, backing: Backing<P>, budgets: Budgets) -> Self {
        Domain {
            backing,
            frontend,
            budgets,
        }
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
        loop {
            let progressed = match self.turn().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("this voter cannot make its transitions durable: {e}");
                    return;
                }
            };
            let event = tokio::select! {
                biased;
                // Voter work that is still outstanding pre-empts waiting
                // on a caller. This branch is taken only when the last
                // turn actually did something, so a voter that cannot
                // progress waits rather than spins.
                () = std::future::ready(()), if progressed => continue,
                event = transport.next_event() => event,
            };
            let Some(event) = event else { return };
            self.on_transport(transport, event, &clock).await;
        }
    }

    /// Give the voter its turn: the local submissions it is owed, then
    /// whatever applying those produced.
    ///
    /// Returns whether anything happened, which is what tells the loop
    /// to come back rather than wait.
    async fn turn(&mut self) -> Result<bool, DriveError> {
        let Backing::Voting(voter) = &mut self.backing else {
            return Ok(false);
        };
        let (mut out, refused) = voter.serve_local(self.budgets.local_per_turn)?;
        out.absorb(voter.execute()?);
        let provenance = voter.provenance();
        let did = !refused.is_empty() || !out.is_empty();
        self.frontend.counts.refused += refused.len() as u64;
        self.carry(out, provenance);
        Ok(did)
    }

    /// Carry out what a voter's round asked for.
    ///
    /// Its evidence and releases go to the collector under this voter's
    /// committed identity, through the same calls a peer's frame makes:
    /// there is no local acknowledgement, and one co-located voter is
    /// one voter's worth of evidence.
    fn carry(&mut self, out: coord_daemon::Outbound, provenance: PeerProvenance) {
        for frame in out.frontend {
            self.on_collector_frame(provenance, &frame);
        }
        // The peer plane is not wired in this build. Frames for other
        // voters, and the timers, views and entropy a machine asked for,
        // are counted rather than dropped silently, so what this process
        // cannot yet do is visible in its own report.
        self.frontend.counts.unserved += (out.peer.len()
            + out.arm.len()
            + out.cancel.len()
            + out.views.len()
            + out.entropy.len()) as u64;
    }

    /// One frame a voter addressed to the trusted collector.
    fn on_collector_frame(&mut self, provenance: PeerProvenance, bytes: &[u8]) {
        let Ok(frame) = one_frame(bytes) else {
            self.frontend.counts.unserved += 1;
            return;
        };
        let delivered = match frame.kind {
            KIND_EVIDENCE => decode_evidence(&frame).ok().and_then(|message| {
                self.frontend
                    .frontend
                    .dispatcher_mut()
                    .on_evidence(provenance, message)
                    .ok()
                    .flatten()
            }),
            KIND_RELEASE => decode_release(&frame).ok().and_then(|released| {
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

    /// Answer the caller a delivery belongs to, gated against a fresh
    /// barrier.
    fn answer(&mut self, delivery: coord_collector::Delivery) {
        let policy = StorePolicySource {
            store: self.backing.applier().store(),
            budget: ViewBudget::default(),
        };
        let delivery = self.frontend.frontend.deliver(delivery, &policy);
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
                    self.on_remote_submission(identity.role, &frame);
                    drop(responder);
                    return;
                }
                let health = ClockHealth::healthy(clock(), CLOCK_UNCERTAINTY_SECONDS);
                let retry_key = invocation_of(&frame);
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
            TransportEvent::PeerFrame {
                provenance,
                kind,
                version,
                payload,
                ..
            } => match &mut self.backing {
                Backing::Voting(voter) => {
                    let frame = coord_types::wire_v1::Frame {
                        kind,
                        version,
                        payload,
                    };
                    let bytes = match coord_types::wire_v1::encode_frame(
                        frame.kind,
                        frame.version,
                        &frame.payload,
                    ) {
                        Ok(b) => b,
                        Err(_) => {
                            self.frontend.counts.unserved += 1;
                            return;
                        }
                    };
                    match voter.on_peer(provenance, bytes) {
                        Ok(out) => {
                            let provenance = voter.provenance();
                            self.carry(out, provenance);
                        }
                        Err(e) => {
                            eprintln!("this voter cannot carry out a peer's frame: {e}");
                        }
                    }
                }
                Backing::Serving(_) => self.frontend.counts.unserved += 1,
            },
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
            TransportEvent::ApiDelivery { .. } => {
                self.frontend.counts.unserved += 1;
            }
            TransportEvent::Connected { .. } => {}
        }
    }

    /// A submission a collector in another process made.
    ///
    /// The same door the local route uses: the role is the one the
    /// transport bound to that peer's certificate, and the frame is
    /// admitted or refused by the same boundary.
    fn on_remote_submission(&mut self, role: coord_types::wire_v1::PeerRole, frame: &Frame) {
        let Backing::Voting(voter) = &mut self.backing else {
            self.frontend.counts.unserved += 1;
            return;
        };
        match voter.on_submission(role, frame) {
            Ok(Ok(out)) => {
                let provenance = voter.provenance();
                self.carry(out, provenance);
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
    let identity = coord_daemon::load_identity(
        &config.identity,
        membership.cluster(),
        membership.domain(),
        coord_transport::role_lanes(coord_types::wire_v1::PeerRole::Frontend)
            .iter()
            .map(|lane| lane.capability())
            .collect(),
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
