//! Putting a submission in front of every voter at once (design Sections
//! 3.2, 4.3).
//!
//! The collector plans the fan-out: it produces the exact `Submit` frame
//! and the voters it must reach, and holds no sockets. The daemon does
//! the delivering, and in doing so answers the two questions the plan
//! leaves open -- which incarnation of each replica is allowed to receive
//! it, and by which route it gets there.
//!
//! Both answers come from the committed membership, never from the plan
//! and never from the caller. A replica identity alone does not say which
//! generation of that node may vote; the committed configuration does,
//! and binding the two here is what stops a frame going to a stale
//! incarnation that would then be counted as a voter.
//!
//! # A voter in this process is still a voter
//!
//! A process that runs the frontend and one of the domain's voters has
//! no reason to dial itself: the frame can go straight into that voter's
//! own bounded ingress. That is a transport optimization and nothing
//! more. The local route skips serialization and the network; it does
//! not skip what the network was there to establish, because the
//! committed configuration is consulted for a local target exactly as it
//! is for a remote one, and the frame enters the voter's ordinary
//! ingress rather than being turned into a decision here.
//!
//! The local route is a *capability*, not an address. A process holds
//! one because it is running that voter, and a request naming a replica
//! id cannot conjure one -- which is what keeps a frontend-only or
//! observer-only process from acquiring a voter route it has no voter
//! for.
//!
//! # What this module reports, and what it does not
//!
//! Queueing is not voting. Every count below says a destination's
//! ingress accepted responsibility for the frame: nothing about whether
//! a voter processed it, whether anything became durable, and certainly
//! not whether a command was applied. Evidence is counted by voter
//! identity, elsewhere, by the collector; a destination this process
//! cannot reach right now simply contributes nothing, and the submission
//! is not failed for it.
//!
//! The reasons a destination got nothing are kept apart on purpose. A
//! plan naming a replica the configuration does not know is a disagreement
//! between the plan and the configuration; a full ingress is a live
//! process under load; an unreachable peer is a network fact. Collapsing
//! the three would make each one look like the others.

use coord_collector::FanOut;
use coord_membership::membership::Membership;
use coord_transport::{Lane, SendError, Transport};
use coord_types::ids::{DomainId, ReplicaId, ReplicaIncarnation};

/// How a frame reached a destination's ingress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Handed to a voter running in this process, through its own
    /// bounded ingress.
    Local,
    /// Given to the transport for a voter in another process.
    Remote,
}

/// One destination whose ingress took the frame.
///
/// This records an ingress accepting responsibility. It is not evidence,
/// not durability and not application: the voter has not necessarily run
/// yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Queued {
    /// The voter.
    pub replica: ReplicaId,
    /// The incarnation the committed configuration names for it.
    pub incarnation: ReplicaIncarnation,
    /// Which route it took.
    pub route: Route,
}

/// Why a planned destination received nothing.
///
/// Four classes, and they are four because they want four different
/// answers. Saturation is a live voter under load and wants another
/// offer shortly. Unavailability is a link that is down and wants
/// another offer too, but tells an operator something different.
/// A plan naming a non-voter is a disagreement with the configuration,
/// which repeating cannot settle. An envelope that cannot fit the route
/// is permanent for this frame. Collapsing any of them into the others
/// would put one on the wrong schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotQueued {
    /// The committed configuration does not name this replica as a voter
    /// of this domain. The frame is not offered to it at all, by either
    /// route, and its presence in a plan is worth reporting: it means the
    /// plan and the configuration disagree.
    NotACommittedVoter,
    /// The destination's ingress is full. The voter exists and is
    /// addressable; there was no room to accept the frame right now.
    ///
    /// Both routes reach this the same way. A local ingress says so
    /// directly; a remote one says `QueueFull` or `TooManyGroups`,
    /// which are the transport's two ways of saying the same thing --
    /// this destination's share of a bounded resource is spent. They
    /// are normalised here so that what a caller's submission meets is
    /// classified by what it *is* rather than by which side of the
    /// process boundary it happened on.
    Saturated,
    /// The frame cannot reach this destination as it stands: it does
    /// not fit what the route may carry. Permanent for this envelope,
    /// so it is not on the congestion schedule.
    Undeliverable(SendError),
    /// There is no route to the destination at the moment.
    Unavailable(SendError),
}

/// Where a planned fan-out actually went.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Dispatched {
    /// Destinations whose ingress took the frame, each tagged with the
    /// route it took, in the order the plan named them.
    pub queued: Vec<Queued>,
    /// Destinations that got nothing, each with the reason, in the order
    /// the plan named them.
    pub rejected: Vec<(ReplicaId, NotQueued)>,
}

impl Dispatched {
    /// What this attempt achieved, in the vocabulary the collector owes
    /// the obligation in.
    ///
    /// The translation is one way on purpose. This module knows about
    /// routes, incarnations and transports; the collector knows about
    /// destinations and whether they still owe an enqueue. Handing it
    /// `SendError` would make the collector's contract depend on which
    /// transport is underneath it.
    pub fn offered(&self, command: coord_types::CommandId) -> coord_collector::Offered {
        let mut outcomes: Vec<(ReplicaId, coord_collector::OfferOutcome)> = self
            .queued
            .iter()
            .map(|q| (q.replica, coord_collector::OfferOutcome::Queued))
            .collect();
        outcomes.extend(self.rejected.iter().map(|(replica, why)| {
            let outcome = match why {
                NotQueued::NotACommittedVoter => coord_collector::OfferOutcome::NotACommittedVoter,
                NotQueued::Saturated => coord_collector::OfferOutcome::Saturated,
                NotQueued::Undeliverable(_) => coord_collector::OfferOutcome::Undeliverable,
                NotQueued::Unavailable(_) => coord_collector::OfferOutcome::Unreachable,
            };
            (*replica, outcome)
        }));
        coord_collector::Offered { command, outcomes }
    }

    /// Frames taken by a voter's ingress in this process.
    pub fn queued_local(&self) -> usize {
        self.queued
            .iter()
            .filter(|q| q.route == Route::Local)
            .count()
    }

    /// Frames the transport admitted for a voter elsewhere.
    pub fn queued_remote(&self) -> usize {
        self.queued
            .iter()
            .filter(|q| q.route == Route::Remote)
            .count()
    }

    /// Planned targets the committed configuration does not name as
    /// voters of this domain.
    pub fn not_a_committed_voter(&self) -> usize {
        self.count(&NotQueued::NotACommittedVoter)
    }

    /// Destinations whose ingress was full.
    pub fn saturated(&self) -> usize {
        self.count(&NotQueued::Saturated)
    }

    /// Destinations there was no route to.
    pub fn unavailable(&self) -> usize {
        self.rejected
            .iter()
            .filter(|(_, why)| matches!(why, NotQueued::Unavailable(_)))
            .count()
    }

    /// Destinations this envelope can never reach as it stands.
    pub fn undeliverable(&self) -> usize {
        self.rejected
            .iter()
            .filter(|(_, why)| matches!(why, NotQueued::Undeliverable(_)))
            .count()
    }

    fn count(&self, reason: &NotQueued) -> usize {
        self.rejected
            .iter()
            .filter(|(_, why)| why == reason)
            .count()
    }
}

/// Somewhere to put one frame for several replicas at once.
///
/// [`Transport`] is the implementation; the trait exists so the decision
/// this module makes -- which incarnation, and whether to offer at all --
/// can be tested without a live endpoint.
pub trait PeerFanOut {
    /// Queue `frame` for each replica independently, one result each.
    fn fan_out(
        &self,
        targets: &[(ReplicaId, ReplicaIncarnation)],
        group: DomainId,
        frame: &[u8],
    ) -> Vec<Result<(), SendError>>;
}

impl PeerFanOut for Transport {
    fn fan_out(
        &self,
        targets: &[(ReplicaId, ReplicaIncarnation)],
        group: DomainId,
        frame: &[u8],
    ) -> Vec<Result<(), SendError>> {
        // A submission is unary work: it takes the unary lane, so a bulk
        // transfer or a watch backlog cannot delay it.
        Transport::fan_out(self, targets, Lane::Unary, group, frame)
    }
}

/// The ingress of a voter running in this process.
///
/// Held by a runtime that is actually running that voter, and bound to
/// the identity that voter runs under. Offering to it is the local
/// equivalent of admitting a frame to the transport: bounded, immediate
/// and non-blocking, saying only that the voter now owes the frame a
/// turn.
pub trait LocalIngress {
    /// The replica this ingress belongs to.
    fn replica(&self) -> ReplicaId;

    /// The incarnation that voter instance is running as.
    ///
    /// Checked against the committed configuration before anything is
    /// offered: a runtime whose incarnation has been superseded is not
    /// this domain's voter any more, whatever it is running.
    fn incarnation(&self) -> ReplicaIncarnation;

    /// The domain that voter votes in.
    fn domain(&self) -> DomainId;

    /// Offer one frame, taking no longer than a bounded push.
    ///
    /// `Err` means the ingress is full right now. It is not a failure of
    /// the submission and it must not stop any other destination from
    /// being offered the frame.
    fn offer(&self, frame: &[u8]) -> Result<(), Saturated>;
}

/// An ingress with no room for another frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Saturated;

/// Which class a transport refusal belongs to.
///
/// The transport reports what it could not do; this says what kind of
/// problem that is. `QueueFull` and `TooManyGroups` are a destination
/// at a bound and will pass; `TooLarge` is this frame against this
/// route and will not; everything else is a link that is not there
/// right now.
fn classify(e: SendError) -> NotQueued {
    match e {
        SendError::QueueFull { .. } | SendError::TooManyGroups { .. } => NotQueued::Saturated,
        SendError::TooLarge { .. } => NotQueued::Undeliverable(e),
        _ => NotQueued::Unavailable(e),
    }
}

/// What `dispatch` decided to do with one planned target.
enum Destination {
    Local(ReplicaIncarnation),
    Remote(ReplicaIncarnation),
    Rejected(NotQueued),
}

/// Offer `plan` to the voters the committed configuration names.
///
/// `local` is this process's own voter ingress, where it has one. A
/// target is delivered locally only when that ingress belongs to the
/// same replica, at the incarnation the committed configuration names,
/// in the domain the membership describes; anything else goes over the
/// wire, because the committed configuration -- not this process's
/// opinion of itself -- decides who the voter is.
///
/// Every destination is offered the frame independently. The remote
/// fan-out happens before the local offer for exactly that reason: a
/// full local ingress must never be able to hold up a submission to the
/// voters that could have taken it.
pub fn dispatch(
    membership: &Membership,
    peers: &dyn PeerFanOut,
    local: Option<&dyn LocalIngress>,
    plan: &FanOut,
) -> Dispatched {
    let local = local.filter(|l| l.domain() == membership.domain());
    let mut plans = Vec::with_capacity(plan.targets.len());
    let mut remote = Vec::new();
    for target in &plan.targets {
        // The incarnation is looked up, not carried. A plan naming a
        // replica that is not a committed voter is dropped here rather
        // than offered to some other generation of that node -- and that
        // check happens before the local route is considered, so a
        // process cannot deliver to its own voter a frame the
        // configuration says that voter may not have.
        let Some(incarnation) = membership.voter_incarnation(target) else {
            plans.push(Destination::Rejected(NotQueued::NotACommittedVoter));
            continue;
        };
        let mine = local.is_some_and(|l| l.replica() == *target && l.incarnation() == incarnation);
        if mine {
            plans.push(Destination::Local(incarnation));
        } else {
            remote.push((*target, incarnation));
            plans.push(Destination::Remote(incarnation));
        }
    }

    let mut results = peers
        .fan_out(&remote, membership.domain(), &plan.frame)
        .into_iter();

    let mut out = Dispatched::default();
    for (target, destination) in plan.targets.iter().zip(plans) {
        match destination {
            Destination::Rejected(why) => out.rejected.push((*target, why)),
            Destination::Local(incarnation) => {
                let offered = local
                    .expect("a local destination is only planned when there is a local ingress")
                    .offer(&plan.frame);
                match offered {
                    Ok(()) => out.queued.push(Queued {
                        replica: *target,
                        incarnation,
                        route: Route::Local,
                    }),
                    Err(Saturated) => out.rejected.push((*target, NotQueued::Saturated)),
                }
            }
            Destination::Remote(incarnation) => {
                match results.next() {
                    Some(Ok(())) => out.queued.push(Queued {
                        replica: *target,
                        incarnation,
                        route: Route::Remote,
                    }),
                    Some(Err(e)) => out.rejected.push((*target, classify(e))),
                    // A transport that answered fewer targets than it was
                    // given has told us nothing about this one. It is
                    // reported as unreachable rather than quietly counted
                    // as queued.
                    None => out
                        .rejected
                        .push((*target, NotQueued::Unavailable(SendError::NotConnected))),
                }
            }
        }
    }
    out
}
