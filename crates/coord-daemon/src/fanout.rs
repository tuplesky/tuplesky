//! Sending a submission to every voter at once (design Sections 3.2,
//! 4.3).
//!
//! The collector plans the fan-out: it produces the exact `Submit` frame
//! and the voters it must reach, and holds no sockets. The daemon does
//! the sending, and in doing so answers the one question the plan leaves
//! open -- which incarnation of each replica is allowed to receive it.
//!
//! That answer comes from the committed membership, never from the plan
//! and never from the caller. A replica identity alone does not say which
//! generation of that node may vote; the committed configuration does,
//! and binding the two here is what stops a frame going to a stale
//! incarnation that would then be counted as a voter.
//!
//! Reachability is not correctness. Evidence is counted by voter
//! identity, so a target this process cannot reach right now simply does
//! not contribute; the submission is not failed for it, and the voters
//! that are reachable still get the frame. Only the caller's own quorum
//! rule decides whether enough of them answered.

use coord_collector::FanOut;
use coord_membership::membership::Membership;
use coord_transport::{Lane, SendError, Transport};
use coord_types::ids::{DomainId, ReplicaId, ReplicaIncarnation};

/// Where a planned fan-out actually went.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Dispatched {
    /// Voters the frame was admitted for, at the incarnation the
    /// committed configuration names.
    pub sent: Vec<(ReplicaId, ReplicaIncarnation)>,
    /// Voters the transport could not admit it for. Not a failure of the
    /// submission: they contribute no evidence and nothing more.
    pub unreachable: Vec<(ReplicaId, SendError)>,
    /// Targets the committed configuration does not name as voters. The
    /// frame is not sent to them at all, and their presence in a plan is
    /// worth reporting: it means the plan and the configuration disagree.
    pub not_a_voter: Vec<ReplicaId>,
}

impl Dispatched {
    /// How many voters received the frame.
    pub fn reached(&self) -> usize {
        self.sent.len()
    }
}

/// Somewhere to put one frame for several replicas at once.
///
/// [`Transport`] is the implementation; the trait exists so the decision
/// this module makes -- which incarnation, and whether to send at all --
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

/// Send `plan` to the voters the committed configuration names.
pub fn dispatch(membership: &Membership, peers: &dyn PeerFanOut, plan: &FanOut) -> Dispatched {
    let mut out = Dispatched::default();
    let mut targets = Vec::with_capacity(plan.targets.len());
    for target in &plan.targets {
        // The incarnation is looked up, not carried. A plan naming a
        // replica that is not a committed voter is dropped here rather
        // than sent to some other generation of that node.
        match membership.voter_incarnation(target) {
            Some(incarnation) => targets.push((*target, incarnation)),
            None => out.not_a_voter.push(*target),
        }
    }
    let results = peers.fan_out(&targets, membership.domain(), &plan.frame);
    for ((replica, incarnation), result) in targets.into_iter().zip(results) {
        match result {
            Ok(()) => out.sent.push((replica, incarnation)),
            Err(e) => out.unreachable.push((replica, e)),
        }
    }
    out
}
