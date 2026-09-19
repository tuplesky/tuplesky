//! The bounded ingress of a voter running in this process (design
//! Sections 3.2, 22.1).
//!
//! A frontend co-located with one of its domain's voters does not dial
//! itself. It puts the submission here, and the voter's own runtime
//! takes it from here on its own turn. That is the whole of what "local
//! delivery" means: a frame skips serialization and the network, and
//! skips nothing else.
//!
//! # Why this is a queue and not a call
//!
//! The frontend could, in principle, step the voter itself while holding
//! the request. It must not. A request handler that drove a voter to
//! completion would put consensus work, storage waits and recovery on
//! the response path of whichever caller happened to arrive first, and
//! would let one caller's submission be charged to another caller's
//! deadline. The queue is what keeps the two kinds of work apart: the
//! frontend's obligation ends when the frame is accepted, and the
//! voter's begins when it next runs.
//!
//! # Local is not free
//!
//! The ingress is bounded in frames *and* in bytes, and a full one
//! refuses. A local route that accepted everything would be an
//! unaccounted queue with the process's memory as its only bound, which
//! is exactly the failure a remote destination's flow control exists to
//! prevent. Refusals are counted, because an ingress that is refusing is
//! a voter falling behind and an operator wants to see it.
//!
//! # The capability
//!
//! [`Ingress`] is built from the committed membership, for a replica the
//! configuration names as a voter, and it is the voter runtime that
//! holds it. [`LocalRoute`] is the handle a frontend may be given. There
//! is no constructor that takes an incarnation, so a process cannot
//! claim a generation of a node the configuration did not commit to, and
//! a process that runs no voter simply never builds one.
//!
//! An ingress also names the role its submitter presents. On the wire
//! that role comes from the peer's bound certificate and decides whether
//! a `Submit` may be admitted at all; in this process it comes from the
//! capabilities this node's own certificate carries, and it is checked
//! here and again where the two routes converge. A role that may not
//! submit on a client's behalf gets no ingress to offer to.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use coord_membership::membership::Membership;
use coord_types::ids::{DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;

use crate::fanout::{LocalIngress, Saturated};

/// How much a voter's local ingress will hold.
///
/// Both bounds apply: a burst of small frames is bounded by `frames`, and
/// a few large ones by `bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IngressBudget {
    /// Most frames waiting at once.
    pub frames: usize,
    /// Most bytes waiting at once, summed over the frames.
    pub bytes: usize,
}

impl Default for IngressBudget {
    fn default() -> Self {
        IngressBudget {
            frames: 256,
            bytes: 1 << 20,
        }
    }
}

#[derive(Debug)]
struct Queue {
    frames: VecDeque<Vec<u8>>,
    bytes: usize,
    refused: u64,
    accepted: u64,
}

#[derive(Debug)]
struct Shared {
    budget: IngressBudget,
    queue: Mutex<Queue>,
}

impl Shared {
    fn offer(&self, frame: &[u8]) -> Result<(), Saturated> {
        let mut queue = self.queue.lock().expect("local ingress poisoned");
        // A frame larger than the whole budget would never fit, however
        // empty the queue is; it is refused rather than admitted as a
        // special case, so the bound means one thing.
        if queue.frames.len() >= self.budget.frames
            || queue.bytes.saturating_add(frame.len()) > self.budget.bytes
        {
            queue.refused += 1;
            return Err(Saturated);
        }
        queue.bytes += frame.len();
        queue.accepted += 1;
        queue.frames.push_back(frame.to_vec());
        Ok(())
    }
}

/// The voter's end of its own ingress.
///
/// Owned by the runtime that runs that voter. Handing out a
/// [`LocalRoute`] is how a frontend in the same process is given the
/// short path; there is no other way to obtain one.
#[derive(Debug)]
pub struct Ingress {
    shared: Arc<Shared>,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    domain: DomainId,
    submitter: PeerRole,
}

impl Ingress {
    /// The ingress of `replica`, for frames a local `submitter`
    /// presents, if the committed configuration names `replica` a voter
    /// of this domain and `submitter` is a role that may submit on a
    /// client's behalf.
    ///
    /// The incarnation is looked up rather than supplied. A runtime
    /// cannot assert which generation of a node it is: the configuration
    /// the cluster agreed on says so, or this is not a voter's ingress
    /// and there is none to build.
    pub fn new(
        membership: &Membership,
        replica: ReplicaId,
        submitter: PeerRole,
        budget: IngressBudget,
    ) -> Option<Ingress> {
        if !coord_collector::ingress::is_collector(submitter) {
            return None;
        }
        let incarnation = membership.voter_incarnation(&replica)?;
        Some(Ingress {
            shared: Arc::new(Shared {
                budget,
                queue: Mutex::new(Queue {
                    frames: VecDeque::new(),
                    bytes: 0,
                    refused: 0,
                    accepted: 0,
                }),
            }),
            replica,
            incarnation,
            domain: membership.domain(),
            submitter,
        })
    }

    /// A handle a frontend in this process may deliver through.
    pub fn route(&self) -> LocalRoute {
        LocalRoute {
            shared: Arc::clone(&self.shared),
            replica: self.replica,
            incarnation: self.incarnation,
            domain: self.domain,
        }
    }

    /// Take up to `frames` waiting frames, oldest first.
    ///
    /// Bounded on purpose: the voter runtime interleaves this with its
    /// own timers, recovery and storage work, and a drain that ran to
    /// exhaustion would let a busy frontend decide how long the voter
    /// went without doing any of them.
    pub fn take(&self, frames: usize) -> Vec<Vec<u8>> {
        let mut queue = self.shared.queue.lock().expect("local ingress poisoned");
        let take = frames.min(queue.frames.len());
        let taken: Vec<Vec<u8>> = queue.frames.drain(..take).collect();
        queue.bytes -= taken.iter().map(Vec::len).sum::<usize>();
        taken
    }

    /// Frames waiting.
    pub fn depth(&self) -> usize {
        self.shared
            .queue
            .lock()
            .expect("local ingress poisoned")
            .frames
            .len()
    }

    /// Bytes waiting.
    pub fn bytes(&self) -> usize {
        self.shared
            .queue
            .lock()
            .expect("local ingress poisoned")
            .bytes
    }

    /// Frames accepted, and frames refused for want of room, since boot.
    pub fn counts(&self) -> (u64, u64) {
        let queue = self.shared.queue.lock().expect("local ingress poisoned");
        (queue.accepted, queue.refused)
    }

    /// The voter this ingress belongs to.
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// The incarnation the committed configuration named for it.
    pub const fn incarnation(&self) -> ReplicaIncarnation {
        self.incarnation
    }

    /// The role a frame taken from here was submitted under.
    pub const fn submitter(&self) -> PeerRole {
        self.submitter
    }
}

/// A frontend's handle on a co-located voter's ingress.
#[derive(Clone, Debug)]
pub struct LocalRoute {
    shared: Arc<Shared>,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    domain: DomainId,
}

impl LocalIngress for LocalRoute {
    fn replica(&self) -> ReplicaId {
        self.replica
    }

    fn incarnation(&self) -> ReplicaIncarnation {
        self.incarnation
    }

    fn domain(&self) -> DomainId {
        self.domain
    }

    fn offer(&self, frame: &[u8]) -> Result<(), Saturated> {
        self.shared.offer(frame)
    }
}
