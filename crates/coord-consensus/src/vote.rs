//! Acknowledgements and learning predicates (design Sections 4.1, 4.3;
//! prototype `MFastAck`, `MLightSlowAck`, `replica/mset.go`,
//! `swift/client.go`).
//!
//! A vote set collects one ballot's acknowledgements for one command. It
//! rejects votes from non-voters (observers), from outside the fixed fast
//! set for the fast path, duplicates and wrong ballots, and binds the
//! leader's proposal. Learning:
//!
//! * fast: the leader's proposal plus fast-set members whose dependency
//!   path digest equals the leader's (prototype client `accept`: checksum
//!   equality), `fast_size` members in total including the leader;
//! * slow: the leader's proposal plus adoption acknowledgements (a slow
//!   acknowledgement, or a fast acknowledgement whose dependency set equals
//!   the leader's; prototype `acceptFastAndSlowAck`), `slow_size` members
//!   in total including the leader.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ReplicaId};
use serde::{Deserialize, Serialize};

use crate::quorum::BallotConfiguration;

/// A fast acknowledgement (`MFastAck`): the sender's local dependencies and
/// its dependency-path digest for the command's keys. The leader's fast
/// acknowledgement is the leader proposal and carries the sequence number.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FastAck {
    /// Sender.
    pub replica: ReplicaId,
    /// Ballot.
    pub ballot: Ballot,
    /// Command.
    pub command: CommandId,
    /// Direct dependencies as the sender ordered them.
    pub deps: Vec<CommandId>,
    /// Per-key path digests through the command (leader synchronization
    /// input for followers).
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Combined digest of the conflict path the sender saw.
    pub path: Digest32,
    /// Leader sequence number (leader proposal only).
    pub seqnum: Option<u64>,
}

/// A slow acknowledgement (`MLightSlowAck`): adoption of the leader's order.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlowAck {
    /// Sender.
    pub replica: ReplicaId,
    /// Ballot.
    pub ballot: Ballot,
    /// Command.
    pub command: CommandId,
}

/// One acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Vote {
    /// Fast acknowledgement or leader proposal.
    Fast(FastAck),
    /// Adoption acknowledgement.
    Slow(SlowAck),
}

impl Vote {
    /// Sender.
    pub const fn replica(&self) -> ReplicaId {
        match self {
            Vote::Fast(f) => f.replica,
            Vote::Slow(s) => s.replica,
        }
    }

    /// Ballot.
    pub const fn ballot(&self) -> Ballot {
        match self {
            Vote::Fast(f) => f.ballot,
            Vote::Slow(s) => s.ballot,
        }
    }

    /// Command.
    pub const fn command(&self) -> CommandId {
        match self {
            Vote::Fast(f) => f.command,
            Vote::Slow(s) => s.command,
        }
    }
}

/// Why a vote was rejected. Nothing was counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VoteError {
    /// The sender is not a voter of the epoch (an observer, or a stranger).
    NotAVoter,
    /// A fast acknowledgement from a voter outside the ballot's fast set.
    NotInFastSet,
    /// The vote names another ballot.
    WrongBallot,
    /// The vote names another command.
    WrongCommand,
    /// The sender already voted for this command in this ballot.
    Duplicate,
    /// The leader must send a proposal, not an adoption acknowledgement.
    LeaderSlowAck,
    /// A non-leader vote carried a leader sequence number.
    ForgedProposal,
}

/// A learned decision for the command.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Learned {
    /// Fast path: the leader's order confirmed by dependency paths.
    Fast {
        /// Dependencies (the leader's).
        deps: Vec<CommandId>,
    },
    /// Slow path: the leader's order adopted by a majority.
    Slow {
        /// Dependencies (the leader's).
        deps: Vec<CommandId>,
    },
}

impl Learned {
    /// The learned dependencies.
    pub fn deps(&self) -> &[CommandId] {
        match self {
            Learned::Fast { deps } | Learned::Slow { deps } => deps,
        }
    }
}

/// One ballot's acknowledgements for one command.
#[derive(Clone, Debug)]
pub struct VoteSet {
    config: BallotConfiguration,
    command: CommandId,
    leader: Option<FastAck>,
    fast: BTreeMap<ReplicaId, FastAck>,
    slow: BTreeSet<ReplicaId>,
}

impl VoteSet {
    /// An empty vote set for `command` under `config`.
    pub const fn new(config: BallotConfiguration, command: CommandId) -> Self {
        VoteSet {
            config,
            command,
            leader: None,
            fast: BTreeMap::new(),
            slow: BTreeSet::new(),
        }
    }

    /// The leader proposal, once received.
    pub const fn proposal(&self) -> Option<&FastAck> {
        self.leader.as_ref()
    }

    /// Voters whose acknowledgement was counted (leader included).
    pub fn voted(&self) -> BTreeSet<ReplicaId> {
        let mut out: BTreeSet<ReplicaId> = self.fast.keys().copied().collect();
        out.extend(self.slow.iter().copied());
        out.extend(self.leader.as_ref().map(|l| l.replica));
        out
    }

    /// Count a vote, or reject it with the reason. A replica may contribute
    /// one fast acknowledgement and one adoption acknowledgement (the
    /// prototype keeps them in separate sets); a second of the same kind
    /// is a duplicate.
    pub fn add(&mut self, vote: Vote) -> Result<(), VoteError> {
        if vote.command() != self.command {
            return Err(VoteError::WrongCommand);
        }
        if vote.ballot() != self.config.ballot {
            return Err(VoteError::WrongBallot);
        }
        let replica = vote.replica();
        if !self.config.is_voter(&replica) {
            return Err(VoteError::NotAVoter);
        }
        let is_leader = replica == self.config.leader();
        match vote {
            Vote::Fast(ack) => {
                if is_leader {
                    if self.leader.is_some() {
                        return Err(VoteError::Duplicate);
                    }
                    self.leader = Some(ack);
                } else {
                    if ack.seqnum.is_some() {
                        return Err(VoteError::ForgedProposal);
                    }
                    if !self.config.fast_eligible(&replica) {
                        return Err(VoteError::NotInFastSet);
                    }
                    if self.fast.contains_key(&replica) {
                        return Err(VoteError::Duplicate);
                    }
                    self.fast.insert(replica, ack);
                }
            }
            Vote::Slow(_) => {
                if is_leader {
                    return Err(VoteError::LeaderSlowAck);
                }
                if !self.slow.insert(replica) {
                    return Err(VoteError::Duplicate);
                }
            }
        }
        Ok(())
    }

    /// The conservative slow predicate only (task-24 learner): the leader
    /// proposal adopted by a majority including the leader.
    pub fn learned_slow(&self) -> Option<Learned> {
        let leader = self.leader.as_ref()?;
        let mut adopting: BTreeSet<ReplicaId> = self.slow.clone();
        adopting.extend(
            self.fast
                .values()
                .filter(|a| same_set(&a.deps, &leader.deps))
                .map(|a| a.replica),
        );
        (adopting.len() + 1 >= self.config.slow_size()).then(|| Learned::Slow {
            deps: leader.deps.clone(),
        })
    }

    /// The learning predicate over the counted votes. Fast learning is
    /// preferred when both hold; both need the leader proposal.
    pub fn learned(&self) -> Option<Learned> {
        let leader = self.leader.as_ref()?;
        let agreeing_paths = self.fast.values().filter(|a| a.path == leader.path).count();
        if agreeing_paths + 1 >= self.config.fast_size() {
            return Some(Learned::Fast {
                deps: leader.deps.clone(),
            });
        }
        let mut adopting: BTreeSet<ReplicaId> = self.slow.clone();
        adopting.extend(
            self.fast
                .values()
                .filter(|a| same_set(&a.deps, &leader.deps))
                .map(|a| a.replica),
        );
        if adopting.len() + 1 >= self.config.slow_size() {
            return Some(Learned::Slow {
                deps: leader.deps.clone(),
            });
        }
        None
    }
}

/// Dependency sets compare as sets (prototype `Dep.Equals`).
pub fn same_set(a: &[CommandId], b: &[CommandId]) -> bool {
    let a: BTreeSet<&CommandId> = a.iter().collect();
    let b: BTreeSet<&CommandId> = b.iter().collect();
    a == b
}
