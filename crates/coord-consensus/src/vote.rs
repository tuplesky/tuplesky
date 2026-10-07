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
//! * slow: the leader's proposal plus `slow_size` adoption
//!   acknowledgements, each a durable ACCEPT copy at the ballot, from any
//!   voters, the leader's own included. Only adoptions count (task-d19).
//!   The prototype's `acceptFastAndSlowAck` also counts a fast
//!   acknowledgement whose dependency set equals the leader's; that is
//!   stricter here (`[EXT: stricter]` in the source mapping). At five
//!   voters a slow decision counting a fast-set member's fast
//!   acknowledgement is not always visible to recovery: a recovering
//!   majority can hold two fast-set pre-accepts of one command with
//!   different dependencies and neither the leader nor an ACCEPT copy, and
//!   then cannot tell which of them was decided.
//!
//!   The leader is not counted for its proposal either. Its proposal row
//!   is PRE-ACCEPT and its acceptance is a separate batch, which a crash
//!   can lose after the followers' adoptions made a majority with it: the
//!   restarted leader then reports the command at PRE-ACCEPT, and a
//!   recovering majority of it and the non-adopters holds no ACCEPT copy
//!   (task-d19, Codex review). So the leader acknowledges its own order
//!   like any adopter, with a slow acknowledgement published once its
//!   acceptance row is durable (`[EXT]` in the source mapping). A slow
//!   decision is then `slow_size` durable ACCEPT copies at its ballot,
//!   every majority of reports holds one, and selection keeps ACCEPT at
//!   the source ballot.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
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
    /// Path digest through the command per key it touches, in key order.
    /// The combined `path` is one-way and cannot be taken apart, so the
    /// anchors a receiver needs to align its own logs travel beside it.
    pub paths: Vec<(Vec<u8>, Digest32)>,
    /// Combined digest of the conflict path the sender saw.
    pub path: Digest32,
    /// Digest of the admission this sender accepted the command under
    /// ([`coord_core::capability::admission_digest`]).
    ///
    /// The command identity says which request this is; it deliberately
    /// says nothing about the credential that admitted it, so a retry
    /// under a rotated credential stays the same command. That leaves
    /// the attested facts -- who was authenticated, under which trust
    /// rule, within what limits -- outside the identity, and they
    /// decide what execution does. So they are bound here instead: a
    /// quorum cannot form across senders that accepted one command as
    /// different facts.
    pub admission: Digest32,
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
    /// Digest of the admission this sender accepted the command under
    /// (see [`FastAck::admission`]). Adopting the leader's order is
    /// still accepting a command, and a replica that adopted it as
    /// different facts would execute different facts.
    pub admission: Digest32,
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

    /// Admission the sender accepted the command under.
    pub const fn admission(&self) -> Digest32 {
        match self {
            Vote::Fast(f) => f.admission,
            Vote::Slow(s) => s.admission,
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
    /// A non-leader vote carried a leader sequence number.
    ForgedProposal,
    /// The leader proposal carried no sequence number; the leader assigns
    /// the order, so nothing can be learned from it.
    MissingSequence,
    /// The sender accepted this command under a different admission than
    /// the votes already counted. Nothing is counted: the two are not
    /// acknowledgements of the same thing, whatever identity they share.
    AdmissionConflict {
        /// What the counted votes accepted.
        counted: Digest32,
    },
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
    admission: Option<Digest32>,
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
            admission: None,
        }
    }

    /// The admission every counted vote accepted this command under,
    /// once anything has been counted.
    pub const fn admission(&self) -> Option<Digest32> {
        self.admission
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
        let admission = vote.admission();
        if vote.command() != self.command {
            return Err(VoteError::WrongCommand);
        }
        if vote.ballot() != self.config.ballot() {
            return Err(VoteError::WrongBallot);
        }
        let replica = vote.replica();
        if !self.config.is_voter(&replica) {
            return Err(VoteError::NotAVoter);
        }
        // One command is one set of attested facts. A vote for the same
        // identity under other facts is not a second opinion about this
        // command; it is a vote about a different one, and counting it
        // would let a quorum form for a command no quorum agreed on.
        match self.admission {
            Some(counted) if counted != vote.admission() => {
                return Err(VoteError::AdmissionConflict { counted });
            }
            _ => {}
        }
        let is_leader = replica == self.config.leader();
        match vote {
            Vote::Fast(ack) => {
                if is_leader {
                    if ack.seqnum.is_none() {
                        return Err(VoteError::MissingSequence);
                    }
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
            // The leader's own adoption counts like anyone's: it says the
            // leader's acceptance row is durable, which its proposal does
            // not (task-d19).
            Vote::Slow(_) => {
                if !self.slow.insert(replica) {
                    return Err(VoteError::Duplicate);
                }
            }
        }
        self.admission = Some(admission);
        Ok(())
    }

    /// The conservative slow predicate only (task-24 learner): the leader
    /// proposal adopted by a majority.
    ///
    /// Only adoption acknowledgements count toward that majority, never a
    /// fast acknowledgement, whatever dependencies it carries, and never
    /// the leader's proposal by itself (task-d19): an adoption is a
    /// durable ACCEPT copy at this ballot, which any later majority of
    /// recovery reports holds and selection keeps, and neither a fast-set
    /// member's PRE-ACCEPT nor the leader's proposal row is. The leader
    /// counts once its own adoption, published when its acceptance row is
    /// durable, has arrived.
    pub fn learned_slow(&self) -> Option<Learned> {
        let leader = self.leader.as_ref()?;
        (self.slow.len() >= self.config.slow_size()).then(|| Learned::Slow {
            deps: leader.deps.clone(),
        })
    }

    /// Whether `replica` has acknowledged the leader's proposal of this
    /// command: an adoption acknowledgement, which a follower publishes
    /// only once it holds the proposal.
    ///
    /// A fast acknowledgement does not say as much. A follower publishes
    /// it when the payload arrives, with no sequence number, before and
    /// independently of any proposal, so it is no evidence that the
    /// proposal ever reached that follower (task-d07).
    pub fn adopted_by(&self, replica: &ReplicaId) -> bool {
        self.slow.contains(replica)
    }

    /// The learning predicate over the counted votes. Fast learning is
    /// preferred when both hold; both need the leader proposal.
    pub fn learned(&self) -> Option<Learned> {
        let leader = self.leader.as_ref()?;
        let agreeing_paths = self
            .fast
            .values()
            .filter(|a| a.path == leader.path && same_set(&a.deps, &leader.deps))
            .count();
        if agreeing_paths + 1 >= self.config.fast_size() {
            return Some(Learned::Fast {
                deps: leader.deps.clone(),
            });
        }
        self.learned_slow()
    }

    /// Why the fast predicate fails over the votes counted so far
    /// (task-d62), or `None` with no leader proposal to compare with.
    ///
    /// Read when a command decided on the slow path executes, it says
    /// what the fast path lacked by then, which is later than the slow
    /// decision: acknowledgements that agreed and arrived after it make
    /// [`MissedFast::SlowFirst`], since the predicate then holds. The
    /// others look at the fast set less the leader. When too few of its
    /// acknowledgements are absent for them to make up the quorum, a
    /// disagreeing one decided it: [`MissedFast::Path`] if any differed
    /// in its path, else [`MissedFast::Deps`]. Otherwise an absent one
    /// did: [`MissedFast::Missing`].
    pub fn missed_fast(&self) -> Option<MissedFast> {
        let leader = self.leader.as_ref()?;
        if matches!(self.learned(), Some(Learned::Fast { .. })) {
            return Some(MissedFast::SlowFirst);
        }
        let (mut agree, mut absent, mut path) = (0usize, 0usize, 0usize);
        for replica in self.config.fast_set() {
            if *replica == leader.replica {
                continue;
            }
            match self.fast.get(replica) {
                None => absent += 1,
                Some(a) if a.path != leader.path => path += 1,
                Some(a) if !same_set(&a.deps, &leader.deps) => {}
                Some(_) => agree += 1,
            }
        }
        let needed = self.config.fast_size().saturating_sub(1);
        Some(if agree + absent >= needed {
            MissedFast::Missing
        } else if path > 0 {
            MissedFast::Path
        } else {
            MissedFast::Deps
        })
    }
}

/// Why a command was not decided on the fast path (task-d62).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MissedFast {
    /// A fast-set acknowledgement saw another conflict path than the
    /// leader's. One sent while a `reordered` marker held its sender's
    /// path is one of these: the acknowledgement does not say so, and
    /// its sender counts it ([`FastPathCounts::acks_reordered`]).
    Path,
    /// A fast-set acknowledgement saw the leader's path but other direct
    /// dependencies.
    Deps,
    /// A fast-set acknowledgement the quorum needed had not arrived when
    /// the command executed.
    Missing,
    /// The fast quorum formed after the slow one had decided.
    SlowFirst,
}

/// What a voter's fast path did (task-d62), counted since its role
/// began; the role hands them over and starts again.
///
/// The `missed_*` counts classify every command the voter established
/// on the slow path, so they add up to those commands; a command whose
/// votes this voter did not count, or a run forced onto the slow path,
/// is [`FastPathCounts::missed_unclassified`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FastPathCounts {
    /// [`MissedFast::Path`].
    pub missed_path: u64,
    /// [`MissedFast::Deps`].
    pub missed_deps: u64,
    /// [`MissedFast::Missing`].
    pub missed_missing: u64,
    /// [`MissedFast::SlowFirst`].
    pub missed_slow_first: u64,
    /// Established on the slow path with no reason this voter could read.
    pub missed_unclassified: u64,
    /// Fast acknowledgements this voter sent as a fast-set follower.
    pub acks: u64,
    /// Of them, those sent while a command reordered behind a
    /// synchronization held this voter's path off every leader path: each
    /// disagrees with the leader in its path, whatever else it saw.
    pub acks_reordered: u64,
}

impl FastPathCounts {
    /// Count a command established on the slow path, for `reason`.
    pub const fn missed(&mut self, reason: Option<MissedFast>) {
        match reason {
            Some(MissedFast::Path) => self.missed_path += 1,
            Some(MissedFast::Deps) => self.missed_deps += 1,
            Some(MissedFast::Missing) => self.missed_missing += 1,
            Some(MissedFast::SlowFirst) => self.missed_slow_first += 1,
            None => self.missed_unclassified += 1,
        }
    }

    /// Add `other`'s counts to these.
    pub const fn add(&mut self, other: &FastPathCounts) {
        self.missed_path += other.missed_path;
        self.missed_deps += other.missed_deps;
        self.missed_missing += other.missed_missing;
        self.missed_slow_first += other.missed_slow_first;
        self.missed_unclassified += other.missed_unclassified;
        self.acks += other.acks;
        self.acks_reordered += other.acks_reordered;
    }
}

/// The reasons of commands established on the slow path, held from the
/// role's execution of each until its driver counts the establishment
/// (task-d62): a driver holding a group's establishments until the group
/// materializes counts one when it carries it out, and the reason waits
/// here for it.
///
/// Bounded: past `bound` the oldest is dropped, and its command, if its
/// establishment is ever carried out, counts as unclassified.
#[derive(Clone, Debug, Default)]
pub struct MissedLog {
    held: VecDeque<(CommandId, Option<MissedFast>)>,
}

impl MissedLog {
    /// Hold `reason` for `command`, established on the slow path.
    pub fn note(&mut self, command: CommandId, reason: Option<MissedFast>, bound: usize) {
        while self.held.len() >= bound.max(1) {
            self.held.pop_front();
        }
        self.held.push_back((command, reason));
    }

    /// The reason held for `command`, taken. Establishments are carried
    /// out in the order they were made, so it is near the front.
    pub fn take(&mut self, command: &CommandId) -> Option<MissedFast> {
        let at = self.held.iter().position(|(c, _)| c == command)?;
        self.held.remove(at).and_then(|(_, reason)| reason)
    }
}

/// Dependency sets compare as sets (prototype `Dep.Equals`).
pub fn same_set(a: &[CommandId], b: &[CommandId]) -> bool {
    let a: BTreeSet<&CommandId> = a.iter().collect();
    let b: BTreeSet<&CommandId> = b.iter().collect();
    a == b
}
