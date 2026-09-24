//! Quorum-certified checkpoint floors (task-52; design Sections 5.3,
//! 23 G5).
//!
//! Task-51 trims protocol state only when *every* configured voter has
//! durably acknowledged the same checkpoint. That is the conservative
//! reference increment, and its cost is stated plainly: one permanently
//! absent voter stops forgetting for ever. This module is the vocabulary
//! and the predicates of the protocol that removes that cost without
//! removing the safety, and task-53 is its durable implementation.
//!
//! The shape is deliberately close to the one consensus already has,
//! because it rests on the same argument:
//!
//! 1. **Prepare.** Somebody proposes a [`FloorCandidate`]: a
//!    configuration epoch, an executed prefix, and a subject digest that
//!    binds cluster, domain, configuration, boundary, state root and
//!    checkpoint format. Two candidates that differ in any of those are
//!    different floors, however close their positions.
//! 2. **Readiness.** A voter records [`Readiness`] only when it durably
//!    holds the complete checkpoint the subject names. Recording it is
//!    a promise, and the promise is what the protocol rests on: this
//!    voter will never again vote from a baseline below that position.
//!    A [`ReadinessLedger`] is one voter's durable row; it moves up, it
//!    never moves down, and it never holds two subjects at one position.
//! 3. **Activation.** A majority of the epoch's voters with readiness
//!    for the *same* candidate is an [`ActivatedFloor`]. The certificate
//!    names its signers, and it is durable before anything is deleted.
//! 4. **Discovery.** Every permitted recovery reads reports from a
//!    majority of the same voter set. Two majorities of one set
//!    intersect, so a recovery always sees at least one signer of the
//!    highest activated floor: [`discover`] returns its position, and a
//!    replica below it installs that state before it votes. Which image
//!    to install is identified when the read allows it
//!    ([`Discovered::subject`], [`Discovered::certified`]); a majority
//!    read can also leave it open, and resolving it is the caller's
//!    (task-53).
//! 5. **Fence.** Once a floor is durable here, a delayed message about
//!    state at or below it is answered from the retained common outcome
//!    ([`FenceVerdict::Retained`]) and never by re-creating the protocol
//!    rows trimming removed.
//!
//! Two things this is careful *not* to be.
//!
//! **A floor is not a ballot artifact.** It belongs to a configuration
//! and outlives every term in it, so nothing here consults a ballot or
//! its leader. Requiring the leader would bind a durable cross-ballot
//! fact to a leadership that changes underneath it, and it would buy
//! nothing: the intersection that makes discovery work is between two
//! majorities of the *same voter set*, and those intersect whoever
//! leads.
//!
//! **Copying a snapshot to a majority is not activation.** What is
//! counted is the durable promise, not possession. A voter that holds
//! the bytes and has recorded nothing can crash, come back with its old
//! baseline, and vote from history the cluster has agreed to forget --
//! which is exactly the counterexample the bounded model freezes.
//!
//! The module is pure: no I/O, no clock, no row encoding. What a
//! subject digest is computed over, where a readiness row lives and how
//! a floor is published are task-53's.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;

use coord_types::identity::Digest32;
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition, ReplicaId};
use serde::{Deserialize, Serialize};

/// The voters one floor is certified by: exactly the configuration's.
///
/// Held separately from [`BallotConfiguration`] on purpose. That type
/// describes one term -- it has a ballot, a leader and a fast set --
/// and a floor has none of those. Sharing it would invite a rule that
/// depended on who happened to be leading when the floor was certified.
///
/// [`BallotConfiguration`]: crate::quorum::BallotConfiguration
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FloorVoters {
    epoch: ConfigurationEpoch,
    voters: BTreeSet<ReplicaId>,
}

impl FloorVoters {
    /// The voters of `epoch`. Refused when empty: a floor certified by
    /// nobody is not a weaker floor, it is no floor.
    pub fn new(epoch: ConfigurationEpoch, voters: BTreeSet<ReplicaId>) -> Option<Self> {
        if voters.is_empty() {
            return None;
        }
        Some(FloorVoters { epoch, voters })
    }

    /// The configuration epoch.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }

    /// The exact voters.
    pub const fn voters(&self) -> &BTreeSet<ReplicaId> {
        &self.voters
    }

    /// Whether `replica` votes in this epoch.
    pub fn is_voter(&self, replica: &ReplicaId) -> bool {
        self.voters.contains(replica)
    }

    /// Signatures a certificate needs: a majority (`N/2 + 1`).
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// What one floor names.
///
/// `subject` is the whole identity of the checkpoint: cluster, domain,
/// configuration, boundary, state root and format, bound together by
/// whoever computes it. It is carried as a digest here because this
/// module decides nothing about the bytes -- only that signers agree on
/// them exactly. A floor whose signers agreed on a position but not on
/// a subject would be a majority certifying two different states at one
/// executed prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FloorCandidate {
    /// Configuration the floor belongs to.
    pub epoch: ConfigurationEpoch,
    /// Executed prefix the checkpoint covers, and at or below which
    /// protocol state may be forgotten.
    pub position: ExecutionPosition,
    /// Digest binding everything else about the checkpoint.
    pub subject: Digest32,
}

/// One voter's durable readiness for a candidate.
///
/// Evidence of a promise, never of possession. A voter records it only
/// once the complete checkpoint the subject names is durably its own,
/// and recording it binds the voter never to vote again from a baseline
/// below the candidate's position.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    /// The voter that promised.
    pub voter: ReplicaId,
    /// What it promised about.
    pub candidate: FloorCandidate,
}

/// Why a voter could not record readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ReadinessError {
    /// The candidate belongs to another configuration.
    EpochMismatch,
    /// This replica does not vote in the epoch. An observer holds
    /// catch-up state, not obligations, and certifies nothing.
    NotAVoter,
    /// The candidate is below what this voter already promised.
    /// Promises only move up: a voter that could lower its own would
    /// undo the fence that makes the floor safe.
    Regression {
        /// What it already holds.
        held: FloorCandidate,
    },
    /// Another subject at the position this voter is already ready for.
    ///
    /// Refused here, once, rather than left for activation to catch:
    /// this is the only rule that stops two majorities certifying
    /// different state at one executed prefix, because a voter that
    /// could be ready for both would be in both of them.
    Competing {
        /// What it already holds.
        held: FloorCandidate,
    },
}

/// One voter's durable readiness row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadinessLedger {
    voter: ReplicaId,
    epoch: ConfigurationEpoch,
    held: Option<FloorCandidate>,
}

impl ReadinessLedger {
    /// An empty ledger for `voter` in `epoch`.
    pub const fn new(voter: ReplicaId, epoch: ConfigurationEpoch) -> Self {
        ReadinessLedger {
            voter,
            epoch,
            held: None,
        }
    }

    /// Rebuild from a durable row.
    pub const fn recovered(
        voter: ReplicaId,
        epoch: ConfigurationEpoch,
        held: Option<FloorCandidate>,
    ) -> Self {
        ReadinessLedger { voter, epoch, held }
    }

    /// What this voter has promised, if anything.
    pub const fn held(&self) -> Option<FloorCandidate> {
        self.held
    }

    /// The baseline this voter may not vote from below.
    pub fn floor(&self) -> ExecutionPosition {
        self.held.map_or(ExecutionPosition::ZERO, |c| c.position)
    }

    /// Record readiness for `candidate`, returning the evidence a
    /// certificate counts.
    ///
    /// Repeating the candidate already held succeeds and changes
    /// nothing: readiness is a promise, and promising the same thing
    /// twice is the same promise. That is what makes a retry after a
    /// lost reply safe.
    pub fn record(
        &mut self,
        voters: &FloorVoters,
        candidate: FloorCandidate,
    ) -> Result<Readiness, ReadinessError> {
        if candidate.epoch != self.epoch || voters.epoch() != self.epoch {
            return Err(ReadinessError::EpochMismatch);
        }
        if !voters.is_voter(&self.voter) {
            return Err(ReadinessError::NotAVoter);
        }
        if let Some(held) = self.held {
            if candidate == held {
                return Ok(Readiness {
                    voter: self.voter,
                    candidate,
                });
            }
            if candidate.position < held.position {
                return Err(ReadinessError::Regression { held });
            }
            if candidate.position == held.position {
                return Err(ReadinessError::Competing { held });
            }
        }
        self.held = Some(candidate);
        Ok(Readiness {
            voter: self.voter,
            candidate,
        })
    }
}

/// Why readiness did not certify a floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ActivationError {
    /// Nothing was offered.
    Empty,
    /// A candidate belongs to another configuration.
    EpochMismatch,
    /// A signature came from a replica that does not vote here.
    NotAVoter {
        /// The sender.
        replica: ReplicaId,
    },
    /// The readiness offered is not all for one candidate. A
    /// certificate is evidence about one floor; merging two would
    /// certify a position no majority ever agreed on.
    Divided,
    /// Fewer than a majority of the epoch's voters are ready.
    NoQuorum {
        /// Distinct voters offered.
        have: usize,
        /// Signatures a certificate needs.
        need: usize,
    },
}

/// A floor a majority of the configuration's voters has certified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ActivatedFloor {
    candidate: FloorCandidate,
    signers: BTreeSet<ReplicaId>,
}

impl ActivatedFloor {
    /// What was certified.
    pub const fn candidate(&self) -> FloorCandidate {
        self.candidate
    }

    /// The executed prefix at or below which protocol state may be
    /// forgotten.
    pub const fn position(&self) -> ExecutionPosition {
        self.candidate.position
    }

    /// The checkpoint identity every signer agreed on.
    pub const fn subject(&self) -> Digest32 {
        self.candidate.subject
    }

    /// The configuration this floor belongs to.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.candidate.epoch
    }

    /// The voters that promised. Retained in the certificate because a
    /// floor's authority is the promises behind it, and an operator
    /// reading one has to be able to see whose they were.
    pub const fn signers(&self) -> &BTreeSet<ReplicaId> {
        &self.signers
    }

    /// Whether a replica whose durable baseline is `baseline` may vote
    /// under this floor.
    ///
    /// Below it the replica holds history the cluster has agreed to
    /// forget, so its votes would be cast from a discarded baseline. It
    /// obtains the floor's state first; nothing about the floor makes
    /// it eligible in the meantime.
    pub fn admits_voter(&self, baseline: ExecutionPosition) -> bool {
        baseline >= self.position()
    }

    /// What a message about `position` may do.
    pub fn verdict(&self, position: ExecutionPosition) -> FenceVerdict {
        if position <= self.position() {
            FenceVerdict::Retained
        } else {
            FenceVerdict::Ordinary
        }
    }
}

/// What a delayed message about some execution position may do under an
/// activated floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum FenceVerdict {
    /// Above the floor: handled as any other message.
    Ordinary,
    /// At or below the floor: answered from the retained common
    /// outcome. The protocol rows trimming removed are never
    /// re-created, whatever the message asks for -- a pre-floor message
    /// that could rebuild them is exactly how forgotten state comes
    /// back.
    Retained,
}

/// Certify `ready` as a floor of `voters`.
///
/// Every offered readiness must name one candidate of this epoch and
/// come from a voter of it; duplicates from one voter count once. A
/// majority certifies.
pub fn activate(
    voters: &FloorVoters,
    ready: &[Readiness],
) -> Result<ActivatedFloor, ActivationError> {
    let Some(first) = ready.first() else {
        return Err(ActivationError::Empty);
    };
    let candidate = first.candidate;
    if candidate.epoch != voters.epoch() {
        return Err(ActivationError::EpochMismatch);
    }
    let mut signers = BTreeSet::new();
    for offered in ready {
        if offered.candidate.epoch != voters.epoch() {
            return Err(ActivationError::EpochMismatch);
        }
        if offered.candidate != candidate {
            return Err(ActivationError::Divided);
        }
        if !voters.is_voter(&offered.voter) {
            return Err(ActivationError::NotAVoter {
                replica: offered.voter,
            });
        }
        signers.insert(offered.voter);
    }
    let need = voters.majority();
    if signers.len() < need {
        return Err(ActivationError::NoQuorum {
            have: signers.len(),
            need,
        });
    }
    Ok(ActivatedFloor { candidate, signers })
}

/// Two activated floors of one configuration disagreed at one position.
///
/// Impossible while readiness is recorded through [`ReadinessLedger`]:
/// a voter refuses a second subject at a position it is already ready
/// for, so two majorities cannot form. It is a typed outcome rather
/// than a panic because discovery reads what other replicas report, and
/// what another replica reports is evidence to check, not a fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct FloorConflict {
    /// The contested position.
    pub position: ExecutionPosition,
    /// One subject certified there.
    pub left: Digest32,
    /// The other.
    pub right: Digest32,
}

/// What a recovery learned from the readiness a majority reported.
///
/// The `position` is the binding part: it is what the recovering
/// replica may not vote from below, and the intersection argument gives
/// it unconditionally. The image is a weaker matter. At most one subject
/// at a position can ever be certified: two certificates would need two
/// majorities, which intersect in a voter that would have had to sign
/// both, and [`ReadinessLedger::record`] refuses that. But voters that
/// never signed may still be ready for a competing subject at the same
/// position, so `subjects` can hold more than one entry *even when one
/// of them is certified*. With three voters, 0 and 2 ready for A certify
/// it, 1 is ready for B, and a majority read of {0, 1} sees one report
/// for each: the position is right and the image is not identified by
/// this read. [`Discovered::certified`] names the image whenever the
/// read itself holds a majority for one subject; otherwise the caller
/// must resolve it with more evidence (the certificate, or a wider
/// read) before installing anything. That resolution is task-53's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Discovered {
    /// Highest position any report is ready for.
    pub position: ExecutionPosition,
    /// The distinct subjects reported at that position.
    pub subjects: BTreeSet<Digest32>,
    /// Every report at that position, as read: the per-subject evidence
    /// that [`Discovered::certified`] counts.
    pub evidence: Vec<Readiness>,
}

impl Discovered {
    /// The one subject, when the reports agree on it.
    pub fn subject(&self) -> Option<Digest32> {
        if self.subjects.len() == 1 {
            self.subjects.iter().next().copied()
        } else {
            None
        }
    }

    /// The distinct voters of the read that reported readiness for
    /// `subject` at the discovered position.
    pub fn ready_for(&self, subject: Digest32) -> BTreeSet<ReplicaId> {
        self.evidence
            .iter()
            .filter(|r| r.candidate.subject == subject)
            .map(|r| r.voter)
            .collect()
    }

    /// The subject a majority of `voters` in this read is ready for, if
    /// any.
    ///
    /// Sound whenever it answers: a majority of one voter set ready for
    /// one candidate is enough to certify it, and uniqueness leaves no
    /// other subject certifiable at that position. It answers `None`
    /// when no subject reaches a majority within the read, which is not
    /// evidence that nothing was certified -- only that this read
    /// cannot tell which.
    pub fn certified(&self, voters: &FloorVoters) -> Option<Digest32> {
        self.subjects.iter().copied().find(|subject| {
            self.ready_for(*subject)
                .iter()
                .filter(|v| voters.is_voter(v))
                .count()
                >= voters.majority()
        })
    }
}

/// The floor a recovery discovers from the readiness a majority
/// reported.
///
/// This is the whole of discovery, and the reason it is sound is the
/// reason a recovery reads a majority in the first place: certifying a
/// floor needed readiness from a majority of the same voter set, two
/// majorities of one set intersect, so at least one report here comes
/// from a voter whose promise is at or above the highest certified
/// floor. Reading fewer reports is not a faster discovery, it is a
/// discovery that can miss -- and a replica that missed would vote from
/// a baseline the cluster has agreed to forget.
///
/// Readiness is what is read, not certificates. A signer promised
/// before any certificate existed and keeps the promise whether or not
/// it ever saw one, so the promises are the evidence that is actually
/// guaranteed to be there.
///
/// The majority is the caller's obligation, and this function cannot
/// check it: `reports` holds only the promises that were read, not who
/// answered, so an empty slice from a majority that promised nothing and
/// an empty slice from a read that reached nobody are the same value,
/// and both return `None`. The caller counts distinct responding voters
/// of the epoch -- whether or not they promised -- and refuses to
/// proceed below a majority before it calls this; in this repository
/// that is `coord-checkpoint`'s `recovery_obligation` (task-53), which
/// answers a short read with `TrimError::NoQuorum`.
pub fn discover(reports: &[Readiness]) -> Option<Discovered> {
    let position = reports.iter().map(|r| r.candidate.position).max()?;
    let evidence: Vec<Readiness> = reports
        .iter()
        .filter(|r| r.candidate.position == position)
        .copied()
        .collect();
    let subjects = evidence.iter().map(|r| r.candidate.subject).collect();
    Some(Discovered {
        position,
        subjects,
        evidence,
    })
}

/// What installing a floor did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum FloorInstall {
    /// The held floor advanced.
    Advanced,
    /// Already at or above it. A late certificate for an older floor is
    /// ordinary traffic, not an error -- and it never lowers what is
    /// held, which is the point.
    AlreadyCovered,
}

/// The activated floor one replica holds. Never lowered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FloorLedger {
    epoch: ConfigurationEpoch,
    held: Option<ActivatedFloor>,
}

impl FloorLedger {
    /// An empty ledger for `epoch`.
    pub const fn new(epoch: ConfigurationEpoch) -> Self {
        FloorLedger { epoch, held: None }
    }

    /// Rebuild from a durable certificate.
    pub const fn recovered(epoch: ConfigurationEpoch, held: Option<ActivatedFloor>) -> Self {
        FloorLedger { epoch, held }
    }

    /// The floor held, if any.
    pub const fn held(&self) -> Option<&ActivatedFloor> {
        self.held.as_ref()
    }

    /// The executed prefix this replica may forget at or below.
    pub fn position(&self) -> ExecutionPosition {
        self.held
            .as_ref()
            .map_or(ExecutionPosition::ZERO, ActivatedFloor::position)
    }

    /// Install `floor`, keeping the higher of the two.
    pub fn install(&mut self, floor: ActivatedFloor) -> Result<FloorInstall, FloorConflict> {
        if floor.epoch() != self.epoch {
            return Ok(FloorInstall::AlreadyCovered);
        }
        match &self.held {
            Some(held) if held.position() > floor.position() => Ok(FloorInstall::AlreadyCovered),
            Some(held) if held.position() == floor.position() => {
                if held.subject() == floor.subject() {
                    Ok(FloorInstall::AlreadyCovered)
                } else {
                    Err(FloorConflict {
                        position: held.position(),
                        left: held.subject(),
                        right: floor.subject(),
                    })
                }
            }
            _ => {
                self.held = Some(floor);
                Ok(FloorInstall::Advanced)
            }
        }
    }
}
