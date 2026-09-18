//! Quorum policy (design Section 4.2; prototype `replica/quorum.go`).
//!
//! Every fast and slow quorum includes the ballot's leader. Slow quorums
//! are majorities. C2 uses one fixed majority-sized fast set bound to the
//! ballot (prototype `QuorumSet.AQ(ballot)`, `fixedMajority`); C1 uses any
//! set of more than three quarters of the voters (prototype
//! `ThreeQuarters`). A different fast set needs a higher ballot.

use alloc::collections::BTreeSet;

use coord_types::ids::{Ballot, ConfigurationEpoch, ReplicaId};
use serde::{Deserialize, Serialize};

/// Which fast-quorum class a ballot uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FastQuorumClass {
    /// Any fast quorum of more than three quarters of the voters.
    C1,
    /// One fixed fast set of majority size, immutable within the ballot.
    C2,
}

/// Why a ballot configuration is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigurationError {
    /// No voters.
    NoVoters,
    /// The ballot's leader is not a voter.
    LeaderNotVoter,
    /// The ballot's epoch differs from the configuration epoch.
    EpochMismatch,
    /// The fast set names a non-voter.
    FastSetNotVoters,
    /// The fast set does not include the leader.
    FastSetExcludesLeader,
    /// The C2 fast set is not exactly a majority.
    FastSetNotMajority,
}

/// The quorum policy of one ballot in one configuration epoch.
///
/// Only [`BallotConfiguration::c2`] and [`BallotConfiguration::c1`]
/// construct one, and decoding goes through the same validation, so every
/// value in existence satisfies the quorum invariants (the leader is a
/// voter in every fast set; a C2 fast set is exactly a majority).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BallotConfiguration {
    /// Configuration epoch (exact voter identities).
    epoch: ConfigurationEpoch,
    /// Ballot; its leader leads every quorum.
    ballot: Ballot,
    /// Exact voters of the epoch.
    voters: BTreeSet<ReplicaId>,
    /// C2: the fixed fast set. C1: every voter is eligible.
    fast_set: BTreeSet<ReplicaId>,
    /// Fast-quorum class.
    class: FastQuorumClass,
}

/// The encoded shape of a configuration; validated before it becomes one.
#[derive(Deserialize)]
struct RawConfiguration {
    epoch: ConfigurationEpoch,
    ballot: Ballot,
    voters: BTreeSet<ReplicaId>,
    fast_set: BTreeSet<ReplicaId>,
    class: FastQuorumClass,
}

impl<'de> Deserialize<'de> for BallotConfiguration {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawConfiguration::deserialize(deserializer)?;
        let config = BallotConfiguration {
            epoch: raw.epoch,
            ballot: raw.ballot,
            voters: raw.voters,
            fast_set: raw.fast_set,
            class: raw.class,
        };
        config
            .validate()
            .map_err(|e| serde::de::Error::custom(alloc::format!("{e:?}")))?;
        Ok(config)
    }
}

impl BallotConfiguration {
    /// Configuration epoch.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }

    /// The ballot.
    pub const fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Exact voters of the epoch.
    pub const fn voters(&self) -> &BTreeSet<ReplicaId> {
        &self.voters
    }

    /// The fast set (every voter for C1).
    pub const fn fast_set(&self) -> &BTreeSet<ReplicaId> {
        &self.fast_set
    }

    /// Fast-quorum class.
    pub const fn class(&self) -> FastQuorumClass {
        self.class
    }

    /// A C2 configuration with a fixed fast set (majority-sized, including
    /// the leader).
    pub fn c2(
        epoch: ConfigurationEpoch,
        ballot: Ballot,
        voters: BTreeSet<ReplicaId>,
        fast_set: BTreeSet<ReplicaId>,
    ) -> Result<Self, ConfigurationError> {
        let config = BallotConfiguration {
            epoch,
            ballot,
            voters,
            fast_set,
            class: FastQuorumClass::C2,
        };
        config.validate()?;
        Ok(config)
    }

    /// The default C2 fast set of a ballot until an operator quorum table
    /// (task-m01) supplies one: the leader plus the next `N/2` voters in
    /// identity order, wrapping. Deterministic on every replica.
    pub fn c2_default(
        epoch: ConfigurationEpoch,
        ballot: Ballot,
        voters: BTreeSet<ReplicaId>,
    ) -> Result<Self, ConfigurationError> {
        let ordered: alloc::vec::Vec<ReplicaId> = voters.iter().copied().collect();
        let Some(start) = ordered.iter().position(|r| *r == ballot.leader) else {
            return Err(ConfigurationError::LeaderNotVoter);
        };
        let size = ordered.len() / 2 + 1;
        let fast_set: BTreeSet<ReplicaId> = (0..size)
            .map(|i| ordered[(start + i) % ordered.len()])
            .collect();
        Self::c2(epoch, ballot, voters, fast_set)
    }

    /// A C1 configuration: any more-than-three-quarters set that includes
    /// the leader is a fast quorum.
    pub fn c1(
        epoch: ConfigurationEpoch,
        ballot: Ballot,
        voters: BTreeSet<ReplicaId>,
    ) -> Result<Self, ConfigurationError> {
        let config = BallotConfiguration {
            epoch,
            ballot,
            fast_set: voters.clone(),
            voters,
            class: FastQuorumClass::C1,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.voters.is_empty() {
            return Err(ConfigurationError::NoVoters);
        }
        if self.ballot.epoch != self.epoch {
            return Err(ConfigurationError::EpochMismatch);
        }
        if !self.voters.contains(&self.ballot.leader) {
            return Err(ConfigurationError::LeaderNotVoter);
        }
        if !self.fast_set.is_subset(&self.voters) {
            return Err(ConfigurationError::FastSetNotVoters);
        }
        if !self.fast_set.contains(&self.ballot.leader) {
            return Err(ConfigurationError::FastSetExcludesLeader);
        }
        if self.class == FastQuorumClass::C2 && self.fast_set.len() != self.slow_size() {
            return Err(ConfigurationError::FastSetNotMajority);
        }
        Ok(())
    }

    /// The voters of the configuration.
    pub const fn voters(&self) -> &BTreeSet<ReplicaId> {
        &self.voters
    }

    /// The fixed fast set (C2) or every voter (C1).
    pub const fn fast_set(&self) -> &BTreeSet<ReplicaId> {
        &self.fast_set
    }

    /// Number of voters.
    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    /// The ballot's leader.
    pub const fn leader(&self) -> ReplicaId {
        self.ballot.leader
    }

    /// Slow quorum size: a majority (`N/2 + 1`).
    pub fn slow_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Fast quorum size: the fixed set for C2, `3N/4 + 1` for C1.
    pub fn fast_size(&self) -> usize {
        match self.class {
            FastQuorumClass::C2 => self.fast_set.len(),
            FastQuorumClass::C1 => (3 * self.voters.len()) / 4 + 1,
        }
    }

    /// Whether `replica` votes in this epoch.
    pub fn is_voter(&self, replica: &ReplicaId) -> bool {
        self.voters.contains(replica)
    }

    /// Whether a fast acknowledgement from `replica` can count toward the
    /// fast path of this ballot.
    pub fn fast_eligible(&self, replica: &ReplicaId) -> bool {
        match self.class {
            FastQuorumClass::C2 => self.fast_set.contains(replica),
            FastQuorumClass::C1 => self.is_voter(replica),
        }
    }

    /// The paper's requirement that any two fast quorums intersect in a
    /// majority. C2 has one fast set, so it holds trivially; C1 holds when
    /// `2 * fast_size - N >= slow_size`.
    pub fn fast_quorums_intersect_in_majority(&self) -> bool {
        match self.class {
            FastQuorumClass::C2 => true,
            FastQuorumClass::C1 => 2 * self.fast_size() >= self.voters.len() + self.slow_size(),
        }
    }
}
