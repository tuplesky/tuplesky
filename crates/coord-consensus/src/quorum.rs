//! Quorum policy (design Section 4.2; prototype `replica/quorum.go`).
//!
//! Every fast and slow quorum includes the ballot's leader. Slow quorums
//! are majorities. C2 uses one fixed majority-sized fast set bound to the
//! ballot (prototype `QuorumSet.AQ(ballot)`, `fixedMajority`); C1 uses any
//! set of more than three quarters of the voters (prototype
//! `ThreeQuarters`). A different fast set needs a higher ballot.

use alloc::collections::BTreeSet;
use alloc::sync::Arc;

use coord_types::ids::{Ballot, ConfigurationEpoch, ReplicaId};
use serde::{Deserialize, Serialize};

/// The voters of one configuration epoch, and what a majority of them is.
///
/// Deliberately not a [`BallotConfiguration`]. That type describes one
/// term -- it has a ballot, a leader and a fast set -- and the facts
/// this one serves are durable across terms: a checkpoint floor
/// (task-52) and a membership handoff (task-54) belong to a
/// configuration and outlive every leadership in it. Sharing the ballot
/// type would invite a rule that depended on who happened to be leading
/// when a certificate was gathered, and would buy nothing: the
/// intersection those protocols rest on is between two majorities of
/// the same voter set, and majorities of a set intersect whoever leads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct EpochVoters {
    epoch: ConfigurationEpoch,
    voters: BTreeSet<ReplicaId>,
}

impl EpochVoters {
    /// The voters of `epoch`. Refused when empty: a certificate signed
    /// by nobody is not a weaker certificate, it is none.
    pub fn new(epoch: ConfigurationEpoch, voters: BTreeSet<ReplicaId>) -> Option<Self> {
        if voters.is_empty() {
            return None;
        }
        Some(EpochVoters { epoch, voters })
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
///
/// The voter sets are shared, never changed after construction, so a
/// clone (one per command's vote set, on every voter and in the
/// collector) is two reference counts rather than two trees (task-d60).
/// The encoding is the sets', unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BallotConfiguration {
    /// Configuration epoch (exact voter identities).
    epoch: ConfigurationEpoch,
    /// Ballot; its leader leads every quorum.
    ballot: Ballot,
    /// Exact voters of the epoch.
    #[serde(serialize_with = "shared_set")]
    voters: Arc<BTreeSet<ReplicaId>>,
    /// C2: the fixed fast set. C1: every voter is eligible.
    #[serde(serialize_with = "shared_set")]
    fast_set: Arc<BTreeSet<ReplicaId>>,
    /// Fast-quorum class.
    class: FastQuorumClass,
}

/// A shared set encodes as the set itself.
fn shared_set<S: serde::Serializer>(
    set: &Arc<BTreeSet<ReplicaId>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    set.as_ref().serialize(serializer)
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
            voters: Arc::new(raw.voters),
            fast_set: Arc::new(raw.fast_set),
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
    pub fn voters(&self) -> &BTreeSet<ReplicaId> {
        &self.voters
    }

    /// The fast set (every voter for C1).
    pub fn fast_set(&self) -> &BTreeSet<ReplicaId> {
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
            voters: Arc::new(voters),
            fast_set: Arc::new(fast_set),
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
            fast_set: Arc::new(voters.clone()),
            voters: Arc::new(voters),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn r(i: u8) -> ReplicaId {
        ReplicaId([i; 16])
    }

    /// The encoded shape before the sets were shared (task-d60).
    #[derive(Serialize)]
    struct Owned {
        epoch: ConfigurationEpoch,
        ballot: Ballot,
        voters: BTreeSet<ReplicaId>,
        fast_set: BTreeSet<ReplicaId>,
        class: FastQuorumClass,
    }

    /// A clone shares the voter sets rather than copying them, and the
    /// encoding is byte for byte what it was with owned sets; it decodes
    /// back to an equal configuration.
    #[test]
    fn a_clone_shares_the_sets_and_the_encoding_is_unchanged() {
        let epoch = ConfigurationEpoch::new(3).unwrap();
        let ballot = Ballot {
            epoch,
            number: 7,
            leader: r(2),
        };
        let voters: BTreeSet<ReplicaId> = (1..=5).map(r).collect();
        for config in [
            BallotConfiguration::c2_default(epoch, ballot, voters.clone()).unwrap(),
            BallotConfiguration::c1(epoch, ballot, voters.clone()).unwrap(),
        ] {
            let clone = config.clone();
            assert!(Arc::ptr_eq(&config.voters, &clone.voters));
            assert!(Arc::ptr_eq(&config.fast_set, &clone.fast_set));
            let owned = Owned {
                epoch,
                ballot,
                voters: voters.clone(),
                fast_set: config.fast_set().clone(),
                class: config.class(),
            };
            let bytes = postcard::to_allocvec(&config).unwrap();
            assert_eq!(bytes, postcard::to_allocvec(&owned).unwrap());
            let back: BallotConfiguration = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(back, config);
        }
    }
}
