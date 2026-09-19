//! The committed configuration (design Sections 10.5.1, 10.5.2): for one
//! epoch, the exact voter incarnations and roles. Genesis initializes it;
//! sealed handoff (task-54+) produces the next epoch. It answers whether
//! a bound peer is a voter at the current key generation.

use std::collections::BTreeMap;

use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation};

use crate::genesis::GenesisManifest;

/// One committed voter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoterEntry {
    /// Node identity.
    pub node: ReplicaId,
    /// Committed key generation / incarnation. A peer with any other
    /// incarnation is a stale or cloned identity and never a voter.
    pub incarnation: ReplicaIncarnation,
}

/// Why a membership could not be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipError {
    /// A voter seed did not parse.
    BadVoter,
    /// Two seeds name the same node.
    DuplicateVoter {
        /// The node named twice.
        node: ReplicaId,
    },
}

/// The committed configuration of one epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    cluster: ClusterId,
    domain: DomainId,
    epoch: ConfigurationEpoch,
    voters: BTreeMap<ReplicaId, Voter>,
}

/// A committed voter: its key generation and the public key that
/// generation stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Voter {
    /// Committed key generation.
    pub incarnation: ReplicaIncarnation,
    /// Committed SubjectPublicKeyInfo DER.
    pub public_key: Vec<u8>,
}

impl Membership {
    /// The initial membership a verified manifest binds.
    pub fn from_genesis(manifest: &GenesisManifest) -> Result<Self, MembershipError> {
        let cluster = manifest.cluster_id().ok_or(MembershipError::BadVoter)?;
        let domain = manifest.domain_id().ok_or(MembershipError::BadVoter)?;
        let epoch = manifest.config_epoch().ok_or(MembershipError::BadVoter)?;
        let mut voters = BTreeMap::new();
        for seed in &manifest.voters {
            let (node, incarnation, public_key) =
                GenesisManifest::voter(seed).ok_or(MembershipError::BadVoter)?;
            if public_key.is_empty() {
                return Err(MembershipError::BadVoter);
            }
            let voter = Voter {
                incarnation,
                public_key,
            };
            if voters.insert(node, voter).is_some() {
                return Err(MembershipError::DuplicateVoter { node });
            }
        }
        Ok(Membership {
            cluster,
            domain,
            epoch,
            voters,
        })
    }

    /// Cluster.
    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }
    /// Domain.
    pub const fn domain(&self) -> DomainId {
        self.domain
    }
    /// Epoch.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }

    /// The committed voters, node order.
    pub fn voters(&self) -> impl Iterator<Item = VoterEntry> + '_ {
        self.voters.iter().map(|(node, voter)| VoterEntry {
            node: *node,
            incarnation: voter.incarnation,
        })
    }

    /// Whether `node` at `incarnation` is a committed voter now.
    pub fn is_current_voter(&self, node: &ReplicaId, incarnation: ReplicaIncarnation) -> bool {
        self.voters.get(node).map(|v| v.incarnation) == Some(incarnation)
    }

    /// Whether `node` at `incarnation` is the current committed voter
    /// *and* presents the key that generation committed to.
    pub fn is_current_voter_key(
        &self,
        node: &ReplicaId,
        incarnation: ReplicaIncarnation,
        public_key: &[u8],
    ) -> bool {
        self.voters
            .get(node)
            .is_some_and(|v| v.incarnation == incarnation && v.public_key == public_key)
    }

    /// The committed incarnation of a voter node, if any.
    pub fn voter_incarnation(&self, node: &ReplicaId) -> Option<ReplicaIncarnation> {
        self.voters.get(node).map(|v| v.incarnation)
    }
}
