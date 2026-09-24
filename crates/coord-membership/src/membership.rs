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
    ///
    /// This is exactly what genesis committed, and it is what a
    /// certificate is compared against: the same bytes a TLS peer
    /// presents as its `SubjectPublicKeyInfo`.
    pub public_key: Vec<u8>,
    /// The same key as the point a signature verifies against.
    ///
    /// Derived from `public_key`, never committed separately: genesis
    /// commits one key, and this is the other way of writing it down.
    /// Signature verification wants the uncompressed EC point that the
    /// `SubjectPublicKeyInfo` carries, and a certificate comparison
    /// wants the whole structure; keeping both derived from one source
    /// is what stops the two checks from ever disagreeing about which
    /// key a voter has.
    ///
    /// Empty when the committed bytes are not a public key this build
    /// can verify with, which fails every signature closed.
    signing_key: Vec<u8>,
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
                signing_key: signing_point(&public_key),
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

    /// Verify an endpoint catalog against this committed configuration.
    ///
    /// A node that holds its epoch's membership can check a catalog
    /// without holding the configuration chain: the chain's part in
    /// [`Configurations::verify_endpoints`] is finding the epoch, and a
    /// node that has one epoch has already found it. The evidence rules
    /// are the same code either way -- a second implementation would be
    /// a second set of rules, and the weaker one would decide.
    ///
    /// [`Configurations::verify_endpoints`]: crate::configuration::Configurations::verify_endpoints
    pub fn verify_endpoints(
        &self,
        catalog: &coord_types::config_v1::EndpointCatalogV1,
    ) -> Result<(), crate::configuration::CatalogError> {
        crate::configuration::verify_endpoint_catalog(self, catalog)
    }
}

/// The uncompressed EC point inside a committed `SubjectPublicKeyInfo`.
///
/// Empty for anything else, so a voter whose committed bytes are not a
/// key this build can verify with simply verifies nothing: the length
/// check in `verify_signature` refuses it, which is the fail-closed
/// answer.
fn signing_point(public_key: &[u8]) -> Vec<u8> {
    use x509_parser::prelude::FromDer;
    // Already a point: a configuration chain commits them this way, and
    // a manifest may too.
    if public_key.len() == 65 && public_key[0] == 0x04 {
        return public_key.to_vec();
    }
    match x509_parser::x509::SubjectPublicKeyInfo::from_der(public_key) {
        Ok((_, spki)) => spki.subject_public_key.data.to_vec(),
        Err(_) => Vec::new(),
    }
}

impl crate::configuration::VoterAuthority for Membership {
    fn cluster(&self) -> ClusterId {
        self.cluster
    }
    fn domain(&self) -> DomainId {
        self.domain
    }
    fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }
    fn committed(&self, node: &ReplicaId) -> Option<(ReplicaIncarnation, &[u8])> {
        self.voters
            .get(node)
            .map(|v| (v.incarnation, v.signing_key.as_slice()))
    }
}
