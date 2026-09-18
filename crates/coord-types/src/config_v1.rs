//! Authoritative configuration records, authenticated hints, catalogs and
//! the configuration frames (task-m01; design Sections 10.5.1-10.5.3).
//!
//! * [`GroupConfigurationV1`] binds cluster and domain, the epoch, the
//!   exact voter identities with their committed incarnation and public
//!   key, the quorum-policy identifier, the previous epoch's certificate
//!   hash and the activation evidence (the genesis admin's signature for
//!   the first epoch, a majority of the previous epoch's voters for a
//!   handoff). Its certificate hash is the chain link.
//! * [`BallotConfigurationV1`] binds a ballot's leader and immutable fast
//!   set to one cluster, domain and configuration certificate under an
//!   epoch, with the voters' recovery promises as evidence.
//! * [`ConfigurationHintV1`] is what an authenticated response or error may
//!   carry: it triggers a refresh and authorizes nothing.
//! * [`EndpointCatalogV1`] and [`ObserverCatalogV1`] change addresses,
//!   certificate routing and serving topology under an endpoint or catalog
//!   generation; neither can name a voter the epoch does not have or a
//!   voter the epoch has under another incarnation.
//! * Bootstrap, subscription and paginated observer-discovery frames use
//!   raw kinds of the configuration range (`0x04xx`); they are dispatched
//!   at the configuration boundary, not by the typed `wire_v1` decoder.
//!
//! Verification (signatures, chain, monotonic installation) is
//! `coord-membership`; this module holds the frozen shapes, bounds,
//! canonical digests and framing. A larger epoch, a directory's word or a
//! fresh address never authorizes anything here.

use alloc::collections::BTreeSet;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::identity::{Digest32, HashDomain};
use crate::ids::{
    Ballot, CatalogGeneration, ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration,
    ReplicaId, ReplicaIncarnation,
};
use crate::wire_v1::{Frame, WireError, encode_frame};

/// Frozen bounds of the configuration schema.
pub mod limits {
    /// Most voters in one epoch.
    pub const MAX_VOTERS: usize = 16;
    /// Most observers in one catalog.
    pub const MAX_OBSERVERS: usize = 256;
    /// Most addresses per endpoint.
    pub const MAX_ADDRESSES: usize = 8;
    /// Longest address string.
    pub const MAX_ADDRESS_BYTES: usize = 256;
    /// Longest region label.
    pub const MAX_REGION_BYTES: usize = 64;
    /// Length of a voter public key (uncompressed P-256 point).
    pub const PUBLIC_KEY_BYTES: usize = 65;
    /// Length of a signature (ES256 fixed `r || s`).
    pub const SIGNATURE_BYTES: usize = 64;
    /// Most records in one bootstrap response.
    pub const MAX_CHAIN_RECORDS: usize = 64;
    /// Most observer entries in one discovery page.
    pub const MAX_PAGE: u16 = 64;
}

/// Schema version of every configuration frame.
pub const VERSION: u16 = 1;

/// Raw kinds of the configuration range.
pub mod kinds {
    /// Client to any node: the chain after a known epoch (or from genesis).
    pub const BOOTSTRAP_REQUEST: u16 = 0x0400;
    /// Node to client: chain records, the current ballot and endpoints.
    pub const BOOTSTRAP_RESPONSE: u16 = 0x0401;
    /// Client to node: notify me past these generations.
    pub const SUBSCRIBE: u16 = 0x0402;
    /// Node to client: an authenticated hint.
    pub const NOTICE: u16 = 0x0403;
    /// Client to node: one page of the observer catalog.
    pub const OBSERVER_DISCOVERY_REQUEST: u16 = 0x0404;
    /// Node to client: the page.
    pub const OBSERVER_DISCOVERY_PAGE: u16 = 0x0405;
}

/// Digest of "nothing before": the previous certificate of the genesis
/// epoch.
pub const NO_PREVIOUS_CERTIFICATE: Digest32 = Digest32([0; 32]);

/// Identifier of a supported quorum policy (design Section 4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QuorumPolicyId(pub u16);

impl QuorumPolicyId {
    /// C2: one fixed majority-sized fast set per ballot (the default).
    pub const C2_FIXED_MAJORITY: QuorumPolicyId = QuorumPolicyId(1);
    /// C1: any fast quorum of more than three quarters of the voters.
    pub const C1_THREE_QUARTERS: QuorumPolicyId = QuorumPolicyId(2);

    /// Whether this build supports the policy.
    pub const fn is_supported(self) -> bool {
        matches!(self.0, 1 | 2)
    }
}

/// One voter of an epoch: identity, committed incarnation and the public
/// key bound to that incarnation. A rotated key is a new incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterRecordV1 {
    /// Node identity.
    pub node: ReplicaId,
    /// Committed incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Uncompressed P-256 public key point (`0x04 || x || y`).
    pub public_key: Vec<u8>,
}

/// A voter's signature over a canonical message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterSignatureV1 {
    /// Signing voter.
    pub node: ReplicaId,
    /// Incarnation whose key signed.
    pub incarnation: ReplicaIncarnation,
    /// ES256 fixed signature (`r || s`).
    pub signature: Vec<u8>,
}

/// How an epoch was activated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivationEvidenceV1 {
    /// The genesis epoch: bound to the verified manifest and signed by the
    /// pinned genesis admin key.
    Genesis {
        /// Digest of the genesis manifest.
        manifest_digest: Digest32,
        /// Admin signature over the activation message.
        admin_signature: Vec<u8>,
    },
    /// A sealed handoff: the previous epoch's voters approve the successor.
    Handoff {
        /// Epoch that sealed and approved.
        old_epoch: ConfigurationEpoch,
        /// The unique terminal certificate of the handoff (task-56).
        terminal_certificate: Digest32,
        /// Approvals of a majority of the old epoch's voters over the
        /// activation message.
        approvals: Vec<VoterSignatureV1>,
    },
}

/// The authoritative record of one configuration epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupConfigurationV1 {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Epoch.
    pub epoch: ConfigurationEpoch,
    /// Exact voters, ascending by node.
    pub voters: Vec<VoterRecordV1>,
    /// Quorum policy.
    pub quorum_policy: QuorumPolicyId,
    /// Certificate hash of the previous epoch's record
    /// ([`NO_PREVIOUS_CERTIFICATE`] for genesis).
    pub previous_certificate: Digest32,
    /// Activation evidence.
    pub activation: ActivationEvidenceV1,
}

/// Why a record, catalog or frame is malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigError {
    /// No voters.
    NoVoters,
    /// More voters than the bound.
    TooManyVoters,
    /// Voters are not sorted ascending by node, or a node repeats.
    VotersNotSortedUnique,
    /// A public key is not an uncompressed P-256 point.
    BadPublicKey,
    /// A signature does not have the fixed length.
    BadSignature,
    /// The quorum policy is not supported.
    UnsupportedPolicy,
    /// Genesis evidence with a previous certificate, or handoff evidence
    /// without one.
    PreviousCertificateShape,
    /// The handoff's old epoch is not the epoch before this one.
    OldEpochNotPrevious,
    /// Two approvals or promises name the same node.
    DuplicateSigner,
    /// The fast set is empty, unsorted or repeats a node.
    BadFastSet,
    /// Ballot epoch differs from the record epoch.
    EpochMismatch,
    /// A collection exceeds its bound.
    TooMany,
    /// A string exceeds its bound.
    TooLong,
    /// The page limit is zero or above the bound.
    BadLimit,
    /// Frame problem.
    Wire(WireError),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Wire(e) => write!(f, "frame: {e}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl core::error::Error for ConfigError {}

impl From<WireError> for ConfigError {
    fn from(e: WireError) -> Self {
        ConfigError::Wire(e)
    }
}

fn check_signature(signature: &[u8]) -> Result<(), ConfigError> {
    if signature.len() != limits::SIGNATURE_BYTES {
        return Err(ConfigError::BadSignature);
    }
    Ok(())
}

fn check_signers(signers: &[VoterSignatureV1]) -> Result<(), ConfigError> {
    if signers.len() > limits::MAX_VOTERS {
        return Err(ConfigError::TooMany);
    }
    let mut seen = BTreeSet::new();
    for s in signers {
        check_signature(&s.signature)?;
        if !seen.insert(s.node) {
            return Err(ConfigError::DuplicateSigner);
        }
    }
    Ok(())
}

fn check_address(address: &str) -> Result<(), ConfigError> {
    if address.len() > limits::MAX_ADDRESS_BYTES {
        return Err(ConfigError::TooLong);
    }
    Ok(())
}

fn digest_parts(domain: HashDomain, parts: &[&[u8]]) -> Digest32 {
    domain.digest(parts)
}

impl GroupConfigurationV1 {
    /// Structural validation: bounds, ordering, policy support and the
    /// evidence shape. Signatures are verified by `coord-membership`.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.voters.is_empty() {
            return Err(ConfigError::NoVoters);
        }
        if self.voters.len() > limits::MAX_VOTERS {
            return Err(ConfigError::TooManyVoters);
        }
        for pair in self.voters.windows(2) {
            if pair[0].node >= pair[1].node {
                return Err(ConfigError::VotersNotSortedUnique);
            }
        }
        for v in &self.voters {
            if v.public_key.len() != limits::PUBLIC_KEY_BYTES || v.public_key[0] != 0x04 {
                return Err(ConfigError::BadPublicKey);
            }
        }
        if !self.quorum_policy.is_supported() {
            return Err(ConfigError::UnsupportedPolicy);
        }
        match &self.activation {
            ActivationEvidenceV1::Genesis {
                admin_signature, ..
            } => {
                if self.previous_certificate != NO_PREVIOUS_CERTIFICATE {
                    return Err(ConfigError::PreviousCertificateShape);
                }
                check_signature(admin_signature)
            }
            ActivationEvidenceV1::Handoff {
                old_epoch,
                approvals,
                ..
            } => {
                if self.previous_certificate == NO_PREVIOUS_CERTIFICATE {
                    return Err(ConfigError::PreviousCertificateShape);
                }
                if old_epoch.checked_next().ok() != Some(self.epoch) {
                    return Err(ConfigError::OldEpochNotPrevious);
                }
                check_signers(approvals)
            }
        }
    }

    /// Whether the record is the genesis epoch.
    pub const fn is_genesis(&self) -> bool {
        matches!(self.activation, ActivationEvidenceV1::Genesis { .. })
    }

    /// The voter record of `node`, if any.
    pub fn voter(&self, node: &ReplicaId) -> Option<&VoterRecordV1> {
        self.voters.iter().find(|v| v.node == *node)
    }

    /// Node identities of the voters.
    pub fn voter_ids(&self) -> BTreeSet<ReplicaId> {
        self.voters.iter().map(|v| v.node).collect()
    }

    /// Size of a majority of the voters.
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Canonical bytes of the identity part of the record (everything
    /// except the signatures).
    fn unsigned_parts(&self) -> Vec<Vec<u8>> {
        let mut parts: Vec<Vec<u8>> = alloc::vec![
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.epoch.to_be_bytes().to_vec(),
        ];
        for v in &self.voters {
            let mut voter = Vec::with_capacity(16 + 8 + v.public_key.len());
            voter.extend_from_slice(v.node.as_bytes());
            voter.extend_from_slice(&v.incarnation.to_be_bytes());
            voter.extend_from_slice(&v.public_key);
            parts.push(voter);
        }
        parts.push(self.quorum_policy.0.to_be_bytes().to_vec());
        parts.push(self.previous_certificate.0.to_vec());
        match &self.activation {
            ActivationEvidenceV1::Genesis {
                manifest_digest, ..
            } => {
                parts.push(alloc::vec![0]);
                parts.push(manifest_digest.0.to_vec());
            }
            ActivationEvidenceV1::Handoff {
                old_epoch,
                terminal_certificate,
                ..
            } => {
                parts.push(alloc::vec![1]);
                parts.push(old_epoch.to_be_bytes().to_vec());
                parts.push(terminal_certificate.0.to_vec());
            }
        }
        parts
    }

    /// The message the activating authority signs: every field except the
    /// signatures themselves.
    pub fn activation_message(&self) -> Digest32 {
        let parts = self.unsigned_parts();
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        digest_parts(HashDomain::ConfigurationActivation, &refs)
    }

    /// The certificate hash: the digest of the complete record including
    /// its evidence. The next epoch names it as `previous_certificate`.
    pub fn certificate_hash(&self) -> Digest32 {
        let encoded = postcard::to_allocvec(self).unwrap_or_default();
        digest_parts(HashDomain::ConfigurationRecord, &[&encoded])
    }
}

/// A ballot's leader and immutable fast set under an epoch, with the
/// voters' promises as evidence that the ballot was recovered.
///
/// A promise is a statement about one configuration of one domain, so the
/// record names that configuration exactly: the cluster and domain it was
/// made in and the certificate hash of the epoch record whose voters made
/// it. All three are signed, so a promise cannot be carried into another
/// context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BallotConfigurationV1 {
    /// Cluster/restore identity the promises were made in.
    pub cluster: ClusterId,
    /// Domain the promises were made in.
    pub domain: DomainId,
    /// Epoch.
    pub epoch: ConfigurationEpoch,
    /// Certificate hash of the epoch's record: the exact configuration the
    /// promisers held when they promised.
    pub configuration_certificate: Digest32,
    /// Ballot (its leader leads every quorum).
    pub ballot: Ballot,
    /// Quorum policy the fast set was chosen under.
    pub quorum_policy: QuorumPolicyId,
    /// Fast set, ascending by node (C2: exactly a majority including the
    /// leader; C1: every voter).
    pub fast_set: Vec<ReplicaId>,
    /// Promises of a majority of the epoch's voters over the ballot
    /// message.
    pub promises: Vec<VoterSignatureV1>,
}

impl BallotConfigurationV1 {
    /// Structural validation.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.ballot.epoch != self.epoch {
            return Err(ConfigError::EpochMismatch);
        }
        if !self.quorum_policy.is_supported() {
            return Err(ConfigError::UnsupportedPolicy);
        }
        if self.fast_set.is_empty() || self.fast_set.len() > limits::MAX_VOTERS {
            return Err(ConfigError::BadFastSet);
        }
        for pair in self.fast_set.windows(2) {
            if pair[0] >= pair[1] {
                return Err(ConfigError::BadFastSet);
            }
        }
        check_signers(&self.promises)
    }

    /// The message every promise signs.
    ///
    /// The preimage opens with the promise's context — cluster, domain and
    /// the certificate hash of the epoch record the promisers held — before
    /// the ballot itself. A message that named only the numbers made an
    /// honest promise from one domain valid evidence in every other domain
    /// that shares the same voter identities, incarnations and keys: with
    /// no key compromise at all, a replayed certificate installed a leader
    /// and fast set those voters never authorized there. Verification
    /// checks the three context fields against the chain that holds them
    /// (`coord-membership`), which is only meaningful because they are
    /// signed here.
    pub fn ballot_message(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = alloc::vec![
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.epoch.to_be_bytes().to_vec(),
            self.configuration_certificate.0.to_vec(),
            self.ballot.number.to_be_bytes().to_vec(),
            self.ballot.leader.as_bytes().to_vec(),
            self.quorum_policy.0.to_be_bytes().to_vec(),
        ];
        for r in &self.fast_set {
            parts.push(r.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        digest_parts(HashDomain::ConfigurationBallot, &refs)
    }
}

/// An authenticated hint: what a voter's response or error may carry to
/// tell a client it is stale. It triggers a refresh and authorizes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConfigurationHintV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Current epoch at the sender.
    pub epoch: ConfigurationEpoch,
    /// Certificate hash of that epoch.
    pub certificate: Digest32,
    /// Current endpoint generation at the sender.
    pub endpoint_generation: EndpointGeneration,
    /// Current observer catalog generation at the sender.
    pub catalog_generation: CatalogGeneration,
}

/// One voter's addresses and certificate routing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointV1 {
    /// Node.
    pub node: ReplicaId,
    /// Incarnation (must be the epoch's committed one).
    pub incarnation: ReplicaIncarnation,
    /// Addresses (`host:port`), bounded.
    pub addresses: Vec<String>,
    /// Digest of the presented certificate's public key, for routing and
    /// pinning hints only.
    pub certificate_fingerprint: Option<Digest32>,
}

/// The endpoint catalog of an epoch under an endpoint generation. It can
/// change addresses and certificate routing; it cannot add, remove or
/// re-incarnate a voter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointCatalogV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Epoch whose voters the endpoints belong to.
    pub epoch: ConfigurationEpoch,
    /// Generation; higher replaces lower within the epoch.
    pub generation: EndpointGeneration,
    /// Endpoints, ascending by node.
    pub endpoints: Vec<EndpointV1>,
    /// Attestation by one voter of the epoch over the catalog message.
    pub attestation: VoterSignatureV1,
}

/// One observer's serving entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserverEntryV1 {
    /// Node.
    pub node: ReplicaId,
    /// Incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Region label.
    pub region: String,
    /// Addresses, bounded.
    pub addresses: Vec<String>,
    /// Declared capability bits (serving topology only).
    pub capabilities: u32,
}

/// The observer catalog of an epoch under a catalog generation. It changes
/// serving topology, never quorum size: no entry may be a voter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserverCatalogV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Epoch.
    pub epoch: ConfigurationEpoch,
    /// Generation; higher replaces lower within the epoch.
    pub generation: CatalogGeneration,
    /// Observers, ascending by node.
    pub observers: Vec<ObserverEntryV1>,
    /// Attestation by one voter of the epoch over the catalog message.
    pub attestation: VoterSignatureV1,
}

fn check_sorted_nodes<T>(items: &[T], node: impl Fn(&T) -> ReplicaId) -> Result<(), ConfigError> {
    for pair in items.windows(2) {
        if node(&pair[0]) >= node(&pair[1]) {
            return Err(ConfigError::VotersNotSortedUnique);
        }
    }
    Ok(())
}

impl EndpointCatalogV1 {
    /// Structural validation.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.endpoints.len() > limits::MAX_VOTERS {
            return Err(ConfigError::TooMany);
        }
        check_sorted_nodes(&self.endpoints, |e| e.node)?;
        for e in &self.endpoints {
            if e.addresses.len() > limits::MAX_ADDRESSES {
                return Err(ConfigError::TooMany);
            }
            for a in &e.addresses {
                check_address(a)?;
            }
        }
        check_signature(&self.attestation.signature)
    }

    /// The message the attesting voter signs.
    pub fn catalog_message(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = alloc::vec![
            alloc::vec![0u8],
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.epoch.to_be_bytes().to_vec(),
            self.generation.to_be_bytes().to_vec(),
        ];
        for e in &self.endpoints {
            parts.push(e.node.as_bytes().to_vec());
            parts.push(e.incarnation.to_be_bytes().to_vec());
            for a in &e.addresses {
                parts.push(a.as_bytes().to_vec());
            }
            parts.push(
                e.certificate_fingerprint
                    .map_or_else(Vec::new, |d| d.0.to_vec()),
            );
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        digest_parts(HashDomain::ConfigurationCatalog, &refs)
    }
}

impl ObserverCatalogV1 {
    /// Structural validation.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.observers.len() > limits::MAX_OBSERVERS {
            return Err(ConfigError::TooMany);
        }
        check_sorted_nodes(&self.observers, |o| o.node)?;
        for o in &self.observers {
            if o.region.len() > limits::MAX_REGION_BYTES {
                return Err(ConfigError::TooLong);
            }
            if o.addresses.len() > limits::MAX_ADDRESSES {
                return Err(ConfigError::TooMany);
            }
            for a in &o.addresses {
                check_address(a)?;
            }
        }
        check_signature(&self.attestation.signature)
    }

    /// The message the attesting voter signs.
    pub fn catalog_message(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = alloc::vec![
            alloc::vec![1u8],
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.epoch.to_be_bytes().to_vec(),
            self.generation.to_be_bytes().to_vec(),
        ];
        for o in &self.observers {
            parts.push(o.node.as_bytes().to_vec());
            parts.push(o.incarnation.to_be_bytes().to_vec());
            parts.push(o.region.as_bytes().to_vec());
            for a in &o.addresses {
                parts.push(a.as_bytes().to_vec());
            }
            parts.push(o.capabilities.to_be_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        digest_parts(HashDomain::ConfigurationCatalog, &refs)
    }

    /// One page of the catalog after `after` (exclusive), at most `limit`
    /// entries, in node order. Deterministic: every node pages the same
    /// catalog the same way.
    pub fn page(&self, after: Option<ReplicaId>, limit: u16) -> ObserverDiscoveryPageV1 {
        let limit = limit.clamp(1, limits::MAX_PAGE) as usize;
        let entries: Vec<ObserverEntryV1> = self
            .observers
            .iter()
            .filter(|o| after.is_none_or(|a| o.node > a))
            .take(limit)
            .cloned()
            .collect();
        let last = entries.last().map(|o| o.node);
        let complete = match last {
            None => true,
            Some(last) => !self.observers.iter().any(|o| o.node > last),
        };
        ObserverDiscoveryPageV1 {
            epoch: self.epoch,
            generation: self.generation,
            entries,
            next: if complete { None } else { last },
            complete,
        }
    }
}

/// Bootstrap: the chain after a known epoch, or from genesis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapRequestV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Highest epoch the client holds, if any.
    pub known_epoch: Option<ConfigurationEpoch>,
    /// Its certificate hash, so a divergent chain is detected.
    pub known_certificate: Option<Digest32>,
}

/// Bootstrap response: records strictly after the known epoch (or from
/// genesis), the current ballot and the current endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapResponseV1 {
    /// Chain records, ascending by epoch, bounded.
    pub records: Vec<GroupConfigurationV1>,
    /// Whether `records` reaches the sender's current epoch.
    pub complete: bool,
    /// Current ballot configuration, if the sender has one.
    pub ballot: Option<BallotConfigurationV1>,
    /// Current endpoint catalog, if the sender has one.
    pub endpoints: Option<EndpointCatalogV1>,
}

impl BootstrapResponseV1 {
    /// Structural validation.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.records.len() > limits::MAX_CHAIN_RECORDS {
            return Err(ConfigError::TooMany);
        }
        for r in &self.records {
            r.validate_shape()?;
        }
        if let Some(b) = &self.ballot {
            b.validate_shape()?;
        }
        if let Some(e) = &self.endpoints {
            e.validate_shape()?;
        }
        Ok(())
    }
}

/// Subscription: notify the client when any generation moves past these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Epoch the client holds.
    pub epoch: ConfigurationEpoch,
    /// Endpoint generation the client holds.
    pub endpoint_generation: EndpointGeneration,
    /// Catalog generation the client holds.
    pub catalog_generation: CatalogGeneration,
}

/// A notice: an authenticated hint pushed to a subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoticeV1 {
    /// The hint.
    pub hint: ConfigurationHintV1,
}

/// One page of observer discovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserverDiscoveryRequestV1 {
    /// Cluster.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Resume after this node (exclusive).
    pub after: Option<ReplicaId>,
    /// Most entries wanted (clamped to [`limits::MAX_PAGE`]).
    pub limit: u16,
}

impl ObserverDiscoveryRequestV1 {
    /// Structural validation.
    pub const fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.limit == 0 || self.limit > limits::MAX_PAGE {
            return Err(ConfigError::BadLimit);
        }
        Ok(())
    }
}

/// A page of the observer catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserverDiscoveryPageV1 {
    /// Epoch of the catalog.
    pub epoch: ConfigurationEpoch,
    /// Catalog generation.
    pub generation: CatalogGeneration,
    /// Entries, ascending by node.
    pub entries: Vec<ObserverEntryV1>,
    /// Cursor for the next page, when incomplete.
    pub next: Option<ReplicaId>,
    /// Whether the catalog is exhausted.
    pub complete: bool,
}

impl ObserverDiscoveryPageV1 {
    /// Structural validation.
    pub fn validate_shape(&self) -> Result<(), ConfigError> {
        if self.entries.len() > limits::MAX_PAGE as usize {
            return Err(ConfigError::TooMany);
        }
        check_sorted_nodes(&self.entries, |o| o.node)?;
        if self.complete != self.next.is_none() {
            return Err(ConfigError::BadLimit);
        }
        Ok(())
    }
}

/// Encode a configuration frame of `kind`.
pub fn encode_message<T: Serialize>(kind: u16, message: &T) -> Result<Vec<u8>, ConfigError> {
    let payload = postcard::to_allocvec(message).map_err(|_| WireError::PayloadTooLarge)?;
    Ok(encode_frame(kind, VERSION, &payload)?)
}

/// Decode a configuration frame of exactly `kind` and version 1; trailing
/// payload bytes are an error.
pub fn decode_message<T: for<'de> Deserialize<'de>>(
    frame: &Frame,
    kind: u16,
) -> Result<T, ConfigError> {
    if frame.kind != kind {
        return Err(ConfigError::Wire(WireError::UnsupportedKind {
            kind: frame.kind,
        }));
    }
    if frame.version != VERSION {
        return Err(ConfigError::Wire(WireError::MalformedPayload));
    }
    let (value, rest): (T, &[u8]) =
        postcard::take_from_bytes(&frame.payload).map_err(|_| WireError::MalformedPayload)?;
    if !rest.is_empty() {
        return Err(ConfigError::Wire(WireError::TrailingPayloadBytes {
            extra: rest.len(),
        }));
    }
    Ok(value)
}
