//! Configuration-chain verification and monotonic installation (task-m01;
//! design Sections 10.5.1-10.5.3).
//!
//! The chain starts at the trusted genesis: the epoch-one record must match
//! the verified manifest (cluster, domain, epoch, exact voter incarnations)
//! and carry the pinned admin key's signature over its activation message.
//! Every later record must name the next epoch, link to the previous
//! certificate hash and carry approvals from a majority of the previous
//! epoch's voters, each verified against the public key that epoch
//! recorded for that exact incarnation. Nothing else advances an epoch: a
//! larger number, a directory's response, a controller's request or a
//! fresh certificate is not evidence.
//!
//! Historical epochs keep their keys, so old ballot promises, catalog
//! attestations and delayed results verify without a live issuer.
//! [`ClientConfiguration`] is the cached, monotonically installed view a
//! collector or client keeps: hints trigger refreshes, endpoint and
//! observer catalogs move generations within an epoch, and a ballot's fast
//! set is immutable once held. Voter-signed evidence is only accepted for
//! the context it was signed for: a ballot certificate names the cluster,
//! domain and configuration certificate its promisers held, and all three
//! must be this chain's, so evidence cannot be carried between domains
//! that happen to share voter identities and keys.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use coord_consensus::quorum::{BallotConfiguration, ConfigurationError};
use coord_types::config_v1::{
    ActivationEvidenceV1, BallotConfigurationV1, BootstrapResponseV1, ConfigError,
    ConfigurationHintV1, EndpointCatalogV1, GroupConfigurationV1, ObserverCatalogV1,
    QuorumPolicyId, VoterSignatureV1, limits,
};
use coord_types::identity::Digest32;
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, crypto};

use crate::genesis::{GenesisManifest, b64url, b64url_decode};
use crate::membership::{Membership, MembershipError};

/// Why signing failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignError {
    /// The key is not an ES256 signing key.
    Key,
}

/// Sign a canonical message with a voter's (or the admin's) ES256 key.
/// Returns the fixed 64-byte `r || s` signature.
pub fn sign_message(key: &EncodingKey, message: &Digest32) -> Result<Vec<u8>, SignError> {
    let encoded = crypto::sign(&message.0, key, Algorithm::ES256).map_err(|_| SignError::Key)?;
    let raw = b64url_decode(&encoded).ok_or(SignError::Key)?;
    if raw.len() != limits::SIGNATURE_BYTES {
        return Err(SignError::Key);
    }
    Ok(raw)
}

/// Verify a fixed ES256 signature against an uncompressed P-256 public key
/// point. Any malformed input is simply "not verified".
pub fn verify_signature(public_key: &[u8], message: &Digest32, signature: &[u8]) -> bool {
    if public_key.len() != limits::PUBLIC_KEY_BYTES || signature.len() != limits::SIGNATURE_BYTES {
        return false;
    }
    let key = DecodingKey::from_ec_der(public_key);
    crypto::verify(&b64url(signature), &message.0, &key, Algorithm::ES256).unwrap_or(false)
}

/// The trusted root of the chain: the verified genesis manifest and the
/// pinned admin key that signed it.
pub struct GenesisAnchor {
    manifest_digest: Digest32,
    membership: Membership,
    admin: DecodingKey,
}

impl GenesisAnchor {
    /// Build from a manifest that [`crate::genesis::verify_genesis`] accepted
    /// and the pinned admin key it was verified with.
    pub fn new(manifest: &GenesisManifest, admin: DecodingKey) -> Result<Self, MembershipError> {
        Ok(GenesisAnchor {
            manifest_digest: manifest.digest(),
            membership: Membership::from_genesis(manifest)?,
            admin,
        })
    }

    /// Manifest digest.
    pub const fn manifest_digest(&self) -> Digest32 {
        self.manifest_digest
    }

    /// The initial membership.
    pub const fn membership(&self) -> &Membership {
        &self.membership
    }
}

/// Why a record did not extend the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainError {
    /// Malformed record.
    Shape(ConfigError),
    /// Another cluster.
    ClusterMismatch,
    /// Another domain.
    DomainMismatch,
    /// The root record is not genesis evidence, or a later one is.
    EvidenceKind,
    /// The root's epoch is not the manifest's.
    GenesisEpochMismatch,
    /// The root names another manifest.
    GenesisManifestMismatch,
    /// The root's voters differ from the manifest's exact incarnations.
    GenesisVotersMismatch,
    /// The admin signature does not verify.
    GenesisSignature,
    /// The epoch is not the next one (a gap or a fabricated larger epoch).
    EpochNotNext {
        /// Expected.
        expected: ConfigurationEpoch,
        /// Found.
        found: ConfigurationEpoch,
    },
    /// The record does not link to the current certificate.
    PreviousCertificateMismatch,
    /// The handoff names another old epoch than the current one.
    HandoffOldEpoch,
    /// An approval comes from a node that is not a voter of the old epoch
    /// (an observer, a learner or a stranger).
    ApprovalNotVoter {
        /// Node.
        node: ReplicaId,
    },
    /// An approval names an incarnation other than the committed one.
    ApprovalWrongIncarnation {
        /// Node.
        node: ReplicaId,
    },
    /// An approval signature does not verify against the recorded key.
    ApprovalSignature {
        /// Node.
        node: ReplicaId,
    },
    /// Fewer valid approvals than a majority of the old voters.
    InsufficientApprovals {
        /// Valid approvals.
        have: usize,
        /// Majority size.
        need: usize,
    },
    /// A record for an already held epoch whose certificate is not the held
    /// one.
    Divergent,
    /// A record below the held epoch: never installed.
    Stale,
}

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainError::Shape(e) => write!(f, "malformed record: {e}"),
            ChainError::EpochNotNext { expected, found } => {
                write!(f, "epoch {found} is not the next epoch {expected}")
            }
            ChainError::InsufficientApprovals { have, need } => {
                write!(f, "{have} valid approvals, majority needs {need}")
            }
            other => write!(f, "{other:?}"),
        }
    }
}

impl std::error::Error for ChainError {}

impl From<ConfigError> for ChainError {
    fn from(e: ConfigError) -> Self {
        ChainError::Shape(e)
    }
}

/// Why a piece of voter-signed evidence was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceError {
    /// The chain holds no such epoch.
    UnknownEpoch,
    /// The signer is not a voter of the epoch.
    NotVoter {
        /// Node.
        node: ReplicaId,
    },
    /// The signer's incarnation is not the committed one.
    WrongIncarnation {
        /// Node.
        node: ReplicaId,
    },
    /// The signature does not verify.
    Signature {
        /// Node.
        node: ReplicaId,
    },
    /// The same voter signed twice.
    Duplicate {
        /// Node.
        node: ReplicaId,
    },
    /// Fewer valid signers than required.
    Insufficient {
        /// Valid signers.
        have: usize,
        /// Required.
        need: usize,
    },
}

/// Why a ballot configuration was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BallotError {
    /// Malformed.
    Shape(ConfigError),
    /// Promise evidence rejected.
    Evidence(EvidenceError),
    /// The fast set violates the source quorum rules.
    Quorum(ConfigurationError),
    /// The record's policy differs from the epoch's.
    PolicyMismatch,
    /// The promises were made in another cluster or domain.
    OriginMismatch,
    /// The promises name another configuration record for the epoch than
    /// the one the chain holds.
    CertificateMismatch,
    /// A different fast set under a ballot already held: the fast set is
    /// immutable within a ballot; a change needs a higher ballot.
    FastSetChanged,
    /// Below the ballot already held.
    Stale,
}

/// Why a catalog was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// Malformed.
    Shape(ConfigError),
    /// Attestation rejected.
    Evidence(EvidenceError),
    /// Another cluster or domain.
    OriginMismatch,
    /// The catalog names an epoch the chain does not hold (refresh the
    /// chain first; a catalog never introduces an epoch).
    UnknownEpoch,
    /// An endpoint names a node that is not a voter of the epoch.
    NotVoter {
        /// Node.
        node: ReplicaId,
    },
    /// An endpoint names a voter under another incarnation.
    WrongIncarnation {
        /// Node.
        node: ReplicaId,
    },
    /// An observer entry names a voter of the epoch.
    ObserverIsVoter {
        /// Node.
        node: ReplicaId,
    },
    /// Not newer than the catalog already held.
    Stale,
}

/// The committed voters of one epoch, and the key each of them signs
/// with.
///
/// A catalog is attested by a voter, so verifying one needs exactly this
/// and nothing else: who the voters are, which generation of each the
/// cluster committed to, and what key that generation stands for.
/// [`Configurations`] can answer it for any epoch of the chain;
/// [`Membership`] can answer it for the one epoch it holds.
///
/// It exists so the answer is the same either way. A daemon that has a
/// committed membership and no configuration chain should not need a
/// second implementation of an evidence check -- a second implementation
/// is a second set of rules, and the weaker one decides.
///
/// [`Membership`]: crate::membership::Membership
pub trait VoterAuthority {
    /// The cluster these voters belong to.
    fn cluster(&self) -> ClusterId;
    /// The domain.
    fn domain(&self) -> DomainId;
    /// The epoch they are the voters of.
    fn epoch(&self) -> ConfigurationEpoch;
    /// The committed incarnation and signing key of `node`, if it is a
    /// voter of this epoch.
    fn committed(&self, node: &ReplicaId) -> Option<(ReplicaIncarnation, &[u8])>;
}

impl VoterAuthority for VerifiedConfiguration {
    fn cluster(&self) -> ClusterId {
        self.record.cluster
    }
    fn domain(&self) -> DomainId {
        self.record.domain
    }
    fn epoch(&self) -> ConfigurationEpoch {
        self.record.epoch
    }
    fn committed(&self, node: &ReplicaId) -> Option<(ReplicaIncarnation, &[u8])> {
        self.record
            .voter(node)
            .map(|v| (v.incarnation, v.public_key.as_slice()))
    }
}

/// Verify an endpoint catalog against a committed voter set.
///
/// Three things are checked and no others. The catalog is of this
/// cluster, domain and epoch. It is attested by a voter of that epoch,
/// at that voter's committed incarnation, with the key the cluster
/// committed to. And every endpoint it names is a voter of that epoch at
/// its committed incarnation.
///
/// What it deliberately does not check is the addresses. An address is a
/// hint about where to find a voter, never a claim about who that voter
/// is: the connection's own binding decides that, from the certificate,
/// against this same committed set. A catalog that sent a replica to the
/// wrong address costs a failed handshake and nothing else.
pub fn verify_endpoint_catalog(
    authority: &impl VoterAuthority,
    catalog: &EndpointCatalogV1,
) -> Result<(), CatalogError> {
    catalog.validate_shape().map_err(CatalogError::Shape)?;
    if catalog.cluster != authority.cluster() || catalog.domain != authority.domain() {
        return Err(CatalogError::OriginMismatch);
    }
    if catalog.epoch != authority.epoch() {
        return Err(CatalogError::UnknownEpoch);
    }
    let s = &catalog.attestation;
    let Some((incarnation, public_key)) = authority.committed(&s.node) else {
        return Err(CatalogError::Evidence(EvidenceError::NotVoter {
            node: s.node,
        }));
    };
    if incarnation != s.incarnation {
        return Err(CatalogError::Evidence(EvidenceError::WrongIncarnation {
            node: s.node,
        }));
    }
    if !verify_signature(public_key, &catalog.catalog_message(), &s.signature) {
        return Err(CatalogError::Evidence(EvidenceError::Signature {
            node: s.node,
        }));
    }
    for e in &catalog.endpoints {
        match authority.committed(&e.node) {
            None => return Err(CatalogError::NotVoter { node: e.node }),
            Some((inc, _)) if inc != e.incarnation => {
                return Err(CatalogError::WrongIncarnation { node: e.node });
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// A record the chain accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedConfiguration {
    record: GroupConfigurationV1,
    certificate: Digest32,
}

impl VerifiedConfiguration {
    /// The record.
    pub const fn record(&self) -> &GroupConfigurationV1 {
        &self.record
    }
    /// Certificate hash.
    pub const fn certificate(&self) -> Digest32 {
        self.certificate
    }
    /// Epoch.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.record.epoch
    }
    /// Voter identities.
    pub fn voters(&self) -> BTreeSet<ReplicaId> {
        self.record.voter_ids()
    }
    /// Committed incarnation of a voter.
    pub fn incarnation(&self, node: &ReplicaId) -> Option<ReplicaIncarnation> {
        self.record.voter(node).map(|v| v.incarnation)
    }
    /// Whether `node` at `incarnation` is a voter of this epoch.
    pub fn is_voter(&self, node: &ReplicaId, incarnation: ReplicaIncarnation) -> bool {
        self.incarnation(node) == Some(incarnation)
    }

    /// Verify one voter signature of this epoch over `message`.
    fn verify_signer(&self, s: &VoterSignatureV1, message: &Digest32) -> Result<(), EvidenceError> {
        let Some(voter) = self.record.voter(&s.node) else {
            return Err(EvidenceError::NotVoter { node: s.node });
        };
        if voter.incarnation != s.incarnation {
            return Err(EvidenceError::WrongIncarnation { node: s.node });
        }
        if !verify_signature(&voter.public_key, message, &s.signature) {
            return Err(EvidenceError::Signature { node: s.node });
        }
        Ok(())
    }

    /// Verify a set of signatures of this epoch's voters over `message`,
    /// requiring at least `need` distinct valid signers. Any invalid
    /// signature rejects the whole set: evidence is exact, never a loose
    /// count.
    fn verify_signers(
        &self,
        signers: &[VoterSignatureV1],
        message: &Digest32,
        need: usize,
    ) -> Result<(), EvidenceError> {
        let mut seen = BTreeSet::new();
        for s in signers {
            self.verify_signer(s, message)?;
            if !seen.insert(s.node) {
                return Err(EvidenceError::Duplicate { node: s.node });
            }
        }
        if seen.len() < need {
            return Err(EvidenceError::Insufficient {
                have: seen.len(),
                need,
            });
        }
        Ok(())
    }
}

/// The verified configuration chain of one domain.
#[derive(Clone, Debug)]
pub struct ConfigurationChain {
    cluster: ClusterId,
    domain: DomainId,
    records: Vec<VerifiedConfiguration>,
}

impl ConfigurationChain {
    /// Start the chain from the trusted genesis and the epoch-one record.
    pub fn from_genesis(
        anchor: &GenesisAnchor,
        root: GroupConfigurationV1,
    ) -> Result<Self, ChainError> {
        root.validate_shape()?;
        let membership = anchor.membership();
        if root.cluster != membership.cluster() {
            return Err(ChainError::ClusterMismatch);
        }
        if root.domain != membership.domain() {
            return Err(ChainError::DomainMismatch);
        }
        let ActivationEvidenceV1::Genesis {
            manifest_digest,
            admin_signature,
        } = &root.activation
        else {
            return Err(ChainError::EvidenceKind);
        };
        if root.epoch != membership.epoch() {
            return Err(ChainError::GenesisEpochMismatch);
        }
        if *manifest_digest != anchor.manifest_digest() {
            return Err(ChainError::GenesisManifestMismatch);
        }
        let expected: BTreeMap<ReplicaId, ReplicaIncarnation> = membership
            .voters()
            .map(|v| (v.node, v.incarnation))
            .collect();
        let found: BTreeMap<ReplicaId, ReplicaIncarnation> = root
            .voters
            .iter()
            .map(|v| (v.node, v.incarnation))
            .collect();
        if expected != found {
            return Err(ChainError::GenesisVotersMismatch);
        }
        let message = root.activation_message();
        let signature = b64url(admin_signature);
        if !crypto::verify(&signature, &message.0, &anchor.admin, Algorithm::ES256).unwrap_or(false)
        {
            return Err(ChainError::GenesisSignature);
        }
        let certificate = root.certificate_hash();
        Ok(ConfigurationChain {
            cluster: root.cluster,
            domain: root.domain,
            records: vec![VerifiedConfiguration {
                record: root,
                certificate,
            }],
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
    /// The current (highest) epoch.
    pub fn current(&self) -> &VerifiedConfiguration {
        self.records.last().expect("a chain has its root")
    }
    /// A historical or current epoch.
    pub fn at(&self, epoch: ConfigurationEpoch) -> Option<&VerifiedConfiguration> {
        self.records.iter().find(|r| r.epoch() == epoch)
    }
    /// Number of epochs held.
    pub fn len(&self) -> usize {
        self.records.len()
    }
    /// Never empty.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Extend the chain by the next epoch's record.
    pub fn extend(
        &mut self,
        next: GroupConfigurationV1,
    ) -> Result<&VerifiedConfiguration, ChainError> {
        next.validate_shape()?;
        if next.cluster != self.cluster {
            return Err(ChainError::ClusterMismatch);
        }
        if next.domain != self.domain {
            return Err(ChainError::DomainMismatch);
        }
        let head = self.current();
        let expected = head
            .epoch()
            .checked_next()
            .map_err(|_| ChainError::EpochNotNext {
                expected: head.epoch(),
                found: next.epoch,
            })?;
        if next.epoch != expected {
            return Err(ChainError::EpochNotNext {
                expected,
                found: next.epoch,
            });
        }
        let ActivationEvidenceV1::Handoff {
            old_epoch,
            approvals,
            ..
        } = &next.activation
        else {
            return Err(ChainError::EvidenceKind);
        };
        if next.previous_certificate != head.certificate() {
            return Err(ChainError::PreviousCertificateMismatch);
        }
        if *old_epoch != head.epoch() {
            return Err(ChainError::HandoffOldEpoch);
        }
        let message = next.activation_message();
        head.verify_signers(approvals, &message, head.record().majority())
            .map_err(|e| match e {
                EvidenceError::NotVoter { node } => ChainError::ApprovalNotVoter { node },
                EvidenceError::WrongIncarnation { node } => {
                    ChainError::ApprovalWrongIncarnation { node }
                }
                EvidenceError::Signature { node } => ChainError::ApprovalSignature { node },
                EvidenceError::Duplicate { .. } => ChainError::Shape(ConfigError::DuplicateSigner),
                EvidenceError::Insufficient { have, need } => {
                    ChainError::InsufficientApprovals { have, need }
                }
                EvidenceError::UnknownEpoch => ChainError::HandoffOldEpoch,
            })?;
        let certificate = next.certificate_hash();
        self.records.push(VerifiedConfiguration {
            record: next,
            certificate,
        });
        Ok(self.current())
    }

    /// Records strictly after `known` (all of them for `None`), at most
    /// `max`, and whether they reach the current epoch. This is what a
    /// node answers a bootstrap with; the answer carries the evidence, so
    /// the answering node's word is never needed.
    ///
    /// `known_certificate` is the certificate hash the client holds for
    /// `known`. When this chain holds that epoch under another certificate
    /// the two have diverged, and an answer of only later records would let
    /// the client apply nothing and believe itself current. The answer then
    /// starts at `known` itself: the held record is the evidence, and the
    /// client's monotonic installation refuses it as divergent for an epoch
    /// it already holds. Without a certificate there is nothing to compare
    /// and the answer is the plain suffix.
    pub fn records_after(
        &self,
        known: Option<ConfigurationEpoch>,
        known_certificate: Option<Digest32>,
        max: usize,
    ) -> (Vec<GroupConfigurationV1>, bool) {
        let max = max.clamp(1, limits::MAX_CHAIN_RECORDS);
        let diverged = match (known, known_certificate) {
            (Some(k), Some(c)) => self.at(k).is_some_and(|held| held.certificate() != c),
            _ => false,
        };
        let records: Vec<GroupConfigurationV1> = self
            .records
            .iter()
            .filter(|r| known.is_none_or(|k| r.epoch() > k || (diverged && r.epoch() == k)))
            .take(max)
            .map(|r| r.record.clone())
            .collect();
        let complete = records
            .last()
            .map_or(known == Some(self.current().epoch()), |r| {
                r.epoch == self.current().epoch()
            });
        (records, complete)
    }

    /// Verify one voter signature of `epoch` over `message` using the key
    /// that epoch recorded. Works for historical epochs without any
    /// issuer.
    pub fn verify_voter_signature(
        &self,
        epoch: ConfigurationEpoch,
        signature: &VoterSignatureV1,
        message: &Digest32,
    ) -> Result<(), EvidenceError> {
        self.at(epoch)
            .ok_or(EvidenceError::UnknownEpoch)?
            .verify_signer(signature, message)
    }

    /// Verify a ballot configuration against its epoch: promises from a
    /// majority of that epoch's exact voters, made for this chain's own
    /// cluster, domain and configuration record, and a fast set the source
    /// quorum rules accept. Returns the consensus configuration.
    ///
    /// The context is checked before the promises are: a signature can only
    /// say which epoch of which domain it was made for because the message
    /// carries all of it. Two domains that share voter identities,
    /// incarnations and keys otherwise accept each other's certificates
    /// whenever their epoch numbers and policies line up, which installs a
    /// leader and fast set that domain's voters never promised.
    pub fn verify_ballot(
        &self,
        ballot: &BallotConfigurationV1,
    ) -> Result<BallotConfiguration, BallotError> {
        ballot.validate_shape().map_err(BallotError::Shape)?;
        if ballot.cluster != self.cluster || ballot.domain != self.domain {
            return Err(BallotError::OriginMismatch);
        }
        let epoch = self
            .at(ballot.epoch)
            .ok_or(BallotError::Evidence(EvidenceError::UnknownEpoch))?;
        if ballot.configuration_certificate != epoch.certificate() {
            return Err(BallotError::CertificateMismatch);
        }
        if ballot.quorum_policy != epoch.record().quorum_policy {
            return Err(BallotError::PolicyMismatch);
        }
        let message = ballot.ballot_message();
        epoch
            .verify_signers(&ballot.promises, &message, epoch.record().majority())
            .map_err(BallotError::Evidence)?;
        let voters = epoch.voters();
        let fast_set: BTreeSet<ReplicaId> = ballot.fast_set.iter().copied().collect();
        let config = match ballot.quorum_policy {
            QuorumPolicyId::C1_THREE_QUARTERS => {
                if fast_set != voters {
                    return Err(BallotError::Quorum(ConfigurationError::FastSetNotVoters));
                }
                BallotConfiguration::c1(ballot.epoch, ballot.ballot, voters)
            }
            _ => BallotConfiguration::c2(ballot.epoch, ballot.ballot, voters, fast_set),
        }
        .map_err(BallotError::Quorum)?;
        Ok(config)
    }

    fn catalog_epoch(
        &self,
        cluster: ClusterId,
        domain: DomainId,
        epoch: ConfigurationEpoch,
    ) -> Result<&VerifiedConfiguration, CatalogError> {
        if cluster != self.cluster || domain != self.domain {
            return Err(CatalogError::OriginMismatch);
        }
        self.at(epoch).ok_or(CatalogError::UnknownEpoch)
    }

    /// Verify an endpoint catalog: attested by a voter of its epoch and
    /// naming only that epoch's voters at their committed incarnation.
    ///
    /// The chain's part is finding the epoch; the verification itself is
    /// [`verify_endpoint_catalog`], which any committed voter set can
    /// anchor. A daemon that holds one epoch's membership and no chain
    /// gets the same answer from the same code.
    pub fn verify_endpoints(&self, catalog: &EndpointCatalogV1) -> Result<(), CatalogError> {
        catalog.validate_shape().map_err(CatalogError::Shape)?;
        let epoch = self.catalog_epoch(catalog.cluster, catalog.domain, catalog.epoch)?;
        verify_endpoint_catalog(epoch, catalog)
    }

    /// Verify an observer catalog: attested by a voter of its epoch and
    /// naming no voter of that epoch.
    pub fn verify_observers(&self, catalog: &ObserverCatalogV1) -> Result<(), CatalogError> {
        catalog.validate_shape().map_err(CatalogError::Shape)?;
        let epoch = self.catalog_epoch(catalog.cluster, catalog.domain, catalog.epoch)?;
        epoch
            .verify_signer(&catalog.attestation, &catalog.catalog_message())
            .map_err(CatalogError::Evidence)?;
        for o in &catalog.observers {
            if epoch.incarnation(&o.node).is_some() {
                return Err(CatalogError::ObserverIsVoter { node: o.node });
            }
        }
        Ok(())
    }
}

/// Outcome of an installation attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Installed {
    /// Newer than what was held: installed.
    Advanced,
    /// Exactly what was held: nothing changed.
    AlreadyHeld,
}

/// What a hint asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HintDecision {
    /// Another cluster or domain: ignored.
    Foreign,
    /// Nothing newer than what is held.
    UpToDate,
    /// Fetch the chain through this epoch (the hint itself installs nothing).
    RefreshConfiguration {
        /// Epoch the sender holds.
        epoch: ConfigurationEpoch,
    },
    /// Fetch the endpoint catalog.
    RefreshEndpoints,
    /// Fetch the observer catalog.
    RefreshObservers,
}

/// Result of applying a bootstrap response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapOutcome {
    /// Epochs newly installed.
    pub advanced: usize,
    /// The ballot's outcome, if one was carried.
    pub ballot: Option<Result<(), BallotError>>,
    /// The endpoint catalog's outcome, if one was carried.
    pub endpoints: Option<Result<Installed, CatalogError>>,
}

/// The cached configuration a collector or client holds.
#[derive(Clone, Debug)]
pub struct ClientConfiguration {
    chain: ConfigurationChain,
    ballot: Option<(BallotConfigurationV1, BallotConfiguration)>,
    endpoints: Option<EndpointCatalogV1>,
    observers: Option<ObserverCatalogV1>,
}

impl ClientConfiguration {
    /// Bootstrap from the trusted genesis and the epoch-one record.
    pub fn bootstrap(
        anchor: &GenesisAnchor,
        root: GroupConfigurationV1,
    ) -> Result<Self, ChainError> {
        Ok(ClientConfiguration {
            chain: ConfigurationChain::from_genesis(anchor, root)?,
            ballot: None,
            endpoints: None,
            observers: None,
        })
    }

    /// The verified chain.
    pub const fn chain(&self) -> &ConfigurationChain {
        &self.chain
    }
    /// The current epoch.
    pub fn current(&self) -> &VerifiedConfiguration {
        self.chain.current()
    }
    /// The held ballot configuration, if any.
    pub fn ballot(&self) -> Option<&BallotConfiguration> {
        self.ballot.as_ref().map(|(_, c)| c)
    }
    /// The held endpoint catalog, if any.
    pub const fn endpoints(&self) -> Option<&EndpointCatalogV1> {
        self.endpoints.as_ref()
    }
    /// The held observer catalog, if any.
    pub const fn observers(&self) -> Option<&ObserverCatalogV1> {
        self.observers.as_ref()
    }

    /// Install a configuration record: monotonic, verified against the
    /// held chain. A record for a held epoch must name the held
    /// certificate.
    ///
    /// The held epoch is compared by certificate, not by bytes. Another
    /// well-formed copy of the same activation (its approvals trimmed to a
    /// different majority, or re-signed) is the same record with other
    /// evidence, and refusing it as divergent would make every honest node
    /// that relayed a different copy look like a fork. A copy that is not
    /// in canonical form is still refused as malformed.
    pub fn install_configuration(
        &mut self,
        record: GroupConfigurationV1,
    ) -> Result<Installed, ChainError> {
        let current = self.chain.current().epoch();
        if record.epoch < current {
            return Err(ChainError::Stale);
        }
        if record.epoch == current {
            record.validate_shape()?;
            return if self.chain.current().certificate() == record.certificate_hash() {
                Ok(Installed::AlreadyHeld)
            } else {
                Err(ChainError::Divergent)
            };
        }
        self.chain.extend(record)?;
        // A new epoch voids the held ballot: its quorum belonged to the old
        // voters. Endpoint and observer catalogs of the old epoch are kept
        // for routing until the new epoch's arrive.
        self.ballot = None;
        Ok(Installed::Advanced)
    }

    /// Apply a bootstrap response: records in order, then the optional
    /// ballot and endpoints. The first rejected record stops the records.
    pub fn apply_bootstrap(
        &mut self,
        response: &BootstrapResponseV1,
    ) -> Result<BootstrapOutcome, ChainError> {
        response.validate_shape()?;
        let mut advanced = 0;
        for record in &response.records {
            if self.install_configuration(record.clone())? == Installed::Advanced {
                advanced += 1;
            }
        }
        let ballot = response
            .ballot
            .as_ref()
            .map(|b| self.install_ballot(b.clone()));
        let endpoints = response
            .endpoints
            .as_ref()
            .map(|e| self.install_endpoints(e.clone()));
        Ok(BootstrapOutcome {
            advanced,
            ballot,
            endpoints,
        })
    }

    /// Install a ballot configuration of the current epoch. A ballot's
    /// fast set is immutable once held; a lower ballot is stale.
    pub fn install_ballot(&mut self, ballot: BallotConfigurationV1) -> Result<(), BallotError> {
        if ballot.epoch != self.chain.current().epoch() {
            return Err(if ballot.epoch < self.chain.current().epoch() {
                BallotError::Stale
            } else {
                BallotError::Evidence(EvidenceError::UnknownEpoch)
            });
        }
        let config = self.chain.verify_ballot(&ballot)?;
        if let Some((held, _)) = &self.ballot {
            match held.ballot.compare_same_epoch(&ballot.ballot) {
                Some(std::cmp::Ordering::Greater) => return Err(BallotError::Stale),
                Some(std::cmp::Ordering::Equal) => {
                    return if held.fast_set == ballot.fast_set {
                        Ok(())
                    } else {
                        Err(BallotError::FastSetChanged)
                    };
                }
                Some(std::cmp::Ordering::Less) | None => {}
            }
        }
        self.ballot = Some((ballot, config));
        Ok(())
    }

    /// Decide what an authenticated hint asks for. A hint never installs
    /// anything.
    pub fn observe_hint(&self, hint: &ConfigurationHintV1) -> HintDecision {
        if hint.cluster != self.chain.cluster() || hint.domain != self.chain.domain() {
            return HintDecision::Foreign;
        }
        let current = self.chain.current();
        if hint.epoch > current.epoch() {
            return HintDecision::RefreshConfiguration { epoch: hint.epoch };
        }
        if hint.epoch < current.epoch() {
            return HintDecision::UpToDate;
        }
        if hint.certificate != current.certificate() {
            // Same epoch, other certificate: the sender diverged; nothing to
            // fetch from it.
            return HintDecision::Foreign;
        }
        if self
            .endpoints
            .as_ref()
            .is_none_or(|e| e.epoch < hint.epoch || e.generation < hint.endpoint_generation)
        {
            return HintDecision::RefreshEndpoints;
        }
        if self
            .observers
            .as_ref()
            .is_none_or(|o| o.epoch < hint.epoch || o.generation < hint.catalog_generation)
        {
            return HintDecision::RefreshObservers;
        }
        HintDecision::UpToDate
    }

    /// Install an endpoint catalog of the current epoch, monotonic by
    /// generation. The voter set is untouched whatever the catalog says.
    pub fn install_endpoints(
        &mut self,
        catalog: EndpointCatalogV1,
    ) -> Result<Installed, CatalogError> {
        let current = self.chain.current().epoch();
        if catalog.epoch < current {
            return Err(CatalogError::Stale);
        }
        self.chain.verify_endpoints(&catalog)?;
        if let Some(held) = &self.endpoints
            && held.epoch == catalog.epoch
        {
            if held.generation > catalog.generation {
                return Err(CatalogError::Stale);
            }
            if held.generation == catalog.generation {
                return if *held == catalog {
                    Ok(Installed::AlreadyHeld)
                } else {
                    Err(CatalogError::Stale)
                };
            }
        }
        self.endpoints = Some(catalog);
        Ok(Installed::Advanced)
    }

    /// Install an observer catalog of the current epoch, monotonic by
    /// generation. Quorum size is untouched whatever the catalog says.
    pub fn install_observers(
        &mut self,
        catalog: ObserverCatalogV1,
    ) -> Result<Installed, CatalogError> {
        let current = self.chain.current().epoch();
        if catalog.epoch < current {
            return Err(CatalogError::Stale);
        }
        self.chain.verify_observers(&catalog)?;
        if let Some(held) = &self.observers
            && held.epoch == catalog.epoch
        {
            if held.generation > catalog.generation {
                return Err(CatalogError::Stale);
            }
            if held.generation == catalog.generation {
                return if *held == catalog {
                    Ok(Installed::AlreadyHeld)
                } else {
                    Err(CatalogError::Stale)
                };
            }
        }
        self.observers = Some(catalog);
        Ok(Installed::Advanced)
    }
}
