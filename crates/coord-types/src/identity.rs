//! Stable invocation identity and domain-separated digests (design Sections
//! 4.4 and 10.5.1).
//!
//! ```text
//! retry_key  = (cluster_id, domain_id, session_id, client_instance_id, request_sequence)
//! command_id = H(protocol_domain, retry_key, canonical_operation)
//! ```
//!
//! Tokens, endpoint generations, membership epochs, ballots, connection and
//! stream identifiers are [`AdmissionContext`], never part of the hash, so a
//! retry after any of them changes keeps the same identity. A different
//! canonical payload under the same retry key is a
//! [`IdentityError::RequestIdentityConflict`].

use core::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{IdentityError, ValidationError};
use crate::ids::{
    Ballot, ClientInstanceId, ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration, LeaseId,
    RequestSequence, SessionId,
};
use crate::logical_v1::LogicalRequest;

/// A 32-byte BLAKE3 output.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest32(pub [u8; 32]);

impl fmt::Debug for Digest32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Digest32(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        f.write_str(")")
    }
}

/// Hash domains. Each purpose uses a distinct BLAKE3 derive-key context, so
/// a command identity can never collide with a checkpoint root or result
/// digest even for identical input bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HashDomain {
    /// Command identity: retry key plus canonical logical request.
    CommandId,
    /// Digest of an established command result.
    CommandResult,
    /// Digest of a persisted batch (journal record) and its guard context.
    JournalBatch,
    /// Root of a `SharedCheckpointV1` canonical traversal.
    SharedCheckpointRoot,
    /// Root of a `LocalRecoveryCheckpointV1`.
    LocalCheckpointRoot,
    /// Chain digest of a finalized frame (task-o01).
    FinalizedFrame,
    /// One-time grant/code commitments stored in `auth_grant_v1`.
    AuthGrantCommitment,
    /// Dependency-path chain digests of per-key conflict logs (task-21).
    DependencyPath,
    /// Admission receipt identities minted at the trusted boundary (task-33).
    AdmissionReceipt,
    /// Hidden private TTL binding identities of Kine writes, derived by the
    /// trusted collector from the stable retry key (task-46; Section 6.6).
    KineBinding,
    /// Certificate hash of a `GroupConfigurationV1` record (task-m01).
    ConfigurationRecord,
    /// Message an old quorum or the genesis admin signs to activate a
    /// configuration (task-m01).
    ConfigurationActivation,
    /// Message the voters sign to bind a ballot's leader and fast set
    /// (task-m01).
    ConfigurationBallot,
    /// Message a voter signs to attest an endpoint or observer catalog
    /// (task-m01).
    ConfigurationCatalog,
}

impl HashDomain {
    /// The derive-key context string. These strings are frozen.
    pub const fn context(self) -> &'static str {
        match self {
            HashDomain::CommandId => "tuplesky coord.v1 2026-09 command-id",
            HashDomain::CommandResult => "tuplesky coord.v1 2026-09 command-result",
            HashDomain::JournalBatch => "tuplesky coord.v1 2026-09 journal-batch",
            HashDomain::SharedCheckpointRoot => "tuplesky coord.v1 2026-09 shared-checkpoint-root",
            HashDomain::LocalCheckpointRoot => "tuplesky coord.v1 2026-09 local-checkpoint-root",
            HashDomain::FinalizedFrame => "tuplesky coord.v1 2026-09 finalized-frame",
            HashDomain::AuthGrantCommitment => "tuplesky coord.v1 2026-09 auth-grant-commitment",
            HashDomain::DependencyPath => "tuplesky coord.v1 2026-09 dependency-path",
            HashDomain::AdmissionReceipt => "tuplesky coord.v1 2026-09 admission-receipt",
            HashDomain::KineBinding => "tuplesky coord.v1 2026-09 kine-binding",
            HashDomain::ConfigurationRecord => "tuplesky coord.v1 2026-09 configuration-record",
            HashDomain::ConfigurationActivation => {
                "tuplesky coord.v1 2026-09 configuration-activation"
            }
            HashDomain::ConfigurationBallot => "tuplesky coord.v1 2026-09 configuration-ballot",
            HashDomain::ConfigurationCatalog => "tuplesky coord.v1 2026-09 configuration-catalog",
        }
    }

    /// Hash length-prefixed parts under this domain. Length prefixes keep
    /// `["ab","c"]` and `["a","bc"]` distinct.
    pub fn digest(self, parts: &[&[u8]]) -> Digest32 {
        let mut hasher = blake3::Hasher::new_derive_key(self.context());
        for part in parts {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        Digest32(*hasher.finalize().as_bytes())
    }
}

/// The stable retry key of one client invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RetryKey {
    /// Cluster/restore identity.
    pub cluster_id: ClusterId,
    /// Domain.
    pub domain_id: DomainId,
    /// Replicated session.
    pub session_id: SessionId,
    /// Client process instance.
    pub client_instance_id: ClientInstanceId,
    /// Per-instance request sequence.
    pub request_sequence: RequestSequence,
}

impl RetryKey {
    /// Fixed 72-byte canonical encoding: four 16-byte IDs then the big-endian
    /// sequence.
    pub fn canonical_bytes(&self) -> [u8; 72] {
        let mut out = [0u8; 72];
        out[0..16].copy_from_slice(self.cluster_id.as_bytes());
        out[16..32].copy_from_slice(self.domain_id.as_bytes());
        out[32..48].copy_from_slice(self.session_id.as_bytes());
        out[48..64].copy_from_slice(self.client_instance_id.as_bytes());
        out[64..72].copy_from_slice(&self.request_sequence.to_be_bytes());
        out
    }
}

/// Command identity: a domain-separated digest of retry key and canonical
/// logical request.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommandId(pub Digest32);

impl fmt::Debug for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommandId{:?}", self.0)
    }
}

impl CommandId {
    /// Derive the identity. The request must be valid and canonical.
    pub fn derive(retry_key: &RetryKey, request: &LogicalRequest) -> Result<Self, ValidationError> {
        let payload = request.canonical_bytes()?;
        Ok(CommandId(
            HashDomain::CommandId.digest(&[&retry_key.canonical_bytes(), &payload]),
        ))
    }

    /// Borrow the raw digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0.0
    }
}

/// The hidden private TTL binding identity of a Kine create/update: the
/// first sixteen bytes of the [`HashDomain::KineBinding`] digest of the
/// retry key's canonical bytes. Every invocation therefore names a fresh
/// binding (identities are never reused, Section 6.6), a transport retry
/// of the same invocation reproduces the same one, and the request payload
/// (which carries the binding) never feeds back into its own derivation.
pub fn kine_binding_id(retry_key: &RetryKey) -> LeaseId {
    let digest = HashDomain::KineBinding.digest(&[&retry_key.canonical_bytes()]);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.0[..16]);
    LeaseId(out)
}

/// Envelope/admission context that accompanies a request but is **not** part
/// of its identity. Changing any field is a retry of the same request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AdmissionContext {
    /// Membership epoch the collector believed current.
    pub epoch: ConfigurationEpoch,
    /// Ballot the collector targeted, if known.
    pub ballot: Option<Ballot>,
    /// Endpoint/routing generation.
    pub endpoint_generation: EndpointGeneration,
    /// Opaque handle of the admitted credential (never the token itself).
    pub credential_handle: u64,
    /// Transport connection identifier (diagnostic only).
    pub connection_id: u64,
    /// Transport stream identifier (diagnostic only).
    pub stream_id: u64,
}

/// Outcome of presenting a command identity under a retry key that may
/// already be bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityCheck {
    /// First presentation of this retry key: bind it.
    New,
    /// Same identity as previously bound: a retry of the same request.
    Retry,
}

/// The rule of Section 4.4: at most the first payload under a retry key is
/// accepted; a different payload is a conflict.
pub fn check_identity(
    bound: Option<&CommandId>,
    presented: &CommandId,
) -> Result<IdentityCheck, IdentityError> {
    match bound {
        None => Ok(IdentityCheck::New),
        Some(existing) if existing == presented => Ok(IdentityCheck::Retry),
        Some(_) => Err(IdentityError::RequestIdentityConflict),
    }
}

/// Bounded outstanding window for request sequences of one client instance
/// (Section 6.5): requests at or below the retired floor are `TooOld`, and a
/// sequence cannot jump beyond the window above the floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceWindow {
    /// Highest retired sequence; everything at or below is never new work.
    pub floor: RequestSequence,
    /// Maximum number of sequences above the floor that may be outstanding.
    pub width: u32,
}

impl SequenceWindow {
    /// Classify a presented sequence.
    pub fn admit(&self, sequence: RequestSequence) -> Result<(), IdentityError> {
        if sequence <= self.floor {
            return Err(IdentityError::SequenceTooOld);
        }
        let distance = sequence.get() - self.floor.get();
        if distance > u64::from(self.width) {
            return Err(IdentityError::SequenceOutOfWindow);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_window_rules() {
        let w = SequenceWindow {
            floor: RequestSequence::new(10).unwrap(),
            width: 4,
        };
        assert_eq!(
            w.admit(RequestSequence::new(10).unwrap()),
            Err(IdentityError::SequenceTooOld)
        );
        assert_eq!(
            w.admit(RequestSequence::new(3).unwrap()),
            Err(IdentityError::SequenceTooOld)
        );
        assert_eq!(w.admit(RequestSequence::new(11).unwrap()), Ok(()));
        assert_eq!(w.admit(RequestSequence::new(14).unwrap()), Ok(()));
        assert_eq!(
            w.admit(RequestSequence::new(15).unwrap()),
            Err(IdentityError::SequenceOutOfWindow)
        );
    }

    #[test]
    fn hash_domains_are_separated_and_length_prefixed() {
        let a = HashDomain::CommandId.digest(&[b"ab", b"c"]);
        let b = HashDomain::CommandId.digest(&[b"a", b"bc"]);
        let c = HashDomain::CommandResult.digest(&[b"ab", b"c"]);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, HashDomain::CommandId.digest(&[b"ab", b"c"]));
    }
}
