//! The unique handoff certificate (task-56; design Sections 4.8-4.9,
//! 10.3, 17.6).
//!
//! After the old configuration is sealed (task-55) and before the
//! successor installs anything (task-57), exactly one thing has to be
//! decided: *what state the successor inherits*. Task-54 says what may
//! conclude it -- a majority of the old voters, after the seal,
//! agreeing on one root and one successor -- and this is the durable
//! form of that conclusion.
//!
//! [`TerminalStateV1`] is what a sealed old voter reports. Every field
//! is something the configuration already decided, never something a
//! coordinator chose:
//!
//! * the boundary: execution position, KV revision, retention floor and
//!   lease authority epoch, exactly as a shared checkpoint names them;
//! * `state_root`: the root of the shared checkpoint of the terminal
//!   common state -- history, retries, results, leases, sessions and
//!   policy are inside it, because they are inside the common
//!   collections;
//! * `closure_root`: a digest of the source-defined selection over the
//!   reports at the seal cut. A command that was potentially chosen
//!   immediately before the fence is in that selection, so a latent old
//!   completion stays represented rather than being lost between the
//!   configurations;
//! * `floor`: the highest activated checkpoint floor (task-53) at the
//!   boundary, so the successor inherits the lineage and cannot be
//!   asked for history the old configuration had already agreed to
//!   forget;
//! * `successors`: the exact incarnations. A different successor set is
//!   a different terminal root, which is what makes racing successor
//!   sets unable to both obtain authority rather than something a check
//!   has to catch.
//!
//! The root binds all of it, so "mixed evidence" is not a judgement
//! call: two reports either produce the same 32 bytes or they do not.
//!
//! What makes the selection stable across a restart is that it is a
//! row. [`publish_certificate`] refuses to replace a published certificate
//! with a different one for the same transition and accepts the
//! identical one, so a replacement coordinator reuses the decision
//! rather than recomputing one that might differ. A full KV snapshot is
//! not sufficient and there is nowhere here to put one: what is bound
//! is the root of a verified checkpoint plus the selection, and neither
//! can be produced by copying files.
//!
//! The module writes rows and computes digests; it drives nothing.
//! Asking the old voters for their reports and telling the successor to
//! install are task-57's.

use std::collections::{BTreeMap, BTreeSet};

use coord_consensus::handoff::{
    HandoffError, SealCertificate, TerminalReport, Transition, select_terminal,
};
use coord_consensus::quorum::EpochVoters;
use coord_consensus::recovery::SyncDecision;
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{EngineError, OrderedRead};
use coord_store_api::registry::Collection;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use serde::{Deserialize, Serialize};

use crate::manifest::CheckpointBoundary;
use crate::trim::{TrimError, decode_record, encode_record};

/// Record kind of the published terminal certificate.
pub const TERMINAL_RECORD_KIND: u16 = 0x0006;
/// Key of the published terminal certificate in `checkpoint_v1`.
pub const TERMINAL_KEY: &[u8] = b"terminal_certificate_v1";

/// What one sealed old voter reports as the terminal state.
///
/// Every field is something the configuration already decided. There is
/// nowhere here for a coordinator to put a choice, which is the point:
/// the certificate is the old quorum's conclusion, and a coordinator
/// that could contribute to it could contribute a different one after a
/// restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalStateV1 {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// The transition this is the terminal state of.
    pub transition: Transition,
    /// The closed boundary: execution position, KV revision, retention
    /// floor and lease authority epoch.
    pub boundary: CheckpointBoundary,
    /// Root of the shared checkpoint of the terminal common state.
    /// History, retries, results, leases, sessions and policy are
    /// inside it because they are inside the common collections.
    pub state_root: Digest32,
    /// Digest of the source-defined selection over the reports at the
    /// seal cut: the potentially chosen commands and their closure.
    pub closure_root: Digest32,
    /// Subject of the highest activated checkpoint floor at the
    /// boundary, or zero when the configuration never activated one.
    pub floor: Digest32,
    /// The exact successor incarnations.
    pub successors: BTreeMap<ReplicaId, ReplicaIncarnation>,
}

/// The 32 bytes a `SyncDecision` is bound by.
///
/// The selection is what resolves potentially chosen commands and their
/// dependency closure, so binding it is what keeps a latent old
/// completion represented across the configurations: a command that was
/// chosen immediately before the fence is in the selection, and a
/// terminal state that omitted it would have a different root.
///
/// What is bound is the selection's content -- the adopted entries, the
/// commands left for re-proposal and the synchronized ballot they were
/// selected from -- and not `ballot`, the ballot the recovery ran
/// under. That one is the coordinator's choice: a replacement repeating
/// the same terminal recovery after its predecessor died picks a higher
/// ballot and selects the same commands, and two old voters agreeing on
/// every command must produce the same root however many coordinators
/// asked them.
pub fn closure_root(decision: &SyncDecision) -> Digest32 {
    let source = postcard::to_allocvec(&decision.source_ballot).unwrap_or_default();
    let entries = postcard::to_allocvec(&decision.entries).unwrap_or_default();
    let reproposed = postcard::to_allocvec(&decision.reproposed).unwrap_or_default();
    HashDomain::HandoffClosure.digest(&[&source, &entries, &reproposed])
}

impl TerminalStateV1 {
    /// The 32 bytes that are the terminal state.
    ///
    /// Everything is inside, the successor incarnations included, so a
    /// different successor set is a different root rather than
    /// something a check has to catch. Written out field by field at
    /// fixed widths: old voters compute it independently and compare
    /// the results, so what it is derived from has to be a decision
    /// rather than a consequence of how a struct serializes today.
    pub fn terminal_root(&self) -> Digest32 {
        let mut parts: Vec<Vec<u8>> = vec![
            self.cluster.as_bytes().to_vec(),
            self.domain.as_bytes().to_vec(),
            self.transition.from.to_be_bytes().to_vec(),
            self.transition.to.to_be_bytes().to_vec(),
            self.transition.subject.0.to_vec(),
            self.boundary.execution_position.to_be_bytes().to_vec(),
            self.boundary.kv_revision.to_be_bytes().to_vec(),
            self.boundary.retention_floor.to_be_bytes().to_vec(),
            self.boundary.lease_authority.to_be_bytes().to_vec(),
            self.state_root.0.to_vec(),
            self.closure_root.0.to_vec(),
            self.floor.0.to_vec(),
            (self.successors.len() as u64).to_be_bytes().to_vec(),
        ];
        for (replica, incarnation) in &self.successors {
            parts.push(replica.as_bytes().to_vec());
            parts.push(incarnation.get().to_be_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        HashDomain::HandoffTerminalRoot.digest(&refs)
    }

    /// The report a sealed old voter sends.
    pub fn report(&self, voter: ReplicaId) -> TerminalReport {
        TerminalReport {
            voter,
            transition: self.transition,
            terminal_root: self.terminal_root(),
        }
    }
}

/// The selected certificate, durably.
///
/// It carries the state rather than only its root, because the
/// successor has to install the state and a root alone says only
/// whether what it installed was right.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalCertificateV1 {
    /// The terminal state every signer agreed on.
    pub state: TerminalStateV1,
    /// The old voters that reported it.
    pub signers: BTreeSet<ReplicaId>,
}

impl TerminalCertificateV1 {
    /// The root the successor must install.
    pub fn terminal_root(&self) -> Digest32 {
        self.state.terminal_root()
    }

    /// The transition it settles.
    pub const fn transition(&self) -> Transition {
        self.state.transition
    }

    /// The exact successor, as the epoch's voters.
    pub fn successor_voters(&self) -> Option<EpochVoters> {
        EpochVoters::new(
            self.state.transition.to,
            self.state.successors.keys().copied().collect(),
        )
    }

    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(TERMINAL_RECORD_KIND, self, "terminal certificate encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(TERMINAL_RECORD_KIND, bytes, "terminal certificate")
    }
}

/// Select the terminal certificate from what a majority of the sealed
/// old voters reported.
///
/// The quorum rule is [`select_terminal`]'s and the seal is required by
/// its signature, so there is no selection before the fence, and every
/// reporter must be one of the seal's signers: the certificate is a
/// majority of *fenced* voters, never a majority some unfenced one
/// completes (see [`select_terminal`]). What this
/// adds is the state itself: the reports carry roots, and a certificate
/// has to carry what the root is *of*, so the caller supplies the
/// states and every one of them must produce the root its reporter
/// sent.
pub fn select_certificate(
    voters: &EpochVoters,
    seal: &SealCertificate,
    reports: &[(ReplicaId, TerminalStateV1)],
) -> Result<TerminalCertificateV1, TrimError> {
    let transition = seal.transition();
    let mut states: BTreeMap<Digest32, TerminalStateV1> = BTreeMap::new();
    let mut offered = Vec::new();
    for (voter, state) in reports {
        if state.transition != transition {
            return Err(TrimError::Handoff(HandoffError::WrongTransition));
        }
        let root = state.terminal_root();
        states.insert(root, state.clone());
        offered.push(state.report(*voter));
    }
    let successor = states
        .values()
        .next()
        .map(|s| s.successors.keys().copied().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let certified =
        select_terminal(voters, seal, &successor, &offered).map_err(TrimError::Handoff)?;
    let state = states
        .remove(&certified.terminal_root())
        .ok_or(TrimError::Handoff(HandoffError::MixedTerminal))?;
    Ok(TerminalCertificateV1 {
        state,
        signers: certified.signers().clone(),
    })
}

/// What is already published here, if anything.
pub fn published_certificate<V: OrderedRead>(
    view: &V,
) -> Result<Option<TerminalCertificateV1>, TrimError> {
    match view.get(Collection::CheckpointV1.id(), TERMINAL_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(TerminalCertificateV1::decode(&bytes)?)),
    }
}

/// The update publishing `next`, checked against what is there.
///
/// A published certificate is the selection, and it is stable: the
/// identical one republishes and any other is refused. That is what a
/// replacement coordinator relies on -- it reuses the decision rather
/// than recomputing one that might differ -- and it is why a restart in
/// the middle of a handoff cannot produce a second destination.
pub fn publish_certificate(
    next: &TerminalCertificateV1,
    published: Option<&TerminalCertificateV1>,
) -> Result<StoreUpdate, TrimError> {
    if let Some(current) = published {
        if current.transition() != next.transition() {
            return Err(TrimError::Handoff(HandoffError::WrongTransition));
        }
        if current.terminal_root() != next.terminal_root() {
            return Err(TrimError::Handoff(HandoffError::MixedTerminal));
        }
    }
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: TERMINAL_KEY.to_vec(),
        value: Some(next.encode()?),
    })
}
