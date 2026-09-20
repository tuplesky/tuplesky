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
//! Task-57 continues in the same file: [`record_install`] is one
//! successor replica's durable record that it *holds* the terminal
//! state, written only from an install receipt whose verified root is
//! the certificate's, and [`activate_successor`] turns a majority of
//! those into the authority to serve. [`LocalEvidence`] is what one
//! store can answer about a handoff, for
//! [`coord_consensus::handoff::resume`] to decide where a replacement
//! coordinator carries on.
//!
//! The module writes rows and computes digests; it drives nothing.
//! Asking the old voters for their reports, moving the checkpoint to
//! the successor and telling anyone to install are the runtime's.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use coord_consensus::handoff::{
    ActivationCertificate, HandoffError, InstallRecord, SealCertificate, TerminalCertificate,
    TerminalReport, Transition, activate, select_terminal,
};
use coord_consensus::quorum::EpochVoters;
use coord_consensus::recovery::SyncDecision;
use coord_core::effect::StoreUpdate;
use coord_store_api::engine::{Direction, EngineError, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use serde::{Deserialize, Serialize};

use crate::install::InstalledCheckpointV1;
use crate::manifest::CheckpointBoundary;
use crate::trim::{TrimError, TrimLimits, corrupt, decode_record, encode_record};

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

// ---------------------------------------------------------------------
// task-57: installing the terminal state and activating the successor.
// ---------------------------------------------------------------------

/// Record kind of a successor replica's installation record.
pub const INSTALL_RECORD_KIND: u16 = 0x0007;
/// Record kind of the published handoff activation.
pub const HANDOFF_ACTIVATION_RECORD_KIND: u16 = 0x0008;
/// Key prefix of the installation records; the replica identity follows.
pub const INSTALL_KEY_PREFIX: &[u8] = b"handoff_install_v1/";
/// Key of the published handoff activation.
pub const HANDOFF_ACTIVATION_KEY: &[u8] = b"handoff_activation_v1";

/// One successor replica's durable record that it holds the terminal
/// state the certificate names.
///
/// Written only from an install receipt whose verified root is the
/// certificate's `state_root`, so it is evidence of *having* the state
/// rather than of having been told to say so. Complete bytes are not
/// the same as the right bytes, and a coordinator's assurance is
/// neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalInstallV1 {
    /// The installing replica.
    pub replica: ReplicaId,
    /// The transition it is part of.
    pub transition: Transition,
    /// The terminal root it installed.
    pub terminal_root: Digest32,
}

impl TerminalInstallV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(INSTALL_RECORD_KIND, self, "handoff install encode")
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(INSTALL_RECORD_KIND, bytes, "handoff install")
    }

    /// What this record is, in the consensus vocabulary.
    pub const fn record(&self) -> InstallRecord {
        InstallRecord {
            replica: self.replica,
            transition: self.transition,
            terminal_root: self.terminal_root,
        }
    }
}

/// `checkpoint_v1` key of one successor replica's installation record.
pub fn install_key(replica: &ReplicaId) -> Vec<u8> {
    let mut key = Vec::with_capacity(INSTALL_KEY_PREFIX.len() + ReplicaId::LEN);
    key.extend_from_slice(INSTALL_KEY_PREFIX);
    key.extend_from_slice(replica.as_bytes());
    key
}

/// The update recording that `replica` installed the certificate's
/// terminal state, from the receipt of the install that did it.
///
/// The receipt's verified root must be the certificate's `state_root`
/// and its boundary the certificate's boundary. That is what "the new
/// quorum installs *identical* terminal state" means here: the record
/// cannot be written from a coordinator saying so, only from an install
/// this replica actually performed and verified.
///
/// A replica outside the successor set writes nothing: it is not part
/// of the quorum that activates, and counting it would let a bystander
/// stand in for a member that holds nothing.
pub fn record_install(
    certificate: &TerminalCertificateV1,
    replica: ReplicaId,
    receipt: &InstalledCheckpointV1,
) -> Result<StoreUpdate, TrimError> {
    if !certificate.state.successors.contains_key(&replica) {
        return Err(TrimError::Handoff(HandoffError::NotAVoter { replica }));
    }
    if receipt.root != certificate.state.state_root
        || receipt.boundary != certificate.state.boundary
    {
        return Err(TrimError::Handoff(HandoffError::WrongTerminalRoot));
    }
    let record = TerminalInstallV1 {
        replica,
        transition: certificate.transition(),
        terminal_root: certificate.terminal_root(),
    };
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: install_key(&replica),
        value: Some(record.encode()?),
    })
}

/// The installation records this node durably holds, in replica order.
pub fn read_installs<V: OrderedRead>(
    view: &V,
    limits: &TrimLimits,
) -> Result<Vec<TerminalInstallV1>, TrimError> {
    limits.validate()?;
    let mut out: Vec<TerminalInstallV1> = Vec::new();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let page = view.scan_page(
            Collection::CheckpointV1.id(),
            &ScanRequest {
                lower: Bound::Included(INSTALL_KEY_PREFIX.to_vec()),
                upper: Bound::Unbounded,
                direction: Direction::Forward,
                resume_after: resume.clone(),
                max_rows: limits.page_rows(),
                max_bytes: limits.page_bytes(),
            },
        )?;
        let mut past_prefix = false;
        for row in &page.rows {
            let Some(suffix) = row.key.strip_prefix(INSTALL_KEY_PREFIX) else {
                past_prefix = true;
                break;
            };
            if out.len() as u32 >= limits.max_acknowledgements {
                return Err(TrimError::AcknowledgementBudget {
                    limit: limits.max_acknowledgements,
                });
            }
            let replica = ReplicaId::from_slice(suffix).map_err(|_| corrupt("install key"))?;
            let record = TerminalInstallV1::decode(&row.value)?;
            if record.replica != replica {
                return Err(TrimError::Engine(corrupt(
                    "install replica differs from its key",
                )));
            }
            out.push(record);
        }
        match page.rows.last() {
            Some(last) if !past_prefix && !page.exhausted => resume = Some(last.key.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// The successor's authority to serve, durably.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffActivationV1 {
    /// The transition it completes.
    pub transition: Transition,
    /// The terminal state the successor installed.
    pub terminal_root: Digest32,
    /// The successor replicas that installed it.
    pub installers: BTreeSet<ReplicaId>,
}

impl HandoffActivationV1 {
    /// Encode as a `checkpoint_v1` row value.
    pub fn encode(&self) -> Result<Vec<u8>, EngineError> {
        encode_record(
            HANDOFF_ACTIVATION_RECORD_KIND,
            self,
            "handoff activation encode",
        )
    }

    /// Decode a `checkpoint_v1` row value.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineError> {
        decode_record(HANDOFF_ACTIVATION_RECORD_KIND, bytes, "handoff activation")
    }
}

/// Activate the successor of `certificate` from its installations.
///
/// A majority of the successor set must have durably installed that
/// exact terminal root. The quorum rule and the root check are
/// task-54's; what this adds is that the successor set comes from the
/// certificate rather than from a caller, so an activation cannot be
/// computed against a different successor than the one the old quorum
/// certified.
pub fn activate_successor(
    certificate: &TerminalCertificateV1,
    installs: &[TerminalInstallV1],
) -> Result<HandoffActivationV1, TrimError> {
    let successor = certificate
        .successor_voters()
        .ok_or(TrimError::Handoff(HandoffError::WrongTransition))?;
    let terminal = TerminalCertificate::recovered(
        certificate.transition(),
        certificate.terminal_root(),
        successor.voters().clone(),
        certificate.signers.clone(),
    );
    let records: Vec<InstallRecord> = installs.iter().map(TerminalInstallV1::record).collect();
    let activation = activate(&successor, &terminal, &records).map_err(TrimError::Handoff)?;
    Ok(HandoffActivationV1 {
        transition: activation.transition(),
        terminal_root: activation.terminal_root(),
        installers: activation.installers().clone(),
    })
}

/// The published activation, if this node has one.
pub fn published_activation<V: OrderedRead>(
    view: &V,
) -> Result<Option<HandoffActivationV1>, TrimError> {
    match view.get(Collection::CheckpointV1.id(), HANDOFF_ACTIVATION_KEY)? {
        None => Ok(None),
        Some(bytes) => Ok(Some(HandoffActivationV1::decode(&bytes)?)),
    }
}

/// The update publishing `next`.
///
/// An activation is reused, never recomputed: the identical one
/// republishes and any other is refused. A duplicate activation is
/// therefore a no-op rather than a second grant of authority, which is
/// what a coordinator retrying after a lost reply needs it to be.
pub fn publish_handoff_activation(
    next: &HandoffActivationV1,
    published: Option<&HandoffActivationV1>,
) -> Result<StoreUpdate, TrimError> {
    if let Some(current) = published {
        if current.transition != next.transition {
            return Err(TrimError::Handoff(HandoffError::WrongTransition));
        }
        if current.terminal_root != next.terminal_root {
            return Err(TrimError::Handoff(HandoffError::WrongTerminalRoot));
        }
    }
    Ok(StoreUpdate {
        collection: Collection::CheckpointV1.id(),
        key: HANDOFF_ACTIVATION_KEY.to_vec(),
        value: Some(next.encode()?),
    })
}

/// What one node's store durably says about a transition.
///
/// Deliberately not the whole of [`Evidence`]: the old voters' stances
/// live on the old voters, and a coordinator gathers them over the
/// wire. This is what a store can answer for itself, and a caller
/// combines it with the stances it collected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalEvidence {
    /// The selected certificate, if published here.
    pub certificate: Option<TerminalCertificateV1>,
    /// Installation records held here.
    pub installs: Vec<TerminalInstallV1>,
    /// The activation, if published here.
    pub activation: Option<HandoffActivationV1>,
}

impl LocalEvidence {
    /// Read everything this store holds about a handoff.
    pub fn read<V: OrderedRead>(view: &V, limits: &TrimLimits) -> Result<Self, TrimError> {
        Ok(LocalEvidence {
            certificate: published_certificate(view)?,
            installs: read_installs(view, limits)?,
            activation: published_activation(view)?,
        })
    }

    /// The consensus-level records, for [`coord_consensus::handoff::resume`].
    pub fn records(
        &self,
    ) -> (
        Option<TerminalCertificate>,
        Vec<InstallRecord>,
        Option<ActivationCertificate>,
    ) {
        let certificate = self.certificate.as_ref().and_then(|c| {
            Some(TerminalCertificate::recovered(
                c.transition(),
                c.terminal_root(),
                c.successor_voters()?.voters().clone(),
                c.signers.clone(),
            ))
        });
        let installs = self
            .installs
            .iter()
            .map(TerminalInstallV1::record)
            .collect();
        let activation = self.activation.as_ref().map(|a| {
            ActivationCertificate::recovered(a.transition, a.terminal_root, a.installers.clone())
        });
        (certificate, installs, activation)
    }
}
