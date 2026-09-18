//! Sealed capabilities: values that can only be constructed by the code that
//! validated the evidence behind them (design Sections 3, 9.3, 18.1).

use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ExecutionPosition, KvRevision, SessionId};
use serde::{Deserialize, Serialize};

/// Evidence presented to [`EstablishedResult::establish`]. Constructing this
/// struct is not establishment; the learning predicate (task-24/task-28)
/// assembles it from durable, quorum-checked state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EstablishmentEvidence {
    /// Command established.
    pub command: CommandId,
    /// Epoch and ballot the learning evidence belongs to.
    pub epoch: ConfigurationEpoch,
    /// Ballot.
    pub ballot: Ballot,
    /// Execution position assigned.
    pub position: ExecutionPosition,
    /// Every predecessor the command's closure depends on, all executed.
    pub closed_predecessors: Vec<CommandId>,
    /// Digest of the exact result.
    pub result_digest: Digest32,
    /// KV revision produced, if any.
    pub revision: Option<KvRevision>,
    /// Whether the learning path was the fast path (measurement only).
    pub fast_path: bool,
}

/// Why evidence was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstablishError {
    /// Ballot epoch differs from the stated epoch.
    EpochMismatch,
    /// Predecessor list contains the command itself or duplicates.
    InconsistentClosure,
    /// Position zero is never an established command.
    ZeroPosition,
}

/// A result whose command, order, authorization and digest were established
/// by the learning predicate. Fields are private; the only constructor is
/// [`EstablishedResult::establish`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstablishedResult {
    command: CommandId,
    epoch: ConfigurationEpoch,
    ballot: Ballot,
    position: ExecutionPosition,
    result_digest: Digest32,
    revision: Option<KvRevision>,
    fast_path: bool,
}

impl EstablishedResult {
    /// Structural validation of evidence. This binds the fields; it does not
    /// prove the quorum predicate, which is the caller's obligation.
    pub fn establish(evidence: EstablishmentEvidence) -> Result<Self, EstablishError> {
        if evidence.ballot.epoch != evidence.epoch {
            return Err(EstablishError::EpochMismatch);
        }
        if evidence.position == ExecutionPosition::ZERO {
            return Err(EstablishError::ZeroPosition);
        }
        let preds = &evidence.closed_predecessors;
        for (i, p) in preds.iter().enumerate() {
            if *p == evidence.command || preds[..i].contains(p) {
                return Err(EstablishError::InconsistentClosure);
            }
        }
        Ok(EstablishedResult {
            command: evidence.command,
            epoch: evidence.epoch,
            ballot: evidence.ballot,
            position: evidence.position,
            result_digest: evidence.result_digest,
            revision: evidence.revision,
            fast_path: evidence.fast_path,
        })
    }

    /// Established command.
    pub const fn command(&self) -> CommandId {
        self.command
    }
    /// Epoch.
    pub const fn epoch(&self) -> ConfigurationEpoch {
        self.epoch
    }
    /// Ballot.
    pub const fn ballot(&self) -> Ballot {
        self.ballot
    }
    /// Execution position.
    pub const fn position(&self) -> ExecutionPosition {
        self.position
    }
    /// Result digest.
    pub const fn result_digest(&self) -> Digest32 {
        self.result_digest
    }
    /// KV revision, if the command mutated KV.
    pub const fn revision(&self) -> Option<KvRevision> {
        self.revision
    }
    /// Whether the fast path established it.
    pub const fn fast_path(&self) -> bool {
        self.fast_path
    }
}

/// A result released to the trusted boundary (task-29; design Sections
/// 4.5, 17.4): an established result together with the exact encoded
/// response. It carries no events, credentials or tokens: those await
/// irrevocable application whatever path released the result. A
/// speculative release is produced by the release gate only when the
/// complete learning predicate has determined the command, its closed
/// predecessor order, its authorization and the exact result from durable
/// evidence; a final release follows materialization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasedResult {
    established: EstablishedResult,
    response: Vec<u8>,
    speculative: bool,
}

impl ReleasedResult {
    /// Construct at the release gate (learning validation code). The
    /// established result is the sealed evidence; `response` is the exact
    /// encoding the client receives and `speculative` says whether it
    /// precedes materialization.
    pub const fn from_gate(
        established: EstablishedResult,
        response: Vec<u8>,
        speculative: bool,
    ) -> Self {
        ReleasedResult {
            established,
            response,
            speculative,
        }
    }

    /// The established result.
    pub const fn established(&self) -> &EstablishedResult {
        &self.established
    }
    /// The exact encoded response.
    pub fn response(&self) -> &[u8] {
        &self.response
    }
    /// Whether the release preceded materialization.
    pub const fn speculative(&self) -> bool {
        self.speculative
    }
}

/// Canonical trusted admission receipt (design Section 9.3): identity and
/// relevant claims verified outside replicated execution, never a raw token.
/// Only a verifier at the trusted boundary constructs it, through
/// [`AdmissionReceipt::from_verifier`] with a [`VerifierToken`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionReceipt {
    session: SessionId,
    rule_generation: u64,
    scope_ceiling: u32,
    receipt_id: Digest32,
    admitted_at_ticks: u64,
}

/// Proof that a verifier ran. It has no public constructor other than
/// [`VerifierToken::for_boundary`], which is only meaningful in verifier
/// code; state machines never receive one.
#[derive(Debug)]
pub struct VerifierToken(());

impl VerifierToken {
    /// Mint a token. Callable only by boundary code that just verified
    /// external credentials; core machines have no reason to call it.
    pub const fn for_boundary() -> Self {
        VerifierToken(())
    }
}

impl AdmissionReceipt {
    /// Construct a receipt at the trusted boundary.
    pub fn from_verifier(
        _token: VerifierToken,
        session: SessionId,
        rule_generation: u64,
        scope_ceiling: u32,
        receipt_id: Digest32,
        admitted_at_ticks: u64,
    ) -> Self {
        AdmissionReceipt {
            session,
            rule_generation,
            scope_ceiling,
            receipt_id,
            admitted_at_ticks,
        }
    }

    /// Session.
    pub const fn session(&self) -> SessionId {
        self.session
    }
    /// Rule/issuer generation the admission relied on.
    pub const fn rule_generation(&self) -> u64 {
        self.rule_generation
    }
    /// Scope ceiling; policy at execution can only narrow it.
    pub const fn scope_ceiling(&self) -> u32 {
        self.scope_ceiling
    }
    /// Unique receipt identity (single use).
    pub const fn receipt_id(&self) -> Digest32 {
        self.receipt_id
    }
    /// Admission tick.
    pub const fn admitted_at_ticks(&self) -> u64 {
        self.admitted_at_ticks
    }
}
