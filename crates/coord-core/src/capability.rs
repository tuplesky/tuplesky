//! Sealed capabilities: values that can only be constructed by the code that
//! validated the evidence behind them (design Sections 3, 9.3, 18.1).

use alloc::vec::Vec;

use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, KvRevision, PrincipalId,
    SessionId, TrustRuleId,
};
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

impl EstablishError {
    /// Stable description.
    pub const fn as_str(self) -> &'static str {
        match self {
            EstablishError::EpochMismatch => "ballot epoch differs from the stated epoch",
            EstablishError::InconsistentClosure => "closure lists the command itself or duplicates",
            EstablishError::ZeroPosition => "position zero is never established",
        }
    }
}

/// A result whose command, order, authorization and digest were established
/// by the learning predicate. Fields are private; the only constructor is
/// [`EstablishedResult::establish`]. Deserialization decodes an untrusted
/// [`EstablishedRecord`] and re-runs the structural checks, so bytes cannot
/// produce a value `establish` would have refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct EstablishedResult {
    command: CommandId,
    epoch: ConfigurationEpoch,
    ballot: Ballot,
    position: ExecutionPosition,
    result_digest: Digest32,
    revision: Option<KvRevision>,
    fast_path: bool,
}

/// The serialized shape of an [`EstablishedResult`]: plain data with no
/// establishment meaning. It is what storage or the wire carries; turning it
/// back into a capability goes through [`EstablishedResult::restore`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstablishedRecord {
    /// Established command.
    pub command: CommandId,
    /// Epoch.
    pub epoch: ConfigurationEpoch,
    /// Ballot.
    pub ballot: Ballot,
    /// Execution position.
    pub position: ExecutionPosition,
    /// Result digest.
    pub result_digest: Digest32,
    /// KV revision, if any.
    pub revision: Option<KvRevision>,
    /// Fast-path marker.
    pub fast_path: bool,
}

impl<'de> Deserialize<'de> for EstablishedResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let record = EstablishedRecord::deserialize(deserializer)?;
        EstablishedResult::restore(record).map_err(|e| serde::de::Error::custom(e.as_str()))
    }
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

    /// Rebuild a capability from its serialized record, re-checking the
    /// structural invariants `establish` enforces (epoch consistency and a
    /// nonzero position). The closure was validated when the record was
    /// produced and is not carried.
    pub fn restore(record: EstablishedRecord) -> Result<Self, EstablishError> {
        if record.ballot.epoch != record.epoch {
            return Err(EstablishError::EpochMismatch);
        }
        if record.position == ExecutionPosition::ZERO {
            return Err(EstablishError::ZeroPosition);
        }
        Ok(EstablishedResult {
            command: record.command,
            epoch: record.epoch,
            ballot: record.ballot,
            position: record.position,
            result_digest: record.result_digest,
            revision: record.revision,
            fast_path: record.fast_path,
        })
    }

    /// The plain record of this capability.
    pub const fn record(&self) -> EstablishedRecord {
        EstablishedRecord {
            command: self.command,
            epoch: self.epoch,
            ballot: self.ballot,
            position: self.position,
            result_digest: self.result_digest,
            revision: self.revision,
            fast_path: self.fast_path,
        }
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

/// What a receipt authorizes its holder to do.
///
/// A receipt attests an authentication. It does not follow that every
/// authentication may create a session: a collector relaying a client's
/// work under a session the cluster already agreed on is doing
/// something different from a broker asserting that a credential it
/// verified maps to a principal. Collapsing the two would make every
/// principal that can submit an identity issuer.
///
/// So the purpose is part of what the verifier attests, it is checked
/// against the authority of the ingress the receipt arrived on, and it
/// is part of what consensus binds to the command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AdmissionPurpose {
    /// Submit work under a session that already exists. The session's
    /// own record decides what the work may do; this receipt asserts
    /// only that the caller presenting it is bound to that session.
    Submit,
    /// Establish the session this receipt names, from a credential the
    /// verifier checked against configured trust. Only an ingress with
    /// session-establishment authority may originate one.
    Establish,
}

/// The instant a verified credential stops admitting new work: whole
/// seconds of the issuer's clock, the scale a token's `exp` uses.
///
/// It is an *admission* constraint and never a replica's branch. It is
/// checked once, at the authentication boundary, under that boundary's
/// configured clock-health assumptions; a replica applying the command
/// does not consult a clock, and recovery does not revalidate an
/// already accepted command against today's. A command admitted before
/// the deadline may therefore finish after it, which is correct: what
/// denies execution later is an ordered session retirement or a trust
/// rule revocation at its own position, not the passage of time.
///
/// Distinct from the boundary's own monotonic admission tick, which is
/// not comparable across processes and is not a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CredentialDeadline(pub u64);

/// What every receipt attests, whatever it is for.
///
/// The cluster and domain are the verifier's own, so a receipt minted
/// for one domain cannot admit work in another; the session is what the
/// receipt is about; the generation and ceiling are what the binding or
/// the credential allowed. None of it may be taken from a command
/// payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct AttestedAdmission {
    /// Cluster the verifier belongs to.
    pub cluster: ClusterId,
    /// Domain the session lives in.
    pub domain: DomainId,
    /// Session the receipt is about: the one being established, or the
    /// one the caller is already bound to.
    pub session: SessionId,
    /// Rule/issuer generation the admission relied on.
    pub rule_generation: u64,
    /// Ceiling the credential and rule allow; execution can only narrow
    /// it further.
    pub scope_ceiling: u32,
    /// Unique receipt identity (single use).
    pub receipt_id: Digest32,
    /// The boundary's own monotonic tick at admission.
    pub admitted_at_ticks: u64,
}

/// What a verifier attests *in addition* when a credential is to
/// establish the session it names.
///
/// Separate from [`AttestedAdmission`] on purpose, and not a pair of
/// optional fields on it. A submission receipt must not be *able* to
/// carry a principal or a trust rule: if it could, every principal that
/// may submit would be one field away from being an identity issuer,
/// and the check that stops it would be a runtime condition rather than
/// a type.
///
/// Every field is a verified fact. The principal is the configured
/// mapping of the credential that was checked, the trust rule
/// identifies the rule actually used, and the deadline is that
/// credential's own. A replica still rechecks the rule and its
/// generation against current replicated policy before it creates
/// anything: this says who was authenticated, not what exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct AttestedEstablishment {
    /// Principal the verified credential maps to under configured trust.
    pub principal: PrincipalId,
    /// The trust rule actually used.
    pub trust_rule: TrustRuleId,
    /// When the verified credential stops admitting new work.
    pub credential_valid_until: CredentialDeadline,
}

/// Canonical trusted admission receipt (design Section 9.3): identity and
/// relevant claims verified outside replicated execution, never a raw token.
/// Only a verifier at the trusted boundary constructs it, through
/// [`AdmissionReceipt::submitting`] or [`AdmissionReceipt::establishing`]
/// with a [`VerifierToken`]. It is deliberately not deserializable: a
/// receipt is minted per admission by the verifier and never restored
/// from bytes.
///
/// # What it is evidence of, and what it is not
///
/// It is evidence that a credential was authenticated, under a named
/// trust rule at a named generation, within named limits. It is *not*
/// evidence that the session exists, that session creation committed,
/// or that an operation is still authorized. Those remain the
/// replicated state machine's to decide at the command's own position:
/// current rules, the consumption state of this receipt, the session's
/// own record and its retirement all outrank anything attested here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AdmissionReceipt {
    attested: AttestedAdmission,
    /// `Some` exactly when the purpose is [`AdmissionPurpose::Establish`].
    establishing: Option<AttestedEstablishment>,
}

/// Proof that a verifier ran. It has no public constructor other than
/// [`VerifierToken::for_boundary`], which only the verifier boundary crates
/// may call: `cargo xtask check-deps` scans every non-test crate for the
/// constructor and rejects any crate outside the reviewed allow list, so a
/// state machine or storage crate cannot label unverified input as admitted.
#[derive(Debug)]
pub struct VerifierToken(());

impl VerifierToken {
    /// Mint a token. Callable only by boundary code that just verified
    /// external credentials; the dependency-policy check enforces the
    /// allow list of crates that may contain this call.
    pub const fn for_boundary() -> Self {
        VerifierToken(())
    }
}

impl AdmissionReceipt {
    /// Mint a receipt for work under a session that already exists.
    ///
    /// It attests only that the caller presenting it is bound to that
    /// session at that generation and ceiling. What the work may do is
    /// the session record's answer at the command's position, not this
    /// receipt's.
    pub const fn submitting(_token: VerifierToken, attested: AttestedAdmission) -> Self {
        AdmissionReceipt {
            attested,
            establishing: None,
        }
    }

    /// Mint a receipt for establishing the session it names.
    ///
    /// Only a verifier with session-establishment authority calls this,
    /// and only from a credential it checked against configured trust.
    /// The replica still decides whether the session exists.
    pub const fn establishing(
        _token: VerifierToken,
        attested: AttestedAdmission,
        establishing: AttestedEstablishment,
    ) -> Self {
        AdmissionReceipt {
            attested,
            establishing: Some(establishing),
        }
    }

    /// What this receipt authorizes.
    pub const fn purpose(&self) -> AdmissionPurpose {
        match self.establishing {
            Some(_) => AdmissionPurpose::Establish,
            None => AdmissionPurpose::Submit,
        }
    }

    /// Everything every receipt attests.
    pub const fn attested(&self) -> AttestedAdmission {
        self.attested
    }

    /// What was attested for establishing a session, where that is what
    /// this receipt is for.
    pub const fn establishment(&self) -> Option<AttestedEstablishment> {
        self.establishing
    }

    /// Cluster the verifier belongs to.
    pub const fn cluster(&self) -> ClusterId {
        self.attested.cluster
    }
    /// Domain the session lives in.
    pub const fn domain(&self) -> DomainId {
        self.attested.domain
    }

    /// Session.
    pub const fn session(&self) -> SessionId {
        self.attested.session
    }
    /// Rule/issuer generation the admission relied on.
    pub const fn rule_generation(&self) -> u64 {
        self.attested.rule_generation
    }
    /// Scope ceiling; policy at execution can only narrow it.
    pub const fn scope_ceiling(&self) -> u32 {
        self.attested.scope_ceiling
    }
    /// Unique receipt identity (single use).
    pub const fn receipt_id(&self) -> Digest32 {
        self.attested.receipt_id
    }
    /// Admission tick.
    pub const fn admitted_at_ticks(&self) -> u64 {
        self.attested.admitted_at_ticks
    }
}
