//! Internal replicated commands: ordered like client commands, planned
//! against the same views, never issued by clients. They come from the
//! service itself (expiry, authority) or from the trusted boundary
//! (admission receipts, grant commitments, policy administration).

use alloc::vec;
use alloc::vec::Vec;

use coord_core::capability::AdmissionReceipt;
use coord_types::identity::Digest32;
use coord_types::ids::{
    LeaseAuthorityEpoch, LeaseGeneration, LeaseId, NamespaceId, PolicyRuleId, PrincipalId,
    SessionId, TrustRuleId,
};
use serde::{Deserialize, Serialize};

use crate::policy::{AdmissionReceiptV1, GrantKind, PolicyRule, TrustRule};

/// A command the service submits to its own ordered history.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InternalCommand {
    /// Conditional lease expiration (Section 7.3): applies only when every
    /// field matches the replicated state, so a renewal ordered first makes
    /// it a no-op and an old leader's epoch is rejected outright.
    ExpireLease {
        /// Namespace of the lease (selects the reverse index to load).
        namespace: NamespaceId,
        /// Lease.
        lease_id: LeaseId,
        /// Generation the scheduler observed.
        generation: LeaseGeneration,
        /// Renewal sequence the scheduler observed.
        expected_renewal_sequence: u64,
        /// Authority epoch the scheduler runs under.
        authority_epoch: LeaseAuthorityEpoch,
    },
    /// Establish a new lease expiry authority epoch (Section 7.2), fencing
    /// every scheduler of an older epoch.
    EstablishLeaseAuthority {
        /// Namespace the command is planned in (leases are domain-wide; the
        /// epoch row is domain-wide too).
        namespace: NamespaceId,
        /// New epoch; must exceed the current one.
        epoch: LeaseAuthorityEpoch,
    },
    /// Consume a single-use admission receipt (and, for a browser/device
    /// login, its pending code commitment) and create the session, after
    /// rechecking the trust rule against current replicated policy.
    ConsumeAdmission {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// The receipt.
        receipt: AdmissionReceiptV1,
        /// Code commitment to consume atomically, if any.
        code: Option<Digest32>,
        /// Refresh family to bind to the session, if any (must be pending
        /// or already belong to the session).
        refresh_family: Option<Digest32>,
        /// Outstanding retry window for the session's clients.
        window: u32,
    },
    /// Retire a session; ordered revocation denies execution at later
    /// positions.
    RetireSession {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// Session.
        session: SessionId,
    },
    /// Order a grant commitment (a code hash or a refresh family's first
    /// secret commitment); entropy was generated outside consensus.
    CommitGrant {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// Commitment digest.
        commitment: Digest32,
        /// Kind (`Code` or `RefreshFamily`).
        kind: GrantKind,
    },
    /// Rotate a refresh family: the presented secret must be the current
    /// one at the expected generation; a retired secret revokes the family
    /// and its session.
    AdvanceRefresh {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// Family commitment (row key).
        family: Digest32,
        /// Commitment of the secret presented.
        presented: Digest32,
        /// Commitment of the next secret.
        next: Digest32,
    },
    /// Write or remove a permission rule.
    PutPolicyRule {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// Principal the rule belongs to.
        principal: PrincipalId,
        /// Rule identity.
        rule: PolicyRuleId,
        /// Rule (`None` removes; `Some` must name the same principal).
        record: Option<PolicyRule>,
    },
    /// Write a trust rule (create, disable, or regenerate).
    PutTrustRule {
        /// Namespace the command is planned in.
        namespace: NamespaceId,
        /// Rule identity.
        rule: TrustRuleId,
        /// New state.
        record: TrustRule,
    },
}

impl InternalCommand {
    /// Build the command that consumes an establishment receipt and
    /// creates the session it names.
    ///
    /// The only way to build [`InternalCommand::ConsumeAdmission`]: its
    /// receipt is derived from an [`AdmissionReceipt`] capability, which
    /// only the trusted admission boundary can mint. A caller holding
    /// nothing but data cannot name a principal, a trust rule or a
    /// credential deadline here, so the command's claims are the
    /// verifier's and never a payload's.
    ///
    /// `None` when the receipt's purpose is merely to submit work:
    /// being admitted under an existing session is not authority to
    /// originate one, and the receipt carries no principal to originate
    /// it with.
    ///
    /// What this builds is a *proposal*. Replicated execution rechecks
    /// the trust rule and its generation against current policy, refuses
    /// a receipt already consumed and refuses a session identity that
    /// already exists; see [`crate::plan_internal`].
    pub fn consume_admission(
        namespace: NamespaceId,
        receipt: &AdmissionReceipt,
        code: Option<Digest32>,
        refresh_family: Option<Digest32>,
        window: u32,
    ) -> Option<Self> {
        Some(InternalCommand::ConsumeAdmission {
            namespace,
            receipt: AdmissionReceiptV1::of(&receipt.facts())?,
            code,
            refresh_family,
            window,
        })
    }

    /// Namespace the command's view is built for.
    pub const fn namespace(&self) -> NamespaceId {
        match self {
            InternalCommand::ExpireLease { namespace, .. }
            | InternalCommand::EstablishLeaseAuthority { namespace, .. }
            | InternalCommand::ConsumeAdmission { namespace, .. }
            | InternalCommand::RetireSession { namespace, .. }
            | InternalCommand::CommitGrant { namespace, .. }
            | InternalCommand::AdvanceRefresh { namespace, .. }
            | InternalCommand::PutPolicyRule { namespace, .. }
            | InternalCommand::PutTrustRule { namespace, .. } => *namespace,
        }
    }

    /// Sessions the command reads or writes.
    pub fn sessions(&self) -> Vec<SessionId> {
        match self {
            InternalCommand::ConsumeAdmission { receipt, .. } => vec![receipt.session],
            InternalCommand::RetireSession { session, .. } => vec![*session],
            _ => Vec::new(),
        }
    }

    /// Grant commitments the command reads or writes (the refresh family's
    /// session is loaded by the view builder when it exists).
    pub fn grants(&self) -> Vec<Digest32> {
        match self {
            InternalCommand::ConsumeAdmission {
                receipt,
                code,
                refresh_family,
                ..
            } => {
                let mut out = vec![receipt.receipt_id];
                out.extend(*code);
                out.extend(*refresh_family);
                out
            }
            InternalCommand::CommitGrant { commitment, .. } => vec![*commitment],
            InternalCommand::AdvanceRefresh { family, .. } => vec![*family],
            _ => Vec::new(),
        }
    }

    /// Trust rules the command reads.
    pub fn trust_rules(&self) -> Vec<TrustRuleId> {
        match self {
            InternalCommand::ConsumeAdmission { receipt, .. } => vec![receipt.trust_rule],
            InternalCommand::PutTrustRule { rule, .. } => vec![*rule],
            _ => Vec::new(),
        }
    }

    /// Lease whose record and attachments the command needs, if any.
    pub const fn lease(&self) -> Option<LeaseId> {
        match self {
            InternalCommand::ExpireLease { lease_id, .. } => Some(*lease_id),
            _ => None,
        }
    }
}
