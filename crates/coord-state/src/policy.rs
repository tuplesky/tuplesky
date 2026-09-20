//! Replicated sessions, trust rules, permission rules and grant
//! commitments (design Sections 9.2-9.3, 20.3).
//!
//! Everything here is ordered replicated state consulted at execution:
//! a session's principal and scope ceiling are immutable, its validity
//! follows the trust rule it was admitted under (disabling or regenerating
//! the rule invalidates its sessions), and permission rules are allow-only
//! (deny by default) over `(principal, action, namespace, key interval)`.
//! Nothing here verifies tokens, signs, reads a clock or touches the
//! network: the trusted boundary submits canonical receipts and
//! commitments, and the state machine consumes them in order.

use alloc::vec::Vec;

use coord_core::capability::AdmissionFacts;
use coord_types::identity::Digest32;
use coord_types::ids::{NamespaceId, PrincipalId, SessionId, TrustRuleId};
use serde::{Deserialize, Serialize};

/// An authorizable action. Each has a bit in a session's scope ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Action {
    /// Read current or historical entries of an interval.
    Read,
    /// Create or update entries.
    Write,
    /// Delete entries.
    Delete,
    /// Grant a native lease.
    LeaseGrant,
    /// Attach entries to a lease one owns.
    LeaseAttach,
    /// Inspect a lease one owns.
    LeaseInspect,
    /// Renew a lease one owns.
    LeaseRenew,
    /// Revoke a lease one owns.
    LeaseRevoke,
    /// Advance the retention floor.
    Compact,
}

impl Action {
    /// Every action, in bit order.
    pub const ALL: [Action; 9] = [
        Action::Read,
        Action::Write,
        Action::Delete,
        Action::LeaseGrant,
        Action::LeaseAttach,
        Action::LeaseInspect,
        Action::LeaseRenew,
        Action::LeaseRevoke,
        Action::Compact,
    ];

    /// The action's bit in a scope ceiling.
    pub const fn bit(self) -> u32 {
        1 << (self as u32)
    }

    /// A ceiling permitting every action.
    pub const FULL_CEILING: u32 = (1 << 9) - 1;
}

/// The key interval an action applies to: `[lower, upper)`, `upper`
/// `None` for an open end.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyInterval {
    /// Inclusive start.
    pub lower: Vec<u8>,
    /// Exclusive end; `None` is unbounded.
    pub upper: Option<Vec<u8>>,
}

impl KeyInterval {
    /// The interval of exactly `key`.
    pub fn exact(key: &[u8]) -> Self {
        let mut upper = key.to_vec();
        upper.push(0);
        KeyInterval {
            lower: key.to_vec(),
            upper: Some(upper),
        }
    }

    /// The whole key space.
    pub const fn all() -> Self {
        KeyInterval {
            lower: Vec::new(),
            upper: None,
        }
    }

    /// Whether `self` contains the whole of `other`.
    pub fn contains(&self, other: &KeyInterval) -> bool {
        if other.lower < self.lower {
            return false;
        }
        match (&self.upper, &other.upper) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(mine), Some(theirs)) => theirs <= mine,
        }
    }
}

/// An allow rule: `principal` may perform `action` on every key of
/// `interval` in `namespace`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PolicyRule {
    /// Principal.
    pub principal: PrincipalId,
    /// Action.
    pub action: Action,
    /// Namespace.
    pub namespace: NamespaceId,
    /// Key interval the permission covers in full.
    pub interval: KeyInterval,
}

/// A trust rule (issuer mapping) sessions are admitted under.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrustRule {
    /// Whether sessions admitted under it may execute.
    pub enabled: bool,
    /// Generation; a session records the generation it was admitted under
    /// and is invalid under any other.
    pub generation: u64,
}

/// A replicated session.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Immutable principal.
    pub principal: PrincipalId,
    /// Immutable privilege ceiling (bits of [`Action`]); policy can only
    /// narrow it.
    pub scope_ceiling: u32,
    /// Trust rule the session was admitted under.
    pub trust_rule: TrustRuleId,
    /// Generation of that rule at admission.
    pub rule_generation: u64,
    /// Whether the session may submit work; retirement is permanent.
    pub active: bool,
    /// Default outstanding retry window for new client instances.
    pub window: u32,
    /// Receipt the session was created from.
    pub receipt_id: Digest32,
    /// Absolute deadline of the session, from its admission. Renewal
    /// signs fresh tokens within the session; it never moves this, so a
    /// session cannot be renewed indefinitely past the credential that
    /// admitted it.
    pub expires_at: u64,
}

/// The canonical admission receipt as the state machine consumes it: what
/// the trusted boundary verified, never a raw token.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdmissionReceiptV1 {
    /// Unique, single-use receipt identity.
    pub receipt_id: Digest32,
    /// Session to create.
    pub session: SessionId,
    /// Principal the credential mapped to.
    pub principal: PrincipalId,
    /// Ceiling the credential and rule allow.
    pub scope_ceiling: u32,
    /// Trust rule the mapping used.
    pub trust_rule: TrustRuleId,
    /// Generation of that rule the verifier saw.
    pub rule_generation: u64,
    /// Absolute deadline the admitted credential allows: the session
    /// created from this receipt ends here, whatever it is renewed to.
    pub expires_at: u64,
}

impl AdmissionReceiptV1 {
    /// The replicated form of what a verifier attested, when what it
    /// attested establishes a session.
    ///
    /// `None` for facts that merely admit work under a session that
    /// already exists: they carry no principal, no trust rule and no
    /// credential deadline, so there is nothing here to create a session
    /// from. That is the point of their being a separate shape.
    ///
    /// This is a change of representation and nothing more. It confers
    /// no authority: what may be done with the result is decided by who
    /// could obtain the facts, which is the admission boundary (through
    /// an [`AdmissionReceipt`] capability) or an accepted command's own
    /// durable record.
    ///
    /// [`AdmissionReceipt`]: coord_core::capability::AdmissionReceipt
    pub const fn of(facts: &AdmissionFacts) -> Option<Self> {
        let Some(establishing) = facts.establishing else {
            return None;
        };
        Some(AdmissionReceiptV1 {
            receipt_id: facts.attested.receipt_id,
            session: facts.attested.session,
            principal: establishing.principal,
            scope_ceiling: facts.attested.scope_ceiling,
            trust_rule: establishing.trust_rule,
            rule_generation: facts.attested.rule_generation,
            expires_at: establishing.credential_valid_until.0,
        })
    }
}

/// What a grant commitment stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GrantKind {
    /// A consumed admission receipt (recorded so it can never be replayed).
    Receipt,
    /// A single-use browser or device code commitment.
    Code,
    /// A refresh family: the commitment of its current secret plus its
    /// rotation generation.
    RefreshFamily,
}

/// Lifecycle of a grant commitment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GrantState {
    /// Ordered, not yet consumed.
    Pending,
    /// Consumed exactly once.
    Consumed,
    /// Revoked (a retired secret was reused, or explicitly).
    Revoked,
}

/// A row of `auth_grant_v1`, keyed by a commitment digest.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GrantRecord {
    /// Kind.
    pub kind: GrantKind,
    /// State.
    pub state: GrantState,
    /// Rotation generation (refresh families).
    pub generation: u64,
    /// Commitment of the current secret (refresh families).
    pub current_secret: Option<Digest32>,
    /// Session the grant created or belongs to.
    pub session: Option<SessionId>,
}

/// The authorization context a request executes under, loaded from
/// replicated state at the same execution point as the plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authorization {
    /// Session record, if the session exists.
    pub session: Option<SessionRecord>,
    /// The session's trust rule, if it exists.
    pub trust_rule: Option<TrustRule>,
    /// Permission rules of the session's principal in this namespace.
    pub rules: Vec<PolicyRule>,
}

impl Authorization {
    /// The session, if it may execute now: active, and its trust rule
    /// enabled at the generation it was admitted under.
    pub fn valid_session(&self) -> Option<&SessionRecord> {
        let session = self.session.as_ref()?;
        let rule = self.trust_rule.as_ref()?;
        (session.active && rule.enabled && rule.generation == session.rule_generation)
            .then_some(session)
    }

    /// Whether `action` on the whole of `interval` in `namespace` is
    /// permitted: inside the session's ceiling and covered in full by one
    /// rule of its principal.
    pub fn permits(&self, namespace: &NamespaceId, action: Action, interval: &KeyInterval) -> bool {
        let Some(session) = self.valid_session() else {
            return false;
        };
        if session.scope_ceiling & action.bit() == 0 {
            return false;
        }
        self.rules.iter().any(|r| {
            r.principal == session.principal
                && r.action == action
                && r.namespace == *namespace
                && r.interval.contains(interval)
        })
    }
}
