//! Independent authorization oracle (design Section 9.2), written from the
//! rules rather than from the production planner: deny by default,
//! permission for every comparison and for the selected branch only, full
//! containment of ranges, explicit lease actions, and a session that can
//! execute only while active under an enabled trust rule at its admission
//! generation.

use coord_types::logical_v1::{BranchOp, CanonicalOperation, KeyRange};

use crate::model::KvModel;

/// An authorizable action; its ceiling bit is its index here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActionKind {
    /// Read.
    Read,
    /// Write.
    Write,
    /// Delete.
    Delete,
    /// Grant a lease.
    LeaseGrant,
    /// Attach to a lease.
    LeaseAttach,
    /// Inspect a lease.
    LeaseInspect,
    /// Renew a lease.
    LeaseRenew,
    /// Revoke a lease.
    LeaseRevoke,
    /// Compact.
    Compact,
}

impl ActionKind {
    /// Ceiling bit.
    pub const fn bit(self) -> u32 {
        1 << (self as u32)
    }
}

/// An allow rule over a key interval (`upper` `None`: unbounded).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    /// Principal.
    pub principal: [u8; 16],
    /// Action.
    pub action: ActionKind,
    /// Namespace.
    pub namespace: [u8; 16],
    /// Inclusive lower bound.
    pub lower: Vec<u8>,
    /// Exclusive upper bound.
    pub upper: Option<Vec<u8>>,
}

/// What the oracle knows about the executing session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionFacts {
    /// Principal.
    pub principal: [u8; 16],
    /// Scope ceiling bits.
    pub ceiling: u32,
    /// Active.
    pub active: bool,
    /// Trust rule enabled.
    pub rule_enabled: bool,
    /// Trust rule generation equals the admission generation.
    pub rule_generation_matches: bool,
}

/// The oracle's decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// The session cannot execute at all.
    SessionInvalid,
    /// Some required permission is missing.
    Denied,
    /// Every required permission is present.
    Allowed,
}

/// The rule set.
#[derive(Clone, Debug, Default)]
pub struct PolicyOracle {
    rules: Vec<Rule>,
}

/// One requirement: an action over `[lower, upper)`.
type Need = (ActionKind, Vec<u8>, Option<Vec<u8>>);

fn exact(key: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut upper = key.to_vec();
    upper.push(0);
    (key.to_vec(), Some(upper))
}

fn of_range(range: &KeyRange) -> (Vec<u8>, Option<Vec<u8>>) {
    match &range.range_end {
        None => exact(&range.key),
        Some(end) => (range.key.clone(), Some(end.clone())),
    }
}

fn everything() -> (Vec<u8>, Option<Vec<u8>>) {
    (Vec::new(), None)
}

fn branch_needs(op: &BranchOp) -> Vec<Need> {
    match op {
        BranchOp::Range(r) => {
            let (l, u) = of_range(&r.range);
            vec![(ActionKind::Read, l, u)]
        }
        BranchOp::Put(p) => {
            let (l, u) = exact(&p.key);
            let mut out = vec![(ActionKind::Write, l.clone(), u.clone())];
            if p.lease.is_some() {
                out.push((ActionKind::LeaseAttach, l.clone(), u.clone()));
            }
            // Returning the previous value is a read.
            if p.prev_kv {
                out.push((ActionKind::Read, l, u));
            }
            out
        }
        BranchOp::DeleteRange(d) => {
            let (l, u) = of_range(&d.range);
            let mut out = vec![(ActionKind::Delete, l.clone(), u.clone())];
            if d.prev_kv {
                out.push((ActionKind::Read, l, u));
            }
            out
        }
    }
}

impl PolicyOracle {
    /// An oracle over `rules`.
    pub fn new(rules: Vec<Rule>) -> Self {
        PolicyOracle { rules }
    }

    fn covered(&self, principal: [u8; 16], namespace: [u8; 16], need: &Need) -> bool {
        let (action, lower, upper) = need;
        self.rules.iter().any(|r| {
            r.principal == principal
                && r.action == *action
                && r.namespace == namespace
                && *lower >= r.lower
                && match (&r.upper, upper) {
                    (None, _) => true,
                    (Some(_), None) => false,
                    (Some(ru), Some(nu)) => nu <= ru,
                }
        })
    }

    /// The permissions `op` needs against `model`'s current state (the
    /// state decides which transaction branch is selected).
    pub fn needs(model: &KvModel, op: &CanonicalOperation) -> Vec<Need> {
        match op {
            CanonicalOperation::Range(r) => {
                let (l, u) = of_range(&r.range);
                vec![(ActionKind::Read, l, u)]
            }
            CanonicalOperation::Put(p) => branch_needs(&BranchOp::Put(p.clone())),
            CanonicalOperation::DeleteRange(d) => branch_needs(&BranchOp::DeleteRange(d.clone())),
            CanonicalOperation::Txn(t) => {
                let mut out: Vec<Need> = t
                    .compares
                    .iter()
                    .map(|c| {
                        let (l, u) = exact(&c.key);
                        (ActionKind::Read, l, u)
                    })
                    .collect();
                let branch = if model.compares_hold(&t.compares) {
                    &t.success
                } else {
                    &t.failure
                };
                for op in branch {
                    out.extend(branch_needs(op));
                }
                out
            }
            CanonicalOperation::Compact { .. } => {
                let (l, u) = everything();
                vec![(ActionKind::Compact, l, u)]
            }
            CanonicalOperation::LeaseGrant { .. } => {
                let (l, u) = everything();
                vec![(ActionKind::LeaseGrant, l, u)]
            }
            CanonicalOperation::LeaseKeepAlive { .. } => {
                let (l, u) = everything();
                vec![(ActionKind::LeaseRenew, l, u)]
            }
            CanonicalOperation::LeaseRevoke { lease_id } => {
                // Revoking deletes the attached keys, so it needs delete
                // permission over each of them besides the revoke action.
                let (l, u) = everything();
                let mut out = vec![(ActionKind::LeaseRevoke, l, u)];
                for key in model.attached_keys(lease_id.0) {
                    let (l, u) = exact(&key);
                    out.push((ActionKind::Delete, l, u));
                }
                out
            }
            CanonicalOperation::LeaseTimeToLive { .. } => {
                let (l, u) = everything();
                vec![(ActionKind::LeaseInspect, l, u)]
            }
            CanonicalOperation::KineCreate(c) => {
                let (l, u) = exact(&c.key);
                vec![(ActionKind::Write, l, u)]
            }
            // Kine update and delete return the entry they saw: a read.
            CanonicalOperation::KineUpdate(c) => {
                let (l, u) = exact(&c.key);
                vec![
                    (ActionKind::Write, l.clone(), u.clone()),
                    (ActionKind::Read, l, u),
                ]
            }
            CanonicalOperation::KineDelete(d) => {
                let (l, u) = exact(&d.key);
                vec![
                    (ActionKind::Delete, l.clone(), u.clone()),
                    (ActionKind::Read, l, u),
                ]
            }
            // It touches no key, so it needs no key permission. What it
            // does need is an admission that may establish a session,
            // which is not a permission over an interval and is checked
            // where admissions are.
            CanonicalOperation::ConsumeAdmission => Vec::new(),
            // The service's own. No caller holds a permission that
            // admits one, and no key interval is what decides: a
            // command carrying an admission at all is refused before
            // any permission is consulted.
            CanonicalOperation::EstablishLeaseAuthority { .. }
            | CanonicalOperation::ExpireLease { .. } => Vec::new(),
        }
    }

    /// Decide whether `session` may run `op` in `namespace`.
    pub fn decide(
        &self,
        model: &KvModel,
        session: &SessionFacts,
        namespace: [u8; 16],
        op: &CanonicalOperation,
    ) -> Decision {
        if !(session.active && session.rule_enabled && session.rule_generation_matches) {
            return Decision::SessionInvalid;
        }
        let needs = Self::needs(model, op);
        let allowed = needs.iter().all(|need| {
            session.ceiling & need.0.bit() != 0 && self.covered(session.principal, namespace, need)
        });
        if allowed {
            Decision::Allowed
        } else {
            Decision::Denied
        }
    }
}
