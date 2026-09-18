//! Trust rules and canonical admission receipts (design Sections 9.2,
//! 9.3): a rule binds a configured issuer, a subject kind, an exact
//! audience and required identity attributes to a destination principal,
//! a scope ceiling and a maximum lifetime; deny by default. A verified
//! identity's claims are data the rule matches on, never permission by
//! themselves. The receipt carries the rule identity and generation the
//! verifier saw; the state machine rechecks current policy before
//! creating the session. No raw token ever enters a receipt.

use std::collections::BTreeMap;

use coord_state::policy::{Action, AdmissionReceiptV1};
use coord_types::identity::HashDomain;
use coord_types::ids::{PrincipalId, SessionId, TrustRuleId};

use crate::clock::{ClockHealth, TimeError};
use crate::verifier::{VerifiedIdentity, Workload};

/// The subject kinds a rule can bind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubjectKind {
    /// A human `(issuer, subject)` identity.
    Human,
    /// A Kubernetes service account.
    KubernetesServiceAccount,
    /// A GitHub Actions workflow.
    GithubActions,
}

/// One trust rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustRuleConfig {
    /// Rule identity (as stored in replicated policy).
    pub id: TrustRuleId,
    /// Generation the verifier configuration carries.
    pub generation: u64,
    /// Whether the rule admits anyone.
    pub enabled: bool,
    /// Issuer configuration name.
    pub issuer: String,
    /// Subject kind.
    pub subject: SubjectKind,
    /// Exact audience the token must carry.
    pub audience: String,
    /// Identity attributes that must match exactly (see [`attributes`]).
    pub required: BTreeMap<String, String>,
    /// Destination principal.
    pub principal: PrincipalId,
    /// Scope ceiling (bits of [`Action`]).
    pub scope_ceiling: u32,
    /// Maximum session lifetime in seconds.
    pub max_lifetime_secs: u64,
}

/// What admission produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admitted {
    /// The canonical receipt for the state machine.
    pub receipt: AdmissionReceiptV1,
    /// The session the receipt creates.
    pub session: SessionId,
    /// Conservative validity end (unix seconds).
    pub valid_until: u64,
    /// Rule used.
    pub rule: TrustRuleId,
}

/// Why no receipt was minted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MintError {
    /// No rule matches: deny by default.
    NoRule,
    /// The matching rule is disabled.
    RuleDisabled(TrustRuleId),
    /// The rule's ceiling has bits outside the action set.
    CeilingTooWide,
    /// No conservative validity remains.
    Time(TimeError),
}

/// The attributes of an identity that rules match on. Humans expose only
/// issuer and subject (never a mutable email); workloads expose their
/// stable platform identifiers.
pub fn attributes(identity: &VerifiedIdentity) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert("issuer".into(), identity.issuer.clone());
    out.insert("subject".into(), identity.subject.clone());
    match &identity.workload {
        Some(Workload::KubernetesServiceAccount {
            namespace,
            name,
            uid,
            pod,
        }) => {
            out.insert("namespace".into(), namespace.clone());
            out.insert("serviceaccount".into(), name.clone());
            if let Some(u) = uid {
                out.insert("serviceaccount_uid".into(), u.clone());
            }
            if let Some(p) = pod {
                out.insert("pod".into(), p.clone());
            }
        }
        Some(Workload::GithubActions {
            repository_id,
            repository_owner_id,
            repository,
            workflow_ref,
            environment,
        }) => {
            out.insert("repository_id".into(), repository_id.clone());
            out.insert("repository_owner_id".into(), repository_owner_id.clone());
            out.insert("repository".into(), repository.clone());
            out.insert("workflow_ref".into(), workflow_ref.clone());
            if let Some(e) = environment {
                out.insert("environment".into(), e.clone());
            }
        }
        None => {}
    }
    out
}

const fn kind_of(identity: &VerifiedIdentity) -> SubjectKind {
    match identity.workload {
        None => SubjectKind::Human,
        Some(Workload::KubernetesServiceAccount { .. }) => SubjectKind::KubernetesServiceAccount,
        Some(Workload::GithubActions { .. }) => SubjectKind::GithubActions,
    }
}

fn matches(
    rule: &TrustRuleConfig,
    identity: &VerifiedIdentity,
    attrs: &BTreeMap<String, String>,
) -> bool {
    rule.issuer == identity.name
        && rule.subject == kind_of(identity)
        && identity.audiences.contains(&rule.audience)
        && rule.required.iter().all(|(k, v)| attrs.get(k) == Some(v))
}

/// Mint the receipt for `identity` under the first matching rule, with
/// `entropy` from the world (session identity and receipt uniqueness).
pub fn mint(
    identity: &VerifiedIdentity,
    rules: &[TrustRuleConfig],
    clock: &ClockHealth,
    entropy: &[u8; 32],
) -> Result<Admitted, MintError> {
    let attrs = attributes(identity);
    let rule = rules
        .iter()
        .find(|r| matches(r, identity, &attrs))
        .ok_or(MintError::NoRule)?;
    if !rule.enabled {
        return Err(MintError::RuleDisabled(rule.id));
    }
    if rule.scope_ceiling & !Action::FULL_CEILING != 0 {
        return Err(MintError::CeilingTooWide);
    }
    if !clock.healthy {
        return Err(MintError::Time(TimeError::ClockUnhealthy));
    }
    let by_token = clock.valid_until(identity.expires_at);
    let by_rule = clock.now.saturating_add(rule.max_lifetime_secs);
    let valid_until = by_token.min(by_rule);
    if valid_until <= clock.now {
        return Err(MintError::Time(TimeError::Expired));
    }
    let mut session = [0u8; 16];
    session.copy_from_slice(&entropy[..16]);
    let session = SessionId(session);
    let receipt_id = HashDomain::AdmissionReceipt.digest(&[
        identity.issuer.as_bytes(),
        identity.subject.as_bytes(),
        entropy,
        &clock.now.to_be_bytes(),
    ]);
    Ok(Admitted {
        receipt: AdmissionReceiptV1 {
            receipt_id,
            session,
            principal: rule.principal,
            scope_ceiling: rule.scope_ceiling,
            trust_rule: rule.id,
            rule_generation: rule.generation,
        },
        session,
        valid_until,
        rule: rule.id,
    })
}
