//! Trusted-boundary bootstrap of sessions and rules (task-18).
//!
//! Production sessions come from consumed admission receipts ordered as
//! internal commands; this module only lowers records a trusted boundary
//! (genesis, tests) already holds into rows.

use coord_core::effect::StoreUpdate;
use coord_state::policy::{Action, PolicyRule, SessionRecord, TrustRule};
use coord_store_api::engine::EngineError;
use coord_store_api::registry::Collection;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{NamespaceId, PolicyRuleId, PrincipalId, SessionId, TrustRuleId};

use crate::codecs;

/// Rows creating an enabled trust rule (identified by the session bytes)
/// and a session of `principal` under it with the full ceiling.
pub fn bootstrap_session(
    session: &SessionId,
    principal: PrincipalId,
    window: u32,
    active: bool,
) -> Result<Vec<StoreUpdate>, EngineError> {
    let trust_rule = TrustRuleId(*session.as_bytes());
    Ok(vec![
        StoreUpdate {
            collection: Collection::PolicyV1.id(),
            key: codecs::trust_rule_key(&trust_rule),
            value: Some(codecs::encode_trust_rule(&TrustRule {
                enabled: true,
                generation: 1,
            })?),
        },
        StoreUpdate {
            collection: Collection::SessionV1.id(),
            key: codecs::session_key(session),
            value: Some(codecs::encode_session(&SessionRecord {
                principal,
                scope_ceiling: Action::FULL_CEILING,
                trust_rule,
                rule_generation: 1,
                active,
                window,
                receipt_id: Digest32([0; 32]),
                // A bootstrapped test session has no admission to end.
                expires_at: u64::MAX,
            })?),
        },
    ])
}

/// Row creating the trust rule a domain's configured issuer signs
/// under, enabled at its first generation.
///
/// Genesis, not administration: a domain that trusted nothing could
/// establish no session, and the first session cannot be the one that
/// authorizes writing the rule that would admit it. Every replica writes
/// it from the same configuration when its store is initialized, so it
/// is initial replicated state in the same sense the membership is.
/// Afterwards it is ordinary replicated policy: `PutTrustRule` disables
/// or regenerates it, and a session admitted under an older generation
/// stops executing at that command's position.
pub fn bootstrap_trust_rule(rule: &TrustRuleId) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::PolicyV1.id(),
        key: codecs::trust_rule_key(rule),
        value: Some(codecs::encode_trust_rule(&TrustRule {
            enabled: true,
            generation: 1,
        })?),
    })
}

/// The identity of a permission rule, derived from what it permits.
///
/// Derived rather than chosen so that a rule a domain's genesis grants
/// is the same row on every replica, and so that writing it twice is
/// writing the same rule rather than accumulating duplicates.
pub fn derived_rule_id(rule: &PolicyRule) -> PolicyRuleId {
    let upper = rule.interval.upper.clone().unwrap_or_default();
    let digest = HashDomain::PolicyRuleIdentity.digest(&[
        rule.principal.as_bytes(),
        rule.namespace.as_bytes(),
        &[rule.action as u8, u8::from(rule.interval.upper.is_some())],
        &rule.interval.lower,
        &upper,
    ]);
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest.0[..16]);
    PolicyRuleId(id)
}

/// Rows granting `principal` every action over the whole of
/// `namespace`: a domain's initial administrator.
///
/// Genesis, like [`bootstrap_trust_rule`]. Permission is allow-only and
/// a fresh domain allows nothing, so a domain with no initial grant is
/// one where the first administrative command is itself unauthorized.
/// Narrowing this, and granting anyone else, is ordinary replicated
/// administration afterwards.
pub fn bootstrap_grant(
    principal: PrincipalId,
    namespace: NamespaceId,
) -> Result<Vec<StoreUpdate>, EngineError> {
    Action::ALL
        .into_iter()
        .map(|action| {
            let record = PolicyRule {
                principal,
                action,
                namespace,
                interval: coord_state::policy::KeyInterval::all(),
            };
            rule_update(&derived_rule_id(&record), &record)
        })
        .collect()
}

/// Row writing one permission rule.
pub fn rule_update(rule: &PolicyRuleId, record: &PolicyRule) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::PolicyV1.id(),
        key: codecs::policy_rule_key(&record.principal, rule),
        value: Some(codecs::encode_policy_rule(record)?),
    })
}
