//! Trusted-boundary bootstrap of sessions and rules (task-18).
//!
//! Production sessions come from consumed admission receipts ordered as
//! internal commands; this module only lowers records a trusted boundary
//! (genesis, tests) already holds into rows.

use coord_core::effect::StoreUpdate;
use coord_state::policy::{Action, PolicyRule, SessionRecord, TrustRule};
use coord_store_api::engine::EngineError;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{PolicyRuleId, PrincipalId, SessionId, TrustRuleId};

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
            })?),
        },
    ])
}

/// Row writing one permission rule.
pub fn rule_update(rule: &PolicyRuleId, record: &PolicyRule) -> Result<StoreUpdate, EngineError> {
    Ok(StoreUpdate {
        collection: Collection::PolicyV1.id(),
        key: codecs::policy_rule_key(&record.principal, rule),
        value: Some(codecs::encode_policy_rule(record)?),
    })
}
