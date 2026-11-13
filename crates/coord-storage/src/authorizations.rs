//! The authorization contexts an [`Applier`] has loaded, kept between
//! commands (#185, lead 15:27).
//!
//! Every client command executes under its session's authorization: the
//! session record, its trust rule, and its principal's permission rules
//! in the namespace ([`load_authorization`]). Loading it is two point
//! reads and a scan, and every voter loads it again for every command,
//! although it changes only when a command writes one of those rows.
//!
//! **What a context depends on.** The rows [`load_authorization`] reads:
//! `session_v1` at the session, and `policy_v1` at the session's trust
//! rule and under its principal's prefix. With the schema's budget,
//! which is a constant, nothing else: no clock, no node setting.
//!
//! **Who writes those rows while an applier runs.** Only the applier
//! itself, landing a plan with a [`Mutation::SessionWrite`],
//! [`Mutation::PolicyRuleWrite`] or [`Mutation::TrustRuleWrite`]:
//! establishing a session, retiring one, a refresh reuse that retires
//! one, and the two policy writes. A projection is reinstalled only at
//! boot, before an applier exists.
//!
//! **Why dropping contexts on those landings is exact.** A command is
//! planned over a view holding every write landed before it, in execution
//! order: the projection, and within a group the group's own rows and
//! those of the groups in flight. So the context a command would load
//! changes exactly when a plan landed before it wrote one of the rows
//! above, and the contexts that read the row are dropped when such a plan
//! lands, before the next command plans:
//!
//! * a [`Mutation::SessionWrite`] names its session, and only that
//!   session's contexts read its row, so only they are dropped (#279's
//!   review, debt 4: an establishment no longer reloads every other
//!   session);
//! * a trust or permission rule may be read by any session's context, so
//!   a rule write empties the cache. Those writes are administered, and
//!   rare.
//!
//! **Where the rows may have moved under the cache.** Whatever the store
//! takes back or settles: a reconcile, wherever it is called from, and a
//! lost execution group. Each empties the cache (#279's review, debt 2),
//! so that it never rests on the voter stopping after the loss.
//!
//! [`writes_authorization`] matches every [`Mutation`] without a
//! wildcard, so a new kind of row write is classified when it is added,
//! and a test pins each kind's class.
//! Debug builds load the context again on every hit and stop on a
//! difference, so every test that executes a command checks the rule.
//!
//! [`Applier`]: crate::apply::Applier

use std::collections::HashMap;

use coord_state::Mutation;
use coord_state::policy::Authorization;
use coord_store_api::engine::OrderedRead;
use coord_types::ids::{NamespaceId, SessionId};

use crate::views::{ViewBudget, ViewBuildError, load_authorization};

/// The most contexts kept. Past it the cache is emptied and refills,
/// which bounds its memory to a few thousand sessions' rules without an
/// eviction order to maintain.
pub(crate) const CAPACITY: usize = 4096;

/// What loading a context under the schema's budget gave: the context,
/// or a refusal because it is larger than the budget. Both are replicated
/// results, the same on every voter. An engine failure is this node's
/// and is never kept.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Loaded {
    Context(Authorization),
    OverBudget,
}

impl Loaded {
    fn of(loaded: Result<Authorization, ViewBuildError>) -> Result<Loaded, ViewBuildError> {
        match loaded {
            Ok(context) => Ok(Loaded::Context(context)),
            Err(ViewBuildError::BudgetExceeded) => Ok(Loaded::OverBudget),
            Err(e) => Err(e),
        }
    }

    fn get(&self) -> Result<Authorization, ViewBuildError> {
        match self {
            Loaded::Context(context) => Ok(context.clone()),
            Loaded::OverBudget => Err(ViewBuildError::BudgetExceeded),
        }
    }
}

/// How the cache has been used since the applier started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuthorizationCacheStats {
    /// Contexts served from the cache.
    pub hits: u64,
    /// Contexts loaded from the view.
    pub loads: u64,
    /// Times the cache was emptied, holding contexts: by a landing that
    /// writes a rule, a reconcile or a lost group.
    pub cleared: u64,
    /// Times a landing that writes a session's record dropped that
    /// session's contexts, holding some.
    pub forgotten: u64,
}

/// Authorization contexts under the schema's budget, by session and
/// namespace.
#[derive(Debug, Default)]
pub(crate) struct AuthorizationCache {
    entries: HashMap<(SessionId, NamespaceId), Loaded>,
    stats: AuthorizationCacheStats,
}

impl AuthorizationCache {
    /// The context `session` executes under in `namespace`, as
    /// [`load_authorization`] would read it from `view` under
    /// [`ViewBudget::SCHEMA`].
    ///
    /// `view` must hold every write landed so far, as the planning view
    /// does.
    pub(crate) fn load<V: OrderedRead>(
        &mut self,
        view: &V,
        namespace: NamespaceId,
        session: &SessionId,
    ) -> Result<Authorization, ViewBuildError> {
        let key = (*session, namespace);
        if let Some(loaded) = self.entries.get(&key) {
            self.stats.hits += 1;
            #[cfg(debug_assertions)]
            if let Ok(fresh) = Loaded::of(load_authorization(
                view,
                namespace,
                session,
                ViewBudget::SCHEMA,
            )) {
                assert_eq!(
                    &fresh, loaded,
                    "a cached authorization context differs from the view's"
                );
            }
            return loaded.get();
        }
        self.stats.loads += 1;
        let loaded = Loaded::of(load_authorization(
            view,
            namespace,
            session,
            ViewBudget::SCHEMA,
        ))?;
        if self.entries.len() >= CAPACITY {
            self.entries.clear();
        }
        let out = loaded.get();
        self.entries.insert(key, loaded);
        out
    }

    /// Drop the contexts that read a row `mutations`, about to land,
    /// write: a written session's own, or all of them for a rule.
    pub(crate) fn landing(&mut self, mutations: &[Mutation]) {
        for mutation in mutations {
            match writes_authorization(mutation) {
                Writes::Nothing => {}
                Writes::Session(session) => self.forget(session),
                Writes::Rules => {
                    self.clear();
                    return;
                }
            }
        }
    }

    /// Drop `session`'s contexts, in every namespace.
    fn forget(&mut self, session: &SessionId) {
        let held = self.entries.len();
        self.entries.retain(|(kept, _), _| kept != session);
        if self.entries.len() < held {
            self.stats.forgotten += 1;
        }
    }

    /// Empty the cache.
    pub(crate) fn clear(&mut self) {
        if !self.entries.is_empty() {
            self.stats.cleared += 1;
        }
        self.entries.clear();
    }

    /// How the cache has been used.
    pub(crate) const fn stats(&self) -> AuthorizationCacheStats {
        self.stats
    }
}

/// Which of the rows [`load_authorization`] reads a mutation writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Writes<'a> {
    /// None of them.
    Nothing,
    /// The record of this session, which only its own contexts read.
    Session(&'a SessionId),
    /// A trust rule or a permission rule, which any session's context may
    /// read.
    Rules,
}

/// Which of the rows [`load_authorization`] reads `mutation` writes: the
/// session's record, a trust rule, or a permission rule.
pub(crate) const fn writes_authorization(mutation: &Mutation) -> Writes<'_> {
    match mutation {
        Mutation::SessionWrite { session, .. } => Writes::Session(session),
        Mutation::PolicyRuleWrite { .. } | Mutation::TrustRuleWrite { .. } => Writes::Rules,
        Mutation::Write { .. }
        | Mutation::Delete { .. }
        | Mutation::LeaseAttach { .. }
        | Mutation::LeaseDetach { .. }
        | Mutation::LeaseWrite { .. }
        | Mutation::GrantWrite { .. }
        | Mutation::LeaseAuthority { .. }
        | Mutation::CompactTo { .. } => Writes::Nothing,
    }
}

#[cfg(test)]
mod tests {
    use coord_state::{
        Action, GrantKind, GrantRecord, GrantState, KeyInterval, KvEntry, LeasePurpose,
        LeaseRecord, LeaseStatus, PolicyRule, SessionRecord, TrustRule,
    };
    use coord_types::identity::Digest32;
    use coord_types::ids::{
        KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, PolicyRuleId, PrincipalId,
        TrustRuleId,
    };

    use super::*;

    const S: SessionId = SessionId([1; 16]);
    const T: SessionId = SessionId([2; 16]);
    const NS: NamespaceId = NamespaceId([3; 16]);
    const NS2: NamespaceId = NamespaceId([4; 16]);
    const ALICE: PrincipalId = PrincipalId([5; 16]);

    fn revision() -> KvRevision {
        KvRevision::new(1).unwrap()
    }

    fn generation() -> LeaseGeneration {
        LeaseGeneration::new(1).unwrap()
    }

    fn entry() -> KvEntry {
        KvEntry {
            value: b"v".to_vec(),
            create_revision: revision(),
            mod_revision: revision(),
            version: 1,
            lease: None,
            lease_generation: None,
        }
    }

    fn session_write(session: SessionId) -> Mutation {
        Mutation::SessionWrite {
            session,
            record: Some(SessionRecord {
                principal: ALICE,
                scope_ceiling: Action::FULL_CEILING,
                trust_rule: TrustRuleId([6; 16]),
                rule_generation: 1,
                active: true,
                window: 16,
                receipt_id: Digest32([7; 32]),
                expires_at: 1_000,
            }),
        }
    }

    fn policy_write() -> Mutation {
        Mutation::PolicyRuleWrite {
            principal: ALICE,
            rule: PolicyRuleId([8; 16]),
            record: Some(PolicyRule {
                principal: ALICE,
                action: Action::Read,
                namespace: NS,
                interval: KeyInterval::all(),
            }),
        }
    }

    fn trust_write() -> Mutation {
        Mutation::TrustRuleWrite {
            rule: TrustRuleId([6; 16]),
            record: TrustRule {
                enabled: false,
                generation: 2,
            },
        }
    }

    /// Each kind of row write, a line a kind: the three that write a row a
    /// context reads, and the eight that do not (#279's review, debt 1).
    /// Flipping any arm of [`writes_authorization`] fails its line.
    #[test]
    fn each_kind_of_row_write_is_classified() {
        let lease = LeaseId([9; 16]);
        assert_eq!(writes_authorization(&session_write(S)), Writes::Session(&S));
        assert_eq!(
            writes_authorization(&Mutation::SessionWrite {
                session: T,
                record: None
            }),
            Writes::Session(&T)
        );
        assert_eq!(writes_authorization(&policy_write()), Writes::Rules);
        assert_eq!(
            writes_authorization(&Mutation::PolicyRuleWrite {
                principal: ALICE,
                rule: PolicyRuleId([8; 16]),
                record: None,
            }),
            Writes::Rules
        );
        assert_eq!(writes_authorization(&trust_write()), Writes::Rules);
        let nothing = [
            Mutation::Write {
                key: b"k".to_vec(),
                entry: entry(),
            },
            Mutation::Delete {
                key: b"k".to_vec(),
                prev: entry(),
            },
            Mutation::LeaseAttach {
                lease,
                key: b"k".to_vec(),
                generation: generation(),
                mod_revision: revision(),
            },
            Mutation::LeaseDetach {
                lease,
                key: b"k".to_vec(),
            },
            Mutation::LeaseWrite {
                lease,
                record: LeaseRecord {
                    namespace: NS,
                    generation: generation(),
                    owner: ALICE,
                    ttl_seconds: 10,
                    renewal_sequence: 0,
                    purpose: LeasePurpose::Native,
                    status: LeaseStatus::Active,
                    attached_keys: 0,
                    attached_bytes: 0,
                },
            },
            Mutation::GrantWrite {
                commitment: Digest32([10; 32]),
                record: GrantRecord {
                    kind: GrantKind::Receipt,
                    state: GrantState::Consumed,
                    generation: 1,
                    current_secret: None,
                    session: Some(S),
                },
            },
            Mutation::LeaseAuthority {
                epoch: LeaseAuthorityEpoch::new(1).unwrap(),
            },
            Mutation::CompactTo {
                revision: revision(),
            },
        ];
        for mutation in &nothing {
            assert_eq!(
                writes_authorization(mutation),
                Writes::Nothing,
                "{mutation:?}"
            );
        }
    }

    /// A cache holding S in two namespaces and T in one.
    fn held() -> AuthorizationCache {
        let mut cache = AuthorizationCache::default();
        for key in [(S, NS), (S, NS2), (T, NS)] {
            cache.entries.insert(key, Loaded::OverBudget);
        }
        cache
    }

    /// A session's record written drops that session's contexts, in every
    /// namespace, and keeps the others' (#279's review, debt 4).
    #[test]
    fn a_session_write_drops_only_its_own_contexts() {
        let mut cache = held();
        cache.landing(&[session_write(S)]);
        assert_eq!(cache.entries.keys().collect::<Vec<_>>(), vec![&(T, NS)]);
        assert_eq!((cache.stats.forgotten, cache.stats.cleared), (1, 0));
        // A session with no context: nothing to drop, nothing counted.
        cache.landing(&[session_write(SessionId([0x22; 16]))]);
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.stats.forgotten, 1);
    }

    /// A rule written, anywhere in a plan, empties the cache.
    #[test]
    fn a_rule_write_empties_the_cache() {
        for rule in [policy_write(), trust_write()] {
            let mut cache = held();
            cache.landing(&[session_write(T), rule, session_write(S)]);
            assert!(cache.entries.is_empty());
            assert_eq!((cache.stats.forgotten, cache.stats.cleared), (1, 1));
        }
        let mut cache = held();
        cache.landing(&[Mutation::CompactTo {
            revision: revision(),
        }]);
        assert_eq!(cache.entries.len(), 3);
        assert_eq!(cache.stats, AuthorizationCacheStats::default());
    }
}
