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
//! boot, before an applier exists, and a lost execution group stops the
//! node.
//!
//! **Why clearing on those landings is exact.** A command is planned over
//! a view holding every write landed before it, in execution order: the
//! projection, and within a group the group's own rows and those of the
//! groups in flight. So the context a command would load changes exactly
//! when a plan landed before it wrote one of the rows above, and the
//! cache is emptied when such a plan lands, before the next command
//! plans. Clearing all of it rather than the rows' sessions keeps the
//! rule one line long; the writes are rare (sessions are established
//! once and policy is administered), so it costs a reload a session.
//!
//! [`writes_authorization`] matches every [`Mutation`] without a
//! wildcard, so a new kind of row write is classified when it is added.
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
    /// Times the cache was emptied by a landing that writes one of the
    /// rows a context reads.
    pub cleared: u64,
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

    /// Empty the cache if `mutations`, about to land, write a row a
    /// context reads.
    pub(crate) fn landing(&mut self, mutations: &[Mutation]) {
        if mutations.iter().any(writes_authorization) {
            self.clear();
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

/// Whether `mutation` writes a row [`load_authorization`] reads: the
/// session's record, a trust rule, or a permission rule.
pub(crate) const fn writes_authorization(mutation: &Mutation) -> bool {
    match mutation {
        Mutation::SessionWrite { .. }
        | Mutation::PolicyRuleWrite { .. }
        | Mutation::TrustRuleWrite { .. } => true,
        Mutation::Write { .. }
        | Mutation::Delete { .. }
        | Mutation::LeaseAttach { .. }
        | Mutation::LeaseDetach { .. }
        | Mutation::LeaseWrite { .. }
        | Mutation::GrantWrite { .. }
        | Mutation::LeaseAuthority { .. }
        | Mutation::CompactTo { .. } => false,
    }
}
