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
//! **What it holds.** A session's contexts sit under that session, one a
//! namespace, so a session's record written drops them in one step. The
//! cache is bounded in bytes, not in contexts ([`CAPACITY_BYTES`]; #279's
//! review, debt 3): a context's size follows its principal's rules, and
//! the schema's budget lets one context load up to 16 MiB. A context
//! larger than the bound is never kept; one that would take the cache past
//! it empties the cache first, which needs no eviction order.
//!
//! [`writes_authorization`] matches every [`Mutation`] without a
//! wildcard, so a new kind of row write is classified when it is added,
//! and a test pins each kind's class. Another test writes each kind's row
//! and lands it, and reads the next context from the view.
//! Debug builds load the context again on every hit and stop on a
//! difference, so every test that executes a command checks the rule.
//!
//! [`Applier`]: crate::apply::Applier

use std::collections::HashMap;
use std::mem::size_of;

use coord_state::Mutation;
use coord_state::policy::{Authorization, PolicyRule};
use coord_store_api::engine::OrderedRead;
use coord_types::ids::{NamespaceId, SessionId};

use crate::views::{ViewBudget, ViewBuildError, load_authorization};

/// The most bytes the contexts kept may take, as [`Loaded::bytes`] counts
/// them. Past it the cache is emptied and refills, without an eviction
/// order to maintain. A context of a session with a handful of rules
/// counts a few hundred bytes, so this holds some ten thousand sessions'.
pub(crate) const CAPACITY_BYTES: usize = 4 << 20;

/// What a kept context costs beyond its rules: its place under its
/// session and its session's place in the map, counted for every context
/// although a session's place is shared by its namespaces.
const ENTRY_BYTES: usize =
    size_of::<(NamespaceId, Loaded)>() + size_of::<(SessionId, Vec<(NamespaceId, Loaded)>)>();

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

    /// What keeping it costs: [`ENTRY_BYTES`], and a context's rules with
    /// the bytes of their intervals' keys. The session and trust-rule
    /// records are inline. It counts lengths, not what the allocator
    /// reserved for them, which can be up to twice as much.
    fn bytes(&self) -> usize {
        ENTRY_BYTES
            + match self {
                Loaded::OverBudget => 0,
                Loaded::Context(context) => {
                    context.rules.len() * size_of::<PolicyRule>()
                        + context
                            .rules
                            .iter()
                            .map(|rule| {
                                rule.interval.lower.len()
                                    + rule.interval.upper.as_ref().map_or(0, Vec::len)
                            })
                            .sum::<usize>()
                }
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
    /// Times the cache was emptied because a context loaded would have
    /// taken it past [`CAPACITY_BYTES`].
    pub overflowed: u64,
    /// Contexts loaded and not kept, each larger than [`CAPACITY_BYTES`]
    /// alone.
    pub too_large: u64,
}

/// Authorization contexts under the schema's budget, by session, then
/// namespace.
#[derive(Debug, Default)]
pub(crate) struct AuthorizationCache {
    /// A session's contexts, one a namespace it has executed in, and the
    /// bytes they count.
    entries: HashMap<SessionId, (Vec<(NamespaceId, Loaded)>, usize)>,
    /// The bytes every context kept counts.
    bytes: usize,
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
        let held = self.entries.get(session).and_then(|(contexts, _)| {
            contexts
                .iter()
                .find(|(kept, _)| *kept == namespace)
                .map(|(_, loaded)| loaded)
        });
        if let Some(loaded) = held {
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
        let out = loaded.get();
        let bytes = loaded.bytes();
        if bytes > CAPACITY_BYTES {
            self.stats.too_large += 1;
            return out;
        }
        if self.bytes + bytes > CAPACITY_BYTES {
            self.stats.overflowed += 1;
            self.entries.clear();
            self.bytes = 0;
        }
        let (contexts, held) = self.entries.entry(*session).or_default();
        contexts.push((namespace, loaded));
        *held += bytes;
        self.bytes += bytes;
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
        if let Some((_, bytes)) = self.entries.remove(session) {
            self.bytes -= bytes;
            self.stats.forgotten += 1;
        }
    }

    /// Empty the cache.
    pub(crate) fn clear(&mut self) {
        if !self.entries.is_empty() {
            self.stats.cleared += 1;
        }
        self.entries.clear();
        self.bytes = 0;
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
    use coord_store_api::engine::{
        CollectionId, Direction, EngineError, Row, RowPage, ScanRequest,
    };
    use coord_store_api::registry::Collection;
    use coord_types::identity::Digest32;
    use coord_types::ids::{
        KvRevision, LeaseAuthorityEpoch, LeaseGeneration, LeaseId, PolicyRuleId, PrincipalId,
        TrustRuleId,
    };

    use crate::codecs;

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
        for (session, namespace) in [(S, NS), (S, NS2), (T, NS)] {
            let loaded = Loaded::OverBudget;
            let bytes = loaded.bytes();
            let (contexts, held) = cache.entries.entry(session).or_default();
            contexts.push((namespace, loaded));
            *held += bytes;
            cache.bytes += bytes;
        }
        cache
    }

    /// The (session, namespace) pairs held, in order, and a check that
    /// the bytes counted are the contexts'.
    fn keys(cache: &AuthorizationCache) -> Vec<(SessionId, NamespaceId)> {
        let mut keys: Vec<_> = cache
            .entries
            .iter()
            .flat_map(|(session, (contexts, _))| contexts.iter().map(|(ns, _)| (*session, *ns)))
            .collect();
        keys.sort();
        let counted: usize = cache
            .entries
            .values()
            .map(|(contexts, bytes)| {
                assert_eq!(
                    *bytes,
                    contexts.iter().map(|(_, l)| l.bytes()).sum::<usize>()
                );
                bytes
            })
            .sum();
        assert_eq!(cache.bytes, counted);
        keys
    }

    /// A session's record written drops that session's contexts, in every
    /// namespace, and keeps the others' (#279's review, debt 4).
    #[test]
    fn a_session_write_drops_only_its_own_contexts() {
        let mut cache = held();
        cache.landing(&[session_write(S)]);
        assert_eq!(keys(&cache), vec![(T, NS)]);
        assert_eq!((cache.stats.forgotten, cache.stats.cleared), (1, 0));
        // A session with no context: nothing to drop, nothing counted.
        cache.landing(&[session_write(SessionId([0x22; 16]))]);
        assert_eq!(keys(&cache), vec![(T, NS)]);
        assert_eq!(cache.stats.forgotten, 1);
    }

    /// A rule written, anywhere in a plan, empties the cache.
    #[test]
    fn a_rule_write_empties_the_cache() {
        for rule in [policy_write(), trust_write()] {
            let mut cache = held();
            cache.landing(&[session_write(T), rule, session_write(S)]);
            assert!(keys(&cache).is_empty());
            assert_eq!(cache.bytes, 0);
            assert_eq!((cache.stats.forgotten, cache.stats.cleared), (1, 1));
        }
        let mut cache = held();
        cache.landing(&[Mutation::CompactTo {
            revision: revision(),
        }]);
        assert_eq!(keys(&cache).len(), 3);
        assert_eq!(cache.stats, AuthorizationCacheStats::default());
    }

    /// Rows by collection and key: the view a context is loaded from.
    #[derive(Default)]
    struct Rows(std::collections::BTreeMap<(CollectionId, Vec<u8>), Vec<u8>>);

    impl OrderedRead for Rows {
        fn get(
            &self,
            collection: CollectionId,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, EngineError> {
            Ok(self.0.get(&(collection, key.to_vec())).cloned())
        }

        fn scan_page(
            &self,
            collection: CollectionId,
            request: &ScanRequest,
        ) -> Result<RowPage, EngineError> {
            let mut rows: Vec<Row> = self
                .0
                .iter()
                .filter(|((c, k), _)| {
                    *c == collection && request.contains(k) && request.past_cursor(k)
                })
                .map(|((_, key), value)| Row {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect();
            if request.direction == Direction::Reverse {
                rows.reverse();
            }
            Ok(RowPage {
                rows,
                exhausted: true,
            })
        }
    }

    impl Rows {
        /// Write the row `mutation` writes, as the materializer lowers it,
        /// for the three kinds a context reads.
        fn write(&mut self, mutation: &Mutation) {
            let (collection, key, value) = match mutation {
                Mutation::SessionWrite { session, record } => (
                    Collection::SessionV1,
                    codecs::session_key(session),
                    record.as_ref().map(|r| codecs::encode_session(r).unwrap()),
                ),
                Mutation::PolicyRuleWrite {
                    principal,
                    rule,
                    record,
                } => (
                    Collection::PolicyV1,
                    codecs::policy_rule_key(principal, rule),
                    record
                        .as_ref()
                        .map(|r| codecs::encode_policy_rule(r).unwrap()),
                ),
                Mutation::TrustRuleWrite { rule, record } => (
                    Collection::PolicyV1,
                    codecs::trust_rule_key(rule),
                    Some(codecs::encode_trust_rule(record).unwrap()),
                ),
                other => panic!("not a row a context reads: {other:?}"),
            };
            let key = (collection.id(), key);
            match value {
                Some(value) => self.0.insert(key, value),
                None => self.0.remove(&key),
            };
        }
    }

    /// A rule of ALICE's in `namespace`.
    fn rule_in(id: u8, namespace: NamespaceId, action: Action) -> Mutation {
        Mutation::PolicyRuleWrite {
            principal: ALICE,
            rule: PolicyRuleId([id; 16]),
            record: Some(PolicyRule {
                principal: ALICE,
                action,
                namespace,
                interval: KeyInterval::all(),
            }),
        }
    }

    /// Each invalidation arm, as the applier takes it: a plan's row is
    /// written, the plan lands, and the next load reads the row from the
    /// view. With any arm of [`writes_authorization`] or [`landing`]
    /// removed, the next load is a hit on the context from before the
    /// write: release builds return it, and debug builds stop on it
    /// (#279's review, debt 1).
    ///
    /// [`landing`]: AuthorizationCache::landing
    #[test]
    fn each_write_a_context_reads_is_read_by_the_next_load() {
        let mut rows = Rows::default();
        let mut cache = AuthorizationCache::default();
        let land = |rows: &mut Rows, cache: &mut AuthorizationCache, mutation: Mutation| {
            rows.write(&mutation);
            cache.landing(std::slice::from_ref(&mutation));
        };
        land(&mut rows, &mut cache, session_write(S));
        land(
            &mut rows,
            &mut cache,
            Mutation::TrustRuleWrite {
                rule: TrustRuleId([6; 16]),
                record: TrustRule {
                    enabled: true,
                    generation: 1,
                },
            },
        );
        land(&mut rows, &mut cache, rule_in(1, NS, Action::Read));
        let load = |cache: &mut AuthorizationCache, rows: &Rows| {
            let mut probe = AuthorizationCache::default();
            let fresh = probe.load(rows, NS, &S).unwrap();
            let cached = cache.load(rows, NS, &S).unwrap();
            assert_eq!(cached, fresh, "the cache answered from before a write");
            cached
        };
        let first = load(&mut cache, &rows);
        assert!(first.valid_session().is_some());
        assert_eq!(first.rules.len(), 1);
        assert_eq!(load(&mut cache, &rows), first, "a hit");
        assert_eq!((cache.stats.loads, cache.stats.hits), (1, 1));

        // The permission rule arm: a rule written in the namespace.
        land(&mut rows, &mut cache, rule_in(2, NS, Action::Write));
        assert_eq!(load(&mut cache, &rows).rules.len(), 2);
        // ... and one deleted.
        land(
            &mut rows,
            &mut cache,
            Mutation::PolicyRuleWrite {
                principal: ALICE,
                rule: PolicyRuleId([2; 16]),
                record: None,
            },
        );
        assert_eq!(load(&mut cache, &rows).rules.len(), 1);
        // The trust rule arm: the session's rule disabled.
        land(&mut rows, &mut cache, trust_write());
        assert!(load(&mut cache, &rows).valid_session().is_none());
        // The session arm: the session's record written again, active
        // under the rule's new generation.
        land(
            &mut rows,
            &mut cache,
            Mutation::TrustRuleWrite {
                rule: TrustRuleId([6; 16]),
                record: TrustRule {
                    enabled: true,
                    generation: 2,
                },
            },
        );
        assert!(load(&mut cache, &rows).valid_session().is_none());
        let Mutation::SessionWrite {
            session,
            record: Some(mut record),
        } = session_write(S)
        else {
            unreachable!()
        };
        record.rule_generation = 2;
        land(
            &mut rows,
            &mut cache,
            Mutation::SessionWrite {
                session,
                record: Some(record),
            },
        );
        assert!(load(&mut cache, &rows).valid_session().is_some());
        // ... and the session retired.
        land(
            &mut rows,
            &mut cache,
            Mutation::SessionWrite {
                session: S,
                record: None,
            },
        );
        assert_eq!(load(&mut cache, &rows).session, None);
        // Each write was read by the load after it: seven loads, one hit.
        assert_eq!((cache.stats.loads, cache.stats.hits), (7, 1));
    }

    /// The bound is in bytes: a cache that would pass it is emptied
    /// before the context is kept, and a context larger than it alone is
    /// never kept (#279's review, debt 3).
    #[test]
    fn the_cache_is_bounded_in_bytes() {
        let mut rows = Rows::default();
        let mut cache = AuthorizationCache::default();
        // A context of ALICE's sessions whose rules take half the bound and
        // a little.
        let half = CAPACITY_BYTES / 2 + 1;
        let lower = vec![b'k'; half];
        rows.write(&Mutation::PolicyRuleWrite {
            principal: ALICE,
            rule: PolicyRuleId([1; 16]),
            record: Some(PolicyRule {
                principal: ALICE,
                action: Action::Read,
                namespace: NS,
                interval: KeyInterval { lower, upper: None },
            }),
        });
        let sessions: Vec<SessionId> = (0x40..0x44).map(|b| SessionId([b; 16])).collect();
        for session in &sessions {
            rows.write(&session_write(*session));
        }
        // Each session's context in NS reads the same large rule.
        cache.load(&rows, NS, &sessions[0]).unwrap();
        assert!(cache.bytes > CAPACITY_BYTES / 2 && cache.bytes <= CAPACITY_BYTES);
        assert_eq!(keys(&cache), vec![(sessions[0], NS)]);
        // A second would pass the bound: the cache is emptied first.
        cache.load(&rows, NS, &sessions[1]).unwrap();
        assert_eq!(keys(&cache), vec![(sessions[1], NS)]);
        assert_eq!(cache.stats.overflowed, 1);
        // Small contexts (NS2 has no rules) fit beside it.
        for session in &sessions {
            cache.load(&rows, NS2, session).unwrap();
        }
        assert_eq!(keys(&cache).len(), 1 + sessions.len());
        assert!(cache.bytes <= CAPACITY_BYTES);
        assert_eq!(cache.stats.overflowed, 1);
        // A context larger than the bound alone is loaded, not kept.
        rows.write(&Mutation::PolicyRuleWrite {
            principal: ALICE,
            rule: PolicyRuleId([2; 16]),
            record: Some(PolicyRule {
                principal: ALICE,
                action: Action::Write,
                namespace: NS,
                interval: KeyInterval {
                    lower: vec![b'k'; CAPACITY_BYTES],
                    upper: None,
                },
            }),
        });
        cache.clear();
        let context = cache.load(&rows, NS, &sessions[2]).unwrap();
        assert_eq!(context.rules.len(), 2);
        assert!(keys(&cache).is_empty());
        assert_eq!((cache.stats.too_large, cache.bytes), (1, 0));
    }
}
