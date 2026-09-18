//! task-18 acceptance at the planner: sessions with immutable principal and
//! ceiling under trust-rule generations, single-use receipts and codes,
//! refresh rotation with reuse revocation, branch-specific comparison and
//! operation permissions, full range containment, explicit lease actions,
//! ordered policy changes after admission, policy advancing execution
//! without a KV revision, and a differential check against the independent
//! oracle.

mod common;

use std::collections::BTreeMap;

use common::*;
use coord_oracle::model::KvModel;
use coord_oracle::policy::{ActionKind, Decision, PolicyOracle, Rule, SessionFacts};
use coord_state::policy::{
    Action, AdmissionReceiptV1, GrantKind, GrantState, KeyInterval, PolicyRule, TrustRule,
};
use coord_state::{InternalCommand, Outcome};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use proptest::prelude::*;

const S1: SessionId = SessionId([0x51; 16]);
const S2: SessionId = SessionId([0x52; 16]);
const RULE: TrustRuleId = TrustRuleId([0x71; 16]);

fn digest(b: u8) -> Digest32 {
    Digest32([b; 32])
}

fn receipt(id: u8, session: SessionId, principal: PrincipalId, ceiling: u32) -> AdmissionReceiptV1 {
    AdmissionReceiptV1 {
        receipt_id: digest(id),
        session,
        principal,
        scope_ceiling: ceiling,
        trust_rule: RULE,
        rule_generation: 1,
    }
}

fn admit(r: AdmissionReceiptV1) -> InternalCommand {
    InternalCommand::ConsumeAdmission {
        namespace: NS,
        receipt: r,
        code: None,
        refresh_family: None,
        window: 16,
    }
}

fn trust(enabled: bool, generation: u64) -> InternalCommand {
    InternalCommand::PutTrustRule {
        namespace: NS,
        rule: RULE,
        record: TrustRule {
            enabled,
            generation,
        },
    }
}

fn allow(id: u8, principal: PrincipalId, action: Action, interval: KeyInterval) -> InternalCommand {
    InternalCommand::PutPolicyRule {
        namespace: NS,
        principal,
        rule: PolicyRuleId([id; 16]),
        record: Some(PolicyRule {
            principal,
            action,
            namespace: NS,
            interval,
        }),
    }
}

fn remove(id: u8, principal: PrincipalId) -> InternalCommand {
    InternalCommand::PutPolicyRule {
        namespace: NS,
        principal,
        rule: PolicyRuleId([id; 16]),
        record: None,
    }
}

fn interval(lo: &[u8], hi: &[u8]) -> KeyInterval {
    KeyInterval {
        lower: lo.to_vec(),
        upper: Some(hi.to_vec()),
    }
}

fn get(range: KeyRange) -> LogicalRequest {
    req(CanonicalOperation::Range(RangeOp {
        range,
        revision: None,
        limit: 0,
        keys_only: false,
        count_only: false,
    }))
}

fn del(range: KeyRange) -> LogicalRequest {
    req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range,
        prev_kv: false,
    }))
}

/// A fixture with an enabled trust rule and Alice's session S1.
fn with_alice() -> Fixture {
    let mut f = Fixture::new();
    assert_eq!(
        f.run_internal(&trust(true, 1)).response.outcome,
        Outcome::PolicyUpdated
    );
    let p = f.run_internal(&admit(receipt(1, S1, ALICE, Action::FULL_CEILING)));
    assert_eq!(p.response.outcome, Outcome::SessionCreated { session: S1 });
    assert!(
        p.revision.is_none(),
        "sessions advance execution without a KV revision"
    );
    f
}

#[test]
fn sessions_are_single_use_receipts_under_current_trust_rules() {
    let mut f = Fixture::new();
    // No trust rule yet: the verifier's word is not enough.
    assert_eq!(
        f.run_internal(&admit(receipt(1, S1, ALICE, Action::FULL_CEILING)))
            .response
            .outcome,
        Outcome::ErrTrustRuleInvalid
    );
    f.run_internal(&trust(true, 1));
    assert_eq!(
        f.run_internal(&admit(receipt(1, S1, ALICE, Action::FULL_CEILING)))
            .response
            .outcome,
        Outcome::SessionCreated { session: S1 }
    );
    let s1 = f.sessions[&S1].clone();
    assert_eq!(s1.principal, ALICE);
    assert_eq!(s1.rule_generation, 1);
    assert_eq!(f.grants[&digest(1)].state, GrantState::Consumed);
    // The receipt cannot be consumed twice, nor can the session identity.
    assert_eq!(
        f.run_internal(&admit(receipt(1, S2, BOB, Action::FULL_CEILING)))
            .response
            .outcome,
        Outcome::ErrReceiptConsumed
    );
    assert_eq!(
        f.run_internal(&admit(receipt(2, S1, BOB, Action::FULL_CEILING)))
            .response
            .outcome,
        Outcome::ErrReceiptConsumed
    );
    assert!(!f.sessions.contains_key(&S2));
    // A receipt at another rule generation is stale.
    let mut stale = receipt(3, S2, BOB, Action::FULL_CEILING);
    stale.rule_generation = 2;
    assert_eq!(
        f.run_internal(&admit(stale)).response.outcome,
        Outcome::ErrTrustRuleInvalid
    );
    // Retirement is ordered and permanent.
    assert_eq!(
        f.run_internal(&InternalCommand::RetireSession {
            namespace: NS,
            session: S1
        })
        .response
        .outcome,
        Outcome::SessionRetired
    );
    assert!(!f.sessions[&S1].active);
    assert_eq!(
        f.run_internal(&InternalCommand::RetireSession {
            namespace: NS,
            session: S1
        })
        .response
        .outcome,
        Outcome::ErrSessionInvalid
    );
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"1", None)).response.outcome,
        Outcome::ErrSessionInvalid
    );
    // Unknown session: deny by default.
    assert_eq!(
        f.run_session(&S2, &put(b"a", b"1", None)).response.outcome,
        Outcome::ErrSessionInvalid
    );
}

#[test]
fn disabling_or_regenerating_the_trust_rule_invalidates_its_sessions() {
    let mut f = with_alice();
    f.run_internal(&allow(1, ALICE, Action::Write, KeyInterval::all()));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"1", None)).response.outcome,
        Outcome::Put { prev: None }
    );
    f.run_internal(&trust(false, 1));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"2", None)).response.outcome,
        Outcome::ErrSessionInvalid
    );
    // Re-enabling at the same generation restores; regenerating does not.
    f.run_internal(&trust(true, 1));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"2", None)).response.outcome,
        Outcome::Put { prev: None }.clone_prev(&f, b"a")
    );
    f.run_internal(&trust(true, 2));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"3", None)).response.outcome,
        Outcome::ErrSessionInvalid
    );
    assert_eq!(f.current[b"a".as_slice()].value, b"2".to_vec());
    // A new session admitted at generation 2 works; S1 stays invalid.
    let mut r = receipt(9, S2, ALICE, Action::FULL_CEILING);
    r.rule_generation = 2;
    f.run_internal(&admit(r));
    assert_eq!(
        f.run_session(&S2, &put(b"a", b"3", None)).response.outcome,
        Outcome::Put { prev: None }.clone_prev(&f, b"a")
    );
}

/// Helper: the `Put` outcome with `prev` taken from the fixture (prev_kv
/// is false in `put`, so it is always `None`); keeps assertions short.
trait ClonePrev {
    fn clone_prev(self, f: &Fixture, key: &[u8]) -> Outcome;
}
impl ClonePrev for Outcome {
    fn clone_prev(self, _f: &Fixture, _key: &[u8]) -> Outcome {
        self
    }
}

#[test]
fn permissions_are_branch_specific_fully_contained_and_explicit_for_leases() {
    let mut f = with_alice();
    // Alice may read [a, m), write [a, c) and delete exactly "b".
    f.run_internal(&allow(1, ALICE, Action::Read, interval(b"a", b"m")));
    f.run_internal(&allow(2, ALICE, Action::Write, interval(b"a", b"c")));
    f.run_internal(&allow(3, ALICE, Action::Delete, KeyInterval::exact(b"b")));
    f.run(&put(b"b", b"0", None)); // trusted seed, revision 1
    // Range permissions must contain the full interval.
    assert!(matches!(
        f.run_session(&S1, &get(KeyRange::interval(b"a".to_vec(), b"m".to_vec())))
            .response
            .outcome,
        Outcome::Range { .. }
    ));
    let p = f.run_session(&S1, &get(KeyRange::interval(b"a".to_vec(), b"n".to_vec())));
    assert_eq!(p.response.outcome, Outcome::ErrPermissionDenied);
    assert!(p.revision.is_none() && p.mutations.is_empty());
    assert_eq!(
        f.run_session(&S1, &get(KeyRange::exact(b"z".to_vec())))
            .response
            .outcome,
        Outcome::ErrPermissionDenied
    );
    assert_eq!(
        f.run_session(&S1, &del(KeyRange::interval(b"a".to_vec(), b"c".to_vec())))
            .response
            .outcome,
        Outcome::ErrPermissionDenied,
        "delete of an interval needs the whole interval"
    );
    assert_eq!(
        f.run_session(&S1, &del(KeyRange::exact(b"b".to_vec())))
            .response
            .outcome,
        Outcome::Delete {
            deleted: 1,
            prev: vec![]
        }
    );
    // Transactions: comparisons need Read; only the selected branch needs
    // its permissions. Comparing "b" (readable) and writing "a" on success
    // versus writing "x" (not writable) on failure.
    let txn = |cmp_key: &[u8], expected: u64| {
        req(CanonicalOperation::Txn(TxnOp {
            compares: vec![Compare {
                key: cmp_key.to_vec(),
                target: CompareTarget::Version,
                result: CompareResult::Equal,
                operand: CompareOperand::Counter(expected),
            }],
            success: vec![BranchOp::Put(PutOp {
                key: b"a".to_vec(),
                value: b"s".to_vec(),
                lease: None,
                prev_kv: false,
            })],
            failure: vec![BranchOp::Put(PutOp {
                key: b"x".to_vec(),
                value: b"f".to_vec(),
                lease: None,
                prev_kv: false,
            })],
        }))
    };
    // "b" was deleted: version 0 -> success branch selected -> allowed.
    assert!(matches!(
        f.run_session(&S1, &txn(b"b", 0)).response.outcome,
        Outcome::Txn {
            succeeded: true,
            ..
        }
    ));
    // Comparison fails -> failure branch writes "x" -> denied; nothing changed.
    let p = f.run_session(&S1, &txn(b"b", 5));
    assert_eq!(p.response.outcome, Outcome::ErrPermissionDenied);
    assert!(!f.current.contains_key(b"x".as_slice()));
    // A comparison on an unreadable key is denied even when the selected
    // branch would be permitted.
    assert_eq!(
        f.run_session(&S1, &txn(b"z", 0)).response.outcome,
        Outcome::ErrPermissionDenied
    );
    // Lease actions are explicit: writing "a" with a lease needs LeaseAttach
    // besides Write; granting needs LeaseGrant.
    assert_eq!(
        f.run_session(&S1, &grant(L1, 10)).response.outcome,
        Outcome::ErrPermissionDenied
    );
    f.run_internal(&allow(4, ALICE, Action::LeaseGrant, KeyInterval::all()));
    assert!(matches!(
        f.run_session(&S1, &grant(L1, 10)).response.outcome,
        Outcome::LeaseGranted { .. }
    ));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"1", Some(L1)))
            .response
            .outcome,
        Outcome::ErrPermissionDenied,
        "attachment needs the lease permission on the key"
    );
    f.run_internal(&allow(5, ALICE, Action::LeaseAttach, interval(b"a", b"b")));
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"1", Some(L1)))
            .response
            .outcome,
        Outcome::Put { prev: None }
    );
    for (r, action) in [
        (keep_alive(L1), Action::LeaseRenew),
        (ttl(L1, true), Action::LeaseInspect),
        (revoke(L1), Action::LeaseRevoke),
    ] {
        assert_eq!(
            f.run_session(&S1, &r).response.outcome,
            Outcome::ErrPermissionDenied,
            "{action:?}"
        );
        f.run_internal(&allow(6 + action as u8, ALICE, action, KeyInterval::all()));
        assert!(
            !matches!(
                f.run_session(&S1, &r).response.outcome,
                Outcome::ErrPermissionDenied
            ),
            "{action:?}"
        );
    }
    // The ceiling narrows policy: Bob's session admits only Read even though
    // a rule allows him to write.
    f.run_internal(&admit(receipt(7, S2, BOB, Action::Read.bit())));
    f.run_internal(&allow(20, BOB, Action::Write, KeyInterval::all()));
    f.run_internal(&allow(21, BOB, Action::Read, KeyInterval::all()));
    assert_eq!(
        f.run_session(&S2, &put(b"q", b"1", None)).response.outcome,
        Outcome::ErrPermissionDenied
    );
    assert!(matches!(
        f.run_session(&S2, &get(KeyRange::exact(b"q".to_vec())))
            .response
            .outcome,
        Outcome::Range { .. }
    ));
    // Compaction is an explicit action too.
    assert_eq!(
        f.run_session(&S1, &req(CanonicalOperation::Compact { revision: rev(1) }))
            .response
            .outcome,
        Outcome::ErrPermissionDenied
    );
}

#[test]
fn policy_changes_after_admission_apply_at_execution_without_a_revision() {
    let mut f = with_alice();
    let p = f.run_internal(&allow(1, ALICE, Action::Write, KeyInterval::all()));
    assert_eq!(p.response.outcome, Outcome::PolicyUpdated);
    assert!(p.revision.is_none() && p.events.is_empty());
    assert_eq!(p.position.get(), 3);
    let request = put(b"a", b"1", None);
    // Admitted (planned) now...
    assert_eq!(
        f.run_session(&S1, &request).response.outcome,
        Outcome::Put { prev: None }
    );
    // ...the rule is removed in order; the same request presented at a later
    // position is denied, and nothing about the removal touched KV.
    let p = f.run_internal(&remove(1, ALICE));
    assert!(p.revision.is_none());
    let p = f.run_session(&S1, &request);
    assert_eq!(p.response.outcome, Outcome::ErrPermissionDenied);
    assert_eq!(f.revision, 1);
}

#[test]
fn codes_are_single_use_and_refresh_reuse_revokes_the_family() {
    let mut f = Fixture::new();
    f.run_internal(&trust(true, 1));
    let code = digest(0xc0);
    let family = digest(0xf0);
    assert_eq!(
        f.run_internal(&InternalCommand::CommitGrant {
            namespace: NS,
            commitment: code,
            kind: GrantKind::Code
        })
        .response
        .outcome,
        Outcome::GrantCommitted
    );
    assert_eq!(
        f.run_internal(&InternalCommand::CommitGrant {
            namespace: NS,
            commitment: code,
            kind: GrantKind::Code
        })
        .response
        .outcome,
        Outcome::ErrGrantExists
    );
    f.run_internal(&InternalCommand::CommitGrant {
        namespace: NS,
        commitment: family,
        kind: GrantKind::RefreshFamily,
    });
    // Consuming the code creates the session and binds the family atomically.
    let consume = |id: u8, session: SessionId| InternalCommand::ConsumeAdmission {
        namespace: NS,
        receipt: receipt(id, session, ALICE, Action::FULL_CEILING),
        code: Some(code),
        refresh_family: Some(family),
        window: 8,
    };
    assert_eq!(
        f.run_internal(&consume(1, S1)).response.outcome,
        Outcome::SessionCreated { session: S1 }
    );
    assert_eq!(f.grants[&code].state, GrantState::Consumed);
    assert_eq!(f.grants[&family].session, Some(S1));
    // The same code cannot create a second session.
    assert_eq!(
        f.run_internal(&consume(2, S2)).response.outcome,
        Outcome::ErrGrantUnavailable
    );
    assert!(!f.sessions.contains_key(&S2));
    // Rotation: the current secret advances the generation.
    let advance = |presented: Digest32, next: Digest32| InternalCommand::AdvanceRefresh {
        namespace: NS,
        family,
        presented,
        next,
    };
    assert_eq!(
        f.run_internal(&advance(family, digest(0xf1)))
            .response
            .outcome,
        Outcome::RefreshAdvanced { generation: 1 }
    );
    assert_eq!(
        f.run_internal(&advance(digest(0xf1), digest(0xf2)))
            .response
            .outcome,
        Outcome::RefreshAdvanced { generation: 2 }
    );
    assert!(f.sessions[&S1].active);
    // A retired secret presented again revokes the family and its session.
    assert_eq!(
        f.run_internal(&advance(digest(0xf1), digest(0xf3)))
            .response
            .outcome,
        Outcome::ErrRefreshReuse
    );
    assert_eq!(f.grants[&family].state, GrantState::Revoked);
    assert!(!f.sessions[&S1].active);
    assert_eq!(
        f.run_internal(&advance(digest(0xf2), digest(0xf4)))
            .response
            .outcome,
        Outcome::ErrGrantUnavailable
    );
    assert_eq!(
        f.run_session(&S1, &put(b"a", b"1", None)).response.outcome,
        Outcome::ErrSessionInvalid
    );
}

// ---------------------------------------------------------------------------
// Differential check against the independent oracle.

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(b"a".to_vec()),
        Just(b"b".to_vec()),
        Just(b"c".to_vec()),
        Just(b"d".to_vec())
    ]
}

fn range() -> impl Strategy<Value = KeyRange> {
    prop_oneof![
        key().prop_map(KeyRange::exact),
        Just(KeyRange::interval(b"a".to_vec(), b"c".to_vec())),
        Just(KeyRange::interval(b"b".to_vec(), b"e".to_vec())),
    ]
}

fn branch_op() -> impl Strategy<Value = BranchOp> {
    prop_oneof![
        (key(), any::<bool>()).prop_map(|(k, l)| BranchOp::Put(PutOp {
            key: k,
            value: b"v".to_vec(),
            lease: l.then_some(L1),
            prev_kv: false
        })),
        range().prop_map(|r| BranchOp::DeleteRange(DeleteRangeOp {
            range: r,
            prev_kv: false
        })),
        range().prop_map(|r| BranchOp::Range(RangeOp {
            range: r,
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false
        })),
    ]
}

fn operation() -> impl Strategy<Value = CanonicalOperation> {
    prop_oneof![
        range().prop_map(|r| CanonicalOperation::Range(RangeOp {
            range: r,
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false
        })),
        (key(), any::<bool>()).prop_map(|(k, l)| CanonicalOperation::Put(PutOp {
            key: k,
            value: b"v".to_vec(),
            lease: l.then_some(L1),
            prev_kv: false
        })),
        range().prop_map(|r| CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: r,
            prev_kv: false
        })),
        (
            prop::collection::vec((key(), 0u64..3), 0..2),
            prop::collection::vec(branch_op(), 0..3),
            prop::collection::vec(branch_op(), 0..3)
        )
            .prop_map(|(c, s, f)| CanonicalOperation::Txn(TxnOp {
                compares: c
                    .into_iter()
                    .map(|(k, v)| Compare {
                        key: k,
                        target: CompareTarget::Version,
                        result: CompareResult::Equal,
                        operand: CompareOperand::Counter(v),
                    })
                    .collect(),
                success: s,
                failure: f
            })),
        Just(CanonicalOperation::LeaseGrant {
            lease_id: L2,
            ttl_seconds: 5
        }),
        Just(CanonicalOperation::LeaseKeepAlive { lease_id: L1 }),
        Just(CanonicalOperation::LeaseRevoke { lease_id: L1 }),
        Just(CanonicalOperation::LeaseTimeToLive {
            lease_id: L1,
            keys: false
        }),
        Just(CanonicalOperation::Compact {
            revision: KvRevision::new(1).unwrap()
        }),
    ]
}

fn action() -> impl Strategy<Value = Action> {
    prop::sample::select(Action::ALL.to_vec())
}

fn oracle_action(a: Action) -> ActionKind {
    match a {
        Action::Read => ActionKind::Read,
        Action::Write => ActionKind::Write,
        Action::Delete => ActionKind::Delete,
        Action::LeaseGrant => ActionKind::LeaseGrant,
        Action::LeaseAttach => ActionKind::LeaseAttach,
        Action::LeaseInspect => ActionKind::LeaseInspect,
        Action::LeaseRenew => ActionKind::LeaseRenew,
        Action::LeaseRevoke => ActionKind::LeaseRevoke,
        Action::Compact => ActionKind::Compact,
    }
}

fn rule() -> impl Strategy<Value = (Action, Vec<u8>, Option<Vec<u8>>)> {
    (
        action(),
        prop_oneof![
            Just((b"".to_vec(), None)),
            Just((b"a".to_vec(), Some(b"c".to_vec()))),
            Just((b"b".to_vec(), Some(b"e".to_vec()))),
            Just((b"a".to_vec(), Some(b"a\0".to_vec()))),
        ],
    )
        .prop_map(|(a, (l, u))| (a, l, u))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]

    #[test]
    fn planner_decisions_match_the_oracle(
        rules in prop::collection::vec(rule(), 0..6),
        ceiling in 0u32..(1 << 9),
        active in any::<bool>(),
        enabled in any::<bool>(),
        generation in 1u64..3,
        op in operation(),
    ) {
        let mut f = Fixture::new();
        let mut model = KvModel::default();
        // Shared seed state (trusted): a lease and two keys, one attached.
        f.run(&grant(L1, 10));
        for (k, lease) in [(&b"a"[..], Some(L1)), (b"c", None)] {
            let op = CanonicalOperation::Put(PutOp {
                key: k.to_vec(),
                value: b"0".to_vec(),
                lease,
                prev_kv: false,
            });
            f.run(&req(op.clone()));
            model.apply(&op, None);
        }
        f.run_internal(&trust(enabled, generation));
        let mut r = receipt(1, S1, ALICE, ceiling);
        r.rule_generation = 1;
        // Admission needs the rule enabled at generation 1; seed the session
        // directly otherwise so every combination is exercised.
        if enabled && generation == 1 {
            f.run_internal(&admit(r));
        } else {
            f.sessions.insert(S1, coord_state::policy::SessionRecord {
                principal: ALICE,
                scope_ceiling: ceiling,
                trust_rule: RULE,
                rule_generation: 1,
                active: true,
                window: 8,
                receipt_id: digest(1),
            });
        }
        if !active {
            f.sessions.get_mut(&S1).unwrap().active = false;
        }
        let mut oracle_rules = Vec::new();
        for (i, (a, l, u)) in rules.iter().enumerate() {
            f.run_internal(&allow(i as u8 + 1, ALICE, *a, KeyInterval { lower: l.clone(), upper: u.clone() }));
            oracle_rules.push(Rule {
                principal: ALICE.0,
                action: oracle_action(*a),
                namespace: NS.0,
                lower: l.clone(),
                upper: u.clone(),
            });
        }
        let oracle = PolicyOracle::new(oracle_rules);
        let facts = SessionFacts {
            principal: ALICE.0,
            ceiling,
            active,
            rule_enabled: enabled,
            rule_generation_matches: generation == 1,
        };
        let expected = oracle.decide(&model, &facts, NS.0, &op);
        let request = req(op);
        // The generator may build a transaction the schema rejects (a key
        // written twice in one branch); authorization is not defined for it.
        prop_assume!(request.validate().is_ok());
        let v = f.view_for_session(&S1);
        let planned = coord_state::plan(&request, &v, &f.limits).unwrap();
        let got = match planned.response.outcome {
            Outcome::ErrSessionInvalid => Decision::SessionInvalid,
            Outcome::ErrPermissionDenied => Decision::Denied,
            _ => Decision::Allowed,
        };
        prop_assert_eq!(got, expected, "{:?}", planned.response.outcome);
        if got != Decision::Allowed {
            prop_assert!(planned.mutations.is_empty() && planned.revision.is_none());
        }
        let _ = BTreeMap::<u8, u8>::new();
    }
}
