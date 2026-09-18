//! Acceptance: create/mod/version metadata, absence, byte intervals,
//! failed/read-only/no-op revisions, one revision for a multi-key mutation,
//! and limits failing before any change.

use std::collections::BTreeMap;

use coord_core::effect::ApplyBase;
use coord_state::planner::apply_to_map;
use coord_state::{
    ApplyPlan, HistoricalView, KvEntry, KvEventKind, Mutation, Outcome, PlanError, PlanLimits,
    ReadView, plan,
};
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([7; 16]);

fn base(pos: u64) -> ApplyBase {
    ApplyBase {
        configuration: ConfigurationEpoch::ZERO,
        execution_position: ExecutionPosition::new(pos).unwrap(),
    }
}

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn view(current: &BTreeMap<Vec<u8>, KvEntry>, revision: u64, pos: u64) -> ReadView {
    let mut v = ReadView::empty(base(pos), NS, rev(revision));
    v.current = current.clone();
    v
}

fn req(op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(NS, op);
    r.canonicalize();
    r
}

fn put(key: &[u8], value: &[u8]) -> LogicalRequest {
    req(CanonicalOperation::Put(PutOp {
        key: key.to_vec(),
        value: value.to_vec(),
        lease: None,
        prev_kv: true,
    }))
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

/// Plan, apply to the fixture map and return the plan; tracks revision and position.
struct Fixture {
    current: BTreeMap<Vec<u8>, KvEntry>,
    revision: u64,
    position: u64,
    limits: PlanLimits,
}

impl Fixture {
    fn new() -> Self {
        Fixture {
            current: BTreeMap::new(),
            revision: 0,
            position: 0,
            limits: PlanLimits::default(),
        }
    }

    fn run(&mut self, request: &LogicalRequest) -> Result<ApplyPlan, PlanError> {
        let v = view(&self.current, self.revision, self.position);
        let p = plan(request, &v, &self.limits)?;
        assert_eq!(p.base, v.base);
        assert_eq!(
            p.position.get(),
            self.position + 1,
            "every command advances execution by one"
        );
        apply_to_map(&mut self.current, &p);
        self.position += 1;
        if let Some(r) = p.revision {
            assert_eq!(
                r.get(),
                self.revision + 1,
                "a mutation takes exactly the next revision"
            );
            self.revision = r.get();
            assert!(!p.events.is_empty());
        } else {
            assert!(
                p.events.is_empty()
                    && p.mutations
                        .iter()
                        .all(|m| matches!(m, Mutation::CompactTo { .. }))
            );
        }
        assert_eq!(
            p.response.revision.get(),
            self.revision,
            "header revision is the post-command revision"
        );
        Ok(p)
    }
}

#[test]
fn create_mod_version_and_absence_metadata() {
    let mut f = Fixture::new();
    let p = f.run(&put(b"k", b"v1")).unwrap();
    assert_eq!(p.revision, Some(rev(1)));
    assert_eq!(p.response.outcome, Outcome::Put { prev: None });
    let e = &f.current[b"k".as_slice()];
    assert_eq!(
        (e.create_revision.get(), e.mod_revision.get(), e.version),
        (1, 1, 1)
    );

    let p = f.run(&put(b"k", b"v2")).unwrap();
    if let Outcome::Put { prev: Some(prev) } = &p.response.outcome {
        assert_eq!(prev.value, b"v1");
    } else {
        panic!("prev_kv requested");
    }
    let e = &f.current[b"k".as_slice()];
    assert_eq!(
        (e.create_revision.get(), e.mod_revision.get(), e.version),
        (1, 2, 2)
    );
    // Same-value put is still a mutation.
    let p = f.run(&put(b"k", b"v2")).unwrap();
    assert_eq!(p.revision, Some(rev(3)));
    assert_eq!(f.current[b"k".as_slice()].version, 3);

    // Absence: read of a missing key is empty and read-only.
    let p = f.run(&get(KeyRange::exact(b"missing".to_vec()))).unwrap();
    assert_eq!(p.revision, None);
    assert_eq!(
        p.response.outcome,
        Outcome::Range {
            items: vec![],
            count: 0,
            more: false
        }
    );
    assert_eq!(p.response.revision, rev(3));
}

#[test]
fn byte_intervals_pagination_and_flags() {
    let mut f = Fixture::new();
    for k in [b"a".as_slice(), b"a\0", b"ab", b"b", b"\xff"] {
        f.run(&put(k, b"x")).unwrap();
    }
    let p = f
        .run(&get(KeyRange::interval(b"a".to_vec(), b"b".to_vec())))
        .unwrap();
    if let Outcome::Range { items, count, more } = &p.response.outcome {
        let keys: Vec<&[u8]> = items.iter().map(|i| i.key.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"a\0", b"ab"]);
        assert_eq!(*count, 3);
        assert!(!more);
    } else {
        panic!();
    }
    let limited = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(b"a".to_vec(), b"c".to_vec()),
        revision: None,
        limit: 2,
        keys_only: true,
        count_only: false,
    }));
    let p = f.run(&limited).unwrap();
    if let Outcome::Range { items, count, more } = &p.response.outcome {
        assert_eq!(items.len(), 2);
        assert!(
            items.iter().all(|i| i.entry.value.is_empty()),
            "keys-only strips values"
        );
        assert_eq!(*count, 4);
        assert!(more);
    } else {
        panic!();
    }
    let counted = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(b"a".to_vec(), b"c".to_vec()),
        revision: None,
        limit: 1,
        keys_only: false,
        count_only: true,
    }));
    let p = f.run(&counted).unwrap();
    assert_eq!(
        p.response.outcome,
        Outcome::Range {
            items: vec![],
            count: 4,
            more: false
        }
    );
}

#[test]
fn delete_of_nothing_is_a_no_op_and_multi_key_delete_shares_one_revision() {
    let mut f = Fixture::new();
    f.run(&put(b"a", b"1")).unwrap();
    f.run(&put(b"b", b"2")).unwrap();
    let none = req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::exact(b"zzz".to_vec()),
        prev_kv: true,
    }));
    let p = f.run(&none).unwrap();
    assert_eq!(p.revision, None, "deleting nothing is not a mutation");
    assert_eq!(
        p.response.outcome,
        Outcome::Delete {
            deleted: 0,
            prev: vec![]
        }
    );
    let both = req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::interval(b"a".to_vec(), b"c".to_vec()),
        prev_kv: true,
    }));
    let p = f.run(&both).unwrap();
    assert_eq!(p.revision, Some(rev(3)));
    assert_eq!(p.events.len(), 2);
    assert!(
        p.events
            .iter()
            .all(|e| e.kind == KvEventKind::Delete && e.prev.is_some())
    );
    if let Outcome::Delete { deleted, prev } = &p.response.outcome {
        assert_eq!(*deleted, 2);
        assert_eq!(prev.len(), 2);
    } else {
        panic!();
    }
    assert!(f.current.is_empty());
}

#[test]
fn transactions_take_one_revision_or_none() {
    let mut f = Fixture::new();
    f.run(&put(b"k", b"v1")).unwrap();
    let cas = |expect: u64, value: &[u8]| {
        req(CanonicalOperation::Txn(TxnOp {
            compares: vec![Compare {
                key: b"k".to_vec(),
                target: CompareTarget::ModRevision,
                result: CompareResult::Equal,
                operand: CompareOperand::Counter(expect),
            }],
            success: vec![
                BranchOp::Put(PutOp {
                    key: b"k".to_vec(),
                    value: value.to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
                BranchOp::Put(PutOp {
                    key: b"k2".to_vec(),
                    value: value.to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
            ],
            failure: vec![BranchOp::Range(RangeOp {
                range: KeyRange::exact(b"k".to_vec()),
                revision: None,
                limit: 0,
                keys_only: false,
                count_only: false,
            })],
        }))
    };
    // Success branch: two keys, one revision, two events with equal revision.
    let p = f.run(&cas(1, b"v2")).unwrap();
    assert_eq!(p.revision, Some(rev(2)));
    assert_eq!(p.events.len(), 2);
    assert!(
        p.events
            .iter()
            .all(|e| e.entry.as_ref().unwrap().mod_revision == rev(2))
    );
    assert_eq!(f.current[b"k2".as_slice()].create_revision, rev(2));
    if let Outcome::Txn { succeeded, results } = &p.response.outcome {
        assert!(*succeeded);
        assert_eq!(results.len(), 2);
    } else {
        panic!();
    }
    // Failed comparison: failure branch is read-only, no revision consumed.
    let p = f.run(&cas(1, b"v3")).unwrap();
    assert_eq!(p.revision, None);
    if let Outcome::Txn { succeeded, results } = &p.response.outcome {
        assert!(!succeeded);
        if let Outcome::Range { items, .. } = &results[0] {
            assert_eq!(items[0].entry.value, b"v2");
        } else {
            panic!();
        }
    } else {
        panic!();
    }
    assert_eq!(f.revision, 2);
    // Comparisons observe pre-transaction state; branch reads observe branch writes.
    let rw = req(CanonicalOperation::Txn(TxnOp {
        compares: vec![],
        success: vec![
            BranchOp::Put(PutOp {
                key: b"j".to_vec(),
                value: b"new".to_vec(),
                lease: None,
                prev_kv: false,
            }),
            BranchOp::Range(RangeOp {
                range: KeyRange::exact(b"j".to_vec()),
                revision: None,
                limit: 0,
                keys_only: false,
                count_only: false,
            }),
        ],
        failure: vec![],
    }));
    let p = f.run(&rw).unwrap();
    if let Outcome::Txn { results, .. } = &p.response.outcome {
        if let Outcome::Range { items, .. } = &results[1] {
            assert_eq!(items[0].entry.value, b"new");
            assert_eq!(items[0].entry.mod_revision, rev(3));
        } else {
            panic!();
        }
    }
}

#[test]
fn historical_reads_use_the_supplied_snapshot() {
    let mut f = Fixture::new();
    f.run(&put(b"k", b"v1")).unwrap();
    f.run(&put(b"k", b"v2")).unwrap();
    let at1 = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(b"k".to_vec()),
        revision: Some(rev(1)),
        limit: 0,
        keys_only: false,
        count_only: false,
    }));
    // Without the historical view the planner asks for a rebuild.
    let v = view(&f.current, f.revision, f.position);
    assert_eq!(
        plan(&at1, &v, &PlanLimits::default()).unwrap_err(),
        PlanError::ViewIncomplete
    );
    let mut v = v;
    let mut entries = BTreeMap::new();
    entries.insert(
        b"k".to_vec(),
        KvEntry {
            value: b"v1".to_vec(),
            create_revision: rev(1),
            mod_revision: rev(1),
            version: 1,
            lease: None,
            lease_generation: None,
        },
    );
    v.historical = Some(HistoricalView {
        revision: rev(1),
        entries,
    });
    let p = plan(&at1, &v, &PlanLimits::default()).unwrap();
    assert_eq!(p.revision, None);
    if let Outcome::Range { items, .. } = &p.response.outcome {
        assert_eq!(items[0].entry.value, b"v1");
    } else {
        panic!();
    }
    // Future and compacted revisions are defined outcomes, not view errors.
    let future = req(CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(b"k".to_vec()),
        revision: Some(rev(9)),
        limit: 0,
        keys_only: false,
        count_only: false,
    }));
    assert_eq!(
        plan(&future, &v, &PlanLimits::default())
            .unwrap()
            .response
            .outcome,
        Outcome::ErrFutureRevision
    );
    v.compact_floor = rev(2);
    assert_eq!(
        plan(&at1, &v, &PlanLimits::default())
            .unwrap()
            .response
            .outcome,
        Outcome::ErrCompacted
    );
    // Compaction itself is a non-KV mutation with no revision.
    let p = f
        .run(&req(CanonicalOperation::Compact { revision: rev(1) }))
        .unwrap();
    assert_eq!(p.revision, None);
    assert_eq!(p.mutations, vec![Mutation::CompactTo { revision: rev(1) }]);
}

#[test]
fn limits_fail_before_any_change() {
    let mut f = Fixture::new();
    for i in 0..5u8 {
        f.run(&put(&[b'k', i], &[0u8; 100])).unwrap();
    }
    let snapshot = f.current.clone();
    f.limits.max_delete_keys = 3;
    let del = req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::interval(b"k".to_vec(), b"l".to_vec()),
        prev_kv: false,
    }));
    assert_eq!(f.run(&del).unwrap_err(), PlanError::TooManyDeletes);
    assert_eq!(f.current, snapshot, "nothing changed");
    f.limits.max_delete_keys = 4096;
    f.limits.max_events_per_revision = 2;
    assert_eq!(f.run(&del).unwrap_err(), PlanError::TooManyEvents);
    f.limits.max_events_per_revision = 4096;
    f.limits.max_response_bytes = 150;
    let read = get(KeyRange::interval(b"k".to_vec(), b"l".to_vec()));
    assert_eq!(f.run(&read).unwrap_err(), PlanError::ResponseTooLarge);
    assert_eq!(f.current, snapshot);
    // Schema violations are rejected through validation.
    let empty = req(CanonicalOperation::Put(PutOp {
        key: vec![],
        value: vec![],
        lease: None,
        prev_kv: false,
    }));
    assert!(matches!(f.run(&empty).unwrap_err(), PlanError::Invalid(_)));
    // Wrong namespace.
    let mut other = put(b"k", b"v");
    other.namespace = NamespaceId([8; 16]);
    assert_eq!(f.run(&other).unwrap_err(), PlanError::NamespaceMismatch);
    // Unknown lease.
    let leased = req(CanonicalOperation::Put(PutOp {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        lease: Some(LeaseId([1; 16])),
        prev_kv: false,
    }));
    assert_eq!(f.run(&leased).unwrap_err(), PlanError::LeaseNotFound);
    // Revision overflow stops.
    let mut v = view(&f.current, KvRevision::MAX.get(), 1);
    v.kv_revision = KvRevision::MAX;
    assert_eq!(
        plan(&put(b"k", b"v"), &v, &PlanLimits::default()).unwrap_err(),
        PlanError::CounterOverflow
    );
}

#[test]
fn lease_attachment_produces_index_mutations() {
    let mut f = Fixture::new();
    let lease = LeaseId([3; 16]);
    let mut v = view(&f.current, 0, 0);
    v.leases.insert(lease);
    let leased = req(CanonicalOperation::Put(PutOp {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        lease: Some(lease),
        prev_kv: false,
    }));
    let p = plan(&leased, &v, &PlanLimits::default()).unwrap();
    assert!(p.mutations.contains(&Mutation::LeaseAttach {
        lease,
        key: b"k".to_vec()
    }));
    apply_to_map(&mut f.current, &p);
    assert_eq!(f.current[b"k".as_slice()].lease, Some(lease));
    // Deleting a leased key detaches it.
    let mut v = view(&f.current, 1, 1);
    v.leases.insert(lease);
    let del = req(CanonicalOperation::DeleteRange(DeleteRangeOp {
        range: KeyRange::exact(b"k".to_vec()),
        prev_kv: false,
    }));
    let p = plan(&del, &v, &PlanLimits::default()).unwrap();
    assert!(p.mutations.contains(&Mutation::LeaseDetach {
        lease,
        key: b"k".to_vec()
    }));
}
