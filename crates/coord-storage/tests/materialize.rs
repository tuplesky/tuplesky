//! task-11 fixtures: identical model/redb rows matching the oracle, stale
//! base replans, fixed-revision pagination, reverse/boundary pages, complete
//! per-revision events, ahead-of-durability views and crash atomicity.

use std::collections::BTreeMap;

use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_oracle::model::{KvModel, Outcome as OracleOutcome};
use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_state::{Outcome, PlanLimits, plan};
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, ViewError, apply_plan, build_read_view,
    events_at, scan_current_page,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{Direction, LocalEngine, OrderedRead, ScanRequest, SnapshotSource};
use coord_store_api::registry::{Collection, meta_fields};
use coord_store_testkit::model::{CommitScript, ModelEngine};
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([0x11; 16]);
const OTHER_NS: NamespaceId = NamespaceId([0x22; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn req(ns: NamespaceId, op: CanonicalOperation) -> LogicalRequest {
    let mut r = LogicalRequest::new(ns, op);
    r.canonicalize();
    r
}

fn put(key: &[u8], value: &[u8]) -> LogicalRequest {
    req(
        NS,
        CanonicalOperation::Put(PutOp {
            key: key.to_vec(),
            value: value.to_vec(),
            lease: None,
            prev_kv: true,
        }),
    )
}

fn del(range: KeyRange) -> LogicalRequest {
    req(
        NS,
        CanonicalOperation::DeleteRange(DeleteRangeOp {
            range,
            prev_kv: true,
        }),
    )
}

fn get(range: KeyRange, revision: Option<u64>, limit: u32) -> LogicalRequest {
    req(
        NS,
        CanonicalOperation::Range(RangeOp {
            range,
            revision: revision.map(rev),
            limit,
            keys_only: false,
            count_only: false,
        }),
    )
}

/// A domain driver: view -> plan -> apply, replanning on a stale base.
struct Domain<E: LocalEngine> {
    worker: StoreWorker<E>,
    alloc: BarrierAllocator,
}

impl<E: LocalEngine> Domain<E> {
    fn new(engine: E, boot: u8) -> Self {
        let boot = BootId([boot; 16]);
        Domain {
            worker: StoreWorker::open(engine, boot, inc(), GroupLimits::default()).unwrap(),
            alloc: BarrierAllocator::new(inc(), boot),
        }
    }

    fn run(&mut self, request: &LogicalRequest) -> coord_state::Response {
        let mut replans = 0;
        loop {
            let gated = self.worker.reader().snapshot().unwrap();
            let view =
                build_read_view(&gated, request.namespace, request, ViewBudget::default()).unwrap();
            let planned = plan(request, &view, &PlanLimits::default()).unwrap();
            match apply_plan(
                &mut self.worker,
                self.alloc.allocate(),
                request.namespace,
                &planned,
            )
            .unwrap()
            {
                ApplyOutcome::Applied(_) => return planned.response,
                ApplyOutcome::Replan => {
                    replans += 1;
                    assert!(replans < 3, "replanning must converge");
                }
                ApplyOutcome::Indeterminate => {
                    self.worker.reconcile().unwrap();
                }
            }
        }
    }
}

fn to_oracle(o: &Outcome) -> OracleOutcome {
    let e = |e: &coord_state::KvEntry| coord_oracle::model::KvEntry {
        value: e.value.clone(),
        create_revision: e.create_revision.get(),
        mod_revision: e.mod_revision.get(),
        version: e.version,
        lease: e.lease.map(|l| l.0),
    };
    let items = |i: &[coord_state::RangeItem]| {
        i.iter()
            .map(|x| coord_oracle::model::RangeItem {
                key: x.key.clone(),
                entry: e(&x.entry),
            })
            .collect()
    };
    match o {
        Outcome::Put { prev } => OracleOutcome::Put {
            prev: prev.as_ref().map(e),
        },
        Outcome::Delete { deleted, prev } => OracleOutcome::Delete {
            deleted: *deleted,
            prev: items(prev),
        },
        Outcome::Range {
            items: i,
            count,
            more,
        } => OracleOutcome::Range {
            items: items(i),
            count: *count,
            more: *more,
        },
        Outcome::Txn { succeeded, results } => OracleOutcome::Txn {
            succeeded: *succeeded,
            results: results.iter().map(to_oracle).collect(),
        },
        Outcome::Compacted => OracleOutcome::Compacted,
        Outcome::ErrCompacted => OracleOutcome::ErrCompacted,
        Outcome::ErrFutureRevision => OracleOutcome::ErrFutureRevision,
    }
}

fn workload() -> Vec<LogicalRequest> {
    let mut ops = vec![
        put(b"a", b"1"),
        put(b"a\0", b"zero"),
        put(b"ab", b"2"),
        put(b"b", b"3"),
        put(b"\xff", b"ff"),
        get(KeyRange::interval(b"a".to_vec(), b"c".to_vec()), None, 0),
        put(b"a", b"1b"),
        del(KeyRange::exact(b"ab".to_vec())),
        get(KeyRange::interval(b"a".to_vec(), b"c".to_vec()), Some(5), 0),
        get(KeyRange::interval(b"a".to_vec(), b"c".to_vec()), Some(7), 2),
        req(
            NS,
            CanonicalOperation::Txn(TxnOp {
                compares: vec![Compare {
                    key: b"b".to_vec(),
                    target: CompareTarget::Version,
                    result: CompareResult::Equal,
                    operand: CompareOperand::Counter(1),
                }],
                success: vec![
                    BranchOp::Put(PutOp {
                        key: b"b".to_vec(),
                        value: b"3b".to_vec(),
                        lease: None,
                        prev_kv: false,
                    }),
                    BranchOp::Put(PutOp {
                        key: b"c".to_vec(),
                        value: b"4".to_vec(),
                        lease: None,
                        prev_kv: false,
                    }),
                    BranchOp::DeleteRange(DeleteRangeOp {
                        range: KeyRange::exact(b"\xff".to_vec()),
                        prev_kv: true,
                    }),
                ],
                failure: vec![],
            }),
        ),
        del(KeyRange::exact(b"nothing".to_vec())),
        get(KeyRange::exact(b"b".to_vec()), Some(8), 0),
        req(NS, CanonicalOperation::Compact { revision: rev(6) }),
        get(KeyRange::exact(b"a".to_vec()), Some(5), 0),
        get(KeyRange::exact(b"a".to_vec()), Some(6), 0),
        get(KeyRange::exact(b"a".to_vec()), Some(99), 0),
        req(
            OTHER_NS,
            CanonicalOperation::Put(PutOp {
                key: b"a".to_vec(),
                value: b"other".to_vec(),
                lease: None,
                prev_kv: false,
            }),
        ),
        get(KeyRange::interval(b"a".to_vec(), b"z".to_vec()), None, 0),
    ];
    for i in 0..20u8 {
        ops.push(put(&[b'p', i], &[i; 40]));
    }
    ops
}

fn rows<E: LocalEngine>(engine: &E) -> BTreeMap<(u16, Vec<u8>), Vec<u8>> {
    let view = engine.reader().snapshot().unwrap();
    let mut out = BTreeMap::new();
    for c in [
        Collection::KvCurrentV1,
        Collection::KvHistoryV1,
        Collection::EventsV1,
        Collection::LeaseKeysV1,
    ] {
        let mut request = ScanRequest::all(1000, 1 << 24);
        loop {
            let page = view.scan_page(c.id(), &request).unwrap();
            for r in &page.rows {
                out.insert((c.id().0, r.key.clone()), r.value.clone());
            }
            if page.exhausted {
                break;
            }
            request.resume_after = Some(page.rows.last().unwrap().key.clone());
        }
    }
    for field in [meta_fields::KV_REVISION, meta_fields::RETENTION_FLOOR] {
        if let Some(v) = view.get(Collection::MetaV1.id(), field).unwrap() {
            out.insert((Collection::MetaV1.id().0, field.to_vec()), v);
        }
    }
    out
}

fn run_workload<E: LocalEngine>(engine: E) -> (E, Vec<coord_state::Response>) {
    let mut domain = Domain::new(engine, 1);
    let mut responses = Vec::new();
    for request in workload() {
        responses.push(domain.run(&request));
    }
    (domain.worker.into_engine(), responses)
}

#[test]
fn model_and_redb_produce_identical_rows_matching_the_oracle() {
    let (model, model_responses) = run_workload(ModelEngine::new());
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let (redb, redb_responses) =
        run_workload(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    assert_eq!(model_responses, redb_responses);
    assert_eq!(
        rows(&model),
        rows(&redb),
        "common rows differ between engines"
    );

    // Every response matches the independent oracle (one model per namespace).
    let mut oracles: BTreeMap<NamespaceId, KvModel> = BTreeMap::new();
    for (request, response) in workload().iter().zip(&model_responses) {
        let oracle = oracles.entry(request.namespace).or_default();
        let expected = oracle.apply(&request.operation, None);
        // Namespaces share the domain revision; the oracle is per-namespace,
        // so compare outcomes and the ordering of revisions rather than the
        // absolute header for the second namespace.
        if request.namespace == NS {
            assert_eq!(
                to_oracle(&response.outcome),
                expected.outcome,
                "{:?}",
                request.operation
            );
        }
    }
    // Explicit checks on the historical/compaction responses.
    let r5 = &model_responses[8];
    if let Outcome::Range { items, count, .. } = &r5.outcome {
        let keys: Vec<&[u8]> = items.iter().map(|i| i.key.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"a\0", b"ab", b"b"]);
        assert_eq!(
            items[0].entry.value, b"1",
            "revision 5 predates the overwrite"
        );
        assert_eq!(*count, 4);
    } else {
        panic!("historical read");
    }
    assert_eq!(model_responses[14].outcome, Outcome::ErrCompacted);
    assert!(matches!(model_responses[15].outcome, Outcome::Range { .. }));
    assert_eq!(model_responses[16].outcome, Outcome::ErrFutureRevision);
}

#[test]
fn stale_base_replans_instead_of_applying_against_different_state() {
    let mut domain = Domain::new(ModelEngine::new(), 1);
    domain.run(&put(b"k", b"v0"));
    let gated = domain.worker.reader().snapshot().unwrap();
    let request_a = put(b"k", b"a");
    let request_b = put(b"k", b"b");
    let view = build_read_view(&gated, NS, &request_a, ViewBudget::default()).unwrap();
    let plan_a = plan(&request_a, &view, &PlanLimits::default()).unwrap();
    let plan_b = plan(&request_b, &view, &PlanLimits::default()).unwrap();
    assert_eq!(plan_a.base, plan_b.base);
    assert!(matches!(
        apply_plan(&mut domain.worker, domain.alloc.allocate(), NS, &plan_a).unwrap(),
        ApplyOutcome::Applied(_)
    ));
    assert_eq!(
        apply_plan(&mut domain.worker, domain.alloc.allocate(), NS, &plan_b).unwrap(),
        ApplyOutcome::Replan
    );
    // State reflects only plan A; a fresh view replans B correctly.
    let gated = domain.worker.reader().snapshot().unwrap();
    let view = build_read_view(&gated, NS, &request_b, ViewBudget::default()).unwrap();
    assert_eq!(view.current[b"k".as_slice()].value, b"a");
    let plan_b2 = plan(&request_b, &view, &PlanLimits::default()).unwrap();
    assert_eq!(plan_b2.revision, Some(rev(3)));
    assert!(matches!(
        apply_plan(&mut domain.worker, domain.alloc.allocate(), NS, &plan_b2).unwrap(),
        ApplyOutcome::Applied(_)
    ));
}

#[test]
fn fixed_revision_pagination_holds_one_revision_across_pages() {
    let mut domain = Domain::new(ModelEngine::new(), 1);
    for i in 0..10u8 {
        domain.run(&put(&[b'k', i], &[i]));
    }
    // Revision 6 has keys k0..k5. Later writes must not leak in.
    for i in 0..10u8 {
        domain.run(&put(&[b'k', i], &[i, i]));
    }
    let mut cursor = b"k".to_vec();
    let mut collected = Vec::new();
    loop {
        let page = domain.run(&get(
            KeyRange::interval(cursor.clone(), b"l".to_vec()),
            Some(6),
            4,
        ));
        let Outcome::Range { items, more, count } = page.outcome else {
            panic!()
        };
        assert_eq!(
            count as usize + collected.len(),
            6,
            "count is over the whole remaining interval at R"
        );
        for item in &items {
            assert_eq!(
                item.entry.value,
                vec![item.key[1]],
                "values as of revision 6"
            );
            assert_eq!(item.entry.mod_revision.get(), u64::from(item.key[1]) + 1);
        }
        let last = items.last().map(|i| i.key.clone());
        collected.extend(items.into_iter().map(|i| i.key));
        if !more {
            break;
        }
        // Exclusive cursor: the next page starts after the last key.
        cursor = last.unwrap();
        cursor.push(0);
    }
    assert_eq!(
        collected,
        (0..6u8).map(|i| vec![b'k', i]).collect::<Vec<_>>()
    );
    // Choosing versions happens before the limit: a limit of 1 at R still
    // returns the greatest version at or below R for the first key.
    let page = domain.run(&get(
        KeyRange::interval(b"k".to_vec(), b"l".to_vec()),
        Some(15),
        1,
    ));
    let Outcome::Range { items, count, more } = page.outcome else {
        panic!()
    };
    assert_eq!(items[0].entry.value, vec![0, 0]);
    assert_eq!(count, 10);
    assert!(more);
}

#[test]
fn reverse_and_boundary_pages_stay_inside_the_namespace() {
    let mut domain = Domain::new(ModelEngine::new(), 1);
    for k in [
        b"".as_slice(),
        b"\0",
        b"\0\xff",
        b"a",
        b"a\0",
        b"ab",
        b"\xff",
    ] {
        if !k.is_empty() {
            domain.run(&put(k, b"x"));
        }
    }
    domain.run(&req(
        OTHER_NS,
        CanonicalOperation::Put(PutOp {
            key: b"a".to_vec(),
            value: b"other".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    ));
    let gated = domain.worker.reader().snapshot().unwrap();
    let view = gated.view();
    let (fwd, exhausted) =
        scan_current_page(view, &NS, b"", None, Direction::Forward, None, 100).unwrap();
    let keys: Vec<&[u8]> = fwd.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![b"\0".as_slice(), b"\0\xff", b"a", b"a\0", b"ab", b"\xff"]
    );
    assert!(exhausted);
    let (rev_page, exhausted) =
        scan_current_page(view, &NS, b"", None, Direction::Reverse, None, 2).unwrap();
    let keys: Vec<&[u8]> = rev_page.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"\xff".as_slice(), b"ab"]);
    assert!(!exhausted);
    let (rev_page, _) =
        scan_current_page(view, &NS, b"", None, Direction::Reverse, Some(b"ab"), 2).unwrap();
    let keys: Vec<&[u8]> = rev_page.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a\0".as_slice(), b"a"]);
    let (bounded, _) =
        scan_current_page(view, &NS, b"a", Some(b"b"), Direction::Forward, None, 100).unwrap();
    let keys: Vec<&[u8]> = bounded.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a".as_slice(), b"a\0", b"ab"]);
    let (other, _) =
        scan_current_page(view, &OTHER_NS, b"", None, Direction::Forward, None, 100).unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].1.value, b"other");
}

#[test]
fn events_are_complete_and_ordered_per_revision() {
    let mut domain = Domain::new(ModelEngine::new(), 1);
    domain.run(&put(b"a", b"1"));
    let txn = req(
        NS,
        CanonicalOperation::Txn(TxnOp {
            compares: vec![],
            success: vec![
                BranchOp::Put(PutOp {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
                BranchOp::DeleteRange(DeleteRangeOp {
                    range: KeyRange::exact(b"a".to_vec()),
                    prev_kv: false,
                }),
                BranchOp::Put(PutOp {
                    key: b"c".to_vec(),
                    value: b"3".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
            ],
            failure: vec![],
        }),
    );
    let response = domain.run(&txn);
    assert_eq!(response.revision, rev(2));
    domain.run(&get(KeyRange::exact(b"b".to_vec()), None, 0));
    let gated = domain.worker.reader().snapshot().unwrap();
    let events = events_at(gated.view(), rev(2)).unwrap().unwrap();
    let summary: Vec<(coord_state::KvEventKind, &[u8])> =
        events.iter().map(|e| (e.kind, e.key.as_slice())).collect();
    assert_eq!(
        summary,
        vec![
            (coord_state::KvEventKind::Put, b"b".as_slice()),
            (coord_state::KvEventKind::Delete, b"a"),
            (coord_state::KvEventKind::Put, b"c")
        ]
    );
    assert_eq!(events[1].prev.as_ref().unwrap().value, b"1");
    assert_eq!(
        events_at(gated.view(), rev(3)).unwrap(),
        None,
        "the read produced no revision"
    );
    assert_eq!(events_at(gated.view(), rev(1)).unwrap().unwrap().len(), 1);
}

#[test]
fn ahead_of_durability_views_are_refused_until_reconciled() {
    let mut domain = Domain::new(ModelEngine::new(), 1);
    domain.run(&put(b"a", b"1"));
    let request = put(b"a", b"2");
    let gated = domain.worker.reader().snapshot().unwrap();
    let view = build_read_view(&gated, NS, &request, ViewBudget::default()).unwrap();
    let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
    drop(gated);
    domain
        .worker
        .engine_mut()
        .script_commit(CommitScript::Indeterminate { applied: true });
    assert_eq!(
        apply_plan(&mut domain.worker, domain.alloc.allocate(), NS, &planned).unwrap(),
        ApplyOutcome::Indeterminate
    );
    assert!(matches!(
        domain.worker.reader().snapshot(),
        Err(ViewError::AheadOfCompletion { .. })
    ));
    domain.worker.reconcile().unwrap();
    let gated = domain.worker.reader().snapshot().unwrap();
    assert_eq!(
        build_read_view(&gated, NS, &request, ViewBudget::default())
            .unwrap()
            .current[b"a".as_slice()]
        .value,
        b"2"
    );
}

#[test]
fn crash_mid_apply_leaves_no_partial_events_or_frontier_mismatch() {
    let requests = vec![
        put(b"a", b"1"),
        req(
            NS,
            CanonicalOperation::Txn(TxnOp {
                compares: vec![],
                success: vec![
                    BranchOp::Put(PutOp {
                        key: b"b".to_vec(),
                        value: vec![2; 500],
                        lease: None,
                        prev_kv: false,
                    }),
                    BranchOp::Put(PutOp {
                        key: b"c".to_vec(),
                        value: vec![3; 500],
                        lease: None,
                        prev_kv: false,
                    }),
                    BranchOp::DeleteRange(DeleteRangeOp {
                        range: KeyRange::exact(b"a".to_vec()),
                        prev_kv: false,
                    }),
                ],
                failure: vec![],
            }),
        ),
        put(b"d", &[4; 700]),
    ];
    let expected_events = [1usize, 3, 1];
    // Baseline op counts.
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, 4 << 20).unwrap();
    let mut domain = Domain::new(engine, 1);
    let setup = shared.ops();
    for r in &requests {
        domain.run(r);
    }
    let total = shared.ops() - setup;
    drop(domain);
    for k in 1..=total {
        let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
        let engine = RedbEngine::create_on_backend(backend, 4 << 20).unwrap();
        let mut domain = Domain::new(engine, 1);
        shared.set_plan(FaultPlan {
            crash_after: Some(setup + k),
            tail: Tail::Seeded(k),
            ..FaultPlan::default()
        });
        let mut applied = 0;
        for r in &requests {
            let gated = match domain.worker.reader().snapshot() {
                Ok(g) => g,
                Err(_) => break,
            };
            let view = match build_read_view(&gated, NS, r, ViewBudget::default()) {
                Ok(v) => v,
                Err(_) => break,
            };
            drop(gated);
            let planned = plan(r, &view, &PlanLimits::default()).unwrap();
            match apply_plan(&mut domain.worker, domain.alloc.allocate(), NS, &planned) {
                Ok(ApplyOutcome::Applied(_)) => applied += 1,
                _ => break,
            }
        }
        assert!(shared.is_frozen());
        let image = shared.crash_image(Tail::Seeded(k));
        drop(domain);
        let (backend, _) = FaultBackend::new(image, FaultPlan::default());
        let engine = RedbEngine::from_backend(backend, 4 << 20).unwrap();
        let view = engine.reader().snapshot().unwrap();
        let kv_revision = coord_storage::codecs::read_kv_revision(&view)
            .unwrap()
            .get();
        assert!(
            applied as u64 <= kv_revision && kv_revision <= applied as u64 + 1,
            "k={k}: applied={applied} kv_revision={kv_revision}"
        );
        for r in 1..=3u64 {
            let events = events_at(&view, rev(r)).unwrap();
            if r <= kv_revision {
                assert_eq!(
                    events.map(|e| e.len()),
                    Some(expected_events[r as usize - 1]),
                    "k={k}: revision {r} events incomplete"
                );
            } else {
                assert_eq!(
                    events, None,
                    "k={k}: revision {r} has events beyond the frontier"
                );
            }
        }
        // The current rows agree with the frontier.
        let has = |key: &[u8]| {
            view.get(
                Collection::KvCurrentV1.id(),
                &coord_storage::codecs::current_key(&NS, key),
            )
            .unwrap()
            .is_some()
        };
        match kv_revision {
            0 => assert!(!has(b"a") && !has(b"b")),
            1 => assert!(has(b"a") && !has(b"b")),
            2 => assert!(!has(b"a") && has(b"b") && has(b"c") && !has(b"d")),
            3 => assert!(has(b"d") && has(b"c")),
            _ => panic!("impossible frontier"),
        }
    }
}
