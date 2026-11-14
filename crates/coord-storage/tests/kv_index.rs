//! task-d105: one version row a revision, with the key index in memory.
//!
//! A put or a delete writes its version only as the revision's event row;
//! reads at a past revision find each key's version through the key index
//! and read its event row. These tests hold that to a model of every
//! revision's state, over randomized histories with deletes and range
//! deletes among them and compactions with garbage collection between:
//! every read at a retained revision answers as the model does, a read at
//! the floor still finds each key, and the index an open rebuilds from the
//! rows equals the one kept live.

use std::collections::BTreeMap;

use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_state::{Outcome, PlanLimits, plan};
use coord_storage::compaction::{GcBudget, RetentionHolds, plan_gc};
use coord_storage::kv_index::KvIndex;
use coord_storage::{
    ApplyOutcome, GroupLimits, StoreWorker, ViewBudget, apply_plan, build_read_view,
};
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([0x11; 16]);
const OTHER: NamespaceId = NamespaceId([0x22; 16]);
const KEYS: &[&[u8]] = &[b"a", b"b", b"ba", b"c", b"d", b"e"];

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

/// A small deterministic generator, so a failing seed is reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Domain<E: LocalEngine> {
    worker: Option<StoreWorker<E>>,
    alloc: BarrierAllocator,
    holds: RetentionHolds,
}

impl<E: LocalEngine> Domain<E> {
    fn open(engine: E) -> Self {
        let boot = BootId([1; 16]);
        let worker = StoreWorker::open(
            engine,
            boot,
            ReplicaIncarnation::new(1).unwrap(),
            GroupLimits::default(),
        )
        .unwrap();
        Domain {
            worker: Some(worker),
            alloc: BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), boot),
            holds: RetentionHolds::default(),
        }
    }

    fn worker(&mut self) -> &mut StoreWorker<E> {
        self.worker.as_mut().expect("open")
    }

    fn run(&mut self, namespace: NamespaceId, operation: CanonicalOperation) -> Outcome {
        let request = LogicalRequest::new(namespace, operation);
        let gated = self.worker().reader().snapshot().unwrap();
        let view = build_read_view(
            &gated,
            namespace,
            PrincipalId([0xaa; 16]),
            &request,
            ViewBudget::default(),
        )
        .unwrap();
        let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        let barrier = self.alloc.allocate();
        let worker = self.worker.as_mut().expect("open");
        assert!(matches!(
            apply_plan(worker, barrier, namespace, &planned, None).unwrap(),
            ApplyOutcome::Applied(_)
        ));
        planned.response.outcome
    }

    fn gc(&mut self) {
        for _ in 0..10_000 {
            let gated = self.worker().reader().snapshot().unwrap();
            let step = gated
                .with_kv_index(|index| {
                    plan_gc(gated.view(), index, &self.holds, GcBudget::default())
                })
                .unwrap()
                .unwrap();
            drop(gated);
            if !step.updates.is_empty() {
                let barrier = self.alloc.allocate();
                let worker = self.worker.as_mut().expect("open");
                worker
                    .submit(PersistBatch {
                        barrier,
                        base: None,
                        updates: step.updates,
                    })
                    .unwrap();
                worker.flush().unwrap();
            }
            if step.done {
                return;
            }
        }
        panic!("gc must terminate");
    }

    /// The index as published, and one an open would build from the rows.
    fn indexes(&mut self) -> (KvIndex, KvIndex) {
        let gated = self.worker().reader().snapshot().unwrap();
        let live = gated.with_kv_index(KvIndex::clone).unwrap();
        let rebuilt = KvIndex::build(gated.view()).unwrap();
        (live, rebuilt)
    }

    /// Close and open again: the index is rebuilt from the rows.
    fn reopen(&mut self) {
        let engine = self.worker.take().expect("open").into_engine();
        let boot = BootId([2; 16]);
        self.worker = Some(
            StoreWorker::open(
                engine,
                boot,
                ReplicaIncarnation::new(1).unwrap(),
                GroupLimits::default(),
            )
            .unwrap(),
        );
        self.alloc = BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), boot);
    }

    fn event_rows(&mut self) -> usize {
        let gated = self.worker().reader().snapshot().unwrap();
        let page = gated
            .view()
            .scan_page(
                Collection::EventsV1.id(),
                &ScanRequest::all(100_000, 1 << 26),
            )
            .unwrap();
        page.rows.len()
    }
}

fn range_all_at(r: u64) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::interval(b"\0".to_vec(), b"\xff".to_vec()),
        revision: Some(rev(r)),
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

fn get_at(key: &[u8], r: u64) -> CanonicalOperation {
    CanonicalOperation::Range(RangeOp {
        range: KeyRange::exact(key.to_vec()),
        revision: Some(rev(r)),
        limit: 0,
        keys_only: false,
        count_only: false,
    })
}

/// Every revision's state: key to value.
type Model = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;

fn items(outcome: &Outcome) -> BTreeMap<Vec<u8>, Vec<u8>> {
    match outcome {
        Outcome::Range { items, .. } => items
            .iter()
            .map(|i| (i.key.clone(), i.entry.value.clone()))
            .collect(),
        other => panic!("not a range: {other:?}"),
    }
}

/// Every read at a retained revision answers as the model.
fn check<E: LocalEngine>(d: &mut Domain<E>, model: &Model, floor: u64, top: u64, label: &str) {
    for r in floor.max(1)..=top {
        let want = model
            .range(..=r)
            .next_back()
            .map(|(_, s)| s.clone())
            .unwrap_or_default();
        let got = items(&d.run(NS, range_all_at(r)));
        assert_eq!(got, want, "{label}: range at {r}");
        for key in KEYS {
            let got = items(&d.run(NS, get_at(key, r)));
            let want: BTreeMap<_, _> = want
                .get(*key)
                .map(|v| (key.to_vec(), v.clone()))
                .into_iter()
                .collect();
            assert_eq!(got, want, "{label}: {key:?} at {r}");
        }
    }
}

fn history<E: LocalEngine>(engine: E, seed: u64) {
    let mut d = Domain::open(engine);
    let mut rng = Rng(seed | 1);
    let mut model: Model = BTreeMap::new();
    let mut state: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut top = 0u64;
    let mut floor = 0u64;
    for step in 0..120u64 {
        let key = KEYS[rng.below(KEYS.len() as u64) as usize].to_vec();
        let op = rng.below(10);
        let changed = if op < 6 {
            let value = format!("v{step}").into_bytes();
            d.run(
                NS,
                CanonicalOperation::Put(PutOp {
                    key: key.clone(),
                    value: value.clone(),
                    lease: None,
                    prev_kv: false,
                }),
            );
            state.insert(key, value);
            true
        } else if op < 8 {
            // A range delete: several events in one revision.
            let end = KEYS[rng.below(KEYS.len() as u64) as usize].to_vec();
            let (lo, hi) = if key <= end { (key, end) } else { (end, key) };
            let hit: Vec<_> = state
                .range(lo.clone()..hi.clone())
                .map(|(k, _)| k.clone())
                .collect();
            if hit.is_empty() {
                continue;
            }
            d.run(
                NS,
                CanonicalOperation::DeleteRange(DeleteRangeOp {
                    range: KeyRange::interval(lo, hi),
                    prev_kv: false,
                }),
            );
            for k in hit {
                state.remove(&k);
            }
            true
        } else if op < 9 {
            // Another namespace's write moves the revision; this one's
            // reads must not see it.
            d.run(
                OTHER,
                CanonicalOperation::Put(PutOp {
                    key,
                    value: b"other".to_vec(),
                    lease: None,
                    prev_kv: false,
                }),
            );
            true
        } else if top > floor + 2 {
            floor = floor.max(1) + rng.below(top - floor.max(1));
            d.run(
                NS,
                CanonicalOperation::Compact {
                    revision: rev(floor),
                },
            );
            d.gc();
            false
        } else {
            continue;
        };
        if changed {
            top += 1;
            model.insert(top, state.clone());
        }
        if step % 30 == 29 {
            check(
                &mut d,
                &model,
                floor,
                top,
                &format!("seed {seed} step {step}"),
            );
            let (live, rebuilt) = d.indexes();
            assert_eq!(
                live, rebuilt,
                "seed {seed} step {step}: the live index is the rows'"
            );
        }
    }
    check(&mut d, &model, floor, top, &format!("seed {seed} end"));
    // Compact to the top, collect, and a read at the floor still finds
    // each key it held.
    if top > 0 {
        d.run(NS, CanonicalOperation::Compact { revision: rev(top) });
        floor = top;
        d.gc();
        check(
            &mut d,
            &model,
            floor,
            top,
            &format!("seed {seed} at the floor"),
        );
    }
    let (live, rebuilt) = d.indexes();
    assert_eq!(live, rebuilt, "seed {seed}: after the last collection");
    d.reopen();
    check(
        &mut d,
        &model,
        floor,
        top,
        &format!("seed {seed} after a restart"),
    );
    let (live, rebuilt) = d.indexes();
    assert_eq!(live, rebuilt, "seed {seed}: after a restart");
}

#[test]
fn reads_at_every_retained_revision_answer_as_the_model_does() {
    for seed in [1, 2, 3, 7, 11, 42, 99, 1234] {
        history(ModelEngine::new(), seed);
    }
}

#[test]
fn reads_through_the_index_on_redb_answer_as_the_model_does() {
    let (backend, _) = coord_redb_faultkit::FaultBackend::new(
        Vec::new(),
        coord_redb_faultkit::FaultPlan::default(),
    );
    let engine = coord_storage_redb::RedbEngine::create_on_backend(backend, 4 << 20).unwrap();
    history(engine, 5);
}

#[test]
fn a_put_writes_no_history_row_and_one_event_row() {
    let mut d = Domain::open(ModelEngine::new());
    d.run(
        NS,
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    let gated = d.worker().reader().snapshot().unwrap();
    let history = gated
        .view()
        .scan_page(Collection::KvHistoryV1.id(), &ScanRequest::all(10, 1 << 20))
        .unwrap();
    assert!(history.rows.is_empty(), "no history row");
    drop(gated);
    assert_eq!(d.event_rows(), 1);
}

#[test]
fn compaction_keeps_each_keys_newest_put_at_or_below_the_floor_and_drops_its_tombstones() {
    let mut d = Domain::open(ModelEngine::new());
    let put = |k: &[u8], v: &[u8]| {
        CanonicalOperation::Put(PutOp {
            key: k.to_vec(),
            value: v.to_vec(),
            lease: None,
            prev_kv: false,
        })
    };
    d.run(NS, put(b"a", b"a1")); // 1
    d.run(NS, put(b"a", b"a2")); // 2
    d.run(NS, put(b"b", b"b1")); // 3
    d.run(
        NS,
        CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::exact(b"b".to_vec()),
            prev_kv: false,
        }),
    ); // 4: b's tombstone
    d.run(NS, put(b"c", b"c1")); // 5
    d.run(NS, put(b"a", b"a3")); // 6
    d.run(NS, CanonicalOperation::Compact { revision: rev(5) });
    d.gc();
    let gated = d.worker().reader().snapshot().unwrap();
    let index = gated.with_kv_index(KvIndex::clone).unwrap();
    drop(gated);
    let revisions = |k: &[u8]| -> Vec<u64> {
        index
            .versions_of(NS, k)
            .iter()
            .map(|v| v.revision.get())
            .collect()
    };
    // a: 2 is its newest at or below 5, 1 is superseded, 6 is above.
    assert_eq!(revisions(b"a"), vec![2, 6]);
    // b: deleted at 4, below the floor; its put and its tombstone go.
    assert_eq!(revisions(b"b"), Vec::<u64>::new());
    // c: written at the floor.
    assert_eq!(revisions(b"c"), vec![5]);
    // A read at the floor still finds a and c, and no b.
    let at_floor = items(&d.run(NS, range_all_at(5)));
    assert_eq!(
        at_floor,
        BTreeMap::from([
            (b"a".to_vec(), b"a2".to_vec()),
            (b"c".to_vec(), b"c1".to_vec())
        ])
    );
}

fn put_op(k: &[u8], v: &[u8]) -> CanonicalOperation {
    CanonicalOperation::Put(PutOp {
        key: k.to_vec(),
        value: v.to_vec(),
        lease: None,
        prev_kv: false,
    })
}

/// A view pinned before a compaction and its collection reads, at a
/// revision the newer floor has passed, the versions the view still holds:
/// the live index has dropped them, so the read takes the view's own event
/// rows (Codex P1 on #245).
#[test]
fn a_view_pinned_before_compaction_reads_the_versions_it_holds() {
    let mut d = Domain::open(ModelEngine::new());
    d.run(NS, put_op(b"a", b"a1")); // 1
    d.run(NS, put_op(b"b", b"b1")); // 2
    let pinned = d.worker().reader().snapshot().unwrap();
    d.run(NS, put_op(b"a", b"a2")); // 3
    d.run(NS, put_op(b"b", b"b2")); // 4
    d.run(NS, CanonicalOperation::Compact { revision: rev(4) });
    d.gc();
    // The live index lost a@1 and b@2.
    let (live, _) = d.indexes();
    assert_eq!(live.versions_of(NS, b"a").len(), 1);
    assert!(live.floor() >= rev(4));
    // The pinned view still answers at 1 and 2 as it did when pinned.
    for (at, want) in [
        (1, vec![(b"a".to_vec(), b"a1".to_vec())]),
        (
            2,
            vec![
                (b"a".to_vec(), b"a1".to_vec()),
                (b"b".to_vec(), b"b1".to_vec()),
            ],
        ),
    ] {
        let request = LogicalRequest::new(NS, range_all_at(at));
        let view = build_read_view(
            &pinned,
            NS,
            PrincipalId([0xaa; 16]),
            &request,
            ViewBudget::default(),
        )
        .unwrap();
        let planned = plan(&request, &view, &PlanLimits::default()).unwrap();
        assert_eq!(
            items(&planned.response.outcome),
            want.into_iter().collect::<BTreeMap<_, _>>(),
            "at {at}"
        );
    }
}

/// A view that counts its point reads.
struct Counting<'a, V> {
    view: &'a V,
    gets: std::cell::Cell<usize>,
}

impl<V: OrderedRead> OrderedRead for Counting<'_, V> {
    fn get(
        &self,
        collection: coord_store_api::engine::CollectionId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, coord_store_api::engine::EngineError> {
        self.gets.set(self.gets.get() + 1);
        self.view.get(collection, key)
    }

    fn scan_page(
        &self,
        collection: coord_store_api::engine::CollectionId,
        request: &ScanRequest,
    ) -> Result<coord_store_api::engine::RowPage, coord_store_api::engine::EngineError> {
        self.view.scan_page(collection, request)
    }
}

/// A historical read reads each chosen version's event row only as its
/// caller takes it, so a caller that stops (a spent budget) stops the
/// reads too (Codex P2 on #245).
#[test]
fn a_historical_read_reads_versions_only_as_its_caller_takes_them() {
    let mut d = Domain::open(ModelEngine::new());
    for i in 0..50u8 {
        d.run(NS, put_op(&[b'k', i], b"v"));
    }
    let gated = d.worker().reader().snapshot().unwrap();
    let index = KvIndex::build(gated.view()).unwrap();
    let counting = Counting {
        view: gated.view(),
        gets: std::cell::Cell::new(0),
    };
    let mut taken = Vec::new();
    coord_storage::kv_index::versions_at(
        &index,
        &counting,
        NS,
        b"k",
        std::ops::Bound::Excluded(b"l".as_slice()),
        rev(50),
        |key, chosen| -> Result<bool, coord_store_api::engine::EngineError> {
            assert!(chosen.is_some());
            taken.push(key);
            Ok(taken.len() < 3)
        },
    )
    .unwrap();
    assert_eq!(taken, vec![vec![b'k', 0], vec![b'k', 1], vec![b'k', 2]]);
    assert_eq!(counting.gets.get(), 3, "one point read a key taken");
}
