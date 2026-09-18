//! task-14: before-floor reads return Compacted, later reads keep untouched
//! old values, holds retain history under active views and watches, GC is
//! incremental and crash-resumable, and model/redb agree.

use std::collections::BTreeMap;

use coord_core::effect::{BootId, PersistBatch};
use coord_core::outbox::BarrierAllocator;
use coord_redb_faultkit::{FaultBackend, FaultPlan};
use coord_state::{Outcome, PlanLimits, Response, plan};
use coord_storage::compaction::{GcBudget, RetentionHolds, cursors, effective_floor, plan_gc};
use coord_storage::watch::replay_from_view;
use coord_storage::{
    ApplyOutcome, CloseReason, GroupLimits, StoreWorker, ViewBudget, WatchHub, WatchItem,
    WatchSpec, apply_plan, build_read_view,
};
use coord_storage_redb::RedbEngine;
use coord_store_api::engine::{LocalEngine, OrderedRead, ScanRequest};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([0x11; 16]);

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn put(k: &[u8], v: &[u8]) -> LogicalRequest {
    LogicalRequest::new(
        NS,
        CanonicalOperation::Put(PutOp {
            key: k.to_vec(),
            value: v.to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

fn del(k: &[u8]) -> LogicalRequest {
    LogicalRequest::new(
        NS,
        CanonicalOperation::DeleteRange(DeleteRangeOp {
            range: KeyRange::exact(k.to_vec()),
            prev_kv: false,
        }),
    )
}

fn get_at(range: KeyRange, r: u64) -> LogicalRequest {
    LogicalRequest::new(
        NS,
        CanonicalOperation::Range(RangeOp {
            range,
            revision: Some(rev(r)),
            limit: 0,
            keys_only: false,
            count_only: false,
        }),
    )
}

fn compact(r: u64) -> LogicalRequest {
    LogicalRequest::new(NS, CanonicalOperation::Compact { revision: rev(r) })
}

struct Domain<E: LocalEngine> {
    worker: StoreWorker<E>,
    alloc: BarrierAllocator,
    hub: WatchHub,
    holds: RetentionHolds,
}

impl<E: LocalEngine> Domain<E> {
    fn new(engine: E) -> Self {
        let boot = BootId([1; 16]);
        let worker = StoreWorker::open(
            engine,
            boot,
            ReplicaIncarnation::new(1).unwrap(),
            GroupLimits::default(),
        )
        .unwrap();
        let gated = worker.reader().snapshot().unwrap();
        let published = coord_storage::codecs::read_kv_revision(gated.view()).unwrap();
        let floor = coord_storage::codecs::read_retention_floor(gated.view()).unwrap();
        drop(gated);
        Domain {
            worker,
            alloc: BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), boot),
            hub: WatchHub::new(published, floor),
            holds: RetentionHolds::default(),
        }
    }

    fn run(&mut self, request: &LogicalRequest) -> Response {
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_read_view(&gated, NS, request, ViewBudget::default()).unwrap();
        let planned = plan(request, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        assert!(matches!(
            apply_plan(&mut self.worker, self.alloc.allocate(), NS, &planned, None).unwrap(),
            ApplyOutcome::Applied(_)
        ));
        if let Some(r) = planned.revision {
            self.hub.publish(NS, r, &planned.events).unwrap();
        }
        if let CanonicalOperation::Compact { .. } = request.operation {
            let gated = self.worker.reader().snapshot().unwrap();
            self.hub.set_retention_floor(
                coord_storage::codecs::read_retention_floor(gated.view()).unwrap(),
            );
        }
        planned.response
    }

    /// Run GC steps until done; returns the number of steps.
    fn gc(&mut self, budget: GcBudget) -> usize {
        let mut steps = 0;
        loop {
            let gated = self.worker.reader().snapshot().unwrap();
            let step = plan_gc(gated.view(), &self.holds, budget).unwrap();
            drop(gated);
            steps += 1;
            if !step.updates.is_empty() {
                self.worker
                    .submit(PersistBatch {
                        barrier: self.alloc.allocate(),
                        base: None,
                        updates: step.updates,
                    })
                    .unwrap();
                assert_eq!(self.worker.flush().unwrap().committed, 1);
            }
            if step.done {
                return steps;
            }
            assert!(steps < 10_000, "gc must terminate");
        }
    }

    fn history_rows(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let gated = self.worker.reader().snapshot().unwrap();
        let mut out = BTreeMap::new();
        for c in [Collection::KvHistoryV1, Collection::EventsV1] {
            let mut request = ScanRequest::all(1000, 1 << 24);
            loop {
                let page = gated.view().scan_page(c.id(), &request).unwrap();
                for r in &page.rows {
                    let mut k = vec![c.id().0 as u8];
                    k.extend_from_slice(&r.key);
                    out.insert(k, r.value.clone());
                }
                if page.exhausted {
                    break;
                }
                request.resume_after = Some(page.rows.last().unwrap().key.clone());
            }
        }
        out
    }

    fn history_revisions_of(&self, key: &[u8]) -> Vec<u64> {
        self.history_rows()
            .keys()
            .filter(|k| k[0] == Collection::KvHistoryV1.id().0 as u8)
            .filter_map(|k| coord_types::ordered_key::decode_history(&k[1..]).ok())
            .filter(|d| d.key == key)
            .map(|d| d.revision.unwrap().get())
            .collect()
    }

    fn event_revisions(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self
            .history_rows()
            .keys()
            .filter(|k| k[0] == Collection::EventsV1.id().0 as u8)
            .map(|k| {
                coord_storage::codecs::decode_event_key(&k[1..])
                    .unwrap()
                    .0
                    .get()
            })
            .collect();
        out.dedup();
        out
    }
}

/// a: v1@1, v2@3, deleted@5, v3@8; b: v1@2 (untouched afterwards); c: v1@4, v2@6, v3@7; d: v1@9, v2@10.
fn workload<E: LocalEngine>(d: &mut Domain<E>) {
    d.run(&put(b"a", b"a1")); // 1
    d.run(&put(b"b", b"b1")); // 2
    d.run(&put(b"a", b"a2")); // 3
    d.run(&put(b"c", b"c1")); // 4
    d.run(&del(b"a")); // 5
    d.run(&put(b"c", b"c2")); // 6
    d.run(&put(b"c", b"c3")); // 7
    d.run(&put(b"a", b"a3")); // 8
    d.run(&put(b"d", b"d1")); // 9
    d.run(&put(b"d", b"d2")); // 10
}

fn reads_at<E: LocalEngine>(d: &mut Domain<E>, from: u64, to: u64) -> Vec<Response> {
    (from..=to)
        .map(|r| d.run(&get_at(KeyRange::interval(b"a".to_vec(), b"z".to_vec()), r)))
        .collect()
}

#[test]
fn compaction_keeps_needed_versions_and_before_floor_reads_are_compacted() {
    let mut d = Domain::new(ModelEngine::new());
    workload(&mut d);
    let before = reads_at(&mut d, 6, 10);
    d.run(&compact(6));
    assert!(d.gc(GcBudget::default()) >= 1);
    // History below the floor: only the newest version at or below 6 per key.
    assert_eq!(
        d.history_revisions_of(b"a"),
        vec![5, 8],
        "a keeps the tombstone at 5 (newest <= 6) and the newer 8"
    );
    assert_eq!(
        d.history_revisions_of(b"b"),
        vec![2],
        "b untouched since 2 keeps its only version"
    );
    assert_eq!(
        d.history_revisions_of(b"c"),
        vec![6, 7],
        "c keeps 6 (newest <= 6) and 7; 4 is gone"
    );
    assert_eq!(
        d.event_revisions(),
        (6..=10).collect::<Vec<_>>(),
        "events below the floor are deleted whole"
    );
    // Reads at or above the floor are unchanged, including b's old value.
    let after = reads_at(&mut d, 6, 10);
    assert_eq!(before, after);
    if let Outcome::Range { items, .. } = &after[0].outcome {
        let keys: Vec<&[u8]> = items.iter().map(|i| i.key.as_slice()).collect();
        assert_eq!(keys, vec![b"b".as_slice(), b"c"]);
        assert_eq!(items[0].entry.value, b"b1");
        assert_eq!(items[1].entry.value, b"c2");
    } else {
        panic!();
    }
    // Before the floor: Compacted, never a partial answer.
    for r in 1..6 {
        assert_eq!(
            d.run(&get_at(KeyRange::exact(b"b".to_vec()), r)).outcome,
            Outcome::ErrCompacted
        );
    }
    // A second GC pass at the same floor is a no-op.
    let rows = d.history_rows();
    d.gc(GcBudget::default());
    assert_eq!(d.history_rows(), rows);
}

#[test]
fn holds_from_active_views_and_watches_retain_history() {
    let mut d = Domain::new(ModelEngine::new());
    workload(&mut d);
    // A paginating view pins revision 3; a watch resumes from 4.
    let page_hold = d.holds.hold(rev(3));
    let watch_hold = d.holds.hold(rev(4));
    d.run(&compact(8));
    let gated = d.worker.reader().snapshot().unwrap();
    assert_eq!(effective_floor(gated.view(), &d.holds).unwrap(), rev(3));
    drop(gated);
    d.gc(GcBudget::default());
    assert_eq!(
        d.history_revisions_of(b"a"),
        vec![3, 5, 8],
        "under the hold at 3, only a@1 is collected"
    );
    assert_eq!(d.event_revisions(), (3..=10).collect::<Vec<_>>());
    // The pinned page is still answerable at 3 through the storage layer:
    // every key's newest version at or below 3 survives (a@3, b@2), even
    // though the API floor of 8 makes a *fresh* read at 3 `Compacted`.
    let at_3: Vec<(Vec<u8>, u64)> = d
        .history_rows()
        .keys()
        .filter(|k| k[0] == Collection::KvHistoryV1.id().0 as u8)
        .filter_map(|k| coord_types::ordered_key::decode_history(&k[1..]).ok())
        .filter(|h| h.revision.unwrap().get() <= 3)
        .map(|h| (h.key, h.revision.unwrap().get()))
        .collect();
    assert_eq!(at_3, vec![(b"a".to_vec(), 3), (b"b".to_vec(), 2)]);
    // Release the page hold: the watch hold at 4 still bounds GC.
    drop(page_hold);
    d.gc(GcBudget::default());
    assert_eq!(
        d.history_revisions_of(b"a"),
        vec![3, 5, 8],
        "a@3 is the newest version at or below 4"
    );
    assert_eq!(d.event_revisions(), (4..=10).collect::<Vec<_>>());
    // Release the watch hold: GC reaches the replicated floor of 8.
    drop(watch_hold);
    assert!(d.holds.is_empty());
    d.gc(GcBudget::default());
    assert_eq!(d.history_revisions_of(b"a"), vec![8]);
    assert_eq!(d.history_revisions_of(b"c"), vec![7]);
    assert_eq!(d.event_revisions(), (8..=10).collect::<Vec<_>>());
    // The replicated floor, not the hold, decides what the API promises:
    // a read at 3 is Compacted once the floor is 8, even though a hold once existed.
    assert_eq!(
        d.run(&get_at(KeyRange::exact(b"a".to_vec()), 3)).outcome,
        Outcome::ErrCompacted
    );
}

#[test]
fn watch_resume_below_the_floor_fails_explicitly_and_above_it_replays() {
    let mut d = Domain::new(ModelEngine::new());
    workload(&mut d);
    d.run(&compact(6));
    d.gc(GcBudget::default());
    let spec = |start: u64| WatchSpec {
        namespace: NS,
        key: b"a".to_vec(),
        range_end: Some(b"z".to_vec()),
        start_revision: Some(rev(start)),
        prev_kv: false,
        progress_notify: false,
        queue_capacity: 64,
    };
    assert_eq!(d.hub.register(spec(5)).unwrap_err(), CloseReason::Compacted);
    let registration = d.hub.register(spec(6)).unwrap();
    let gated = d.worker.reader().snapshot().unwrap();
    let (from, through) = registration.replay.unwrap();
    replay_from_view(&d.hub, gated.view(), registration.id, NS, from, through).unwrap();
    drop(gated);
    let mut revisions = Vec::new();
    while let Some(item) = d.hub.next(registration.id, |_| true) {
        if let WatchItem::Batch(b) = item {
            revisions.push(b.revision.get());
        }
    }
    assert_eq!(
        revisions,
        vec![6, 7, 8, 9, 10],
        "no gap: every retained revision replays"
    );
}

#[test]
fn gc_is_incremental_crash_resumable_and_identical_across_engines() {
    // Model engine with a tiny budget: many steps, cursors persisted.
    let mut model = Domain::new(ModelEngine::new());
    workload(&mut model);
    model.run(&compact(8));
    let tiny = GcBudget {
        max_examined: 2,
        max_deletes: 2,
    };
    let steps = model.gc(tiny);
    assert!(steps > 3, "tiny budget needs several steps: {steps}");
    let expected = model.history_rows();

    // Crash between steps: a fresh boot reads the persisted cursor and
    // finishes the pass; the result is the same.
    let mut crashy = Domain::new(ModelEngine::new());
    workload(&mut crashy);
    crashy.run(&compact(8));
    for _ in 0..2 {
        let gated = crashy.worker.reader().snapshot().unwrap();
        let step = plan_gc(gated.view(), &crashy.holds, tiny).unwrap();
        drop(gated);
        crashy
            .worker
            .submit(PersistBatch {
                barrier: crashy.alloc.allocate(),
                base: None,
                updates: step.updates,
            })
            .unwrap();
        crashy.worker.flush().unwrap();
    }
    let mut engine = crashy.worker.into_engine();
    engine.crash_and_reopen();
    let mut resumed = Domain::new(engine);
    let gated = resumed.worker.reader().snapshot().unwrap();
    let (history_cursor, _) = cursors(gated.view()).unwrap();
    assert!(
        history_cursor.is_some_and(|c| !c.done && c.floor == rev(8)),
        "cursor persisted before the crash"
    );
    drop(gated);
    resumed.gc(tiny);
    assert_eq!(resumed.history_rows(), expected);

    // redb produces identical rows.
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let mut redb = Domain::new(RedbEngine::create_on_backend(backend, 4 << 20).unwrap());
    workload(&mut redb);
    redb.run(&compact(8));
    redb.gc(GcBudget::default());
    assert_eq!(redb.history_rows(), expected);

    // Raising the floor restarts the pass under the new floor.
    redb.run(&compact(10));
    redb.gc(GcBudget::default());
    assert_eq!(redb.history_revisions_of(b"d"), vec![10]);
    assert_eq!(redb.event_revisions(), vec![10]);
    let gated = redb.worker.reader().snapshot().unwrap();
    let (c, e) = cursors(gated.view()).unwrap();
    assert_eq!(c.map(|c| (c.floor, c.done)), Some((rev(10), true)));
    assert_eq!(e.through, rev(9));
}
