//! task-13: no gap around registration, slow consumers close with a resume
//! point, fragmented revisions stay atomic, progress never overtakes
//! pending events, cancellation resumes, compaction and authorization.

use coord_core::effect::BootId;
use coord_core::outbox::BarrierAllocator;
use coord_state::{KvEvent, KvEventKind, PlanLimits, plan};
use coord_storage::watch::{
    Fragment, ReplayError, ReplayFromViewError, RevisionAssembler, chunk, replay_from_view,
};
use coord_storage::{
    ApplyOutcome, CloseReason, GroupLimits, StoreWorker, ViewBudget, WatchBatch, WatchHub,
    WatchItem, WatchSpec, apply_plan, build_read_view,
};
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;
use coord_types::logical_v1::*;

const NS: NamespaceId = NamespaceId([0x11; 16]);

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn event(key: &[u8]) -> KvEvent {
    KvEvent {
        kind: KvEventKind::Put,
        key: key.to_vec(),
        entry: None,
        prev: None,
    }
}

fn spec(key: &[u8], end: Option<&[u8]>, start: Option<u64>) -> WatchSpec {
    WatchSpec {
        namespace: NS,
        key: key.to_vec(),
        range_end: end.map(<[u8]>::to_vec),
        start_revision: start.map(rev),
        prev_kv: false,
        progress_notify: true,
        queue_capacity: 8,
    }
}

fn drain(hub: &WatchHub, id: coord_storage::WatchId) -> Vec<WatchItem> {
    let mut out = Vec::new();
    while let Some(item) = hub.next(id, |_| true) {
        let closed = matches!(item, WatchItem::Closed { .. });
        out.push(item);
        if closed {
            break;
        }
    }
    out
}

fn revisions(items: &[WatchItem]) -> Vec<u64> {
    items
        .iter()
        .filter_map(|i| {
            if let WatchItem::Batch(b) = i {
                Some(b.revision.get())
            } else {
                None
            }
        })
        .collect()
}

/// Storage-backed domain that publishes each applied revision to the hub.
struct Domain {
    worker: StoreWorker<ModelEngine>,
    alloc: BarrierAllocator,
    hub: WatchHub,
}

impl Domain {
    fn new() -> Self {
        let boot = BootId([1; 16]);
        let worker = StoreWorker::open(
            ModelEngine::new(),
            boot,
            ReplicaIncarnation::new(1).unwrap(),
            GroupLimits::default(),
        )
        .unwrap();
        Domain {
            worker,
            alloc: BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), boot),
            hub: WatchHub::new(KvRevision::ZERO, KvRevision::ZERO),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> KvRevision {
        self.put_in(NS, key, value)
    }

    fn put_in(&mut self, ns: NamespaceId, key: &[u8], value: &[u8]) -> KvRevision {
        let mut request = LogicalRequest::new(
            ns,
            CanonicalOperation::Put(PutOp {
                key: key.to_vec(),
                value: value.to_vec(),
                lease: None,
                prev_kv: false,
            }),
        );
        request.canonicalize();
        self.apply(&request).unwrap()
    }

    fn apply(&mut self, request: &LogicalRequest) -> Option<KvRevision> {
        let ns = request.namespace;
        let gated = self.worker.reader().snapshot().unwrap();
        let view = build_read_view(
            &gated,
            ns,
            PrincipalId([0xaa; 16]),
            request,
            ViewBudget::default(),
        )
        .unwrap();
        let planned = plan(request, &view, &PlanLimits::default()).unwrap();
        drop(gated);
        assert!(matches!(
            apply_plan(&mut self.worker, self.alloc.allocate(), ns, &planned, None).unwrap(),
            ApplyOutcome::Applied(_)
        ));
        // Publish only after the durable commit, in revision order.
        if let Some(r) = planned.revision {
            self.hub.publish(ns, r, &planned.events).unwrap();
        }
        planned.revision
    }

    fn register_and_replay(&self, spec: WatchSpec) -> Result<coord_storage::WatchId, CloseReason> {
        let registration = self.hub.register(spec)?;
        if let Some((from, through)) = registration.replay {
            let gated = self.worker.reader().snapshot().unwrap();
            match replay_from_view(&self.hub, gated.view(), registration.id, NS, from, through) {
                Ok(()) => {}
                Err(ReplayFromViewError::Compacted { .. }) => return Err(CloseReason::Compacted),
                Err(e) => panic!("{e:?}"),
            }
        }
        Ok(registration.id)
    }
}

#[test]
fn mutation_during_registration_appears_exactly_once_with_no_gap() {
    let mut d = Domain::new();
    d.put(b"a", b"1");
    d.put(b"a", b"2");
    // Register at frontier 2 but replay lazily: a mutation lands in between.
    let registration = d.hub.register(spec(b"a", None, Some(1))).unwrap();
    assert_eq!(registration.replay, Some((rev(1), rev(2))));
    d.put(b"a", b"3"); // revision 3, published live before replay finished
    let gated = d.worker.reader().snapshot().unwrap();
    replay_from_view(&d.hub, gated.view(), registration.id, NS, rev(1), rev(2)).unwrap();
    drop(gated);
    let items = drain(&d.hub, registration.id);
    assert_eq!(
        revisions(&items),
        vec![1, 2, 3],
        "replay through the frontier, then live, no gap and no duplicate"
    );
    // The last delivered batch is revision 3 itself, so no separate progress
    // is owed: progress only reports history processed beyond a delivered batch.
    assert!(matches!(items.last(), Some(WatchItem::Batch(b)) if b.revision == rev(3)));
    assert_eq!(d.hub.next(registration.id, |_| true), None);
    // Replay beyond the frontier or out of order is refused.
    assert_eq!(
        d.hub.replay(registration.id, NS, rev(4), &[]),
        Err(ReplayError::ReplayFinished)
    );
    let other = d.hub.register(spec(b"a", None, Some(1))).unwrap();
    assert_eq!(
        d.hub.replay(other.id, NS, rev(9), &[]),
        Err(ReplayError::BeyondFrontier { frontier: rev(3) })
    );
    d.hub.replay(other.id, NS, rev(1), &[event(b"a")]).unwrap();
    assert_eq!(
        d.hub.replay(other.id, NS, rev(1), &[]),
        Err(ReplayError::OutOfOrder {
            processed_through: rev(1)
        })
    );
    // Live items are held back until replay completes.
    d.put(b"a", b"4");
    assert!(
        d.hub
            .next(other.id, |_| true)
            .is_some_and(|i| matches!(i, WatchItem::Batch(b) if b.revision == rev(1)))
    );
    assert_eq!(
        d.hub.next(other.id, |_| true),
        None,
        "live batches wait for replay completion"
    );
    d.hub.replay_complete(other.id).unwrap();
    assert_eq!(revisions(&drain(&d.hub, other.id)), vec![4]);
}

#[test]
fn slow_consumer_is_closed_with_a_resume_point_never_skipped_over() {
    let mut d = Domain::new();
    let id = d.register_and_replay(spec(b"a", None, None)).unwrap();
    for i in 0..12u8 {
        d.put(b"a", &[i]);
    }
    let items = drain(&d.hub, id);
    let delivered = revisions(&items);
    assert_eq!(
        delivered,
        (1..=8).collect::<Vec<_>>(),
        "queued batches are delivered in order"
    );
    assert_eq!(
        items.last(),
        Some(&WatchItem::Closed {
            reason: CloseReason::SlowConsumer,
            last_complete_revision: rev(8)
        })
    );
    assert_eq!(d.hub.open_watches(), 0);
    // Resume from the last complete revision: the omitted changes replay.
    let id = d.register_and_replay(spec(b"a", None, Some(9))).unwrap();
    assert_eq!(revisions(&drain(&d.hub, id)), vec![9, 10, 11, 12]);
}

#[test]
fn fragmented_multi_key_revision_remains_atomic() {
    let batch = WatchBatch {
        revision: rev(7),
        events: vec![
            event(b"a"),
            event(b"b"),
            event(b"c"),
            event(b"d"),
            event(b"e"),
        ],
    };
    let fragments = chunk(&batch, 2);
    assert_eq!(fragments.len(), 3);
    assert!(fragments[..2].iter().all(|f| !f.complete) && fragments[2].complete);
    let mut assembler = RevisionAssembler::default();
    assert_eq!(assembler.push(fragments[0].clone()).unwrap(), None);
    assert!(assembler.has_partial(), "nothing exposed before completion");
    assert_eq!(assembler.push(fragments[1].clone()).unwrap(), None);
    assert_eq!(
        assembler.push(fragments[2].clone()).unwrap(),
        Some(batch.clone())
    );
    assert!(!assembler.has_partial());
    // Interleaving another revision into a partial one is an error.
    let mut assembler = RevisionAssembler::default();
    assembler.push(fragments[0].clone()).unwrap();
    let foreign = Fragment {
        revision: rev(8),
        events: vec![],
        complete: true,
    };
    assert!(assembler.push(foreign).is_err());
    // An empty revision batch is one complete fragment.
    assert_eq!(
        chunk(
            &WatchBatch {
                revision: rev(9),
                events: vec![]
            },
            2
        )
        .len(),
        1
    );
}

#[test]
fn progress_cannot_overtake_pending_events_and_filters_advance_it() {
    let mut d = Domain::new();
    let id = d.register_and_replay(spec(b"a", None, None)).unwrap();
    d.put(b"a", b"1"); // matches
    d.put(b"zzz", b"x"); // does not match
    d.put(b"a", b"2"); // matches
    let first = d.hub.next(id, |_| true).unwrap();
    assert!(matches!(first, WatchItem::Batch(ref b) if b.revision == rev(1)));
    // Revision 3 is still queued: no progress may be announced yet.
    let second = d.hub.next(id, |_| true).unwrap();
    assert!(
        matches!(second, WatchItem::Batch(ref b) if b.revision == rev(3)),
        "{second:?}"
    );
    assert_eq!(
        d.hub.next(id, |_| true),
        None,
        "progress is not repeated when nothing advanced"
    );
    d.put(b"zzz", b"y"); // revision 4, filtered out
    assert_eq!(
        d.hub.next(id, |_| true),
        Some(WatchItem::Progress(rev(4))),
        "filtered history processed through 4"
    );
    assert_eq!(d.hub.next(id, |_| true), None);
    // Progress never goes above what was delivered while a batch is pending.
    d.put(b"a", b"3"); // 5
    d.put(b"zzz", b"z"); // 6
    let items = drain(&d.hub, id);
    assert_eq!(
        items[0],
        WatchItem::Batch(WatchBatch {
            revision: rev(5),
            events: vec![KvEvent {
                kind: KvEventKind::Put,
                key: b"a".to_vec(),
                entry: items_entry(&items[0]),
                prev: None
            }]
        })
    );
    assert_eq!(items[1], WatchItem::Progress(rev(6)));
}

fn items_entry(item: &WatchItem) -> Option<coord_state::KvEntry> {
    if let WatchItem::Batch(b) = item {
        b.events[0].entry.clone()
    } else {
        None
    }
}

#[test]
fn cancellation_and_hub_close_are_resumable_and_authorization_gates_output() {
    let mut d = Domain::new();
    let id = d.register_and_replay(spec(b"a", Some(b"b"), None)).unwrap();
    d.put(b"a1", b"1");
    d.put(b"a2", b"2");
    assert!(d.hub.cancel(id));
    let items = drain(&d.hub, id);
    assert_eq!(
        revisions(&items),
        vec![1, 2],
        "queued batches drain before the close"
    );
    assert_eq!(
        items.last(),
        Some(&WatchItem::Closed {
            reason: CloseReason::Cancelled,
            last_complete_revision: rev(2)
        })
    );
    assert!(!d.hub.cancel(id), "already gone");
    // Output authorization: a denied batch closes the watch at the last
    // complete revision, and the denied revision is not delivered.
    let id = d
        .register_and_replay(spec(b"a", Some(b"b"), Some(1)))
        .unwrap();
    let mut calls = 0;
    let first = d.hub.next(id, |_| {
        calls += 1;
        true
    });
    assert!(matches!(first, Some(WatchItem::Batch(_))));
    let denied = d.hub.next(id, |b| b.revision != rev(2));
    assert_eq!(
        denied,
        Some(WatchItem::Closed {
            reason: CloseReason::Unauthorized,
            last_complete_revision: rev(1)
        })
    );
    // Compacted start is refused at registration.
    d.hub.set_retention_floor(rev(2));
    assert_eq!(
        d.hub.register(spec(b"a", None, Some(1))).unwrap_err(),
        CloseReason::Compacted
    );
    assert!(d.hub.register(spec(b"a", None, Some(2))).is_ok());
    // Hub close reaches every watch after draining.
    let id = d.register_and_replay(spec(b"a", Some(b"b"), None)).unwrap();
    d.put(b"a3", b"3");
    d.hub.close();
    let items = drain(&d.hub, id);
    assert_eq!(revisions(&items), vec![3]);
    assert!(matches!(
        items.last(),
        Some(WatchItem::Closed {
            reason: CloseReason::HubClosed,
            ..
        })
    ));
    assert!(
        d.hub.publish(NS, rev(4), &[]).is_err(),
        "closed hub rejects publication"
    );
}

#[test]
fn publication_must_be_contiguous_and_in_order() {
    let hub = WatchHub::new(rev(5), KvRevision::ZERO);
    assert!(hub.publish(NS, rev(7), &[]).is_err(), "gap");
    assert!(hub.publish(NS, rev(5), &[]).is_err(), "repeat");
    hub.publish(NS, rev(6), &[]).unwrap();
    assert_eq!(hub.published(), rev(6));
}

#[test]
fn replay_keeps_each_events_stored_namespace() {
    const OTHER: NamespaceId = NamespaceId([0x99; 16]);
    let mut d = Domain::new();
    d.put_in(OTHER, b"a", b"other"); // revision 1: same key, other namespace
    d.put(b"a", b"mine"); // revision 2
    let id = d.register_and_replay(spec(b"a", None, Some(1))).unwrap();
    let items = drain(&d.hub, id);
    assert_eq!(
        revisions(&items),
        vec![2],
        "the revision written in another namespace must not be relabelled as ours"
    );
    // Live publication is namespace-exact as well.
    d.put_in(OTHER, b"a", b"other2"); // 3
    d.put(b"a", b"mine2"); // 4
    assert_eq!(revisions(&drain(&d.hub, id)), vec![4]);
}

#[test]
fn a_start_revision_beyond_the_frontier_suppresses_earlier_live_revisions() {
    let mut d = Domain::new();
    for i in 0..5u8 {
        d.put(b"a", &[i]); // 1..=5
    }
    let registration = d.hub.register(spec(b"a", None, Some(8))).unwrap();
    assert_eq!(registration.replay, None);
    d.put(b"a", b"6");
    d.put(b"a", b"7");
    assert_eq!(
        d.hub.next(registration.id, |_| true),
        None,
        "revisions below the inclusive start are neither delivered nor progress"
    );
    d.put(b"a", b"8");
    d.put(b"a", b"9");
    let items = drain(&d.hub, registration.id);
    assert_eq!(revisions(&items), vec![8, 9]);
}

#[test]
fn a_full_replay_queue_can_be_drained_and_the_revision_retried() {
    let mut d = Domain::new();
    d.put(b"a", b"1");
    d.put(b"a", b"2");
    let mut small = spec(b"a", None, Some(1));
    small.queue_capacity = 1;
    let registration = d.hub.register(small).unwrap();
    assert_eq!(registration.replay, Some((rev(1), rev(2))));
    let gated = d.worker.reader().snapshot().unwrap();
    d.hub
        .replay(registration.id, NS, rev(1), &[event(b"a")])
        .unwrap();
    assert_eq!(
        d.hub.replay(registration.id, NS, rev(2), &[event(b"a")]),
        Err(ReplayError::QueueFull)
    );
    // Drain, then the same revision is accepted (not OutOfOrder).
    assert!(matches!(
        d.hub.next(registration.id, |_| true),
        Some(WatchItem::Batch(b)) if b.revision == rev(1)
    ));
    d.hub
        .replay(registration.id, NS, rev(2), &[event(b"a")])
        .unwrap();
    drop(gated);
    d.hub.replay_complete(registration.id).unwrap();
    assert_eq!(revisions(&drain(&d.hub, registration.id)), vec![2]);
}
