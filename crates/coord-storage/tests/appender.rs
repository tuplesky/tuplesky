//! A journal-first store whose journal appends leave the caller's thread
//! (task-d54): the shared journal is lent to an appender for one synced
//! group append and taken back with the outcome.
//!
//! The appender here is mostly the manual one, so each test stops the
//! pipeline exactly where it means to: an append lent and not started, an
//! append synced and not taken back, transitions queued behind an append
//! that is out. Every boot that ends at one of those points recovers, at
//! the next attach, exactly what the journal actually holds.

use std::sync::Arc;
use std::sync::mpsc::channel;
use std::time::Duration;

use coord_core::effect::{BarrierId, BootId, PersistBatch, StoreUpdate};
use coord_core::event::{StorageError, StorageEvent};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_storage::journaled::{
    DomainStatus, JournalLimits, JournaledStore, Submission, TransitionKind,
};
use coord_storage::{ManualAppendHandle, ManualAppender, ThreadAppender};
use coord_store_testkit::journal::{AppendScript, ModelJournal};
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const A: DomainId = DomainId([0xa1; 16]);
const BOOT: BootId = BootId([0x77; 16]);

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn ballot() -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number: 1,
        leader: REPLICA,
    }
}

fn shard() -> ShardId {
    ShardId::new(0).unwrap()
}

fn kv(key: &[u8], value: &[u8]) -> StoreUpdate {
    StoreUpdate {
        collection: coord_store_api::registry::Collection::KvCurrentV1.id(),
        key: key.to_vec(),
        value: Some(value.to_vec()),
    }
}

/// The `n`th transition every test queues: a write of key `n % 3`, so
/// later transitions overwrite earlier ones and order matters.
fn nth(n: u64) -> Vec<StoreUpdate> {
    vec![kv(&[b'k', (n % 3) as u8], &n.to_be_bytes())]
}

struct World {
    store: JournaledStore<ModelJournal, ModelEngine>,
    barriers: BarrierAllocator,
    next: u64,
}

impl World {
    fn new() -> Self {
        let mut store = JournaledStore::open(
            ModelJournal::new(),
            CLUSTER,
            REPLICA,
            inc(),
            BOOT,
            JournalLimits::default(),
        )
        .unwrap();
        store.attach(A, shard(), ModelEngine::new()).unwrap();
        World {
            store,
            barriers: BarrierAllocator::new(inc(), BOOT),
            next: 1,
        }
    }

    /// The same world, its journal appends lent to a manual appender.
    fn lending() -> (Self, ManualAppendHandle<ModelJournal>) {
        let mut world = World::new();
        let (appender, handle) = ManualAppender::new();
        world.store.pipeline_journal(Box::new(appender)).unwrap();
        assert!(world.store.journal_pipelined());
        (world, handle)
    }

    /// Queue the next `count` transitions.
    fn queue(&mut self, count: u64) -> Vec<BarrierId> {
        let mut barriers = Vec::new();
        for _ in 0..count {
            let barrier = self.barriers.allocate();
            self.store
                .submit(Submission {
                    domain: A,
                    ballot: ballot(),
                    kind: TransitionKind::Protocol,
                    batch: PersistBatch {
                        barrier,
                        base: None,
                        updates: nth(self.next),
                    },
                })
                .unwrap();
            self.next += 1;
            barriers.push(barrier);
        }
        barriers
    }

    /// Queue an application at `position` under ballot `number`, planned
    /// on the base the store hands out now.
    fn apply(&mut self, number: u64, position: u64) -> BarrierId {
        let barrier = self.barriers.allocate();
        let base = self.store.application_base(A).unwrap();
        self.store
            .submit(Submission {
                domain: A,
                ballot: Ballot {
                    epoch: base.configuration,
                    number,
                    leader: REPLICA,
                },
                kind: TransitionKind::Application {
                    position: ExecutionPosition::new(position).unwrap(),
                    revision: None,
                    result_digest: coord_types::identity::Digest32([position as u8; 32]),
                },
                batch: PersistBatch {
                    barrier,
                    base: Some(base),
                    updates: nth(self.next),
                },
            })
            .unwrap();
        self.next += 1;
        barrier
    }

    fn durable(&self) -> LocalJournalSeq {
        self.store.frontiers(A).unwrap().durable()
    }
}

/// The barriers `events` say are journal-durable, in order.
fn durable(events: &[StorageEvent]) -> Vec<BarrierId> {
    events
        .iter()
        .filter_map(|e| match e {
            StorageEvent::JournalDurable { barrier_id, .. } => Some(*barrier_id),
            _ => None,
        })
        .collect()
}

/// The barriers `events` say were definitely not committed, in order.
fn failed(events: &[StorageEvent]) -> Vec<BarrierId> {
    events
        .iter()
        .filter_map(|e| match e {
            StorageEvent::Failed {
                barrier_id,
                error: StorageError::DefinitelyNotCommitted,
            } => Some(*barrier_id),
            _ => None,
        })
        .collect()
}

/// What the projection holds once `count` transitions are journaled and
/// materialized with no appender at all.
fn reference_kv(count: u64) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let mut world = World::new();
    world.queue(count);
    world.store.flush().unwrap();
    kv_rows(&world.store)
}

/// Rows of the domain's projection, the boot records aside.
fn kv_rows(store: &JournaledStore<ModelJournal, ModelEngine>) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let kv = coord_store_api::registry::Collection::KvCurrentV1.id().0;
    store
        .projection(A)
        .unwrap()
        .durable_rows()
        .into_iter()
        .filter(|(c, _, _)| *c == kv)
        .collect()
}

/// End the boot where it stands and attach the projection again.
fn reboot(world: World) -> JournaledStore<ModelJournal, ModelEngine> {
    let (journal, mut engines) = world.store.into_parts();
    let (_, engine) = engines.pop().expect("one domain");
    let mut next = JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BootId([0x88; 16]),
        JournalLimits::default(),
    )
    .unwrap();
    next.attach(A, shard(), engine).unwrap();
    let frontiers = next.frontiers(A).unwrap();
    assert_eq!(frontiers.materialized(), frontiers.durable());
    assert_eq!(next.status(A), Some(DomainStatus::Ready));
    next
}

#[test]
fn an_append_lends_the_journal_and_reports_durability_only_once_taken_back() {
    let (mut world, handle) = World::lending();
    let before = world.durable();
    let syncs = world.store.journal_syncs();
    let barriers = world.queue(3);

    let report = world.store.append().unwrap();
    assert!(durable(&report.events).is_empty());
    assert_eq!(report.appends, 0, "nothing is counted before it is synced");
    assert!(world.store.appending());
    assert!(handle.waiting());
    assert_eq!(world.store.queued(A), 0, "the group is sealed and lent");
    assert_eq!(world.durable(), before);
    assert_eq!(world.store.journal_syncs(), syncs);

    // Not finished: nothing is taken back, and nothing else goes.
    let report = world.store.append().unwrap();
    assert!(report.events.is_empty());
    assert!(handle.waiting());

    assert!(handle.run());
    assert!(world.store.appending(), "synced, not yet taken back");
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), barriers);
    assert_eq!(report.appends, 1);
    assert_eq!(report.journaled, 3);
    assert!(!world.store.appending());
    assert_eq!(world.durable().get(), before.get() + 3);
    assert_eq!(world.store.cost().appends, 1);
    // What the journal is lent for is journaled, not materialized.
    assert_eq!(world.store.unmaterialized(A), 3);
}

#[test]
fn what_is_queued_behind_an_append_out_goes_as_the_next_group() {
    let (mut world, handle) = World::lending();
    let first = world.queue(2);
    world.store.append().unwrap();
    let second = world.queue(2);
    // Lent: the two behind it wait in the queue.
    world.store.append().unwrap();
    assert_eq!(world.store.queued(A), 2);

    assert!(handle.run());
    // Taken back, and the two behind it lent at once.
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), first);
    assert_eq!(world.store.queued(A), 0);
    assert!(handle.waiting());

    assert!(handle.run());
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), second);
    assert!(!world.store.appending());

    world.store.flush().unwrap();
    assert_eq!(kv_rows(&world.store), reference_kv(4));
}

#[test]
fn a_definite_failure_taken_back_fails_the_group_and_what_queued_behind_it() {
    let (mut world, handle) = World::lending();
    world
        .store
        .journal_mut()
        .unwrap()
        .script_append(AppendScript::DefinitelyNotCommitted);
    let before = world.durable();
    let first = world.queue(2);
    world.store.append().unwrap();
    let behind = world.queue(1);
    assert!(handle.run());

    let report = world.store.append().unwrap();
    let expected: Vec<BarrierId> = first.into_iter().chain(behind).collect();
    assert_eq!(failed(&report.events), expected);
    assert!(durable(&report.events).is_empty());
    assert_eq!(world.durable(), before);
    assert_eq!(world.store.queued(A), 0);
    assert!(!world.store.appending());
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
}

fn an_indeterminate_append(applied: bool) {
    let (mut world, handle) = World::lending();
    world
        .store
        .journal_mut()
        .unwrap()
        .script_append(AppendScript::Indeterminate { applied });
    let before = world.durable();
    let first = world.queue(2);
    world.store.append().unwrap();
    world.queue(1);
    assert!(handle.run());

    let report = world.store.append().unwrap();
    assert!(report.indeterminate);
    assert_eq!(world.store.status(A), Some(DomainStatus::JournalUncertain));
    // An uncertain stream seals nothing until it is reconciled.
    assert!(!world.store.appending());
    assert_eq!(world.store.queued(A), 1);

    let report = world.store.reconcile(A).unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    if applied {
        assert_eq!(durable(&report.events), first);
        assert_eq!(world.durable().get(), before.get() + 2);
        assert_eq!(world.store.queued(A), 1, "the one behind it is still owed");
    } else {
        assert_eq!(failed(&report.events).len(), 3);
        assert_eq!(world.durable(), before);
        assert_eq!(world.store.queued(A), 0);
    }
}

#[test]
fn an_indeterminate_append_that_happened_is_reconciled_as_durable() {
    an_indeterminate_append(true);
}

#[test]
fn an_indeterminate_append_that_did_not_happen_is_reconciled_as_absent() {
    an_indeterminate_append(false);
}

#[test]
fn a_reconcile_takes_an_append_out_back_first_and_settles_it() {
    let (mut world, handle) = World::lending();
    world
        .store
        .journal_mut()
        .unwrap()
        .script_append(AppendScript::Indeterminate { applied: true });
    let first = world.queue(2);
    world.store.append().unwrap();
    assert!(handle.run());
    // Synced, outcome in doubt, not taken back: the reconcile takes it
    // back and settles it in one call, so its caller sees it settled
    // rather than in doubt.
    let report = world.store.reconcile(A).unwrap();
    assert_eq!(durable(&report.events), first);
    assert!(!report.indeterminate);
    assert!(!world.store.appending());
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
}

#[test]
fn a_flush_takes_an_append_out_back_and_materializes_it() {
    let (mut world, handle) = World::lending();
    let first = world.queue(2);
    world.store.append().unwrap();
    let second = world.queue(1);
    assert!(handle.waiting());

    let report = world.store.flush().unwrap();
    let expected: Vec<BarrierId> = first.into_iter().chain(second).collect();
    assert_eq!(durable(&report.events), expected);
    assert_eq!(report.appends, 2);
    assert!(!world.store.appending());
    assert!(!handle.waiting(), "the store ran the append it waited for");
    assert_eq!(world.store.unmaterialized(A), 0);
    assert_eq!(kv_rows(&world.store), reference_kv(3));
}

#[test]
fn a_drain_takes_an_append_out_back() {
    let (mut world, _handle) = World::lending();
    let barriers = world.queue(2);
    world.store.append().unwrap();
    let report = world.store.drain().unwrap();
    assert_eq!(durable(&report.events), barriers);
    assert!(!world.store.appending());
    assert_eq!(world.store.unmaterialized(A), 2);
}

#[test]
fn a_boot_that_ends_with_an_append_lent_and_not_started_recovers_without_it() {
    let (mut world, handle) = World::lending();
    world.queue(2);
    world.store.flush().unwrap();
    world.queue(3);
    world.store.append().unwrap();
    assert!(handle.waiting());
    let next = reboot(world);
    assert!(!handle.waiting(), "the journal was given back unrun");
    assert_eq!(kv_rows(&next), reference_kv(2));
}

#[test]
fn a_boot_that_ends_after_an_append_and_before_its_outcome_is_taken_recovers_it() {
    let (mut world, handle) = World::lending();
    world.queue(2);
    world.store.flush().unwrap();
    world.queue(3);
    world.store.append().unwrap();
    assert!(handle.run());
    assert!(handle.finished());
    // The journal holds what the store never took back: the next boot
    // reads the stream's actual durable head and replays it.
    let next = reboot(world);
    assert_eq!(kv_rows(&next), reference_kv(5));
}

#[test]
fn a_checkpoint_publication_takes_an_append_out_back_first() {
    let (mut world, _handle) = World::lending();
    world.queue(3);
    world.store.flush().unwrap();
    let after = world.queue(1);
    world.store.append().unwrap();
    assert!(world.store.appending());
    let represented = world.store.frontiers(A).unwrap().materialized();
    let pointer = coord_journal_api::frontier::CheckpointPointerV1 {
        origin: world.store.origin(A).unwrap(),
        represented,
        format: 1,
        manifest_digest: coord_types::identity::Digest32([0x5a; 32]),
        checkpoint_id: coord_types::identity::Digest32([0x5b; 32]),
    };
    world.store.publish_checkpoint(A, &pointer).unwrap();
    assert!(!world.store.appending());
    // What the append taken back made durable is reported with the next
    // report, not dropped.
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), after);
}

#[test]
fn a_thread_appender_syncs_off_the_callers_thread_and_wakes_it() {
    let mut world = World::new();
    let (woke, wakes) = channel::<()>();
    let woke = std::sync::Mutex::new(woke);
    let appender = ThreadAppender::new(Arc::new(move || {
        let _ = woke.lock().unwrap().send(());
    }))
    .unwrap();
    world.store.pipeline_journal(Box::new(appender)).unwrap();

    let mut expected = Vec::new();
    let mut seen = Vec::new();
    for _ in 0..5 {
        expected.extend(world.queue(2));
        let report = world.store.append().unwrap();
        seen.extend(durable(&report.events));
        if world.store.appending() {
            wakes.recv_timeout(Duration::from_secs(10)).unwrap();
            let report = world.store.append().unwrap();
            seen.extend(durable(&report.events));
        }
    }
    let report = world.store.drain().unwrap();
    seen.extend(durable(&report.events));
    assert_eq!(seen, expected);
    world.store.flush().unwrap();
    assert_eq!(kv_rows(&world.store), reference_kv(10));
}

/// A fence takes an append out back before it tests what is queued: the
/// group lent is in no frontier until it is taken back, so a fence before
/// it would test an application queued behind it against the frontier
/// before the group, refuse it, and move the queued frontier back past
/// the group, which would leave the next application planned on a base
/// the projection refuses.
#[test]
fn a_fence_takes_an_append_out_back_before_it_tests_what_is_queued() {
    let (mut world, handle) = World::lending();
    let first = world.apply(1, 1);
    world.store.append().unwrap();
    assert!(handle.waiting());
    // The new leader's own application, planned behind the group out.
    let second = world.apply(2, 2);
    let base = world.store.application_base(A).unwrap();

    let refused = world
        .store
        .fence(
            A,
            Ballot {
                epoch: base.configuration,
                number: 2,
                leader: REPLICA,
            },
        )
        .unwrap();
    assert!(
        refused.is_empty(),
        "refused behind a group out: {refused:?}"
    );
    assert!(!world.store.appending(), "the fence took the append back");
    assert_eq!(world.store.queued(A), 1);
    assert_eq!(world.store.application_base(A).unwrap(), base);

    // What the fence took back goes out with the next report, and the
    // application behind it is lent with it.
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), vec![first]);
    assert!(handle.run());
    let report = world.store.append().unwrap();
    assert_eq!(durable(&report.events), vec![second]);
    assert_eq!(world.store.application_base(A).unwrap(), base);

    // Both extend the projection: nothing is quarantined.
    world.store.flush().unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    assert_eq!(world.store.unmaterialized(A), 0);
}
