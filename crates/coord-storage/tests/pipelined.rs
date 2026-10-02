//! A pipelined journal-first store (task-d52): the projection's commits
//! are lent to a materializer and taken back with their outcomes.
//!
//! The materializer here is the manual one, so each test stops the
//! pipeline exactly where it means to: a commit handed over and not
//! started, a commit returned and not taken back, records journaled
//! behind a commit that is out. Every boot that ends at one of those
//! points recovers, at the next attach, exactly the state the same
//! transitions leave in a store that commits on its own thread.

use coord_core::effect::{BarrierId, BootId, PersistBatch, StoreUpdate};
use coord_core::event::StorageEvent;
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::stream::ShardId;
use coord_storage::journaled::{
    DomainStatus, JournalLimits, JournaledStore, Submission, TransitionKind,
};
use coord_storage::{ManualHandle, ManualMaterializer};
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::{CommitScript, ModelEngine};
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

/// The `n`th transition every test journals: a write of key `n % 3`, so
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

    /// The same world, its projection commits lent to a manual
    /// materializer.
    fn pipelined() -> (Self, ManualHandle<ModelEngine>) {
        let mut world = World::new();
        let (materializer, handle) = ManualMaterializer::new();
        world.store.pipeline(Box::new(materializer)).unwrap();
        assert!(world.store.pipelined());
        (world, handle)
    }

    /// Queue the next `count` transitions and journal them.
    fn journal(&mut self, count: u64) -> Vec<BarrierId> {
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
        let report = self.store.append().unwrap();
        assert!(!report.indeterminate);
        barriers
    }

    fn engine(&self) -> &ModelEngine {
        self.store.projection(A).expect("the engine is not lent")
    }
}

/// What the projection holds once `count` transitions are journaled and
/// materialized on the store's own thread.
fn reference(count: u64) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let mut world = World::new();
    world.journal(count);
    world.store.flush().unwrap();
    world.engine().durable_rows()
}

/// The barriers `events` say materialized, in order.
fn materialized(events: &[StorageEvent]) -> Vec<BarrierId> {
    events
        .iter()
        .filter_map(|e| match e {
            StorageEvent::Materialized { barrier_id, .. } => Some(*barrier_id),
            _ => None,
        })
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

/// Rows of the domain's projection, the boot record aside: each boot
/// records itself, so two boots' projections differ there and nowhere
/// else.
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

fn reference_kv(count: u64) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let kv = coord_store_api::registry::Collection::KvCurrentV1.id().0;
    reference(count)
        .into_iter()
        .filter(|(c, _, _)| *c == kv)
        .collect()
}

#[test]
fn a_hand_off_lends_the_commit_and_reports_materialization_only_once_taken_back() {
    let (mut world, handle) = World::pipelined();
    let durable_before = world.store.frontiers(A).unwrap().materialized();
    let first = world.journal(3);

    let report = world.store.hand_off().unwrap();
    assert!(materialized(&report.events).is_empty());
    assert_eq!(report.commits, 0);
    assert!(world.store.lent(A));
    assert!(world.store.projection(A).is_none(), "the engine is lent");
    assert_eq!(handle.waiting(), 1);
    assert_eq!(world.store.unmaterialized(A), 3);
    assert_eq!(
        world.store.frontiers(A).unwrap().materialized(),
        durable_before
    );

    // Journaled behind the commit that is out: owed, and not lent until
    // that commit is back.
    let second = world.journal(2);
    world.store.hand_off().unwrap();
    assert_eq!(handle.waiting(), 1, "one commit out per domain");
    assert_eq!(world.store.unmaterialized(A), 5);

    // The commit runs. Its rows are readable through the gate at once,
    // and the store reports nothing until it takes the outcome back.
    assert!(handle.run());
    let stamped = world
        .store
        .reader(A)
        .unwrap()
        .snapshot()
        .expect("the gate moved with the commit")
        .meta()
        .stamp
        .journal_seq();
    assert!(stamped > durable_before);
    assert_eq!(
        world.store.frontiers(A).unwrap().materialized(),
        durable_before,
        "the store has not taken the outcome"
    );

    let report = world.store.hand_off().unwrap();
    assert_eq!(materialized(&report.events), first);
    assert_eq!(report.commits, 1);
    assert_eq!(world.store.frontiers(A).unwrap().materialized(), stamped);
    assert_eq!(handle.waiting(), 1, "the two behind it are lent next");

    let report = world.store.drain().unwrap();
    assert_eq!(materialized(&report.events), second);
    assert!(!world.store.lent(A));
    assert_eq!(world.store.unmaterialized(A), 0);
    let frontiers = world.store.frontiers(A).unwrap();
    assert_eq!(frontiers.materialized(), frontiers.durable());
    assert_eq!(world.engine().durable_rows(), reference(5));
}

#[test]
fn a_store_that_is_not_pipelined_materializes_a_hand_off_on_its_own_thread() {
    let mut world = World::new();
    let barriers = world.journal(3);
    let report = world.store.hand_off().unwrap();
    assert_eq!(materialized(&report.events), barriers);
    assert!(!world.store.lent(A));
    assert_eq!(world.store.unmaterialized(A), 0);
}

#[test]
fn a_commit_refused_gives_its_records_back_ahead_of_those_journaled_behind_it() {
    let (mut world, handle) = World::pipelined();
    world
        .engine()
        .script_commit(CommitScript::DefinitelyNotCommitted);
    let first = world.journal(3);
    world.store.hand_off().unwrap();
    let second = world.journal(2);
    assert!(handle.run());

    // Taken back refused: the records are owed again, in journal order,
    // ahead of the two behind them, and all five go out as the next
    // commit.
    let report = world.store.hand_off().unwrap();
    assert!(materialized(&report.events).is_empty());
    assert_eq!(
        world.store.status(A),
        Some(DomainStatus::MaterializationDeferred)
    );
    assert_eq!(world.store.unmaterialized(A), 5);
    assert_eq!(handle.waiting(), 1);

    assert!(handle.run());
    let report = world.store.hand_off().unwrap();
    let expected: Vec<BarrierId> = first.into_iter().chain(second).collect();
    assert_eq!(materialized(&report.events), expected);
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    assert_eq!(world.engine().durable_rows(), reference(5));
}

/// An indeterminate commit is settled from the projection's own stamp,
/// and only the records it took are settled with it: the ones journaled
/// behind it are still owed.
fn an_indeterminate_commit(applied: bool) {
    let (mut world, handle) = World::pipelined();
    world
        .engine()
        .script_commit(CommitScript::Indeterminate { applied });
    let first = world.journal(3);
    world.store.hand_off().unwrap();
    let second = world.journal(2);
    assert!(handle.run());

    let report = world.store.hand_off().unwrap();
    assert!(report.indeterminate);
    assert_eq!(
        world.store.status(A),
        Some(DomainStatus::MaterializationUncertain)
    );
    assert!(!world.store.lent(A), "a commit in doubt is not lent again");
    assert_eq!(handle.waiting(), 0);

    let report = world.store.reconcile(A).unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    if applied {
        // The three it took are settled; the two behind it are owed.
        assert_eq!(materialized(&report.events), first);
        assert_eq!(world.store.unmaterialized(A), 2);
        world.store.hand_off().unwrap();
        let report = world.store.drain().unwrap();
        assert_eq!(materialized(&report.events), second);
    } else {
        // Absent: all five are redone, in order, from the journal.
        let expected: Vec<BarrierId> = first.into_iter().chain(second).collect();
        assert_eq!(materialized(&report.events), expected);
    }
    assert_eq!(world.store.unmaterialized(A), 0);
    assert_eq!(world.engine().durable_rows(), reference(5));
}

#[test]
fn an_indeterminate_commit_that_happened_settles_only_the_records_it_took() {
    an_indeterminate_commit(true);
}

#[test]
fn an_indeterminate_commit_that_did_not_happen_is_redone_with_the_records_behind_it() {
    an_indeterminate_commit(false);
}

#[test]
fn a_boot_that_ends_with_a_commit_handed_over_and_not_started_recovers_the_journal() {
    let (mut world, handle) = World::pipelined();
    world.journal(3);
    world.store.hand_off().unwrap();
    assert_eq!(handle.waiting(), 1);
    let next = reboot(world);
    assert_eq!(handle.waiting(), 0, "the engine was given back unrun");
    assert_eq!(kv_rows(&next), reference_kv(3));
}

#[test]
fn a_boot_that_ends_after_a_commit_and_before_its_outcome_is_taken_recovers_the_journal() {
    let (mut world, handle) = World::pipelined();
    world.journal(3);
    world.store.hand_off().unwrap();
    assert!(handle.run());
    assert_eq!(handle.finished(), 1);
    // The projection is ahead of what the store took back. The next boot
    // replays from the projection's own stamp, so nothing is applied
    // twice and nothing is skipped.
    let next = reboot(world);
    assert_eq!(kv_rows(&next), reference_kv(3));
}

#[test]
fn a_boot_that_ends_with_records_journaled_behind_a_commit_out_recovers_them_all() {
    let (mut world, handle) = World::pipelined();
    world.journal(3);
    world.store.hand_off().unwrap();
    world.journal(2);
    assert!(handle.run());
    world.store.hand_off().unwrap();
    // The first commit was taken back and the second lent; it ends there,
    // unrun, with three more journaled behind it.
    world.journal(3);
    assert_eq!(handle.waiting(), 1);
    let next = reboot(world);
    assert_eq!(kv_rows(&next), reference_kv(8));
}

#[test]
fn a_checkpoint_publication_takes_a_commit_out_back_first() {
    let (mut world, handle) = World::pipelined();
    let barriers = world.journal(3);
    world.store.hand_off().unwrap();
    assert_eq!(handle.waiting(), 1);
    let represented = world.store.frontiers(A).unwrap().materialized();
    let pointer = coord_journal_api::frontier::CheckpointPointerV1 {
        origin: world.store.origin(A).unwrap(),
        represented,
        format: 1,
        manifest_digest: coord_types::identity::Digest32([0x5a; 32]),
        checkpoint_id: coord_types::identity::Digest32([0x5b; 32]),
    };
    world.store.publish_checkpoint(A, &pointer).unwrap();
    assert!(!world.store.lent(A));
    // What the commit taken back completed is reported with the next
    // report, not dropped.
    let report = world.store.hand_off().unwrap();
    assert_eq!(materialized(&report.events), barriers);
}
