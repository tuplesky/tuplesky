//! The replay-backed projection profile (task-j06, Section 17.3.4's
//! `journaled-replay`): projection commits are working commits, visible
//! at once and made durable on a cadence, before every checkpoint
//! publication and when the caller asks. A crash rolls the projection
//! back to its last durable commit, and the next attach replays the
//! journal above it before the domain serves.
//!
//! The model engine loses every working commit at a crash and persists
//! all of them with the next durable commit, as redb does with its
//! `Durability::None` commits. The same cases over redb itself, with a
//! crash at each of its writes and syncs, are in `journaled_real.rs`.

use std::num::NonZeroU32;

use coord_core::effect::{BarrierId, BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::stream::ShardId;
use coord_storage::ManualMaterializer;
use coord_storage::journaled::{
    CommitHold, DomainStatus, DurableCadence, JournalLimits, JournaledError, JournaledStore,
    ProjectionProfile, Submission, TransitionKind,
};
use coord_store_api::engine::LocalEngine;
use coord_store_testkit::journal::ModelJournal;
use coord_store_testkit::model::{CommitScript, ModelEngine, ModelReader, ModelWrite};
use coord_types::identity::Digest32;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const A: DomainId = DomainId([0xa1; 16]);
const BOOT: BootId = BootId([0x77; 16]);
const NEXT_BOOT: BootId = BootId([0x88; 16]);

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

fn cadence(commits: u32, records: u32) -> DurableCadence {
    DurableCadence {
        commits: NonZeroU32::new(commits).unwrap(),
        records: NonZeroU32::new(records).unwrap(),
    }
}

/// A cadence no test reaches: every durable commit is forced.
fn never() -> DurableCadence {
    cadence(u32::MAX, u32::MAX)
}

/// The `n`th transition every test queues: a write of key `n % 3`, so
/// later transitions overwrite earlier ones and order matters.
fn nth(n: u64) -> Vec<StoreUpdate> {
    vec![StoreUpdate {
        collection: coord_store_api::registry::Collection::KvCurrentV1.id(),
        key: vec![b'k', (n % 3) as u8],
        value: Some(n.to_be_bytes().to_vec()),
    }]
}

fn open(journal: ModelJournal, boot: BootId) -> JournaledStore<ModelJournal, ModelEngine> {
    JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        boot,
        JournalLimits::default(),
    )
    .unwrap()
}

struct World {
    store: JournaledStore<ModelJournal, ModelEngine>,
    barriers: BarrierAllocator,
    next: u64,
}

impl World {
    /// A store under `profile`, its one domain attached under it.
    fn new(profile: ProjectionProfile) -> Self {
        let mut store = open(ModelJournal::new(), BOOT);
        if let ProjectionProfile::Replay(cadence) = profile {
            store.replay_projection(cadence).unwrap();
        }
        store.attach(A, shard(), ModelEngine::new()).unwrap();
        World {
            store,
            barriers: BarrierAllocator::new(inc(), BOOT),
            next: 1,
        }
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

    /// Journal and materialize `count` transitions, one commit each.
    fn commit_each(&mut self, count: u64) {
        for _ in 0..count {
            self.queue(1);
            let report = self.store.flush().unwrap();
            assert_eq!(report.commits, 1);
        }
    }

    fn engine(&self) -> &ModelEngine {
        self.store.projection(A).expect("the engine is held")
    }

    fn materialized(&self) -> LocalJournalSeq {
        self.store.frontiers(A).unwrap().materialized()
    }

    fn projection_durable(&self) -> LocalJournalSeq {
        self.store.projection_durable(A).unwrap()
    }

    /// A pointer for a checkpoint that represents the projection as it
    /// stands.
    fn pointer(&self) -> CheckpointPointerV1 {
        CheckpointPointerV1 {
            origin: self.store.origin(A).unwrap(),
            represented: self.materialized(),
            format: 1,
            manifest_digest: Digest32([0x5a; 32]),
            checkpoint_id: Digest32([0x5b; 32]),
        }
    }
}

fn kv_only(rows: Vec<(u16, Vec<u8>, Vec<u8>)>) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let kv = coord_store_api::registry::Collection::KvCurrentV1.id().0;
    rows.into_iter().filter(|(c, _, _)| *c == kv).collect()
}

/// What the projection holds once `count` transitions are journaled and
/// materialized under the strict profile.
fn reference_kv(count: u64) -> Vec<(u16, Vec<u8>, Vec<u8>)> {
    let mut world = World::new(ProjectionProfile::Strict);
    world.queue(count);
    world.store.flush().unwrap();
    kv_only(world.engine().durable_rows())
}

/// End the boot with a crash: the projection loses every working commit.
/// Then open the journal again under `profile` and attach what the crash
/// left at `baseline`.
fn crash_and_attach(
    world: World,
    profile: ProjectionProfile,
    baseline: LocalJournalSeq,
) -> Result<JournaledStore<ModelJournal, ModelEngine>, JournaledError> {
    let (journal, mut engines) = world.store.into_parts();
    let (_, mut engine) = engines.pop().expect("one domain");
    engine.crash_and_reopen();
    let mut next = open(journal, NEXT_BOOT);
    if let ProjectionProfile::Replay(cadence) = profile {
        next.replay_projection(cadence).unwrap();
    }
    next.validate_projection(A, &engine, baseline).unwrap();
    next.attach_with_baseline(A, shard(), engine, baseline)?;
    Ok(next)
}

/// The recovered domain serves everything the journal holds.
fn assert_recovered(store: &JournaledStore<ModelJournal, ModelEngine>, count: u64) {
    let frontiers = store.frontiers(A).unwrap();
    assert_eq!(frontiers.materialized(), frontiers.durable());
    assert_eq!(store.status(A), Some(DomainStatus::Ready));
    let engine = store.projection(A).unwrap();
    assert_eq!(kv_only(engine.visible_rows()), reference_kv(count));
}

#[test]
fn the_strict_profile_is_the_default_and_commits_every_projection_durably() {
    let mut world = World::new(ProjectionProfile::Strict);
    assert_eq!(world.store.projection_profile(), ProjectionProfile::Strict);
    world.commit_each(4);
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
    // `request_durable_projection` and `sync_projections` change nothing.
    world.store.request_durable_projection();
    world.store.sync_projections().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
}

#[test]
fn working_commits_lost_by_a_crash_are_replayed_from_the_journal_before_the_domain_serves() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    let attached = world.materialized();
    world.commit_each(6);
    // Every commit is visible, none durable past the attach's.
    assert!(world.engine().working_commits() >= 6);
    assert_eq!(kv_only(world.engine().visible_rows()), reference_kv(6));
    assert_ne!(kv_only(world.engine().durable_rows()), reference_kv(6));
    assert!(world.projection_durable() < world.materialized());
    assert!(world.projection_durable() <= attached);

    let next = crash_and_attach(world, profile, LocalJournalSeq::ZERO).unwrap();
    assert_recovered(&next, 6);
}

#[test]
fn the_cadence_bounds_working_commits_by_count() {
    let mut world = World::new(ProjectionProfile::Replay(cadence(3, u32::MAX)));
    world.store.sync_projections().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
    world.commit_each(2);
    assert_eq!(world.engine().working_commits(), 2);
    assert!(world.projection_durable() < world.materialized());
    // The third commit since the last durable one is durable itself, and
    // persists the two before it.
    world.commit_each(1);
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
    assert_eq!(kv_only(world.engine().durable_rows()), reference_kv(3));
    world.commit_each(2);
    assert_eq!(world.engine().working_commits(), 2);
}

#[test]
fn the_cadence_bounds_working_commits_by_records() {
    let mut world = World::new(ProjectionProfile::Replay(cadence(u32::MAX, 4)));
    world.store.sync_projections().unwrap();
    world.queue(2);
    world.store.flush().unwrap();
    assert_eq!(world.engine().working_commits(), 1);
    // Two more records reach four: this commit is durable.
    world.queue(2);
    world.store.flush().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
    // One group of records past the bound commits durably on its own.
    world.queue(5);
    world.store.flush().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
}

#[test]
fn a_requested_durable_commit_is_the_next_one() {
    let mut world = World::new(ProjectionProfile::Replay(never()));
    world.commit_each(3);
    assert!(world.engine().working_commits() > 0);
    world.store.request_durable_projection();
    // A request waits for nothing: the commits so far are still working.
    assert!(world.engine().working_commits() > 0);
    world.commit_each(1);
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
    // The request is spent.
    world.commit_each(1);
    assert_eq!(world.engine().working_commits(), 1);
}

#[test]
fn sync_projections_makes_every_working_commit_durable_so_a_crash_loses_none() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(4);
    assert!(world.engine().working_commits() > 0);
    world.store.sync_projections().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
    assert_eq!(kv_only(world.engine().durable_rows()), reference_kv(4));
    let next = crash_and_attach(world, profile, LocalJournalSeq::ZERO).unwrap();
    assert_recovered(&next, 4);
}

#[test]
fn a_checkpoint_publication_makes_the_projection_durable_before_the_prefix_is_retired() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(5);
    assert!(world.projection_durable() < world.materialized());
    let pointer = world.pointer();
    world.store.publish_checkpoint(A, &pointer).unwrap();
    // `C <= M_durable`: the crash below rolls the projection back no
    // further than the checkpoint.
    assert!(world.projection_durable() >= pointer.represented);
    // Working commits after the publication are lost by the crash, and
    // replayed from above the retired prefix.
    world.commit_each(3);
    assert!(world.engine().working_commits() > 0);
    let next = crash_and_attach(world, profile, pointer.represented).unwrap();
    assert_recovered(&next, 8);
}

#[test]
fn a_projection_behind_the_published_baseline_is_refused_as_invalid() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(3);
    let pointer = world.pointer();
    world.store.publish_checkpoint(A, &pointer).unwrap();
    // An empty projection in place of the one the crash left: it would
    // have to replay through the retired prefix.
    let (journal, _) = world.store.into_parts();
    let mut next = open(journal, NEXT_BOOT);
    next.replay_projection(never()).unwrap();
    let empty = ModelEngine::new();
    assert!(matches!(
        next.validate_projection(A, &empty, pointer.represented),
        Err(JournaledError::ProjectionInvalid(_))
    ));
    let refused = next
        .attach_with_baseline(A, shard(), empty, pointer.represented)
        .unwrap_err();
    assert!(
        matches!(refused, JournaledError::ProjectionInvalid(_)),
        "{refused}"
    );
    assert!(next.frontiers(A).is_none(), "nothing was attached");
}

#[test]
fn a_projection_whose_stamp_is_not_the_journals_record_is_refused_as_invalid() {
    // Two journals of the same stream that part after the boot record:
    // the projection of one does not continue the other.
    let mut ours = World::new(ProjectionProfile::Strict);
    ours.commit_each(2);
    let mut theirs = World::new(ProjectionProfile::Strict);
    theirs.next = 100;
    theirs.commit_each(4);
    let (_, mut engines) = ours.store.into_parts();
    let (_, engine) = engines.pop().unwrap();
    let (journal, _) = theirs.store.into_parts();
    let mut next = open(journal, NEXT_BOOT);
    next.replay_projection(never()).unwrap();
    assert!(matches!(
        next.validate_projection(A, &engine, LocalJournalSeq::ZERO),
        Err(JournaledError::ProjectionInvalid(_))
    ));
    let refused = next.attach(A, shard(), engine).unwrap_err();
    assert!(
        matches!(refused, JournaledError::ProjectionInvalid(_)),
        "{refused}"
    );
}

#[test]
fn a_materializer_job_commits_working_and_the_cadence_counts_it() {
    let mut world = World::new(ProjectionProfile::Replay(cadence(2, u32::MAX)));
    world.store.sync_projections().unwrap();
    let (materializer, handle) = ManualMaterializer::new();
    world.store.pipeline(Box::new(materializer)).unwrap();
    world.queue(1);
    world.store.append().unwrap();
    world.store.hand_off().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(!world.store.lent(A));
    assert_eq!(world.engine().working_commits(), 1);
    // The second is durable on the cadence, off the store's thread too.
    world.queue(1);
    world.store.append().unwrap();
    world.store.hand_off().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(!world.store.lent(A));
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
}

/// The model engine without the working-state capability: its working
/// commits are durable ones as far as the store can know.
struct WithoutWorkingState(ModelEngine);

impl LocalEngine for WithoutWorkingState {
    type Reader = ModelReader;
    type Write<'a> = ModelWrite<'a>;

    fn reader(&self) -> ModelReader {
        self.0.reader()
    }

    fn begin_write(&mut self) -> Result<ModelWrite<'_>, coord_store_api::engine::EngineError> {
        self.0.begin_write()
    }
}

#[test]
fn the_replay_profile_is_refused_over_an_engine_without_working_state() {
    let mut store: JournaledStore<ModelJournal, WithoutWorkingState> = JournaledStore::open(
        ModelJournal::new(),
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap();
    assert_eq!(
        store.replay_projection(never()).unwrap_err(),
        JournaledError::WorkingStateUnsupported
    );
    assert_eq!(store.projection_profile(), ProjectionProfile::Strict);
    store
        .attach(A, shard(), WithoutWorkingState(ModelEngine::new()))
        .unwrap();
}

#[test]
fn a_projection_at_the_journals_head_is_validated_by_the_record_at_its_stamp() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(3);
    world.store.sync_projections().unwrap();
    let (journal, mut engines) = world.store.into_parts();
    let (_, engine) = engines.pop().unwrap();
    let next = open(journal, NEXT_BOOT);
    next.validate_projection(A, &engine, LocalJournalSeq::ZERO)
        .unwrap();
    // Another journal's projection at the same sequence is not this one's.
    let mut theirs = World::new(ProjectionProfile::Strict);
    theirs.next = 100;
    theirs.commit_each(3);
    let (_, mut engines) = theirs.store.into_parts();
    let (_, foreign) = engines.pop().unwrap();
    assert!(matches!(
        next.validate_projection(A, &foreign, LocalJournalSeq::ZERO),
        Err(JournaledError::ProjectionInvalid(_))
    ));
}

/// A projection commit whose outcome was uncertain and that reconciles as
/// applied may have been a working commit or a durable one: the
/// projection is made durable then, so the cadence's bound and the
/// durable frontier survive the uncertainty.
#[test]
fn a_reconciled_commit_leaves_the_projection_durable() {
    let mut world = World::new(ProjectionProfile::Replay(never()));
    world.commit_each(2);
    assert!(world.projection_durable() < world.materialized());
    world
        .engine()
        .script_commit(CommitScript::Indeterminate { applied: true });
    world.queue(1);
    world.store.append().unwrap();
    let report = world.store.materialize().unwrap();
    assert!(report.indeterminate);
    assert_eq!(
        world.store.status(A),
        Some(DomainStatus::MaterializationUncertain)
    );
    world.store.reconcile(A).unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    assert_eq!(world.projection_durable(), world.materialized());
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(kv_only(world.engine().durable_rows()), reference_kv(3));
}

/// task-j06 review: a domain that goes idle on working commits has no next
/// commit to take a request for a durable one. Once a request has met no
/// commit, `sync_idle_projections` makes them durable, so the time bound
/// holds on an idle domain too and a crash after it replays nothing; a
/// request a commit took is spent there, and leaves nothing to sync.
#[test]
fn an_idle_domain_is_synced_once_its_request_meets_no_commit() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(3);
    assert!(world.store.projection_volatile(A));
    // Nothing asked for: nothing synced.
    let working = world.engine().working_commits();
    world.store.sync_idle_projections().unwrap();
    assert_eq!(world.engine().working_commits(), working);
    // A request the next commit takes is spent there.
    world.store.request_durable_projection();
    world.commit_each(1);
    assert_eq!(world.engine().working_commits(), 0);
    world.commit_each(2);
    world.store.sync_idle_projections().unwrap();
    assert_eq!(world.engine().working_commits(), 2);
    // A request no commit takes: the domain is idle, and is synced.
    world.store.request_durable_projection();
    world.store.sync_idle_projections().unwrap();
    assert_eq!(world.engine().working_commits(), 0);
    assert!(!world.store.projection_volatile(A));
    assert_eq!(world.projection_durable(), world.materialized());
    assert_eq!(kv_only(world.engine().durable_rows()), reference_kv(6));
    let next = crash_and_attach(world, profile, LocalJournalSeq::ZERO).unwrap();
    assert_recovered(&next, 6);
}

/// The idle sync leaves a domain whose engine the materializer has: the
/// commit out there carries the request, and is made durable by it.
#[test]
fn an_idle_sync_leaves_a_lent_engine_to_the_commit_it_has_out() {
    let mut world = World::new(ProjectionProfile::Replay(never()));
    world.store.sync_projections().unwrap();
    let (materializer, handle) = ManualMaterializer::new();
    world.store.pipeline(Box::new(materializer)).unwrap();
    world.queue(1);
    world.store.append().unwrap();
    world.store.hand_off().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert_eq!(world.engine().working_commits(), 1);
    world.store.request_durable_projection();
    world.queue(1);
    world.store.append().unwrap();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A));
    world.store.sync_idle_projections().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(!world.store.lent(A));
    assert_eq!(world.engine().working_commits(), 0);
    assert_eq!(world.projection_durable(), world.materialized());
}

/// The strict profile has nothing volatile and nothing to sync.
#[test]
fn the_strict_profile_has_no_volatile_projection() {
    let mut world = World::new(ProjectionProfile::Strict);
    world.commit_each(2);
    assert!(!world.store.projection_volatile(A));
    world.store.request_durable_projection();
    world.store.sync_idle_projections().unwrap();
    assert_eq!(world.projection_durable(), world.materialized());
}

/// task-d55: the attach says what it replayed -- from where the crash
/// left the projection to the journal's head -- and a projection the
/// crash left at the head replays nothing.
#[test]
fn the_attach_reports_what_it_replayed() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(6);
    let left = world.projection_durable();
    let head = world.store.frontiers(A).unwrap().durable();
    assert!(left < head);
    let next = crash_and_attach(world, profile, LocalJournalSeq::ZERO).unwrap();
    let replayed = next.replayed(A).expect("attached");
    assert_eq!((replayed.from, replayed.through), (left, head));
    assert_eq!(replayed.records(), head.get() - left.get());

    let mut world = World::new(profile);
    world.commit_each(3);
    world.store.sync_projections().unwrap();
    let next = crash_and_attach(world, profile, LocalJournalSeq::ZERO).unwrap();
    assert_eq!(next.replayed(A).expect("attached").records(), 0);
    assert_eq!(next.replayed(DomainId([0xb2; 16])), None);
}

/// task-d55: a publication reports its steps apart, and under the strict
/// profile it spends nothing making the projection durable, which every
/// commit already was.
#[test]
fn a_strict_publication_spends_nothing_on_the_projections_sync() {
    let mut world = World::new(ProjectionProfile::Strict);
    world.commit_each(4);
    let pointer = world.pointer();
    let published = world.store.publish_checkpoint(A, &pointer).unwrap();
    assert_eq!(published.phases.sync, std::time::Duration::ZERO);
    assert!(published.retired);
}

/// An image produced off the domain thread (task-d51) represents what
/// was materialized when it was pinned, and the projection's own cadence
/// has usually made that durable by the time it is published. Then the
/// publication forces no sync, and a crash after it still rolls the
/// projection back no further than `C`.
#[test]
fn a_publication_already_durable_at_its_checkpoint_forces_no_sync() {
    let profile = ProjectionProfile::Replay(never());
    let mut world = World::new(profile);
    world.commit_each(3);
    world.store.sync_projections().unwrap();
    let pointer = world.pointer();
    assert_eq!(world.projection_durable(), pointer.represented);
    // Working commits after the pin, as the domain keeps committing
    // while the image is written.
    world.commit_each(2);
    assert!(world.projection_durable() < world.materialized());
    let published = world.store.publish_checkpoint(A, &pointer).unwrap();
    assert_eq!(published.phases.sync, std::time::Duration::ZERO);
    assert!(published.retired);
    assert!(world.projection_durable() >= pointer.represented);
    assert!(
        world.projection_durable() < world.materialized(),
        "the working commits after the pin stay working"
    );
    let next = crash_and_attach(world, profile, pointer.represented).unwrap();
    assert_recovered(&next, 5);
}

/// task-d66: a store given a thread clock times each of a publication's
/// steps by it, between a reading at the step's start and one at its
/// end; one without a clock, or whose clock has no reading, says so.
#[test]
fn a_publication_times_its_steps_by_the_store_s_thread_clock() {
    use coord_storage::ThreadTime;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    // Each reading is one more: a step timed by two readings took one.
    static READS: AtomicU64 = AtomicU64::new(0);
    fn counting() -> Option<ThreadTime> {
        let n = READS.fetch_add(1, Ordering::Relaxed);
        Some(ThreadTime {
            on_cpu: Duration::from_millis(n),
            run_queue: Duration::from_millis(2 * n),
        })
    }
    fn unreadable() -> Option<ThreadTime> {
        None
    }
    let one = ThreadTime {
        on_cpu: Duration::from_millis(1),
        run_queue: Duration::from_millis(2),
    };

    let mut world = World::new(ProjectionProfile::Strict);
    world.store.time_thread(counting);
    world.commit_each(4);
    let pointer = world.pointer();
    let published = world.store.publish_checkpoint(A, &pointer).unwrap();
    let thread = published.phases.thread.expect("a clock was given");
    assert_eq!(
        (thread.drain, thread.append, thread.retire),
        (one, one, one)
    );
    // The strict profile has no sync to time.
    assert_eq!(thread.sync, ThreadTime::default());

    let mut world = World::new(ProjectionProfile::Strict);
    world.commit_each(4);
    let pointer = world.pointer();
    let published = world.store.publish_checkpoint(A, &pointer).unwrap();
    assert_eq!(published.phases.thread, None);

    let mut world = World::new(ProjectionProfile::Strict);
    world.store.time_thread(unreadable);
    world.commit_each(4);
    let pointer = world.pointer();
    let published = world.store.publish_checkpoint(A, &pointer).unwrap();
    assert_eq!(published.phases.thread, None);
}

/// A commit hold of `records` records and `wait`.
fn hold(records: u32, wait: std::time::Duration) -> CommitHold {
    CommitHold {
        records: NonZeroU32::new(records).unwrap(),
        wait,
    }
}

/// A store under the replay profile with a manual materializer and the
/// commit hold `hold` (task-d86), its attach synced so the counts start
/// from nothing.
fn held(hold: CommitHold) -> (World, coord_storage::ManualHandle<ModelEngine>) {
    let mut world = World::new(ProjectionProfile::Replay(never()));
    world.store.sync_projections().unwrap();
    world.store.hold_projection_commits(hold).unwrap();
    let (materializer, handle) = ManualMaterializer::new();
    world.store.pipeline(Box::new(materializer)).unwrap();
    (world, handle)
}

/// Queue and journal `count` transitions, then hand off.
fn journal_and_hand_off(world: &mut World, count: u64) {
    world.queue(count);
    world.store.append().unwrap();
    world.store.hand_off().unwrap();
}

/// The hold keeps records back until there are as many as it names, and
/// then they go out as one commit (task-d86).
#[test]
fn a_held_commit_goes_out_once_the_hold_s_records_are_pending() {
    let (mut world, handle) = held(hold(3, std::time::Duration::from_secs(3600)));
    let before = world.materialized();
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A), "one record is held");
    assert_eq!(world.store.unmaterialized(A), 1);
    let due = world
        .store
        .projection_commit_due()
        .expect("something is held");
    assert!(due > std::time::Instant::now() + std::time::Duration::from_secs(3000));
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A), "two are held");
    assert!(world.store.commit_holds() >= 2);
    journal_and_hand_off(&mut world, 1);
    assert!(world.store.lent(A), "the third lends all three");
    assert_eq!(world.store.projection_commit_due(), None);
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(!world.store.lent(A));
    assert_eq!(world.store.unmaterialized(A), 0);
    assert_eq!(world.materialized().get(), before.get() + 3);
    assert_eq!(world.engine().working_commits(), 1, "one commit for three");
    assert_eq!(kv_only(world.engine().visible_rows()), reference_kv(3));
}

/// Records journaled while an earlier commit is out wait from when they
/// arrived, not from when that commit came back (task-d86): once it is
/// back, records that have already waited the hold's `wait` go out at
/// once.
#[test]
fn records_journaled_while_a_commit_is_out_wait_from_their_arrival() {
    let wait = std::time::Duration::from_millis(20);
    let (mut world, handle) = held(hold(100, wait));
    journal_and_hand_off(&mut world, 1);
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A), "the asked record is out");
    journal_and_hand_off(&mut world, 1);
    assert!(world.store.lent(A), "the second waits behind the first");
    let arrived = std::time::Instant::now();
    std::thread::sleep(wait * 2);
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(
        world.store.lent(A),
        "the second has waited longer than the hold and goes out as the first comes back"
    );
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert_eq!(world.store.unmaterialized(A), 0);
    // And the due time a caller sleeps to is the arrival's, not later.
    journal_and_hand_off(&mut world, 1);
    let due = world.store.projection_commit_due().expect("held");
    assert!(due <= std::time::Instant::now() + wait);
    assert!(due >= arrived + wait);
}

/// Records journaled behind a commit that is out are stamped as they
/// arrive, not by the next hand-off (task-d86): with nothing handed off
/// between their append and the commit's return, those that have
/// already waited the hold's `wait` go out as that commit comes back.
/// Stamped only by the hand-off, they would wait a whole `wait` again
/// from then.
#[test]
fn records_journaled_behind_a_lent_commit_are_stamped_without_a_hand_off() {
    let wait = std::time::Duration::from_millis(100);
    let (mut world, handle) = held(hold(100, wait));
    journal_and_hand_off(&mut world, 1);
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A), "the asked record is out");
    // Journaled behind it, and nothing handed off.
    world.queue(1);
    world.store.append().unwrap();
    assert!(world.store.lent(A));
    assert_eq!(world.store.unmaterialized(A), 2);
    std::thread::sleep(wait * 3);
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(
        world.store.lent(A),
        "the second has waited longer than the hold since it was journaled"
    );
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert_eq!(world.store.unmaterialized(A), 0);
}

/// Something that waits for the projection asks, and the next hand-off
/// lends what is held at once (task-d86).
#[test]
fn an_asked_commit_is_not_held() {
    let (mut world, handle) = held(hold(100, std::time::Duration::from_secs(3600)));
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A));
    world.store.request_projection_commit();
    let due = world.store.projection_commit_due().expect("held");
    assert!(due <= std::time::Instant::now(), "an ask is due now");
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A));
    assert!(handle.run());
    world.store.hand_off().unwrap();
    // The ask went with the commit it lent: the next record is held.
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A));
}

/// An ask made before the record it waits for is journaled stands until
/// a commit is lent, so that record is not held when it arrives
/// (task-d86): a group's watch events are asked for as it is handed off,
/// with its append possibly still out.
#[test]
fn an_ask_made_before_its_record_is_journaled_lends_the_record() {
    let (mut world, _handle) = held(hold(100, std::time::Duration::from_secs(3600)));
    world.store.request_projection_commit();
    journal_and_hand_off(&mut world, 1);
    assert!(world.store.lent(A));
}

/// Held records go out at the hold's time on a domain nothing else
/// arrives on (task-d86): the due time is what the caller's loop wakes
/// on.
#[test]
fn a_held_commit_goes_out_at_the_hold_s_time() {
    let wait = std::time::Duration::from_millis(30);
    let (mut world, handle) = held(hold(100, wait));
    let started = std::time::Instant::now();
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A));
    let due = world.store.projection_commit_due().expect("held");
    assert!(due >= started + wait);
    assert!(due <= std::time::Instant::now() + wait);
    while std::time::Instant::now() < due {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A));
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert_eq!(world.store.unmaterialized(A), 0);
}

/// A checkpoint queued for a projection not durable through it waits for
/// the next commit, so that commit is not held (task-d86).
#[test]
fn a_queued_checkpoint_asks_for_the_held_commit() {
    let (mut world, handle) = held(hold(100, std::time::Duration::from_secs(3600)));
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A));
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A), "held again");
    let pointer = world.pointer();
    world.store.queue_checkpoint(A, &pointer).unwrap();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A), "the queued pointer asked for it");
}

/// The hold is refused under the strict profile, where a commit is a
/// record's durability, and over a store that materializes on its
/// caller's thread it holds nothing (task-d86).
#[test]
fn the_hold_is_the_pipelined_replay_profile_s_alone() {
    let mut strict = World::new(ProjectionProfile::Strict);
    assert!(matches!(
        strict
            .store
            .hold_projection_commits(hold(8, std::time::Duration::from_secs(1))),
        Err(JournaledError::HoldUnderStrict)
    ));
    let mut unpipelined = World::new(ProjectionProfile::Replay(never()));
    unpipelined
        .store
        .hold_projection_commits(hold(8, std::time::Duration::from_secs(1)))
        .unwrap();
    unpipelined.queue(1);
    unpipelined.store.append().unwrap();
    let report = unpipelined.store.hand_off().unwrap();
    assert_eq!(report.commits, 1, "materialized on the caller's thread");
    assert_eq!(unpipelined.store.projection_commit_due(), None);
}

/// Records held when the node crashes are in the journal, and the next
/// attach replays them as it replays any working commit (task-d86).
#[test]
fn records_held_at_a_crash_are_replayed_from_the_journal() {
    let profile = ProjectionProfile::Replay(never());
    let (mut world, handle) = held(hold(100, std::time::Duration::from_secs(3600)));
    let durable = world.projection_durable();
    journal_and_hand_off(&mut world, 2);
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    // Two more journaled and held, never committed.
    journal_and_hand_off(&mut world, 2);
    assert!(!world.store.lent(A));
    assert_eq!(world.store.unmaterialized(A), 2);
    drop(handle);
    let next = crash_and_attach(world, profile, durable).unwrap();
    assert_recovered(&next, 4);
}

/// An ask reaches the records submitted when it is made, so a commit of
/// records held from before it does not spend it (task-d86): the record
/// it was made for goes out as soon as it is journaled, not after the
/// hold. Review's case on #234: one record held, a second queued and the
/// ask made, the hand-off lending the first.
#[test]
fn an_ask_is_not_spent_by_a_commit_of_records_held_before_it() {
    let (mut world, handle) = held(hold(100, std::time::Duration::from_secs(3600)));
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A), "one record is held");
    world.queue(1);
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A), "the held record goes out on the ask");
    world.store.append().unwrap();
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert!(
        world.store.lent(A),
        "the record the ask was made for goes out once journaled, not an hour later"
    );
    assert!(handle.run());
    world.store.hand_off().unwrap();
    assert_eq!(world.store.unmaterialized(A), 0);
    // That commit spent it: the next record is held.
    journal_and_hand_off(&mut world, 1);
    assert!(!world.store.lent(A));
}

/// A domain whose last commit is in doubt is not lent by a hand-off, so
/// the records journaled behind it are not due either until it is
/// reconciled (task-d86): a caller that hands off at the due time would
/// otherwise spin on a time in the past.
#[test]
fn a_commit_in_doubt_leaves_nothing_due_until_it_is_reconciled() {
    let (mut world, handle) = held(hold(100, std::time::Duration::from_millis(1)));
    world
        .engine()
        .script_commit(CommitScript::Indeterminate { applied: true });
    journal_and_hand_off(&mut world, 1);
    world.store.request_projection_commit();
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A));
    // Two journaled behind the commit while it is out.
    journal_and_hand_off(&mut world, 2);
    assert!(handle.run());
    let report = world.store.hand_off().unwrap();
    assert!(report.indeterminate);
    assert_eq!(
        world.store.status(A),
        Some(DomainStatus::MaterializationUncertain)
    );
    std::thread::sleep(std::time::Duration::from_millis(5));
    for pass in 0..3 {
        assert_eq!(
            world.store.projection_commit_due(),
            None,
            "pass {pass}: due while the hand-off lends nothing"
        );
        world.store.hand_off().unwrap();
        assert!(!world.store.lent(A));
    }
    world.store.reconcile(A).unwrap();
    assert_eq!(world.store.status(A), Some(DomainStatus::Ready));
    assert_eq!(world.store.unmaterialized(A), 2);
    let due = world
        .store
        .projection_commit_due()
        .expect("the two are held");
    assert!(due <= std::time::Instant::now());
    world.store.hand_off().unwrap();
    assert!(world.store.lent(A), "lent once reconciled");
}
