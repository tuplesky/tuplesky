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
    DomainStatus, DurableCadence, JournalLimits, JournaledError, JournaledStore, ProjectionProfile,
    Submission, TransitionKind,
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
