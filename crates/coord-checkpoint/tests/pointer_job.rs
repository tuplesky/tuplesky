//! The local checkpoint's pointer published on the journal worker
//! (task-d66).
//!
//! [`LocalBaseline::queue_local`] queues the pointer of an image that is
//! written; the store's next group carries it, and the retirement runs
//! after that append on the same thread, the appender's when the store
//! has one. [`LocalBaseline::take_local`] gives the publication once the
//! group is taken back. What that must keep of task-j04's order:
//!
//! * the pointer is durable before anything is retired, and the
//!   frontier's `C` advances only once the group is taken back;
//! * a crash with the pointer queued, or with its group lent and not
//!   run, recovers the previous baseline; one after the group ran
//!   recovers the new one; either loads, every time;
//! * a group that definitely failed leaves the previous publication
//!   standing, and one whose outcome is unknown publishes nothing the
//!   caller may assume;
//! * under the replay profile a group carries the pointer only once the
//!   projection is durable through what it represents.

use std::num::NonZeroU32;

use coord_checkpoint::local::{InstallLocalLimits, LocalLimits, install_local};
use coord_checkpoint::store::LocalCheckpointStore;
use coord_checkpoint::{BaselineError, LocalBaseline, QueuedPointer};
use coord_core::effect::{BootId, PersistBatch, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_journal_api::failure::JournalFailure;
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::record::RecordBody;
use coord_journal_api::stream::ShardId;
use coord_storage::journaled::{
    DurableCadence, JournalLimits, JournaledError, JournaledStore, Submission, TransitionKind,
};
use coord_storage::{JournaledDomain, ManualAppendHandle, ManualAppender};
use coord_store_api::registry::Collection;
use coord_store_testkit::journal::{AppendScript, JournalEvent, ModelJournal};
use coord_store_testkit::model::ModelEngine;
use coord_types::ids::*;

const CLUSTER: ClusterId = ClusterId([0x11; 16]);
const REPLICA: ReplicaId = ReplicaId([0x22; 16]);
const DOMAIN: DomainId = DomainId([0xa1; 16]);
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

type Domain = JournaledDomain<ModelJournal, ModelEngine>;
type Store = JournaledStore<ModelJournal, ModelEngine>;

fn store(journal: ModelJournal) -> Store {
    JournaledStore::open(
        journal,
        CLUSTER,
        REPLICA,
        inc(),
        BOOT,
        JournalLimits::default(),
    )
    .unwrap()
}

fn open(journal: ModelJournal, engine: ModelEngine) -> Domain {
    let mut store = store(journal);
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), engine)
        .unwrap();
    JournaledDomain::new(store, DOMAIN, ballot()).expect("attached")
}

/// A domain whose appends run only when the test says so.
fn lent(domain: &mut Domain) -> ManualAppendHandle<ModelJournal> {
    let (appender, appends) = ManualAppender::new();
    domain
        .store_mut()
        .pipeline_journal(Box::new(appender))
        .unwrap();
    appends
}

/// Queue one protocol transition.
fn submit(domain: &mut Domain, barriers: &mut BarrierAllocator, key: &[u8]) {
    domain
        .store_mut()
        .submit(Submission {
            domain: DOMAIN,
            ballot: ballot(),
            kind: TransitionKind::Protocol,
            batch: PersistBatch {
                barrier: barriers.allocate(),
                base: None,
                updates: vec![StoreUpdate {
                    collection: Collection::ProtocolV1.id(),
                    key: key.to_vec(),
                    value: Some(b"v".to_vec()),
                }],
            },
        })
        .unwrap();
}

/// One protocol transition, flushed and materialized.
fn protocol(domain: &mut Domain, barriers: &mut BarrierAllocator, key: &[u8]) {
    submit(domain, barriers, key);
    domain.store_mut().flush().unwrap();
}

/// An image of what is materialized now, written and its pointer queued.
fn queue_image(domain: &mut Domain, images: &LocalCheckpointStore) -> QueuedPointer {
    let pinned = domain.pin_local().unwrap().expect("something to represent");
    let written = pinned
        .write(images, &LocalLimits::default(), None)
        .expect("written");
    domain.queue_local(written).expect("queued")
}

/// The pointer recovery would select from `journal`, read by a store
/// that opens it as the next boot would.
fn selected(journal: ModelJournal) -> (Option<CheckpointPointerV1>, ModelJournal) {
    let probe = store(journal);
    let pointer = probe.recovery_baseline(DOMAIN).expect("read");
    let (journal, _) = probe.into_parts();
    (pointer, journal)
}

/// A domain recovered from `pointer`'s image over `journal`.
fn recover(
    journal: ModelJournal,
    images: &LocalCheckpointStore,
    pointer: &CheckpointPointerV1,
) -> Domain {
    let checkpoint = images.load(pointer).expect("the selected image loads");
    let mut engine = ModelEngine::new();
    install_local(
        &mut engine,
        &checkpoint,
        &pointer.origin,
        &InstallLocalLimits::default(),
    )
    .expect("installed");
    open(journal, engine)
}

/// A domain with a first publication, made on this thread, and one more
/// record since: what each case below publishes over.
fn published_once(
    images: &LocalCheckpointStore,
    barriers: &mut BarrierAllocator,
) -> (Domain, CheckpointPointerV1) {
    let mut domain = open(ModelJournal::new(), ModelEngine::new());
    protocol(&mut domain, barriers, b"promise");
    let first = domain
        .publish_local(images, &LocalLimits::default())
        .unwrap()
        .expect("published");
    protocol(&mut domain, barriers, b"vote-c1");
    (domain, first.pointer)
}

#[test]
fn a_queued_pointer_rides_the_next_group_and_its_retirement_follows_on_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let (mut domain, first) = published_once(&images, &mut barriers);
    let appends = lent(&mut domain);
    let queued = queue_image(&mut domain, &images);
    let represented = queued.written.pointer.represented;

    // Queued, nothing written: the pointer alone is what a lowering
    // would journal, and a second one is refused meanwhile.
    assert_eq!(domain.store().queued(DOMAIN), 1);
    assert!(domain.take_local(&queued).is_none());
    assert!(matches!(
        domain
            .store_mut()
            .publish_checkpoint(DOMAIN, &queued.written.pointer),
        Err(JournaledError::CheckpointQueued)
    ));

    // A transition queued after it: the group carries both, the pointer
    // first in the domain's entry, ahead of what was queued after it.
    submit(&mut domain, &mut barriers, b"vote-c2");
    assert_eq!(domain.store().queued(DOMAIN), 1, "the pointer rides along");
    let report = domain.store_mut().append().unwrap();
    assert_eq!(report.journaled, 0, "lent, not taken back");
    assert!(appends.waiting());
    assert_eq!(domain.store().queued(DOMAIN), 0);
    assert!(domain.take_local(&queued).is_none(), "in flight");

    // The worker appends and retires; `C` waits for the take-back.
    assert!(appends.run());
    assert!(domain.take_local(&queued).is_none(), "run, not taken back");
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        first.represented
    );
    let report = domain.store_mut().take_back_append().unwrap();
    assert_eq!(report.journaled, 2, "the transition and the pointer");
    assert_eq!(report.events.len(), 1, "only the transition has a barrier");
    let mut publication = domain
        .take_local(&queued)
        .expect("taken back")
        .expect("published");
    assert_eq!(publication.represented, represented);
    assert!(publication.retired);
    assert!(publication.phases.journal.queued >= publication.phases.journal.append);
    assert_eq!(publication.phases.journal.drain, std::time::Duration::ZERO);
    assert!(
        publication.phases.journal.thread.is_none(),
        "not this thread's"
    );
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        represented
    );
    assert!(domain.take_local(&queued).is_none(), "taken once");
    coord_checkpoint::reclaim_local(&images, &mut publication).expect("reclaimed");
    assert_eq!(publication.reclaimed, 1);

    // The journal's own order: the entry durable, then the retirement.
    let (journal, _) = domain.into_store().into_parts();
    let events = journal.events();
    let durable = events
        .iter()
        .rposition(|e| matches!(e, JournalEvent::Durable { .. }))
        .unwrap();
    let retired = events
        .iter()
        .rposition(
            |e| matches!(e, JournalEvent::Retired { through, .. } if *through == represented),
        )
        .expect("retired through the new pointer");
    assert!(
        durable < retired,
        "the pointer is durable before the retirement"
    );
    let stream = events
        .iter()
        .find_map(|e| match e {
            JournalEvent::Retired { stream, .. } => Some(*stream),
            _ => None,
        })
        .unwrap();
    let records = journal.records(stream);
    let [.., pointer, last] = records else {
        panic!("the entry's records");
    };
    assert!(
        matches!(pointer.body(), RecordBody::PublishLocalCheckpoint(p) if p.represented == represented),
        "the pointer goes ahead of the transition queued after it"
    );
    assert!(!matches!(
        last.body(),
        RecordBody::PublishLocalCheckpoint(_)
    ));
    let (pointer, _) = selected(journal);
    assert_eq!(pointer, Some(publication.pointer));
}

/// A domain whose queue fills every group still carries its pointer
/// (review of #164): the pointer goes right after the transitions queued
/// before it, and what is queued after it waits behind it. Behind the
/// whole queue, a domain kept busy enough never carried it, and retired
/// nothing.
#[test]
fn a_pointer_goes_behind_what_was_queued_before_it_and_ahead_of_what_follows() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let (mut domain, _) = published_once(&images, &mut barriers);
    let group = JournalLimits::default().group.max_records;
    // More queued than a group holds, before and after the pointer.
    let before = group + group / 2;
    for i in 0..before {
        submit(&mut domain, &mut barriers, format!("before-{i}").as_bytes());
    }
    let queued = queue_image(&mut domain, &images);
    let represented = queued.written.pointer.represented;
    let mut appended = 0usize;
    let mut groups = 0usize;
    let published = loop {
        // Every turn, more than a group's worth arrives behind it.
        for i in 0..group + 1 {
            submit(
                &mut domain,
                &mut barriers,
                format!("after-{groups}-{i}").as_bytes(),
            );
        }
        let report = domain.store_mut().append().unwrap();
        appended += report.journaled;
        groups += 1;
        if let Some(outcome) = domain.take_local(&queued) {
            break outcome.expect("published");
        }
        assert!(
            groups < 4,
            "the pointer was overtaken: {appended} records appended"
        );
    };
    assert_eq!(published.represented, represented);
    assert!(published.retired);
    assert_eq!(
        appended,
        before + 1 + (2 * group - before - 1),
        "the pointer took its place behind the transitions queued before it"
    );
    // In the journal: the pointer right after the last transition queued
    // before it, and the first one queued after it right after the pointer.
    let (journal, _) = domain.into_store().into_parts();
    let stream = journal
        .events()
        .iter()
        .find_map(|e| match e {
            JournalEvent::Retired { stream, .. } => Some(*stream),
            _ => None,
        })
        .unwrap();
    let records = journal.records(stream);
    let at = records
        .iter()
        .position(|r| {
            matches!(r.body(), RecordBody::PublishLocalCheckpoint(p) if p.represented == represented)
        })
        .expect("the pointer is in the journal");
    let key = |r: &coord_journal_api::record::JournalRecordV1| -> Vec<u8> {
        r.body()
            .updates()
            .first()
            .expect("a transition")
            .key
            .clone()
    };
    assert_eq!(
        key(&records[at - 1]),
        format!("before-{}", before - 1).into_bytes()
    );
    assert!(key(&records[at + 1]).starts_with(b"after-"));
}

#[test]
fn a_pointer_alone_goes_in_an_entry_of_its_own_on_this_thread_without_an_appender() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let (mut domain, _) = published_once(&images, &mut barriers);
    let queued = queue_image(&mut domain, &images);
    let report = domain.store_mut().append().unwrap();
    assert_eq!(report.journaled, 1);
    assert!(
        report.events.is_empty(),
        "the publication carries no barrier"
    );
    let publication = domain
        .take_local(&queued)
        .expect("written on this thread")
        .expect("published");
    assert!(publication.retired);
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        queued.written.pointer.represented
    );
    // The next publication goes as before.
    protocol(&mut domain, &mut barriers, b"vote-c2");
    let next = domain
        .publish_local(&images, &LocalLimits::default())
        .unwrap()
        .expect("published");
    assert!(next.represented > publication.represented);
}

/// task-j04's crash points, with the crash while the pointer is queued,
/// while its group is lent and not run, and once it has run and is not
/// taken back: the previous baseline, the previous one, then the new
/// one, and each loads and serves again.
#[test]
fn a_crash_with_the_pointer_queued_or_in_flight_recovers_a_baseline_that_loads() {
    #[derive(Clone, Copy, Debug)]
    enum Crash {
        Queued,
        Lent,
        Ran,
    }
    for crash in [Crash::Queued, Crash::Lent, Crash::Ran] {
        let dir = tempfile::tempdir().unwrap();
        let images = LocalCheckpointStore::open(dir.path()).unwrap();
        let mut barriers = BarrierAllocator::new(inc(), BOOT);
        let (mut domain, first) = published_once(&images, &mut barriers);
        let appends = lent(&mut domain);
        let queued = queue_image(&mut domain, &images);
        if !matches!(crash, Crash::Queued) {
            domain.store_mut().append().unwrap();
            assert!(appends.waiting());
        }
        if matches!(crash, Crash::Ran) {
            assert!(appends.run());
        }
        // The crash: nothing of the owner's survives but the journal and
        // the images.
        let (journal, _) = domain.into_store().into_parts();
        let (pointer, journal) = selected(journal);
        let expected = match crash {
            Crash::Queued | Crash::Lent => first,
            Crash::Ran => queued.written.pointer,
        };
        assert_eq!(pointer, Some(expected), "{crash:?}");
        let mut recovered = recover(journal, &images, &expected);
        assert!(
            recovered.store().frontiers(DOMAIN).unwrap().materialized() >= expected.represented,
            "{crash:?}: the image and the suffix replayed over it"
        );
        // And it publishes again, reclaiming whatever nothing selects.
        protocol(&mut recovered, &mut barriers, b"vote-c2");
        let next = recovered
            .publish_local(&images, &LocalLimits::default())
            .unwrap()
            .expect("published");
        assert_eq!(
            images.images().unwrap(),
            vec![next.pointer.checkpoint_id],
            "{crash:?}"
        );
    }
}

/// Write the pointer's group on the worker, with `script` its outcome,
/// and take it back.
fn write_scripted(
    domain: &mut Domain,
    appends: &ManualAppendHandle<ModelJournal>,
    script: AppendScript,
) {
    domain
        .store_mut()
        .journal_mut()
        .unwrap()
        .script_append(script);
    domain.store_mut().append().unwrap();
    assert!(appends.run());
    let _ = domain.store_mut().take_back_append();
}

#[test]
fn a_group_that_definitely_failed_leaves_the_previous_publication_standing() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let (mut domain, first) = published_once(&images, &mut barriers);
    let appends = lent(&mut domain);
    let queued = queue_image(&mut domain, &images);
    write_scripted(&mut domain, &appends, AppendScript::DefinitelyNotCommitted);
    let failed = domain.take_local(&queued).expect("taken back");
    assert!(
        matches!(
            failed,
            Err(BaselineError::Journal(JournaledError::Journal(
                JournalFailure::Definite(_)
            )))
        ),
        "{failed:?}"
    );
    assert!(!domain.store().checkpoint_queued(DOMAIN));
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        first.represented,
        "nothing was written, and the prior publication stands"
    );
    assert!(domain.store().journal().events().iter().all(|e| !matches!(
        e,
        JournalEvent::Retired { through, .. } if *through == queued.written.pointer.represented
    )));

    // The next publication is queued and goes.
    let queued = queue_image(&mut domain, &images);
    domain.store_mut().append().unwrap();
    assert!(appends.run());
    domain.store_mut().take_back_append().unwrap();
    let publication = domain.take_local(&queued).unwrap().expect("published");
    assert!(publication.retired);
    let (journal, _) = domain.into_store().into_parts();
    let (pointer, _) = selected(journal);
    assert_eq!(pointer, Some(publication.pointer));
}

/// An append whose outcome is unknown publishes nothing the caller may
/// assume and retires nothing. What recovery selects is the journal's
/// to say: here the group reached it, so the new baseline, whose image
/// nothing reclaimed.
#[test]
fn a_group_whose_outcome_is_unknown_retires_nothing_and_recovers_either_baseline() {
    for applied in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let images = LocalCheckpointStore::open(dir.path()).unwrap();
        let mut barriers = BarrierAllocator::new(inc(), BOOT);
        let (mut domain, first) = published_once(&images, &mut barriers);
        let appends = lent(&mut domain);
        let queued = queue_image(&mut domain, &images);
        write_scripted(
            &mut domain,
            &appends,
            AppendScript::Indeterminate { applied },
        );
        let failed = domain.take_local(&queued).expect("taken back");
        assert!(
            matches!(
                failed,
                Err(BaselineError::Journal(JournaledError::Journal(
                    JournalFailure::Indeterminate(_)
                )))
            ),
            "{failed:?}"
        );
        assert!(domain.store().journal().events().iter().all(|e| !matches!(
            e,
            JournalEvent::Retired { through, .. } if *through == queued.written.pointer.represented
        )));
        domain.store_mut().reconcile(DOMAIN).unwrap();
        assert_eq!(
            domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
            first.represented,
            "applied={applied}"
        );
        let (journal, _) = domain.into_store().into_parts();
        let (pointer, journal) = selected(journal);
        let expected = if applied {
            queued.written.pointer
        } else {
            first
        };
        assert_eq!(pointer, Some(expected), "applied={applied}");
        let recovered = recover(journal, &images, &expected);
        assert!(
            recovered.store().frontiers(DOMAIN).unwrap().materialized() >= expected.represented
        );
    }
}

/// Under the replay profile the pointer waits for the projection to be
/// durable through what it represents, and the next commit is asked to
/// be the durable one: nothing is forced on this thread.
#[test]
fn under_the_replay_profile_the_pointer_waits_for_the_next_durable_projection_commit() {
    let dir = tempfile::tempdir().unwrap();
    let images = LocalCheckpointStore::open(dir.path()).unwrap();
    let mut barriers = BarrierAllocator::new(inc(), BOOT);
    let mut store = store(ModelJournal::new());
    let never = DurableCadence {
        commits: NonZeroU32::MAX,
        records: NonZeroU32::MAX,
    };
    store.replay_projection(never).unwrap();
    store
        .attach(DOMAIN, ShardId::new(0).unwrap(), ModelEngine::new())
        .unwrap();
    let mut domain = JournaledDomain::new(store, DOMAIN, ballot()).expect("attached");
    protocol(&mut domain, &mut barriers, b"promise");
    let queued = queue_image(&mut domain, &images);
    let represented = queued.written.pointer.represented;
    assert!(domain.store().projection_durable(DOMAIN).unwrap() < represented);

    // Not durable yet: no group carries it.
    assert_eq!(domain.store().queued(DOMAIN), 0);
    let report = domain.store_mut().append().unwrap();
    assert_eq!(report.journaled, 0);
    assert!(domain.take_local(&queued).is_none());

    // The next commit is the durable one, and the group after it
    // carries the pointer.
    protocol(&mut domain, &mut barriers, b"vote-c1");
    assert!(domain.store().projection_durable(DOMAIN).unwrap() >= represented);
    assert!(
        domain.take_local(&queued).is_none(),
        "that group went first"
    );
    assert_eq!(domain.store().queued(DOMAIN), 1);
    let report = domain.store_mut().append().unwrap();
    assert_eq!(report.journaled, 1);
    let publication = domain.take_local(&queued).unwrap().expect("published");
    assert_eq!(publication.represented, represented);
    assert_eq!(publication.phases.journal.sync, std::time::Duration::ZERO);
    assert_eq!(
        domain.store().frontiers(DOMAIN).unwrap().checkpoint(),
        represented
    );
}
