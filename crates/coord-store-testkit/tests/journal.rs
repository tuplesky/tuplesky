//! The model journal enforces the contract; the initialization world is
//! clean for every schedule of the honest configuration and each deliberate
//! misbehavior is caught by the named violation.

use std::num::NonZeroU32;

use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::event::StorageEvent;
use coord_journal_api::engine::{JournalEngine, ReadBudget};
use coord_journal_api::failure::{JournalErrorClass, JournalFailure};
use coord_journal_api::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1};
use coord_journal_api::group::{GroupEntry, GroupLimits, GroupWrite};
use coord_journal_api::head::{HeadState, Reconciled, StreamHead};
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1, RecordBody,
    RecordDraft, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{
    ShardId, StorageStreamId, StreamAllocator, StreamHighWater, StreamKey,
};
use coord_store_api::registry::Collection;
use coord_store_testkit::initialization::{
    FailKind, InitMisbehavior, Violation, WorldConfig, explore,
};
use coord_store_testkit::journal::{AppendScript, JournalEvent, ModelJournal};
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};

fn key(domain: u8) -> StreamKey {
    StreamKey {
        cluster: ClusterId([1; 16]),
        domain: DomainId([domain; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn origin(domain: u8, stream: StorageStreamId) -> RecordOrigin {
    let k = key(domain);
    RecordOrigin {
        cluster: k.cluster,
        domain: k.domain,
        replica: ReplicaId([7; 16]),
        incarnation: k.incarnation,
        stream,
    }
}

fn barrier(n: u64) -> BarrierId {
    BarrierId {
        node_generation: ReplicaIncarnation::new(1).unwrap(),
        boot_id: BootId([0xb0; 16]),
        sequence: n,
    }
}

fn context() -> TransitionContext {
    let epoch = ConfigurationEpoch::new(1).unwrap();
    TransitionContext {
        boot: BootId([0xb0; 16]),
        configuration: epoch,
        ballot: Ballot {
            epoch,
            number: 1,
            leader: ReplicaId([7; 16]),
        },
    }
}

fn transition(tag: u8) -> RecordBody {
    RecordBody::ProtocolTransition {
        context: context(),
        updates: vec![StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: vec![tag],
            value: Some(vec![tag, tag]),
        }],
    }
}

fn genesis(origin: RecordOrigin) -> JournalRecordV1 {
    JournalRecordV1::seal(RecordDraft {
        origin,
        seq: LocalJournalSeq::new(1).unwrap(),
        predecessor: GENESIS_PREDECESSOR,
        body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
            format: JOURNAL_RECORD_FORMAT_V1,
        }),
    })
    .unwrap()
}

fn chain(
    origin: RecordOrigin,
    after: &JournalRecordV1,
    bodies: Vec<RecordBody>,
) -> Vec<JournalRecordV1> {
    let mut out = Vec::new();
    let mut seq = after.seq();
    let mut pred = after.digest();
    for body in bodies {
        seq = seq.checked_next().unwrap();
        let r = JournalRecordV1::seal(RecordDraft {
            origin,
            seq,
            predecessor: pred,
            body,
        })
        .unwrap();
        pred = r.digest();
        out.push(r);
    }
    out
}

fn group(entries: Vec<(BarrierId, StorageStreamId, Vec<JournalRecordV1>)>) -> GroupWrite {
    let mut g = GroupWrite::new(GroupLimits::DEFAULT);
    for (b, s, r) in entries {
        g.push(GroupEntry::new(b, s, r).unwrap()).unwrap();
    }
    g
}

/// A journal with the mappings of streams for domains 1 and 2 durable.
fn journal_with_streams() -> (
    ModelJournal,
    StreamAllocator,
    StorageStreamId,
    StorageStreamId,
) {
    let mut journal = ModelJournal::new();
    let mut allocator = StreamAllocator::new();
    let mut ids = Vec::new();
    for domain in [1u8, 2] {
        let mapping = allocator
            .allocate(key(domain), ShardId::new(0).unwrap())
            .unwrap();
        journal
            .persist_mapping(allocator.high_water(), &mapping)
            .unwrap();
        allocator.mapping_durable(mapping.stream).unwrap();
        ids.push(mapping.stream);
    }
    (journal, allocator, ids[0], ids[1])
}

#[test]
fn mapping_must_be_durable_before_use_and_ids_are_distinct() {
    let mut journal = ModelJournal::new();
    let mut allocator = StreamAllocator::new();
    let mapping = allocator
        .allocate(key(1), ShardId::new(0).unwrap())
        .unwrap();
    let g = group(vec![(
        barrier(1),
        mapping.stream,
        vec![genesis(origin(1, mapping.stream))],
    )]);
    let failure = journal.append_group(&g).unwrap_err();
    assert!(failure.is_definite());
    assert_eq!(failure.error().class, JournalErrorClass::GuardRejected);
    assert_eq!(journal.appends(), 0);

    let (journal, allocator, s1, s2) = journal_with_streams();
    assert_ne!(s1, s2);
    let (hw, mappings) = journal.mappings().unwrap();
    assert_eq!(hw, StreamHighWater::from_durable(2));
    assert_eq!(mappings.len(), 2);
    let restored = StreamAllocator::restore(hw, mappings).unwrap();
    assert_eq!(restored.lookup(&key(1)), Some(s1));
    assert_eq!(restored.lookup(&key(2)), Some(s2));
    assert_eq!(allocator.high_water(), hw);
}

#[test]
fn empty_stream_expects_genesis_under_its_mapped_identity() {
    // Stream 1 is mapped to domain 1: a validly sealed genesis claiming
    // domain 2 must not start the chain under the wrong identity, so the
    // expected origin comes from the durable mapping, not from the record.
    let (mut journal, _, s1, _) = journal_with_streams();
    let foreign = genesis(origin(2, s1));
    let err = journal
        .append_group(&group(vec![(barrier(1), s1, vec![foreign])]))
        .unwrap_err();
    assert!(err.is_definite());
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
    assert_eq!(journal.durable_head(s1).unwrap(), LocalJournalSeq::ZERO);
    assert_eq!(journal.appends(), 0);
    // The genesis sealed for the mapped identity is accepted.
    journal
        .append_group(&group(vec![(barrier(2), s1, vec![genesis(origin(1, s1))])]))
        .unwrap();
    assert_eq!(
        journal.durable_head(s1).unwrap(),
        LocalJournalSeq::new(1).unwrap()
    );
}

#[test]
fn retired_mapping_refuses_appends() {
    let (mut journal, mut allocator, s1, s2) = journal_with_streams();
    journal
        .append_group(&group(vec![(barrier(1), s2, vec![genesis(origin(2, s2))])]))
        .unwrap();
    // Retirement is persisted like any other mapping change; the identifier
    // stays reserved but the stream is closed to appends.
    let retired = allocator.retire(s2).unwrap();
    journal
        .persist_mapping(allocator.high_water(), &retired)
        .unwrap();
    let g2 = journal.records(s2)[0].clone();
    let more = chain(origin(2, s2), &g2, vec![transition(1)]);
    let err = journal
        .append_group(&group(vec![(barrier(2), s2, more)]))
        .unwrap_err();
    assert!(err.is_definite());
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
    assert_eq!(
        journal.durable_head(s2).unwrap(),
        LocalJournalSeq::new(1).unwrap()
    );
    assert_eq!(journal.appends(), 1);
    // The retired mapping is still reported, so restore keeps the
    // reservation, and the other stream is unaffected.
    let (hw, mappings) = journal.mappings().unwrap();
    assert!(mappings.iter().any(|m| m.stream == s2 && m.retired));
    assert_eq!(
        StreamAllocator::restore(hw, mappings)
            .unwrap()
            .lookup(&key(2)),
        Some(s2)
    );
    journal
        .append_group(&group(vec![(barrier(3), s1, vec![genesis(origin(1, s1))])]))
        .unwrap();
}

#[test]
fn multi_stream_group_completes_each_stream_exactly() {
    let (mut journal, _, s1, s2) = journal_with_streams();
    let g1 = genesis(origin(1, s1));
    let g2 = genesis(origin(2, s2));
    let more1 = chain(origin(1, s1), &g1, vec![transition(1), transition(2)]);
    let mut e1 = vec![g1];
    e1.extend(more1);
    let g = group(vec![(barrier(1), s1, e1), (barrier(2), s2, vec![g2])]);
    let receipt = journal.append_group(&g).unwrap();
    assert_eq!(receipt.completions.len(), 2);
    assert_eq!(
        receipt.completions[0].last,
        LocalJournalSeq::new(3).unwrap()
    );
    assert_eq!(
        receipt.completions[1].last,
        LocalJournalSeq::new(1).unwrap()
    );
    assert_eq!(
        receipt.completions[0].storage_event(),
        StorageEvent::JournalDurable {
            barrier_id: barrier(1),
            journal_seq: LocalJournalSeq::new(3).unwrap(),
        }
    );
    // The byte count is evidence only; the sequences are the caller's.
    assert!(receipt.written.0 > 0);
    assert_ne!(receipt.written.0, receipt.completions[0].last.get());
    assert_eq!(
        journal.durable_head(s1).unwrap(),
        LocalJournalSeq::new(3).unwrap()
    );
    assert_eq!(
        journal.durable_head(s2).unwrap(),
        LocalJournalSeq::new(1).unwrap()
    );
    assert!(matches!(
        journal.events(),
        [
            JournalEvent::MappingPersisted { .. },
            JournalEvent::MappingPersisted { .. },
            JournalEvent::Durable { last, .. },
            JournalEvent::Durable { .. }
        ] if last.get() == 3
    ));

    // Suffix reads are bounded and never report a decode problem as EOF.
    let page = journal
        .read_suffix(s1, LocalJournalSeq::ZERO, ReadBudget::new(2, u32::MAX))
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert!(!page.exhausted);
    let rest = journal
        .read_suffix(s1, page.records[1].seq(), ReadBudget::new(2, u32::MAX))
        .unwrap();
    assert_eq!(rest.records.len(), 1);
    assert!(rest.exhausted);
}

#[test]
fn stale_index_wrong_origin_and_broken_chain_are_rejected_before_append() {
    let (mut journal, _, s1, s2) = journal_with_streams();
    let g1 = genesis(origin(1, s1));
    journal
        .append_group(&group(vec![(barrier(1), s1, vec![g1.clone()])]))
        .unwrap();
    // Replaying the genesis at index one again: index mismatch.
    let err = journal
        .append_group(&group(vec![(barrier(2), s1, vec![g1.clone()])]))
        .unwrap_err();
    assert!(err.is_definite());
    // A record of domain 2 sealed for stream 1: origin mismatch.
    let foreign = chain(origin(2, s1), &g1, vec![transition(1)]);
    let err = journal
        .append_group(&group(vec![(barrier(3), s1, foreign)]))
        .unwrap_err();
    assert!(err.is_definite());
    // Predecessor not chained onto the durable head.
    let mut broken = chain(origin(1, s1), &g1, vec![transition(1)]);
    broken[0] = JournalRecordV1::seal(RecordDraft {
        origin: origin(1, s1),
        seq: broken[0].seq(),
        predecessor: Digest32([9; 32]),
        body: transition(1),
    })
    .unwrap();
    let err = journal
        .append_group(&group(vec![(barrier(4), s1, broken)]))
        .unwrap_err();
    assert!(err.is_definite());
    assert_eq!(
        journal.durable_head(s1).unwrap(),
        LocalJournalSeq::new(1).unwrap()
    );
    assert_eq!(journal.appends(), 1);
    // A group is all or nothing: stream 2's valid entry is not appended
    // beside stream 1's rejected one.
    let bad = chain(origin(1, s1), &g1, vec![transition(1)]);
    let mut bad = bad;
    bad[0] = JournalRecordV1::seal(RecordDraft {
        origin: origin(1, s1),
        seq: LocalJournalSeq::new(9).unwrap(),
        predecessor: g1.digest(),
        body: transition(1),
    })
    .unwrap();
    let err = journal
        .append_group(&group(vec![
            (barrier(5), s2, vec![genesis(origin(2, s2))]),
            (barrier(6), s1, bad),
        ]))
        .unwrap_err();
    assert!(err.is_definite());
    assert_eq!(journal.durable_head(s2).unwrap(), LocalJournalSeq::ZERO);
}

#[test]
fn definite_and_indeterminate_outcomes_drive_the_head() {
    let (mut journal, _, s1, _) = journal_with_streams();
    let o = origin(1, s1);
    let g1 = genesis(o);
    let mut head = StreamHead::open(s1, LocalJournalSeq::ZERO);
    head.reserve(barrier(1), NonZeroU32::new(1).unwrap())
        .unwrap();
    journal
        .append_group(&group(vec![(barrier(1), s1, vec![g1.clone()])]))
        .unwrap();
    head.complete_durable(barrier(1)).unwrap();

    // Definite: nothing appended, reservation released.
    journal.script_append(AppendScript::DefinitelyNotCommitted);
    let r2 = chain(o, &g1, vec![transition(2)]);
    head.reserve(barrier(2), NonZeroU32::new(1).unwrap())
        .unwrap();
    let err = journal
        .append_group(&group(vec![(barrier(2), s1, r2.clone())]))
        .unwrap_err();
    assert!(matches!(err, JournalFailure::Definite(_)));
    head.fail_definite(barrier(2)).unwrap();
    assert_eq!(
        journal.durable_head(s1).unwrap(),
        LocalJournalSeq::new(1).unwrap()
    );

    // Indeterminate, absent: reconciliation from the actual head retries.
    journal.script_append(AppendScript::Indeterminate { applied: false });
    head.reserve(barrier(3), NonZeroU32::new(1).unwrap())
        .unwrap();
    let err = journal
        .append_group(&group(vec![(barrier(3), s1, r2.clone())]))
        .unwrap_err();
    assert!(matches!(err, JournalFailure::Indeterminate(_)));
    head.fail_indeterminate(barrier(3)).unwrap();
    assert!(matches!(head.state(), HeadState::Uncertain(_)));
    let recovered = journal.durable_head(s1).unwrap();
    assert!(matches!(
        head.reconcile(recovered),
        Ok(Reconciled::Absent(_))
    ));

    // Indeterminate, present: the record is durable although the callback
    // was lost; reconciliation restores it without claiming it was
    // acknowledged.
    journal.script_append(AppendScript::Indeterminate { applied: true });
    head.reserve(barrier(4), NonZeroU32::new(1).unwrap())
        .unwrap();
    let err = journal
        .append_group(&group(vec![(barrier(4), s1, r2.clone())]))
        .unwrap_err();
    assert!(matches!(err, JournalFailure::Indeterminate(_)));
    head.fail_indeterminate(barrier(4)).unwrap();
    let recovered = journal.durable_head(s1).unwrap();
    assert_eq!(recovered, LocalJournalSeq::new(2).unwrap());
    assert!(matches!(
        head.reconcile(recovered),
        Ok(Reconciled::Present(_))
    ));
    assert_eq!(head.durable(), recovered);
    assert_eq!(journal.records(s1).len(), 2);
    assert!(
        journal
            .events()
            .iter()
            .any(|e| e == &JournalEvent::Failed { definite: false })
    );
}

#[test]
fn retirement_requires_a_durable_pointer_below_the_publication() {
    let (mut journal, _, s1, _) = journal_with_streams();
    let o = origin(1, s1);
    let g1 = genesis(o);
    let pointer = CheckpointPointerV1 {
        origin: o,
        represented: LocalJournalSeq::new(2).unwrap(),
        format: LOCAL_CHECKPOINT_FORMAT_V1,
        manifest_digest: Digest32([5; 32]),
        checkpoint_id: Digest32([6; 32]),
    };
    let records = chain(
        o,
        &g1,
        vec![
            transition(1),
            RecordBody::PublishLocalCheckpoint(pointer),
            transition(3),
        ],
    );
    let mut all = vec![g1];
    all.extend(records);
    // Retiring before the pointer is durable is refused.
    let err = journal.retire_prefix(s1, &pointer).unwrap_err();
    assert!(err.is_definite());
    journal
        .append_group(&group(vec![(barrier(1), s1, all)]))
        .unwrap();
    let other = CheckpointPointerV1 {
        manifest_digest: Digest32([0; 32]),
        ..pointer
    };
    assert!(journal.retire_prefix(s1, &other).unwrap_err().is_definite());
    journal.retire_prefix(s1, &pointer).unwrap();
    // The publication record and the suffix after it survive.
    let remaining: Vec<u64> = journal.records(s1).iter().map(|r| r.seq().get()).collect();
    assert_eq!(remaining, vec![3, 4]);
    assert_eq!(
        journal.durable_head(s1).unwrap(),
        LocalJournalSeq::new(4).unwrap()
    );
    // A required suffix starting inside the retired prefix is a gap, not
    // an empty page.
    let err = journal
        .read_suffix(s1, LocalJournalSeq::ZERO, ReadBudget::new(8, u32::MAX))
        .unwrap_err();
    assert_eq!(err.class, JournalErrorClass::Corrupt);
    let page = journal
        .read_suffix(s1, pointer.represented, ReadBudget::new(8, u32::MAX))
        .unwrap();
    assert_eq!(page.records.len(), 2);
}

#[test]
fn honest_world_is_clean_under_every_schedule_and_failure() {
    let fails = [
        None,
        Some((2, FailKind::Definite)),
        Some((2, FailKind::IndeterminatePresent)),
        Some((2, FailKind::IndeterminateAbsent)),
        Some((3, FailKind::Definite)),
        Some((4, FailKind::Definite)),
        Some((4, FailKind::IndeterminateAbsent)),
    ];
    for fail_append in fails {
        let explored = explore(WorldConfig {
            misbehavior: None,
            fail_append,
        })
        .unwrap_or_else(|v| panic!("{fail_append:?}: {v:?}"));
        assert!(explored.schedules >= 2, "{fail_append:?}: {explored:?}");
        assert!(explored.states > explored.schedules);
    }
}

type MisbehaviorCase = (
    InitMisbehavior,
    Option<(u64, FailKind)>,
    fn(&Violation) -> bool,
);

#[test]
fn each_misbehavior_is_caught_by_the_named_violation() {
    let cases: [MisbehaviorCase; 4] = [
        (InitMisbehavior::IndexBeforeDurable, None, |v| {
            matches!(v, Violation::VisibleBeforeDurable { .. })
        }),
        (InitMisbehavior::SplitInstall, None, |v| {
            matches!(v, Violation::HalfStateVisible { .. })
        }),
        (InitMisbehavior::PlaceholderVisible, None, |v| {
            matches!(v, Violation::HalfStateVisible { .. })
        }),
        (
            InitMisbehavior::GuardFromVolatile,
            Some((3, FailKind::Definite)),
            |v| matches!(v, Violation::VoteFromVolatileState { .. }),
        ),
    ];
    for (misbehavior, fail_append, expected) in cases {
        let result = explore(WorldConfig {
            misbehavior: Some(misbehavior),
            fail_append,
        });
        match result {
            Err(v) if expected(&v) => {}
            other => panic!("{misbehavior:?} went undetected: {other:?}"),
        }
    }
}
