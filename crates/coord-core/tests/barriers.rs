//! Durable-barrier gating, boot fencing, timer generations and sealed
//! capabilities.

use coord_core::capability::{
    EstablishError, EstablishedRecord, EstablishedResult, EstablishmentEvidence,
};
use coord_core::effect::{BarrierId, BootId, Effect, EffectContext, PeerId, TimerId};
use coord_core::event::{StorageError, StorageEvent};
use coord_core::machine::ClockSnapshot;
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend, ReleaseError, TimerTable};
use coord_core::ports::{ClockSource, EntropySource};
use coord_sim::ports::{CountingEntropy, ManualClock};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::*;

const BOOT_A: BootId = BootId([0xa; 16]);
const BOOT_B: BootId = BootId([0xb; 16]);

fn incarnation() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn ballot(epoch: u64, number: u64) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(epoch).unwrap(),
        number,
        leader: ReplicaId([1; 16]),
    }
}

fn context(boot: BootId, ballot: Ballot) -> EffectContext {
    EffectContext {
        domain: DomainId([2; 16]),
        replica_incarnation: incarnation(),
        boot_id: boot,
        configuration: ballot.epoch,
        ballot,
        required_journal_seq: LocalJournalSeq::new(5).unwrap(),
    }
}

fn send(boot: BootId, ballot: Ballot, requires: Vec<BarrierId>) -> PendingSend {
    PendingSend {
        context: context(boot, ballot),
        requires,
        to: PeerId {
            replica: ReplicaId([3; 16]),
            incarnation: incarnation(),
        },
        frame: vec![1, 2, 3],
    }
}

/// A completion whose reported sequence covers the cut `context` requires.
fn durable(barrier: BarrierId) -> StorageEvent {
    StorageEvent::JournalDurable {
        barrier_id: barrier,
        journal_seq: LocalJournalSeq::new(5 + barrier.sequence).unwrap(),
    }
}

#[test]
fn two_barrier_effect_cannot_release_after_one() {
    let mut alloc = BarrierAllocator::new(incarnation(), BOOT_A);
    let (b1, b2) = (alloc.allocate(), alloc.allocate());
    assert_ne!(b1, b2);
    let mut outbox = Outbox::new(BOOT_A);
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b1, b2]));
    assert!(outbox.observe(&durable(b1)));
    assert!(
        outbox.release(&ballot(1, 1)).is_empty(),
        "one of two barriers is not enough"
    );
    assert_eq!(outbox.pending().len(), 1);
    // Materialization is not journal durability.
    assert!(!outbox.observe(&StorageEvent::Materialized {
        barrier_id: b2,
        journal_seq: LocalJournalSeq::new(2).unwrap()
    }));
    assert!(outbox.release(&ballot(1, 1)).is_empty());
    assert!(outbox.observe(&durable(b2)));
    let released = outbox.release(&ballot(1, 1));
    assert_eq!(released.len(), 1);
    assert!(
        matches!(&released[0], Effect::SendWhenDurable { requires, .. } if requires == &vec![b1, b2])
    );
    assert!(outbox.pending().is_empty());
    // Releasing again sends nothing twice.
    assert!(outbox.release(&ballot(1, 1)).is_empty());
}

#[test]
fn release_requires_the_context_journal_sequence() {
    let mut alloc = BarrierAllocator::new(incarnation(), BOOT_A);
    let b1 = alloc.allocate();
    let mut outbox = Outbox::new(BOOT_A);
    // The context requires the journal durable through sequence 5.
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b1]));
    // The barrier completes, but the reported sequence is below the cut.
    assert!(outbox.observe(&StorageEvent::JournalDurable {
        barrier_id: b1,
        journal_seq: LocalJournalSeq::new(3).unwrap(),
    }));
    assert!(outbox.is_durable(&b1));
    assert!(
        outbox.release(&ballot(1, 1)).is_empty(),
        "durable barrier below the required cut must not release"
    );
    // A later completion of another barrier carries the cut forward.
    let b2 = alloc.allocate();
    assert!(outbox.observe(&StorageEvent::JournalDurable {
        barrier_id: b2,
        journal_seq: LocalJournalSeq::new(5).unwrap(),
    }));
    assert_eq!(outbox.durable_through().get(), 5);
    assert_eq!(outbox.release(&ballot(1, 1)).len(), 1);

    // An effect naming no barrier still waits for the cut it names.
    let mut outbox = Outbox::new(BOOT_A);
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![]));
    assert!(outbox.release(&ballot(1, 1)).is_empty());
    let b3 = alloc.allocate();
    assert!(outbox.observe(&StorageEvent::JournalDurable {
        barrier_id: b3,
        journal_seq: LocalJournalSeq::new(4).unwrap(),
    }));
    assert!(outbox.release(&ballot(1, 1)).is_empty());
    let b4 = alloc.allocate();
    assert!(outbox.observe(&StorageEvent::JournalDurable {
        barrier_id: b4,
        journal_seq: LocalJournalSeq::new(6).unwrap(),
    }));
    assert_eq!(outbox.release(&ballot(1, 1)).len(), 1);
    // A wrong-boot completion never moves the cut.
    let mut outbox = Outbox::new(BOOT_A);
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![]));
    let mut other = BarrierAllocator::new(incarnation(), BOOT_B);
    let foreign = other.allocate();
    assert!(!outbox.observe(&StorageEvent::JournalDurable {
        barrier_id: foreign,
        journal_seq: LocalJournalSeq::new(9).unwrap(),
    }));
    assert_eq!(outbox.durable_through(), LocalJournalSeq::ZERO);
    assert!(outbox.release(&ballot(1, 1)).is_empty());
}

#[test]
fn established_result_deserialization_revalidates() {
    let record = EstablishedRecord {
        command: CommandId(Digest32([1; 32])),
        epoch: ConfigurationEpoch::new(1).unwrap(),
        ballot: ballot(1, 3),
        position: ExecutionPosition::new(9).unwrap(),
        result_digest: Digest32([7; 32]),
        revision: None,
        fast_path: false,
    };
    let bytes = postcard::to_allocvec(&record).unwrap();
    let restored: EstablishedResult = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.record(), record);
    assert_eq!(postcard::to_allocvec(&restored).unwrap(), bytes);
    let mut bad = record.clone();
    bad.epoch = ConfigurationEpoch::new(2).unwrap();
    let bytes = postcard::to_allocvec(&bad).unwrap();
    assert!(postcard::from_bytes::<EstablishedResult>(&bytes).is_err());
    assert_eq!(
        EstablishedResult::restore(bad).unwrap_err(),
        EstablishError::EpochMismatch
    );
    let mut bad = record;
    bad.position = ExecutionPosition::ZERO;
    let bytes = postcard::to_allocvec(&bad).unwrap();
    assert!(postcard::from_bytes::<EstablishedResult>(&bytes).is_err());
}

#[test]
fn wrong_boot_completion_never_authorizes() {
    let mut old = BarrierAllocator::new(incarnation(), BOOT_A);
    let stale_barrier = old.allocate();
    let mut outbox = Outbox::new(BOOT_B);
    let mut alloc = BarrierAllocator::new(incarnation(), BOOT_B);
    let b = alloc.allocate();
    assert_eq!(
        b.sequence, stale_barrier.sequence,
        "same sequence number, different boot"
    );
    outbox.publish(send(BOOT_B, ballot(1, 1), vec![b]));
    // A completion carrying the old boot's barrier is bookkeeping only.
    assert!(!outbox.observe(&durable(stale_barrier)));
    assert_eq!(outbox.stale_completions(), 1);
    assert!(!outbox.is_durable(&b));
    assert!(outbox.release(&ballot(1, 1)).is_empty());
    // An effect produced by another boot is dropped at publication.
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![]));
    let dropped = outbox.take_dropped();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].1, ReleaseError::WrongBoot);
    // The current boot's completion works.
    assert!(outbox.observe(&durable(b)));
    assert_eq!(outbox.release(&ballot(1, 1)).len(), 1);
}

#[test]
fn duplicate_and_failed_completions() {
    let mut alloc = BarrierAllocator::new(incarnation(), BOOT_A);
    let (b1, b2) = (alloc.allocate(), alloc.allocate());
    let mut outbox = Outbox::new(BOOT_A);
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b1]));
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b2]));
    assert!(outbox.observe(&durable(b1)));
    assert!(
        !outbox.observe(&durable(b1)),
        "duplicate completion is idempotent"
    );
    assert!(outbox.observe(&StorageEvent::Failed {
        barrier_id: b2,
        error: StorageError::Indeterminate
    }));
    assert!(!outbox.observe(&StorageEvent::Failed {
        barrier_id: b2,
        error: StorageError::Indeterminate
    }));
    let released = outbox.release(&ballot(1, 1));
    assert_eq!(released.len(), 1, "only the durable effect releases");
    let dropped = outbox.take_dropped();
    assert_eq!(dropped.len(), 1);
    assert_eq!(
        dropped[0].1,
        ReleaseError::BarrierFailed(StorageError::Indeterminate)
    );
    // A later durable completion for a failed barrier does not resurrect it.
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b2]));
    outbox.observe(&durable(b2));
    assert!(outbox.release(&ballot(1, 1)).is_empty());
    assert_eq!(
        outbox.take_dropped()[0].1,
        ReleaseError::BarrierFailed(StorageError::Indeterminate)
    );
}

#[test]
fn obsolete_ballot_completion_updates_bookkeeping_but_never_authorizes() {
    let mut alloc = BarrierAllocator::new(incarnation(), BOOT_A);
    let b = alloc.allocate();
    let mut outbox = Outbox::new(BOOT_A);
    outbox.publish(send(BOOT_A, ballot(1, 1), vec![b]));
    // The promise moved to a higher ballot in the same boot before the
    // callback arrived (same-boot election, Section 4.8).
    assert!(outbox.observe(&durable(b)));
    assert!(outbox.is_durable(&b), "durability bookkeeping is kept");
    assert!(outbox.release(&ballot(1, 2)).is_empty());
    assert_eq!(outbox.take_dropped()[0].1, ReleaseError::ObsoleteBallot);
    // A newer epoch also obsoletes an older epoch's effect.
    outbox.publish(send(BOOT_A, ballot(1, 2), vec![b]));
    assert!(outbox.release(&ballot(2, 0)).is_empty());
    assert_eq!(outbox.take_dropped()[0].1, ReleaseError::ObsoleteBallot);
    // An effect at the current ballot releases.
    outbox.publish(send(BOOT_A, ballot(2, 0), vec![b]));
    assert_eq!(outbox.release(&ballot(2, 0)).len(), 1);
}

#[test]
fn timer_generations_ignore_obsolete_fires() {
    let mut timers = TimerTable::default();
    let Effect::ArmTimer { id: first, .. } = timers.arm(7, 10) else {
        panic!()
    };
    assert!(timers.accept(first));
    let Effect::ArmTimer { id: second, .. } = timers.arm(7, 10) else {
        panic!()
    };
    assert!(
        !timers.accept(first),
        "re-arming obsoletes the earlier generation"
    );
    assert!(timers.accept(second));
    timers.cancel(7);
    assert!(!timers.accept(second));
    assert!(
        !timers.accept(TimerId {
            name: 8,
            generation: 1
        }),
        "unknown timer"
    );
}

#[test]
fn established_result_requires_consistent_evidence() {
    let command = CommandId(Digest32([1; 32]));
    let other = CommandId(Digest32([2; 32]));
    let good = EstablishmentEvidence {
        command,
        epoch: ConfigurationEpoch::new(1).unwrap(),
        ballot: ballot(1, 3),
        position: ExecutionPosition::new(9).unwrap(),
        closed_predecessors: vec![other],
        result_digest: Digest32([7; 32]),
        revision: Some(KvRevision::new(4).unwrap()),
        fast_path: true,
    };
    let established = EstablishedResult::establish(good.clone()).unwrap();
    assert_eq!(established.command(), command);
    assert_eq!(established.position().get(), 9);
    assert!(established.fast_path());
    let mut bad = good.clone();
    bad.epoch = ConfigurationEpoch::new(2).unwrap();
    assert_eq!(
        EstablishedResult::establish(bad).unwrap_err(),
        EstablishError::EpochMismatch
    );
    let mut bad = good.clone();
    bad.closed_predecessors = vec![command];
    assert_eq!(
        EstablishedResult::establish(bad).unwrap_err(),
        EstablishError::InconsistentClosure
    );
    let mut bad = good.clone();
    bad.closed_predecessors = vec![other, other];
    assert_eq!(
        EstablishedResult::establish(bad).unwrap_err(),
        EstablishError::InconsistentClosure
    );
    let mut bad = good;
    bad.position = ExecutionPosition::ZERO;
    assert_eq!(
        EstablishedResult::establish(bad).unwrap_err(),
        EstablishError::ZeroPosition
    );
}

#[test]
fn test_ports_are_deterministic() {
    let mut e1 = CountingEntropy::default();
    let mut e2 = CountingEntropy::default();
    let (mut a, mut b) = ([0u8; 20], [0u8; 20]);
    e1.fill(&mut a);
    e2.fill(&mut b);
    assert_eq!(a, b);
    let mut c = [0u8; 20];
    e1.fill(&mut c);
    assert_ne!(a, c);

    let mut clock = ManualClock::new(1_000);
    assert_eq!(
        clock.now(),
        ClockSnapshot {
            monotonic_ticks: 0,
            wall_lower_ms: 1_000,
            wall_upper_ms: 1_000,
            healthy: true
        }
    );
    clock.advance(5, 50);
    assert_eq!(clock.now().monotonic_ticks, 5);
    assert!(clock.now().usable());
    clock.set_healthy(false);
    assert!(!clock.now().usable());
}
