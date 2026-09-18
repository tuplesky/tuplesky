//! task-s04 acceptance, faulted half: a history produced under real
//! storage faults is judged on its own, against its own acknowledged
//! outcomes, and never against another engine's failure-free history.
//!
//! The redb reference runs over the fault-injecting backend of task-09
//! (distinct volatile and durable images, scripted crash points, torn and
//! reordered unsynced tails). Every invocation whose application returned
//! must, after the crash image is reopened, still resolve to exactly the
//! same position, revision and result digest. Work that never returned may
//! be present or absent: it is not required to match, and comparing two
//! faulted histories with each other is not part of the acceptance.

use std::collections::BTreeMap;

use coord_redb_faultkit::{FaultBackend, FaultPlan, Tail};
use coord_storage::GroupLimits;
use coord_storage_redb::RedbEngine;
use coord_store_bench::driver::{Applied, Domain};
use coord_store_bench::workload::{Op, OpKind, WorkloadSpec, generate};
use coord_types::identity::Digest32;
use coord_types::ids::KvRevision;

const CACHE: usize = 4 << 20;

type Acknowledged = BTreeMap<u64, (Option<KvRevision>, Digest32)>;

fn spec() -> WorkloadSpec {
    let mut spec = WorkloadSpec::smoke();
    spec.prefill_ops = 24;
    spec.warmup_ops = 0;
    spec.measured_ops = 24;
    // A resolved retention floor depends on the revision the operation runs
    // at, so a compaction is not a stable invocation to replay after a
    // crash; the acknowledged set below excludes it deliberately.
    spec.compact_every = 0;
    spec
}

fn replayable(op: &Op) -> bool {
    !matches!(op.kind(), OpKind::Compact)
}

/// Drive the workload until the scripted crash, returning the outcomes the
/// caller actually learned and the surviving durable image.
fn drive_to_crash(crash_after: u64, tail: Tail) -> (Acknowledged, Vec<u8>) {
    let workload = generate(&spec());
    let (backend, shared) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    let mut domain = Domain::bootstrap(engine, GroupLimits::default()).unwrap();
    let mut acknowledged = Acknowledged::new();
    for op in &workload.prefill {
        if let Ok(Applied::Executed { revision, result }) = domain.apply(op)
            && replayable(op)
        {
            acknowledged.insert(op.sequence(), (revision, result));
        }
    }
    let setup = shared.ops();
    shared.set_plan(FaultPlan {
        crash_after: Some(setup + crash_after),
        tail,
        ..FaultPlan::default()
    });
    for op in &workload.measured {
        match domain.apply(op) {
            Ok(Applied::Executed { revision, result }) if replayable(op) => {
                acknowledged.insert(op.sequence(), (revision, result));
            }
            Ok(_) => {}
            // The engine froze: this invocation and everything after it
            // never returned, so nothing about them is promised.
            Err(_) => break,
        }
        if shared.is_frozen() {
            break;
        }
    }
    let image = shared.crash_image(tail);
    drop(domain);
    (acknowledged, image)
}

#[test]
fn every_acknowledged_outcome_survives_a_faulted_history() {
    let workload = generate(&spec());
    let by_sequence: BTreeMap<u64, Op> = workload
        .prefill
        .iter()
        .chain(workload.measured.iter())
        .filter(|op| replayable(op))
        .map(|op| (op.sequence(), op.clone()))
        .collect();
    let mut crashed = 0;
    for crash_after in [3u64, 9, 17, 31] {
        for tail in [Tail::None, Tail::All, Tail::Seeded(crash_after)] {
            let (acknowledged, image) = drive_to_crash(crash_after, tail);
            assert!(
                !acknowledged.is_empty(),
                "the prefix before the crash must acknowledge something"
            );
            crashed += 1;
            if crash_after == 3 {
                assert!(
                    acknowledged.len() < by_sequence.len(),
                    "an early crash must leave some work unacknowledged"
                );
            }
            let (backend, _) = FaultBackend::new(image, FaultPlan::default());
            let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
            let mut recovered = Domain::attach(engine, GroupLimits::default()).unwrap();
            for (sequence, promised) in &acknowledged {
                let op = &by_sequence[sequence];
                match recovered.apply(op).unwrap() {
                    Applied::Executed { revision, result } => assert_eq!(
                        (revision, result),
                        *promised,
                        "crash_after={crash_after} tail={tail:?} sequence={sequence}: an \
                         acknowledged outcome was not reproduced after recovery"
                    ),
                    other => panic!("sequence {sequence} resolved to {other:?}"),
                }
            }
            // The recovered replica is usable; unacknowledged work may be
            // present or absent and is not compared with anything.
            let observable = recovered.observable().unwrap();
            assert!(observable.session_rows > 0, "the session survived");
            assert!(observable.executed_rows >= acknowledged.len() as u64);
        }
    }
    assert_eq!(crashed, 12);
}

#[test]
fn a_faulted_history_is_not_required_to_match_a_failure_free_one() {
    // The failure-free history of the same workload on the same engine.
    let workload = generate(&spec());
    let (backend, _) = FaultBackend::new(Vec::new(), FaultPlan::default());
    let engine = RedbEngine::create_on_backend(backend, CACHE).unwrap();
    let mut domain = Domain::bootstrap(engine, GroupLimits::default()).unwrap();
    for op in workload.prefill.iter().chain(workload.measured.iter()) {
        domain.apply(op).unwrap();
    }
    let clean = domain.observable().unwrap();
    drop(domain);

    // A history crashed in the middle of the measured phase. Whether its
    // state equals the clean one depends entirely on which unacknowledged
    // work survived, so the harness never asserts equality here; what it
    // does assert is that the surviving state is coherent and never ahead
    // of the clean run.
    let (acknowledged, image) = drive_to_crash(9, Tail::Seeded(5));
    let (backend, _) = FaultBackend::new(image, FaultPlan::default());
    let engine = RedbEngine::from_backend(backend, CACHE).unwrap();
    let recovered = Domain::attach(engine, GroupLimits::default()).unwrap();
    let faulted = recovered.observable().unwrap();
    assert!(
        faulted.kv_revision <= clean.kv_revision,
        "a crashed replica cannot be ahead of the complete history"
    );
    assert!(
        faulted.executed_rows >= acknowledged.len() as u64,
        "every acknowledged command is still executed"
    );
}
