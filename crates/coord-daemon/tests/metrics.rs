//! task-61 acceptance: a scripted tail spike is attributed to the stage
//! that caused it; an unavailable metric is never zero; sync,
//! commit-return and backpressure stay distinct; labels are bounded by
//! construction and a rendered snapshot carries nothing secret; and
//! reading diagnostics cannot block anything.

use std::time::Duration;

use coord_daemon::metrics::{
    Durability, Frontiers, Headroom, Lane, LaneReading, Latency, MAX_REPORTED_SHARDS, Measure,
    MetricsSnapshot, Recorder, ShardIndex, ShardReading, Stage, StageReading, Unavailable,
};
use coord_daemon::role::RoleSet;

fn roles(spec: &str) -> RoleSet {
    RoleSet::parse(spec).expect("a usable role set")
}

fn voter() -> RoleSet {
    roles("voter-frontend-observer")
}

/// The question an incident asks is *which stage*, and separate
/// per-stage accounting is what answers it.
#[test]
fn a_scripted_tail_spike_is_attributed_to_the_stage_that_caused_it() {
    let recorder = Recorder::new();
    let roles = voter();

    // A workload where every stage is exercised and exactly one of them
    // is slow. A single end-to-end latency would show "things got
    // slower" and say nothing about where.
    for _ in 0..100 {
        for stage in [
            Stage::Admission,
            Stage::ClientTransit,
            Stage::FanOut,
            Stage::Journal,
            Stage::Materialization,
        ] {
            recorder.entered(stage);
            recorder.completed(stage, Duration::from_micros(200));
        }
    }
    // The spike: the journal, once, badly.
    recorder.entered(Stage::Journal);
    recorder.completed(Stage::Journal, Duration::from_millis(900));

    let stages = recorder.snapshot_stages(&roles);
    let peak = |stage: Stage| {
        let reading = stages.iter().find(|r| r.stage == stage).expect("a reading");
        *reading
            .metrics
            .observed()
            .expect("this role has the stage")
            .latency
            .peak()
            .observed()
            .expect("samples")
    };

    assert!(
        peak(Stage::Journal) >= Duration::from_millis(900),
        "the spike was not attributed to the journal"
    );
    for quiet in [
        Stage::Admission,
        Stage::ClientTransit,
        Stage::FanOut,
        Stage::Materialization,
    ] {
        assert!(
            peak(quiet) < Duration::from_millis(10),
            "{} absorbed a spike that was the journal's",
            quiet.name()
        );
    }

    // And the mean does not hide it: the maximum is reported alongside,
    // because an average over a hundred fast operations buries the one
    // that mattered.
    let journal = stages
        .iter()
        .find(|r| r.stage == Stage::Journal)
        .and_then(|r| r.metrics.observed())
        .expect("a journal reading");
    let mean = *journal.latency.measure().observed().expect("samples");
    assert!(
        mean < peak(Stage::Journal) / 4,
        "the mean and the peak are indistinguishable, so the tail is hidden"
    );
}

/// Absence is a fact, and the reason distinguishes cases an operator
/// would respond to differently. None of them is zero.
#[test]
fn an_unavailable_metric_is_never_reported_as_zero() {
    // A stage nothing has passed through: the counts are honestly zero,
    // but the latency has no samples and says so. A zero latency would
    // read as "instantaneous", which is the opposite of "untested".
    let empty = Latency::default();
    assert_eq!(
        empty.measure(),
        Measure::Unavailable(Unavailable::NoSamples)
    );
    assert_eq!(empty.peak(), Measure::Unavailable(Unavailable::NoSamples));
    assert!(!empty.measure().is_observed());

    // A role that does not have the stage at all. A frontend-only node
    // casts no vote, so its consensus stages are not slow, missing or
    // healthy -- they do not exist.
    let recorder = Recorder::new();
    let frontend = roles("frontend");
    let stages = recorder.snapshot_stages(&frontend);
    for consensus in [
        Stage::FanOut,
        Stage::DependencyClosure,
        Stage::EvidenceLearning,
        Stage::Recovery,
    ] {
        let reading = stages
            .iter()
            .find(|r| r.stage == consensus)
            .expect("every stage is reported");
        assert_eq!(
            reading.metrics.why(),
            Some(Unavailable::NotThisRole),
            "{} was reported for a node that does not have it",
            consensus.name()
        );
    }
    // And the stages it does have are present, with honest zeroes for
    // the counts: "nothing has happened here" is a different statement
    // from "this does not exist here", and both are said.
    let admission = stages
        .iter()
        .find(|r| r.stage == Stage::Admission)
        .and_then(|r| r.metrics.observed())
        .expect("a frontend admits");
    assert_eq!(admission.entered, 0);
    assert_eq!(
        admission.latency.measure(),
        Measure::Unavailable(Unavailable::NoSamples)
    );

    // An unconfigured bound has no headroom. Reporting zero would read
    // as "full", which is exactly backwards.
    let unbounded = Headroom { used: 7, bound: 0 };
    assert_eq!(
        unbounded.remaining(),
        Measure::Unavailable(Unavailable::NoBound)
    );
    assert_eq!(
        unbounded.pressure_permille(),
        Measure::Unavailable(Unavailable::NoBound)
    );
    let bounded = Headroom {
        used: 250,
        bound: 1000,
    };
    assert_eq!(bounded.remaining(), Measure::Observed(750));
    assert_eq!(bounded.pressure_permille(), Measure::Observed(250));
}

/// A sync, the whole operation and backpressure are three answers to
/// three different questions.
#[test]
fn sync_commit_return_and_backpressure_stay_distinct() {
    let mut durability = Durability::default();
    // A device that is fine, a queue that is not: the sync is quick and
    // the caller still waits. A combined "write latency" would blame
    // the disk.
    for _ in 0..10 {
        durability.sync.record(Duration::from_micros(500));
        durability.commit_return.record(Duration::from_millis(40));
    }
    durability.backpressure.record(Duration::from_millis(5));

    let sync = *durability.sync.measure().observed().expect("samples");
    let whole = *durability
        .commit_return
        .measure()
        .observed()
        .expect("samples");
    assert!(
        whole > sync * 10,
        "commit-return collapsed into sync, so the queue is invisible"
    );
    assert!(durability.backpressure.measure().is_observed());

    // And backpressure with no samples is unavailable rather than zero:
    // "we never refused anything" and "we do not measure refusals" are
    // different, and a node that declines work is not a slow node.
    let quiet = Durability {
        sync: durability.sync,
        commit_return: durability.commit_return,
        backpressure: Latency::default(),
    };
    assert_eq!(
        quiet.backpressure.measure(),
        Measure::Unavailable(Unavailable::NoSamples)
    );
}

/// The journal and the projection are different positions, and the gap
/// between them is the number that matters.
#[test]
fn the_journal_and_the_projection_are_reported_separately() {
    let frontiers = Frontiers {
        journal: 1000,
        materialized: 940,
        checkpoint: 500,
    };
    assert_eq!(frontiers.unmaterialized(), 60, "the node is behind its log");
    assert_eq!(frontiers.unreclaimed(), 440, "what a reclaim would replay");

    // Caught up is not the same as nothing to do, and the invariant
    // `C <= M <= J` means neither gap can go negative and be read as
    // progress.
    let level = Frontiers {
        journal: 10,
        materialized: 10,
        checkpoint: 10,
    };
    assert_eq!(level.unmaterialized(), 0);
    assert_eq!(level.unreclaimed(), 0);
    let impossible = Frontiers {
        journal: 5,
        materialized: 9,
        checkpoint: 20,
    };
    assert_eq!(impossible.unmaterialized(), 0);
    assert_eq!(impossible.unreclaimed(), 0);
}

/// Labels are bounded because the types have no room for anything else.
#[test]
fn every_label_has_a_finite_frozen_domain() {
    // Stages and lanes: finite lists, distinct identifiers, distinct
    // low-cardinality names.
    let mut ids: Vec<u16> = Stage::ALL.iter().map(|s| s.id()).collect();
    let ordered = ids.clone();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), Stage::ALL.len(), "two stages share an id");
    assert_eq!(ordered, ids, "the stages are not in identifier order");

    let mut names: Vec<&str> = Stage::ALL
        .iter()
        .map(|s| s.name())
        .chain(Lane::ALL.iter().map(|l| l.name()))
        .collect();
    let count = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(count, names.len(), "two labels share a name");
    assert!(
        names
            .iter()
            .all(|n| n.len() <= 20 && n.chars().all(|c| c.is_ascii_lowercase() || c == '-')),
        "a label is not a short lowercase constant: {names:?}"
    );

    // Shards are bounded by construction: a node with more than the
    // reporting bound aggregates rather than growing a series per shard.
    assert!(ShardIndex::new(0).is_some());
    assert!(ShardIndex::new(MAX_REPORTED_SHARDS - 1).is_some());
    assert_eq!(
        ShardIndex::new(MAX_REPORTED_SHARDS),
        None,
        "an unbounded shard label was admitted"
    );
    assert_eq!(ShardIndex::new(u16::MAX), None);
}

/// A rendered snapshot is numbers and frozen enums, so there is nothing
/// secret in it to find.
#[test]
fn a_rendered_snapshot_carries_no_secret_or_key_shaped_text() {
    let recorder = Recorder::new();
    recorder.entered(Stage::Admission);
    recorder.completed(Stage::Admission, Duration::from_millis(3));
    let snapshot = MetricsSnapshot {
        stages: recorder.snapshot_stages(&voter()),
        lanes: Lane::ALL
            .iter()
            .map(|lane| LaneReading {
                lane: *lane,
                queue_wait: Measure::Observed(Duration::from_micros(80)),
                credit_wait: Measure::Unavailable(Unavailable::NoSamples),
                frames: 12,
                refused: 0,
                headroom: Measure::Observed(4096),
            })
            .collect(),
        shards: vec![ShardReading {
            shard: ShardIndex::new(0).expect("shard zero"),
            headroom: Measure::Observed(1 << 20),
            pressure_permille: Measure::Observed(120),
        }],
        durability: Measure::Observed(Durability::default()),
        frontiers: Measure::Observed(Frontiers {
            journal: 90,
            materialized: 88,
            checkpoint: 40,
        }),
        view_age: Measure::Observed(Duration::from_millis(12)),
        engine_pressure: Measure::Observed(Headroom { used: 3, bound: 10 }),
    };
    let rendered = serde_json::to_string(&snapshot).expect("a snapshot renders");

    // The scan Section 22.3 asks for. These are shapes a credential, a
    // token or a raw identity would take; none of them can appear,
    // because nothing of that kind is ever recorded.
    for pattern in [
        "BEGIN", "PRIVATE", "Bearer", "eyJ", "secret", "token", "password", "key=",
    ] {
        assert!(
            !rendered.contains(pattern),
            "a rendered snapshot contains {pattern:?}"
        );
    }
    // No long hex or base64 runs: an identity, digest or key smuggled in
    // as a label would show up as one.
    let longest = rendered
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(
        longest <= 20,
        "a rendered snapshot carries a {longest}-character run, which is identity-shaped"
    );

    // And it round-trips, so the admin endpoint serves the same thing
    // the node measured.
    let parsed: MetricsSnapshot = serde_json::from_str(&rendered).expect("round trip");
    assert_eq!(parsed, snapshot);
    assert!(parsed.stage(Stage::Admission).is_some());
}

/// Readers and writers run concurrently and the recorder stays
/// consistent.
///
/// What actually guarantees that a diagnostics reader cannot stall a
/// voter is that [`Recorder`] contains no lock -- there is nothing to
/// take, so there is nothing to hold. This test cannot prove the
/// absence of a lock; what it shows is that a reader making thousands
/// of snapshots never starves a writer and never observes a torn
/// accounting, which is the behaviour that absence produces.
#[test]
fn reading_diagnostics_never_blocks_the_work_being_measured() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let recorder = Arc::new(Recorder::new());
    let stop = Arc::new(AtomicBool::new(false));
    let roles = voter();

    let writer = {
        let recorder = Arc::clone(&recorder);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut written = 0u64;
            while !stop.load(Ordering::Relaxed) {
                recorder.entered(Stage::Journal);
                recorder.completed(Stage::Journal, Duration::from_micros(10));
                written += 1;
            }
            written
        })
    };

    // Wait until the writer has actually started. On a loaded machine
    // the reader can finish its two thousand snapshots before the writer
    // thread is ever scheduled, and a run where the two never overlapped
    // shows nothing about either of them -- so this waits for the
    // overlap instead of assuming the scheduler provides it.
    let started = std::time::Instant::now();
    while recorder.stage(Stage::Journal).entered == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the writer thread never started"
        );
        std::thread::yield_now();
    }

    // Snapshot repeatedly while the writer runs. If a reader could
    // block a writer this would deadlock or starve; it cannot, because
    // there is no lock to take.
    let mut readings = 0u64;
    for _ in 0..2_000 {
        let stages = recorder.snapshot_stages(&roles);
        let journal = stages
            .iter()
            .find(|r| r.stage == Stage::Journal)
            .and_then(|r| r.metrics.observed())
            .expect("a journal reading");
        assert!(
            journal.entered >= journal.completed,
            "a snapshot observed more completions than entries"
        );
        readings += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let written = writer.join().expect("the writer finished");

    assert_eq!(readings, 2_000);
    assert!(written > 0, "the writer never ran alongside the readers");
    let final_reading = recorder.stage(Stage::Journal);
    assert_eq!(final_reading.completed, written);
    assert_eq!(final_reading.latency.count, written);
}

/// Every stage is reported for every role, so a missing series is
/// always a stated absence rather than a hole.
#[test]
fn every_stage_is_accounted_for_under_every_role() {
    for set in [
        voter(),
        roles("frontend"),
        roles("observer"),
        roles("voter"),
    ] {
        let stages = Recorder::new().snapshot_stages(&set);
        assert_eq!(
            stages.len(),
            Stage::ALL.len(),
            "a role dropped a stage instead of reporting it unavailable"
        );
        let reported: Vec<Stage> = stages.iter().map(|r: &StageReading| r.stage).collect();
        assert_eq!(reported, Stage::ALL.to_vec(), "stages are out of order");
        // Transport accounting exists wherever there is a transport, so
        // it is never role-unavailable.
        for always in [Stage::StreamCredits, Stage::BulkInterference] {
            assert!(
                stages
                    .iter()
                    .find(|r| r.stage == always)
                    .expect("reported")
                    .metrics
                    .is_observed(),
                "{} was unavailable for a node that has a transport",
                always.name()
            );
        }
    }
}
