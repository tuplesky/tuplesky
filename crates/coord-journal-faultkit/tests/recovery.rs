//! task-j05 step 2: the journal's two recovery policies against damaged
//! files.
//!
//! What the pinned engine does (`pipe_builder.rs`, raft-engine 097c499):
//! under `TolerateTailCorruption` the first broken batch anywhere in the
//! last log file is taken for a torn tail, and the file is truncated there
//! (a broken file header truncates it to nothing); under
//! `AbsoluteConsistency` any broken batch refuses the open, and the zeros
//! the engine preallocates after the last batch read as one. So:
//!
//! * a crash's torn, unsynced tail opens under `TolerateTailCorruption`
//!   with every acknowledged group (the crash matrix in `crash.rs` runs
//!   under it, the default), and is refused under `AbsoluteConsistency`
//!   after most crashes;
//! * damage inside the synced prefix of the last file is refused under
//!   `AbsoluteConsistency`, and under `TolerateTailCorruption` it truncates
//!   every acknowledged group from the damage on and opens. That is a
//!   permissive recovery losing a durable prefix silently, which task-j05
//!   rules out; the guard is the next change (task-j05's entry), and the
//!   characterization test below flips to a refusal with it.

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

use coord_journal_faultkit::{FaultFs, FaultPlan, Op, Tail};
use coord_journal_raft_engine::{JournalOptions, OpenError, RaftEngineJournal, RecoveryPolicy};

use common::*;

fn with(policy: RecoveryPolicy) -> JournalOptions {
    JournalOptions {
        recovery: policy,
        ..options()
    }
}

/// What reopening under a policy did.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Refused,
    HeldEverything,
    LostAcknowledged,
}

fn reopen(root: &Path, acked: &Acked, policy: RecoveryPolicy) -> (Outcome, Option<OpenError>) {
    match RaftEngineJournal::open_existing(root, identity(), &with(policy)) {
        Err(e) => (Outcome::Refused, Some(e)),
        Ok(journal) => {
            drop(journal);
            let held = catch_unwind(AssertUnwindSafe(|| {
                check_with(root, acked, "reopen", &with(policy))
            }));
            match held {
                Ok(()) => (Outcome::HeldEverything, None),
                Err(_) => (Outcome::LostAcknowledged, None),
            }
        }
    }
}

/// A finished workload, every group synced and acknowledged, with one byte
/// flipped in the middle of the write at `offset` of `file`.
fn damaged(dir: &Path, file: &str, offset: u64, len: usize) -> Acked {
    let root = dir.join("j");
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let acked = workload(&root, fs, &shared);
    assert_eq!(shared.unsynced(), 0);
    let path = root.join(file);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[offset as usize + len / 2] ^= 0x40;
    std::fs::write(&path, &bytes).unwrap();
    acked
}

/// The log file's writes, each a synced batch once the workload ends.
fn log_writes() -> Vec<(String, u64, usize)> {
    census()
        .into_iter()
        .filter_map(|op| match op {
            Op::Write { file, offset, len } if file.ends_with(".raftlog") => {
                Some((file, offset, len))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn absolute_consistency_refuses_damage_anywhere_in_the_synced_log() {
    let writes = log_writes();
    assert!(writes.len() > usize::from(GROUPS));
    for (file, offset, len) in writes {
        let dir = tempfile::tempdir().unwrap();
        let acked = damaged(dir.path(), &file, offset, len);
        let (outcome, _) = reopen(
            &dir.path().join("j"),
            &acked,
            RecoveryPolicy::AbsoluteConsistency,
        );
        assert_eq!(outcome, Outcome::Refused, "damage at {offset}+{len}");
    }
}

#[test]
fn absolute_consistency_never_opens_short_and_refuses_most_crashes() {
    let total = census().len() as u64;
    let (mut held, mut refused_after_ack) = (0, 0);
    for k in 1..=total {
        for tail in [Tail::None, Tail::All, Tail::Seeded(k)] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("j");
            let plan = FaultPlan {
                crash_after: Some(k),
                ..FaultPlan::default()
            };
            let (fs, shared) = FaultFs::new(plan);
            let acked = workload(&root, fs, &shared);
            shared.materialize(tail).unwrap();
            let (outcome, _) = reopen(&root, &acked, RecoveryPolicy::AbsoluteConsistency);
            match outcome {
                Outcome::LostAcknowledged => panic!("crash after {k}, {tail:?}: opened short"),
                Outcome::HeldEverything => held += 1,
                Outcome::Refused if acked.created => refused_after_ack += 1,
                Outcome::Refused => {}
            }
        }
    }
    // Not a serving policy: the preallocated zeros after the last batch
    // read as a broken batch, so most crashes, even with nothing unsynced
    // lost, refuse to open.
    assert!(
        refused_after_ack > held,
        "held {held}, refused {refused_after_ack}"
    );
}

/// The finding of step 2, characterized: under the default policy, damage
/// inside the synced prefix of the last log file opens the journal without
/// every group it acknowledged, or empties the file and reports it
/// uninitialized when the damage is in the file's header. The guard that
/// refuses these opens is the next change; with it, every case here is
/// refused and this test asserts that instead.
#[test]
fn today_tolerate_tail_truncates_acknowledged_groups_behind_damage_in_the_last_file() {
    let mut lost = Vec::new();
    let mut wiped = Vec::new();
    for (file, offset, len) in log_writes() {
        let dir = tempfile::tempdir().unwrap();
        let acked = damaged(dir.path(), &file, offset, len);
        let (outcome, error) = reopen(
            &dir.path().join("j"),
            &acked,
            RecoveryPolicy::TolerateTailCorruption,
        );
        match (outcome, error) {
            (Outcome::LostAcknowledged, _) => lost.push(offset),
            (Outcome::Refused, Some(OpenError::NotInitialized)) => wiped.push(offset),
            (Outcome::Refused, _) => {}
            (Outcome::HeldEverything, _) => {
                panic!("damage at {offset}+{len} left every group readable")
            }
        }
    }
    assert!(
        !lost.is_empty(),
        "no damage point opened short any more: the guard landed, flip this test"
    );
    assert!(
        !wiped.is_empty(),
        "header damage no longer reads as uninitialized: flip this test"
    );
    eprintln!("opened short after damage at {lost:?}; emptied to uninitialized at {wiped:?}");
}
