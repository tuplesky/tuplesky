//! task-j05 step 2: the journal's two recovery policies against damaged
//! files.
//!
//! What the pinned engine does (`pipe_builder.rs`, raft-engine 097c499):
//! under `TolerateTailCorruption` the first broken batch anywhere in the
//! last log file is taken for a torn tail, and the file is truncated there
//! (a broken file header truncates it to nothing), and the last batch of
//! any file is truncated if its entries block is broken; under
//! `AbsoluteConsistency` any broken batch refuses the open, and the zeros
//! the engine preallocates after the last batch read as one.
//!
//! The adapter runs the engine over its tail guard (`coord_journal_raft_engine::guard`,
//! #211), which refuses every one of those truncations that is not of a
//! torn, unacknowledged write. So:
//!
//! * a crash's torn, unsynced tail opens under `TolerateTailCorruption`
//!   with every acknowledged group when the device writes whole sectors
//!   (the crash matrix in `crash.rs`, the default policy, sector tears),
//!   and is refused under `AbsoluteConsistency` after most crashes;
//! * a tear inside a sector, which such a device can't leave, may be
//!   refused, and is never opened short;
//! * damage anywhere in the synced log is refused under both policies,
//!   and under `TolerateTailCorruption` the files are left as they were.
//!   Before the guard, damage in the last file opened short, or emptied
//!   the file so that it read as uninitialized.

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
/// flipped at `at` of `file`.
fn damaged(dir: &Path, file: &str, at: usize) -> Acked {
    damaged_with(dir, file, at, 0x40)
}

/// [`damaged`], the byte XOR `mask`.
fn damaged_with(dir: &Path, file: &str, at: usize, mask: u8) -> Acked {
    let root = dir.join("j");
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let acked = workload(&root, fs, &shared);
    assert_eq!(shared.unsynced(), 0);
    flip_with(&root.join(file), at, mask);
    acked
}

fn flip(path: &Path, at: usize) {
    flip_with(path, at, 0x40);
}

fn flip_with(path: &Path, at: usize, mask: u8) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[at] ^= mask;
    std::fs::write(path, &bytes).unwrap();
}

/// Every log file under `root`, by name, with its bytes.
fn log_files(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap())
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
        .filter(|(name, _)| name.ends_with(".raftlog") || name.ends_with(".rewrite"))
        .map(|(name, path)| (name, std::fs::read(path).unwrap()))
        .collect();
    files.sort();
    files
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
        let acked = damaged(dir.path(), &file, offset as usize + len / 2);
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

/// The guard (#211): under the default policy, one byte XOR 0x01, 0x40 or
/// 0xff in any synced write of the log, at any of eight places (five in a batch's
/// header -- its length, compression type and item offset -- its middle,
/// and two in its footer checksum; in the file header, its magic and its
/// alignment), refuses the open as corrupt, names the file, and leaves
/// every log file as it was. Before the guard, damage like this
/// opened short or emptied the file (step 2's finding).
#[test]
fn tolerate_tail_refuses_damage_in_the_synced_log_and_leaves_the_files() {
    let (mut refused, mut by_guard, mut held) = (0, 0, Vec::new());
    for (file, offset, len) in log_writes() {
        let offset = offset as usize;
        for (place, at) in [
            ("byte 0", offset),
            ("byte 3", offset + 3),
            ("byte 7", offset + 7),
            ("byte 12", offset + 12),
            ("byte 15", offset + 15),
            ("middle", offset + len / 2),
            ("fourth from last", offset + len - 4),
            ("last byte", offset + len - 1),
        ] {
            for mask in [0x01u8, 0x40, 0xff] {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().join("j");
                let acked = damaged_with(dir.path(), &file, at, mask);
                let before = log_files(&root);
                let (outcome, error) =
                    reopen(&root, &acked, RecoveryPolicy::TolerateTailCorruption);
                let case = format!(
                    "{place} of the write at {offset}+{len} of {file} (byte {at}, xor {mask:#04x})"
                );
                match (outcome, error) {
                    (Outcome::Refused, Some(OpenError::Corrupt(reason))) => {
                        refused += 1;
                        if reason.contains("recovery would have truncated it") {
                            assert!(reason.contains(&file), "{case}: {reason}");
                            by_guard += 1;
                        }
                    }
                    // Bytes the engine reads past, losing nothing: the file
                    // header's alignment, or the compression type of a
                    // batch with no entries to decompress.
                    (Outcome::HeldEverything, None)
                        if (offset == 0 && at >= len - 8)
                            || (place == "byte 7" && mask == 0x01) =>
                    {
                        held.push(case.clone())
                    }
                    other => panic!("{case}: {other:?}"),
                }
                assert_eq!(log_files(&root), before, "{case}: the log files changed");
            }
        }
    }
    assert!(
        by_guard > 0 && refused >= by_guard,
        "refused {refused}, by the guard {by_guard}"
    );
    eprintln!(
        "refused {refused} damaged opens, {by_guard} of them by the guard; {} held everything: {held:#?}",
        held.len()
    );
}

/// A file with a later one after it was finished and synced before the
/// later one was created, so the engine's truncation of its last batch,
/// on a broken entries block, is refused, whatever the bytes look like.
#[test]
fn a_broken_batch_in_a_file_with_a_later_one_is_refused() {
    let small = JournalOptions {
        target_file_size_bytes: 1024,
        ..options()
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    workload_with(&root, fs, &shared, &small);
    assert_eq!(shared.unsynced(), 0);
    // The last write of the first file: its last batch.
    let first = "0000000000000001.raftlog";
    let offset = shared
        .log()
        .into_iter()
        .filter_map(|op| match op {
            Op::Write { file, offset, .. } if file == first => Some(offset as usize),
            _ => None,
        })
        .next_back()
        .unwrap();
    assert!(offset > 0, "the first file holds a batch");
    assert!(
        log_files(&root)
            .iter()
            .any(|(name, _)| name == "0000000000000002.raftlog"),
        "the workload rotated"
    );
    // Inside its entries block, just past the batch header.
    flip(&root.join(first), offset + 16 + 2);
    let before = log_files(&root);
    let opened = RaftEngineJournal::open_existing(&root, identity(), &with_options(&small));
    match opened {
        Err(OpenError::Corrupt(reason)) => {
            assert!(reason.contains(first), "{reason}");
            assert!(
                reason.contains("a later file of the same queue exists"),
                "{reason}"
            );
        }
        other => panic!("opened or failed otherwise: {:?}", other.map(|_| ())),
    }
    assert_eq!(log_files(&root), before, "the log files changed");
}

/// The last file's header zeroed, with its acknowledged batches whole
/// after it (#211 re-review): only a sync could have put them there, so
/// the open refuses rather than empty the file.
#[test]
fn a_zeroed_header_on_the_last_file_with_batches_after_it_is_refused() {
    let small = JournalOptions {
        target_file_size_bytes: 1024,
        ..options()
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    workload_with(&root, fs, &shared, &small);
    assert_eq!(shared.unsynced(), 0);
    let files = log_files(&root);
    let (last, bytes) = files
        .iter()
        .filter(|(name, _)| name.ends_with(".raftlog"))
        .max_by(|a, b| a.0.cmp(&b.0))
        .unwrap();
    assert_ne!(last, "0000000000000001.raftlog", "the workload rotated");
    let batches = shared
        .log()
        .into_iter()
        .filter(|op| matches!(op, Op::Write { file, offset, .. } if file == last && *offset > 0))
        .count();
    assert!(batches > 0, "the last file holds acknowledged batches");
    let mut zeroed = bytes.clone();
    zeroed[..69].fill(0);
    std::fs::write(root.join(last), &zeroed).unwrap();
    let before = log_files(&root);
    let opened = RaftEngineJournal::open_existing(&root, identity(), &with_options(&small));
    match opened {
        Err(OpenError::Corrupt(reason)) => {
            assert!(reason.contains(last.as_str()), "{reason}");
            assert!(
                reason.contains("the file header is zeros, but a whole batch follows it"),
                "{reason}"
            );
        }
        other => panic!("opened or failed otherwise: {:?}", other.map(|_| ())),
    }
    assert_eq!(log_files(&root), before, "the log files changed");
}

fn with_options(options: &JournalOptions) -> JournalOptions {
    JournalOptions {
        recovery: RecoveryPolicy::TolerateTailCorruption,
        ..*options
    }
}

/// A crash whose unsynced writes tear at any byte, which a device that
/// writes whole sectors can't leave: the default policy may refuse such a
/// crash image (no unwritten sector to tell it from damage), and never
/// opens it without a group it acknowledged.
#[test]
fn tolerate_tail_never_opens_short_after_a_byte_torn_crash() {
    let total = census().len() as u64;
    let (mut held, mut refused) = (0, 0);
    for k in 1..=total {
        for seed in 1..=3 {
            let tail = Tail::Seeded(k * 16 + seed);
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("j");
            let plan = FaultPlan {
                crash_after: Some(k),
                ..FaultPlan::default()
            };
            let (fs, shared) = FaultFs::new(plan);
            let acked = workload(&root, fs, &shared);
            shared.materialize(tail).unwrap();
            let (outcome, error) = reopen(&root, &acked, RecoveryPolicy::TolerateTailCorruption);
            match outcome {
                Outcome::LostAcknowledged => panic!("crash after {k}, {tail:?}: opened short"),
                Outcome::HeldEverything => held += 1,
                Outcome::Refused if acked.created => {
                    assert!(
                        matches!(error, Some(OpenError::Corrupt(_))),
                        "crash after {k}, {tail:?}: {error:?}"
                    );
                    refused += 1;
                }
                Outcome::Refused => {}
            }
        }
    }
    assert!(held > refused, "held {held}, refused {refused}");
    eprintln!("byte tears: held {held}, refused {refused}");
}
