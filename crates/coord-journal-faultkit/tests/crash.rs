//! task-j05 step 1: the pinned raft-engine journal over [`FaultFs`].
//!
//! Every counted file operation of a small workload is a crash point; each
//! is crashed under every tail policy, and the journal reopened over the
//! crash image must hold every group it acknowledged, as a chain that
//! verifies, and nothing past what was attempted. Write failures, ENOSPC
//! and sync failures are scripted at every ordinal they can hit. The audit
//! names the files the journal touches without the adapter.

mod common;

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use coord_journal_faultkit::{FaultFs, FaultPlan, Op, Tail};

use common::*;

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

#[test]
fn the_census_counts_writes_and_syncs_of_the_log() {
    let ops = census();
    eprintln!("census: {} operations: {ops:?}", ops.len());
    let writes = ops.iter().filter(|o| matches!(o, Op::Write { .. })).count();
    let syncs = ops.iter().filter(|o| matches!(o, Op::Sync { .. })).count();
    // One synced write a group at least: the creation's metadata, the
    // mapping, and every group.
    assert!(writes >= usize::from(GROUPS) + 2, "{ops:?}");
    assert!(syncs >= usize::from(GROUPS) + 2, "{ops:?}");
}

#[test]
fn a_crash_at_every_operation_keeps_every_acknowledged_group() {
    let total = census().len() as u64;
    let tails = [
        Tail::None,
        Tail::All,
        Tail::Seeded(1),
        Tail::Seeded(2),
        Tail::Seeded(3),
    ];
    for k in 1..=total {
        for tail in tails {
            let plan = FaultPlan {
                crash_after: Some(k),
                ..FaultPlan::default()
            };
            run(
                plan,
                Some(tail),
                &format!("crash after {k} of {total}, {tail:?}"),
            );
        }
    }
}

#[test]
fn a_failed_write_never_acknowledges_and_loses_nothing_acknowledged() {
    let ops = census();
    for (i, op) in ops.iter().enumerate() {
        if !matches!(op, Op::Write { .. }) {
            continue;
        }
        let n = i as u64 + 1;
        for prefix in [0, 512, 4096] {
            for tail in [None, Some(Tail::None), Some(Tail::Seeded(n))] {
                // With a crash tail, the crash lands at the fault itself,
                // so no later sync (the close's among them) persists what
                // the fault left unsynced.
                let plan = FaultPlan {
                    fail_write_at: Some(n),
                    crash_after: tail.map(|_| n),
                    fail_write_prefix: prefix,
                    ..FaultPlan::default()
                };
                let shared = run(
                    plan,
                    tail,
                    &format!("write {n} fails after {prefix} bytes, then {tail:?}"),
                );
                assert!(shared.log().contains(&Op::Failed), "write {n} not reached");
            }
        }
    }
}

#[test]
fn enospc_never_acknowledges_and_loses_nothing_acknowledged() {
    let ops = census();
    let total = ops.len() as u64;
    for (i, op) in ops.iter().enumerate() {
        if !matches!(op, Op::Write { .. } | Op::SetLen { .. }) {
            continue;
        }
        let n = i as u64 + 1;
        // A one-operation window, and one that never closes.
        for window in [(n, n), (n, u64::MAX)] {
            for tail in [None, Some(Tail::None)] {
                let plan = FaultPlan {
                    enospc: Some(window),
                    crash_after: tail.map(|_| n),
                    ..FaultPlan::default()
                };
                run(
                    plan,
                    tail,
                    &format!("ENOSPC over {window:?} of {total}, then {tail:?}"),
                );
            }
        }
    }
}

#[test]
fn a_failed_sync_never_acknowledges_and_loses_nothing_acknowledged() {
    let ops = census();
    for (i, op) in ops.iter().enumerate() {
        if !matches!(op, Op::Sync { .. }) {
            continue;
        }
        let n = i as u64 + 1;
        for persisted in [Tail::None, Tail::All, Tail::Seeded(n)] {
            for after in [Tail::None, Tail::All] {
                let plan = FaultPlan {
                    fail_sync_at: Some(n),
                    crash_after: Some(n),
                    tail: persisted,
                    ..FaultPlan::default()
                };
                let shared = run(
                    plan,
                    Some(after),
                    &format!("sync {n} fails persisting {persisted:?}, crash keeps {after:?}"),
                );
                assert!(shared.log().contains(&Op::Failed), "sync {n} not reached");
            }
        }
    }
}

/// The matrix's check has teeth: a device that lies about one sync of a
/// group's write loses that acknowledged group at the crash, and the check
/// says so. Without this, a check that never looked would pass the matrix.
#[test]
fn a_lying_sync_is_caught_by_the_check() {
    let ops = census();
    let syncs: Vec<u64> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| matches!(op, Op::Sync { .. }))
        .map(|(i, _)| i as u64 + 1)
        .collect();
    let mut caught = 0;
    for &n in &syncs {
        let plan = FaultPlan {
            lie_on_sync_at: Some(n),
            // Crash right after the lie, before a later sync can cover it.
            crash_after: Some(n),
            ..FaultPlan::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("j");
        let (fs, shared) = FaultFs::new(plan);
        let acked = workload(&root, fs, &shared);
        shared.materialize(Tail::None).unwrap();
        if catch_unwind(AssertUnwindSafe(|| check(&root, &acked, "lie"))).is_err() {
            caught += 1;
        }
    }
    // Crashing right after the lie means the acknowledgement it bought is
    // never observed. So crash one operation later instead: the lied-about
    // group's acknowledgement has returned, and the next real sync, which
    // would persist its bytes too, never runs.
    let mut caught_later = 0;
    for &n in &syncs {
        let plan = FaultPlan {
            lie_on_sync_at: Some(n),
            crash_after: Some(n + 1),
            ..FaultPlan::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("j");
        let (fs, shared) = FaultFs::new(plan);
        let acked = workload(&root, fs, &shared);
        shared.materialize(Tail::None).unwrap();
        if catch_unwind(AssertUnwindSafe(|| check(&root, &acked, "lie"))).is_err() {
            caught_later += 1;
        }
    }
    assert_eq!(
        caught, 0,
        "a lie whose acknowledgement nobody saw is no loss"
    );
    // Every lie loses an acknowledged write (the creation, the mapping or a
    // group) except the last two: the last group's sync is followed by the
    // close's, which persists its bytes, and the close's own sync
    // acknowledges nothing.
    assert!(
        caught_later + 2 >= syncs.len(),
        "caught {caught_later} lies of {} across {syncs:?}",
        syncs.len()
    );
}

// ---------------------------------------------------------------------------
// The kit itself
// ---------------------------------------------------------------------------

#[test]
fn a_crash_image_is_the_synced_bytes_plus_the_chosen_tail() {
    use raft_engine::env::{FileSystem, Handle};
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f");
    for (tail, expect) in [
        (Tail::None, b"synced".to_vec()),
        (Tail::All, b"synced-unsynced".to_vec()),
    ] {
        let (fs, shared) = FaultFs::new(FaultPlan::default());
        let handle = Arc::new(fs.create(&path).unwrap());
        let mut writer = fs.new_writer(handle.clone()).unwrap();
        writer.write_all(b"synced").unwrap();
        handle.sync().unwrap();
        writer.write_all(b"-unsynced").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"synced-unsynced");
        assert_eq!(shared.unsynced(), 1);
        shared.materialize(tail).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expect);
        // Halted: nothing reaches the files after the crash.
        writer.write_all(b"late").unwrap();
        handle.sync().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expect);
        std::fs::remove_file(&path).unwrap();
    }
}

/// A failed sync that persisted everything leaves nothing pending, so no
/// later crash image replays an older overlapping write over what it
/// persisted (Codex review on #210).
#[test]
fn a_failed_sync_that_persisted_everything_leaves_nothing_pending() {
    use raft_engine::env::{FileSystem, Handle};
    use std::io::{Seek, SeekFrom, Write};

    for seed in 0..16 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let plan = FaultPlan {
            // Ordinal 1 is the create, 2 and 3 the writes, 4 the sync.
            fail_sync_at: Some(4),
            tail: Tail::All,
            ..FaultPlan::default()
        };
        let (fs, shared) = FaultFs::new(plan);
        let handle = Arc::new(fs.create(&path).unwrap());
        let mut writer = fs.new_writer(handle.clone()).unwrap();
        writer.write_all(b"aaaa").unwrap();
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_all(b"bb").unwrap();
        assert!(handle.sync().is_err());
        assert_eq!(shared.unsynced(), 0);
        shared.materialize(Tail::Seeded(seed)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"bbaa", "seed {seed}");
    }
}

#[test]
fn a_short_write_lands_whole_sectors_and_reports_eio() {
    use raft_engine::env::FileSystem;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f");
    let plan = FaultPlan {
        // Ordinal 1 is the create; 2 is the write.
        fail_write_at: Some(2),
        fail_write_prefix: 1000,
        ..FaultPlan::default()
    };
    let (fs, _shared) = FaultFs::new(plan);
    let handle = Arc::new(fs.create(&path).unwrap());
    let mut writer = fs.new_writer(handle).unwrap();
    let err = writer.write(&[7u8; 2000]).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(5));
    assert_eq!(std::fs::read(&path).unwrap(), vec![7u8; 512]);
}

#[test]
fn a_crash_scripted_at_a_rejected_operation_still_halts() {
    use raft_engine::env::FileSystem;
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f");
    let plan = FaultPlan {
        // Ordinal 1 is the create; 2 is the write ENOSPC rejects.
        enospc: Some((2, 2)),
        crash_after: Some(2),
        ..FaultPlan::default()
    };
    let (fs, shared) = FaultFs::new(plan);
    let handle = Arc::new(fs.create(&path).unwrap());
    let mut writer = fs.new_writer(handle).unwrap();
    let err = writer.write(b"x").unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::StorageFull);
    assert!(shared.is_frozen());
}

// ---------------------------------------------------------------------------
// The audit
// ---------------------------------------------------------------------------

/// Every file the journal leaves in its directory is either one the kit
/// tracked or one named here as reached without the adapter. A file that
/// is neither is a new bypass, and fails this test until it is named.
#[test]
fn files_the_journal_touches_without_the_adapter_are_named() {
    // raft-engine's directory lock (`pipe_builder.rs`) is created with
    // `std::fs`, never through the `FileSystem`.
    const UNCONTROLLED: &[&str] = &["LOCK"];

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let acked = workload(&root, fs, &shared);
    assert!(acked.through.is_some());
    let tracked: BTreeSet<String> = shared
        .files()
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();
    let mut untracked = BTreeSet::new();
    for entry in std::fs::read_dir(&root).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if !tracked.contains(&name) {
            untracked.insert(name);
        }
    }
    let named: BTreeSet<String> = UNCONTROLLED.iter().map(|s| s.to_string()).collect();
    assert_eq!(untracked, named, "tracked: {tracked:?}");
    // And the directory operations the adapter does see are in the log.
    assert!(
        shared
            .log()
            .iter()
            .any(|op| matches!(op, Op::Dir { what } if what.starts_with("create")))
    );
}
