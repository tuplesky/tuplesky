//! task-j05 step 1: the pinned raft-engine journal over [`FaultFs`].
//!
//! Every counted file operation of a small workload is a crash point; each
//! is crashed under every tail policy, and the journal reopened over the
//! crash image must hold every group it acknowledged, as a chain that
//! verifies, and nothing past what was attempted. Write failures, ENOSPC
//! and sync failures are scripted at every ordinal they can hit. The audit
//! names the files the journal touches without the adapter.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;

use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_journal_api::engine::{JournalEngine, ReadBudget};
use coord_journal_api::group::{GroupEntry, GroupLimits, GroupWrite};
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1, RecordBody,
    RecordDraft, RecordExpectation, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{ShardId, StorageStreamId, StreamAllocator, StreamKey};
use coord_journal_faultkit::{FaultFs, FaultPlan, Op, Shared, Tail};
use coord_journal_raft_engine::{JournalIdentity, JournalOptions, RaftEngineJournal};
use coord_store_api::registry::Collection;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};

/// Groups the workload appends after creating the journal and its stream.
const GROUPS: u8 = 6;

// ---------------------------------------------------------------------------
// Fixtures (as in coord-journal-raft-engine's own tests)
// ---------------------------------------------------------------------------

fn identity() -> JournalIdentity {
    JournalIdentity {
        cluster: ClusterId([1; 16]),
        replica: ReplicaId([7; 16]),
    }
}

fn options() -> JournalOptions {
    JournalOptions {
        target_file_size_bytes: 1024 * 1024,
        purge_threshold_bytes: 8 * 1024 * 1024,
        ..JournalOptions::default()
    }
}

fn key() -> StreamKey {
    StreamKey {
        cluster: ClusterId([1; 16]),
        domain: DomainId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn origin(stream: StorageStreamId) -> RecordOrigin {
    let k = key();
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

fn transition(tag: u8, size: usize) -> RecordBody {
    let epoch = ConfigurationEpoch::new(1).unwrap();
    RecordBody::ProtocolTransition {
        context: TransitionContext {
            boot: BootId([0xb0; 16]),
            configuration: epoch,
            ballot: Ballot {
                epoch,
                number: 1,
                leader: ReplicaId([7; 16]),
            },
        },
        updates: vec![StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: vec![tag],
            value: Some(vec![tag; size]),
        }],
    }
}

/// The chain the workload appends: a genesis, then one record a group.
fn records(stream: StorageStreamId) -> Vec<JournalRecordV1> {
    let origin = origin(stream);
    let genesis = JournalRecordV1::seal(RecordDraft {
        origin,
        seq: LocalJournalSeq::new(1).unwrap(),
        predecessor: GENESIS_PREDECESSOR,
        body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
            format: JOURNAL_RECORD_FORMAT_V1,
        }),
    })
    .unwrap();
    let mut out = vec![genesis];
    for tag in 1..=GROUPS {
        let prev = out.last().unwrap();
        let next = JournalRecordV1::seal(RecordDraft {
            origin,
            seq: prev.seq().checked_next().unwrap(),
            predecessor: prev.digest(),
            // Sizes that straddle a sector, so torn writes land mid-record.
            body: transition(tag, 300 + 97 * tag as usize),
        })
        .unwrap();
        out.push(next);
    }
    out
}

fn group(n: u64, stream: StorageStreamId, records: Vec<JournalRecordV1>) -> GroupWrite {
    let mut g = GroupWrite::new(GroupLimits::DEFAULT);
    g.push(GroupEntry::new(barrier(n), stream, records).unwrap())
        .unwrap();
    g
}

fn read_all(journal: &impl JournalEngine, stream: StorageStreamId) -> Vec<JournalRecordV1> {
    let mut out = Vec::new();
    let mut after = LocalJournalSeq::ZERO;
    loop {
        let page = journal
            .read_suffix(stream, after, ReadBudget::new(2, u32::MAX))
            .unwrap();
        if let Some(last) = page.records.last() {
            after = last.seq();
        }
        out.extend(page.records);
        if page.exhausted {
            return out;
        }
    }
}

// ---------------------------------------------------------------------------
// The workload and its check
// ---------------------------------------------------------------------------

/// What the workload saw acknowledged before the faults stopped it.
#[derive(Debug, Default)]
struct Acked {
    created: bool,
    stream: Option<StorageStreamId>,
    /// Highest sequence an `append_group` returned `Ok` for.
    through: Option<LocalJournalSeq>,
    /// Highest sequence any `append_group` was called with.
    attempted: Option<LocalJournalSeq>,
    /// What every call returned, frozen or not (#210 review): the fields
    /// above count only acknowledgements given before the crash, so a test
    /// asking whether a fault was acknowledged reads these.
    calls: Vec<Call>,
}

/// One call of the workload: the operations it issued and whether it
/// returned `Ok`.
#[derive(Debug)]
struct Call {
    what: &'static str,
    /// The ordinals the call's operations took, `from..=to`; empty when it
    /// issued none.
    from: u64,
    to: u64,
    ok: bool,
}

impl Acked {
    /// The call whose operations include ordinal `n`.
    fn call_at(&self, n: u64) -> Option<&Call> {
        self.calls.iter().find(|c| (c.from..=c.to).contains(&n))
    }
}

/// Run `f`, catching a panic, and record what it returned.
fn call<T, E>(
    acked: &mut Acked,
    shared: &Shared,
    what: &'static str,
    f: impl FnOnce() -> Result<T, E>,
) -> Option<T> {
    let from = shared.ops() + 1;
    let result = catch_unwind(AssertUnwindSafe(f));
    let ok = matches!(result, Ok(Ok(_)));
    acked.calls.push(Call {
        what,
        from,
        to: shared.ops(),
        ok,
    });
    match result {
        Ok(Ok(value)) => Some(value),
        _ => None,
    }
}

/// Create the journal over `fs`, persist one stream mapping, and append
/// the genesis group and then one group a record, stopping at the first
/// error or panic. Panics are caught: the pinned engine panics on a failed
/// sync (`expect("pipe::sync()")`), and a frozen file system fails every
/// sync after the crash.
fn workload(root: &Path, fs: Arc<FaultFs>, shared: &Shared) -> Acked {
    let mut acked = Acked::default();
    let journal = call(&mut acked, shared, "create", || {
        RaftEngineJournal::create_with_file_system(root, identity(), &options(), fs)
    });
    let Some(mut journal) = journal else {
        return acked;
    };
    // An acknowledgement after the crash was never given: the process
    // died before it.
    if shared.is_frozen() {
        quietly_drop(journal);
        return acked;
    }
    acked.created = true;
    let mut allocator = StreamAllocator::new();
    let mapping = allocator.allocate(key(), ShardId::new(0).unwrap()).unwrap();
    let persisted = call(&mut acked, shared, "mapping", || {
        journal.persist_mapping(allocator.high_water(), &mapping)
    });
    if persisted.is_none() || shared.is_frozen() {
        quietly_drop(journal);
        return acked;
    }
    acked.stream = Some(mapping.stream);
    let all = records(mapping.stream);
    // The genesis and the first record together, then one record a group.
    let mut batches = vec![all[..2].to_vec()];
    batches.extend(all[2..].iter().map(|r| vec![r.clone()]));
    for (i, batch) in batches.into_iter().enumerate() {
        let last = batch.last().unwrap().seq();
        acked.attempted = Some(last);
        let g = group(i as u64 + 1, mapping.stream, batch);
        match call(&mut acked, shared, "group", || journal.append_group(&g)) {
            Some(_) if !shared.is_frozen() => acked.through = Some(last),
            _ => break,
        }
    }
    quietly_drop(journal);
    acked
}

fn quietly_drop<T>(value: T) {
    let _ = catch_unwind(AssertUnwindSafe(move || drop(value)));
}

/// Reopen over the files as they now are, with the real file system, and
/// check what the journal holds against what was acknowledged.
fn check(root: &Path, acked: &Acked, case: &str) {
    let opened = RaftEngineJournal::open_existing(root, identity(), &options());
    let journal = match opened {
        Ok(journal) => journal,
        Err(e) => {
            // Only a journal whose creation was never acknowledged may be
            // missing or unreadable.
            assert!(!acked.created, "{case}: created journal refused: {e:?}");
            return;
        }
    };
    let Some(stream) = acked.stream else {
        // The mapping was never acknowledged; whether it is there or not,
        // no record was.
        return;
    };
    let mappings = journal.mappings().unwrap();
    let Some(_) = mappings.1.iter().find(|m| m.stream == stream) else {
        panic!("{case}: acknowledged mapping lost");
    };
    let held = read_all(&journal, stream);
    let mut expect = RecordExpectation::genesis(origin(stream));
    for r in &held {
        r.verify(&expect)
            .unwrap_or_else(|e| panic!("{case}: chain broken at {:?}: {e:?}", r.seq()));
        expect = expect.after(r).unwrap();
    }
    let head = held.last().map(|r| r.seq());
    if let Some(through) = acked.through {
        assert!(
            head.is_some_and(|h| h >= through),
            "{case}: acknowledged through {through:?}, reopened head {head:?}"
        );
    }
    match acked.attempted {
        Some(attempted) => assert!(
            head.is_none_or(|h| h <= attempted),
            "{case}: head {head:?} past anything attempted ({attempted:?})"
        ),
        None => assert!(head.is_none(), "{case}: records nobody appended"),
    }
}

/// The ordinals and kinds of a fault-free run, to choose fault points from.
fn census() -> Vec<Op> {
    let dir = tempfile::tempdir().unwrap();
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let acked = workload(&dir.path().join("j"), fs, &shared);
    assert_eq!(
        acked.through,
        Some(LocalJournalSeq::new(u64::from(GROUPS) + 1).unwrap())
    );
    shared
        .log()
        .into_iter()
        .filter(|op| !matches!(op, Op::Rejected | Op::Failed))
        .collect()
}

fn run(plan: FaultPlan, tail: Option<Tail>, case: &str) -> (Arc<Shared>, Acked) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (fs, shared) = FaultFs::new(plan);
    let acked = workload(&root, fs, &shared);
    if let Some(tail) = tail {
        // A scripted crash halted the device before the journal's close
        // could sync what the fault left behind.
        if plan.crash_after.is_some() {
            assert!(
                shared.is_frozen(),
                "{case}: the crash point was never reached"
            );
        }
        shared.materialize(tail).unwrap();
    }
    check(&root, &acked, case);
    (shared, acked)
}

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
    let mut refused = 0;
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
                let case = format!("write {n} fails after {prefix} bytes, then {tail:?}");
                let (shared, acked) = run(plan, tail, &case);
                assert!(shared.log().contains(&Op::Failed), "write {n} not reached");
                // The call that issued the failed write never returned
                // `Ok`: with the crash at the same ordinal, recovery is
                // checked only against earlier acknowledgements, which
                // would pass an engine that swallowed the error (#210
                // review).
                if let Some(c) = acked.call_at(n) {
                    assert!(!c.ok, "{case}: the {} call returned Ok", c.what);
                    refused += 1;
                }
            }
        }
    }
    assert!(refused > 0, "no failed write fell inside a call");
}

#[test]
fn enospc_never_acknowledges_and_loses_nothing_acknowledged() {
    let ops = census();
    let mut refused = 0;
    let total = ops.len() as u64;
    for (i, op) in ops.iter().enumerate() {
        if !matches!(
            op,
            Op::Write { .. } | Op::SetLen { .. } | Op::Allocate { .. }
        ) {
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
                let case = format!("ENOSPC over {window:?} of {total}, then {tail:?}");
                let (_, acked) = run(plan, tail, &case);
                // Nor the call whose write ENOSPC refused (#210 review). A
                // refused length change is raft-engine's preallocation,
                // which it logs and writes past (`LogFileWriter::write`),
                // so that call may still return `Ok`: what it
                // acknowledged is what recovery is checked against.
                if matches!(op, Op::Write { .. })
                    && let Some(c) = acked.call_at(n)
                {
                    assert!(!c.ok, "{case}: the {} call returned Ok", c.what);
                    refused += 1;
                }
            }
        }
    }
    assert!(refused > 0, "no refused write fell inside a call");
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
                let case =
                    format!("sync {n} fails persisting {persisted:?}, crash keeps {after:?}");
                let (shared, acked) = run(plan, Some(after), &case);
                assert!(shared.log().contains(&Op::Failed), "sync {n} not reached");
                // The call that issued the failed sync never returned `Ok`,
                // frozen or not: the crash at the same ordinal would
                // otherwise hide an engine that swallowed the error.
                if let Some(c) = acked.call_at(n) {
                    assert!(!c.ok, "{case}: the {} call returned Ok", c.what);
                }
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
        // The lie did buy an acknowledgement: the call that issued it
        // returned `Ok`, after the crash, so the check rightly counts it
        // as never given. Only the close's sync belongs to no call.
        if let Some(c) = acked.call_at(n) {
            assert!(c.ok, "lie at {n}: the {} call didn't return Ok", c.what);
        }
    }
    // Crashing right after the lie means the acknowledgement it bought is
    // never observed. So crash one operation later instead: the lied-about
    // group's acknowledgement has returned, and the next real sync, which
    // would persist its bytes too, never runs.
    let mut caught_later = 0;
    let mut uncaught = Vec::new();
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
        } else {
            uncaught.push((n, acked.call_at(n).map(|c| c.what), acked.calls.len()));
        }
    }
    // The lies that lose nothing, named: syncs no call issued (the close's,
    // which acknowledge nothing), and at most the last group's, whose bytes
    // a close's sync would persist. Any other is a blind spot.
    let last_group = uncaught
        .iter()
        .filter(|(_, what, _)| *what == Some("group"))
        .count();
    let close = uncaught
        .iter()
        .filter(|(_, what, _)| what.is_none())
        .count();
    assert!(
        last_group <= 1 && last_group + close == uncaught.len() && uncaught.len() <= 2,
        "uncaught lies other than the last group's and the close's: {uncaught:?}"
    );
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
    use coord_journal_raft_engine::DirSync;
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
        fs.sync_dir(dir.path()).unwrap();
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
    use coord_journal_raft_engine::DirSync;
    use raft_engine::env::{FileSystem, Handle};
    use std::io::{Seek, SeekFrom, Write};

    for seed in 0..16 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let plan = FaultPlan {
            // Ordinal 1 is the create, 2 the directory sync, 3 and 4 the
            // writes, 5 the sync.
            fail_sync_at: Some(5),
            tail: Tail::All,
            ..FaultPlan::default()
        };
        let (fs, shared) = FaultFs::new(plan);
        let handle = Arc::new(fs.create(&path).unwrap());
        fs.sync_dir(dir.path()).unwrap();
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

/// A created file's entry needs a directory sync, and a renamed one's
/// too: until one covers it, a crash loses the created file and puts the
/// renamed one back under its old name, however much of their bytes were
/// synced (#210 review).
#[test]
fn an_entry_no_directory_sync_covered_is_undone_at_a_crash() {
    use coord_journal_raft_engine::DirSync;
    use raft_engine::env::{FileSystem, Handle};
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let handle = Arc::new(fs.create(&a).unwrap());
    let mut writer = fs.new_writer(handle.clone()).unwrap();
    writer.write_all(b"bytes").unwrap();
    handle.sync().unwrap();
    assert_eq!(shared.unsynced_entries(), vec![a.clone()]);
    shared.materialize(Tail::All).unwrap();
    assert!(!a.exists(), "a created file outlived its unsynced entry");

    let (fs, shared) = FaultFs::new(FaultPlan::default());
    let handle = Arc::new(fs.create(&a).unwrap());
    let mut writer = fs.new_writer(handle.clone()).unwrap();
    writer.write_all(b"bytes").unwrap();
    handle.sync().unwrap();
    fs.sync_dir(dir.path()).unwrap();
    assert!(shared.unsynced_entries().is_empty());
    fs.rename(&a, &b).unwrap();
    assert_eq!(shared.unsynced_entries(), vec![b.clone()]);
    shared.materialize(Tail::All).unwrap();
    assert!(!b.exists(), "a rename outlived its unsynced entry");
    assert_eq!(std::fs::read(&a).unwrap(), b"bytes");
}

/// A directory sync the device lied about leaves the entries as they
/// were: the lie is caught by the check, so the matrix depends on the
/// adapter's directory sync and not on a kind model.
#[test]
fn a_directory_sync_the_device_lied_about_loses_the_created_journal() {
    let ops = census();
    let dir_syncs: Vec<u64> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| matches!(op, Op::DirSync { .. }))
        .map(|(i, _)| i as u64 + 1)
        .collect();
    // The journal's directory, then the directory that held it.
    assert_eq!(dir_syncs.len(), 2, "{ops:?}");
    let total = ops.len() as u64;
    let plan = FaultPlan {
        lie_on_dir_sync_at: Some(dir_syncs[0]),
        // Crash once everything was acknowledged, keeping every byte: only
        // the entry is lost.
        crash_after: Some(total),
        ..FaultPlan::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (fs, shared) = FaultFs::new(plan);
    let acked = workload(&root, fs, &shared);
    assert!(acked.created && acked.through.is_some());
    shared.materialize(Tail::All).unwrap();
    let caught = catch_unwind(AssertUnwindSafe(|| check(&root, &acked, "lying dir sync")));
    assert!(caught.is_err(), "a lost first log file went unnoticed");
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
