//! Fixtures shared by the journal fault kit's test files.
#![allow(dead_code)]

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
pub const GROUPS: u8 = 6;

// ---------------------------------------------------------------------------
// Fixtures (as in coord-journal-raft-engine's own tests)
// ---------------------------------------------------------------------------

pub fn identity() -> JournalIdentity {
    JournalIdentity {
        cluster: ClusterId([1; 16]),
        replica: ReplicaId([7; 16]),
    }
}

pub fn options() -> JournalOptions {
    JournalOptions {
        target_file_size_bytes: 1024 * 1024,
        purge_threshold_bytes: 8 * 1024 * 1024,
        ..JournalOptions::default()
    }
}

pub fn key() -> StreamKey {
    StreamKey {
        cluster: ClusterId([1; 16]),
        domain: DomainId([1; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

pub fn origin(stream: StorageStreamId) -> RecordOrigin {
    let k = key();
    RecordOrigin {
        cluster: k.cluster,
        domain: k.domain,
        replica: ReplicaId([7; 16]),
        incarnation: k.incarnation,
        stream,
    }
}

pub fn barrier(n: u64) -> BarrierId {
    BarrierId {
        node_generation: ReplicaIncarnation::new(1).unwrap(),
        boot_id: BootId([0xb0; 16]),
        sequence: n,
    }
}

pub fn transition(tag: u8, size: usize) -> RecordBody {
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
pub fn records(stream: StorageStreamId) -> Vec<JournalRecordV1> {
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

pub fn group(n: u64, stream: StorageStreamId, records: Vec<JournalRecordV1>) -> GroupWrite {
    let mut g = GroupWrite::new(GroupLimits::DEFAULT);
    g.push(GroupEntry::new(barrier(n), stream, records).unwrap())
        .unwrap();
    g
}

pub fn read_all(journal: &impl JournalEngine, stream: StorageStreamId) -> Vec<JournalRecordV1> {
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
pub struct Acked {
    pub created: bool,
    pub stream: Option<StorageStreamId>,
    /// Highest sequence an `append_group` returned `Ok` for.
    pub through: Option<LocalJournalSeq>,
    /// Highest sequence any `append_group` was called with.
    pub attempted: Option<LocalJournalSeq>,
    /// What every call returned, frozen or not (#210 review): the fields
    /// above count only acknowledgements given before the crash, so a test
    /// asking whether a fault was acknowledged reads these.
    pub calls: Vec<Call>,
}

/// One call of the workload: the operations it issued and whether it
/// returned `Ok`.
#[derive(Debug)]
pub struct Call {
    pub what: &'static str,
    /// The ordinals the call's operations took, `from..=to`; empty when it
    /// issued none.
    pub from: u64,
    pub to: u64,
    pub ok: bool,
}

impl Acked {
    /// The call whose operations include ordinal `n`.
    pub fn call_at(&self, n: u64) -> Option<&Call> {
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
pub fn workload(root: &Path, fs: Arc<FaultFs>, shared: &Shared) -> Acked {
    workload_with(root, fs, shared, &options())
}

/// [`workload`] under `options`.
pub fn workload_with(
    root: &Path,
    fs: Arc<FaultFs>,
    shared: &Shared,
    options: &JournalOptions,
) -> Acked {
    let mut acked = Acked::default();
    let journal = call(&mut acked, shared, "create", || {
        RaftEngineJournal::create_with_file_system(root, identity(), options, fs)
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

pub fn quietly_drop<T>(value: T) {
    let _ = catch_unwind(AssertUnwindSafe(move || drop(value)));
}

/// Reopen over the files as they now are, with the real file system, and
/// check what the journal holds against what was acknowledged.
pub fn check(root: &Path, acked: &Acked, case: &str) {
    check_with(root, acked, case, &options());
}

/// [`check`] under `options` (its recovery policy among them).
pub fn check_with(root: &Path, acked: &Acked, case: &str, options: &JournalOptions) {
    let opened = RaftEngineJournal::open_existing(root, identity(), options);
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
pub fn census() -> Vec<Op> {
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

pub fn run(plan: FaultPlan, tail: Option<Tail>, case: &str) -> (Arc<Shared>, Acked) {
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
