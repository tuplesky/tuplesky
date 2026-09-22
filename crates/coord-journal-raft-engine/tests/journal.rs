//! Codec round trips and adversarial inputs, sync-before-success, stream
//! order and isolation, real multi-group batches with the byte-count-to-
//! barrier mapping, retirement under a durable pointer, fail-closed
//! lifecycle, and fail-stop after the engine's in-write sync panic.

use std::io::{Read, Result as IoResult, Seek, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_journal_api::engine::{JournalEngine, ReadBudget};
use coord_journal_api::failure::{JournalErrorClass, JournalFailure};
use coord_journal_api::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1};
use coord_journal_api::group::{GroupEntry, GroupLimits, GroupWrite};
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1,
    MAX_RECORD_BYTES, RecordBody, RecordDraft, RecordExpectation, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{
    ShardId, StorageStreamId, StreamAllocator, StreamHighWater, StreamKey, StreamMappingV1,
};
use coord_journal_raft_engine::{
    JournalIdentity, JournalOptions, OpenError, RaftEngineJournal, RecordCodec,
};
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};
use raft_engine::ValueCodec;
use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission};

// ---------------------------------------------------------------------------
// Fault file system: counts syncs and can make every sync fail, which at
// the pinned revision panics inside a nonempty `Engine::write`.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Faults {
    fail_sync: AtomicBool,
    syncs: AtomicU64,
}

struct FaultFs {
    inner: DefaultFileSystem,
    faults: Arc<Faults>,
}

struct FaultHandle {
    inner: Arc<<DefaultFileSystem as FileSystem>::Handle>,
    faults: Arc<Faults>,
}

impl Handle for FaultHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.inner.truncate(offset)
    }
    fn file_size(&self) -> IoResult<usize> {
        self.inner.file_size()
    }
    fn sync(&self) -> IoResult<()> {
        self.faults.syncs.fetch_add(1, Ordering::SeqCst);
        if self.faults.fail_sync.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected sync failure"));
        }
        self.inner.sync()
    }
}

impl FileSystem for FaultFs {
    type Handle = FaultHandle;
    type Reader = <DefaultFileSystem as FileSystem>::Reader;
    type Writer = <DefaultFileSystem as FileSystem>::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        Ok(FaultHandle {
            inner: Arc::new(self.inner.create(path)?),
            faults: self.faults.clone(),
        })
    }
    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> IoResult<Self::Handle> {
        Ok(FaultHandle {
            inner: Arc::new(self.inner.open(path, perm)?),
            faults: self.faults.clone(),
        })
    }
    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        self.inner.delete(path)
    }
    fn rename<P: AsRef<Path>>(&self, src: P, dst: P) -> IoResult<()> {
        self.inner.rename(src, dst)
    }
    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        self.inner.new_reader(handle.inner.clone())
    }
    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        self.inner.new_writer(handle.inner.clone())
    }
}

// Keep the unused-trait warnings away for Reader/Writer bounds.
#[allow(dead_code)]
fn _bounds<R: Read + Seek, W: Write + Seek>() {}

// ---------------------------------------------------------------------------
// Fixtures
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

fn transition(tag: u8, size: usize) -> RecordBody {
    RecordBody::ProtocolTransition {
        context: context(),
        updates: vec![StoreUpdate {
            collection: Collection::ProtocolV1.id(),
            key: vec![tag],
            value: Some(vec![tag; size]),
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
    after_seq: LocalJournalSeq,
    after_digest: Digest32,
    bodies: Vec<RecordBody>,
) -> Vec<JournalRecordV1> {
    let mut out = Vec::new();
    let mut seq = after_seq;
    let mut pred = after_digest;
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

/// A stream handle for tests: tracks the head/digest the caller knows.
struct Stream {
    id: StorageStreamId,
    origin: RecordOrigin,
    seq: LocalJournalSeq,
    digest: Digest32,
}

impl Stream {
    fn next(&mut self, bodies: Vec<RecordBody>) -> Vec<JournalRecordV1> {
        let records = if self.seq == LocalJournalSeq::ZERO {
            let g = genesis(self.origin);
            let mut v = vec![g.clone()];
            v.extend(chain(self.origin, g.seq(), g.digest(), bodies));
            v
        } else {
            chain(self.origin, self.seq, self.digest, bodies)
        };
        let last = records.last().unwrap();
        self.seq = last.seq();
        self.digest = last.digest();
        records
    }
}

/// Create a journal with durable mappings for the given domains.
fn create<F: FileSystem>(
    dir: &Path,
    fs: Arc<F>,
    domains: &[u8],
) -> (RaftEngineJournal<F>, StreamAllocator, Vec<Stream>) {
    let mut journal =
        RaftEngineJournal::create_with_file_system(dir, identity(), &options(), fs).unwrap();
    let mut allocator = StreamAllocator::new();
    let mut streams = Vec::new();
    for &d in domains {
        let mapping = allocator
            .allocate(key(d), ShardId::new(0).unwrap())
            .unwrap();
        journal
            .persist_mapping(allocator.high_water(), &mapping)
            .unwrap();
        allocator.mapping_durable(mapping.stream).unwrap();
        streams.push(Stream {
            id: mapping.stream,
            origin: origin(d, mapping.stream),
            seq: LocalJournalSeq::ZERO,
            digest: GENESIS_PREDECESSOR,
        });
    }
    (journal, allocator, streams)
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
// Codec
// ---------------------------------------------------------------------------

#[test]
fn codec_round_trips_and_rejects_adversarial_entries() {
    let s = StorageStreamId::FIRST;
    let g = genesis(origin(1, s));
    let r = chain(origin(1, s), g.seq(), g.digest(), vec![transition(1, 16)]).remove(0);
    let mut buf = b"prefix".to_vec();
    RecordCodec::encode_to(&r, &mut buf).unwrap();
    assert_eq!(&buf[..6], b"prefix", "encode_to appends only");
    let bytes = buf[6..].to_vec();
    assert_eq!(RecordCodec::decode(&bytes).unwrap(), r);
    assert_eq!(RecordCodec::encode_to_vec(&r).unwrap(), bytes);

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        RecordCodec::decode(&trailing),
        Err(raft_engine::Error::Corruption(_))
    ));
    for cut in 0..bytes.len() {
        assert!(RecordCodec::decode(&bytes[..cut]).is_err(), "cut {cut}");
    }
    for i in 0..bytes.len() {
        let mut t = bytes.clone();
        t[i] ^= 0x40;
        assert!(RecordCodec::decode(&t).is_err(), "flip {i}");
    }
    assert!(RecordCodec::decode(&vec![0u8; MAX_RECORD_BYTES + 1]).is_err());
    assert!(RecordCodec::decode(&[]).is_err());
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[test]
fn lifecycle_is_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, identity(), &options()),
        Err(OpenError::NotInitialized)
    ));
    let journal = RaftEngineJournal::create(&root, identity(), &options()).unwrap();
    assert!(matches!(
        RaftEngineJournal::create(&root, identity(), &options()),
        Err(OpenError::AlreadyInitialized)
    ));
    // A second opener is refused while the first holds the directory.
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, identity(), &options()),
        Err(OpenError::Busy)
    ));
    drop(journal);
    let other = JournalIdentity {
        replica: ReplicaId([8; 16]),
        ..identity()
    };
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, other, &options()),
        Err(OpenError::IdentityMismatch("replica"))
    ));
    let other_cluster = JournalIdentity {
        cluster: ClusterId([2; 16]),
        ..identity()
    };
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, other_cluster, &options()),
        Err(OpenError::IdentityMismatch("cluster"))
    ));
    // A directory with unrelated contents is never a journal to create in.
    let junk = dir.path().join("junk");
    std::fs::create_dir_all(&junk).unwrap();
    std::fs::write(junk.join("file"), b"x").unwrap();
    assert!(matches!(
        RaftEngineJournal::create(&junk, identity(), &options()),
        Err(OpenError::AlreadyInitialized)
    ));
    assert!(matches!(
        RaftEngineJournal::open_existing(&junk, identity(), &options()),
        Err(OpenError::NotInitialized | OpenError::Corrupt(_) | OpenError::Engine(_))
    ));
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    assert_eq!(reopened.identity().unwrap(), identity());
    assert_eq!(reopened.mappings().unwrap().0, StreamHighWater::NONE);
}

#[test]
fn open_existing_on_an_empty_directory_leaves_it_creatable() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("empty");
    std::fs::create_dir_all(&root).unwrap();
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, identity(), &options()),
        Err(OpenError::NotInitialized)
    ));
    // A refused open must not have written engine files into the directory.
    assert!(
        std::fs::read_dir(&root).unwrap().next().is_none(),
        "open-existing wrote into an uninitialized directory"
    );
    let journal = RaftEngineJournal::create(&root, identity(), &options()).unwrap();
    assert_eq!(journal.identity().unwrap(), identity());
    drop(journal);
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    assert_eq!(reopened.identity().unwrap(), identity());
}

#[cfg(unix)]
#[test]
fn non_utf8_directory_is_refused_without_touching_the_disk() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(OsStr::from_bytes(b"j\xff"));
    assert!(matches!(
        RaftEngineJournal::create(&root, identity(), &options()),
        Err(OpenError::InvalidOptions(_))
    ));
    assert!(!root.exists(), "create made a directory it could not open");
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, identity(), &options()),
        Err(OpenError::InvalidOptions(_))
    ));
    // The lossy spelling of the path must not have been used instead.
    assert!(
        std::fs::read_dir(dir.path()).unwrap().next().is_none(),
        "a lossy-converted path was created"
    );
}

#[test]
fn mapping_is_durable_before_use_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, mut allocator, streams) = create(&root, Arc::new(DefaultFileSystem), &[1, 2]);
    let unmapped = StorageStreamId::from_durable(9).unwrap();
    let err = journal
        .append_group(&group(vec![(
            barrier(1),
            unmapped,
            vec![genesis(origin(3, unmapped))],
        )]))
        .unwrap_err();
    assert!(err.is_definite());
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
    // A mapping cannot be rewritten to another identity or exceed the mark.
    let m = allocator.usable(streams[0].id).unwrap();
    let changed = StreamMappingV1 { key: key(5), ..*m };
    assert!(
        journal
            .persist_mapping(allocator.high_water(), &changed)
            .unwrap_err()
            .is_definite()
    );
    let above = StreamMappingV1 {
        stream: StorageStreamId::from_durable(50).unwrap(),
        key: key(6),
        ..*m
    };
    assert!(
        journal
            .persist_mapping(allocator.high_water(), &above)
            .unwrap_err()
            .is_definite()
    );
    let dup_key = StreamMappingV1 {
        stream: StorageStreamId::from_durable(3).unwrap(),
        key: key(1),
        ..*m
    };
    assert!(
        journal
            .persist_mapping(StreamHighWater::from_durable(3), &dup_key)
            .unwrap_err()
            .is_definite()
    );
    // Retirement is permanent: the retired row cannot be rewritten as
    // active, and the stream stays closed to appends.
    let retired = allocator.retire(streams[1].id).unwrap();
    assert!(retired.retired);
    journal
        .persist_mapping(allocator.high_water(), &retired)
        .unwrap();
    let revived = StreamMappingV1 {
        retired: false,
        ..retired
    };
    let err = journal
        .persist_mapping(allocator.high_water(), &revived)
        .unwrap_err();
    assert!(err.is_definite());
    assert!(
        journal
            .persist_mapping(allocator.high_water(), &retired)
            .is_ok(),
        "rewriting the retired row unchanged is allowed"
    );
    let err = journal
        .append_group(&group(vec![(
            barrier(1),
            streams[1].id,
            vec![genesis(streams[1].origin)],
        )]))
        .unwrap_err();
    assert!(err.is_definite());
    drop(journal);
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    let (hw, mappings) = reopened.mappings().unwrap();
    assert_eq!(hw, allocator.high_water());
    assert_eq!(mappings, allocator.mappings());
    assert!(
        mappings
            .iter()
            .any(|m| m.stream == streams[1].id && m.retired)
    );
    assert_eq!(
        reopened.durable_head(streams[0].id).unwrap(),
        LocalJournalSeq::ZERO
    );
    assert!(reopened.durable_head(unmapped).is_err());
}

/// A well-framed record below the head that breaks the predecessor chain
/// passes the engine's checksums; the open-time walk over the retained
/// suffix must still refuse it rather than vouch for the intact head.
#[test]
fn reopen_refuses_a_chain_break_below_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, _, mut streams) = create(&root, Arc::new(DefaultFileSystem), &[1]);
    let s = &mut streams[0];
    let records = s.next(vec![transition(1, 8), transition(2, 8), transition(3, 8)]);
    journal
        .append_group(&group(vec![(barrier(1), s.id, records.clone())]))
        .unwrap();
    drop(journal);
    // The untouched journal reopens with its head.
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    assert_eq!(reopened.durable_head(s.id).unwrap(), s.seq);
    drop(reopened);

    // Replace entry 2 with a correctly sealed record for the same position
    // and predecessor but another body, and keep the original 3 and 4: the
    // head and every digest are self-consistent, only the link from 3 to
    // the forged 2 is broken.
    let forged = JournalRecordV1::seal(RecordDraft {
        origin: s.origin,
        seq: records[1].seq(),
        predecessor: records[0].digest(),
        body: transition(9, 8),
    })
    .unwrap();
    assert_ne!(forged.digest(), records[1].digest());
    {
        let engine = raft_engine::Engine::open(raft_engine::Config {
            dir: root.to_str().unwrap().to_owned(),
            target_file_size: raft_engine::ReadableSize(options().target_file_size_bytes),
            purge_threshold: raft_engine::ReadableSize(options().purge_threshold_bytes),
            ..raft_engine::Config::default()
        })
        .unwrap();
        let mut batch = raft_engine::LogBatch::default();
        batch
            .add_entries_with::<coord_journal_raft_engine::RecordExt, RecordCodec>(
                s.id.get(),
                &[forged, records[2].clone(), records[3].clone()],
            )
            .unwrap();
        engine.write(&mut batch, true).unwrap();
        assert_eq!(engine.last_index(s.id.get()), Some(s.seq.get()));
    }
    assert!(matches!(
        RaftEngineJournal::open_existing(&root, identity(), &options()),
        Err(OpenError::Corrupt(_))
    ));
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

#[test]
fn multi_group_batch_syncs_once_and_maps_bytes_to_barriers() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let faults = Arc::new(Faults::default());
    let fs = Arc::new(FaultFs {
        inner: DefaultFileSystem,
        faults: faults.clone(),
    });
    let (mut journal, _, mut streams) = create(&root, fs, &[1, 2, 3]);
    let before = faults.syncs.load(Ordering::SeqCst);
    let stats_before = journal.stats().unwrap();
    let e1 = streams[0].next(vec![transition(1, 8), transition(2, 8)]);
    let e2 = streams[1].next(vec![]);
    let e3 = streams[2].next(vec![transition(3, 40_000)]);
    let g = group(vec![
        (barrier(1), streams[0].id, e1),
        (barrier(2), streams[1].id, e2),
        (barrier(3), streams[2].id, e3),
    ]);
    let receipt = journal.append_group(&g).unwrap();
    // One synced engine write for the whole group.
    assert!(faults.syncs.load(Ordering::SeqCst) > before);
    let stats = journal.stats().unwrap();
    assert_eq!(stats.groups, stats_before.groups + 1);
    assert_eq!(stats.syncs, stats_before.syncs + 1);
    assert_eq!(stats.records, stats_before.records + 6);
    assert_eq!(stats.bytes, stats_before.bytes + receipt.written.0);
    // The byte count is evidence (batches above the engine's compression
    // threshold are compressed on disk); completions are the caller's
    // sequences.
    assert!(receipt.written.0 > 0);
    assert!(
        receipt
            .completions
            .iter()
            .all(|c| c.last.get() != receipt.written.0)
    );
    assert_eq!(receipt.completions.len(), 3);
    assert_eq!(receipt.completions[0].barrier, barrier(1));
    assert_eq!(receipt.completions[0].stream, streams[0].id);
    assert_eq!(receipt.completions[0].first.get(), 1);
    assert_eq!(receipt.completions[0].last.get(), 3);
    assert_eq!(receipt.completions[1].last.get(), 1);
    assert_eq!(receipt.completions[2].barrier, barrier(3));
    assert_eq!(receipt.completions[2].last.get(), 2);
    assert_eq!(journal.recent_receipts().unwrap(), vec![receipt.clone()]);
    for s in &streams {
        assert_eq!(journal.durable_head(s.id).unwrap(), s.seq);
    }
    // Every stream reads back its own records, in order, verified.
    for s in &streams {
        let records = read_all(&journal, s.id);
        assert_eq!(records.last().unwrap().seq(), s.seq);
        assert_eq!(records.last().unwrap().digest(), s.digest);
        let mut expect = RecordExpectation::genesis(s.origin);
        for r in &records {
            r.verify(&expect).unwrap();
            expect = expect.after(r).unwrap();
        }
    }
    // The large-record path is a separate bounded group.
    let big = streams[2].next(vec![RecordBody::ProtocolTransition {
        context: context(),
        updates: (0..3u8)
            .map(|i| StoreUpdate {
                collection: Collection::ProtocolV1.id(),
                key: vec![9, i],
                value: Some(vec![i; 1024 * 1024]),
            })
            .collect(),
    }]);
    let mut large = GroupWrite::new(GroupLimits::LARGE_RECORD);
    large
        .push(GroupEntry::new(barrier(4), streams[2].id, big).unwrap())
        .unwrap();
    let receipt = journal.append_group(&large).unwrap();
    assert!(receipt.written.0 > 0);
    assert_eq!(read_all(&journal, streams[2].id).len(), 3);
}

#[test]
fn stale_or_foreign_entries_are_rejected_before_append_and_streams_stay_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, _, mut streams) = create(&root, Arc::new(DefaultFileSystem), &[1, 2]);
    let g1 = streams[0].next(vec![transition(1, 8)]);
    journal
        .append_group(&group(vec![(barrier(1), streams[0].id, g1.clone())]))
        .unwrap();
    let stats = journal.stats().unwrap();
    // Replaying the same records: index mismatch, definite.
    let err = journal
        .append_group(&group(vec![(barrier(2), streams[0].id, g1.clone())]))
        .unwrap_err();
    assert!(matches!(err, JournalFailure::Definite(_)));
    // A record of domain 2 sealed for stream 1's origin: origin mismatch.
    let foreign = chain(
        origin(2, streams[0].id),
        streams[0].seq,
        streams[0].digest,
        vec![transition(5, 8)],
    );
    assert!(
        journal
            .append_group(&group(vec![(barrier(3), streams[0].id, foreign)]))
            .unwrap_err()
            .is_definite()
    );
    // A group with one bad entry appends nothing: stream 2 stays empty.
    let good2 = streams[1].next(vec![transition(2, 8)]);
    let bad1 = chain(
        streams[0].origin,
        LocalJournalSeq::new(9).unwrap(),
        streams[0].digest,
        vec![transition(1, 8)],
    );
    assert!(
        journal
            .append_group(&group(vec![
                (barrier(4), streams[1].id, good2.clone()),
                (barrier(5), streams[0].id, bad1),
            ]))
            .unwrap_err()
            .is_definite()
    );
    assert_eq!(
        journal.durable_head(streams[1].id).unwrap(),
        LocalJournalSeq::ZERO
    );
    assert_eq!(journal.stats().unwrap(), stats, "no write was submitted");
    assert!(!journal.is_fail_stopped());
    // Stream 2 then proceeds independently, and stream 1 is untouched.
    journal
        .append_group(&group(vec![(barrier(6), streams[1].id, good2)]))
        .unwrap();
    assert_eq!(journal.durable_head(streams[1].id).unwrap(), streams[1].seq);
    assert_eq!(journal.durable_head(streams[0].id).unwrap(), streams[0].seq);
    let more1 = streams[0].next(vec![transition(7, 8), transition(8, 8)]);
    journal
        .append_group(&group(vec![(barrier(7), streams[0].id, more1)]))
        .unwrap();
    let r1 = read_all(&journal, streams[0].id);
    let r2 = read_all(&journal, streams[1].id);
    assert_eq!(
        r1.iter().map(|r| r.seq().get()).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        r2.iter().map(|r| r.seq().get()).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(r1.iter().all(|r| r.origin().domain == DomainId([1; 16])));
    assert!(r2.iter().all(|r| r.origin().domain == DomainId([2; 16])));
    // Reopen recovers exactly the same heads and records.
    drop(journal);
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    assert_eq!(
        reopened.durable_head(streams[0].id).unwrap(),
        streams[0].seq
    );
    assert_eq!(
        reopened.durable_head(streams[1].id).unwrap(),
        streams[1].seq
    );
    assert_eq!(read_all(&reopened, streams[0].id), r1);
    assert_eq!(read_all(&reopened, streams[1].id), r2);
    // Bounded suffix reads honor the byte budget and never lie about EOF.
    let page = reopened
        .read_suffix(streams[0].id, LocalJournalSeq::ZERO, ReadBudget::new(10, 1))
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(!page.exhausted);
    let page = reopened
        .read_suffix(streams[0].id, streams[0].seq, ReadBudget::new(10, 1))
        .unwrap();
    assert!(page.records.is_empty() && page.exhausted);
}

// ---------------------------------------------------------------------------
// Retirement
// ---------------------------------------------------------------------------

#[test]
fn retirement_requires_the_durable_pointer_and_keeps_the_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, _, mut streams) = create(&root, Arc::new(DefaultFileSystem), &[1]);
    let s = &mut streams[0];
    let pointer = CheckpointPointerV1 {
        origin: s.origin,
        represented: LocalJournalSeq::new(2).unwrap(),
        format: LOCAL_CHECKPOINT_FORMAT_V1,
        manifest_digest: Digest32([5; 32]),
        checkpoint_id: Digest32([6; 32]),
    };
    // No publication yet: refused.
    let first = s.next(vec![transition(1, 8)]);
    journal
        .append_group(&group(vec![(barrier(1), s.id, first)]))
        .unwrap();
    assert!(
        journal
            .retire_prefix(s.id, &pointer)
            .unwrap_err()
            .is_definite()
    );
    let published = s.next(vec![
        RecordBody::PublishLocalCheckpoint(pointer),
        transition(3, 8),
    ]);
    journal
        .append_group(&group(vec![(barrier(2), s.id, published)]))
        .unwrap();
    // A different pointer than the durable publication is refused.
    let other = CheckpointPointerV1 {
        manifest_digest: Digest32([0; 32]),
        ..pointer
    };
    assert!(
        journal
            .retire_prefix(s.id, &other)
            .unwrap_err()
            .is_definite()
    );
    let syncs = journal.stats().unwrap().syncs;
    journal.retire_prefix(s.id, &pointer).unwrap();
    assert_eq!(
        journal.stats().unwrap().syncs,
        syncs + 1,
        "compaction is synced"
    );
    assert_eq!(journal.stats().unwrap().compactions, 1);
    // Idempotent.
    journal.retire_prefix(s.id, &pointer).unwrap();
    assert_eq!(journal.stats().unwrap().compactions, 1);
    let remaining: Vec<u64> = read_all_from(&journal, s.id, pointer.represented)
        .iter()
        .map(|r| r.seq().get())
        .collect();
    assert_eq!(remaining, vec![3, 4]);
    // Reading below the retired prefix is a gap, never an empty page.
    let err = journal
        .read_suffix(s.id, LocalJournalSeq::ZERO, ReadBudget::new(8, u32::MAX))
        .unwrap_err();
    assert_eq!(err.class, JournalErrorClass::Corrupt);
    assert_eq!(journal.durable_head(s.id).unwrap(), s.seq);
    // The engine's purge suggestions are diagnostics only.
    let suggestions = journal.maintain().unwrap();
    assert!(suggestions.iter().all(|id| id.get() != 0));
    assert_eq!(journal.durable_head(s.id).unwrap(), s.seq);
    // Reopen keeps the retired floor and the pointer.
    drop(journal);
    let mut reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    assert_eq!(reopened.durable_head(s.id).unwrap(), s.seq);
    assert!(
        reopened
            .read_suffix(s.id, LocalJournalSeq::ZERO, ReadBudget::new(8, u32::MAX))
            .is_err()
    );
    assert_eq!(read_all_from(&reopened, s.id, pointer.represented).len(), 2);
    reopened.retire_prefix(s.id, &pointer).unwrap();
    assert_eq!(reopened.stats().unwrap().compactions, 0, "already retired");
    let more = s.next(vec![transition(9, 8)]);
    reopened
        .append_group(&group(vec![(barrier(3), s.id, more)]))
        .unwrap();
    assert_eq!(read_all_from(&reopened, s.id, pointer.represented).len(), 3);
}

fn read_all_from(
    journal: &impl JournalEngine,
    stream: StorageStreamId,
    mut after: LocalJournalSeq,
) -> Vec<JournalRecordV1> {
    let mut out = Vec::new();
    loop {
        let page = journal
            .read_suffix(stream, after, ReadBudget::new(1, u32::MAX))
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
// Fail-stop
// ---------------------------------------------------------------------------

#[test]
fn sync_failure_inside_a_write_panics_and_fail_stops_until_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let faults = Arc::new(Faults::default());
    let fs = Arc::new(FaultFs {
        inner: DefaultFileSystem,
        faults: faults.clone(),
    });
    let (mut journal, _, mut streams) = create(&root, fs, &[1]);
    let first = streams[0].next(vec![transition(1, 8)]);
    journal
        .append_group(&group(vec![(barrier(1), streams[0].id, first)]))
        .unwrap();
    let head_before = journal.durable_head(streams[0].id).unwrap();

    faults.fail_sync.store(true, Ordering::SeqCst);
    let next = streams[0].next(vec![transition(2, 8)]);
    let g = group(vec![(barrier(2), streams[0].id, next)]);
    // The pinned engine panics through `expect("pipe::sync()")`; the
    // adapter does not catch it. The supervisor (here: the test) sees the
    // panic.
    let outcome = catch_unwind(AssertUnwindSafe(|| journal.append_group(&g)));
    assert!(outcome.is_err(), "sync failure must not be swallowed");
    faults.fail_sync.store(false, Ordering::SeqCst);

    // Every later use is refused: the shared engine is uncertain.
    assert!(journal.is_fail_stopped());
    let err = journal.durable_head(streams[0].id).unwrap_err();
    assert_eq!(err.class, JournalErrorClass::Corrupt);
    assert!(err.diagnostic.contains("fail-stop"));
    let retry = journal.append_group(&g).unwrap_err();
    assert!(matches!(retry, JournalFailure::Indeterminate(_)));
    assert!(
        journal
            .read_suffix(streams[0].id, LocalJournalSeq::ZERO, ReadBudget::new(1, 1))
            .is_err()
    );
    assert!(journal.mappings().is_err());

    // A fresh open recovers the actual valid records: the head is either
    // the old one or the panicked write's, and everything present verifies.
    drop(journal);
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    let head = reopened.durable_head(streams[0].id).unwrap();
    assert!(head == head_before || head == streams[0].seq, "{head:?}");
    let records = read_all(&reopened, streams[0].id);
    assert_eq!(records.last().unwrap().seq(), head);
    let mut expect = RecordExpectation::genesis(streams[0].origin);
    for r in &records {
        r.verify(&expect).unwrap();
        expect = expect.after(r).unwrap();
    }
    assert!(!reopened.is_fail_stopped());
}

#[test]
fn a_retired_stream_never_becomes_active_again() {
    // Only the key and the shard were compared, so persisting a stream's
    // own older, active mapping brought a retired stream back. Nothing
    // else says a retired stream stays retired, so after a reopen it
    // would take work again.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, allocator, streams) = create(&root, Arc::new(DefaultFileSystem), &[1, 2]);
    let active = *allocator.usable(streams[0].id).unwrap();
    assert!(!active.retired, "the stream starts active");

    let retired = StreamMappingV1 {
        retired: true,
        ..active
    };
    journal
        .persist_mapping(allocator.high_water(), &retired)
        .expect("retiring a stream is allowed");

    // The same mapping as before retirement: same key, same shard, and
    // active. It is refused.
    let err = journal
        .persist_mapping(allocator.high_water(), &active)
        .unwrap_err();
    assert!(err.is_definite(), "{err:?}");
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
    // Re-persisting the retirement is idempotent.
    journal
        .persist_mapping(allocator.high_water(), &retired)
        .expect("retiring again changes nothing");

    // And it stays retired across a reopen, where the refusal is applied
    // against what was actually durable.
    drop(journal);
    let mut reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    let (hw, mappings) = reopened.mappings().unwrap();
    let recovered = mappings
        .iter()
        .find(|m| m.stream == active.stream)
        .expect("the mapping survived");
    assert!(recovered.retired, "retirement is durable");
    let err = reopened.persist_mapping(hw, &active).unwrap_err();
    assert!(err.is_definite(), "{err:?}");
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
}

#[test]
fn a_mapping_that_keeps_its_key_keeps_its_shard() {
    // Only a changed key was checked against `adopts`, which compares the
    // shard, so the same key under another shard slipped past: a stream's
    // placement could be rewritten and survive a reopen, although an
    // authorized replacement changes only the generation the stream
    // serves.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("j");
    let (mut journal, allocator, streams) = create(&root, Arc::new(DefaultFileSystem), &[1]);
    let mapping = *allocator.usable(streams[0].id).unwrap();
    let elsewhere = ShardId::new(1).unwrap();
    assert_ne!(mapping.shard, elsewhere);

    let moved = StreamMappingV1 {
        shard: elsewhere,
        ..mapping
    };
    let err = journal
        .persist_mapping(allocator.high_water(), &moved)
        .unwrap_err();
    assert!(err.is_definite(), "{err:?}");
    assert_eq!(err.error().class, JournalErrorClass::GuardRejected);
    // Re-persisting the mapping as it is changes nothing.
    journal
        .persist_mapping(allocator.high_water(), &mapping)
        .expect("the same mapping is idempotent");

    // A carried stream changes exactly its generation: the shard has to
    // come along unchanged, before and after the adoption.
    let later = StreamKey {
        incarnation: ReplicaIncarnation::new(2).unwrap(),
        ..mapping.key
    };
    let carried_elsewhere = StreamMappingV1 {
        key: later,
        shard: elsewhere,
        ..mapping
    };
    let err = journal
        .persist_mapping(allocator.high_water(), &carried_elsewhere)
        .unwrap_err();
    assert!(err.is_definite(), "{err:?}");
    let carried = StreamMappingV1 {
        key: later,
        ..mapping
    };
    journal
        .persist_mapping(allocator.high_water(), &carried)
        .expect("an authorized replacement carries the stream forward");
    let moved = StreamMappingV1 {
        shard: elsewhere,
        ..carried
    };
    let err = journal
        .persist_mapping(allocator.high_water(), &moved)
        .unwrap_err();
    assert!(err.is_definite(), "{err:?}");

    drop(journal);
    let reopened = RaftEngineJournal::open_existing(&root, identity(), &options()).unwrap();
    let (_, mappings) = reopened.mappings().unwrap();
    assert_eq!(mappings, vec![carried], "only the generation moved");
}
