//! `RaftEngineJournal`: the `JournalEngine` implementation over the pinned
//! engine (design Sections 17.3.1-17.3.3, 17.15, 17.16.3-17.16.4).
//!
//! Mapping: one engine region per `StorageStreamId`, one entry per
//! `JournalRecordV1` at index `LocalJournalSeq`, region `0` for journal
//! metadata. Every group is validated against the accepted durable head
//! (mapping durable, origin, index, predecessor chain, digest) before a
//! `LogBatch` is built, so every rejection at that stage is definite. The
//! batch is written with `sync = true`; the engine's return value is a byte
//! count that the receipt records as evidence while the completions come
//! from the caller-owned group.
//!
//! Fail-stop: at the pinned revision a synchronization failure inside a
//! nonempty write panics (`expect("pipe::sync()")`). The adapter neither
//! catches that panic nor retries: it propagates to the supervisor, the
//! journal mutex is poisoned, and every later call reports a fail-stop
//! error. An ordinary error returned after submission is treated the same
//! way (the shared engine state is uncertain). Recovery is a fresh open,
//! which derives the actual valid records and heads from the log files.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use coord_journal_api::engine::{JournalEngine, ReadBudget, RecordPage};
use coord_journal_api::failure::{JournalError, JournalErrorClass, JournalFailure};
use coord_journal_api::frontier::CheckpointPointerV1;
use coord_journal_api::group::{GroupReceipt, GroupWrite};
use coord_journal_api::head::WrittenBytes;
use coord_journal_api::record::{
    GENESIS_PREDECESSOR, JournalRecordV1, RecordBody, RecordExpectation, RecordOrigin,
};
use coord_journal_api::stream::{
    StorageStreamId, StreamAllocator, StreamHighWater, StreamMappingV1,
};
use coord_types::identity::Digest32;
use coord_types::ids::{ClusterId, LocalJournalSeq, ReplicaId};
use raft_engine::env::{DefaultFileSystem, FileSystem};
use raft_engine::{Command, Config, Engine, LogBatch, ReadableSize, RecoveryMode};

use crate::codec::{RecordCodec, RecordExt};
use crate::metadata::{
    HIGH_WATER_KEY, IDENTITY_KEY, JOURNAL_FORMAT_V1, JournalIdentityV1, METADATA_REGION,
    MetadataValueV1, POINTER_KEY, STREAM_KEY_PREFIX, decode_value, encode_value, stream_key,
    stream_key_end,
};

/// Identity a journal directory is created for and must match on open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalIdentity {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Replica.
    pub replica: ReplicaId,
}

/// Engine recovery mode. `TolerateAnyCorruption` is never offered: a
/// permissive mode cannot silently discard acknowledged state (Section
/// 17.16.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPolicy {
    /// Any corruption stops recovery.
    AbsoluteConsistency,
    /// Only a corrupted tail record is tolerated (the qualified torn-tail
    /// case); damage inside the durable prefix stops recovery.
    TolerateTailCorruption,
}

/// Engine tuning recorded by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalOptions {
    /// Target size of one log file.
    pub target_file_size_bytes: u64,
    /// Total size above which the engine suggests compaction (never acted
    /// on automatically).
    pub purge_threshold_bytes: u64,
    /// Recovery mode.
    pub recovery: RecoveryPolicy,
    /// How many recent receipts to keep for diagnostics.
    pub recent_receipts: usize,
}

impl Default for JournalOptions {
    fn default() -> Self {
        JournalOptions {
            target_file_size_bytes: 64 * 1024 * 1024,
            purge_threshold_bytes: 1024 * 1024 * 1024,
            recovery: RecoveryPolicy::TolerateTailCorruption,
            recent_receipts: 64,
        }
    }
}

/// Why a journal could not be created or opened. Every variant is a hard
/// stop; none invites creation or reinitialization.
#[derive(Debug)]
pub enum OpenError {
    /// The directory is held by another opener.
    Busy,
    /// Creation on a directory that already has contents.
    AlreadyInitialized,
    /// Open on a directory that does not exist or carries no identity row.
    NotInitialized,
    /// Identity row field differs from the expectation.
    IdentityMismatch(&'static str),
    /// Directory format not supported by this build.
    UnsupportedFormat(u16),
    /// Options the engine refuses.
    InvalidOptions(String),
    /// Metadata or entries are inconsistent with the contract.
    Corrupt(String),
    /// The engine failed to open or write.
    Engine(String),
    /// I/O failure.
    Io(std::io::Error),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::Busy => f.write_str("journal directory is locked by another opener"),
            OpenError::AlreadyInitialized => f.write_str("journal directory already has contents"),
            OpenError::NotInitialized => f.write_str("journal directory not initialized"),
            OpenError::IdentityMismatch(field) => {
                write!(f, "journal {field} does not match expected identity")
            }
            OpenError::UnsupportedFormat(v) => write!(f, "unsupported journal format {v}"),
            OpenError::InvalidOptions(e) => write!(f, "invalid journal options: {e}"),
            OpenError::Corrupt(e) => write!(f, "journal corrupt or inconsistent: {e}"),
            OpenError::Engine(e) => write!(f, "engine: {e}"),
            OpenError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<std::io::Error> for OpenError {
    fn from(e: std::io::Error) -> Self {
        OpenError::Io(e)
    }
}

/// Measured write activity (Section 17.3.3: measure actual bytes, records
/// and syncs).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Groups written successfully.
    pub groups: u64,
    /// Records written successfully.
    pub records: u64,
    /// Bytes the engine reported for successful groups.
    pub bytes: u64,
    /// Synced writes issued (groups, mapping updates and compactions).
    pub syncs: u64,
    /// Synced compactions issued.
    pub compactions: u64,
}

/// Recovered or maintained state of one stream.
#[derive(Clone, Copy, Debug)]
struct StreamState {
    /// Origin of the stream's records (`None` before the first record).
    origin: Option<RecordOrigin>,
    /// First retained sequence (`ZERO` when nothing was retired and
    /// nothing written).
    first: LocalJournalSeq,
    /// Durable head.
    durable: LocalJournalSeq,
    /// Digest of the record at the head.
    last_digest: Digest32,
    /// Sequence of the latest durable checkpoint publication.
    pointer: Option<LocalJournalSeq>,
}

impl StreamState {
    const EMPTY: StreamState = StreamState {
        origin: None,
        first: LocalJournalSeq::ZERO,
        durable: LocalJournalSeq::ZERO,
        last_digest: GENESIS_PREDECESSOR,
        pointer: None,
    };
}

struct Inner<F: FileSystem> {
    engine: Engine<F>,
    identity: JournalIdentityV1,
    high_water: StreamHighWater,
    mappings: BTreeMap<StorageStreamId, StreamMappingV1>,
    streams: BTreeMap<StorageStreamId, StreamState>,
    stats: WriteStats,
    receipts: VecDeque<GroupReceipt>,
    recent_limit: usize,
    fail_stop: Option<String>,
}

/// The raft-engine journal of one shard set.
pub struct RaftEngineJournal<F: FileSystem = DefaultFileSystem> {
    inner: Mutex<Inner<F>>,
    dir: PathBuf,
}

fn redact(e: &dyn fmt::Display) -> String {
    let s = e.to_string();
    if s.len() > 200 {
        s[..200].to_owned()
    } else {
        s
    }
}

fn definite(diagnostic: impl Into<String>) -> JournalFailure {
    JournalFailure::rejected_before_append(diagnostic)
}

fn fail_stop_error(reason: &str) -> JournalError {
    JournalError::new(
        JournalErrorClass::Corrupt,
        format!("journal fail-stop: {reason}"),
    )
}

fn classify_engine(e: &raft_engine::Error) -> JournalError {
    let class = match e {
        raft_engine::Error::Corruption(_)
        | raft_engine::Error::Codec(_)
        | raft_engine::Error::Protobuf(_)
        | raft_engine::Error::EntryCompacted
        | raft_engine::Error::EntryNotFound => JournalErrorClass::Corrupt,
        raft_engine::Error::Io(io) if io.kind() == std::io::ErrorKind::StorageFull => {
            JournalErrorClass::NoSpace
        }
        raft_engine::Error::Io(_) | raft_engine::Error::Other(_) => JournalErrorClass::Io,
        raft_engine::Error::TryAgain(_) | raft_engine::Error::Full => JournalErrorClass::Busy,
        raft_engine::Error::InvalidArgument(_) => JournalErrorClass::Unsupported,
    };
    JournalError::new(class, redact(e))
}

fn engine_open_error(e: raft_engine::Error) -> OpenError {
    match e {
        raft_engine::Error::InvalidArgument(m) => OpenError::InvalidOptions(m),
        raft_engine::Error::Corruption(m) => OpenError::Corrupt(m),
        raft_engine::Error::Io(io) => OpenError::Io(io),
        raft_engine::Error::Other(o) if o.to_string().contains("lock") => OpenError::Busy,
        other => OpenError::Engine(redact(&other)),
    }
}

fn config(dir: &Path, options: &JournalOptions) -> Config {
    Config {
        dir: dir.to_string_lossy().into_owned(),
        recovery_mode: match options.recovery {
            RecoveryPolicy::AbsoluteConsistency => RecoveryMode::AbsoluteConsistency,
            RecoveryPolicy::TolerateTailCorruption => RecoveryMode::TolerateTailCorruption,
        },
        target_file_size: ReadableSize(options.target_file_size_bytes),
        purge_threshold: ReadableSize(options.purge_threshold_bytes),
        // Files are never recycled: a fresh file never carries stale
        // records from an earlier use.
        enable_log_recycle: false,
        prefill_for_recycle: false,
        ..Config::default()
    }
}

fn put_value(
    batch: &mut LogBatch,
    region: u64,
    key: &[u8],
    value: &MetadataValueV1,
) -> Result<(), JournalFailure> {
    let bytes = encode_value(value).map_err(|e| definite(format!("metadata encode: {e}")))?;
    batch
        .put(region, key.to_vec(), bytes)
        .map_err(|e| definite(format!("metadata put: {}", redact(&e))))
}

impl RaftEngineJournal<DefaultFileSystem> {
    /// Create a journal in an empty (or absent) directory.
    pub fn create(
        dir: &Path,
        identity: JournalIdentity,
        options: &JournalOptions,
    ) -> Result<Self, OpenError> {
        Self::create_with_file_system(dir, identity, options, Arc::new(DefaultFileSystem))
    }

    /// Open an existing journal whose identity must match.
    pub fn open_existing(
        dir: &Path,
        identity: JournalIdentity,
        options: &JournalOptions,
    ) -> Result<Self, OpenError> {
        Self::open_existing_with_file_system(dir, identity, options, Arc::new(DefaultFileSystem))
    }
}

impl<F: FileSystem> RaftEngineJournal<F> {
    /// [`RaftEngineJournal::create`] over a caller-provided file system
    /// (fault adapters in qualification, task-j05).
    pub fn create_with_file_system(
        dir: &Path,
        identity: JournalIdentity,
        options: &JournalOptions,
        file_system: Arc<F>,
    ) -> Result<Self, OpenError> {
        if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
            return Err(OpenError::AlreadyInitialized);
        }
        std::fs::create_dir_all(dir)?;
        let engine = Engine::open_with_file_system(config(dir, options), file_system)
            .map_err(engine_open_error)?;
        if !engine.is_empty() {
            return Err(OpenError::Corrupt(
                "fresh directory holds entries".to_owned(),
            ));
        }
        let row = JournalIdentityV1 {
            format: JOURNAL_FORMAT_V1,
            cluster: identity.cluster,
            replica: identity.replica,
        };
        let mut batch = LogBatch::default();
        put_value(
            &mut batch,
            METADATA_REGION,
            IDENTITY_KEY,
            &MetadataValueV1::Identity(row),
        )
        .map_err(|e| OpenError::Corrupt(e.to_string()))?;
        put_value(
            &mut batch,
            METADATA_REGION,
            HIGH_WATER_KEY,
            &MetadataValueV1::HighWater(0),
        )
        .map_err(|e| OpenError::Corrupt(e.to_string()))?;
        engine
            .write(&mut batch, true)
            .map_err(|e| OpenError::Engine(redact(&e)))?;
        Ok(Self::assemble(
            engine,
            row,
            StreamHighWater::NONE,
            BTreeMap::new(),
            BTreeMap::new(),
            options,
            dir,
        ))
    }

    /// [`RaftEngineJournal::open_existing`] over a caller-provided file
    /// system.
    pub fn open_existing_with_file_system(
        dir: &Path,
        identity: JournalIdentity,
        options: &JournalOptions,
        file_system: Arc<F>,
    ) -> Result<Self, OpenError> {
        if !dir.is_dir() {
            return Err(OpenError::NotInitialized);
        }
        let engine = Engine::open_with_file_system(config(dir, options), file_system)
            .map_err(engine_open_error)?;
        let row = match engine.get(METADATA_REGION, IDENTITY_KEY) {
            None => return Err(OpenError::NotInitialized),
            Some(bytes) => match decode_value(&bytes) {
                Ok(MetadataValueV1::Identity(row)) => row,
                _ => return Err(OpenError::Corrupt("identity row".to_owned())),
            },
        };
        if row.format != JOURNAL_FORMAT_V1 {
            return Err(OpenError::UnsupportedFormat(row.format));
        }
        if row.cluster != identity.cluster {
            return Err(OpenError::IdentityMismatch("cluster"));
        }
        if row.replica != identity.replica {
            return Err(OpenError::IdentityMismatch("replica"));
        }
        let high_water = match engine.get(METADATA_REGION, HIGH_WATER_KEY) {
            Some(bytes) => match decode_value(&bytes) {
                Ok(MetadataValueV1::HighWater(v)) => StreamHighWater::from_durable(v),
                _ => return Err(OpenError::Corrupt("high-water row".to_owned())),
            },
            None => return Err(OpenError::Corrupt("high-water row missing".to_owned())),
        };
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        engine
            .scan_raw_messages(
                METADATA_REGION,
                Some(STREAM_KEY_PREFIX),
                Some(&stream_key_end()),
                false,
                |k, v| {
                    rows.push((k.to_vec(), v.to_vec()));
                    true
                },
            )
            .map_err(|e| OpenError::Engine(redact(&e)))?;
        let mut mappings = BTreeMap::new();
        for (key, value) in rows {
            let mapping = match decode_value(&value) {
                Ok(MetadataValueV1::Mapping(m)) => m,
                _ => return Err(OpenError::Corrupt("stream mapping row".to_owned())),
            };
            if key != stream_key(mapping.stream) {
                return Err(OpenError::Corrupt("stream mapping key".to_owned()));
            }
            if mapping.key.cluster != row.cluster {
                return Err(OpenError::Corrupt("stream mapping cluster".to_owned()));
            }
            mappings.insert(mapping.stream, mapping);
        }
        // The allocator's own restore rejects duplicates and identifiers
        // above the high-water mark.
        StreamAllocator::restore(high_water, mappings.values().copied())
            .map_err(|e| OpenError::Corrupt(format!("stream mappings: {e}")))?;

        let mut streams = BTreeMap::new();
        for region in engine.raft_groups() {
            if region == METADATA_REGION {
                continue;
            }
            let stream = StorageStreamId::from_durable(region)
                .map_err(|e| OpenError::Corrupt(format!("region {region}: {e}")))?;
            let Some(mapping) = mappings.get(&stream) else {
                return Err(OpenError::Corrupt(format!(
                    "entries for unmapped stream {region}"
                )));
            };
            let (Some(first), Some(last)) = (engine.first_index(region), engine.last_index(region))
            else {
                // Key/value rows without entries: nothing durable in the
                // stream; a pointer without entries is inconsistent.
                if engine.get(region, POINTER_KEY).is_some() {
                    return Err(OpenError::Corrupt(format!(
                        "pointer without entries in stream {region}"
                    )));
                }
                continue;
            };
            let record = engine
                .get_entry_with::<RecordExt, RecordCodec>(region, last)
                .map_err(|e| OpenError::Corrupt(format!("stream {region} head: {}", redact(&e))))?
                .ok_or_else(|| OpenError::Corrupt(format!("stream {region} head missing")))?;
            let origin = *record.origin();
            if origin.stream != stream
                || origin.cluster != row.cluster
                || origin.replica != row.replica
                || origin.domain != mapping.key.domain
                || origin.incarnation != mapping.key.incarnation
            {
                return Err(OpenError::Corrupt(format!(
                    "stream {region} origin does not match its mapping"
                )));
            }
            if record.seq().get() != last {
                return Err(OpenError::Corrupt(format!(
                    "stream {region} head index disagrees with its record"
                )));
            }
            let pointer = match engine.get(region, POINTER_KEY) {
                None => None,
                Some(bytes) => match decode_value(&bytes) {
                    Ok(MetadataValueV1::Pointer(seq)) if seq >= first && seq <= last => {
                        Some(LocalJournalSeq::new(seq).map_err(|_| {
                            OpenError::Corrupt(format!("stream {region} pointer row"))
                        })?)
                    }
                    _ => {
                        return Err(OpenError::Corrupt(format!("stream {region} pointer row")));
                    }
                },
            };
            streams.insert(
                stream,
                StreamState {
                    origin: Some(origin),
                    first: LocalJournalSeq::new(first)
                        .map_err(|_| OpenError::Corrupt("first index".to_owned()))?,
                    durable: record.seq(),
                    last_digest: record.digest(),
                    pointer,
                },
            );
        }
        Ok(Self::assemble(
            engine, row, high_water, mappings, streams, options, dir,
        ))
    }

    fn assemble(
        engine: Engine<F>,
        identity: JournalIdentityV1,
        high_water: StreamHighWater,
        mappings: BTreeMap<StorageStreamId, StreamMappingV1>,
        streams: BTreeMap<StorageStreamId, StreamState>,
        options: &JournalOptions,
        dir: &Path,
    ) -> Self {
        RaftEngineJournal {
            inner: Mutex::new(Inner {
                engine,
                identity,
                high_water,
                mappings,
                streams,
                stats: WriteStats::default(),
                receipts: VecDeque::new(),
                recent_limit: options.recent_receipts,
                fail_stop: None,
            }),
            dir: dir.to_path_buf(),
        }
    }

    /// Directory.
    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Identity recorded in the directory.
    pub fn identity(&self) -> Result<JournalIdentity, JournalError> {
        let inner = self.lock()?;
        Ok(JournalIdentity {
            cluster: inner.identity.cluster,
            replica: inner.identity.replica,
        })
    }

    /// Whether the journal is fail-stopped (a write panicked or failed
    /// after submission). Only a fresh open recovers.
    pub fn is_fail_stopped(&self) -> bool {
        match self.inner.lock() {
            Ok(inner) => inner.fail_stop.is_some(),
            Err(_) => true,
        }
    }

    /// Measured write activity since open.
    pub fn stats(&self) -> Result<WriteStats, JournalError> {
        Ok(self.lock()?.stats)
    }

    /// The most recent receipts (byte count to barrier/stream/sequence
    /// mapping), oldest first.
    pub fn recent_receipts(&self) -> Result<Vec<GroupReceipt>, JournalError> {
        Ok(self.lock()?.receipts.iter().cloned().collect())
    }

    /// Run the engine's physical purge and report the streams it suggests
    /// compacting. Suggestions are diagnostics only: logical history is
    /// retired solely through [`JournalEngine::retire_prefix`] under a
    /// durable checkpoint pointer.
    pub fn maintain(&self) -> Result<Vec<StorageStreamId>, JournalError> {
        let mut inner = self.lock()?;
        match inner.engine.purge_expired_files() {
            Ok(regions) => Ok(regions
                .into_iter()
                .filter_map(|r| StorageStreamId::from_durable(r).ok())
                .collect()),
            Err(e) => {
                let err = classify_engine(&e);
                inner.fail_stop = Some(format!("purge failed: {err}"));
                Err(fail_stop_error(&format!("purge failed: {err}")))
            }
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner<F>>, JournalError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| fail_stop_error("a write panicked; shared engine state is uncertain"))?;
        if let Some(reason) = &inner.fail_stop {
            return Err(fail_stop_error(reason));
        }
        Ok(inner)
    }
}

impl<F: FileSystem> Inner<F> {
    fn expectation(
        &self,
        stream: StorageStreamId,
        first: &JournalRecordV1,
    ) -> Result<RecordExpectation, JournalFailure> {
        let Some(mapping) = self.mappings.get(&stream) else {
            return Err(definite("stream mapping not durable"));
        };
        if mapping.retired {
            return Err(definite("stream retired"));
        }
        let state = self
            .streams
            .get(&stream)
            .copied()
            .unwrap_or(StreamState::EMPTY);
        match state.origin {
            Some(origin) => Ok(RecordExpectation {
                origin,
                seq: state
                    .durable
                    .checked_next()
                    .map_err(|_| definite("stream sequence exhausted"))?,
                predecessor: state.last_digest,
            }),
            None => {
                let origin = *first.origin();
                if origin.stream != stream
                    || origin.cluster != self.identity.cluster
                    || origin.replica != self.identity.replica
                    || origin.domain != mapping.key.domain
                    || origin.incarnation != mapping.key.incarnation
                {
                    return Err(definite("record origin does not match the stream mapping"));
                }
                Ok(RecordExpectation::genesis(origin))
            }
        }
    }

    fn synced_write(&mut self, batch: &mut LogBatch, what: &str) -> Result<usize, JournalFailure> {
        // A synchronization failure inside this call panics at the pinned
        // revision; the panic propagates and poisons the journal mutex.
        match self.engine.write(batch, true) {
            Ok(bytes) => {
                self.stats.syncs += 1;
                Ok(bytes)
            }
            Err(e) => {
                let err = classify_engine(&e);
                self.fail_stop = Some(format!("{what} failed after submission: {err}"));
                Err(JournalFailure::after_submission(err))
            }
        }
    }
}

impl<F: FileSystem> JournalEngine for RaftEngineJournal<F> {
    fn append_group(&mut self, group: &GroupWrite) -> Result<GroupReceipt, JournalFailure> {
        let mut inner = self.lock().map_err(JournalFailure::Indeterminate)?;
        if group.is_empty() {
            return Err(definite("empty group"));
        }
        // Validate everything before anything is submitted.
        struct Planned {
            stream: StorageStreamId,
            origin: RecordOrigin,
            last: LocalJournalSeq,
            last_digest: Digest32,
            pointer: Option<LocalJournalSeq>,
        }
        let mut planned: Vec<Planned> = Vec::with_capacity(group.entries().len());
        for entry in group.entries() {
            let records = entry.records();
            let mut expect = inner.expectation(entry.stream(), &records[0])?;
            let mut pointer = None;
            for record in records {
                record
                    .verify(&expect)
                    .map_err(|e| definite(format!("record guard rejected: {e}")))?;
                if matches!(record.body(), RecordBody::PublishLocalCheckpoint(_)) {
                    pointer = Some(record.seq());
                }
                expect = expect
                    .after(record)
                    .map_err(|_| definite("stream sequence exhausted"))?;
            }
            planned.push(Planned {
                stream: entry.stream(),
                origin: expect.origin,
                last: entry.last(),
                last_digest: expect.predecessor,
                pointer,
            });
        }
        let mut batch = LogBatch::default();
        for (entry, plan) in group.entries().iter().zip(&planned) {
            batch
                .add_entries_with::<RecordExt, RecordCodec>(entry.stream().get(), entry.records())
                .map_err(|e| JournalFailure::Definite(classify_engine(&e)))?;
            if let Some(seq) = plan.pointer {
                put_value(
                    &mut batch,
                    entry.stream().get(),
                    POINTER_KEY,
                    &MetadataValueV1::Pointer(seq.get()),
                )?;
            }
        }
        let bytes = inner.synced_write(&mut batch, "group write")?;
        for plan in planned {
            let state = inner
                .streams
                .entry(plan.stream)
                .or_insert(StreamState::EMPTY);
            if state.origin.is_none() {
                state.origin = Some(plan.origin);
                state.first = LocalJournalSeq::new(1).expect("one");
            }
            state.durable = plan.last;
            state.last_digest = plan.last_digest;
            if plan.pointer.is_some() {
                state.pointer = plan.pointer;
            }
        }
        inner.stats.groups += 1;
        inner.stats.records += group.record_count() as u64;
        inner.stats.bytes += bytes as u64;
        let receipt = group
            .receipt(WrittenBytes(bytes as u64))
            .map_err(|e| definite(e.to_string()))?;
        if inner.recent_limit > 0 {
            if inner.receipts.len() >= inner.recent_limit {
                inner.receipts.pop_front();
            }
            inner.receipts.push_back(receipt.clone());
        }
        Ok(receipt)
    }

    fn durable_head(&self, stream: StorageStreamId) -> Result<LocalJournalSeq, JournalError> {
        let inner = self.lock()?;
        if !inner.mappings.contains_key(&stream) {
            return Err(JournalError::new(
                JournalErrorClass::GuardRejected,
                "unknown stream",
            ));
        }
        Ok(inner
            .streams
            .get(&stream)
            .map_or(LocalJournalSeq::ZERO, |s| s.durable))
    }

    fn read_suffix(
        &self,
        stream: StorageStreamId,
        after: LocalJournalSeq,
        budget: ReadBudget,
    ) -> Result<RecordPage, JournalError> {
        let inner = self.lock()?;
        if !inner.mappings.contains_key(&stream) {
            return Err(JournalError::new(
                JournalErrorClass::GuardRejected,
                "unknown stream",
            ));
        }
        let Some(state) = inner.streams.get(&stream).copied() else {
            return Ok(RecordPage {
                records: Vec::new(),
                exhausted: true,
            });
        };
        if after >= state.durable {
            return Ok(RecordPage {
                records: Vec::new(),
                exhausted: true,
            });
        }
        let begin = after.get() + 1;
        if begin < state.first.get() {
            return Err(JournalError::new(
                JournalErrorClass::Corrupt,
                "required suffix starts inside the retired prefix",
            ));
        }
        let end = state.durable.get().min(
            after
                .get()
                .saturating_add(u64::from(budget.max_records.get())),
        ) + 1;
        let mut records: Vec<JournalRecordV1> = Vec::new();
        inner
            .engine
            .fetch_entries_to_with::<RecordExt, RecordCodec>(
                stream.get(),
                begin,
                end,
                Some(budget.max_bytes.get() as usize),
                &mut records,
            )
            .map_err(|e| JournalError::new(JournalErrorClass::Corrupt, redact(&e)))?;
        let Some(first) = records.first() else {
            return Err(JournalError::new(
                JournalErrorClass::Corrupt,
                "engine returned no entries inside the durable range",
            ));
        };
        let Some(origin) = state.origin else {
            return Err(JournalError::new(
                JournalErrorClass::Corrupt,
                "entries without a recovered origin",
            ));
        };
        let mut expect = RecordExpectation {
            origin,
            seq: LocalJournalSeq::new(begin)
                .map_err(|_| JournalError::new(JournalErrorClass::Corrupt, "sequence"))?,
            predecessor: first.predecessor(),
        };
        for record in &records {
            record.verify(&expect).map_err(|e| {
                JournalError::new(JournalErrorClass::Corrupt, format!("suffix record: {e}"))
            })?;
            expect = expect
                .after(record)
                .map_err(|_| JournalError::new(JournalErrorClass::Corrupt, "sequence"))?;
        }
        let exhausted = records[records.len() - 1].seq() == state.durable;
        Ok(RecordPage { records, exhausted })
    }

    fn retire_prefix(
        &mut self,
        stream: StorageStreamId,
        pointer: &CheckpointPointerV1,
    ) -> Result<(), JournalFailure> {
        let mut inner = self.lock().map_err(JournalFailure::Indeterminate)?;
        let Some(state) = inner.streams.get(&stream).copied() else {
            return Err(definite("unknown stream"));
        };
        let Some(pointer_seq) = state.pointer else {
            return Err(definite("no durable checkpoint publication in the stream"));
        };
        let record = inner
            .engine
            .get_entry_with::<RecordExt, RecordCodec>(stream.get(), pointer_seq.get())
            .map_err(|e| JournalFailure::Definite(classify_engine(&e)))?
            .ok_or_else(|| definite("publication record missing"))?;
        match record.body() {
            RecordBody::PublishLocalCheckpoint(p) if p == pointer => {}
            _ => return Err(definite("pointer is not the stream's durable publication")),
        }
        if pointer.represented >= pointer_seq {
            return Err(definite("pointer does not precede its publication"));
        }
        let target = pointer
            .represented
            .checked_next()
            .map_err(|_| definite("sequence"))?;
        if target <= state.first {
            // Already retired through this pointer: idempotent.
            return Ok(());
        }
        let mut batch = LogBatch::default();
        batch.add_command(
            stream.get(),
            Command::Compact {
                index: target.get(),
            },
        );
        inner.synced_write(&mut batch, "compaction")?;
        inner.stats.compactions += 1;
        if let Some(s) = inner.streams.get_mut(&stream) {
            s.first = target;
        }
        Ok(())
    }

    fn mappings(&self) -> Result<(StreamHighWater, Vec<StreamMappingV1>), JournalError> {
        let inner = self.lock()?;
        Ok((inner.high_water, inner.mappings.values().copied().collect()))
    }

    fn persist_mapping(
        &mut self,
        high_water: StreamHighWater,
        mapping: &StreamMappingV1,
    ) -> Result<(), JournalFailure> {
        let mut inner = self.lock().map_err(JournalFailure::Indeterminate)?;
        if high_water < inner.high_water || mapping.stream.get() > high_water.get() {
            return Err(definite("high-water mark must cover the mapping"));
        }
        if mapping.key.cluster != inner.identity.cluster {
            return Err(definite(
                "mapping cluster differs from the journal identity",
            ));
        }
        if let Some(existing) = inner.mappings.get(&mapping.stream)
            && (existing.key != mapping.key || existing.shard != mapping.shard)
        {
            return Err(definite(
                "mapping identity of an allocated stream cannot change",
            ));
        }
        if inner
            .mappings
            .values()
            .any(|m| m.key == mapping.key && m.stream != mapping.stream)
        {
            return Err(definite("stream key already mapped"));
        }
        let mut batch = LogBatch::default();
        put_value(
            &mut batch,
            METADATA_REGION,
            HIGH_WATER_KEY,
            &MetadataValueV1::HighWater(high_water.get()),
        )?;
        put_value(
            &mut batch,
            METADATA_REGION,
            &stream_key(mapping.stream),
            &MetadataValueV1::Mapping(*mapping),
        )?;
        inner.synced_write(&mut batch, "mapping write")?;
        inner.high_water = high_water;
        inner.mappings.insert(mapping.stream, *mapping);
        Ok(())
    }
}
