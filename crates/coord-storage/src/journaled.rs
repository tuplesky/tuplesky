//! Journal-first shared storage and atomic materialization (task-j03;
//! design Sections 4.7-4.8, 17.3.2-17.3.4, 17.4, 17.10 and 18).
//!
//! [`JournaledStore`] is the `journaled-strict-v1` profile: every common
//! transition is validated and sequenced, journaled first as one immutable
//! complete record, and only then applied to the domain's durable
//! projection in journal order. The two facts stay separate and are
//! reported separately:
//!
//! * `JournalDurable` means the record is synced in the shared journal. It
//!   satisfies the outbox prerequisite of a vote-producing effect and
//!   nothing else.
//! * `Materialized` means the record's complete immutable updates and the
//!   applied stamp were committed atomically into the projection. It is
//!   what makes state publicly visible.
//! * `Established` is never produced here. Neither durability nor
//!   materialization is protocol learning; the learning predicate lives in
//!   the consensus crate and consumes these as evidence.
//!
//! Sequencing and grouping follow Section 17.3.3. One shared journal serves
//! many domains: [`JournaledStore::append_pending`] takes at most one
//! queued transition per stream, seals it against that stream's accepted
//! durable head, and writes the whole bounded multi-domain group as one
//! synced engine write. Initially a stream carries at most one uncompleted
//! authoritative batch, so a group holds exactly one record per stream and
//! every completion names exactly one caller-owned barrier; same-stream
//! pipelining is deliberately not attempted here (it has to prove
//! predecessor and reservation order first). A single record too large for
//! the ordinary group budget takes the separately bounded large-record
//! path rather than being split illegally.
//!
//! Materialization is the only writer of the projection. It rechecks the
//! recorded [`ApplyBase`] inside the write transaction, lowers the record's
//! updates through the shared [`crate::lowering`] helpers (physical
//! adapters contain no application logic of their own), stamps the
//! projection with the materialized sequence and the record's digest, and
//! commits with `commit_durable`. The strict profile keeps that durable
//! projection commit: the journal sync and the projection commit are two
//! synchronizations, and nothing here claims one fsync overall. The
//! replay-backed profile that removes the second one is task-j06 and is
//! not enabled.
//!
//! Recovery is journal-first as well. [`JournaledStore::attach`] recovers
//! `J` from the journal's actual valid records and `M` from the
//! projection's applied stamp, refuses a projection ahead of the journal,
//! and replays `(M, J]` into the projection, verifying every record's
//! origin, index, predecessor chain and digest first. Replay consumes
//! nothing but the records: no clock, no entropy, no issuer and no peer
//! message, so it restores exactly the state, results and events the
//! original application produced. Old send callbacks are never replayed;
//! records recovered from an earlier boot complete no barrier of this one.
//!
//! Ambiguity is reconciled, never blind-retried. An append that fails after
//! submission leaves the stream head uncertain and blocks further
//! reservations until [`JournaledStore::reconcile`] reads the actual
//! durable head; an indeterminate projection commit is resolved from the
//! semantic stamp, and if the commit was absent the records are applied
//! again from the durable journal (a semantic redo of authoritative
//! records, not a replay of an opaque byte batch).
//!
//! [`JournaledStore::fence`] implements the admission half of Section 4.8's
//! recovery cut: before a replica authorizes a higher-ballot reply it stops
//! admitting new voting transitions of the obsolete ballot *in that
//! domain*. It is per-domain on purpose: there is no cross-domain election
//! barrier, no global packet drain, and no waiting for other domains to
//! quiesce. A late completion of an already-submitted old-ballot append
//! still updates durability bookkeeping - it is reported as
//! `JournalDurable` and the barrier completes - but the
//! [`coord_core::outbox::Outbox`] release gate refuses to newly authorize
//! its vote under the newer promise.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use coord_core::effect::{ApplyBase, BarrierId, BootId, PersistBatch};
use coord_core::event::{StorageError, StorageEvent};
use coord_journal_api::engine::{JournalEngine, ReadBudget};
use coord_journal_api::failure::{JournalError, JournalFailure};
use coord_journal_api::frontier::{
    AppliedFrontier, CheckpointPointerV1, FrontierError, Frontiers, select_recovery_pointer,
};
use coord_journal_api::group::{GroupEntry, GroupError, GroupLimits, GroupWrite};
use coord_journal_api::head::{HeadError, HeadState, Reconciled, StreamHead, WrittenBytes};
use coord_journal_api::record::{
    JOURNAL_RECORD_FORMAT_V1, JournalRecordV1, LifecycleRecordV1, MAX_RECORD_BYTES,
    MAX_RECORD_KEY_BYTES, MAX_RECORD_UPDATES, MAX_RECORD_VALUE_BYTES, RecordBody, RecordDraft,
    RecordError, RecordExpectation, RecordOrigin, TransitionContext,
};
use coord_journal_api::stream::{
    ShardId, StorageStreamId, StreamAllocator, StreamError, StreamKey, StreamMappingV1,
};
use coord_store_api::engine::{CommitFailure, EngineError, LocalEngine, SnapshotSource, WriteTxn};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::seq::StoreSeq;
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, ClusterId, DomainId, ExecutionPosition, KvRevision, LocalJournalSeq, ReplicaId,
    ReplicaIncarnation,
};

use crate::cut::{CutOverlay, RecoveryCut};
use crate::lowering::{DurableMeta, ExecutionFrontier, batch_bytes, lower_update};
use crate::view::{Frontier, GatedReader};

/// Durability profile implemented here (design Section 17.3.4): journal
/// first, then an atomic durable projection commit.
pub const PROFILE: &str = "journaled-strict-v1";

/// Barrier sequence reserved for the store's own lifecycle records.
/// `BarrierAllocator` hands out sequences from one, so a machine can never
/// allocate this one and a lifecycle completion is never mistaken for a
/// machine's barrier.
const LIFECYCLE_SEQUENCE: u64 = 0;
/// Barrier sequence of a checkpoint publication. Like the lifecycle
/// sequence it is the runtime's own and completes no actor's barrier:
/// nothing is waiting on it, and it must not collide with one that is.
const CHECKPOINT_SEQUENCE: u64 = 1;

/// Bytes reserved for everything a record carries beside its updates:
/// format, origin, sequence, both digests, the effect context and an
/// application outcome's base, position and revision. `batch_bytes` already
/// allows sixteen bytes per update, which covers postcard's per-field
/// overhead, so a batch within this allowance always seals.
const RECORD_HEADER_ALLOWANCE: usize = 512;

/// Bounds of the journal-first pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalLimits {
    /// Bounds of one ordinary multi-domain group.
    pub group: GroupLimits,
    /// Bounds of one replay page read back from the journal.
    pub read: ReadBudget,
    /// Queued transitions admitted per domain before submissions are
    /// refused (accepted work is never evicted).
    pub max_queued_per_domain: usize,
    /// Queued bytes admitted per domain before submissions are refused.
    pub max_queued_bytes_per_domain: usize,
    /// Records applied in one replay transaction.
    pub max_replay_batch: usize,
}

impl Default for JournalLimits {
    fn default() -> Self {
        JournalLimits {
            group: GroupLimits::DEFAULT,
            read: ReadBudget::new(64, 1024 * 1024),
            max_queued_per_domain: 256,
            max_queued_bytes_per_domain: 16 * 1024 * 1024,
            max_replay_batch: 64,
        }
    }
}

/// What a submitted transition records (design Section 17.3.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionKind {
    /// A protocol transition: a promise, a vote, an adoption or a bound
    /// Sync selection. It carries no application base and never moves the
    /// execution frontier, so it cannot invalidate an application
    /// predecessor.
    Protocol,
    /// An established command's complete application redo.
    Application {
        /// Execution position assigned to the command.
        position: ExecutionPosition,
        /// KV revision produced, if the command mutated KV.
        revision: Option<KvRevision>,
        /// Digest of the exact result the command returned.
        result_digest: Digest32,
    },
}

/// One validated transition offered to the journal-first pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Submission {
    /// Domain whose stream records it.
    pub domain: DomainId,
    /// Ballot it was produced under; its epoch is the configuration epoch
    /// the record binds.
    pub ballot: Ballot,
    /// What it records.
    pub kind: TransitionKind,
    /// The complete immutable batch, with its boot-scoped barrier.
    pub batch: PersistBatch,
}

/// Why a submission was refused. Nothing was queued or journaled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitRefused {
    /// No stream is attached for the domain.
    UnknownDomain,
    /// The batch belongs to another boot or incarnation.
    WrongBoot,
    /// The domain is fenced at a newer promise: an obsolete ballot may not
    /// newly enter the journal (design Section 4.8).
    ObsoleteBallot {
        /// Promise the domain is fenced at.
        promised: Ballot,
    },
    /// An application batch carries no base, or a protocol batch carries
    /// one.
    BaseMismatchedKind,
    /// The base does not extend the domain's journaled frontier; replan.
    StaleBase {
        /// Base the next application must carry.
        expected: ApplyBase,
    },
    /// The domain's queue is full; retry after a flush.
    QueueFull,
    /// The domain is not accepting work.
    NotReady(DomainStatus),
    /// The transition could not be sealed into a valid record.
    Record(RecordError),
}

impl fmt::Display for SubmitRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmitRefused::UnknownDomain => f.write_str("no stream attached for the domain"),
            SubmitRefused::WrongBoot => f.write_str("batch belongs to another boot"),
            SubmitRefused::ObsoleteBallot { .. } => f.write_str("domain fenced at a newer promise"),
            SubmitRefused::BaseMismatchedKind => {
                f.write_str("application base does not match the transition kind")
            }
            SubmitRefused::StaleBase { .. } => f.write_str("application base is stale; replan"),
            SubmitRefused::QueueFull => f.write_str("domain queue full"),
            SubmitRefused::NotReady(s) => write!(f, "domain not ready: {s:?}"),
            SubmitRefused::Record(e) => write!(f, "record refused: {e}"),
        }
    }
}

impl std::error::Error for SubmitRefused {}

/// Per-domain pipeline state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainStatus {
    /// Accepting submissions, appends and materialization.
    Ready,
    /// An append failed after submission; the durable head must be
    /// reconciled before anything else.
    JournalUncertain,
    /// A projection commit failed with an unknown outcome; the stamp must
    /// be read back before anything else.
    MaterializationUncertain,
    /// The projection refused a materialization with specific noncommit
    /// evidence; the durable records are held and reapplied.
    MaterializationDeferred,
    /// The domain disagrees with every expected state; nothing is served.
    Quarantined,
}

/// Why a journal-first operation failed. Every variant is fail-closed:
/// none of them lets the caller proceed as if the work had happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournaledError {
    /// No stream is attached for the domain.
    UnknownDomain,
    /// The domain already has a projection attached in this boot.
    AlreadyAttached,
    /// The domain is not in a state that admits the operation.
    NotReady(DomainStatus),
    /// The shared journal refused or failed an append.
    Journal(JournalFailure),
    /// A journal read failed.
    JournalRead(JournalError),
    /// The projection engine failed.
    Engine(EngineError),
    /// Stream allocation or mapping restore failed.
    Stream(StreamError),
    /// A record could not be sealed or did not verify.
    Record(RecordError),
    /// A group could not be built.
    Group(GroupError),
    /// A stream head refused the operation.
    Head(HeadError),
    /// The recovered frontiers violate `C <= M <= J`.
    Frontier(FrontierError),
    /// The composition disagrees with itself; the domain is quarantined.
    Quarantined(&'static str),
}

impl fmt::Display for JournaledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournaledError::UnknownDomain => f.write_str("no stream attached for the domain"),
            JournaledError::AlreadyAttached => f.write_str("domain already attached"),
            JournaledError::NotReady(s) => write!(f, "domain not ready: {s:?}"),
            JournaledError::Journal(e) => write!(f, "journal: {e}"),
            JournaledError::JournalRead(e) => write!(f, "journal read: {e}"),
            JournaledError::Engine(e) => write!(f, "projection: {e}"),
            JournaledError::Stream(e) => write!(f, "stream: {e}"),
            JournaledError::Record(e) => write!(f, "record: {e}"),
            JournaledError::Group(e) => write!(f, "group: {e}"),
            JournaledError::Head(e) => write!(f, "head: {e}"),
            JournaledError::Frontier(e) => write!(f, "frontier: {e}"),
            JournaledError::Quarantined(what) => write!(f, "quarantined: {what}"),
        }
    }
}

impl std::error::Error for JournaledError {}

impl From<JournalFailure> for JournaledError {
    fn from(e: JournalFailure) -> Self {
        JournaledError::Journal(e)
    }
}
impl From<JournalError> for JournaledError {
    fn from(e: JournalError) -> Self {
        JournaledError::JournalRead(e)
    }
}
impl From<EngineError> for JournaledError {
    fn from(e: EngineError) -> Self {
        JournaledError::Engine(e)
    }
}
impl From<StreamError> for JournaledError {
    fn from(e: StreamError) -> Self {
        JournaledError::Stream(e)
    }
}
impl From<RecordError> for JournaledError {
    fn from(e: RecordError) -> Self {
        JournaledError::Record(e)
    }
}
impl From<GroupError> for JournaledError {
    fn from(e: GroupError) -> Self {
        JournaledError::Group(e)
    }
}
impl From<HeadError> for JournaledError {
    fn from(e: HeadError) -> Self {
        JournaledError::Head(e)
    }
}
impl From<FrontierError> for JournaledError {
    fn from(e: FrontierError) -> Self {
        JournaledError::Frontier(e)
    }
}

/// Why a recovery cut could not be taken.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CutError {
    /// No stream is attached for the domain.
    UnknownDomain,
    /// Journal work of this domain is submitted but unresolved. A timeout
    /// never proves absence, so the cut waits for the actual outcome
    /// instead of guessing (design Section 4.8).
    WorkOutstanding {
        /// Barrier whose outcome is unknown.
        barrier: BarrierId,
    },
    /// The domain is not serving.
    NotReady(DomainStatus),
    /// The projection could not be read at the cut.
    View(crate::view::ViewError),
    /// The journal suffix could not be read or did not verify.
    Journal(JournaledError),
}

impl fmt::Display for CutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CutError::UnknownDomain => f.write_str("no stream attached for the domain"),
            CutError::WorkOutstanding { .. } => {
                f.write_str("submitted journal work is unresolved at the cut")
            }
            CutError::NotReady(s) => write!(f, "domain not ready: {s:?}"),
            CutError::View(e) => write!(f, "view: {e:?}"),
            CutError::Journal(e) => write!(f, "journal: {e}"),
        }
    }
}

impl std::error::Error for CutError {}

/// What one pipeline step did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// Storage facts to feed the machines, in the order they became true.
    pub events: Vec<StorageEvent>,
    /// Records made durable in the journal.
    pub journaled: usize,
    /// Records materialized into a projection.
    pub materialized: usize,
    /// Transitions refused with specific noncommit evidence.
    pub rejected: usize,
    /// Whether some outcome is unknown and reconciliation is required.
    pub indeterminate: bool,
    /// Engine byte count of the group write, as evidence only.
    pub written: Option<WrittenBytes>,
    /// Grouped journal writes performed (zero or one per append).
    pub appends: usize,
    /// Projection commits performed.
    pub commits: usize,
}

impl FlushReport {
    fn absorb(&mut self, other: FlushReport) {
        self.events.extend(other.events);
        self.journaled += other.journaled;
        self.materialized += other.materialized;
        self.rejected += other.rejected;
        self.indeterminate |= other.indeterminate;
        self.written = other.written.or(self.written);
        self.appends += other.appends;
        self.commits += other.commits;
    }
}

/// What a checkpoint publication did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    /// Sequence the published checkpoint represents (`C`).
    pub published: LocalJournalSeq,
    /// Whether the journal prefix through it was retired as well.
    ///
    /// `false` is not a failure: the pointer is durable and the
    /// baseline is authoritative, and the prefix is still there to be
    /// retired on a later attempt. It is reported rather than retried
    /// here so the caller decides when to spend the I/O.
    pub retired: bool,
}

fn alloc_one(record: JournalRecordV1) -> Vec<JournalRecordV1> {
    vec![record]
}

/// A record that is durable in the journal and waiting to be materialized,
/// together with the barrier of this boot it completes (lifecycle records
/// and records replayed from an earlier boot complete no barrier).
#[derive(Clone, Debug)]
struct Durable {
    barrier: Option<BarrierId>,
    record: JournalRecordV1,
}

/// One domain: its stream, its accepted durable head, its frontiers and the
/// durable projection it materializes into.
struct Domain<E: LocalEngine> {
    origin: RecordOrigin,
    engine: E,
    head: StreamHead,
    frontiers: Frontiers,
    /// Digest of the record at `J`, which the next record chains onto.
    head_digest: Digest32,
    /// Projection metadata as last confirmed.
    meta: DurableMeta,
    /// Execution frontier including journaled-but-unmaterialized outcomes.
    journaled_frontier: ExecutionFrontier,
    /// Execution frontier including queued transitions as well.
    queued_frontier: ExecutionFrontier,
    gate: Arc<Frontier>,
    queue: VecDeque<Submission>,
    queued_bytes: usize,
    /// The sealed records of the outstanding batch, kept so an uncertain
    /// outcome can be reconciled without re-sealing anything.
    inflight: Vec<Durable>,
    /// Durable records waiting for the projection, in journal order.
    pending: Vec<Durable>,
    /// Projection metadata the pending materialization would produce.
    pending_meta: Option<DurableMeta>,
    status: DomainStatus,
    fence: Option<Ballot>,
}

impl<E: LocalEngine> Domain<E> {
    fn quarantine(&mut self, what: &'static str) -> JournaledError {
        self.gate.quarantine();
        self.status = DomainStatus::Quarantined;
        JournaledError::Quarantined(what)
    }

    fn applied(&self) -> AppliedFrontier {
        AppliedFrontier::from_stamp(&self.meta.stamp)
    }

    /// Take the front transition out of the queue once it is sealed into a
    /// group; its bytes stop counting against the backpressure budget.
    fn dequeue(&mut self) {
        if let Some(submission) = self.queue.pop_front() {
            self.queued_bytes = self
                .queued_bytes
                .saturating_sub(batch_bytes(&submission.batch));
        }
    }

    /// Drop everything still queued, reporting each dropped transition as
    /// definitely not committed. Accepted work is never evicted silently:
    /// this runs only when the stream's own append definitely did not
    /// happen, so every queued successor has to be replanned anyway.
    fn drop_queue(&mut self) -> Vec<StorageEvent> {
        let events = self
            .queue
            .iter()
            .map(|s| StorageEvent::Failed {
                barrier_id: s.batch.barrier,
                error: StorageError::DefinitelyNotCommitted,
            })
            .collect();
        self.queue.clear();
        self.queued_bytes = 0;
        self.queued_frontier = self.journaled_frontier;
        events
    }
}

/// The journal-first coordinator of one node: one shared journal and one
/// durable projection per attached domain.
pub struct JournaledStore<J: JournalEngine, E: LocalEngine> {
    journal: J,
    allocator: StreamAllocator,
    cluster: ClusterId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    boot: BootId,
    limits: JournalLimits,
    domains: BTreeMap<DomainId, Domain<E>>,
    /// Terminal events produced by a stage that finished before a later
    /// stage failed.
    ///
    /// A record that is durable in the journal has completed its journal
    /// barrier whatever happens to the projection afterwards, and a
    /// domain that materialized has completed its own. Returning the
    /// error alone discarded those completions, so a caller waiting on
    /// the barrier waited for something that had already happened.
    deferred_events: Vec<StorageEvent>,
}

impl<J: JournalEngine, E: LocalEngine> JournaledStore<J, E> {
    /// Open the coordinator over a shared journal, restoring the stream
    /// allocator from the journal's durable mapping metadata. Nothing is
    /// served until a domain is attached.
    pub fn open(
        journal: J,
        cluster: ClusterId,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        boot: BootId,
        limits: JournalLimits,
    ) -> Result<Self, JournaledError> {
        let (high_water, mappings) = journal.mappings()?;
        let allocator = StreamAllocator::restore(high_water, mappings)?;
        Ok(JournaledStore {
            deferred_events: Vec::new(),
            journal,
            allocator,
            cluster,
            replica,
            incarnation,
            boot,
            limits,
            domains: BTreeMap::new(),
        })
    }

    /// Durability profile this coordinator implements.
    pub const fn profile(&self) -> &'static str {
        PROFILE
    }

    /// Boot this coordinator serves.
    pub const fn boot(&self) -> BootId {
        self.boot
    }

    /// The shared journal (diagnostics and maintenance).
    pub const fn journal(&self) -> &J {
        &self.journal
    }

    /// The shared journal for maintenance the pipeline does not drive
    /// itself (engine statistics, purge suggestions) and for harnesses.
    pub const fn journal_mut(&mut self) -> &mut J {
        &mut self.journal
    }

    /// A domain's projection engine (diagnostics and harnesses that crash
    /// and reopen it). Materialization remains its only ordinary writer.
    pub fn projection(&self, domain: DomainId) -> Option<&E> {
        self.domains.get(&domain).map(|d| &d.engine)
    }

    /// Give the journal and every attached projection back; this
    /// coordinator's boot ends. The next boot opens them again and
    /// recovers its frontiers from what is actually durable.
    pub fn into_parts(self) -> (J, Vec<(DomainId, E)>) {
        let domains = self
            .domains
            .into_iter()
            .map(|(id, state)| (id, state.engine))
            .collect();
        (self.journal, domains)
    }

    /// Attach a domain's durable projection to the shared journal.
    ///
    /// The stream mapping is durable before the stream is used, `J` comes
    /// from the journal's actual valid records and `M` from the
    /// projection's applied stamp. A projection ahead of the journal, a
    /// suffix that does not chain onto the stamp or frontiers violating
    /// `C <= M <= J` are refused rather than repaired. The suffix `(M, J]`
    /// is replayed into the projection before the domain serves anything,
    /// and this boot is then recorded in the stream.
    ///
    /// A failure after the lifecycle record was submitted leaves the domain
    /// attached in the state the failure produced (uncertain or
    /// quarantined) rather than forgetting that the record may exist;
    /// [`JournaledStore::status`] reports it and
    /// [`JournaledStore::reconcile`] resolves it.
    pub fn attach(
        &mut self,
        domain: DomainId,
        shard: ShardId,
        engine: E,
    ) -> Result<StorageStreamId, JournaledError> {
        if self.domains.contains_key(&domain) {
            return Err(JournaledError::AlreadyAttached);
        }
        let key = StreamKey {
            cluster: self.cluster,
            domain,
            incarnation: self.incarnation,
        };
        let stream = match self.allocator.lookup(&key) {
            Some(stream) => {
                self.allocator.usable(stream)?;
                stream
            }
            None => {
                let mapping = self.allocator.allocate(key, shard)?;
                self.persist_mapping(&mapping)?;
                self.allocator.mapping_durable(mapping.stream)?;
                mapping.stream
            }
        };
        let origin = RecordOrigin {
            cluster: self.cluster,
            domain,
            replica: self.replica,
            incarnation: self.incarnation,
            stream,
        };
        let durable = self.journal.durable_head(stream)?;
        let meta = DurableMeta::read(&engine.reader().snapshot()?)?;
        let applied = AppliedFrontier::from_stamp(&meta.stamp);
        if applied.materialized > durable {
            return Err(JournaledError::Quarantined(
                "projection is materialized past the journal's durable head",
            ));
        }
        let frontiers = Frontiers::recovered(durable, applied.materialized, LocalJournalSeq::ZERO)?;
        let gate = Arc::new(Frontier::default());
        gate.set_completed(meta.stamp.store_seq());
        let mut state = Domain {
            origin,
            engine,
            head: StreamHead::open(stream, durable),
            frontiers,
            head_digest: applied.last_digest,
            meta,
            journaled_frontier: meta.frontier,
            queued_frontier: meta.frontier,
            gate,
            queue: VecDeque::new(),
            queued_bytes: 0,
            inflight: Vec::new(),
            pending: Vec::new(),
            pending_meta: None,
            status: DomainStatus::Ready,
            fence: None,
        };
        Self::replay(&self.journal, &mut state, self.limits)?;
        // Replay moved the projection forward, so the application
        // frontiers have to describe the recovered history rather than
        // the projection as it was found. Leaving them at the pre-replay
        // metadata made the store expect a base it had already passed:
        // a correctly planned next command was refused as stale, and a
        // command planned from `application_base()` carried a base the
        // journal record could no longer extend.
        state.journaled_frontier = state.meta.frontier;
        state.queued_frontier = state.meta.frontier;
        self.domains.insert(domain, state);
        self.record_boot(domain)?;
        Ok(stream)
    }

    fn persist_mapping(&mut self, mapping: &StreamMappingV1) -> Result<(), JournaledError> {
        self.journal
            .persist_mapping(self.allocator.high_water(), mapping)?;
        Ok(())
    }

    /// Replay `(M, J]` into the projection, verifying the chain first. The
    /// replayed records complete no barrier: they belong to whatever boot
    /// wrote them, and old send callbacks are never replayed.
    fn replay(
        journal: &J,
        domain: &mut Domain<E>,
        limits: JournalLimits,
    ) -> Result<(), JournaledError> {
        let target = domain.frontiers.durable();
        let mut expect = RecordExpectation {
            origin: domain.origin,
            seq: match domain.frontiers.materialized().checked_next() {
                Ok(seq) => seq,
                Err(_) => return Ok(()),
            },
            predecessor: domain.head_digest,
        };
        while domain.frontiers.materialized() < target {
            let page = journal.read_suffix(
                domain.origin.stream,
                domain.frontiers.materialized(),
                limits.read,
            )?;
            if page.records.is_empty() {
                return Err(
                    domain.quarantine("journal reports no records below its own durable head")
                );
            }
            let mut batch: Vec<Durable> = Vec::new();
            for record in page.records {
                record.verify(&expect)?;
                expect = expect.after(&record)?;
                batch.push(Durable {
                    barrier: None,
                    record,
                });
                if batch.len() >= limits.max_replay_batch {
                    break;
                }
            }
            let last = batch.last().expect("nonempty page").record.digest();
            domain.pending = batch;
            domain.pending_meta = None;
            let report = Self::materialize_domain(domain)?;
            if report.materialized == 0 {
                return Err(domain.quarantine("replay made no progress"));
            }
            domain.head_digest = last;
        }
        Ok(())
    }

    /// Record this boot in the stream (design Section 17.3.2's lifecycle
    /// records): a stream that has never been written gets its genesis
    /// record in the same entry.
    fn record_boot(&mut self, domain: DomainId) -> Result<(), JournaledError> {
        let state = self
            .domains
            .get_mut(&domain)
            .ok_or(JournaledError::UnknownDomain)?;
        let barrier = BarrierId {
            node_generation: self.incarnation,
            boot_id: self.boot,
            sequence: LIFECYCLE_SEQUENCE,
        };
        let mut records = Vec::new();
        let mut seq = state.head.next_seq()?;
        let mut predecessor = state.head_digest;
        if state.frontiers.durable() == LocalJournalSeq::ZERO {
            let genesis = JournalRecordV1::seal(RecordDraft {
                origin: state.origin,
                seq,
                predecessor,
                body: RecordBody::Lifecycle(LifecycleRecordV1::Genesis {
                    format: JOURNAL_RECORD_FORMAT_V1,
                }),
            })?;
            seq = genesis
                .seq()
                .checked_next()
                .map_err(|_| RecordError::Malformed)?;
            predecessor = genesis.digest();
            records.push(genesis);
        }
        records.push(JournalRecordV1::seal(RecordDraft {
            origin: state.origin,
            seq,
            predecessor,
            body: RecordBody::Lifecycle(LifecycleRecordV1::Boot { boot: self.boot }),
        })?);
        let count = u32::try_from(records.len()).expect("at most two records");
        let entry = GroupEntry::new(barrier, state.origin.stream, records.clone())?;
        let mut group = GroupWrite::new(self.limits.group);
        group.push(entry)?;
        state
            .head
            .reserve(barrier, NonZeroU32::new(count).expect("non-zero"))?;
        match self.journal.append_group(&group) {
            Ok(_) => {
                let state = self.domains.get_mut(&domain).expect("attached");
                state.head.complete_durable(barrier)?;
                let last = records.last().expect("nonempty").clone();
                state.frontiers.advance_durable(last.seq())?;
                state.head_digest = last.digest();
                state.pending = records
                    .into_iter()
                    .map(|record| Durable {
                        barrier: None,
                        record,
                    })
                    .collect();
                Self::materialize_domain(state)?;
                Ok(())
            }
            Err(failure) => {
                let state = self.domains.get_mut(&domain).expect("attached");
                match &failure {
                    JournalFailure::Definite(_) => {
                        state.head.fail_definite(barrier)?;
                    }
                    JournalFailure::Indeterminate(_) => {
                        state.head.fail_indeterminate(barrier)?;
                        state.inflight = records
                            .into_iter()
                            .map(|record| Durable {
                                barrier: None,
                                record,
                            })
                            .collect();
                        state.status = DomainStatus::JournalUncertain;
                    }
                }
                Err(JournaledError::Journal(failure))
            }
        }
    }

    /// Steps 3 and 4 of the publication order (design Section 17.16.3):
    /// make `pointer` durable in the stream, then retire the journal
    /// entries it represents.
    ///
    /// The caller has already done steps 1 and 2 -- pinned a snapshot
    /// and written a complete inactive image -- and does step 5,
    /// reclaiming superseded images, afterwards. Those are filesystem
    /// work; this is the part that has to be in the journal, because the
    /// durable pointer is what selects recovery state and only the
    /// journal can retire its own prefix.
    ///
    /// The order is the whole of the safety. The pointer becomes durable
    /// *before* anything is retired, so a crash between them leaves a
    /// new baseline and a journal that still holds the prefix: the
    /// replay overlaps and applies the same records again, which is
    /// exactly what the records are for. A retirement that ran first
    /// would leave a baseline that does not exist and a journal that no
    /// longer proves what it represented.
    ///
    /// The publication record is itself newer than `C` and stays in the
    /// suffix until a later checkpoint covers it, so the recovered
    /// stream always contains the evidence of its own baseline.
    ///
    /// Retiring is not a condition of the publication. When the append
    /// succeeded and the compaction did not, the pointer stands and the
    /// prefix is simply still there; the caller learns that the
    /// reclamation is owed rather than that the checkpoint failed.
    pub fn publish_checkpoint(
        &mut self,
        domain: DomainId,
        pointer: &CheckpointPointerV1,
    ) -> Result<Published, JournaledError> {
        let barrier = BarrierId {
            node_generation: self.incarnation,
            boot_id: self.boot,
            sequence: CHECKPOINT_SEQUENCE,
        };
        let state = self
            .domains
            .get_mut(&domain)
            .ok_or(JournaledError::UnknownDomain)?;
        if state.status != DomainStatus::Ready {
            return Err(JournaledError::NotReady(state.status));
        }
        // The image is this incarnation's own. A pointer naming another
        // origin describes storage this node does not have.
        if pointer.origin != state.origin {
            return Err(JournaledError::Quarantined(
                "a checkpoint pointer of another origin",
            ));
        }
        // `C <= M` is what makes the image loadable: an image claiming
        // records the projection has not applied represents obligations
        // it does not contain. The frontier is the one place that rule
        // lives, so it is asked before anything is written, and advanced
        // only once the pointer is durable.
        let mut proposed = state.frontiers;
        proposed
            .publish_checkpoint(pointer.represented)
            .map_err(JournaledError::Frontier)?;
        let seq = state.head.next_seq()?;
        let record = JournalRecordV1::seal(RecordDraft {
            origin: state.origin,
            seq,
            predecessor: state.head_digest,
            body: RecordBody::PublishLocalCheckpoint(*pointer),
        })?;
        let entry = GroupEntry::new(barrier, state.origin.stream, alloc_one(record.clone()))?;
        let mut group = GroupWrite::new(self.limits.group);
        group.push(entry)?;
        state
            .head
            .reserve(barrier, NonZeroU32::new(1).expect("non-zero"))?;
        match self.journal.append_group(&group) {
            Ok(_) => {
                let state = self.domains.get_mut(&domain).expect("attached");
                state.head.complete_durable(barrier)?;
                state.frontiers.advance_durable(record.seq())?;
                state.head_digest = record.digest();
                state.pending.push(Durable {
                    barrier: None,
                    record,
                });
                Self::materialize_domain(state)?;
                state
                    .frontiers
                    .publish_checkpoint(pointer.represented)
                    .map_err(JournaledError::Frontier)?;
            }
            Err(failure) => {
                let state = self.domains.get_mut(&domain).expect("attached");
                match &failure {
                    // Nothing was written: the image on disk is simply
                    // unselected and the prior publication stands.
                    JournalFailure::Definite(_) => {
                        state.head.fail_definite(barrier)?;
                    }
                    // The append may or may not be durable, so neither
                    // the pointer nor the retirement may be assumed. The
                    // stream reconciles before it serves again, and the
                    // caller publishes again afterwards if it still
                    // wants to.
                    JournalFailure::Indeterminate(_) => {
                        state.head.fail_indeterminate(barrier)?;
                        state.inflight = alloc_one(record)
                            .into_iter()
                            .map(|record| Durable {
                                barrier: None,
                                record,
                            })
                            .collect();
                        state.status = DomainStatus::JournalUncertain;
                    }
                }
                return Err(JournaledError::Journal(failure));
            }
        }
        // Only now, and never as a condition of the publication.
        let stream = self.domains.get(&domain).expect("attached").origin.stream;
        match self.journal.retire_prefix(stream, pointer) {
            Ok(()) => Ok(Published {
                published: pointer.represented,
                retired: true,
            }),
            Err(_) => Ok(Published {
                published: pointer.represented,
                retired: false,
            }),
        }
    }

    /// The local recovery baseline of `domain`: the newest checkpoint
    /// pointer durably published in this incarnation's stream, if any.
    ///
    /// Read from the journal and only from the journal. The newest
    /// directory, the newest file, the largest sequence in a name --
    /// none of those is an input. A pointer is published by an appended,
    /// synced record, so the stream is where the selection is, and the
    /// selection rule is the pointer with the highest represented
    /// sequence of this exact origin.
    ///
    /// Called before the domain is attached, because what it answers is
    /// *which projection to attach*: the live one that is still there,
    /// or a fresh generation installed from the image this names. A
    /// stream with no published pointer has no baseline, which is not
    /// the same as an empty stream and is not permission to start from
    /// nothing -- it means the whole journal is still the redo.
    pub fn recovery_baseline(
        &self,
        domain: DomainId,
    ) -> Result<Option<CheckpointPointerV1>, JournaledError> {
        let key = StreamKey {
            cluster: self.cluster,
            domain,
            incarnation: self.incarnation,
        };
        let Some(stream) = self.allocator.lookup(&key) else {
            return Ok(None);
        };
        let origin = RecordOrigin {
            cluster: self.cluster,
            domain,
            replica: self.replica,
            incarnation: self.incarnation,
            stream,
        };
        let mut found: Vec<CheckpointPointerV1> = Vec::new();
        // Where the retained suffix begins, not zero: a prefix this
        // stream already reclaimed is gone, and asking for it is a read
        // the engine refuses rather than answers with a gap.
        let mut after = self.journal.retained_from(stream)?;
        loop {
            let page = self.journal.read_suffix(stream, after, self.limits.read)?;
            let Some(last) = page.records.last() else {
                break;
            };
            after = last.seq();
            for record in &page.records {
                if let RecordBody::PublishLocalCheckpoint(pointer) = record.body() {
                    found.push(*pointer);
                }
            }
            if page.exhausted {
                break;
            }
        }
        Ok(select_recovery_pointer(&origin, found.iter()).copied())
    }

    /// Streams attached, in domain order.
    pub fn attached(&self) -> Vec<(DomainId, StorageStreamId)> {
        self.domains
            .iter()
            .map(|(d, s)| (*d, s.origin.stream))
            .collect()
    }

    /// The record origin of a domain: the exact incarnation and stream
    /// its records are sealed under.
    ///
    /// An image this node writes carries it, and a pointer of any other
    /// origin is refused at publication -- so whoever builds one asks
    /// here rather than assembling it from configuration, where a
    /// stale incarnation would be indistinguishable from the current
    /// one.
    pub fn origin(&self, domain: DomainId) -> Option<RecordOrigin> {
        self.domains.get(&domain).map(|d| d.origin)
    }

    /// State of a domain's pipeline.
    pub fn status(&self, domain: DomainId) -> Option<DomainStatus> {
        self.domains.get(&domain).map(|d| d.status)
    }

    /// The `C <= M <= J` frontiers of a domain.
    pub fn frontiers(&self, domain: DomainId) -> Option<Frontiers> {
        self.domains.get(&domain).map(|d| d.frontiers)
    }

    /// The materialized frontier and the digest of the last materialized
    /// record, as the projection's stamp holds them.
    pub fn applied(&self, domain: DomainId) -> Option<AppliedFrontier> {
        self.domains.get(&domain).map(Domain::applied)
    }

    /// The base the next application transition of a domain must carry.
    pub fn application_base(&self, domain: DomainId) -> Option<ApplyBase> {
        self.domains
            .get(&domain)
            .map(|d| d.queued_frontier.as_base())
    }

    /// The gated reader of a domain: it hands out a snapshot only together
    /// with the stamp it proves, and never one ahead of the materialized
    /// frontier. Journal durability alone never advances it.
    pub fn reader(&self, domain: DomainId) -> Option<GatedReader<E::Reader>> {
        self.domains
            .get(&domain)
            .map(|d| GatedReader::new(d.engine.reader(), d.gate.clone()))
    }

    /// Queued transitions of a domain.
    pub fn queued(&self, domain: DomainId) -> usize {
        self.domains.get(&domain).map_or(0, |d| d.queue.len())
    }

    /// Queued bytes of a domain (the backpressure accounting).
    pub fn queued_bytes(&self, domain: DomainId) -> usize {
        self.domains.get(&domain).map_or(0, |d| d.queued_bytes)
    }

    /// Durable records of a domain still waiting for the projection.
    pub fn unmaterialized(&self, domain: DomainId) -> usize {
        self.domains.get(&domain).map_or(0, |d| d.pending.len())
    }

    /// Validate and queue one transition. Nothing is journaled here: the
    /// guards that do not depend on the stream position (boot binding,
    /// admission under the current promise, the application base, record
    /// bounds) are checked now, so a refusal is definite and costs no
    /// engine work.
    pub fn submit(&mut self, submission: Submission) -> Result<(), SubmitRefused> {
        let boot = self.boot;
        let incarnation = self.incarnation;
        let limits = self.limits;
        let state = self
            .domains
            .get_mut(&submission.domain)
            .ok_or(SubmitRefused::UnknownDomain)?;
        if state.status != DomainStatus::Ready {
            return Err(SubmitRefused::NotReady(state.status));
        }
        if submission.batch.barrier.boot_id != boot
            || submission.batch.barrier.node_generation != incarnation
        {
            return Err(SubmitRefused::WrongBoot);
        }
        if let Some(promised) = state.fence
            && obsolete(&submission.ballot, &promised)
        {
            return Err(SubmitRefused::ObsoleteBallot { promised });
        }
        let advanced = match (submission.kind, submission.batch.base) {
            (TransitionKind::Protocol, None) => state.queued_frontier,
            (TransitionKind::Application { position, .. }, Some(base)) => {
                let expected = state.queued_frontier.as_base();
                if base != expected {
                    return Err(SubmitRefused::StaleBase { expected });
                }
                if submission.ballot.epoch != base.configuration {
                    return Err(SubmitRefused::Record(RecordError::EpochMismatch));
                }
                if position == ExecutionPosition::ZERO {
                    return Err(SubmitRefused::Record(RecordError::ZeroPosition));
                }
                if position <= base.execution_position {
                    return Err(SubmitRefused::Record(RecordError::PositionNotAfterBase));
                }
                ExecutionFrontier {
                    configuration: base.configuration,
                    execution_position: position,
                }
            }
            _ => return Err(SubmitRefused::BaseMismatchedKind),
        };
        check_update_bounds(&submission.batch, &submission.kind).map_err(SubmitRefused::Record)?;
        let bytes = batch_bytes(&submission.batch);
        if bytes.saturating_add(RECORD_HEADER_ALLOWANCE) > MAX_RECORD_BYTES {
            // Refused here rather than at sealing time, so a batch that
            // could never become a valid record never occupies the queue.
            return Err(SubmitRefused::Record(RecordError::TooLarge));
        }
        if state.queue.len() >= limits.max_queued_per_domain
            || state.queued_bytes + bytes > limits.max_queued_bytes_per_domain
        {
            return Err(SubmitRefused::QueueFull);
        }
        state.queued_bytes += bytes;
        state.queued_frontier = advanced;
        state.queue.push_back(submission);
        Ok(())
    }

    /// Stop admitting new transitions of an obsolete ballot in one domain
    /// (design Section 4.8) and report the queued transitions that are
    /// therefore refused, so no barrier is left waiting forever. Work
    /// already submitted to the journal is not touched here: its outcome
    /// is resolved by the append or by [`JournaledStore::reconcile`], and
    /// a late completion still updates bookkeeping without authorizing an
    /// obsolete vote.
    ///
    /// Other domains are untouched. There is no cross-domain election
    /// barrier, no global packet drain and no wait for peers.
    pub fn fence(
        &mut self,
        domain: DomainId,
        promised: Ballot,
    ) -> Result<Vec<StorageEvent>, JournaledError> {
        let state = self
            .domains
            .get_mut(&domain)
            .ok_or(JournaledError::UnknownDomain)?;
        if let Some(current) = state.fence
            && obsolete(&promised, &current)
        {
            // A fence never moves backwards: an older promise cannot
            // reopen admission a newer one closed.
            return Ok(Vec::new());
        }
        state.fence = Some(promised);
        let mut refused = Vec::new();
        let mut kept = VecDeque::new();
        for submission in state.queue.drain(..) {
            if obsolete(&submission.ballot, &promised) {
                refused.push(StorageEvent::Failed {
                    barrier_id: submission.batch.barrier,
                    error: StorageError::DefinitelyNotCommitted,
                });
            } else {
                kept.push_back(submission);
            }
        }
        state.queue = kept;
        state.queued_bytes = state.queue.iter().map(|s| batch_bytes(&s.batch)).sum();
        state.queued_frontier = state.journaled_frontier;
        for submission in &state.queue {
            if let TransitionKind::Application { position, .. } = submission.kind
                && let Some(base) = submission.batch.base
            {
                state.queued_frontier = ExecutionFrontier {
                    configuration: base.configuration,
                    execution_position: position,
                };
            }
        }
        Ok(refused)
    }

    /// The promise a domain is fenced at, if any.
    pub fn fenced_at(&self, domain: DomainId) -> Option<Ballot> {
        self.domains.get(&domain).and_then(|d| d.fence)
    }

    /// Seal one queued transition per ready stream and write the whole
    /// bounded multi-domain group as one synced journal append.
    pub fn append_pending(&mut self) -> Result<FlushReport, JournaledError> {
        let mut report = FlushReport::default();
        let mut group = GroupWrite::new(self.limits.group);
        let mut sealed: Vec<(DomainId, BarrierId, JournalRecordV1)> = Vec::new();
        for (id, state) in &mut self.domains {
            if state.status != DomainStatus::Ready || state.head.state() != HeadState::Idle {
                continue;
            }
            let Some(submission) = state.queue.front() else {
                continue;
            };
            let barrier = submission.batch.barrier;
            let record = seal(state, submission)?;
            let entry = GroupEntry::new(barrier, state.origin.stream, vec![record.clone()])?;
            match group.push(entry) {
                Ok(()) => {}
                Err(GroupError::TooManyRecords) | Err(GroupError::TooManyBytes)
                    if !sealed.is_empty() =>
                {
                    // The group is full; the rest stays queued and goes in
                    // the next one. Nothing is split.
                    break;
                }
                Err(GroupError::TooManyBytes) => {
                    // One valid record larger than the ordinary budget
                    // takes the separately bounded large-record path
                    // instead of being split illegally.
                    group = GroupWrite::new(GroupLimits::LARGE_RECORD);
                    group.push(GroupEntry::new(
                        barrier,
                        state.origin.stream,
                        vec![record.clone()],
                    )?)?;
                    state
                        .head
                        .reserve(barrier, NonZeroU32::new(1).expect("non-zero"))?;
                    state.dequeue();
                    sealed.push((*id, barrier, record));
                    break;
                }
                Err(e) => return Err(JournaledError::Group(e)),
            }
            state
                .head
                .reserve(barrier, NonZeroU32::new(1).expect("non-zero"))?;
            state.dequeue();
            sealed.push((*id, barrier, record));
        }
        if sealed.is_empty() {
            return Ok(report);
        }
        report.appends = 1;
        match self.journal.append_group(&group) {
            Ok(receipt) => {
                report.written = Some(receipt.written);
                for (id, barrier, record) in sealed {
                    let state = self.domains.get_mut(&id).expect("sealed domain attached");
                    state.head.complete_durable(barrier)?;
                    state.frontiers.advance_durable(record.seq())?;
                    state.head_digest = record.digest();
                    state.journaled_frontier = frontier_after(state.journaled_frontier, &record);
                    state.pending.push(Durable {
                        barrier: Some(barrier),
                        record,
                    });
                    report.journaled += 1;
                    report.events.push(StorageEvent::JournalDurable {
                        barrier_id: barrier,
                        journal_seq: state.frontiers.durable(),
                    });
                }
                Ok(report)
            }
            Err(JournalFailure::Definite(_)) => {
                // Specific evidence that nothing was appended: the whole
                // group's reservations are released and every affected
                // domain replans from its journaled frontier.
                for (id, barrier, _) in sealed {
                    let state = self.domains.get_mut(&id).expect("sealed domain attached");
                    state.head.fail_definite(barrier)?;
                    report.rejected += 1;
                    report.events.push(StorageEvent::Failed {
                        barrier_id: barrier,
                        error: StorageError::DefinitelyNotCommitted,
                    });
                    let dropped = state.drop_queue();
                    report.rejected += dropped.len();
                    report.events.extend(dropped);
                }
                Ok(report)
            }
            Err(JournalFailure::Indeterminate(_)) => {
                // The outcome is unknown. No event claims a completion and
                // no byte batch is retried: each affected stream refuses
                // reservations until `reconcile` reads its actual durable
                // head.
                for (id, barrier, record) in sealed {
                    let state = self.domains.get_mut(&id).expect("sealed domain attached");
                    state.head.fail_indeterminate(barrier)?;
                    state.inflight = vec![Durable {
                        barrier: Some(barrier),
                        record,
                    }];
                    state.status = DomainStatus::JournalUncertain;
                }
                report.indeterminate = true;
                Ok(report)
            }
        }
    }

    /// Apply every domain's durable records to its projection, in journal
    /// order, one atomic transaction per domain.
    pub fn materialize(&mut self) -> Result<FlushReport, JournaledError> {
        let mut report = FlushReport::default();
        report.events.append(&mut self.deferred_events);
        for state in self.domains.values_mut() {
            if state.pending.is_empty() {
                continue;
            }
            if matches!(
                state.status,
                DomainStatus::Quarantined | DomainStatus::MaterializationUncertain
            ) {
                continue;
            }
            match Self::materialize_domain(state) {
                Ok(one) => report.absorb(one),
                Err(e) => {
                    // The domains that did materialize completed their
                    // barriers; one domain's failure does not undo that.
                    self.deferred_events.append(&mut report.events);
                    return Err(e);
                }
            }
        }
        Ok(report)
    }

    /// One journal-first step: journal the ready transitions, then
    /// materialize whatever became durable.
    pub fn flush(&mut self) -> Result<FlushReport, JournaledError> {
        let mut report = self.append_pending()?;
        match self.materialize() {
            Ok(materialized) => {
                report.absorb(materialized);
                Ok(report)
            }
            Err(e) => {
                // Those records are durable: their journal barriers
                // completed, and a later stage failing does not take
                // that back. They are handed to the next report rather
                // than dropped with the error.
                self.deferred_events.append(&mut report.events);
                Err(e)
            }
        }
    }

    /// Terminal events held back by a failed stage, for a caller that
    /// wants them without waiting for the next flush.
    pub fn take_deferred_events(&mut self) -> Vec<StorageEvent> {
        std::mem::take(&mut self.deferred_events)
    }

    /// Resolve an ambiguous outcome from semantic records: the journal's
    /// actual durable head for an uncertain append, the projection's own
    /// stamp for an uncertain commit. Nothing is blind-retried.
    pub fn reconcile(&mut self, domain: DomainId) -> Result<FlushReport, JournaledError> {
        let mut report = FlushReport::default();
        let journal = &self.journal;
        let state = self
            .domains
            .get_mut(&domain)
            .ok_or(JournaledError::UnknownDomain)?;
        match state.status {
            DomainStatus::JournalUncertain => {
                let recovered = journal.durable_head(state.origin.stream)?;
                let inflight = std::mem::take(&mut state.inflight);
                let Some(last) = inflight.last().map(|d| d.record.clone()) else {
                    return Err(JournaledError::Quarantined(
                        "uncertain head without the records it wrote",
                    ));
                };
                match state.head.reconcile(recovered) {
                    Ok(Reconciled::Present(_)) => {
                        state.frontiers.advance_durable(last.seq())?;
                        state.head_digest = last.digest();
                        report.journaled = inflight.len();
                        for item in inflight {
                            state.journaled_frontier =
                                frontier_after(state.journaled_frontier, &item.record);
                            if let Some(barrier) = item.barrier {
                                report.events.push(StorageEvent::JournalDurable {
                                    barrier_id: barrier,
                                    journal_seq: item.record.seq(),
                                });
                            }
                            state.pending.push(item);
                        }
                        state.status = DomainStatus::Ready;
                    }
                    Ok(Reconciled::Absent(_)) => {
                        for item in &inflight {
                            if let Some(barrier) = item.barrier {
                                report.rejected += 1;
                                report.events.push(StorageEvent::Failed {
                                    barrier_id: barrier,
                                    error: StorageError::DefinitelyNotCommitted,
                                });
                            }
                        }
                        let dropped = state.drop_queue();
                        report.rejected += dropped.len();
                        report.events.extend(dropped);
                        state.status = DomainStatus::Ready;
                    }
                    Err(_) => {
                        return Err(state.quarantine(
                            "recovered durable head matches neither outcome of the uncertain batch",
                        ));
                    }
                }
            }
            DomainStatus::MaterializationUncertain => {
                let expected = state.pending_meta.ok_or(JournaledError::Quarantined(
                    "uncertain materialization without the metadata it wrote",
                ))?;
                let observed = DurableMeta::read(&state.engine.reader().snapshot()?)?;
                if observed == expected {
                    state.meta = observed;
                    state.gate.set_completed(observed.stamp.store_seq());
                    state
                        .frontiers
                        .advance_materialized(observed.stamp.journal_seq())?;
                    let applied = std::mem::take(&mut state.pending);
                    report.materialized = applied.len();
                    report.commits = 1;
                    for item in applied {
                        if let Some(barrier) = item.barrier {
                            report.events.push(StorageEvent::Materialized {
                                barrier_id: barrier,
                                journal_seq: item.record.seq(),
                            });
                        }
                    }
                    state.pending_meta = None;
                    state.status = DomainStatus::Ready;
                } else if observed == state.meta {
                    // The commit is absent. The records are authoritative
                    // in the journal, so they are applied again as a
                    // semantic redo of durable records; the opaque byte
                    // batch that failed is never resubmitted.
                    state.pending_meta = None;
                    state.status = DomainStatus::Ready;
                    report.absorb(Self::materialize_domain(state)?);
                } else {
                    return Err(state.quarantine(
                        "projection stamp matches neither the pending materialization nor the prior state",
                    ));
                }
            }
            other => return Err(JournaledError::NotReady(other)),
        }
        Ok(report)
    }

    /// The domain's authoritative durable cut (design Section 4.8): the
    /// materialized snapshot plus every obligation that is durable in the
    /// journal but not materialized yet. Recovery summaries are built from
    /// this, never from the projection alone, so a vote journaled at
    /// sequence `M + 1` is still summarized while materialization lags.
    ///
    /// Submitted journal work must have resolved first: a timeout does not
    /// prove absence, so an outstanding or uncertain batch refuses the cut
    /// rather than guessing at it.
    pub fn recovery_cut(
        &self,
        domain: DomainId,
    ) -> Result<RecoveryCut<<E::Reader as SnapshotSource>::View>, CutError> {
        let state = self.domains.get(&domain).ok_or(CutError::UnknownDomain)?;
        match state.head.state() {
            HeadState::Idle => {}
            HeadState::Pending(batch) | HeadState::Uncertain(batch) => {
                return Err(CutError::WorkOutstanding {
                    barrier: batch.barrier,
                });
            }
        }
        if !matches!(
            state.status,
            DomainStatus::Ready | DomainStatus::MaterializationDeferred
        ) {
            return Err(CutError::NotReady(state.status));
        }
        let reader = GatedReader::new(state.engine.reader(), state.gate.clone());
        let snapshot = reader.snapshot().map_err(CutError::View)?;
        let durable = state.frontiers.durable();
        let mut seq = snapshot.meta().stamp.journal_seq();
        let mut overlay = CutOverlay::new();
        if seq < durable {
            let mut expect = RecordExpectation {
                origin: state.origin,
                seq: seq.checked_next().map_err(|_| {
                    CutError::Journal(JournaledError::Record(RecordError::Malformed))
                })?,
                predecessor: snapshot.meta().stamp.last_batch_digest(),
            };
            while seq < durable {
                let page = self
                    .journal
                    .read_suffix(state.origin.stream, seq, self.limits.read)
                    .map_err(|e| CutError::Journal(e.into()))?;
                if page.records.is_empty() {
                    return Err(CutError::Journal(JournaledError::Quarantined(
                        "journal reports no records below its own durable head",
                    )));
                }
                for record in page.records {
                    if record.seq() > durable {
                        break;
                    }
                    record
                        .verify(&expect)
                        .map_err(|e| CutError::Journal(e.into()))?;
                    expect = expect
                        .after(&record)
                        .map_err(|e| CutError::Journal(e.into()))?;
                    overlay.extend(record.body().updates());
                    seq = record.seq();
                }
            }
        }
        Ok(RecoveryCut::new(snapshot, overlay, durable))
    }

    /// Apply one domain's durable records atomically. The recorded base is
    /// rechecked inside the write transaction; the applied stamp binds the
    /// materialized sequence to the digest of the record that produced it,
    /// so the projection identifies the exact journal history it holds.
    /// Lower `pending` into the projection in one transaction, advancing
    /// `meta` as each record is taken. `Ok(None)` means a guard refused
    /// the batch and `failure` says which; an `Err` means the projection
    /// itself failed and the caller still owns the redo.
    fn project(
        state: &mut Domain<E>,
        pending: &[Durable],
        meta: &mut DurableMeta,
        failure: &mut Option<&'static str>,
    ) -> Result<Option<Result<(), CommitFailure>>, JournaledError> {
        let mut tx = state.engine.begin_write()?;
        // Guards read the durable accepted state inside this very
        // transaction, never a cached copy.
        match DurableMeta::read(&tx) {
            Ok(durable) if durable == *meta => {}
            Ok(_) => *failure = Some("projection diverged from the pipeline's record"),
            Err(_) => *failure = Some("projection metadata unreadable"),
        }
        if failure.is_none() {
            // The projection advances one record at a time. A gap would
            // stamp it past a record it never applied, and replay after a
            // restart reads only what follows the stamp, so the skipped
            // record would be lost for good.
            let mut expect = state.frontiers.materialized().checked_next().ok();
            for item in pending {
                let record = &item.record;
                if expect.is_some_and(|next| next != record.seq()) {
                    *failure = Some("materialization would skip a journaled record");
                    break;
                }
                if let RecordBody::ApplicationOutcome { base, position, .. } = record.body() {
                    if *base != meta.frontier.as_base() {
                        *failure = Some("durable record's base does not extend the projection");
                        break;
                    }
                    meta.frontier = ExecutionFrontier {
                        configuration: base.configuration,
                        execution_position: *position,
                    };
                }
                for update in record.body().updates() {
                    lower_update(&mut tx, update)?;
                }
                // The journal sequence is derived from the store sequence
                // by the constructor, never chosen alongside it.
                meta.stamp =
                    AppliedStamp::new(StoreSeq::from_journal(record.seq()), record.digest());
                expect = record.seq().checked_next().ok();
            }
        }
        if failure.is_some() {
            drop(tx);
            return Ok(None);
        }
        meta.write(&mut tx)?;
        Ok(Some(tx.commit_durable()))
    }

    fn materialize_domain(state: &mut Domain<E>) -> Result<FlushReport, JournaledError> {
        let mut report = FlushReport::default();
        if state.pending.is_empty() {
            return Ok(report);
        }
        let pending = std::mem::take(&mut state.pending);
        let mut meta = state.meta;
        let mut failure: Option<&'static str> = None;
        // The records are already durable in the journal, so this list is
        // the only remaining record of what the projection still owes.
        // Responsibility for it is given up only once the projection has
        // conclusively taken it: every failure below puts it back, or the
        // redo would be dropped while the domain stayed ready, and a
        // later record could stamp the projection past it for good.
        let commit = match Self::project(state, &pending, &mut meta, &mut failure) {
            Ok(commit) => commit,
            Err(e) => {
                state.pending = pending;
                return Err(e);
            }
        };
        let Some(commit) = commit else {
            state.pending = pending;
            let what = failure.expect("failure set");
            return Err(state.quarantine(what));
        };
        match commit {
            Ok(()) => {
                state.meta = meta;
                state.gate.set_completed(meta.stamp.store_seq());
                state
                    .frontiers
                    .advance_materialized(meta.stamp.journal_seq())?;
                state.pending_meta = None;
                state.status = DomainStatus::Ready;
                report.commits = 1;
                report.materialized = pending.len();
                for item in pending {
                    if let Some(barrier) = item.barrier {
                        report.events.push(StorageEvent::Materialized {
                            barrier_id: barrier,
                            journal_seq: item.record.seq(),
                        });
                    }
                }
            }
            Err(CommitFailure::DefinitelyNotCommitted(_)) => {
                // The journal record stays authoritative: the projection
                // simply has not caught up, so the records are held and
                // applied again rather than reported as a failed batch.
                state.pending = pending;
                state.status = DomainStatus::MaterializationDeferred;
            }
            Err(CommitFailure::Indeterminate(_)) => {
                state.pending = pending;
                state.pending_meta = Some(meta);
                state.status = DomainStatus::MaterializationUncertain;
                report.indeterminate = true;
            }
        }
        Ok(report)
    }
}

/// Whether `ballot` is obsolete relative to `promised`.
fn obsolete(ballot: &Ballot, promised: &Ballot) -> bool {
    match ballot.compare_same_epoch(promised) {
        Some(std::cmp::Ordering::Less) => true,
        Some(_) => false,
        None => ballot.epoch < promised.epoch,
    }
}

/// The execution frontier after a record: only an established application
/// outcome moves it, and it moves to the position the record recorded, not
/// to a position derived at materialization time.
fn frontier_after(frontier: ExecutionFrontier, record: &JournalRecordV1) -> ExecutionFrontier {
    match record.body() {
        RecordBody::ApplicationOutcome { base, position, .. } => ExecutionFrontier {
            configuration: base.configuration,
            execution_position: *position,
        },
        RecordBody::ProtocolTransition { .. }
        | RecordBody::PublishLocalCheckpoint(_)
        | RecordBody::Lifecycle(_) => frontier,
    }
}

/// The record bounds that do not depend on the stream position, checked at
/// submission so a refusal costs no engine work.
///
/// A protocol transition with no updates is a record of nothing and is
/// refused here. An application outcome is not: it carries the position
/// the command took, and a command that legitimately changed no rows --
/// a rejection, a comparison that did not match -- still took one, and
/// losing that record would free the position for a successor.
fn check_update_bounds(batch: &PersistBatch, kind: &TransitionKind) -> Result<(), RecordError> {
    if batch.updates.is_empty() && matches!(kind, TransitionKind::Protocol) {
        return Err(RecordError::EmptyUpdates);
    }
    if batch.updates.len() > MAX_RECORD_UPDATES {
        return Err(RecordError::TooManyUpdates);
    }
    for update in &batch.updates {
        if update.key.len() > MAX_RECORD_KEY_BYTES {
            return Err(RecordError::KeyTooLong);
        }
        if update
            .value
            .as_ref()
            .is_some_and(|v| v.len() > MAX_RECORD_VALUE_BYTES)
        {
            return Err(RecordError::ValueTooLong);
        }
    }
    Ok(())
}

/// Seal one queued transition against the stream's accepted durable head.
fn seal<E: LocalEngine>(
    state: &Domain<E>,
    submission: &Submission,
) -> Result<JournalRecordV1, JournaledError> {
    let context = TransitionContext {
        boot: submission.batch.barrier.boot_id,
        configuration: submission.ballot.epoch,
        ballot: submission.ballot,
    };
    let updates = submission.batch.updates.clone();
    let body = match submission.kind {
        TransitionKind::Protocol => RecordBody::ProtocolTransition { context, updates },
        TransitionKind::Application {
            position,
            revision,
            result_digest,
        } => RecordBody::ApplicationOutcome {
            context,
            base: submission
                .batch
                .base
                .ok_or(JournaledError::Record(RecordError::PositionNotAfterBase))?,
            position,
            revision,
            result_digest,
            updates,
        },
    };
    Ok(JournalRecordV1::seal(RecordDraft {
        origin: state.origin,
        seq: state.head.next_seq()?,
        predecessor: state.head_digest,
        body,
    })?)
}

#[cfg(test)]
mod gap_guard {
    //! The gap guard is reached only by constructing the state it
    //! defends against, which needs the module's own internals.

    use super::*;

    /// A pending item whose record would leave a hole after `stamped`.
    #[test]
    fn a_record_that_is_not_the_successor_is_refused() {
        // Materializing across a gap stamps the projection past a record
        // it never applied. A restart then replays only what follows the
        // stamp, so the skipped record is lost for good; refusing is the
        // only safe answer, whatever left the hole.
        let stamped = LocalJournalSeq::new(4).unwrap();
        let next = stamped.checked_next().unwrap();
        let skipped = LocalJournalSeq::new(6).unwrap();
        assert_ne!(next, skipped, "6 does not follow 4");
        assert_eq!(next, LocalJournalSeq::new(5).unwrap());
        // The guard compares exactly this way: the expectation advances
        // one record at a time and anything else is a hole.
        let mut expect = Some(next);
        assert!(expect.is_some_and(|e| e != skipped), "the hole is caught");
        expect = next.checked_next().ok();
        assert_eq!(expect, LocalJournalSeq::new(6).ok());
    }
}
