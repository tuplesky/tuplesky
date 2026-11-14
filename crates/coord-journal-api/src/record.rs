//! `JournalRecordV1`: the immutable complete record (design Section 17.3.2).
//!
//! A record is bound by origin/incarnation and stream, local sequence,
//! format, the digest of its predecessor in the stream and its own digest
//! over all of that plus the complete immutable logical updates. Typed
//! bodies cover protocol transitions, established application outcomes,
//! local checkpoint publication and lifecycle metadata. The logical updates
//! are the same `StoreUpdate` rows (ordered keys, `StoreEnvelopeV1` values)
//! the state adapter materializes, so codecs stay shared.
//!
//! The durable encoding is postcard under [`JOURNAL_RECORD_FORMAT_V1`],
//! versioned independently of the native transport (`wire_v1`), which this
//! crate does not reference. A record can only be built by
//! [`JournalRecordV1::seal`] or [`JournalRecordV1::decode`]; both validate
//! bounds and the latter re-derives the digest, so no field can change after
//! sealing.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use coord_core::effect::{ApplyBase, BootId, StoreUpdate};
use coord_store_api::envelope::MAX_ENVELOPE_PAYLOAD;
use coord_types::identity::{Digest32, HashDomain};
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, KvRevision,
    LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};
use serde::{Deserialize, Serialize};

use crate::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1, OLDEST_POINTER_FORMAT};
use crate::stream::StorageStreamId;

/// The durable record format this build writes: 2 since the batching-rule
/// doc's 10.7, step 1b, whose records may hold a group's application
/// outcomes. The name is the original constant's; what it names is the
/// registry's `current`. Bumped only by a reviewed schema change;
/// unrelated to any wire version.
pub const JOURNAL_RECORD_FORMAT_V1: u16 =
    coord_types::formats::Format::JournalRecord.current() as u16;
/// Largest encoded record admitted (the separately bounded large-record
/// path; ordinary groups are far smaller).
pub const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
/// Most logical updates in one record.
pub const MAX_RECORD_UPDATES: usize = 4096;
/// Longest ordered key in an update.
pub const MAX_RECORD_KEY_BYTES: usize = 16 * 1024;
/// Longest encoded value in an update (an envelope at its payload limit).
pub const MAX_RECORD_VALUE_BYTES: usize = MAX_ENVELOPE_PAYLOAD + 16;
/// Predecessor digest of the first record of a stream.
pub const GENESIS_PREDECESSOR: Digest32 = Digest32([0; 32]);
/// The first record format whose records may hold a group's application
/// outcomes ([`RecordBody::ApplicationGroup`]). This build writes it (the
/// batching-rule doc's 10.7, step 1b); the release before it (step 1a)
/// reads it, so a voter can go back there.
pub const JOURNAL_RECORD_FORMAT_GROUPS: u16 = 2;
const _: () = assert!(
    coord_types::formats::Format::JournalRecord.supports(JOURNAL_RECORD_FORMAT_GROUPS as u32)
);

/// Whether this build reads records of `format`.
fn readable(format: u16) -> bool {
    coord_types::formats::Format::JournalRecord.supports(format as u32)
}

/// Where a record comes from: the local identity and stream that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordOrigin {
    /// Cluster/restore identity.
    pub cluster: ClusterId,
    /// Domain.
    pub domain: DomainId,
    /// Replica.
    pub replica: ReplicaId,
    /// Replica incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Stream allocated for `(cluster, domain, incarnation)`.
    pub stream: StorageStreamId,
}

impl RecordOrigin {
    /// Fixed 64-byte canonical encoding used inside the digest.
    pub fn canonical_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out[0..16].copy_from_slice(self.cluster.as_bytes());
        out[16..32].copy_from_slice(self.domain.as_bytes());
        out[32..48].copy_from_slice(self.replica.as_bytes());
        out[48..56].copy_from_slice(&self.incarnation.to_be_bytes());
        out[56..64].copy_from_slice(&self.stream.get().to_be_bytes());
        out
    }
}

/// The effect context a transition was produced under (design Sections
/// 4.8 and 18.1). Replay verifies it; it never re-evaluates clocks or
/// issuers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionContext {
    /// Boot that produced the transition.
    pub boot: BootId,
    /// Configuration epoch.
    pub configuration: ConfigurationEpoch,
    /// Ballot.
    pub ballot: Ballot,
}

/// Lifecycle metadata records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LifecycleRecordV1 {
    /// The first record of every stream: establishes origin and format
    /// before any service.
    Genesis {
        /// Record format the stream is written in.
        format: u16,
    },
    /// A boot of the incarnation started writing this stream.
    Boot {
        /// Boot identity.
        boot: BootId,
    },
}

/// Typed record bodies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordBody {
    /// A protocol transition (promise, vote, adoption, Sync binding).
    ProtocolTransition {
        /// Effect context.
        context: TransitionContext,
        /// Complete immutable updates.
        updates: Vec<StoreUpdate>,
    },
    /// An established command's complete application redo: state delta,
    /// execution/revision, exact result digest, dedup, lease/policy rows
    /// and frontiers, all as updates, plus the guards replay rechecks.
    ApplicationOutcome {
        /// Effect context.
        context: TransitionContext,
        /// Base the plan was computed against; materialization rechecks it.
        base: ApplyBase,
        /// Execution position assigned.
        position: ExecutionPosition,
        /// KV revision produced, if the command mutated KV.
        revision: Option<KvRevision>,
        /// Digest of the exact result.
        result_digest: Digest32,
        /// Complete immutable updates.
        updates: Vec<StoreUpdate>,
    },
    /// Durable publication of a local recovery checkpoint.
    PublishLocalCheckpoint(CheckpointPointerV1),
    /// Lifecycle metadata.
    Lifecycle(LifecycleRecordV1),
    /// The application outcomes of a run of established commands, in
    /// execution order: the same facts as one
    /// [`RecordBody::ApplicationOutcome`] each, in one record (the
    /// batching-rule doc's 10.7, step 1). Only in records of
    /// [`JOURNAL_RECORD_FORMAT_GROUPS`] and later.
    ///
    /// The first outcome extends `base`, and each later one the position
    /// before it. The updates are every outcome's, in order, so the
    /// record's updates read as the per-command records' would one after
    /// another.
    ApplicationGroup {
        /// Effect context, the same for every outcome.
        context: TransitionContext,
        /// Base the first outcome's plan was computed against.
        base: ApplyBase,
        /// Each command's outcome, in execution order.
        outcomes: Vec<GroupOutcomeV1>,
        /// Every outcome's complete immutable updates, in order.
        updates: Vec<StoreUpdate>,
    },
}

/// One command's application outcome in a
/// [`RecordBody::ApplicationGroup`]: what a
/// [`RecordBody::ApplicationOutcome`] carries but its context, its base
/// and its updates, which the group holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupOutcomeV1 {
    /// Execution position assigned: the one after the outcome before it,
    /// or after the group's base for the first.
    pub position: ExecutionPosition,
    /// KV revision produced, if the command mutated KV.
    pub revision: Option<KvRevision>,
    /// Digest of the exact result.
    pub result_digest: Digest32,
    /// How many of the group's updates are this one's, taken in order
    /// after those of the outcomes before it. A command that changed no
    /// rows has none, and still took its position.
    pub updates: u32,
}

impl RecordBody {
    /// The logical updates materialization applies (empty for pointers and
    /// lifecycle records).
    pub fn updates(&self) -> &[StoreUpdate] {
        match self {
            RecordBody::ProtocolTransition { updates, .. }
            | RecordBody::ApplicationOutcome { updates, .. }
            | RecordBody::ApplicationGroup { updates, .. } => updates,
            RecordBody::PublishLocalCheckpoint(_) | RecordBody::Lifecycle(_) => &[],
        }
    }

    /// How the record moves the execution frontier: the base its first
    /// application outcome extends, and the position its last takes.
    /// `None` for a record that holds no application outcome.
    pub fn execution(&self) -> Option<(ApplyBase, ExecutionPosition)> {
        match self {
            RecordBody::ApplicationOutcome { base, position, .. } => Some((*base, *position)),
            RecordBody::ApplicationGroup { base, outcomes, .. } => {
                outcomes.last().map(|last| (*base, last.position))
            }
            RecordBody::ProtocolTransition { .. }
            | RecordBody::PublishLocalCheckpoint(_)
            | RecordBody::Lifecycle(_) => None,
        }
    }

    /// How many established commands' application outcomes the record
    /// holds.
    pub fn application_outcomes(&self) -> usize {
        match self {
            RecordBody::ApplicationOutcome { .. } => 1,
            RecordBody::ApplicationGroup { outcomes, .. } => outcomes.len(),
            RecordBody::ProtocolTransition { .. }
            | RecordBody::PublishLocalCheckpoint(_)
            | RecordBody::Lifecycle(_) => 0,
        }
    }
}

/// Why a record was rejected at sealing, decoding or verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordError {
    /// Sequence zero is never a record.
    ZeroSequence,
    /// A transition or outcome without updates.
    EmptyUpdates,
    /// More than [`MAX_RECORD_UPDATES`].
    TooManyUpdates,
    /// A key longer than [`MAX_RECORD_KEY_BYTES`].
    KeyTooLong,
    /// A value longer than [`MAX_RECORD_VALUE_BYTES`].
    ValueTooLong,
    /// Encoded record longer than [`MAX_RECORD_BYTES`].
    TooLarge,
    /// Execution position zero is never established.
    ZeroPosition,
    /// The ballot's epoch, the context epoch and the base epoch disagree.
    EpochMismatch,
    /// The outcome's position is not the one right after its base.
    PositionNotAfterBase,
    /// The pointer names another origin.
    PointerOriginMismatch,
    /// The pointer's represented sequence is not below the publication
    /// record (a publication is newer than `C`).
    PointerNotBelowRecord,
    /// Unsupported record or checkpoint format.
    UnsupportedFormat {
        /// Format found.
        found: u16,
    },
    /// A genesis record not at sequence one with the genesis predecessor.
    GenesisNotFirst,
    /// The first record of a stream is not genesis.
    FirstNotGenesis,
    /// Origin cluster differs from the expected one.
    ClusterMismatch,
    /// Origin domain differs.
    DomainMismatch,
    /// Origin replica differs.
    ReplicaMismatch,
    /// Origin incarnation differs.
    IncarnationMismatch,
    /// Origin stream differs.
    StreamMismatch,
    /// Sequence differs from the expected next index.
    IndexMismatch {
        /// Expected.
        expected: LocalJournalSeq,
        /// Found.
        found: LocalJournalSeq,
    },
    /// Predecessor digest does not chain onto the previous record.
    PredecessorMismatch,
    /// Stored digest differs from the re-derived one.
    DigestMismatch,
    /// Encoding ended early.
    Truncated,
    /// Bytes remained after a complete record.
    TrailingBytes,
    /// Encoding is not a valid record.
    Malformed,
    /// A group of application outcomes in a record of a format before
    /// [`JOURNAL_RECORD_FORMAT_GROUPS`].
    GroupBeforeItsFormat,
    /// A group of no application outcomes.
    EmptyGroup,
    /// A group's outcomes don't account for exactly its updates.
    GroupUpdatesMismatch,
    /// The bytes decode to a valid record but are not its encoding: a
    /// varint written longer than it needs, which decodes to the same
    /// value. No encoder of this format writes one.
    NonCanonical,
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordError::IndexMismatch { expected, found } => {
                write!(f, "index mismatch: expected {expected}, found {found}")
            }
            RecordError::UnsupportedFormat { found } => write!(f, "unsupported format {found}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl core::error::Error for RecordError {}

/// What a caller expects the next record of a stream to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecordExpectation {
    /// Origin.
    pub origin: RecordOrigin,
    /// Sequence.
    pub seq: LocalJournalSeq,
    /// Digest of the previous record.
    pub predecessor: Digest32,
}

impl RecordExpectation {
    /// The genesis expectation of a stream.
    pub const fn genesis(origin: RecordOrigin) -> Self {
        RecordExpectation {
            origin,
            seq: match LocalJournalSeq::new(1) {
                Ok(seq) => seq,
                Err(_) => panic!("one is in range"),
            },
            predecessor: GENESIS_PREDECESSOR,
        }
    }

    /// The expectation after `record` (which satisfied this one).
    pub fn after(&self, record: &JournalRecordV1) -> Result<Self, RecordError> {
        Ok(RecordExpectation {
            origin: self.origin,
            seq: record
                .seq
                .checked_next()
                .map_err(|_| RecordError::Malformed)?,
            predecessor: record.digest,
        })
    }
}

/// The mutable input to [`JournalRecordV1::seal`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDraft {
    /// Origin.
    pub origin: RecordOrigin,
    /// Sequence the stream head reserved.
    pub seq: LocalJournalSeq,
    /// Digest of the previous record of the stream.
    pub predecessor: Digest32,
    /// Body.
    pub body: RecordBody,
}

#[derive(Serialize, Deserialize)]
struct RecordWireV1 {
    format: u16,
    origin: RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: RecordBody,
    digest: Digest32,
}

/// [`RecordWireV1`] borrowed from a record: the same fields in the same
/// order, so the same bytes, encoded without copying the body (task-d60).
#[derive(Serialize)]
#[serde(rename = "RecordWireV1")]
struct RecordWireRefV1<'a> {
    format: u16,
    origin: &'a RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: &'a RecordBody,
    digest: Digest32,
}

/// A postcard flavor that appends to a caller's buffer.
struct Append<'a>(&'a mut Vec<u8>);

impl postcard::ser_flavors::Flavor for Append<'_> {
    type Output = ();

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.push(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<()> {
        Ok(())
    }
}

/// An immutable complete record.
///
/// The body is shared: a sealed record never changes, and the storage
/// path holds the same record in the group it appends and in what it
/// materializes, so a clone is a reference count rather than a copy of
/// every update (task-d60).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecordV1 {
    format: u16,
    origin: RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: Arc<RecordBody>,
    digest: Digest32,
    /// The encoding's length, counted once where the record is made: at
    /// [`Self::seal`], and at [`Self::decode`] as the bytes decoded. A
    /// function of the fields above, so it never disagrees with them.
    encoded_len: usize,
}

fn digest_of(
    format: u16,
    origin: &RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: &Digest32,
    body_bytes: &[u8],
) -> Digest32 {
    HashDomain::JournalBatch.digest(&[
        &format.to_be_bytes(),
        &origin.canonical_bytes(),
        &seq.to_be_bytes(),
        &predecessor.0,
        body_bytes,
    ])
}

/// The bounds every record's updates are held to.
///
/// Emptiness is not one of them here, because whether an empty set is
/// meaningful depends on what else the record carries; see
/// [`check_updates`].
fn check_update_sizes(updates: &[StoreUpdate]) -> Result<(), RecordError> {
    if updates.len() > MAX_RECORD_UPDATES {
        return Err(RecordError::TooManyUpdates);
    }
    for u in updates {
        if u.key.len() > MAX_RECORD_KEY_BYTES {
            return Err(RecordError::KeyTooLong);
        }
        if u.value
            .as_ref()
            .is_some_and(|v| v.len() > MAX_RECORD_VALUE_BYTES)
        {
            return Err(RecordError::ValueTooLong);
        }
    }
    Ok(())
}

/// The bounds a record whose updates are its whole content is held to.
///
/// A protocol transition says what it says by writing rows: a ballot, a
/// promise, a vote, a bound selection. One with no rows is a record of
/// nothing, and accepting it would put an entry in the journal that
/// replay must carry and cannot act on.
///
/// An application outcome is not like that, which is why it does not go
/// through here. It carries an execution position, a revision and the
/// digest of the exact result, and a command that legitimately changed
/// no rows -- a rejection, a comparison that did not match -- still took
/// its position. Losing that record would leave the position free for a
/// successor and let two replicas disagree about which command holds it.
fn check_updates(updates: &[StoreUpdate]) -> Result<(), RecordError> {
    if updates.is_empty() {
        return Err(RecordError::EmptyUpdates);
    }
    check_update_sizes(updates)
}

fn check_body(
    format: u16,
    origin: &RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: &Digest32,
    body: &RecordBody,
) -> Result<(), RecordError> {
    if seq == LocalJournalSeq::ZERO {
        return Err(RecordError::ZeroSequence);
    }
    let first = seq.get() == 1;
    let genesis = matches!(
        body,
        RecordBody::Lifecycle(LifecycleRecordV1::Genesis { .. })
    );
    if genesis && (!first || *predecessor != GENESIS_PREDECESSOR) {
        return Err(RecordError::GenesisNotFirst);
    }
    if first && !genesis {
        return Err(RecordError::FirstNotGenesis);
    }
    match body {
        RecordBody::ProtocolTransition { context, updates } => {
            if context.ballot.epoch != context.configuration {
                return Err(RecordError::EpochMismatch);
            }
            check_updates(updates)
        }
        RecordBody::ApplicationOutcome {
            context,
            base,
            position,
            updates,
            ..
        } => {
            if context.ballot.epoch != context.configuration
                || base.configuration != context.configuration
            {
                return Err(RecordError::EpochMismatch);
            }
            if *position == ExecutionPosition::ZERO {
                return Err(RecordError::ZeroPosition);
            }
            // Established history is contiguous: the planner and the strict
            // materializer assign every command the position right after
            // its base, so a record encoding a gap could never be reconciled
            // with the frontier replay derives from the base.
            let next = base
                .execution_position
                .checked_next()
                .map_err(|_| RecordError::PositionNotAfterBase)?;
            if *position != next {
                return Err(RecordError::PositionNotAfterBase);
            }
            check_update_sizes(updates)
        }
        RecordBody::ApplicationGroup {
            context,
            base,
            outcomes,
            updates,
        } => {
            if format < JOURNAL_RECORD_FORMAT_GROUPS {
                return Err(RecordError::GroupBeforeItsFormat);
            }
            if context.ballot.epoch != context.configuration
                || base.configuration != context.configuration
            {
                return Err(RecordError::EpochMismatch);
            }
            // A record of nothing: no position taken, no row written.
            if outcomes.is_empty() {
                return Err(RecordError::EmptyGroup);
            }
            // Contiguous, as each per-command outcome is with its base:
            // the first takes the position after the group's base, and
            // each later one the position after the one before it.
            let mut before = base.execution_position;
            let mut counted = 0usize;
            for outcome in outcomes {
                let next = before
                    .checked_next()
                    .map_err(|_| RecordError::PositionNotAfterBase)?;
                if outcome.position != next {
                    return Err(RecordError::PositionNotAfterBase);
                }
                before = next;
                counted = counted
                    .checked_add(outcome.updates as usize)
                    .ok_or(RecordError::GroupUpdatesMismatch)?;
            }
            if counted != updates.len() {
                return Err(RecordError::GroupUpdatesMismatch);
            }
            check_update_sizes(updates)
        }
        RecordBody::PublishLocalCheckpoint(pointer) => {
            // Any format a pointer has named decodes: the record's layout
            // didn't change with the image's, and a journal that retains a
            // pointer published before a raise must still open. The image
            // is refused where it is loaded (#247, Codex).
            if !(OLDEST_POINTER_FORMAT..=LOCAL_CHECKPOINT_FORMAT_V1).contains(&pointer.format) {
                return Err(RecordError::UnsupportedFormat {
                    found: pointer.format,
                });
            }
            if pointer.origin != *origin {
                return Err(RecordError::PointerOriginMismatch);
            }
            if pointer.represented >= seq {
                return Err(RecordError::PointerNotBelowRecord);
            }
            Ok(())
        }
        RecordBody::Lifecycle(LifecycleRecordV1::Genesis { format }) => {
            if !readable(*format) {
                return Err(RecordError::UnsupportedFormat { found: *format });
            }
            Ok(())
        }
        RecordBody::Lifecycle(LifecycleRecordV1::Boot { .. }) => Ok(()),
    }
}

fn encode_body(body: &RecordBody) -> Result<Vec<u8>, RecordError> {
    postcard::to_allocvec(body).map_err(|_| RecordError::TooLarge)
}

impl JournalRecordV1 {
    /// Seal a draft at the format this build writes: validate bounds and
    /// guards, derive the digest.
    pub fn seal(draft: RecordDraft) -> Result<Self, RecordError> {
        Self::seal_as(JOURNAL_RECORD_FORMAT_V1, draft)
    }

    /// Seal a draft at `format`, one this build reads. A stream's writer
    /// seals at the format it writes ([`Self::seal`]). This is for a
    /// reader's fixtures: the records of a format its successor writes,
    /// such as [`JOURNAL_RECORD_FORMAT_GROUPS`].
    pub fn seal_as(format: u16, draft: RecordDraft) -> Result<Self, RecordError> {
        if !readable(format) {
            return Err(RecordError::UnsupportedFormat { found: format });
        }
        check_body(
            format,
            &draft.origin,
            draft.seq,
            &draft.predecessor,
            &draft.body,
        )?;
        let body_bytes = encode_body(&draft.body)?;
        let digest = digest_of(
            format,
            &draft.origin,
            draft.seq,
            &draft.predecessor,
            &body_bytes,
        );
        let mut record = JournalRecordV1 {
            format,
            origin: draft.origin,
            seq: draft.seq,
            predecessor: draft.predecessor,
            body: Arc::new(draft.body),
            digest,
            encoded_len: 0,
        };
        record.encoded_len = postcard::experimental::serialized_size(&record.wire())
            .map_err(|_| RecordError::TooLarge)?;
        if record.encoded_len > MAX_RECORD_BYTES {
            return Err(RecordError::TooLarge);
        }
        Ok(record)
    }

    /// Format version.
    pub const fn format(&self) -> u16 {
        self.format
    }
    /// Origin.
    pub const fn origin(&self) -> &RecordOrigin {
        &self.origin
    }
    /// Sequence within the stream.
    pub const fn seq(&self) -> LocalJournalSeq {
        self.seq
    }
    /// Predecessor digest.
    pub const fn predecessor(&self) -> Digest32 {
        self.predecessor
    }
    /// Body.
    pub fn body(&self) -> &RecordBody {
        &self.body
    }
    /// Digest over format, origin, sequence, predecessor and body.
    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    /// Verify the record against what the stream expects: its format,
    /// origin fields, index and predecessor chain.
    ///
    /// The body's bounds and guards and the digest over the whole record
    /// are not checked again here (#185, lead 17:43): they hold of every
    /// value of this type.
    /// - **A record is made two ways:** [`JournalRecordV1::seal`] checks
    ///   the body and derives the digest from its encoding, and
    ///   [`JournalRecordV1::decode`] checks the body and re-derives the
    ///   digest from the bytes it read, refusing a mismatch.
    /// - **Nothing changes it after that:** the fields are private and the
    ///   body is shared immutably.
    /// - **What that check protected against,** then, is only a record
    ///   corrupted in this process's memory between its making and its
    ///   append. Bytes corrupted after the append, on the way to the
    ///   disk or on it, are what [`JournalRecordV1::decode`] refuses when
    ///   they are read back, and a record received from elsewhere is
    ///   decoded before it is a record.
    ///
    /// The append verifies every record a moment after this process
    /// sealed it, and the body's encoding and digest again were 40% of
    /// the appender's thread in testing's profile on the 9V45 runner
    /// (#98). Debug builds still derive the digest again here, and stop
    /// on a difference.
    pub fn verify(&self, expect: &RecordExpectation) -> Result<(), RecordError> {
        if !readable(self.format) {
            return Err(RecordError::UnsupportedFormat { found: self.format });
        }
        if self.origin.cluster != expect.origin.cluster {
            return Err(RecordError::ClusterMismatch);
        }
        if self.origin.domain != expect.origin.domain {
            return Err(RecordError::DomainMismatch);
        }
        if self.origin.replica != expect.origin.replica {
            return Err(RecordError::ReplicaMismatch);
        }
        if self.origin.incarnation != expect.origin.incarnation {
            return Err(RecordError::IncarnationMismatch);
        }
        if self.origin.stream != expect.origin.stream {
            return Err(RecordError::StreamMismatch);
        }
        if self.seq != expect.seq {
            return Err(RecordError::IndexMismatch {
                expected: expect.seq,
                found: self.seq,
            });
        }
        if self.predecessor != expect.predecessor {
            return Err(RecordError::PredecessorMismatch);
        }
        #[cfg(debug_assertions)]
        assert_eq!(
            self.derived(),
            Ok(self.digest),
            "a record's digest no longer matches its body"
        );
        Ok(())
    }

    /// The body checked and the digest derived again from the record's
    /// fields: what [`JournalRecordV1::seal`] and
    /// [`JournalRecordV1::decode`] establish, for a debug build to check.
    #[cfg(debug_assertions)]
    fn derived(&self) -> Result<Digest32, RecordError> {
        check_body(
            self.format,
            &self.origin,
            self.seq,
            &self.predecessor,
            &self.body,
        )?;
        let body_bytes = encode_body(&self.body)?;
        Ok(digest_of(
            self.format,
            &self.origin,
            self.seq,
            &self.predecessor,
            &body_bytes,
        ))
    }

    fn wire(&self) -> RecordWireRefV1<'_> {
        RecordWireRefV1 {
            format: self.format,
            origin: &self.origin,
            seq: self.seq,
            predecessor: self.predecessor,
            body: &self.body,
            digest: self.digest,
        }
    }

    /// Encoded length in bytes, counted once when the record was sealed
    /// or decoded.
    pub fn encoded_len(&self) -> Result<usize, RecordError> {
        Ok(self.encoded_len)
    }

    /// Durable encoding, into a buffer sized once.
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut buf = Vec::with_capacity(self.encoded_len);
        self.encode_into(&mut buf)?;
        Ok(buf)
    }

    /// Append the durable encoding to `buf`, the bytes [`Self::encode`]
    /// returns. The room is reserved once, from the length counted when
    /// the record was made, rather than grown a field at a time. On an
    /// error `buf` is left as it was.
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), RecordError> {
        let start = buf.len();
        buf.reserve(self.encoded_len);
        postcard::serialize_with_flavor(&self.wire(), Append(buf)).map_err(|_| {
            buf.truncate(start);
            RecordError::TooLarge
        })?;
        debug_assert_eq!(buf.len() - start, self.encoded_len);
        Ok(())
    }

    /// Decode exactly: bounds are checked before nested allocation is
    /// trusted, the digest is re-derived, trailing bytes are rejected, and
    /// a decode error is never end of data.
    ///
    /// Only the record's own encoding is taken. A varint written longer
    /// than it needs decodes to the same value, and the digest is derived
    /// from the decoded fields and the body encoded again, so such bytes
    /// would pass every other check while being longer than the record's
    /// encoding (#284, Codex). They are refused, so a decoded record
    /// encodes to exactly the bytes it was read from, and its stored
    /// length is theirs. Every field encodes to at least its canonical
    /// length, so equal lengths mean equal bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(RecordError::TooLarge);
        }
        let (wire, rest): (RecordWireV1, &[u8]) =
            postcard::take_from_bytes(bytes).map_err(|e| match e {
                postcard::Error::DeserializeUnexpectedEnd => RecordError::Truncated,
                _ => RecordError::Malformed,
            })?;
        if !rest.is_empty() {
            return Err(RecordError::TrailingBytes);
        }
        if !readable(wire.format) {
            return Err(RecordError::UnsupportedFormat { found: wire.format });
        }
        check_body(
            wire.format,
            &wire.origin,
            wire.seq,
            &wire.predecessor,
            &wire.body,
        )?;
        let body_bytes = encode_body(&wire.body)?;
        let derived = digest_of(
            wire.format,
            &wire.origin,
            wire.seq,
            &wire.predecessor,
            &body_bytes,
        );
        if derived != wire.digest {
            return Err(RecordError::DigestMismatch);
        }
        let record = JournalRecordV1 {
            format: wire.format,
            origin: wire.origin,
            seq: wire.seq,
            predecessor: wire.predecessor,
            body: Arc::new(wire.body),
            digest: wire.digest,
            encoded_len: bytes.len(),
        };
        let canonical = postcard::experimental::serialized_size(&record.wire())
            .map_err(|_| RecordError::TooLarge)?;
        if canonical != bytes.len() {
            return Err(RecordError::NonCanonical);
        }
        Ok(record)
    }
}
