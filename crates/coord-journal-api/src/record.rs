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

use crate::frontier::{CheckpointPointerV1, LOCAL_CHECKPOINT_FORMAT_V1};
use crate::stream::StorageStreamId;

/// Durable record format version. Bumped only by a reviewed schema change;
/// unrelated to any wire version.
pub const JOURNAL_RECORD_FORMAT_V1: u16 = 1;
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
}

impl RecordBody {
    /// The logical updates materialization applies (empty for pointers and
    /// lifecycle records).
    pub fn updates(&self) -> &[StoreUpdate] {
        match self {
            RecordBody::ProtocolTransition { updates, .. }
            | RecordBody::ApplicationOutcome { updates, .. } => updates,
            RecordBody::PublishLocalCheckpoint(_) | RecordBody::Lifecycle(_) => &[],
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
    /// The outcome does not extend its base.
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

/// An immutable complete record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecordV1 {
    format: u16,
    origin: RecordOrigin,
    seq: LocalJournalSeq,
    predecessor: Digest32,
    body: RecordBody,
    digest: Digest32,
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

fn check_updates(updates: &[StoreUpdate]) -> Result<(), RecordError> {
    if updates.is_empty() {
        return Err(RecordError::EmptyUpdates);
    }
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

fn check_body(
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
            if *position <= base.execution_position {
                return Err(RecordError::PositionNotAfterBase);
            }
            check_updates(updates)
        }
        RecordBody::PublishLocalCheckpoint(pointer) => {
            if pointer.format != LOCAL_CHECKPOINT_FORMAT_V1 {
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
            if *format != JOURNAL_RECORD_FORMAT_V1 {
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
    /// Seal a draft: validate bounds and guards, derive the digest.
    pub fn seal(draft: RecordDraft) -> Result<Self, RecordError> {
        check_body(&draft.origin, draft.seq, &draft.predecessor, &draft.body)?;
        let body_bytes = encode_body(&draft.body)?;
        let digest = digest_of(
            JOURNAL_RECORD_FORMAT_V1,
            &draft.origin,
            draft.seq,
            &draft.predecessor,
            &body_bytes,
        );
        let record = JournalRecordV1 {
            format: JOURNAL_RECORD_FORMAT_V1,
            origin: draft.origin,
            seq: draft.seq,
            predecessor: draft.predecessor,
            body: draft.body,
            digest,
        };
        if record.encoded_len()? > MAX_RECORD_BYTES {
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
    pub const fn body(&self) -> &RecordBody {
        &self.body
    }
    /// Digest over format, origin, sequence, predecessor and body.
    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    /// Verify the record against what the stream expects: origin fields,
    /// index, predecessor chain and re-derived digest.
    pub fn verify(&self, expect: &RecordExpectation) -> Result<(), RecordError> {
        if self.format != JOURNAL_RECORD_FORMAT_V1 {
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
        check_body(&self.origin, self.seq, &self.predecessor, &self.body)?;
        let body_bytes = encode_body(&self.body)?;
        let derived = digest_of(
            self.format,
            &self.origin,
            self.seq,
            &self.predecessor,
            &body_bytes,
        );
        if derived != self.digest {
            return Err(RecordError::DigestMismatch);
        }
        Ok(())
    }

    /// Encoded length in bytes.
    pub fn encoded_len(&self) -> Result<usize, RecordError> {
        Ok(self.encode()?.len())
    }

    /// Durable encoding.
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let wire = RecordWireV1 {
            format: self.format,
            origin: self.origin,
            seq: self.seq,
            predecessor: self.predecessor,
            body: self.body.clone(),
            digest: self.digest,
        };
        postcard::to_allocvec(&wire).map_err(|_| RecordError::TooLarge)
    }

    /// Decode exactly: bounds are checked before nested allocation is
    /// trusted, the digest is re-derived, trailing bytes are rejected, and
    /// a decode error is never end of data.
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
        if wire.format != JOURNAL_RECORD_FORMAT_V1 {
            return Err(RecordError::UnsupportedFormat { found: wire.format });
        }
        check_body(&wire.origin, wire.seq, &wire.predecessor, &wire.body)?;
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
        Ok(JournalRecordV1 {
            format: wire.format,
            origin: wire.origin,
            seq: wire.seq,
            predecessor: wire.predecessor,
            body: wire.body,
            digest: wire.digest,
        })
    }
}
