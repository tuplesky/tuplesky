//! Stream positions and the accepted durable head (design Sections 17.3.1,
//! 17.3.2 and 17.15).
//!
//! Within one stream `LocalJournalSeq` is strictly increasing and becomes the
//! engine entry index. It is a distinct checked counter in `coord-types`
//! with no conversion to or from any other counter, so the type system
//! refuses it wherever a KV revision, execution position, ballot, fencing
//! generation or cross-replica order belongs:
//!
//! ```compile_fail
//! use coord_types::ids::{KvRevision, LocalJournalSeq};
//! let seq = LocalJournalSeq::new(7).unwrap();
//! let _revision: KvRevision = seq;
//! ```
//!
//! ```compile_fail
//! use coord_types::ids::{ExecutionPosition, LocalJournalSeq};
//! let seq = LocalJournalSeq::new(7).unwrap();
//! let _position: ExecutionPosition = seq.into();
//! ```
//!
//! ```compile_fail
//! use coord_types::ids::{Ballot, ConfigurationEpoch, LocalJournalSeq, ReplicaId};
//! let seq = LocalJournalSeq::new(7).unwrap();
//! let _ballot = Ballot { epoch: ConfigurationEpoch::ZERO, number: seq, leader: ReplicaId([0; 16]) };
//! ```
//!
//! ```compile_fail
//! use coord_types::ids::{LeaseGeneration, LocalJournalSeq, ReplicaIncarnation};
//! let seq = LocalJournalSeq::new(7).unwrap();
//! let _fence: ReplicaIncarnation = seq;
//! let _generation: LeaseGeneration = seq;
//! ```
//!
//! ```compile_fail
//! use coord_journal_api::head::WrittenBytes;
//! use coord_types::ids::LocalJournalSeq;
//! let written = WrittenBytes(4096);
//! let _head: LocalJournalSeq = written.into();
//! ```
//!
//! [`StreamHead`] tracks the accepted durable head of one stream. Initially
//! one uncompleted authoritative batch per stream avoids ambiguous
//! reservations while many streams group together. A definite failure
//! before append releases the reservation; an indeterminate failure after
//! submission blocks further reservations until the actual durable head has
//! been recovered and reconciled. Timeout never proves absence.

use core::fmt;
use core::num::NonZeroU32;

use coord_core::effect::BarrierId;
use coord_types::error::CounterOverflow;
use coord_types::ids::LocalJournalSeq;
use serde::{Deserialize, Serialize};

use crate::stream::StorageStreamId;

/// A position inside one stream. A bare `LocalJournalSeq` is meaningless
/// without its stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamPosition {
    /// Stream.
    pub stream: StorageStreamId,
    /// Sequence within the stream.
    pub seq: LocalJournalSeq,
}

/// Byte count returned by a grouped engine write. It is evidence that the
/// write returned, never a sequence, frontier or durability proof by
/// itself; the caller maps success to its own reserved sequences.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WrittenBytes(pub u64);

/// The single outstanding authoritative batch of a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingBatch {
    /// Barrier the caller will complete.
    pub barrier: BarrierId,
    /// First reserved sequence.
    pub first: LocalJournalSeq,
    /// Last reserved sequence.
    pub last: LocalJournalSeq,
}

/// Outstanding state of a stream head.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HeadState {
    /// No batch outstanding.
    Idle,
    /// One batch submitted; its outcome is not known yet.
    Pending(PendingBatch),
    /// The submitted batch failed indeterminately; nothing may be reserved
    /// until the actual durable head is reconciled.
    Uncertain(PendingBatch),
}

/// What reconciliation found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reconciled {
    /// The uncertain batch is fully durable; the head advanced.
    Present(PendingBatch),
    /// The uncertain batch is absent; its sequences were never occupied.
    Absent(PendingBatch),
}

/// Why a head operation was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HeadError {
    /// A batch is already outstanding on this stream.
    BatchOutstanding {
        /// Its barrier.
        barrier: BarrierId,
    },
    /// The head is uncertain; reconcile first.
    Uncertain {
        /// The uncertain batch's barrier.
        barrier: BarrierId,
    },
    /// No batch is outstanding, or the barrier does not name it.
    NotOutstanding,
    /// The head is not uncertain, so there is nothing to reconcile.
    NotUncertain,
    /// The recovered head is inside the uncertain batch: a partial batch is
    /// not the qualified torn-tail case and quarantines the stream.
    PartialBatch {
        /// Recovered durable head.
        recovered: LocalJournalSeq,
    },
    /// The recovered head is neither the old head nor the batch's last
    /// sequence.
    HeadMismatch {
        /// Recovered durable head.
        recovered: LocalJournalSeq,
    },
    /// The sequence space is exhausted; sequences never wrap.
    Overflow,
}

impl fmt::Display for HeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeadError::BatchOutstanding { .. } => f.write_str("batch outstanding on stream"),
            HeadError::Uncertain { .. } => f.write_str("stream head uncertain; reconcile"),
            HeadError::NotOutstanding => f.write_str("no such outstanding batch"),
            HeadError::NotUncertain => f.write_str("stream head is not uncertain"),
            HeadError::PartialBatch { recovered } => {
                write!(f, "partial batch recovered at {recovered}")
            }
            HeadError::HeadMismatch { recovered } => {
                write!(f, "recovered head {recovered} matches neither outcome")
            }
            HeadError::Overflow => f.write_str("local sequence exhausted"),
        }
    }
}

impl core::error::Error for HeadError {}

impl From<CounterOverflow> for HeadError {
    fn from(_: CounterOverflow) -> Self {
        HeadError::Overflow
    }
}

/// The accepted durable head of one stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamHead {
    stream: StorageStreamId,
    durable: LocalJournalSeq,
    state: HeadState,
}

impl StreamHead {
    /// Open a head at the durable sequence recovered from the actual valid
    /// suffix (never a volatile head or an old completion token).
    /// `LocalJournalSeq::ZERO` means the stream is empty.
    pub const fn open(stream: StorageStreamId, durable: LocalJournalSeq) -> Self {
        StreamHead {
            stream,
            durable,
            state: HeadState::Idle,
        }
    }

    /// Stream.
    pub const fn stream(&self) -> StorageStreamId {
        self.stream
    }

    /// Last durable sequence.
    pub const fn durable(&self) -> LocalJournalSeq {
        self.durable
    }

    /// Outstanding state.
    pub const fn state(&self) -> HeadState {
        self.state
    }

    /// Sequence the next record will take.
    pub fn next_seq(&self) -> Result<LocalJournalSeq, HeadError> {
        Ok(self.durable.checked_next()?)
    }

    /// Reserve `records` contiguous sequences for one batch. Refused while
    /// a batch is outstanding or the head is uncertain.
    pub fn reserve(
        &mut self,
        barrier: BarrierId,
        records: NonZeroU32,
    ) -> Result<PendingBatch, HeadError> {
        match self.state {
            HeadState::Idle => {}
            HeadState::Pending(p) => {
                return Err(HeadError::BatchOutstanding { barrier: p.barrier });
            }
            HeadState::Uncertain(p) => return Err(HeadError::Uncertain { barrier: p.barrier }),
        }
        let first = self.durable.checked_next()?;
        let last = LocalJournalSeq::new(
            first
                .get()
                .checked_add(u64::from(records.get()) - 1)
                .ok_or(HeadError::Overflow)?,
        )
        .map_err(|_| HeadError::Overflow)?;
        let pending = PendingBatch {
            barrier,
            first,
            last,
        };
        self.state = HeadState::Pending(pending);
        Ok(pending)
    }

    fn take_pending(&mut self, barrier: BarrierId) -> Result<PendingBatch, HeadError> {
        match self.state {
            HeadState::Pending(p) if p.barrier == barrier => Ok(p),
            _ => Err(HeadError::NotOutstanding),
        }
    }

    /// The outstanding batch is durable: the head advances to its last
    /// sequence.
    pub fn complete_durable(&mut self, barrier: BarrierId) -> Result<PendingBatch, HeadError> {
        let p = self.take_pending(barrier)?;
        self.durable = p.last;
        self.state = HeadState::Idle;
        Ok(p)
    }

    /// The outstanding batch definitely did not append (guard rejection or
    /// specific noncommit evidence): the reservation is released and the
    /// sequences were never occupied.
    pub fn fail_definite(&mut self, barrier: BarrierId) -> Result<PendingBatch, HeadError> {
        let p = self.take_pending(barrier)?;
        self.state = HeadState::Idle;
        Ok(p)
    }

    /// The outstanding batch's outcome is unknown: the head becomes
    /// uncertain and refuses reservations until reconciled.
    pub fn fail_indeterminate(&mut self, barrier: BarrierId) -> Result<PendingBatch, HeadError> {
        let p = self.take_pending(barrier)?;
        self.state = HeadState::Uncertain(p);
        Ok(p)
    }

    /// Resolve an uncertain head from the durable head actually recovered
    /// from valid records. Only the old head (batch absent) or the batch's
    /// last sequence (batch present) is acceptable.
    pub fn reconcile(&mut self, recovered: LocalJournalSeq) -> Result<Reconciled, HeadError> {
        let p = match self.state {
            HeadState::Uncertain(p) => p,
            _ => return Err(HeadError::NotUncertain),
        };
        if recovered == self.durable {
            self.state = HeadState::Idle;
            Ok(Reconciled::Absent(p))
        } else if recovered == p.last {
            self.durable = p.last;
            self.state = HeadState::Idle;
            Ok(Reconciled::Present(p))
        } else if recovered >= p.first && recovered < p.last {
            Err(HeadError::PartialBatch { recovered })
        } else {
            Err(HeadError::HeadMismatch { recovered })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::effect::BootId;
    use coord_types::ids::ReplicaIncarnation;

    fn barrier(n: u64) -> BarrierId {
        BarrierId {
            node_generation: ReplicaIncarnation::new(1).unwrap(),
            boot_id: BootId([9; 16]),
            sequence: n,
        }
    }

    fn seq(n: u64) -> LocalJournalSeq {
        LocalJournalSeq::new(n).unwrap()
    }

    fn head() -> StreamHead {
        StreamHead::open(StorageStreamId::FIRST, seq(10))
    }

    #[test]
    fn one_outstanding_batch_per_stream() {
        let mut h = head();
        let p = h.reserve(barrier(1), NonZeroU32::new(3).unwrap()).unwrap();
        assert_eq!((p.first, p.last), (seq(11), seq(13)));
        assert_eq!(
            h.reserve(barrier(2), NonZeroU32::new(1).unwrap()),
            Err(HeadError::BatchOutstanding {
                barrier: barrier(1)
            })
        );
        assert_eq!(
            h.complete_durable(barrier(2)),
            Err(HeadError::NotOutstanding)
        );
        h.complete_durable(barrier(1)).unwrap();
        assert_eq!(h.durable(), seq(13));
        assert_eq!(h.next_seq(), Ok(seq(14)));
    }

    #[test]
    fn definite_failure_releases_without_advancing() {
        let mut h = head();
        h.reserve(barrier(1), NonZeroU32::new(2).unwrap()).unwrap();
        h.fail_definite(barrier(1)).unwrap();
        assert_eq!(h.durable(), seq(10));
        let p = h.reserve(barrier(2), NonZeroU32::new(1).unwrap()).unwrap();
        assert_eq!(p.first, seq(11));
    }

    #[test]
    fn indeterminate_failure_blocks_until_reconciled() {
        let mut h = head();
        h.reserve(barrier(1), NonZeroU32::new(2).unwrap()).unwrap();
        h.fail_indeterminate(barrier(1)).unwrap();
        assert_eq!(
            h.reserve(barrier(2), NonZeroU32::new(1).unwrap()),
            Err(HeadError::Uncertain {
                barrier: barrier(1)
            })
        );
        assert_eq!(
            h.reconcile(seq(11)),
            Err(HeadError::PartialBatch { recovered: seq(11) })
        );
        assert_eq!(
            h.reconcile(seq(20)),
            Err(HeadError::HeadMismatch { recovered: seq(20) })
        );
        let mut absent = h;
        assert!(matches!(
            absent.reconcile(seq(10)),
            Ok(Reconciled::Absent(_))
        ));
        assert_eq!(absent.durable(), seq(10));
        assert!(matches!(h.reconcile(seq(12)), Ok(Reconciled::Present(_))));
        assert_eq!(h.durable(), seq(12));
        assert_eq!(h.reconcile(seq(12)), Err(HeadError::NotUncertain));
    }

    #[test]
    fn sequences_never_wrap() {
        let mut h = StreamHead::open(StorageStreamId::FIRST, LocalJournalSeq::MAX);
        assert_eq!(h.next_seq(), Err(HeadError::Overflow));
        assert_eq!(
            h.reserve(barrier(1), NonZeroU32::new(1).unwrap()),
            Err(HeadError::Overflow)
        );
        let mut near = StreamHead::open(StorageStreamId::FIRST, seq(u64::MAX - 1));
        assert_eq!(
            near.reserve(barrier(1), NonZeroU32::new(2).unwrap()),
            Err(HeadError::Overflow)
        );
        assert_eq!(near.state(), HeadState::Idle);
    }
}
