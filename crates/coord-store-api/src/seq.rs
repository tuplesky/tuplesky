//! The local applied stamp.

use core::fmt;

use coord_types::ids::LocalJournalSeq;
use serde::{Deserialize, Serialize};

/// `StoreSeq` is the projection's applied stamp. It maps one-to-one to the
/// local journal sequence of the last materialized record (the mapping is
/// [`StoreSeq::from_journal`] / [`StoreSeq::journal_seq`]); it is not an
/// execution position, KV revision, ballot or cross-replica order, and it is
/// excluded from every common hash. Ordinary code cannot construct it from
/// a raw integer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StoreSeq(LocalJournalSeq);

impl StoreSeq {
    /// The stamp of an empty projection (nothing materialized).
    pub const INITIAL: StoreSeq = StoreSeq(LocalJournalSeq::ZERO);

    /// The stamp corresponding to a materialized journal sequence.
    pub const fn from_journal(seq: LocalJournalSeq) -> Self {
        StoreSeq(seq)
    }

    /// The journal sequence this stamp represents.
    pub const fn journal_seq(self) -> LocalJournalSeq {
        self.0
    }
}

impl fmt::Debug for StoreSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreSeq({})", self.0.get())
    }
}
