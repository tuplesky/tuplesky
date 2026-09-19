//! Frontiers and the checkpoint pointer (design Sections 17.16.2-17.16.4).
//!
//! Per stream, track `J` (known durable journal head), `M` (completed
//! materialization) and `C` (published durable local checkpoint boundary)
//! with `C <= M <= J`. Execution position and KV revision are tracked
//! elsewhere and are not derivable from these. Native database visibility
//! may precede callbacks; public views still require proven frontiers.

use core::fmt;

use coord_store_api::envelope::AppliedStamp;
use coord_store_api::seq::StoreSeq;
use coord_types::identity::Digest32;
use coord_types::ids::LocalJournalSeq;
use serde::{Deserialize, Serialize};

use crate::record::RecordOrigin;

/// Format version of `LocalRecoveryCheckpointV1` manifests a pointer may
/// reference.
pub const LOCAL_CHECKPOINT_FORMAT_V1: u16 = 1;

/// The durable pointer appended (as a `PublishLocalCheckpoint` record) once
/// a complete inactive checkpoint and its manifest are synced. The newest
/// durable pointer, never the newest directory, selects recovery state
/// (Section 17.16.3 step 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointPointerV1 {
    /// Origin whose complete logical storage the checkpoint holds.
    pub origin: RecordOrigin,
    /// Represented sequence `C`: every local obligation through it is inside.
    pub represented: LocalJournalSeq,
    /// Manifest format.
    pub format: u16,
    /// Digest of the validated manifest.
    pub manifest_digest: Digest32,
    /// Checkpoint identity (the `LocalCheckpointRoot`).
    pub checkpoint_id: Digest32,
}

/// Choose the recovery baseline among durably published pointers of one
/// origin: the highest represented sequence. Pointers of another origin are
/// ignored; a directory listing is not an input to this decision.
pub fn select_recovery_pointer<'a>(
    origin: &RecordOrigin,
    published: impl IntoIterator<Item = &'a CheckpointPointerV1>,
) -> Option<&'a CheckpointPointerV1> {
    published
        .into_iter()
        .filter(|p| p.origin == *origin && p.format == LOCAL_CHECKPOINT_FORMAT_V1)
        .max_by_key(|p| p.represented)
}

/// Why a frontier could not move.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrontierError {
    /// The recovered triple violates `C <= M <= J`.
    Inconsistent,
    /// The durable head would not advance.
    DurableNotAdvancing,
    /// Materialization would not advance or would pass the durable head.
    MaterializeOutOfRange,
    /// A checkpoint would regress or would cover unmaterialized records.
    CheckpointOutOfRange,
}

impl fmt::Display for FrontierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            FrontierError::Inconsistent => "frontiers violate C <= M <= J",
            FrontierError::DurableNotAdvancing => "durable head must advance",
            FrontierError::MaterializeOutOfRange => "materialization must stay within (M, J]",
            FrontierError::CheckpointOutOfRange => "checkpoint must stay within [C, M]",
        };
        f.write_str(text)
    }
}

impl core::error::Error for FrontierError {}

/// The three frontiers of one stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Frontiers {
    durable: LocalJournalSeq,
    materialized: LocalJournalSeq,
    checkpoint: LocalJournalSeq,
}

impl Frontiers {
    /// An empty stream.
    pub const EMPTY: Frontiers = Frontiers {
        durable: LocalJournalSeq::ZERO,
        materialized: LocalJournalSeq::ZERO,
        checkpoint: LocalJournalSeq::ZERO,
    };

    /// Rebuild after crash from the actual valid suffix (`J`), the applied
    /// stamp (`M`) and the selected pointer (`C`).
    pub fn recovered(
        durable: LocalJournalSeq,
        materialized: LocalJournalSeq,
        checkpoint: LocalJournalSeq,
    ) -> Result<Self, FrontierError> {
        if checkpoint <= materialized && materialized <= durable {
            Ok(Frontiers {
                durable,
                materialized,
                checkpoint,
            })
        } else {
            Err(FrontierError::Inconsistent)
        }
    }

    /// `J`.
    pub const fn durable(&self) -> LocalJournalSeq {
        self.durable
    }

    /// `M`.
    pub const fn materialized(&self) -> LocalJournalSeq {
        self.materialized
    }

    /// `C`.
    pub const fn checkpoint(&self) -> LocalJournalSeq {
        self.checkpoint
    }

    /// Records through this sequence are represented by the published
    /// checkpoint and may be retired by a later durable compaction.
    pub const fn reclaimable_through(&self) -> LocalJournalSeq {
        self.checkpoint
    }

    /// The journal is durable through `to`.
    pub fn advance_durable(&mut self, to: LocalJournalSeq) -> Result<(), FrontierError> {
        if to <= self.durable {
            return Err(FrontierError::DurableNotAdvancing);
        }
        self.durable = to;
        Ok(())
    }

    /// Materialization completed through `to` (durable first).
    pub fn advance_materialized(&mut self, to: LocalJournalSeq) -> Result<(), FrontierError> {
        if to <= self.materialized || to > self.durable {
            return Err(FrontierError::MaterializeOutOfRange);
        }
        self.materialized = to;
        Ok(())
    }

    /// A checkpoint representing `to` is durably published.
    pub fn publish_checkpoint(&mut self, to: LocalJournalSeq) -> Result<(), FrontierError> {
        if to < self.checkpoint || to > self.materialized {
            return Err(FrontierError::CheckpointOutOfRange);
        }
        self.checkpoint = to;
        Ok(())
    }
}

/// The materialized frontier of a stream together with the digest of the
/// last applied record. It maps one-to-one to the projection's `StoreSeq`
/// ([`AppliedFrontier::store_seq`]) and to the persisted `AppliedStamp`;
/// there is no second uncorrelated counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppliedFrontier {
    /// Last materialized sequence (`M`).
    pub materialized: LocalJournalSeq,
    /// Digest of the last materialized record.
    pub last_digest: Digest32,
}

impl AppliedFrontier {
    /// Nothing materialized.
    pub const INITIAL: AppliedFrontier = AppliedFrontier {
        materialized: LocalJournalSeq::ZERO,
        last_digest: Digest32([0; 32]),
    };

    /// The store sequence this frontier represents.
    pub const fn store_seq(&self) -> StoreSeq {
        StoreSeq::from_journal(self.materialized)
    }

    /// The applied stamp persisted atomically with the projection update.
    ///
    /// The stamp derives its journal sequence from the store sequence
    /// rather than taking one: the two are one fact, and the constructor
    /// is what keeps them so.
    pub fn stamp(&self) -> AppliedStamp {
        AppliedStamp::new(self.store_seq(), self.last_digest)
    }

    /// Rebuild from a decoded stamp (the one-to-one mapping was already
    /// verified by the stamp decoder).
    pub const fn from_stamp(stamp: &AppliedStamp) -> Self {
        AppliedFrontier {
            materialized: stamp.journal_seq(),
            last_digest: stamp.last_batch_digest(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::StorageStreamId;
    use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};

    fn seq(n: u64) -> LocalJournalSeq {
        LocalJournalSeq::new(n).unwrap()
    }

    fn origin(domain: u8) -> RecordOrigin {
        RecordOrigin {
            cluster: ClusterId([1; 16]),
            domain: DomainId([domain; 16]),
            replica: ReplicaId([3; 16]),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
            stream: StorageStreamId::FIRST,
        }
    }

    fn pointer(domain: u8, represented: u64) -> CheckpointPointerV1 {
        CheckpointPointerV1 {
            origin: origin(domain),
            represented: seq(represented),
            format: LOCAL_CHECKPOINT_FORMAT_V1,
            manifest_digest: Digest32([represented as u8; 32]),
            checkpoint_id: Digest32([0xcc; 32]),
        }
    }

    #[test]
    fn frontiers_keep_c_le_m_le_j() {
        assert_eq!(
            Frontiers::recovered(seq(5), seq(6), seq(1)),
            Err(FrontierError::Inconsistent)
        );
        assert_eq!(
            Frontiers::recovered(seq(5), seq(3), seq(4)),
            Err(FrontierError::Inconsistent)
        );
        let mut f = Frontiers::recovered(seq(5), seq(3), seq(1)).unwrap();
        assert_eq!(
            f.advance_materialized(seq(6)),
            Err(FrontierError::MaterializeOutOfRange)
        );
        assert_eq!(
            f.advance_materialized(seq(3)),
            Err(FrontierError::MaterializeOutOfRange)
        );
        assert_eq!(
            f.publish_checkpoint(seq(4)),
            Err(FrontierError::CheckpointOutOfRange)
        );
        assert_eq!(
            f.publish_checkpoint(LocalJournalSeq::ZERO),
            Err(FrontierError::CheckpointOutOfRange)
        );
        assert_eq!(
            f.advance_durable(seq(5)),
            Err(FrontierError::DurableNotAdvancing)
        );
        f.advance_durable(seq(8)).unwrap();
        f.advance_materialized(seq(8)).unwrap();
        f.publish_checkpoint(seq(8)).unwrap();
        assert_eq!(f.reclaimable_through(), seq(8));
        assert_eq!(f, Frontiers::recovered(seq(8), seq(8), seq(8)).unwrap());
        // The frontier tracks no execution position or revision.
        assert_eq!(Frontiers::EMPTY.durable(), LocalJournalSeq::ZERO);
    }

    #[test]
    fn applied_frontier_maps_one_to_one_to_store_seq() {
        let f = AppliedFrontier {
            materialized: seq(42),
            last_digest: Digest32([7; 32]),
        };
        assert_eq!(f.store_seq(), StoreSeq::from_journal(seq(42)));
        let stamp = f.stamp();
        assert_eq!(stamp.journal_seq(), seq(42));
        assert_eq!(stamp.store_seq().journal_seq(), seq(42));
        let decoded = AppliedStamp::from_envelope(&stamp.to_envelope().unwrap()).unwrap();
        assert_eq!(AppliedFrontier::from_stamp(&decoded), f);
        assert_eq!(AppliedFrontier::INITIAL.store_seq(), StoreSeq::INITIAL);
    }

    #[test]
    fn recovery_selects_the_highest_durable_pointer_of_the_origin() {
        let older = pointer(1, 10);
        let newest = pointer(1, 30);
        let other_domain = pointer(2, 90);
        let wrong_format = CheckpointPointerV1 {
            format: 2,
            ..pointer(1, 50)
        };
        let published = [older, other_domain, newest, wrong_format];
        assert_eq!(
            select_recovery_pointer(&origin(1), &published),
            Some(&newest)
        );
        assert_eq!(select_recovery_pointer(&origin(3), &published), None);
    }
}
