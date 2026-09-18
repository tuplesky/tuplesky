//! The strict single-writer store worker.

use std::collections::VecDeque;
use std::sync::Arc;

use coord_core::effect::{BootId, PersistBatch};
use coord_core::event::{StorageError, StorageEvent};
use coord_store_api::engine::{
    CommitFailure, EngineError, ErrorClass, LocalEngine, SnapshotSource, WriteTxn,
};
use coord_store_api::envelope::AppliedStamp;
use coord_store_api::seq::StoreSeq;
use coord_types::identity::Digest32;
use coord_types::ids::{LocalJournalSeq, ReplicaIncarnation};

use crate::lowering::{DurableMeta, ExecutionFrontier, batch_bytes, batch_digest, lower_update};
use crate::view::{Frontier, GatedReader};

/// Grouping budgets (design Section 17.3.3: 64 records / 256 KiB targets,
/// no idle wait, a separately bounded larger-record path).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupLimits {
    /// Maximum batches per durable transaction.
    pub max_records: usize,
    /// Target bytes per durable transaction.
    pub max_bytes: usize,
    /// Hard bound for one batch admitted on its own.
    pub max_single_batch_bytes: usize,
    /// Maximum queued bytes before submissions are refused (backpressure).
    pub max_queued_bytes: usize,
}

impl Default for GroupLimits {
    fn default() -> Self {
        GroupLimits {
            max_records: 64,
            max_bytes: 256 * 1024,
            max_single_batch_bytes: 8 * 1024 * 1024,
            max_queued_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Worker state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerState {
    /// Accepting submissions and flushes.
    Ready,
    /// A commit outcome is unknown; `reconcile` must run before anything else.
    NeedsReconcile,
    /// The projection disagrees with every expected state; nothing is served.
    Quarantined,
}

/// Why a submission was refused. Nothing was queued.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitError {
    /// The batch belongs to another boot.
    WrongBoot,
    /// Larger than the single-batch hard bound.
    BatchTooLarge,
    /// Queue is full; retry after a flush (accepted work is never evicted).
    QueueFull,
    /// Worker is not ready.
    NotReady(WorkerState),
}

/// Result of one flush.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Storage facts to feed the machine (durable, materialized, failed).
    pub events: Vec<StorageEvent>,
    /// Batches committed durably.
    pub committed: usize,
    /// Batches rejected by guard validation.
    pub rejected: usize,
    /// Whether the group's outcome is unknown and reconciliation is needed.
    pub indeterminate: bool,
}

struct PendingGroup {
    batches: Vec<PersistBatch>,
    seqs: Vec<LocalJournalSeq>,
    expected: DurableMeta,
}

/// The worker.
pub struct StoreWorker<E: LocalEngine> {
    engine: E,
    boot: BootId,
    incarnation: ReplicaIncarnation,
    limits: GroupLimits,
    queue: VecDeque<PersistBatch>,
    queued_bytes: usize,
    meta: DurableMeta,
    frontier: Arc<Frontier>,
    state: WorkerState,
    pending: Option<PendingGroup>,
}

impl<E: LocalEngine> StoreWorker<E> {
    /// Open the worker for this boot, recovering the durable metadata.
    pub fn open(
        engine: E,
        boot: BootId,
        incarnation: ReplicaIncarnation,
        limits: GroupLimits,
    ) -> Result<Self, EngineError> {
        let meta = DurableMeta::read(&engine.reader().snapshot()?)?;
        let frontier = Arc::new(Frontier::default());
        frontier.set_completed(meta.stamp.store_seq);
        Ok(StoreWorker {
            engine,
            boot,
            incarnation,
            limits,
            queue: VecDeque::new(),
            queued_bytes: 0,
            meta,
            frontier,
            state: WorkerState::Ready,
            pending: None,
        })
    }

    /// Boot this worker serves.
    pub fn boot(&self) -> BootId {
        self.boot
    }

    /// Incarnation this worker serves.
    pub fn incarnation(&self) -> ReplicaIncarnation {
        self.incarnation
    }

    /// Current state.
    pub fn state(&self) -> &WorkerState {
        &self.state
    }

    /// Durable metadata as last confirmed.
    pub fn meta(&self) -> &DurableMeta {
        &self.meta
    }

    /// Base the next application batch must carry.
    pub fn application_base(&self) -> coord_core::effect::ApplyBase {
        self.meta.frontier.as_base()
    }

    /// Gated reader.
    pub fn reader(&self) -> GatedReader<E::Reader> {
        GatedReader::new(self.engine.reader(), self.frontier.clone())
    }

    /// Queued batches.
    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// The engine (for harnesses that crash and reopen it).
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    /// Give the engine back (the worker's boot ends).
    pub fn into_engine(self) -> E {
        self.engine
    }

    /// Queue a batch.
    pub fn submit(&mut self, batch: PersistBatch) -> Result<(), SubmitError> {
        if self.state != WorkerState::Ready {
            return Err(SubmitError::NotReady(self.state.clone()));
        }
        if batch.barrier.boot_id != self.boot || batch.barrier.node_generation != self.incarnation {
            return Err(SubmitError::WrongBoot);
        }
        let bytes = batch_bytes(&batch);
        if bytes > self.limits.max_single_batch_bytes {
            return Err(SubmitError::BatchTooLarge);
        }
        if self.queued_bytes + bytes > self.limits.max_queued_bytes {
            return Err(SubmitError::QueueFull);
        }
        self.queued_bytes += bytes;
        self.queue.push_back(batch);
        Ok(())
    }

    fn take_group(&mut self) -> Vec<PersistBatch> {
        let mut group = Vec::new();
        let mut bytes = 0usize;
        while let Some(front) = self.queue.front() {
            let cost = batch_bytes(front);
            if !group.is_empty()
                && (group.len() >= self.limits.max_records || bytes + cost > self.limits.max_bytes)
            {
                break;
            }
            bytes += cost;
            let batch = self.queue.pop_front().expect("front exists");
            self.queued_bytes -= batch_bytes(&batch);
            group.push(batch);
        }
        group
    }

    fn durable_events(seqs: &[LocalJournalSeq], batches: &[PersistBatch]) -> Vec<StorageEvent> {
        let mut events = Vec::with_capacity(batches.len() * 2);
        for (batch, seq) in batches.iter().zip(seqs) {
            events.push(StorageEvent::JournalDurable {
                barrier_id: batch.barrier,
                journal_seq: *seq,
            });
            events.push(StorageEvent::Materialized {
                barrier_id: batch.barrier,
                journal_seq: *seq,
            });
        }
        events
    }

    /// Lower one bounded group into a durable transaction.
    pub fn flush(&mut self) -> Result<FlushOutcome, EngineError> {
        if self.state != WorkerState::Ready {
            return Err(EngineError::new(ErrorClass::Busy, "worker not ready"));
        }
        let group = self.take_group();
        if group.is_empty() {
            return Ok(FlushOutcome::default());
        }
        let mut outcome = FlushOutcome::default();
        let mut accepted: Vec<PersistBatch> = Vec::new();
        let mut seqs: Vec<LocalJournalSeq> = Vec::new();
        let mut meta = self.meta;
        let commit_result = {
            let mut tx = self.engine.begin_write()?;
            // Guards validate against the durable accepted state, read
            // inside this very transaction.
            let durable = match DurableMeta::read(&tx) {
                Ok(d) => d,
                Err(e) => {
                    drop(tx);
                    self.frontier.quarantine();
                    self.state = WorkerState::Quarantined;
                    return Err(EngineError::new(
                        ErrorClass::Corrupt,
                        format!("durable metadata unreadable: {e}"),
                    ));
                }
            };
            if durable != self.meta {
                drop(tx);
                self.frontier.quarantine();
                self.state = WorkerState::Quarantined;
                return Err(EngineError::new(
                    ErrorClass::Corrupt,
                    "durable metadata diverged from the worker's record",
                ));
            }
            for batch in group {
                if let Some(base) = batch.base
                    && base != meta.frontier.as_base()
                {
                    outcome.rejected += 1;
                    outcome.events.push(StorageEvent::Failed {
                        barrier_id: batch.barrier,
                        error: StorageError::DefinitelyNotCommitted,
                    });
                    continue;
                }
                for update in &batch.updates {
                    lower_update(&mut tx, update)?;
                }
                let seq =
                    meta.stamp.journal_seq.checked_next().map_err(|_| {
                        EngineError::new(ErrorClass::Limit, "store sequence exhausted")
                    })?;
                let digest = batch_digest(&meta.stamp.last_batch_digest, &batch);
                meta.stamp = AppliedStamp {
                    store_seq: StoreSeq::from_journal(seq),
                    journal_seq: seq,
                    last_batch_digest: digest,
                };
                if let Some(base) = batch.base {
                    meta.frontier = ExecutionFrontier {
                        configuration: base.configuration,
                        execution_position: base.execution_position.checked_next().map_err(
                            |_| EngineError::new(ErrorClass::Limit, "execution position exhausted"),
                        )?,
                    };
                }
                seqs.push(seq);
                accepted.push(batch);
            }
            if accepted.is_empty() {
                tx.abort()?;
                return Ok(outcome);
            }
            meta.write(&mut tx)?;
            tx.commit_durable()
        };
        match commit_result {
            Ok(()) => {
                self.meta = meta;
                self.frontier.set_completed(meta.stamp.store_seq);
                outcome.committed = accepted.len();
                outcome
                    .events
                    .extend(Self::durable_events(&seqs, &accepted));
                Ok(outcome)
            }
            Err(CommitFailure::DefinitelyNotCommitted(_)) => {
                for batch in &accepted {
                    outcome.events.push(StorageEvent::Failed {
                        barrier_id: batch.barrier,
                        error: StorageError::DefinitelyNotCommitted,
                    });
                }
                outcome.rejected += accepted.len();
                Ok(outcome)
            }
            Err(CommitFailure::Indeterminate(_)) => {
                self.state = WorkerState::NeedsReconcile;
                self.pending = Some(PendingGroup {
                    batches: accepted,
                    seqs,
                    expected: meta,
                });
                outcome.indeterminate = true;
                Ok(outcome)
            }
        }
    }

    /// Resolve an indeterminate commit from the semantic stamp: the group is
    /// either fully present (its stamp digest matches) or fully absent (the
    /// previous stamp is still current). Anything else quarantines.
    pub fn reconcile(&mut self) -> Result<FlushOutcome, EngineError> {
        if self.state != WorkerState::NeedsReconcile {
            return Err(EngineError::new(ErrorClass::Busy, "nothing to reconcile"));
        }
        let pending = self.pending.take().expect("pending group");
        let observed = DurableMeta::read(&self.engine.reader().snapshot()?)?;
        let mut outcome = FlushOutcome::default();
        if observed == pending.expected {
            self.meta = observed;
            self.frontier.set_completed(observed.stamp.store_seq);
            self.state = WorkerState::Ready;
            outcome.committed = pending.batches.len();
            outcome.events = Self::durable_events(&pending.seqs, &pending.batches);
            Ok(outcome)
        } else if observed == self.meta {
            self.state = WorkerState::Ready;
            outcome.rejected = pending.batches.len();
            for batch in &pending.batches {
                outcome.events.push(StorageEvent::Failed {
                    barrier_id: batch.barrier,
                    error: StorageError::DefinitelyNotCommitted,
                });
            }
            Ok(outcome)
        } else {
            self.frontier.quarantine();
            self.state = WorkerState::Quarantined;
            Err(EngineError::new(
                ErrorClass::Corrupt,
                "projection stamp matches neither the pending group nor the prior state",
            ))
        }
    }

    /// Digest of the last durable batch (diagnostic).
    pub fn last_digest(&self) -> Digest32 {
        self.meta.stamp.last_batch_digest
    }
}
