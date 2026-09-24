//! The actor-owned logical outbox (design Section 4.8).
//!
//! Immutable eligible evidence is *published* logically here and only
//! *transmitted* once every durable prerequisite holds. Releasing is gated
//! on four independent facts, each checked on every release:
//!
//! 1. every required barrier completed with `JournalDurable` (a two-barrier
//!    effect cannot release after one), and the journal is durable through
//!    the sequence the effect's context names (`required_journal_seq`): a
//!    completion that reports a lower sequence, or an effect that names no
//!    barrier at all, waits for the durable frontier to reach it;
//! 2. every completion belongs to the current boot (a wrong-boot completion
//!    is recorded as stale bookkeeping and never authorizes);
//! 3. no required barrier failed (a failed effect is dropped, not retried);
//! 4. the effect's ballot is not obsolete relative to the current promise (a
//!    late callback may complete bookkeeping without authorizing an
//!    obsolete vote).
//!
//! Duplicate completions are idempotent.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;

use coord_types::ids::{Ballot, LocalJournalSeq};

use crate::effect::{BarrierId, BootId, Effect, EffectContext, PeerId};
use crate::event::{StorageError, StorageEvent};

/// A pending send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingSend {
    /// Binding context.
    pub context: EffectContext,
    /// Required barriers.
    pub requires: Vec<BarrierId>,
    /// Destination.
    pub to: PeerId,
    /// Frame bytes.
    pub frame: Vec<u8>,
}

/// Why a pending send was dropped rather than released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseError {
    /// A required barrier failed.
    BarrierFailed(StorageError),
    /// The effect's ballot is older than the current promise.
    ObsoleteBallot,
    /// The effect was produced by a different boot than the current one.
    WrongBoot,
}

/// The logical outbox of one domain actor.
#[derive(Debug)]
pub struct Outbox {
    boot: Option<BootId>,
    durable: BTreeSet<BarrierId>,
    /// Highest journal sequence any current-boot `JournalDurable` reported.
    /// Journal durability is prefix-ordered, so this is the durable cut.
    durable_through: LocalJournalSeq,
    failed: BTreeMap<BarrierId, StorageError>,
    /// Completions from other boots: bookkeeping only.
    stale_completions: u64,
    pending: Vec<PendingSend>,
    dropped: Vec<(PendingSend, ReleaseError)>,
}

impl Outbox {
    /// Empty outbox for `boot`. Everything from earlier boots is forgotten:
    /// old sends are never replayed after restart.
    pub fn new(boot: BootId) -> Self {
        Outbox {
            boot: Some(boot),
            durable: BTreeSet::new(),
            durable_through: LocalJournalSeq::ZERO,
            failed: BTreeMap::new(),
            stale_completions: 0,
            pending: Vec::new(),
            dropped: Vec::new(),
        }
    }

    /// Current boot.
    pub fn boot(&self) -> Option<BootId> {
        self.boot
    }

    /// Enqueue a send effect. Effects from another boot are dropped at once.
    pub fn publish(&mut self, send: PendingSend) {
        if self.boot != Some(send.context.boot_id) {
            self.dropped.push((send, ReleaseError::WrongBoot));
            return;
        }
        self.pending.push(send);
    }

    /// Record a storage fact. Returns whether it changed current-boot
    /// bookkeeping (a duplicate or wrong-boot completion returns `false`).
    pub fn observe(&mut self, event: &StorageEvent) -> bool {
        let Some(barrier) = event.barrier() else {
            return false;
        };
        if self.boot != Some(barrier.boot_id) {
            self.stale_completions += 1;
            return false;
        }
        match event {
            StorageEvent::JournalDurable { journal_seq, .. } => {
                let advanced = *journal_seq > self.durable_through;
                if advanced {
                    self.durable_through = *journal_seq;
                }
                self.durable.insert(barrier) || advanced
            }
            StorageEvent::Materialized { .. } => false,
            StorageEvent::Failed { error, .. } => {
                if self.failed.contains_key(&barrier) {
                    return false;
                }
                self.failed.insert(barrier, *error);
                true
            }
            StorageEvent::LocalCheckpointPublished { .. } => false,
        }
    }

    /// Whether a barrier is durable in this boot.
    pub fn is_durable(&self, barrier: &BarrierId) -> bool {
        self.durable.contains(barrier)
    }

    /// Whether a barrier of this boot failed. A send requiring it was
    /// dropped and will never release; neither would a repeat of it.
    pub fn is_failed(&self, barrier: &BarrierId) -> bool {
        self.failed.contains_key(barrier)
    }

    /// Journal sequence the current boot has been reported durable through.
    pub fn durable_through(&self) -> LocalJournalSeq {
        self.durable_through
    }

    /// Number of completions ignored because they belonged to another boot.
    pub fn stale_completions(&self) -> u64 {
        self.stale_completions
    }

    /// Release every send whose prerequisites hold under the current ballot.
    /// Sends with a failed barrier or an obsolete ballot are dropped and
    /// reported through [`Outbox::take_dropped`].
    pub fn release(&mut self, current_ballot: &Ballot) -> Vec<Effect> {
        let mut released = Vec::new();
        let mut keep = Vec::new();
        for send in self.pending.drain(..) {
            let obsolete = match send.context.ballot.compare_same_epoch(current_ballot) {
                Some(core::cmp::Ordering::Less) => true,
                Some(_) => false,
                None => send.context.ballot.epoch < current_ballot.epoch,
            };
            if obsolete {
                self.dropped.push((send, ReleaseError::ObsoleteBallot));
                continue;
            }
            if let Some(err) = send.requires.iter().find_map(|b| self.failed.get(b)) {
                let err = *err;
                self.dropped.push((send, ReleaseError::BarrierFailed(err)));
                continue;
            }
            let barriers_durable = send.requires.iter().all(|b| self.durable.contains(b));
            let cut_durable = send.context.required_journal_seq <= self.durable_through;
            if barriers_durable && cut_durable {
                released.push(Effect::SendWhenDurable {
                    context: send.context,
                    requires: send.requires,
                    to: send.to,
                    frame: send.frame,
                });
            } else {
                keep.push(send);
            }
        }
        self.pending = keep;
        released
    }

    /// Sends still waiting.
    pub fn pending(&self) -> &[PendingSend] {
        &self.pending
    }

    /// Take the sends dropped since the last call, with reasons.
    pub fn take_dropped(&mut self) -> Vec<(PendingSend, ReleaseError)> {
        core::mem::take(&mut self.dropped)
    }
}

/// Allocator of per-boot barrier sequences.
#[derive(Debug)]
pub struct BarrierAllocator {
    node_generation: coord_types::ids::ReplicaIncarnation,
    boot_id: BootId,
    next: u64,
}

impl BarrierAllocator {
    /// New allocator for this boot.
    pub const fn new(
        node_generation: coord_types::ids::ReplicaIncarnation,
        boot_id: BootId,
    ) -> Self {
        BarrierAllocator {
            node_generation,
            boot_id,
            next: 1,
        }
    }

    /// Next barrier; sequences never repeat within a boot.
    pub fn allocate(&mut self) -> BarrierId {
        let sequence = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("barrier sequence exhausted");
        BarrierId {
            node_generation: self.node_generation,
            boot_id: self.boot_id,
            sequence,
        }
    }
}

/// Timer bookkeeping with logical generations (design Section 18.2).
#[derive(Debug, Default)]
pub struct TimerTable {
    generations: BTreeMap<u32, u64>,
}

impl TimerTable {
    /// Arm (or re-arm) a timer; returns the effect to emit. The previous
    /// generation becomes obsolete.
    pub fn arm(&mut self, name: u32, after_ticks: u64) -> Effect {
        let generation = self.generations.entry(name).or_insert(0);
        *generation += 1;
        Effect::ArmTimer {
            id: crate::effect::TimerId {
                name,
                generation: *generation,
            },
            after_ticks,
        }
    }

    /// Cancel a timer; later fires of any existing generation are ignored.
    pub fn cancel(&mut self, name: u32) -> Effect {
        let generation = self.generations.entry(name).or_insert(0);
        *generation += 1;
        Effect::CancelTimer {
            id: crate::effect::TimerId {
                name,
                generation: *generation,
            },
        }
    }

    /// Whether a fired timer is current. Obsolete generations return `false`.
    pub fn accept(&self, id: crate::effect::TimerId) -> bool {
        self.generations
            .get(&id.name)
            .is_some_and(|g| *g == id.generation)
    }
}
