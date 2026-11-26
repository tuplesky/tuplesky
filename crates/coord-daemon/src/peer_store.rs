//! The runtime's check of the store each peer voter speaks from
//! (task-d121, C1).
//!
//! Every promise and every report page names the store it came from
//! ([`StoreInstanceId`]). The first one from a peer, under an
//! incarnation, is recorded: the row ([`PeerStoreV1`]) is journaled, and
//! the message waits until it is durable before the machine sees it, so
//! nothing the machine does with it can outlive the record across a
//! crash. One from the same store is handed on. One from another store is
//! refused: the machine never sees it, and the refusal is logged and
//! counted. That is a voter wiped and initialized again under its own
//! identity, which a campaign must not count as the voter it was.
//!
//! A voter that comes back through membership as a new generation
//! (task-d41) does so under a new incarnation, which has no row yet.
//!
//! A Sync names the store of each voter its leader counted. A voter that
//! installs it records each one it has no row for, and never overwrites
//! one: a counted store that disagrees with a row is logged and counted,
//! and the Sync is installed all the same, since refusing it would stall
//! the domain over a record this voter can't arbitrate.
//!
//! The rows are the runtime's, like a floor's readiness (task-d27) and a
//! feature's support (task-d99): the machines do no I/O and keep no row
//! per peer.

use std::collections::{BTreeMap, VecDeque};

use coord_checkpoint::peer_store::{PeerStoreV1, read_peer_stores};
use coord_checkpoint::trim::TrimLimits;
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::outbox::BarrierAllocator;
use coord_store_api::engine::OrderedRead;
use coord_types::ids::{ReplicaId, ReplicaIncarnation, StoreInstanceId};

/// What the check did, since boot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerStoreCounts {
    /// A peer's first store under an incarnation, journaled.
    pub recorded: u64,
    /// A message from the store already recorded, handed on.
    pub matched: u64,
    /// A message held behind its peer's row, still in the journal.
    pub held: u64,
    /// A message from another store than the one recorded, refused.
    pub refused: u64,
    /// A row's batch that failed: its held messages are dropped, and the
    /// peer's next message is recorded again.
    pub failed: u64,
    /// A store learned from an installed Sync, for a peer with no row.
    pub learned: u64,
    /// A store an installed Sync counted that disagrees with the row.
    pub disagreed: u64,
}

/// What to do with a peer's promise or report page.
#[derive(Debug, PartialEq, Eq)]
pub enum Check {
    /// Its store is the one recorded and durable: hand it on.
    Pass,
    /// Its store is recorded and the row is still in the journal: hold it
    /// until `barrier` is durable.
    Wait(BarrierId),
    /// Its store is new for this peer and incarnation: journal `updates`
    /// under `barrier`, and hold it until that is durable.
    Record {
        /// The row's batch.
        barrier: BarrierId,
        /// The row.
        updates: Vec<StoreUpdate>,
    },
    /// Another store than the one recorded: refuse it.
    Refused {
        /// The store recorded for this peer and incarnation.
        recorded: StoreInstanceId,
    },
}

/// What became of the row batches, in order.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Settled {
    /// Batches now durable: what they held may go on.
    pub durable: Vec<BarrierId>,
    /// Batches that failed: what they held is dropped.
    pub failed: Vec<BarrierId>,
}

#[derive(Debug)]
struct Batch {
    barrier: BarrierId,
    key: (ReplicaId, ReplicaIncarnation),
}

/// A voter's record of the store each peer spoke from.
#[derive(Debug)]
pub struct PeerStores {
    /// Every row this store holds or has journaled, and the batch it is
    /// in while that is not yet durable.
    rows: BTreeMap<(ReplicaId, ReplicaIncarnation), (StoreInstanceId, Option<BarrierId>)>,
    unsettled: VecDeque<Batch>,
    barriers: BarrierAllocator,
    /// Refusals since the last [`PeerStores::take_rejections`].
    rejections: Vec<String>,
    /// Since boot.
    pub counts: PeerStoreCounts,
}

impl PeerStores {
    /// A voter's check, from the rows its store already holds.
    pub fn open<V: OrderedRead>(
        view: &V,
        incarnation: ReplicaIncarnation,
        boot: BootId,
    ) -> Result<Self, String> {
        let rows = read_peer_stores(view, &TrimLimits::default())
            .map_err(|e| format!("the peer store rows: {e:?}"))?;
        Ok(PeerStores {
            rows: rows
                .into_iter()
                .map(|row| ((row.voter, row.incarnation), (row.store, None)))
                .collect(),
            unsettled: VecDeque::new(),
            barriers: BarrierAllocator::new(incarnation, boot).for_peer_stores(),
            rejections: Vec::new(),
            counts: PeerStoreCounts::default(),
        })
    }

    /// The store recorded for `voter` under `incarnation`, durable or not.
    pub fn recorded(
        &self,
        voter: &ReplicaId,
        incarnation: ReplicaIncarnation,
    ) -> Option<StoreInstanceId> {
        self.rows
            .get(&(*voter, incarnation))
            .map(|(store, _)| *store)
    }

    /// Check a promise or report page from `voter`, under `incarnation`,
    /// that names `store`.
    pub fn check(
        &mut self,
        voter: ReplicaId,
        incarnation: ReplicaIncarnation,
        store: StoreInstanceId,
    ) -> Result<Check, String> {
        let key = (voter, incarnation);
        match self.rows.get(&key) {
            Some((held, None)) if *held == store => {
                self.counts.matched += 1;
                Ok(Check::Pass)
            }
            Some((held, Some(barrier))) if *held == store => {
                self.counts.held += 1;
                Ok(Check::Wait(*barrier))
            }
            Some((held, _)) => {
                let recorded = *held;
                self.counts.refused += 1;
                self.rejections.push(format!(
                    "a promise or report from voter {voter:?} (incarnation {}) names store \
                     {store:?}, not the store {recorded:?} recorded for it: refused, as a voter \
                     wiped and initialized again under its identity (task-d121)",
                    incarnation.get()
                ));
                Ok(Check::Refused { recorded })
            }
            None => {
                let row = PeerStoreV1 {
                    voter,
                    incarnation,
                    store,
                };
                let update = row
                    .update()
                    .map_err(|e| format!("the peer store row: {e:?}"))?;
                let barrier = self.barriers.allocate();
                self.rows.insert(key, (store, Some(barrier)));
                self.unsettled.push_back(Batch { barrier, key });
                self.counts.recorded += 1;
                Ok(Check::Record {
                    barrier,
                    updates: vec![update],
                })
            }
        }
    }

    /// A store an installed Sync from `leader` counted for `voter` under
    /// `incarnation`: the row to journal under its barrier if there is
    /// none, and nothing otherwise. Nothing waits on that batch.
    pub fn learn(
        &mut self,
        leader: ReplicaId,
        voter: ReplicaId,
        incarnation: ReplicaIncarnation,
        store: StoreInstanceId,
    ) -> Result<Option<(BarrierId, Vec<StoreUpdate>)>, String> {
        let key = (voter, incarnation);
        match self.rows.get(&key) {
            Some((held, _)) if *held == store => Ok(None),
            Some((held, _)) => {
                let recorded = *held;
                self.counts.disagreed += 1;
                self.rejections.push(format!(
                    "a Sync from leader {leader:?} counted store {store:?} for voter {voter:?} \
                     (incarnation {}), not the store {recorded:?} recorded for it: installed, \
                     and the row kept (task-d121)",
                    incarnation.get()
                ));
                Ok(None)
            }
            None => {
                let row = PeerStoreV1 {
                    voter,
                    incarnation,
                    store,
                };
                let update = row
                    .update()
                    .map_err(|e| format!("the peer store row: {e:?}"))?;
                let barrier = self.barriers.allocate();
                self.rows.insert(key, (store, Some(barrier)));
                self.unsettled.push_back(Batch { barrier, key });
                self.counts.learned += 1;
                Ok(Some((barrier, vec![update])))
            }
        }
    }

    /// Take what became of the row batches, in order. A failed batch's row
    /// is forgotten, so the peer's next message is recorded again.
    pub fn settle(
        &mut self,
        is_durable: impl Fn(&BarrierId) -> bool,
        is_failed: impl Fn(&BarrierId) -> bool,
    ) -> Settled {
        let mut settled = Settled::default();
        while let Some(batch) = self.unsettled.front() {
            let barrier = batch.barrier;
            let key = batch.key;
            if is_failed(&barrier) {
                self.unsettled.pop_front();
                self.counts.failed += 1;
                if matches!(self.rows.get(&key), Some((_, Some(b))) if *b == barrier) {
                    self.rows.remove(&key);
                }
                settled.failed.push(barrier);
                continue;
            }
            if !is_durable(&barrier) {
                break;
            }
            self.unsettled.pop_front();
            if let Some((_, in_flight)) = self.rows.get_mut(&key)
                && *in_flight == Some(barrier)
            {
                *in_flight = None;
            }
            settled.durable.push(barrier);
        }
        settled
    }

    /// Whether a row batch is still waiting on the journal.
    pub fn unsettled(&self) -> bool {
        !self.unsettled.is_empty()
    }

    /// The refusals since the last call, rendered for the node's log.
    pub fn take_rejections(&mut self) -> Vec<String> {
        core::mem::take(&mut self.rejections)
    }
}
