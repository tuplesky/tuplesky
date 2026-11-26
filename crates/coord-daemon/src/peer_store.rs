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
//! A candidate's `NewLeader` names its store too (C4). It is checked
//! against the row and records none: a voter that recorded another store
//! for the candidate refuses to promise, and one with no row promises as
//! before. A candidate wiped and initialized again under its identity
//! counts its own empty report as the voter it was, and no check on what
//! it receives can see that. So the check covers a candidate whose store
//! reached a voter the campaign needs, through the candidate's own
//! promises and through the Syncs that counted them.
//!
//! A Sync names the store of each voter its leader counted. A voter that
//! installs it records each one it has no row for, and never overwrites
//! one: a counted store that disagrees with a row is logged and counted,
//! and the Sync is installed all the same, since refusing it would stall
//! the domain over a record this voter can't arbitrate. The one
//! exception is the leader's own store (C4): a Sync whose leader this
//! voter recorded under another store is refused before the machine
//! sees it, as that leader's `NewLeader` is.
//!
//! A row is read from the store by its key the first time its peer and
//! incarnation come up after a boot, and kept. No history is read whole,
//! so however many incarnations a peer has had, a node reopens every set
//! of rows it wrote (from Astra's review of C1).
//!
//! The messages waiting on a row are held in [`HeldFrames`], which
//! bounds them: a peer that retries while its row is in the journal
//! leaves one copy of each frame, and past the bounds a frame is dropped,
//! as a lost one would be. The candidate asks for a promise again, and
//! the report's pages are asked for again, once the row is durable.
//!
//! The rows are the runtime's, like a floor's readiness (task-d27) and a
//! feature's support (task-d99): the machines do no I/O and keep no row
//! per peer.

use std::collections::{BTreeMap, VecDeque};

use coord_checkpoint::peer_store::PeerStoreV1;
use coord_core::effect::{BarrierId, BootId, StoreUpdate};
use coord_core::event::PeerProvenance;
use coord_core::outbox::BarrierAllocator;
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
    /// A candidate's `NewLeader` from another store than the one
    /// recorded, refused (C4).
    pub refused_candidates: u64,
    /// A Sync whose leader's own counted store is not the one recorded
    /// for it, refused (C4).
    pub refused_syncs: u64,
    /// A row's batch that failed: its held messages are dropped, and the
    /// peer's next message is recorded again.
    pub failed: u64,
    /// A store learned from an installed Sync, for a peer with no row.
    pub learned: u64,
    /// A store an installed Sync counted that disagrees with the row.
    pub disagreed: u64,
    /// A message held already, byte for byte, and not held twice.
    pub duplicates: u64,
    /// A message dropped because the held messages were at a bound.
    pub shed: u64,
}

/// What names a leader's store, checked without recording it (C4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Named {
    /// A candidate's `NewLeader`.
    Candidate,
    /// A Sync, counting its leader's own store.
    SyncLeader,
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
    /// Every row read or journaled since boot, and the batch it is in
    /// while that is not yet durable.
    rows: BTreeMap<(ReplicaId, ReplicaIncarnation), (StoreInstanceId, Option<BarrierId>)>,
    unsettled: VecDeque<Batch>,
    barriers: BarrierAllocator,
    /// Refusals since the last [`PeerStores::take_rejections`].
    rejections: Vec<String>,
    /// Since boot.
    pub counts: PeerStoreCounts,
}

impl PeerStores {
    /// A voter's check. It reads no row now: each is read by key when its
    /// peer and incarnation first come up.
    pub fn new(incarnation: ReplicaIncarnation, boot: BootId) -> Self {
        PeerStores {
            rows: BTreeMap::new(),
            unsettled: VecDeque::new(),
            barriers: BarrierAllocator::new(incarnation, boot).for_peer_stores(),
            rejections: Vec::new(),
            counts: PeerStoreCounts::default(),
        }
    }

    /// The store recorded for `voter` under `incarnation` that this voter
    /// read or journaled since boot, durable or not.
    pub fn recorded(
        &self,
        voter: &ReplicaId,
        incarnation: ReplicaIncarnation,
    ) -> Option<StoreInstanceId> {
        self.rows
            .get(&(*voter, incarnation))
            .map(|(store, _)| *store)
    }

    /// The row for `key`: the one kept since boot, or else the durable
    /// one `durable` reads, kept from now on.
    fn row(
        &mut self,
        key: (ReplicaId, ReplicaIncarnation),
        durable: impl FnOnce() -> Result<Option<StoreInstanceId>, String>,
    ) -> Result<Option<(StoreInstanceId, Option<BarrierId>)>, String> {
        if let Some(row) = self.rows.get(&key) {
            return Ok(Some(*row));
        }
        Ok(durable()?.map(|store| {
            self.rows.insert(key, (store, None));
            (store, None)
        }))
    }

    /// Journal a row recording `store` for `key`.
    fn journal(
        &mut self,
        key: (ReplicaId, ReplicaIncarnation),
        store: StoreInstanceId,
    ) -> Result<(BarrierId, Vec<StoreUpdate>), String> {
        let row = PeerStoreV1 {
            voter: key.0,
            incarnation: key.1,
            store,
        };
        let update = row
            .update()
            .map_err(|e| format!("the peer store row: {e:?}"))?;
        let barrier = self.barriers.allocate();
        self.rows.insert(key, (store, Some(barrier)));
        self.unsettled.push_back(Batch { barrier, key });
        Ok((barrier, vec![update]))
    }

    /// Check a promise or report page from `voter`, under `incarnation`,
    /// that names `store`. `durable` reads the row from the store if this
    /// voter has not read or written it since boot.
    pub fn check(
        &mut self,
        voter: ReplicaId,
        incarnation: ReplicaIncarnation,
        store: StoreInstanceId,
        durable: impl FnOnce() -> Result<Option<StoreInstanceId>, String>,
    ) -> Result<Check, String> {
        let key = (voter, incarnation);
        match self.row(key, durable)? {
            Some((held, None)) if held == store => {
                self.counts.matched += 1;
                Ok(Check::Pass)
            }
            Some((held, Some(barrier))) if held == store => {
                self.counts.held += 1;
                Ok(Check::Wait(barrier))
            }
            Some((recorded, _)) => {
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
                let (barrier, updates) = self.journal(key, store)?;
                self.counts.recorded += 1;
                Ok(Check::Record { barrier, updates })
            }
        }
    }

    /// Check the store a candidate's `NewLeader`, or a Sync counting its
    /// own leader, names for `leader` under `incarnation` (C4), recording
    /// nothing. `Some(recorded)` where this voter recorded another store
    /// for it, durable or not: the frame is refused, logged and counted.
    /// Without a row it goes on, and a Sync's counted set is learned once
    /// it is installed, as before.
    pub fn check_leader(
        &mut self,
        leader: ReplicaId,
        incarnation: ReplicaIncarnation,
        store: StoreInstanceId,
        named: Named,
        durable: impl FnOnce() -> Result<Option<StoreInstanceId>, String>,
    ) -> Result<Option<StoreInstanceId>, String> {
        match self.row((leader, incarnation), durable)? {
            Some((recorded, _)) if recorded != store => {
                let what = match named {
                    Named::Candidate => {
                        self.counts.refused_candidates += 1;
                        "a NewLeader from candidate"
                    }
                    Named::SyncLeader => {
                        self.counts.refused_syncs += 1;
                        "a Sync counting the store of its leader"
                    }
                };
                self.rejections.push(format!(
                    "{what} {leader:?} (incarnation {}) names store {store:?}, not the store \
                     {recorded:?} recorded for it: refused, as a candidate wiped and \
                     initialized again under its identity (task-d121)",
                    incarnation.get()
                ));
                Ok(Some(recorded))
            }
            _ => Ok(None),
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
        durable: impl FnOnce() -> Result<Option<StoreInstanceId>, String>,
    ) -> Result<Option<(BarrierId, Vec<StoreUpdate>)>, String> {
        let key = (voter, incarnation);
        match self.row(key, durable)? {
            Some((held, _)) if held == store => Ok(None),
            Some((recorded, _)) => {
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
                let learned = self.journal(key, store)?;
                self.counts.learned += 1;
                Ok(Some(learned))
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

    /// Count what [`HeldFrames::hold`] did with a frame it did not hold.
    pub fn count_unheld(&mut self, held: Held) {
        match held {
            Held::Held => {}
            Held::Duplicate => self.counts.duplicates += 1,
            Held::Shed => self.counts.shed += 1,
        }
    }

    /// The refusals since the last call, rendered for the node's log.
    pub fn take_rejections(&mut self) -> Vec<String> {
        core::mem::take(&mut self.rejections)
    }
}

/// How many messages, and how many bytes, may wait on peer store rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldLimits {
    /// Frames from one peer and incarnation.
    pub peer_frames: usize,
    /// Bytes from one peer and incarnation.
    pub peer_bytes: usize,
    /// Bytes across every peer.
    pub bytes: usize,
}

impl Default for HeldLimits {
    /// A report's pages from one peer fit with room to spare; a promise
    /// is small. Retries of one frame are held once whatever the bounds.
    fn default() -> Self {
        HeldLimits {
            peer_frames: 64,
            peer_bytes: 4 << 20,
            bytes: 16 << 20,
        }
    }
}

/// What [`HeldFrames::hold`] did with a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Held {
    /// Held until its row is durable.
    Held,
    /// The same frame from the same peer is held already: dropped.
    Duplicate,
    /// At a bound: dropped, as a lost frame is.
    Shed,
}

/// The promises and report pages waiting on their peer's store row, in
/// the order they came, within [`HeldLimits`].
#[derive(Debug, Default)]
pub struct HeldFrames {
    frames: Vec<(BarrierId, PeerProvenance, Vec<u8>)>,
    bytes: usize,
    limits: HeldLimits,
}

impl HeldFrames {
    /// Nothing held, within `limits`.
    pub fn new(limits: HeldLimits) -> Self {
        HeldFrames {
            frames: Vec::new(),
            bytes: 0,
            limits,
        }
    }

    /// Hold `frame` from `provenance` until `barrier` is durable, unless
    /// the same frame from the same peer is held already, or the bounds
    /// are reached.
    pub fn hold(&mut self, barrier: BarrierId, provenance: PeerProvenance, frame: Vec<u8>) -> Held {
        let peer = (provenance.from(), provenance.incarnation());
        let mut peer_frames = 0;
        let mut peer_bytes = 0;
        for (_, held, bytes) in &self.frames {
            if (held.from(), held.incarnation()) != peer {
                continue;
            }
            if *bytes == frame {
                return Held::Duplicate;
            }
            peer_frames += 1;
            peer_bytes += bytes.len();
        }
        if peer_frames >= self.limits.peer_frames
            || peer_bytes + frame.len() > self.limits.peer_bytes
            || self.bytes + frame.len() > self.limits.bytes
        {
            return Held::Shed;
        }
        self.bytes += frame.len();
        self.frames.push((barrier, provenance, frame));
        Held::Held
    }

    /// Drop the frames held behind a failed batch, and take those whose
    /// batch is durable, in the order they came.
    pub fn release(&mut self, settled: &Settled) -> Vec<(PeerProvenance, Vec<u8>)> {
        let mut ready = Vec::new();
        let mut kept = Vec::with_capacity(self.frames.len());
        for (barrier, provenance, frame) in core::mem::take(&mut self.frames) {
            if settled.failed.contains(&barrier) {
                self.bytes -= frame.len();
            } else if settled.durable.contains(&barrier) {
                self.bytes -= frame.len();
                ready.push((provenance, frame));
            } else {
                kept.push((barrier, provenance, frame));
            }
        }
        self.frames = kept;
        ready
    }

    /// How many frames are held.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// How many bytes are held.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn barrier(n: u64) -> BarrierId {
        let mut alloc = BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), BootId([1; 16]))
            .for_peer_stores();
        let mut b = alloc.allocate();
        for _ in 0..n {
            b = alloc.allocate();
        }
        b
    }

    fn from(peer: u8) -> PeerProvenance {
        PeerProvenance::from_local_voter(ReplicaId([peer; 16]), ReplicaIncarnation::new(1).unwrap())
    }

    /// A retry of a frame already held is held once (from Astra's review
    /// of C1: three copies of one page were held).
    #[test]
    fn a_retried_frame_is_held_once() {
        let mut held = HeldFrames::new(HeldLimits::default());
        for _ in 0..3 {
            let _ = held.hold(barrier(0), from(2), vec![7; 100]);
        }
        assert_eq!(held.len(), 1);
        assert_eq!(held.bytes(), 100);
        assert_eq!(
            held.hold(barrier(0), from(2), vec![7; 100]),
            Held::Duplicate
        );
        // The same bytes from another peer are that peer's frame.
        assert_eq!(held.hold(barrier(1), from(3), vec![7; 100]), Held::Held);
    }

    /// Frames past a peer's bounds, or the voter's, are shed; what was
    /// held is released, in order, once its batch is durable, and dropped
    /// if it failed.
    #[test]
    fn frames_past_the_bounds_are_shed() {
        let limits = HeldLimits {
            peer_frames: 2,
            peer_bytes: 250,
            bytes: 400,
        };
        let mut held = HeldFrames::new(limits);
        assert_eq!(held.hold(barrier(0), from(2), vec![1; 100]), Held::Held);
        assert_eq!(held.hold(barrier(0), from(2), vec![2; 100]), Held::Held);
        assert_eq!(
            held.hold(barrier(0), from(2), vec![3; 10]),
            Held::Shed,
            "frames"
        );
        assert_eq!(held.hold(barrier(1), from(3), vec![4; 200]), Held::Held);
        assert_eq!(
            held.hold(barrier(1), from(3), vec![5; 60]),
            Held::Shed,
            "bytes of one peer"
        );
        assert_eq!(
            held.hold(barrier(2), from(4), vec![6; 120]),
            Held::Shed,
            "bytes of all"
        );
        assert_eq!(held.bytes(), 400);
        let ready = held.release(&Settled {
            durable: vec![barrier(0)],
            failed: vec![barrier(1)],
        });
        assert_eq!(
            ready.iter().map(|(_, f)| f[0]).collect::<Vec<_>>(),
            vec![1, 2],
            "in the order they came"
        );
        assert!(held.is_empty());
        assert_eq!(held.bytes(), 0);
        assert_eq!(held.hold(barrier(3), from(4), vec![6; 120]), Held::Held);
    }

    /// A row is read by key when its peer first comes up, and kept: the
    /// check never reads more than the rows it is asked about.
    #[test]
    fn a_row_is_read_by_key_once() {
        let mut stores = PeerStores::new(ReplicaIncarnation::new(1).unwrap(), BootId([1; 16]));
        let peer = ReplicaId([2; 16]);
        let inc = ReplicaIncarnation::new(9).unwrap();
        let store = StoreInstanceId([5; 16]);
        let mut reads = 0;
        let check = stores
            .check(peer, inc, store, || {
                reads += 1;
                Ok(Some(store))
            })
            .unwrap();
        assert_eq!(check, Check::Pass);
        let check = stores
            .check(peer, inc, StoreInstanceId([6; 16]), || {
                reads += 1;
                Ok(Some(store))
            })
            .unwrap();
        assert!(matches!(check, Check::Refused { recorded } if recorded == store));
        assert_eq!(reads, 1, "read once, then kept");
    }
}
