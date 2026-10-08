//! The actor-owned logical outbox (design Section 4.8).
//!
//! Immutable eligible evidence is *published* logically here and only
//! *transmitted* once every durable prerequisite holds. Releasing is gated
//! on four independent facts, each holding for every send released:
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
///
/// A held send is filed under the one fact it still waits for, so a
/// completion visits the sends it can release and no others (task-d69):
/// the newest required barrier not yet durable, or, once every barrier
/// is, the journal sequence it names. The ballot and failure gates are
/// checked where they can change: every held send when the ballot moves
/// or a barrier fails, and otherwise only the sends published since the
/// last release.
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
    /// Every send still held, by publication order.
    held: BTreeMap<u64, Held>,
    /// The order the next published send takes.
    next: u64,
    /// Held sends waiting on a barrier, by that barrier.
    on_barrier: BTreeSet<(BarrierId, u64)>,
    /// Held sends whose barriers are durable, waiting on the cut.
    on_cut: BTreeSet<(LocalJournalSeq, u64)>,
    /// Held sends whose prerequisites hold, released at the next release.
    ready: BTreeSet<u64>,
    /// Sends published since the last release, not yet checked against
    /// its ballot or the failed barriers.
    fresh: Vec<u64>,
    /// The ballot of the last release: every send held across it was
    /// checked against it.
    ballot: Option<Ballot>,
    /// A barrier failed since the last release.
    newly_failed: bool,
    /// Held sends by destination.
    held_to: Vec<(PeerId, usize)>,
    dropped: Vec<(PendingSend, ReleaseError)>,
    /// Held sends looked at by a completion or a release.
    #[cfg(test)]
    visits: u64,
}

/// A held send and where it is filed.
#[derive(Debug)]
struct Held {
    send: PendingSend,
    filed: Filed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Filed {
    Barrier(BarrierId),
    Cut(LocalJournalSeq),
    Ready,
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
            held: BTreeMap::new(),
            next: 0,
            on_barrier: BTreeSet::new(),
            on_cut: BTreeSet::new(),
            ready: BTreeSet::new(),
            fresh: Vec::new(),
            ballot: None,
            newly_failed: false,
            held_to: Vec::new(),
            dropped: Vec::new(),
            #[cfg(test)]
            visits: 0,
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
        let order = self.next;
        self.next += 1;
        match self.held_to.iter_mut().find(|(to, _)| *to == send.to) {
            Some((_, count)) => *count += 1,
            None => self.held_to.push((send.to, 1)),
        }
        let filed = self.filing(&send);
        self.file(order, filed);
        self.held.insert(order, Held { send, filed });
        self.fresh.push(order);
    }

    /// Where a send waits: on its newest barrier not yet durable, then on
    /// the cut. Barriers become durable in order, so a send filed under
    /// its newest one finds the rest durable when that one is.
    fn filing(&self, send: &PendingSend) -> Filed {
        if let Some(barrier) = send
            .requires
            .iter()
            .rev()
            .find(|b| !self.durable.contains(b))
        {
            return Filed::Barrier(*barrier);
        }
        if send.context.required_journal_seq > self.durable_through {
            return Filed::Cut(send.context.required_journal_seq);
        }
        Filed::Ready
    }

    fn file(&mut self, order: u64, filed: Filed) {
        match filed {
            Filed::Barrier(barrier) => self.on_barrier.insert((barrier, order)),
            Filed::Cut(seq) => self.on_cut.insert((seq, order)),
            Filed::Ready => self.ready.insert(order),
        };
    }

    fn unfile(&mut self, order: u64, filed: Filed) {
        match filed {
            Filed::Barrier(barrier) => self.on_barrier.remove(&(barrier, order)),
            Filed::Cut(seq) => self.on_cut.remove(&(seq, order)),
            Filed::Ready => self.ready.remove(&order),
        };
    }

    /// File again the held sends in `orders`, whose filing just became
    /// true.
    fn refile(&mut self, orders: Vec<u64>) {
        for order in orders {
            #[cfg(test)]
            {
                self.visits += 1;
            }
            let Some(held) = self.held.get(&order) else {
                continue;
            };
            let filed = self.filing(&held.send);
            self.file(order, filed);
            if let Some(held) = self.held.get_mut(&order) {
                held.filed = filed;
            }
        }
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
                let inserted = self.durable.insert(barrier);
                if inserted {
                    let waiting: Vec<u64> = self
                        .on_barrier
                        .range((barrier, 0)..=(barrier, u64::MAX))
                        .map(|(_, order)| *order)
                        .collect();
                    for order in &waiting {
                        self.on_barrier.remove(&(barrier, *order));
                    }
                    self.refile(waiting);
                }
                if advanced {
                    let mut reached = Vec::new();
                    while let Some(&(seq, order)) = self.on_cut.first() {
                        if seq > self.durable_through {
                            break;
                        }
                        self.on_cut.pop_first();
                        reached.push(order);
                    }
                    self.refile(reached);
                }
                inserted || advanced
            }
            StorageEvent::Materialized { .. } => false,
            StorageEvent::Failed { error, .. } => {
                if self.failed.contains_key(&barrier) {
                    return false;
                }
                self.failed.insert(barrier, *error);
                self.newly_failed = true;
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

    /// Why `send` is dropped at a release under `current_ballot`, if it is.
    fn refusal(&self, send: &PendingSend, current_ballot: &Ballot) -> Option<ReleaseError> {
        let obsolete = match send.context.ballot.compare_same_epoch(current_ballot) {
            Some(core::cmp::Ordering::Less) => true,
            Some(_) => false,
            None => send.context.ballot.epoch < current_ballot.epoch,
        };
        if obsolete {
            return Some(ReleaseError::ObsoleteBallot);
        }
        if self.failed.is_empty() {
            return None;
        }
        send.requires
            .iter()
            .find_map(|b| self.failed.get(b))
            .map(|err| ReleaseError::BarrierFailed(*err))
    }

    /// Release every send whose prerequisites hold under the current ballot.
    /// Sends with a failed barrier or an obsolete ballot are dropped and
    /// reported through [`Outbox::take_dropped`].
    ///
    /// What is released, what is dropped and why, and the order of both
    /// (publication order) are those of checking every held send; a debug
    /// build checks that on every call.
    pub fn release(&mut self, current_ballot: &Ballot) -> Vec<Effect> {
        #[cfg(debug_assertions)]
        let expected = self.walk(current_ballot);
        // Whether a send is refused depends on the ballot and on the
        // failed barriers alone, and a send held across a release was
        // checked against both: only a new ballot or a new failure makes
        // it worth checking again.
        let sweep = self.ballot != Some(*current_ballot) || self.newly_failed;
        let fresh = core::mem::take(&mut self.fresh);
        let candidates: Vec<u64> = if sweep {
            self.held.keys().copied().collect()
        } else {
            fresh
        };
        self.ballot = Some(*current_ballot);
        self.newly_failed = false;
        #[cfg(debug_assertions)]
        let mut refused = Vec::new();
        for order in candidates {
            #[cfg(test)]
            {
                self.visits += 1;
            }
            let Some(held) = self.held.get(&order) else {
                continue;
            };
            if let Some(reason) = self.refusal(&held.send, current_ballot) {
                let send = self.take(order);
                #[cfg(debug_assertions)]
                refused.push((order, reason));
                self.dropped.push((send, reason));
            }
        }
        let ready = core::mem::take(&mut self.ready);
        #[cfg(debug_assertions)]
        let released_orders: Vec<u64> = ready.iter().copied().collect();
        let released = ready
            .into_iter()
            .map(|order| {
                let send = self.take(order);
                Effect::SendWhenDurable {
                    context: send.context,
                    requires: send.requires,
                    to: send.to,
                    frame: send.frame,
                }
            })
            .collect();
        #[cfg(debug_assertions)]
        assert_eq!(
            (released_orders, refused),
            expected,
            "the indexed release differs from the full walk"
        );
        released
    }

    /// Remove a held send from the outbox and from where it is filed.
    fn take(&mut self, order: u64) -> PendingSend {
        let held = self.held.remove(&order).expect("a held send");
        self.unfile(order, held.filed);
        if let Some(index) = self.held_to.iter().position(|(to, _)| *to == held.send.to) {
            self.held_to[index].1 -= 1;
            if self.held_to[index].1 == 0 {
                self.held_to.swap_remove(index);
            }
        }
        held.send
    }

    /// What checking every held send in order releases and refuses: the
    /// rule the index keeps, checked against it in debug builds.
    #[cfg(debug_assertions)]
    fn walk(&self, current_ballot: &Ballot) -> (Vec<u64>, Vec<(u64, ReleaseError)>) {
        let mut released = Vec::new();
        let mut refused = Vec::new();
        for (order, held) in &self.held {
            let send = &held.send;
            if let Some(reason) = self.refusal(send, current_ballot) {
                refused.push((*order, reason));
                continue;
            }
            // Newest first, as the walk before the index did: a send
            // still waiting costs one lookup.
            let barriers_durable = send.requires.iter().rev().all(|b| self.durable.contains(b));
            let cut_durable = send.context.required_journal_seq <= self.durable_through;
            if barriers_durable && cut_durable {
                released.push(*order);
            }
        }
        (released, refused)
    }

    /// Number of sends still waiting.
    pub fn held(&self) -> usize {
        self.held.len()
    }

    /// Number of sends to `to` still waiting.
    pub fn held_to(&self, to: &PeerId) -> usize {
        self.held_to
            .iter()
            .find(|(peer, _)| peer == to)
            .map_or(0, |(_, count)| *count)
    }

    /// Sends still waiting, in publication order.
    pub fn pending(&self) -> impl Iterator<Item = &PendingSend> {
        self.held.values().map(|held| &held.send)
    }

    /// Take the sends dropped since the last call, with reasons.
    pub fn take_dropped(&mut self) -> Vec<(PendingSend, ReleaseError)> {
        core::mem::take(&mut self.dropped)
    }
}

/// The first sequence of the application's barriers.
///
/// A replica's protocol machine and its applier each allocate barriers
/// for the same boot and hand them to the same store, and a barrier is
/// told apart from another only by its sequence. Both counting from one
/// made them the same barrier: the applier took a protocol batch's
/// `Materialized` as its own command's, and reported the command applied
/// while its own batch was still queued. So the sequences are split in
/// two, and the upper half is the applier's.
pub const APPLICATION_BARRIERS: u64 = 1 << 63;

/// The first sequence of the runtime's own barriers: batches a replica's
/// runtime persists for itself, beside its protocol machine and its
/// applier, such as a forgetting floor's readiness (task-d27). Below
/// [`APPLICATION_BARRIERS`], so the applier hands their facts on.
pub const RUNTIME_BARRIERS: u64 = 1 << 62;

/// Whether `barrier` was allocated by an applier ([`BarrierAllocator::for_application`]).
pub const fn is_application(barrier: &BarrierId) -> bool {
    barrier.sequence >= APPLICATION_BARRIERS
}

/// Allocator of per-boot barrier sequences.
#[derive(Debug)]
pub struct BarrierAllocator {
    node_generation: coord_types::ids::ReplicaIncarnation,
    boot_id: BootId,
    next: u64,
    /// The first sequence this allocator may not issue.
    end: u64,
}

impl BarrierAllocator {
    /// New allocator for this boot, below [`RUNTIME_BARRIERS`].
    pub const fn new(
        node_generation: coord_types::ids::ReplicaIncarnation,
        boot_id: BootId,
    ) -> Self {
        BarrierAllocator {
            node_generation,
            boot_id,
            next: 1,
            end: RUNTIME_BARRIERS,
        }
    }

    /// The same allocator moved to the runtime's own sequences, between
    /// the protocol's and the application's.
    #[must_use]
    pub const fn for_runtime(mut self) -> Self {
        if self.next < RUNTIME_BARRIERS {
            self.next = RUNTIME_BARRIERS;
        }
        self.end = APPLICATION_BARRIERS;
        self
    }

    /// The same allocator moved to the application's half of the
    /// sequences, which no protocol allocator of the boot reaches.
    #[must_use]
    pub const fn for_application(mut self) -> Self {
        if self.next < APPLICATION_BARRIERS {
            self.next = APPLICATION_BARRIERS;
        }
        self.end = u64::MAX;
        self
    }

    /// Next barrier; sequences never repeat within a boot.
    pub fn allocate(&mut self) -> BarrierId {
        let sequence = self.next;
        assert!(sequence < self.end, "barrier sequence exhausted");
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use coord_types::ids::{ConfigurationEpoch, DomainId, ReplicaId, ReplicaIncarnation};

    const BOOT: BootId = BootId([7; 16]);

    fn ballot(number: u64) -> Ballot {
        Ballot {
            epoch: ConfigurationEpoch::new(1).unwrap(),
            number,
            leader: ReplicaId([1; 16]),
        }
    }

    fn send(requires: Vec<BarrierId>) -> PendingSend {
        PendingSend {
            context: EffectContext {
                domain: DomainId([2; 16]),
                replica_incarnation: ReplicaIncarnation::new(1).unwrap(),
                boot_id: BOOT,
                configuration: ConfigurationEpoch::new(1).unwrap(),
                ballot: ballot(1),
                required_journal_seq: LocalJournalSeq::ZERO,
            },
            requires,
            to: PeerId {
                replica: ReplicaId([3; 16]),
                incarnation: ReplicaIncarnation::new(1).unwrap(),
            },
            frame: Vec::new(),
        }
    }

    fn durable(barrier: BarrierId, seq: u64) -> StorageEvent {
        StorageEvent::JournalDurable {
            barrier_id: barrier,
            journal_seq: LocalJournalSeq::new(seq).unwrap(),
        }
    }

    #[test]
    fn a_completion_visits_only_the_sends_it_releases() {
        let mut barriers = BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), BOOT);
        let near = barriers.allocate();
        let far = barriers.allocate();
        let mut outbox = Outbox::new(BOOT);
        for _ in 0..5_000 {
            outbox.publish(send(vec![near, far]));
        }
        outbox.publish(send(vec![near]));
        // The first release checks every send against its ballot.
        assert!(outbox.release(&ballot(1)).is_empty());
        assert_eq!(outbox.held(), 5_001);

        outbox.visits = 0;
        assert!(outbox.observe(&durable(near, 1)));
        let released = outbox.release(&ballot(1));
        assert_eq!(released.len(), 1);
        assert_eq!(outbox.visits, 1, "one completion, one send looked at");
        assert_eq!(outbox.held(), 5_000);

        // Nothing new: a release looks at nothing.
        outbox.visits = 0;
        assert!(outbox.release(&ballot(1)).is_empty());
        assert_eq!(outbox.visits, 0);

        // The far barrier releases the rest, in publication order.
        assert!(outbox.observe(&durable(far, 2)));
        assert_eq!(outbox.release(&ballot(1)).len(), 5_000);
        assert_eq!(outbox.held(), 0);
        assert_eq!(outbox.held_to(&send(vec![]).to), 0);
    }

    #[test]
    fn a_new_ballot_or_failure_checks_every_held_send() {
        let mut barriers = BarrierAllocator::new(ReplicaIncarnation::new(1).unwrap(), BOOT);
        let (a, b) = (barriers.allocate(), barriers.allocate());
        let mut outbox = Outbox::new(BOOT);
        outbox.publish(send(vec![a]));
        outbox.publish(send(vec![a, b]));
        assert!(outbox.release(&ballot(1)).is_empty());
        // A failure of the older barrier drops the send filed under the
        // newer one, by the first failed barrier it requires.
        assert!(outbox.observe(&StorageEvent::Failed {
            barrier_id: a,
            error: StorageError::DefinitelyNotCommitted,
        }));
        assert!(outbox.release(&ballot(1)).is_empty());
        let dropped = outbox.take_dropped();
        assert_eq!(dropped.len(), 2);
        assert!(dropped.iter().all(|(_, reason)| *reason
            == ReleaseError::BarrierFailed(StorageError::DefinitelyNotCommitted)));
        // A send that is ready is still dropped when the ballot moved past it.
        outbox.publish(send(vec![]));
        assert!(outbox.release(&ballot(2)).is_empty());
        assert_eq!(outbox.take_dropped()[0].1, ReleaseError::ObsoleteBallot);
        assert_eq!(outbox.held(), 0);
    }
}
