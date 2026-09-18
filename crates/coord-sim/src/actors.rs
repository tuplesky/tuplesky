//! Reference actors for the simulator's own tests.
//!
//! [`DurableEcho`] persists each admitted request and acknowledges it only
//! through a `SendWhenDurable` effect that requires the batch's barrier.
//! [`EagerEcho`] is the deliberately faulty variant: it acknowledges with no
//! barrier requirement, so an acknowledgement can reach the client before
//! (or without) the value ever becoming durable. The [`crate::oracle`]
//! catches exactly that after a crash.

use coord_core::effect::{
    BarrierId, BootId, CollectionId, Effect, EffectContext, PeerId, PersistBatch, StoreUpdate,
};
use coord_core::event::{Event, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{BarrierAllocator, Outbox, PendingSend};
use coord_types::ids::{
    Ballot, ConfigurationEpoch, DomainId, LocalJournalSeq, ReplicaId, ReplicaIncarnation,
};

/// Collection used by the echo actors.
pub const ECHO_COLLECTION: CollectionId = CollectionId(0x0007);
/// The external client every acknowledgement goes to.
pub const CLIENT: PeerId = PeerId {
    replica: ReplicaId([0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

/// Factory producing a fresh machine for a (re)booted node.
pub type MachineFactory =
    Box<dyn Fn(ReplicaId) -> Box<dyn DeterministicMachine<Event = Event, Effect = Effect>>>;

fn ballot(replica: ReplicaId) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::ZERO,
        number: 0,
        leader: replica,
    }
}

/// Shared echo state.
#[derive(Debug)]
struct EchoState {
    replica: ReplicaId,
    boot: Option<BootId>,
    incarnation: ReplicaIncarnation,
    alloc: Option<BarrierAllocator>,
    outbox: Option<Outbox>,
    /// Requests recovered from the durable image at boot.
    recovered: usize,
}

impl EchoState {
    fn new(replica: ReplicaId) -> Self {
        EchoState {
            replica,
            boot: None,
            incarnation: ReplicaIncarnation::ZERO,
            alloc: None,
            outbox: None,
            recovered: 0,
        }
    }

    fn context(&self, seq: u64) -> EffectContext {
        EffectContext {
            domain: DomainId([1; 16]),
            replica_incarnation: self.incarnation,
            boot_id: self.boot.expect("booted"),
            configuration: ConfigurationEpoch::ZERO,
            ballot: ballot(self.replica),
            required_journal_seq: LocalJournalSeq::new(seq).expect("bounded"),
        }
    }

    fn boot(&mut self, boot_id: BootId, incarnation: ReplicaIncarnation) {
        self.boot = Some(boot_id);
        self.incarnation = incarnation;
        self.alloc = Some(BarrierAllocator::new(incarnation, boot_id));
        self.outbox = Some(Outbox::new(boot_id));
    }

    fn persist(&mut self, frame: &[u8]) -> (BarrierId, Effect) {
        let barrier = self.alloc.as_mut().expect("booted").allocate();
        let batch = PersistBatch {
            barrier,
            base: None,
            updates: vec![StoreUpdate {
                collection: ECHO_COLLECTION,
                key: frame.to_vec(),
                value: Some(frame.to_vec()),
            }],
        };
        (barrier, Effect::Persist(batch))
    }
}

/// Correct actor: acknowledge only after the barrier is durable.
pub struct DurableEcho(EchoState);

impl DurableEcho {
    /// New actor for `replica`.
    pub fn new(replica: ReplicaId) -> Self {
        DurableEcho(EchoState::new(replica))
    }
}

impl DeterministicMachine for DurableEcho {
    type Event = Event;
    type Effect = Effect;

    fn step(&mut self, event: Event) -> Vec<Effect> {
        let s = &mut self.0;
        match event {
            Event::Boot {
                boot_id,
                incarnation,
            } => {
                s.boot(boot_id, incarnation);
                Vec::new()
            }
            Event::ViewReady { rows, .. } => {
                s.recovered = rows.len();
                Vec::new()
            }
            Event::Admitted(req) => {
                let (barrier, persist) = s.persist(&req.frame);
                let context = s.context(barrier.sequence);
                let outbox = s.outbox.as_mut().expect("booted");
                outbox.publish(PendingSend {
                    context,
                    requires: vec![barrier],
                    to: CLIENT,
                    frame: req.frame,
                });
                vec![persist]
            }
            Event::Storage(storage) => {
                let outbox = s.outbox.as_mut().expect("booted");
                outbox.observe(&storage);
                if let StorageEvent::JournalDurable { .. } = storage {
                    return outbox.release(&ballot(s.replica));
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}

/// Deliberately faulty actor: acknowledges without any durable prerequisite.
pub struct EagerEcho(EchoState);

impl EagerEcho {
    /// New actor for `replica`.
    pub fn new(replica: ReplicaId) -> Self {
        EagerEcho(EchoState::new(replica))
    }
}

impl DeterministicMachine for EagerEcho {
    type Event = Event;
    type Effect = Effect;

    fn step(&mut self, event: Event) -> Vec<Effect> {
        let s = &mut self.0;
        match event {
            Event::Boot {
                boot_id,
                incarnation,
            } => {
                s.boot(boot_id, incarnation);
                Vec::new()
            }
            Event::Admitted(req) => {
                let (barrier, persist) = s.persist(&req.frame);
                // The omitted prerequisite: `requires` is empty.
                let send = Effect::SendWhenDurable {
                    context: s.context(barrier.sequence),
                    requires: Vec::new(),
                    to: CLIENT,
                    frame: req.frame,
                };
                vec![persist, send]
            }
            _ => Vec::new(),
        }
    }
}
