//! The deterministic world: runs machines behind storage, network, clock
//! and lifecycle models under one ordered schedule.

use std::collections::BTreeMap;

use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_core::effect::{BarrierId, BootId, Effect, EffectContext, PeerId, TimerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageError, StorageEvent,
};
use coord_core::machine::{ClockSnapshot, DeterministicMachine};
use coord_types::identity::Digest32;
use coord_types::ids::{ReplicaId, ReplicaIncarnation, SessionId};
use serde::{Deserialize, Serialize};

use crate::actors::{CLIENT, DurableEcho, EagerEcho};
use crate::network::Network;
use crate::replay::{ActorKind, Fault, Scenario};
use crate::rng::NamedStreams;
use crate::schedule::{EventKey, Schedule};
use crate::storage::StorageModel;

/// Node index within a scenario.
pub type NodeId = u16;

type Machine = Box<dyn DeterministicMachine<Event = Event, Effect = Effect>>;

/// Replica identity of a node index (never the all-zero client identity).
pub fn replica_of(node: NodeId) -> ReplicaId {
    let mut id = [0u8; 16];
    id[0] = 0xA0;
    id[14..16].copy_from_slice(&node.to_be_bytes());
    ReplicaId(id)
}

fn node_of(replica: &ReplicaId) -> Option<NodeId> {
    if replica.0[0] != 0xA0 {
        return None;
    }
    Some(u16::from_be_bytes([replica.0[14], replica.0[15]]))
}

#[derive(Debug)]
enum Pending {
    Admit {
        node: NodeId,
        frame: Vec<u8>,
    },
    StorageDone {
        node: NodeId,
        barrier: BarrierId,
    },
    StorageFail {
        node: NodeId,
        barrier: BarrierId,
        error: StorageError,
    },
    Deliver {
        from: NodeId,
        to: NodeId,
        frame: Vec<u8>,
    },
    ClientDeliver {
        from: NodeId,
        frame: Vec<u8>,
    },
    Timer {
        node: NodeId,
        id: TimerId,
    },
    View {
        node: NodeId,
        request: u64,
        rows: Vec<(u16, Vec<u8>, Vec<u8>)>,
    },
    Entropy {
        node: NodeId,
        request: u64,
    },
    Fault(Fault),
}

struct Node {
    machine: Option<Machine>,
    storage: StorageModel,
    boot: Option<BootId>,
    boot_tick: u64,
    incarnation: ReplicaIncarnation,
    clock_offset_ms: u64,
    last_clock_tick: Option<u64>,
    /// Whether the current boot's machine has received `Event::Boot`.
    booted: bool,
    persists: u64,
    held: Vec<(EffectContext, Vec<BarrierId>, PeerId, Vec<u8>)>,
}

/// What a single step did; the driver uses it to run oracles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepOutcome {
    /// An event was dispatched to a node (or dropped because it was down).
    Dispatched,
    /// A node crashed; volatile state was lost.
    Crashed(NodeId),
    /// A node restarted with a new boot identity.
    Restarted(NodeId),
    /// The client received a frame from a node.
    ClientAck(NodeId),
    /// A fault or bookkeeping step with no machine input.
    Other,
}

/// Summary of a completed run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReport {
    /// BLAKE3 digest of the dispatched-event trace.
    pub trace_digest: Digest32,
    /// Digest of the client-visible history.
    pub visible_digest: Digest32,
    /// Number of dispatched events.
    pub steps: u64,
    /// Final virtual tick.
    pub final_tick: u64,
    /// Draw counts per RNG substream.
    pub draws: BTreeMap<String, u64>,
    /// Crashes performed.
    pub crashes: u32,
}

/// The world.
pub struct World {
    scenario: Scenario,
    rng: NamedStreams,
    schedule: Schedule<Pending>,
    nodes: Vec<Node>,
    network: Network,
    client_acks: Vec<(NodeId, Vec<u8>)>,
    trace: blake3::Hasher,
    trace_len: u64,
    crashes: u32,
    events_before_boot: u64,
    stale_incarnation_sends: u64,
    started: bool,
}

impl World {
    /// Build a world for `scenario`; nothing runs until [`World::step`].
    pub fn new(scenario: Scenario) -> Self {
        if let Err(e) = scenario.validate() {
            panic!("invalid scenario: {e}");
        }
        let mut rng = NamedStreams::new(scenario.seed);
        let mut nodes = Vec::new();
        for _ in 0..scenario.nodes {
            let clock_offset_ms = rng.below("clock", 1_000);
            nodes.push(Node {
                machine: None,
                storage: StorageModel::default(),
                boot: None,
                boot_tick: 0,
                incarnation: ReplicaIncarnation::new(1).expect("bounded"),
                clock_offset_ms,
                last_clock_tick: None,
                booted: false,
                persists: 0,
                held: Vec::new(),
            });
        }
        let network = Network::new(scenario.network.clone());
        World {
            scenario,
            rng,
            schedule: Schedule::default(),
            nodes,
            network,
            client_acks: Vec::new(),
            trace: blake3::Hasher::new_derive_key("tuplesky coord-sim trace v1"),
            trace_len: 0,
            crashes: 0,
            events_before_boot: 0,
            stale_incarnation_sends: 0,
            started: false,
        }
    }

    /// Events delivered to a machine before its `Boot` event in the current
    /// boot; always zero, kept as an observable invariant.
    pub fn events_before_boot(&self) -> u64 {
        self.events_before_boot
    }

    /// Sends addressed to a node's obsolete or unknown incarnation and
    /// therefore dropped (the `PeerId` fencing contract).
    pub fn stale_incarnation_sends(&self) -> u64 {
        self.stale_incarnation_sends
    }

    /// Scenario being run.
    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    /// Current virtual tick.
    pub fn now(&self) -> u64 {
        self.schedule.now()
    }

    /// Storage model of a node (for oracles).
    pub fn storage(&self, node: NodeId) -> &StorageModel {
        &self.nodes[node as usize].storage
    }

    /// Frames the client received, in delivery order, with their sender.
    pub fn client_acks(&self) -> &[(NodeId, Vec<u8>)] {
        &self.client_acks
    }

    fn make_machine(&self, node: NodeId) -> Machine {
        let replica = replica_of(node);
        match self.scenario.actor {
            ActorKind::DurableEcho => Box::new(DurableEcho::new(replica)),
            ActorKind::EagerEcho => Box::new(EagerEcho::new(replica)),
        }
    }

    fn start(&mut self) {
        self.started = true;
        for node in 0..self.scenario.nodes {
            self.boot(node);
        }
        let mut tick = 0;
        for i in 0..self.scenario.workload.requests {
            tick += self.rng.range(
                "workload",
                self.scenario.workload.min_gap,
                self.scenario.workload.max_gap,
            );
            let node = (i % u32::from(self.scenario.nodes)) as NodeId;
            let frame = format!("req-{i}").into_bytes();
            self.schedule
                .insert_at(tick, Pending::Admit { node, frame });
        }
        for fault in self.scenario.faults.clone() {
            self.schedule.insert_at(fault.tick(), Pending::Fault(fault));
        }
    }

    fn boot(&mut self, node: NodeId) {
        let boot_id = BootId(self.rng.bytes16("boot"));
        let machine = self.make_machine(node);
        let n = &mut self.nodes[node as usize];
        n.machine = Some(machine);
        n.boot = Some(boot_id);
        n.boot_tick = self.schedule.now();
        n.last_clock_tick = None;
        n.booted = false;
        n.held.clear();
        let incarnation = n.incarnation;
        let rows = n.storage.durable_rows();
        self.dispatch(
            node,
            Event::Boot {
                boot_id,
                incarnation,
            },
        );
        self.dispatch(node, Event::ViewReady { request: 0, rows });
    }

    fn crash(&mut self, node: NodeId) {
        let n = &mut self.nodes[node as usize];
        n.machine = None;
        n.boot = None;
        n.storage.crash();
        n.held.clear();
        self.crashes += 1;
        // Pending completions for the lost volatile batches and timers of the
        // dead boot vanish; deliveries are dropped on arrival while down.
        self.schedule.retain(|_, p| !matches!(p, Pending::StorageDone { node: x, .. } | Pending::StorageFail { node: x, .. } | Pending::Timer { node: x, .. } | Pending::View { node: x, .. } | Pending::Entropy { node: x, .. } if *x == node));
        self.record(b"crash", node, &[]);
    }

    fn record(&mut self, kind: &[u8], node: NodeId, payload: &[u8]) {
        self.trace.update(&self.schedule.now().to_be_bytes());
        self.trace.update(&node.to_be_bytes());
        self.trace.update(&(kind.len() as u64).to_be_bytes());
        self.trace.update(kind);
        self.trace.update(&(payload.len() as u64).to_be_bytes());
        self.trace.update(payload);
        self.trace_len += 1;
    }

    fn clock(&self, node: NodeId) -> ClockSnapshot {
        let n = &self.nodes[node as usize];
        let now = self.schedule.now();
        let wall = now.saturating_mul(1_000).saturating_add(n.clock_offset_ms);
        ClockSnapshot {
            monotonic_ticks: now - n.boot_tick,
            wall_lower_ms: wall,
            wall_upper_ms: wall + 1,
            healthy: true,
        }
    }

    fn dispatch(&mut self, node: NodeId, event: Event) {
        if self.nodes[node as usize].machine.is_none() {
            self.record(b"dropped-down", node, &[]);
            return;
        }
        let now = self.schedule.now();
        let is_boot = matches!(event, Event::Boot { .. });
        if !is_boot && !self.nodes[node as usize].booted {
            self.events_before_boot += 1;
        }
        // `Boot` is the first event a machine sees: the clock snapshot for
        // this tick is injected before the first non-boot event instead.
        if !is_boot && self.nodes[node as usize].last_clock_tick != Some(now) {
            self.nodes[node as usize].last_clock_tick = Some(now);
            let clock = self.clock(node);
            self.record(b"clock", node, &clock.monotonic_ticks.to_be_bytes());
            let effects = self.nodes[node as usize]
                .machine
                .as_mut()
                .expect("alive")
                .step(Event::Clock(clock));
            self.apply_effects(node, effects);
        }
        let kind: &[u8] = match &event {
            Event::Boot { .. } => b"boot",
            Event::Storage(_) => b"storage",
            Event::Timer(_) => b"timer",
            Event::Clock(_) => b"clock",
            Event::Peer(_) => b"peer",
            Event::Admitted(_) => b"admitted",
            Event::ViewReady { .. } => b"view",
            Event::Entropy { .. } => b"entropy",
            Event::ConnectionClosed { .. } => b"closed",
        };
        let payload = format!("{event:?}");
        self.record(kind, node, payload.as_bytes());
        if is_boot {
            self.nodes[node as usize].booted = true;
        }
        let effects = self.nodes[node as usize]
            .machine
            .as_mut()
            .expect("alive")
            .step(event);
        self.apply_effects(node, effects);
    }

    fn apply_effects(&mut self, node: NodeId, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Persist(batch) => {
                    let n = &mut self.nodes[node as usize];
                    n.persists += 1;
                    let nth = n.persists;
                    let barrier = batch.barrier;
                    n.storage.submit(batch);
                    let forced_fail = self.scenario.faults.iter().any(|f| matches!(f, Fault::FailBatch { node: x, nth: k } if *x == node && *k == nth));
                    let random_fail = self.rng.chance("disk.fail", self.scenario.storage.fail_ppm);
                    let delay = self.rng.range(
                        "disk.delay",
                        self.scenario.storage.min_delay,
                        self.scenario.storage.max_delay,
                    );
                    if forced_fail || random_fail {
                        self.schedule.insert_after(
                            delay,
                            Pending::StorageFail {
                                node,
                                barrier,
                                error: StorageError::Indeterminate,
                            },
                        );
                    } else {
                        self.schedule
                            .insert_after(delay, Pending::StorageDone { node, barrier });
                    }
                }
                Effect::SendWhenDurable {
                    context,
                    requires,
                    to,
                    frame,
                } => {
                    self.send(node, context, requires, to, frame);
                }
                Effect::ArmTimer { id, after_ticks } => {
                    self.schedule
                        .insert_after(after_ticks, Pending::Timer { node, id });
                }
                Effect::CancelTimer { id } => {
                    self.schedule.retain(|_, p| !matches!(p, Pending::Timer { node: x, id: t } if *x == node && t.name == id.name && t.generation <= id.generation));
                }
                Effect::ReadView(req) => {
                    let rows = self.nodes[node as usize]
                        .storage
                        .durable_rows()
                        .into_iter()
                        .filter(|(c, k, _)| {
                            req.prefixes
                                .iter()
                                .any(|(pc, pk)| pc.0 == *c && k.starts_with(pk))
                        })
                        .collect();
                    self.schedule.insert_after(
                        1,
                        Pending::View {
                            node,
                            request: req.request,
                            rows,
                        },
                    );
                }
                Effect::Established(result) => {
                    let payload = format!("{result:?}");
                    self.record(b"established", node, payload.as_bytes());
                }
                Effect::Released(result) => {
                    let payload = format!("{result:?}");
                    self.record(b"released", node, payload.as_bytes());
                }
                Effect::RequestEntropy { request } => {
                    self.schedule
                        .insert_after(1, Pending::Entropy { node, request });
                }
            }
        }
    }

    /// Honor a send effect: the runtime transmits only when every required
    /// barrier is durable in this boot. Effects from a previous boot are
    /// dropped (boot fence).
    fn send(
        &mut self,
        node: NodeId,
        context: EffectContext,
        requires: Vec<BarrierId>,
        to: PeerId,
        frame: Vec<u8>,
    ) {
        let n = &mut self.nodes[node as usize];
        if n.boot != Some(context.boot_id) {
            self.record(b"send-wrong-boot", node, &[]);
            return;
        }
        if !requires.iter().all(|b| n.storage.is_durable(b)) {
            n.held.push((context, requires, to, frame));
            return;
        }
        self.transmit(node, to, frame);
    }

    fn release_held(&mut self, node: NodeId) {
        let held = std::mem::take(&mut self.nodes[node as usize].held);
        for (context, requires, to, frame) in held {
            self.send(node, context, requires, to, frame);
        }
    }

    fn transmit(&mut self, from: NodeId, to: PeerId, frame: Vec<u8>) {
        if to == CLIENT {
            let delivery = self.network.decide(&mut self.rng, from, u16::MAX);
            for delay in delivery.copies {
                self.schedule.insert_after(
                    delay,
                    Pending::ClientDeliver {
                        from,
                        frame: frame.clone(),
                    },
                );
            }
            return;
        }
        let Some(target) = node_of(&to.replica) else {
            self.record(b"send-unknown-peer", from, &[]);
            return;
        };
        if usize::from(target) >= self.nodes.len() {
            self.record(b"send-unknown-peer", from, &[]);
            return;
        }
        // A `PeerId` addresses one exact incarnation. Anything else is fenced
        // here exactly as a production transport must fence it.
        if to.incarnation != self.nodes[usize::from(target)].incarnation {
            self.stale_incarnation_sends += 1;
            self.record(
                b"send-stale-incarnation",
                from,
                &to.incarnation.get().to_be_bytes(),
            );
            return;
        }
        let delivery = self.network.decide(&mut self.rng, from, target);
        for delay in delivery.copies {
            self.schedule.insert_after(
                delay,
                Pending::Deliver {
                    from,
                    to: target,
                    frame: frame.clone(),
                },
            );
        }
    }

    /// Execute one scheduled item. `None` when the schedule is exhausted or
    /// the scenario's tick budget is spent.
    pub fn step(&mut self) -> Option<StepOutcome> {
        if !self.started {
            self.start();
        }
        let (key, pending) = self.schedule.pop()?;
        if key.tick > self.scenario.max_ticks {
            return None;
        }
        Some(match pending {
            Pending::Admit { node, frame } => {
                let receipt = AdmissionReceipt::from_verifier(
                    VerifierToken::for_boundary(),
                    SessionId([0x5e; 16]),
                    1,
                    u32::MAX,
                    Digest32(*blake3::hash(&frame).as_bytes()),
                    key.tick,
                );
                self.dispatch(node, Event::Admitted(AdmittedRequest { receipt, frame }));
                StepOutcome::Dispatched
            }
            Pending::StorageDone { node, barrier } => {
                let Some(seq) = self.nodes[node as usize].storage.complete(barrier) else {
                    return Some(StepOutcome::Other);
                };
                self.dispatch(
                    node,
                    Event::Storage(StorageEvent::JournalDurable {
                        barrier_id: barrier,
                        journal_seq: seq,
                    }),
                );
                self.release_held(node);
                StepOutcome::Dispatched
            }
            Pending::StorageFail {
                node,
                barrier,
                error,
            } => {
                if !self.nodes[node as usize].storage.fail(barrier, error) {
                    return Some(StepOutcome::Other);
                }
                self.dispatch(
                    node,
                    Event::Storage(StorageEvent::Failed {
                        barrier_id: barrier,
                        error,
                    }),
                );
                StepOutcome::Dispatched
            }
            Pending::Deliver { from, to, frame } => {
                let provenance = PeerProvenance::from_transport(
                    replica_of(from),
                    self.nodes[from as usize].incarnation,
                    u64::from(from) << 16 | u64::from(to),
                );
                self.dispatch(
                    to,
                    Event::Peer(AuthenticatedPeerMessage::new(provenance, frame)),
                );
                StepOutcome::Dispatched
            }
            Pending::ClientDeliver { from, frame } => {
                self.record(b"client-ack", from, &frame);
                self.client_acks.push((from, frame));
                StepOutcome::ClientAck(from)
            }
            Pending::Timer { node, id } => {
                self.dispatch(node, Event::Timer(id));
                StepOutcome::Dispatched
            }
            Pending::View {
                node,
                request,
                rows,
            } => {
                self.dispatch(node, Event::ViewReady { request, rows });
                StepOutcome::Dispatched
            }
            Pending::Entropy { node, request } => {
                let mut bytes = [0u8; 32];
                bytes[..16].copy_from_slice(&self.rng.bytes16("entropy"));
                bytes[16..].copy_from_slice(&self.rng.bytes16("entropy"));
                self.dispatch(node, Event::Entropy { request, bytes });
                StepOutcome::Dispatched
            }
            Pending::Fault(fault) => match fault {
                Fault::Crash { node, .. } => {
                    if self.nodes[node as usize].machine.is_none() {
                        return Some(StepOutcome::Other);
                    }
                    self.crash(node);
                    StepOutcome::Crashed(node)
                }
                Fault::Restart { node, .. } => {
                    if self.nodes[node as usize].machine.is_some() {
                        return Some(StepOutcome::Other);
                    }
                    self.boot(node);
                    StepOutcome::Restarted(node)
                }
                Fault::Cut { from, to, .. } => {
                    self.network.cut(from, to);
                    StepOutcome::Other
                }
                Fault::Heal { from, to, .. } => {
                    self.network.heal(from, to);
                    StepOutcome::Other
                }
                Fault::FailBatch { .. } => StepOutcome::Other,
            },
        })
    }

    /// Run to completion, invoking `on_outcome` after every step.
    pub fn run(&mut self, mut on_outcome: impl FnMut(&World, &StepOutcome)) -> RunReport {
        while let Some(outcome) = self.step() {
            on_outcome(self, &outcome);
        }
        self.report()
    }

    /// Report for the run so far.
    pub fn report(&self) -> RunReport {
        let mut visible = blake3::Hasher::new_derive_key("tuplesky coord-sim visible history v1");
        for (node, frame) in &self.client_acks {
            visible.update(&node.to_be_bytes());
            visible.update(&(frame.len() as u64).to_be_bytes());
            visible.update(frame);
        }
        RunReport {
            trace_digest: Digest32(*self.trace.finalize().as_bytes()),
            visible_digest: Digest32(*visible.finalize().as_bytes()),
            steps: self.trace_len,
            final_tick: self.schedule.now(),
            draws: self.rng.draws().clone(),
            crashes: self.crashes,
        }
    }

    /// Every node's replica identity.
    pub fn replicas(&self) -> Vec<ReplicaId> {
        (0..self.scenario.nodes).map(replica_of).collect()
    }

    /// Whether a node is currently up.
    pub fn is_up(&self, node: NodeId) -> bool {
        self.nodes[node as usize].machine.is_some()
    }

    /// Key of the last scheduled event (diagnostic).
    pub fn pending_events(&self) -> usize {
        self.schedule.len()
    }

    /// Explicit key type re-export for drivers that inspect ordering.
    pub fn key_type() -> core::marker::PhantomData<EventKey> {
        core::marker::PhantomData
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{ActorKind, Scenario};

    #[test]
    fn sends_to_a_stale_incarnation_are_fenced_before_scheduling() {
        let mut world = World::new(Scenario::new([1; 32], ActorKind::DurableEcho, 2));
        let before = world.schedule.len();
        let stale = PeerId {
            replica: replica_of(1),
            incarnation: ReplicaIncarnation::new(2).unwrap(),
        };
        world.transmit(0, stale, vec![1]);
        assert_eq!(world.stale_incarnation_sends(), 1);
        assert_eq!(world.schedule.len(), before, "nothing was scheduled");
        let current = PeerId {
            replica: replica_of(1),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        };
        world.transmit(0, current, vec![1]);
        assert_eq!(world.stale_incarnation_sends(), 1);
        assert!(
            world.schedule.len() > before,
            "the exact incarnation is delivered"
        );
        // A replica outside the scenario is unknown, not a panic.
        let unknown = PeerId {
            replica: replica_of(7),
            incarnation: ReplicaIncarnation::new(1).unwrap(),
        };
        world.transmit(0, unknown, vec![1]);
    }
}
