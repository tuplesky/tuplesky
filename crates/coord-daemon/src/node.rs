//! Driving one voter: events in, effects carried out (design Sections
//! 3.1, 17.3, 22.1).
//!
//! The protocol machines are pure. They take an [`Event`] and return
//! [`Effect`]s, and they touch neither disk nor socket -- which is what
//! lets the simulator replay them and the model checker reason about
//! them, and it means a real process has to do everything they describe.
//! That work is this module.
//!
//! Three of the rules it keeps are not visible in a machine's own code,
//! because the machine states them by *describing* a send rather than
//! performing one:
//!
//! * A send waits for every barrier it named to be durable. A vote or a
//!   promise that reaches a peer before the record behind it survives a
//!   crash is a vote this replica cannot honour afterwards, which is the
//!   one thing the protocol may never do.
//! * A send from a previous boot is dropped rather than transmitted. A
//!   frame prepared before a crash describes a state this process no
//!   longer has; the boot fence is what stops it being sent on as though
//!   it did.
//! * Effects beget effects. A persisted batch produces storage facts,
//!   which the machine turns into more effects, and a driver that carried
//!   out only the first round would stall a replica that had already
//!   decided what to do next.
//!
//! [`Outbox`] holds the first two; this module holds the third, and
//! routes what is left: peer frames to peers, evidence and releases to
//! the trusted collector, timers and entropy to the runtime that owns
//! real time and real randomness.

use coord_collector::frontend_frame;
use coord_consensus::{AppliedOutcome, Follower, Leader, PayloadRecordV1};
use coord_core::effect::{Effect, PeerId, TimerId};
use coord_core::event::{Event, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{Outbox, PendingSend};
use coord_storage::journaled::TransitionKind;
use coord_storage::{Applier, Persistence};
use coord_types::CommandId;
use coord_types::ids::Ballot;

use crate::metrics::{Recorder, Stage};

/// A voter's protocol machine in its current role.
///
/// Leader and follower are the same replica at different ballots, not
/// different processes: a campaign replaces one with the other in place,
/// and everything around it -- store, outbox, connections -- carries on.
#[derive(Debug)]
pub enum Machine {
    /// Leader of the current ballot.
    Leader(Box<Leader>),
    /// Follower of the current ballot.
    Follower(Box<Follower>),
}

impl Machine {
    /// Feed one event and take the effects it produced.
    pub fn step(&mut self, event: Event) -> Vec<Effect> {
        match self {
            Machine::Leader(m) => m.step(event),
            Machine::Follower(m) => m.step(event),
        }
    }

    /// Take what this machine refused since the last call, rendered.
    ///
    /// A machine records a refusal and carries on; nothing in the
    /// protocol is owed to a frame it would not accept. But a refusal
    /// that nobody reads is two problems: a list that grows for as long
    /// as the process runs, and a caller whose stream is held for an
    /// answer that was decided against and never sent. The driver
    /// drains this every turn, so the list stays bounded and the reason
    /// reaches the node's log.
    ///
    /// Rendered here rather than returned as two different enums,
    /// because the two roles refuse different things and a caller of
    /// this wants to say what happened, not to match on it.
    pub fn take_rejections(&mut self) -> Vec<String> {
        match self {
            Machine::Leader(m) => m
                .take_rejections()
                .into_iter()
                .map(|r| format!("{r:?}"))
                .collect(),
            Machine::Follower(m) => m
                .take_rejections()
                .into_iter()
                .map(|r| format!("{r:?}"))
                .collect(),
        }
    }

    /// Propose one of the service's own commands, where this replica
    /// leads. A follower proposes nothing: leading is what the ballot
    /// says, and a replica that does not lead has no order to offer.
    pub fn propose_service(&mut self, frame: &[u8]) -> Vec<Effect> {
        match self {
            Machine::Leader(m) => m.propose_service(frame),
            Machine::Follower(_) => Vec::new(),
        }
    }

    /// Whether this replica is holding a command it knows by identity
    /// and not by content.
    pub fn wants_payloads(&self) -> bool {
        self.missing_payloads() > 0
    }

    /// How many commands this replica knows by identity and not by
    /// content.
    pub fn missing_payloads(&self) -> usize {
        match self {
            Machine::Leader(_) => 0,
            Machine::Follower(m) => m.missing_payloads().len(),
        }
    }

    /// How many payload transfers a peer has answered this replica
    /// with.
    ///
    /// What paces a replica catching up. The missing count cannot: it
    /// moves because new commands arrive by identity as well as because
    /// old ones were answered, so under load it is never still and a
    /// replica that asked again whenever it moved would ask on every
    /// turn. This moves only when a peer replied, which is exactly the
    /// condition for the next ask to go at once rather than on the
    /// retry floor.
    pub const fn payloads_answered(&self) -> u64 {
        match self {
            Machine::Leader(_) => 0,
            Machine::Follower(m) => m.payloads_answered(),
        }
    }

    /// Ask `from` for the payloads this replica lacks. Only a follower
    /// lacks one: a leader holds every payload it proposed.
    pub fn request_payloads(&mut self, from: coord_types::ids::ReplicaId) -> Vec<Effect> {
        match self {
            Machine::Leader(_) => Vec::new(),
            Machine::Follower(m) => m.request_payloads(from),
        }
    }

    /// Whether this replica currently leads.
    pub const fn leads(&self) -> bool {
        matches!(self, Machine::Leader(_))
    }

    /// The next command whose turn it is to be applied, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        match self {
            Machine::Leader(m) => m.next_executable(),
            Machine::Follower(m) => m.next_executable(),
        }
    }

    /// The payload of a command this replica has learned.
    pub fn payload(&self, command: &CommandId) -> Option<PayloadRecordV1> {
        match self {
            Machine::Leader(m) => m.payload(command).cloned(),
            Machine::Follower(m) => m.payload(command).cloned(),
        }
    }

    /// Report what applying `command` produced.
    fn applied(
        &mut self,
        command: CommandId,
        outcome: &AppliedOutcome,
    ) -> Result<Vec<Effect>, DriveError> {
        let reported = match self {
            Machine::Leader(m) => m.applied(command, outcome),
            Machine::Follower(m) => m.applied(command, outcome),
        };
        reported.map_err(|e| DriveError::Submit(format!("{e:?}")))
    }
}

/// What a round of effects asks the runtime to do.
///
/// Everything here is already permitted: a peer frame in `peer` has had
/// its barriers made durable and its boot checked, and a frame that did
/// not pass either is simply not in it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outbound {
    /// Frames to transmit, each to one peer.
    pub peer: Vec<(PeerId, Vec<u8>)>,
    /// Frames for the trusted collector: evidence and released results.
    pub frontend: Vec<Vec<u8>>,
    /// Timers to arm, as (timer, ticks from now).
    pub arm: Vec<(TimerId, u64)>,
    /// Timers to cancel: every generation at or below each one.
    pub cancel: Vec<TimerId>,
    /// Read views the machine asked for and the driver could not build
    /// here, as (correlation, base) -- the runtime answers them with
    /// [`Event::ViewReady`].
    pub views: Vec<coord_core::effect::ReadViewRequest>,
    /// Entropy requests, answered with [`Event::Entropy`].
    pub entropy: Vec<u64>,
}

impl Outbound {
    /// Fold another round's requests into this one, keeping order.
    pub fn absorb(&mut self, other: Outbound) {
        self.peer.extend(other.peer);
        self.frontend.extend(other.frontend);
        self.arm.extend(other.arm);
        self.cancel.extend(other.cancel);
        self.views.extend(other.views);
        self.entropy.extend(other.entropy);
    }

    /// Whether the round asked for nothing.
    pub fn is_empty(&self) -> bool {
        self.peer.is_empty()
            && self.frontend.is_empty()
            && self.arm.is_empty()
            && self.cancel.is_empty()
            && self.views.is_empty()
            && self.entropy.is_empty()
    }
}

/// Why a round could not be carried out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriveError {
    /// A batch the machine asked to persist was refused before it reached
    /// the engine (an ordering guard, a bound).
    Submit(String),
    /// The engine failed. The caller quarantines: a replica that cannot
    /// make its own transitions durable must stop, not carry on from
    /// memory.
    Engine(String),
    /// A frame the collector is owed could not be encoded. The round is
    /// refused rather than the frame dropped: evidence that is silently
    /// lost looks exactly like a voter that did not answer.
    Encode(String),
}

impl core::fmt::Display for DriveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DriveError::Submit(e) => write!(f, "batch refused: {e}"),
            DriveError::Engine(e) => write!(f, "engine failed: {e}"),
            DriveError::Encode(e) => write!(f, "frame not encodable: {e}"),
        }
    }
}

impl core::error::Error for DriveError {}

/// One voter: its machine, its store and the sends it is holding.
///
/// `P` is where this replica's batches become durable. A voter on the
/// journal-first profile and one on the reference profile differ in that
/// and in nothing else the driver can see, which is why it is a
/// parameter rather than two drivers.
pub struct Node<P: Persistence> {
    machine: Machine,
    applier: Applier<P>,
    outbox: Outbox,
    frontend: PeerId,
    /// Rounds of effects carried out since boot (diagnostic).
    pub rounds: u64,
    /// Commands applied since boot (diagnostic).
    pub executed: u64,
    withheld: u64,
    withheld_evidence: u64,
    /// Where the journal and materialization stages are recorded
    /// (task-61), when the process keeps a recorder.
    recorder: Option<std::sync::Arc<Recorder>>,
    /// The command execution is waiting for a payload for, if any.
    awaiting: Option<CommandId>,
}

impl<P: Persistence> Node<P> {
    /// A node over `machine` and `applier`, publishing to `frontend`.
    pub fn new(machine: Machine, applier: Applier<P>, frontend: PeerId) -> Self {
        let boot = applier.store().boot();
        Node {
            machine,
            applier,
            outbox: Outbox::new(boot),
            frontend,
            rounds: 0,
            executed: 0,
            withheld: 0,
            withheld_evidence: 0,
            recorder: None,
            awaiting: None,
        }
    }

    /// Record this node's journal and materialization work in
    /// `recorder` (task-61).
    ///
    /// The points are here because this is where the work happens: a
    /// flush of the round's protocol transitions is the
    /// [`Stage::Journal`], and applying one executable command is the
    /// [`Stage::Materialization`]. Counting them anywhere else would be a
    /// second accounting of the same work, one step removed from it.
    pub fn record_into(&mut self, recorder: std::sync::Arc<Recorder>) {
        self.recorder = Some(recorder);
    }

    /// Time `work` as one pass through `stage`: entered before it runs,
    /// then completed with its duration or refused on an error.
    fn measured<T, E>(
        recorder: Option<&Recorder>,
        stage: Stage,
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let Some(recorder) = recorder else {
            return work();
        };
        recorder.entered(stage);
        let started = std::time::Instant::now();
        let result = work();
        match &result {
            Ok(_) => recorder.completed(stage, started.elapsed()),
            Err(_) => recorder.refused(stage),
        }
        result
    }

    /// The machine.
    pub const fn machine(&self) -> &Machine {
        &self.machine
    }

    /// Take what the machine refused since the last call, rendered.
    pub fn take_rejections(&mut self) -> Vec<String> {
        self.machine.take_rejections()
    }

    /// The command this replica cannot execute because it does not hold
    /// the payload, if execution is waiting on one.
    pub const fn awaiting(&self) -> Option<CommandId> {
        self.awaiting
    }

    /// Propose one of the service's own commands and carry out what that
    /// produced.
    pub fn propose_service(
        &mut self,
        frame: &[u8],
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let effects = self.machine.propose_service(frame);
        self.carry_out(effects, ballot)
    }

    /// Whether this replica is holding a command it knows by identity
    /// and not by content.
    pub fn wants_payloads(&self) -> bool {
        self.machine.wants_payloads()
    }

    /// How many commands this replica knows by identity and not by
    /// content.
    pub fn missing_payloads(&self) -> usize {
        self.machine.missing_payloads()
    }

    /// How many payload transfers a peer has answered this replica
    /// with.
    pub const fn payloads_answered(&self) -> u64 {
        self.machine.payloads_answered()
    }

    /// Ask `from` for the payloads this replica lacks.
    pub fn request_payloads(
        &mut self,
        from: coord_types::ids::ReplicaId,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let effects = self.machine.request_payloads(from);
        self.carry_out(effects, ballot)
    }

    /// The applier (watch hub, reader, store).
    pub const fn applier(&self) -> &Applier<P> {
        &self.applier
    }

    /// The applier, mutably.
    pub const fn applier_mut(&mut self) -> &mut Applier<P> {
        &mut self.applier
    }

    /// Sends waiting on a barrier that is not durable yet.
    pub fn held(&self) -> usize {
        self.outbox.pending().len()
    }

    /// How many sends this node has ever had to hold back, and how many
    /// of those were evidence for the collector.
    ///
    /// A send that was described before its record landed and went out
    /// anyway is indistinguishable, afterwards, from one that waited: the
    /// bytes are the same and the peer received them either way. The
    /// difference only exists while the round is running, so it is
    /// counted while it is observable.
    ///
    /// The evidence count is the one an operator wants. A voter whose
    /// disk is slow holds its votes, and from the collector's side that
    /// is indistinguishable from a voter that is partitioned or gone;
    /// this is what tells the two apart.
    pub const fn held_at_least_once(&self) -> (u64, u64) {
        (self.withheld, self.withheld_evidence)
    }

    /// Feed one event and carry out everything it produced.
    ///
    /// `ballot` is the ballot the replica is at now: a held send whose
    /// context names an older one is dropped when it is finally released,
    /// because the state it described has been superseded.
    pub fn on_event(&mut self, event: Event, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let effects = self.machine.step(event);
        self.carry_out(effects, ballot)
    }

    /// Carry out `effects`, and everything they lead to.
    fn carry_out(&mut self, effects: Vec<Effect>, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let mut out = Outbound::default();
        let mut queue = effects;
        // A bound on rounds, not on work: every round must make progress
        // through storage, and a machine that answered its own storage
        // facts with more storage facts for ever would otherwise spin
        // here rather than be visible as a fault.
        for _ in 0..MAX_ROUNDS {
            if queue.is_empty() {
                return Ok(out);
            }
            self.rounds += 1;
            let (round, next) = self.one_round(queue, ballot)?;
            out.absorb(round);
            queue = next;
        }
        Err(DriveError::Engine(format!(
            "storage did not settle in {MAX_ROUNDS} rounds"
        )))
    }

    /// One pass over `effects`: returns what the runtime must do and the
    /// effects the storage facts of this pass produced.
    fn one_round(
        &mut self,
        effects: Vec<Effect>,
        ballot: &Ballot,
    ) -> Result<(Outbound, Vec<Effect>), DriveError> {
        let mut out = Outbound::default();
        let mut persisted = false;
        for effect in effects {
            // A released result is the leader's release-gate output. It
            // is not a `SendWhenDurable` and rests on no barrier of its
            // own: the release rule has already decided it may be
            // disclosed, so it goes to the collector now.
            //
            // Evidence does *not* take this path, although it is also the
            // collector's. Evidence is a vote -- "I have this and I will
            // not forget it" -- and a vote that reaches the collector
            // before the record behind it is durable is a promise this
            // replica cannot honour after a crash. It goes through the
            // outbox with every other send, and becomes a frame only when
            // that send is released.
            if matches!(effect, Effect::Released(_))
                && let Some(frame) = frontend_frame(&effect, self.frontend)
            {
                out.frontend
                    .push(frame.map_err(|e| DriveError::Encode(format!("{e:?}")))?);
                continue;
            }
            match effect {
                Effect::Persist(batch) => {
                    // Everything a protocol machine asks to persist is a
                    // protocol transition: a ballot, a promise, a vote, a
                    // bound selection. None of them carries an
                    // application base or moves the execution frontier,
                    // and the execution redo is not one of them -- that
                    // comes from applying a command, which is where its
                    // position, revision and result are known.
                    self.applier
                        .store_mut()
                        .submit(batch, TransitionKind::Protocol)
                        .map_err(|e| DriveError::Submit(e.to_string()))?;
                    persisted = true;
                }
                Effect::SendWhenDurable {
                    context,
                    requires,
                    to,
                    frame,
                } => {
                    // Not sent here, even when nothing is required: the
                    // outbox is what checks the boot fence, and a send
                    // that skipped it would be the one send that could
                    // cross a crash.
                    self.outbox.publish(PendingSend {
                        context,
                        requires,
                        to,
                        frame,
                    });
                }
                Effect::ArmTimer { id, after_ticks } => out.arm.push((id, after_ticks)),
                Effect::CancelTimer { id } => out.cancel.push(id),
                Effect::ReadView(request) => out.views.push(request),
                Effect::RequestEntropy { request } => out.entropy.push(request),
                // An established result is the leader's own record that a
                // command is decided. The collector learns of it through
                // the release, which carries the response; publishing the
                // establishment too would disclose an outcome before the
                // release rule had admitted it.
                Effect::Established(_) => {}
                Effect::Released(_) => unreachable!("routed to the collector above"),
                #[expect(
                    unreachable_patterns,
                    reason = "every variant is named above; this is the guard against a new one"
                )]
                other => {
                    return Err(DriveError::Submit(format!("unhandled effect {other:?}")));
                }
            }
        }

        let mut next = Vec::new();
        if persisted {
            // One lowering moves one group, and a group takes one batch
            // per domain. A round that submitted more than one -- two
            // adoptions decided together, a proposal beside the
            // acceptance its predecessor unblocked -- leaves the rest
            // queued, and nothing comes back for them on its own: the
            // next lowering happens only because something else was
            // persisted. So the queue would lag by one for ever, and
            // whatever was submitted last would never become durable at
            // all. A replica that never reports a batch durable never
            // releases the vote that waited on it, and the quorum that
            // vote belongs to does not form.
            //
            // The bound is the queue's own depth when this started, so a
            // lowering that moves nothing -- an append still in flight,
            // an uncertain head -- ends the loop rather than spinning;
            // the next round lowers again.
            let mut attempts = self.applier.store().queued() + 1;
            loop {
                let store = self.applier.store_mut();
                let outcome =
                    Self::measured(self.recorder.as_deref(), Stage::Journal, || store.lower())
                        .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
                // Indeterminate is not "failed": the group's outcome is
                // unknown, so the caller reconciles rather than assuming
                // either answer. It surfaces as an engine failure here so
                // it cannot be mistaken for a clean round.
                if outcome.indeterminate {
                    return Err(DriveError::Engine("group outcome indeterminate".into()));
                }
                for event in outcome.events {
                    self.outbox.observe(&event);
                    next.extend(self.machine.step(Event::Storage(event)));
                }
                attempts -= 1;
                if self.applier.store().queued() == 0 || attempts == 0 {
                    break;
                }
            }
        }
        for waiting in self.outbox.pending() {
            self.withheld += 1;
            if waiting.to == self.frontend {
                self.withheld_evidence += 1;
            }
        }
        // Whatever became durable in this round releases the sends that
        // were waiting on it -- including sends published in this very
        // round, which is why the release happens after the flush.
        for effect in self.outbox.release(ballot) {
            let Effect::SendWhenDurable { to, .. } = &effect else {
                unreachable!("the outbox releases only sends: {effect:?}");
            };
            // A frame for the collector is never also a peer's: it is
            // published to the trusted boundary and to nowhere else.
            if *to == self.frontend {
                let frame = frontend_frame(&effect, self.frontend)
                    .expect("a send to the frontend is a frontend frame")
                    .map_err(|e| DriveError::Encode(format!("{e:?}")))?;
                out.frontend.push(frame);
                continue;
            }
            let Effect::SendWhenDurable { to, frame, .. } = effect else {
                unreachable!("checked above")
            };
            out.peer.push((to, frame));
        }
        Ok((out, next))
    }

    /// Apply every command whose turn has come, in order.
    ///
    /// Materialization is ordered and it is not optional: a command the
    /// replica has learned but not applied holds up every command after
    /// it, so this runs to exhaustion rather than a batch at a time. Each
    /// outcome goes back to the machine, which is how execution advances
    /// and how a result becomes releasable.
    ///
    /// Speculative execution (task-29) is the leader's separate path and
    /// is not driven here. Without it a result is released on the final
    /// path rather than the early one -- slower, never wrong -- which is
    /// the preview's behaviour and not the architecture's.
    pub fn execute(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let mut out = Outbound::default();
        while let Some(command) = self.machine.next_executable() {
            // A command whose turn has come but whose payload this
            // replica does not hold is not something to skip past: the
            // order is the whole of the guarantee, and going on would
            // apply a later command first.
            let Some(payload) = self.machine.payload(&command) else {
                // Not something to skip past -- the order is the whole
                // of the guarantee -- and not a fault either. A replica
                // learns a command's identity from evidence and its
                // content from a submission or a peer, and the two can
                // arrive in either order; a command the leader proposed
                // out of its own scheduler has no submission coming at
                // all. So execution stops here, the command is named,
                // and the runtime asks for what it is missing.
                self.awaiting = Some(command);
                return Ok(out);
            };
            self.awaiting = None;
            let applier = &mut self.applier;
            let outcome = Self::measured(self.recorder.as_deref(), Stage::Materialization, || {
                applier.apply(command, &payload)
            })
            .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
            self.executed += 1;
            let effects = self.machine.applied(command, &outcome)?;
            out.absorb(self.carry_out(effects, ballot)?);
        }
        Ok(out)
    }

    /// Storage facts the runtime observed outside a round (a reconcile, a
    /// late completion), fed back the same way.
    pub fn on_storage(
        &mut self,
        event: StorageEvent,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        self.outbox.observe(&event);
        let effects = self.machine.step(Event::Storage(event));
        self.carry_out(effects, ballot)
    }

    /// Sends the outbox dropped rather than transmitted, and why.
    ///
    /// A dropped send is not a silent loss: it is a frame the protocol
    /// prepared under a boot or a ballot that no longer holds, and the
    /// count of them is how an operator sees a replica that is being
    /// fenced rather than one that is merely quiet.
    pub fn dropped(&mut self) -> Vec<(PendingSend, coord_core::outbox::ReleaseError)> {
        self.outbox.take_dropped()
    }
}

/// How many times storage facts may produce further effects within one
/// event before the driver calls it a fault.
const MAX_ROUNDS: usize = 32;
