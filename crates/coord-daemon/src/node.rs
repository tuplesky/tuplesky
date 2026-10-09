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

use std::collections::VecDeque;

use coord_collector::frontend_frame;
use coord_consensus::{AppliedOutcome, Follower, Leader, PayloadRecordV1, SyncDecision};
use coord_core::effect::{Effect, PeerId, PersistBatch, TimerId};
use coord_core::event::{Event, StorageError, StorageEvent};
use coord_core::machine::DeterministicMachine;
use coord_core::outbox::{Outbox, PendingSend};
use coord_storage::journaled::TransitionKind;
use coord_storage::{Applier, Persistence, Refused};
use coord_types::CommandId;
use coord_types::ids::{Ballot, ExecutionPosition};

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

    /// What the leader's re-sends did since the last call (task-d49);
    /// nothing for a follower, which re-sends nothing.
    pub fn take_resend_counts(&mut self) -> coord_consensus::ResendCounts {
        match self {
            Machine::Leader(m) => m.take_resend_counts(),
            Machine::Follower(_) => coord_consensus::ResendCounts::default(),
        }
    }

    /// What the fast path did since the last call (task-d62): the
    /// acknowledgements a follower sent. The reasons a command missed the
    /// fast path are taken one by one ([`Machine::take_missed_fast`]).
    pub fn take_fast_path_counts(&mut self) -> coord_consensus::FastPathCounts {
        match self {
            Machine::Leader(_) => coord_consensus::FastPathCounts::default(),
            Machine::Follower(m) => m.take_fast_path_counts(),
        }
    }

    /// What the fast path did since [`Machine::take_fast_path_counts`]
    /// was last called (task-d62).
    pub fn fast_path_counts(&self) -> coord_consensus::FastPathCounts {
        match self {
            Machine::Leader(_) => coord_consensus::FastPathCounts::default(),
            Machine::Follower(m) => m.fast_path_counts(),
        }
    }

    /// Why `command`, established on the slow path, missed the fast one
    /// (task-d62), taken; None when the role holds no reason for it.
    pub fn take_missed_fast(&mut self, command: &CommandId) -> Option<coord_consensus::MissedFast> {
        match self {
            Machine::Leader(m) => m.take_missed_fast(command),
            Machine::Follower(m) => m.take_missed_fast(command),
        }
    }

    /// Keep what a leader commits for [`Machine::take_learned`]
    /// (task-d62); a follower commits nothing from its own leading.
    pub fn observe_learning(&mut self) {
        if let Machine::Leader(m) = self {
            m.observe_learning();
        }
    }

    /// What a leader committed since the last call (task-d62).
    pub fn take_learned(&mut self) -> Vec<CommandId> {
        match self {
            Machine::Leader(m) => m.take_learned(),
            Machine::Follower(_) => Vec::new(),
        }
    }

    /// The pre-acceptances this replica holds that the leader has not
    /// ordered, and those it has ordered a later command past (task-d62).
    /// None for a leader, which orders what it pre-accepts: its own path
    /// log's suffix is not left unsynchronized by anyone else.
    pub fn unordered(&self) -> (usize, std::collections::BTreeSet<CommandId>) {
        match self {
            Machine::Leader(_) => (0, std::collections::BTreeSet::new()),
            Machine::Follower(m) => m.table().unordered(),
        }
    }

    /// For a leader, the commands its own path logs hold pending, which it
    /// never synchronizes (task-d68); nothing for a follower.
    pub fn leader_log_pending(&self) -> usize {
        match self {
            Machine::Leader(m) => m.table().pending_in_logs(),
            Machine::Follower(_) => 0,
        }
    }

    /// Send the voters, again, the proposals they have not voted on
    /// (task-d07), and the commit frontier (task-d09). Only a leader has
    /// proposals to send or a frontier to announce.
    pub fn resend_unvoted(&mut self, per_voter: usize) -> Vec<Effect> {
        match self {
            Machine::Leader(m) => {
                let mut effects = m.resend_unvoted(per_voter);
                effects.extend(m.announce_committed());
                effects
            }
            Machine::Follower(_) => Vec::new(),
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

    /// Ask the voters that promised this replica's campaign for the report
    /// pages that have not arrived (task-d28). Only a follower campaigns.
    pub fn request_report_pages(&mut self) -> Vec<Effect> {
        match self {
            Machine::Leader(_) => Vec::new(),
            Machine::Follower(m) => m.request_report_pages(),
        }
    }

    /// What this replica executed through.
    pub const fn executed_through(&self) -> coord_types::ids::ExecutionPosition {
        match self {
            Machine::Leader(m) => m.executed_through(),
            Machine::Follower(m) => m.executed_through(),
        }
    }

    /// The command this replica holds two decisions of (task-d14): other
    /// admission facts than it committed or executed it under. Only a
    /// follower finds one, at installation or in its own selection.
    pub const fn halted(&self) -> Option<CommandId> {
        match self {
            Machine::Leader(_) => None,
            Machine::Follower(m) => m.halted(),
        }
    }

    /// The selected entries no order keeps, when this replica's selection
    /// held a dependency cycle (task-d21): an invariant violation it
    /// halted on. A candidate finds it before binding; a leader handed
    /// one anyway leads nothing.
    pub fn recovery_cycle(&self) -> Option<&[CommandId]> {
        match self {
            Machine::Leader(m) => m.recovery_cycle(),
            Machine::Follower(m) => m.recovery_cycle(),
        }
    }

    /// A pulled command whose execution here disagreed with its donor's
    /// (task-d08). Only a follower catches up.
    pub fn catch_up_divergence(&self) -> Option<&coord_consensus::CatchUpDivergence> {
        match self {
            Machine::Leader(_) => None,
            Machine::Follower(m) => m.catch_up_divergence(),
        }
    }

    /// Whether this replica holds work it has not executed (task-d08).
    pub fn holds_unexecuted(&self) -> bool {
        match self {
            Machine::Leader(_) => false,
            Machine::Follower(m) => m.holds_unexecuted(),
        }
    }

    /// Whether a catch-up page is in hand (task-d08).
    pub fn catching_up(&self) -> bool {
        match self {
            Machine::Leader(_) => false,
            Machine::Follower(m) => m.catching_up(),
        }
    }

    /// Catch-up pages taken and pulled commands executed (task-d08).
    pub const fn catch_up_counts(&self) -> (u64, u64) {
        match self {
            Machine::Leader(_) => (0, 0),
            Machine::Follower(m) => m.catch_up_counts(),
        }
    }

    /// Ask `donor` for the commands it executed after this replica's
    /// frontier (task-d08). A leader has nobody to ask.
    pub fn request_catch_up(&mut self, donor: coord_types::ids::ReplicaId) -> Vec<Effect> {
        match self {
            Machine::Leader(_) => Vec::new(),
            Machine::Follower(m) => m.request_catch_up(donor),
        }
    }

    /// The configuration this replica votes in.
    pub const fn identity(&self) -> &coord_consensus::ConfigurationIdentity {
        match self {
            Machine::Leader(m) => m.ballots().identity(),
            Machine::Follower(m) => m.ballots().identity(),
        }
    }

    /// Whether this replica is synchronized at `ballot`: it leads it, or
    /// it follows it and installed its Sync (task-d08).
    pub fn synchronized_at(&self, ballot: &Ballot) -> bool {
        match self {
            Machine::Leader(m) => m.config_quorum().ballot() == *ballot,
            Machine::Follower(m) => {
                m.quorum().ballot() == *ballot && m.ballots().synced() == *ballot
            }
        }
    }

    /// Whether this replica is synchronized at `ballot` or at a later
    /// ballot of its epoch: what it executed then includes everything
    /// decided at `ballot`, so it can serve a voter still there its
    /// executed history (task-d33). A voter that promised a ballot of its
    /// own the others never followed, and was refused as behind, asks at
    /// the ballot it last synchronized; a donor answering only its own
    /// ballot left it behind for good.
    pub fn synchronized_at_or_after(&self, ballot: &Ballot) -> bool {
        let at_or_after = |own: Ballot| {
            own.compare_same_epoch(ballot)
                .is_some_and(|o| o != core::cmp::Ordering::Less)
        };
        match self {
            Machine::Leader(m) => at_or_after(m.config_quorum().ballot()),
            Machine::Follower(m) => {
                m.quorum().ballot() == m.ballots().synced() && at_or_after(m.ballots().synced())
            }
        }
    }

    /// The highest ballot this replica refused to a candidate as behind
    /// (task-d10), which its own next campaign has to go above.
    pub fn outranked(&self) -> Option<Ballot> {
        match self {
            Machine::Leader(m) => m.ballots().outranked(),
            Machine::Follower(m) => m.ballots().outranked(),
        }
    }

    /// The highest ballot this replica has promised, counting a promise
    /// whose row is not durable yet (task-d01).
    ///
    /// Counting the one in flight is the point. The store stamps what it
    /// records with the ballot the voter holds, and a promise row stamped
    /// with the ballot before it is a promise recorded under a ballot the
    /// replica had already left -- which a fence at the new ballot then
    /// refuses as obsolete.
    pub fn promised(&self) -> Ballot {
        let ballots = match self {
            Machine::Leader(m) => m.ballots(),
            Machine::Follower(m) => m.ballots(),
        };
        let durable = ballots.promised();
        match ballots.in_flight() {
            Some(pending)
                if pending.ballot.compare_same_epoch(&durable)
                    == Some(core::cmp::Ordering::Greater) =>
            {
                pending.ballot
            }
            _ => durable,
        }
    }

    /// The ballot this replica votes and counts in: the one whose Sync it
    /// adopted, which a promise to a higher ballot does not change until
    /// that ballot's Sync arrives.
    pub fn active(&self) -> Ballot {
        match self {
            Machine::Leader(m) => m.config_quorum().ballot(),
            Machine::Follower(m) => m.quorum().ballot(),
        }
    }

    /// Whether this replica currently leads.
    pub const fn leads(&self) -> bool {
        matches!(self, Machine::Leader(_))
    }

    /// Whether this replica has a campaign of its own under way: still
    /// collecting promises or reports, or bound and not yet active.
    pub const fn campaigning(&self) -> bool {
        match self {
            Machine::Leader(_) => false,
            Machine::Follower(m) => m.campaign_state().is_some(),
        }
    }

    /// The next command whose turn it is to be applied, if any.
    pub fn next_executable(&self) -> Option<CommandId> {
        match self {
            Machine::Leader(m) => m.next_executable(),
            Machine::Follower(m) => m.next_executable(),
        }
    }

    /// Whether this replica holds the payload of `command`.
    pub fn holds_payload(&self, command: &CommandId) -> bool {
        match self {
            Machine::Leader(m) => m.payload(command).is_some(),
            Machine::Follower(m) => m.payload(command).is_some(),
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
    /// Answers to reads the leader read barrier held, each an encoded
    /// `ReadAnswerV1` frame for the collector that sent the read
    /// (task-d50).
    pub reads: Vec<(crate::voter::Origin, Vec<u8>)>,
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
        self.reads.extend(other.reads);
    }

    /// Whether the round asked for nothing.
    pub fn is_empty(&self) -> bool {
        self.peer.is_empty()
            && self.frontend.is_empty()
            && self.arm.is_empty()
            && self.cancel.is_empty()
            && self.views.is_empty()
            && self.entropy.is_empty()
            && self.reads.is_empty()
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
    /// This voter's store is fenced at a promise above the ballot a
    /// transition was stamped with, and the voter cannot serve on: a
    /// leader's own work, or a committed command's apply, refused by the
    /// fence (task-d01). The fence is held in memory only, so a restart
    /// resumes at the promised ballot, as a follower that campaigns.
    Fenced(String),
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
            DriveError::Fenced(e) => write!(f, "fenced at a newer promise: {e}"),
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
    /// Always `Some` outside [`Node::change_role`], which takes it for the
    /// length of a role conversion: the machines convert by value.
    machine: Option<Machine>,
    applier: Applier<P>,
    outbox: Outbox,
    frontend: PeerId,
    /// Rounds of effects carried out since boot (diagnostic).
    pub rounds: u64,
    /// Commands applied since boot (diagnostic).
    pub executed: u64,
    /// Protocol transitions the store refused at submit because it is
    /// fenced at a newer promise (diagnostic, task-d01).
    pub fenced: u64,
    withheld: u64,
    withheld_evidence: u64,
    /// Where the journal and materialization stages are recorded
    /// (task-61), when the process keeps a recorder.
    recorder: Option<std::sync::Arc<Recorder>>,
    /// The command execution is waiting for a payload for, if any.
    awaiting: Option<CommandId>,
    /// The selection this replica won its ballot with, while it leads
    /// that ballot (task-d01). A voter that was away when it was
    /// published is sent it again when it promises the ballot.
    won: Option<SyncDecision>,
    /// This node's executed commands in position order, once a peer has
    /// asked to catch up from them (task-d08).
    order: Option<crate::catch_up::ExecutedOrder>,
    /// Catch-up pages this node served (diagnostic, task-d08).
    pub pages_served: u64,
    /// Storage facts reached the outbox outside a round, so it may hold
    /// sends they released that no round has handed out yet.
    unreleased: bool,
    /// The forgetting floor this voter agrees with its peers, when it
    /// takes part (task-d27).
    floor: Option<crate::floor::Floor>,
    /// Whether this node lowers in groups (task-d47): a round leaves what
    /// it persisted queued for [`Node::flush`], and execution applies the
    /// commands whose turn has come as groups.
    grouped: bool,
    /// Whether a [`Node::flush`] is running: its rounds lower what they
    /// persist rather than leave it queued.
    flushing: bool,
    /// What the commands of a staged execution group led to, carried out
    /// once [`Node::finish`] has materialized the group (task-d47).
    staged: Vec<Effect>,
    /// What groups handed to a pipelined store led to, each held until the
    /// projection has committed through its position (task-d52), oldest
    /// first.
    releases: VecDeque<(ExecutionPosition, Vec<Effect>)>,
    /// What this replica's re-sends did, over every ballot it led since
    /// boot (task-d49).
    resends: coord_consensus::ResendCounts,
    /// Commands this replica established, by the path that decided them
    /// (task-d50).
    pub established: Established,
    /// What the fast path did since boot (task-d62): why each command
    /// established on the slow path missed it, counted as its
    /// establishment is, and the acknowledgements of every role this
    /// replica left.
    fast_path: coord_consensus::FastPathCounts,
    /// From learned to released, per command this replica led (task-d62).
    release: crate::learned::ReleaseTiming,
    /// When each pre-acceptance the leader ordered a later command past
    /// was first seen (task-d62).
    unordered_since: std::collections::BTreeMap<CommandId, std::time::Instant>,
}

/// The pre-acceptances a replica holds that the leader has not ordered
/// (task-d62), as last observed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unordered {
    /// Pre-acceptances the leader has not ordered.
    pub pending: u64,
    /// Of them, those it has ordered a later command past.
    pub reordered: u64,
    /// How long the oldest of those has been seen, at the observation's
    /// resolution.
    pub oldest: std::time::Duration,
    /// For a leader, the commands its own path log holds pending
    /// (task-d68).
    pub leader_log: u64,
}

/// Commands a replica established since boot, by the learning path that
/// decided them (task-d50).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Established {
    /// Decided by a fast quorum's matching dependency paths.
    pub fast: u64,
    /// Decided by a majority of adoptions.
    pub slow: u64,
}

/// Most commands one execution group applies before it is lowered
/// (task-d47). Half the journal group's 64 records, so the protocol's
/// batches queued with them still fit beside them in one append.
const GROUP_COMMANDS: usize = 32;

/// Queued batches past which execution lowers what is queued before it
/// applies more (task-d47): half of `JournalLimits`' default 256 per
/// domain.
const GROUP_QUEUE_ROOM: usize = 128;

/// Durable records a pipelined store may owe its projection before
/// execution waits for the materializer (task-d52): four journal groups.
/// Past it the learner is held, not dropped: the domain thread drains the
/// commit out, and the next group is planned over what it committed.
const PIPELINE_RECORDS: usize = 256;

impl<P: Persistence> Node<P> {
    /// A node over `machine` and `applier`, publishing to `frontend`.
    pub fn new(machine: Machine, mut applier: Applier<P>, frontend: PeerId) -> Self {
        // The machine shares the applier's store: what the applier's
        // lowerings make durable for the machine's batches is handed back.
        applier.share_foreign();
        let boot = applier.store().boot();
        let mut node = Node {
            machine: Some(machine),
            applier,
            outbox: Outbox::new(boot),
            frontend,
            rounds: 0,
            executed: 0,
            fenced: 0,
            withheld: 0,
            withheld_evidence: 0,
            recorder: None,
            awaiting: None,
            won: None,
            order: None,
            pages_served: 0,
            unreleased: false,
            floor: None,
            grouped: false,
            flushing: false,
            staged: Vec::new(),
            releases: VecDeque::new(),
            resends: coord_consensus::ResendCounts::default(),
            established: Established::default(),
            fast_path: coord_consensus::FastPathCounts::default(),
            release: crate::learned::ReleaseTiming::default(),
            unordered_since: std::collections::BTreeMap::new(),
        };
        node.machine_mut().observe_learning();
        node
    }

    /// Whether a command's turn has come and this replica holds its
    /// payload: whether [`Node::execute`] would apply something now.
    pub fn can_execute(&self) -> bool {
        let machine = self.machine();
        machine
            .next_executable()
            .is_some_and(|command| machine.holds_payload(&command))
    }

    /// Whether this node lowers in groups ([`Node::lower_in_groups`]).
    pub const fn lowers_in_groups(&self) -> bool {
        self.grouped
    }

    /// Lower in groups (task-d47, design Section 17.3.3).
    ///
    /// A round then submits what its machine persisted and leaves it
    /// queued: [`Node::flush`] lowers everything queued as one group,
    /// one journal write and one projection transaction, and releases the
    /// sends that waited on it. The runtime flushes once it has no more
    /// events ready, or after a bounded number of them. Execution applies
    /// the commands whose turn has come as groups of up to
    /// `GROUP_COMMANDS` and lowers each group once
    /// ([`Applier::begin_group`]).
    ///
    /// Nothing is released earlier than before: a send still waits on the
    /// barrier it names, and a group's results are handed out only once
    /// the group has materialized. Things are released later, by up to the
    /// rest of the group.
    pub fn lower_in_groups(&mut self) {
        self.grouped = true;
    }

    /// Lower everything queued as one group and hand what that made
    /// durable to the outbox and the machine (task-d47).
    ///
    /// What the machine does in answer is carried out at once, its own
    /// batches lowered with it, so a flush leaves nothing it produced
    /// queued.
    ///
    /// Lowering in groups, a flush journals and does not materialize: a
    /// send waits on its batch's journal durability and on nothing else,
    /// and the projection takes what the flush journaled with the next
    /// execution group, which materializes anyway. When no command is
    /// ready to execute, the flush materializes it itself, so the
    /// projection never waits on execution that is not coming.
    pub fn flush(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let was = core::mem::replace(&mut self.flushing, true);
        let flushed = self.flush_queued(ballot);
        self.flushing = was;
        flushed
    }

    fn flush_queued(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let mut next = Vec::new();
        self.lower_queued(&mut next)?;
        // A round releases what became durable, even with nothing more
        // to carry out.
        self.unreleased = true;
        let mut out = self.carry_out(next, ballot)?;
        if self.applier.pipelined() {
            // The projection follows the journal from the materializer's
            // thread: what this flush journaled is handed to it, and what
            // it has committed is released (task-d52).
            if !self.applier.in_group() {
                out.absorb(self.settle(ballot)?);
            }
            return Ok(out);
        }
        if self.grouped
            && !self.applier.in_group()
            && self.applier.store().unmaterialized() > 0
            && !self.can_execute()
        {
            let mut next = Vec::new();
            self.lower_once(&mut next, Lowering::Lower)?;
            out.absorb(self.carry_out(next, ballot)?);
        }
        Ok(out)
    }

    /// Lower until nothing is queued, or a lowering moves nothing.
    ///
    /// One lowering takes everything queued that fits one journal group.
    /// The bound is the queue's own depth when this started, and a
    /// lowering that moves nothing -- an append still out on the
    /// appender's thread (task-d54), an uncertain head -- ends the loop
    /// rather than spinning; the next round lowers again. Lowering in
    /// groups, it journals only ([`Node::flush`]).
    fn lower_queued(&mut self, next: &mut Vec<Effect>) -> Result<(), DriveError> {
        let mut attempts = self.applier.store().queued() + 1;
        while self.applier.store().queued() > 0 && attempts > 0 {
            let before = self.applier.store().queued();
            let lowering = if self.grouped {
                Lowering::Journal
            } else {
                Lowering::Lower
            };
            self.lower_once(next, lowering)?;
            if self.applier.store().queued() >= before {
                break;
            }
            attempts -= 1;
        }
        Ok(())
    }

    /// Take part in agreeing a forgetting floor (task-d27).
    pub fn keep_floor(&mut self, floor: crate::floor::Floor) {
        self.floor = Some(floor);
    }

    /// The forgetting floor, when this voter takes part.
    pub const fn floor(&self) -> Option<&crate::floor::Floor> {
        self.floor.as_ref()
    }

    /// A peer's promise about a floor boundary, over its own link: its
    /// row, and the floor it may activate, are journaled.
    pub fn hear_floor(
        &mut self,
        from: coord_types::ids::ReplicaId,
        readiness: &[u8],
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let Some(floor) = self.floor.as_mut() else {
            return Ok(Outbound::default());
        };
        let Ok(gated) = self.applier.store().reader().snapshot() else {
            return Ok(Outbound::default());
        };
        let Some(updates) = floor.hear(gated.view(), from, readiness) else {
            return Ok(Outbound::default());
        };
        drop(gated);
        let effects = floor_effects(floor, updates, None, ballot);
        self.carry_out(effects, ballot)
    }

    /// Hand the floor what became of its batches, so it reclaims the
    /// images superseded by one that is durable, and only then.
    /// The batch to journal when this voter's own promise, now durable,
    /// activates a floor with the promises heard while it was in flight.
    fn settle_floor(&mut self, ballot: &Ballot) -> Vec<Effect> {
        let Some(floor) = self.floor.as_mut() else {
            return Vec::new();
        };
        let outbox = &self.outbox;
        match floor.settle(|b| outbox.is_durable(b), |b| outbox.is_failed(b)) {
            Some(updates) => floor_effects(floor, updates, None, ballot),
            None => Vec::new(),
        }
    }

    /// Export, keep and promise the checkpoint at the boundary this
    /// node just applied.
    fn floor_boundary(
        &mut self,
        position: coord_types::ids::ExecutionPosition,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let Some(floor) = self.floor.as_mut() else {
            return Ok(Outbound::default());
        };
        let gated = match self.applier.store().reader().snapshot() {
            Ok(gated) => gated,
            Err(e) => {
                floor.refuse(crate::floor::FloorRefusal::Export(format!("{e:?}")));
                return Ok(Outbound::default());
            }
        };
        let Some(promised) = floor.boundary(gated.view(), position) else {
            return Ok(Outbound::default());
        };
        drop(gated);
        let effects = floor_effects(floor, promised.updates, Some(promised.readiness), ballot);
        self.carry_out(effects, ballot)
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
    pub fn machine(&self) -> &Machine {
        self.machine
            .as_ref()
            .expect("a node always holds a machine")
    }

    fn machine_mut(&mut self) -> &mut Machine {
        self.machine
            .as_mut()
            .expect("a node always holds a machine")
    }

    /// Start a campaign for `ballot`, which names this replica as its
    /// leader, and carry out what that produced (task-d01).
    ///
    /// Only a follower campaigns: a leader already leads, and its ballot
    /// is the one a campaign would be trying to replace. The caller moves
    /// the voter's ballot to `ballot` first, so the promise this replica
    /// makes itself is stamped with the ballot it promises.
    pub fn campaign(&mut self, ballot: Ballot, at: &Ballot) -> Result<Outbound, DriveError> {
        let effects = match self.machine_mut() {
            Machine::Follower(f) => f.campaign(ballot),
            Machine::Leader(_) => Vec::new(),
        };
        self.carry_out(effects, at)
    }

    /// Publish a selection this replica bound durably before it last
    /// stopped, and carry out what that produced.
    ///
    /// A campaign that bound its Sync and then crashed must publish that
    /// Sync and no other at its ballot (task-26); this is how a restart
    /// finishes it rather than leaving the bound selection unpublished.
    pub fn resume_campaign(
        &mut self,
        decision: SyncDecision,
        at: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let effects = match self.machine_mut() {
            Machine::Follower(f) => f.resume_campaign(decision),
            Machine::Leader(_) => Vec::new(),
        };
        self.carry_out(effects, at)
    }

    /// Change role when the machine says so, and carry out what the
    /// change produced; `None` when nothing changed (task-d01).
    ///
    /// The machines decide and never convert themselves: a follower that
    /// won its campaign reports it (`won`), and a leader that promised a
    /// higher ballot reports that it is `deposed`. The conversion is the
    /// driver's, and it happens here, in place, with the store, the outbox
    /// and every connection carrying on -- the same replica at a different
    /// ballot, which is what [`Machine`] says a role is. A deposed leader
    /// that already holds the new leader's Sync replays it into the
    /// follower it becomes, so the selection is adopted rather than lost.
    pub fn change_role(&mut self, at: &Ballot) -> Result<Option<Outbound>, DriveError> {
        let change = match self.machine() {
            Machine::Follower(f) => f.won().is_some(),
            Machine::Leader(l) => l.deposed(),
        };
        if !change {
            return Ok(None);
        }
        let counts = self.machine_mut().take_resend_counts();
        self.resends.add(&counts);
        self.leave_role();
        let machine = self.machine.take().expect("a node always holds a machine");
        let (machine, effects) = match machine {
            Machine::Follower(f) => {
                let decision = f.won().cloned().expect("checked above");
                let quorum = f.quorum().clone();
                let (leader, effects) =
                    Leader::from_recovered(f.into_recovered(), quorum, &decision);
                self.won = Some(decision);
                (Machine::Leader(Box::new(leader)), effects)
            }
            Machine::Leader(l) => {
                let quorum = l.config_quorum();
                let pending = l.pending_sync().cloned();
                let mut follower = Follower::from_recovered(l.into_recovered(), quorum);
                self.won = None;
                let effects = match pending {
                    Some((from, decision)) => follower.on_sync(from, decision),
                    None => Vec::new(),
                };
                (Machine::Follower(Box::new(follower)), effects)
            }
        };
        self.machine = Some(machine);
        self.machine_mut().observe_learning();
        self.carry_out(effects, at).map(Some)
    }

    /// Keep what the role about to be replaced counted (task-d62). The
    /// commands it had learned and not released are not timed: the role
    /// that would release them is going.
    fn leave_role(&mut self) {
        let counts = self.machine_mut().take_fast_path_counts();
        self.fast_path.add(&counts);
        self.release.clear();
    }

    /// What the fast path did since boot (task-d62).
    pub fn fast_path_counts(&self) -> coord_consensus::FastPathCounts {
        let mut counts = self.fast_path;
        counts.add(&self.machine().fast_path_counts());
        counts
    }

    /// From learned to released, over the commands this replica led
    /// (task-d62).
    pub const fn release_split(&self) -> crate::learned::ReleaseSplit {
        self.release.split()
    }

    /// Look at the pre-acceptances this replica holds that the leader
    /// has not ordered, at `now` (task-d62). The age of one the leader
    /// ordered a later command past runs from the first observation that
    /// saw it, so it is as fine as the observations are frequent.
    pub fn observe_unordered(&mut self, now: std::time::Instant) -> Unordered {
        let (pending, reordered) = self.machine().unordered();
        self.unordered_since.retain(|c, _| reordered.contains(c));
        for command in &reordered {
            self.unordered_since.entry(*command).or_insert(now);
        }
        Unordered {
            leader_log: self.machine().leader_log_pending() as u64,
            pending: pending as u64,
            reordered: reordered.len() as u64,
            oldest: self
                .unordered_since
                .values()
                .min()
                .map_or(std::time::Duration::ZERO, |since| {
                    now.saturating_duration_since(*since)
                }),
        }
    }

    /// Stamp what the leader committed since the last stamp (task-d62).
    fn stamp_learned(&mut self) {
        let learned = self.machine_mut().take_learned();
        if !learned.is_empty() {
            self.release.learned(learned, std::time::Instant::now());
        }
    }

    /// Give up the lead without having been deposed, so that this replica
    /// can campaign above a ballot it refused as behind (task-d10).
    ///
    /// The conversion is the one a deposed leader makes in
    /// [`Node::change_role`]: the same replica at the same ballot, as a
    /// follower, with its promises, table and outbox carried over. What
    /// the leader had proposed and not decided is recovered by the
    /// campaign that follows, as after any change of leader. A follower
    /// is left as it is.
    pub fn step_down(&mut self, at: &Ballot) -> Result<Outbound, DriveError> {
        if !matches!(self.machine(), Machine::Leader(_)) {
            return Ok(Outbound::default());
        }
        let counts = self.machine_mut().take_resend_counts();
        self.resends.add(&counts);
        self.leave_role();
        let Some(Machine::Leader(l)) = self.machine.take() else {
            unreachable!("checked above")
        };
        let quorum = l.config_quorum();
        let follower = Follower::from_recovered(l.into_recovered(), quorum);
        self.won = None;
        self.machine = Some(Machine::Follower(Box::new(follower)));
        self.carry_out(Vec::new(), at)
    }

    /// The selection this replica won the ballot it leads with; `None`
    /// for a leader of the genesis ballot, which nobody campaigned for.
    pub const fn won(&self) -> Option<&SyncDecision> {
        self.won.as_ref()
    }

    /// What this replica's re-sends did since boot (task-d49).
    pub fn resend_counts(&self) -> coord_consensus::ResendCounts {
        let mut counts = self.resends;
        if let Machine::Leader(m) = self.machine() {
            counts.add(&m.resend_counts());
        }
        counts
    }

    /// Take what the machine refused since the last call, rendered.
    pub fn take_rejections(&mut self) -> Vec<String> {
        self.machine_mut().take_rejections()
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
        let effects = self.machine_mut().propose_service(frame);
        self.carry_out(effects, ballot)
    }

    /// Whether this replica is holding a command it knows by identity
    /// and not by content.
    pub fn wants_payloads(&self) -> bool {
        self.machine().wants_payloads()
    }

    /// How many commands this replica knows by identity and not by
    /// content.
    pub fn missing_payloads(&self) -> usize {
        self.machine().missing_payloads()
    }

    /// How many payload transfers a peer has answered this replica
    /// with.
    pub fn payloads_answered(&self) -> u64 {
        self.machine().payloads_answered()
    }

    /// Send the voters, again, the proposals they have not voted on
    /// (task-d07), and the commit frontier (task-d09).
    pub fn resend_proposals(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let effects = self
            .machine_mut()
            .resend_unvoted(coord_consensus::RESEND_PER_VOTER);
        self.carry_out(effects, ballot)
    }

    /// Ask `from` for the payloads this replica lacks.
    pub fn request_payloads(
        &mut self,
        from: coord_types::ids::ReplicaId,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let effects = self.machine_mut().request_payloads(from);
        self.carry_out(effects, ballot)
    }

    /// Ask the voters that promised this replica's campaign for the report
    /// pages that have not arrived (task-d28).
    pub fn request_report_pages(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let effects = self.machine_mut().request_report_pages();
        self.carry_out(effects, ballot)
    }

    /// Ask `donor` for the commands it executed after this replica's
    /// frontier (task-d08).
    pub fn request_catch_up(
        &mut self,
        donor: coord_types::ids::ReplicaId,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let effects = self.machine_mut().request_catch_up(donor);
        self.carry_out(effects, ballot)
    }

    /// Answer `from`'s ask for what this node executed after `after`, at
    /// `ballot` (task-d08).
    ///
    /// Answered only to a voter of this configuration other than this
    /// one, and only by a donor synchronized at the ballot the requester
    /// asked at, that has not itself stopped: a page from anyone else, or
    /// at another ballot, would be dropped by the requester anyway. What
    /// is served is what this node executed and holds durable rows for,
    /// no further than its own frontier.
    pub fn serve_catch_up(
        &mut self,
        from: coord_types::ids::ReplicaId,
        ballot: Ballot,
        after: coord_types::ids::ExecutionPosition,
    ) -> Outbound {
        let mut out = Outbound::default();
        let machine = self.machine();
        let identity = machine.identity();
        if from == identity.replica
            || !identity.voters.contains(&from)
            || !machine.synchronized_at_or_after(&ballot)
            || machine.halted().is_some()
            || machine.catch_up_divergence().is_some()
        {
            return out;
        }
        let epoch = identity.epoch;
        let through = machine.executed_through();
        if after >= through {
            return out;
        }
        if self.order.is_none() {
            self.order = crate::catch_up::ExecutedOrder::read(&self.applier);
        }
        let Some(order) = self.order.as_ref() else {
            return out;
        };
        let entries = crate::catch_up::page(&self.applier, order, epoch, after, through);
        if entries.is_empty() {
            return out;
        }
        self.pages_served += 1;
        out.peer.push((
            PeerId {
                replica: from,
                incarnation: coord_types::ids::ReplicaIncarnation::ZERO,
            },
            coord_consensus::ProtocolMessage::CatchUpPage {
                ballot,
                after,
                through,
                entries,
            }
            .encode(),
        ));
        out
    }

    /// Give the applier back; this node's boot ends where it stands
    /// (harnesses that end a boot at a chosen point and reopen its store).
    pub fn into_applier(self) -> Applier<P> {
        self.applier
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
        self.outbox.held()
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
        let effects = self.machine_mut().step(event);
        self.carry_out(effects, ballot)
    }

    /// Carry out `effects`, and everything they lead to.
    fn carry_out(&mut self, effects: Vec<Effect>, ballot: &Ballot) -> Result<Outbound, DriveError> {
        self.stamp_learned();
        let mut out = Outbound::default();
        let mut queue = effects;
        self.absorb_foreign(&mut queue);
        // A round releases what became durable, so one runs even with
        // nothing to carry out when the facts arrived outside a round.
        let mut release = core::mem::take(&mut self.unreleased);
        // A bound on rounds, not on work: every round must make progress
        // through storage, and a machine that answered its own storage
        // facts with more storage facts for ever would otherwise spin
        // here rather than be visible as a fault.
        for _ in 0..MAX_ROUNDS {
            if queue.is_empty() && !release {
                queue = self.settle_floor(ballot);
                if queue.is_empty() {
                    return Ok(out);
                }
                continue;
            }
            release = false;
            self.rounds += 1;
            self.stamp_learned();
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
        let mut refused = Vec::new();
        let mut next = Vec::new();
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
                    let barrier_id = batch.barrier;
                    self.make_room(&batch, &mut next)?;
                    match self
                        .applier
                        .store_mut()
                        .submit(batch, TransitionKind::Protocol)
                    {
                        Ok(()) => persisted = true,
                        // The store is fenced at a promise above the ballot
                        // this is stamped with: the voter's promise did not
                        // become durable and it went back to the ballot it
                        // had, and the fence did not (`Voter::follow_machine`).
                        // The transition will never be durable here, which
                        // is what the fence says of the work it refuses from
                        // the queue, and it is said the same way: the
                        // barrier fails and the machine hears so. A refusal
                        // the voter expects is not a reason to stop serving.
                        // A leader does not serve on from here. Every batch
                        // it makes is stamped below the fence and refused
                        // the same way, so it would stop proposing while
                        // still leading, and neither it nor the voters that
                        // hold its links would campaign: the domain would
                        // stall. Ended instead, it restarts as a follower
                        // of its ballot and campaigns.
                        Err(Refused::Fenced) if self.machine().leads() => {
                            return Err(DriveError::Fenced(
                                "a transition this voter made while leading".into(),
                            ));
                        }
                        Err(Refused::Fenced) => {
                            self.fenced += 1;
                            refused.push(StorageEvent::Failed {
                                barrier_id,
                                error: StorageError::DefinitelyNotCommitted,
                            });
                        }
                        Err(e) => return Err(DriveError::Submit(e.to_string())),
                    }
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
                Effect::Established(result) => {
                    let command = result.command();
                    if result.fast_path() {
                        self.established.fast += 1;
                    } else {
                        self.established.slow += 1;
                        let reason = self.machine_mut().take_missed_fast(&command);
                        self.fast_path.missed(reason);
                    }
                    self.release.released(&command, std::time::Instant::now());
                }
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

        for event in refused {
            self.outbox.observe(&event);
            next.extend(self.machine_mut().step(Event::Storage(event)));
        }
        // Lowered here, everything this round submitted: nothing comes
        // back for a queued batch on its own, so whatever was left queued
        // would never become durable unless something else was persisted
        // after it, and the vote it carried would never be released.
        //
        // Lowering in groups (task-d47), the round leaves it queued
        // instead, and the runtime's `Node::flush` lowers it with
        // whatever the events after this one queue.
        if persisted && (!self.grouped || self.flushing) {
            self.lower_queued(&mut next)?;
        }
        self.withheld += self.outbox.held() as u64;
        self.withheld_evidence += self.outbox.held_to(&self.frontend) as u64;
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

    /// Lower one group and hand what it made durable to the outbox and
    /// the machine; the machine's answers join `next`. How it lowers is
    /// `lowering`'s.
    fn lower_once(&mut self, next: &mut Vec<Effect>, lowering: Lowering) -> Result<(), DriveError> {
        let store = self.applier.store_mut();
        let mut outcome = Self::measured(
            self.recorder.as_deref(),
            Stage::Journal,
            || match lowering {
                Lowering::Lower => store.lower(),
                Lowering::Journal => store.journal(),
                Lowering::TakeBack => store.take_back(),
                Lowering::Await => store.finish_append(),
            },
        )
        .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
        // Indeterminate is not "failed": the group's outcome is unknown,
        // and what settles it is the store's own record, read back by a
        // reconcile, never an assumption either way. It is settled here,
        // as the applier settles its own. Left for later, the store
        // refused every protocol batch after it as not ready, and the
        // only reconcile that ran was the applier's, when a command next
        // executed: on a replica whose next command waits on the very
        // votes that batch carried, never.
        if outcome.indeterminate {
            let settled = self
                .applier
                .store_mut()
                .reconcile()
                .map_err(|e| DriveError::Engine(format!("{e:?}")))?;
            outcome.events.extend(settled.events);
            outcome.indeterminate = settled.indeterminate;
        }
        for event in outcome.events {
            // A staged execution group's batches are journaled with the
            // protocol's (task-d47); they are the applier's to complete,
            // and none of the machine's. One definitely not journaled is
            // a command reported applied that will not be.
            if event
                .barrier()
                .is_some_and(|barrier| coord_core::outbox::is_application(&barrier))
            {
                if matches!(
                    event,
                    StorageEvent::Failed {
                        error: coord_core::event::StorageError::DefinitelyNotCommitted,
                        ..
                    }
                ) {
                    return Err(DriveError::Engine(
                        "an execution group was definitely not journaled".into(),
                    ));
                }
                continue;
            }
            self.outbox.observe(&event);
            next.extend(self.machine_mut().step(Event::Storage(event)));
        }
        // Still unknown after a reconcile: an engine failure, so it
        // cannot be mistaken for a clean round.
        if outcome.indeterminate {
            return Err(DriveError::Engine("group outcome indeterminate".into()));
        }
        Ok(())
    }

    /// Hand the storage facts the applier met about other batches to the
    /// outbox and the machine; the machine's answers join `next`.
    ///
    /// Applying a command lowers whatever the store had queued, and
    /// reconciling settles whatever was uncertain, the protocol's
    /// batches with the application's. The facts about those batches are
    /// this node's to deliver, and the sends they release go out with
    /// the next round.
    fn absorb_foreign(&mut self, next: &mut Vec<Effect>) {
        for event in self.applier.take_foreign() {
            self.unreleased = true;
            self.outbox.observe(&event);
            next.extend(self.machine_mut().step(Event::Storage(event)));
        }
    }

    /// Lower what is queued until the store has room for `batch`.
    ///
    /// One step of a machine can ask for more batches than the queue
    /// holds. Installing a Sync writes a row per selected command and a
    /// new leader re-proposes each of them, and a selection names what
    /// every reporter still holds, which on a domain that has served for
    /// a while is far more than the queue's depth. Refused as full, the
    /// voter stopped -- the new leader, or the follower installing its
    /// Sync, exactly when the domain was electing one -- and it met the
    /// same selection when it came back. The queue drains by lowering,
    /// which the end of the round does anyway; it happens here as well,
    /// as often as the batch needs. A lowering that moves nothing leaves
    /// the refusal to `submit`, as before.
    fn make_room(
        &mut self,
        batch: &PersistBatch,
        next: &mut Vec<Effect>,
    ) -> Result<(), DriveError> {
        while !self.applier.store().has_room(batch) {
            let before = self.applier.store().queued();
            if before == 0 {
                break;
            }
            // With the journal's appends on the appender's thread
            // (task-d54), the room is made by lending the next group, as
            // a flush would: this thread waits for the append out, if one
            // is, and not for the next group's sync as well.
            if self.applier.store().journal_pipelined() {
                if self.applier.store().appending() {
                    self.lower_once(next, Lowering::Await)?;
                }
                self.lower_once(next, Lowering::Journal)?;
            } else {
                self.lower_once(next, Lowering::Lower)?;
            }
            if self.applier.store().queued() >= before {
                break;
            }
        }
        Ok(())
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
        self.run_executions(ballot, false)
    }

    /// Apply the commands whose turn has come, as [`Node::execute`] does,
    /// but leave the last group staged: submitted, not lowered, and its
    /// results held (task-d47). A [`Node::flush`] then journals it with
    /// the protocol's batches in one append, and [`Node::finish`]
    /// materializes it and hands out its results.
    pub fn stage(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        self.run_executions(ballot, true)
    }

    /// Materialize the staged execution group, if there is one, and carry
    /// out what its commands led to (task-d47).
    pub fn finish(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        if !self.applier.in_group() {
            return Ok(Outbound::default());
        }
        let mut held = core::mem::take(&mut self.staged);
        self.close_group(&mut held, ballot)
    }

    fn run_executions(&mut self, ballot: &Ballot, stage: bool) -> Result<Outbound, DriveError> {
        let mut out = Outbound::default();
        // What the commands of an open group led to: carried out once the
        // group has materialized, never before (task-d47).
        let mut held: Vec<Effect> = core::mem::take(&mut self.staged);
        loop {
            let Some(command) = self.machine().next_executable() else {
                if self.applier.in_group() {
                    if stage {
                        self.staged = held;
                        break;
                    }
                    // What the group led to may let more commands execute.
                    out.absorb(self.close_group(&mut held, ballot)?);
                    continue;
                }
                break;
            };
            // A command whose turn has come but whose payload this
            // replica does not hold is not something to skip past: the
            // order is the whole of the guarantee, and going on would
            // apply a later command first.
            let Some(payload) = self.machine().payload(&command) else {
                // Not something to skip past -- the order is the whole
                // of the guarantee -- and not a fault either. A replica
                // learns a command's identity from evidence and its
                // content from a submission or a peer, and the two can
                // arrive in either order; a command the leader proposed
                // out of its own scheduler has no submission coming at
                // all. So execution stops here, the command is named,
                // and the runtime asks for what it is missing.
                self.awaiting = Some(command);
                if self.applier.in_group() {
                    if stage {
                        self.staged = held;
                    } else {
                        out.absorb(self.close_group(&mut held, ballot)?);
                    }
                }
                return Ok(out);
            };
            self.awaiting = None;
            if self.applier.grouped() >= GROUP_COMMANDS {
                out.absorb(self.close_group(&mut held, ballot)?);
            }
            // The protocol's batches a runtime lowering in groups left
            // queued go first when they fill half the store's queue, so an
            // application batch is never refused as the queue is full.
            if self.grouped && self.applier.store().queued() >= GROUP_QUEUE_ROOM {
                out.absorb(self.close_group(&mut held, ballot)?);
                out.absorb(self.flush(ballot)?);
            }
            let grouping = self.grouped && self.applier.begin_group();
            let applier = &mut self.applier;
            let outcome =
                match Self::measured(self.recorder.as_deref(), Stage::Materialization, || {
                    applier.apply(command, &payload)
                }) {
                    // The group and the protocol's queued batches leave no
                    // room for this command's batch: lower them, and apply
                    // the same command again (task-d47). Nothing of it was
                    // submitted.
                    Err(coord_storage::ApplyError::GroupFull) => {
                        out.absorb(self.close_group(&mut held, ballot)?);
                        out.absorb(self.flush(ballot)?);
                        continue;
                    }
                    applied => applied.map_err(|e| Self::apply_error(&command, &e))?,
                };
            self.executed += 1;
            if let Some(order) = self.order.as_mut()
                && !order.note(outcome.position, command)
            {
                self.order = None;
            }
            // What the apply's lowerings made durable for other batches
            // is delivered by `carry_out`, after the command's own
            // outcome. This order is an observation, not a mechanism.
            // `applied` depends on none of those facts, so taking the
            // outcome first is safe. Delivered before it, under load a
            // lagging voter's callers went unanswered
            // (`a_replica_that_falls_behind_...`: 18 of 24 runs four at a
            // time on four cores, against 3 of 16 on the base and 4 of 24
            // delivered here), and why is not established. The likeliest
            // loss is `Leader::applied`, which releases a result only
            // while leading and never retries a release it skipped; that
            // gap, and the base's 3 of 16, are open in the notes ("A
            // result the leader executed while not leading").
            //
            // Within a group the machine hears the outcome now, which is
            // what lets it name the next command, and what it answers is
            // held until the group has materialized: a release, a send
            // and a result all wait for the group (task-d47).
            let effects = self.machine_mut().applied(command, &outcome)?;
            self.release.applied(&command, std::time::Instant::now());
            self.stamp_learned();
            // At a floor boundary the view is exactly the command's: the
            // apply returned once its batch was readable, and only a
            // command moves the frontier (task-d27). A retry answered from
            // the record carries its original position, behind the
            // frontier, and is no boundary. A group ends at the boundary,
            // so it too is read with nothing after it applied.
            let frontier = self.machine().executed_through();
            let boundary = self
                .floor
                .as_ref()
                .is_some_and(|floor| floor.due(outcome.position, frontier));
            if grouping {
                held.extend(effects);
                if boundary {
                    out.absorb(self.close_group_waiting(&mut held, ballot)?);
                }
            } else {
                out.absorb(self.carry_out(effects, ballot)?);
            }
            if boundary {
                out.absorb(self.floor_boundary(outcome.position, ballot)?);
            }
        }
        Ok(out)
    }

    /// Close the open execution group and carry out what its commands led
    /// to once it has materialized (task-d47).
    ///
    /// On a pipelined store (task-d52) the group is journaled and its
    /// projection commit handed to the materializer, and what it led to is
    /// held until the projection has committed through it
    /// ([`Node::settle`]). Execution goes on meanwhile, planning over the
    /// groups in flight. Past `PIPELINE_RECORDS` owed it waits for them.
    fn close_group(
        &mut self,
        held: &mut Vec<Effect>,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        self.stamp_closed(held);
        if !self.applier.pipelined() {
            return self.close_group_waiting(held, ballot);
        }
        let applier = &mut self.applier;
        let position = Self::measured(self.recorder.as_deref(), Stage::Journal, || {
            applier.hand_off_group()
        })
        .map_err(Self::group_error)?;
        let effects = core::mem::take(held);
        let mut out = match position {
            Some(position) => {
                self.releases.push_back((position, effects));
                Outbound::default()
            }
            None if self.releases.is_empty() => self.carry_out(effects, ballot)?,
            // Nothing of its own in flight, but what it led to still comes
            // after what the groups before it led to.
            None => {
                let last = self.releases.back_mut().expect("checked above");
                last.1.extend(effects);
                Outbound::default()
            }
        };
        out.absorb(self.release_through_settled(ballot)?);
        if self.applier.store().unmaterialized() > PIPELINE_RECORDS {
            out.absorb(self.drain(ballot)?);
        }
        Ok(out)
    }

    /// Take back what a pipelined store's materializer has committed and
    /// carry out what the groups it completed led to (task-d52).
    ///
    /// The runtime calls this when the materializer says a commit has
    /// finished, and a flush calls it too. A result, a send or a watch
    /// event of a group goes out here and never before the projection has
    /// committed the group: the same rule as a group materialized on this
    /// thread, kept across the hand-off.
    ///
    /// A journal append out on the appender's thread (task-d54) is taken
    /// back here as well, once it has finished: what it made durable
    /// releases the sends that waited on it and its facts go to the
    /// machine. What was queued meanwhile is not lent here: the runtime's
    /// next flush lends it, once the events that arrived during the sync
    /// have been taken in, so the group holds all of them -- as a sync on
    /// this thread left them all to the flush after it.
    pub fn settle(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let mut out = Outbound::default();
        if self.applier.store().appending() {
            let mut next = Vec::new();
            self.lower_once(&mut next, Lowering::TakeBack)?;
            self.unreleased = true;
            out.absorb(self.carry_out(next, ballot)?);
        }
        if self.applier.pipelined() {
            out.absorb(self.release_through_settled(ballot)?);
        }
        Ok(out)
    }

    /// Flush, then wait on this thread until no journal append is out
    /// (task-d54): each append out is taken back, what it made durable is
    /// carried out, and what that queued is flushed in turn. What was
    /// queued when this was called is journaled when it returns, as a
    /// flush that appends on this thread leaves it, unless a lowering
    /// moved nothing.
    ///
    /// For a caller about to act on what the journal holds rather than on
    /// what it will: a fence tests what is queued against the frontier
    /// the journal reaches, and a group out is in no frontier until it is
    /// taken back.
    pub fn flush_journaled(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let mut out = self.flush(ballot)?;
        while self.applier.store().appending() {
            let mut next = Vec::new();
            self.lower_once(&mut next, Lowering::Await)?;
            self.unreleased = true;
            out.absorb(self.carry_out(next, ballot)?);
            out.absorb(self.flush(ballot)?);
        }
        Ok(out)
    }

    /// Wait for every group handed off to materialize, and carry out what
    /// they led to (task-d52).
    pub fn drain(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        if !self.applier.pipelined() {
            return Ok(Outbound::default());
        }
        let applier = &mut self.applier;
        let through = Self::measured(self.recorder.as_deref(), Stage::Materialization, || {
            applier.drain()
        })
        .map_err(Self::group_error)?;
        self.release_through(through, ballot)
    }

    /// Groups handed off whose results are still held (task-d52).
    pub fn awaiting_materialization(&self) -> usize {
        self.releases.len()
    }

    fn release_through_settled(&mut self, ballot: &Ballot) -> Result<Outbound, DriveError> {
        let through = self.applier.settle().map_err(Self::group_error)?;
        self.release_through(through, ballot)
    }

    /// Carry out what the groups handed off through `through` led to, in
    /// the order they were handed off.
    fn release_through(
        &mut self,
        through: ExecutionPosition,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        let mut effects = Vec::new();
        while self
            .releases
            .front()
            .is_some_and(|(position, _)| *position <= through)
        {
            let (_, released) = self.releases.pop_front().expect("checked above");
            effects.extend(released);
        }
        // Run even with nothing released: the materializer's facts about
        // the protocol's batches are this node's to deliver.
        self.carry_out(effects, ballot)
    }

    fn group_error(e: coord_storage::ApplyError) -> DriveError {
        match e {
            // Its commands were reported applied and their results are
            // held here, unreleased. The journal is the record, so the
            // replica restarts from it rather than plan them again.
            coord_storage::ApplyError::GroupLost => {
                DriveError::Engine("an execution group was definitely not journaled".into())
            }
            other => DriveError::Engine(format!("{other:?}")),
        }
    }

    /// Lower the open execution group until it has materialized, then
    /// carry out what its commands led to (task-d47). On a pipelined
    /// store what the groups in flight before it led to is carried out
    /// first.
    fn close_group_waiting(
        &mut self,
        held: &mut Vec<Effect>,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        self.stamp_closed(held);
        let applier = &mut self.applier;
        Self::measured(self.recorder.as_deref(), Stage::Journal, || {
            applier.finish_group()
        })
        .map_err(Self::group_error)?;
        let mut out = Outbound::default();
        if !self.releases.is_empty() {
            let through = self.applier.drain().map_err(Self::group_error)?;
            out.absorb(self.release_through(through, ballot)?);
        }
        let effects = core::mem::take(held);
        out.absorb(self.carry_out(effects, ballot)?);
        Ok(out)
    }

    /// The group whose commands established `held` closes now (task-d62).
    fn stamp_closed(&mut self, held: &[Effect]) {
        let now = std::time::Instant::now();
        for effect in held {
            if let Effect::Established(result) = effect {
                self.release.closed(&result.command(), now);
            }
        }
    }

    /// The driver's account of an apply that failed.
    fn apply_error(command: &CommandId, e: &coord_storage::ApplyError) -> DriveError {
        // A committed command has to be applied at its position,
        // so a refused apply cannot be failed and forgotten the
        // way a protocol transition is. Nothing but a promise at
        // or above the fence moves it, and this voter does not
        // make one on its own; a restart does.
        if matches!(e, coord_storage::ApplyError::Engine(engine) if coord_storage::materialize::is_fenced(engine))
        {
            let id: String = command.0.0[..4]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            DriveError::Fenced(format!("the apply of command {id}"))
        } else {
            DriveError::Engine(format!("{e:?}"))
        }
    }

    /// Storage facts the runtime observed outside a round (a reconcile, a
    /// late completion), fed back the same way.
    pub fn on_storage(
        &mut self,
        event: StorageEvent,
        ballot: &Ballot,
    ) -> Result<Outbound, DriveError> {
        self.outbox.observe(&event);
        let effects = self.machine_mut().step(Event::Storage(event));
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

/// The batch that journals a floor's rows under its own barrier, and,
/// for this voter's own promise, the sends to its peers that wait on it.
fn floor_effects(
    floor: &mut crate::floor::Floor,
    updates: Vec<coord_core::effect::StoreUpdate>,
    readiness: Option<Vec<u8>>,
    ballot: &Ballot,
) -> Vec<Effect> {
    let barrier = floor.barrier();
    let mut effects = vec![Effect::Persist(PersistBatch {
        barrier,
        base: None,
        updates,
    })];
    if let Some(readiness) = readiness {
        let (incarnation, boot_id) = floor.stamp();
        let context = coord_core::effect::EffectContext {
            domain: floor.origin().domain,
            replica_incarnation: incarnation,
            boot_id,
            configuration: floor.voters().epoch(),
            ballot: *ballot,
            required_journal_seq: coord_types::ids::LocalJournalSeq::ZERO,
        };
        let frame = coord_consensus::ProtocolMessage::FloorReadiness { readiness }.encode();
        for peer in floor.peers() {
            effects.push(Effect::SendWhenDurable {
                context,
                requires: vec![barrier],
                to: PeerId {
                    replica: peer,
                    incarnation: coord_types::ids::ReplicaIncarnation::ZERO,
                },
                frame: frame.clone(),
            });
        }
    }
    effects
}

/// How [`Node::lower_once`] lowers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lowering {
    /// Journal and materialize ([`Persistence::lower`]).
    Lower,
    /// Journal only ([`Persistence::journal`], task-d47).
    Journal,
    /// Take back a journal append that has finished, and lend nothing
    /// ([`Persistence::take_back`], task-d54).
    TakeBack,
    /// Wait for the journal append out, take it back, and lend nothing
    /// ([`Persistence::finish_append`], task-d54).
    Await,
}
