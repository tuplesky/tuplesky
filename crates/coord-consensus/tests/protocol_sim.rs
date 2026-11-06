//! task-d30: the real replica machines in a deterministic simulator, under
//! a protocol oracle.
//!
//! `Leader` and `Follower` run as they run in `coordd`: full learning, the
//! role changes a won campaign or a deposition asks for, restarts rebuilt
//! from durable rows only, and the leader's re-send. Around them, one seeded
//! schedule decides every interleaving:
//!
//! * the network delivers in any order, loses, duplicates, and holds some
//!   messages back until a later ballot (old-ballot messages);
//! * each node's journal completes its batches in submission order at
//!   times of the schedule's choosing, and a crash loses whatever is not
//!   durable;
//! * nodes crash, restart and campaign at random.
//!
//! The protocol oracle checks, after every step:
//!
//! 1. **One order.** Every replica executes a prefix of one sequence.
//! 2. **Promises never lowered.** A node's durable promise row never
//!    decreases and is never below a ballot it published `Promise` for.
//! 3. **Decisions keep their dependencies.** A command has one dependency
//!    set: the one a shadow collector learned it with from the evidence
//!    sent to the frontend (`VoteSet::learned`, as the collector does), and
//!    the one every replica executed it with.
//! 4. **No halt.** No replica stops on two decisions of one command, a
//!    recovery cycle, or incompatible accepted copies.
//! 5. **Budgets** (task-d33). Every limit of the resource contract (design
//!    Section 13.1) the machines hold, checked at its peak after every
//!    step: table records and tombstones, held proposals, the ledger and
//!    retry-key bindings, report pages and assemblies, the work of
//!    installing a Sync, the size of a Sync, a frame and a journal record.
//! 6. **Progress after healing** (task-d33). Once the faults stop and
//!    admission pauses -- every node restarted, the live ones too, no loss, no duplication,
//!    nothing held back, campaigns only as `coordd`'s election makes them --
//!    every admitted command settles and every voter executes as far as
//!    the others, within a budget of steps. A command settles as the
//!    collector settles it: learned from the evidence, answered from a
//!    record of its execution, or refused for another command bound under
//!    its retry key that executed. The frontend offers what has not
//!    settled again, as the collector re-offers (design Section 5.5, O1).
//!    Only with catch-up on: without it a voter left behind has no means
//!    to heal, and the sweep without catch-up holds oracles 1 to 5
//!    (task-d71).
//! 7. **Released results** (task-d90). A result is released as soon as a
//!    voter executes its command in memory, the earliest the release rule
//!    of design Section 4.5 ("Releasing before the execution group is
//!    durable") may, and every execution of that command anywhere, before
//!    a crash or after one, and once healed, gives the same `ResponseV1`
//!    bytes. A response here is a function of the order (its position, the
//!    command before it on its key, and how many came before it there), as
//!    a put's revision and previous value are, so an execution anywhere
//!    else in a voter's order gives other bytes.
//! 8. **Forgetting by the journal** (task-d90, point 2). A voter forgets a
//!    command only once its execution is durable there.
//! 9. **Each vote on its payload row** (task-d90, point 4). A fast or slow
//!    acknowledgement a voter sends, a leader's own proposal once its batch
//!    is durable, and a report entry it names a payload present in, rest
//!    on the command's payload row durable at that voter. And the converse,
//!    which the selection rests on: a pre-acceptance a selection leaves out
//!    for want of a payload at every reporter was never decided, unless a
//!    voter executed it durably. Those exempt ones are counted apart
//!    (`left_out_executed`), since they rest on catch-up instead.
//!
//! Scenarios are the checklist's failure-test matrix rows 1 to 5, 9, 10,
//! 12 and 14, and rows 16 and 17, which are row 9's with acknowledgements
//! between followers lost (task-d77), at three and five voters, with table
//! capacity 32. `PROTOCOL_SIM_PEER_ACK_LOSS` (parts per million) sets
//! that loss in every row (task-d78), as the weekly sweep does. Seeds that
//! once failed are kept in `fixtures/protocol_sim/seeds.json`, with the
//! catch-up and the loss they were found under, and replayed on every run.
//!
//! Rows 19 to 22 are task-d90's. In every other row a node flushes its
//! journal before it executes, and an execution is durable as it is taken,
//! as the models apply. In row 19 an execution's group joins the journal
//! behind the batches before it, as `coordd`'s does, and is durable only
//! when the journal reaches it; the machines count it durable then, as
//! `coordd`'s node reports it (`executions_durable_through`). One voter's
//! journal at a time is slow to reach its groups, so executions pile up
//! ahead of their durability, and every voter is crashed at once at each
//! cut between a decision, the release of its result and its group's
//! durability. Its table capacity is 8, so a voter's executions past a
//! command reach the window it forgets past. Row 20 is oracle 9's sweep.
//! Row 21 fails a follower's batch of a payload row and its dependency
//! row, so a later vote's batch can write the dependency row alone, and
//! crashes a follower while it writes the payload row of such a record.
//! Row 22 is row 19's late execution with the leader crashed inside
//! task-d89's window: after it counted a fast quorum over its own
//! proposal, before that proposal's batch is durable. With the gate
//! removed from `Leader::learn` it fails (five voters, seeds 3 and 5, in
//! CI's twelve), and its kept seeds replay those failures; row 19, which
//! never crashes the leader there, passes without the gate. Rows 19 and
//! 20 each have a negative
//! control that must fail: row 19's machines counting executions durable
//! as they are taken, and row 20's followers' payload rows dropped on their
//! way to the store.
//!
//! task-d98's leader sends its proposal before its batch is durable, to
//! the followers that cannot make a fast quorum from it. Rows 1 and 2 run
//! every seed again with it on and with the three faults that reach it: a
//! leader journal that lags its followers', and the leader crashed, or
//! deposed (cut off while a follower campaigns, then joined again), while
//! a proposal it sent early is not durable. Row 18 is those faults more
//! often, with more loss. Its negative control sends the early copy to the
//! fast-set followers too, and must fail: at three voters one of them then
//! commits over a vote the leader may lose.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};

use coord_consensus::{
    AppliedOutcome, BallotConfiguration, CommandRecord, ConfigurationIdentity, FastAck, Follower,
    FollowerConfig, FollowerRejection, Leader, LeaderConfig, LearningMode, ProtocolMessage,
    RecoveryError, ReplicaRole, SyncDecision, Vote, VoteSet, decode_dependency, decode_payload,
    decode_promise, decode_sync,
};
use coord_core::capability::{AdmissionReceipt, AttestedAdmission, VerifierToken};
use coord_core::effect::{BarrierId, BootId, Effect, PeerId};
use coord_core::event::{
    AdmittedRequest, AuthenticatedPeerMessage, Event, PeerProvenance, StorageEvent,
};
use coord_core::machine::DeterministicMachine;
use coord_sim::rng::NamedStreams;
use coord_sim::storage::StorageModel;
use coord_store_api::registry::Collection;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::wire_v1::{MessageV1, RequestV1};
use coord_types::{CommandId, RetryKey};
use serde::Deserialize;

const CAPACITY: usize = 32;

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: r(leader),
    }
}

fn identity(me: u8, n: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..n).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(b: Ballot, n: u8) -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), b, (0..n).map(r).collect()).unwrap()
}

/// The knobs of one scenario. Rates are per million draws.
#[derive(Clone, Copy, Debug)]
struct Knobs {
    /// Checklist matrix row.
    row: u8,
    /// Steps per run.
    steps: u32,
    /// Commands admitted over the run.
    commands: u64,
    /// Commands admitted by one admission action.
    burst: u64,
    /// Chance per step of an admission action.
    admit: u32,
    /// Chance each node is given an admission (a node left out learns
    /// the payload from a peer).
    reach: u32,
    /// Admit at the followers first and at the leader only later.
    followers_first: bool,
    /// Chance an admission is presented again later (a retry).
    retry: u32,
    /// Message loss, duplication, and hold-back until a later ballot.
    loss: u32,
    dup: u32,
    hold: u32,
    /// Crash, restart and campaign chances per step.
    crash: u32,
    restart: u32,
    campaign: u32,
    /// Crash the node that currently leads, rather than any node.
    crash_leader: bool,
    /// Every command writes one key.
    one_key: bool,
    /// Chance each node is given a retry (the rest are left out, as by a
    /// client or collector that died part way through).
    retry_reach: u32,
    /// Chance a retry carries another payload under the same retry key.
    new_payload: u32,
    /// Loss of evidence on its way to the frontend.
    frontend_loss: u32,
    /// Chance per step the collector restarts, losing what it counted.
    collector_crash: u32,
    /// Crash a node while it campaigns, when one does.
    crash_candidate: bool,
    /// While healing, lose each voter's first report page to each
    /// campaign, once: a campaign completes only if it asks for a lost
    /// page again (task-d28).
    heal_page_loss: bool,
    /// Loss of a `FastAck` or `SlowAck` to a voter other than its ballot's
    /// leader, while not healing (task-d77). Such a voter learns a commit
    /// from the leader's frontier alone, as every follower does with
    /// acknowledgements to the leader only (task-d74).
    peer_ack_loss: u32,
    /// An execution's group reaches the journal after its outcome is
    /// taken, in journal order with the protocol batches, as `coordd`'s
    /// does, and the machines count an execution durable only once its
    /// group is (task-d90, point 2). Off, a node flushes its journal
    /// before it executes and an execution is durable when it is taken,
    /// as the models apply.
    late_execution: bool,
    /// Chance a journal completion whose next entry is an execution
    /// group is put off, at the one voter whose journal is slow, which
    /// changes every [`LAG_SPELL`] steps (task-d90): its executions pile
    /// up ahead of their durability, the window a result released before
    /// its group is durable opens, while the others go on deciding.
    execution_lag: u32,
    /// Chance per step that every live voter crashes at once, drawn only
    /// while the domain is at one of the cuts between a decision, the
    /// release of its result and its execution group's durability
    /// (task-d90).
    total_crash: u32,
    /// The negative control of point 2: the machines count an execution
    /// durable when it is taken, so a voter forgets by what it executed
    /// in memory.
    count_in_memory: bool,
    /// Table capacity: the retirement window a voter forgets past.
    capacity: usize,
    /// Chance a follower's batch writing a command's payload row and its
    /// dependency row fails, definitely not committed, when the journal
    /// reaches it (task-d90, point 4). Later batches still complete, so a
    /// later vote's batch can write the dependency row without the payload
    /// row, the state every vote point 4 closed rested on.
    payload_fail: u32,
    /// Chance per step a follower is crashed while a batch writes the
    /// payload row of a command whose dependency row is durable there
    /// without one (task-d90, point 4): restored, the record holds the
    /// dependency row without the payload row.
    payload_crash: u32,
    /// The negative control of point 4: a follower's payload rows never
    /// reach its store, while the follower goes on as if they were
    /// written, as a vote that went out without waiting on its payload
    /// batch, from a voter that then crashed before the batch was
    /// durable.
    drop_payload_rows: bool,
    /// Chance per step the leader is crashed while it has counted a fast
    /// quorum over a proposal of its own whose batch is not durable:
    /// task-d89's window, where its gate commits nothing until the batch
    /// is (task-d90's acceptance).
    gate_crash: u32,
    /// The negative control of the converse of oracle 9: a voter's report
    /// names none of its pre-acceptances' payloads present, rows durable
    /// or not, as a report whose bookkeeping of served payloads drifted.
    hide_preaccepted_payloads: bool,
    /// The leader sends a fresh proposal before its batch is durable, to
    /// the followers that cannot make a fast quorum from it (task-d98).
    early: bool,
    /// Chance a journal completion that falls to the leading node is put
    /// off: its batches complete later than its followers' (task-d98).
    leader_journal_lag: u32,
    /// Chance per step the leader is crashed while it holds a proposal it
    /// sent early whose batch is not durable (task-d98).
    window_crash: u32,
    /// Chance per step the leader is deposed in that window: cut off from
    /// its peers while a follower campaigns, and joined again later
    /// (task-d98).
    window_depose: u32,
    /// The negative control of task-d98's rows: an early proposal reaches
    /// the fast-set followers too, as a copy of the frame, as if the
    /// leader sent it to them early. At three voters one of them then
    /// commits a fast decision over a vote the leader may lose, and the
    /// oracle has to say so.
    early_to_fast_set: bool,
}

impl Knobs {
    const BASE: Knobs = Knobs {
        row: 0,
        steps: 2_500,
        commands: 60,
        burst: 1,
        admit: 60_000,
        reach: 1_000_000,
        followers_first: false,
        retry: 0,
        loss: 20_000,
        dup: 20_000,
        hold: 0,
        crash: 1_500,
        restart: 20_000,
        campaign: 2_000,
        crash_leader: false,
        one_key: false,
        retry_reach: 1_000_000,
        new_payload: 0,
        frontend_loss: 0,
        collector_crash: 0,
        crash_candidate: false,
        heal_page_loss: false,
        peer_ack_loss: 0,
        late_execution: false,
        execution_lag: 0,
        total_crash: 0,
        count_in_memory: false,
        capacity: CAPACITY,
        payload_fail: 0,
        payload_crash: 0,
        drop_payload_rows: false,
        gate_crash: 0,
        hide_preaccepted_payloads: false,
        early: false,
        leader_journal_lag: 0,
        window_crash: 0,
        window_depose: 0,
        early_to_fast_set: false,
    };

    /// These knobs with task-d98's early proposals and the three faults
    /// that reach them: a leader journal that lags its followers', and
    /// the leader crashed or deposed while a proposal it sent early is
    /// not durable.
    fn with_early(self) -> Knobs {
        Knobs {
            early: true,
            leader_journal_lag: 500_000,
            window_crash: 30_000,
            window_depose: 15_000,
            ..self
        }
    }

    /// The checklist's matrix rows 1 to 5, 9, 10, 12 and 14, rows 16
    /// and 17, and rows 18 to 22.
    fn row(row: u8) -> Knobs {
        let base = Knobs { row, ..Knobs::BASE };
        match row {
            // Fill capacity, fail the leader: bursts past the table, and
            // the leader is the node that crashes.
            1 => Knobs {
                commands: 90,
                burst: 12,
                admit: 20_000,
                crash_leader: true,
                crash: 2_500,
                ..base
            },
            // Different maximum tentative sets: each admission reaches a
            // random subset, messages are lost, the leader fails.
            2 => Knobs {
                reach: 550_000,
                loss: 60_000,
                crash_leader: true,
                ..base
            },
            // Dense conflicts, early missing dependency: one key, and
            // admissions that miss most nodes, so proposals and votes
            // name dependencies whose payloads have not arrived.
            3 => Knobs {
                one_key: true,
                reach: 350_000,
                admit: 90_000,
                ..base
            },
            // Acknowledgement before payload, hold expires, repeat: the
            // followers acknowledge before the leader has the command,
            // and admissions are presented again.
            4 => Knobs {
                followers_first: true,
                retry: 300_000,
                dup: 60_000,
                ..base
            },
            // Delayed old-ballot messages: many held back until a later
            // ballot, and frequent campaigns.
            10 => Knobs {
                hold: 80_000,
                campaign: 6_000,
                crash: 2_500,
                ..base
            },
            // Client or collector dies mid-dissemination: admissions reach
            // a few nodes, the collector restarts and forgets what it
            // counted, and the client presents the command again to a
            // few nodes more (task-d33).
            5 => Knobs {
                reach: 450_000,
                retry: 600_000,
                retry_reach: 400_000,
                collector_crash: 3_000,
                crash: 2_000,
                ..base
            },
            // Repeated interrupted elections: frequent campaigns, a
            // candidate crashed while it campaigns, and history long
            // enough to pass the sweeps, so a budget that grows with
            // ballots or with history shows (task-d33).
            9 => Knobs {
                steps: 9_000,
                commands: 260,
                admit: 80_000,
                campaign: 12_000,
                crash: 3_000,
                restart: 30_000,
                crash_candidate: true,
                heal_page_loss: true,
                ..base
            },
            // Loss beyond the repair-cache window: most evidence to the
            // frontend is lost, so a command settles from the record of
            // its execution or not at all (task-d33).
            12 => Knobs {
                frontend_loss: 700_000,
                loss: 40_000,
                crash: 2_500,
                ..base
            },
            // Lost response, same-identity and new-payload retries: evidence
            // to the frontend lost, commands presented again, some with
            // another payload under the same retry key (task-d33).
            14 => Knobs {
                frontend_loss: 300_000,
                retry: 400_000,
                new_payload: 350_000,
                dup: 40_000,
                ..base
            },
            // Row 9's elections with every acknowledgement to a follower
            // lost, and with half of them (task-d77): a follower that
            // misses a command's commit while a voter that executed it
            // retires it past its window (seeds 362 and 863 here, 550 and
            // 633 in row 17, at three voters).
            16 | 17 => Knobs {
                row,
                peer_ack_loss: if row == 16 { 1_000_000 } else { 500_000 },
                ..Knobs::row(9)
            },
            // The leader's proposal before its batch is durable (task-d98):
            // a leader journal that lags its followers', and the leader
            // crashed or deposed while a proposal it sent early is not
            // durable, more often than rows 1 and 2's early runs do it,
            // with messages lost so a follower may miss the early copy.
            18 => Knobs {
                crash_leader: true,
                crash: 2_000,
                loss: 60_000,
                window_crash: 60_000,
                window_depose: 30_000,
                ..base.with_early()
            },
            // Results released before their execution group is durable
            // (task-d90): execution groups that reach the journal late, and
            // every voter crashed at once at each cut between a decision,
            // the release of its result and its group's durability. Row
            // 1's leader crashes and bursts, so commands are decided and
            // executed in runs that a crash cuts across.
            19 => Knobs {
                row,
                late_execution: true,
                execution_lag: 980_000,
                total_crash: 6_000,
                capacity: 8,
                ..Knobs::row(1)
            },
            // Each vote on a payload row durable at the voter (task-d90,
            // point 4): row 2's admissions that miss most nodes and lost
            // messages, and a follower crashed while it writes the payload
            // row of a record durable without one. Without a failed batch
            // no record is durable without its payload row, so the crash
            // never fires here: row 20 is oracle 9's sweep and its negative
            // control, and row 21 reaches the cut.
            20 => Knobs {
                row,
                reach: 450_000,
                campaign: 6_000,
                payload_crash: 300_000,
                ..Knobs::row(2)
            },
            // Row 20 with a follower's batch of a payload row and its
            // dependency row failed, definitely not committed, so a later
            // vote's batch writes the dependency row alone, and the crash
            // restores such a record (task-d90, point 4). Its seeds found
            // a deposed leader's report of a payload row it never wrote,
            // and a selection that re-proposed a pre-acceptance whose
            // payload no voter held, so its campaign never bound.
            21 => Knobs {
                row,
                payload_fail: 150_000,
                ..Knobs::row(20)
            },
            // Row 19's late execution with the leader crashed inside
            // task-d89's window: after it counted a fast quorum over its
            // own proposal, and before that proposal's batch is durable.
            // Its gate commits nothing there, so nothing is executed or
            // released over a vote a restarted leader's report would deny.
            // Without the crash of every voter: the leader alone.
            22 => Knobs {
                row,
                total_crash: 0,
                gate_crash: 250_000,
                ..Knobs::row(19)
            },
            // Row 18's early proposals, leader journal lag and window
            // crashes with row 19's late execution and its cuts: both let
            // something leave before it is durable (#197 review, for the
            // batch head).
            23 => Knobs {
                row,
                late_execution: true,
                execution_lag: 980_000,
                total_crash: 6_000,
                capacity: 8,
                ..Knobs::row(18)
            },
            _ => unreachable!("not a scenario of the simulator: row {row}"),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Leader(Leader),
    Follower(Follower),
}

/// One entry of a node's journal, in the order it was submitted.
#[derive(Clone, Copy, Debug)]
enum Entry {
    /// A protocol batch.
    Batch(BarrierId),
    /// The execution group of the node's executions through this count
    /// (task-d90's late execution): durable with every entry before it.
    Group(usize),
}

struct Node {
    role: Option<Role>,
    storage: StorageModel,
    /// Entries submitted and not yet complete, in journal order.
    journal: VecDeque<Entry>,
    /// Executed commands, in order, as the node's materializer took them.
    executed: Vec<CommandId>,
    /// How many of `executed` are durable with the application rows: all
    /// of them unless execution groups reach the journal late (task-d90).
    /// A crash loses the rest.
    durable_executed: usize,
    /// Commands whose payload row this node is writing over a durable
    /// dependency row with no durable payload row (task-d90, row 20).
    payload_window: BTreeSet<CommandId>,
    /// Its batches writing a payload row and a dependency row, which row
    /// 20 may fail (task-d90).
    failable: BTreeSet<BarrierId>,
    boot: u8,
    /// Highest ballot this node published `Promise` for.
    promised_out: Option<Ballot>,
    /// Highest durable promise row seen.
    durable_promise: Option<Ballot>,
}

/// The rules every candidate here selects under: one build everywhere,
/// every voter on point 4, as a domain that activated `ReleaseAtExecution`
/// is (task-d90).
const RULES: coord_consensus::SelectionRules = coord_consensus::SelectionRules {
    leave_out_unheld: true,
};

impl Node {
    fn alive(&self) -> bool {
        self.role.is_some()
    }

    /// Count executions durable only as the journal holds their groups
    /// (task-d90), as `coordd`'s node does.
    fn track(&mut self) {
        match self.role.as_mut().expect("alive") {
            Role::Leader(m) => m.track_execution_durability(),
            Role::Follower(m) => m.track_execution_durability(),
        }
    }

    fn step(&mut self, e: Event) -> Vec<Effect> {
        match self.role.as_mut().expect("alive") {
            Role::Leader(m) => m.step(e),
            Role::Follower(m) => {
                let mut effects = m.step(e);
                // A campaign waiting on the rows of commands this node
                // executed and forgot is answered by its runtime from its
                // durable rows, as `coordd`'s is (task-d77).
                loop {
                    let wanted = m.executed_rows_wanted();
                    if wanted.is_empty() {
                        break effects;
                    }
                    let rows: BTreeMap<CommandId, CommandRecord> =
                        dependency_rows(&self.storage).into_iter().collect();
                    effects.extend(
                        m.executed_rows(wanted.into_iter().map(|c| (c, rows.get(&c).cloned()))),
                    );
                }
            }
        }
    }
}

struct Msg {
    from: u8,
    to: u8,
    frame: Vec<u8>,
}

/// What the shadow collector holds for one (ballot, command).
#[derive(Default)]
struct Shadow {
    set: Option<VoteSet>,
    admission: Option<Digest32>,
    /// Leader replies waiting for an admission to count them under.
    leader: Vec<FastAck>,
}

/// A command's pending second presentation.
struct Retry {
    at_step: u32,
    seq: u64,
    key: u8,
    /// 0 for the payload first presented, 1 for another under its key.
    variant: u8,
    nodes: Vec<u8>,
}

/// What a command was presented as, to present it again.
#[derive(Clone, Copy)]
struct Offer {
    seq: u64,
    key: u8,
    variant: u8,
    /// The node whose frontend took the caller's request: its collector
    /// holds the entry and answers it from that node's own durable
    /// record once the entry is one only the record answers.
    host: u8,
}

/// Commands a key may be the latest of: the most tombstones a table keeps
/// beyond its capacity (design Section 13.1).
const KEYS: usize = 4;

/// Rounds the domain has, once the faults stop, to settle what it
/// admitted (oracle 6). In a round every message in flight is delivered,
/// every journal completes, and every node executes what it can.
const HEAL_BUDGET: u32 = 400;
/// Rounds between the frontend's offers of what has not settled.
const OFFER_EVERY: u32 = 8;
/// Rounds between the runtime's timers while healing.
const TIMER_EVERY: u32 = 2;
/// Rounds a voter without a leader waits before it campaigns, while
/// healing (`coordd`'s election patience).
const PATIENCE: u32 = 6;
/// Rounds a promise, or a campaign of its own, is given to synchronize,
/// and the longest wait between campaigns (the election's ceiling).
const CEILING: u32 = 48;
/// Steps one voter's journal is the slow one, with an execution lag.
const LAG_SPELL: u32 = 300;

struct Sim {
    n: u8,
    knobs: Knobs,
    seed: u64,
    rng: NamedStreams,
    nodes: Vec<Node>,
    net: Vec<Msg>,
    /// Messages held back until the next campaign.
    held: Vec<Msg>,
    admitted: u64,
    retries: Vec<Retry>,
    /// Admissions presented to followers and still owed to the leader.
    owed_to_leader: Vec<(u32, u64, u8)>,
    highest_ballot: u64,
    shadow: BTreeMap<(u64, ReplicaId, CommandId), Shadow>,
    /// Whether a follower's timer asks for the leader's executed history
    /// (task-d08's pacer). On unless `PROTOCOL_SIM_NO_CATCH_UP` is set.
    /// It was off while it reached the fast-path gap that #116's leader
    /// adoption and #118's path-log alignment (F7) closed.
    catch_up: bool,
    /// The admission each command was submitted under, as the collector
    /// stores it beside its entry: what a leader reply is counted under.
    submitted: BTreeMap<CommandId, Digest32>,
    /// The one dependency set of each decided command, and who said so.
    decided: BTreeMap<CommandId, (BTreeSet<CommandId>, String)>,
    /// Commands some voter counted a quorum over: a leader's commits from
    /// its votes, and a follower's commit as an acknowledgement reaches it
    /// (the converse of oracle 9, task-d90).
    counted: BTreeSet<CommandId>,
    /// The one execution order.
    order: Vec<CommandId>,
    step: u32,
    /// Counters for the report.
    stats: Stats,
    /// Recent actions, kept when `PROTOCOL_SIM_TRACE` is set.
    trace: Option<VecDeque<String>>,
    /// Every command admitted, as it was presented (oracle 6).
    offers: BTreeMap<CommandId, Offer>,
    /// Commands the frontend learned from evidence.
    learned: BTreeSet<CommandId>,
    /// Commands a voter refused for another bound under their retry key.
    refused_for: BTreeMap<CommandId, CommandId>,
    /// Commands a voter told the frontend it keeps no payload of any
    /// more (`Refused { Forgotten }`): the collector answers them from
    /// the durable record of their execution.
    forgotten: BTreeSet<CommandId>,
    /// Commands the collector presented again after the faults stopped.
    reoffered: BTreeSet<CommandId>,
    /// Commands executed anywhere: each has a record that answers it.
    executed_anywhere: BTreeSet<CommandId>,
    /// Faults have stopped.
    healing: bool,
    /// Entries and edges of the Syncs each node's current machine was
    /// given to install (oracle 5).
    sync_work: Vec<u64>,
    /// The most entries of any one Sync each node was given, its own
    /// selection included: how far recovery work may take its table past
    /// the capacity.
    sync_entries: Vec<usize>,
    /// The most commands of any one catch-up page each node was given:
    /// pulled commands enter a full table too (task-d24).
    pulled_entries: Vec<usize>,
    /// When each node last restarted, and when it last began a campaign.
    booted_at: Vec<u32>,
    campaign_at: Vec<u32>,
    /// Each voter's election, as `coordd` keeps one (task-d01, task-d10):
    /// since when it has had no leader, its wait before the next campaign,
    /// and the promised ballot not yet synchronized with since when.
    leaderless_since: Vec<Option<u32>>,
    backoff: Vec<u32>,
    unsynced: Vec<Option<(Ballot, u32)>>,
    /// The first round after the faults stopped.
    heal_start: u32,
    /// Report pages lost while healing: (from, to, ballot number, page).
    pages_lost: BTreeSet<(u8, u8, u64, u32)>,
    /// The ballot of each node's campaign selection last counted as Sync
    /// work: a candidate installs its own selection as a Sync.
    selection_counted: Vec<Option<Ballot>>,
    /// Each node's catch-up pacer, as `coordd` keeps one (task-d08):
    /// whether it has asked since it started, and the frontier at its last
    /// ask with the asks gone unanswered since.
    probed: Vec<bool>,
    asked_at: Vec<Option<usize>>,
    unanswered: Vec<usize>,
    /// Oracle 7 (task-d90): the result each command was first released
    /// with, as the bytes of its response, taken when a voter executed it
    /// in memory: what the release rule may hand a caller before the
    /// execution group is durable.
    released: BTreeMap<CommandId, Vec<u8>>,
    /// The voter whose journal is slow (task-d90's execution lag).
    lagging: Option<u8>,
    /// Commands seen in task-d89's window at a leader.
    gate_seen: BTreeSet<CommandId>,
    /// Report pages oracle 9 read, by node, boot, ballot number and page:
    /// a page asked for again is served as first sent (task-d28), and is
    /// read at the cut it was taken at.
    pages_read: BTreeSet<(u8, u8, u64, u32)>,
    /// Whether each node, leading, holds a proposal it sent early whose
    /// batch is not durable yet (task-d98): set when the copy leaves,
    /// cleared when its journal has nothing left in flight.
    window: Vec<bool>,
    /// A node cut off from its peers, and the step it is joined again
    /// (task-d98's deposition).
    partition: Option<(u8, u32)>,
}

#[derive(Default, Debug, Clone, Copy)]
struct Stats {
    executed: usize,
    learned: usize,
    campaigns: u32,
    crashes: u32,
    /// Crashes of a node that was leading its ballot.
    leader_crashes: u32,
    /// Catch-up pages answered, and commands executed from them.
    catch_up_pages: u32,
    caught_up: u64,
    ballots: u64,
    /// Steps healing took (the largest, in a total).
    heal_steps: u32,
    /// Commands settled as refused for another under their key.
    conflicts: usize,
    /// Peaks of oracle 5, over every node.
    peak_records: usize,
    peak_ledger: usize,
    peak_bindings: usize,
    peak_held: usize,
    /// Largest ratio of Sync examinations to the Sync work given, in
    /// hundredths.
    peak_examinations_pct: u64,
    /// What the run decided, folded into one number: every node's
    /// execution order and every learned command's dependencies
    /// (task-d46). A change that must not move a decision -- a data
    /// structure under the machines -- leaves it as it was, seed for
    /// seed.
    decisions: u64,
    /// Results released, and executions compared with a released result
    /// (oracle 7, task-d90).
    released: usize,
    compared: usize,
    /// Executions a crash took back before their group was durable, and
    /// the most one voter held ahead of its durable groups.
    executions_lost: usize,
    peak_undurable: usize,
    /// Crashes of every voter at once (task-d90), and the cuts each was
    /// at: a command learned and executed nowhere; a result released
    /// with its group durable nowhere; and an execution durable at one
    /// voter and not at another.
    total_crashes: u32,
    /// Votes checked against a durable payload row (oracle 9), followers
    /// crashed while writing a payload row over a durable dependency row,
    /// and records restored with a dependency row and no payload row
    /// (task-d90, point 4).
    votes_checked: usize,
    payload_failures: u32,
    payload_windows: u32,
    payload_crashes: u32,
    restored_without_payload: usize,
    cut_decided: u32,
    cut_released: u32,
    cut_durable: u32,
    /// Commands the leader counted a fast quorum over before its proposal
    /// batch was durable, and leader crashes while one was (task-d89's
    /// window).
    gate_windows: usize,
    gate_crashes: u32,
    /// Pre-acceptances a selection left out for want of a payload row at
    /// any reporter, each checked never to have been decided.
    left_out: usize,
    /// Of those, the ones that were decided and pass only because a voter
    /// had executed them durably: they rest on catch-up and task-d77, not
    /// on the payload argument (#197 review).
    left_out_executed: usize,
    /// Proposal copies that left before their batch was durable
    /// (task-d98).
    early_sends: u64,
    /// Leaders crashed, and leaders deposed, while a proposal they sent
    /// early was not durable (task-d98).
    window_crashes: u32,
    window_deposes: u32,
}

/// FNV-1a over `bytes`, continuing from `hash`.
fn fold(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

impl Sim {
    fn new(n: u8, knobs: Knobs, seed: u64) -> Self {
        let mut master = [0u8; 32];
        master[..8].copy_from_slice(&seed.to_be_bytes());
        master[8] = n;
        master[9] = knobs.row;
        let mut nodes = Vec::new();
        for me in 0..n {
            let role = if me == 0 {
                let mut leader = Leader::new(
                    LeaderConfig {
                        identity: identity(0, n),
                        quorum: quorum(ballot(0, 0), n),
                        genesis: ballot(0, 0),
                        frontend: FRONTEND,
                        capacity: knobs.capacity,
                    },
                    None,
                    ExecutionPosition::ZERO,
                );
                leader.set_learning(LearningMode::Full);
                leader.observe_learning();
                leader.set_early_proposal(knobs.early);
                Role::Leader(leader)
            } else {
                let mut follower = Follower::new(FollowerConfig {
                    identity: identity(me, n),
                    quorum: quorum(ballot(0, 0), n),
                    genesis: ballot(0, 0),
                    frontend: FRONTEND,
                    capacity: knobs.capacity,
                });
                follower.set_learning(LearningMode::Full);
                follower.set_selection_rules(RULES);
                Role::Follower(follower)
            };
            nodes.push(Node {
                role: Some(role),
                storage: StorageModel::default(),
                journal: VecDeque::new(),
                executed: Vec::new(),
                durable_executed: 0,
                payload_window: BTreeSet::new(),
                failable: BTreeSet::new(),
                boot: 1,
                promised_out: None,
                durable_promise: None,
            });
        }
        let mut sim = Sim {
            n,
            knobs,
            seed,
            rng: NamedStreams::new(master),
            nodes,
            net: Vec::new(),
            held: Vec::new(),
            admitted: 0,
            retries: Vec::new(),
            owed_to_leader: Vec::new(),
            highest_ballot: 0,
            shadow: BTreeMap::new(),
            submitted: BTreeMap::new(),
            catch_up: std::env::var_os("PROTOCOL_SIM_NO_CATCH_UP").is_none(),
            decided: BTreeMap::new(),
            counted: BTreeSet::new(),
            order: Vec::new(),
            step: 0,
            stats: Stats::default(),
            trace: std::env::var_os("PROTOCOL_SIM_TRACE").map(|_| VecDeque::new()),
            offers: BTreeMap::new(),
            learned: BTreeSet::new(),
            refused_for: BTreeMap::new(),
            forgotten: BTreeSet::new(),
            reoffered: BTreeSet::new(),
            executed_anywhere: BTreeSet::new(),
            healing: false,
            sync_work: vec![0; usize::from(n)],
            sync_entries: vec![0; usize::from(n)],
            pulled_entries: vec![0; usize::from(n)],
            booted_at: vec![0; usize::from(n)],
            campaign_at: vec![0; usize::from(n)],
            leaderless_since: vec![None; usize::from(n)],
            backoff: vec![PATIENCE; usize::from(n)],
            unsynced: vec![None; usize::from(n)],
            heal_start: 0,
            pages_lost: BTreeSet::new(),
            selection_counted: vec![None; usize::from(n)],
            probed: vec![false; usize::from(n)],
            asked_at: vec![None; usize::from(n)],
            unanswered: vec![0; usize::from(n)],
            released: BTreeMap::new(),
            lagging: None,
            gate_seen: BTreeSet::new(),
            pages_read: BTreeSet::new(),
            window: vec![false; usize::from(n)],
            partition: None,
        };
        for i in 0..n {
            if sim.tracks() {
                sim.nodes[usize::from(i)].track();
            }
            let effects = sim.nodes[usize::from(i)].step(boot_event(1));
            sim.handle(i, effects);
        }
        sim
    }

    /// Whether the machines count executions durable as their groups
    /// reach the journal (task-d90): with late execution, unless the
    /// negative control counts them in memory.
    fn tracks(&self) -> bool {
        self.knobs.late_execution && !self.knobs.count_in_memory
    }

    fn note(&mut self, what: impl FnOnce() -> String) {
        if let Some(t) = self.trace.as_mut() {
            if t.len() >= trace_lines() {
                t.pop_front();
            }
            t.push_back(format!("{:>5} {}", self.step, what()));
        }
    }

    fn fail(&self, what: &str) -> ! {
        if let Some(t) = &self.trace {
            for line in t {
                eprintln!("{line}");
            }
        }
        panic!(
            "protocol oracle, row {} at {} voters, seed {}, step {}: {what}",
            self.knobs.row, self.n, self.seed, self.step
        )
    }

    fn chance(&mut self, name: &str, per_million: u32) -> bool {
        per_million > 0 && self.rng.chance(name, per_million)
    }

    fn below(&mut self, name: &str, bound: usize) -> usize {
        self.rng.below(name, bound as u64) as usize
    }

    fn leader_index(&self) -> Option<u8> {
        (0..self.n).find(|i| {
            matches!(&self.nodes[usize::from(*i)].role, Some(Role::Leader(l)) if l.is_leading())
        })
    }

    /// Carry out a node's effects.
    fn handle(&mut self, i: u8, effects: Vec<Effect>) {
        for e in effects {
            match e {
                Effect::Persist(batch) => {
                    // Oracle 5: one journal record (design Section 13.1),
                    // counted as the journal admits it: `batch_bytes` of
                    // coord-storage (sixteen bytes per update and 64 per
                    // batch) plus the record header's allowance of 512,
                    // against `MAX_RECORD_BYTES` and `MAX_RECORD_UPDATES`
                    // of coord-journal-api. A batch past it is refused as
                    // `TooLarge` before it is queued. At capacity 32 with
                    // one-byte keys no batch comes near it: the check is
                    // vacuous at this scale, and there for a larger one.
                    let bytes: usize = batch
                        .updates
                        .iter()
                        .map(|u| u.key.len() + u.value.as_ref().map_or(0, Vec::len) + 16)
                        .sum::<usize>()
                        + 64
                        + 512;
                    if batch.updates.len() > 4_096 || bytes > 4 << 20 {
                        self.fail(&format!(
                            "node {i} wrote a journal record of {} updates and {bytes} bytes",
                            batch.updates.len()
                        ));
                    }
                    let mut batch = batch;
                    let follower =
                        matches!(self.nodes[usize::from(i)].role, Some(Role::Follower(_)));
                    if follower && self.knobs.payload_crash > 0 {
                        self.note_payload_window(i, &batch);
                    }
                    if follower && self.knobs.payload_fail > 0 && writes_payload_and_row(&batch) {
                        self.nodes[usize::from(i)].failable.insert(batch.barrier);
                    }
                    if follower && self.knobs.drop_payload_rows {
                        let payloads = Collection::PayloadV1.id();
                        batch
                            .updates
                            .retain(|u| u.collection != payloads || u.value.is_none());
                    }
                    let node = &mut self.nodes[usize::from(i)];
                    node.journal.push_back(Entry::Batch(batch.barrier));
                    node.storage.submit(batch);
                }
                Effect::SendWhenDurable {
                    requires,
                    to,
                    frame,
                    ..
                } => {
                    // Oracle 5: a protocol frame, and a Sync's entries. The
                    // frame bound, like the journal record's, is vacuous
                    // at this scale.
                    if frame.len() > 4 << 20 {
                        self.fail(&format!("node {i} sent a frame of {} bytes", frame.len()));
                    }
                    let message = ProtocolMessage::decode(&frame);
                    if let Ok(m) = &message {
                        self.check_vote_payload(i, m);
                    }
                    let (frame, message) = match message {
                        Ok(ProtocolMessage::ReportPage(p))
                            if self.knobs.hide_preaccepted_payloads =>
                        {
                            let p = hide_preaccepted(p);
                            (
                                ProtocolMessage::ReportPage(p.clone()).encode(),
                                Ok(ProtocolMessage::ReportPage(p)),
                            )
                        }
                        other => (frame, other),
                    };
                    // A proposal that waits on nothing left before its
                    // batch was durable: every other copy names the batch
                    // (task-d98).
                    if let Ok(ProtocolMessage::Proposal(p)) = &message
                        && requires.is_empty()
                    {
                        self.stats.early_sends += 1;
                        if !self.nodes[usize::from(i)].journal.is_empty() {
                            self.window[usize::from(i)] = true;
                        }
                        self.note(|| format!("early node {i} -> {}", to.replica.0[0]));
                        // The negative control: the copy reaches the
                        // fast-set followers as well.
                        if self.knobs.early_to_fast_set {
                            let q = quorum(p.ballot, self.n);
                            if !q.fast_eligible(&to.replica) {
                                for f in q.fast_set().iter().map(|f| f.0[0]) {
                                    if f != i {
                                        self.net.push(Msg {
                                            from: i,
                                            to: f,
                                            frame: frame.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    if let Ok(ProtocolMessage::Sync(d)) = &message
                        && d.entries.len()
                            > 5 * coord_consensus::max_report_entries(self.knobs.capacity)
                    {
                        self.fail(&format!(
                            "node {i} sent a Sync of {} entries",
                            d.entries.len()
                        ));
                    }
                    if let Ok(ProtocolMessage::Promise { ballot: b, .. }) = &message {
                        let node = &mut self.nodes[usize::from(i)];
                        if node
                            .promised_out
                            .is_none_or(|p| b.compare_same_epoch(&p).is_some_and(|o| o.is_gt()))
                        {
                            node.promised_out = Some(*b);
                        }
                    }
                    if to == FRONTEND {
                        if !self.healing && self.chance("frontend-loss", self.knobs.frontend_loss) {
                            continue;
                        }
                        if let Ok(m) = message {
                            self.at_frontend(i, m);
                        }
                        continue;
                    }
                    // An acknowledgement to a voter other than the ballot's
                    // leader (task-d77).
                    if !self.healing {
                        let to_a_follower = match &message {
                            Ok(ProtocolMessage::FastAck(a)) => a.ballot.leader != to.replica,
                            Ok(ProtocolMessage::SlowAck(a)) => a.ballot.leader != to.replica,
                            _ => false,
                        };
                        if to_a_follower && self.chance("peer-ack-loss", self.knobs.peer_ack_loss) {
                            continue;
                        }
                    }
                    let to = to.replica.0[0];
                    // Cut off from its peers (task-d98's deposition).
                    if self.partition.is_some_and(|(p, _)| p == i || p == to) {
                        continue;
                    }
                    let msg = Msg { from: i, to, frame };
                    // Faults stop while healing: nothing is held back.
                    if !self.healing && self.chance("hold", self.knobs.hold) {
                        self.held.push(msg);
                    } else {
                        self.net.push(msg);
                    }
                }
                _ => {}
            }
        }
    }

    /// The shadow collector: count what the frontend receives, as the
    /// collector does, and record what it learns.
    fn at_frontend(&mut self, from: u8, message: ProtocolMessage) {
        let (b, command) = match &message {
            ProtocolMessage::FastAck(a) => (a.ballot, a.command),
            ProtocolMessage::SlowAck(a) => (a.ballot, a.command),
            ProtocolMessage::LeaderReply {
                ballot: b, command, ..
            } => (*b, *command),
            // Another command is bound under this one's retry key: the
            // collector settles it as a conflict from the record of that
            // command (task-d22).
            ProtocolMessage::Refused {
                command,
                refusal: coord_consensus::SubmissionRefusal::OtherCommand { bound },
                ..
            } => {
                self.refused_for.insert(*command, *bound);
                return;
            }
            ProtocolMessage::Refused {
                command,
                refusal: coord_consensus::SubmissionRefusal::Forgotten,
                ..
            } => {
                self.forgotten.insert(*command);
                return;
            }
            _ => return,
        };
        let n = self.n;
        let shadow = self
            .shadow
            .entry((b.number, b.leader, command))
            .or_default();
        let set = shadow
            .set
            .get_or_insert_with(|| VoteSet::new(quorum(b, n), command));
        // A leader reply is compact: the collector counts it under the
        // admission its entry was submitted under, never under whatever
        // acknowledgement came first.
        if shadow.admission.is_none() {
            shadow.admission = self.submitted.get(&command).copied();
        }
        match message {
            ProtocolMessage::FastAck(a) => {
                let _ = set.add(Vote::Fast(a));
            }
            ProtocolMessage::SlowAck(a) => {
                let _ = set.add(Vote::Slow(a));
            }
            ProtocolMessage::LeaderReply {
                ballot: b,
                command,
                seqnum,
                deps,
                path,
            } => shadow.leader.push(FastAck {
                replica: r(from),
                ballot: b,
                command,
                deps,
                paths: Vec::new(),
                path,
                admission: Digest32([0; 32]),
                seqnum: Some(seqnum),
            }),
            _ => unreachable!(),
        }
        if let Some(admission) = shadow.admission {
            for mut reply in core::mem::take(&mut shadow.leader) {
                reply.admission = admission;
                let _ = set.add(Vote::Fast(reply));
            }
        }
        if let Some(learned) = set.learned() {
            let deps = learned.deps().to_vec();
            if self.trace.is_some() && !self.decided.contains_key(&command) {
                let voted = format!(
                    "{:?}",
                    set.voted().iter().map(|r| r.0[0]).collect::<Vec<_>>()
                );
                let kind = format!("{learned:?}").chars().take(4).collect::<String>();
                self.note(|| {
                    format!(
                        "frontend learned {} in {} {kind} deps[{}] voters {voted}",
                        short(&command),
                        bn(&b),
                        shorts(&deps)
                    )
                });
            }
            if self.learned.insert(command) {
                self.stats.learned += 1;
            }
            self.decide(command, &deps, &format!("learned at the frontend in {b:?}"));
        }
    }

    fn decide(&mut self, command: CommandId, deps: &[CommandId], source: &str) {
        let set: BTreeSet<CommandId> = deps.iter().copied().collect();
        match self.decided.get(&command) {
            None => {
                self.decided.insert(command, (set, source.to_owned()));
            }
            Some((held, first)) if *held != set => self.fail(&format!(
                "{command:?} has two dependency sets: {held:?} ({first}) and {set:?} ({source})"
            )),
            Some(_) => {}
        }
    }

    /// Change roles as `coordd`'s driver does (`Node::change_role`).
    fn convert(&mut self, i: u8) {
        let node = &mut self.nodes[usize::from(i)];
        let effects = match node.role.take() {
            Some(Role::Leader(l)) if l.deposed() => {
                let q = l.config_quorum();
                let pending = l.pending_sync().cloned();
                let mut f = Follower::from_recovered(l.into_recovered(), q);
                f.set_selection_rules(RULES);
                // A new machine: its examinations count from here, against
                // what it is given from here, its own carried selection
                // included (task-d34), bounded by the table.
                self.sync_work[usize::from(i)] = 2 * self.knobs.capacity as u64;
                let effects = match pending {
                    Some((from, d)) => {
                        self.sync_work[usize::from(i)] += sync_size(&d);
                        let most = &mut self.sync_entries[usize::from(i)];
                        *most = (*most).max(sync_entries(&d));
                        f.on_sync(from, d)
                    }
                    None => Vec::new(),
                };
                node.role = Some(Role::Follower(f));
                self.window[usize::from(i)] = false;
                if let Some(t) = self.trace.as_mut() {
                    t.push_back(format!("{:>5} node {i} deposed", self.step));
                }
                effects
            }
            Some(Role::Follower(f)) if f.won().is_some() => {
                let decision = f.won().cloned().expect("won");
                if let Some(t) = self.trace.as_mut()
                    && let Some(c) = f.campaign_state()
                {
                    if let Ok(w) = std::env::var("PROTOCOL_SIM_WATCH") {
                        for r in c.reports() {
                            for e in r.entries.iter().filter(|e| short(&e.command) == w) {
                                t.push_back(format!(
                                    "{:>5} report of {} for {}: {e:?}",
                                    self.step,
                                    r.replica.0[0],
                                    bn(&r.ballot)
                                ));
                            }
                        }
                    }
                    for r in c.reports() {
                        t.push_back(format!(
                            "{:>5} node {i} selected from {} synced {} [{}]",
                            self.step,
                            r.replica.0[0],
                            bn(&r.committed_ballot),
                            r.entries
                                .iter()
                                .map(|e| format!(
                                    "{}:{:?}<{}>",
                                    short(&e.command),
                                    e.phase,
                                    shorts(&e.deps)
                                ))
                                .collect::<Vec<_>>()
                                .join(" ")
                        ));
                    }
                }
                let q = f.quorum().clone();
                let (mut l, effects) = Leader::from_recovered(f.into_recovered(), q, &decision);
                l.set_learning(LearningMode::Full);
                l.observe_learning();
                // The node's setting, given to every leader it becomes,
                // as `coordd`'s node does (task-d98).
                l.set_early_proposal(self.knobs.early);
                node.role = Some(Role::Leader(l));
                if let Some(t) = self.trace.as_mut() {
                    t.push_back(format!(
                        "{:>5} node {i} leads {}",
                        self.step,
                        describe(&ProtocolMessage::Sync(decision))
                    ));
                }
                effects
            }
            other => {
                node.role = other;
                return;
            }
        };
        self.handle(i, effects);
    }

    fn step_node(&mut self, i: u8, event: Event) {
        let decided_before = match &self.nodes[usize::from(i)].role {
            Some(Role::Follower(f)) => f.campaign_state().and_then(|c| c.decision()).is_some(),
            _ => true,
        };
        let watched_reports = match &self.nodes[usize::from(i)].role {
            Some(Role::Follower(f)) if std::env::var_os("PROTOCOL_SIM_WATCH").is_some() => {
                f.campaign_state().map(|c| c.reports().to_vec())
            }
            _ => None,
        };
        let phase_watch: Option<(CommandId, Option<coord_consensus::Phase>, Vec<CommandId>)> =
            std::env::var("PROTOCOL_SIM_WATCH_PHASE")
                .ok()
                .and_then(|w| {
                    let c = self
                        .order
                        .iter()
                        .chain(self.offers.keys())
                        .find(|c| short(c) == w)
                        .copied()?;
                    let deps = match &self.nodes[usize::from(i)].role {
                        Some(Role::Leader(l)) => l.table().record(&c).map(|r| r.deps.clone()),
                        Some(Role::Follower(f)) => f.table().record(&c).map(|r| r.deps.clone()),
                        None => None,
                    };
                    Some((c, self.phase(i, &c), deps.unwrap_or_default()))
                });
        let acked = match &event {
            Event::Peer(m) => match ProtocolMessage::decode(m.frame()) {
                Ok(ProtocolMessage::FastAck(a)) => Some(a.command),
                Ok(ProtocolMessage::SlowAck(a)) => Some(a.command),
                _ => None,
            },
            _ => None,
        };
        let effects = self.nodes[usize::from(i)].step(event);
        match &mut self.nodes[usize::from(i)].role {
            Some(Role::Leader(l)) => self.counted.extend(l.take_learned()),
            Some(Role::Follower(f)) => {
                if let Some(c) = acked
                    && f.table().phase_of(&c) >= Some(coord_consensus::Phase::Commit)
                {
                    self.counted.insert(c);
                }
            }
            None => {}
        }
        let mut watch_notes: Vec<String> = Vec::new();
        if let Some((c, before, deps_before)) = phase_watch {
            let deps = match &self.nodes[usize::from(i)].role {
                Some(Role::Leader(l)) => l.table().record(&c).map(|r| r.deps.clone()),
                Some(Role::Follower(f)) => f.table().record(&c).map(|r| r.deps.clone()),
                None => None,
            }
            .unwrap_or_default();
            let after = self.phase(i, &c);
            if after != before || deps != deps_before {
                watch_notes.push(format!(
                    "node {i} {} {before:?}<{}> -> {after:?}<{}>",
                    short(&c),
                    shorts(&deps_before),
                    shorts(&deps)
                ));
            }
        }
        if let Some(reports) = watched_reports
            && let Ok(w) = std::env::var("PROTOCOL_SIM_WATCH")
            && let Some(Role::Follower(f)) = &self.nodes[usize::from(i)].role
            && f.campaign_state().is_none()
        {
            for r in &reports {
                let e = r.entries.iter().find(|e| short(&e.command) == w);
                let after: Vec<String> = r
                    .entries
                    .iter()
                    .filter(|e| e.deps.iter().any(|d| short(d) == w))
                    .map(|e| format!("{}:{:?}", short(&e.command), e.phase))
                    .collect();
                watch_notes.push(format!(
                    "node {i} campaign ended; report {} synced {} {:?} named by [{}]",
                    r.replica.0[0],
                    bn(&r.committed_ballot),
                    e.map(|e| (e.phase, shorts(&e.deps), e.seqnum)),
                    after.join(" ")
                ));
            }
        }
        if !decided_before
            && let Ok(w) = std::env::var("PROTOCOL_SIM_WATCH")
            && let Some(Role::Follower(f)) = &self.nodes[usize::from(i)].role
            && let Some(c) = f.campaign_state()
            && let Some(d) = c.decision()
        {
            let lines: Vec<String> = c
                .reports()
                .iter()
                .map(|r| {
                    let e = r.entries.iter().find(|e| short(&e.command) == w);
                    format!(
                        "  from {} synced {} {:?}",
                        r.replica.0[0],
                        bn(&r.committed_ballot),
                        e.map(|e| (e.phase, shorts(&e.deps), e.seqnum))
                    )
                })
                .collect();
            if std::env::var_os("PROTOCOL_SIM_WATCH_ALL").is_some() {
                for r in c.reports() {
                    let all: Vec<String> = r
                        .entries
                        .iter()
                        .map(|e| {
                            format!(
                                "{}:{:?}<{}>s{}p{:02x}",
                                short(&e.command),
                                e.phase,
                                shorts(&e.deps),
                                e.seqnum,
                                e.path.0[0]
                            )
                        })
                        .collect();
                    let text = format!("  report {} [{}]", r.replica.0[0], all.join(" "));
                    watch_notes.push(text);
                }
                let sel: Vec<String> = d
                    .entries
                    .values()
                    .map(|e| format!("{}:{:?}<{}>", short(&e.command), e.phase, shorts(&e.deps)))
                    .collect();
                watch_notes.push(format!("  selection [{}]", sel.join(" ")));
            }
            let fate = if d.entries.keys().any(|c| short(c) == w) {
                "selected"
            } else if d.reproposed.iter().any(|c| short(c) == w) {
                "re-proposed"
            } else {
                "absent"
            };
            let text = format!(
                "node {i} decided {} with {w} {fate}:\n{}",
                bn(&d.ballot),
                lines.join("\n")
            );
            watch_notes.push(text);
        }
        for text in watch_notes {
            self.note(|| text);
        }
        if !decided_before {
            self.check_left_out(i);
        }
        self.handle(i, effects);
        self.convert(i);
    }

    fn admit(&mut self, seq: u64, key: u8, variant: u8, at: &[u8]) {
        let mut value = seq.to_be_bytes().to_vec();
        value.push(variant);
        let request = LogicalRequest::new(
            NamespaceId([5; 16]),
            CanonicalOperation::Put(PutOp {
                key: vec![key],
                value,
                lease: None,
                prev_kv: false,
            }),
        );
        let rk = RetryKey {
            cluster_id: ClusterId([1; 16]),
            domain_id: DomainId([2; 16]),
            session_id: SessionId([3; 16]),
            client_instance_id: ClientInstanceId([4; 16]),
            request_sequence: RequestSequence::new(seq).unwrap(),
        };
        let frame = MessageV1::Request(RequestV1::new(rk, &request, 0, 0).unwrap())
            .encode()
            .unwrap();
        let command = CommandId::derive(&rk, &request).unwrap();
        let host = at.first().copied().unwrap_or(0);
        self.offers.entry(command).or_insert(Offer {
            seq,
            key,
            variant,
            host,
        });
        self.note(|| format!("admit {} seq {seq}/{variant} at {at:?}", short(&command)));
        for &i in at {
            if !self.nodes[usize::from(i)].alive() {
                continue;
            }
            let receipt = AdmissionReceipt::submitting(
                VerifierToken::for_boundary(),
                AttestedAdmission {
                    cluster: ClusterId([1; 16]),
                    domain: DomainId([2; 16]),
                    session: SessionId([3; 16]),
                    rule_generation: 1,
                    scope_ceiling: u32::MAX,
                    receipt_id: Digest32([9; 32]),
                    admitted_at_ticks: 0,
                },
            );
            self.submitted.insert(
                command,
                coord_core::capability::admission_digest(Some(&receipt.facts()), 0),
            );
            let known = self.holds(i, &command);
            self.step_node(
                i,
                Event::Admitted(AdmittedRequest {
                    receipt,
                    frame: frame.clone(),
                }),
            );
            // Oracle 5: new work is admitted only into the table's
            // unreserved part (task-d24). Work that finishes a decision
            // -- a command a Sync or a campaign selected, or whose turn
            // has come -- takes the reserve and does not stay in
            // PRE-ACCEPT.
            if !known && self.phase(i, &command) == Some(coord_consensus::Phase::PreAccept) {
                let records = self.table_len(i);
                let capacity = self.knobs.capacity;
                if records > capacity - capacity / coord_consensus::RECOVERY_RESERVE_PARTS {
                    self.fail(&format!(
                        "node {i} admitted {} into the reserve: {records} records",
                        short(&command)
                    ));
                }
            }
        }
    }

    fn admit_next(&mut self) {
        for _ in 0..self.knobs.burst {
            if self.admitted >= self.knobs.commands {
                return;
            }
            self.admitted += 1;
            let seq = self.admitted;
            let key = if self.knobs.one_key {
                1
            } else {
                self.below("key", 4) as u8
            };
            let leader = self.leader_index();
            let mut at: Vec<u8> = Vec::new();
            for i in 0..self.n {
                if self.chance("reach", self.knobs.reach) {
                    at.push(i);
                }
            }
            if at.is_empty() {
                at.push(self.below("reach-one", usize::from(self.n)) as u8);
            }
            if self.knobs.followers_first
                && let Some(l) = leader
                && let Some(p) = at.iter().position(|i| *i == l)
            {
                at.remove(p);
                let later = self.step + 1 + self.below("owed", 200) as u32;
                self.owed_to_leader.push((later, seq, key));
            }
            self.admit(seq, key, 0, &at);
            if self.chance("retry", self.knobs.retry) {
                let later = self.step + 1 + self.below("retry-at", 400) as u32;
                let mut nodes: Vec<u8> = Vec::new();
                for i in 0..self.n {
                    if self.knobs.retry_reach >= 1_000_000
                        || self.chance("retry-reach", self.knobs.retry_reach)
                    {
                        nodes.push(i);
                    }
                }
                let variant = u8::from(self.chance("new-payload", self.knobs.new_payload));
                self.retries.push(Retry {
                    at_step: later,
                    seq,
                    key,
                    variant,
                    nodes,
                });
            }
        }
    }

    fn due_admissions(&mut self) {
        let now = self.step;
        let owed: Vec<(u32, u64, u8)> = self
            .owed_to_leader
            .iter()
            .copied()
            .filter(|o| o.0 <= now)
            .collect();
        self.owed_to_leader.retain(|o| o.0 > now);
        for (_, seq, key) in owed {
            if let Some(l) = self.leader_index() {
                self.admit(seq, key, 0, &[l]);
            }
        }
        let due: Vec<Retry> = {
            let (due, keep): (Vec<Retry>, Vec<Retry>) = core::mem::take(&mut self.retries)
                .into_iter()
                .partition(|r| r.at_step <= now);
            self.retries = keep;
            due
        };
        for retry in due {
            self.admit(retry.seq, retry.key, retry.variant, &retry.nodes);
        }
    }

    fn deliver(&mut self) {
        let k = self.below("pick", self.net.len());
        let msg = if self.chance("dup", self.knobs.dup) {
            let m = &self.net[k];
            Msg {
                from: m.from,
                to: m.to,
                frame: m.frame.clone(),
            }
        } else {
            self.net.swap_remove(k)
        };
        if self.chance("loss", self.knobs.loss) {
            return;
        }
        self.deliver_msg(msg);
    }

    /// Hand one message to its node.
    fn deliver_msg(&mut self, msg: Msg) {
        if !self.nodes[usize::from(msg.to)].alive() {
            return;
        }
        // Cut off from its peers (task-d98's deposition).
        if self
            .partition
            .is_some_and(|(p, _)| p == msg.to || p == msg.from)
        {
            return;
        }
        if self.trace.is_some() {
            let d = ProtocolMessage::decode(&msg.frame)
                .map_or_else(|e| format!("{e:?}"), |m| describe(&m));
            let (f, t) = (msg.from, msg.to);
            self.note(|| format!("deliver {f}->{t} {d}"));
        }
        // A catch-up ask is answered by the donor's runtime from its
        // durable rows, not by its machine (task-d08).
        if let Ok(ProtocolMessage::CatchUpRequest { ballot: b, after }) =
            ProtocolMessage::decode(&msg.frame)
        {
            // `coordd`'s donor answers at any ballot of its epoch
            // (`Node::serve_catch_up`, task-d81): what it executed is a
            // prefix of the one executed order, whatever ballot it is
            // synchronized at.
            let epoch = match &self.nodes[usize::from(msg.to)].role {
                Some(Role::Leader(l)) => Some(l.config_quorum().ballot().epoch),
                Some(Role::Follower(f)) => Some(f.quorum().ballot().epoch),
                None => None,
            };
            if epoch != Some(b.epoch) {
                return;
            }
            let page = self.page(msg.to, b, after);
            self.stats.catch_up_pages += 1;
            self.net.push(Msg {
                from: msg.to,
                to: msg.from,
                frame: page.encode(),
            });
            return;
        }
        if let Ok(ProtocolMessage::Sync(d)) = ProtocolMessage::decode(&msg.frame)
            && matches!(
                self.nodes[usize::from(msg.to)].role,
                Some(Role::Follower(_))
            )
        {
            self.sync_work[usize::from(msg.to)] += sync_size(&d);
            self.given_sync(msg.to, &d);
        }
        if let Ok(ProtocolMessage::CatchUpPage { entries, .. }) =
            ProtocolMessage::decode(&msg.frame)
        {
            let most = &mut self.pulled_entries[usize::from(msg.to)];
            *most = (*most).max(entries.len());
        }
        let event = Event::Peer(AuthenticatedPeerMessage::new(
            PeerProvenance::from_transport(r(msg.from), ReplicaIncarnation::new(1).unwrap(), 1),
            msg.frame,
        ));
        self.step_node(msg.to, event);
    }

    /// What `coordd` serves for a catch-up ask at `ballot` after
    /// `after`: the donor's executed commands from there, each with its
    /// durable payload and dependency row, stopping at the first it
    /// cannot show.
    fn page(&self, donor: u8, ballot: Ballot, after: ExecutionPosition) -> ProtocolMessage {
        let node = &self.nodes[usize::from(donor)];
        let payloads: BTreeMap<CommandId, coord_consensus::PayloadRecordV1> =
            payload_rows(&node.storage).into_iter().collect();
        let rows: BTreeMap<CommandId, CommandRecord> =
            dependency_rows(&node.storage).into_iter().collect();
        // Its durable executions: what its store holds (task-d90).
        let executed = &node.executed[..node.durable_executed];
        let mut entries = Vec::new();
        for (i, c) in executed
            .iter()
            .enumerate()
            .skip(usize::try_from(after.get()).unwrap_or(usize::MAX))
            .take(coord_consensus::MAX_CATCH_UP_COMMANDS)
        {
            let Some(payload) = payloads.get(c) else {
                break;
            };
            let response = response_of(&executed[..i], c, |x| self.offers.get(x).map(|o| o.key));
            entries.push(coord_consensus::CatchUpEntry {
                command: *c,
                payload: payload.clone(),
                decided: rows.get(c).cloned(),
                position: ExecutionPosition::new(i as u64 + 1).unwrap(),
                revision: None,
                result_digest: digest_of(&response),
            });
        }
        ProtocolMessage::CatchUpPage {
            ballot,
            after,
            through: ExecutionPosition::new(executed.len() as u64).unwrap(),
            entries,
        }
    }

    fn complete(&mut self, i: u8) {
        let entry = self.nodes[usize::from(i)]
            .journal
            .pop_front()
            .expect("an entry");
        let barrier = match entry {
            Entry::Batch(barrier)
                if !self.healing
                    && self.nodes[usize::from(i)].failable.remove(&barrier)
                    && matches!(self.nodes[usize::from(i)].role, Some(Role::Follower(_)))
                    && self.chance("payload-fail", self.knobs.payload_fail) =>
            {
                // Definitely not committed (task-d90, row 20): the batch
                // never happens, and the journal goes on past it.
                let error = coord_core::event::StorageError::DefinitelyNotCommitted;
                self.nodes[usize::from(i)].storage.fail(barrier, error);
                self.stats.payload_failures += 1;
                self.note(|| format!("failed node {i} {barrier:?}"));
                self.step_node(
                    i,
                    Event::Storage(StorageEvent::Failed {
                        barrier_id: barrier,
                        error,
                    }),
                );
                return;
            }
            Entry::Batch(barrier) => barrier,
            Entry::Group(through) => {
                // The group is durable, with every entry before it: the
                // machine may count those executions durable (task-d90),
                // as `coordd`'s node reports an established result's
                // position once its group is in the journal.
                let node = &mut self.nodes[usize::from(i)];
                node.durable_executed = node.durable_executed.max(through);
                let position = ExecutionPosition::new(through as u64).unwrap();
                match node.role.as_mut().expect("alive") {
                    Role::Leader(m) => m.executions_durable_through(position),
                    Role::Follower(m) => m.executions_durable_through(position),
                }
                self.note(|| format!("durable node {i} executions through {through}"));
                return;
            }
        };
        let seq = self.nodes[usize::from(i)]
            .storage
            .complete(barrier)
            .expect("volatile");
        if self.nodes[usize::from(i)].journal.is_empty() {
            self.window[usize::from(i)] = false;
        }
        self.note(|| format!("durable node {i} {barrier:?}"));
        if self.nodes[usize::from(i)].journal.is_empty() {
            self.nodes[usize::from(i)].payload_window.clear();
        }
        self.step_node(
            i,
            Event::Storage(StorageEvent::JournalDurable {
                barrier_id: barrier,
                journal_seq: seq,
            }),
        );
        self.check_leader_vote(i, barrier);
        self.check_promise(i);
    }

    fn execute(&mut self, i: u8) {
        // `coordd` applies through the same single-writer journal as its
        // protocol batches, so an execution is durable only with every
        // batch queued before it. Without late execution the journal is
        // flushed first and the execution is durable as it is taken; with
        // it, the execution's group joins the journal behind them, and
        // the outcome is taken now (task-d90).
        if !self.knobs.late_execution {
            while !self.nodes[usize::from(i)].journal.is_empty() {
                self.complete(i);
            }
        }
        if !self.executable(i) {
            return;
        }
        let node = &mut self.nodes[usize::from(i)];
        let (c, deps, position) = match node.role.as_ref().expect("alive") {
            Role::Leader(m) => {
                let c = m.next_executable().expect("executable");
                (
                    c,
                    m.table().record(&c).map(|r| r.deps.clone()),
                    m.executed_through(),
                )
            }
            Role::Follower(m) => {
                let c = m.next_executable().expect("executable");
                (
                    c,
                    m.table().record(&c).map(|r| r.deps.clone()),
                    m.executed_through(),
                )
            }
        };
        let position = position.checked_next().unwrap();
        let response = response_of(&node.executed, &c, |x| self.offers.get(x).map(|o| o.key));
        let outcome = AppliedOutcome {
            position,
            revision: None,
            result_digest: digest_of(&response),
            response: response.clone(),
        };
        let (effects, pulled) = match node.role.as_mut().expect("alive") {
            Role::Leader(m) => (m.applied(c, &outcome), false),
            Role::Follower(m) => {
                let before = m.catch_up_counts().1;
                let effects = m.applied(c, &outcome);
                // A pulled command whose execution disagrees with the
                // donor's stops the voter: here it is an oracle failure.
                if let Some(d) = m.catch_up_divergence() {
                    let d = d.clone();
                    self.fail(&format!("node {i} diverged from its catch-up donor: {d:?}"));
                }
                (effects, m.catch_up_counts().1 > before)
            }
        };
        let effects = match effects {
            Ok(e) => e,
            Err(e) => self.fail(&format!("node {i} could not apply {c:?}: {e:?}")),
        };
        if pulled {
            self.stats.caught_up += 1;
        }
        let k = self.nodes[usize::from(i)].executed.len();
        {
            let d = deps.as_deref().map(shorts).unwrap_or_default();
            self.note(|| format!("execute node {i} #{} {} deps[{d}]", k + 1, short(&c)));
        }
        let late = self.knobs.late_execution;
        let node = &mut self.nodes[usize::from(i)];
        node.executed.push(c);
        if late {
            node.journal.push_back(Entry::Group(node.executed.len()));
        } else {
            node.durable_executed = node.executed.len();
        }
        self.stats.executed += 1;
        // Oracle 1: one order.
        match self.order.get(k) {
            Some(expected) if *expected != c => self.fail(&format!(
                "node {i} executed {c:?} at position {} where another executed {expected:?}",
                k + 1
            )),
            Some(_) => {}
            None => {
                self.order.push(c);
                self.executed_anywhere.insert(c);
            }
        }
        // Oracle 7 (task-d90): a result is released once its command is
        // executed in memory, and every execution of it anywhere, before
        // a crash or after, gives the same bytes.
        self.compare_released(i, c, response);
        // Oracle 3: the dependencies it was executed with.
        if let Some(deps) = deps {
            self.decide(c, &deps, &format!("executed by node {i} at {}", k + 1));
        }
        self.handle(i, effects);
        self.convert(i);
    }

    /// Note the commands whose payload row `batch` writes at node `i` over
    /// a dependency row durable there without one (row 20): a crash before
    /// the batch is durable restores the record without its payload row.
    fn note_payload_window(&mut self, i: u8, batch: &coord_core::effect::PersistBatch) {
        let payloads = Collection::PayloadV1.id();
        let written: Vec<CommandId> = batch
            .updates
            .iter()
            .filter(|u| u.collection == payloads && u.value.is_some())
            .filter_map(|u| u.key.as_slice().try_into().ok())
            .map(|k: [u8; 32]| CommandId(Digest32(k)))
            .collect();
        if written.is_empty() {
            return;
        }
        let node = &mut self.nodes[usize::from(i)];
        let without = records_without_payload(&node.storage);
        for c in written {
            if without.contains(&c) {
                node.payload_window.insert(c);
                self.stats.payload_windows += 1;
            }
        }
    }

    /// Oracle 9 (task-d90, point 4): a vote that leaves node `i` -- a
    /// fast or slow acknowledgement, or a report entry that names its
    /// payload present -- rests on the command's payload row durable
    /// there. Recovery re-executes a decided command, so a vote counted
    /// toward a decision without one could leave a released command's
    /// payload durable nowhere once every voter crashes.
    fn check_vote_payload(&mut self, i: u8, message: &ProtocolMessage) {
        let commands: Vec<CommandId> = match message {
            ProtocolMessage::FastAck(a) if a.replica == r(i) => vec![a.command],
            ProtocolMessage::SlowAck(a) if a.replica == r(i) => vec![a.command],
            ProtocolMessage::ReportPage(p) => {
                let boot = self.nodes[usize::from(i)].boot;
                if !self.pages_read.insert((i, boot, p.ballot.number, p.page)) {
                    return;
                }
                p.entries
                    .iter()
                    .filter(|e| e.payload_present)
                    .map(|e| e.command)
                    .collect()
            }
            _ => return,
        };
        let held: BTreeSet<CommandId> = payload_rows(&self.nodes[usize::from(i)].storage)
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        if let Some(c) = commands.iter().find(|c| !held.contains(c)) {
            let what = match message {
                ProtocolMessage::ReportPage(_) => "reported the payload of",
                _ => "voted for",
            };
            self.fail(&format!(
                "node {i} {what} {} with no durable payload row",
                short(c)
            ));
        }
        self.stats.votes_checked += commands.len();
    }

    /// The converse of oracle 9 (task-d90, #197 review): a selection
    /// leaves out a pre-acceptance no reporter holds a payload row for,
    /// which rests on every counted vote's voter reporting its payload
    /// present. So nothing the rule leaves out was ever counted in a
    /// quorum, nor learned or executed anywhere, unless a voter executed
    /// it durably.
    fn check_left_out(&mut self, i: u8) {
        let Some(Role::Follower(f)) = &self.nodes[usize::from(i)].role else {
            return;
        };
        let Some(c) = f.campaign_state() else {
            return;
        };
        let Some(d) = c.decision() else {
            return;
        };
        let waited_on: BTreeSet<CommandId> = d
            .entries
            .values()
            .flat_map(|e| e.deps.iter().copied())
            .collect();
        let mut seen: BTreeSet<CommandId> = BTreeSet::new();
        let mut present: BTreeSet<CommandId> = BTreeSet::new();
        let mut accepted: BTreeSet<CommandId> = BTreeSet::new();
        for r in c.reports() {
            for e in &r.entries {
                seen.insert(e.command);
                if e.payload_present {
                    present.insert(e.command);
                }
                if e.phase >= coord_consensus::Phase::Accept {
                    accepted.insert(e.command);
                }
            }
        }
        let left_out: Vec<CommandId> = seen
            .into_iter()
            .filter(|c| {
                !accepted.contains(c)
                    && !present.contains(c)
                    && !waited_on.contains(c)
                    && !d.entries.contains_key(c)
                    && !d.reproposed.contains(c)
            })
            .collect();
        self.stats.left_out += left_out.len();
        // One a voter executed durably is left out rightly: its executors
        // forgot it and leave it out of their reports, and re-proposed it
        // would be executed twice (task-d77). Catch-up serves it.
        let executed_durably = |c: &CommandId| {
            self.nodes
                .iter()
                .any(|n| n.executed[..n.durable_executed].contains(c))
        };
        let decided: Vec<&CommandId> = left_out
            .iter()
            .filter(|c| self.counted.contains(*c) || self.decided.contains_key(*c))
            .collect();
        let exempt = decided.iter().filter(|c| executed_durably(c)).count();
        self.stats.left_out_executed += exempt;
        if let Some(c) = decided.into_iter().find(|c| !executed_durably(c)) {
            let how = self
                .decided
                .get(c)
                .map_or("counted in a quorum".to_owned(), |(_, how)| how.clone());
            self.fail(&format!(
                "node {i}'s selection for {} left out {}, which was decided: {how}",
                bn(&d.ballot),
                short(c)
            ));
        }
    }

    /// Oracle 9 for a leader's own vote: its proposal, which counts once
    /// the proposal's batch is durable (task-d89), rests on a payload row
    /// durable at the leader then.
    fn check_leader_vote(&mut self, i: u8, barrier: BarrierId) {
        let Some(Role::Leader(m)) = &self.nodes[usize::from(i)].role else {
            return;
        };
        let proposed: Vec<CommandId> = self
            .offers
            .keys()
            .filter(|c| m.proposal(c).is_some_and(|p| p.barrier == barrier))
            .copied()
            .collect();
        if proposed.is_empty() {
            return;
        }
        let held: BTreeSet<CommandId> = payload_rows(&self.nodes[usize::from(i)].storage)
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        if let Some(c) = proposed.iter().find(|c| !held.contains(c)) {
            self.fail(&format!(
                "leader {i}'s proposal of {} is durable with no durable payload row",
                short(c)
            ));
        }
        self.stats.votes_checked += proposed.len();
    }

    /// Oracle 7 for node `i`'s execution of `c` with `response`.
    fn compare_released(&mut self, i: u8, c: CommandId, response: Vec<u8>) {
        match self.released.get(&c) {
            Some(first) if *first != response => self.fail(&format!(
                "node {i} executed {} to a result other than the one released: \
                 {response:02x?}, released {first:02x?}",
                short(&c)
            )),
            Some(_) => self.stats.compared += 1,
            None => {
                self.stats.released += 1;
                self.released.insert(c, response);
            }
        }
    }

    /// Oracle 2 for node `i`.
    fn check_promise(&mut self, i: u8) {
        let node = &self.nodes[usize::from(i)];
        let Some(row) = promise_row(&node.storage) else {
            return;
        };
        if let Some(before) = node.durable_promise
            && row
                .promised
                .compare_same_epoch(&before)
                .is_some_and(|o| o.is_lt())
        {
            self.fail(&format!(
                "node {i}'s durable promise fell from {before:?} to {:?}",
                row.promised
            ));
        }
        if let Some(out) = node.promised_out
            && row
                .promised
                .compare_same_epoch(&out)
                .is_some_and(|o| o.is_lt())
        {
            self.fail(&format!(
                "node {i}'s durable promise {:?} is below the Promise it published for {out:?}",
                row.promised
            ));
        }
        self.nodes[usize::from(i)].durable_promise = Some(row.promised);
    }

    /// Oracle 4, and the rejections drained so they do not pile up.
    fn check_halts(&mut self) {
        for i in 0..self.n {
            let node = &mut self.nodes[usize::from(i)];
            let why = match node.role.as_mut() {
                Some(Role::Follower(f)) => {
                    let rejections = f.take_rejections();
                    if let Some(t) = self.trace.as_mut() {
                        for r in &rejections {
                            let text = format!("{r:?}");
                            t.push_back(format!(
                                "{:>5} node {i} rejected {}",
                                self.step,
                                text.chars().take(200).collect::<String>()
                            ));
                        }
                    }
                    let bad = rejections.into_iter().find(|r| {
                        matches!(
                            r,
                            FollowerRejection::RecoveryCycle { .. }
                                | FollowerRejection::IncompatibleAdmission { .. }
                                | FollowerRejection::Campaign(
                                    RecoveryError::IncompatibleAccepted { .. }
                                        | RecoveryError::IncompatibleAdmission { .. }
                                )
                        )
                    });
                    match (bad, f.halted()) {
                        (Some(r), _) => Some(format!("{r:?}")),
                        (None, Some(c)) => Some(format!("halted on {c:?}")),
                        _ => None,
                    }
                }
                Some(Role::Leader(l)) => {
                    let _ = l.take_rejections();
                    l.recovery_cycle().map(|c| format!("recovery cycle {c:?}"))
                }
                None => None,
            };
            if let Some(why) = why {
                self.fail(&format!("node {i} stopped: {why}"));
            }
        }
    }

    /// Node `i` was given the Sync `d`: its table may hold that many
    /// commands past the capacity.
    fn given_sync(&mut self, i: u8, d: &SyncDecision) {
        let most = &mut self.sync_entries[usize::from(i)];
        *most = (*most).max(sync_entries(d));
    }

    fn crash(&mut self, i: u8) {
        // Every frame travels on its own stream of a connection the crash
        // ends: what was in flight to or from this node is gone with it.
        // (Within a live connection frames are delayed, reordered and
        // lost at will.)
        self.note(|| format!("crash node {i}"));
        // Healing restarts every node; a leader it takes down is no
        // fault of the schedule's.
        if !self.healing && self.leader_index() == Some(i) {
            self.stats.leader_crashes += 1;
        }
        self.net.retain(|m| m.from != i && m.to != i);
        self.held.retain(|m| m.from != i && m.to != i);
        self.window[usize::from(i)] = false;
        let node = &mut self.nodes[usize::from(i)];
        node.role = None;
        node.journal.clear();
        node.payload_window.clear();
        node.failable.clear();
        node.storage.crash();
        // What its materializer executed past its durable groups is gone
        // with the journal (task-d90).
        let lost = node.executed.len() - node.durable_executed;
        node.executed.truncate(node.durable_executed);
        self.stats.executions_lost += lost;
        if lost > 0 {
            self.note(|| format!("node {i} lost {lost} executions"));
        }
        self.stats.crashes += 1;
        self.check_promise(i);
    }

    /// Restart node `i` from its durable rows, as `coordd` does.
    fn restart(&mut self, i: u8) {
        let n = self.n;
        let node = &mut self.nodes[usize::from(i)];
        let promise = promise_row(&node.storage);
        let active = promise.as_ref().map_or(ballot(0, 0), |p| {
            if p.synced
                .compare_same_epoch(&ballot(0, 0))
                .is_some_and(|o| o.is_gt())
            {
                p.synced
            } else {
                ballot(0, 0)
            }
        });
        let syncs: Vec<(Ballot, SyncDecision)> = sync_rows(&node.storage)
            .into_iter()
            .map(|d| (d.ballot, d))
            .collect();
        self.sync_work[usize::from(i)] = syncs.iter().map(|(_, d)| sync_size(d)).sum();
        // The table reloads the rows earlier recovery work wrote, and a
        // Sync row may have gone since: the most this node was ever given
        // stands.
        let rows = syncs
            .iter()
            .map(|(_, d)| sync_entries(d))
            .max()
            .unwrap_or(0);
        let most = &mut self.sync_entries[usize::from(i)];
        *most = (*most).max(rows);
        self.booted_at[usize::from(i)] = self.step;
        self.probed[usize::from(i)] = false;
        self.asked_at[usize::from(i)] = None;
        self.unanswered[usize::from(i)] = 0;
        let executed = ExecutionPosition::new(node.executed.len() as u64).unwrap();
        if self.knobs.payload_crash > 0 {
            self.stats.restored_without_payload += records_without_payload(&node.storage).len();
        }
        let mut f = Follower::recover_with_syncs(
            FollowerConfig {
                identity: identity(i, n),
                genesis: ballot(0, 0),
                quorum: quorum(active, n),
                frontend: FRONTEND,
                capacity: self.knobs.capacity,
            },
            promise,
            None,
            dependency_rows(&node.storage),
            payload_rows(&node.storage),
            syncs,
            executed,
        )
        .restore_execution(executed, node.executed.iter().copied())
        .restore_payloads(payload_rows(&node.storage));
        f.set_learning(LearningMode::Full);
        f.set_selection_rules(RULES);
        // Restored from rows, its executions are the journal's (task-d90).
        if self.knobs.late_execution && !self.knobs.count_in_memory {
            f.track_execution_durability();
        }
        node.boot = node.boot.wrapping_add(1).max(1);
        let boot = node.boot;
        node.role = Some(Role::Follower(f));
        self.note(|| format!("restart node {i} at {}", bn(&active)));
        self.step_node(i, boot_event(boot));
    }

    fn campaign(&mut self, i: u8) {
        self.highest_ballot += 1;
        self.stats.campaigns += 1;
        let b = ballot(self.highest_ballot, i);
        self.note(|| format!("campaign node {i} {}", bn(&b)));
        self.campaign_at[usize::from(i)] = self.step;
        // Whatever was held back arrives from here on: messages of the
        // ballots before this one.
        self.net.append(&mut self.held);
        let effects = match self.nodes[usize::from(i)].role.as_mut() {
            Some(Role::Follower(f)) => f.campaign(b),
            _ => return,
        };
        self.handle(i, effects);
        self.convert(i);
    }

    /// The periodic work of `coordd`'s timers: the leader re-sends what
    /// voters have not voted on and its commit frontier (task-d09, the one
    /// way a follower learns since task-d74), a follower asks its leader for payloads
    /// it lacks, a campaign asks for the report pages that have not
    /// arrived (task-d28), and one that holds work it does not execute
    /// asks for the leader's executed history (task-d08's pacer).
    fn timers(&mut self) {
        for i in 0..self.n {
            let executable = self.executable(i);
            let at = self.nodes[usize::from(i)].executed.len();
            let donor = self.catch_up_donor(i);
            let effects = match self.nodes[usize::from(i)].role.as_mut() {
                Some(Role::Leader(l)) => {
                    let mut effects = l.resend_unvoted(coord_consensus::RESEND_PER_VOTER);
                    effects.extend(l.announce_committed());
                    effects
                }
                Some(Role::Follower(f)) => {
                    let leader = f.quorum().leader();
                    let mut effects = f.request_report_pages();
                    if !f.missing_payloads().is_empty() {
                        effects.extend(f.request_payloads(leader));
                    }
                    // `coordd`'s pacer (task-d08): a voter that holds work
                    // it cannot execute asks, and so does one that has not
                    // asked since it started, whatever it holds; the
                    // leader first, then the other voters in turn.
                    let holds = f.holds_unexecuted() || !self.probed[usize::from(i)];
                    if self.catch_up && !executable && holds && !f.catching_up() {
                        let k = usize::from(i);
                        self.probed[k] = true;
                        if self.asked_at[k] == Some(at) {
                            self.unanswered[k] += 1;
                        } else {
                            self.unanswered[k] = 0;
                        }
                        self.asked_at[k] = Some(at);
                        effects.extend(f.request_catch_up(r(donor)));
                    }
                    effects
                }
                None => Vec::new(),
            };
            self.handle(i, effects);
            self.convert(i);
        }
    }

    /// Whom node `i`'s pacer asks next: the leader for its first two
    /// asks at one frontier, then the other voters in turn.
    fn catch_up_donor(&self, i: u8) -> u8 {
        let leader = match &self.nodes[usize::from(i)].role {
            Some(Role::Follower(f)) => f.quorum().leader().0[0],
            _ => return i,
        };
        let unanswered = self.unanswered[usize::from(i)];
        if unanswered < 2 && leader != i {
            return leader;
        }
        let others: Vec<u8> = (0..self.n).filter(|v| *v != i).collect();
        others[(unanswered.saturating_sub(2)) % others.len()]
    }

    fn executable(&self, i: u8) -> bool {
        match &self.nodes[usize::from(i)].role {
            Some(Role::Leader(m)) => m.next_executable().is_some(),
            Some(Role::Follower(m)) => m.next_executable().is_some(),
            None => false,
        }
    }

    fn run(&mut self) -> Stats {
        let majority = usize::from(self.n) / 2 + 1;
        for step in 0..self.knobs.steps {
            self.step = step;
            self.due_admissions();
            if self.chance("admit", self.knobs.admit) {
                self.admit_next();
            }
            if self.chance("collector-crash", self.knobs.collector_crash) {
                // What the collector counted is gone; what reached the
                // voters is not.
                self.note(|| "collector restarts".to_owned());
                self.shadow.clear();
                self.submitted.clear();
            }
            let alive: Vec<u8> = (0..self.n)
                .filter(|i| self.nodes[usize::from(*i)].alive())
                .collect();
            let dead: Vec<u8> = (0..self.n)
                .filter(|i| !self.nodes[usize::from(*i)].alive())
                .collect();
            if self.partition.is_some_and(|(_, until)| step >= until) {
                let (p, _) = self.partition.take().expect("checked");
                self.note(|| format!("node {p} joined again"));
            }
            // task-d98's faults, each while the leader holds a proposal
            // it sent early whose batch is not durable. Drawn only in the
            // rows that set them, so every other row's draws are as they
            // were.
            let in_window = self.leader_index().filter(|l| self.window[usize::from(*l)]);
            if self.knobs.execution_lag > 0 && step % LAG_SPELL == 0 {
                self.lagging = Some(self.below("lagging", usize::from(self.n)) as u8);
            }
            // task-d90's crash of every voter, drawn only in the rows that
            // set it and only at a cut, so every other row's draws are as
            // they were.
            let cuts = if self.knobs.total_crash > 0 {
                self.cuts()
            } else {
                [false; 3]
            };
            if cuts.iter().any(|c| *c)
                && !alive.is_empty()
                && self.chance("total-crash", self.knobs.total_crash)
            {
                self.stats.total_crashes += 1;
                self.stats.cut_decided += u32::from(cuts[0]);
                self.stats.cut_released += u32::from(cuts[1]);
                self.stats.cut_durable += u32::from(cuts[2]);
                self.note(|| format!("every voter crashes at cuts {cuts:?}"));
                for i in alive.iter().copied() {
                    self.crash(i);
                }
            } else if self.knobs.payload_crash > 0
                && alive.len() > majority
                && let Some(f) = alive
                    .iter()
                    .copied()
                    .find(|i| !self.nodes[usize::from(*i)].payload_window.is_empty())
                && self.chance("payload-crash", self.knobs.payload_crash)
            {
                // Between a follower's payload batch and the vote that
                // waits on it (task-d90, point 4).
                self.stats.payload_crashes += 1;
                self.note(|| format!("node {f} crashes writing a payload row"));
                self.crash(f);
            } else if self.knobs.gate_crash > 0
                && alive.len() > majority
                && let Some(l) = self.gate_open()
                && self.chance("gate-crash", self.knobs.gate_crash)
            {
                // task-d89's window (task-d90's acceptance).
                self.stats.gate_crashes += 1;
                self.note(|| format!("leader {l} crashes in the gate's window"));
                self.crash(l);
            } else if self.knobs.window_crash > 0
                && let Some(l) = in_window
                && alive.len() > majority
                && self.chance("window-crash", self.knobs.window_crash)
            {
                self.stats.window_crashes += 1;
                self.crash(l);
            } else if self.knobs.window_depose > 0
                && let Some(l) = in_window
                && self.partition.is_none()
                && self.chance("window-depose", self.knobs.window_depose)
            {
                // Cut off: what was in flight to or from it is lost, and
                // a follower that can no longer hear it campaigns.
                self.stats.window_deposes += 1;
                let until = step + 40 + self.below("partition-length", 200) as u32;
                self.note(|| format!("node {l} cut off until {until}"));
                self.partition = Some((l, until));
                self.net.retain(|m| m.from != l && m.to != l);
                let followers: Vec<u8> = alive
                    .iter()
                    .copied()
                    .filter(|i| *i != l)
                    .filter(|i| matches!(self.nodes[usize::from(*i)].role, Some(Role::Follower(_))))
                    .collect();
                if !followers.is_empty() {
                    let who = followers[self.below("depose-candidate", followers.len())];
                    self.campaign(who);
                }
            } else if alive.len() > majority && self.chance("crash", self.knobs.crash) {
                let candidates: Vec<u8> = alive
                    .iter()
                    .copied()
                    .filter(|i| self.campaigning(*i))
                    .collect();
                let victim = if self.knobs.crash_candidate && !candidates.is_empty() {
                    candidates[self.below("candidate-victim", candidates.len())]
                } else if self.knobs.crash_leader
                    && let Some(l) = self.leader_index()
                {
                    l
                } else {
                    alive[self.below("victim", alive.len())]
                };
                self.crash(victim);
            } else if !dead.is_empty() && self.chance("restart", self.knobs.restart) {
                let back = dead[self.below("back", dead.len())];
                self.restart(back);
            } else if self.chance("campaign", self.knobs.campaign)
                || (self.leader_index().is_none() && self.chance("leaderless", 20_000))
            {
                let followers: Vec<u8> = alive
                    .iter()
                    .copied()
                    .filter(|i| matches!(self.nodes[usize::from(*i)].role, Some(Role::Follower(_))))
                    .collect();
                if !followers.is_empty() {
                    let who = followers[self.below("candidate", followers.len())];
                    self.campaign(who);
                }
            } else if self.chance("timers", 30_000) {
                self.timers();
            } else {
                self.work();
            }
            self.check_halts();
            self.check_budgets();
            self.check_forgetting();
        }
        self.heal();
        self.check_released();
        self.stats.ballots = self.highest_ballot;
        self.stats.gate_windows = self.gate_seen.len();
        let mut decisions = 0xcbf2_9ce4_8422_2325;
        for (i, node) in self.nodes.iter().enumerate() {
            decisions = fold(decisions, &(i as u64).to_be_bytes());
            for c in &node.executed {
                decisions = fold(decisions, &c.0.0);
            }
        }
        for (c, (deps, _)) in &self.decided {
            decisions = fold(decisions, &c.0.0);
            for d in deps {
                decisions = fold(decisions, &d.0.0);
            }
        }
        self.stats.decisions = decisions;
        self.stats
    }

    /// The leader, while it has counted a fast quorum over a proposal of
    /// its own whose batch is not durable (task-d89's window).
    fn gate_open(&mut self) -> Option<u8> {
        let l = self.leader_index()?;
        let Some(Role::Leader(m)) = &self.nodes[usize::from(l)].role else {
            return None;
        };
        let open: Vec<CommandId> = self
            .offers
            .keys()
            .filter(|c| {
                m.proposal(c).is_some_and(|p| !p.durable)
                    && m.votes(c).is_some_and(|v| v.learned().is_some())
            })
            .copied()
            .collect();
        if open.is_empty() {
            return None;
        }
        self.gate_seen.extend(open);
        Some(l)
    }

    /// The cuts the domain is at, of task-d90's three (crash tests at
    /// every cut): a command the frontend learned that no voter has
    /// executed; a result released whose group no voter holds durable;
    /// and an execution durable at one live voter and not at another.
    fn cuts(&self) -> [bool; 3] {
        let executed_nowhere = self
            .learned
            .iter()
            .any(|c| self.nodes.iter().all(|n| !n.executed.contains(c)));
        let durable_nowhere = self.nodes.iter().any(|n| {
            n.executed[n.durable_executed..].iter().any(|c| {
                self.nodes
                    .iter()
                    .all(|m| !m.executed[..m.durable_executed].contains(c))
            })
        });
        let live: Vec<usize> = self
            .nodes
            .iter()
            .filter(|n| n.alive())
            .map(|n| n.durable_executed)
            .collect();
        let uneven = live.iter().max() != live.iter().min();
        [executed_nowhere, durable_nowhere, uneven]
    }

    /// Oracle 8 (task-d90, point 2): a voter forgets a command only once
    /// its execution is durable there. A forgotten command is refused and
    /// left out of the voter's reports on the ground that its effects are
    /// in the voter's durable state; one whose group a crash can still
    /// take back is not.
    fn check_forgetting(&mut self) {
        for i in 0..self.n {
            let node = &self.nodes[usize::from(i)];
            let undurable = &node.executed[node.durable_executed..];
            self.stats.peak_undurable = self.stats.peak_undurable.max(undurable.len());
            let table = match &node.role {
                Some(Role::Leader(l)) => l.table(),
                Some(Role::Follower(f)) => f.table(),
                None => continue,
            };
            if let Some(c) = undurable.iter().find(|c| table.forgotten(c)) {
                let c = *c;
                self.fail(&format!(
                    "node {i} forgot {} before its execution was durable \
                     ({} of {} executions durable)",
                    short(&c),
                    node.durable_executed,
                    node.executed.len()
                ));
            }
        }
    }

    /// Oracle 7 once healed (task-d90): every voter's execution of every
    /// released command, read again from its order, gives the released
    /// bytes.
    fn check_released(&mut self) {
        for i in 0..self.n {
            let executed = self.nodes[usize::from(i)].executed.clone();
            for (k, c) in executed.iter().enumerate() {
                let response =
                    response_of(&executed[..k], c, |x| self.offers.get(x).map(|o| o.key));
                if self.released.get(c).is_some_and(|r| *r != response) {
                    self.fail(&format!(
                        "after healing node {i} holds {} at {} with a result other than \
                         the one released",
                        short(c),
                        k + 1
                    ));
                }
            }
        }
    }

    /// One unit of ordinary work: a delivery, a journal completion or an
    /// execution.
    fn work(&mut self) {
        let journals: Vec<u8> = (0..self.n)
            .filter(|i| !self.nodes[usize::from(*i)].journal.is_empty())
            .collect();
        let runnable: Vec<u8> = (0..self.n).filter(|i| self.executable(*i)).collect();
        let kinds = [
            !self.net.is_empty(),
            !journals.is_empty(),
            !runnable.is_empty(),
        ];
        let choices: Vec<usize> = (0..3).filter(|k| kinds[*k]).collect();
        if !choices.is_empty() {
            match choices[self.below("work", choices.len())] {
                0 => self.deliver(),
                1 => {
                    let i = journals[self.below("journal", journals.len())];
                    // An execution group put off (task-d90): the journal
                    // completes in order, so what is behind it waits too. Or a
                    // leader's journal that lags its followers' (task-d98).
                    if (self.knobs.execution_lag > 0
                        && self.lagging == Some(i)
                        && matches!(
                            self.nodes[usize::from(i)].journal.front(),
                            Some(Entry::Group(_))
                        )
                        && self.chance("execution-lag", self.knobs.execution_lag))
                        || (self.knobs.leader_journal_lag > 0
                            && self.leader_index() == Some(i)
                            && self.chance("leader-journal-lag", self.knobs.leader_journal_lag))
                    {
                        return;
                    }
                    self.complete(i);
                }
                _ => {
                    let i = runnable[self.below("run", runnable.len())];
                    self.execute(i);
                }
            }
        }
    }

    /// Each node's role, for a report: `L` leads, `l` holds the leader
    /// role without leading, `C` campaigns, `F` follows, `-` is down; with
    /// its promised ballot's number and table size.
    fn roles(&self) -> String {
        (0..self.n)
            .map(|i| match &self.nodes[usize::from(i)].role {
                Some(Role::Leader(l)) => format!(
                    "{}b{}t{}",
                    if l.is_leading() { "L" } else { "l" },
                    l.config_quorum().ballot().number,
                    l.table().len()
                ),
                Some(Role::Follower(f)) => format!(
                    "{}b{}t{}",
                    if self.campaigning(i) { "C" } else { "F" },
                    f.quorum().ballot().number,
                    f.table().len()
                ),
                None => "-".to_owned(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn holds(&self, i: u8, command: &CommandId) -> bool {
        match &self.nodes[usize::from(i)].role {
            Some(Role::Leader(l)) => l.table().record(command).is_some(),
            Some(Role::Follower(f)) => f.table().record(command).is_some(),
            None => false,
        }
    }

    fn phase(&self, i: u8, command: &CommandId) -> Option<coord_consensus::Phase> {
        match &self.nodes[usize::from(i)].role {
            Some(Role::Leader(l)) => l.table().phase_of(command),
            Some(Role::Follower(f)) => f.table().phase_of(command),
            None => None,
        }
    }

    fn table_len(&self, i: u8) -> usize {
        match &self.nodes[usize::from(i)].role {
            Some(Role::Leader(l)) => l.table().len(),
            Some(Role::Follower(f)) => f.table().len(),
            None => 0,
        }
    }

    fn campaigning(&self, i: u8) -> bool {
        matches!(
            &self.nodes[usize::from(i)].role,
            Some(Role::Follower(f)) if f.campaign_state().is_some() && f.won().is_none()
        )
    }

    /// Oracle 5: every limit of the resource contract the machines hold
    /// (design Section 13.1), at its peak.
    fn check_budgets(&mut self) {
        let n = usize::from(self.n);
        let majority = n / 2 + 1;
        let capacity = self.knobs.capacity;
        // A report's pages: its live records and its retirement window,
        // at most one page past what they take (task-d28).
        let report_pages = (2 * capacity).div_ceil(coord_consensus::MAX_PAGE_ENTRIES) + 1;
        // The ledger, payloads, votes and bindings: the live and recently
        // retired commands, swept once they pass four tables; the peak is
        // those four, the live table and the retirement window the sweep
        // goes down to, six tables in all.
        let kept = 6 * capacity;
        for i in 0..self.n {
            let node = &self.nodes[usize::from(i)];
            let Some(role) = node.role.as_ref() else {
                continue;
            };
            let (table, ledger, bindings) = match role {
                Role::Leader(l) => (l.table(), l.ledger().len(), l.bindings_held()),
                Role::Follower(f) => (f.table(), f.ledger().len(), f.bindings_held()),
            };
            let mut over = Vec::new();
            let records = table.len();
            // Admitted work stops an eighth short of the capacity; recovery
            // work (a Sync's entries, a candidate's selection, pulled
            // commands) may pass it, bounded by the largest Sync and the
            // largest catch-up page this node was given, and a command
            // whose turn has come by one record (task-d71); nothing
            // executed is kept past it (task-d24, design Section 13.1).
            let (sync, pulled) = (
                self.sync_entries[usize::from(i)],
                self.pulled_entries[usize::from(i)],
            );
            if records > capacity + sync + pulled + 1 {
                let mut phases: BTreeMap<String, usize> = BTreeMap::new();
                for (_, r) in table.records() {
                    *phases.entry(format!("{:?}", r.phase)).or_default() += 1;
                }
                let placeholders = records - phases.values().sum::<usize>();
                let kind = matches!(role, Role::Leader(_));
                over.push(format!(
                    "{records} table records, capacity {capacity}, largest Sync {sync}, \
                     largest catch-up page {pulled}, leader {kind}: {phases:?}, \
                     {placeholders} placeholders"
                ));
            }
            let tombstones = table.tombstones().len();
            if tombstones > capacity + KEYS {
                over.push(format!("{tombstones} tombstones"));
            }
            if ledger > kept {
                over.push(format!("{ledger} ledger records"));
            }
            if bindings > kept {
                over.push(format!("{bindings} retry-key bindings"));
            }
            let mut held = 0;
            let mut examinations_pct = 0;
            if let Role::Follower(f) = role {
                held = f.held().len();
                if held > coord_consensus::HELD_PROPOSAL_SLACK * capacity {
                    over.push(format!("{held} held proposals"));
                }
                if let Some(c) = f.campaign_state() {
                    if c.pages_held() > n * report_pages {
                        over.push(format!("{} candidate report pages", c.pages_held()));
                    }
                    // Once a majority could be complete, then once per
                    // report that completes and once per payload or
                    // execution the selection may have waited on.
                    if c.assemblies() > (n - majority + 1) as u64 + c.supply_moves() {
                        over.push(format!(
                            "{} report assemblies, {} supplies",
                            c.assemblies(),
                            c.supply_moves()
                        ));
                    }
                }
                let served =
                    f.report_pages_held() - f.campaign_state().map_or(0, |c| c.pages_held());
                if served > report_pages {
                    over.push(format!("{served} served report pages"));
                }
                // Installing a Sync examines an entry once per change to
                // what it waits on: linear in its entries and edges
                // (task-d26). A candidate's own selection is installed as
                // one. What a replica carries into a Sync it was given,
                // the commits it holds that the Sync leaves out (task-d80),
                // is installed with it.
                if let Some(d) = f.campaign_state().and_then(|c| c.decision())
                    && self.selection_counted[usize::from(i)] != Some(d.ballot)
                {
                    self.selection_counted[usize::from(i)] = Some(d.ballot);
                    self.sync_work[usize::from(i)] += sync_size(d);
                    let entries = sync_entries(d);
                    let most = &mut self.sync_entries[usize::from(i)];
                    *most = (*most).max(entries);
                }
                let work = self.sync_work[usize::from(i)] + f.sync_carried();
                let examinations = f.sync_examinations();
                if examinations > 4 * work {
                    over.push(format!(
                        "{examinations} Sync examinations for {work} entries and edges"
                    ));
                }
                examinations_pct = (examinations * 100).checked_div(work).unwrap_or(0);
            }
            let s = &mut self.stats;
            s.peak_records = s.peak_records.max(records);
            s.peak_ledger = s.peak_ledger.max(ledger);
            s.peak_bindings = s.peak_bindings.max(bindings);
            s.peak_held = s.peak_held.max(held);
            s.peak_examinations_pct = s.peak_examinations_pct.max(examinations_pct);
            if !over.is_empty() {
                self.fail(&format!("node {i} over its budget: {}", over.join("; ")));
            }
        }
    }

    /// Whether `command` has an answer its collector can give, as
    /// `coordd`'s does: it learned the decision, or the entry is one only
    /// the durable record answers and the record of the node that hosts
    /// the collector holds it. An entry becomes one when the collector
    /// asks for it again, or a voter refuses it as `Forgotten` or bound to
    /// another command (`Collector::half_established`, task-d22), and
    /// `coordd` then settles it from its own node's executed rows
    /// (`settle_from_record`), or as a conflict when that node's record
    /// binds the key to the other command and executed it
    /// (`settle_conflict_from_record`). What the simulator knows some
    /// other node executed is not an answer.
    fn settled(&self, command: &CommandId) -> bool {
        if self.learned.contains(command) {
            return true;
        }
        let from_record = self.reoffered.contains(command)
            || self.forgotten.contains(command)
            || self.refused_for.contains_key(command);
        if !from_record {
            return false;
        }
        let host = &self.nodes[usize::from(self.offers[command].host)];
        host.executed.contains(command)
            || self
                .refused_for
                .get(command)
                .is_some_and(|bound| host.executed.contains(bound))
    }

    fn unsettled(&self) -> Vec<CommandId> {
        self.offers
            .keys()
            .filter(|c| !self.settled(c))
            .copied()
            .collect()
    }

    /// Every admitted command settled, every decision the frontend
    /// learned or a voter executed is in the one order, and every voter
    /// executed as far as the others and has nothing left it could
    /// execute. A decision learned from a durable majority that the next
    /// selection dropped would otherwise pass as settled.
    fn healed(&self) -> bool {
        self.leader_index().is_some()
            && (0..self.n).all(|i| {
                self.nodes[usize::from(i)].executed.len() == self.order.len() && !self.executable(i)
            })
            && self.learned.is_subset(&self.executed_anywhere)
            && self
                .decided
                .keys()
                .all(|c| self.executed_anywhere.contains(c))
            && self.unsettled().is_empty()
    }

    /// Oracle 6: stop the faults and admission, and give the domain a
    /// budget of steps to settle everything it admitted.
    fn heal(&mut self) {
        self.healing = true;
        self.partition = None;
        self.note(|| "faults stop".to_owned());
        self.net.append(&mut self.held);
        self.retries.clear();
        self.owed_to_leader.clear();
        // Every node restarts, the live ones too: what the domain settles
        // from here it settles from durable rows, as after a restart of
        // the whole domain, not from volatile tables and journal writes a
        // restart would discard.
        for i in 0..self.n {
            if self.nodes[usize::from(i)].alive() {
                self.crash(i);
            }
            self.restart(i);
        }
        let start = self.step + 1;
        self.heal_start = start;
        for k in 0..HEAL_BUDGET {
            self.step = start + k;
            if k % OFFER_EVERY == 0 && self.leader_index().is_some() {
                self.offer_unsettled();
            }
            if k % TIMER_EVERY == 0 {
                self.timers();
            }
            self.heal_election();
            // One round: what is in flight arrives, in the order sent.
            for msg in core::mem::take(&mut self.net) {
                if self.lose_page(&msg) {
                    continue;
                }
                self.deliver_msg(msg);
                self.check_halts();
            }
            for i in 0..self.n {
                while !self.nodes[usize::from(i)].journal.is_empty() {
                    self.complete(i);
                }
                while self.executable(i) {
                    self.execute(i);
                }
            }
            self.check_halts();
            self.check_budgets();
            if self.healed() {
                self.stats.heal_steps = k;
                self.stats.conflicts = self
                    .offers
                    .keys()
                    .filter(|c| !self.learned.contains(c) && !self.executed_anywhere.contains(c))
                    .count();
                return;
            }
        }
        // Without catch-up a voter left behind has no way to heal, and the
        // domain was never meant to: task-d08's pacer is how it does. The
        // run still holds every safety and budget oracle up to here, which
        // is what the sweep without catch-up is for (task-d71).
        if !self.catch_up {
            return;
        }
        let unsettled = self.unsettled();
        let executed: Vec<usize> = self.nodes.iter().map(|n| n.executed.len()).collect();
        let phases: Vec<String> = unsettled
            .iter()
            .take(4)
            .map(|c| {
                let at: Vec<String> = (0..self.n)
                    .map(|i| format!("{:?}", self.phase(i, c)))
                    .collect();
                format!("{} {}", short(c), at.join("/"))
            })
            .collect();
        let lost: Vec<CommandId> = self
            .decided
            .keys()
            .filter(|c| !self.executed_anywhere.contains(c))
            .copied()
            .collect();
        self.fail(&format!(
            "not healed {HEAL_BUDGET} rounds after the faults stopped: roles {}, \
             executed {executed:?} of {}, {} unsettled [{}]; phases {}; \
             {} decided and never executed [{}]",
            self.roles(),
            self.order.len(),
            unsettled.len(),
            shorts(&unsettled),
            phases.join(", "),
            lost.len(),
            shorts(&lost)
        ));
    }

    /// Whether a report page is lost while healing: each voter's first
    /// page of its report to each campaign, once (`heal_page_loss`). The
    /// faults have stopped otherwise, so a campaign that did not ask for
    /// a lost page again (task-d28) is one the ceiling catches.
    fn lose_page(&mut self, msg: &Msg) -> bool {
        if !self.knobs.heal_page_loss {
            return false;
        }
        let Ok(ProtocolMessage::ReportPage(p)) = ProtocolMessage::decode(&msg.frame) else {
            return false;
        };
        if p.page != 0
            || !self
                .pages_lost
                .insert((msg.from, msg.to, p.ballot.number, p.page))
        {
            return false;
        }
        let (f, t, b) = (msg.from, msg.to, bn(&p.ballot));
        self.note(|| format!("lose {f}->{t} report page 0 for {b}"));
        true
    }

    /// The collector presents what has not settled again, to every node
    /// (design Section 5.5, O1).
    fn offer_unsettled(&mut self) {
        let all: Vec<u8> = (0..self.n).collect();
        for c in self.unsettled() {
            self.reoffered.insert(c);
            let o = self.offers[&c];
            self.admit(o.seq, o.key, o.variant, &all);
        }
    }

    /// Whether voter `i` has a leader, as `coordd`'s election decides
    /// it: it leads, or holds the leader role on its way to leading; or
    /// the ballot it promised names another voter that is up, and has
    /// synchronized here or is still within the ceiling to; or it is
    /// campaigning and its campaign is within the ceiling.
    fn led(&mut self, i: u8) -> bool {
        let now = self.step;
        let k = usize::from(i);
        match &self.nodes[k].role {
            Some(Role::Leader(_)) => true,
            Some(Role::Follower(f)) => {
                let promised = f.ballots().promised();
                if promised.leader == r(i) {
                    return f.campaign_state().is_some()
                        && now - self.campaign_at[k].max(self.heal_start) < CEILING;
                }
                if !self.nodes[usize::from(promised.leader.0[0])].alive() {
                    return false;
                }
                if f.quorum().ballot() == promised {
                    self.unsynced[k] = None;
                    return true;
                }
                let since = match self.unsynced[k] {
                    Some((b, since)) if b == promised => since,
                    _ => {
                        self.unsynced[k] = Some((promised, now));
                        now
                    }
                };
                now - since < CEILING
            }
            None => false,
        }
    }

    /// Elections once the faults stop, as `coordd` makes them: a voter
    /// without a leader campaigns after its patience, jittered, and waits
    /// twice as long after each campaign that did not produce one, up to
    /// the ceiling.
    ///
    /// And task-d28's property: a campaign whose promises a majority
    /// still holds, none of them restarted since, completes in its own
    /// ballot within the ceiling. A lost report page is asked for again.
    fn heal_election(&mut self) {
        let now = self.step;
        let majority = usize::from(self.n) / 2 + 1;
        for i in 0..self.n {
            let k = usize::from(i);
            let Some(Role::Follower(f)) = &self.nodes[k].role else {
                continue;
            };
            let Some(c) = f.campaign_state() else {
                continue;
            };
            let live = f.won().is_none()
                && c.promised().len() >= majority
                && c.promised().iter().all(|p| {
                    let v = usize::from(p.0[0]);
                    self.booted_at[v] <= self.campaign_at[k]
                        && match &self.nodes[v].role {
                            Some(Role::Leader(l)) => l.ballots().promised() == c.ballot(),
                            Some(Role::Follower(g)) => g.ballots().promised() == c.ballot(),
                            None => false,
                        }
                });
            // A campaign from before the faults stopped is timed from
            // when they did.
            if live && now - self.campaign_at[k].max(self.heal_start) >= CEILING {
                let b = c.ballot();
                let missing = c.missing_pages();
                self.fail(&format!(
                    "node {i}'s campaign for {} held by a majority did not complete in \
                     {CEILING} rounds; pages missing {missing:?}",
                    bn(&b)
                ));
            }
        }
        for i in 0..self.n {
            let k = usize::from(i);
            if !self.nodes[k].alive() {
                continue;
            }
            if self.led(i) {
                self.leaderless_since[k] = None;
                self.backoff[k] = PATIENCE;
                continue;
            }
            let since = *self.leaderless_since[k].get_or_insert(now);
            if now - since < self.backoff[k] + u32::from(i) {
                continue;
            }
            if matches!(self.nodes[k].role, Some(Role::Follower(_))) {
                self.backoff[k] = (self.backoff[k] * 2).min(CEILING);
                self.leaderless_since[k] = Some(now);
                self.campaign(i);
            }
        }
    }
}

/// The entries and edges a Sync gives a follower to install.
/// The commands a Sync puts in a table: its entries and its re-proposals.
fn sync_entries(d: &SyncDecision) -> usize {
    d.entries.len() + d.reproposed.len()
}

fn sync_size(d: &SyncDecision) -> u64 {
    d.entries
        .values()
        .map(|e| 1 + e.deps.len() as u64)
        .sum::<u64>()
        + d.reproposed.len() as u64
}

/// Trace lines kept: `PROTOCOL_SIM_TRACE_LINES`, or 4,000.
fn trace_lines() -> usize {
    std::env::var("PROTOCOL_SIM_TRACE_LINES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_000)
}

fn short(c: &CommandId) -> String {
    format!("{:02x}{:02x}", c.0.0[0], c.0.0[1])
}

fn shorts(cs: &[CommandId]) -> String {
    cs.iter().map(short).collect::<Vec<_>>().join(",")
}

fn bn(b: &Ballot) -> String {
    format!("b{}/{}", b.number, b.leader.0[0])
}

fn describe(m: &ProtocolMessage) -> String {
    match m {
        ProtocolMessage::Proposal(a) => format!(
            "Proposal {} {} deps[{}] seq{:?} path {:02x}{:02x}",
            bn(&a.ballot),
            short(&a.command),
            shorts(&a.deps),
            a.seqnum,
            a.path.0[0],
            a.path.0[1]
        ),
        ProtocolMessage::FastAck(a) => format!(
            "FastAck {} {} from {} deps[{}] path {:02x}{:02x}",
            bn(&a.ballot),
            short(&a.command),
            a.replica.0[0],
            shorts(&a.deps),
            a.path.0[0],
            a.path.0[1]
        ),
        ProtocolMessage::SlowAck(a) => format!(
            "SlowAck {} {} from {}",
            bn(&a.ballot),
            short(&a.command),
            a.replica.0[0]
        ),
        ProtocolMessage::LeaderReply {
            ballot,
            command,
            deps,
            ..
        } => format!(
            "LeaderReply {} {} deps[{}]",
            bn(ballot),
            short(command),
            shorts(deps)
        ),
        ProtocolMessage::Sync(d) => format!(
            "Sync {} src {} entries[{}] repro[{}]",
            bn(&d.ballot),
            bn(&d.source_ballot),
            d.entries
                .values()
                .map(|e| format!("{}:{:?}<{}>", short(&e.command), e.phase, shorts(&e.deps)))
                .collect::<Vec<_>>()
                .join(" "),
            d.reproposed.iter().map(short).collect::<Vec<_>>().join(",")
        ),
        ProtocolMessage::ReportPage(p) => format!(
            "ReportPage from {} for {} synced {} page {}/{} [{}]",
            p.replica.0[0],
            bn(&p.ballot),
            bn(&p.committed_ballot),
            p.page,
            p.total,
            p.entries
                .iter()
                .map(|e| format!("{}:{:?}<{}>", short(&e.command), e.phase, shorts(&e.deps)))
                .collect::<Vec<_>>()
                .join(" ")
        ),
        ProtocolMessage::CatchUpPage {
            ballot,
            after,
            through,
            entries,
        } => format!(
            "CatchUpPage {} after {} through {} [{}]",
            bn(ballot),
            after.get(),
            through.get(),
            entries
                .iter()
                .map(|e| short(&e.command))
                .collect::<Vec<_>>()
                .join(",")
        ),
        ProtocolMessage::CatchUpRequest { ballot, after } => {
            format!("CatchUpRequest {} after {}", bn(ballot), after.get())
        }
        other => {
            let t = format!("{other:?}");
            t.chars().take(120).collect()
        }
    }
}

/// The commands `storage` holds a dependency row of without a payload
/// row (row 20). A Sync's entries are left out: one restored without a
/// record of its own comes back as a placeholder, which nothing votes on.
fn records_without_payload(storage: &StorageModel) -> BTreeSet<CommandId> {
    let held: BTreeSet<CommandId> = payload_rows(storage).into_iter().map(|(c, _)| c).collect();
    dependency_rows(storage)
        .into_iter()
        .map(|(c, _)| c)
        .filter(|c| !held.contains(c))
        .collect()
}

/// Whether `batch` writes a command's payload row and a dependency row
/// (row 20's failable batches).
fn writes_payload_and_row(batch: &coord_core::effect::PersistBatch) -> bool {
    let payloads = Collection::PayloadV1.id();
    let protocol = Collection::ProtocolV1.id();
    batch
        .updates
        .iter()
        .any(|u| u.collection == payloads && u.value.is_some())
        && batch.updates.iter().any(|u| {
            u.collection == protocol && u.key.len() == 41 && u.key[8] == 0x01 && u.value.is_some()
        })
}

/// The response a node's materializer gives `c` executed after
/// `executed` (oracle 7, task-d90): its position, the command, the
/// command before it on its key and how many came before it there, as a
/// put's revision and previous value would say. It is a function of the
/// order alone, as a result is of the decided commands, so a voter that
/// executes `c` anywhere else in its order gives other bytes.
fn response_of(
    executed: &[CommandId],
    c: &CommandId,
    key_of: impl Fn(&CommandId) -> Option<u8>,
) -> Vec<u8> {
    let key = key_of(c);
    let same: Vec<&CommandId> = executed.iter().filter(|x| key_of(x) == key).collect();
    let mut response = Vec::with_capacity(81);
    response.extend_from_slice(&(executed.len() as u64 + 1).to_be_bytes());
    response.extend_from_slice(&c.0.0);
    response.extend_from_slice(&same.last().map_or([0; 32], |p| p.0.0));
    response.extend_from_slice(&(same.len() as u64).to_be_bytes());
    response.push(key.unwrap_or(0xff));
    response
}

/// A result digest of `response`: four lanes of [`fold`].
fn digest_of(response: &[u8]) -> Digest32 {
    let mut out = [0u8; 32];
    for (lane, chunk) in out.chunks_mut(8).enumerate() {
        let h = fold(0xcbf2_9ce4_8422_2325 ^ lane as u64, response);
        chunk.copy_from_slice(&h.to_be_bytes());
    }
    Digest32(out)
}

fn boot_event(boot: u8) -> Event {
    Event::Boot {
        boot_id: BootId([boot; 16]),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
    }
}

fn dependency_rows(storage: &StorageModel) -> Vec<(CommandId, CommandRecord)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 41 && k[8] == 0x01)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[9..].try_into().unwrap())),
                decode_dependency(&v).unwrap(),
            )
        })
        .collect()
}

fn promise_row(storage: &StorageModel) -> Option<coord_consensus::PromiseRecordV1> {
    storage
        .durable_rows()
        .into_iter()
        .find(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 9)
        .map(|(_, _, v)| decode_promise(&v).unwrap())
}

fn sync_rows(storage: &StorageModel) -> Vec<SyncDecision> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, k, _)| *c == Collection::ProtocolV1.id().0 && k.len() == 33 && k[8] == 0x03)
        .map(|(_, _, v)| decode_sync(&v).unwrap().decision)
        .collect()
}

fn payload_rows(storage: &StorageModel) -> Vec<(CommandId, coord_consensus::PayloadRecordV1)> {
    storage
        .durable_rows()
        .into_iter()
        .filter(|(c, _, _)| *c == Collection::PayloadV1.id().0)
        .map(|(_, k, v)| {
            (
                CommandId(Digest32(k[..].try_into().unwrap())),
                decode_payload(&v).unwrap(),
            )
        })
        .collect()
}

/// Run one scenario for one seed; `Err` carries the oracle's (or the
/// machine's) panic message.
///
/// `catch_up` sets catch-up as it says rather than as the environment
/// does: a kept seed found without it is replayed without it. And
/// `peer_ack_loss` sets the loss of acknowledgements to followers as it
/// says rather than as the row and the environment do: a kept seed found
/// under that loss is replayed under it (task-d78). `early` runs the row
/// with task-d98's early proposals and their faults (`Knobs::with_early`),
/// as rows 1 and 2 also run.
fn run_kept(
    n: u8,
    row: u8,
    seed: u64,
    catch_up: Option<bool>,
    peer_ack_loss: Option<u32>,
    early: bool,
) -> Result<Stats, String> {
    let mut knobs = row_knobs(row);
    if early {
        knobs = knobs.with_early();
    }
    if let Some(loss) = peer_ack_loss {
        knobs.peer_ack_loss = loss;
    }
    run_knobs(n, knobs, seed, catch_up)
}

/// One run with `knobs`, its panic caught and said with its seed.
fn run_knobs(n: u8, knobs: Knobs, seed: u64, catch_up: Option<bool>) -> Result<Stats, String> {
    let row = knobs.row;
    let early = if knobs.early {
        " with early proposals"
    } else {
        ""
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut sim = Sim::new(n, knobs, seed);
        if let Some(on) = catch_up {
            sim.catch_up = on;
        }
        sim.run()
    }));
    outcome.map_err(|e| {
        let text = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        format!("row {row}{early} at {n} voters, seed {seed}: {text}")
    })
}

/// A row's knobs, with `PROTOCOL_SIM_PEER_ACK_LOSS` (parts per million)
/// setting the loss of acknowledgements to followers in every row
/// (task-d78): the weekly sweep runs every row under it, which is what
/// found row 4's seed 720, and rows 16 and 17 alone would not have.
fn row_knobs(row: u8) -> Knobs {
    let mut knobs = Knobs::row(row);
    if let Some(loss) = std::env::var("PROTOCOL_SIM_PEER_ACK_LOSS")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        knobs.peer_ack_loss = loss;
    }
    knobs
}

/// Seeds a negative control may take to fail (task-d98, task-d90).
const NEGATIVE_CONTROL_SEEDS: u64 = 200;

/// Seeds per scenario and size. `PROTOCOL_SIM_SEEDS` raises it for a
/// longer local search.
fn seeds() -> u64 {
    std::env::var("PROTOCOL_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12)
}

fn run_row(row: u8) {
    // Silence the per-seed panic output; failures are gathered and
    // reported together, with their seeds.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut failures = Vec::new();
    let mut totals = Stats::default();
    // Runs that learned nothing, and runs that did, at three and five.
    let (mut idle, mut busy) = ([0u32; 2], [0u32; 2]);
    // Rows 1 and 2 run each seed again with task-d98's early proposals
    // and the faults that reach them, so the rows keep what they read
    // with today's barrier and read the early send too.
    // Row 22 too: early proposals put followers' pre-acceptances out
    // ahead of the leader's batch, in the window it crashes the leader in.
    let variants: &[bool] = if row == 1 || row == 2 || row == 22 {
        &[false, true]
    } else {
        &[false]
    };
    for n in [3u8, 5] {
        for (seed, early) in (0..seeds()).flat_map(|s| variants.iter().map(move |e| (s, *e))) {
            match run_kept(n, row, seed, None, None, early) {
                Ok(s) => {
                    if s.learned == 0 {
                        idle[usize::from(n == 5)] += 1;
                    } else {
                        busy[usize::from(n == 5)] += 1;
                    }
                    totals.executed += s.executed;
                    totals.learned += s.learned;
                    totals.campaigns += s.campaigns;
                    totals.crashes += s.crashes;
                    totals.leader_crashes += s.leader_crashes;
                    totals.catch_up_pages += s.catch_up_pages;
                    totals.caught_up += s.caught_up;
                    totals.ballots += s.ballots;
                    totals.heal_steps = totals.heal_steps.max(s.heal_steps);
                    totals.conflicts += s.conflicts;
                    totals.peak_records = totals.peak_records.max(s.peak_records);
                    totals.peak_ledger = totals.peak_ledger.max(s.peak_ledger);
                    totals.peak_bindings = totals.peak_bindings.max(s.peak_bindings);
                    totals.peak_held = totals.peak_held.max(s.peak_held);
                    totals.peak_examinations_pct =
                        totals.peak_examinations_pct.max(s.peak_examinations_pct);
                    totals.decisions = fold(totals.decisions, &s.decisions.to_be_bytes());
                    totals.released += s.released;
                    totals.compared += s.compared;
                    totals.executions_lost += s.executions_lost;
                    totals.peak_undurable = totals.peak_undurable.max(s.peak_undurable);
                    totals.total_crashes += s.total_crashes;
                    totals.cut_decided += s.cut_decided;
                    totals.votes_checked += s.votes_checked;
                    totals.payload_crashes += s.payload_crashes;
                    totals.payload_windows += s.payload_windows;
                    totals.payload_failures += s.payload_failures;
                    totals.restored_without_payload += s.restored_without_payload;
                    totals.cut_released += s.cut_released;
                    totals.cut_durable += s.cut_durable;
                    totals.gate_windows += s.gate_windows;
                    totals.gate_crashes += s.gate_crashes;
                    totals.left_out += s.left_out;
                    totals.early_sends += s.early_sends;
                    totals.window_crashes += s.window_crashes;
                    totals.window_deposes += s.window_deposes;
                    totals.left_out_executed += s.left_out_executed;
                }
                Err(e) => failures.push(e),
            }
        }
    }
    std::panic::set_hook(hook);
    println!("row {row}: {totals:?}; runs that learned nothing (3, 5 voters): {idle:?}");
    // Said here as well as in the assertion: rows run side by side, and
    // another row's silenced hook may be the one installed when this one
    // panics.
    for f in &failures {
        eprintln!("FAILED {f}");
    }
    assert!(
        failures.is_empty(),
        "{} failing seeds:\n{}",
        failures.len(),
        failures.join("\n")
    );
    // The scenario did something: commands executed, and elections and
    // crashes happened.
    assert!(
        totals.executed > 0 && totals.campaigns > 0,
        "row {row} ran idle: {totals:?}"
    );
    // At each size, most runs learned something: the frontend's oracles
    // (one dependency set, every learned decision executed) are not
    // vacuous there.
    for (size, (i, b)) in ["three", "five"].iter().zip(idle.iter().zip(busy)) {
        assert!(
            b > *i,
            "row {row} at {size} voters: {i} runs learned nothing, {b} did"
        );
    }
    // Row 1 is defined by a leader failing: a schedule that never took
    // the crash branch against a leader has lost its coverage.
    if row == 1 {
        assert!(
            totals.leader_crashes > 0,
            "row 1 crashed no leader: {totals:?}"
        );
    }
    // Row 19 is defined by its cuts (task-d90): every voter crashed at
    // each of them, executions lost to it, and results released and then
    // executed again.
    if row == 19 || row == 23 {
        assert!(
            totals.cut_decided > 0
                && totals.cut_released > 0
                && totals.cut_durable > 0
                && totals.executions_lost > 0
                && totals.compared > 0,
            "row 19 missed a cut: {totals:?}"
        );
    }
    // Row 21 is defined by its failed payload batches (task-d90, point
    // 4): records restored with a dependency row and no payload row, and
    // a follower crashed while it wrote one's payload row again.
    if row == 21 {
        assert!(
            totals.payload_failures > 0
                && totals.restored_without_payload > 0
                && totals.payload_crashes > 0,
            "row 21 missed its cut: {totals:?}"
        );
    }
    // Row 22 is defined by its leader crashes in task-d89's window.
    if row == 22 {
        assert!(
            totals.gate_windows > 0 && totals.gate_crashes > 0,
            "row 22 never crashed a leader in the gate's window: {totals:?}"
        );
    }
    // Oracle 9 ran.
    assert!(
        totals.votes_checked > 0,
        "row {row} checked no vote: {totals:?}"
    );
    // Rows that send early read the window they were made for: proposals
    // left early, and leaders were crashed and deposed while one was not
    // durable (task-d98).
    if variants.contains(&true) || row == 18 || row == 23 {
        assert!(
            totals.early_sends > 0 && totals.window_crashes > 0 && totals.window_deposes > 0,
            "row {row} never reached the early window: {totals:?}"
        );
    }
}

#[test]
fn row_1_fill_capacity_and_fail_the_leader() {
    run_row(1);
}

#[test]
fn row_2_different_maximum_tentative_sets() {
    run_row(2);
}

#[test]
fn row_3_dense_conflicts_and_an_early_missing_dependency() {
    run_row(3);
}

#[test]
fn row_4_acknowledgement_before_payload_repeated() {
    run_row(4);
}

#[test]
fn row_5_client_or_collector_dies_mid_dissemination() {
    run_row(5);
}

#[test]
fn row_9_repeated_interrupted_elections() {
    run_row(9);
}

#[test]
fn row_10_delayed_old_ballot_messages() {
    run_row(10);
}

#[test]
fn row_12_loss_beyond_the_repair_cache_window() {
    run_row(12);
}

#[test]
fn row_14_lost_responses_and_retries_under_one_identity() {
    run_row(14);
}

#[test]
fn row_16_repeated_elections_with_every_acknowledgement_to_a_follower_lost() {
    run_row(16);
}

#[test]
fn row_17_repeated_elections_with_half_the_acknowledgements_to_a_follower_lost() {
    run_row(17);
}

#[test]
fn row_18_the_leaders_proposal_before_its_batch_is_durable() {
    run_row(18);
}

/// Row 18's negative control (task-d98): with the early proposal reaching
/// the fast-set follower too, a seed at three voters fails the oracle.
/// That follower then counts the proposal with its own fast
/// acknowledgement, which at three voters is a fast quorum, and commits
/// over a vote the leader may lose. If no seed failed, the row would not
/// reach the case the per-follower rule exists for.
#[test]
fn row_18_fails_when_the_fast_set_follower_gets_the_proposal_early_too() {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let knobs = Knobs {
        early_to_fast_set: true,
        ..Knobs::row(18)
    };
    let budget = NEGATIVE_CONTROL_SEEDS.max(seeds());
    let failed = (0..budget).find_map(|seed| run_knobs(3, knobs, seed, None).err());
    std::panic::set_hook(hook);
    let Some(failure) = failed else {
        panic!("row 18 passed {budget} seeds with the fast-set follower sent early too");
    };
    println!("the negative control failed as it should: {failure}");
}

#[test]
fn row_23_early_proposals_with_late_execution() {
    run_row(23);
}

#[test]
fn row_19_results_released_before_their_execution_group_is_durable() {
    run_row(19);
}

#[test]
fn row_20_each_vote_on_a_durable_payload_row() {
    run_row(20);
}

#[test]
fn row_21_a_payload_batch_failed_under_a_later_vote() {
    run_row(21);
}

#[test]
fn row_22_the_leader_crashed_in_the_gates_window() {
    run_row(22);
}

/// Row 20's negative control (task-d90, point 4): with a follower's
/// payload rows dropped on their way to its store, some vote leaves on a
/// payload row that is not there, and oracle 9 says so.
#[test]
fn row_20_fails_when_a_vote_does_not_rest_on_its_payload_row() {
    let knobs = Knobs {
        drop_payload_rows: true,
        ..Knobs::row(20)
    };
    let found = first_failure(3, knobs).or_else(|| first_failure(5, knobs));
    println!("row 20 without payload rows: {found:?}");
    assert!(
        found.is_some(),
        "no seed of row 20 failed with followers' payload rows dropped"
    );
}

/// A single-page report with no pre-acceptance's payload named present,
/// paginated again so its digests hold (`hide_preaccepted_payloads`).
fn hide_preaccepted(page: coord_consensus::ReportPage) -> coord_consensus::ReportPage {
    if page.total != 1 {
        return page;
    }
    let mut report = coord_consensus::RecoveryReport {
        replica: page.replica,
        ballot: page.ballot,
        committed_ballot: page.committed_ballot,
        entries: page.entries.clone(),
    };
    // Rebuilt unchanged, it is the page sent, or it is left alone.
    if coord_consensus::paginate(&report, coord_consensus::MAX_PAGE_ENTRIES).first() != Some(&page)
    {
        return page;
    }
    for e in &mut report.entries {
        if e.phase < coord_consensus::Phase::Accept {
            e.payload_present = false;
        }
    }
    coord_consensus::paginate(&report, coord_consensus::MAX_PAGE_ENTRIES).remove(0)
}

/// The converse of oracle 9's negative control (task-d90, #197 review):
/// with voters' reports naming no pre-acceptance's payload present, a
/// selection leaves out a command a quorum decided, and the check says so.
#[test]
fn row_1_fails_when_reports_hide_counted_payloads() {
    let knobs = Knobs {
        hide_preaccepted_payloads: true,
        ..Knobs::row(1)
    };
    let found = first_failure_with(3, knobs, "left out")
        .or_else(|| first_failure_with(5, knobs, "left out"));
    println!("row 1 with pre-acceptances' payloads hidden: {found:?}");
    assert!(
        found.as_deref().is_some_and(|f| f.contains("left out")),
        "no seed of row 1 failed the converse of oracle 9 with payloads hidden: {found:?}"
    );
}

/// The first seed at which `knobs` fail, at `n` voters, with the failure.
fn first_failure(n: u8, knobs: Knobs) -> Option<String> {
    first_failure_with(n, knobs, "")
}

/// The first seed at which `knobs` fail, at `n` voters, with a failure
/// that says `what`.
fn first_failure_with(n: u8, knobs: Knobs, what: &str) -> Option<String> {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let found = (0..NEGATIVE_CONTROL_SEEDS).find_map(|seed| {
        catch_unwind(AssertUnwindSafe(|| Sim::new(n, knobs, seed).run()))
            .err()
            .map(|e| {
                let text = e
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_default();
                format!("seed {seed}: {text}")
            })
            .filter(|f| f.contains(what))
    });
    std::panic::set_hook(hook);
    found
}

/// Row 19's negative control (task-d90, point 2): with the machines
/// counting executions durable as they are taken, a voter forgets a
/// command by what it executed in memory, and some seed fails.
#[test]
fn row_19_fails_when_a_voter_forgets_by_its_executions_in_memory() {
    let knobs = Knobs {
        count_in_memory: true,
        ..Knobs::row(19)
    };
    let found = first_failure(3, knobs).or_else(|| first_failure(5, knobs));
    println!("row 19 counting in memory: {found:?}");
    assert!(
        found.is_some(),
        "no seed of row 19 failed with executions counted in memory"
    );
}

/// A seed kept because it once failed.
#[derive(Deserialize)]
struct Kept {
    row: u8,
    voters: u8,
    seed: u64,
    /// Whether the seed found it with catch-up on (task-d71); absent, as
    /// the environment says.
    #[serde(default)]
    catch_up: Option<bool>,
    /// The loss of acknowledgements to followers it found it under, in
    /// parts per million (task-d78); absent, as the row and the
    /// environment say.
    #[serde(default)]
    peer_ack_loss: Option<u32>,
    /// Whether it found it with task-d98's early proposals; absent, not.
    #[serde(default)]
    early: bool,
    /// What it found, for the reader.
    #[allow(dead_code)]
    found: String,
}

#[test]
fn every_kept_seed_passes() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/protocol_sim/seeds.json"
    );
    let kept: Vec<Kept> = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let failures: Vec<String> = kept
        .iter()
        .filter_map(|k| {
            run_kept(
                k.voters,
                k.row,
                k.seed,
                k.catch_up,
                k.peer_ack_loss,
                k.early,
            )
            .err()
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `PROTOCOL_SIM_ONE=row,voters,seed` runs that one seed, with
/// `PROTOCOL_SIM_TRACE=1` for its trace. Without it, nothing runs.
#[test]
fn one_seed_from_the_environment() {
    let Ok(spec) = std::env::var("PROTOCOL_SIM_ONE") else {
        return;
    };
    let v: Vec<u64> = spec.split(',').map(|x| x.trim().parse().unwrap()).collect();
    let mut knobs = row_knobs(v[0] as u8);
    // `PROTOCOL_SIM_EARLY=1` runs it with task-d98's early proposals, as
    // rows 1 and 2 also run; `=fast-set` as row 18's negative control.
    match std::env::var("PROTOCOL_SIM_EARLY").as_deref() {
        Ok("fast-set") => knobs.early_to_fast_set = true,
        Ok(_) => knobs = knobs.with_early(),
        Err(_) => {}
    }
    let stats = Sim::new(v[1] as u8, knobs, v[2]).run();
    println!("{stats:?}");
}
