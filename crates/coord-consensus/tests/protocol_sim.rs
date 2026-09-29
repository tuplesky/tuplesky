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
//!    admission pauses -- every node restarted, no loss, no duplication,
//!    nothing held back, campaigns only as `coordd`'s election makes them --
//!    every admitted command settles and every voter executes as far as
//!    the others, within a budget of steps. A command settles as the
//!    collector settles it: learned from the evidence, answered from a
//!    record of its execution, or refused for another command bound under
//!    its retry key that executed. The frontend offers what has not
//!    settled again, as the collector re-offers (design Section 5.5, O1).
//!
//! Scenarios are the checklist's failure-test matrix rows 1 to 5, 9, 10,
//! 12 and 14, at three and five voters, with table capacity 32. Seeds that
//! once failed are kept in `fixtures/protocol_sim/seeds.json` and replayed
//! on every run.

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
    };

    /// The checklist's matrix rows 1 to 5, 9, 10, 12 and 14.
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
            _ => unreachable!("not a scenario of the simulator: row {row}"),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Role {
    Leader(Leader),
    Follower(Follower),
}

struct Node {
    role: Option<Role>,
    storage: StorageModel,
    /// Batches submitted and not yet complete, in journal order.
    journal: VecDeque<BarrierId>,
    /// Executed commands, in order. Durable with the application rows.
    executed: Vec<CommandId>,
    boot: u8,
    /// Highest ballot this node published `Promise` for.
    promised_out: Option<Ballot>,
    /// Highest durable promise row seen.
    durable_promise: Option<Ballot>,
}

impl Node {
    fn alive(&self) -> bool {
        self.role.is_some()
    }

    fn step(&mut self, e: Event) -> Vec<Effect> {
        match self.role.as_mut().expect("alive") {
            Role::Leader(m) => m.step(e),
            Role::Follower(m) => m.step(e),
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
    /// Commands executed anywhere: each has a record that answers it.
    executed_anywhere: BTreeSet<CommandId>,
    /// Faults have stopped.
    healing: bool,
    /// Entries and edges of the Syncs each node's current machine was
    /// given to install (oracle 5).
    sync_work: Vec<u64>,
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
                        capacity: CAPACITY,
                    },
                    None,
                    ExecutionPosition::ZERO,
                );
                leader.set_learning(LearningMode::Full);
                Role::Leader(leader)
            } else {
                let mut follower = Follower::new(FollowerConfig {
                    identity: identity(me, n),
                    quorum: quorum(ballot(0, 0), n),
                    genesis: ballot(0, 0),
                    frontend: FRONTEND,
                    capacity: CAPACITY,
                });
                follower.set_learning(LearningMode::Full);
                Role::Follower(follower)
            };
            nodes.push(Node {
                role: Some(role),
                storage: StorageModel::default(),
                journal: VecDeque::new(),
                executed: Vec::new(),
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
            order: Vec::new(),
            step: 0,
            stats: Stats::default(),
            trace: std::env::var_os("PROTOCOL_SIM_TRACE").map(|_| VecDeque::new()),
            offers: BTreeMap::new(),
            learned: BTreeSet::new(),
            refused_for: BTreeMap::new(),
            executed_anywhere: BTreeSet::new(),
            healing: false,
            sync_work: vec![0; usize::from(n)],
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
        };
        for i in 0..n {
            let effects = sim.nodes[usize::from(i)].step(boot_event(1));
            sim.handle(i, effects);
        }
        sim
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
                    // Oracle 5: one journal record (design Section 13.1).
                    let bytes: usize = batch
                        .updates
                        .iter()
                        .map(|u| u.key.len() + u.value.as_ref().map_or(0, Vec::len))
                        .sum();
                    if batch.updates.len() > 4_096 || bytes > 4 << 20 {
                        self.fail(&format!(
                            "node {i} wrote a journal record of {} updates and {bytes} bytes",
                            batch.updates.len()
                        ));
                    }
                    let node = &mut self.nodes[usize::from(i)];
                    node.journal.push_back(batch.barrier);
                    node.storage.submit(batch);
                }
                Effect::SendWhenDurable { to, frame, .. } => {
                    // Oracle 5: a protocol frame, and a Sync's entries.
                    if frame.len() > 4 << 20 {
                        self.fail(&format!("node {i} sent a frame of {} bytes", frame.len()));
                    }
                    let message = ProtocolMessage::decode(&frame);
                    if let Ok(ProtocolMessage::Sync(d)) = &message
                        && d.entries.len() > 5 * coord_consensus::max_report_entries(CAPACITY)
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
                    let to = to.replica.0[0];
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
            self.stats.learned += 1;
            self.learned.insert(command);
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
                // A new machine: its examinations count from here, against
                // what it is given from here, its own carried selection
                // included (task-d34), bounded by the table.
                self.sync_work[usize::from(i)] = 2 * CAPACITY as u64;
                let effects = match pending {
                    Some((from, d)) => {
                        self.sync_work[usize::from(i)] += sync_size(&d);
                        f.on_sync(from, d)
                    }
                    None => Vec::new(),
                };
                node.role = Some(Role::Follower(f));
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
        let effects = self.nodes[usize::from(i)].step(event);
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
        self.offers
            .entry(command)
            .or_insert(Offer { seq, key, variant });
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
                if records > CAPACITY - CAPACITY / coord_consensus::RECOVERY_RESERVE_PARTS {
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
            // `coordd`'s donor answers only while it is synchronized at
            // that ballot or a later one of the epoch
            // (`Node::serve_catch_up`).
            let at_or_after = |own: Ballot| own.compare_same_epoch(&b).is_some_and(|o| o.is_ge());
            let synchronized = match &self.nodes[usize::from(msg.to)].role {
                Some(Role::Leader(l)) => at_or_after(l.config_quorum().ballot()),
                Some(Role::Follower(f)) => {
                    f.quorum().ballot() == f.ballots().synced() && at_or_after(f.ballots().synced())
                }
                None => false,
            };
            if !synchronized {
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
        let mut entries = Vec::new();
        for (i, c) in node
            .executed
            .iter()
            .enumerate()
            .skip(usize::try_from(after.get()).unwrap_or(usize::MAX))
            .take(coord_consensus::MAX_CATCH_UP_COMMANDS)
        {
            let Some(payload) = payloads.get(c) else {
                break;
            };
            entries.push(coord_consensus::CatchUpEntry {
                command: *c,
                payload: payload.clone(),
                decided: rows.get(c).cloned(),
                position: ExecutionPosition::new(i as u64 + 1).unwrap(),
                revision: None,
                result_digest: Digest32(c.0.0),
            });
        }
        ProtocolMessage::CatchUpPage {
            ballot,
            after,
            through: ExecutionPosition::new(node.executed.len() as u64).unwrap(),
            entries,
        }
    }

    fn complete(&mut self, i: u8) {
        let barrier = self.nodes[usize::from(i)]
            .journal
            .pop_front()
            .expect("a batch");
        let seq = self.nodes[usize::from(i)]
            .storage
            .complete(barrier)
            .expect("volatile");
        self.note(|| format!("durable node {i} {barrier:?}"));
        self.step_node(
            i,
            Event::Storage(StorageEvent::JournalDurable {
                barrier_id: barrier,
                journal_seq: seq,
            }),
        );
        self.check_promise(i);
    }

    fn execute(&mut self, i: u8) {
        // `coordd` applies through the same single-writer journal as its
        // protocol batches and waits for the application batch to be
        // durable, so an execution is durable only with every batch
        // queued before it: the journal is flushed first.
        while !self.nodes[usize::from(i)].journal.is_empty() {
            self.complete(i);
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
        let outcome = AppliedOutcome {
            position,
            revision: None,
            result_digest: Digest32(c.0.0),
            response: c.0.0.to_vec(),
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
        self.nodes[usize::from(i)].executed.push(c);
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
        // Oracle 3: the dependencies it was executed with.
        if let Some(deps) = deps {
            self.decide(c, &deps, &format!("executed by node {i} at {}", k + 1));
        }
        self.handle(i, effects);
        self.convert(i);
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

    fn crash(&mut self, i: u8) {
        // Every frame travels on its own stream of a connection the crash
        // ends: what was in flight to or from this node is gone with it.
        // (Within a live connection frames are delayed, reordered and
        // lost at will.)
        self.note(|| format!("crash node {i}"));
        if self.leader_index() == Some(i) {
            self.stats.leader_crashes += 1;
        }
        self.net.retain(|m| m.from != i && m.to != i);
        self.held.retain(|m| m.from != i && m.to != i);
        let node = &mut self.nodes[usize::from(i)];
        node.role = None;
        node.journal.clear();
        node.storage.crash();
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
        self.booted_at[usize::from(i)] = self.step;
        self.probed[usize::from(i)] = false;
        self.asked_at[usize::from(i)] = None;
        self.unanswered[usize::from(i)] = 0;
        let executed = ExecutionPosition::new(node.executed.len() as u64).unwrap();
        let mut f = Follower::recover_with_syncs(
            FollowerConfig {
                identity: identity(i, n),
                genesis: ballot(0, 0),
                quorum: quorum(active, n),
                frontend: FRONTEND,
                capacity: CAPACITY,
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
    /// voters have not voted on, a follower asks its leader for payloads
    /// it lacks, a campaign asks for the report pages that have not
    /// arrived (task-d28), and one that holds work it does not execute
    /// asks for the leader's executed history (task-d08's pacer).
    fn timers(&mut self) {
        for i in 0..self.n {
            let executable = self.executable(i);
            let at = self.nodes[usize::from(i)].executed.len();
            let donor = self.catch_up_donor(i);
            let effects = match self.nodes[usize::from(i)].role.as_mut() {
                Some(Role::Leader(l)) => l.resend_unvoted(coord_consensus::RESEND_PER_VOTER),
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
            if alive.len() > majority && self.chance("crash", self.knobs.crash) {
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
        }
        self.heal();
        self.stats.ballots = self.highest_ballot;
        self.stats
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
        // A report's pages: its live records and its retirement window,
        // at most one page past what they take (task-d28).
        let report_pages = (2 * CAPACITY).div_ceil(coord_consensus::MAX_PAGE_ENTRIES) + 1;
        // The ledger, payloads, votes and bindings: the live and recently
        // retired commands, swept once they pass four tables; the peak is
        // that plus the live table and the retirement window it sweeps
        // down to.
        let kept = 6 * CAPACITY;
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
            // commands, a command whose turn has come) may pass it, bounded
            // by the largest Sync, and nothing executed is kept past it
            // (task-d24, design Section 13.1).
            if records
                > CAPACITY - CAPACITY / coord_consensus::RECOVERY_RESERVE_PARTS
                    + 5 * coord_consensus::max_report_entries(CAPACITY)
            {
                let mut phases: BTreeMap<String, usize> = BTreeMap::new();
                for (_, r) in table.records() {
                    *phases.entry(format!("{:?}", r.phase)).or_default() += 1;
                }
                let placeholders = records - phases.values().sum::<usize>();
                over.push(format!(
                    "{records} table records, capacity {CAPACITY}: {phases:?}, {placeholders} placeholders"
                ));
            }
            let tombstones = table.tombstones().len();
            if tombstones > CAPACITY + KEYS {
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
                if held > 8 * CAPACITY {
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
                // one.
                if let Some(d) = f.campaign_state().and_then(|c| c.decision())
                    && self.selection_counted[usize::from(i)] != Some(d.ballot)
                {
                    self.selection_counted[usize::from(i)] = Some(d.ballot);
                    self.sync_work[usize::from(i)] += sync_size(d);
                }
                let work = self.sync_work[usize::from(i)];
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

    /// Whether `command` has an answer the collector can give.
    fn settled(&self, command: &CommandId) -> bool {
        self.learned.contains(command)
            || self.executed_anywhere.contains(command)
            || self
                .refused_for
                .get(command)
                .is_some_and(|bound| self.executed_anywhere.contains(bound))
    }

    fn unsettled(&self) -> Vec<CommandId> {
        self.offers
            .keys()
            .filter(|c| !self.settled(c))
            .copied()
            .collect()
    }

    /// Every admitted command settled, and every voter executed as far as
    /// the others and has nothing left it could execute.
    fn healed(&self) -> bool {
        self.leader_index().is_some()
            && (0..self.n).all(|i| {
                self.nodes[usize::from(i)].executed.len() == self.order.len() && !self.executable(i)
            })
            && self.unsettled().is_empty()
    }

    /// Oracle 6: stop the faults and admission, and give the domain a
    /// budget of steps to settle everything it admitted.
    fn heal(&mut self) {
        self.healing = true;
        self.note(|| "faults stop".to_owned());
        self.net.append(&mut self.held);
        self.retries.clear();
        self.owed_to_leader.clear();
        for i in 0..self.n {
            if !self.nodes[usize::from(i)].alive() {
                self.restart(i);
            }
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
        self.fail(&format!(
            "not healed {HEAL_BUDGET} rounds after the faults stopped: roles {}, \
             executed {executed:?} of {}, {} unsettled [{}]; phases {}",
            self.roles(),
            self.order.len(),
            unsettled.len(),
            shorts(&unsettled),
            phases.join(", ")
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
fn run_one(n: u8, row: u8, seed: u64) -> Result<Stats, String> {
    let knobs = Knobs::row(row);
    let outcome = catch_unwind(AssertUnwindSafe(|| Sim::new(n, knobs, seed).run()));
    outcome.map_err(|e| {
        let text = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        format!("row {row} at {n} voters, seed {seed}: {text}")
    })
}

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
    for n in [3u8, 5] {
        for seed in 0..seeds() {
            match run_one(n, row, seed) {
                Ok(s) => {
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
                }
                Err(e) => failures.push(e),
            }
        }
    }
    std::panic::set_hook(hook);
    println!("row {row}: {totals:?}");
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
    // Row 1 is defined by a leader failing: a schedule that never took
    // the crash branch against a leader has lost its coverage.
    if row == 1 {
        assert!(
            totals.leader_crashes > 0,
            "row 1 crashed no leader: {totals:?}"
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

/// A seed kept because it once failed.
#[derive(Deserialize)]
struct Kept {
    row: u8,
    voters: u8,
    seed: u64,
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
        .filter_map(|k| run_one(k.voters, k.row, k.seed).err())
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
    let stats = Sim::new(v[1] as u8, Knobs::row(v[0] as u8), v[2]).run();
    println!("{stats:?}");
}
