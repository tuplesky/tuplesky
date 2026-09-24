//! Bounded observability (task-61; design Sections 13, 22.3).
//!
//! Four rules shape every type here, and each of them exists because
//! the obvious alternative is actively misleading:
//!
//! * **An unavailable metric is not zero.** A frontend has no journal;
//!   a stage nobody has exercised has no latency. Reporting `0` for
//!   either is a lie an operator acts on -- a dashboard showing zero
//!   commit latency during an outage reads as "everything is fast".
//!   Every reading is a [`Measure`], and absence says *why* it is
//!   absent.
//! * **Labels are bounded by construction.** There is no map of strings
//!   anywhere in this module. A [`Stage`], a [`Lane`] and a
//!   [`ShardIndex`] are the only things a series is broken down by, and
//!   each has a finite frozen domain, so no key, command identity,
//!   session, principal or caller-supplied name can become a label.
//!   Enforcing it in the types is the only enforcement that holds: a
//!   naming convention is one careless `format!` away from a cardinality
//!   explosion and a data leak in the same line.
//! * **The distinct durability quantities stay distinct.** A sync, the
//!   whole-operation commit-return and backpressure are three different
//!   numbers that a single "write latency" would blur. Section 22.3
//!   asks for whole-operation queue and commit-return, not only sync,
//!   and [`Durability`] keeps all three.
//! * **Reading metrics cannot block anything.** Every counter is an
//!   atomic and [`Recorder::snapshot`] takes no lock, so a slow or stuck
//!   diagnostics reader cannot stall a voter. Diagnostics that could
//!   block consensus would be a liability rather than an aid, and the
//!   only way to be sure is to make blocking impossible rather than
//!   unlikely.
//!
//! What is deliberately absent: anything derived from user data.
//! There are no key names, no command identities, no principals, no
//! addresses and no payload bytes, in any field or any label. The
//! snapshot is numbers and frozen enums, so a scan of a rendered
//! snapshot for secret patterns has nothing to find -- not because it
//! was filtered, but because nothing of that kind was ever put in.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Why a reading is not available.
///
/// Absence is a fact about the node, and the reason is the useful part:
/// "this role has no journal" and "nothing has happened yet" call for
/// entirely different operator responses, and both differ from zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Unavailable {
    /// This role does not have the thing being measured at all. A
    /// frontend has no journal; an observer casts no vote.
    NotThisRole,
    /// Instrumented, but nothing has been observed yet. A latency with
    /// no samples is not a fast one.
    NoSamples,
    /// The subsystem is quarantined, so any reading would describe a
    /// state the node has stopped trusting.
    Quarantined,
    /// The bound this would be measured against is not configured, so
    /// there is no headroom to report.
    NoBound,
    /// This process has no instrumentation point for it: nothing records
    /// the stage, so any count would be a zero nobody measured. Distinct
    /// from [`Unavailable::NoSamples`], which is instrumented and idle.
    NotInstrumented,
}

/// A reading, or the reason there is none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Measure<T> {
    /// Observed.
    Observed(T),
    /// Not available, and why.
    Unavailable(Unavailable),
}

impl<T> Measure<T> {
    /// The observed value, if any.
    pub const fn observed(&self) -> Option<&T> {
        match self {
            Measure::Observed(value) => Some(value),
            Measure::Unavailable(_) => None,
        }
    }

    /// Whether this is an observation.
    pub const fn is_observed(&self) -> bool {
        matches!(self, Measure::Observed(_))
    }

    /// Why there is no reading, if there is none.
    pub const fn why(&self) -> Option<Unavailable> {
        match self {
            Measure::Observed(_) => None,
            Measure::Unavailable(reason) => Some(*reason),
        }
    }
}

/// The stages a node's work passes through, instrumented separately.
///
/// The list is design Section 22.3's, frozen: a stage is added by a
/// reviewed change here and never by a caller passing a name. Measuring
/// them separately is the point -- a single end-to-end latency cannot
/// tell an operator whether a tail spike is verification, the journal,
/// or a peer's queue, which is exactly the question an incident asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum Stage {
    /// Admission and credential verification at the trusted boundary.
    Admission = 0x0001,
    /// Client transit: the caller's request on the wire.
    ClientTransit = 0x0002,
    /// Fan-out to peers.
    FanOut = 0x0003,
    /// Dependency closure.
    DependencyClosure = 0x0004,
    /// The durable journal.
    Journal = 0x0005,
    /// Materialization into the projection.
    Materialization = 0x0006,
    /// Learning from evidence.
    EvidenceLearning = 0x0007,
    /// Stream credits and flow control.
    StreamCredits = 0x0008,
    /// Watches.
    Watches = 0x0009,
    /// Checkpoint publication and rewriting.
    Checkpoint = 0x000a,
    /// Bulk traffic's interference with everything else.
    BulkInterference = 0x000b,
    /// Recovery.
    Recovery = 0x000c,
}

impl Stage {
    /// Every stage, in identifier order.
    pub const ALL: [Stage; 12] = [
        Stage::Admission,
        Stage::ClientTransit,
        Stage::FanOut,
        Stage::DependencyClosure,
        Stage::Journal,
        Stage::Materialization,
        Stage::EvidenceLearning,
        Stage::StreamCredits,
        Stage::Watches,
        Stage::Checkpoint,
        Stage::BulkInterference,
        Stage::Recovery,
    ];

    /// The frozen identifier.
    pub const fn id(self) -> u16 {
        self as u16
    }

    /// A stable low-cardinality name.
    pub const fn name(self) -> &'static str {
        match self {
            Stage::Admission => "admission",
            Stage::ClientTransit => "client-transit",
            Stage::FanOut => "fan-out",
            Stage::DependencyClosure => "dependency-closure",
            Stage::Journal => "journal",
            Stage::Materialization => "materialization",
            Stage::EvidenceLearning => "evidence-learning",
            Stage::StreamCredits => "stream-credits",
            Stage::Watches => "watches",
            Stage::Checkpoint => "checkpoint",
            Stage::BulkInterference => "bulk-interference",
            Stage::Recovery => "recovery",
        }
    }

    /// Whether a node in `roles` has this stage at all.
    ///
    /// What turns a missing reading into [`Unavailable::NotThisRole`]
    /// rather than a gap somebody has to investigate.
    pub fn applies(self, roles: &crate::role::RoleSet) -> bool {
        use crate::role::Role;
        let has = |role: Role| roles.roles().contains(&role);
        match self {
            // Every role journals and materializes its own storage.
            Stage::Journal | Stage::Materialization | Stage::Checkpoint => {
                has(Role::Voter) || has(Role::Observer)
            }
            // Consensus stages belong to a voter.
            Stage::FanOut
            | Stage::DependencyClosure
            | Stage::EvidenceLearning
            | Stage::Recovery => has(Role::Voter),
            // The caller-facing stages belong to whatever serves callers.
            Stage::Admission | Stage::ClientTransit | Stage::Watches => {
                has(Role::Frontend) || has(Role::Observer)
            }
            // Transport accounting exists wherever there is a transport.
            Stage::StreamCredits | Stage::BulkInterference => true,
        }
    }
}

/// A transport lane, as a bounded label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Lane {
    /// Control.
    Control = 0,
    /// Bulk.
    Bulk = 1,
    /// Unary requests.
    Unary = 2,
    /// Watches.
    Watch = 3,
}

impl Lane {
    /// Every lane, in identifier order.
    pub const ALL: [Lane; 4] = [Lane::Control, Lane::Bulk, Lane::Unary, Lane::Watch];

    /// A stable low-cardinality name.
    pub const fn name(self) -> &'static str {
        match self {
            Lane::Control => "control",
            Lane::Bulk => "bulk",
            Lane::Unary => "unary",
            Lane::Watch => "watch",
        }
    }
}

/// A shard index, bounded by construction.
///
/// A shard is a label; a domain identity is not. The shard set is small
/// and fixed by configuration, so breaking a series down by shard stays
/// bounded, while a per-domain series on a multi-tenant node would grow
/// with the tenants -- which is the cardinality explosion Section 13
/// warns about, and also a way to disclose which domains exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardIndex(u16);

/// Most shards a node reports separately. Beyond this the accounting is
/// aggregated rather than allowed to grow.
pub const MAX_REPORTED_SHARDS: u16 = 64;

impl ShardIndex {
    /// Construct, refusing an index beyond the reporting bound.
    pub const fn new(index: u16) -> Option<ShardIndex> {
        if index < MAX_REPORTED_SHARDS {
            Some(ShardIndex(index))
        } else {
            None
        }
    }

    /// Raw index.
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// Count, total and maximum of one measured wait.
///
/// Deliberately not a mean alone: an average hides exactly the tail an
/// incident is about. The count is what makes [`Latency::measure`] able
/// to say "no samples" instead of reporting a zero nobody should trust.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Latency {
    /// Samples.
    pub count: u64,
    /// Sum of the observed waits.
    pub total: Duration,
    /// Longest observed wait.
    pub max: Duration,
}

impl Latency {
    /// Record one observation.
    pub fn record(&mut self, wait: Duration) {
        self.count = self.count.saturating_add(1);
        self.total = self.total.saturating_add(wait);
        if wait > self.max {
            self.max = wait;
        }
    }

    /// The mean, or why there is none.
    ///
    /// A latency with no samples is unavailable, never zero: a stage
    /// nothing has passed through is not a fast stage.
    pub fn measure(&self) -> Measure<Duration> {
        if self.count == 0 {
            return Measure::Unavailable(Unavailable::NoSamples);
        }
        // Divide by the whole count. `Duration / u32` would cap the
        // denominator at `u32::MAX` while the total kept growing, and a
        // busy stage passes four billion samples in about half a day,
        // after which the reported mean would inflate without bound.
        let nanos = self.total.as_nanos() / u128::from(self.count);
        Measure::Observed(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ))
    }

    /// The longest observed wait, or why there is none.
    pub fn peak(&self) -> Measure<Duration> {
        if self.count == 0 {
            return Measure::Unavailable(Unavailable::NoSamples);
        }
        Measure::Observed(self.max)
    }
}

/// The three durability quantities, kept apart.
///
/// Blurring them is the classic way to misread a storage incident: a
/// fast `sync` with a slow `commit_return` means the queue is the
/// problem, and a slow `sync` with little `backpressure` means the
/// device is. One combined "write latency" answers neither question.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Durability {
    /// The synchronizing write itself.
    pub sync: Latency,
    /// The whole operation as its caller experiences it: time queued
    /// plus the commit, to the point the caller is answered.
    pub commit_return: Latency,
    /// Time spent refused or waiting because a bound was reached.
    /// Backpressure is not slowness; it is the system declining work.
    pub backpressure: Latency,
}

/// Remaining room against a configured bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Headroom {
    /// In use now.
    pub used: u64,
    /// The bound.
    pub bound: u64,
}

impl Headroom {
    /// What is left, or why there is no answer.
    ///
    /// An unconfigured bound has no headroom -- and reporting `0` for
    /// one would read as "full", which is the opposite of the truth.
    pub const fn remaining(&self) -> Measure<u64> {
        if self.bound == 0 {
            return Measure::Unavailable(Unavailable::NoBound);
        }
        Measure::Observed(self.bound.saturating_sub(self.used))
    }

    /// Used as a fraction of the bound in tenths of a percent, so the
    /// reading stays an integer and carries no floating point into a
    /// consensus-adjacent process.
    pub const fn pressure_permille(&self) -> Measure<u64> {
        if self.bound == 0 {
            return Measure::Unavailable(Unavailable::NoBound);
        }
        Measure::Observed(self.used.saturating_mul(1000) / self.bound)
    }
}

/// The three storage frontiers, reported separately (`C <= M <= J`).
///
/// Journal and materialization are different things and a single
/// "storage position" would hide the gap between them -- which is the
/// number that says whether a node is keeping up with its own durable
/// log or merely writing to it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontiers {
    /// `J`: the durable journal head.
    pub journal: u64,
    /// `M`: completed materialization.
    pub materialized: u64,
    /// `C`: the published local checkpoint boundary.
    pub checkpoint: u64,
}

impl Frontiers {
    /// Records durable but not yet materialized.
    pub const fn unmaterialized(&self) -> u64 {
        self.journal.saturating_sub(self.materialized)
    }

    /// Records materialized but not yet represented by a checkpoint:
    /// what a reclamation would still have to replay.
    pub const fn unreclaimed(&self) -> u64 {
        self.materialized.saturating_sub(self.checkpoint)
    }
}

/// One stage's accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageMetrics {
    /// Operations that entered the stage.
    pub entered: u64,
    /// Operations that completed it.
    pub completed: u64,
    /// Operations the stage refused.
    pub refused: u64,
    /// How long the stage took.
    pub latency: Latency,
}

impl StageMetrics {
    /// Operations in the stage right now.
    pub const fn in_flight(&self) -> u64 {
        self.entered
            .saturating_sub(self.completed)
            .saturating_sub(self.refused)
    }
}

/// One stage's reading, with the reason when there is none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageReading {
    /// The stage.
    pub stage: Stage,
    /// Its accounting, or why this node has none.
    pub metrics: Measure<StageMetrics>,
}

/// A complete, secret-free snapshot.
///
/// Every field is a number or a frozen enum. Nothing here is derived
/// from a key, a command, a principal, an address or a payload, so a
/// scan of a rendered snapshot for secret patterns finds nothing --
/// because nothing of that kind was ever recorded, not because it was
/// filtered afterwards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Per-stage readings, in stage order.
    pub stages: Vec<StageReading>,
    /// Per-lane transport accounting, in lane order.
    pub lanes: Vec<LaneReading>,
    /// Per-shard budgets, in shard order and bounded.
    pub shards: Vec<ShardReading>,
    /// Storage durability, or why this node has none.
    pub durability: Measure<Durability>,
    /// Storage frontiers, or why this node has none.
    pub frontiers: Measure<Frontiers>,
    /// Age of the newest pinned read view.
    pub view_age: Measure<Duration>,
    /// Engine pressure as the storage engine reports it.
    pub engine_pressure: Measure<Headroom>,
}

impl MetricsSnapshot {
    /// The reading for one stage.
    pub fn stage(&self, stage: Stage) -> Option<&StageReading> {
        self.stages.iter().find(|r| r.stage == stage)
    }
}

/// One lane's transport accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneReading {
    /// The lane.
    pub lane: Lane,
    /// Time from enqueue to being picked.
    pub queue_wait: Measure<Duration>,
    /// Time from being picked to holding budget and a stream.
    pub credit_wait: Measure<Duration>,
    /// Frames handed to the transport, or why they are not counted.
    /// A count, but still a [`Measure`]: a lane nothing counts must not
    /// report that it carried nothing.
    pub frames: Measure<u64>,
    /// Frames refused at the queue, or why they are not counted.
    pub refused: Measure<u64>,
    /// Room left in the lane's byte budget.
    pub headroom: Measure<u64>,
}

/// One shard's budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReading {
    /// The shard.
    pub shard: ShardIndex,
    /// Room left in its budget.
    pub headroom: Measure<u64>,
    /// How hard it is being pushed, in tenths of a percent.
    pub pressure_permille: Measure<u64>,
}

/// The live, lock-free recorder.
///
/// Every field is an atomic and [`Recorder::snapshot`] takes no lock, so
/// a diagnostics reader -- however slow, however stuck -- cannot stall a
/// voter. That is a property of the type rather than a rule callers
/// follow: there is no lock here to take.
///
/// A recorder also knows which stages its owner actually records. A
/// stage nothing in the process feeds is reported as
/// [`Unavailable::NotInstrumented`] rather than as observed zeroes:
/// "a voter that served writes did no journal work" is exactly the
/// false statement a zero would make.
#[derive(Debug)]
pub struct Recorder {
    stages: [StageCells; Stage::ALL.len()],
    /// Bit `i` is set when [`Stage::ALL`]`[i]` has an instrumentation
    /// point in the owning process.
    instrumented: u16,
}

impl Default for Recorder {
    fn default() -> Self {
        Recorder::new()
    }
}

#[derive(Debug, Default)]
struct StageCells {
    entered: AtomicU64,
    completed: AtomicU64,
    refused: AtomicU64,
    samples: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Recorder {
    /// A recorder with nothing observed, whose owner records every
    /// stage.
    pub fn new() -> Self {
        Recorder::instrumenting(&Stage::ALL)
    }

    /// A recorder with nothing observed, whose owner records only
    /// `stages`. Every other stage is reported as
    /// [`Unavailable::NotInstrumented`], so a process that wires some
    /// stages and not others says which is which.
    pub fn instrumenting(stages: &[Stage]) -> Self {
        let instrumented = stages
            .iter()
            .fold(0u16, |bits, stage| bits | (1 << Self::index(*stage)));
        Recorder {
            stages: Default::default(),
            instrumented,
        }
    }

    /// Whether the owner records `stage`.
    pub fn instruments(&self, stage: Stage) -> bool {
        self.instrumented & (1 << Self::index(stage)) != 0
    }

    fn index(stage: Stage) -> usize {
        Stage::ALL
            .iter()
            .position(|s| *s == stage)
            .expect("every stage is in ALL")
    }

    fn cells(&self, stage: Stage) -> &StageCells {
        &self.stages[Self::index(stage)]
    }

    /// One operation entered `stage`.
    pub fn entered(&self, stage: Stage) {
        self.cells(stage).entered.fetch_add(1, Ordering::Relaxed);
    }

    /// One operation completed `stage` after `took`.
    ///
    /// The order of the writes is part of the reading protocol in
    /// [`Recorder::stage`]: the sample is counted before its wait joins
    /// the total, and the completion is published last, each with
    /// release semantics, so a reader that acquires the later value is
    /// guaranteed to see the earlier ones.
    pub fn completed(&self, stage: Stage, took: Duration) {
        let cells = self.cells(stage);
        let nanos = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX);
        cells.samples.fetch_add(1, Ordering::Relaxed);
        cells.total_nanos.fetch_add(nanos, Ordering::Release);
        cells.max_nanos.fetch_max(nanos, Ordering::Relaxed);
        cells.completed.fetch_add(1, Ordering::Release);
    }

    /// One operation was refused by `stage`.
    pub fn refused(&self, stage: Stage) {
        self.cells(stage).refused.fetch_add(1, Ordering::Release);
    }

    /// The accounting of one stage as it stands.
    ///
    /// There is no lock, so the counters are read one at a time while
    /// writers keep going, and the reading order is what keeps the
    /// result coherent. A counter is loaded before the one it may not
    /// exceed: completions and refusals before entries, and the total
    /// before the sample count. Every entry precedes its completion or
    /// refusal, and every sample is counted before its wait joins the
    /// total, so acquiring the later counter shows at least the earlier
    /// ones. A snapshot therefore never reports more completions than
    /// entries, an in-flight count below zero, or a mean inflated by a
    /// wait whose sample it has not counted; the only skew left is the
    /// handful of recordings in progress at the instant of the read.
    pub fn stage(&self, stage: Stage) -> StageMetrics {
        let cells = self.cells(stage);
        let completed = cells.completed.load(Ordering::Acquire);
        let refused = cells.refused.load(Ordering::Acquire);
        let entered = cells.entered.load(Ordering::Relaxed);
        let total_nanos = cells.total_nanos.load(Ordering::Acquire);
        let samples = cells.samples.load(Ordering::Relaxed);
        StageMetrics {
            entered,
            completed,
            refused,
            latency: Latency {
                count: samples,
                total: Duration::from_nanos(total_nanos),
                max: Duration::from_nanos(cells.max_nanos.load(Ordering::Relaxed)),
            },
        }
    }

    /// Every stage's reading for a node in `roles`.
    ///
    /// A stage this role does not have reports
    /// [`Unavailable::NotThisRole`]; one it has but nothing in this
    /// process records reports [`Unavailable::NotInstrumented`]; and one
    /// that is recorded but has not been exercised reports its zero
    /// counts with an unavailable latency. Those are three different
    /// statements and are meant to look different.
    pub fn snapshot_stages(&self, roles: &crate::role::RoleSet) -> Vec<StageReading> {
        Stage::ALL
            .iter()
            .map(|stage| StageReading {
                stage: *stage,
                metrics: if !stage.applies(roles) {
                    Measure::Unavailable(Unavailable::NotThisRole)
                } else if !self.instruments(*stage) {
                    Measure::Unavailable(Unavailable::NotInstrumented)
                } else {
                    Measure::Observed(self.stage(*stage))
                },
            })
            .collect()
    }
}
