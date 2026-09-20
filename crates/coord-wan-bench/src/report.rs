//! What a run reports (design Section 22.3).
//!
//! Three rules shape this, and they are the same three the daemon's own
//! metrics follow. A metric this harness did not obtain is
//! [`Absent`]-with-a-reason and never a zero. Every latency is reported
//! as three separate distributions -- the wait before the operation
//! started, the operation itself, and the whole thing from its
//! *scheduled* arrival -- because a closed-loop harness that reported
//! only the middle one would hide exactly the queueing a WAN run exists
//! to expose. And the durability the domain was running under is a name
//! the operator supplies and this file records verbatim: there is no
//! headline number without one.

use std::collections::BTreeMap;

use coord_store_bench::measure::{Percentiles, Resources};
use serde::{Deserialize, Serialize};

/// The report format. Frozen with the other formats of the registry.
pub const WAN_RUN_FORMAT_V1: u16 = 1;

/// Why a metric is not here.
///
/// It is never "zero". A reader has to be able to tell "nothing
/// happened" from "nobody measured", and the difference decides whether
/// a comparison is allowed at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Absent {
    /// Nothing was sampled.
    NoSamples,
    /// The domain publishes it, but this run had no way to read it: the
    /// daemon renders its snapshot on its own startup and shutdown
    /// report, not on a socket a benchmark can poll.
    NoEndpoint,
    /// It belongs to a layer this harness does not sit on. Disk and WAN
    /// counters of a *remote* node are the clear case: this process can
    /// account for its own host and must not invent another's.
    NotOnThisHost,
    /// The operator did not state it, and it is not guessable.
    NotStated,
}

/// A measurement or a stated absence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Measured<T> {
    /// What was measured.
    Observed(T),
    /// Why there is nothing.
    Absent(Absent),
}

impl<T> Measured<T> {
    /// The value, if any.
    pub const fn observed(&self) -> Option<&T> {
        match self {
            Measured::Observed(value) => Some(value),
            Measured::Absent(_) => None,
        }
    }

    /// Why it is missing, if it is.
    pub const fn why(&self) -> Option<Absent> {
        match self {
            Measured::Observed(_) => None,
            Measured::Absent(reason) => Some(*reason),
        }
    }
}

/// How the load was scheduled. This is the offered load, not the load
/// that happened.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleReport {
    /// Scheduled inter-arrival time. Zero is a closed loop, which is a
    /// different experiment and is labelled as one.
    pub arrival_ns: u64,
    /// Operations scheduled before measurement starts.
    pub warmup_ops: u64,
    /// Operations measured.
    pub measured_ops: u64,
    /// Callers driving them, each with its own session and connection.
    pub callers: u32,
    /// Per-operation deadline.
    pub deadline_ms: u32,
    /// Whether arrivals were generated on a schedule or as fast as the
    /// callers could take them.
    pub open_loop: bool,
}

/// What actually happened, beside what was asked for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Achieved {
    /// Operations scheduled during the measured window.
    pub offered: u64,
    /// Operations that returned an established outcome.
    pub completed: u64,
    /// Operations the domain refused with a reason.
    pub refused: u64,
    /// Operations whose outcome this caller never learned.
    pub unknown: u64,
    /// Operations that never left the queue before the run ended.
    pub abandoned: u64,
    /// Wall time of the measured window.
    pub wall_ns: u64,
    /// Scheduled operations per second.
    pub offered_per_second: Measured<u64>,
    /// Completed operations per second over the same window.
    pub achieved_per_second: Measured<u64>,
}

/// One operation kind's three distributions and its refusals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathReport {
    /// Samples behind these percentiles.
    pub samples: u64,
    /// Scheduled arrival to the moment a caller picked it up. This is
    /// the coordinated-omission term: under a schedule the harness
    /// cannot keep up with, it grows and says so.
    pub queue: Percentiles,
    /// Sent to answer: the operation itself.
    pub service: Percentiles,
    /// Scheduled arrival to answer. The number a client experiences, and
    /// the only one a headline may quote.
    pub whole: Percentiles,
    /// Refusals by bounded reason.
    pub refusals: BTreeMap<String, u64>,
}

/// What the domain was, as configured. Declared, not measured: this
/// harness drives a domain, it does not discover its topology, and a
/// figure it inferred would be a guess in a report that must not carry
/// guesses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// A name for the shape, for example `single-host-3` or
    /// `two-region-2-2-1`.
    pub label: String,
    /// Committed voters.
    pub voters: u32,
    /// The frontend the callers reached.
    pub frontend: String,
    /// Per-region declarations, as the operator set them up.
    pub regions: Vec<Region>,
    /// The one-way delay and loss the operator applied, if any, exactly
    /// as stated. `NotStated` on a single host, which is the honest
    /// answer and not "zero milliseconds".
    pub impairment: Measured<String>,
}

/// One declared region.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Region {
    /// Its name.
    pub name: String,
    /// Voters placed in it.
    pub voters: u32,
}

/// Metrics the domain keeps and this run could not read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerSide {
    /// Journal synchronization.
    pub sync: Measured<Percentiles>,
    /// Commit-return, which is a different thing from synchronization
    /// and is reported separately or not at all.
    pub commit_return: Measured<Percentiles>,
    /// Queue depth at the stages the daemon names.
    pub queues: Measured<BTreeMap<String, u64>>,
    /// Per-node disk bytes.
    pub disk: Measured<u64>,
    /// Bytes across the impaired links.
    pub wan: Measured<u64>,
}

impl ServerSide {
    /// Everything the daemon renders only on its own startup and
    /// shutdown report, stated as absent with that reason.
    pub const fn unreadable() -> Self {
        ServerSide {
            sync: Measured::Absent(Absent::NoEndpoint),
            commit_return: Measured::Absent(Absent::NoEndpoint),
            queues: Measured::Absent(Absent::NoEndpoint),
            disk: Measured::Absent(Absent::NotOnThisHost),
            wan: Measured::Absent(Absent::NotOnThisHost),
        }
    }
}

/// One benchmark run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WanRunV1 {
    /// Format identifier.
    pub format: u16,
    /// The operator's label for this run.
    pub label: String,
    /// When it started, seconds since the epoch.
    pub started_at: u64,
    /// The workload seed, so the same offered work can be replayed.
    pub seed: u64,
    /// The durability the domain was running under, named by the
    /// operator and recorded verbatim.
    pub durability: String,
    /// The topology, as declared.
    pub topology: Topology,
    /// What was asked for.
    pub schedule: ScheduleReport,
    /// What happened.
    pub achieved: Achieved,
    /// Per operation kind.
    pub paths: BTreeMap<String, PathReport>,
    /// This process's own resources over the measured window.
    pub resources: Resources,
    /// What the domain knows and this run did not read.
    pub server: ServerSide,
    /// Everything a reader has to carry with these numbers.
    pub caveats: Vec<String>,
}

/// Caveats every run carries, whatever it measured.
pub const CAVEATS: &[&str] = &[
    "Client-observed latency only. The daemon's own stage, synchronization and \
     commit-return metrics are rendered on its startup and shutdown report and are \
     stated as absent here rather than estimated.",
    "The offered load is what the schedule asked for. Where the queue distribution \
     grows, the callers did not keep up and the achieved rate, not the scheduled \
     one, is what was measured.",
    "A single-host run measures codec, transport and consensus over loopback. It is \
     not a WAN result and must not be labelled as one; the impairment field says \
     what was applied.",
    "The credential this run presents is minted by the qualification harness rather \
     than federated from an identity provider, so the token exchange is outside \
     what is measured here.",
];
