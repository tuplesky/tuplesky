//! The driver: scheduled arrivals, bounded callers, and a report of what
//! actually happened (design Sections 14.1, 21.5, 22.3).
//!
//! Arrivals are scheduled against absolute instants from one start, so a
//! caller that falls behind does not push the next arrival later --
//! which is the whole of coordinated omission. When the callers cannot
//! keep up, the wait shows up in the queue distribution and in the gap
//! between the offered and the achieved rate, rather than disappearing
//! into a shorter measured window.
//!
//! A closed loop is still available and is labelled as one: it is the
//! right experiment for "how fast can this go", and the wrong one for
//! "what does a client see at this rate".

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coord_harness::domain::Provisioned;
use coord_harness::issuer::Minter;
use coord_store_bench::measure::{ProcessCounters, Samples};
use coord_types::ids::NamespaceId;
use coord_types::logical_v1::LogicalRequest;
use rand_core::SeedableRng;

use crate::caller::{Answer, Caller, CallerError};
use crate::report::{
    Absent, Achieved, CAVEATS, Measured, PathReport, ScheduleReport, ServerSide, Topology,
    WAN_RUN_FORMAT_V1, WanRunV1,
};
use crate::workload::{Kind, Workload};

/// How a run is asked for.
#[derive(Clone, Debug)]
pub struct RunSpec {
    /// The operator's label.
    pub label: String,
    /// Seed of the offered work.
    pub seed: u64,
    /// The durability the domain is running under, named by the
    /// operator. A run without one is refused.
    pub durability: String,
    /// The topology, as declared.
    pub topology: Topology,
    /// Scheduled inter-arrival time; zero is a closed loop.
    pub arrival_ns: u64,
    /// Operations offered before measurement starts.
    pub warmup_ops: u64,
    /// Operations measured.
    pub measured_ops: u64,
    /// Callers, each with its own session and connection.
    pub callers: u32,
    /// Per-operation deadline.
    pub deadline: Duration,
    /// The offered work.
    pub workload: Workload,
    /// Which voter's frontend each caller reaches; they are distributed
    /// round-robin over this many.
    pub frontends: u32,
}

/// What stopped a run.
#[derive(Debug)]
pub enum RunError {
    /// The provisioned domain could not be read.
    Material(String),
    /// A caller could not connect or bind.
    Caller(CallerError),
    /// The run was asked for in a shape that cannot be measured.
    Refused(String),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Material(e) => write!(f, "{e}"),
            RunError::Caller(e) => write!(f, "{e}"),
            RunError::Refused(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RunError {}

/// One scheduled arrival.
struct Arrival {
    /// When it was supposed to start.
    scheduled: Instant,
    /// What it asks for.
    request: LogicalRequest,
    /// Which distribution it belongs to.
    kind: Kind,
    /// Whether it is inside the measured window.
    measured: bool,
}

/// One completed arrival.
struct Sample {
    kind: Kind,
    measured: bool,
    queue_ns: u64,
    service_ns: u64,
    whole_ns: u64,
    answer: Answer,
}

/// The namespace a run drives, taken from the provisioned grant so the
/// requests are authorized rather than refused.
fn namespace_of(provisioned: &Provisioned) -> Result<NamespaceId, RunError> {
    let hex = &provisioned.namespace;
    if hex.len() != 32 {
        return Err(RunError::Material("the namespace is not 16 bytes".into()));
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| RunError::Material("the namespace is not hexadecimal".into()))?;
    }
    Ok(NamespaceId(out))
}

/// Drive one run against the domain provisioned in `dir`.
pub async fn run(dir: &Path, mut spec: RunSpec) -> Result<WanRunV1, RunError> {
    if spec.durability.trim().is_empty() {
        return Err(RunError::Refused(
            "a run has no headline without a named durability; pass --durability".into(),
        ));
    }
    if spec.measured_ops == 0 {
        return Err(RunError::Refused(
            "a run measures at least one operation".into(),
        ));
    }
    if spec.callers == 0 {
        return Err(RunError::Refused("a run needs at least one caller".into()));
    }
    let provisioned =
        Provisioned::read(dir).map_err(|e| RunError::Material(format!("harness.json: {e}")))?;
    spec.workload.namespace = namespace_of(&provisioned)?;
    let minter = Minter::of(&provisioned).map_err(|e| RunError::Material(format!("{e}")))?;

    let frontends = spec.frontends.clamp(1, provisioned.voters.len() as u32);
    let mut callers = Vec::new();
    for index in 0..spec.callers {
        let voter = (index % frontends) as usize;
        callers.push(
            Caller::connect(dir, &provisioned, &minter, voter, index as u16)
                .await
                .map_err(RunError::Caller)?,
        );
    }

    // A closed loop gets a channel exactly as deep as the callers, so
    // the scheduler blocks and the offered rate is the achieved one by
    // construction. An open loop's channel is unbounded on purpose: the
    // schedule is the experiment, and a bound would quietly turn it back
    // into a closed loop.
    let open_loop = spec.arrival_ns > 0;
    let (arrivals_tx, arrivals_rx) = if open_loop {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arrival>();
        (Sender::Unbounded(tx), Receiver::Unbounded(rx))
    } else {
        let (tx, rx) = tokio::sync::mpsc::channel::<Arrival>(spec.callers as usize);
        (Sender::Bounded(tx), Receiver::Bounded(rx))
    };
    let arrivals_rx = Arc::new(tokio::sync::Mutex::new(arrivals_rx));
    let (samples_tx, mut samples_rx) = tokio::sync::mpsc::unbounded_channel::<Sample>();

    let deadline = spec.deadline;
    let mut workers = Vec::new();
    for mut caller in callers {
        let queue = Arc::clone(&arrivals_rx);
        let out = samples_tx.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let arrival = {
                    let mut queue = queue.lock().await;
                    match queue.recv().await {
                        Some(arrival) => arrival,
                        None => break,
                    }
                };
                let started = Instant::now();
                let answer = caller.ask(&arrival.request, deadline).await;
                let completed = Instant::now();
                let _ = out.send(Sample {
                    kind: arrival.kind,
                    measured: arrival.measured,
                    queue_ns: started
                        .saturating_duration_since(arrival.scheduled)
                        .as_nanos() as u64,
                    service_ns: completed.saturating_duration_since(started).as_nanos() as u64,
                    whole_ns: completed
                        .saturating_duration_since(arrival.scheduled)
                        .as_nanos() as u64,
                    answer,
                });
            }
        }));
    }
    drop(samples_tx);

    let started_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(spec.seed);
    let workload = spec.workload;
    let total = spec.warmup_ops + spec.measured_ops;
    let origin = Instant::now();
    let mut measured_started = origin;
    let mut offered = 0u64;
    // The resource reading is taken again when the measured window
    // opens, so the delta is over the same window as the latencies. In
    // an open loop the scheduling loop is the run: reading the counters
    // only after it would cover the final drain and nothing else.
    let mut before = ProcessCounters::read();

    for index in 0..total {
        let measured = index >= spec.warmup_ops;
        let scheduled = if open_loop {
            origin + Duration::from_nanos(spec.arrival_ns.saturating_mul(index))
        } else {
            Instant::now()
        };
        if open_loop {
            let now = Instant::now();
            if scheduled > now {
                tokio::time::sleep(scheduled - now).await;
            }
        }
        if measured && offered == 0 {
            measured_started = scheduled;
            before = ProcessCounters::read();
        }
        if measured {
            offered += 1;
        }
        let (kind, request) = workload.next(&mut rng);
        let arrival = Arrival {
            scheduled,
            request,
            kind,
            measured,
        };
        if arrivals_tx.send(arrival).await.is_err() {
            break;
        }
    }
    drop(arrivals_tx);

    for worker in workers {
        let _ = worker.await;
    }
    let after = ProcessCounters::read();
    let wall_ns = Instant::now()
        .saturating_duration_since(measured_started)
        .as_nanos() as u64;

    let mut paths: BTreeMap<Kind, (Samples, Samples, Samples, BTreeMap<String, u64>)> =
        BTreeMap::new();
    let mut completed = 0u64;
    let mut refused = 0u64;
    let mut unknown = 0u64;
    let mut seen = 0u64;
    while let Some(sample) = samples_rx.recv().await {
        if !sample.measured {
            continue;
        }
        seen += 1;
        let entry = paths.entry(sample.kind).or_insert_with(|| {
            (
                Samples::new(),
                Samples::new(),
                Samples::new(),
                BTreeMap::new(),
            )
        });
        entry.0.push(sample.queue_ns);
        entry.1.push(sample.service_ns);
        entry.2.push(sample.whole_ns);
        *entry.3.entry(sample.answer.reason()).or_insert(0) += 1;
        match sample.answer {
            Answer::Established => completed += 1,
            Answer::Refused(_) => refused += 1,
            Answer::Unknown => unknown += 1,
        }
    }

    let per_second = |count: u64| -> Measured<u64> {
        if wall_ns == 0 {
            Measured::Absent(Absent::NoSamples)
        } else {
            Measured::Observed(count.saturating_mul(1_000_000_000) / wall_ns)
        }
    };

    let mut caveats: Vec<String> = CAVEATS.iter().map(|c| (*c).to_string()).collect();
    if !open_loop {
        caveats.push(
            "Closed loop: arrivals waited for a caller, so this is a saturation measurement \
             and its latencies are not what a client sees at a given rate."
                .into(),
        );
    }
    if offered > seen {
        caveats.push(format!(
            "{} scheduled operations never reached a caller before the run ended.",
            offered - seen
        ));
    }

    Ok(WanRunV1 {
        format: WAN_RUN_FORMAT_V1,
        label: spec.label,
        started_at,
        seed: spec.seed,
        durability: spec.durability,
        topology: spec.topology,
        schedule: ScheduleReport {
            arrival_ns: spec.arrival_ns,
            warmup_ops: spec.warmup_ops,
            measured_ops: spec.measured_ops,
            callers: spec.callers,
            deadline_ms: spec.deadline.as_millis().min(u128::from(u32::MAX)) as u32,
            open_loop,
        },
        achieved: Achieved {
            offered,
            completed,
            refused,
            unknown,
            abandoned: offered.saturating_sub(seen),
            wall_ns,
            offered_per_second: per_second(offered),
            achieved_per_second: per_second(completed),
        },
        paths: paths
            .into_iter()
            .map(|(kind, (queue, service, whole, refusals))| {
                (
                    kind.name().to_owned(),
                    PathReport {
                        samples: whole.len() as u64,
                        queue: queue.summary(),
                        service: service.summary(),
                        whole: whole.summary(),
                        refusals,
                    },
                )
            })
            .collect(),
        resources: before.since(&after, 0),
        server: ServerSide::unreadable(),
        caveats,
    })
}

/// The arrival channel, in whichever shape the loop calls for.
enum Sender {
    Bounded(tokio::sync::mpsc::Sender<Arrival>),
    Unbounded(tokio::sync::mpsc::UnboundedSender<Arrival>),
}

impl Sender {
    async fn send(&self, arrival: Arrival) -> Result<(), ()> {
        match self {
            Sender::Bounded(tx) => tx.send(arrival).await.map_err(|_| ()),
            Sender::Unbounded(tx) => tx.send(arrival).map_err(|_| ()),
        }
    }
}

enum Receiver {
    Bounded(tokio::sync::mpsc::Receiver<Arrival>),
    Unbounded(tokio::sync::mpsc::UnboundedReceiver<Arrival>),
}

impl Receiver {
    async fn recv(&mut self) -> Option<Arrival> {
        match self {
            Receiver::Bounded(rx) => rx.recv().await,
            Receiver::Unbounded(rx) => rx.recv().await,
        }
    }
}
