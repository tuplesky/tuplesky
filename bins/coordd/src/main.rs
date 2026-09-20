//! `coordd`: parse strict configuration, report the composed roles and
//! the readiness requirements, and run. With `--check` it validates and
//! exits; otherwise it opens the store it already has, binds the
//! configured listeners and stays up. Consensus and serving arrive with
//! the later integration tasks, so a started process reports `Live` and
//! never `Ready`. Everything it prints is secret-safe.
//!
//! `coordd init` is separate on purpose: it creates a node's first
//! generation, and nothing else ever does. A daemon that created a store
//! when it failed to find one would turn a lost disk, an unmounted
//! volume or a mistyped path into a fresh, empty, valid node -- which
//! would then vote, having forgotten everything it had promised.
//!
//! Initialization also pins the genesis manifest it was run under, and
//! every later start requires the same one (see `genesis`): the manifest
//! is a file, and a node that re-adopted whatever the file said on each
//! start would vote by the operator's latest edit rather than by the
//! configuration every replica agreed on.

mod genesis;
mod membership;
mod serve;
mod store;

use std::process::ExitCode;

use clap::Parser;
use coord_daemon::{Config, Diagnostics, Lifecycle, QuarantineReason, Readiness, bind_listeners};

#[derive(Parser)]
#[command(name = "coordd", about = "TupleSky node daemon (reference preview)")]
struct Cli {
    /// Path to the strict TOML configuration.
    #[arg(long)]
    config: std::path::PathBuf,
    /// Validate the configuration and exit without starting.
    #[arg(long)]
    check: bool,
    /// What to do. Omitted, the daemon serves.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Create this node's first store generation, once.
    ///
    /// Deliberate and separate: nothing else creates a store, so a
    /// missing one is always reported rather than repaired.
    Init,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let text = match std::fs::read_to_string(&cli.config) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read config: {e}");
            return ExitCode::from(2);
        }
    };
    let config = match Config::parse(&text) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid configuration: {e:?}");
            return ExitCode::from(2);
        }
    };
    let roles = config.role_set().expect("validated");
    let lifecycle = Lifecycle::new(roles.clone());
    let diagnostics = Diagnostics::snapshot(&roles, &lifecycle, 0);
    println!(
        "coordd domain={} roles={:?} phase={} votes={}",
        config.domain,
        diagnostics.roles,
        diagnostics.phase,
        roles.votes()
    );
    // Who this node is, from the genesis manifest and from its own
    // certificate. Nothing here invents an identity: a node that could
    // be told who it was could be told it was somebody else.
    let placed = match membership::place(
        &config.cluster_manifest,
        &config.identity.node_certificate,
        roles.votes(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    // The identity above was read out of the certificate, so the
    // certificate has to be one this domain issued, held by this
    // process, before any store is opened under it: a self-signed leaf
    // claiming a voter's node URI would otherwise open that voter's store.
    if let Err(e) = coord_daemon::load_identity(
        &config.identity,
        placed.membership.cluster(),
        placed.membership.domain(),
        Vec::new(),
    )
    .and_then(|identity| coord_daemon::verify_identity(&identity, &config.identity))
    {
        eprintln!("{e}");
        return ExitCode::from(2);
    }
    println!(
        "node replica={} incarnation={} role={:?} voters={}",
        short(&placed.replica.0),
        placed.incarnation.get(),
        placed.role,
        placed.membership.voters().count()
    );

    if let Some(Command::Init) = cli.command {
        return match store::open_storage(
            &config,
            store::Intent::Initialize,
            placed.membership.cluster(),
            placed.membership.domain(),
            placed.replica,
            placed.incarnation,
        );
        let mut generation = match opened {
            Ok(generation) => generation,
            // An initialization that created the generation and stopped
            // before it pinned the manifest is finished here rather than
            // refused: the generation has never been served, since a start
            // refuses an unpinned store, so there is no history to lose,
            // and refusing both commands would leave the node with no way
            // forward but deleting its store by hand.
            Err(already @ store::StoreError::AlreadyInitialized { .. }) => {
                match store::open(
                    &config,
                    store::Intent::Serve,
                    placed.membership.cluster(),
                    placed.membership.domain(),
                    placed.replica,
                    placed.incarnation,
                )
                .ok()
                .and_then(|mut generation| {
                    (genesis::unfinished(&mut generation) == Some(true)).then_some(generation)
                }) {
                    Some(generation) => generation,
                    None => {
                        eprintln!("{already}");
                        return ExitCode::from(2);
                    }
                }
            }
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        };
        return match genesis::check(
            &mut generation,
            &placed.manifest,
            genesis::Intent::Initialize,
        ) {
            Ok(storage) => {
                println!("initialized {}", storage.generation.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::from(2)
            }
        };
    }

    if cli.check {
        return ExitCode::SUCCESS;
    }
    // Without --check the daemon starts: it binds what the configuration
    // names and stays up. Validating and exiting successfully either way
    // meant `coordd` never ran at all, and said nothing about it.
    let mut lifecycle = lifecycle;
    // The store is opened before a listener exists. A process that bound
    // first would accept connections it could not serve, and a caller
    // cannot tell that from one it is merely slow to answer.
    let storage = match store::open_storage(
        &config,
        store::Intent::Serve,
        placed.membership.cluster(),
        placed.membership.domain(),
        placed.replica,
        placed.incarnation,
    ) {
        Ok(s) => s,
        Err(e) => {
            lifecycle.quarantine(QuarantineReason::Disk);
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    // Attaching is where the journal's frontier and the projection's are
    // checked against each other and the journal's suffix is replayed
    // into the projection. It has happened by now, which is what makes
    // the next line true rather than hopeful.
    println!(
        "storage projection={} journaled_through={:?} owed={}",
        storage.generation.display(),
        storage
            .domain
            .store()
            .frontiers(placed.membership.domain())
            .map(|f| f.durable().get()),
        storage
            .domain
            .store()
            .unmaterialized(placed.membership.domain()),
    );
    lifecycle.observe(Readiness {
        storage_ready: true,
        ..Readiness::default()
    });
    // The frontend is built before a listener exists. A process that
    // bound first and then found it had no keys to verify callers with
    // would accept connections and refuse every one of them, while
    // reporting itself live throughout -- which reads as a client
    // problem and is the most expensive kind of misconfiguration to
    // find. This is the order that makes `live` mean it.
    let boot = storage.boot;
    let applier = match coord_storage::Applier::new(
        storage.domain,
        coord_core::outbox::BarrierAllocator::new(placed.incarnation, boot),
    ) {
        Ok(a) => a,
        Err(e) => {
            lifecycle.quarantine(QuarantineReason::Disk);
            eprintln!("cannot serve from this store: {e:?}");
            return ExitCode::from(2);
        }
    };
    // A process that votes runs its voter here, over the same store the
    // frontend reads. One writer, one domain: a second handle on this
    // store would be a second writer's worth of opportunity, and the
    // profile has exactly one.
    let backing = if roles.votes() {
        match voter(&placed, applier, boot) {
            Ok(v) => serve::Backing::Voting(Box::new(v)),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        }
    } else {
        serve::Backing::Serving(Box::new(applier))
    };
    // The local route is the voter's to hand over, and it exists only
    // because a voter is running here. A frontend-only process gets
    // `None` and reaches every voter over the wire.
    let local = match &backing {
        serve::Backing::Voting(v) => Some(v.route()),
        serve::Backing::Serving(_) => None,
    };
    let frontend = match serve::Frontend::new(&config, placed.membership.clone(), local) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let mut domain = serve::Domain::new(frontend, backing, serve::Budgets::default());
    println!(
        "frontend ready waiting={} voting={}",
        domain.waiting(),
        roles.votes()
    );

    let mut listeners = match bind_listeners(&config.listen) {
        Ok(l) => l,
        Err(e) => {
            lifecycle.quarantine(QuarantineReason::Listeners);
            eprintln!("cannot bind {}: {}", e.listener, e.reason);
            return ExitCode::from(2);
        }
    };
    lifecycle.observe(Readiness {
        storage_ready: true,
        listeners_up: true,
        ..Readiness::default()
    });
    for (name, address) in listeners.addresses() {
        println!("listening {name}={address}");
    }

    // Readiness is not liveness. The listeners are up and the frontend
    // can decide, but a voter is ready only on fresh quorum evidence,
    // which arrives with the peer loop; the process says `live` and not
    // `ready`, which is the truth about it.
    let diagnostics = Diagnostics::snapshot(&roles, &lifecycle, 0);
    println!("coordd phase={}", diagnostics.phase);

    // The socket is taken here, while the listeners are still this
    // function's: the endpoint serves on the socket that was bound, not
    // on the address it reported.
    let api_socket = listeners.take_api();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(async move {
        let Some(socket) = api_socket else {
            eprintln!("this process serves clients but bound no api listener");
            return ExitCode::from(2);
        };
        let mut transport = match serve::api_endpoint(&config, &placed.membership, socket) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        };
        domain.run(&mut transport, now_seconds).await;
        let counts = domain.counts();
        eprintln!(
            "the api plane ended: queued_local={} queued_remote={} not_a_voter={} \
saturated={} unavailable={} refused={} released={} watches={} unserved={}",
            counts.queued_local,
            counts.queued_remote,
            counts.not_a_voter,
            counts.saturated,
            counts.unavailable,
            counts.refused,
            counts.released,
            counts.watches,
            counts.unserved
        );
        ExitCode::from(1)
    })
}

/// This node's voter, booted, over the store it will write.
///
/// The role it takes is not a setting. The committed configuration says
/// which ballot the epoch starts at and who leads it; this replica is
/// that leader or it is a follower of it, and either way it recovers to
/// the execution position its own store is already at rather than to
/// zero -- a machine that started from zero would plan a command the
/// storage frontier had already passed.
fn voter(
    placed: &membership::Placed,
    applier: coord_storage::Applier<store::Persistence>,
    boot: coord_core::effect::BootId,
) -> Result<coord_daemon::Voter<store::Persistence>, String> {
    use coord_consensus::{
        ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig, LearningMode,
        ReplicaRole,
    };

    let m = &placed.membership;
    let collector = coord_daemon::voter::collector_peer(m)
        .ok_or("a committed voter holds the identity reserved for this domain's collector")?;
    let ballot = serve::genesis_ballot(m).map_err(|e| e.to_string())?;
    let quorum = serve::quorum_of(m).map_err(|e| e.to_string())?;
    let identity = ConfigurationIdentity {
        cluster: m.cluster(),
        domain: m.domain(),
        epoch: m.epoch(),
        voters: m.voters().map(|v| v.node).collect(),
        replica: placed.replica,
        incarnation: placed.incarnation,
        role: ReplicaRole::Voter,
    };
    // What this replica already owes, from the authoritative record.
    //
    // Not from the projection: on the journal-first profile a promise or
    // a vote can be durable in the journal and not materialized yet, and
    // a replica that recovered from the projection alone would come back
    // contradicting a vote it had already sent. The seam only offers the
    // authoritative answer, so there is no second source to pick by
    // mistake.
    let recovered = {
        use coord_storage::Persistence;
        applier
            .store()
            .recovered(m.epoch(), coord_storage::views::ViewBudget::default())
            .map_err(|e| format!("this replica cannot read what it owes: {e:?}"))?
    };
    // The position the next command takes is the store's, not the
    // summary's: it is what the applier validates a batch's base
    // against, and a machine that planned against anything else would
    // have its first command refused for a stale base.
    let executed_through = {
        use coord_storage::Persistence;
        applier.store().application_base().execution_position
    };
    println!(
        "recovered promise={:?} records={} payloads={} executed={} frontier={} position={}",
        recovered.promise.as_ref().map(|p| p.promised.number),
        recovered.records.len(),
        recovered.payloads.len(),
        recovered.executed.len(),
        recovered.frontier.get(),
        executed_through.get(),
    );
    let machine = if placed.replica == ballot.leader {
        let mut leader = Leader::new(
            LeaderConfig {
                identity,
                quorum,
                genesis: ballot,
                frontend: collector,
                capacity: 64,
            },
            recovered.promise.clone(),
            executed_through,
        );
        leader.set_learning(LearningMode::Full);
        coord_daemon::Machine::Leader(Box::new(leader))
    } else {
        let mut follower = Follower::recover(
            FollowerConfig {
                identity,
                quorum,
                genesis: ballot,
                frontend: collector,
                capacity: 64,
            },
            recovered.promise.clone(),
            recovered.records.clone(),
            recovered.payloads.clone(),
            executed_through,
        )
        .restore_execution(executed_through, recovered.executed.iter().map(|(c, _)| *c))
        .restore_payloads(recovered.payloads.clone());
        follower.set_learning(LearningMode::Full);
        coord_daemon::Machine::Follower(Box::new(follower))
    };
    let ingress = coord_daemon::Ingress::new(
        m,
        placed.replica,
        coord_types::wire_v1::PeerRole::Frontend,
        coord_daemon::IngressBudget::default(),
    )
    .ok_or("this process votes, but the committed configuration does not name it a voter")?;
    let mut voter = coord_daemon::Voter::new(
        coord_daemon::Node::new(machine, applier, collector),
        ingress,
        ballot,
    );
    voter
        .boot(boot, placed.incarnation)
        .map_err(|e| format!("this voter cannot record its own boot: {e}"))?;
    Ok(voter)
}

/// Wall-clock seconds. A binding's validity is stated in them, so this
/// is the one place the process reads the clock for that purpose.
fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A short, stable rendering of an identity for an operator's eye. It is
/// an identity, not a secret, and the full value is in the certificate.
fn short(bytes: &[u8; 16]) -> String {
    bytes[..4].iter().map(|b| format!("{b:02x}")).collect()
}
