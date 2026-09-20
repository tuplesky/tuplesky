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

mod backup;
mod membership;
mod peers;
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
    /// Report what this node's credential is against committed
    /// membership, and when it has to be renewed. Starts nothing.
    ///
    /// The operator-facing answer to "why is this node not voting". A
    /// peer that refuses a binding tells the node only that it was
    /// refused, which is the right amount to tell it; replacing a node
    /// is done here, so the distinctions have to be available here.
    Inspect,
    /// Take a backup of this node's selected generation (task-59).
    ///
    /// The common state at the boundary this node has materialized, and
    /// nothing node-private: a backup restores a cluster, never a
    /// voter. The procedure is `docs/operations/disaster-recovery.md`.
    Backup {
        /// Directory to write the backup into. It must not already hold
        /// one.
        #[arg(long)]
        out: std::path::PathBuf,
    },
    /// Verify a backup without restoring it, and print its recovery
    /// point.
    Verify {
        /// The backup directory.
        #[arg(long)]
        dir: std::path::PathBuf,
    },
    /// Establish this node's first generation from a backup of another
    /// cluster.
    ///
    /// Not recovery: it creates a *new* cluster whose membership comes
    /// from this node's own genesis, and it requires a fencing
    /// attestation that the cluster the backup came from has been
    /// isolated. Read the runbook first.
    Restore {
        /// The backup directory.
        #[arg(long)]
        dir: std::path::PathBuf,
        /// The operator's fencing attestation.
        #[arg(long)]
        fencing: std::path::PathBuf,
        /// Print what the restore would do and write nothing.
        #[arg(long)]
        plan: bool,
    },
}

/// Write the domain's genesis policy: the trust rule its configured
/// issuer signs under, and the permissions its genesis grants.
///
/// Part of initializing the domain, not of serving it. Permission is
/// allow-only and a session exists only under a trust rule replicated
/// policy holds enabled, so a domain initialized without these could
/// establish no session and authorize no command -- and the command
/// that would write them would itself need a session to be authorized.
/// Every replica writes the same rows from the same configuration, as
/// it does the genesis membership.
///
/// It is not an execution: the batch carries no application base and
/// takes no execution position. Everything after genesis goes through
/// ordered replicated commands.
fn write_genesis_policy(
    config: &Config,
    storage: &mut store::Storage,
    incarnation: coord_types::ids::ReplicaIncarnation,
) -> Result<(), String> {
    use coord_storage::Persistence;

    let mut updates = Vec::new();
    if let Some(sts) = &config.sts {
        let rule = coord_daemon::config::identity_bytes(&sts.trust_rule)
            .ok_or("sts.trust_rule is not an identity")?;
        updates.push(
            coord_storage::policy::bootstrap_trust_rule(&coord_types::ids::TrustRuleId(rule))
                .map_err(|e| format!("the trust rule could not be encoded: {e:?}"))?,
        );
    }
    for grant in &config.grant {
        let principal = coord_daemon::config::identity_bytes(&grant.principal)
            .ok_or("grant.principal is not an identity")?;
        let namespace = coord_daemon::config::identity_bytes(&grant.namespace)
            .ok_or("grant.namespace is not an identity")?;
        updates.extend(
            coord_storage::policy::bootstrap_grant(
                coord_types::ids::PrincipalId(principal),
                coord_types::ids::NamespaceId(namespace),
            )
            .map_err(|e| format!("a genesis grant could not be encoded: {e:?}"))?,
        );
    }
    if updates.is_empty() {
        return Ok(());
    }
    let rows = updates.len();
    let mut alloc = coord_core::outbox::BarrierAllocator::new(incarnation, storage.boot);
    storage
        .domain
        .submit(
            coord_core::effect::PersistBatch {
                barrier: alloc.allocate(),
                base: None,
                updates,
            },
            coord_storage::journaled::TransitionKind::Protocol,
        )
        .map_err(|e| format!("the genesis policy was refused: {e:?}"))?;
    // Durable before `init` reports success: a store that said it was
    // initialized and was not would serve a domain that trusts nothing,
    // and nothing later writes these rows.
    for _ in 0..8 {
        if storage.domain.queued() == 0 && storage.domain.unmaterialized() == 0 {
            break;
        }
        let lowered = storage
            .domain
            .lower()
            .map_err(|e| format!("the genesis policy could not be made durable: {e:?}"))?;
        if lowered.indeterminate {
            storage
                .domain
                .reconcile()
                .map_err(|e| format!("the genesis policy could not be resolved: {e:?}"))?;
        }
    }
    if storage.domain.queued() > 0 || storage.domain.unmaterialized() > 0 {
        return Err("the genesis policy did not become durable".into());
    }
    println!("genesis policy rows={rows}");
    Ok(())
}

/// How an inspection reports a credential. Bounded words, never keys.
fn credential_state(change: &coord_membership::CredentialChange) -> String {
    use coord_membership::CredentialChange as C;
    match change {
        C::Renewal => "renewal".into(),
        C::UncommittedKey => "uncommitted-key".into(),
        C::RequiresCommit {
            committed,
            presented,
        } => format!(
            "requires-commit committed={} presented={}",
            committed.get(),
            presented.get()
        ),
        C::Stale {
            committed,
            presented,
        } => format!(
            "stale committed={} presented={}",
            committed.get(),
            presented.get()
        ),
        C::NotAVoter => "not-a-voter".into(),
    }
}

/// How an inspection reports a renewal decision.
fn renewal_state(renewal: &coord_node_issuer::Renewal) -> String {
    match renewal {
        coord_node_issuer::Renewal::Wait { until } => format!("wait until={until}"),
        coord_node_issuer::Renewal::Due => "due".into(),
        coord_node_issuer::Renewal::Expired => "expired".into(),
    }
}

/// What a backup says about itself: where it came from and what its
/// recovery point is. Printed before anything is decided, because the
/// recovery point is the number an operator reconciles against callers
/// and not an implementation detail.
fn report_backup(manifest: &coord_checkpoint::BackupManifestV1) {
    println!(
        "backup source={} domain={} configuration={} taken_at={}",
        short(&manifest.source.0),
        short(&manifest.domain.0),
        manifest.configuration.get(),
        manifest.taken_at,
    );
    println!(
        "recovery_point position={} revision={} retention_floor={} root={}",
        manifest.boundary.execution_position.get(),
        manifest.boundary.kv_revision.get(),
        manifest.boundary.retention_floor.get(),
        hex16(&manifest.root.0),
    );
}

/// The first eight bytes of a digest, for a line an operator reads.
fn hex16(bytes: &[u8; 32]) -> String {
    bytes[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// How the restore disposes of one class of state.
fn disposition(d: coord_checkpoint::Disposition) -> &'static str {
    use coord_checkpoint::Disposition as D;
    match d {
        D::RestoredAtBoundary => "restored-at-boundary",
        D::NotCarried => "not-carried",
        D::Invalidated => "invalidated",
        D::Revoked => "revoked",
        D::Resynchronized => "resynchronized",
    }
}

/// Plan, and where asked, carry out a restore into this node's first
/// generation (task-59).
///
/// The successor cluster is this node's own committed one: the operator
/// has already established the new cluster's genesis and credentials,
/// and the restore fills its first generation instead of `init`
/// creating an empty one. Nothing here can establish that the old
/// cluster was isolated, so the attestation is required and checked
/// against the exact backup.
fn restore(
    config: &Config,
    placed: &membership::Placed,
    dir: &std::path::Path,
    fencing: &std::path::Path,
    plan_only: bool,
) -> ExitCode {
    let held = match backup::read(dir) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let attestation = match backup::fencing(fencing) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    report_backup(&held.manifest);
    let configuration = placed.membership.epoch();
    let plan = match backup::plan(
        &held,
        placed.membership.cluster(),
        configuration,
        &attestation,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    println!(
        "restore source={} successor={} configuration={}",
        short(&plan.source.0),
        short(&plan.successor.0),
        plan.configuration.get(),
    );
    println!(
        "disposition kv={} retries={} configurations={} sessions={} leases={} watches={}",
        disposition(plan.kv),
        disposition(plan.retries),
        disposition(plan.configurations),
        disposition(plan.sessions),
        disposition(plan.leases),
        disposition(plan.watches),
    );
    if plan_only {
        println!("restore planned only; nothing was written");
        return ExitCode::SUCCESS;
    }
    // The restore creates this node's first generation, exactly as
    // `init` would, and fills it before anything attaches it to the
    // journal. That window is the only one in which a whole store's
    // worth of rows may be written directly: afterwards a projection
    // has frontiers, and writing behind them is what quarantines a
    // node.
    let mut outcome = None;
    let storage = store::open_storage_with(
        config,
        store::Intent::Initialize,
        placed.membership.cluster(),
        placed.membership.domain(),
        placed.replica,
        placed.incarnation,
        |engine| {
            outcome = Some(backup::carry_out(engine, &held, &plan, &attestation));
            Ok(())
        },
    );
    let restored = match outcome {
        Some(Ok(r)) => r,
        Some(Err(e)) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
        None => {
            eprintln!("the restore never reached this node's projection");
            return ExitCode::from(2);
        }
    };
    let mut storage = match storage {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    // The successor's own genesis policy, exactly as `init` writes it.
    // The backup deliberately carried none: the old cluster's trust
    // rules and permissions are its authority, not this one's.
    if let Err(e) = write_genesis_policy(config, &mut storage, placed.incarnation) {
        eprintln!("{e}");
        return ExitCode::from(2);
    }
    println!(
        "restored rows={} detached={} commits={} into {}",
        restored.receipt.rows,
        restored.receipt.detached,
        restored.commits,
        storage.generation.display(),
    );
    for (collection, rows) in &restored.receipt.dropped {
        println!("dropped collection={collection} rows={rows}");
    }
    ExitCode::SUCCESS
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
    // Inspecting comes before placement. `place` refuses to continue
    // when the committed configuration does not name this certificate as
    // a voter -- which is exactly the state an operator runs `inspect`
    // to understand. The operator-facing answer to "why is this node not
    // voting" has to be available precisely when the node cannot serve.
    if let Some(Command::Inspect) = cli.command {
        let policy = coord_node_issuer::RenewalPolicy::default();
        return match membership::inspect(
            &config.cluster_manifest,
            &config.identity.node_certificate,
            now_seconds(),
            &policy,
        ) {
            Ok(report) => {
                println!(
                    "credential replica={} incarnation={} role={:?} state={}",
                    short(&report.replica.0),
                    report.incarnation.get(),
                    report.role,
                    credential_state(&report.credential),
                );
                println!(
                    "leaf issued_at={} expires_at={} renewal={}",
                    report.leaf.issued_at,
                    report.leaf.expires_at,
                    renewal_state(&report.renewal),
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::from(2)
            }
        };
    }

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
        ) {
            Ok(mut storage) => {
                if let Err(e) = write_genesis_policy(&config, &mut storage, placed.incarnation) {
                    eprintln!("{e}");
                    return ExitCode::from(2);
                }
                println!("initialized {}", storage.generation.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::from(2)
            }
        };
    }

    // Verifying a backup reads no store at all: an operator checks a
    // backup from anywhere, including from a machine whose own store is
    // the one that was lost.
    // What this build is, before it opens anything (task-60). Formats
    // are what it reads and writes; features are what the cluster may
    // have turned on. An operator reconciling a mixed fleet reads this
    // line on every node and compares it.
    println!(
        "build schema={} journal={} shared_checkpoint={} local_checkpoint={} backup={} features={}",
        coord_types::formats::Format::StoreSchema.current(),
        coord_types::formats::Format::JournalRecord.current(),
        coord_types::formats::Format::SharedCheckpoint.current(),
        coord_types::formats::Format::LocalCheckpoint.current(),
        coord_types::formats::Format::Backup.current(),
        coord_types::formats::Supported::features()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
            .join(","),
    );

    if let Some(Command::Verify { dir }) = &cli.command {
        return match backup::read(dir) {
            Ok(held) => {
                report_backup(&held.manifest);
                println!("backup verified chunks={}", held.chunks.len());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::from(2)
            }
        };
    }

    if let Some(Command::Backup { out }) = &cli.command {
        return match store::open_storage(
            &config,
            store::Intent::Serve,
            placed.membership.cluster(),
            placed.membership.domain(),
            placed.replica,
            placed.incarnation,
        ) {
            Ok(storage) => {
                let origin = coord_checkpoint::export::CheckpointOrigin {
                    cluster: placed.membership.cluster(),
                    domain: placed.membership.domain(),
                };
                let Some(engine) = storage
                    .domain
                    .store()
                    .projection(placed.membership.domain())
                else {
                    eprintln!("this node has no projection of its own domain to back up");
                    return ExitCode::from(2);
                };
                match backup::take(engine, origin, out, now_seconds()) {
                    Ok(manifest) => {
                        report_backup(&manifest);
                        println!("backup written {}", backup::describe(out).display());
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        ExitCode::from(2)
                    }
                }
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::from(2)
            }
        };
    }

    if let Some(Command::Restore { dir, fencing, plan }) = &cli.command {
        return restore(&config, &placed, dir, fencing, *plan);
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
        "storage projection={} journaled_through={:?} owed={} baseline={}",
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
        // What this node would recover from, and that it loads: the
        // image was opened above, before the domain was attached.
        storage.baseline.as_ref().map_or(0, |p| p.represented.get()),
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
    let checkpoints = storage.checkpoints;
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
    // Where the other voters are, before a listener exists. A voter that
    // came up without knowing how to reach its peers would serve
    // requests it could never establish, and would look from outside
    // like a slow cluster rather than a misconfigured one.
    let peers = match peers::resolve(
        &placed.membership,
        placed.replica,
        config.cluster_endpoints.as_deref(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    println!(
        "peers reachable={} of {}",
        peers.len(),
        placed.membership.voters().count().saturating_sub(1)
    );
    // A frontend submits on a client's behalf, and that is the
    // collector's authority, not the node's. A node certificate binds
    // one role, so a process that serves callers and has other voters
    // to submit to needs the collector credential as well -- and is
    // refused here, before a listener exists, rather than at the first
    // request it could admit and then not deliver.
    if roles.needs_api_listener()
        && !peers.is_empty()
        && config.identity.collector_certificate.is_none()
    {
        eprintln!(
            "this process serves clients and submits to {} other voter(s), \
             and names no collector credential to submit as",
            peers.len()
        );
        return ExitCode::from(2);
    }
    let frontend = match serve::Frontend::new(&config, placed.membership.clone(), local) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    // The voters this process's own collector submits to. The same
    // voters the peer plane dials, reached on their other listener as
    // an API-class client: a submission is a collector's frame and a
    // vote is a voter's, and they are not the same conversation.
    let links = if roles.needs_api_listener() {
        peers.clone()
    } else {
        Vec::new()
    };
    let mut domain = serve::Domain::new(frontend, backing, serve::Budgets::default())
        // Where this node keeps its own recovery images, and how much
        // unrepresented journal it tolerates before making one. Local
        // to this node: no replicated result depends on the answer.
        .with_checkpoints(checkpoints, config.limits.checkpoint_after_records);
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
    let peer_socket = listeners.take_peer();
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
        // The peer plane, where this process has peers to reach. A
        // voter with none skips it: there is no socket to serve and
        // nobody to dial, and binding one would be a listener nothing
        // could arrive on.
        if !peers.is_empty() {
            let Some(socket) = peer_socket else {
                eprintln!("this node votes alongside peers but bound no peer listener");
                return ExitCode::from(2);
            };
            let plane =
                match serve::peer_endpoint(&config, &placed.membership, socket, placed.replica) {
                    Ok(t) => serve::PeerPlane::new(
                        t,
                        placed.membership.domain(),
                        placed.incarnation,
                        peers,
                    ),
                    Err(e) => {
                        eprintln!("{e}");
                        return ExitCode::from(2);
                    }
                };
            domain = domain.with_peers(plane);
        }
        domain = domain.with_links(serve::CollectorLinks::new(links));
        domain.run(&mut transport, now_seconds).await;
        eprintln!(
            "peers connected={} submittable={}",
            domain.reachable(),
            domain.submittable(&transport)
        );
        let (published, failed) = domain.checkpoints();
        eprintln!("checkpoints published={published} failed={failed}");
        let counts = domain.counts();
        eprintln!(
            "the api plane ended: queued_local={} queued_remote={} not_a_voter={} \
saturated={} unavailable={} refused={} released={} returned={} unreturnable={} \
watches={} unserved={}",
            counts.queued_local,
            counts.queued_remote,
            counts.not_a_voter,
            counts.saturated,
            counts.unavailable,
            counts.refused,
            counts.released,
            counts.returned,
            counts.unreturnable,
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
        let mut leader = Leader::new_sealed(
            LeaderConfig {
                identity,
                quorum,
                genesis: ballot,
                frontend: collector,
                capacity: 64,
            },
            recovered.promise.clone(),
            // A replica comes back sealed because its row says so
            // (task-55). Passing it here is what makes a restart unable
            // to resume old service.
            recovered.seal,
            executed_through,
        );
        leader.set_learning(LearningMode::Full);
        coord_daemon::Machine::Leader(Box::new(leader))
    } else {
        let mut follower = Follower::recover_with_syncs(
            FollowerConfig {
                identity,
                quorum,
                genesis: ballot,
                frontend: collector,
                capacity: 64,
            },
            recovered.promise.clone(),
            recovered.seal,
            recovered.records.clone(),
            recovered.payloads.clone(),
            recovered.syncs.clone(),
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
        (m.cluster(), m.domain()),
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
