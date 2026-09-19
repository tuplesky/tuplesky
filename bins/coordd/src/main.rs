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
    let mut frontend = match serve::Frontend::new(
        &config,
        placed.membership.clone(),
        applier,
        // No voter runs in this process yet, so every voter --
        // including this node, where it is one -- is reached over the
        // wire. The local ingress is wired when the voter loop is.
        None,
    ) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    println!("frontend ready waiting={}", frontend.waiting());

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
        frontend.run(&mut transport, now_seconds).await;
        let counts = frontend.counts();
        eprintln!(
            "the api plane ended: queued_local={} queued_remote={} not_a_voter={} \
saturated={} unavailable={} watches={} unserved={}",
            counts.queued_local,
            counts.queued_remote,
            counts.not_a_voter,
            counts.saturated,
            counts.unavailable,
            counts.watches,
            counts.unserved
        );
        ExitCode::from(1)
    })
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
