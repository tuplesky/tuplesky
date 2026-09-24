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
        let opened = store::open(
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
            Ok(_) => {
                println!("initialized {}", generation.directory().display());
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
    let mut generation = match store::open(
        &config,
        store::Intent::Serve,
        placed.membership.cluster(),
        placed.membership.domain(),
        placed.replica,
        placed.incarnation,
    ) {
        Ok(g) => g,
        Err(e) => {
            lifecycle.quarantine(QuarantineReason::Disk);
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    // The store is this node's only under the manifest it was
    // initialized with. Anything else -- another set of voters, another
    // policy, under the same cluster and domain -- is a genesis
    // quarantine, not a new configuration to adopt.
    if let Err(e) = genesis::check(&mut generation, &placed.manifest, genesis::Intent::Serve) {
        lifecycle.quarantine(e.quarantine_reason());
        eprintln!("{e}");
        return ExitCode::from(2);
    }
    println!("store {}", generation.directory().display());
    lifecycle.observe(Readiness {
        storage_ready: true,
        ..Readiness::default()
    });
    let listeners = match bind_listeners(&config.listen) {
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
    let diagnostics = Diagnostics::snapshot(&roles, &lifecycle, 0);
    println!("coordd phase={}", diagnostics.phase);
    // Consensus and serving arrive with the later integration tasks, so
    // the process holds its listeners and reports Live rather than Ready:
    // it is up, and it is honest that it is not yet serving.
    eprintln!("reference preview: listeners bound; serving not enabled in this build");
    loop {
        std::thread::park();
    }
}

/// A short, stable rendering of an identity for an operator's eye. It is
/// an identity, not a secret, and the full value is in the certificate.
fn short(bytes: &[u8; 16]) -> String {
    bytes[..4].iter().map(|b| format!("{b:02x}")).collect()
}
