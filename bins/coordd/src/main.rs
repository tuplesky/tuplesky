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
    // A node's identity: for the reference preview it comes from the
    // configured domain, and the genesis manifest supplies the rest when
    // membership is wired. Nothing here invents one.
    let identity = preview_identity(&config);

    if let Some(Command::Init) = cli.command {
        return match store::open(
            &config,
            store::Intent::Initialize,
            identity.0,
            identity.1,
            identity.2,
            identity.3,
        ) {
            Ok(generation) => {
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
    let generation = match store::open(
        &config,
        store::Intent::Serve,
        identity.0,
        identity.1,
        identity.2,
        identity.3,
    ) {
        Ok(g) => g,
        Err(e) => {
            lifecycle.quarantine(QuarantineReason::Disk);
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
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

/// The identity this preview node runs under.
///
/// Membership binds a node to a replica and an incarnation from the
/// genesis manifest (task-42); until the daemon reads that manifest, the
/// preview derives a stable identity from the configured domain so a
/// node reopens its own store rather than a different one. It is derived,
/// never random: a node that generated a fresh identity each start would
/// fail to open the store it wrote last time, and the failure would look
/// like corruption.
fn preview_identity(
    config: &Config,
) -> (
    coord_types::ids::ClusterId,
    coord_types::ids::DomainId,
    coord_types::ids::ReplicaId,
    coord_types::ids::ReplicaIncarnation,
) {
    let domain = name_digest(config.domain.as_bytes());
    let replica = name_digest(config.state_directory.as_bytes());
    (
        coord_types::ids::ClusterId(name_digest(config.cluster_manifest.as_bytes())),
        coord_types::ids::DomainId(domain),
        coord_types::ids::ReplicaId(replica),
        coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
    )
}

/// A stable 16-byte identity for a name. Not a security primitive: it
/// exists so the same configuration reopens the same store.
fn name_digest(name: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let mut state: u64 = 0xcbf2_9ce4_8422_2325;
    for (i, byte) in name.iter().enumerate() {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
        out[i % 16] ^= (state >> 32) as u8;
    }
    out[0] |= 1;
    out
}
