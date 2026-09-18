//! `coordd`: parse strict configuration, report the composed roles and
//! the readiness requirements, and exit. Actually binding listeners and
//! running consensus is the reference preview's runtime, gated behind the
//! later lifecycle and integration tasks; this binary validates the
//! configuration a deployment supplies and prints a secret-safe summary.

use std::process::ExitCode;

use clap::Parser;
use coord_daemon::{Config, Diagnostics, Lifecycle};

#[derive(Parser)]
#[command(name = "coordd", about = "TupleSky node daemon (reference preview)")]
struct Cli {
    /// Path to the strict TOML configuration.
    #[arg(long)]
    config: std::path::PathBuf,
    /// Validate the configuration and exit without starting.
    #[arg(long)]
    check: bool,
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
    if cli.check {
        return ExitCode::SUCCESS;
    }
    // The runtime (listener binding, consensus, serving) is gated behind
    // the later lifecycle and integration tasks; the preview validates
    // and reports rather than claiming to serve.
    eprintln!("reference preview: runtime not enabled in this build");
    ExitCode::SUCCESS
}
