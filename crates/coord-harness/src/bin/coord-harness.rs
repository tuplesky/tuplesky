//! `coord-harness`: stand a real domain up so something outside
//! `cargo test` can drive it (task-48, task-62).
//!
//! `provision` writes the domain; `up` runs it and stays up; `issuer`
//! runs the credential endpoint alone; `dsn` prints the data source a
//! Kine build is given. The commands are separate because the steps are:
//! a certification run provisions once, keeps the directory as evidence,
//! and can restart the daemons against it.
#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use coord_harness::domain::{Plan, Provisioned};

#[derive(Parser)]
#[command(
    name = "coord-harness",
    about = "Provision and run a TupleSky domain for qualification runs"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a provisioned domain into a directory.
    Provision {
        /// The run directory. It is created if absent.
        #[arg(long)]
        dir: PathBuf,
        /// How many voters the genesis commits, and how many `up` runs.
        #[arg(long, default_value_t = 3)]
        voters: u8,
        /// The port the Kubernetes storage edge binds; 0 picks a free
        /// one. A Kubernetes run pins it, because the API server's
        /// configuration names it.
        #[arg(long, default_value_t = 0)]
        edge_port: u16,
    },
    /// Provision if needed, start every voter and the credential
    /// endpoint, and stay up until the process is stopped.
    Up {
        /// The run directory.
        #[arg(long)]
        dir: PathBuf,
        /// The `coordd` binary to run.
        #[arg(long)]
        coordd: PathBuf,
        /// Voters, when this provisions.
        #[arg(long, default_value_t = 3)]
        voters: u8,
        /// Storage edge port, when this provisions.
        #[arg(long, default_value_t = 0)]
        edge_port: u16,
    },
    /// Run the credential endpoint alone, in the foreground.
    Issuer {
        /// The run directory.
        #[arg(long)]
        dir: PathBuf,
    },
    /// Print the `coord://` data source a Kine build is given. It names
    /// the credential file; it carries no credential.
    Dsn {
        /// The run directory.
        #[arg(long)]
        dir: PathBuf,
        /// Which voter's frontend to address, one-based.
        #[arg(long, default_value_t = 1)]
        voter: usize,
    },
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("coord-harness: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Provision {
            dir,
            voters,
            edge_port,
        } => {
            let provisioned = provision(&dir, voters, edge_port)?;
            println!("{}", serde_json::to_string_pretty(&provisioned)?);
            Ok(())
        }
        Command::Up {
            dir,
            coordd,
            voters,
            edge_port,
        } => up(&dir, &coordd, voters, edge_port),
        Command::Issuer { dir } => {
            let endpoint = Arc::new(coord_harness::issuer::Endpoint::bind(&dir)?);
            println!("issuer listening {}", endpoint.address()?);
            std::io::stdout().flush()?;
            endpoint.serve()?;
            Ok(())
        }
        Command::Dsn { dir, voter } => {
            let provisioned = Provisioned::read(&dir)?;
            println!("{}", dsn(&provisioned, voter)?);
            Ok(())
        }
    }
}

fn provision(
    dir: &std::path::Path,
    voters: u8,
    edge_port: u16,
) -> Result<Provisioned, Box<dyn std::error::Error>> {
    if voters == 0 {
        return Err("a domain with no voters has no quorum".into());
    }
    Ok(coord_harness::provision(&Plan {
        directory: dir.to_path_buf(),
        voters,
        edge_port,
    })?)
}

/// The data source a Kine build is given. Every field in it is a public
/// identifier or a path; the workload credential stays in the file the
/// `assertion` option names, which is why the flag's own help says the
/// DSN is never logged.
fn dsn(provisioned: &Provisioned, voter: usize) -> Result<String, Box<dyn std::error::Error>> {
    let node = provisioned
        .voters
        .get(voter.checked_sub(1).ok_or("voters are numbered from one")?)
        .ok_or("no such voter")?;
    Ok(format!(
        "coord://{frontend}?cluster={cluster}&domain={domain}&namespace={namespace}\
&server-name={server_name}&assertion={assertion}&sts={sts}&sts-ca={sts_ca}&audience={audience}",
        frontend = node.api,
        cluster = provisioned.cluster,
        domain = provisioned.domain,
        namespace = provisioned.namespace,
        server_name = provisioned.server_name,
        assertion = provisioned.issuer.assertion.display(),
        sts = provisioned.issuer.url,
        sts_ca = provisioned.issuer.ca.display(),
        audience = provisioned.resource,
    ))
}

fn up(
    dir: &std::path::Path,
    coordd: &std::path::Path,
    voters: u8,
    edge_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let provisioned = if dir.join("harness.json").is_file() {
        Provisioned::read(dir)?
    } else {
        provision(dir, voters, edge_port)?
    };

    // The credential endpoint first: a frontend verifies tokens against
    // the keys it read at startup, but a caller cannot get one before
    // there is somewhere to exchange.
    let endpoint = Arc::new(coord_harness::issuer::Endpoint::bind(dir)?);
    let issuer_address = endpoint.address()?;
    std::thread::spawn(move || {
        if let Err(err) = endpoint.serve() {
            eprintln!("coord-harness: the credential endpoint stopped: {err}");
        }
    });

    coord_harness::initialize(coordd, &provisioned)?;
    let mut daemons = coord_harness::start_all(coordd, &provisioned)?;

    let pids: Vec<u32> = daemons.iter().map(coord_harness::Daemon::pid).collect();
    // One identifier per line, the last one terminated: a shell that
    // reads this file line by line drops an unterminated last line, and
    // the process it names would outlive the run and contend with the
    // next one for its ports.
    std::fs::write(
        dir.join("harness.pids"),
        pids.iter()
            .map(|pid| format!("{pid}\n"))
            .collect::<String>(),
    )?;

    println!(
        "harness ready voters={} issuer={issuer_address} edge={} frontend={}",
        daemons.len(),
        provisioned.edge.listen,
        provisioned.frontend()
    );
    println!("harness dsn {}", dsn(&provisioned, 1)?);
    std::io::stdout().flush()?;

    // Stay up, and notice if one of them does not. A certification run
    // that kept going after a voter died would report the wrong thing
    // about the cluster it was measuring.
    loop {
        std::thread::sleep(std::time::Duration::from_millis(500));
        for daemon in &mut daemons {
            if !daemon.alive() {
                return Err(format!(
                    "node {} stopped; its output is {}",
                    daemon.node,
                    daemon.log.display()
                )
                .into());
            }
        }
    }
}
