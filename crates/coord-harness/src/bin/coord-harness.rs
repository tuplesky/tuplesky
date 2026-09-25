//! `coord-harness`: stand a real domain up so something outside
//! `cargo test` can drive it (task-48, task-62).
//!
//! `provision` writes the domain; `up` runs it and stays up; `start`
//! runs one voter of it, for a domain whose voters are on different
//! hosts; `issuer` runs the credential endpoint alone; `dsn` prints the
//! data source a Kine build is given. The commands are separate because
//! the steps are: a certification run provisions once, keeps the
//! directory as evidence, and can restart the daemons against it.
#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use coord_harness::domain::{Address, Plan, Provisioned};

#[derive(Parser)]
#[command(
    name = "coord-harness",
    about = "Provision and run a TupleSky domain for qualification runs"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// How a domain is provisioned, shared by `provision` and by `up` when
/// it has to provision.
#[derive(Args)]
struct Provisioning {
    /// How many voters the genesis commits, and how many `up` runs.
    /// Defaults to 3, or to the number of voters `--hosts` places.
    #[arg(long)]
    voters: Option<u8>,
    /// The port the Kubernetes storage edge binds; 0 picks a free
    /// one. A Kubernetes run pins it, because the API server's
    /// configuration names it.
    #[arg(long, default_value_t = 0)]
    edge_port: u16,
    /// Place each voter on its own host:
    /// `n1=host:api_port:peer_port,n2=...`, each host an IP literal (an
    /// IPv6 one in brackets) or a DNS name. The catalog lists these
    /// addresses, each node listens on its fixed ports, its certificates
    /// carry its host, and each `nN/` directory becomes a bundle that
    /// runs wherever it is copied. Without it every voter is on this
    /// host's loopback, exactly as before.
    #[arg(long)]
    hosts: Option<String>,
    /// Listen on the unspecified address instead of each named host, for
    /// hosts reached at an address that is not on any of their
    /// interfaces. A host given by DNS name always is; a loopback
    /// address never is.
    #[arg(long)]
    listen_any: bool,
    /// The host an API server reaches the storage edge at, when that is
    /// not this host's loopback. Its server certificate carries it.
    #[arg(long)]
    edge_host: Option<String>,
    /// Where the credential endpoint listens, `host:port`, when a Kine
    /// build on another host has to reach it. Its certificate carries
    /// the host, and `coord-harness issuer` then binds that host and no
    /// other off-loopback address.
    #[arg(long, value_parser = Address::parse)]
    issuer_listen: Option<Address>,
}

#[derive(Subcommand)]
enum Command {
    /// Write a provisioned domain into a directory.
    Provision {
        /// The run directory. It is created if absent.
        #[arg(long)]
        dir: PathBuf,
        #[command(flatten)]
        provisioning: Provisioning,
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
        #[command(flatten)]
        provisioning: Provisioning,
    },
    /// Initialize one voter if it has no store yet, start it from its
    /// own directory, wait until it serves, and stay in the foreground
    /// until it exits. This is how a voter of a multi-host domain is run
    /// on its host.
    Start {
        /// The directory holding the voter's `nN/` bundle: the run
        /// directory, or wherever the bundle was copied to.
        #[arg(long)]
        dir: PathBuf,
        /// Which voter, one-based.
        #[arg(long)]
        node: u8,
        /// The `coordd` binary to run.
        #[arg(long)]
        coordd: PathBuf,
    },
    /// Run the credential endpoint alone, in the foreground.
    Issuer {
        /// The run directory.
        #[arg(long)]
        dir: PathBuf,
        /// Bind this address instead of the provisioned one. Anything
        /// but loopback is refused unless the domain was provisioned
        /// with `--issuer-listen` for that host, on that port; the
        /// unspecified address on that port is accepted then too.
        #[arg(long)]
        listen: Option<std::net::SocketAddr>,
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
        Command::Provision { dir, provisioning } => {
            let provisioned = provision(&dir, provisioning)?;
            println!("{}", serde_json::to_string_pretty(&provisioned)?);
            Ok(())
        }
        Command::Up {
            dir,
            coordd,
            provisioning,
        } => up(&dir, &coordd, provisioning),
        Command::Start { dir, node, coordd } => start(&dir, node, &coordd),
        Command::Issuer { dir, listen } => {
            let endpoint = Arc::new(coord_harness::issuer::Endpoint::bind_on(&dir, listen)?);
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
    provisioning: Provisioning,
) -> Result<Provisioned, Box<dyn std::error::Error>> {
    let hosts = match provisioning.hosts.as_deref() {
        Some(spec) => coord_harness::domain::parse_hosts(spec)?,
        None => Vec::new(),
    };
    let voters = match (provisioning.voters, hosts.len()) {
        (Some(voters), 0) => voters,
        (None, 0) => 3,
        (None, placed) => u8::try_from(placed).map_err(|_| "more voters than a genesis holds")?,
        (Some(voters), placed) if usize::from(voters) == placed => voters,
        (Some(voters), placed) => {
            return Err(format!("--voters {voters} and a host list placing {placed}").into());
        }
    };
    if voters == 0 {
        return Err("a domain with no voters has no quorum".into());
    }
    Ok(coord_harness::provision(&Plan {
        hosts,
        listen_any: provisioning.listen_any,
        edge_host: provisioning.edge_host,
        issuer_listen: provisioning.issuer_listen,
        ..Plan::loopback(dir.to_path_buf(), voters, provisioning.edge_port)
    })?)
}

/// Run one voter from its bundle until it exits.
///
/// What it prints is what an operator on that host watches for: one
/// `harness node-ready` line once the daemon has said `phase=live`, and
/// the daemon's own output in `coordd.log` beside its configuration,
/// where the `peers connected=` and `voters submittable=` lines say
/// whether it has found the others. The daemon's process identifier is
/// written to `coordd.pid` there too, so the voter can be killed
/// without killing this process first; this one then exits with it.
fn start(
    dir: &std::path::Path,
    node: u8,
    coordd: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = dir.join(format!("n{node}"));
    let config = directory.join("coordd.toml");
    if !config.is_file() {
        return Err(format!("{} is not a provisioned voter", config.display()).into());
    }
    let label = format!("n{node}");
    coord_harness::run::initialize_node(coordd, &label, &config, &directory)?;
    let mut daemon = coord_harness::run::start_node(coordd, &label, &config, &directory)?;
    let pid = directory.join("coordd.pid");
    std::fs::write(&pid, format!("{}\n", daemon.pid()))?;
    println!(
        "harness node-ready node={label} pid={} api={} output={}",
        daemon.pid(),
        daemon.api,
        daemon.log.display()
    );
    std::io::stdout().flush()?;
    let status = daemon.wait()?;
    let _ = std::fs::remove_file(&pid);
    println!("harness node-stopped node={label} status={status}");
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} stopped: {status}").into())
    }
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
    provisioning: Provisioning,
) -> Result<(), Box<dyn std::error::Error>> {
    let provisioned = if dir.join("harness.json").is_file() {
        Provisioned::read(dir)?
    } else {
        provision(dir, provisioning)?
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

    // Stay up, and notice when one of them does not. A voter that stops
    // is reported at once, on its own line, so a run can tell which one
    // went and when. The others are kept up for as long as they are a
    // quorum: losing one of three is the regional failover a
    // certification run exercises, and tearing the survivors down with
    // it would certify an outage instead. Only once fewer than a quorum
    // remain does the harness stop, because a run that kept going then
    // would report the wrong thing about the domain it was measuring.
    let mut stopped = vec![false; daemons.len()];
    loop {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let mut alive = 0;
        let mut changed = false;
        for (daemon, gone) in daemons.iter_mut().zip(stopped.iter_mut()) {
            if daemon.alive() {
                alive += 1;
                continue;
            }
            if !*gone {
                *gone = true;
                changed = true;
                println!(
                    "harness voter-stopped node={} output={}",
                    daemon.node,
                    daemon.log.display()
                );
            }
        }
        match coord_harness::run::standing(alive, daemons.len()) {
            coord_harness::run::Standing::Serving { alive, of } => {
                if changed {
                    println!("harness serving voters={alive} of={of}");
                    std::io::stdout().flush()?;
                }
            }
            coord_harness::run::Standing::Lost { alive, of } => {
                return Err(format!(
                    "only {alive} of {of} voters are still running, fewer than a quorum; \
                     each stopped voter's output is named on its voter-stopped line"
                )
                .into());
            }
        }
    }
}
