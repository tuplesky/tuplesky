//! A domain provisioned for separate hosts, run on this one (task-d04).
//!
//! `coordd` has no single-host assumption of its own: it binds what its
//! configuration says, learns its peers from the signed catalog and
//! checks every peer's certificate against the host it dialled. The
//! harness had one -- every catalog address, listener and certificate
//! name was `127.0.0.1` -- so a domain across machines had never been
//! provisioned, and nothing showed that the three agree once they stop
//! being loopback. This drives `coord-harness provision --hosts` the way
//! the runbook (`docs/operations/multi-host-test.md`) does, with every
//! "host" a different address of the loopback range: each voter listens
//! only on its own address, on the same fixed ports as the others; a
//! peer that dials `127.0.0.3` accepts only a certificate valid for
//! `127.0.0.3`; and each node runs from a copy of its bundle somewhere
//! other than where it was provisioned.
//!
//! The same flow through the Kubernetes storage edge, with a Go Kine
//! build in front of voter 1, is `scripts/e2e/multi-host-local.sh`; this
//! is the part that needs nothing but the daemon.

use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::time::Duration;

use coord_harness::domain::{
    JournalPlan, JournalProfile, Plan, Provisioned, parse_hosts, provision,
};
use coord_wan_bench::{Answer, Caller};

const HOSTS: [&str; 3] = ["127.0.0.2", "127.0.0.3", "127.0.0.4"];

fn binary() -> PathBuf {
    // The integration test's own executable sits beside the binary under
    // test, whichever profile built it.
    let mut path = std::env::current_exe().expect("test binary");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join("coordd")
}

fn workspace(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("coordd-multi-host-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("workspace");
    path
}

/// A UDP port free on every one of `HOSTS` and not already `taken`.
///
/// One port for all three, because that is what a deployment does and
/// what only per-address listeners allow on one machine: three voters
/// each bound to `0.0.0.0` on one port would collide.
fn free_everywhere(taken: &[u16]) -> u16 {
    loop {
        let port = UdpSocket::bind((HOSTS[0], 0))
            .and_then(|s| s.local_addr())
            .expect("a free port")
            .port();
        if !taken.contains(&port) && HOSTS.iter().all(|h| UdpSocket::bind((*h, port)).is_ok()) {
            return port;
        }
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
    }
}

/// The counts a daemon's log reports on lines starting `prefix`, in
/// order: `peers connected=` or `voters submittable=`, each followed by
/// `N of M`.
fn reported(said: &str, prefix: &str) -> Vec<usize> {
    said.lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .filter_map(|rest| rest.split(' ').next()?.parse().ok())
        .collect()
}

/// Whether both of a voter's planes last reported every other voter.
fn meshed(said: &str) -> bool {
    reported(said, "peers connected=").last() == Some(&2)
        && reported(said, "voters submittable=").last() == Some(&2)
}

/// Whether a voter last said its policy is fresh (#320): a voter binds
/// nobody until its leader has confirmed its store, after its first
/// start and after a restart alike, so a caller binds once it has.
fn policy_fresh(said: &str) -> bool {
    said.lines()
        .rev()
        .find(|line| line.contains("this voter's policy is "))
        .is_some_and(|line| line.contains("this voter's policy is fresh"))
}

/// One voter, run from its bundle the way `coord-harness start` runs it.
struct Voter {
    bundle: PathBuf,
    daemon: Option<coord_harness::Daemon>,
}

impl Voter {
    /// `init` for the first start, as `coord-harness start --init`; a
    /// restart resumes the voter's own state and never initializes.
    fn start(&mut self, n: usize, init: bool) {
        let label = format!("n{n}");
        let config = self.bundle.join("coordd.toml");
        if init {
            coord_harness::run::initialize_node(&binary(), &label, &config, &self.bundle)
                .expect("initialized from the bundle");
        } else {
            coord_harness::run::resume_node(&label, &self.bundle).expect("resumed from its state");
        }
        self.daemon = Some(
            coord_harness::run::start_node(&binary(), &label, &config, &self.bundle)
                .unwrap_or_else(|e| panic!("{e}:\n{}", self.said())),
        );
    }

    /// Everything the daemon has written, across restarts.
    fn said(&self) -> String {
        std::fs::read_to_string(self.bundle.join("coordd.log")).unwrap_or_default()
    }

    /// Wait up to `seconds` for what it has said since byte `from` to
    /// satisfy `test`.
    fn waits_until(&self, seconds: u64, from: usize, test: impl Fn(&str) -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
        loop {
            if test(self.said().get(from..).unwrap_or_default()) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn put(provisioned: &Provisioned, key: &[u8]) -> coord_types::logical_v1::LogicalRequest {
    let mut namespace = [0u8; 16];
    for (i, byte) in namespace.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&provisioned.namespace[i * 2..i * 2 + 2], 16).expect("hex");
    }
    let mut logical = coord_types::logical_v1::LogicalRequest::new(
        coord_types::ids::NamespaceId(namespace),
        coord_types::logical_v1::CanonicalOperation::Put(coord_types::logical_v1::PutOp {
            key: key.to_vec(),
            value: b"placed".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    logical
}

fn get(provisioned: &Provisioned, key: &[u8]) -> coord_types::logical_v1::LogicalRequest {
    let put = put(provisioned, key);
    let mut logical = coord_types::logical_v1::LogicalRequest::new(
        put.namespace,
        coord_types::logical_v1::CanonicalOperation::Range(coord_types::logical_v1::RangeOp {
            range: coord_types::logical_v1::KeyRange::exact(key.to_vec()),
            revision: None,
            limit: 0,
            keys_only: false,
            count_only: false,
        }),
    );
    logical.canonicalize();
    logical
}

/// A voter started after the domain served its first write serves reads
/// (task-d07).
///
/// The decisive case from the Jepsen client. Voters 1 and 2 serve a write
/// before voter 3 exists; its proposal went out to a peer that was not
/// linked, and was dropped. Nothing sent it again, and every later
/// proposal depends on it through the conservative key, so voter 3 held
/// everything and executed nothing: a read through it waited for ever,
/// while writes still succeeded because the leader answers them. The
/// leader now sends a proposal again until every voter has voted on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_voter_started_after_the_first_write_serves_reads() {
    if HOSTS.iter().any(|h| UdpSocket::bind((*h, 0)).is_err()) {
        eprintln!("skipped: {HOSTS:?} are not all assigned on this machine");
        return;
    }
    let dir = workspace("late");
    let api = free_everywhere(&[]);
    let peer = free_everywhere(&[api]);
    let spec = HOSTS
        .iter()
        .enumerate()
        .map(|(i, host)| format!("n{}={host}:{api}:{peer}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let run = dir.join("run");
    let provisioned = provision(&Plan {
        hosts: parse_hosts(&spec).expect("a host list"),
        ..Plan::loopback(run.clone(), 3, 0)
    })
    .expect("provisioned");
    let mut voters: Vec<Voter> = (1..=3)
        .map(|n| Voter {
            bundle: run.join(format!("n{n}")),
            daemon: None,
        })
        .collect();
    for (i, voter) in voters.iter_mut().enumerate().take(2) {
        voter.start(i + 1, true);
    }
    for (i, voter) in voters.iter().enumerate().take(2) {
        assert!(
            voter.waits_until(60, 0, |said| reported(said, "voters submittable=").last()
                == Some(&1)),
            "voter {} did not reach the other started voter:\n{}",
            i + 1,
            voter.said()
        );
    }
    let minter = coord_harness::issuer::Minter::of(&provisioned).expect("the minter");
    assert!(
        voters[0].waits_until(60, 0, policy_fresh),
        "voter 1's policy never turned fresh:\n{}",
        voters[0].said()
    );
    let mut first = Caller::connect(&run, &provisioned, &minter, 0, 0)
        .await
        .expect("a caller bound at voter 1");
    assert_eq!(
        first
            .ask(&put(&provisioned, b"early"), Duration::from_secs(30))
            .await,
        Answer::Established,
        "two voters of three did not establish a write"
    );

    // Its first start, late.
    voters[2].start(3, true);
    for (i, voter) in voters.iter().enumerate() {
        assert!(
            voter.waits_until(60, 0, meshed),
            "voter {} did not reach every other voter:\n{}",
            i + 1,
            voter.said()
        );
    }
    assert!(
        voters[2].waits_until(60, 0, policy_fresh),
        "the late voter's policy never turned fresh:\n{}",
        voters[2].said()
    );
    let mut late = Caller::connect(&run, &provisioned, &minter, 2, 1)
        .await
        .expect("a caller bound at the late voter");
    // A fresh session's first read at the late voter can be held back as
    // pending until the voter has projected the session, as at a returned
    // voter: it is asked again until it is answered (`read_value`).
    assert_eq!(
        read_value(&mut late, &provisioned, b"early").await,
        Some(b"placed".to_vec()),
        "a read through the voter started late was not served:\n-- 1 --\n{}\n-- 2 --\n{}\n-- 3 --\n{}",
        voters[0].said(),
        voters[1].said(),
        voters[2].said()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Three voters placed on three addresses come up, serve a request,
/// and take back a voter that was killed and restarted (task-d04, with
/// the re-dial of task-d03).
///
/// Voter 3 is killed without closing anything, so its links end for the
/// survivors only at
/// the transport's idle timeout, and the survivors have to dial it again
/// on both planes when it returns. The request after that goes in at
/// voter 3's own frontend, so it is the returned voter that serves it.
#[tokio::test(flavor = "multi_thread")]
async fn a_domain_placed_on_three_addresses_serves_and_takes_back_a_restarted_voter() {
    takes_back_a_restarted_voter("placed", JournalPlan::default()).await;
}

/// The same under the replay-backed profile (task-j06), as
/// `coord-harness provision --journal-profile replay` writes it, with a
/// cadence that makes nothing durable on its own: voter 3 is killed with
/// its projection's commits since its start still working ones, comes
/// back by replaying them from its journal, and serves a read of the
/// write it had applied before the kill. Its startup metrics carry the
/// projection's durable frontier, which only the replay profile reports,
/// so the run is the profile it was provisioned as.
#[tokio::test(flavor = "multi_thread")]
async fn a_voter_killed_under_the_replay_profile_comes_back_and_serves() {
    takes_back_a_restarted_voter(
        "placed-replay",
        JournalPlan {
            profile: JournalProfile::Replay,
            projection_durable_commits: Some(u32::MAX),
            projection_durable_records: Some(u32::MAX),
            projection_durable_ms: Some(3_600_000),
            ..JournalPlan::default()
        },
    )
    .await;
}

/// The value a read of one key came back with: `None` for a read that
/// was not established or found nothing.
async fn read_value(caller: &mut Caller, provisioned: &Provisioned, key: &[u8]) -> Option<Vec<u8>> {
    // A node that has not yet projected the caller's session holds a
    // read's result back as pending rather than refusing it, and the
    // caller resolves (coord-session's output gate): a voter that has
    // just come back can hold a fresh session's first read that way. A
    // read is asked again, as a new invocation, until it is answered.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match caller
            .call(&get(provisioned, key), Duration::from_secs(30))
            .await
        {
            Ok(coord_sdk::Outcome::Established { result, .. }) => {
                return match postcard::from_bytes::<coord_state::Response>(&result)
                    .ok()?
                    .outcome
                {
                    coord_state::Outcome::Range { items, .. } => {
                        items.first().map(|item| item.entry.value.clone())
                    }
                    _ => None,
                };
            }
            Ok(coord_sdk::Outcome::Unknown) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => return None,
        }
    }
}

/// The startup metrics lines of what a voter said, in order.
fn startup_metrics(said: &str) -> Vec<serde_json::Value> {
    said.lines()
        .filter_map(|line| line.strip_prefix("metrics "))
        .filter_map(|json| serde_json::from_str(json).ok())
        .collect()
}

async fn takes_back_a_restarted_voter(name: &str, journal: JournalPlan) {
    // 127.0.0.2 and up are loopback on Linux and not assigned by default
    // elsewhere (macOS): there is nothing to place a voter at.
    if HOSTS.iter().any(|h| UdpSocket::bind((*h, 0)).is_err()) {
        eprintln!("skipped: {HOSTS:?} are not all assigned on this machine");
        return;
    }
    let replay = journal.profile == JournalProfile::Replay;
    let dir = workspace(name);
    let api = free_everywhere(&[]);
    let peer = free_everywhere(&[api]);
    let spec = HOSTS
        .iter()
        .enumerate()
        .map(|(i, host)| format!("n{}={host}:{api}:{peer}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let run = dir.join("run");
    let provisioned = provision(&Plan {
        hosts: parse_hosts(&spec).expect("a host list"),
        journal: journal.clone(),
        ..Plan::loopback(run.clone(), 3, 0)
    })
    .expect("provisioned");
    assert_eq!(provisioned.journal_profile, journal.profile);
    for (i, node) in provisioned.voters.iter().enumerate() {
        // What the daemon will read is the profile the plan named.
        let config = coord_daemon::config::Config::parse(
            &std::fs::read_to_string(&node.config).expect("the voter's configuration"),
        )
        .expect("a configuration the daemon accepts");
        assert_eq!(config.journal.replays_projection(), replay);
        assert_eq!(node.api, format!("{}:{api}", HOSTS[i]));
        assert_eq!(node.peer, format!("{}:{peer}", HOSTS[i]));
    }

    // Each bundle is copied away from where it was written, as it would
    // be to another machine, and the copy is what runs.
    let mut voters: Vec<Voter> = (1..=3)
        .map(|n| {
            let bundle = dir.join(format!("h{n}")).join(format!("n{n}"));
            copy_tree(&run.join(format!("n{n}")), &bundle);
            Voter {
                bundle,
                daemon: None,
            }
        })
        .collect();
    for (i, voter) in voters.iter_mut().enumerate() {
        voter.start(i + 1, true);
    }
    for (i, voter) in voters.iter().enumerate() {
        assert!(
            voter.waits_until(60, 0, meshed),
            "voter {} did not reach every other voter at its placed address:\n{}",
            i + 1,
            voter.said()
        );
    }

    let minter = coord_harness::issuer::Minter::of(&provisioned).expect("the minter");
    assert!(
        voters[0].waits_until(60, 0, policy_fresh),
        "voter 1's policy never turned fresh:\n{}",
        voters[0].said()
    );
    let mut caller = Caller::connect(&run, &provisioned, &minter, 0, 0)
        .await
        .expect("a caller bound at voter 1");
    assert_eq!(
        caller
            .ask(&put(&provisioned, b"before"), Duration::from_secs(30))
            .await,
        Answer::Established,
        "the placed domain did not establish a request"
    );

    // Voter 3 has applied the write before it goes: under the replay
    // profile, a working commit its kill loses. The caller's session is
    // a moment old, and a voter that has not projected it yet answers
    // the read pending (the output gate's "not yet"), so the read is
    // asked again until it is answered, as `read_value` does.
    if replay {
        assert!(
            voters[2].waits_until(60, 0, policy_fresh),
            "voter 3's policy never turned fresh:\n{}",
            voters[2].said()
        );
        let mut at_three = Caller::connect(&run, &provisioned, &minter, 2, 2)
            .await
            .expect("a caller bound at voter 3");
        assert_eq!(
            read_value(&mut at_three, &provisioned, b"before").await,
            Some(b"placed".to_vec()),
            "voter 3 did not serve the write before it was killed:\n{}",
            voters[2].said()
        );
    }

    // Voter 3 goes, without closing anything.
    voters[2].daemon = None;
    for (i, voter) in voters.iter().take(2).enumerate() {
        assert!(
            voter.waits_until(90, 0, |said| reported(said, "voters submittable=").last()
                == Some(&1)),
            "survivor {} did not see voter 3 go:\n{}",
            i + 1,
            voter.said()
        );
    }
    let since: Vec<usize> = voters.iter().map(|v| v.said().len()).collect();
    voters[2].start(3, false);
    // The restarted voter reports the projection's durable frontier
    // apart from what it has applied exactly when it runs the replay
    // profile. The harness writes the log from its own thread, so the
    // startup snapshot can reach it just after `start` returns.
    voters[2].waits_until(30, since[2], |said| !startup_metrics(said).is_empty());
    let restarted = startup_metrics(voters[2].said().get(since[2]..).unwrap_or_default());
    let frontiers = restarted
        .first()
        .and_then(|m| m.pointer("/frontiers/Observed"))
        .unwrap_or_else(|| {
            panic!(
                "no startup metrics after the restart:\n{}",
                voters[2].said()
            )
        })
        .clone();
    assert_eq!(
        frontiers.get("projection_durable").is_some(),
        replay,
        "{frontiers}"
    );
    for (i, voter) in voters.iter().enumerate() {
        assert!(
            voter.waits_until(60, since[i], meshed),
            "voter {} did not get every link back after voter 3 returned:\n{}",
            i + 1,
            voter.said()
        );
    }

    // A restarted voter is stale until its leader confirms its store
    // again (#320).
    assert!(
        voters[2].waits_until(60, since[2], policy_fresh),
        "the returned voter's policy never turned fresh:\n{}",
        voters[2].said()
    );
    let mut returned = Caller::connect(&run, &provisioned, &minter, 2, 1)
        .await
        .expect("a caller bound at the returned voter");
    assert_eq!(
        returned
            .ask(&put(&provisioned, b"after"), Duration::from_secs(30))
            .await,
        Answer::Established,
        "the returned voter's frontend did not establish a request:\n-- 1 --\n{}\n-- 2 --\n{}\n-- 3 --\n{}",
        voters[0].said(),
        voters[1].said(),
        voters[2].said()
    );
    // And the write from before the kill is read through it, value and
    // all: a replay that restored nothing would still establish a read
    // of a missing key.
    assert_eq!(
        read_value(&mut returned, &provisioned, b"before").await,
        Some(b"placed".to_vec()),
        "the returned voter did not read back the write from before its kill:\n{}",
        voters[2].said()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A voter cut off from both others stops binding within its policy
/// allowance, and binds and serves again once it is back (#320
/// follow-up).
///
/// Voters 1 and 2 are stopped (`SIGSTOP`), not killed: nothing closes,
/// and voter 3 just hears nothing, as across a partition. Its freshness
/// asks go unanswered -- sent to a stopped leader, or, if it leads, with
/// no quorum to confirm its rounds -- so once the allowance after the
/// last ask it sent before the cut has run, its policy is stale: it says
/// so, and refuses a new binding as unavailable before it reads the
/// credential. Continued, the two answer again, and voter 3 turns fresh,
/// binds a caller and serves a read of the write from before the cut.
///
/// What a stale voter would otherwise have disclosed after a revoke is
/// not driven here: no daemon command revokes a grant, so the serve and
/// binding tests write the revoke into the store. This covers the cut
/// and the heal.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_off_voter_stops_binding_and_binds_again_once_healed() {
    // A short allowance, so the cut takes seconds; two of them, so a
    // voter linked and current never goes stale between asks on a
    // loaded runner.
    const ALLOWANCE_MILLIS: u64 = 2_000;
    if HOSTS.iter().any(|h| UdpSocket::bind((*h, 0)).is_err()) {
        eprintln!("skipped: {HOSTS:?} are not all assigned on this machine");
        return;
    }
    let dir = workspace("cut");
    let api = free_everywhere(&[]);
    let peer = free_everywhere(&[api]);
    let spec = HOSTS
        .iter()
        .enumerate()
        .map(|(i, host)| format!("n{}={host}:{api}:{peer}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let run = dir.join("run");
    let provisioned = provision(&Plan {
        hosts: parse_hosts(&spec).expect("a host list"),
        // Any limit writes the `[limits]` section the allowance goes in;
        // this one at the daemon's default.
        max_admitted_per_second: Some(1_000),
        ..Plan::loopback(run.clone(), 3, 0)
    })
    .expect("provisioned");
    let mut voters: Vec<Voter> = (1..=3)
        .map(|n| Voter {
            bundle: run.join(format!("n{n}")),
            daemon: None,
        })
        .collect();
    for voter in &voters {
        let path = voter.bundle.join("coordd.toml");
        let written = std::fs::read_to_string(&path).expect("the voter's configuration");
        let edited = written.replacen(
            "\n[limits]\n",
            &format!("\n[limits]\nmax_policy_staleness_millis = {ALLOWANCE_MILLIS}\n"),
            1,
        );
        assert_ne!(
            edited, written,
            "no [limits] section to put the allowance in"
        );
        let config = coord_daemon::config::Config::parse(&edited)
            .expect("a configuration the daemon accepts");
        assert_eq!(config.limits.max_policy_staleness_millis, ALLOWANCE_MILLIS);
        std::fs::write(&path, edited).expect("written");
    }
    for (i, voter) in voters.iter_mut().enumerate() {
        voter.start(i + 1, true);
    }
    for (i, voter) in voters.iter().enumerate() {
        assert!(
            voter.waits_until(60, 0, meshed),
            "voter {} did not reach every other voter:\n{}",
            i + 1,
            voter.said()
        );
        assert!(
            voter.waits_until(60, 0, policy_fresh),
            "voter {}'s policy never turned fresh:\n{}",
            i + 1,
            voter.said()
        );
    }
    let minter = coord_harness::issuer::Minter::of(&provisioned).expect("the minter");
    let mut before = Caller::connect(&run, &provisioned, &minter, 2, 0)
        .await
        .expect("a caller bound at voter 3");
    assert_eq!(
        before
            .ask(&put(&provisioned, b"before"), Duration::from_secs(30))
            .await,
        Answer::Established,
        "the domain did not establish a write"
    );
    assert_eq!(
        read_value(&mut before, &provisioned, b"before").await,
        Some(b"placed".to_vec()),
        "voter 3 did not serve the write before the cut:\n{}",
        voters[2].said()
    );

    // The cut. Whatever happens, the two are continued before anything
    // is asserted, so a failure leaves no stopped daemon behind.
    let signal = |voter: &Voter, signal: &str| {
        let pid = voter.daemon.as_ref().expect("running").pid();
        let status = std::process::Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .expect("kill runs");
        assert!(status.success(), "kill {signal} {pid}");
    };
    let since = voters[2].said().len();
    signal(&voters[0], "-STOP");
    signal(&voters[1], "-STOP");
    let stopped = std::time::Instant::now();
    let stale = |said: &str| !policy_fresh(said) && said.contains("this voter's policy is stale");
    let went_stale = voters[2].waits_until(30, since, stale);
    // Every stamp is an ask's send time, and every ask an answer could
    // still come back for was sent before the cut: past the allowance
    // after it, no answer opens the policy, whatever was in flight.
    let past = stopped + Duration::from_millis(ALLOWANCE_MILLIS + 250);
    if let Some(wait) = past.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(wait).await;
    }
    let cut_off = Caller::connect(&run, &provisioned, &minter, 2, 1).await;
    // The daemon says why it refused before it closes the link, but its
    // output reaches the log through the harness's reader and writer
    // threads, so the caller can see the close before the line is in the
    // file. Wait for the refusal to be said, whatever its reason, and
    // then read the reason.
    let _ = voters[2].waits_until(10, since, |said| said.contains("a binding was refused: "));
    let said_at_the_cut = voters[2].said().get(since..).unwrap_or_default().to_owned();
    signal(&voters[0], "-CONT");
    signal(&voters[1], "-CONT");
    assert!(
        went_stale,
        "the cut-off voter never said its policy is stale:\n{}",
        voters[2].said()
    );
    assert!(
        cut_off.is_err(),
        "a cut-off voter bound a caller:\n{said_at_the_cut}"
    );
    assert!(
        said_at_the_cut.contains("a binding was refused: Stale"),
        "the cut-off voter refused the binding for another reason:\n{said_at_the_cut}"
    );

    // The heal: the voter turns fresh again, binds, and serves.
    for (i, voter) in voters.iter().enumerate() {
        assert!(
            voter.waits_until(60, 0, meshed),
            "voter {} did not get its links back:\n{}",
            i + 1,
            voter.said()
        );
    }
    assert!(
        voters[2].waits_until(60, since, policy_fresh),
        "the healed voter's policy never turned fresh again:\n{}",
        voters[2].said()
    );
    let mut healed = Caller::connect(&run, &provisioned, &minter, 2, 2)
        .await
        .expect("a caller bound at the healed voter");
    assert_eq!(
        read_value(&mut healed, &provisioned, b"before").await,
        Some(b"placed".to_vec()),
        "the healed voter did not serve the write from before the cut:\n-- 1 --\n{}\n-- 2 --\n{}\n-- 3 --\n{}",
        voters[0].said(),
        voters[1].said(),
        voters[2].said()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// task-d55: a checkpoint interval the harness provisions is the one the
/// daemon reads, and the rest of the `[limits]` it has to write beside it
/// are the daemon's own defaults, so setting the one does not quietly set
/// the others.
#[test]
fn a_provisioned_checkpoint_interval_leaves_every_other_limit_at_its_default() {
    let dir = workspace("checkpoint-interval");
    for interval in [None, Some(0), Some(65_536)] {
        let run = dir.join(format!("{interval:?}"));
        let provisioned = provision(&Plan {
            checkpoint_after_records: interval,
            ..Plan::loopback(run.clone(), 3, 0)
        })
        .expect("provisioned");
        for node in &provisioned.voters {
            let config = coord_daemon::config::Config::parse(
                &std::fs::read_to_string(&node.config).expect("the voter's configuration"),
            )
            .expect("a configuration the daemon accepts");
            let defaults = coord_daemon::config::Limits::default();
            assert_eq!(
                config.limits,
                coord_daemon::config::Limits {
                    checkpoint_after_records: interval,
                    ..defaults
                }
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
