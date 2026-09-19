//! Acceptance for the daemon's own command surface (task-43).
//!
//! These drive the built binary, because what they hold is true of the
//! process and not of a function: which command creates a store, which
//! refuses to, and what a process does before it accepts a connection.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

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
    path.push(format!("coordd-cli-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("workspace");
    path
}

const CLUSTER: [u8; 16] = [0x11; 16];
const DOMAIN: [u8; 16] = [0x22; 16];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
    }
    out
}

fn pem(label: &str, der: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut body = String::new();
    for chunk in der.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                body.push(A[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            } else {
                body.push('=');
            }
        }
    }
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// The genesis manifest this domain agrees on: three voters, of which
/// the first is the node under test.
fn genesis(dir: &Path) {
    let voters: Vec<serde_json::Value> = (1u8..=3)
        .map(|n| {
            serde_json::json!({
                "node": hex(&[n; 16]),
                "incarnation": 1,
                "public_key": b64url(&[n; 32]),
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "cluster": hex(&CLUSTER),
        "domain": hex(&DOMAIN),
        "epoch": 1,
        "voters": voters,
        "issuer_roots": [b64url(&[0xca; 8])],
        "wif_rules": [{ "issuer": "test" }],
        "admin": hex(&[0xa; 16]),
        "protocol_version": 1,
    });
    std::fs::write(
        dir.join("genesis.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest"),
    )
    .expect("write manifest");
}

/// This node's credentials: a certificate carrying the node-identity URI
/// SAN the issuer binds, because that -- not a setting -- is what says
/// which replica a process is. It is issued by an authority the trust
/// bundle holds, because a certificate nothing trusted issued is not an
/// identity at all.
fn credentials(dir: &Path, replica: u8, role: coord_types::wire_v1::PeerRole) {
    let (authority, issuer) = authority();
    issue(dir, &issuer, replica, role);
    std::fs::write(dir.join("roots.pem"), pem("CERTIFICATE", authority.der())).expect("roots");
}

/// An issuing authority: its certificate, for a trust bundle, and the
/// issuer that signs with it.
fn authority() -> (rcgen::Certificate, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let certificate = params.self_signed(&key).expect("ca");
    (certificate, rcgen::Issuer::new(params, key))
}

/// Write a node certificate and key `issuer` signed.
fn issue(
    dir: &Path,
    issuer: &rcgen::Issuer<'static, rcgen::KeyPair>,
    replica: u8,
    role: coord_types::wire_v1::PeerRole,
) {
    let identity = coord_node_issuer::NodeIdentity {
        cluster: coord_types::ids::ClusterId(CLUSTER),
        node: coord_types::ids::ReplicaId([replica; 16]),
        incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
        role,
    };
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    params.subject_alt_names = vec![rcgen::SanType::URI(
        coord_node_issuer::node_uri(&identity)
            .try_into()
            .expect("uri"),
    )];
    let certificate = params.signed_by(&key, issuer).expect("issued");

    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", certificate.der())).expect("cert");
    let key_path = dir.join("node.key");
    let _ = std::fs::remove_file(&key_path);
    std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

/// A configuration whose listeners are ephemeral loopback ports, so the
/// test never depends on a fixed port or on IPv6 being available.
fn config(dir: &Path) -> PathBuf {
    genesis(dir);
    credentials(dir, 1, coord_types::wire_v1::PeerRole::Voter);
    let text = format!(
        r#"config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
domain = "control-plane-test"
state_directory = "{root}"

[listen]
api_quic = "127.0.0.1:0"
peer_quic = "127.0.0.1:0"

[capability]
writer_queue_bytes = 16777216
buffer_bytes_per_subscription = 8388608
max_live_subscriptions = 4096

[state]
root = "state"

[journal]
root = "journal"
shards = 1

[identity]
trust_bundle = "{root}/roots.pem"
node_certificate = "{root}/node.pem"
node_key = "{root}/node.key"
"#,
        root = dir.display()
    );
    let path = dir.join("coordd.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

struct Run {
    code: Option<i32>,
    out: String,
    err: String,
}

/// Run `coordd` and wait for it to finish, within a bound.
///
/// The bound is not a convenience. Every case here is one where the
/// daemon must *stop*, and the way each of them regresses is that the
/// daemon starts instead -- at which point it holds its listeners and
/// parks, for ever. Without a bound the regression is a test suite that
/// hangs rather than one that fails, and a hang says nothing about
/// which case broke.
fn run(config: &Path, args: &[&str]) -> Run {
    let mut child = Command::new(binary())
        .arg("--config")
        .arg(config)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("coordd started");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().expect("wait") {
            Some(_) => break,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("coordd {args:?} did not stop within 30s: it started instead of refusing");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let output = child.wait_with_output().expect("output");
    Run {
        code: output.status.code(),
        out: String::from_utf8_lossy(&output.stdout).into_owned(),
        err: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// A node serves the store it already has. Finding none is reported --
/// never repaired.
///
/// This is the guard every other one is downstream of. A daemon that
/// created a store when it failed to find one would turn a lost disk, an
/// unmounted volume or a mistyped path into a fresh, empty, *valid*
/// node, which would then vote, having forgotten everything it had ever
/// promised.
#[test]
fn a_missing_store_stops_the_daemon_rather_than_being_created() {
    let dir = workspace("missing");
    let config = config(&dir);

    let refused = run(&config, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("no store at"),
        "the refusal did not say what was missing: {}",
        refused.err
    );
    // And it says what to do, because the right answer differs: a
    // genuinely new node is initialized, an existing one is found.
    assert!(refused.err.contains("coordd init"));
    // A failed start creates nothing at all -- not the projection, and
    // not the journal that would be its authority. Either one left
    // behind is a node that looks initialized to the next start.
    assert!(
        !dir.join("state").exists(),
        "a failed start created a projection anyway"
    );
    assert!(
        !dir.join("journal").exists(),
        "a failed start created a journal anyway"
    );
}

/// Initialization is its own command, it works once, and a second one is
/// refused: running it against a node that already has a store would
/// give that node an empty history under its own identity.
#[test]
fn initializing_is_deliberate_and_happens_exactly_once() {
    let dir = workspace("init");
    let config = config(&dir);

    let first = run(&config, &["init"]);
    assert_eq!(first.code, Some(0), "{}{}", first.out, first.err);
    assert!(
        first.out.contains("gen-000001"),
        "initialization did not say what it made: {}",
        first.out
    );
    assert!(dir.join("state").join("gen-000001").is_dir());

    let second = run(&config, &["init"]);
    assert_eq!(second.code, Some(2));
    assert!(
        second.err.contains("already exists"),
        "a second initialization was not refused: {}",
        second.err
    );
    // The first generation is untouched: the refusal is a refusal, not a
    // partial replacement.
    assert!(dir.join("state").join("gen-000001").is_dir());
}

/// A configuration a node cannot serve is refused under `--check` --
/// which is where an operator would want to find out, rather than at
/// the first start in production.
#[test]
fn a_configuration_this_build_cannot_serve_is_refused_under_check() {
    let dir = workspace("profile");
    let path = config(&dir);
    let text = std::fs::read_to_string(&path).expect("read");

    // The experimental engine is built and tested, so naming it is a
    // deliberate act rather than a typo. It carries no production
    // support, so it is refused all the same.
    std::fs::write(
        &path,
        text.replace("[state]\nroot", "[state]\nengine = \"fjall\"\nroot"),
    )
    .expect("write");
    let checked = run(&path, &["--check"]);
    assert_eq!(checked.code, Some(2), "{}{}", checked.out, checked.err);
    assert!(
        checked.err.contains("ExperimentalEngine"),
        "the refusal did not name the engine: {}",
        checked.err
    );

    // Without it, the same configuration checks out.
    std::fs::write(&path, text).expect("restore");
    assert_eq!(run(&path, &["--check"]).code, Some(0));
}

/// `--check` validates and does nothing else: it does not create a
/// store, and it does not bind a listener. An operator runs it on a
/// machine that is not meant to be serving.
#[test]
fn checking_a_configuration_touches_nothing() {
    let dir = workspace("check");
    let config = config(&dir);

    let checked = run(&config, &["--check"]);
    assert_eq!(checked.code, Some(0), "{}{}", checked.out, checked.err);
    assert!(
        !dir.join("state").exists(),
        "--check created a store: {}",
        checked.out
    );
    assert!(
        !checked.out.contains("listening"),
        "--check bound a listener: {}",
        checked.out
    );
}

/// A node's identity comes from its own certificate, not from a setting,
/// and the committed configuration decides whether that identity may
/// vote.
///
/// A node that could be told who it was could be told it was somebody
/// else: two processes configured with the same replica identity would
/// each open that replica's store, each vote under it, and between them
/// break the one thing a replica promises. A certificate cannot be
/// handed round that way, because the peers that matter check it.
#[test]
fn a_node_that_is_not_a_committed_voter_does_not_vote() {
    let dir = workspace("stranger");
    let path = config(&dir);

    // The configured role votes, and the certificate names a replica the
    // genesis does name: this is the node it claims to be.
    assert_eq!(run(&path, &["--check"]).code, Some(0));

    // Re-issued for a replica the committed configuration does not name.
    credentials(&dir, 9, coord_types::wire_v1::PeerRole::Voter);
    let refused = run(&path, &["--check"]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("does not name"),
        "the refusal did not say why: {}",
        refused.err
    );

    // A process that does not vote needs only to be of this cluster, so
    // the same certificate serves an observer.
    let text = std::fs::read_to_string(&path).expect("read");
    std::fs::write(
        &path,
        text.replace(
            "role = \"voter-frontend-observer\"",
            "role = \"frontend-observer\"",
        ),
    )
    .expect("write");
    let observer = run(&path, &["--check"]);
    assert_eq!(observer.code, Some(0), "{}{}", observer.out, observer.err);
}

/// A certificate that says nothing about which replica it is, is not an
/// identity. The refusal happens before the store is opened, because a
/// process that got that far would already have taken a replica's store
/// under a name nothing vouched for.
#[test]
fn a_certificate_without_a_node_identity_is_not_an_identity() {
    let dir = workspace("anonymous");
    let path = config(&dir);

    // A perfectly good certificate that simply carries no node URI.
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut params = rcgen::CertificateParams::new(vec!["node.local".to_string()]).expect("params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let anonymous = params.self_signed(&key).expect("self-signed");
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", anonymous.der())).expect("cert");

    let refused = run(&path, &["--check"]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("no node identity"),
        "the refusal did not say what was missing: {}",
        refused.err
    );
    assert!(
        !dir.join("state").exists(),
        "a node with no identity opened a store"
    );
}

/// A certificate of another cluster is refused here, not only at the
/// first handshake: a node that got past this would open this domain's
/// store under its own identity before any peer ever saw its
/// certificate.
#[test]
fn a_certificate_of_another_cluster_never_reaches_this_domains_store() {
    let dir = workspace("foreign");
    let path = config(&dir);

    // Same node identity shape, different cluster.
    let identity = coord_node_issuer::NodeIdentity {
        cluster: coord_types::ids::ClusterId([0x77; 16]),
        node: coord_types::ids::ReplicaId([1; 16]),
        incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
        role: coord_types::wire_v1::PeerRole::Voter,
    };
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.subject_alt_names = vec![rcgen::SanType::URI(
        coord_node_issuer::node_uri(&identity)
            .try_into()
            .expect("uri"),
    )];
    let foreign = params.self_signed(&key).expect("self-signed");
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", foreign.der())).expect("cert");

    let refused = run(&path, &["init"]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("another cluster"),
        "the refusal did not name the mismatch: {}",
        refused.err
    );
    assert!(
        !dir.join("state").exists(),
        "a foreign node initialized this domain's store"
    );
}

/// Start `coordd` and read its startup report, then stop it.
///
/// A serving daemon does not exit, so the smoke test reads what it says
/// about itself on the way up and then ends it. Reading is bounded: a
/// daemon that never reaches `live` is a failure to report, not a test
/// to hang.
fn start_and_report(config: &Path) -> String {
    use std::io::{BufRead, BufReader};

    let mut child = Command::new(binary())
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("coordd started");
    let stdout = child.stdout.take().expect("piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut report = String::new();
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let live = line.contains("phase=live");
            report.push_str(&line);
            report.push('\n');
            if live {
                break;
            }
        }
        let _ = tx.send(report);
    });
    let report = rx
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| {
            let _ = child.kill();
            panic!("coordd did not reach a serving state within 30s")
        });
    let _ = child.kill();
    let _ = child.wait();
    report
}

/// The serving path opens the shared journal, attaches this domain's
/// projection to it, and replays whatever the projection owed -- and a
/// restart picks up exactly that state rather than a fresh one.
///
/// This is the composition's own evidence: attaching is where the
/// journal's frontier and the projection's are checked against each
/// other, so a daemon that reports a journaled frontier and nothing owed
/// has actually been through it. It is integration evidence and not a
/// durability qualification; task-j05 owns that, under real filesystem
/// and power-loss faults.
#[test]
fn the_serving_path_opens_the_journal_and_survives_a_restart() {
    let dir = workspace("restart");
    let path = config(&dir);

    let initialized = run(&path, &["init"]);
    assert_eq!(initialized.code, Some(0), "{}", initialized.err);
    assert!(
        dir.join("journal").is_dir(),
        "initialization made no journal: the serving profile has no authority"
    );

    let first = start_and_report(&path);
    assert!(
        first.contains("owed=0"),
        "the projection was left owing the journal: {first}"
    );
    let frontier = |report: &str| {
        report
            .lines()
            .find(|l| l.starts_with("storage "))
            .and_then(|l| {
                l.split("journaled_through=Some(")
                    .nth(1)?
                    .split(')')
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or_else(|| panic!("no journal frontier reported: {report}"))
    };
    let before = frontier(&first);
    assert!(before > 0, "the journal recorded nothing: {first}");

    // The same node, started again. It opens what it had: the frontier
    // does not go backwards, nothing is owed, and it did not quietly
    // make itself a new store.
    let second = start_and_report(&path);
    let after = frontier(&second);
    assert!(
        after >= before,
        "the journal frontier went backwards across a restart: {before} then {after}"
    );
    assert!(second.contains("owed=0"), "{second}");
    assert!(
        second.contains("gen-000001"),
        "the restart did not reopen the generation it had: {second}"
    );
    assert!(
        !dir.join("state").join("gen-000002").exists(),
        "a restart created a second generation"
    );
}
