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
///
/// `voter_one_key` is that node's actual public key, because genesis
/// commits to the key and not merely to the name. Without it the node
/// starts -- nothing it does alone checks the key -- and then no peer
/// will accept it, which is the failure the committed configuration
/// exists to cause.
fn genesis(dir: &Path, voter_one_key: Option<&[u8]>) {
    genesis_of(dir, 3, voter_one_key);
}

/// The same manifest with `count` committed voters.
///
/// One voter is a real configuration, not a shortcut: its quorum is
/// itself, so a single process can carry a request all the way through
/// consensus and back. Three is what says that one running voter is not
/// a quorum -- the same code, a different agreement.
fn genesis_of(dir: &Path, count: u8, voter_one_key: Option<&[u8]>) {
    let voters: Vec<serde_json::Value> = (1u8..=count)
        .map(|n| {
            let key = match (n, voter_one_key) {
                (1, Some(spki)) => b64url(spki),
                _ => b64url(&[n; 32]),
            };
            serde_json::json!({
                "node": hex(&[n; 16]),
                "incarnation": 1,
                "public_key": key,
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

/// The domain's certificate authority: one root, everything else issued
/// under it, as a real deployment has it.
///
/// A self-signed leaf would do for the node's own startup checks and for
/// nothing else: a caller cannot be issued a certificate the node
/// trusts, so nothing could ever connect to it. A test that only ever
/// started the daemon would not have noticed.
pub struct Ca {
    key: rcgen::KeyPair,
    certificate: rcgen::Certificate,
    /// The node certificate's SubjectPublicKeyInfo, which is what
    /// genesis commits to for a voter.
    node_spki: Vec<u8>,
}

impl Ca {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key).expect("ca cert");
        Ca {
            key,
            certificate,
            node_spki: Vec::new(),
        }
    }

    /// Issue a certificate carrying the node-identity URI SAN the issuer
    /// binds, because that -- not a setting -- is what says which
    /// replica a process is.
    fn issue(
        &self,
        name: &str,
        cluster: [u8; 16],
        replica: u8,
        role: coord_types::wire_v1::PeerRole,
    ) -> (rcgen::Certificate, rcgen::KeyPair) {
        let identity = coord_node_issuer::NodeIdentity {
            cluster: coord_types::ids::ClusterId(cluster),
            node: coord_types::ids::ReplicaId([replica; 16]),
            incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
            role,
        };
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
        let mut params =
            rcgen::CertificateParams::new(vec![name.to_string()]).expect("leaf params");
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.subject_alt_names = vec![
            rcgen::SanType::DnsName(name.try_into().expect("dns name")),
            rcgen::SanType::URI(
                coord_node_issuer::node_uri(&identity)
                    .try_into()
                    .expect("uri"),
            ),
        ];
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let issuer = rcgen::Issuer::from_params(&ca_params, &self.key);
        let certificate = params.signed_by(&key, &issuer).expect("leaf cert");
        (certificate, key)
    }

    fn root_pem(&self) -> String {
        pem("CERTIFICATE", self.certificate.der())
    }
}

/// This node's credentials, issued under the domain's authority.
fn credentials(dir: &Path, replica: u8, role: coord_types::wire_v1::PeerRole) -> Ca {
    credentials_of_cluster(dir, CLUSTER, replica, role)
}

fn credentials_of_cluster(
    dir: &Path,
    cluster: [u8; 16],
    replica: u8,
    role: coord_types::wire_v1::PeerRole,
) -> Ca {
    let mut ca = Ca::new();
    let (certificate, key) = ca.issue(SERVER_NAME, cluster, replica, role);
    ca.node_spki = spki_of(certificate.der());
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", certificate.der())).expect("cert");
    std::fs::write(dir.join("roots.pem"), ca.root_pem()).expect("roots");
    let key_path = dir.join("node.key");
    std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    ca
}

/// The name peers connect to this node by.
const SERVER_NAME: &str = "node.coordd.test";

/// The issuer's published keys, as a frontend reads them at startup.
///
/// A real key ring rather than a stub: the frontend parses these into
/// the verifier it will hold, so a shape it would reject at runtime has
/// to be rejected here too.
fn sts_keys(dir: &Path) -> coord_sts::KeyRing {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("sts key");
    let ring = coord_sts::KeyRing::new(
        coord_sts::SigningKey::from_pkcs8_der("coordd-test-1", &key.serialize_der())
            .expect("signing key"),
    );
    std::fs::write(
        dir.join("sts-jwks.json"),
        serde_json::to_vec_pretty(&ring.jwks()).expect("jwks"),
    )
    .expect("write jwks");
    ring
}

/// A token this domain's frontend will accept, signed by the keys it
/// reads at startup.
fn service_token(ring: &coord_sts::KeyRing, session: [u8; 16]) -> String {
    let issued = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs();
    ring.sign(&coord_sts::ServiceClaims {
        iss: "https://sts.test".into(),
        sub: hex(&[0xa; 16]),
        aud: "control-plane-test".into(),
        sid: hex(&session),
        scope: 0xffff,
        rule: hex(&session),
        generation: 1,
        jti: hex(&[2u8; 32]),
        iat: issued,
        exp: issued + 3600,
    })
    .expect("signing")
}

/// A configuration whose listeners are ephemeral loopback ports, so the
/// test never depends on a fixed port or on IPv6 being available.
fn config(dir: &Path) -> PathBuf {
    let ca = credentials(dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis(dir, Some(&ca.node_spki));
    let _ = sts_keys(dir);
    config_only(dir)
}

/// Just the TOML, for a caller that wrote the fixture itself.
fn config_only(dir: &Path) -> PathBuf {
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

[sts]
issuer = "https://sts.test"
resource = "control-plane-test"
jwks = "{root}/sts-jwks.json"
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
        let mut live = false;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            live = line.contains("phase=live");
            report.push_str(&line);
            report.push('\n');
            if live {
                break;
            }
        }
        // Reaching the end of its output is not reaching a serving
        // state: a daemon that printed most of a startup report and then
        // died would otherwise look like one that came up.
        let _ = tx.send(live.then_some(report));
    });
    let report = rx
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| {
            let _ = child.kill();
            panic!("coordd did not reach a serving state within 30s")
        });
    let _ = child.kill();
    let output = child.wait_with_output().expect("output");
    report.unwrap_or_else(|| {
        panic!(
            "coordd stopped before it was serving:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
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

/// A running daemon, with the address it is serving on, ended on drop.
struct Running {
    child: std::process::Child,
    api: std::net::SocketAddr,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start `coordd` and leave it serving.
fn start(config: &Path) -> Running {
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
        let mut api = None;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(rest) = line.strip_prefix("listening api_quic=") {
                api = rest.parse::<std::net::SocketAddr>().ok();
            }
            if line.contains("phase=live") {
                break;
            }
        }
        let _ = tx.send(api);
    });
    let api = rx
        .recv_timeout(Duration::from_secs(30))
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("coordd did not report a serving api listener within 30s")
        });
    Running { child, api }
}

/// A caller with a bound session against a running daemon.
///
/// Everything here is real: the QUIC handshake against the certificate
/// the daemon loaded from disk, the peer binder built from the committed
/// membership, and the session token verified against the keys the
/// daemon read at startup. None of it is stubbed, and each piece would
/// fail the handshake or the binding on its own if it were not wired.
///
/// It dials with quinn directly rather than through `Transport`.
/// `Transport::send` opens a bidirectional stream and drops the
/// receiving half, so it cannot read a unary answer: it is the path a
/// frontend uses to deliver *to* a node it dialed, not the path a caller
/// uses to ask one a question. The caller's shape -- one stream, the
/// request written, the answer read on it -- is what the Go client
/// implements and what the daemon's responder answers on, so that is
/// what this drives. Giving the Rust SDK that shape is its own task.
struct Caller {
    connection: quinn::Connection,
    session: coord_types::ids::SessionId,
    _endpoint: quinn::Endpoint,
    // Kept alive: dropping the control stream ends the negotiation the
    // daemon bound this connection under.
    _control: quinn::SendStream,
}

impl Caller {
    /// Dial `daemon`, negotiate as a client of this domain, and bind
    /// `session` with a token the daemon's own keys verify.
    async fn bind(daemon: &Running, ca: &Ca, ring: &coord_sts::KeyRing, session: [u8; 16]) -> Self {
        use coord_types::wire_v1::PeerRole;

        let (certificate, key) = ca.issue("caller.coordd.test", CLUSTER, 0x0c, PeerRole::Client);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls_pki_types::CertificateDer::from(
                ca.certificate.der().to_vec(),
            ))
            .expect("ca root");
        let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("tls13")
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![rustls_pki_types::CertificateDer::from(
                    certificate.der().to_vec(),
                )],
                rustls_pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .expect("client auth");
        tls.alpn_protocols = vec![coord_transport::ALPN_API.to_vec()];
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().expect("loopback"))
            .expect("client endpoint");
        endpoint.set_default_client_config(quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("quic tls"),
        )));
        let connection = endpoint
            .connect(daemon.api, SERVER_NAME)
            .expect("dialable")
            .await
            .expect("the caller reached the daemon");

        // Negotiation: the control stream carries the hello that
        // declares what this caller is, which the daemon binds against
        // its committed membership before anything else happens.
        let (mut control, _control_recv) = connection.open_bi().await.expect("control stream");
        let hello = coord_types::wire_v1::MessageV1::Hello(coord_types::wire_v1::HelloV1 {
            role: PeerRole::Client,
            cluster_id: coord_types::ids::ClusterId(CLUSTER),
            domain_id: coord_types::ids::DomainId(DOMAIN),
            incarnation: None,
            capabilities: coord_types::wire_v1::BoundedVec::new(vec![
                coord_transport::Lane::Unary.capability(),
            ])
            .expect("bounded"),
        })
        .encode()
        .expect("hello");
        control.write_all(&hello).await.expect("hello written");

        let token = service_token(ring, session);
        let bind = coord_session::bind_frame(token.as_bytes()).expect("bind frame");
        let answer = ask(&connection, &bind)
            .await
            .expect("the daemon answered the binding within the bound");
        let ack = coord_session::decode_bind_ack(&answer).expect("a binding acknowledgement");
        assert_eq!(
            ack.session,
            coord_types::ids::SessionId(session),
            "the daemon bound a different session than the token named"
        );
        assert!(ack.expires_at > 0, "a binding with no validity");

        Caller {
            connection,
            session: ack.session,
            _endpoint: endpoint,
            _control: control,
        }
    }

    /// The retry key of this caller's `sequence`th invocation.
    fn invocation(&self, sequence: u64) -> coord_types::RetryKey {
        coord_types::RetryKey {
            cluster_id: coord_types::ids::ClusterId(CLUSTER),
            domain_id: coord_types::ids::DomainId(DOMAIN),
            session_id: self.session,
            client_instance_id: coord_types::ids::ClientInstanceId([0x0c; 16]),
            request_sequence: coord_types::ids::RequestSequence::new(sequence).expect("nonzero"),
        }
    }

    /// One `Put`, as a client would send it.
    fn put(&self, sequence: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            coord_types::ids::NamespaceId([0x5e; 16]),
            coord_types::logical_v1::CanonicalOperation::Put(coord_types::logical_v1::PutOp {
                key: key.to_vec(),
                value: value.to_vec(),
                lease: None,
                prev_kv: false,
            }),
        );
        logical.canonicalize();
        coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(self.invocation(sequence), &logical, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded")
    }
}

/// Write `frame` on a fresh stream and read the answer the daemon
/// writes back on it, or `None` if it never comes.
async fn ask(connection: &quinn::Connection, frame: &[u8]) -> Option<coord_types::wire_v1::Frame> {
    let (mut send, mut recv) = connection.open_bi().await.expect("request stream");
    send.write_all(frame).await.expect("written");
    send.finish().expect("finished");
    let bytes = tokio::time::timeout(Duration::from_secs(20), async {
        let mut bytes = Vec::new();
        let mut buf = [0u8; 4096];
        while let Some(n) = recv.read(&mut buf).await.expect("readable") {
            bytes.extend_from_slice(&buf[..n]);
        }
        bytes
    })
    .await
    .ok()?;
    if bytes.is_empty() {
        return None;
    }
    let mut reader = coord_types::wire_v1::FrameReader::new();
    reader.push(&bytes).expect("within the reader bound");
    reader.next_frame().expect("a frame")
}

/// A caller binds a session against the running daemon, and the daemon
/// answers from the composition it actually has.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_binds_a_session_against_the_running_daemon() {
    let dir = workspace("bind");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis(&dir, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);

    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    assert_eq!(caller.session, coord_types::ids::SessionId([0x44; 16]));
}

/// A request is carried all the way through by the voter in this
/// process, and the caller is answered on the stream it asked on.
///
/// This is the served-request gate, and it is a different claim from
/// the binding above. A `Bind` is answered by the frontend alone; this
/// is answered only if every stage happened: the frontend admitted it
/// and planned a fan-out, the plan resolved to the voter running here
/// and went into its ingress rather than onto a socket, the voter
/// proposed it, the record became durable, the evidence reached the
/// collector under this voter's committed identity and met the
/// committed quorum rule, the command was executed and materialized,
/// and the release was gated and written back.
///
/// The configuration commits one voter, so that quorum is this voter --
/// a real agreement of a real configuration, not a bypass. The next test
/// commits three and shows that one of them is not a quorum, with the
/// same code.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_is_served_end_to_end_by_the_voter_in_this_process() {
    let dir = workspace("served");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;

    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .expect("the daemon never answered the request");
    let decoded = coord_types::wire_v1::decode(&answer).expect("a decodable answer");
    let coord_types::wire_v1::MessageV1::Response(response) = decoded else {
        panic!("the daemon answered a request with something else: {decoded:?}");
    };
    // The answer is about the command the caller asked for, which is
    // the one identity the whole path is keyed by: the collector
    // retained it under this retry key, the voter proposed and executed
    // exactly it, and the release was matched back to the stream that
    // is holding.
    //
    // Its *outcome* is whatever this cluster's committed policy says
    // for a session with no rules written for it yet, and that is not
    // what this test is about. What it holds is that an answer came
    // back at all, addressed to this command, which it does only if
    // every stage above happened.
    let mut logical = coord_types::logical_v1::LogicalRequest::new(
        coord_types::ids::NamespaceId([0x5e; 16]),
        coord_types::logical_v1::CanonicalOperation::Put(coord_types::logical_v1::PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    assert_eq!(
        response.command_id,
        coord_types::CommandId::derive(&caller.invocation(1), &logical).expect("derivable"),
        "the daemon answered with some other command's result"
    );

    // And it is the result the *replicated* execution produced, decoded
    // here rather than taken on trust.
    //
    // It is a rejection, and the right one. Nothing in this build writes
    // the session row a command's execution authorizes against: a
    // session is established by a replicated command, and driving that
    // from the bind is its own task (see the plan's task-j09). Until it
    // exists, every command any cluster this binary starts can execute
    // is refused at execution with `SessionInvalid` -- which is the
    // correct fail-closed answer, and is still a full trip through the
    // machinery above.
    let coord_types::wire_v1::OutcomeV1::Ok { result, .. } = &response.outcome else {
        panic!("the daemon answered with a transport-level error: {response:?}");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(
        executed.outcome,
        coord_state::Outcome::ErrRejected {
            reason: coord_state::RejectionReason::SessionInvalid
        },
        "the session row is not written by anything yet; see task-j09"
    );
}

/// The same invocation is answered the same way after a restart.
///
/// The daemon stops, the process that held the collector's retained
/// results and every in-memory command goes with it, and the next
/// process comes up on the store the first one left: it recovers, it
/// serves, and the same invocation gets the same answer.
///
/// What this does *not* yet hold is retry resolution from the durable
/// record. The command here is refused at execution, and a semantic
/// admission refusal deliberately records nothing under the retry key
/// -- so there is nothing retained to resolve to, and the second trip
/// re-derives the same refusal rather than finding it. Holding the
/// resolution path needs a command that executes, which needs the
/// session row nothing writes yet (task-j09). Saying so is more use
/// than a test that named a property it does not check.
///
/// The restart is ordinary: the process is stopped and started again.
/// That is integration evidence and not a durability qualification --
/// what a torn write or a lost fsync does is task-j05's, and this says
/// nothing about it.
#[tokio::test(flavor = "multi_thread")]
async fn the_same_invocation_is_answered_the_same_way_after_a_restart() {
    let dir = workspace("retry");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
        let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .expect("the daemon answered the request");
        response_of(&answer)
    };

    // A different process, the same store, and the same invocation. The
    // collector that retained this result in memory is gone with the
    // process that held it; what answers now is the record the command
    // left behind.
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .expect("the daemon answered the retry");
    let again = response_of(&answer);

    assert_eq!(
        again.command_id, first.command_id,
        "the same invocation became a different command after a restart"
    );
    assert_eq!(
        again.outcome, first.outcome,
        "the same invocation was answered differently by the next process"
    );
}

/// A process that cannot verify a caller does not come up saying it can.
///
/// A JWKS that is valid JSON and holds no key this build can verify with
/// refuses every caller. A daemon that treated "the file parsed" as "the
/// verifier is configured" would bind its listeners, report itself live,
/// and reject every connection -- which from outside is indistinguishable
/// from a client problem, and is the most expensive kind of
/// misconfiguration to find.
#[test]
fn a_frontend_that_cannot_verify_a_caller_refuses_to_start() {
    let dir = workspace("nokeys");
    let path = config(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    // Valid JSON, a well-formed key set, and not one key in it.
    std::fs::write(
        dir.join("sts-jwks.json"),
        serde_json::to_vec(&serde_json::json!({ "keys": [] })).expect("jwks"),
    )
    .expect("write");
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("verify a caller"),
        "the refusal did not say what was unusable: {}",
        refused.err
    );
    // And it stopped before it bound anything: a listener that came up
    // is a listener something could connect to.
    assert!(
        !refused.out.contains("listening"),
        "a daemon that could verify nobody bound a listener anyway: {}",
        refused.out
    );

    // A key with a `kid` but no usable public point is no better, and
    // fails for the same reason rather than by parsing differently.
    std::fs::write(
        dir.join("sts-jwks.json"),
        serde_json::to_vec(&serde_json::json!({
            "keys": [{ "kid": "one", "kty": "EC", "crv": "P-256" }]
        }))
        .expect("jwks"),
    )
    .expect("write");
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(refused.err.contains("verify a caller"), "{}", refused.err);
}

/// The response a frame carries.
fn response_of(frame: &coord_types::wire_v1::Frame) -> coord_types::wire_v1::ResponseV1 {
    match coord_types::wire_v1::decode(frame).expect("a decodable answer") {
        coord_types::wire_v1::MessageV1::Response(r) => r,
        other => panic!("the daemon answered a request with something else: {other:?}"),
    }
}

/// One voter in this process is one voter, not a quorum.
///
/// The same request, the same code, the same local route -- and three
/// committed voters instead of one. The frame reaches this node's voter
/// without a network hop, the voter proposes it, and the collector
/// counts exactly one contribution: its own. Nothing is released,
/// because nothing has agreed.
///
/// A co-located voter that pre-counted itself, or that let the frontend
/// treat a queued frame as an acknowledgement, would answer this caller.
#[tokio::test(flavor = "multi_thread")]
async fn one_co_located_voter_is_not_a_quorum() {
    let dir = workspace("noquorum");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 3, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;

    let (mut send, mut recv) = caller.connection.open_bi().await.expect("request stream");
    send.write_all(&caller.put(1, b"k", b"v"))
        .await
        .expect("written");
    send.finish().expect("finished");

    // Held, not answered and not closed. The caller is waiting on
    // evidence from two voters that are not running, which is the
    // correct thing for it to be waiting on.
    let mut buf = [0u8; 4096];
    let early = tokio::time::timeout(Duration::from_secs(3), recv.read(&mut buf)).await;
    assert!(
        early.is_err(),
        "one voter answered for a quorum of three: {early:?}"
    );
}

/// A certificate's SubjectPublicKeyInfo, as the binder reads it.
fn spki_of(der: &[u8]) -> Vec<u8> {
    use x509_parser::prelude::FromDer;
    let (_, x509) =
        x509_parser::certificate::X509Certificate::from_der(der).expect("a parseable certificate");
    x509.public_key().raw.to_vec()
}

/// Genesis commits to a voter's key, not merely to its name, and a node
/// holding some other key finds out at startup.
///
/// Without this it starts perfectly well -- nothing it does alone checks
/// the key -- and is then refused by every peer it meets, which looks
/// like a network problem. It is this node's own problem, it is knowable
/// before anything is served, and the peers are right to refuse it.
#[test]
fn a_voter_whose_key_the_configuration_does_not_commit_to_stops_at_startup() {
    let dir = workspace("wrongkey");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    let _ = sts_keys(&dir);
    // The manifest names this node as a voter and commits to some other
    // key.
    genesis(&dir, None);
    let path = config_only(&dir);

    let refused = run(&path, &["--check"]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("different key"),
        "the refusal did not name the mismatch: {}",
        refused.err
    );
    assert!(
        !dir.join("state").exists() && !dir.join("journal").exists(),
        "a node with an uncommitted key opened durable storage"
    );

    // The same node, with the manifest committing to the key it holds.
    genesis(&dir, Some(&ca.node_spki));
    let accepted = run(&path, &["--check"]);
    assert_eq!(accepted.code, Some(0), "{}{}", accepted.out, accepted.err);
}
